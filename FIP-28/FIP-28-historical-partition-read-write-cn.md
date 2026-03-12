# FIP-28：支持过期分区的写入与查询

## 一、问题背景

Fluss 的自动分区表（主键表 / Log 表）开启 lake tiering 后，旧分区会被自动过期。过期后出现两个问题：

1. **写入割裂**：迟到的数据写到已过期分区会被拒绝
2. **查询割裂**（主键表）：对已过期分区做 point lookup 返回 null，即使数据在湖里还在

用户视角很矛盾：湖上批读还能看到数据，但 Fluss 在线路径却读写不了。

**适用范围**：仅支持开启 Paimon lake storage 的表。主键表有硬依赖——历史写入需要从湖里查旧值来生成正确的 changelog，没有湖后端就做不了。Log 表虽然技术上不依赖湖（追加写），但为了一致的用户体验，也限定在 lake-enabled 表上。

## 二、核心思路

引入一个特殊分区 `__historical__`，作为所有已过期分区的统一写入/查询入口：

```text
                    分区存在？
                       │
              ┌───Yes──┴──No───┐
              ▼                ▼
         正常分区写入     重定向到 __historical__
         正常分区查询     通过 __historical__ 做 lake 回查
```

- `__historical__` 常驻，不会被自动过期，正常复制和 WAL
- 写入时 row 数据不变，原始分区信息保留在 partition columns 中

## 三、RPC 扩展

写入和查询都复用现有 RPC，不引入新的 RPC 类型。扩展方式一致：在 per-bucket 请求中加可选的 `partition_name` 字段，携带原始分区名。

- **PutKv**：`PbPutKvReqForBucket` 新增 `optional string partition_name`，用于 key-only delete（`row == null`）时 server 端确定原始分区
- **Lookup**：`PbLookupReqForBucket` 新增 `optional string partition_name`，用于 server 端识别需要查询哪个湖分区

Server 端统一根据目标是否 `__historical__` 分区来决定处理方式（同步本地 vs 异步 lake I/O），不需要在 RPC 层区分。

## 四、写入路径

### 4.1 Log 表

流程简单——追加写，不需要旧值查询：

```text
Client                                    Server
  │                                         │
  ├─ 解析 record 的 partition columns        │
  ├─ 分区存在？                              │
  │   ├─ Yes → 正常写入                      │
  │   └─ No  → 重定向到 __historical__       │
  │           计算目标 bucket                 │
  │                                         │
  ├─ 发送 LogAppendRequest ───────────────→  │
  │                                         ├─ 追加到 __historical__ WAL
  │                                         ├─ 正常复制和 ACK
  │  ◄─────────────────────── 响应 ─────────┤
```

- **bucket 策略**：有 bucket key 按哈希分 bucket，无 bucket key 用 sticky 策略
- **消费**：下游订阅 `__historical__` 的 bucket，从 row 的 partition columns 恢复原始分区
- **offset 连续性**：`__historical__` 和原始分区的 offset 无关联。原始分区过期时，其数据已被消费完毕（Fluss 侧消费或湖侧 tiering 完成），`__historical__` 只接收过期后新到的迟到数据

### 4.2 主键表

比 Log 表复杂，核心在于需要旧值查询来生成正确的 changelog（`-U` / `+U`）。

#### Client 端

与 Log 表类似，检测分区过期后重定向到 `__historical__`，按 bucket key 哈希计算目标 bucket。

额外地，对于 key-only delete（`row == null`），client 在 RPC 请求中携带 `partition_name` 字段，因为没有 row 可以提取分区信息。

#### Server 端：per-partition 状态隔离

核心挑战是 **key 冲突**——主键编码不包含分区列，不同原始分区的相同主键编码一样：

```text
dt=2020, id=1  ──encode──→  key = encode(id=1)
dt=2019, id=1  ──encode──→  key = encode(id=1)   ← 相同！
```

解决方案：`__historical__` 内部按原始分区隔离 KV 状态，每个原始分区有独立的 prewrite buffer 和 RocksDB Column Family：

```text
__historical__ partition（一个 bucket 内）
  │
  ├─ dt=2020
  │    ├─ KvPreWriteBuffer（内存）
  │    └─ RocksDB CF: "dt=2020"
  │
  ├─ dt=2019
  │    ├─ KvPreWriteBuffer（内存）
  │    └─ RocksDB CF: "dt=2019"
  │
  └─ dt=2018
       ├─ KvPreWriteBuffer（内存）
       └─ RocksDB CF: "dt=2018"
```

**为什么用 per-partition CF 而不是把分区编码进 key？**
- key 编解码不用改，避免 put/get/delete/replay/snapshot 的兼容性改动
- 每个分区的 CF 可以在 tiering 完成后独立删除
- 概念上对齐：路由单位是原始分区，本地状态隔离也按分区

代价是 CF 数量随活跃的历史分区增长，需要运维监控。

#### Server 端处理流程

```text
收到 __historical__ 的 PK 写入请求
         │
         ▼
提取原始分区：
  - upsert（row != null）：从 row 的 partition columns
  - delete（row == null）：从 RPC 的 partition_name 字段
         │
         ▼
查找或创建该分区的写入上下文（prewrite buffer + RocksDB CF）
         │
         ▼
旧值查询：buffer → RocksDB CF → Lake 回查（Paimon）
         │
         ▼
生成 changelog（INSERT / -U+U / DELETE）→ 写入 WAL → 更新 prewrite buffer
```

**旧值查询链**：

| 步骤  | 来源                              | 延迟          |
|-----|---------------------------------|-------------|
| 1   | 分区 prewrite buffer（内存）          | 极快          |
| 2   | 分区 RocksDB CF（本地磁盘）             | 快           |
| 3   | Lake 回查（Paimon LocalTableQuery） | 不可控（远程 I/O） |

第三步的延迟问题通过性能隔离解决（见第六节）。

### 4.3 分区状态清理

Tiering 完成后，该原始分区的本地状态可以立即清理：

- 丢弃内存 prewrite buffer
- 删除对应的 RocksDB CF

判断条件：维护每个原始分区在 `__historical__` 中最后写入的 log offset（`partitionEndOffset`），当 tiering 同步进度 >= 该 offset 时即可清理。

### 4.4 恢复流程

`__historical__` partition 重启时：

```text
1. RocksDB 打开，恢复已有的 per-partition CF handles
         │
         ▼
2. 从 recover offset 回放到 high watermark
   → 按原始分区路由，直接写入对应 CF（持久化状态）
         │
         ▼
3. 更新 flushed offset
         │
         ▼
4. 从 high watermark 回放到 log end
   → 按原始分区路由，写入对应 prewrite buffer（内存状态）
```

回放过程中每条 record 都带有原始分区路由信息，分区隔离端到端保持一致。

### 4.5 TTL 场景的限制

存在一个已知的 changelog 间隙风险：如果某个 Flink job 还没消费完某个原始分区的 changelog，该分区就被 TTL 过期了，之后新写入被重定向到 `__historical__`。此时 job 可能会跳过原始分区的尾部 changelog。

当前假设：原始分区过期时，其 changelog 已被下游完整消费。`__historical__` 只作为过期后的追加路径，不保证与原分区 offset 的连续性。

## 五、查询路径（主键表）

复用标准 `LookupRequest`，附带 `partition_name` 标识原始分区：

```text
Client                                          Server
  │                                               │
  ├─ 分区不存在                                    │
  ├─ 路由到 __historical__ partition 的 bucket     │
  │                                               │
  ├─ 发送 LookupRequest（附带 partition_name）────→ │
  │                                               ├─ 目标是 __historical__？
  │                                               │   ├─ 否 → 同步查询本地 RocksDB
  │                                               │   └─ 是 → 提交到 ioExecutor
  │                                               │          → Paimon LocalTableQuery
  │  ◄──────────────────────────────── 响应 ──────┤
```

Paimon 实现基于 `LocalTableQuery`，按 (partition, bucket) 缓存文件索引，首次查询加载，后续复用。

## 六、性能隔离

### 6.1 问题

Fluss 当前的写路径完全同步在 RPC 线程上执行。如果历史 PK 写在 RPC 线程上等待 lake 旧值查询，会阻塞该线程上**所有** RPC 处理（包括实时分区读写）。

### 6.2 线程隔离

**Client 端**：sender/accumulator 层将实时分区和 `__historical__` 分区的 record 累积到独立的 batch 中，发送为独立请求。Server 收到的请求要么纯实时、要么纯历史，可以在请求级别直接分发。

**Server 端**：`ReplicaManager` 复用 `ioExecutor`（bounded 线程池），将历史分区操作从 RPC 线程卸载：

- **写入**：`putRecordsToKv()` 判断目标是否 `__historical__`，是则提交到 `ioExecutor` 异步执行，RPC 线程立即释放
- **查询**：`lookups()` 判断目标是否 `__historical__`，是则提交到 `ioExecutor` 异步执行 lake 查询

写入和查询共用同一个 `ioExecutor`，统一控制 lake I/O 总并发。

```text
实时 write:    PutKvRequest    ──→ RPC Thread ──→ 同步执行 ──→ RocksDB
实时 lookup:   LookupRequest   ──→ RPC Thread ──→ 同步执行 ──→ RocksDB

历史 write:    PutKvRequest    ──→ RPC Thread ──→ ioExecutor ──→ Lake I/O + RocksDB
历史 lookup:   LookupRequest   ──→ RPC Thread ──→ ioExecutor ──→ Paimon
```

实时路径不经过 ioExecutor，零影响。

### 6.3 Server 端流控

`ioExecutor` 使用 bounded queue（默认暂定 16，可配置）作为流控机制。队列满时返回 `HISTORICAL_PARTITION_THROTTLED` 错误码，client 收到后走现有重试机制 backoff 重试。

队列大小的考量：排队时间 + 执行时间超过请求超时时间的请求是无用功。最终值需要通过 benchmark 实际 lake I/O 延迟来确定。调优公式：

```
queue_size = 线程数 × (可接受最大排队等待时间 / 平均 lake I/O 延迟)
```

## 七、兼容性

### 旧行为（本 FIP 之前）

写过期分区在之前就是不可用的：

- `dynamicPartitionEnabled = true`：client 动态创建过期分区，但 `AutoPartitionManager` 下一轮 TTL 检查立即删除，形成创建/删除循环
- `dynamicPartitionEnabled = false`：client 直接抛 `PartitionNotExistException`
- 对过期分区做 point lookup 返回 null，即使数据在湖里还在

两种情况下，写入和查询过期分区都无法得到正确结果。

### 新行为（本 FIP）

- 写过期分区重定向到 `__historical__` 分区，写入成功
- 查过期分区通过 `LookupRequest`（附带 `partition_name`）回查湖存储，返回正确结果

### 兼容性

| 场景 | 行为                                                                                                                    |
|------|-----------------------------------------------------------------------------------------------------------------------|
| 老 client → 新 server | 正常读写不受影响。老 client 没有重定向到 `__historical__` 的逻辑，行为退化为旧行为（创建/删除循环或 `PartitionNotExistException`）                         |
| 新 client → 老 server | 正常读写不受影响。新 client 尝试重定向到 `__historical__`，但老 server 没有对应的处理逻辑（无 per-partition CF、无 lake 旧值查询），写入/查询失败——等同于旧行为，不需要版本检查 |
