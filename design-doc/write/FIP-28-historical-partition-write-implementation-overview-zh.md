# FIP-28 历史分区写入实现说明

## 1. 文档范围

本文基于以下连续五个 commit 的最终代码，说明 historical partition write 从服务端本地状态、PK 写入、恢复与清理、Paimon tiering 到客户端端到端路由分别做了什么，以及关键设计背后的原因。

| Commit | 主题 |
| --- | --- |
| `8c8641d70ea2d1903027786a50625e704bee8f68` | `[server] Add historical KV storage primitives` |
| `d53c560c4` | `[server] Add historical primary-key write processor` |
| `46aa8b802` | `[server] Add historical write dispatch and recovery` |
| `29aa38aa9` | `[lake] Support Paimon historical partition tiering` |
| `92e04c031748e0f641127c2d09aa4882e8ccddd1` | `[client] Enable end-to-end historical partition writes` |

以上范围共修改 71 个文件，增加约 7129 行、删除约 714 行。`bc934bab1` 中完成的 PutKv RPC `originalPartitionName` 字段和第一阶段 client batching plumbing 是这些 commit 的前置条件，不属于本文的 commit 范围；本文只在解释完整链路时引用它。

本文描述的是 `92e04c031` 结束时的最终代码。早期 plan 或 review 记录中出现过、但最终代码已经删除的方案，例如维护独立的 `reroutedWriteBatches` 索引，不作为当前实现的一部分。

## 2. 解决的问题与支持边界

原始 auto partition 被 retention 删除后，数据仍可能因为迟到、补数或修正而需要写入。普通写入路径要求原始 Fluss partition 仍存在，因此不能直接写到已经删除的 partition。FIP-28 使用一个 `__historical__` system partition 承载这些写入，同时保留 row 中的原始 partition columns，最终仍把数据写回 Paimon 的原始 partition。

这一组 commit 最终支持：

- append-only/log table 的历史分区写入；
- primary-key table 的历史 upsert、partial update、delete 和 old-value merge；
- 多个原始 partition 共享一个 historical system partition；
- historical WAL 的复制、leader failover 后的懒恢复；
- historical 本地 KV cache 与 Paimon lake 的组合 lookup；
- historical WAL tiering 到 Paimon 原始 partition；
- tiering 完成后的本地 KV cache 清理；
- client 的 metadata 解析、retry、幂等 sequence、`flush()` 和 request batching。

当前 historical 路由只适用于同时满足以下条件的缺失 partition：

1. 表启用了 auto partition；
2. 表启用了 data lake；
3. lake format 是 Paimon；
4. partition name 可以按表的 partition keys 和 auto partition time unit 正确解析；
5. auto partition retention 已配置；
6. partition time 早于当前 retention 窗口。

`client.writer.dynamic-create-partition.enabled=false` 不会禁止符合上述条件的 historical write。代码先判断 historical eligibility，再处理普通 dynamic partition create 的开关。

## 3. 需要区分的四种身份

Historical write 的客户端代码同时保留了 original queue 和 actual RPC target。理解下面四种身份后，后续的 reroute、metadata refresh 和 sequence 逻辑会清楚很多。

假设迟到数据属于原始 partition `dt=20240101`，该 partition 已删除，对应的 system partition 是 `dt=__historical__`：

| 身份 | 示例 | 用途 |
| --- | --- | --- |
| original physical path，记为 `O` | `table/dt=20240101` | accumulator queue 的 key，定义“同一原分区、同一 bucket”的顺序 |
| historical physical path，记为 `H` | `table/dt=__historical__` | metadata refresh 和实际 RPC 的物理目标 |
| actual `TableBucket` | `(tableId, historicalPartitionId, bucketId)` | leader 查找、RPC response 关联和幂等 sequence 分配 |
| `originalPartitionName` | `20240101` 或完整多级 partition name | PK historical key 隔离、lake old-value lookup 和错误校验 |

最终状态下：

```text
WriteBatch.physicalTablePath()             = O
ReadyWriteBatch.targetPhysicalTablePath()  = H
ReadyWriteBatch.tableBucket()               = H/bucket
WriteBatch.originalPartitionName           = O 的 partition name
```

保留 `O` 作为 queue key 的原因是顺序要求只针对相同 original partition、相同 bucket。不同 original partitions 即使映射到相同 `H/bucket`，也不要求建立彼此之间的业务顺序。

PK write 与 log write 对 `originalPartitionName` 的使用不同：

- PutKv RPC 会把它放到每个 bucket request 中。服务端要用它构造 composite key，并到 Paimon 的对应原始 partition 查询旧值。
- ProduceLog RPC 不增加该字段。Log record 自身保留完整 row，partition columns 已经能够表达原始 partition。客户端 batch 仍保存该值，用于 reroute 状态和统一的 retry 处理。

## 4. 总体数据链路

```text
WriterClient
  |
  | resolve original partition O
  | O missing + expired + eligible
  v
HistoricalPartitionResolver ---- create/resolve ----> historical partition H
  |
  v
RecordAccumulator
  | queue key remains O
  | actual target becomes H/bucket
  | preserve order and assign idempotent sequence on H
  v
Sender
  | PutKv: carries originalPartitionName
  | ProduceLog: row keeps original partition columns
  v
TabletServer / historical replica
  |-- log table: append replicated historical log
  |
  `-- PK table:
        local historical state -> Paimon old-value fallback
        -> merge/CDC WAL -> replicated historical log
  |
  v
Paimon tiering
  | derive original partition from every row
  v
Paimon original partition
```

本地 historical KV 不是持久化的权威数据。它是为了 PK merge 和 lookup 建立的可删除 cache；WAL 和已经提交的 Paimon 数据共同提供恢复来源。

## 5. Commit 1：Historical KV Storage Primitives

Commit：`8c8641d70ea2d1903027786a50625e704bee8f68`

### 5.1 抽象正常与 historical KV 的状态访问

新增 `KvStateAccessor`，把 `KvTablet` 原来直接访问 prewrite buffer 和 RocksDB 的逻辑抽象为：

- `encodeKey()`；
- `lookup()`；
- `insert()`、`update()`、`delete()`；
- `truncateTo()`；
- `flush()`。

`NormalKvStateAccessor` 保留普通 KV tablet 的原行为。后续 historical write 通过 `HistoricalKvStateAccessor` 使用另一种 key 编码和生命周期，而共享相同的 KV merge/WAL processor。

这个抽象把“如何计算一条 upsert 的最终值”和“最终值保存在哪里”拆开。Historical write 需要复用普通 PK 表的 merge、auto increment、CDC 和幂等语义，但不能复用普通 `KvTablet` 的 snapshot 生命周期。

### 5.2 增加三态 lookup result

新增 `KvStateLookupResult`：

```text
NOT_FOUND  本地从未保存该 key
PRESENT    本地保存了 value
DELETED    本地明确保存了 delete tombstone
```

`NOT_FOUND` 和 `DELETED` 必须分开。Historical state 查不到 key 时，可以继续去 Paimon 查询；如果本地已经记录 delete，则不能 fallback，否则会把 Paimon 中尚未被新 snapshot 覆盖的旧 row 重新读出来。

例如：

```text
Paimon: key=1 -> old-value
local historical state: key=1 -> DELETE
```

若只用 `null` 表示两种情况，lookup 会把 local delete 当成本地 miss，再返回 Paimon 的 `old-value`。三态结果使 delete 能直接终止 fallback。

### 5.3 Historical composite key

多个 original partitions 可以共享同一个 historical `TableBucket` 和同一个 RocksDB。仅使用 primary key 会发生碰撞：

```text
partition=20240101, primaryKey=1
partition=20240102, primaryKey=1
```

`HistoricalKvKeyCodec` 使用以下布局：

```text
4-byte big-endian UTF-8 partition-name length
+ partition-name UTF-8 bytes
+ original primary-key bytes
```

先写长度可以无歧义地区分 partition name 与 primary key 的边界，也避免 `("ab", "c")` 和 `("a", "bc")` 编码为同一串 bytes。长度使用 UTF-8 byte length，不使用 Java character 数量。

### 5.4 Tombstone 的落盘表示

`HistoricalKvBatchWriter` 把 delete 作为零长度 byte array 写入 RocksDB，普通 value 则禁止为空。

这样零长度值可以唯一表示 durable tombstone。使用 RocksDB physical delete 会丢失“该 key 已被 historical WAL 删除”的信息，下一次 lake fallback 又可能读到旧值。

### 5.5 `HistoricalKvHandle`

每个 historical `TableBucket` 对应一个 `HistoricalKvHandle`，内部包含：

- 一个本地 RocksDB；
- 一个 `KvPreWriteBuffer`；
- read/write lock；
- `lastAccessTime`；
- close/drop 生命周期。

Handle 使用 replica 的 `kvTabletDir`。Historical replica 不会同时创建普通 `KvTablet`，因此同一目录不存在两个 KV engine 竞争。每次创建 handle 前会删除该目录，因为历史 KV 是可恢复 cache，不能把上一次进程或 leader epoch 留下的文件直接当成有效状态。

锁的职责如下：

- read lock 保护 lookup，避免 lookup 过程中 handle 被 cleanup 删除；
- write lock 覆盖一整个 write/recovery/flush 操作；
- `tryAcquireWriteLock()` 让 cleanup 在有活跃 read/write 时直接跳过，而不是长时间阻塞请求；
- `dropUnderWriteLock()` 用于 manager 已经持有 write lock 的删除路径，避免重复加锁。

### 5.6 `HistoricalKvManager` 的两层 handle map

Manager 同时维护 `handles` 和 `readyHandles`：

- `handles` 包含已经分配、可能正在恢复的 handle；
- `readyHandles` 只包含可以被在线 lookup/write 使用的完整状态。

普通懒创建通过 `getOrCreate()` 立即进入 ready。恢复通过：

```text
createForRecovery() -> replay all required WAL -> markReady()
```

恢复过程中不放入 `readyHandles`。否则 lookup 可能看到只 replay 了一半的状态，并错误地把剩余 key 当成 local miss 后去 lake fallback。

Manager 在 lifecycle lock 下先找到 ready handle，再获取 handle read/write lock，最后才释放 lifecycle lock。这一固定锁顺序避免 cleanup 在“找到 handle”和“真正获取 handle lock”之间把它删除。

## 6. Commit 2：Historical Primary-Key Write Processor

Commit：`d53c560c4`

### 6.1 从 `KvTablet` 提取 `KvWriteProcessor`

原 `KvTablet` 中负责以下语义的代码被提取到 `KvWriteProcessor`：

- schema ID 和 target columns 校验；
- merge mode 和 row merger；
- old-value 读取；
- partial update、delete behavior、auto increment；
- WAL/FULL changelog 构建；
- writer ID 和 batch sequence 写入 WAL；
- duplicate/error 时回滚 prewrite buffer；
- 空 CDC batch 仍推进 sequence。

普通 `KvTablet` 通过 `NormalKvStateAccessor` 调用它，historical path 通过 historical accessor 调用它。

这次提取看起来 diff 较大，但 historical replica 不创建普通 `KvTablet`。如果复制一份 processor，normal 与 historical 对 merge engine、delete、auto increment 或空 batch 的处理很容易逐渐分叉。抽取 state-independent processor 保证两条路径共享同一套写语义。

即使一个 input batch 没有生成 CDC row，例如删除不存在的 key，processor 仍 append 一个空 WAL batch。原因是 writer ID/batch sequence 的服务端状态也需要推进；直接返回会让下一批收到 `OutOfOrderSequenceException`。

### 6.2 本地状态优先、Paimon fallback

`HistoricalLakeFallbackStateAccessor` 的读取顺序是：

```text
prewrite buffer / historical RocksDB
  |
  |-- PRESENT -> 使用本地 value
  |-- DELETED -> 视为不存在，不再访问 lake
  `-- NOT_FOUND -> 查询 Paimon 原始 partition
```

所有 insert/update/delete/truncate/flush 仍写向本地 accessor。Paimon 只作为历史基线的 old-value source。

这一层使 historical partial update 能读取 Paimon 中的旧 row，然后复用普通 merge engine 生成完整的新 value 和 CDC WAL。Delete 同样可以从 lake 找到旧 row，生成正确的 delete changelog。

### 6.3 `HistoricalPkWriteProcessor`

一批 historical PutKv 的处理过程为：

1. 从 RPC 读取 `originalPartitionName`；
2. 按表的 partition keys 解析为 `ResolvedPartitionSpec`；
3. 为 actual historical `TableBucket` 获取 handle；
4. 创建携带 original partition name 的 `HistoricalKvStateAccessor`；
5. 包装为带 Paimon fallback 的 accessor；
6. 在 handle write lock 下调用共享 `KvWriteProcessor.putAsLeader()`；
7. append historical CDC WAL，并更新 leader high watermark。

Write lock 覆盖整个 batch，而不是只保护单次 RocksDB put。一个 batch 内部包含 old-value lookup、多个 mutation、WAL append 和失败 rollback；如果中间允许另一个 batch 或 cleanup 进入，rollback 可能截断其他 batch 的 mutation。

### 6.4 Historical replica 不创建普通 `KvTablet`

`Replica` 在构造时根据 physical partition name 计算一次 `historicalPartition`。成为 leader 时：

- normal PK replica 创建普通 `KvTablet` 和 periodic snapshot；
- historical PK replica 不创建普通 `KvTablet`，只在首次写入时懒创建 `KvWriteProcessor`；
- historical local state 由 `HistoricalKvManager` 管理。

普通 PutKv 请求如果发到 historical replica 会被拒绝。Historical PutKv 走独立入口，仍检查 leader 和 min ISR，并复用原有 high-watermark/acks 语义。

## 7. Commit 3：Server Dispatch、Flow Control、Recovery 与 Cleanup

Commit：`46aa8b802`

### 7.1 PutKv 请求分类与校验

`ReplicaManager.dispatchPutRecordsToKv()` 根据 bucket request 是否携带 `originalPartitionName` 分类：

- 全部不携带：normal PutKv；
- 全部携带：historical PutKv；
- 同一个 RPC 混合两类：`InvalidPartitionException`。

Historical request 还会校验：

- `requiredAcks` 合法；
- actual target replica 确实是 historical partition；
- client version 支持当前 PK table 格式。

RPC 字段负责声明请求语义，server replica metadata 再验证 actual target。这样 normal PutKv 不能仅因为误发到 `H` 就被当成 historical write。

### 7.2 为什么异步处理前要 `copyToHeap()`

普通 PutKv 在 RPC worker 调用栈内完成。Historical write 可能执行 lazy recovery 和 Paimon lookup，因此被提交到 I/O executor。

RPC decode 得到的 `DefaultKvRecordBatch` 可能引用 Netty request buffer；request handler 返回后，该 buffer 可以被释放或复用。异步任务若继续持有原 view，会读取已经失效或被覆盖的内存。`copyToHeap()` 为 historical task 创建独立 ownership，任务完成前不依赖 RPC buffer 生命周期。

### 7.3 共享 historical request limiter

新增配置：

```text
netty.server.max-queued-historical-requests = 50
```

`HistoricalRequestLimiter` 用一个 semaphore 同时限制 historical lookup 和 historical PutKv 的 queued + running bucket requests。Permit 在成功、异常和同步提交失败路径都会释放，并且 `close()` 幂等。

Historical operation 可能涉及远端 WAL、Paimon catalog、Paimon lookup 和 RocksDB 创建。仅把它们放到 I/O executor 不能限制积压量；没有 limiter 时，大量慢请求仍会占用 heap batch copy、future 和 executor queue。

容量耗尽时 server 返回 `HistoricalPartitionThrottledException`。Client historical KV batch 使用带 jitter 的 exponential backoff：初始 100 ms、倍数 2、上限 5 s、jitter 0.2，然后按普通 retry budget 重试。

### 7.4 `HistoricalPartitionTaskExecutor`

Task executor 为每个 `TableBucket` 维护一个 FIFO queue：

- 同一 bucket 同时只运行一个 task；
- 不同 buckets 可以在共享 I/O executor 上并行；
- 当前 task 失败只完成自己的 future，后续 task 仍继续；
- follower/lifecycle change 可以 cancel queued task，但不强制 interrupt 正在执行的 task；
- bucket 再次成为 leader 后通过 `reset()` 恢复接收任务；
- shutdown 拒绝 queued task，并有上限地等待 running task。

同一 bucket 的 recovery、write 和 cleanup 都会修改同一个 disposable handle 和同一段 WAL offset 状态，因此需要串行。使用全局单线程会让互不相关的 buckets 互相阻塞，per-bucket queue 保留了跨 bucket 并行度。

### 7.5 懒恢复流程

`HistoricalKvLifecycleManager` 合并同一 bucket 的并发 recovery future。Write 调用 `recoverIfNeeded()`，historical lookup 调用 `ensureRecovered()`；只有 ready handle 不存在时才 replay。

`HistoricalKvRecoverer` 的恢复区间为：

```text
startOffset = lakeLogEndOffset（如果可用）
           或 logStartOffset（没有 lake offset）

replay range = [startOffset, localLogEndOffset)
```

选择 `lakeLogEndOffset` 的原因是此前数据已经能通过 Paimon fallback 读取，本地 cache 只需要物化尚未 tier 的 WAL tail。若 replay 起点早于 `localLogStartOffset`，先通过 `RemoteLogFetcher` 读 remote WAL，再从本地 log 继续。

Recovery 直接 replay CDC WAL，不重新执行 merge：

- `UPDATE_BEFORE` 是旧值，跳过；
- `INSERT` 和 `UPDATE_AFTER` 写入最终 value；
- `DELETE` 写 tombstone；
- original partition name 从 row 的 partition columns 重建；
- primary key 与重建出的 partition name 一起编码为 composite key。

恢复时记录 `committedEnd = highWatermark`：

- 低于 high watermark 的 mutation 可以 flush 到 RocksDB；
- high watermark 之后的 mutation 保留在 prewrite buffer；
- replay 完成后再读取一次最新 high watermark 并 flush，然后 `markReady()`。

Handle 在整个 replay 完成前保持 hidden。任何异常都会 invalidate 并删除这个不完整 handle，下次请求可以重新恢复。

### 7.6 Historical KV 是 cache，为什么仍需要 flush

“可删除 cache”表示不依赖 RocksDB snapshot 做跨 leader 的持久恢复，不表示所有 mutation 都可以永远留在内存中。

Flush 仍承担以下职责：

1. 把 high watermark 以下、已经提交的 prewrite mutation 移入 RocksDB，控制内存占用；
2. 保留与 normal KV processor 相同的 offset-based truncate/rollback 边界；
3. 让长时间存在的 leader 可以持续使用本地 cache，而不必因 prewrite buffer 增长频繁重建；
4. lookup 同时读取 prewrite buffer 和 RocksDB，flush 前后语义不变。

`Replica.mayFlushKv()` 在 normal replica 上 flush 普通 `KvTablet`，在 historical replica 上调用 `HistoricalKvManager.flushIfReady()`。这里的 flush 只更新本地 cache；真正的可恢复来源仍是 replicated WAL 和 Paimon。

### 7.7 Cleanup

有两条 cleanup 触发路径：

1. Lake offset notification 表明 `lakeLogEndOffset >= localLogEndOffset` 时，说明当前本地 historical WAL 已全部 tier，立即提交 cleanup；
2. 周期任务检查 ready handles，只有 fully tiered 且超过 `kv.historical.idle-timeout`（默认 3 小时）才尝试 cleanup。

Cleanup task 在 per-bucket executor 中再次验证：

- replica 仍是 leader；
- replica 仍是 historical partition；
- 当前 handle 与提交任务时的 expected handle 是同一实例；
- lake end 仍覆盖 local end；
- idle cleanup 的访问时间仍满足阈值。

最后通过 `tryInvalidateBucket()` 获取 write lock。若正有 lookup/write，cleanup 返回 false，留给下一次触发，避免删除活跃状态。

Replica 变 follower、被删除或 lifecycle 改变时也会 cancel bucket task 并 invalidate historical handle。下一个 leader 通过 WAL 懒恢复。

## 8. Commit 4：Paimon Historical Tiering

Commit：`29aa38aa9`

### 8.1 Normal partition 的 fixed partition 不适用于 historical WAL

Normal Fluss partition 的一个 lake writer 只对应一个 Paimon partition，因此可以在初始化时把 physical partition name 解析为 `fixedPartition`，后续整批复用。

Historical system partition 不同。一个 `H/bucket` 的 WAL 可以包含多个 original partitions：

```text
H/bucket-0:
  row(dt=20240101, ...)
  row(dt=20240102, ...)
```

如果把 `__historical__` 当成 fixed Paimon partition，数据会被提交到错误的 system partition。`RecordWriter` 因此增加 `historicalPartition`：

- normal writer 保留 fixed-partition fast path；
- historical writer 对每条 `LogRecord` 调用 Paimon `tableWrite.getPartition(row)`，从 row 的原始 partition columns 得到真实 Paimon partition。

Append-only writer 与 merge-tree writer 都使用同一套 `prepareRecordAndGetPartition()`。

### 8.2 Historical Arrow batch

Normal append-only Arrow path 继续调用 `writeBundle(fixedPartition, ...)`，一次把整个 Arrow batch 写入同一 partition。

Historical Arrow batch 仍使用 `pollRecordBatch()` 得到的 `ArrowBundleRecords`，但在 Paimon writer 内逐 row 迭代：

```text
for each row:
    partition = tableWrite.getPartition(row)
    write(partition, bucket, row)
```

这保留了 Arrow poll/batch decode 路径，只放弃“整批只有一个 partition”的写入优化。原因是一个 historical Arrow batch 本身可能包含多个 original partitions，不能安全调用只接受一个 fixed partition 的 `writeBundle()`。

### 8.3 一个 writer 可以产生多个 commit messages

Normal writer 对应一个 fixed partition，原代码要求 `prepareCommit()` 恰好产生一条 `CommitMessage`。Historical writer 可能触达多个 original partitions，因此可能产生多条 message。

改动包括：

- `PaimonWriteResult` 从单个 message 改为非空 immutable list；
- `PaimonLakeCommitter` 展开所有 write results 中的 message；
- 只有 normal writer 继续校验 message 数量为 1；
- historical writer 接受多条 message。

`PaimonWriteResultSerializer` 的 `CURRENT_VERSION` 仍为 1，但内容从单 message 改为 `CommitMessageSerializer.serializeList()`。当前代码明确采用直接格式升级，没有读取旧单-message bytes 的兼容分支；滚动升级或恢复旧序列化状态时不能把版本号 1 理解为 wire format 完全兼容。

## 9. Commit 5：Client End-to-End Historical Write

Commit：`92e04c031748e0f641127c2d09aa4882e8ccddd1`

### 9.1 Lookup 与 write 共用 resolver

`HistoricalPartitionResolver` 从 lookup package 移到 client metadata package，lookup 与 write 共用同一个实现。

Resolver 的步骤是：

1. 根据 original partition 构造 historical partition spec；多级 partition 只把 auto partition key 替换为 `__historical__`，保留静态前缀；
2. 先查 metadata cache；
3. cache miss 时强制刷新 historical path；
4. 仍不存在时调用 `Admin.createPartition(..., ignoreIfExists=true)`；
5. create 完成后再次刷新 metadata，并返回 partition ID。

相同 `(tableId, tablePath, originalPartitionName)` 的并发 resolve 共用一个 in-flight future；future 完成后从 map 移除，失败后下一次调用可以重试。

Create 已成功但 metadata refresh 仍拿不到 partition ID 时，代码返回 `UnknownTableOrBucketException`，而不是 `PartitionNotExistException`。前者是 retriable metadata error；此时更可能是新 partition 尚未被 metadata cache 观察到，不应把 pending batch 当成确定不存在而终止。

### 9.2 `DynamicPartitionCreator` 同时解析 normal 和 historical target

`ResolvedWriteTarget` 统一表达：

```text
normal:     physical path = O, originalPartitionName = null, partitionId = null
historical: physical path = H, originalPartitionName = O name, partitionId = H id
```

`resolveWriteTarget()` 的判断顺序是：

1. 非分区表直接返回 normal；
2. original partition 在 cache 中存在，返回 normal；
3. 已确认 historical 的 original path，重新 resolve H，避免每条 record 都强制刷新 O；
4. 强制刷新 O；
5. O 存在，返回 normal；
6. O 不存在且满足 historical eligibility，resolve/create H；
7. 否则按原有 dynamic partition create 或 `PartitionNotExistException` 处理。

`confirmedHistoricalPartitions` 以 original physical path 保存 table ID。Table ID 用于避免 drop/recreate 后的新表复用旧表的 historical routing decision。Metadata cache 再次发现 O 存在时会删除该标记，恢复 normal routing。

### 9.3 Append 前已确认 O 不存在的流程

`WriterClient.doSend()` 在 append 前 resolve target：

1. original record 的 path 是 O；
2. resolver 返回 historical target H；
3. record copy 增加 `originalPartitionName`，但其 accumulator path 仍保持 O；
4. bucket assigner 使用 H 的 metadata；
5. record append 到 O 对应的 queue；
6. append 后调用 `RecordAccumulator.rerouteToHistorical(O, H)`；
7. 若没有旧 O attempt，target switch 立即完成；否则进入 barrier。

Record 仍进入 O queue，是为了让同一 original partition、同一 bucket 的所有 batch 留在同一 deque。若直接把 record 改成 H path，不同 original partitions 会全部聚合到相同 queue，原分区维度的 batch 隔离和顺序语义都会丢失。

### 9.4 Batch 已经发到 O 后才发现 partition 不存在

这条路径处理 metadata cache 与 server 当前状态的时间差：

```text
append 时 client 仍认为 O 存在
send 到 server 时 O 已被 retention 删除
```

流程为：

1. O 返回 `UnknownTableOrBucket` 等 retriable invalid metadata error；
2. batch 先按原 sequence 重新进入 O deque；
3. Sender 失效 O 的 bucket metadata；
4. 下一轮 `ready()` 无法找到 O leader，把 O 放入 `unknownLeaderTables`；
5. metadata refresh 明确返回 `PartitionNotExistException`；
6. Sender 同时失效 O 的 bucket 和 partition-ID metadata；
7. `rerouteBatches(O)` 重新执行 `resolveWriteTarget()`；
8. 确认满足历史条件后，为 queue 设置 pending H；
9. barrier 完成后实际 target 切换为 H；
10. 下一轮 drain 使用 H 的 leader 和 sequence 发送同一批 batch。

Reroute 不 abort batch、不迁移 callback、不重新 append record。`IncompleteBatches` 中仍是同一个 `WriteBatch` 和 future。

### 9.5 Reroute barrier 为什么存在

假设同一 O/bucket 按 append 顺序有 `b0`、`b1`、`b2`，其中：

```text
b0 已发往 O，sequence(O)=3
b1 已发往 O，sequence(O)=4
b2 仍在 deque，尚未分配 sequence
```

若 `b0` 先收到 O 不存在后立即发布 H，`b2` 可以先从 deque 发往 H；`b1` 稍后才返回并改发 H。最终 H 的执行顺序可能是 `b2 -> b1`，违反同一 original partition、同一 bucket 的 append 顺序。

当前 barrier 按以下步骤处理：

1. `pendingHistoricalTarget=H`，但 `partitionId` 暂时仍保留 O；
2. `getReadyDeque()` 在 pending target 存在时隐藏整条 queue；
3. `hasInflightBatchOutsideQueue()` 检查旧 O 的 idempotence entry 中，是否还有属于当前 O path、但尚未回到当前 deque 的 batch；
4. `b1` 未返回时检查为 true，queue 不能 drain；
5. `b1` 返回后，`reEnqueue()` 按旧 sequence 恢复 deque 顺序；
6. 所有旧 O attempts 都回队后，从 O 的 idempotence entry 移除它们，并清除旧 writer state；
7. 一次性发布 H 的 partition ID 和 metadata path；
8. 下一轮 drain 按 deque 顺序为它们分配 H sequence。

按定义验证顺序：

```text
reroute 前 append/order relation: b0 < b1 < b2
旧 response 乱序后，reEnqueue 依据 sequence(O) 恢复: b0 < b1
未发送的 b2 原本位于二者之后，因此 deque: b0 < b1 < b2
drain 从 deque head 依次分配 sequence(H)
所以 sequence(H,b0) < sequence(H,b1) < sequence(H,b2)
```

Barrier 只等待 idempotence entry 中 `physicalTablePath == 当前 O` 的 batch。多个 original queues 共享同一个 H entry 时，Q1 不会等待属于 Q2 的 batch，避免两个 queue 相互等待。

### 9.6 为什么旧 O sequence 不能直接带到 H

Idempotence sequence 的作用域是 actual `TableBucket`。`O/bucket` 的 sequence 0 与 `H/bucket` 的 sequence 0 属于两套服务端状态。

如果把 O 的 `(writerId=W, sequence=0)` 直接用于 H：

- H 可能已经处理过 W/0，把新数据判定为 duplicate；
- client 对 duplicate sequence 按成功完成，数据却可能没有写入；
- client 的 H entry 也没有正确推进 next sequence，后续 batch 可能再次使用 0。

`rerouteBatchToHistorical()` 因此：

1. 从旧 actual `TableBucket` 的 in-flight entry 移除 batch；
2. 调用 `resetWriterState(NO_WRITER_ID, NO_BATCH_SEQUENCE)`；
3. 给 batch 增加 original partition context；
4. 保持 batch 在原 deque；
5. 后续 normal drain path 使用 H 的 `IdempotenceBucketEntry.nextSequence()` 重新分配并登记。

多个 original queues 可能共享一个 `H/bucket`。`IdempotenceManager` 以 actual `TableBucket` 为 key，并在 synchronized 方法中分配、递增 sequence，因此这些 queue 使用统一且不重复的 H sequence。

### 9.7 较晚返回的 O attempt

Queue 可能已经切到 H，但另一个早先发到 O 的 attempt 才返回。`reEnqueue()` 比较 response 的 `TableBucket.partitionId` 与 queue 当前 target ID：

- 相同：这是当前 target 的普通 retry，保留 sequence；
- 不同且 queue 已是 historical：这是旧 O 的 late response，从旧 entry 移除并清除旧 sequence，再加入 H retry。

Historical routing path 使用 `BucketAndWriteBatches -> deque` 的固定加锁顺序，使 late response 回队与 target switch 互斥。完全没有 historical routing 的 normal fast path 仍只使用 deque lock，避免所有 normal retry 都承担额外 routing lock。

### 9.8 Drain 为什么改为扫描 accumulator queues

Cluster metadata 只能表达一个 physical path 对应的 bucket locations。Historical routing 后可能出现：

```text
queue(O1) -> H/bucket-0
queue(O2) -> H/bucket-0
```

若 drain 只遍历 Cluster 的 H bucket，只会得到一个 location，无法分别找到 O1 和 O2 的 deque。

`getAllBucketsInCurrentNode()` 因此直接遍历 `writeBatches`：

1. 用 map entry key 保留 original queue path；
2. 用 queue 保存的 actual partition ID 和 bucket ID 构造 `TableBucket`；
3. 从 Cluster 查询这个 actual `TableBucket` 的 leader；
4. 构造“queue path 是 O、actual bucket 是 H”的 synthetic `BucketLocation`。

这也避免维护另一份需要 activate/deactivate/cleanup 的 rerouted queue 索引。代价是 drain 会扫描 accumulator 中已经创建过的 queue/bucket entries，而不是只扫描 Cluster 当前 available buckets。

### 9.9 Actual target metadata path

`ReadyWriteBatch.targetPhysicalTablePath` 记录本次 RPC destination 的 metadata path：

- normal write：等于 batch path；
- historical write：等于 H，而 batch path 仍是 O。

Historical batch 在 H 上收到 `NotLeader` 或 `LeaderNotAvailable` 后：

1. batch 回到 O queue；
2. Sender 失效 H 的 bucket metadata；
3. `ready()` 查不到 H/bucket leader 时，把 H 加到 `unknownLeaderTables`；
4. metadata updater 刷新 H；
5. partition ID 不变、leader 更新后，batch 以同一 H sequence 重试。

如果这里错误刷新 O，resolver 只能再次确认 O 不存在，无法恢复 H 的 leader metadata，batch 和 `flush()` 会一直等待。

### 9.10 Request grouping 为什么必须拆分重复 `TableBucket`

不同 original queues 可以映射到同一个 actual `H/bucket`。PutKv 和 ProduceLog response 都只通过 `TableBucket` 关联 batch；server decoder 也会把 request data 放入 `Map<TableBucket, ...>`。

一个 RPC 中若有两个相同 `TableBucket`：

- client response map 会覆盖一个 batch；
- server request map 也可能覆盖一份 records；
- 被覆盖 batch 的数据或 callback 会丢失，`flush()` 可能永久等待。

`Sender` 增加两类 packing：

- `packProduceLogRequestGroups()`：保持输入顺序，并保证每个 group 内 `TableBucket` 唯一；
- `packPutKvRequestGroups()`：除 `TableBucket` 唯一外，还把 normal 和 historical batch 分开。

Client `recordsByBucket()` 和 server `getProduceLogData()` 都增加 duplicate check，使非法重复从静默覆盖变成明确失败。

KV 的 target columns 和 merge mode 仍对完整 table-level batch list 校验。拆成多个 RPC 不会允许原本不兼容的 batches 混写。

### 9.11 Lake snapshot 可见性与本地 KV cleanup

Historical data tiering 完成后，本地 handle 可以清理。若 `HistoricalLakeLookupManager` 继续复用 cleanup 前打开的 Paimon lookuper，它可能仍绑定旧 snapshot；本地状态已经删除后，lookup 就可能短暂读不到刚 tier 的数据。

最终代码在 lake offset notification 中先调用：

```text
requireLakeSnapshot(tableId, notifiedSnapshotId)
```

然后才提交 local KV cleanup。Lookup cache 为每个 table 记录最大的 required snapshot ID：

- schema ID 相同，并且 cached lookuper 的 `minimumSnapshotId >= requiredSnapshotId`，才复用；
- 否则下一次 lookup 懒创建新 lookuper；
- 新 lookuper 先执行当前 lookup，成功后才替换旧实例；
- refresh 失败时关闭新实例、保留旧实例供后续 retry，但当前 lookup 返回异常，不返回可能过期的旧结果；
- cache invalidation 与 lookup 获取 entry 的竞态通过 `invalidated` 标记和 retry loop 处理。

Required snapshot 是 table-level 的，因为 lake notification 按 bucket 到达，而 cached lookuper 按 table ID 共享。取最大 snapshot ID 能覆盖各 bucket 已完成 tiering 的最新通知。

### 9.12 `flush()` 语义

Client historical reroute 始终复用原 `WriteBatch`、request future 和 `IncompleteBatches` entry。`flush()` 的步骤仍是：

1. `beginFlush()` 让未满 batch 立即 eligible；
2. 获取调用开始时所有 incomplete request futures；
3. 等待这些 futures 成功或失败完成。

如果 batch 正在 barrier 中，queue 暂时不可 drain，但 future 没有被替换或提前完成。所有旧 O attempts 返回、切换到 H、H 写入满足 acks 后，原 future 才完成。因此 `flush()` 会等待 historical write 真正完成，不只等待 reroute。

同一 original partition、同一 bucket 的乱序问题由 deque + barrier + actual-target sequence 共同处理。不同 original partitions 之间没有额外顺序保证，这是当前明确的语义范围。

## 10. 完整的 Historical PK Upsert 示例

假设 Paimon 中已有：

```text
partition=20240101, id=1, value=old
```

Fluss 原 partition 已删除，client 写入：

```text
partition=20240101, id=1, value=new
```

完整过程如下：

1. Client 确认 `20240101` 已过 retention，resolve/create `__historical__`；
2. record 留在 `20240101/bucket` queue，actual target 设为 historical bucket；
3. Sender 为 historical actual `TableBucket` 分配 sequence；
4. PutKv bucket request 携带 `originalPartitionName=20240101`；
5. Server 获得 request permit，复制 batch 到 heap，并提交 per-bucket task；
6. ready handle 不存在时，从 lake end 到 local end 懒恢复 WAL tail；
7. local composite key lookup 为 `NOT_FOUND`；
8. Paimon lookuper 在 partition `20240101` 查询到 `old`；
9. 共享 `KvWriteProcessor` 执行 merge，生成 `UPDATE_BEFORE(old)` 与 `UPDATE_AFTER(new)`（FULL changelog 场景）；
10. new value 进入 historical prewrite buffer，CDC WAL append 到 historical log；
11. acks 条件满足后 client future 和 `flush()` 完成；
12. Paimon tiering 从 `UPDATE_AFTER` row 的 partition columns 得到 `20240101`，写回原 partition；
13. lake offset 覆盖 local end后，server 记录 required snapshot并删除本地 handle；
14. 后续 lookup 刷新 Paimon lookuper，从新 snapshot读到 `new`。

Delete 的区别是本地写入 tombstone。Tiering 完成前 lookup 看到 `DELETED` 并停止 lake fallback；tiering 完成和 cache cleanup 后，Paimon 新 snapshot 已移除该 row。

## 11. 测试覆盖

这一组 commit 新增或扩展的测试主要覆盖以下行为。

### 11.1 Historical KV primitives

- tombstone 写入和空 value 拒绝；
- original partition composite-key 隔离；
- update/delete/truncate；
- handle access time 与 close/drop；
- concurrent get-or-create；
- recovery handle 在 `markReady()` 前不可见；
- bucket/table invalidation。

### 11.2 PK processor、lookup 与 lifecycle

- lake old-value fallback；
- local PRESENT/DELETED 优先于 lake；
- historical insert/update/delete；
- per-bucket serial、cross-bucket parallel；
- cancel/reset 与 bounded shutdown wait；
- historical limiter 耗尽和异常释放 permit；
- lazy lookuper cache、schema/lifecycle invalidation；
- required snapshot refresh；
- refresh 失败保留旧 lookuper；
- idle expiration。

### 11.3 Client routing 与幂等

- resolver cache、metadata refresh、create、并发 future 合并和失败重试；
- 多级 partition 保留静态前缀；
- normal、expired historical、dynamic-create-disabled 路由；
- confirmed historical route cache；
- 不同 original partitions 不进入同一个 KV batch；
- O 到 H 的原地 reroute；
- 等待 O in-flight attempts 的 barrier；
- O response 乱序时恢复 deque；
- log batch 保存 original context；
- historical target metadata refresh；
- PutKv normal/historical 分组；
- ProduceLog 相同 historical `TableBucket` 拆组；
- normal/historical invalid metadata 失效不同 actual target。

### 11.4 Paimon 与端到端

- PK 和 append-only historical tiering；
- historical Arrow row-by-row partition routing；
- 一个 historical writer 写多个 original partitions；
- expired log partition 写入 historical log；
- historical PK update/delete；
- lookup 在 write、leader failover、tiering 和 local cache cleanup 前后的结果；
- replicated historical WAL 在新 leader 上懒恢复。

## 12. 当前代码的明确边界与需要继续确认的点

### 12.1 Historical system partition 删除重建

当前 retry 支持 H 的 leader 变化且 partition ID 不变。若 H 被删除，并以相同 path 重新创建为新的 partition ID，queue 仍保存旧 H ID；当前 Sender 不会把已经 historical 的 O queue 自动 reconcile 到新 H ID。Historical system partition 按设计不应被普通 retention 删除，因此实现没有覆盖手工删除重建。

### 12.2 Paimon write result serializer

Serializer 版本仍是 1，但 bytes 已从单 commit message 改成 message list。代码没有旧格式兼容分支。部署和状态恢复流程需要接受这个直接升级决定，或在合并前单独增加版本迁移。

### 12.3 Routing cache 的生命周期

`confirmedHistoricalPartitions` 会在 original partition 重新出现或 table ID 变化时删除，但没有按 idle/size 主动淘汰。偶尔补写少量历史分区时影响有限；长期、大规模覆盖大量 original partitions 时需要评估 map 增长。

### 12.4 Append 与 reroute 的并发窗口

`WriterClient` 可能在根据旧 metadata 得到 normal target 后暂停，Sender 同时把同一 O queue 切到 H，随后该 writer thread 才 append 一个没有 `originalPartitionName` 的 record。当前代码没有让 append 与 routing state 共用完整锁来关闭这个低概率窗口。

### 12.5 Normal dynamic partition 的 partition ID 回填

从最终代码看，`BucketAndWriteBatches.partitionId` 在 queue 创建时读取一次，之后只在 historical reroute 完成时更新。`bucketReady()` 在该字段为 null 时会请求 metadata refresh，但没有再把 Cluster 中新出现的 normal partition ID 写回 queue。

因此需要在合并前确认 normal dynamic-create 场景：如果 queue 创建时 normal partition 尚不存在，异步 create 完成后是否还有其他路径为该 queue 回填 partition ID。本文检查的 `RecordAccumulator` 最终代码中没有看到该赋值；若没有外部保证，这会使 normal dynamic-create batch 一直处于 unknown leader 状态。这个点属于从当前代码直接观察到的兼容性风险，不应写成已解决能力。

## 13. 关键类索引

| 领域 | 关键类 |
| --- | --- |
| State abstraction | `KvStateAccessor`、`KvStateLookupResult`、`NormalKvStateAccessor` |
| Historical local KV | `HistoricalKvHandle`、`HistoricalKvManager`、`HistoricalKvStateAccessor`、`HistoricalKvKeyCodec`、`HistoricalKvBatchWriter` |
| Shared PK processing | `KvWriteProcessor`、`HistoricalLakeFallbackStateAccessor`、`HistoricalPkWriteProcessor`、`HistoricalPkWriteManager` |
| Recovery/lifecycle | `HistoricalKvRecoverer`、`HistoricalKvLifecycleManager`、`HistoricalPartitionTaskExecutor`、`HistoricalRequestLimiter` |
| Server dispatch | `Replica`、`ReplicaManager`、`TabletService`、`ServerRpcMessageUtils` |
| Client routing | `DynamicPartitionCreator`、`ResolvedWriteTarget`、`HistoricalPartitionResolver`、`WriterClient` |
| Client queue/retry | `RecordAccumulator`、`ReadyWriteBatch`、`Sender`、`IdempotenceManager`、`IdempotenceBucketEntry` |
| Paimon tiering | `PaimonLakeWriter`、`RecordWriter`、`AppendOnlyWriter`、`AppendOnlyArrowBatchHelper`、`MergeTreeWriter`、`PaimonWriteResult`、`PaimonLakeCommitter` |
| Lake lookup visibility | `HistoricalLakeLookupManager` |

## 14. 总结

这五个 commit 为 historical write 建立了三层读写结构：

```text
historical prewrite buffer / RocksDB cache
                +
replicated historical CDC WAL
                +
Paimon committed original partitions
```

Client 保留 original queue 来定义局部顺序，使用 actual historical `TableBucket` 做 leader 查找和幂等 sequence；server 复用 normal PK merge/WAL processor，但使用可删除的 historical state accessor；Paimon tiering 再根据 row 内容恢复原始 partition。Reroute barrier、三态 lookup、composite key、per-bucket executor 和 required lake snapshot 分别解决跨 target 保序、delete 防回生、共享 H key 隔离、状态生命周期并发和 cleanup 后 snapshot 可见性问题。
