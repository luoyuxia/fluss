# FIP-28 Historical Write PR 2 实施计划

## PR 标题

```text
[server] Add historical KV storage primitives
```

## 目标

本 PR 为 historical primary-key write 建立 server 内部的本地状态基础，包括：

- 每个 historical `TableBucket` 一个懒创建的、可丢弃的 RocksDB 实例。
- 隔离 original partition key space 的 composite key codec。
- 能区分“本地不存在”和“已经删除”的 tombstone 语义。
- 可 rollback 的 historical prewrite buffer。
- bucket、table 和 server 级显式生命周期管理。
- 供后续 historical processor 复用普通 `KvTablet` merge/WAL 逻辑的窄 state-access 扩展点。

本 PR 只交付可直接单测的 storage primitives，不把它接入 `TabletService.putKv()`。合并后，正常 client write 不会创建或访问 historical RocksDB。

## 与总计划的关系

本 PR 对应 `FIP-28-historical-partition-write-pr-plan-zh.md` 中的：

```text
PR 2: Historical KV Storage Primitives
```

前置依赖：

- PR 1 已让 PutKv RPC、client batch 和 server holder 能保存 nullable `originalPartitionName`。

后续依赖：

- PR 3 使用本 PR 的 composite key、三态 lookup、historical state accessor 和 rollback 能力实现 row merge、lake old-value fallback 与 WAL append。
- PR 4 使用本 PR 的 handle lock 和显式 lifecycle 增加 lazy recovery、tiered cleanup、idle eviction，并把 processor 接入在线请求。

## 前置假设

- `TableBucket` 表示实际的 `__historical__` physical partition bucket，其中包含 table id、historical partition id 和 bucket id。
- 一个 historical bucket 内可以同时保存多个已经过期的 original partitions。
- original primary key 的编码不包含 partition columns，因此相同 primary key bytes 可能同时出现在不同 original partitions。
- historical RocksDB 只保存尚未完全由 lake 覆盖的本地状态，不参与普通 KV snapshot/checkpoint。
- historical state 可以在 restart、replica reassignment 或 cleanup 后从 lake offset 和 WAL 重建；具体 recovery 在 PR 4 实现。
- encoded normal value 至少包含 schema id prefix，零长度 byte array 不会是合法 row value。
- 同一个 historical bucket 的完整 write batch 在 handle write lock 下执行；PR 4 的 keyed serial executor 还会保证不同 write batches 的 FIFO 顺序。

## 非目标

本 PR 不实现：

- `TabletService` 或 `ReplicaManager` 的 historical write dispatch。
- original partition eligibility、格式、过期状态或 table capability 校验。
- 从 row/RPC 提取 original partition name。
- lake old-value lookup。
- row merge、CDC/WAL 构造或 WAL append 的 historical 调用入口。
- historical point lookup 的 online/local-first 接入。
- restart、failover、leader promotion 或 remote WAL recovery。
- keyed serial executor、historical request limiter 或 retry backoff。
- tiered offset cleanup、idle timeout 或周期 eviction。
- historical RocksDB snapshot/checkpoint。
- 新的用户配置项或用户文档。
- 修改普通 KV 的 key encoding、物理 delete、snapshot 或 lookup 语义。

## 当前实现约束

### 1. 普通 `KvTablet` 把 state access 写死在类内

当前 `KvTablet` 直接持有：

```text
RocksDBKv
KvPreWriteBuffer
```

`processUpsert()`、`processDeletion()`、rollback 和 flush 直接访问这两个对象。如果 PR 3 复制整套 processing 方法，只替换 key 和 RocksDB，后续普通写语义的修改需要维护两份实现，merge engine、partial update、auto-increment 和 WAL behavior 也容易产生差异。

PR 2 只抽取本地 state access，不搬运 row processing 和 WAL 构造逻辑。

### 2. `KvPreWriteBuffer` 已经具备需要的 rollback 能力

现有 prewrite buffer 已支持：

- `insert`、`update` 和逻辑 `delete`。
- 优先读取尚未 flush 的最新值。
- 按 log sequence number flush。
- `DUPLICATED` 和 `ERROR` 两种 `truncateTo()` rollback。

这些语义与 historical write 一致，不需要新建第二套 buffer。差异只发生在 delete flush：

```text
normal delete      -> KvBatchWriter.delete(key)
historical delete  -> KvBatchWriter.put(key, TOMBSTONE_VALUE)
```

因此 tombstone 应在底层 batch-writer adapter 中实现，`KvPreWriteBuffer` 本身保持不变。

### 3. nullable byte array 无法表达 historical lookup 所需状态

historical old-value lookup 至少有三种结果：

```text
NOT_FOUND  本地从未写过或本地状态已被整体清理，PR 3 可以 fallback 到 lake
PRESENT    本地有 encoded row value，直接作为 old value
DELETED    prewrite delete 或 RocksDB tombstone，必须停止 lookup，不能 fallback 到 lake
```

如果继续只返回 nullable `byte[]`，`NOT_FOUND` 和 `DELETED` 都会变成 null，delete 后会错误地从 lake 读回旧值。

### 4. historical state 不能与普通 RocksDB 共用目录或实例

普通 KV tablet 的 RocksDB 位于：

```text
<kvTabletDir>/db
```

historical state 不参与普通 snapshot，并且允许被整体删除。它必须使用与普通 KV tablet 相邻、但不匹配 `kv-*` prefix 的独立目录：

```text
<kvTabletDir parent>/historical-kv-<bucketId>/db
```

例如普通目录为 `<partitionDir>/kv-3/db` 时，historical 目录为 `<partitionDir>/historical-kv-3/db`。

如果把 historical RocksDB 放进 `<kvTabletDir>/historical-kv`，重启时 `TabletManagerBase.listTabletsToLoad()` 仍会先发现外层 `kv-3`，并可能把只包含 historical state 的目录当成普通 KV tablet 加载。使用不以 `kv-` 开头的 sibling directory 可以避免这条恢复路径。

### 5. RocksDB 和 prewrite buffer 都不是可任意并发关闭的对象

PR 4 会允许 historical lookup 与 cleanup 并发。handle 必须在 PR 2 就建立读写锁边界：

- local lookup 获取 read lock。
- 一个完整 write batch 获取 write lock。
- close/drop 获取 write lock。
- PR 4 cleanup 使用同一把 write lock。

不能只给单次 `put()`、`delete()` 分别加锁，否则 lookup 可能看到同一 batch 写到一半、WAL 尚未 append 的临时状态。

## 核心设计

### 1. Composite key codec

新增 `HistoricalKvKeyCodec`，唯一编码格式为：

```text
4-byte big-endian partitionNameLength
+ UTF-8 partitionName bytes
+ original primary key bytes
```

示例：

```text
partitionName = "dt=2026-07-12"
primaryKey    = [0x01, 0x02]

compositeKey =
    [00 00 00 0D]
    + UTF-8("dt=2026-07-12")
    + [01 02]
```

这里的长度必须按 UTF-8 byte length 计算。以 `partitionName = "地区=杭州"` 为例：

1. 先调用 UTF-8 encoder 得到 `partitionBytes`。
2. length prefix 写入 `partitionBytes.length`。
3. 不能使用 `partitionName.length()`，因为它返回 UTF-16 code unit 数量。

长度前缀用于建立无歧义边界。逐步检查容易碰撞的两组输入：

```text
(partition="ab", key="c") -> [length=2]["ab"]["c"]
(partition="a",  key="bc") -> [length=1]["a"]["bc"]
```

两组编码的前四个 bytes 不同，因此结果不同。

codec 约束：

- `originalPartitionName` non-null 且非空。
- `originalPrimaryKey` non-null；允许零长度 key。
- 分配 byte array 前检查 `4 + partitionBytes.length + key.length` 不溢出。
- 使用明确的 UTF-8 charset 和 big-endian 写入，不依赖 platform default charset。
- 只暴露一个 encode 入口；put、delete、lookup 以及 PR 4 recovery 都调用该入口。
- 本 PR 不增加 decode、prefix scan 或 range-delete API，因为当前写、查和恢复都不需要反向解析 composite key。

普通 KV 继续直接使用 original primary key bytes，不经过该 codec。

### 2. Tombstone writer adapter

新增一个只用于 historical state 的 `KvBatchWriter` adapter，例如：

```java
final class HistoricalKvBatchWriter implements KvBatchWriter {
    private static final byte[] TOMBSTONE_VALUE = new byte[0];

    private final KvBatchWriter delegate;
}
```

行为：

```text
put(key, value)  -> delegate.put(key, value)
delete(key)      -> delegate.put(key, TOMBSTONE_VALUE)
flush()          -> delegate.flush()
close()          -> delegate.close()
```

必须在 `put()` 拒绝零长度 value，避免调用方把 tombstone 当成普通 encoded row 写入。不要把可变的 tombstone byte array 通过 getter 暴露给调用方。

`KvPreWriteBuffer.delete()` 仍把 delete 表示为 `Value.of(null)`。flush 时，它调用 adapter 的 `delete(key)`，adapter 再把该操作转换为 RocksDB `put(emptyValue)`。这样：

- normal prewrite buffer 和 normal batch writer 不变。
- pending delete 在 prewrite 层仍可识别。
- flushed delete 在 RocksDB 层变成 tombstone。
- recovery 在 PR 4 可以复用同一个 historical mutation sink，不单独实现另一种 delete 规则。

### 3. 三态 local lookup result

新增只读 result holder，例如 `KvStateLookupResult`：

```text
Status.NOT_FOUND
Status.PRESENT + non-empty encoded value
Status.DELETED
```

该类型使用 `@Internal public final`，使 `kv` 与 `kv.historical` package 可以共享它，但不把它声明为用户 API。不要用两个 nullable 字段组合状态。factory method 应保证：

- `notFound()` 不携带 value。
- `deleted()` 不携带 value。
- `present(value)` 只接受 non-null、non-empty value。

为保持 normal write hot path 的 allocation behavior，不要求 defensive copy encoded value；getter 和 Javadoc 必须明确返回值只读，调用方不能修改 byte array。

historical lookup 顺序固定为：

```text
1. 查询 KvPreWriteBuffer
   - 返回 null                   -> 继续查 RocksDB
   - Value.get() == null         -> DELETED
   - Value.get() is non-empty    -> PRESENT

2. 查询 RocksDB
   - 返回 null                   -> NOT_FOUND
   - value.length == 0           -> DELETED
   - value.length > 0            -> PRESENT
```

由此逐步得到 lake fallback 规则：

- 只有 `NOT_FOUND` 表示本地没有决定性状态。
- `PRESENT` 已经给出最新本地 old value。
- `DELETED` 明确表示最新本地操作是 delete。
- 因此 PR 3 只能在 `NOT_FOUND` 时查询 lake。

pending delete 和 flushed tombstone 必须返回相同的 `DELETED`，不能因为 flush boundary 改变可观察结果。

### 4. Historical handle

新增 `HistoricalKvHandle`，每个实例只对应一个 historical `TableBucket`，持有：

```text
TableBucket
historical directory
RocksDBKv
KvPreWriteBuffer
historical KvBatchWriter adapter
ReadWriteLock
lastAccessTime
closed state
```

handle 提供的 storage operations 至少包括：

- composite/local key lookup。
- insert、update 和 delete mutation。
- `flush(exclusiveOffset)`。
- `truncateTo(offset, DUPLICATED|ERROR)`。
- close and delete。
- last-access update/read。

并发契约：

- handle 提供 package-private 的 read-lock/write-lock execution helper，允许调用方在一次 callback 中完成多个 state operations。
- PR 3 必须在同一个 write-lock scope 内完成整批 old-value lookup、prewrite mutations、WAL append 和失败 rollback。
- PR 4 local point lookup 在 read-lock scope 内读取 prewrite 和 RocksDB。
- close/drop 获取 write lock，设置 closed 后按 `prewrite buffer -> RocksDB -> directory` 顺序释放。
- closed handle 的新操作立即失败，不能继续访问 native RocksDB object。
- lock helper 进入时更新 `lastAccessTime`；显式 close/drop 不把 lifecycle 操作计为业务访问。

本 PR 只记录 last-access，不启动 idle scanner。

### 5. Historical manager

新增 `HistoricalKvManager`，负责：

```text
Map<TableBucket, HistoricalKvHandle>
```

推荐由 `KvManager` 创建并持有它，因为 `KvManager` 已经拥有：

- server `Configuration`。
- `TabletServerMetricGroup`。
- shared RocksDB `RateLimiter`。
- KV tablet creation/deletion lifecycle。

manager 同时接收 `Clock`：生产环境使用 `SystemClock`，测试使用 manual clock。handle 初始化和每次业务访问都从该 clock 读取时间，避免在实现中散落 `System.currentTimeMillis()`，也为 PR 4 idle eviction 保留可测试的时间语义。

manager 的最小 API：

```text
getOrCreate(tableBucket, kvTabletDir)
getIfPresent(tableBucket)
invalidateBucket(tableBucket)
invalidateTable(tableId)
close()
```

`getOrCreate()` 行为：

1. 在 manager lifecycle lock 下检查 manager 未关闭。
2. 已存在 handle 时校验其 directory 与本次传入的 `kvTabletDir` 一致，然后返回同一实例。
3. 不存在时根据 `kvTabletDir` 确定性地创建 sibling `<kvTabletDir parent>/historical-kv-<bucketId>`。
4. 使用 `RocksDBResourceContainer`、`RocksDBKvBuilder` 和 shared rate limiter 创建一个 default-CF RocksDB。
5. 用 historical batch-writer adapter 创建 `KvPreWriteBuffer`。
6. 所有资源创建成功后才把 handle 放入 map。
7. 中途失败时关闭已经创建的资源，并删除 incomplete directory。

manager lifecycle 操作需要与并发 create 互斥，避免旧 handle 被移除后，新 handle 正在同一目录创建时，旧 cleanup 又把目录删掉。bucket/table invalidation 属于低频控制操作，可以在 manager lifecycle lock 下完成 remove 和 close/delete，不需要为此增加复杂的 per-key state machine。

显式 lifecycle：

- `KvManager.dropKv(tableBucket)` 在删除普通 tablet directory 前先 `invalidateBucket(tableBucket)`；historical directory 是 sibling，不能依赖普通 directory 的递归删除顺带清理。
- table drop 可以调用 `invalidateTable(tableId)`；即使上层逐 bucket drop，该 API 也要独立可用和可测试。
- `KvManager.shutdown()` 先关闭 historical manager，再关闭 shared RocksDB rate limiter。
- replica removal 最终复用 `dropKv()` 的 bucket invalidation；如果当前 stop path 不删除 KV，则由 PR 4 在 replica lifecycle 接入处补调用，但 PR 2 先提供正确 API。
- close/invalidate 都要幂等；一个 handle close 失败不能阻止 manager 继续关闭其他 handles，最终聚合或记录异常。

historical RocksDB 不加入 `KvManager.currentKvs`，不参与普通 `loadKv()`、snapshot 或 remote snapshot deletion。

### 6. RocksDB 创建参数

historical RocksDB 复用当前 server KV 配置：

- `ConfigOptions.KV_WRITE_BATCH_SIZE`。
- `RocksDBResourceContainer` 的 DB/column options。
- `KvManager` 的 shared RocksDB rate limiter。
- 现有 KV flush counter 和 latency histogram；本 PR 不增加新 metrics。

本 PR 不给 historical RocksDB 注册普通 `KvTablet` snapshot statistics，也不把它注册为一个额外 table metric source。需要区分 historical/normal RocksDB metrics 时再单独设计，避免 PR 2 扩大 metric lifecycle。

### 7. 最小 state-access 扩展点

新增 `@Internal` 的窄 `KvStateAccessor` 或等价接口，把 `KvTablet` row processing 当前直接调用的本地 state 操作收口。由于 normal implementation 位于 `kv` package、historical implementation 位于 `kv.historical` package，Java visibility 需要是 public；`@Internal` 明确它不属于用户 API：

```text
encodeKey(originalPrimaryKey)
lookup(encodedKey) -> KvStateLookupResult
insert(encodedKey, value, logOffset)
update(encodedKey, value, logOffset)
delete(encodedKey, logOffset)
truncateTo(offset, reason)
flush(exclusiveOffset)
```

提供两个实现：

```text
NormalKvStateAccessor
    key: identity
    lookup: prewrite -> normal RocksDB
    delete flush: physical delete

HistoricalKvStateAccessor
    key: HistoricalKvKeyCodec(originalPartitionName, originalPrimaryKey)
    lookup: historical prewrite -> historical RocksDB, preserves DELETED
    delete flush: tombstone
```

`HistoricalKvStateAccessor` 在创建时固定 non-empty original partition name，不能让同一 accessor 在 records 之间切换 partition context。

PR 2 对 `KvTablet` 的改动只做以下机械替换：

- public normal `putAsLeader()` 仍固定使用 `NormalKvStateAccessor`。
- `processKvRecords()`、`processUpsert()` 和 `processDeletion()` 的 state read/write 改为调用 accessor。
- normal accessor 遇到 pending delete 时把 `DELETED` 映射成现有的 absent old-value 语义。
- duplicated/error 分支通过 accessor 调用相同的 `truncateTo()`。
- normal `flush()` 通过 accessor 调用现有 prewrite flush。
- `multiGet()`、snapshot、row count 和 normal RocksDB lifecycle 保持现有实现。

本 PR 不增加接受 historical context 的 public/package-private `putAsLeader()` overload，也不让 `KvTablet` 执行 historical WAL append。PR 3 再把同一套 processing core 暴露给 historical processor，并为 `NOT_FOUND` 增加 lake decorator。这样 PR 2 的 refactor 有 normal regression tests 覆盖，同时不会提前交付半成品 historical processor。

不要创建 `HistoricalKvTablet` 并复制整个 `KvTablet`。也不要把 lake API 放进 `KvStateAccessor`；本 PR 的 accessor 只描述本地 state。

## 详细实施步骤

### Step 1: 增加 composite key codec

新增：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvKeyCodec.java
```

实现：

- `encode(String originalPartitionName, byte[] originalPrimaryKey)`。
- UTF-8 byte length prefix。
- big-endian layout。
- null、empty partition name 和 size overflow 校验。
- 不缓存或复用调用方传入的 mutable key array。

### Step 2: 增加 lookup result 和 tombstone writer

新增：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/KvStateLookupResult.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvBatchWriter.java
```

实现：

- `NOT_FOUND`、`PRESENT`、`DELETED` 三态 result。
- historical delete 到 empty-value put 的转换。
- empty normal value rejection。
- delegate flush/close。

`KvStateLookupResult` 和 `KvStateAccessor` 都需要跨 `kv`/`kv.historical` package 使用，因此采用 `@Internal public`；除这些必要类型外，其余 helper 保持 package-private。

### Step 3: 抽取 normal state accessor

新增或调整：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/KvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/NormalKvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java
```

要求：

- normal accessor 包装现有 `KvPreWriteBuffer` 和 `RocksDBKv`。
- identity key encoding 不复制 key，保持当前 allocation behavior。
- normal lookup 的结果转换不改变当前 old-value behavior。
- normal delete 仍最终调用 RocksDB physical delete。
- `KvTablet` 的 merge、schema evolution、auto-increment、WAL builder 和 error handling 不重写。

### Step 4: 实现 historical handle 和 accessor

新增：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvHandle.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvStateAccessor.java
```

要求：

- handle 创建独立 RocksDB 和 historical prewrite buffer。
- accessor 固定 original partition name，并统一调用 codec。
- local lookup 保留三态结果。
- pending delete 和 flushed tombstone 结果一致。
- batch write-lock scope 内可以执行多条 mutation 后统一 rollback/flush。
- close/drop 幂等，并在删除 directory 前关闭所有 native resources。

### Step 5: 实现 manager 和 `KvManager` ownership

新增/修改：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvManager.java
fluss-server/src/main/java/org/apache/fluss/server/kv/KvManager.java
```

要求：

- `KvManager` constructor 创建 historical manager。
- manager 复用 configuration、metrics 和 shared rate limiter。
- 提供 focused testing accessor 或 package-private getter，不增加用户 API。
- `dropKv()` 先 invalidates historical handle，再 drop normal KV directory。
- `shutdown()` 先关闭 handles，再关闭 shared rate limiter。
- `getOrCreate()` 失败不在 map 或磁盘留下可被误用的 incomplete handle。

### Step 6: 补充生命周期与状态注释

Javadoc/注释必须明确：

- historical RocksDB 是 disposable local cache/state，不是事实源。
- `TableBucket` 是 actual historical target，original partition 只进入 composite key。
- tombstone 为什么不能使用 physical delete。
- 三态 lookup 中只有 `NOT_FOUND` 允许 lake fallback。
- handle write lock 必须覆盖完整 batch 和 rollback boundary。
- directory 为什么使用非 `kv-*` sibling，而不是放进普通 KV tablet directory。

不要在本 PR 注释中宣称 historical write 已经对用户可用。

## 预计文件范围

新增的主要实现文件：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/KvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/KvStateLookupResult.java
fluss-server/src/main/java/org/apache/fluss/server/kv/NormalKvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvKeyCodec.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvBatchWriter.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvHandle.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvManager.java
```

预计修改：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java
fluss-server/src/main/java/org/apache/fluss/server/kv/KvManager.java
```

预计新增测试：

```text
fluss-server/src/test/java/org/apache/fluss/server/kv/historical/HistoricalKvKeyCodecTest.java
fluss-server/src/test/java/org/apache/fluss/server/kv/historical/HistoricalKvBatchWriterTest.java
fluss-server/src/test/java/org/apache/fluss/server/kv/historical/HistoricalKvHandleTest.java
fluss-server/src/test/java/org/apache/fluss/server/kv/historical/HistoricalKvManagerTest.java
```

实际实现时，如果 `NormalKvStateAccessor` 或 lookup result 足够小，可以作为 package-private class 放在同一文件；不要为了与计划文件名一一对应而增加无意义文件。

## 测试计划

### HistoricalKvKeyCodecTest

覆盖：

- ASCII partition name。
- UTF-8 partition name，验证 prefix 等于 UTF-8 bytes 长度而不是 Java string length。
- 多级 partition name，例如 `dt=2026-07-12/region=cn`。
- empty primary key。
- 包含零 byte 和任意 binary bytes 的 primary key。
- 较长 partition name 和 primary key。
- `("ab", "c")` 与 `("a", "bc")` 编码不同。
- 同一 partition/key 多次编码结果稳定。
- 不同 partition、相同 primary key 编码不同。
- null/empty partition name 和 null key 被拒绝。

### HistoricalKvBatchWriterTest

使用 recording `KvBatchWriter` 验证：

- normal non-empty value 原样调用 delegate `put()`。
- delete 调用 delegate `put(key, emptyValue)`，不调用 delegate `delete()`。
- empty value 不能通过普通 `put()` 写入。
- flush 和 close 只转发一次并保持异常。

### HistoricalKvHandleTest

使用临时目录和真实 RocksDB 覆盖：

- insert 在 prewrite 中立即返回 `PRESENT`。
- update 覆盖同 key 的 pending value。
- delete 在 flush 前返回 `DELETED`。
- flush 后 prewrite entry 消失，RocksDB lookup 仍返回 `DELETED`。
- RocksDB 内 delete 对应 value 长度为 0，key 没有被物理删除。
- 从未写过的 key 返回 `NOT_FOUND`。
- `NOT_FOUND`、`PRESENT`、`DELETED` 不互相混淆。
- 同一 original primary key 在两个 original partitions 中互不影响。
- empty value 不能被解释为普通 encoded row。
- partial flush 前后 lookup 结果一致。
- `truncateTo(..., DUPLICATED)` 恢复 batch 前 value。
- `truncateTo(..., ERROR)` 恢复 batch 前 value。
- insert 后 rollback 回到 `NOT_FOUND`。
- delete 后 rollback 恢复之前的 `PRESENT`。
- close 幂等，close 后 operation 失败。
- drop 关闭 RocksDB 并删除 `historical-kv-<bucketId>` directory。

### HistoricalKvManagerTest

覆盖：

- 同一个 `TableBucket` 重复 `getOrCreate()` 返回同一 handle。
- 不同 bucket 返回不同 handle 和不同 directory。
- 同 table 不同 partition id 不共享 handle。
- 相同 bucket 但不同 base directory 被拒绝。
- `invalidateBucket()` 只删除目标 handle。
- `invalidateTable()` 删除该 table 的全部 handles，不影响其他 table。
- `close()` 删除所有 handles，并且幂等。
- close 后不能再次创建 handle。
- 一个 handle close 失败时仍继续处理其他 handles。
- create 中途失败不会把 handle 放入 map，也不会留下 incomplete directory。
- concurrent `getOrCreate()` 对同一 bucket 只创建一个 RocksDB。
- create 与 invalidate 不会同时操作同一个 directory。
- last-access time 在业务 read/write lock scope 更新，可通过 manual clock 验证。

### Normal KvTablet regression

继续运行现有：

```text
KvTabletTest
KvTabletMergeModeTest
KvTabletSchemaEvolutionTest
```

重点验证：

- normal insert/update/delete 的 RocksDB 内容不变。
- normal delete 仍是 physical delete，不写 empty-value tombstone。
- DEFAULT/OVERWRITE merge mode 不变。
- partial update 和 configured row merger 不变。
- duplicated batch rollback 不变。
- WAL append error rollback 不变。
- prewrite flush offset 和 row count behavior 不变。
- normal `multiGet()` 和 snapshot behavior 不变。

不需要新增 `TabletServiceTest` 或 RPC test，因为本 PR 不接入在线请求。

## 兼容性

### Wire compatibility

本 PR 不修改 protobuf、client request 或 response，没有 wire compatibility 变化。

### Local disk compatibility

- 普通 RocksDB directory 和内容不变。
- 新目录只在内部 focused call 首次创建 historical handle 时出现。
- historical state 不作为持久化格式承诺；restart/recovery 可以先删除再重建。
- `TabletManagerBase` 不扫描 `historical-kv-<bucketId>` 为普通 KV tablet。

### Rolling upgrade

- PR 2 没有在线入口，old/new server 混部时正常 PutKv behavior 不变。
- historical state format 在 PR 4 online enable 前仍可以调整。
- 不允许 PR 2 的存在成为 client 启用 historical routing 的判断条件。

## 本 PR 必须保证的行为

### 风险 1: 不同 original partitions 的相同 PK 相互覆盖

防护：所有 historical state operations 统一通过 length-prefixed composite key codec；普通 key codec 不变。

### 风险 2: delete 后从 lake 复活旧值

防护：pending delete 和 flushed tombstone 都返回 `DELETED`；只有 `NOT_FOUND` 才允许 PR 3 fallback 到 lake。

### 风险 3: tombstone 被当成合法 row value

防护：historical writer 的普通 `put()` 拒绝 empty value，lookup result 也不允许 `PRESENT(emptyValue)`。

### 风险 4: 修改 `KvPreWriteBuffer` 影响 normal delete

防护：不修改 buffer 的 delete/flush 判断；只在 historical `KvBatchWriter` adapter 中把 delete 转成 tombstone put。

### 风险 5: 为 historical path 复制 `KvTablet` processing

防护：抽取窄 `KvStateAccessor`，普通路径先迁移并用现有 tests 验证；merge/WAL/auto-increment 逻辑仍保留单份。

### 风险 6: close/delete 与 lookup 并发导致 native crash

防护：handle 统一使用读写锁；lookup 持 read lock，完整 write batch 和 close/drop 持 write lock。

### 风险 7: create/invalidate 竞争删除新实例目录

防护：manager lifecycle lock 串行化同一 map 和 directory 的 create、remove、close/delete。

### 风险 8: shutdown 先关闭 shared rate limiter

防护：先关闭 historical handles，再关闭 `KvManager` shared RocksDB rate limiter。

### 风险 9: historical directory 被普通 recovery 扫描

防护：使用 `<kvTabletDir parent>/historical-kv-<bucketId>/db`，directory 不带 `kv-` prefix，也不加入 `currentKvs`。

### 风险 10: PR 2 提前暴露不可恢复的在线路径

防护：不修改 `TabletService`、`ReplicaManager` dispatch 和 client routing；没有生产请求能够获得 historical state accessor。

## 验证命令

运行 focused storage tests：

```bash
./mvnw test -pl fluss-server \
  -Dtest=HistoricalKvKeyCodecTest,HistoricalKvBatchWriterTest,HistoricalKvHandleTest,HistoricalKvManagerTest
```

运行 normal KV regression：

```bash
./mvnw test -pl fluss-server \
  -Dtest=KvTabletTest,KvTabletMergeModeTest,KvTabletSchemaEvolutionTest,KvPreWriteBufferTest
```

格式和模块验证：

```bash
./mvnw spotless:check -pl fluss-server
./mvnw verify -DskipITs -pl fluss-server -am
```

检查 Java 8 compatibility：

```bash
./mvnw clean install -DskipTests -Pjava8 -pl fluss-server -am
```

## 完成标准

以下条件全部满足后，PR 2 才算完成：

- composite key 使用 4-byte big-endian UTF-8 byte-length prefix。
- 不同 original partitions 的相同 primary key 不冲突。
- 每个 historical `TableBucket` 最多存在一个 active handle 和一个 default-CF RocksDB。
- historical RocksDB 只在显式内部 `getOrCreate()` 时懒创建。
- historical directory 与普通 `<kvTabletDir>/db` 隔离，且不被普通 tablet loader 识别。
- historical delete flush 写 empty-value tombstone，不执行 physical delete。
- pending delete 和 flushed tombstone 都返回 `DELETED`。
- local miss 返回 `NOT_FOUND`，与 `DELETED` 明确区分。
- historical prewrite 支持 `DUPLICATED` 和 `ERROR` rollback。
- `KvPreWriteBuffer` 的 normal behavior 不修改。
- normal `KvTablet` 的 put/delete/merge/WAL/flush/snapshot tests 保持通过。
- handle 的 complete-batch write lock、lookup read lock 和 close/drop write lock 契约明确且有测试。
- bucket/table/server 显式 lifecycle 可以关闭并删除所有 historical resources。
- shutdown 顺序不会让 handle 使用已经关闭的 shared rate limiter。
- 本 PR 不修改 RPC dispatch、lake lookup、recovery、cleanup 或 client routing。
- focused tests、Spotless 和受影响模块验证通过。

## 合并后的行为

PR 2 合并后：

- server 内部可以按 historical bucket 创建独立 local state，并通过 composite key 隔离多个 original partitions。
- insert、update、delete、flush 和 rollback primitives 可以在 focused tests 中使用。
- delete 在 prewrite 和 RocksDB 两层都有明确的 `DELETED` 语义，为 PR 3 的 lake fallback 提供正确边界。
- 普通 `KvTablet` 已通过窄 state accessor 为后续复用做好准备，但其线上行为不变。
- 没有生产 RPC 会进入 historical storage，正常 client 也不会创建 historical RocksDB。
- historical write 仍不可用；完整能力还依赖 PR 3 的 processor、PR 4 的 dispatch/recovery/cleanup，以及后续 client routing 和 tiering PR。
