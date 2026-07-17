# FIP-28 Historical Write PR 4 实施计划

## PR 标题

```text
[server] Enable historical primary-key write serving
```

## 目标

本 PR 将 PR 3 已完成的 historical primary-key write processor 接入线上 `PutKv` RPC，并补齐 server 真正接收 historical write 前必须具备的运行时能力：

- `TabletService` 根据 RPC bucket 是否携带 `original_partition_name` 分流 normal write 与 historical write。
- normal write 保持当前同步处理路径和性能特征。
- historical write 在离开 RPC thread 前复制到 heap，并异步运行在 TabletServer `ioExecutor`。
- 同一个 historical `TableBucket` 的 write、recovery、cleanup 按 FIFO 串行，不同 bucket 可以并发。
- historical lookup 与 write 共享有界准入控制，避免 lake I/O、WAL recovery 和 RocksDB 工作无限堆积。
- historical write 完成后继续复用当前 delayed write、`requiredAcks` 和 response callback 语义。
- 第一次 historical write 或 lookup 从 lake offset、remote WAL 和 local WAL 懒恢复 disposable historical KV state。
- high watermark 推进时把 historical prewrite state flush 到 RocksDB。
- lake tiering 追平后安全删除 historical RocksDB，并支持仅针对 fully-tiered handle 的 idle cleanup。
- replica demotion、stop、table drop 和 server shutdown 会取消排队任务并释放资源，不留下悬挂 future。
- lookup/write client 对共享的 historical throttle error 使用指数退避。

本 PR 不启用 client expired-partition write fallback。正常 writer 在 PR 6 合并前仍不会主动把 expired partition write 重定向到 `__historical__` partition。

## 与总计划的关系

本 PR 对应 `FIP-28-historical-partition-write-pr-plan-zh.md` 中的：

```text
PR 4: Server Dispatch + Flow Control + Recovery + Cleanup
```

前置依赖：

- PR 1 已让 `PutKvRequest` bucket 携带 nullable `original_partition_name`，并让 client 将 normal/historical batch 拆成不同 RPC。
- PR 1 已提供 `PutKvDataForBucket` 和 `ServerRpcMessageUtils.toPutKvDataForBuckets()`，server 可以在 decode 后保留 original partition context。
- PR 2 已提供 `HistoricalKvManager`、`HistoricalKvHandle`、composite key、tombstone 和 local tri-state lookup。
- PR 3 已提供 `HistoricalPkWriteProcessor`、`HistoricalPkWriteManager`、local-first lake fallback，以及 `ReplicaManager.historicalPutKv()` 的内部入口。
- historical lookup 已提供 actual target validation、Paimon `LakeTableLookuper` 和 lookup client throttle backoff。

后续依赖：

- PR 5 修改 Paimon historical tiering，使一个 `__historical__` WAL bucket 中属于多个 original partitions 的 row 能写回各自的 Paimon partition。
- PR 6 启用 client expired-partition eligibility、metadata fallback 和 write target redirect，正常 writer 才开始主动发送 historical write。
- PR 7 再补端到端 rollout、metrics 和兼容性验证。

## 前置假设

- `PutKvDataForBucket.tableBucket()` 是实际 RPC target，historical write 时指向 `__historical__` physical partition bucket。
- `PutKvDataForBucket.originalPartitionName()` 是 row 原本所属的 expired business partition，同时作为 normal/historical write 的 RPC routing signal。
- 不携带 `original_partition_name` 的 request 走 normal path；携带该字段的 request 走 historical path。
- actual replica metadata不参与dispatch，但两个处理路径仍需校验target：historical path拒绝normal replica，normal path拒绝historical replica。
- `LogTablet.getLakeLogEndOffset()` 是 exclusive offset。值为 `N` 表示 `[0, N)` 已进入 lake，recovery 从 `N` 开始读取 WAL。
- `LogTablet.getHighWatermark()` 和 `localLogEndOffset()` 也使用 exclusive offset，因此 committed 与 uncommitted recovery 范围分别是 `[start, highWatermark)` 和 `[highWatermark, localLogEndOffset)`。
- historical WAL row 保留完整 row 和 original partition columns；DELETE WAL row 保留被删除的 old row，因此 recovery 可以重建 original partition name 和 tombstone composite key。
- `RemoteLogFetcher.fetch(start, localStart)` 可以覆盖 recovery start 早于 local log start 的部分；如果 remote WAL 也不完整，recovery 必须失败，不能跳过 offset gap 后返回部分 state。
- historical local state 是可删除的 cache/state materialization。删除后可以使用 lake state 加 WAL 重新构建，不参与 ordinary KV snapshot/checkpoint。
- 当前 `Replica.putHistoricalRecordsToLeader()` 已在 `leaderIsrUpdateLock` read lock 下完成 leader、ISR、append 和 high-watermark 校验，PR 4 继续把它作为 historical write 的最后 fencing boundary。
- client PR 1 已隔离 normal/historical request，但 server 仍需拒绝手工构造或旧实现发送的 mixed request，不能依赖 client 保证正确性。

## 非目标

本 PR 不实现：

- client 对 expired partition 的识别和自动 redirect。
- Paimon writer 按 WAL row 动态选择 original partition；该能力属于 PR 5。
- append-only/log table historical write。
- Iceberg、Lance、Hudi 等其他 lake format 的 historical write。
- historical RocksDB snapshot、checkpoint 或 follower standby state。
- follower 持续维护 historical RocksDB。
- historical prefix lookup、scan 或 `insertIfNotExists`。
- normal/historical mixed `PutKvRequest` 的部分执行或跨路径 aggregation；第一版整请求拒绝。
- 修改 `PutKvResponse` correlation key；同一个 request 内仍要求每个 `TableBucket` 最多出现一次。
- durable cleanup marker。cleanup 后第一次访问仍通过 lake offset 和 WAL 判断需要恢复的范围。
- 改变 normal write 的 retry backoff；新增 backoff 只对 historical throttle 生效。

## 当前实现约束

### 1. `TabletService.putKv()` 仍丢失 historical context

当前入口使用：

```text
getPutKvData(request)
    -> Map<TableBucket, KvRecordBatch>
    -> ReplicaManager.putRecordsToKv()
```

该 decoder 不保留 `original_partition_name`。PR 1 新增的 `toPutKvDataForBuckets()` 尚未接入 online path。

PR 4 应让 `TabletService.putKv()` 只 decode 一次 `Map<TableBucket, PutKvDataForBucket>`，然后根据 request中的 `originalPartitionName` 决定调用normal或historical path。完成迁移后删除或停止使用重复的normal-only decoder，避免两个decoder逐渐产生行为差异。

### 2. RPC 字段决定 dispatch，处理路径校验 actual target

dispatch只需要检查 `original_partition_name`：

```text
original_partition_name is null
    -> normal write path

original_partition_name is non-null
    -> historical write path
```

这样 `TabletService` 不需要为了dispatch提前解析每个 `Replica`。target mismatch由具体处理路径兜底：

- `ReplicaManager.historicalPutKv()` 已检查 `replica.isHistoricalPartition()`；normal replica携带original name时会得到明确的`InvalidPartitionException`。
- normal path需要在 `Replica.putRecordsToLeader()` 中先检查 `!isHistoricalPartition()`；historical replica缺少original name时返回明确的`InvalidPartitionException`，不能继续走到`kvTablet == null`。

server仍需在任何mutation前扫描同一request中的字段一致性。只要有的bucket携带original name、有的不携带，就整请求拒绝mixed request。该扫描不查询replica metadata。

### 3. 当前 historical manager 不保证同 bucket FIFO

`HistoricalPkWriteManager.put()` 当前直接执行：

```text
CompletableFuture.supplyAsync(task, ioExecutor)
```

两个针对同一 `TableBucket` 的 request 可能在不同 worker 并发运行。`HistoricalKvHandle` 的 write lock 可以避免同时修改 RocksDB，但无法保证锁获取顺序等于 RPC 提交顺序。writer id 和 batch sequence 语义要求同 bucket 按接收顺序执行。

代码库当前没有可直接复用的 keyed serial executor。PR 4 应新增一个小型 server-internal executor wrapper，不引入新依赖，也不创建额外线程池。

### 4. async historical task 不能继续引用 RPC buffer

`ServerRpcMessageUtils` 通过 `recordsSlice` 构造 `DefaultKvRecordBatch`，其 `MemorySegment` 可能指向 RPC/Netty request buffer。normal write 在 RPC call stack 内同步消费该 buffer；historical write 会延迟到 `ioExecutor` 执行。

historical permit 获取成功后、任务入队前，必须把完整 `KvRecordBatch` 复制到独立 heap byte array。permit 获取失败时不应先复制大 batch。

### 5. delayed write 当前假设 local processing 已同步完成

`ReplicaManager.putRecordsToKv()` 先得到所有 bucket 的 `PutKvResultForBucket`，再调用：

```text
maybeAddDelayedWrite(timeoutMs, requiredAcks, requestBucketSize, results, callback)
```

historical path 不能在 task 刚提交时调用 delayed write。它必须等待 request 中所有 historical bucket future 得到 initial append result，再把完整 result map 交给同一个 `maybeAddDelayedWrite()`。

这样 `acks=0/1/-1`、partial bucket failure、timeout 和 follower replication 等待继续使用现有语义。

### 6. lookup semaphore 不能覆盖 historical write 与 recovery

`HistoricalLakeLookupManager` 当前私有持有 `Semaphore lookupPermits`，只限制 point lookup。historical write 可能执行 lake old-value lookup，first access 还可能执行 remote WAL recovery，资源成本不低于 lookup。

PR 4 应把 semaphore 提取成由 lookup/write 共享的 request limiter。permit 粒度是一个 historical bucket request，不是 row、key、RPC 或内部 lake lookup 次数。

### 7. historical handle 当前没有 restart recovery lifecycle

`HistoricalKvManager.getOrCreate()` 会删除目标目录并创建空 RocksDB。PR 3 允许 first write 创建 handle，但 restart 后 first lookup 仍会把 missing handle 解释为 local miss 并直接 fallback lake。这会漏掉已经进入 historical WAL、尚未 tier 到 lake 的记录。

PR 4 必须保证 first write 和 first lookup 在读取或修改 local state 前完成 WAL recovery；并发 first access 只能执行一次 recovery，其他访问复用同一次结果或排在同一个 keyed recovery task 后。

### 8. high watermark 推进目前不会 flush historical prewrite

`Replica.mayFlushKv()` 当前只处理普通 `KvTablet`：

```text
if (kvTablet != null) {
    kvTablet.flush(newHighWatermark, ...)
}
```

historical replica 故意不创建普通 `KvTablet`，所以 historical prewrite buffer 中已复制完成的 mutation 不会随 high watermark 推进而 flush 到 RocksDB。

PR 4 必须增加 historical 分支，将 exclusive high watermark 传给当前 handle 的 `flush()`。该调用应与 handle write/recovery lock 协调；没有 READY handle 时直接跳过，不能为了 flush 创建空 state。

### 9. cleanup 不能在 lookup 持有 stale handle reference 时删除目录

当前 lookup 先 `getIfPresent()`，之后才获取 handle read lock。如果 cleanup 在两步之间从 manager 移除并关闭 handle，lookup 可能对 closed handle 执行读取。

PR 4 应让 manager 提供带生命周期保护的 read helper，保证“取当前 handle + 获取 read lock”与 cleanup 的“确认 current handle + try write lock + remove/drop”使用固定锁顺序：

```text
HistoricalKvManager lifecycle lock
    -> HistoricalKvHandle state lock
```

manager lifecycle lock 只保护 handle identity确认和 state lock获取；拿到 read lock后立即释放 manager lock，再执行 RocksDB lookup，不能让一个bucket的慢lookup阻塞其他bucket。cleanup 获取不到 write lock时立即跳过，不能阻塞 point lookup。

### 10. throttle error 和 write retry 仍是 lookup-specific

当前 wire error 为：

```text
HISTORICAL_LOOKUP_THROTTLED = 71
HistoricalLookupThrottledException
```

write `Sender` 已能 retry `RetriableException`，但 `RecordAccumulator` 仍有 retry backoff TODO；re-enqueued batch 会立刻再次 drain，可能形成 throttle busy loop。

PR 4 需要把 error 语义泛化到 historical partition request，并给 historical write batch 保存 next retry time。`ready()` 和 `drain()` 都要遵守该时间，normal batch 保持当前行为。

### 11. lifecycle 与 shutdown 顺序需要先停任务再关依赖

当前 TabletServer shutdown 顺序会先关闭 `KvManager`、`RemoteLogManager` 和 `LogManager`，最后才关闭 `ReplicaManager`。PR 4 的 queued recovery/write/cleanup task 会使用这些依赖。

PR 4 必须调整相关关闭顺序：先停止 RPC intake 和 historical task admission，取消/完成 pending task，再关闭 historical lookup、KV、remote log和 local log资源，最后关闭共享 `ioExecutor`。

## 核心设计

### 1. 基于 RPC 字段的 request dispatch

`TabletService.putKv()` 使用 contextual decoder 后调用新的 request-level入口，例如：

```java
replicaManager.dispatchPutRecordsToKv(
        timeoutMs,
        requiredAcks,
        putDataByBucket,
        targetColumns,
        mergeMode,
        apiVersion,
        callback);
```

`ReplicaManager` 在任何mutation前只遍历 `PutKvDataForBucket.originalPartitionName()`：

| Request bucket fields | Dispatch结果 |
|---|---|
| 全部 `originalPartitionName == null` | normal path |
| 全部 `originalPartitionName != null` | historical path |
| null与non-null同时存在 | 整请求返回`InvalidPartitionException` |

具体执行路径再校验actual target：

- normal path把holder转换为 `Map<TableBucket, KvRecordBatch>`，调用现有`putRecordsToKv()`；每个replica在`putRecordsToLeader()`中拒绝historical target。
- historical path进入新的request-level async入口；现有`historicalPutKv()`检查每个replica确实是historical target。
- replica missing/offline继续返回现有bucket error。

mixed request 必须整请求拒绝，因为 normal path 会同步 append，而 historical path 会异步 append。先执行一侧再发现另一侧无法安全聚合会产生非预期 partial write。PR 1 已让正常 client 避免生成 mixed request，因此该分支主要用于 server defensive validation。

### 2. historical request 执行链

每个 historical bucket 的执行顺序为：

```text
RPC field dispatch + actual historical target validation
    -> try acquire one shared historical permit
    -> copy KvRecordBatch to heap
    -> enqueue by TableBucket
    -> lazy recover if state is not READY
    -> HistoricalPkWriteProcessor.process()
    -> produce PutKvResultForBucket
    -> release permit
```

request-level 聚合顺序为：

```text
all bucket initial futures complete
    -> combine success/error results
    -> maybeAddDelayedWrite(...)
    -> complete PutKvResponse callback exactly once
```

关键要求：

- limiter reject、heap copy failure、executor reject、task failure、cancellation 和正常完成都必须生成 bucket result 并释放 permit。
- permit 从准入成功一直持有到该 bucket 的 write/recovery future 完成，限制的是 queued + running historical work。
- internal lake lookup 不额外获取 permit；其调用已经包含在 historical write bucket permit 中。
- response aggregation 不在 `ioExecutor` worker 上阻塞等待 future，使用 `CompletableFuture` composition 或 completion counter。
- callback 不在 task queue lock 或 `HistoricalKvHandle` lock 内执行。

### 3. shared historical limiter

新增 package-private `HistoricalRequestLimiter`，由 `ReplicaManager` 构造一个实例并同时传给 `HistoricalLakeLookupManager` 与 historical write dispatch。

配置继续使用：

```text
netty.server.max-queued-historical-requests
```

配置说明改为 write/lookup 共享的 queued + running historical bucket requests。值必须大于 0。

推荐 limiter 返回一次性 permit token，而不是让调用方直接操作 `Semaphore`：

```java
Optional<Permit> tryAcquire();

interface Permit extends AutoCloseable {
    void close();
}
```

token 内部用 `AtomicBoolean` 保证 exactly-once release。这样 future completion、executor rejection 和显式 cancellation 可以统一调用 `close()`，避免 double release 把上限越放越大。

normal write 完全绕过该 limiter。

### 4. per-bucket serial task executor

新增 package-private `HistoricalPartitionTaskExecutor`，包装共享 `ioExecutor`：

```text
Map<TableBucket, BucketQueue>

BucketQueue:
    FIFO deque<QueuedTask>
    running flag
    generation/cancelled state
```

行为约束：

- `submit(tableBucket, callable)` 按提交顺序放入该 bucket queue。
- 一个 bucket 同一时间最多运行一个 task。
- 当前 task 完成后再把下一个 task提交到 `ioExecutor`；不要在一个 worker 中永久 drain 整个 queue，避免 hot bucket 长时间占用 worker。
- 不同 bucket 的 head task 可以同时运行。
- task 抛出的异常只完成自己的 future，不阻止同 bucket 后续 task继续执行。
- `ioExecutor.execute()` reject 时，当前和该次无法继续调度的 task 都以明确 exception 完成，不能留在 `running=true` 状态。
- `cancelBucket()` 失败完成尚未开始的 task，并递增 generation，阻止旧 recovery结果在生命周期切换后发布为 READY。
- 已经运行的 write 不强制 interrupt。它持有 replica lifecycle read lock；demotion/delete 获取 write lock时会等待当前 append 完成，然后清理 handle。
- `close()` 停止新 admission，失败完成所有 queued task；共享 `ioExecutor` 由 TabletServer 统一关闭，该 wrapper 不拥有线程池。

historical write、recovery 和 cleanup 使用同一个 keyed executor。lookup 的实际 RocksDB/lake read仍可在不同 I/O worker 并发，但其 first-access recovery gate 必须通过同一个 bucket queue。

### 5. historical state lifecycle

每个 historical bucket 使用以下逻辑状态：

```text
UNINITIALIZED
    -> RECOVERING
    -> READY

RECOVERING --failure/cancel--> UNINITIALIZED
READY --cleanup/demotion/drop--> UNINITIALIZED
```

实现可以在 `HistoricalKvManager` 中显式保存状态，也可以由 per-bucket queue + current recovery future 提供等价 coalescing，但必须满足：

- 同一 bucket 同时最多一个 recovery。
- concurrent first write/lookup 都等待同一次 recovery结果。
- handle 只有完整 replay成功后才能被 lookup/write 观察为 READY。
- failure 时关闭并删除 incomplete handle，移除 recovery marker；下一次访问可以重新尝试。
- cancel/demotion 后旧 generation 的 recovery不能重新注册 handle。
- READY 表示 handle 已覆盖 recovery启动时需要的 WAL 范围，不表示它永远不会有 prewrite records。

推荐由 server-side `HistoricalKvLifecycleManager` 协调 task executor、recoverer 和 cleanup，`HistoricalKvManager` 继续只负责 handle/目录的 storage lifecycle。这样不会让底层 KV manager 反向依赖 `ReplicaManager`、`RemoteLogManager` 或 RPC dispatch。

### 6. recovery offset 与 state重建

first access 在 actual leader replica 的 lifecycle read lock 下获取一致的 recovery边界：

```text
lakeEnd = logTablet.getLakeLogEndOffset()
availableStart = logTablet.logStartOffset()
start = lakeEnd >= 0 ? lakeEnd : availableStart

committedEnd = logTablet.getHighWatermark()
localEnd = logTablet.localLogEndOffset()
```

边界校验：

- `start > localEnd`：lake offset/log metadata 不一致，recovery失败。
- `committedEnd > localEnd`：log metadata 不一致，recovery失败。
- `start < localLogStartOffset()`：必须通过 `RemoteLogFetcher` 补齐 `[start, localLogStartOffset)`。
- remote segment 缺失或中间出现 offset gap：recovery失败并删除 incomplete handle。
- `start >= localEnd`：创建空 READY handle即可；后续 lookup会 fallback lake。

recovery 开始时先确认 local disk仍允许写入，关闭并删除该 bucket 遗留的 historical RocksDB目录，再创建空 handle。旧目录中的内容不能作为恢复起点，因为它没有独立的durable replay offset，无法证明与当前leader epoch和lake offset一致。

replay分两段：

```text
[start, committedEnd)，仅当 start < committedEnd
    -> 直接写 historical RocksDB

[max(start, committedEnd), localEnd)
    -> 按真实 log offset 写 historical prewrite buffer
```

如果 `start >= committedEnd`，第一段是空集，只执行第二段。该情况可以出现在local high-watermark checkpoint暂时落后于已通知的lake end offset时，不能仅因为两个offset的大小关系拒绝recovery。

恢复完成前再次读取当前 high watermark，并对 prewrite执行一次 `flush(currentHighWatermark)`，覆盖 recovery期间 follower ack推进但没有再次触发 flush 的竞态。

#### WAL record 转换

对每条 `LogRecord`：

1. 根据 batch/schema id 获取正确 `RowType` 和 `LogRecordReadContext`。
2. 跳过 `UPDATE_BEFORE`；最终 state由 `UPDATE_AFTER` 表示。
3. 从 row 的 partition columns 按 table partition key 顺序提取 typed values。
4. 使用 `PartitionUtils.convertValueOfType()` 转换成与 client `PartitionGetter` 一致的 partition value string。
5. 构造 `ResolvedPartitionSpec` 并得到 `originalPartitionName`。
6. 用现有 primary-key encoder 生成 original primary key bytes。
7. 调用 `HistoricalKvKeyCodec`/`HistoricalKvStateAccessor.encodeKey()` 生成 composite key。
8. INSERT/UPDATE_AFTER 写 encoded value；DELETE 写 historical tombstone。

DELETE 必须使用 WAL old row 中的 partition columns 和 primary key。不能只从 actual `__historical__` target 推断 original partition，也不能把 DELETE 当成普通 RocksDB key removal，否则 lake fallback 会复活已删除数据。

#### 与普通 recovery 的复用边界

优先复用 `KvRecoverHelper` 已有的以下基础设施：

- `RemoteLogFetcher`。
- `LogTablet.read()` 的 `HIGH_WATERMARK`/`LOG_END` isolation。
- Arrow/Compacted `LogRecordReadContext` 创建方式。
- schema getter 和 key/value encoder。

不要让 historical recovery伪装成普通 `KvTablet` recovery。ordinary helper 同时维护 row count、auto-increment、snapshot offset和 normal key layout，直接复用其完整 `recover()` 会写错 state。若为了减少重复提取公共 reader，应只抽取“按 offset 顺序读取 local/remote WAL batch”的窄 helper，并保持普通 recovery行为不变。

### 7. high watermark flush

`Replica.mayFlushKv(newHighWatermark)` 增加 historical分支：

```text
normal replica with KvTablet
    -> existing KvTablet.flush()

historical replica with READY handle
    -> HistoricalKvHandle.flush(exclusiveHighWatermark)

historical replica without READY handle
    -> no-op
```

flush 不触发 recovery，也不创建 RocksDB。若 historical handle 正在 recovery，recovery完成前不可见；recovery末尾的 catch-up flush负责覆盖这次 watermark。

flush failure沿用 KV storage fatal/error处理原则，不能先推进 high watermark 再静默丢失 prewrite mutation。

### 8. tiering cleanup 与 idle cleanup

#### offset-driven cleanup

`ReplicaManager.notifyLakeTableOffset()` 更新一个 historical bucket 的 lake metadata 后执行轻量 candidate check：

```text
lakeLogEndOffset >= localLogEndOffset
```

满足时只提交 cleanup task，不在 coordinator RPC thread 上关闭 RocksDB。

cleanup task进入同一个 per-bucket serial executor，并在执行时重新获取 actual replica/handle和 offset：

1. replica 仍在线、仍是 leader、仍是 historical partition。
2. current handle 仍是 candidate check时对应的 READY handle/generation。
3. `lakeLogEndOffset >= localLogEndOffset` 仍成立。
4. handle `lastAccessTime`/cleanup reason仍满足要求。
5. `tryLock()` 获取 handle write lock。
6. 获取失败立即跳过，保留 handle。
7. 获取成功后从 manager移除 current handle、关闭 RocksDB并删除整个目录。

cleanup 与 historical write通过 keyed executor 串行；cleanup 与已经运行的 lookup通过 handle read/write lock协调。

#### idle cleanup

新增配置：

```text
kv.historical.idle-timeout
```

建议默认值为 `3 h`，与 historical lake lookuper cache 的 idle 周期保持同一数量级。配置值必须大于 0。

周期任务只扫描 manager中已经 READY 的 handle。第一版 idle handle同时满足以下条件才提交 cleanup：

```text
now - lastAccessTime >= idleTimeout
lakeLogEndOffset >= localLogEndOffset
```

idle 但尚未 fully tiered 的 handle不能删除，否则下一次访问会重复执行 potentially expensive remote WAL recovery。周期扫描只做候选收集，实际 offset recheck和删除仍在 per-bucket executor中完成。

测试可以通过现有 injectable `Clock` 和 package-private `runCleanupOnce()` 触发，不依赖真实等待。

### 9. generalized throttle error

新增：

```text
HistoricalPartitionThrottledException extends RetriableException
Errors.HISTORICAL_PARTITION_THROTTLED = 71
```

兼容要求：

- wire code继续是 `71`。
- default message改为 historical partition lookup/write共享语义。
- `Errors.forCode(71)` 构造新的 generalized exception。
- 为 source migration保留 deprecated `HistoricalLookupThrottledException`，可让它继承 generalized exception；server 新代码不再抛旧类型。
- `LookupSender` 改为识别 `HistoricalPartitionThrottledException`，已有 lookup backoff参数保持 `100ms / 2x / 5s / jitter`。
- write `Sender` 识别同一个 generalized exception，只对携带 original partition context 的 KV batch设置 historical throttle backoff。

本 PR 直接把 `Errors` enum constant 重命名为 `HISTORICAL_PARTITION_THROTTLED`，并更新所有 rpc/client tests。该 enum symbol是 Java source变更；wire compatibility由 numeric code `71` 保证。旧 exception class作为 deprecated subtype保留，不再由code 71反序列化生成；`Errors.forException(oldException)` 仍可沿 superclass映射到code 71。

### 10. write Sender backoff

`Sender` 复用 `ExponentialBackoff`：

```text
initial = 100 ms
multiplier = 2
max = 5 s
jitter = 与 LookupSender 相同
```

`WriteBatch` 增加 `nextRetryTimeMs`，默认 `0`。收到 historical throttle 时：

1. 使用当前 attempts计算 delay。
2. 设置 `nextRetryTimeMs = now + delay`。
3. 按现有 idempotence规则 re-enqueue batch。
4. `RecordAccumulator.ready()` 对 head batch计算剩余 backoff，并更新 `nextReadyCheckDelayMs`。
5. `RecordAccumulator.drain()` 再检查一次 deadline，防止 ready check 与 drain之间的时间/queue变化绕过 backoff。

backoff deadline 优先于 full、linger expired、buffer exhausted、flush in progress 等 sendable 条件。否则正在 flush 的 writer仍可能对 server形成 throttle busy loop。

非 throttle retriable error保持当前立即 re-enqueue语义；normal write不增加额外延迟。

### 11. replica 与 server lifecycle

以下 lifecycle event 都要通知 historical task/lifecycle manager：

| Event | 处理 |
|---|---|
| leader -> follower | 停止该 bucket 新 admission，取消 queued task，等待 running leader operation退出，invalidate handle |
| new leader epoch | 丢弃旧 generation state；第一次 write/lookup lazy recovery |
| stopReplica(delete=true/false) | cancel bucket queue，完成 pending future，invalidate handle |
| table/partition drop | cancel对应 bucket/table task，关闭 lookuper，删除 historical handle |
| server shutdown | close admission，cancel所有 queued task，停止 idle scheduler，再关闭 KV/remote log/log依赖 |

queued task不能只依赖提交时捕获的 `Replica` reference。执行前要确认：

- `ReplicaManager` 当前 hosted replica仍是同一个实例。
- replica仍是 leader和 historical partition。
- leader epoch/generation与 admission context一致。

write执行时 `Replica.putHistoricalRecordsToLeader()` 再做一次最终 leader/ISR检查。

## 详细实施步骤

### 步骤 1：接入 contextual PutKv decoder

修改 `TabletService.putKv()`：

- 使用 `toPutKvDataForBuckets(request)`。
- merge mode、target columns、api version和 response构造保持现有逻辑。
- 调用新的 request-level dispatch入口。
- online path不再调用 `getPutKvData()`；若无其他调用，删除该重复 helper及对应无效测试。

### 步骤 2：实现基于 RPC 字段的 request kind检测

在 `ReplicaManager` 中新增独立、可单测的request-kind helper：

- 只扫描每个holder的`originalPartitionName`，不解析replica metadata。
- 全null返回NORMAL，全non-null返回HISTORICAL，混合字段返回INVALID。
- mixed request在mutation前为所有bucket生成错误，不调用normal/historical processor。
- normal request最终仍进入现有`putRecordsToKv()`，避免改动normal hot path内部实现。
- historical request进入request-level historical async path。
- `historicalPutKv()`保留actual historical target校验。
- `Replica.putRecordsToLeader()`增加historical-target guard，在读取`kvTablet`前抛出明确的`InvalidPartitionException`。

### 步骤 3：实现 shared limiter 和 generalized error

- 新增 `HistoricalRequestLimiter` 和一次性 permit token。
- 从 `HistoricalLakeLookupManager` 删除私有 semaphore，改为注入 shared limiter。
- lookup reject直接返回 generalized throttle error。
- historical write在 heap copy前获取 permit。
- 修改 `ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS` 描述。
- 新增 generalized exception并将 `Errors` code 71映射到它。
- 更新 `ApiErrorTest`、lookup sender和相关测试。

### 步骤 4：增加 heap copy

为 `DefaultKvRecordBatch` 或 server-side utility增加明确的 heap-copy primitive：

```text
source MemorySegment[position, position + sizeInBytes)
    -> new byte[]
    -> MemorySegment.wrap(bytes)
    -> new DefaultKvRecordBatch at position 0
```

要求：

- 不改变 source buffer position。
- 完整复制 header、CRC和records。
- copy后 `sizeInBytes/schemaId/writerId/batchSequence/records` 与 source一致。
- 只在 historical permit成功后调用。

### 步骤 5：增加 per-bucket task executor

- 实现 FIFO queue、different-bucket parallelism、failure isolation、cancel bucket/table和 close。
- 使用现有 `ioExecutor`，不创建新线程。
- 给每个 queue维护 generation。
- completion和user callback移出内部 synchronized block。
- executor reject路径清理 running状态并完成 future。
- 用 deterministic test executor/latches验证顺序，不使用 sleep猜测。

### 步骤 6：改造 historical write request aggregation

- 将 `HistoricalPkWriteManager` 从直接 `supplyAsync` 改为通过 per-bucket executor提交。
- 新增 request-level `ReplicaManager.putHistoricalRecordsToKv()`。
- required acks和 local disk writable在 request入口校验。
- 每个 bucket task执行前 revalidate replica/epoch，执行 recovery gate，再调用 PR 3 processor。
- 收集所有 `PutKvResultForBucket` 后调用现有 `maybeAddDelayedWrite()`。
- error result也计入 request bucket size，保持 partial failure和 delayed write判定一致。
- 所有bucket在request-kind validation/limiter阶段失败时直接完成callback，不创建delayed operation。

### 步骤 7：实现 historical recovery primitive

新增 focused `HistoricalKvRecoverer`：

- 输入 actual leader `Replica`、empty handle、recovery start/HW/LEO snapshot。
- 复用 `RemoteLogFetcher` 获取 local start之前的 WAL。
- 复用现有 schema getter、log format reader、primary key/value encoder。
- 从每条 row提取 original partition name。
- committed段批量写 RocksDB；uncommitted段写 prewrite。
- DELETE写 tombstone，UPDATE_BEFORE跳过。
- recovery结束执行 catch-up flush。
- 全部成功后才 publish READY handle。
- 任意异常关闭 remote fetcher、drop incomplete handle并允许下次重试。

### 步骤 8：让 write 与 lookup共享 recovery gate

- historical write task在 processor前调用 `recoverIfNeeded()`。
- historical point lookup在执行 local-first lookup前调用 async `ensureRecovered()`，不得在 `ioExecutor` worker中阻塞等待另一个同 executor future。
- first lookup的 recovery task进入 keyed executor；future完成后再 compose实际 lookup。
- concurrent first accesses观察同一个 recovery future或同一个 queue generation，不重复建库/回放。
- `HistoricalLakeLookupManager.lookupValue()` 作为 historical write内部 synchronous lake fallback，不再次触发 recovery或获取 permit。

### 步骤 9：补 historical high-watermark flush

- 修改 `Replica.mayFlushKv()` 或增加等价 callback。
- READY historical handle调用 `flush(exclusiveHighWatermark)`。
- absent/recovering handle no-op。
- flush与 write/recovery使用 handle write lock。
- 验证 `acks=-1` follower推进、single replica立即推进和recovery期间推进三种情况。

### 步骤 10：实现 offset/idle cleanup

- `notifyLakeTableOffset()` 更新完整 batch的 offsets后再收集 cleanup candidates；response callback只调用一次，不能继续放在 bucket loop中重复执行。
- historical candidate提交到 per-bucket executor。
- task重新验证 replica identity、generation、offset和idle条件。
- `HistoricalKvManager` 增加 safe `tryInvalidate`/manager-scoped read API，固定 lifecycle lock -> handle lock顺序。
- manager-scoped read只在获取handle read lock前短暂持有lifecycle lock，实际lookup不持有全局manager lock。
- 新增 `KV_HISTORICAL_IDLE_TIMEOUT` 配置和周期扫描。
- cleanup skip不返回 client error；记录 debug日志和可观测计数即可，由后续 offset notification或周期扫描重试。

### 步骤 11：接入 client write backoff

- `LookupSender` 识别 generalized exception。
- `Sender` 增加 historical throttle `ExponentialBackoff`。
- `WriteBatch` 保存 retry deadline。
- `RecordAccumulator.ready()` 和 `drain()` 同时检查 deadline。
- 使用 injectable clock和零 jitter backoff构造路径编写 deterministic unit tests。
- normal retry测试确认行为未改变。

### 步骤 12：收口 lifecycle 和 shutdown

- `makeFollowers()`、`stopReplica()`、drop/delete path在资源删除前 cancel对应 queue。
- leader promotion使用新 generation，保留 lazy recovery。
- table lookuper invalidation与 historical state invalidation保持一致。
- `ReplicaManager.shutdown()` 先关闭 historical admission/task manager和 idle task。
- 调整 `TabletServer.stopServices()` 顺序，使 `ReplicaManager` 停止 historical task早于 `KvManager`、`RemoteLogManager` 和 `LogManager` shutdown。
- shared `ioExecutor` 仍最后统一 graceful shutdown。

## 预计文件范围

### fluss-common

可能修改：

- `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`
- `fluss-common/src/main/java/org/apache/fluss/exception/HistoricalPartitionThrottledException.java`
- `fluss-common/src/main/java/org/apache/fluss/exception/HistoricalLookupThrottledException.java`
- `fluss-common/src/main/java/org/apache/fluss/record/DefaultKvRecordBatch.java`

测试：

- `fluss-common/src/test/java/org/apache/fluss/record/DefaultKvRecordBatchTest.java`
- config option相关测试（如项目现有测试有固定 option清单）

### fluss-rpc

修改：

- `fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/Errors.java`

测试：

- `fluss-rpc/src/test/java/org/apache/fluss/rpc/protocol/ApiErrorTest.java`

### fluss-client

修改：

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupSender.java`
- `fluss-client/src/main/java/org/apache/fluss/client/write/Sender.java`
- `fluss-client/src/main/java/org/apache/fluss/client/write/WriteBatch.java`
- `fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java`

测试：

- `fluss-client/src/test/java/org/apache/fluss/client/lookup/LookupSenderTest.java`
- `fluss-client/src/test/java/org/apache/fluss/client/write/SenderTest.java`
- `fluss-client/src/test/java/org/apache/fluss/client/write/RecordAccumulatorTest.java`

### fluss-server

修改：

- `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`
- `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletServer.java`
- `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalLakeLookupManager.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPkWriteManager.java`
- `fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvManager.java`
- `fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvHandle.java`

建议新增：

- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalRequestLimiter.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPartitionTaskExecutor.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalKvLifecycleManager.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalKvRecoverer.java`

测试：

- `fluss-server/src/test/java/org/apache/fluss/server/tablet/TabletServiceTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaManagerTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalLakeLookupManagerTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalPkWriteManagerTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalPartitionTaskExecutorTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalKvRecovererTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/kv/historical/HistoricalKvManagerTest.java`
- focused restart/leader promotion ITCase；优先复用现有 `TabletServiceITCase` 或 historical write test fixture，避免另起重量级集群基类。

最终文件名和类拆分可以按实现复杂度微调，但不要把 dispatch、recovery、cleanup全部堆进 `ReplicaManager` 单个类中。

## 测试计划

### 1. dispatch 与 validation

- 不携带original name的request调用normal path。
- 携带original name的request调用historical path。
- normal target携带original name时先进入historical path，再由historical target校验返回bucket-level invalid partition。
- historical target缺少original name时先进入normal path，再由normal target guard返回bucket-level invalid partition。
- dispatch过程不查询replica metadata。
- mixed normal/historical request在任何 append前整请求失败。
- mixed request失败后验证 normal/historical log end offset都未变化。
- unknown/offline replica保留现有 bucket error。
- normal-only多 bucket response、target columns和merge mode保持现有行为。

### 2. heap ownership

- historical task排队后释放/复用原 RPC buffer，processor仍能正确读取 batch。
- copied batch的CRC、schema、writer id、batch sequence和record count与source一致。
- limiter已满时不调用heap copy。
- heap copy失败时释放permit并返回bucket error。

### 3. FIFO 与并行

- 同 bucket task 1阻塞时task 2不开始；task 1完成后task 2开始。
- task 1失败不阻止同 bucket task 2。
- 不同 bucket task可以同时开始。
- cancel bucket失败完成queued task，不影响其他bucket。
- executor reject不会留下running queue或未完成future。
- close后新submit立即失败，已有queued future都完成。

### 4. flow control

- lookup占满shared permits后historical write被throttle。
- write占满shared permits后historical lookup被throttle。
- normal write在permits耗尽时仍可执行。
- permit按historical bucket计数；一个多bucket RPC消耗多个permit。
- success、processor failure、recovery failure、copy failure、cancel和executor reject都恢复原permit数。
- error code仍为71，client decode为generalized retriable exception。

### 5. async aggregation 与 acks

- 多 historical buckets全部完成initial append后才调用response callback。
- 一个bucket失败、一个成功时response分别准确，成功bucket按`acks=-1`进入delayed write。
- 所有bucket admission失败时不创建delayed write。
- `acks=0/1/-1`与normal path相同。
- timeout时返回现有delayed write error，callback只执行一次。
- response中的log end offset对应每个bucket的append结果。

### 6. recovery offset边界

- `lakeEnd == localEnd`：创建empty READY handle，不replay。
- `lakeEnd < highWatermark == localEnd`：全部直接写RocksDB。
- `lakeEnd < highWatermark < localEnd`：前段RocksDB、后段prewrite。
- lake offset不存在：从available log start恢复。
- start早于local start：先remote WAL、再local WAL，顺序连续。
- remote WAL缺失/offset gap：recovery失败并删除handle。
- recovery期间HW推进：结束catch-up flush后对应记录已进入RocksDB。
- recovery失败后第二次访问重新执行并可成功。
- concurrent first write/lookup只创建一次handle、执行一次replay。

### 7. recovery record语义

- INSERT恢复为present value。
- UPDATE_BEFORE被跳过，UPDATE_AFTER成为最终value。
- DELETE old row恢复为tombstone，lookup不会fallback lake复活旧值。
- 同一PK在两个original partitions中恢复为两个composite keys。
- multi-level partition按partition key顺序构造original name。
- Arrow与Compacted log format都能读取。
- schema id变化时使用对应schema解码和编码value。
- malformed/null partition value导致recovery失败，不发布partial READY state。

### 8. historical prewrite flush

- historical append在HW之前只存在prewrite。
- HW推进到exclusive offset后mutation flush到RocksDB并从prewrite移除。
- no handle时HW推进不创建state。
- flush failure阻止HW静默推进或进入明确fatal/error path。
- recovery与HW并发时最终state不遗漏mutation。

### 9. cleanup

- `lakeEnd < localEnd`不提交/不执行cleanup。
- candidate提交后offset条件失效，task recheck并跳过。
- fully tiered后删除handle和目录。
- cleanup后first lookup/write可以lazy创建并恢复。
- cleanup排在同 bucket write之后，不能删除尚未append/tiered的新state。
- lookup持有read lock时cleanup `tryLock`失败并快速返回。
- cleanup skip后下一次offset notification可以重试成功。
- stale generation cleanup不能删除新leader创建的handle。

### 10. idle cleanup

- 未到idle timeout不删除。
- idle但未fully tiered不删除。
- idle且fully tiered删除。
- 每次lookup/write更新last access time。
- periodic scan关闭后不再提交task。
- manual clock测试不使用真实sleep。

### 11. client backoff

- lookup与write都识别generalized throttle exception。
- historical write retry delay按100ms、200ms、400ms增长并cap到5s。
- jitter在允许范围内；deterministic test使用0 jitter。
- deadline前`ready()`不返回该batch leader，`drain()`也不取出batch。
- `nextReadyCheckDelayMs`不大于最早retry deadline。
- deadline到达后batch可重试。
- normal retriable write不应用historical throttle backoff。
- idempotence enabled/disabled都保持现有re-enqueue和sequence规则。

### 12. lifecycle 与 focused ITCase

- queued write在leader demotion后以not-leader/cancel error完成。
- running write完成后demotion再删除handle，不发生use-after-close。
- leader promotion后第一次lookup/write从WAL恢复。
- `stopReplica(delete=false)`也取消pending historical task并丢弃leader-only state。
- table drop取消task、关闭lookuper和删除handle。
- server shutdown没有悬挂future、permit leak或executor rejection噪声。
- restart focused ITCase：写入historical WAL但不tier，重启leader后lookup仍返回该值。
- leader promotion focused ITCase：新leader从remote/local WAL恢复insert/update/delete和multiple original partitions。

## 兼容性

### Wire 兼容

- `PutKvRequest.original_partition_name` 是PR 1已有optional field，本PR不新增proto字段。
- throttle error numeric code保持71。
- `PutKvResponse`字段和bucket correlation方式不变。
- 新server继续接受normal request；normal request不携带original name时行为不变。

### Client/server版本组合

- old client -> new server：只发送normal write，继续走normal path。
- new PR 1 client -> new server：在PR 6启用fallback前仍主要发送normal write；手工historical request可被server处理。
- future PR 6 client -> old server：rollout时必须通过版本/feature gate避免发送old server不能处理的historical request；该gate不在本PR实现。
- server返回code 71时，更新后的lookup/write client都把它视为retriable historical throttle。

### State 兼容

- historical RocksDB是disposable state，不承诺跨版本复用；recovery开始会删除旧目录并从lake/WAL重建。
- normal KV snapshot和directory layout不变。
- composite key和tombstone格式继续使用PR 2定义，本PR不引入第二套编码。

## 本 PR 必须防住的风险

### 风险 1：RPC 字段完成 dispatch 后缺少 target兜底校验

后果：malformed request可能把normal replica送入historical path，或把historical replica送入normal path；如果具体路径不校验target，可能返回不清晰错误或访问不存在的ordinary `KvTablet`。

防护：historical path保留`replica.isHistoricalPartition()`校验；normal path在读取`kvTablet`前显式拒绝historical replica。RPC字段只决定route，不替代处理路径的安全校验。

### 风险 2：mixed request发生partial write

后果：normal path已同步append，historical path才失败，client无法判断整批状态。

防护：完整request-kind扫描早于任何mutation；第一版mixed-field request整请求拒绝。

### 风险 3：RPC buffer在async task执行前被释放

后果：CRC错误、row内容损坏或native memory use-after-free。

防护：permit成功后、enqueue前完整copy to heap；测试主动复用source buffer。

### 风险 4：同bucket乱序破坏writer idempotence和old-value merge

后果：后提交batch先执行，出现out-of-order sequence或基于错误old value合并。

防护：所有historical write/recovery/cleanup共用per-bucket FIFO executor；锁只负责互斥，不承担顺序保证。

### 风险 5：future/permit在异常路径泄漏

后果：client永久等待，shared limiter最终永久耗尽。

防护：一次性permit token；每个accepted bucket总有result future；copy/reject/cancel/exception测试检查permit恢复。

### 风险 6：recovery跳过remote WAL gap

后果：local miss错误fallback lake，漏掉尚未tier的新值或delete tombstone。

防护：offset范围连续校验；remote segment缺失即整次recovery失败并删除partial state。

### 风险 7：DELETE recovery没有tombstone

后果：lake中的旧row被lookup重新返回。

防护：DELETE使用old row重建original partition与composite key，写PR 2 tombstone格式。

### 风险 8：high watermark推进但historical prewrite不flush

后果：已提交数据长期占用heap，cleanup/recovery边界与local state不一致。

防护：`mayFlushKv()`增加READY historical handle分支；recovery末尾再做catch-up flush。

### 风险 9：cleanup删除正在lookup或新generation的handle

后果：lookup读closed RocksDB，或old task删除新leader state。

防护：固定manager/handle锁顺序、write `tryLock`、expected handle + generation检查、task执行时offset recheck。

### 风险 10：throttle retry形成busy loop

后果：client持续重发，加重server overload。

防护：generalized retriable error；write batch保存retry deadline；`ready()`和`drain()`双重检查指数退避。

### 风险 11：shutdown先关闭KV/log再取消task

后果：queued task访问已关闭manager，产生unknown errors或资源泄漏。

防护：先关闭historical admission/task manager，再关闭其依赖，`ioExecutor`最后shutdown。

### 风险 12：PR 4合并后被误认为client功能已经开放

后果：在PR 5 tiering尚未支持multi-original-partition前提前产生线上historical WAL。

防护：本PR不修改expired write resolver/fallback开关；release note和PR描述明确client routing仍关闭。

## 验证命令

先执行格式化和静态检查：

```bash
./mvnw spotless:apply
./mvnw spotless:check
./mvnw validate
```

运行受影响模块测试：

```bash
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server -am
```

开发期间可先运行focused tests：

```bash
./mvnw test -pl fluss-common -Dtest=DefaultKvRecordBatchTest
./mvnw test -pl fluss-rpc -Dtest=ApiErrorTest
./mvnw test -pl fluss-client -Dtest=LookupSenderTest,SenderTest,RecordAccumulatorTest
./mvnw test -pl fluss-server -Dtest=HistoricalPartitionTaskExecutorTest,HistoricalKvRecovererTest,HistoricalLakeLookupManagerTest,HistoricalPkWriteManagerTest
./mvnw test -pl fluss-server -Dtest=TabletServiceTest,ReplicaManagerTest
```

如新增restart/leader promotion ITCase，再单独运行对应测试类并记录命令和结果。

提交前检查完整diff：

```bash
git diff --check
git diff main...HEAD
```

重点确认：

- normal write hot path只有decoder/dispatch外层变化，内部processor和delayed write语义未改。
- 所有async historical batch都已heap copy。
- code 71未变化。
- request callback只完成一次，尤其是`notifyLakeTableOffset()`多bucket loop。
- lifecycle event取消task早于handle/KV/log资源关闭。
- 没有在`ioExecutor` worker中同步等待另一个提交到同一executor的future。

## 完成标准

- `TabletService.putKv()` 能按`original_partition_name`是否存在正确分流normal/historical write。
- normal/historical context mismatch和mixed request有明确、经过测试的错误行为。
- historical request在async执行前完成heap copy。
- 同bucket FIFO、different-bucket parallel、cancel和executor reject全部有deterministic tests。
- lookup/write共享limiter，所有completion path无permit leak。
- historical async结果正确接入现有delayed write和`requiredAcks`语义。
- first write/lookup能从lake end offset之后的remote/local WAL完整恢复state。
- recovery正确处理multi-partition composite key、update和delete tombstone。
- high watermark推进会flush historical prewrite。
- fully-tiered offset cleanup和idle cleanup不会与lookup/write发生unsafe race。
- demotion、stop、drop和shutdown不会留下pending future或stale state。
- write/lookup client都对generalized throttle error进行bounded exponential backoff。
- normal write、normal retry和ordinary KV recovery相关回归测试通过。
- full affected-module verify、Spotless、Checkstyle/RAT校验通过。

## 合并后的行为

PR 4合并后，server具备安全接收historical primary-key write RPC所需的dispatch、异步隔离、FIFO、flow control、recovery和cleanup能力。手工构造或后续client发送的合法historical request可以写入`__historical__` WAL，并在尚未tier到lake时通过local-first lookup读取；restart或leader promotion后可以从lake offset之后的WAL重建该state。

normal client行为仍不变。expired partition write不会自动redirect到historical partition。Paimon historical multi-partition tiering由PR 5完成，client fallback由PR 6开启。
