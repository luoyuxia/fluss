# FIP-28 Historical Write Pending Batch 原地重路由实施计划

## 1. 文档目的

本文档描述 historical partition write 在以下竞态场景中的补充设计：

```text
WriterClient 解析 write target 时 original partition 仍存在
    -> record 进入 original partition 的 RecordAccumulator queue
    -> partition 随后因 retention cleanup 被删除
    -> Sender 刷新 metadata 时收到 PartitionNotExistException
```

现有 PR 6 方案主要覆盖 record 进入 accumulator 之前即可确认 original partition 缺失的场景。本文档补充 accumulator 中已有 pending batches 时的处理，目标是在不 abort 并逐条重建 record 的情况下，将这些 batches 原地迁移到 historical system partition。

本文档是 `FIP-28-historical-partition-write-pr6-plan-zh.md` 的补充计划。若本文档与 PR 6 中“已经进入 accumulator 的 normal batch 不做原地 retarget”这一非目标冲突，以本文档为后续实现依据。

## 2. 背景

### 2.1 正常历史写路由

当前 writer 在 append record 前调用：

```text
WriterClient.doSend()
    -> DynamicPartitionCreator.resolveWriteTarget()
    -> 检查 original partition metadata
    -> original partition 缺失且满足 historical eligibility
    -> 解析或创建 historical system partition
    -> 修改 WriteRecord physical target
    -> RecordAccumulator.append()
```

该路径在 append 前已经固定最终 physical target，不存在 batch 迁移问题。

### 2.2 metadata 检查与发送之间存在时间窗口

original partition 可能在 target resolve 后、实际发送前被删除：

```text
T1: metadata cache 中 original partition 存在
T2: WriterClient 将 record append 到 original path
T3: retention cleanup 删除 original partition
T4: RecordAccumulator.ready() 发现 leader metadata 不完整
T5: Sender.updatePhysicalTableMetadata() 收到 PartitionNotExistException
```

此时 record 已经被编码进 `WriteBatch`，并保存在：

```text
RecordAccumulator.writeBatches
    original PhysicalTablePath
        -> bucket id
            -> Deque<WriteBatch>
```

### 2.3 当前 abort + callback fallback 的问题

当前工作区中的过渡实现采用以下流程：

```text
metadata update 发现 partition missing
    -> abort original path 下的 pending batches
    -> batch callback 收到 PartitionNotExistException
    -> WriterClient callback 解析 historical target
    -> 复制 WriteRecord 并重新 append
```

该方案存在以下问题。

#### 2.3.1 normal dynamic partition 可能被误 abort

normal dynamic partition create 是异步操作：

```text
start create normal partition
    -> record 立即进入 accumulator
    -> partition 尚未在 metadata 中可见
```

此时 metadata RPC 也可能暂时返回 `PartitionNotExistException`。如果 Sender 仅根据异常类型 abort batch，会把正常等待动态创建完成的 writes 提前失败。

#### 2.3.2 callback 重建产生顺序窗口

`abortBatches()` 会同步触发 record callbacks。历史分区解析期间，新的 `send()` 可以并发进入 writer。等待同一个 historical partition future 的线程在 future 完成后没有严格的先后调度保证，后到 record 可能先于旧 batch 的 fallback record 进入 historical queue。

#### 2.3.3 flush 需要额外补偿

旧 batch abort 后会从 accumulator 的 incomplete 集合中移除，而 replacement batch 还未 append。为了防止 `flush()` 在这段时间内提前返回，当前实现额外维护 `historicalFallbacksInProgress`。

原地重路由可以让 batch 始终保留在 accumulator 中，由现有 incomplete accounting 直接保证 flush 语义。

#### 2.3.4 record payload 无需重建

historical write 需要改变的是路由上下文：

- physical target 从 original partition 改为 historical system partition；
- PK write 增加 `originalPartitionName`；
- partition id 和 leader 改为 historical partition 对应值。

已经编码的 row、key、bucket key 和 mutation payload 不需要变化。因此可以复用现有 `WriteBatch` 内容，只修改尚未发送 batch 的路由字段。

## 3. Normal path 与 historical reroute 的关系

### 3.1 normal dynamic partition

normal path 的 logical path 和 physical path 相同：

```text
logicalPath = physicalPath = P
```

处理流程为：

```text
P 不存在
    -> 异步创建 P
    -> batch 保留在 P 的 deque
    -> metadata 中 P 可见
    -> 正常 ready/drain
```

创建前后不需要移动 accumulator queue。

### 3.2 historical partition

historical write 的 logical path 与 physical path 不同：

```text
logicalPath = original partition P
physicalPath = historical system partition H
```

处理流程为：

```text
P 已过期且不存在
    -> 解析或创建 H
    -> pending batch physical target 从 P 改为 H
    -> PK batch 保存 originalPartitionName=P.partitionName
    -> batch 迁移到 H 的 deque
    -> 正常 ready/drain
```

两条路径可以复用“分区就绪前不 drain”的模型。historical path 额外执行一次 pending batch 路由迁移。

## 4. 目标

本方案需要满足以下目标：

1. original partition 在 record 入队后被删除时，eligible historical writes 可以继续写入 historical system partition。
2. 已入队 batch 不通过 abort callback 拆成 records 后重建。
3. 同一 original path、同一 bucket 中，迁移前已有 batches 的 deque 顺序保持不变。
4. 迁移开始后，新 send 不得越过迁移前的 pending batches。
5. normal dynamic partition 正在创建时不得触发 historical reroute 或 batch abort。
6. `flush()`、close 和 incomplete batch accounting 继续使用 RecordAccumulator 的现有机制。
7. 普通 table、非 historical candidate partition 和已发送 batch 的语义保持明确。
8. 不新增 RPC 字段，不修改 server historical write contract。

## 5. 非目标

本方案不处理：

- 已经发送并可能被 server 接收的 batch 的透明迁移。
- 带有已分配 writer sequence 的 batch 跨 `TableBucket` 迁移。
- original partition 与 historical system partition 之间的 offset continuity。
- 跨 bucket 的全局写入顺序。
- 多线程 `send()` 调用之间未由现有 writer API 承诺的全局顺序。
- 修改 metadata RPC，使其直接返回全部缺失 partition paths。
- 通过解析异常 message 获取缺失 path。
- historical partition target bucket 数量与原表 bucket 数量不同的场景。historical system partition 属于同一张表，应使用相同 bucket topology。

## 6. 核心设计决定

### 6.1 不引入完整枚举状态机

使用以下结构作为 path 级迁移屏障：

```java
ConcurrentMap<PhysicalTablePath, CompletableFuture<ResolvedWriteTarget>>
        historicalReroutes;
```

状态由 map 和 future 表达：

```text
map 中不存在                         normal routing
future 未完成                         resolving/migrating
future 正常完成                       historical target ready
future 异常完成                       reroute failed
```

不新增 `NORMAL/RESOLVING/HISTORICAL/FAILED` enum。迁移逻辑仍必须把 future 注册、batch 迁移和 future completion 作为有序步骤执行。

### 6.2 Future 在迁移完成后才成功完成

future 的完成点必须位于 pending batches 迁移之后：

```text
register future
    -> resolve/create historical partition
    -> migrate existing pending batches
    -> complete future
    -> release new sends waiting on this path
```

如果在迁移前完成 future，等待中的新 record 可以先进入 historical deque，破坏同一 original path 的顺序。

### 6.3 只迁移未发送 batch

允许迁移的 batch 必须满足：

```java
batch.attempts() == 0
batch has not been drained
batch has no assigned idempotent sequence
batch final state is still incomplete
```

原因是已发送 batch 的前一次 attempt 可能已经被 server 接收。将其改写到另一个 `TableBucket` 会产生重复写入风险，同时原 bucket 的 writer sequence 不能直接复用到 historical bucket。

如果一个 original path 下存在 in-flight 或 attempted batch，本次不执行透明原地迁移。第一版应采用保守失败或等待已有 attempt 完成的策略，不能把后续未发送 batches 单独迁移到前面。

### 6.4 修改完整路由上下文

一次 historical reroute 至少更新：

```text
WriteBatch.physicalTablePath
KvWriteBatch.originalPartitionName
RecordAccumulator.writeBatches map key
BucketAndWriteBatches.partitionId
source/target bucket deques
```

只修改 `WriteBatch.physicalTablePath` 不够，因为 `RecordAccumulator.ready()` 通过 `writeBatches` 的 map key 查询 metadata，PutKv request 又从 `KvWriteBatch` 读取 original partition name。

### 6.5 不修改 encoded record bytes

KV record bytes、Arrow/Indexed log payload 和 callback 列表保持原对象。PK historical request 的 `originalPartitionName` 在 `ClientRpcMessageUtils.makePutKvRequest()` 构建 RPC 时从 batch 单独读取，不属于 encoded KV record bytes。

### 6.6 normal dynamic create 优先保留

如果 `DynamicPartitionCreator` 表明 original path 正在异步创建：

```text
keep batches queued
do not abort
do not historical-reroute
retry metadata after creation becomes visible
```

`PartitionNotExistException` 在该阶段表示 metadata visibility race，不表示 expired historical fallback。

## 7. 建议的数据结构调整

### 7.1 `ResolvedWriteTarget`

historical target 需要携带 resolver 已确认的 partition id：

```java
final class ResolvedWriteTarget {
    private final PhysicalTablePath physicalTablePath;
    private final boolean historical;
    private final @Nullable String originalPartitionName;
    private final @Nullable Long partitionId;
}
```

约束：

- normal target 可以不携带 partition id；
- historical target 必须携带 historical partition id；
- historical target 的 original partition name 必须非空。

### 7.2 `BucketAndWriteBatches`

保存创建 queue 时使用的 `TableInfo`，供 Sender 异常路径判断 historical eligibility：

```java
private static class BucketAndWriteBatches {
    final TableInfo tableInfo;
    final boolean isPartitionedTable;
    volatile @Nullable Long partitionId;
    final Map<Integer, Deque<WriteBatch>> batches;
}
```

同一 physical path 不应混入不同 table id 或不兼容 TableInfo。必要时在 `computeIfAbsent` 后增加 table id 一致性检查。

### 7.3 `WriteBatch`

将 physical route 从 immutable construction field 调整为只允许 pending 阶段修改：

```java
private volatile PhysicalTablePath physicalTablePath;

void reroute(PhysicalTablePath targetPath) {
    checkState(attempts() == 0, "Sent batches cannot be rerouted.");
    checkState(!hasBatchSequence(), "Sequenced batches cannot be rerouted.");
    this.physicalTablePath = targetPath;
}
```

需要增加显式的 pending/final-state 检查，禁止完成或 abort 后再次迁移。

### 7.4 `KvWriteBatch`

允许 pending KV batch 在迁移时设置 original partition name：

```java
private @Nullable String originalPartitionName;

void rerouteToHistorical(
        PhysicalTablePath targetPath, String originalPartitionName) {
    reroute(targetPath);
    checkNotNull(originalPartitionName);
    this.originalPartitionName = originalPartitionName;
}
```

normal KV batch 在迁移前必须满足 `originalPartitionName == null`。已经是 historical batch 时不应再次修改为另一个 original partition。

log batch 只修改 physical path。其 row payload 已包含 original partition columns，不设置 PK 专用 RPC 字段。

### 7.5 `RecordAccumulator` route barrier

第一版可以由 `RecordAccumulator` 持有 route future，因为它同时负责：

- append 与 reroute 之间的竞态关闭；
- pending batch queue 的迁移；
- flush/incomplete accounting。

建议增加：

```java
private final ConcurrentMap<
                PhysicalTablePath,
                CompletableFuture<ResolvedWriteTarget>>
        historicalReroutes = new ConcurrentHashMap<>();
```

后续如果 lookup/write 需要共享更广泛的 routing state，再抽取独立 router。第一版不先增加通用 routing framework。

## 8. 详细流程

### 8.1 append 前检查已存在 route barrier

`WriterClient.doSend()` 或 `DynamicPartitionCreator.resolveWriteTarget()` 首先检查：

```java
ResolvedWriteTarget reroutedTarget =
        accumulator.awaitHistoricalRerouteIfPresent(originalPath);
if (reroutedTarget != null) {
    return reroutedTarget;
}
```

如果 future 未完成，调用线程等待迁移完成。当前 historical target resolve 本身已经可能阻塞调用线程，因此第一版不新增额外 async-send accounting。

### 8.2 注册 barrier

当确认 original partition missing 且符合 historical eligibility 时：

```java
CompletableFuture<ResolvedWriteTarget> candidate = new CompletableFuture<>();
CompletableFuture<ResolvedWriteTarget> existing =
        historicalReroutes.putIfAbsent(originalPath, candidate);

if (existing != null) {
    return await(existing);
}
```

只有成功注册 candidate 的线程执行 historical partition resolve/create 和 pending batch migration。其他线程等待同一个 future。

### 8.3 解析 historical target

owner 调用现有 `HistoricalPartitionResolver`：

```java
long historicalPartitionId =
        historicalPartitionResolver
                .resolveHistoricalPartitionId(tableInfo, originalPartitionName)
                .get();

ResolvedWriteTarget target =
        ResolvedWriteTarget.historical(
                historicalPath,
                originalPartitionName,
                historicalPartitionId);
```

historical partition path 继续使用 `PartitionUtils.toHistoricalPartitionSpec()` 计算，不进行字符串拼接。

### 8.4 迁移 pending batches

概念接口：

```java
void reroutePendingBatches(
        PhysicalTablePath originalPath,
        ResolvedWriteTarget historicalTarget);
```

伪代码：

```java
BucketAndWriteBatches source = writeBatches.get(originalPath);
if (source == null) {
    return;
}

verifyAllBatchesArePending(source);

BucketAndWriteBatches target =
        writeBatches.computeIfAbsent(
                historicalTarget.physicalTablePath(),
                ignored ->
                        new BucketAndWriteBatches(
                                source.tableInfo,
                                historicalTarget.partitionId(),
                                true));

for (Integer bucketId : source.batches.keySet()) {
    Deque<WriteBatch> sourceDeque = source.batches.get(bucketId);
    Deque<WriteBatch> targetDeque =
            target.batches.computeIfAbsent(bucketId, ignored -> new ArrayDeque<>());

    lockInStableOrder(sourceDeque, targetDeque);
    try {
        while (!sourceDeque.isEmpty()) {
            WriteBatch batch = sourceDeque.pollFirst();
            if (batch.isLogBatch()) {
                batch.reroute(historicalTarget.physicalTablePath());
            } else {
                ((KvWriteBatch) batch)
                        .rerouteToHistorical(
                                historicalTarget.physicalTablePath(),
                                historicalTarget.originalPartitionName());
            }
            targetDeque.addLast(batch);
        }
    } finally {
        unlockInReverseOrder();
    }
}

target.partitionId = historicalTarget.partitionId();
writeBatches.remove(originalPath, source);
```

实现时不能直接使用嵌套 `synchronized` 且依赖不稳定的调用顺序。应按 bucket id 和对象 identity 建立稳定锁顺序，或者使用 path 级显式锁，避免两个并发迁移目标互换时死锁。

### 8.5 完成 barrier

只有迁移成功后才完成 future：

```java
reroutePendingBatches(originalPath, target);
candidate.complete(target);
```

失败时：

```java
candidate.completeExceptionally(error);
historicalReroutes.remove(originalPath, candidate);
accumulator.abortBatches(originalPath, toException(error));
```

失败 entry 必须移除，允许下一次独立 send 重试 historical resolve。

### 8.6 Sender 下一轮正常发送

当前 `ReadyCheckResult` 是 reroute 前生成的，不应尝试在同一轮直接 drain 新 target。迁移完成后：

```text
本轮继续处理其他 ready nodes
下一轮 accumulator.ready(latestCluster)
    -> 遍历 historical path
    -> 使用 historical partition id/leader
    -> 正常 drain/send
```

## 9. 关闭 append 与迁移竞态

只在 `WriterClient` 开头检查 future 不足以避免竞态：

```text
append thread: 检查 map 不存在
reroute thread: 注册 future
append thread: 向 original deque append
reroute thread: 已经完成 deque 快照
```

`RecordAccumulator.append()` 需要至少在两个位置重新检查 route barrier。

### 9.1 append existing batch 前

```java
synchronized (deque) {
    if (historicalReroutes.containsKey(originalPath)) {
        // Release deque lock, await target, then retry append with the target path.
        continue appendLoop;
    }
    RecordAppendResult result = tryAppend(...);
}
```

不能持有 deque lock 等待 future，否则 migration 需要同一把锁时会死锁。

### 9.2 分配内存后创建新 batch 前

memory allocation 可能阻塞，因此不能在 allocation 期间持有 route lock。分配完成、重新获得 deque lock 后再次检查：

```java
synchronized (deque) {
    if (historicalReroutes.containsKey(originalPath)) {
        // Return unused memory in the existing finally block and retry on target.
        continue appendLoop;
    }
    return appendNewBatch(...);
}
```

迁移线程必须先注册 future，再开始遍历 deques。这样：

- 注册前已经持有 deque lock 的 append 会先完成，随后被 migration 捕获；
- 注册后才获得 deque lock 的 append 会观察到 barrier，不再写入 source deque；
- allocation 中的 append 会在最终 enqueue 前观察到 barrier，并释放未使用 memory。

## 10. Sender 异常处理

### 10.1 批量 metadata 异常仍需定位 path

当前 metadata RPC 在一个 partition 缺失时整体异常完成，`PartitionNotExistException` 没有结构化携带对应 `PhysicalTablePath`。因此多 path 请求仍需降级确认。

第一版处理：

```text
unknownLeaderTables size == 1
    -> 直接确认该 path 是异常候选

unknownLeaderTables size > 1
    -> 对 unresolved partition paths 做 singleton metadata check
    -> 只处理确认 missing 的 path
```

这是 error path 的过渡处理。长期方案是 metadata RPC 返回 per-path error 或缺失 path 列表。

### 10.2 missing path 分类

确认某个 path missing 后按以下顺序处理：

```java
if (dynamicPartitionCreator.isPartitionCreationInProgress(path)) {
    // Normal dynamic partition is not visible yet. Keep batches queued.
    return;
}

TableInfo tableInfo = accumulator.getTableInfo(path);
if (tableInfo != null
        && isHistoricalPartitionCandidate(
                tableInfo, path.getPartitionName(), Instant.now())) {
    dynamicPartitionCreator.resolveAndRerouteHistoricalTarget(path, tableInfo);
    return;
}

accumulator.abortBatches(path, partitionNotExistException);
```

`PhysicalTablePath` 只有 partition name，不包含 auto partition strategy、lake format 或 retention 配置，不能单独判断 historical eligibility。

### 10.3 建议重命名 helper

当前 helper 名称：

```text
recheckAndAbortMissingPartitionBatches
```

完成本方案后建议改为：

```text
recheckAndHandleMissingPartitions
```

因为 missing path 可能进入三种结果：

- normal partition creation in progress：保留 queue；
- historical candidate：原地 reroute；
- 其他 missing partition：abort。

## 11. DynamicPartitionCreator 调整

### 11.1 暴露 normal partition creation 状态

增加 package-private 查询：

```java
boolean isPartitionCreationInProgress(PhysicalTablePath path) {
    return inflightPartitionsToCreate.contains(path);
}
```

该状态只用于避免 Sender 把 normal create visibility race 误判为 terminal missing partition。

### 11.2 historical resolve 统一经过 barrier

`resolveWriteTarget()` 在 historical candidate 分支中不能绕过 accumulator barrier：

```java
return accumulator.resolveAndRerouteHistoricalTarget(
        originalPath,
        () -> resolveHistoricalTargetInternal(originalPath, tableInfo));
```

这样无论触发者是正常 `send()` 线程还是 Sender metadata error path，都共享同一个 path future，并在 target future 完成前迁移旧 batches。

### 11.3 normal create 行为保持不变

以下行为不修改：

- current/future missing partition 继续走 normal validation；
- `dynamicPartitionEnabled=false` 时 normal missing partition 继续失败；
- normal create 继续使用 `Admin.createPartition(..., ignoreIfExists=true)`；
- create failure 继续通过 fatal error handler 处理 pending writes。

## 12. WriterClient 调整

完成原地 reroute 后，删除 callback fallback 机制：

```text
maybeWrapHistoricalFallback()
historicalFallbacksInProgress
WriteRecord.copy() 仅为 fallback 服务的代码
flush() 中 historicalFallbacksInProgress 补偿循环
```

`flush()` 恢复为等待 accumulator incomplete batches。迁移期间 batch 没有被 complete 或 abort，因此 flush 不会提前返回。

WriterClient constructor 需要在 Sender 启动前完成以下对象初始化：

```text
RecordAccumulator
HistoricalPartitionResolver / DynamicPartitionCreator
missing partition handler
Sender
```

避免 Sender thread 在 historical routing components 尚未初始化时进入异常处理。

## 13. 顺序保证

### 13.1 保证范围

本方案保证：

- 同一 original physical path；
- 同一 bucket；
- 尚未 drain 的 pending batches；
- 迁移开始前已经进入 source deque 的顺序；
- 迁移开始后通过同一 barrier 等待的新 writes 不会先于 source batches 入队。

### 13.2 不保证范围

本方案不声明：

- 不同 buckets 之间的全局顺序；
- 不同 original partitions 共享 historical bucket 时的业务顺序；
- 多线程调用 `send()` 但调用顺序本身没有 happens-before 关系时的全局顺序；
- 已经发送 batch 与 pending batch 之间的透明跨 partition 迁移顺序。

### 13.3 target deque 已有 batches

同一个 historical physical bucket 可能已经包含其他 original partitions 的 pending batches。迁移时：

- source deque 内部顺序必须保持；
- 同一 original path 在 barrier 完成前不得产生新的 target batches；
- source batches 可以追加到 target deque 尾部；
- 不同 original partitions 之间不提供业务顺序；
- 如果 target deque 中存在带 sequence 的 re-enqueued batch，仍按现有 idempotence ordering 规则处理，不能把未分配 sequence 的 source batch插到已发送 sequence 之前。

如果实现无法证明 target deque merge 满足 idempotence manager 约束，应在第一版检测后拒绝迁移，而不是猜测插入位置。

## 14. Idempotence 与 retry

### 14.1 尚未发送 batch

metadata 缺失发生在 `ready()` 后、`drain()` 前时，正常情况下 batch 尚未分配 sequence，可以安全修改 physical target。

### 14.2 attempted 或 sequenced batch

以下 batch 不允许原地迁移：

```java
batch.attempts() > 0
batch.hasBatchSequence()
```

处理选择：

1. 等待 in-flight attempt 完成；若成功则无需迁移该 batch。
2. 若 attempt 明确以 `PartitionNotExistException` 失败，则按现有失败语义结束，调用方下一次独立 send 重新解析 historical target。
3. 不把后续 pending batch 绕过该 batch 单独迁移。

第一版优先实现明确失败，不增加跨 target sequence translation。

### 14.3 throttle 和 leader retry

historical target 完成迁移并进入正常 send path 后，继续复用：

- historical throttling backoff；
- leader/not-leader metadata invalidation；
- retry count；
- idempotence manager；
- request grouping by normal/historical/original partition name。

## 15. 错误处理

### 15.1 historical resolve/create 失败

执行顺序：

```text
complete route future exceptionally
abort original pending batches with the resolved cause
remove failed route entry
```

异常完成的 future 在 batch abort 期间继续保留在 map 中。等待线程和 reentrant callback send 会立即观察到同一个失败，不会阻塞 Sender，也不会在 source batches 尚未结束时启动下一次 resolve。全部 source batches abort 后再移除 entry，允许后续独立 send 重试。

### 15.2 migration validation 失败

如果发现 attempted batch、sequence、in-flight state 或不兼容 target topology：

- 不做部分迁移；
- source map 和 deques 保持原状；
- route future 异常完成；
- 所有 source batches 使用同一个明确异常结束；
- 日志包含 original path、historical path 和拒绝原因。

### 15.3 非 PartitionNotExistException

metadata update 的 timeout、network、authorization 和其他异常继续按现有逻辑传播或 retry，不能触发 historical reroute。

## 16. 具体文件改动计划

### 16.1 `ResolvedWriteTarget.java`

- historical target 增加 partition id。
- 增加构造参数校验。
- 保持 package-private immutable value object。

### 16.2 `WriteBatch.java`

- physical path 从 final 改为 pending 阶段可修改。
- 增加 package-private `reroute()`。
- 增加 pending、attempt 和 sequence 校验。

### 16.3 `KvWriteBatch.java`

- original partition name允许在 pending reroute 时从 null 设置为非空。
- 增加 `rerouteToHistorical()`。
- 禁止二次改写为不同 original partition。

### 16.4 `RecordAccumulator.java`

- `BucketAndWriteBatches` 保存 TableInfo。
- 增加 historical route future map。
- 增加 route registration/await/failure cleanup。
- 增加 `getTableInfo()`。
- 增加 `reroutePendingBatches()`。
- `append()` 在 existing batch append 前和 memory allocation 后重新检查 route barrier。
- `ready()` 在 source path 正在迁移时跳过该 path。
- 增加 attempted/sequenced batch validation。

### 16.5 `DynamicPartitionCreator.java`

- 增加 `isPartitionCreationInProgress()`。
- historical target resolve 经过 RecordAccumulator barrier。
- historical resolver 返回 partition id 并写入 `ResolvedWriteTarget`。
- normal async create 保持原逻辑。

### 16.6 `Sender.java`

- `recheckAndAbortMissingPartitionBatches()` 改为 missing partition 分类处理。
- 单 path 时避免重复 metadata RPC。
- 多 path 时只在 error path 做精确确认。
- normal create in progress 时保留 batch。
- historical candidate 调用 reroute。
- 其他 missing path 才 abort。
- reroute 后不在当前 stale `ReadyCheckResult` 中 drain source/target path。

### 16.7 `WriterClient.java`

- 在 Sender 启动前初始化 reroute dependencies。
- 删除 callback copy/rebuild fallback。
- 删除额外 flush counter。
- 保留 append 前直接 historical resolve 的 common path。

### 16.8 `WriteRecord.java`

- 如果 `copy()` 只服务于 callback fallback，则删除该方法。
- 保留 `withOriginalPartitionContext()` 供 append 前补充 original partition context。

## 17. 测试计划

### 17.1 `RecordAccumulatorTest`

新增测试：

1. 同一 bucket 多个 pending batches 从 original path 迁移到 historical path，deque 顺序保持。
2. 多 bucket migration 各自保持 source deque 顺序。
3. KV batch迁移后设置正确的 original partition name。
4. log batch迁移后只修改 physical path。
5. encoded record count、callbacks、memory segments 和 incomplete count 迁移前后不变。
6. target path 已有其他 original partition batches 时不发生 batch 合并污染。
7. attempted batch 拒绝迁移且 source queues 不发生部分修改。
8. sequenced batch 拒绝迁移。
9. route barrier 注册后，append 不再进入 source deque。
10. append 在 memory allocation 期间发生 reroute，未使用 memory 被正常归还。

### 17.2 `DynamicPartitionCreatorTest`

新增或调整测试：

1. normal partition create in progress 返回 normal target，并可查询 creation state。
2. historical target resolve 返回 partition id。
3. concurrent historical resolves 共享同一个 route future。
4. route failure 后 entry 被清理，下一次调用可以重试。
5. dynamic partition disabled 不影响 eligible historical system partition create。

### 17.3 `SenderTest`

新增测试：

1. 单一 unknown path 的 PartitionNotExistException 不重复发送 singleton metadata RPC。
2. normal dynamic create in progress 时不 abort batches。
3. historical candidate missing 时调用 reroute，不触发 callbacks failure。
4. non-historical missing path 继续 abort。
5. batched metadata 中一个 missing path、一个 valid path时只处理 missing path。
6. reroute 完成后本轮不使用 stale ReadyCheckResult drain target，下一轮正常发送。

### 17.4 `WriterClientTest` 或 focused concurrency test

新增测试：

1. `send(record); flush();` 在 reroute 完成和 historical batch ack 前不会返回。
2. old pending batch migration期间的新 send 等待 barrier。
3. barrier 完成后旧 batch 先于等待中的新 record进入 historical deque。
4. historical resolve失败时旧 batch和等待中的新 send收到一致错误。
5. normal path send不读取或创建 historical route state。

### 17.5 端到端测试

在 `FlussLakeTableITCase` 或现有 historical write ITCase 中覆盖：

1. record append 后删除 original partition，再触发 Sender metadata refresh，record 最终写入 historical partition。
2. 同一 bucket 连续多条记录在 migration 后保持可观察顺序。
3. normal partition 异步创建与 Sender metadata refresh 竞态不会导致 batch 失败。
4. PK historical request携带 original partition name，Paimon tiering后仍写回 original partition。
5. log historical write只切换 physical target，row partition columns保持原值。

## 18. 验证命令

实现完成后至少运行：

```bash
./mvnw spotless:apply

./mvnw test \
  -pl fluss-client \
  -Dtest=RecordAccumulatorTest,DynamicPartitionCreatorTest,SenderTest

./mvnw test \
  -pl fluss-common \
  -Dtest=PartitionUtilsTest

./mvnw verify -pl fluss-client
```

涉及 lake end-to-end wiring 后再运行对应 Paimon/ITCase module。最终执行：

```bash
git diff --check
./mvnw spotless:check
```

## 19. 实施顺序

### Step 1: 建立 batch reroute primitive

- 扩展 `ResolvedWriteTarget`。
- 给 `WriteBatch`/`KvWriteBatch` 增加 pending reroute 方法。
- 为 `BucketAndWriteBatches` 保存 TableInfo。
- 增加 accumulator migration 单测。

验收标准：不连接 Sender，仅通过单测可以把 pending source batches 完整迁移到 historical target。

### Step 2: 增加 route Future barrier

- 增加 path -> future map。
- 完成 route owner/coalescing/error cleanup。
- 在 `append()` 两个关键位置关闭并发竞态。
- 增加 concurrency tests。

验收标准：迁移开始后的 append 不会进入 source deque，future 只在旧 batches 迁移完成后放行。

### Step 3: 接入 DynamicPartitionCreator

- direct historical resolve 经过同一 barrier。
- 暴露 normal create in-progress 查询。
- resolver result保留partition id。

验收标准：append 前 missing 和 append 后 missing 两种场景共享同一 target resolve/migration机制。

### Step 4: 接入 Sender missing partition path

- 处理 batched metadata异常定位。
- normal creating/historical/non-historical 三类分流。
- 删除 abort callback fallback。

验收标准：normal create 不误失败，historical pending batches 不经过 callback 重建。

### Step 5: flush、idempotence 与端到端验证

- 删除额外 fallback counter。
- 验证 attempted/sequenced batch保护。
- 执行 focused tests、client verify 和 lake ITCase。

## 20. 风险与缓解

### 风险 1: append 与 migration 竞态导致 batch 遗留在 source map

缓解：先注册 barrier；append 在 existing batch 和 new batch enqueue 前双重检查；等待 future 时不持有 deque lock。

### 风险 2: source/target deque 嵌套锁死锁

缓解：使用稳定锁顺序或 path 级显式 migration lock；不得依赖 HashMap iteration order。

### 风险 3: idempotence sequence 跨 TableBucket 复用

缓解：第一版只迁移 `attempts()==0` 且无 sequence 的 batch；其他情况明确拒绝。

### 风险 4: normal create visibility race 被误判为 historical missing

缓解：优先查询 `inflightPartitionsToCreate`；创建中保持 queue，不 abort、不 reroute。

### 风险 5: route future完成过早导致新 write越过旧 batches

缓解：先完成 source migration，再 complete future。

### 风险 6: completed route 长期缓存后 original partition 被重新创建

缓解：第一版明确 route entry 生命周期。若 completed entry 只作为迁移 barrier，应在安全放行等待线程后移除，并确保 original metadata 已 invalid；若选择缓存 redirect，则必须在 metadata 发现 original partition重新出现时失效。实现前不采用无限期无失效缓存。

### 风险 7: target deque 已有 sequenced batch

缓解：迁移前检查 target queue 和 idempotence state。无法证明插入位置安全时拒绝透明迁移，并通过测试固定行为。

## 21. 方案验收标准

实现完成需要同时满足：

1. normal dynamic partition create竞态测试通过，没有 batch 被误 abort。
2. original partition 入队后删除时，pending batches 原地迁移到 historical target。
3. 迁移过程中 batch callbacks不因 PartitionNotExistException被提前触发。
4. 同一 source deque 的 batch 顺序保持。
5. 新 send 在旧 batches迁移完成后才进入 historical queue。
6. `flush()` 不依赖额外 historical fallback counter。
7. attempted/sequenced batch不会被跨 target改写。
8. normal write hot path不增加 metadata RPC。
9. formatted diff、client focused tests和相关 lake ITCase通过。

## 22. 后续优化

本文档范围完成后可以独立考虑：

- metadata response 返回 per-path error，去掉 singleton recheck RPC。
- 将 route barrier 从 RecordAccumulator 抽取为通用 `WriteRoutingManager`。
- 对 attempted batch 设计跨 target 的显式 replay protocol。
- 为 route resolve latency、reroute batch count 和 rejected migration 增加 metrics。
- 对 completed historical redirects 增加有界 cache 和 metadata-driven invalidation。
