# FIP-28 Historical Write PR 6 实施计划

## PR 标题

```text
[client] Enable end-to-end historical partition writes
```

## 目标

本 PR 是 historical partition write 系列中最后一个 PR。它不再新增 server-side historical storage 或 tiering primitive，而是在 client 侧正式打开 expired partition write fallback，并用端到端测试串起 PR 1 到 PR 5 已完成的能力。

本 PR 完成后：

- append-only/log table 向已过期且已从 Fluss metadata 删除的 Paimon partition 写入时，会把 physical target 重定向到对应的 `__historical__` system partition。
- primary-key table 的 late upsert/delete 会重定向到 historical PK write pipeline，并携带原始 partition name。
- 写入的 row payload 不发生变化，partition columns 继续保存 original partition values。
- 单 partition key 表使用 `__historical__`；多 partition key 表按 static partition prefix 使用独立的 historical partition，例如 `region=us$__historical__`。
- 第一次 historical write 会解析或懒创建 historical system partition；该创建不受 client `dynamicPartitionEnabled` 控制。
- historical target 在 bucket assignment 和 accumulator append 前确定，避免 record 先进入错误的 normal partition queue。
- log table 继续使用普通 PutLog RPC；PK table 通过 PR 1 的 `original_partition_name` 进入 historical PutKv RPC。
- historical log/PK data 经 PR 5 tiering 后写回 original Paimon partition，而不是在 Paimon 中创建 `__historical__` partition。
- normal partition write、normal dynamic partition create、bucket assignment、batching、retry 和异常语义保持不变。

## 与总计划的关系

本 PR 对应 `FIP-28-historical-partition-write-pr-plan-zh.md` 中的：

```text
PR 6: Enable End-to-End Historical Write
```

前置依赖：

- historical lookup 已提供 expired partition eligibility、historical partition create、Paimon lake lookup 和 local-first lookup 能力。
- PR 1 已让 historical PK `WriteRecord`、`KvWriteBatch` 和 PutKv bucket RPC 保存 `original_partition_name`，并隔离 normal/historical request batching。
- PR 2 已提供 historical composite key、tombstone、RocksDB handle 和 local tri-state lookup。
- PR 3 已提供 historical PK write processor、lake old-value fallback 和 changelog merge 语义。
- PR 4 已把 historical PutKv 接入 online dispatch，并提供 flow control、per-bucket FIFO、recovery、flush、cleanup 和 client throttle backoff。
- PR 5 已让 Paimon tiering 根据每条 WAL row 的 original partition columns 选择实际 Paimon partition。

本 PR 是唯一主动改变正常 writer 对 expired partition 行为的 PR。PR 1 到 PR 5 合并后，server 和 tiering 已能处理手工构造的 historical write，但正常 client 仍不会主动产生这类请求；本 PR 合并后才形成用户可见的端到端能力。

本系列到 PR 6 结束，不再假设另有 PR 7 补充 rollout 或测试。若后续需要 metrics、feature negotiation 或 historical Arrow按partition分组的direct-bundle优化，应作为独立follow-up讨论。

## 前置假设

- writer 收到的初始 `WriteRecord.getPhysicalTablePath()` 是根据 row partition columns 得到的 original physical target。
- row 中的 partition columns 是业务数据的一部分。重定向只能修改 `WriteRecord` 的 physical target，不能修改 row。
- original partition 只要仍存在于当前 metadata，就继续走 normal path；client 不根据时间主动把一个仍存在的 partition 改判为 historical。
- cache miss 后必须强制刷新一次 original partition metadata。只有刷新后仍确认 missing，才能评估 historical fallback。
- historical eligibility 继续使用 FIP-28 的统一规则：auto-partitioned、Paimon lake-enabled、partition name 合法，并且 auto partition value 早于 retention boundary。
- eligibility 只证明 partition name 符合 expired 规则，不证明该 original partition 过去一定存在过。
- historical system partition 使用 `Admin.createPartition(..., ignoreIfExists = true)` 幂等创建，因此并发 create 可以安全收敛。
- `HistoricalPartitionResolver` 的 in-flight future 只用于合并并发 resolve/create；成功结果最终由 `MetadataUpdater` cache 保存，失败 future 必须从 map 移除。
- `dynamicPartitionEnabled` 只控制 normal user partition 的动态创建，不控制 historical system partition 的创建。
- `RecordAccumulator` 允许 partition id 暂时为空，但 PR 6 仍要求 historical resolve/create 在 append 前完成。这样首次 historical resolve 的失败可以只失败当前 send，而不会留下永远等待 metadata 的 batch。
- PK historical write 的 `originalPartitionName` 来自重定向前的 original physical path，而不是从 key-only delete 的 nullable row 中重新提取。
- log historical write 不需要新增 RPC partition field；row payload 本身已经保存 original partition values。
- normal/historical PK batches 已由 PR 1 隔离；PR 6 不修改 PutKv response correlation、idempotence key 或 Sender request grouping。
- PR 5 已保证 historical Paimon tiering 不把 `__historical__` 当作 Paimon partition name。
- 当前 server 对 historical system partition create 要求 `READ` 权限，而真正的 PutLog/PutKv 要求 `WRITE` 权限。因此首次需要懒创建 historical partition 的 writer principal 需要同时具备 `READ + WRITE`；本 PR 不静默放宽该 server authorization contract。

## 非目标

本 PR 不实现：

- historical server dispatch、heap copy、limiter、recovery、cleanup 或 RocksDB lifecycle；这些属于 PR 2 到 PR 4。
- Paimon multi-partition writer或historical Arrow逐行write；这些属于PR 5。
- Iceberg、Lance、Hudi 或未启用 lake 的表的 historical write。
- 非 auto-partitioned table 的 missing partition fallback。
- malformed、current、future 或仍在 retention window 内的 partition 的 historical fallback。
- 证明 original partition 曾经存在，或维护 original partition existence registry。
- 把 `__historical__` 隐藏在 `listPartitions` 或普通 partition discovery 之外。
- 把 historical log offset 与 expired original partition offset 拼成连续 changelog。
- historical prefix lookup、scan union、`insertIfNotExists` 或跨 partition transaction。
- 修改 row partition columns、primary key bytes 或 bucket key bytes。
- 把 original partition name 加入 bucket hash；bucket assignment 继续只使用现有 bucket key。
- historical Arrow按partition group/slice的direct-bundle优化；第一版沿用PR 5的batch fetch + Paimon逐行write。
- 对已经进入 accumulator 的 normal batch做原地 retarget。若 original partition 在 batch 入队后才被并发删除，当前 batch 可以按现有语义失败，调用方下一次 send 再重新解析 target。
- 新增 client/server feature negotiation 或单独 feature flag。
- 修改 historical system partition 的 authorization policy。当前 `READ` create + `WRITE` data write 的权限组合会被明确记录和验证；若要支持仅有 `WRITE` 权限的 principal 首次创建 historical partition，需要单独调整 server authorization 设计。
- 为 PR 1 到 PR 5 已覆盖的 RPC serialization、server processor 和 RocksDB primitive 重复增加单元测试。

## 当前实现约束

### 1. `WriterClient.doSend()` 在 target check 后仍固定使用 original path

当前流程为：

```text
WriteRecord(original physical path)
    -> DynamicPartitionCreator.checkAndCreatePartitionAsync(original path)
    -> bucketAssignerMap.computeIfAbsent(original path)
    -> assignBucket()
    -> RecordAccumulator.append(original record)
```

`DynamicPartitionCreator` 只做检查和异步 create，不返回 write target。因此即使它识别出 expired partition，也没有办法让后续 bucket assignment 和 accumulator 改用 `__historical__`。

PR 6 必须让 partition check 返回结构化的 resolved target，并让 `WriterClient` 后续所有操作都使用同一个 resolved result。

### 2. `DynamicPartitionCreator` 过早应用 `dynamicPartitionEnabled`

当前 `forceCheckPartitionExist()` 捕获 `PartitionNotExistException` 后，在 `dynamicPartitionEnabled=false` 时立即抛错：

```text
original partition missing
    -> dynamic partition disabled
    -> throw PartitionNotExistException
```

这会在 historical eligibility 之前拒绝请求。但 historical partition 是 system partition，即使 normal dynamic create 被关闭，也必须允许 eligible expired write 创建或复用 `__historical__`。

PR 6 需要把以下两个问题拆开：

```text
Does the original partition exist?
Should a missing normal partition be dynamically created?
```

只有 historical fallback 不成立时，第二个问题才参与 normal path 决策。

### 3. 当前 auto-partition validation 会直接拒绝 expired value

normal dynamic create path 使用 `validateAutoPartitionTime()`。它会把早于 retention boundary 的 partition 判为 out-of-date 并抛出 `InvalidPartitionException`。

这个校验对 normal create 是正确的，但 historical write 必须先识别 eligible expired partition，再决定是否执行 normal validation。顺序反过来会让所有 late write 在 redirect 前被拒绝。

同时，PR 6 不能简单删除该校验。malformed、non-lake 或不符合 historical 条件的 missing partition 仍需进入原 normal path，并保持原异常类型和消息。

### 4. `HistoricalPartitionResolver` 目前被锁在 lookup package

当前 resolver 位于：

```text
org.apache.fluss.client.lookup.HistoricalPartitionResolver
```

它已经实现：

- historical spec/path 计算；
- metadata cache lookup；
- forced metadata refresh；
- idempotent system partition create；
- create 后再次 refresh；
- concurrent future coalescing；
- failed future removal。

write path 若复制这套逻辑，会产生两份 historical create 和错误处理实现。PR 6 应把 resolver 移到 lookup/write 可共享的 client-internal package，并只调整 lookup import/constructor，不改变现有 lookup 行为。

### 5. target resolve 不能在 append 之后异步完成

一个看似简单的实现是：

```text
send(record)
    -> start async historical resolve
    -> return
    -> resolve complete 后再 append
```

但 `flush()` 只等待已经进入 accumulator 的 record。若用户紧接着调用：

```text
send(record);
flush();
```

`flush()` 可能在 async resolve 完成前返回，违反现有 writer contract。close/abort 也会遇到同类 pending future ownership 问题。

第一版采用更小且确定的方案：historical cache miss 的 resolve/create 在调用线程等待完成，成功后再 append；失败时 record 尚未入队。该等待只发生在 missing original partition 的 historical fallback 上，normal write hot path 不增加异步 accounting。

### 6. physical target 必须在 bucket assignment 前固定

`bucketAssignerMap` 按 `PhysicalTablePath` 缓存 assigner。Sticky/RoundRobin assigner 内部也保存 physical path，并根据该 path 查询 available buckets。

如果先用 original path 获取 assigner，再把 record 改写到 historical path，会出现：

- sticky assigner 查询 original partition buckets；
- accumulator queue key 与 record target 不一致；
- metadata refresh 仍围绕 original partition；
- request 最终无法路由到 historical leader。

因此 resolved historical path 必须同时用于：

```text
bucketAssignerMap key
BucketAssigner construction
RecordAccumulator path
WriteRecord physical target
Sender metadata lookup
```

### 7. log 与 PK 的 routing context 不相同

两类 historical write 都重写 physical target，但 RPC context 不同：

| write 类型 | physical target | row partition columns | `original_partition_name` |
| --- | --- | --- | --- |
| normal log | original | original | 不携带 |
| historical log | `__historical__` | original | 不携带 |
| normal PK | original | original 或 key-only delete | 不携带 |
| historical PK | `__historical__` | original 或 null row | 携带 original partition name |

若 historical log 错误携带 original name，会把 lookup/PK 专用语义扩散到 PutLog；若 historical PK 漏掉该字段，server 会把请求分发到 normal PutKv path。

### 8. multi-level partition 不能全表共用一个 historical path

对于 partition keys `[region, dt]` 且 auto key 为 `dt`：

```text
region=us$dt=20200101 -> region=us$dt=__historical__
region=eu$dt=20200101 -> region=eu$dt=__historical__
```

resolver 必须复用 `PartitionUtils.toHistoricalPartitionSpec()`，只替换 auto partition key，保留其他 static values。不能用字符串后缀拼接或固定返回单一 `__historical__`。

### 9. create failure 与 retry 必须有明确 ownership

normal dynamic create 当前异步执行，fatal error handler 会 abort pending batches。Historical target resolve 若也先 append 再异步 create，会让 create failure 影响范围难以判断。

PR 6 采用：

- historical resolve/create 失败：当前 send 在 append 前失败；in-flight future 被移除，下一次 send 可重试。
- normal dynamic create 失败：保留当前 fatal error handler 和 batch abort 行为。
- `HISTORICAL_PARTITION_THROTTLED`：复用 PR 4 的 per-batch exponential backoff。
- not-leader、leader movement 和 stale bucket metadata：复用 Sender 现有 metadata refresh/retry。
- original partition 在 batch 入队后被删除：不原地重写 batch；当前 operation 失败后由下一次 send 重新 resolve。

### 10. 端到端测试必须覆盖 client、server 与 Paimon 三段

只验证 `ResolvedWriteTarget` 不足以证明功能完成。PR 6 最少要证明：

```text
client expired detection/redirect
    -> historical log or PutKv RPC
    -> local/WAL visibility
    -> Paimon tiering to original partition
    -> lookup/recovery/cleanup 后结果不变
```

同时不应把 PR 2 到 PR 5 已有的内部 primitive 单测复制到 PR 6。PR 6 的 server/lake 测试重点是跨组件 wiring 和用户可见结果。

## 核心设计

### 1. `ResolvedWriteTarget`

在 client write package 新增一个小型 immutable value object：

```java
final class ResolvedWriteTarget {
    private final PhysicalTablePath physicalTablePath;
    private final boolean historical;
    private final @Nullable String originalPartitionName;

    static ResolvedWriteTarget normal(PhysicalTablePath path) { ... }

    static ResolvedWriteTarget historical(
            PhysicalTablePath historicalPath, String originalPartitionName) { ... }
}
```

不为它增加与当前需求无关的 builder、继承层次或 generic routing abstraction。

构造时保证：

- normal target：`historical=false` 且 `originalPartitionName=null`。
- historical target：`historical=true`、physical path 是 historical system partition，且 original partition name 非空。
- `physicalTablePath` 始终非空。

`historical` 字段不能只由 `originalPartitionName` 在最终 `WriteRecord` 上推断，因为 historical log record 会使用 historical physical target，但不会把 original name写入 RPC context。

### 2. 统一 historical eligibility helper

把 lookup-specific 名称：

```text
isHistoricalLookupCandidatePartition(...)
```

泛化为 lookup/write 共用的：

```text
isHistoricalPartitionCandidate(...)
```

方法继续只负责纯 routing eligibility：

1. table 是 auto-partitioned；
2. data lake enabled；
3. lake format 是 Paimon；
4. partition name 可以按完整 partition keys 解析；
5. auto partition key 可解析；
6. auto partition value 格式合法；
7. retention 配置有效；
8. partition value 严格早于 earliest retained boundary。

该 helper 不查询 metadata，也不创建 partition。original partition 是否 missing 必须由 caller 先通过 cache + refresh 确认。

新增泛化后的方法作为唯一实现，`PrimaryKeyLookuper` 和 write path 都使用它。现有 public static `isHistoricalLookupCandidatePartition()` 保留为一个不标记 deprecated 的薄委托，避免为了名称泛化制造不必要的 source/binary compatibility 风险；其中不能复制 eligibility 逻辑。

### 3. 共享 `HistoricalPartitionResolver`

将 resolver 移到：

```text
org.apache.fluss.client.metadata.HistoricalPartitionResolver
```

选择现有 metadata package 是因为它的职责就是把 table/original partition context 解析成可路由的 historical partition metadata；不为单个类新增新的顶层 package。

resolver 保持 async API，lookup 可以继续 compose future：

```java
CompletableFuture<Long> resolveHistoricalPartitionId(
        TableInfo tableInfo, String originalPartitionName);
```

内部流程保持：

```text
toHistoricalPartitionSpec(original)
    -> get cached historical partition id
    -> refresh historical metadata once
    -> get id again
    -> create historical partition with ignoreIfExists=true
    -> refresh metadata again
    -> require partition id
```

并发语义：

- 同一个 resolver 实例内，相同 `HistoricalPartitionKey` 的 concurrent call 共享一个 future。
- completion 后从 in-flight map 删除 entry；成功结果由 metadata cache 承担，失败不会永久毒化后续请求。
- create 使用 `ignoreIfExists=true`，不同 client 或不同 resolver 实例间的并发 create 由 coordinator 幂等收敛。

PR 6 共享的是实现和语义，不强制让 `FlussConnection` 同时持有 lookup/write 的单例 resolver。Lookup 和 Writer 生命周期不同，各自实例更简单；跨实例 create 仍由 server idempotence 保证。

### 4. `DynamicPartitionCreator` 升级为 write target resolver

将：

```java
void checkAndCreatePartitionAsync(
        PhysicalTablePath physicalTablePath, TableInfo tableInfo)
```

收口为返回 target 的入口，例如：

```java
ResolvedWriteTarget resolveWriteTarget(
        PhysicalTablePath originalPath, TableInfo tableInfo);
```

非 partitioned table 由 `WriterClient` 直接使用 normal target；partitioned table 的状态机如下：

```text
1. original partition id in metadata cache?
       yes -> normal(originalPath)
       no  -> continue

2. force refresh original partition metadata
       exists -> normal(originalPath)
       missing -> continue
       other error -> preserve current FlussRuntimeException behavior

3. isHistoricalPartitionCandidate(tableInfo, originalPartitionName, now)?
       yes -> resolve/create historical system partition
              wait until resolver completes
              return historical(historicalPath, originalPartitionName)
       no  -> continue normal missing-partition path

4. dynamicPartitionEnabled?
       no  -> throw the original PartitionNotExistException
       yes -> continue

5. strictly parse and validate normal auto partition time
       invalid/out-of-date -> preserve current InvalidPartitionException

6. trigger current async normal partition create
       return normal(originalPath)
```

关键点：

- cache hit 不做额外 RPC，normal hot path不变。
- refresh 后发现 original partition 存在时，绝不 historical redirect。
- historical eligibility 在 normal dynamic-create flag 和 out-of-date validation 之前判断。
- historical create 完成前不返回 target。
- non-eligible path 继续执行当前 normal validation/create/error 逻辑。

### 5. historical resolve 与 `flush()` 的时序

write path 调用 resolver 的 future 时，第一版在 send caller thread 等待：

```text
resolveHistoricalPartitionId(...).get()
    -> success: build ResolvedWriteTarget and append
    -> failure: unwrap cause and fail current send before append
    -> interrupted: restore interrupt flag and fail current send
```

这样不需要在 `RecordAccumulator` 外再维护一套 pending-resolution counter，也不需要修改 `flush()`、`close()` 和 abort lifecycle。

性能边界：

- original metadata cache hit：无新增 RPC/等待。
- original cache miss but refresh hit：与当前 dynamic partition check 相同，执行一次 refresh。
- first eligible late write：可能等待 original refresh、historical refresh/create 和 create 后 refresh。
- historical partition 已在 metadata：resolver future 立即完成或只做一次必要 refresh。
- 后续 late write：metadata cache 命中 historical partition，额外开销受限。

### 6. `WriterClient.doSend()` 的新执行顺序

目标流程：

```text
original WriteRecord
    -> resolve ResolvedWriteTarget
    -> create routed WriteRecord
    -> read current Cluster
    -> get/create BucketAssigner by resolved physical path
    -> assign bucket using existing bucket key/sticky rule
    -> append routed record to accumulator
```

伪代码：

```java
WriteRecord routedRecord = record;
PhysicalTablePath resolvedPath = record.getPhysicalTablePath();

if (tableInfo.isPartitioned()) {
    ResolvedWriteTarget target =
            dynamicPartitionCreator.resolveWriteTarget(resolvedPath, tableInfo);
    String rpcOriginalPartitionName =
            target.isHistorical() && record.getWriteFormat().isKv()
                    ? target.originalPartitionName()
                    : null;
    routedRecord =
            record.withOriginalPartitionContext(
                    target.physicalTablePath(), rpcOriginalPartitionName);
    resolvedPath = target.physicalTablePath();
}

BucketAssigner assigner =
        bucketAssignerMap.computeIfAbsent(
                resolvedPath,
                path -> createBucketAssigner(tableInfo, path, conf));
int bucketId = assigner.assignBucket(routedRecord.getBucketKey(), cluster);
accumulator.append(routedRecord, callback, cluster, bucketId, ...);
```

实现时应避免同时保留 original/routed 两套 path 变量并在后续混用。完成 target resolution 后，bucket assignment、trace log、retry append 和 accumulator 都只引用 routed record/path。

### 7. PK 与 log record 改写

historical PK：

```text
physical path = historical system partition
row = original row, or null for key-only delete
key bytes = unchanged
bucket key bytes = unchanged
originalPartitionName = original physical partition name
```

historical log：

```text
physical path = historical system partition
row = original row
bucket key bytes = unchanged
originalPartitionName = null
```

normal record：

```text
physical path = original partition
row/key/bucket key = unchanged
originalPartitionName = null
```

复用 PR 1 已有的 `WriteRecord.withOriginalPartitionContext()`；不新增第二个 mutable setter，也不让 caller 直接构造 historical context。

### 8. bucket assignment 与 batching

有 bucket key 的 log/PK table：

```text
bucketId = existing BucketingFunction(bucketKeyBytes, numBuckets)
```

不把 original partition name 加入 hash。这样 online historical write、Paimon bucket 和 union-read 规划继续对齐。

无 bucket key 的 log table：

- 使用 resolved historical path 创建现有 Sticky/RoundRobin assigner。
- 同一 historical physical path 复用 assigner state。
- sticky window/new batch 行为与 normal log write 相同。

batching：

- normal/historical physical path 天然进入不同 accumulator queue。
- 同一 historical bucket 内，不同 original partitions 的 PK record 继续由 PR 1 的 `originalPartitionName` 拆成不同 `KvWriteBatch`/request group。
- historical log record 可以在同一 historical bucket batch 中混合多个 original partitions，因为每条 row 自己保存完整 partition columns，PutLog 不使用 bucket-level original partition field。
- idempotence sequence、writer id 和 in-flight limit 继续按实际 `TableBucket` 管理。

### 9. metadata、create 和 retry 语义

original partition：

- cache hit：normal target。
- cache miss + refresh hit：normal target。
- cache miss + refresh confirms missing：才允许 historical eligibility。

historical partition：

- cache hit：复用。
- refresh hit：复用。
- missing：idempotent create，再 refresh。
- create/refresh transient failure：当前 send 失败；future entry 移除，下一次 send 重新尝试。

write request：

- historical throttle：复用 PR 4 retry backoff，不能重新走 normal partition。
- not-leader/replica movement：复用现有 Sender metadata refresh。
- retry batch 继续保存其 resolved physical target 和 original PK context，不与 normal batch合并。
- 一个已入队 normal batch 不做 in-place historical retarget；该 race 由 callback failure + caller retry 收敛。

### 10. 端到端数据流

append-only：

```text
expired row
    -> client resolves __historical__ target
    -> existing log bucket assignment
    -> PutLog to historical replica
    -> historical WAL/log consumption
    -> PR 5 reads row partition columns
    -> Paimon original partition
```

primary-key：

```text
expired upsert/delete
    -> client resolves __historical__ target
    -> existing PK bucket assignment
    -> PutKv(original_partition_name)
    -> PR 4 historical dispatch/recovery/flow control
    -> PR 3 local/lake old-value merge
    -> historical WAL + composite local state
    -> PR 5 writes row to original Paimon partition
```

### 11. Authorization 语义

当前 server 已把两类操作分开授权：

```text
create historical system partition -> READ
PutLog / PutKv                    -> WRITE
```

PR 6 复用 lookup 的 system partition resolver，因此首次 late write 在 historical partition 尚不存在时需要 `READ + WRITE`。如果 historical partition 已由其他 client 创建，后续 data write 只经过正常 `WRITE` authorization。

这一点对 PK write 容易理解，因为 historical merge 可能从 lake 读取 old value；append-only late write 技术上不需要 lake read，但当前标准 CreatePartition RPC 无法表达“为 lookup 创建”还是“为 write 创建”。本 PR 保持既有 server contract并在兼容性说明中公开该限制，不在 client 侧绕过 authorization。

## 详细实施步骤

### 步骤 1：泛化 historical eligibility helper

修改 `PartitionUtils`：

- 将 lookup-specific candidate 方法重命名为 lookup/write shared 名称。
- 保留旧 lookup-specific public static 方法作为无 deprecated 标记的薄委托。
- 保持 auto-partition、Paimon、format 和 retention boundary 算法不变。
- 更新 `PrimaryKeyLookuper` 和 `PartitionUtilsTest` 调用点。
- 不把 metadata existence 查询塞入纯 helper。

验证：现有 historical lookup candidate tests 全部保持通过。

### 步骤 2：移动并共享 `HistoricalPartitionResolver`

- 将类从 lookup package 移到 client metadata package。
- 保留 `@Internal`，只暴露 lookup/write 需要的 constructor 和 resolve API。
- 更新 `TableLookup`、`PrimaryKeyLookuper` 的 import/类型。
- 保持 cache -> refresh -> create -> refresh 行为。
- 保持 in-flight future completion/removal 语义。
- 为 resolver 增加 focused tests，尤其覆盖 failed future 后重试。

验证：historical lookup focused test 和 ITCase 行为不变。

### 步骤 3：新增 `ResolvedWriteTarget`

- 增加 normal/historical static factory。
- 校验 target path、historical flag 和 original partition name invariant。
- 不暴露可变 setter。
- 类保持 package-private；只有确有跨 package需求时才提升为 `@Internal public`。

验证：通过 `DynamicPartitionCreatorTest` 的 target assertions 覆盖，不单独为简单 data holder 增加低价值测试类。

### 步骤 4：重构 `DynamicPartitionCreator`

- 注入 shared `HistoricalPartitionResolver`。
- 将 forced metadata refresh 与 dynamic-create-enabled decision 分离。
- 按定义顺序实现 original cache、refresh、historical eligibility、normal fallback。
- historical path 等待 resolver future，成功后返回 historical target。
- normal path继续异步 create，并复用当前 in-flight set/fatal error handler。
- normal create 的日志、异常包装和 batch abort 行为保持不变。
- partition name 为 null 时不进入该类，或直接返回 normal target，二者择一并保持调用点简单。

验证：focused unit tests 覆盖完整 target decision matrix。

### 步骤 5：在 `WriterClient.doSend()` 接入 resolved target

- 在读取/创建 bucket assigner 前 resolve target。
- historical PK 使用 `withOriginalPartitionContext(historicalPath, originalPartitionName)`。
- historical log 使用 `withOriginalPartitionContext(historicalPath, null)`。
- normal path 保持 original path 和 null context。
- retry append/new batch 分支继续使用 routed record 和 resolved assigner。
- trace/error message打印实际 target；必要时同时包含 original target 便于诊断，但不增加新 metrics。

验证：编译期确保后续逻辑没有继续引用 stale original path。

### 步骤 6：补 client focused tests

- 新增 `DynamicPartitionCreatorTest`。
- 新增或移动 `HistoricalPartitionResolverTest`。
- 仅在必要时扩展 `RecordAccumulatorTest`/`SenderTest`，确认 PR 6 生成的 routed PK context 与 PR 1 batching 接口衔接。
- 不重复测试 protobuf field 的基础序列化；PR 1 已覆盖该层。
- 不为 `WriterClient` 增加大规模 test-only abstraction；关键 wiring 由 target unit test 和真实 client/server ITCase 双重覆盖。

### 步骤 7：补 client/server historical write ITCase

在现有 auto-partition/lake test infrastructure 上验证：

- expired append-only write 自动创建并路由到 historical partition。
- expired PK insert/upsert/delete 自动进入 historical server path。
- `dynamicPartitionEnabled=false` 不阻止 historical system partition create。
- normal current partition dynamic-create regression 保持不变。
- malformed/non-eligible partition 仍得到原错误。

### 步骤 8：补 Paimon end-to-end tiering ITCase

在 `fluss-lake-paimon` 新增专门的 historical write ITCase，复用 `HistoricalPartitionLookupITCase` 和 `PaimonTieringITCase` 的 cluster/tiering helpers：

1. 用较大 retention 创建并写入 original partition。
2. 触发 snapshot/tiering，把 old value写入 Paimon。
3. 缩短 retention 并删除 original Fluss partition。
4. 使用正常 AppendWriter/UpsertWriter 写 expired row。
5. 验证 `__historical__` Fluss partition、log/lookup/changelog。
6. 启动 tiering，等待 historical offset 追平。
7. 验证 Paimon original partition 的最终数据。
8. 验证 Paimon metadata 中没有 `__historical__` partition。

### 步骤 9：补 recovery、failover 和 cleanup E2E

- historical write 未 tier 完时重启 leader TabletServer，验证 WAL replay 后 lookup结果相同。
- 停止当前 leader触发 follower promotion，验证后续 write/lookup 正确。
- tiering 追平并触发 historical KV cleanup 后，验证 lookup 从 lake 返回相同 value/tombstone 结果。
- 生命周期测试只断言用户可见语义和必要的 handle state；PR 4 已覆盖 executor/lock/cleanup primitive，不在 PR 6 重复所有内部并发测试。

### 步骤 10：执行 normal regression 与兼容性检查

- normal append/upsert/delete/lookup/tiering tests。
- normal dynamic partition create enabled/disabled tests。
- historical lookup regression。
- client/common/server/Paimon affected modules verify。
- Spotless 和 license checks。

同时确认现有 historical partition authorization test 仍通过，并在 writer-facing ITCase 或测试说明中明确：首次 lazy create 需要 `READ + WRITE`，不能把 authorization failure误判为 historical routing失败。

## 预计文件范围

以下为预计范围，实际实现可按当前分支类名做小幅调整；不应借本 PR 重构无关 writer 或 metadata 代码。

### fluss-common

修改：

```text
fluss-common/src/main/java/org/apache/fluss/utils/PartitionUtils.java
fluss-common/src/test/java/org/apache/fluss/utils/PartitionUtilsTest.java
```

内容：

- 泛化 historical candidate helper 名称。
- 保持原 eligibility 算法与测试矩阵。

### fluss-client main

移动/新增：

```text
fluss-client/src/main/java/org/apache/fluss/client/metadata/HistoricalPartitionResolver.java
fluss-client/src/main/java/org/apache/fluss/client/write/ResolvedWriteTarget.java
```

删除旧位置：

```text
fluss-client/src/main/java/org/apache/fluss/client/lookup/HistoricalPartitionResolver.java
```

修改：

```text
fluss-client/src/main/java/org/apache/fluss/client/lookup/PrimaryKeyLookuper.java
fluss-client/src/main/java/org/apache/fluss/client/lookup/TableLookup.java
fluss-client/src/main/java/org/apache/fluss/client/write/DynamicPartitionCreator.java
fluss-client/src/main/java/org/apache/fluss/client/write/WriterClient.java
```

按需仅更新注释：

```text
fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java
```

`WriteRecord` 已有 `withOriginalPartitionContext()`，不应再增加重复字段或 factory。

### fluss-client tests

新增/移动：

```text
fluss-client/src/test/java/org/apache/fluss/client/metadata/HistoricalPartitionResolverTest.java
fluss-client/src/test/java/org/apache/fluss/client/write/DynamicPartitionCreatorTest.java
```

按需修改：

```text
fluss-client/src/test/java/org/apache/fluss/client/write/RecordAccumulatorTest.java
fluss-client/src/test/java/org/apache/fluss/client/write/SenderTest.java
fluss-client/src/test/java/org/apache/fluss/client/table/AutoPartitionedTableITCase.java
```

### fluss-lake-paimon tests

建议新增：

```text
fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/write/HistoricalPartitionWriteITCase.java
```

也可以按现有测试 package 习惯放入 `tiering`，但不要把所有场景塞进已有大型 `PaimonTieringITCase`。

### 不应修改

除非 E2E 暴露前序 PR 的明确 wiring bug，本 PR 不应修改：

```text
PutKv protobuf
TabletService historical dispatch
HistoricalPkWriteProcessor
HistoricalKvManager / HistoricalKvHandle
HistoricalPartitionTaskExecutor
Paimon historical row partition writer core
```

## 测试计划

### 1. `PartitionUtilsTest`

保留并扩展 shared candidate matrix：

- expired single-key partition -> true。
- earliest retained boundary -> false。
- current/future partition -> false。
- malformed partition -> false。
- multi-key partition 且 auto key 非第一列 -> 正确判断。
- non-auto table -> false。
- lake disabled -> false。
- non-Paimon lake format -> false。
- negative/unbounded retention -> false。

### 2. `HistoricalPartitionResolverTest`

覆盖：

- historical partition id 已在 cache，直接返回且不 create。
- cache miss、forced refresh 后出现，返回 id 且不 create。
- refresh 后仍 missing，执行一次 idempotent create，再 refresh 得到 id。
- 多个 concurrent call 共享同一个 in-flight resolve/create。
- create future 失败时所有 waiter 收到相同失败。
- failed future 从 map 移除，下一次 call 可以重新 create并成功。
- create 成功但 refresh 后仍没有 id，返回明确 `PartitionNotExistException`。
- multi-level original partition解析到保留 static prefix 的 historical path。

### 3. `DynamicPartitionCreatorTest`

normal path：

- original partition cache hit -> normal target，不 refresh、不 create。
- cache miss、refresh hit -> normal target，不 create。
- current missing partition + dynamic enabled -> normal target并触发现有 async create。
- current missing partition + dynamic disabled -> 原 `PartitionNotExistException`。
- malformed partition + dynamic enabled -> 原 `InvalidPartitionException` 和消息。
- non-Paimon expired partition -> 不 historical fallback，保留 normal path异常。
- lake-disabled/non-auto expired partition -> 不 historical fallback。

historical path：

- expired + metadata missing + auto + Paimon lake -> historical target。
- `dynamicPartitionEnabled=false` 时相同场景仍 historical target。
- target path 为 `__historical__`，original partition name 保留。
- resolver failure 时没有 normal create，也没有成功 target。
- concurrent same-partition write 只触发一个 in-flight resolve/create。
- failed resolve 后下一次 write可以重试。
- multi-key不同 static prefix得到不同 historical target。
- 同一 static prefix不同 expired time得到同一 historical physical target。

### 4. Writer routing focused coverage

验证 wiring：

- bucket assigner map 使用 resolved historical path，不使用 original path。
- historical PK routed record 携带 original partition name。
- historical log routed record 的 original partition name 为 null。
- row object/partition fields、key bytes 和 bucket key bytes 未修改。
- PK hash bucket 与同 bucket key normal rule一致。
- append-only bucket-key rule一致。
- append-only sticky/round-robin assigner在 historical path上正常工作。
- normal record 仍使用 original path 和 null context。
- `send(); flush();` 在首次 historical create 场景不会提前返回或漏 record。

若直接 unit-test `WriterClient` 需要大量 test-only injection，优先通过 `DynamicPartitionCreatorTest` + accumulator现有测试 + ITCase 覆盖，不为单个调用顺序引入新抽象。

### 5. Client/server routing ITCase

- 使用 `dynamicPartitionEnabled=false` 的 client写 eligible expired log partition成功。
- 使用 `dynamicPartitionEnabled=false` 的 client写 eligible expired PK partition成功。
- `admin.listPartitionInfos()` 包含 historical system partition。
- expired original partition不会被重新创建。
- current retained partition仍走 normal physical partition。
- non-lake auto table写 expired partition继续失败。
- malformed partition继续失败，且没有创建 historical partition。

### 6. Append-only end-to-end

- late append 写入 historical physical partition。
- 从 historical bucket 的普通 log scanner消费到 row。
- row partition columns等于 original expired partition，而不是 `__historical__`。
- 有 bucket key时 bucket id与现有 hash规则一致。
- 无 bucket key时 sticky行为与 normal append一致。
- historical log tier 到 original Paimon partition。
- Paimon partition list不包含 `__historical__`。

### 7. Primary-key insert/update/delete end-to-end

insert：

- lake 中没有 old value时，historical insert成功。
- immediate lookup返回新值。

update：

- 先把 old value tier 到 Paimon并删除 original Fluss partition。
- late upsert从 lake获取 old value。
- historical changelog包含正确 `UPDATE_BEFORE + UPDATE_AFTER`。
- immediate lookup返回新值。
- tiering 后 Paimon original partition返回新值。

delete：

- 对只提供 primary key 的 delete，RPC original partition name仍正确。
- local state写入 tombstone，immediate lookup为空。
- tiering 后 lookup仍为空，不能从 lake复活旧值。
- cleanup/drop local historical state后 lookup仍为空。

### 8. original partition isolation

- 两个 expired original partitions映射到同一 historical physical partition/bucket。
- 使用相同 primary key写不同 value。
- batch/RPC context不会混合。
- historical composite key不会冲突。
- 分别 lookup返回各自 value。
- tiering 后分别落到两个 original Paimon partitions。

### 9. multi-level partition

以 `[region, dt]` 且 auto key 为 `dt` 为例：

- `us + expired dt` -> `us + __historical__`。
- `eu + expired dt` -> `eu + __historical__`。
- 两个 historical partitions有独立 partition id/bucket/WAL。
- row static/auto partition columns均保持 original values。
- Paimon tiering落到正确 `region + original dt` partition。

### 10. recovery、failover 与 cleanup

- historical write append 后、tiering 前重启当前 leader。
- restart 后 first lookup触发或复用 recovery，返回相同 value/tombstone。
- leader failover 后继续 historical upsert，writer retry和顺序正确。
- tiering追平后 historical local handle被清理。
- cleanup 后 lookup从 lake返回相同结果。
- cleanup 后再写一条 late record，可重新懒恢复/创建 local state并成功。

### 11. normal regression

- normal append-only write。
- normal PK upsert/delete。
- normal point lookup。
- normal Paimon tiering。
- normal dynamic create enabled。
- normal dynamic create disabled。
- historical lookup现有 ITCase。
- normal Sender retry/idempotence tests。

## 兼容性

### Wire compatibility

本 PR 不修改 protobuf：

- PK original partition field由 PR 1 已增加，仍为 optional。
- log historical write继续使用现有 PutLog RPC。
- normal request不会设置 historical context。

### Old client -> new server/tiering

- old client不会主动把 expired write重定向到 historical partition。
- normal write行为不变。
- expired write仍表现为之前的 `PartitionNotExistException` 或 normal dynamic create/drop问题。
- 新 server可以继续接受不带 original partition field 的 normal request。

### New client -> old server/tiering

- normal partition write不受影响。
- eligible expired write会尝试创建/写入 historical system partition。
- 未包含 PR 1 到 PR 4 的旧 server无法正确处理 historical PK write；未包含 PR 5 的旧 tiering无法保证写回 original Paimon partition。
- 不新增版本探测；该组合不支持 historical write，失败不应静默写错 normal partition。

推荐 rollout 顺序：

```text
1. 升级 Coordinator/TabletServer，使 PR 1-4 server能力可用
2. 升级 Paimon tiering service，使 PR 5可用
3. 最后升级/启用包含 PR 6 的 client writer
```

### State compatibility

- PR 6 不改变 local KV key、historical RocksDB directory、WAL record或snapshot格式。
- old client产生的 normal WAL继续由新 tiering处理。
- new client historical WAL依赖 PR 5 tiering语义。

### Source/API compatibility

- `HistoricalPartitionResolver` 是 `@Internal` client implementation，移动 package不属于 public table API。
- `ResolvedWriteTarget` 保持 package-private。
- 泛化 candidate helper 时保留原 public static lookup方法作为薄委托，不使用 `@Deprecated`，也不保留两份判断逻辑。
- `WriterClient.send()`、AppendWriter 和 UpsertWriter public API不变。
- `DynamicPartitionCreator` 虽为 public class，但属于 client internal write implementation；只在当前仓库由 `WriterClient` 构造。实现时仍应检查 API compatibility工具结果。

## 本 PR 必须防住的风险

### 风险 1：仍先执行 normal out-of-date validation

后果：eligible late write在 redirect前被 `InvalidPartitionException` 拒绝，PR 6实际没有打开功能。

防护：确认 original missing后，先判断 historical candidate；只有 candidate=false才进入 normal `validateAutoPartitionTime()`。

### 风险 2：`dynamicPartitionEnabled=false` 提前拒绝 historical write

后果：system partition create错误地受 user dynamic-create配置控制。

防护：先完成 historical eligibility/resolve，再在 normal fallback分支检查 dynamic flag。

### 风险 3：metadata cache stale导致仍存在 partition被误路由

后果：同一业务 partition同时收到 normal/historical write，破坏 changelog和消费语义。

防护：cache miss后必须强制 refresh original partition；refresh hit永远返回 normal target。

### 风险 4：target resolve晚于 accumulator append

后果：record进入 original queue，或 `flush()` 在 record真正入队前返回。

防护：historical resolver完成后再构造 routed record、assign bucket和append；第一版不使用 accumulator外的 async pending resolution。

### 风险 5：bucket assigner仍按 original path创建

后果：sticky/round-robin查询错误 buckets，Sender也无法找到 historical leader。

防护：target resolution后只使用 resolved path作为 assigner map key和 constructor参数。

### 风险 6：historical log错误设置 original RPC context

后果：把 PK/lookup专用的 bucket-level partition语义扩散到 PutLog，甚至触发错误 dispatch假设。

防护：只有 `writeFormat.isKv()` 的 historical record设置 `originalPartitionName`；log只改 physical path。

### 风险 7：historical PK漏设置 original partition name

后果：server按 normal PutKv处理 historical replica，key-only delete也失去 original partition身份。

防护：historical PK routed record必须通过已有 `withOriginalPartitionContext(path, originalName)` 构造，E2E覆盖 insert/update/key-only delete。

### 风险 8：修改 row partition columns

后果：consumer看到 `__historical__` 而不是业务 partition，PR 5也会把数据写入错误 Paimon partition。

防护：`withOriginalPartitionContext()` 只复制 record metadata，row/key/bucketKey引用和值保持不变；E2E同时验证 log scan和Paimon partition。

### 风险 9：multi-level partition丢失 static prefix

后果：不同 region/tenant的late data进入同一个 historical partition，造成隔离错误。

防护：只使用 `toHistoricalPartitionSpec()` 替换 auto key；测试 auto key不同位置和多个 static prefix。

### 风险 10：failed resolver future永久缓存

后果：一次 coordinator transient failure后，后续所有 write立即复用失败结果，无法恢复。

防护：completion时用 `remove(key, future)` 删除 in-flight entry；测试失败后第二次成功。

### 风险 11：并发 create形成风暴或 already-exists被当成fatal

后果：多个 writer同时 abort batch，historical partition首次写入不稳定。

防护：resolver实例内合并 concurrent future；跨实例使用 `ignoreIfExists=true`；create后统一 refresh metadata。

### 风险 12：normal dynamic partition语义漂移

后果：current/future/malformed/non-lake partition的异常类型或创建行为改变。

防护：candidate=false后调用原 normal分支；保留现有 validation、in-flight set、fatal error handler和focused regression tests。

### 风险 13：同一 historical bucket的不同 original PK进入同一 batch

后果：一个 bucket-level original partition name被应用到其他 partition的records，产生错误 composite key和lake lookup。

防护：保留 PR 1 `KvWriteBatch.originalPartitionName` consistency和Sender request grouping；PR 6只负责正确填充 routed record。

### 风险 14：新 client在旧 tiering上静默写入错误 Paimon partition

后果：Fluss historical write成功，但lake数据进入 `__historical__` 或丢失original partition语义。

防护：明确 server/tiering-first rollout；E2E必须在PR 5实现上验证 Paimon不存在historical partition。

### 风险 15：E2E只验证立即 lookup，不验证lake最终状态

后果：测试只覆盖 historical RocksDB cache，无法发现 PR 5 tiering wiring错误或cleanup后数据回退。

防护：每个关键 PK/log case都至少验证 immediate local结果和tiering/cleanup后的Paimon或lake lookup结果。

### 风险 16：首次 historical write 的权限要求被忽略

后果：只有 `WRITE` 权限的 principal 在 historical partition 不存在时会在 create 阶段收到 `AuthorizationException`，即使 append-only data path本身不需要 lake read；用户可能误以为 routing 或 server write故障。

防护：计划和 release note 明确当前首次 lazy create 需要 `READ + WRITE`，保留现有 create authorization test，并确保 client传播原始 authorization cause。若产品要求 write-only principal也能首次创建，必须先单独修改 server authorization contract，不能在 client重试中绕过。

## 验证命令

先运行 common/client focused tests：

```bash
./mvnw test -pl fluss-common -Dtest=PartitionUtilsTest
./mvnw test -pl fluss-client -Dtest='HistoricalPartitionResolverTest,DynamicPartitionCreatorTest'
./mvnw test -pl fluss-client -Dtest='RecordAccumulatorTest,SenderTest,KvWriteBatchTest'
```

运行 client integration/regression：

```bash
./mvnw test -pl fluss-client -Dtest=AutoPartitionedTableITCase
./mvnw test -pl fluss-client -Dtest=FlussLakeTableITCase
```

运行 Paimon historical end-to-end：

```bash
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=HistoricalPartitionLookupITCase
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=HistoricalPartitionWriteITCase
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=PaimonTieringITCase
```

运行 affected modules：

```bash
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server,fluss-lake/fluss-lake-paimon
./mvnw verify -pl fluss-flink/fluss-flink-common
```

格式与静态检查：

```bash
./mvnw spotless:check
```

若完整 Paimon ITCase 在本地环境耗时较长，至少先跑 focused client tests 和单个 `HistoricalPartitionWriteITCase`；PR 合并前仍需由 CI 完成 affected module verify。

## 完成标准

代码与行为：

- [ ] historical eligibility helper 可被 lookup/write共享，lookup行为不变。
- [ ] `HistoricalPartitionResolver` 只有一份实现，lookup/write不复制 create逻辑。
- [ ] original cache hit或refresh hit时，write继续走 normal target。
- [ ] eligible expired Paimon auto partition解析为正确 historical target。
- [ ] historical system partition create不受 `dynamicPartitionEnabled` 控制。
- [ ] historical resolve/create失败发生在 accumulator append前，并允许下次重试。
- [ ] `WriterClient` 在 bucket assignment前应用 resolved target。
- [ ] historical PK携带 original partition name，historical log不携带。
- [ ] row/key/bucket key未被改写。
- [ ] multi-level partition保留 static prefix。
- [ ] normal dynamic create和异常语义保持不变。
- [ ] historical create/write 的 `READ + WRITE` authorization要求已记录，authorization failure保留原 cause。

端到端：

- [ ] append-only late row可从 historical bucket消费。
- [ ] append-only late row tier到 original Paimon partition。
- [ ] PK historical insert成功。
- [ ] PK historical update从lake读取old value并产生正确changelog。
- [ ] PK key-only delete不会在lookup/tiering/cleanup后复活。
- [ ] 两个 original partitions映射到同一 historical bucket时状态隔离。
- [ ] 多级 partition映射到不同 static-prefix historical partitions。
- [ ] TabletServer restart和leader failover后结果正确。
- [ ] tiering追平并cleanup后lake lookup结果一致。
- [ ] Paimon metadata中没有 `__historical__` partition。

验证：

- [ ] common/client focused tests通过。
- [ ] historical lookup regression通过。
- [ ] historical write Paimon ITCase通过。
- [ ] normal writer/tiering regression通过。
- [ ] affected modules verify通过。
- [ ] Spotless通过。

## 合并后的行为

PR 6 合并后，满足以下条件的 missing partition write 会自动进入 historical path：

```text
auto-partitioned
+ Paimon lake-enabled
+ partition name合法
+ partition早于retention boundary
+ original metadata在refresh后仍missing
```

append-only write 会进入 historical log；primary-key upsert/delete 会进入 historical KV pipeline。两者都保留 row 中的 original partition columns，并最终由 Paimon tiering写回 original partition。

仍保留的第一版边界：

- historical eligibility不证明 original partition曾经存在。
- historical changelog是独立 supplemental stream，不与expired partition offset连续。
- historical Arrow tiering保留batch fetch，在Paimon writer内逐行write。
- historical RocksDB按整 bucket cleanup，持续late write可能延迟清理。
- original partition在batch入队后才被删除时，当前 batch不做原地retarget；caller重试后才进入historical path。
- 新 client应在PR 1到PR 5 server/tiering能力部署后再 rollout。
- historical partition尚未创建时，writer principal需要当前 server contract要求的 `READ + WRITE` 权限。
