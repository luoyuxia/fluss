# FIP-28 Historical Write PR 3 实施计划

## PR 标题

```text
[server] Add historical primary-key write processor
```

## 目标

本 PR 实现可直接调用和单测的 historical primary-key write processor，完成以下能力：

- 根据 actual target partition metadata 和 original partition name 校验 historical write。
- 复用普通 `KvTablet` 的 schema validation、row merger、partial update、auto-increment、WAL builder 和 writer idempotence 语义。
- historical old-value lookup 按 local prewrite、historical RocksDB、Paimon lake 的顺序执行。
- historical local delete/tombstone 阻止 lake fallback。
- historical mutation 使用 PR 2 的 composite key 和 tombstone state。
- WAL 继续保存 original partition columns。
- WAL append 失败或 duplicated batch 时回滚 historical prewrite state。
- historical point lookup 优先返回尚未 tier 到 Paimon 的 local state。

本 PR 不修改 `TabletService.putKv()` 的线上 dispatch。正常 client write 仍走普通 `putRecordsToKv()`；只有 focused tests 和内部 manager-level API 会调用 historical processor。

## 与总计划的关系

本 PR 对应 `FIP-28-historical-partition-write-pr-plan-zh.md` 中的：

```text
PR 3: Historical PK Write Processor
```

前置依赖：

- PR 1 已提供 `PutKvDataForBucket`，其中保存 actual `TableBucket`、`KvRecordBatch` 和 nullable `originalPartitionName`。
- PR 2 已提供 `HistoricalKvManager`、`HistoricalKvHandle`、`HistoricalKvStateAccessor`、composite key、三态 local lookup 和 tombstone mutation。
- historical lookup 系列 PR 已提供 `PartitionUtils` historical eligibility helpers、`HistoricalLakeLookupManager` 和 Paimon `LakeTableLookuper`。

后续依赖：

- PR 4 将本 PR 的 processor 接入 `TabletService`/`ReplicaManager` online dispatch，并增加 shared flow control、per-bucket FIFO、recovery 和 cleanup。
- PR 5 保证本 PR 生成的 historical WAL 按 row 中的 original partition columns tier 到正确的 Paimon partition。
- PR 6 在 client 侧启用 expired partition write fallback，开始主动发送 historical PK write。

## 前置假设

- `PutKvDataForBucket.tableBucket()` 表示实际写入的 `__historical__` physical partition bucket。
- `PutKvDataForBucket.originalPartitionName()` 表示 row 原本所属的 expired business partition。
- historical target replica 当前可以通过 `TableBucket` 解析到实际 `PhysicalTablePath`；server 不信任 RPC 字段自行推断 target partition name。
- original primary key bytes 与现有 historical lookup 发送给 `LakeTableLookuper` 的 key bytes 使用相同编码。
- historical local value 与 lake lookup 返回值都使用 Fluss encoded value 格式，可以交给现有 `ValueDecoder`。
- historical handle 的 write lock 覆盖一个完整 batch 的 old-value lookup、mutation、WAL append 和 rollback。
- PR 3 的 processor 运行在 TabletServer I/O worker。PR 3 的内部 manager-level API负责这一执行边界；PR 4 再在同一入口增加 keyed serial execution 和 limiter。
- PR 3 不负责 restart/failover recovery。local handle 不存在时，point lookup 直接 fallback 到 lake；不能假设 WAL 已经 replay 到 local state。

## 非目标

本 PR 不实现：

- `TabletService.putKv()` 根据 `original_partition_name` 选择 historical path。
- normal/historical mixed request 的线上处理。
- request heap copy、historical semaphore、throttle error 或 client backoff。
- per-historical-bucket keyed serial executor。
- historical state restart recovery、remote WAL replay 或 leader promotion recovery。
- lake offset 追平后的 RocksDB cleanup、idle eviction 或新配置项。
- Paimon historical multi-partition tiering。
- client expired partition eligibility 和 write target redirect。
- append-only/log table historical write。
- Iceberg、Lance 或其他 lake format historical write。
- historical RocksDB snapshot/checkpoint。
- original partition existence registry。
- prefix lookup 或 scan historical local state。

## 当前实现约束

### 1. `KvTablet.putAsLeader()` 仍把 processing core 与 normal state 绑定

PR 2 已把 key lookup 和 mutation 收口到 `KvStateAccessor`，但当前 public entry 固定使用：

```text
KvTablet.kvLock
NormalKvStateAccessor
normal RocksDB open check
```

`processKvRecords()`、row merger、WAL builder 和 append/rollback 仍是 `KvTablet` private methods。Historical processor 不能复制这些方法，否则以下语义会形成两份实现：

- schema evolution validation。
- target columns 和 partial update。
- configured merge engines。
- auto-increment update。
- WAL/FULL changelog image。
- writer id、batch sequence 和 duplicated detection。
- Arrow/Compacted/Indexed WAL construction。

本 PR 要抽出一个只负责“KV records -> state mutations + WAL”的 shared processing core。Normal `KvTablet` 和 historical processor 都调用同一实现。

### 2. lake fallback 同时需要 original key 和 encoded storage key

PR 2 的 processing flow 先执行：

```text
original primary key
    -> stateAccessor.encodeKey()
    -> encoded key
    -> stateAccessor.lookup(encoded key)
```

historical local storage 需要 encoded composite key：

```text
partition length + original partition bytes + original primary key
```

Paimon lookup 需要 original primary key，不能收到 composite key。当前 `lookup(Key)` 签名在 key encoding 后丢失了 original key context。

本 PR 需要让 old-value lookup 同时获得：

```text
originalPrimaryKey  -> lake lookup
encodedStorageKey   -> prewrite/RocksDB lookup
```

推荐把 `KvStateAccessor.lookup()` 调整为：

```java
KvStateLookupResult lookup(byte[] originalPrimaryKey, Key encodedStorageKey)
        throws Exception;
```

normal accessor 忽略 `originalPrimaryKey`；historical lake-fallback accessor 同时使用两种 key。不要给 composite key 增加只为 lake lookup 服务的 decode 路径。

### 3. `HistoricalLakeLookupManager.lookup()` 不能在 processor 内递归调度

现有 historical point lookup 通过 `CompletableFuture.supplyAsync(..., ioExecutor)` 执行。Historical write processor 自身也必须在 `ioExecutor` 工作线程运行。

如果 processor 在 I/O worker 中调用 async `lookup()` 后同步等待，会出现以下问题：

- 同一 executor 上发生嵌套提交。
- executor 饱和时可能自等待。
- 每条 row 都额外创建 future 和 bucket result wrapper。
- lookup permit 被按 key 重复计算，和 PR 4 的 bucket-request permit 冲突。

本 PR 应从 `HistoricalLakeLookupManager` 提取或暴露 package-private synchronous lake-value lookup：

```text
lookupValue(tableInfo, originalPartitionSpec, bucketId, originalPrimaryKey)
```

该方法继续复用现有 table-level lookuper cache 和 schema replacement 逻辑，但不再提交 executor，也不获取 lookup permit。调用方必须已经位于 historical I/O task 中。

现有 point lookup async entry 继续负责 executor 和 permit，并在内部调用同一个 synchronous lookup primitive。

### 4. target partition 和 original partition 是两个独立身份

同一个 historical request 同时包含：

```text
actual target:
    TableBucket.partitionId
    Replica.physicalTablePath.partitionName = staticPrefix/.../__historical__

original partition:
    PutKvDataForBucket.originalPartitionName
    row partition columns = staticPrefix/.../expiredTime
```

Processor 必须根据 `TableBucket` 获取 actual `Replica`，再读取 replica 上的 physical partition metadata。不能只因为 RPC 带了 original partition name 就进入 historical path。

对于多级 partition，actual historical partition 与 original partition 必须共享所有 non-auto partition values。可以逐步校验：

1. 严格解析 original partition name 得到 original spec。
2. 用 `PartitionUtils.toHistoricalPartitionSpec()` 把 auto key 替换为 `__historical__`。
3. 严格解析 replica physical partition name 得到 actual target spec。
4. 比较两个 resolved specs。
5. 任一 key/value 不一致都拒绝该 bucket batch。

### 5. local tombstone 和 local miss 的行为不同

PR 2 已定义三态 local lookup：

```text
PRESENT    local value is authoritative
DELETED    local delete is authoritative
NOT_FOUND  local state has no decision; lake fallback is allowed
```

Historical write 和 historical point lookup 都必须保持这一规则：

- `PRESENT`：直接使用 local encoded value。
- `DELETED`：映射为业务 not-found，不访问 lake。
- `NOT_FOUND`：查询 Paimon。
- lake 返回 null：最终业务 not-found。
- lake 返回 value：包装为 `PRESENT`。

不能先把三态 result 转成 nullable `byte[]` 再决定是否 fallback；这样会把 `DELETED` 与 `NOT_FOUND` 合并。

### 6. 完整 batch 需要单一 rollback boundary

一个 historical batch 可能包含多条 upsert/delete。任何一条记录发生以下错误时，之前写入 historical prewrite 的 mutation 都要回滚：

- row partition 与 RPC original partition 不一致。
- lake lookup 失败。
- row merge 或 auto-increment 失败。
- WAL build/append 失败。
- append 结果为 duplicated。

因此完整处理必须位于同一个 `HistoricalKvHandle.withWriteLock()` scope：

```text
capture logEndOffsetBeforeBatch
validate/process all records
append WAL
duplicated -> truncate(DUPLICATED)
exception  -> truncate(ERROR)
```

PR 2 的 accessor 单操作锁是独立调用安全边界；PR 3 仍要增加 outer batch lock。`ReentrantReadWriteLock` 允许 accessor 在 outer write lock 内重入。

### 7. historical point lookup 已经是线上能力

`TabletService.lookup()` 当前会把带 original partition name 的请求交给 `ReplicaManager.historicalLookups()`。PR 3 修改 local-first lookup 后会影响该线上路径。

安全边界如下：

- handle 已存在：先读 local state，再按三态决定是否访问 lake。
- handle 不存在：直接访问 lake，不在 PR 3 为 lookup 创建空 handle。
- PR 3 尚无线上 historical write dispatch，因此正常部署不会产生新的 local write state。
- focused processor tests 创建的 handle 可以立即被 local-first lookup 看到。
- PR 4 合并 recovery 后，first access 才可以执行 get-or-create + replay。

## 核心设计

### 1. Shared KV write processing core

从 `KvTablet` 抽取窄的 shared processor，例如：

```text
KvWriteProcessor
```

它负责：

- 获取并校验 latest schema。
- 校验 input schema id。
- 选择 DEFAULT/OVERWRITE row merger。
- 配置 target columns。
- 获取 schema-specific auto-increment updater。
- 创建 WAL builder 并设置 writer id/batch sequence。
- 遍历 `KvRecordBatch`。
- lookup old value、merge row、写 state mutation。
- append WAL。
- duplicated/error rollback。
- 释放 WAL/Arrow resources。

它接收：

```text
KvRecordBatch
targetColumns
MergeMode
KvStateAccessor
optional per-record validator
```

它不负责：

- 选择 normal 或 historical path。
- 获取 historical handle。
- historical eligibility validation。
- executor、flow control 或 request aggregation。
- required-acks delayed completion。
- snapshot、row count 或 state flush lifecycle。

Normal `KvTablet.putAsLeader()` 继续持有 `kvLock`，检查 normal RocksDB open，然后使用 `NormalKvStateAccessor` 调用 shared processor。Normal observable behavior保持不变。

Historical processor 在 handle outer write lock 内，使用 lake-fallback historical accessor 调用同一个 shared processor。

shared processor 的 lifecycle 需要与 replica/tablet lifecycle 对齐。实现时优先让它只持有现有 processing 所需资源，并提供明确 `close()`；不要让 historical processor 依赖 ordinary RocksDB 或 ordinary snapshot state。这样 PR 4 可以让 historical replica 跳过 normal KV recovery。

### 2. Historical lake-fallback state accessor

在 PR 2 的 local accessor 外增加一个 decorator，例如：

```text
HistoricalLakeFallbackStateAccessor
    -> HistoricalKvStateAccessor localDelegate
    -> HistoricalLakeValueLookup lakeLookup
    -> ResolvedPartitionSpec originalPartitionSpec
    -> bucketId
```

key encoding 和 mutation 全部委托给 local accessor：

```text
encodeKey      -> composite key
insert/update  -> historical prewrite
delete         -> historical tombstone mutation
truncate/flush -> historical handle
```

lookup 执行：

```text
localDelegate.lookup(originalKey, encodedKey)
    PRESENT -> return PRESENT
    DELETED -> return DELETED
    NOT_FOUND -> lakeLookup.lookup(originalKey, context)
        null  -> NOT_FOUND
        value -> PRESENT(value)
```

decorator 不缓存 per-key result。table-level Paimon resources 继续由现有 lookuper cache 管理。

### 3. Historical request context and validation

新增 immutable processing context 或在 processor entry 中一次性解析以下信息：

```text
Replica
TableInfo
actual historical TableBucket
actual historical ResolvedPartitionSpec
original ResolvedPartitionSpec
original canonical partition name
HistoricalKvHandle
Historical lake lookup context
```

validation 顺序固定为：

1. `PutKvDataForBucket.tableBucket()` 与 target replica bucket 一致。
2. replica 当前是 leader；required acks 满足当前 ISR 条件。
3. actual replica 是 primary-key table replica。
4. actual replica physical partition name non-null。
5. actual physical partition 满足当前 table partition spec 的 historical system partition 规则。
6. table 是 auto-partitioned。
7. table 已开启 lake，并且 table lake format 是 Paimon。
8. request 的 `originalPartitionName` non-null 且非空。
9. original partition name 可以按 table partition keys 严格解析。
10. `PartitionUtils.isHistoricalLookupCandidatePartition(tableInfo, name, now)` 返回 true。
11. `toHistoricalPartitionSpec(tableInfo, originalName)` 与 actual physical partition spec 相等。
12. 每个非-delete row 中提取出的 partition spec 与 request original spec 相等。

步骤 5 和 11 都需要保留：

- 步骤 5 防止 normal partition 因为 request 带 original name 而进入 historical state。
- 步骤 11 防止多级 partition 的 static prefix 串写。

时间判断使用注入的 `Clock`：

```java
Instant.ofEpochMilli(clock.milliseconds())
```

不要在 processor 内直接调用 `Instant.now()`，测试需要稳定覆盖 retention boundary。

### 4. Row partition consistency validator

新增只在 historical path 启用的 per-record validator。它接收 decoded input `BinaryRow` 和该 batch 的 schema context。

upsert row：

1. 根据 input schema 找到每个 partition key 的 field index 和 `DataType`。
2. 从 row 读取 non-null partition values。
3. 使用 `PartitionUtils.convertValueOfType()` 生成 canonical string values。
4. 构造 `ResolvedPartitionSpec`。
5. 与 request 中已解析的 original spec 比较。

delete record：

- `KvRecord.getRow()` 为 null。
- 不尝试从 key 反解 partition columns。
- request `originalPartitionName` 是唯一来源。

partial update 仍可校验 row partition：client 写入的 `InternalRow` 保持完整 table field count，`targetColumns` 只控制 merger 更新哪些列，不改变 `KvRecord` row 的 schema layout。

validator 在 shared processor 的 record loop 中、写入该 record mutation 前执行。若 batch 前面的 records 已产生 pending mutations，外层 error rollback 会恢复到 batch 开始 offset。增加一个测试让 mismatch record 位于 batch 中间或末尾，验证整个 batch 没有残留。

### 5. Historical PK write processor and manager-level entry

新增 synchronous processor，例如：

```text
HistoricalPkWriteProcessor.process(
    Replica,
    PutKvDataForBucket,
    targetColumns,
    MergeMode,
    requiredAcks)
```

建议由一个内部 manager 持有 processor 和 `ioExecutor`，提供 future-based focused entry：

```text
HistoricalPkWriteManager.put(...)
    -> submit complete bucket task to ioExecutor
    -> return CompletableFuture<PutKvResultForBucket>
```

PR 3 的 manager-level entry 只供 focused tests 和 PR 4 后续接线使用。它不从 `TabletService` 调用。

Replica boundary 继续负责：

- 持有 `leaderIsrUpdateLock` read lock。
- 校验 local leadership。
- 校验 `requiredAcks` 对应的 in-sync replica 数量。
- processor 成功 append 后调用现有 `maybeIncrementLeaderHW()`。

processor boundary 负责：

- historical request validation。
- handle get-or-create。
- complete-batch handle write lock。
- shared KV processing core。
- local/lake old-value lookup。

Manager boundary 负责：

- 把 slow lake I/O 移出 RPC thread。
- 把 exception 转换成 bucket result/future failure。
- executor reject 时完成 future。

PR 3 不在 manager 中增加 semaphore 或 FIFO queue。Handle write lock保证同 bucket 不会并发修改 state，但不承诺并发提交顺序；PR 4 增加 keyed serial executor 后再提供 FIFO contract。

### 6. Historical state directory acquisition

Historical processor 不应依赖 `replica.getKvTablet()` 或 normal KV directory 已经创建。PR 4 会让 historical replica 跳过 normal snapshot-based KV state。

推荐让 `HistoricalKvManager` 根据 replica tablet parent directory 创建 handle：

```text
replica.getTabletParentDir()
    -> historical-kv-<bucketId>
```

可以把 PR 2 的 internal API 从：

```text
getOrCreate(tableBucket, kvTabletDir)
```

调整为接收 tablet parent directory，或增加语义明确的 overload。目录结果仍保持：

```text
<historical partition directory>/historical-kv-<bucketId>/db
```

不要为了获得 parent directory 而创建空的 `<...>/kv-<bucketId>` normal tablet directory。

### 7. Local-first historical point lookup

扩展现有 historical point lookup 的每个 key 处理：

```text
HistoricalKvManager.getIfPresent(tableBucket)
    absent -> lake lookup
    present -> create HistoricalKvStateAccessor(originalPartitionName)
        local PRESENT -> return value
        local DELETED -> return null
        local NOT_FOUND -> lake lookup
```

point lookup 和 write 使用同一 composite key codec、同一 original partition parsing 和同一个 synchronous lake-value primitive。

local point lookup 获取 handle read lock。Historical write 的 outer write lock覆盖完整 batch，因此 point lookup 只能看到 batch 前或 batch 后状态，不会看到 WAL append 前的中间 mutations。

PR 3 不在 lookup miss 时创建 handle，也不触发 WAL recovery。

### 8. WAL、idempotence 和 high watermark

Historical write 继续 append 到 actual `__historical__` bucket 的 `LogTablet`：

- WAL row 使用 merge 后的 original business row。
- row partition columns保持 original values。
- WAL 不写 composite key，也不把 partition column替换成 `__historical__`。
- writer id 和 batch sequence从原 `KvRecordBatch` 复制到 WAL builder。
- duplicated result不保留 historical prewrite mutation。
- error result使用 `TruncateReason.ERROR` 回滚。
- success result返回 `appendInfo.lastOffset() + 1` 作为 bucket log end offset。
- `requiredAcks=0/1/-1` 的合法性和 ISR 校验复用普通 Replica 逻辑。
- PR 3 只推进 local high watermark；等待 follower replication 的 `DelayedWrite` aggregation 在 PR 4 online dispatch 接入。

### 9. Changelog semantics

Historical processor必须与普通 PK write保持一致：

```text
lake/local old value absent + upsert
    -> INSERT

old value present + full/partial update
    -> WAL image: UPDATE_AFTER
    -> FULL image: UPDATE_BEFORE + UPDATE_AFTER

delete + old value present
    -> DELETE or merger-defined update

delete + old value absent/deleted
    -> no data change, but empty WAL batch仍推进 writer sequence
```

configured merge engines继续决定 FIRST_ROW、VERSIONED、AGGREGATION 等行为。Historical path不另写简化 merger。

## 详细实施步骤

### Step 1: 调整 state lookup contract

修改：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/KvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/NormalKvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java
```

要求：

- lookup 同时接收 original primary key 和 encoded storage key。
- normal accessor lookup结果和 allocation behavior不变。
- historical local accessor仍只查 prewrite + historical RocksDB，不直接依赖 lake plugin。
- processing loop保留 `keyBytes`，不能只传 composite key。
- `DELETED` 与 `NOT_FOUND` 在进入 old-value resolution 前保持可区分。

### Step 2: 抽取 shared KV write processor

新增或重构：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/KvWriteProcessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java
```

要求：

- 搬移现有 schema/merge/WAL processing，不重新实现。
- normal `putAsLeader()` 仍在 `kvLock` 下执行。
- normal RocksDB open check仍只属于 normal entry。
- shared processor接受 state accessor 和 nullable/no-op record validator。
- rollback以 batch 开始前的 `localLogEndOffset` 为边界。
- WAL builder在 finally 中释放。
- processor close释放它自己拥有的 Arrow writer resources。
- snapshot、flush、multiGet、prefix lookup和 row count仍留在 `KvTablet`。

### Step 3: 提取 synchronous lake-value lookup

修改或新增：

```text
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalLakeLookupManager.java
```

可选新增一个窄 collaborator：

```text
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalLakeValueLookup.java
```

要求：

- synchronous primitive复用现有 cached `LakeTableLookuper`。
- lookup context使用 original resolved partition spec、actual bucket id、latest schema id和 row type。
- schema id变化时替换并关闭旧 lookuper。
- processor调用不再次提交 `ioExecutor`、不再次获取 permit。
- 现有 async historical point lookup行为和 throttling保持。
- test可以注入 fake lake lookup，避免 historical processor unit test依赖 Paimon plugin。

### Step 4: 实现 request validation 和 row validator

新增建议：

```text
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPkWriteValidator.java
```

或者作为 processor 的 package-private helper，避免单独 public API。

要求：

- actual target从 `Replica.getPhysicalTablePath()` 读取。
- original partition严格解析一次并复用 resolved spec。
- eligibility使用注入 `Clock`。
- expected historical spec与 actual spec逐项比较。
- upsert row使用 input schema提取 partition fields。
- delete只信任 RPC original partition。
- error使用现有 `InvalidPartitionException`、`InvalidTableException` 或更精确的已有异常，不增加 wire error code。
- error message包含 table path、actual target和 original partition，不能打印整批 row bytes。

### Step 5: 实现 historical lake-fallback accessor

新增建议：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalLakeFallbackStateAccessor.java
```

如果 lake collaborator 位于 `replica` package，可把 decorator放在 processor相邻 package，避免 `kv.historical` 依赖 replica implementation；核心要求是依赖方向清楚且不形成 package cycle。

要求：

- local `PRESENT` 和 `DELETED` 不调用 lake。
- 只有 local `NOT_FOUND` 调用 lake。
- lake key使用 original primary key。
- lake returned value通过 `KvStateLookupResult.present()` 校验 non-empty。
- mutation/rollback全部委托给 PR 2 historical accessor。

### Step 6: 实现 Historical PK processor 和内部 manager

新增建议：

```text
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPkWriteProcessor.java
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPkWriteManager.java
```

修改：

```text
fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java
fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvManager.java
```

要求：

- manager由 `ReplicaManager` ownership并在 shutdown时关闭。
- manager可以通过 actual `TableBucket` 获取 replica。
- complete bucket processing在 `ioExecutor` 执行。
- Replica entry持 leader read lock并处理 required acks/HW。
- processor在 handle write lock中调用 shared KV processor。
- 每个失败都完成 future，不留下悬挂 test request。
- executor reject不会泄漏 handle lock或 incomplete result。
- 不修改 `TabletService.putKv()`、`ServerRpcMessageUtils.getPutKvData()` 或 normal online call chain。

### Step 7: 接入 local-first point lookup

修改：

```text
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalLakeLookupManager.java
fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java
```

要求：

- lookup从 `HistoricalKvManager.getIfPresent()` 开始。
- handle absent不创建 state。
- local lookup使用 original partition-specific accessor和 handle read lock。
- tombstone返回 null且不访问 lake。
- local miss继续复用现有 lake lookup。
- bucket result顺序、错误转换和 response callback行为保持。

### Step 8: 补充 normal regression 和 lifecycle

要求：

- shared processor extraction后 normal tests保持通过。
- shared Arrow writer/auto-increment resources只关闭一次。
- historical processor/manager shutdown不先关闭 `HistoricalLakeLookupManager` 正在使用的 lookuper。
- table invalidation继续同时清理 cached lake lookuper和 historical handle。
- PR 3 不启动 recovery/cleanup background task。

## 预计文件范围

预计新增：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/KvWriteProcessor.java
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPkWriteProcessor.java
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPkWriteManager.java
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPkWriteValidator.java
```

可能新增：

```text
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalLakeValueLookup.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalLakeFallbackStateAccessor.java
```

预计修改：

```text
fluss-server/src/main/java/org/apache/fluss/server/kv/KvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/NormalKvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvStateAccessor.java
fluss-server/src/main/java/org/apache/fluss/server/kv/historical/HistoricalKvManager.java
fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalLakeLookupManager.java
fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java
fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java
```

预计新增测试：

```text
fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalPkWriteProcessorTest.java
fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalPkWriteValidatorTest.java
fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalPkWriteManagerTest.java
```

预计修改测试：

```text
fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalLakeLookupManagerTest.java
fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletTest.java
fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletMergeModeTest.java
fluss-server/src/test/java/org/apache/fluss/server/kv/KvTabletSchemaEvolutionTest.java
fluss-server/src/test/java/org/apache/fluss/server/kv/historical/HistoricalKvHandleTest.java
```

实际实现时可以合并足够小的 package-private validator/decorator，避免为了匹配计划文件名制造额外类型。Shared processor和 historical processor的职责边界需要保留。

## 测试计划

### HistoricalPkWriteValidatorTest

覆盖 table/request-level validation：

- actual target是合法 historical partition。
- normal target即使携带 original partition name也被拒绝。
- request缺少 original partition name被拒绝。
- empty original partition name被拒绝。
- malformed original partition name被拒绝。
- current partition被拒绝。
- future partition被拒绝。
- retention window内 partition被拒绝。
- retention boundary前 partition被接受。
- `numToRetain < 0` 时不允许 historical write。
- non-auto-partitioned table被拒绝。
- non-lake table被拒绝。
- Iceberg/Lance table被拒绝。
- non-PK table被拒绝。
- multi-level partition static prefix匹配时接受。
- multi-level partition static prefix不匹配时拒绝。
- actual historical spec缺少/多出 partition key时拒绝。
- manual clock跨 retention boundary时结果可重复。

覆盖 row/RPC consistency：

- full upsert row partition与 RPC一致。
- UTF-8、多级 partition value可以正确提取。
- row partition与 RPC不一致时拒绝。
- batch第二条或最后一条 row不一致时拒绝整个 batch。
- partial update row仍按完整 input row提取 partition。
- key-only delete不要求 row，使用 RPC original partition。
- null partition column被拒绝。

### HistoricalPkWriteProcessorTest

使用 fake synchronous lake lookup、真实 historical handle和测试 `LogTablet` 覆盖：

- local/lake都无旧值时生成 INSERT。
- lake有旧值时生成正确 update changelog。
- FULL image生成 `UPDATE_BEFORE + UPDATE_AFTER`。
- WAL image生成 `UPDATE_AFTER`。
- partial update以 lake old value补齐未更新 columns。
- DEFAULT mode使用 configured merger。
- OVERWRITE mode绕过 configured merger，保持普通语义。
- FIRST_ROW 对 lake existing row不产生实际 change。
- VERSIONED/AGGREGATION 至少各覆盖一个与普通 path一致的 focused case。
- auto-increment使用现有 updater和 ID range。
- key-only delete从 RPC original partition构造 composite key。
- delete命中 lake old value后生成 DELETE并写 local tombstone。
- delete命中 local tombstone时不再访问 lake，也不复活 old row。
- pending local value优先于 lake value。
- flushed RocksDB value优先于 lake value。
- local miss才调用 lake。
- lake lookup exception回滚整个 batch。
- row partition mismatch回滚该 batch前面已经写入的 mutations。
- WAL append exception执行 `TruncateReason.ERROR`。
- duplicated append执行 `TruncateReason.DUPLICATED`。
- retry相同 writer id/batch sequence不会留下重复 local mutation。
- empty-change batch仍推进 writer sequence。
- 两个 original partitions中的相同 PK互不影响。
- 两个 static prefixes映射到不同 historical targets，不交叉写入。
- WAL row保留 original partition columns。
- WAL内不出现 `__historical__` partition value。

### HistoricalPkWriteManagerTest

覆盖 execution/lifecycle：

- complete bucket task运行在指定 I/O executor。
- caller thread不会执行 lake lookup。
- invalid required acks返回 bucket error。
- not-leader返回 bucket error。
- insufficient ISR返回普通 existing error。
- success后推进 high watermark的行为与普通 Replica entry一致。
- executor reject完成 future且不创建/invalidate错误的 handle。
- manager close后新请求失败。
- PR 3不承诺并发 submit FIFO；测试只验证 handle outer lock避免同 bucket并发 mutation。

### Historical local-first lookup tests

扩展 `HistoricalLakeLookupManagerTest`：

- handle absent直接查询 lake。
- prewrite `PRESENT` 返回 local value，不调用 lake。
- RocksDB `PRESENT` 返回 local value，不调用 lake。
- pending `DELETED` 返回 null，不调用 lake。
- flushed tombstone返回 null，不调用 lake。
- local `NOT_FOUND` 查询 lake。
- local miss且 lake miss返回 null。
- 同一 PK在不同 original partitions返回各自 local value。
- lookup与write lock竞争时看不到 half-applied batch。
- handle并发 invalidation时返回明确 bucket error，不触发 native resource use-after-close。

### Normal KV regression

继续运行：

```text
KvTabletTest
KvTabletMergeModeTest
KvTabletSchemaEvolutionTest
KvPreWriteBufferTest
```

重点验证：

- normal full upsert/delete行为不变。
- normal old-value lookup不访问 lake。
- normal key仍是 original primary key，不增加 partition prefix。
- normal delete仍执行 physical RocksDB delete。
- partial update、merge engine和 auto-increment不变。
- schema evolution validation不变。
- writer id/batch sequence和 duplicated rollback不变。
- WAL/FULL changelog image不变。
- flush、snapshot、row count、multiGet和 prefix lookup不变。

本 PR 不新增 `TabletServiceTest`，因为 `TabletService.putKv()` dispatch保持关闭。可以增加一条守护测试确认线上 `putKv()` 仍调用 normal decoder/path，但不要用 online RPC 测试 historical processor。

## 兼容性

### Wire compatibility

- 本 PR 不修改 protobuf field、error code或 response schema。
- PR 1 的 optional `original_partition_name` wire format保持。
- 普通 client不发送 original partition name，线上路径不变。

### Normal write compatibility

- shared processor extraction必须保持 normal write结果、offset和 exception behavior。
- normal state accessor不接受 lake collaborator。
- normal request不创建 historical handle。
- normal table不执行 historical eligibility检查。

### Local disk compatibility

- historical composite key和 tombstone格式沿用 PR 2。
- 不修改 ordinary RocksDB directory或 on-disk key/value。
- historical handle directory从 tablet parent确定时，最终路径仍为 PR 2 已测试的 `historical-kv-<bucketId>/db`。
- PR 3 不把 historical directory加入普通 tablet loader。

### Rolling upgrade

- `TabletService.putKv()` 未接线，old/new server混部时 normal write behavior相同。
- historical point lookup在 handle不存在时仍是现有 lake-only结果。
- local-first分支只会观察当前进程内已经创建的 historical handle。
- client historical routing必须等 PR 4 recovery/dispatch 和 PR 5 tiering完成后再启用。

## 本 PR 必须保证的行为

### 风险 1: lake lookup误用 composite key

防护：state lookup contract同时传 original primary key和 encoded storage key；lake collaborator只接收 original key。

### 风险 2: tombstone后从 Paimon复活旧值

防护：local lookup在决定 fallback前保留三态；`DELETED` 直接返回业务 not-found，只有 `NOT_FOUND` 查询 lake。

### 风险 3: historical path与 normal path merge语义漂移

防护：抽取一份 shared KV write processor；两条路径复用 schema、merger、auto-increment、WAL和 rollback代码。

### 风险 4: 多级 partition写入错误 static prefix

防护：从 original spec计算 expected historical spec，再与 replica actual physical spec逐项比较。

### 风险 5: row partition与 RPC original partition不一致

防护：每个 upsert row在 mutation前提取并比较 resolved spec；任一 mismatch触发整个 batch rollback。

### 风险 6: batch中途失败留下 pending state

防护：完整 batch位于同一 handle write lock和 shared processor rollback boundary；exception统一 truncate到 batch前 offset。

### 风险 7: nested ioExecutor lookup自等待

防护：processor使用 synchronous lake-value primitive；只有外层 manager/point lookup async entry负责 executor submission。

### 风险 8: local point lookup看到未 append WAL的中间状态

防护：write持 handle outer write lock，point lookup持 handle read lock；WAL append和 rollback包含在 write lock内。

### 风险 9: PR 3依赖 ordinary KvTablet，阻碍 PR 4跳过 snapshot recovery

防护：shared processing core不依赖 normal RocksDB；historical handle从 replica tablet parent创建，不通过 `replica.getKvTablet()` 获取目录或 processor state。

### 风险 10: PR 3提前暴露不可恢复的线上 write

防护：不修改 `TabletService.putKv()` 和 normal `ReplicaManager.putRecordsToKv()` dispatch；内部 entry只被 focused tests和后续 PR调用。

### 风险 11: slow lake lookup阻塞 RPC thread

防护：内部 historical write manager把完整 bucket task提交到 TabletServer `ioExecutor`。PR 4再增加准入上限和 per-bucket FIFO。

### 风险 12: async write success后 required-acks语义不完整

防护：PR 3复用 Replica的 leader/ISR校验和 high-watermark update；`DelayedWrite` response aggregation明确留在 PR 4 online dispatch，PR 3 tests不宣称已支持生产 RPC acks completion。

## 验证命令

运行 historical processor focused tests：

```bash
./mvnw test -pl fluss-server \
  -Dtest=HistoricalPkWriteValidatorTest,HistoricalPkWriteProcessorTest,HistoricalPkWriteManagerTest
```

运行 historical storage/lookup regression：

```bash
./mvnw test -pl fluss-server \
  -Dtest=HistoricalKvKeyCodecTest,HistoricalKvBatchWriterTest,HistoricalKvHandleTest,HistoricalKvManagerTest,HistoricalLakeLookupManagerTest
```

运行 normal KV regression：

```bash
./mvnw test -pl fluss-server \
  -Dtest=KvTabletTest,KvTabletMergeModeTest,KvTabletSchemaEvolutionTest,KvPreWriteBufferTest
```

格式、Checkstyle和模块验证：

```bash
./mvnw validate -pl fluss-server
./mvnw verify -DskipITs -pl fluss-server -am
```

检查 Java 8 compatibility：

```bash
./mvnw clean install -DskipTests -Pjava8 -pl fluss-server -am
```

## 完成标准

以下条件全部满足后，PR 3才算完成：

- historical processor根据 replica actual metadata验证 target是合法 `__historical__` partition。
- 只接受 PK、auto-partitioned、Paimon lake-enabled table的 expired original partition。
- multi-level original partition与 target static prefix严格匹配。
- 每个 upsert row的 partition spec与 RPC original partition一致。
- key-only delete只使用 RPC original partition构造 composite key。
- old-value lookup顺序固定为 prewrite、historical RocksDB、Paimon lake。
- lake lookup始终使用 original primary key，不使用 composite key。
- pending delete和flushed tombstone都阻止 lake fallback。
- lake value可以参与普通 full update、partial update和 configured merge engine。
- historical insert/update/delete生成与 normal path一致的 changelog image。
- WAL row保留 original partition columns。
- writer id/batch sequence、empty-change batch和 duplicated detection复用普通逻辑。
- WAL error、lake error、row validation error和 duplicated batch都正确 rollback historical prewrite。
- 完整 batch持有 handle write lock，point lookup持 read lock。
- local-first point lookup覆盖 prewrite、RocksDB、tombstone和 lake fallback。
- normal `KvTablet` behavior在 shared processor extraction后保持。
- historical task通过 I/O executor的内部 manager-level entry执行。
- `TabletService.putKv()` 和 normal online dispatch未修改。
- PR 3不实现 recovery、cleanup、flow control、keyed FIFO、tiering或 client routing。
- focused tests、normal regression、Spotless/Checkstyle和 Java 8 compilation通过。

## 合并后的行为

PR 3合并后：

- server内部具备完整 historical PK batch processing能力。
- processor可以从 local state或 Paimon读取 old value，并复用普通 merge/WAL逻辑写入 historical state和 `__historical__` WAL。
- focused tests可以验证 late insert、update、partial update、delete、tombstone和 rollback。
- historical point lookup可以立即看到当前进程内尚未 tier到 Paimon的 local write，并且 delete不会从 lake复活。
- normal `TabletService.putKv()` 仍不会调用 historical processor。
- restart/failover后 local state仍没有恢复保证。
- 完整线上能力继续依赖 PR 4的 dispatch/recovery/cleanup、PR 5的 Paimon historical tiering和 PR 6的 client routing。
