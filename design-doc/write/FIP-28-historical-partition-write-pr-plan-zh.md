# FIP-28 历史分区写入 PR 拆分计划

## 目标

本文档把 FIP-28 historical partition write 的实现拆成 6 个可独立 review 和合并的 PR。拆分顺序先增加协议和 batching 表达能力，再建立 historical KV 状态、恢复与清理机制，然后修正 Paimon tiering，最后启用客户端历史写路由和端到端测试。

本计划覆盖 historical partition write，不重复实现已经完成的 historical partition lookup 基础能力。

## 前置条件

本计划假设 historical partition lookup 系列 PR 已经合并，并且代码库已经具备以下能力：

- `PartitionUtils` 可以识别、构造和校验 `__historical__` partition。
- coordinator 可以通过现有 `createPartition` RPC 幂等创建 historical system partition。
- `AutoPartitionManager` 不会按 TTL 删除 historical partition。
- client 已经有 `HistoricalPartitionResolver`，可以解析或创建 historical partition id。
- `LakeStorage` 已经提供 `LakeTableLookuper` SPI。
- Paimon 已经实现 `PaimonLakeTableLookuper`。
- server 已经有 historical lookup 的 `ioExecutor` 隔离、准入流控和 lookuper cache。

如果 lookup 系列 PR 尚未全部合并，应先按 lookup PR plan 完成公共能力，避免 write path 再实现一套 historical partition eligibility、创建和 lake lookup 逻辑。

## 总体边界

第一版支持的能力：

- 支持 append-only/log table 和 primary-key table 的历史分区写入。
- 只支持 auto-partitioned、Paimon lake-enabled 表。
- 只处理原始分区已从 Fluss metadata 缺失、分区名合法、并且按 auto partition retention 规则判断为 expired 的写入。
- client 将物理写入目标改为对应静态前缀下的 `__historical__` partition。
- row 中的原始 partition columns 保持不变。
- historical PK write 在 RPC 中携带 original partition name。
- historical PK write 的旧值读取顺序为 prewrite buffer、historical RocksDB、Paimon lake。
- historical WAL tiering 后写入 Paimon 中的原始 partition，而不是 `__historical__` partition。
- historical RocksDB 不做 snapshot/checkpoint，通过 WAL replay 恢复。

第一版不支持的能力：

- Iceberg、Lance、Hudi historical write。
- 按 original partition 增量 `DeleteRange` 清理 historical RocksDB。
- historical Arrow batch 的多 partition 分组写入优化。
- historical partition 与原始 partition 之间的 offset continuity 保证。
- 证明原始 partition 曾经存在的 authoritative registry。

关键设计决定：

- `PbPutKvReqForBucket.partition_name` 是 historical PK upsert 和 delete 的统一 original partition 来源。upsert 可以额外校验 row 中的 partition columns 与 RPC 字段一致。
- historical PK batching key 是 `(historical TableBucket, original partition name)`。
- 同一个 `PutKvRequest` 中不允许出现相同 `TableBucket`、不同 original partition name 的 bucket request；client 直接拆成不同 RPC。
- server 只在 historical target 上执行 lake old-value fallback。普通 PK write path 不进入 lake lookup。
- 第一版historical Arrow log split继续通过`pollRecordBatch()`读取，在Paimon writer内逐行提取partition并写入。
- client historical fallback 在最后一个 PR 才启用。在此前 PR 中，新增能力只通过 focused tests 验证，不改变正常写入路由。

## PR 1: PutKv RPC + Client Write Batching Plumbing

### 目标

为 historical PK write 增加 original partition name 的协议字段和 client batching 表达能力。该 PR 不启用 expired partition write fallback。

### 实现范围

扩展 put-kv RPC：

```protobuf
message PbPutKvReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  required bytes records = 3;
  optional string partition_name = 4;
}
```

重新生成 RPC classes：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

client write plumbing：

- 给 `WriteRecord`、`WriteBatch` 和 `KvWriteBatch` 增加 `@Nullable String originalPartitionName`。
- 普通写入的 `originalPartitionName` 为 null。
- 增加能够表达 original partition name 的 write batch key。该 key 至少包含 physical table path、bucket id 和 nullable original partition name。
- `RecordAccumulator` 不能只通过 historical physical path 和 bucket id 聚合 PK batch。
- 同一 historical bucket 中，`partition_name = "2000"` 和 `partition_name = "2001"` 必须进入不同 `KvWriteBatch`。
- `ClientRpcMessageUtils.makePutKvRequest` 只有在 batch 的 original partition name 非 null 时才设置 `partition_name`。
- `Sender` 将普通写、historical write、不同 original partition name 的 historical write 拆成不同 request group。
- 同一个 request group 中不能出现两个相同 `TableBucket` 的 batch，避免 response 只按 `TableBucket` 关联时产生覆盖或错误完成 batch。
- `ProduceLogRequest` 不增加 partition name 字段。log table 的原始 partition 身份继续保存在 row 中。
- 普通 write batching、linger、retry 和 response dispatch 行为保持不变。

server plumbing：

- 新增 `PutKvDataForBucket`，包含 `TableBucket`、`KvRecordBatch` 和 `@Nullable String partitionName`。
- `ServerRpcMessageUtils` 解析并保留 `partition_name`，不再立即丢失 bucket-level metadata。
- 普通 put-kv 请求未携带 `partition_name` 时保持当前路径。
- 该 PR 可以完成字段解析，但不能把携带 `partition_name` 的请求路由到 historical KV pipeline。

兼容 Rust client 的手写 RPC message 定义，确保 optional field 的 tag 与 Java proto 一致。

### 测试要求

- `RecordAccumulatorTest` 覆盖相同 historical bucket、不同 original partition name 不会合批。
- `SenderTest` 覆盖普通写和 historical write 被拆成不同 RPC。
- `SenderTest` 覆盖相同 `TableBucket`、不同 original partition name 被拆成不同 RPC。
- `ClientRpcMessageUtilsTest` 验证普通 batch 不设置 `partition_name`。
- `ClientRpcMessageUtilsTest` 验证 historical batch 正确设置 `partition_name`。
- `ServerRpcMessageUtilsTest` 验证 `partition_name` 可以完整解析到 `PutKvDataForBucket`。
- 普通 append、upsert、delete 的现有 batching 和 retry 测试继续通过。

### 合并后的行为边界

合并后，client 和 server 已经能够传输 historical PK write 所需的 original partition name，client accumulator 也能正确隔离不同 original partition 的 batch。

expired partition write 仍保持旧行为。`WriterClient` 不会重定向到 `__historical__`，server 也不会执行 historical KV write。

兼容性边界：

- 老 client 不发送 `partition_name`，新 server 按普通 put-kv 处理。
- 新 client 在最后一个 PR 启用 historical fallback 前不会主动发送 `partition_name`。

## PR 2: Historical KV Storage Primitives

### 目标

建立 per-historical-bucket 的非持久化 KV 状态、composite key 和 tombstone 语义。该 PR 只提供可单测的存储能力，不接入 TabletService 在线请求。

### 实现范围

新增 `HistoricalKvManager`，负责管理 `TableBucket` 到 `HistoricalKvHandle` 的映射：

- historical RocksDB 按 bucket 懒创建。
- 数据目录放在对应 KV tablet 目录旁的独立子目录，例如 `historical-kv/`。
- 每个 historical bucket 只使用一个 RocksDB 实例和 default column family。
- handle 保存 RocksDB、historical prewrite buffer、last-access time 和并发控制对象。
- table drop、replica removal 和 TabletServer shutdown 时可以按 table/bucket 关闭并删除 handle。
- 该 PR 只实现显式 lifecycle；idle eviction 和 tiered cleanup 放到 PR 4。

新增 composite key codec：

```text
4-byte big-endian partitionNameLength
+ UTF-8 partitionName
+ originalPrimaryKey
```

codec 要求：

- partition name 使用 UTF-8 bytes 的长度，而不是 Java character 数量。
- 编码必须避免 `("ab", "c")` 与 `("a", "bc")` 发生碰撞。
- lookup、put、delete 和 recovery 使用同一个 codec。
- 普通 KV key codec 不做修改。

新增 historical tombstone：

```java
private static final byte[] TOMBSTONE_VALUE = new byte[0];
```

- historical delete flush 时向 RocksDB 写入 tombstone，不调用物理 `delete()`。
- historical RocksDB lookup 遇到 tombstone 时返回明确的 deleted 状态，调用方不能继续 fallback 到 lake。
- 正常 encoded value 至少包含 schema id prefix，空 byte array 可以保留给 tombstone。
- 普通 `KvPreWriteBuffer` 的 delete flush 行为保持不变。

为后续复用 `KvTablet` merge/WAL 逻辑，增加最小的 state-access 扩展点：

- key transformation：普通路径使用原始 key，historical 路径生成 composite key。
- old-value lookup：普通路径使用 prewrite + normal RocksDB，historical 路径后续增加 lake fallback。
- mutation sink：普通路径物理 delete，historical 路径写 tombstone。
- truncate/rollback：失败或 duplicated batch 时都可以恢复到 batch 前状态。

优先抽取窄接口或 package-private context，不复制完整的 `processUpsert()`、`processDeletion()`、row merger 和 WAL builder 实现。

### 测试要求

- composite key codec 覆盖 ASCII、UTF-8、多级 partition name、空 primary key 和长 key。
- 覆盖容易碰撞的 partition/key 组合编码结果不同。
- `HistoricalKvManagerTest` 覆盖同一 bucket 复用 handle、不同 bucket 隔离 handle。
- 覆盖 historical put、update、delete 和 tombstone lookup。
- 覆盖 tombstone 不被解释成普通 encoded value。
- 覆盖 close、invalidate bucket、invalidate table 和 shutdown lifecycle。
- 覆盖 normal `KvTablet` put/delete/merge tests 在抽取 state-access 扩展点后保持不变。
- 覆盖 duplicated/error truncate 对 historical prewrite buffer 的回滚语义。

### 合并后的行为边界

合并后，server 内部具备 historical KV state primitives，但没有在线 RPC 会进入该路径，也不会创建 historical RocksDB。

该 PR 不做 lake old-value lookup、不做 WAL recovery、不做 tiered cleanup，也不修改 historical point lookup 的返回结果。

## PR 3: Historical PK Write Processor

### 目标

在不打开 client fallback 和 TabletService dispatch 的前提下，完成可直接调用和单测的 historical PK write processor，包括 row merge、lake old-value fallback、WAL append 和 local-first lookup。

### 实现范围

新增 historical write processor，输入至少包含：

- `Replica` 或写入所需的 tablet context。
- `PutKvDataForBucket`。
- `TableInfo`。
- target columns 和 merge mode。
- required acks。

historical request validation：

1. 根据 target `partitionId` 获取实际 physical partition metadata。
2. 验证 target partition 是符合当前表 partition spec 的 `__historical__`。
3. 验证表是 primary-key、auto-partitioned、Paimon lake-enabled 表。
4. 验证请求携带 original `partition_name`。
5. 严格解析 original partition name，并验证它是 expired auto partition candidate。
6. 验证 original partition name 与 historical static prefix 匹配。
7. 对 upsert，从 row partition columns 提取 partition name，并验证它与 RPC 字段一致。
8. 对 key-only delete，以 RPC `partition_name` 为唯一 original partition 来源。

old-value lookup 顺序：

```text
historical prewrite buffer
    -> historical RocksDB
    -> LakeTableLookuper
```

具体语义：

- prewrite 或 RocksDB 找到普通 value 时直接返回。
- 找到 tombstone 时返回 not-found，不能查询 lake。
- local state 不存在该 composite key 时，通过现有 Paimon `LakeTableLookuper` 查询 original partition 和 bucket。
- lake value 继续使用当前 schema/value decoder 参与 row merge。
- lake lookup 必须发生在 `ioExecutor` 工作线程，processor 不能假设自己运行在 RPC thread。

写入处理：

- 复用 `KvTablet` 的 schema validation、row merger、auto-increment、WAL builder 和 writer id/batch sequence 幂等逻辑。
- historical state 使用 composite key，WAL row 保持原始 partition columns。
- insert/update/delete 生成与普通 PK write 一致的 changelog image。
- historical delete 向 prewrite buffer 写 tombstone mutation。
- WAL append 失败或 batch duplicated 时，truncate historical prewrite state。
- 成功 append WAL 后按现有 replica 逻辑推进 high watermark。

扩展 historical point lookup 为 local-first：

```text
historical prewrite buffer
    -> historical RocksDB
    -> Paimon lake
```

这样已经写入 historical WAL、但尚未 tier 到 Paimon 的数据可以立即被 point lookup 看到。tombstone 必须阻止 lake fallback。

该 PR 只提供 processor 和 manager 级调用入口。`TabletService.putKv()` 仍不根据 `partition_name` dispatch 到该 processor。

### 测试要求

- server 单测覆盖 historical insert 在 lake 无旧值时生成 INSERT。
- 覆盖 historical update 从 lake 获取旧值并生成 `UPDATE_BEFORE + UPDATE_AFTER`。
- 覆盖 partial update 和 configured merge engine 复用普通语义。
- 覆盖 key-only delete 从 RPC `partition_name` 构造 composite key。
- 覆盖 delete 写 tombstone，随后 lookup 不会从 lake 复活旧值。
- 覆盖同一 primary key 在两个 original partitions 中互不影响。
- 覆盖 row partition 与 RPC `partition_name` 不一致时拒绝整个 bucket batch。
- 覆盖 malformed/current/future/non-expired original partition name 被拒绝。
- 覆盖 batch duplicate 和 WAL append error 后 historical prewrite state 正确回滚。
- 覆盖 local-first historical lookup 的 prewrite、RocksDB、tombstone 和 lake fallback 四种分支。

### 合并后的行为边界

合并后，historical PK processor 可以通过 focused tests 完整处理 historical write 和 local-first lookup，但生产 RPC 入口还没有启用。

该 PR 不保证 restart/failover 后恢复，也不做 RocksDB cleanup。保持 TabletService dispatch 关闭可以避免在 recovery 合并前暴露不完整的在线写路径。

## PR 4: Server Dispatch + Flow Control + Recovery + Cleanup

### 目标

把 historical PK processor 接入 TabletService，并补齐异步执行、同 bucket 顺序、restart recovery 和 tiering 后清理。client fallback 仍保持关闭。

### 实现范围

TabletService 和 ReplicaManager dispatch：

- 普通 `PutKvRequest` 继续走当前同步 `putRecordsToKv()` 路径。
- request 中存在 historical bucket metadata 时，整个对应 bucket request 进入 historical path。
- historical target 缺少 `partition_name` 时返回 bucket-level validation error。
- normal target 携带 `partition_name` 时拒绝，不能静默走普通 KV。
- 一个 request 混合 normal 和 historical bucket 时，优先拆分处理；如果现有 response aggregation 无法安全表达，第一版直接拒绝 mixed request，并由 client 保证隔离。
- 每个异步 historical bucket request 在离开 RPC thread 前调用 `KvRecordBatch.copyToHeap()`。

异步执行和顺序：

- 复用 TabletServer `ioExecutor`。
- 增加 keyed serial executor，以 historical `TableBucket` 为 key。
- 同一个 historical bucket 的 write task 按提交顺序 FIFO 执行。
- 不同 bucket 可以并发执行。
- delayed produce、acks 和 response callback 在异步写完成后继续使用当前语义。
- replica leadership 变化、table drop 和 shutdown 时，未执行任务需要失败完成，不能悬挂 client future。

historical flow control：

- 将现有 lookup-only historical semaphore 提取为 write/lookup 共享 limiter。
- 继续使用 `netty.server.max-queued-historical-requests` 作为共享准入上限。
- permit 粒度按 historical bucket request 计算。
- 获取 permit 失败时不复制大 batch、不提交 `ioExecutor`，直接返回 throttle error。
- 成功、失败、取消和 executor reject 路径都释放 permit。
- 将 `HISTORICAL_LOOKUP_THROTTLED` 泛化为 `HISTORICAL_PARTITION_THROTTLED` 时保留原 error code，避免 wire code 改变。
- lookup client 和 write client 都识别泛化后的 retriable error。
- write `Sender` 使用 100ms 起始、2 倍增长、最大 5s、带 jitter 的 historical throttle backoff。
- normal write 不经过 historical limiter。

historical recovery：

- historical replica 不创建或恢复普通 snapshot-based KV state。
- `HistoricalKvManager` handle 增加 `UNINITIALIZED`、`RECOVERING`、`READY` lifecycle，或者使用等价的 future coalescing。
- 第一次 historical write 或 lookup 触发 lazy recovery；并发访问等待同一个 recovery future。
- recovery 开始时关闭并删除旧 historical RocksDB 目录，从空实例开始。
- 从 exclusive `lakeLogEndOffset` 开始 replay；没有 lake offset 时从可用 log start 开始。
- 当 recovery offset 早于 local log start 时，复用 `RemoteLogFetcher` 读取 remote WAL。
- `[lakeLogEndOffset, highWatermark)` 的记录直接写入 historical RocksDB。
- `[highWatermark, localLogEndOffset)` 的记录写入 historical prewrite buffer。
- WAL row 保留原始 partition columns，recovery 从 row 中解析 original partition name 并重新编码 composite key。
- DELETE WAL 包含被删除的 old row，recovery 将其转换为 historical tombstone。
- recovery 失败时关闭并删除 incomplete handle；下次访问可以重试。
- follower 不需要持续维护 historical KV；leader promotion 后第一次访问执行同样的 recovery。

tiering 后清理：

- 在 `notifyLakeTableOffset()` 更新 historical bucket 的 exclusive lake log end offset 后检查清理条件。
- 只有 `lakeLogEndOffset >= localLogEndOffset` 时才提交 cleanup candidate。
- cleanup task 进入同一个 per-bucket serial executor，与 historical write 串行。
- task 执行时重新读取两个 offset；条件不再满足时跳过。
- 每个 handle 使用读写锁：local lookup 获取 read lock，cleanup 使用 `tryLock()` 获取 write lock。
- 获取 write lock 失败时跳过，之后由下一次 offset update 或周期任务重试。
- 条件仍成立时关闭并删除整个 historical RocksDB，下一次访问再懒创建。
- `kv.historical.idle-timeout` 增加周期检查。第一版只在数据已经完全 tiered 时删除 idle handle，避免 idle eviction 触发额外 WAL recovery。
- table drop、replica stop 和 server shutdown 取消 cleanup task 并清理 handle。

### 测试要求

- `TabletServiceTest` 覆盖 normal request 携带 `partition_name` 被拒绝。
- 覆盖 historical request 缺少 `partition_name` 被拒绝。
- 覆盖 mixed normal/historical request 的明确行为。
- 覆盖 async dispatch 前执行 heap copy，RPC buffer 释放后任务仍可读取 batch。
- 覆盖同 bucket FIFO、不同 bucket 并发。
- 覆盖 semaphore 满时 write/lookup 都返回 throttle error。
- 覆盖 normal write 不受 historical semaphore 影响。
- 覆盖所有 completion path 释放 permit。
- client 测试覆盖 historical throttle retry/backoff。
- recovery 单测覆盖 lake offset、high watermark 和 local log end 三段边界。
- 覆盖 remote WAL replay。
- 覆盖 recovery 中 upsert、delete tombstone 和多个 original partitions。
- 覆盖并发 first access 只执行一次 recovery。
- 覆盖 recovery failure 后可以重试。
- cleanup 测试覆盖 offset 未追平不删除、追平后删除、重新检查失败时跳过。
- 覆盖 cleanup 与 write 串行、cleanup 与 lookup 的读写锁竞争。
- 覆盖 idle timeout 只删除已经完全 tiered 的 handle。
- restart 和 leader promotion focused ITCase 验证 historical local state 可以从 WAL 重建。

### 合并后的行为边界

合并后，新 server 已经可以安全处理手工构造的 historical PK write request，并具备 recovery、cleanup、flow control 和 local-first lookup。

正常 client 仍不会把 expired partition write 重定向到 historical partition。这样可以先部署 server 能力，再在 PR 6 打开 client 行为。

## PR 5: Paimon Historical Tiering

### 目标

确保 `__historical__` WAL 中混合的多个 original partitions 被写回各自的 Paimon partition，并保持 bucket 对齐。该 PR 不启用 client fallback。

### 实现范围

当前 Paimon `RecordWriter` 在构造时根据 `WriterInitContext.partition()` 固定 Paimon partition。historical split 不能继续使用该假设，因为一个 `__historical__` bucket 中可以包含多个 original partitions。

修改 Paimon writer：

- 根据 `WriterInitContext.partition()` 判断当前 split 是否为 historical partition。
- normal split 继续在构造时解析并缓存 fixed Paimon partition。
- historical split 在每条 `LogRecord` 写入前设置 `FlussRecordAsPaimonRow`，然后从 row partition columns 提取 Paimon `BinaryRow` partition。
- 优先复用 `TableWriteImpl.getPartition()` 或现有 `PaimonConversions`，不重新维护一套 partition type conversion。
- append-only writer 使用动态 partition 和当前 Fluss bucket 调用 Paimon write。
- merge-tree writer 使用动态 partition、当前 bucket 和现有 primary key extractor。
- row 中的 partition columns 和 changelog type 不修改。
- bucket-unaware append-only table 继续遵守当前 bucket=0 规则。
- historical split 最终不能在 Paimon 中创建值为 `__historical__` 的 partition。

Arrow batch path：

- `TieringSplitReader`继续使用`pollRecordBatch()`批量读取historical Arrow data。
- normal writer继续使用`AppendOnlyArrowBatchHelper`的direct `writeBundle()` fast path。
- historical writer复用helper生成的`ArrowBundleRecords`，逐行提取actual Paimon partition并调用row write。
- 第一版不按partition对batch进行group/slice，也不实现multi-partition direct bundle。
- 继续复用现有batch truncate、stopping offset、tiered offset和max timestamp语义。

完成和 commit 语义：

- 同一个historical bucket writer会生成多个Paimon partitions的commit messages，不能继续假设`commitMessages.size() == 1`。
- 调整`PaimonWriteResult`、serializer和committer聚合逻辑，保存并提交完整message list。
- serializer继续使用version 1，但把payload直接替换为list layout；不提供旧version 1 singleton payload兼容。
- 调整只限定在 historical multi-partition writer 必需的范围，normal writer 行为保持不变。

### 测试要求

- `PaimonTieringTest` 覆盖 normal partition writer 仍写入 fixed partition。
- 覆盖一个 historical bucket 中两个 original partitions 写入两个 Paimon partitions。
- 覆盖 append-only 和 primary-key Paimon table。
- 覆盖不同 original partitions 中相同 primary key 不发生跨 partition merge。
- 覆盖 Paimon metadata 中不存在 `__historical__` partition。
- 覆盖 multi-partition writer 的 complete/commit message 聚合。
- 覆盖historical ARROW log table继续batch fetch、在Paimon writer内逐行写，并保持offset/timestamp正确。
- 覆盖 normal ARROW log table 继续使用 batch fast path。
- `PaimonTieringITCase` 验证 historical WAL tiering 后可以从原始 Paimon partition 读到数据。

### 合并后的行为边界

合并后，tiering service 可以正确处理 historical split 中的多 original partition 数据，但正常 client 仍不会主动产生 historical write。

该 PR 不修改 server historical KV、client eligibility 或 write routing。

## PR 6: Enable End-to-End Historical Write

### 目标

复用前面 PR 的协议、server 和 tiering 能力，在 client 侧启用 append-only 和 primary-key table 的 expired partition write fallback，并完成端到端验证。

### 实现范围

共享 historical partition resolver：

- 将 lookup package 中的 `HistoricalPartitionResolver` 移到 lookup/write 可共享的位置，或者提取 shared resolver 并保留 lookup adapter。
- resolver 继续使用 `ConcurrentHashMap<HistoricalPartitionKey, CompletableFuture<Long>>` 合并并发 resolve/create。
- failed future 从 map 移除，便于下一次 write 重试。
- resolver 不读取 `dynamicPartitionEnabled`；historical partition 是 system partition。
- lookup 的现有行为和测试保持不变。

新增 `ResolvedWriteTarget`，至少包含：

- 实际写入的 `PhysicalTablePath`。
- 是否为 historical write。
- `@Nullable String originalPartitionName`。

修改 `DynamicPartitionCreator` 或相邻 write resolver，使 missing partition 按以下顺序处理：

1. 检查 metadata cache 中是否存在 original partition；存在时返回原始 target。
2. 强制刷新 partition metadata；刷新后存在时返回原始 target。
3. 严格解析 original partition name。
4. 检查表是否 auto-partitioned。
5. 检查表是否 Paimon lake-enabled。
6. 检查 original partition 是否早于 retention boundary。
7. 只有步骤 3 到 6 全部成立时，解析或创建对应 historical system partition，并返回 historical target。
8. 不满足 historical eligibility 时，继续执行当前 normal dynamic partition create/validation 行为，不能改变 retained/current partition 的正常创建语义。
9. normal path 最终失败时，保持当前异常类型和错误信息，不把 malformed、future、non-lake partition 错误路由到 historical partition。

write target 应在 bucket assignment 和 accumulator append 之前解析完成：

- `WriterClient.doSend()` 使用 resolved physical target 获取 bucket assigner。
- 有 bucket key 的 log/PK table 继续按现有 bucket key 计算 bucket。
- 无 bucket key 的 log table 对 historical physical partition 使用现有 sticky bucket 行为。
- `WriteRecord` 中的 row 保持不变，因此 row partition columns 仍表示 original partition。
- historical PK `WriteRecord` 设置 original partition name，后续通过 PR 1 的 batching 和 RPC 字段发送。
- historical log `WriteRecord` 不需要 RPC partition field，只重写 physical target。
- normal write 的 `originalPartitionName` 保持 null。

client retry：

- historical partition create 的 coordinator transient error 允许当前 write 按现有策略失败或重试。
- concurrent create 的 already-exists 收敛为成功并刷新 metadata。
- server 返回 `HISTORICAL_PARTITION_THROTTLED` 时，使用 PR 4 的 exponential backoff。
- metadata stale、not-leader 和 replica movement 继续复用当前 write retry/metadata refresh。
- historical batch 与 normal batch 的 retry state 隔离。

端到端行为：

- append-only late row 写入 historical physical partition，row 保留 original partition columns。
- consumer 可以通过普通 partition discovery 发现并消费 `__historical__` buckets。
- PK late upsert/delete 进入 historical KV pipeline。
- PK update 可以从 Paimon 获取 old value，并生成正确 changelog。
- tiering 后 append-only 和 PK 数据都进入原始 Paimon partition。

### 测试要求

client focused tests：

- original partition 在 cache 中存在时走普通 target。
- cache miss、refresh 后存在时仍走普通 target。
- expired、metadata missing、Paimon lake-enabled auto partition 路由到 historical target。
- malformed、current、future、non-auto-partition、non-lake、non-Paimon partition 不会 historical fallback。
- `dynamicPartitionEnabled=false` 时 eligible historical write 仍可创建 system partition。
- retained/current partition 的 normal dynamic create 行为保持不变。
- 多 partition keys 根据 static prefix 路由到不同 historical partitions。
- 并发 write 只触发一次 historical resolve/create。
- append-only sticky/bucket-key assignment 与普通规则一致。
- PK batch 携带 original partition name，log batch 不携带。

端到端集成测试：

- append-only table 写 expired partition 后，`listPartitions` 包含 `__historical__`。
- 从 historical bucket 消费到 row，partition columns 等于 original partition。
- append-only historical data tier 到原始 Paimon partition。
- PK historical insert 在 lake 无旧值时成功。
- PK historical update 从 lake 读取 old value，产生 `UPDATE_BEFORE + UPDATE_AFTER`。
- PK key-only delete 写 tombstone，立即 lookup 和 tiering 后 lookup 都不会返回旧值。
- 两个 original partitions 映射到同一 historical bucket 时，batch、KV state 和 lookup 互不污染。
- 多级 partition 的不同 static prefixes 使用不同 historical partitions。
- TabletServer restart 后 historical write/lookup 正确。
- leader failover 后 historical state 通过 WAL replay 恢复。
- tiering 追平后 historical RocksDB 被清理，后续 lookup 从 lake 返回相同结果。
- normal append/upsert/delete/lookup/tiering regression tests 继续通过。

### 合并后的行为边界

合并后，eligible expired auto partition 的 append-only 和 primary-key write 可以完整经过 client redirect、historical WAL/KV、Paimon tiering 和 recovery 路径。

仍保留的第一版边界：

- client/server 通过 partition name 和 retention 规则判断 expired，不证明该 original partition 曾经存在。
- historical changelog 是独立的 supplemental stream，不承诺与已经过期的 original partition offset 连续。
- historical Arrow log tiering保留batch fetch、在Paimon writer内逐行写；按partition group/slice的direct-bundle优化留给后续PR。
- historical RocksDB 采用整 bucket drop，持续不断的 late write 可能延迟清理。

## 推荐合并顺序

1. PR 1: PutKv RPC + Client Write Batching Plumbing
2. PR 2: Historical KV Storage Primitives
3. PR 3: Historical PK Write Processor
4. PR 4: Server Dispatch + Flow Control + Recovery + Cleanup
5. PR 5: Paimon Historical Tiering
6. PR 6: Enable End-to-End Historical Write

依赖关系：

- PR 1 和 PR 2 基本独立，可以并行开发。
- PR 3 依赖 PR 1、PR 2 和已经合并的 lake lookup SPI。
- PR 4 依赖 PR 3。
- PR 5 只依赖 historical partition 公共语义，可以和 PR 2 到 PR 4 并行开发。
- PR 6 依赖 PR 1 到 PR 5，并且是唯一主动改变 client expired partition write 行为的 PR。

## 验证建议

每个 PR 先跑 focused tests，再跑受影响模块：

```bash
./mvnw test -pl fluss-client -Dtest=RecordAccumulatorTest,SenderTest,ClientRpcMessageUtilsTest
./mvnw test -pl fluss-server -Dtest='*Historical*Kv*Test,*Historical*Write*Test'
./mvnw test -pl fluss-server -Dtest=KvTabletTest,KvTabletMergeModeTest
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=PaimonTieringTest
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest='*Historical*Write*ITCase'
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server,fluss-lake/fluss-lake-paimon
./mvnw verify -pl fluss-flink/fluss-flink-common
./mvnw spotless:check
```

如果包含 proto 变更，先生成代码：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

本文档本身只新增设计文档，不需要运行 Maven 测试。
