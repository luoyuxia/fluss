# FIP-28 历史分区点查 PR 拆分计划

## 目标

本文档把 FIP-28 historical partition lookup 的实现拆成 5 个可独立 review 和合并的 PR。拆分目标是先建立公共语义和协议字段，再接入 lake lookup 能力，最后打开端到端历史点查，并把流控作为独立收尾 PR。

本计划只覆盖 historical partition lookup，不覆盖 historical write path。

## 总体边界

第一版支持的能力：

- 只支持主键表 point lookup。
- 只支持 auto-partitioned、Paimon lake-enabled 表。
- 只支持原始分区已经从 Fluss metadata 中缺失、且按 auto partition retention 规则判断为 expired 的分区。
- lookup 请求仍路由到 `__historical__` system partition 的 bucket。
- server 侧通过 Paimon lake storage 做 point lookup。

第一版不支持的能力：

- historical write。
- `insertIfNotExists` 的 historical lookup。
- historical RocksDB、prewrite buffer、WAL replay。
- prefix lookup。
- Iceberg、Lance、Hudi historical lookup。

关键假设：

- PR 2 只做 RPC 和 client plumbing，不启用 historical fallback。
- PR 4 才真正打开 end-to-end historical lookup。
- MVP 接受不校验 original partition 当前是否存在，也不校验 original partition 是否曾经存在。
- historical lookup 流控作为 PR 5 单独实现。

## PR 1: Common + Coordinator Historical Partition

### 目标

建立 historical partition 的公共判断规则，并允许 coordinator 通过现有 `createPartition` RPC 创建合法的 `__historical__` system partition。

### 实现范围

在 `fluss-common` 增加 historical partition 公共工具，优先放在 `PartitionUtils` 或相邻工具类中：

- 增加 `HISTORICAL_PARTITION_VALUE = "__historical__"`。
- 增加 `isHistoricalPartitionName(TableInfo tableInfo, String partitionName)`。
- 增加 `toHistoricalPartitionSpec(TableInfo tableInfo, String originalPartitionName)`。
- 增加 `isExpiredAutoPartition(TableInfo tableInfo, String partitionName, Instant now)`。
- 增加 `getAutoPartitionKeyIndex(TableInfo tableInfo)`。

`isExpiredAutoPartition` 必须按规则逐步判断：

1. 检查表是 partitioned 且启用了 auto partition。
2. 检查表启用了 data lake。
3. 检查 lake format 是 `DataLakeFormat.PAIMON`。
4. 按 `tableInfo.getPartitionKeys()` 严格解析 `partitionName`。
5. 定位 auto partition key：配置了 `autoPartitionStrategy.key()` 时使用该 key，否则使用第一个 partition key。
6. 取出 auto partition key 对应的值。
7. 检查该值符合 auto partition time format。
8. 按配置时区和 retention count 计算最早保留分区值。
9. 只有当最早保留分区值按字符串顺序大于 auto partition value 时，才判定该分区在时间维度上 expired。

在 coordinator `createPartition` 中增加 historical system partition 分支：

- 普通 partition create 行为保持不变，仍需要 WRITE 权限。
- historical system partition create 使用 READ 权限授权。
- server 根据 table metadata 和 partition spec 识别 historical request，不依赖 client 传入 flag。
- historical spec 必须包含完整 partition keys，不能缺 key 或多 key。
- 只有 auto partition key 的值可以是 `__historical__`。
- 非 auto partition values 仍按普通 partition value 规则校验。
- 表必须 auto-partitioned、启用 lake，且 lake format 是 Paimon。
- historical create 跳过 `validateAutoPartitionTime`。
- 推荐要求 `ignoreIfExists=true`，并把并发 create 中的 already-exists 收敛为成功。

在 `AutoPartitionManager.dropPartitions` 中跳过 historical partition，避免 TTL 删除 `__historical__`。

### 测试要求

- `PartitionUtilsTest` 覆盖单分区键和多分区键。
- 覆盖合法 expired partition、非法 partition name、future/current retained partition。
- 覆盖 non-lake table 和 non-Paimon lake table。
- coordinator 测试覆盖普通 partition create 仍需要 WRITE 权限。
- coordinator 测试覆盖合法 historical system partition create 只需要 READ 权限。
- 覆盖 `__historical__` 出现在非 auto partition key 上时失败。
- 覆盖 missing、extra、malformed partition keys 失败且不写 metadata。
- 覆盖 `ignoreIfExists=true` 下并发创建 historical partition 的幂等行为。
- 覆盖 auto partition expiration 不会 drop `__historical__`。

### 合并后的行为边界

合并后，server 已经理解并能创建合法 historical system partition。普通 lookup 和 client lookup 路由仍不变，不会自动从 Paimon lake storage 回查过期分区。

MVP 在该 PR 中不增加 original partition 的 authoritative existence check。coordinator 只校验目标 historical spec 合法，不证明 original partition 当前不存在或曾经存在。

## PR 2: Lookup RPC + Client Plumbing

### 目标

为 historical lookup 搭好 RPC 字段和 client batching 数据结构，让后续 PR 可以安全携带 original partition name。该 PR 不启用 historical fallback。

### 实现范围

扩展 lookup RPC：

```protobuf
message PbLookupReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  repeated bytes keys = 3;
  optional string partition_name = 4;
}
```

重新生成 RPC classes：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

client lookup plumbing：

- 给 lookup query 增加 `@Nullable String partitionName`，该字段表示 original partition name。
- 新增 `LookupBatchKey`，包含 `TableBucket tableBucket` 和 `@Nullable String partitionName`。
- 普通 lookup 的 `partitionName` 为 null。
- 后续 historical lookup 按 `(historical table bucket, original partition name)` 分组。
- `LookupSender` dispatch 使用 `LookupBatchKey`，不能只按 `TableBucket`。
- 只有 batch key 的 `partitionName` 非 null 时，才设置 `PbLookupReqForBucket.partition_name`。
- 同一个 `PbLookupReqForBucket` 最多携带一个 original partition name。
- 同一个 `LookupRequest` 中不能混入相同 `TableBucket` 但不同 `partitionName` 的 bucket request；第一版直接拆成不同 RPC，避免 response 只按 `TableBucket` 标识时产生歧义。

可以同时添加 `HistoricalPartitionResolver` 的空接线或独立类骨架，但不能在 `PrimaryKeyLookuper` 中启用 expired partition fallback。

### 测试要求

- `LookupSenderTest` 验证同一个 historical `TableBucket` 下，不同 original partition name 会被拆成不同 RPC。
- `LookupSenderTest` 验证不同 `TableBucket` 的后续 historical lookup 可以 batch，且不丢 `partitionName`。
- `ClientRpcMessageUtilsTest` 验证只有非 null `partitionName` 的 batch 设置 `partition_name`。
- 普通 lookup 的现有 batching 和 response dispatch 测试继续通过。

### 合并后的行为边界

合并后，新 client 和新 server 之间可以传输 `partition_name`，但主键 lookup 遇到 missing partition 时仍保持旧行为，不会创建 `__historical__`，也不会查询 lake。

兼容性边界：

- 老 client 不发送 `partition_name`，新 server 按普通 lookup 处理。
- 新 client 只有在后续 PR 启用 historical path 后才会发送 `partition_name`。

## PR 3: Lake Lookup SPI + Paimon Implementation

### 目标

在 lake SPI 中增加表级 point lookup 能力，并提供 Paimon 实现。该 PR 提供能力，不把它接入在线 lookup 主路径。

### 实现范围

在 `fluss-common` lake SPI 中增加：

- `LakeTableLookuper` 接口。
- `LakeTableLookuper.LookupContext`，包含 partition spec、bucket id、schema id。
- `LakeStorage#createLakeTableLookuper(TablePath tablePath)` default method，默认抛出 `UnsupportedOperationException`。

在 `fluss-lake-paimon` 中实现 `PaimonLakeTableLookuper`：

- 使用 Paimon `FileStoreTable.newLocalTableQuery()`。
- 把 `LookupContext.partitionSpec` 转成 Paimon partition `BinaryRow`。
- 把传入 key bytes 包装为 Paimon lookup key row。server 不做 Fluss compacted key 到 Paimon key 的重新编码。
- lookup 前按 `(partition, bucket)` 刷新 Paimon files。
- 对 `DataFileMeta` 按 file name 去重后再调用 `LocalTableQuery.refreshFiles`。
- 调用 `LocalTableQuery.lookup(partition, bucketId, keyRow)`。
- Paimon 返回 null 时返回 null。
- Paimon 返回 row 时，按 Fluss value encoding 返回带 schema id prefix 的 value bytes。
- 用同步块或显式 lock 保护 Paimon query mutable state。
- `close()` 关闭 `LocalTableQuery` 和相关 catalog/table resources。

### 测试要求

- 针对 Paimon partition conversion 写 focused tests。
- 针对 value encoding 写 focused tests。
- 覆盖 missing row 返回 null。
- 覆盖同一 `(partition, bucket)` refresh files 去重。
- 覆盖 `close()` 可以重复调用或按项目约定安全关闭。

### 合并后的行为边界

合并后，Paimon lake storage 可以被 server 侧代码调用进行 point lookup。但 Fluss 在线 lookup 请求仍不会自动进入该 SPI，端到端 historical lookup 还未启用。

该 PR 不新增 historical lookup 流控，不修改 client lookup fallback 行为。

## PR 4: Enable End-to-End Historical Lookup

### 目标

把 PR 1 到 PR 3 的能力串起来，启用主键表 expired partition 到 Paimon lake storage 的端到端 historical lookup。

### 实现范围

client 侧：

- 增加 connection-scoped 或 lookup-client-scoped `HistoricalPartitionResolver`。
- resolver 输入 `TableInfo` 和 original partition name，计算 historical partition spec。
- resolver 先查 metadata cache，再刷新 partition metadata，仍缺失时调用：

```java
admin.createPartition(tablePath, historicalSpec.toPartitionSpec(), true)
```

- create 成功或 already-exists 后刷新 metadata 并返回 historical partition id。
- 用 `ConcurrentHashMap<HistoricalPartitionKey, CompletableFuture<Long>>` 合并同一 table/original partition 的 in-flight resolve。
- failed future 从 map 移除，便于下次重试。
- resolver 不读取 `dynamicPartitionEnabled`。

`PrimaryKeyLookuper` 路由修改：

- 普通 partition id 解析成功时保持现有路径。
- 捕获 `PartitionNotExistException` 后，先处理 `insertIfNotExists`。如果开启，返回明确 unsupported 错误。
- 用 `isExpiredAutoPartition(tableInfo, originalPartitionName, Instant.now())` 判断是否 eligible。
- 非 eligible missing partition 保持旧行为，返回 empty lookup result。
- eligible expired partition 通过 resolver 获取 historical partition id。
- bucket id 使用和 Paimon lake bucket 对齐的 bucket key bytes 计算。
- historical lookup 使用 Paimon lake primary key encoder，不复用可能输出 Fluss compacted encoding 的普通 `primaryKeyEncoder`。
- lookup query 发送到 `TableBucket(tableId, historicalPartitionId, bucketId)`，并携带 original partition name。

server 侧：

- `ServerRpcMessageUtils`、`TabletService`、`ReplicaManager` 保留 bucket-level `partitionName`。
- 新增 request holder，例如 `LookupDataForBucket`，包含 `TableBucket`、keys、`@Nullable partitionName`。
- 不能用 `Map<TableBucket, List<byte[]>>` 表示 historical lookup request，因为相同 historical bucket 可能对应不同 original partition name。
- 普通 lookup 仍走 local replica KV。
- historical lookup 必须验证目标 `TableBucket` 对应的 partition name 是合法 historical partition。
- 普通 partition bucket 携带 `partitionName` 时拒绝。
- historical partition bucket 缺少 `partitionName` 时拒绝。
- server 侧严格解析 original partition name，并重新执行 `isExpiredAutoPartition`。
- server 侧验证表是主键表、auto-partitioned、Paimon lake-enabled。
- validation failure 返回明确 bucket-level `ApiError`，不能 fallback 到普通 local lookup。
- lake lookup 提交到 tablet server 现有 `ioExecutor`。
- 对每个 key 调用 `LakeTableLookuper.lookup(key, context)`，保持结果顺序。
- `ReplicaManager` 或相邻 manager 缓存 `LakeTableLookuper`，并在 shutdown 时关闭。

集成测试：

- 创建 Paimon lake-enabled auto-partitioned 主键表。
- 写入当前合法分区的一行。
- 确认该行已 tier 到 Paimon，优先复用现有 helper。
- 让原始分区过期并从 metadata 删除。
- 对包含原始 partition value 的 key 做 lookup。
- 断言返回行等于写入行。
- 断言 `listPartitionInfos` 包含生成的 `__historical__` partition。

### 测试要求

- client 单测覆盖普通 partition lookup 不变。
- client 单测覆盖 missing 但非 expired partition 仍返回 empty。
- client 单测覆盖 expired partition 路由到 historical partition id。
- client 单测覆盖 original partition name 传到 `LookupClient`。
- client 单测覆盖 kv format v2 且非默认 bucket key 时，historical lookup 发送 Paimon-encoded key。
- server 单测覆盖普通 partition bucket 携带 `partitionName` 时拒绝。
- server 单测覆盖 historical bucket 缺少 `partitionName` 时拒绝。
- server 单测覆盖 original partition name malformed/current/future/not expired 时拒绝。
- server 单测覆盖合法 historical lookup 走 lake lookup 并保持结果顺序。
- Paimon historical lookup E2E test 通过。

### 合并后的行为边界

合并后，eligible expired auto partition 的主键 lookup 可以从 Paimon lake storage 返回结果。

仍保留的 MVP 边界：

- 不校验 original partition 当前是否存在。
- 不校验 original partition 是否曾经存在。
- stale client 理论上可能把仍存在但本地解析失败的 old partition 误路由到 lake。
- READ 用户理论上可以构造合法形态的 historical spec 并触发创建，例如多分区键表里的 `fake-prefix$__historical__`。
- historical lookup 只依赖现有 lookup retry 和 server `ioExecutor` 隔离，还没有独立流控。

## PR 5: Historical Lookup Flow Control

### 目标

限制 historical lookup 对普通 online lookup 的服务端资源影响。该 PR 在 PR 4 的端到端功能之上实现，第一版只做服务端准入流控和 throttle retry/backoff，不增加 client-side historical inflight ratio，也不新增 metrics。

### 实现范围

server 侧：

- 新增 `netty.server.max-queued-historical-requests` 配置，作为 historical lookup request 的准入上限。
- 在 `ReplicaManager` 或 `HistoricalPartitionLookupManager` 中增加 historical lookup semaphore，容量来自 `netty.server.max-queued-historical-requests`。
- historical lookup 进入 lake lookup 前先 `tryAcquire()`。
- 获取 permit 失败时不提交到 `ioExecutor`，直接返回明确 throttle `ApiError`。
- permit 粒度第一版按 bucket request 计算。
- historical lookup 成功、失败、取消路径都释放 permit。
- normal lookup 不经过 historical semaphore。

client 侧：

- 不新增 `client.lookup.historical-inflight-ratio`。
- 第一版不拆分 `LookupSender` 的 normal/historical inflight permits。
- 收到 historical throttle error 后，复用现有 retry/backoff；如现有路径不能表达延迟重试，只补最小的 historical throttle retry/backoff 处理。

配置建议：

- `netty.server.max-queued-historical-requests`

该配置与 `netty.server.max-queued-requests` 分开，避免通过 ratio 隐式推导 historical 容量。默认值可以先设为保守值，例如 50；推荐不超过 `netty.server.max-queued-requests`。

metrics 建议：

- 第一版暂不增加 dedicated historical lookup metrics。
- 继续复用现有 request/error 观测能力；如后续排查需要，再单独补 historical lookup total/throttled/latency/inflight metrics。

### 测试要求

- server semaphore 满时，historical lookup 返回 throttle error，不进入 `ioExecutor`。
- normal lookup 不受 historical semaphore 影响。
- historical lookup 成功、失败、取消路径都释放 permit。
- `netty.server.max-queued-historical-requests` 控制 historical semaphore 容量；显式配置能覆盖默认值。
- client 收到 historical throttle error 后会 retry/backoff。

### 合并后的行为边界

合并后，server 侧 historical lookup 超限会返回 throttle error，不再继续提交 lake lookup 工作；client 侧仍暂时共用现有 lookup inflight permits，不提供独立 historical ratio 配置。

该 PR 不改变 PR 4 的 eligibility 语义，也不补充 original partition existence check。后续如果要收紧 MVP 风险，可以单独引入 expired partition tombstone、drop registry、lake partition existence check 或 authoritative metadata check。

## 推荐合并顺序

1. PR 1: Common + Coordinator Historical Partition
2. PR 2: Lookup RPC + Client Plumbing
3. PR 3: Lake Lookup SPI + Paimon Implementation
4. PR 4: Enable End-to-End Historical Lookup
5. PR 5: Historical Lookup Flow Control

PR 2 和 PR 3 在代码依赖上基本独立，可以并行开发。PR 4 依赖 PR 1、PR 2、PR 3。PR 5 依赖 PR 4。

## 验证建议

每个 PR 先跑 focused tests，再跑受影响模块：

```bash
./mvnw test -pl fluss-common -Dtest=PartitionUtilsTest
./mvnw test -pl fluss-client -Dtest=LookupSenderTest,ClientRpcMessageUtilsTest
./mvnw test -pl fluss-server -Dtest=AutoPartitionManagerTest
./mvnw test -pl fluss-server -Dtest='*Historical*Lookup*Test'
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest='*Paimon*Lookup*Test'
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server,fluss-lake/fluss-lake-paimon
./mvnw spotless:check
```

如果包含 proto 变更，先生成代码：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

本文档本身只新增设计文档，不需要运行 Maven 测试。
