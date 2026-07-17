# FIP-28 Historical Partition Lookup PR 4 实施计划

## 目标

PR 4 把 PR 1 到 PR 3 已经落地的能力串起来，真正启用主键表过期分区到 Paimon lake storage 的端到端 historical lookup。

该 PR 合并后，用户对已经从 Fluss metadata 中删除、但按 auto partition retention 判断为 expired 的原始分区做 primary-key lookup 时，client 会把请求路由到 `__historical__` system partition，server 会从 Paimon lake table 做 point lookup 并返回 Fluss KV value bytes。

## 前置依赖

该计划默认以下能力已经存在：

- PR 1 已提供 historical partition 公共语义：
  - `PartitionUtils.HISTORICAL_PARTITION_VALUE`
  - `PartitionUtils.isHistoricalPartitionName(...)`
  - `PartitionUtils.isHistoricalLookupCandidatePartition(...)`
  - `PartitionUtils.validateHistoricalPartitionSpec(...)`
  - coordinator 已允许合法创建 `__historical__` system partition。
- PR 2 已完成 lookup RPC 和 client plumbing：
  - `PbLookupReqForBucket.partition_name`
  - `LookupQuery.partitionName`
  - `LookupBatchKey(TableBucket, partitionName)`
  - `LookupSender` 按 `(TableBucket, partitionName)` 分组并避免同一 RPC 中出现重复 `TableBucket`。
- PR 3 已提供 lake lookup SPI 和 Paimon 实现：
  - `LakeTableLookuper`
  - `LakeStorage#createLakeTableLookuper(TablePath, LookuperContext)`
  - `PaimonLakeTableLookuper`
  - `ConfigOptions.IO_TMP_DIR`

## 当前代码状态

Client 侧：

- `PrimaryKeyLookuper` 当前在 `PartitionNotExistException` 后直接返回 empty result。
- `LookupClient.lookup(..., @Nullable String partitionName)` 已能携带 original partition name。
- `LookupSender` 已能把 `partitionName` 写入 `PbLookupReqForBucket`。
- `KeyEncoder.ofPrimaryKeyEncoder(...)` 对 kv format v2 且非默认 bucket key 的 lake table 会返回 Fluss compacted encoder；historical lookup 不能复用它作为 Paimon lookup key encoder。

Server 侧：

- `ServerRpcMessageUtils.toLookupData(...)` 当前返回 `Map<TableBucket, List<byte[]>>`，会丢掉 `partition_name`。
- `TabletService.lookup(...)` 当前只把 lookup data 交给 `ReplicaManager.lookups(...)`。
- `ReplicaManager.lookups(...)` 当前只从本地 KV replica 查，不识别 historical bucket。
- `TabletServerMetadataCache` 已能通过 table id、partition id 找到 `TablePath` 和 `PhysicalTablePath`。
- `ReplicaManager` 当前没有 lake storage/plugin lifecycle，需要在 PR 4 补齐。

## 非目标

PR 4 不做以下事情：

- 不支持 historical write。
- 不支持 `insertIfNotExists` 的 historical lookup。
- 不支持 prefix historical lookup。
- 不支持 Iceberg、Hudi、Lance historical lookup。
- 不做 historical lookup 流控、quota、metrics；PR 5 只先补流控，metrics 另起后续。
- 不刷新 Paimon lake snapshot。第一版仍采用 PR 3 的保守策略：lookup 初始化后不 refresh 新 snapshot。
- 不证明 original partition 曾经存在。MVP 只要求 original partition 当前 missing，且按规则是 historical lookup candidate。

## Step 1: 增加 HistoricalPartitionResolver

新增 client 侧类：

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/HistoricalPartitionResolver.java`

职责：

- 输入 `TableInfo` 和 original partition name。
- 计算对应的 historical system partition spec。
- 确保 historical partition 已在 coordinator metadata 中创建。
- 返回 historical partition id。
- 合并同一个 table/original partition 的并发 resolve。

推荐字段：

```java
private final MetadataUpdater metadataUpdater;
private final Admin admin;
private final ConcurrentHashMap<HistoricalPartitionKey, CompletableFuture<Long>> inflightResolves;
```

推荐新增 package-private key：

```java
final class HistoricalPartitionKey {
    private final long tableId;
    private final TablePath tablePath;
    private final String originalPartitionName;
}
```

resolve 流程：

1. 解析 original partition name：
   - `ResolvedPartitionSpec.fromPartitionName(tableInfo.getPartitionKeys(), originalPartitionName)`
2. 找到 auto partition key index：
   - `PartitionUtils.getAutoPartitionKeyIndex(...)`
3. 构造 historical partition spec：
   - 复制 original partition values。
   - 只把 auto partition key 对应的 value 替换为 `HISTORICAL_PARTITION_VALUE`。
   - 多分区键表保留非 auto partition key 的原值。
4. 先查 metadata cache 中是否已有 historical partition id。
5. cache miss 时调用 `metadataUpdater.updatePhysicalTableMetadata(...)` 刷新 historical partition metadata。
6. 仍 miss 时调用：

   ```java
   admin.createPartition(tablePath, historicalSpec.toPartitionSpec(), true)
   ```

7. create 成功或 already exists 后，再刷新 metadata。
8. 从 metadata cache 取 historical partition id 并完成 future。
9. future 失败时从 `inflightResolves` 移除，允许下一次 lookup 重试。
10. future 成功后也可以从 `inflightResolves` 移除；partition id 已经在 metadata cache 中，后续 lookup 走 cache 即可。

如果 PR 1 没有提供 `toHistoricalPartitionSpec(TableInfo, originalPartitionName)` helper，PR 4 可以在 `PartitionUtils` 中补一个小 helper，避免 resolver 自己长期持有 partition value 替换逻辑。

## Step 2: 把 resolver 接入 TableLookup 创建链路

修改：

- `fluss-client/src/main/java/org/apache/fluss/client/table/FlussTable.java`
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/TableLookup.java`
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/PrimaryKeyLookuper.java`

接线方式：

- `FlussTable.newLookup()` 把 `conn.getAdmin()` 传给 `TableLookup`。
- `TableLookup` 持有 `Admin` 或直接持有 `HistoricalPartitionResolver`。
- 只有创建 primary-key lookuper 时需要 resolver。
- prefix lookup 不接入 resolver。

推荐在 `TableLookup#createLookuper()` 中为 primary-key path 创建：

```java
HistoricalPartitionResolver resolver =
        new HistoricalPartitionResolver(metadataUpdater, admin);
```

也可以在 `TableLookup` 构造时创建并复用；保持 lookup 实例级别即可，不需要全 connection 全局单例。

## Step 3: 修改 PrimaryKeyLookuper 路由

修改：

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/PrimaryKeyLookuper.java`

新增字段：

- `HistoricalPartitionResolver historicalPartitionResolver`
- `KeyEncoder paimonPrimaryKeyEncoder`

`paimonPrimaryKeyEncoder` 只用于 historical lookup。它必须按 Paimon BinaryRow 编码 primary key，不能复用普通 `primaryKeyEncoder`：

- 对 kv format v1/default bucket key，普通 encoder 可能已经是 Paimon encoder。
- 对 kv format v2 且非默认 bucket key，普通 encoder 是 Fluss compacted encoder。
- Paimon lake lookup 必须收到 Paimon-encoded primary key bytes。

推荐直接使用：

```java
new PaimonKeyEncoder(lookupRowType, tableInfo.getPhysicalPrimaryKeys())
```

lookup 流程调整：

1. 编码 normal primary key bytes：
   - `primaryKeyEncoder.encodeKey(lookupKey)`
2. 编码 bucket key bytes：
   - 继续使用 `bucketKeyEncoder`。
   - bucket id 必须与 Paimon lake bucket 对齐。
3. 如果表未分区：
   - 维持当前普通 lookup 路径。
4. 如果表分区：
   - 先通过 `partitionGetter.getPartition(lookupKey)` 取 original partition name。
   - 尝试按普通路径解析 partition id。
5. 普通 partition id 解析成功：
   - 维持当前普通 lookup 路径。
   - `partitionName` 传 `null`。
6. 捕获 `PartitionNotExistException`：
   - 如果 `insertIfNotExists=true`，返回明确 unsupported 错误。
   - 调用 `PartitionUtils.isHistoricalLookupCandidatePartition(tableInfo, originalPartitionName, Instant.now())`。
   - 如果不是 candidate，保持旧行为，返回 empty result。
   - 如果是 candidate，进入 historical path。
7. historical path：
   - 用 resolver 获取 historical partition id。
   - 计算 bucket id。
   - 用 `paimonPrimaryKeyEncoder` 编码 lookup key。
   - 发送到 `TableBucket(tableId, historicalPartitionId, bucketId)`。
   - 调用 `lookupClient.lookup(..., false, originalPartitionName)`。
   - response handling 仍复用 `handleLookupResponse(...)`。

注意：

- 不要在 client 侧验证 original partition 是否曾经存在。
- 不要读取 `dynamicPartitionEnabled`；historical lookup 与 dynamic partition 写入创建开关无关。
- 非 candidate missing partition 继续返回 empty，不能抛新错误改变旧行为。

## Step 4: 保留 server 侧 partitionName

修改：

- `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java`
- `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`

新增 request holder，推荐放在 `fluss-rpc` entity 或 server 侧 entity 包中：

```java
public final class LookupDataForBucket {
    private final TableBucket tableBucket;
    private final List<byte[]> keys;
    private final @Nullable String partitionName;
}
```

调整 `ServerRpcMessageUtils.toLookupData(...)`：

- 不能继续只返回 `Map<TableBucket, List<byte[]>>`。
- 必须把 `PbLookupReqForBucket.partition_name` 保留下来。
- 推荐返回 `List<LookupDataForBucket>`。
- 如果同一个 RPC 中出现重复 `TableBucket`，server 可以直接返回 bucket-level error，避免 response 只按 `TableBucket` 标识时产生歧义。

`TabletService.lookup(...)` 调整：

- 普通 lookup 继续支持旧请求。
- `insertIfNotExists=true` 时，如果任一 bucket request 携带 `partitionName`，直接返回明确错误。
- authorization 仍以 `TableBucket` 为单位做 READ/WRITE 校验。
- 校验通过后，把 `List<LookupDataForBucket>` 交给新的 `ReplicaManager.lookups(...)` overload。

## Step 5: 在 ReplicaManager 中区分普通 lookup 和 historical lookup

修改：

- `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

新增 overload：

```java
public void lookups(
        List<LookupDataForBucket> lookupData,
        short apiVersion,
        Consumer<Map<TableBucket, LookupResultForBucket>> responseCallback)
```

处理逻辑：

1. 按 `partitionName == null` 拆成 normal lookup 和 historical lookup。
2. normal lookup：
   - 转回 `Map<TableBucket, List<byte[]>>`。
   - 复用现有 local KV lookup 逻辑。
3. historical lookup：
   - 交给 `HistoricalLakeLookupManager`。
   - 使用 `ioExecutor` 执行 lake IO。
4. mixed request：
   - normal 和 historical 的结果合并成同一个 `Map<TableBucket, LookupResultForBucket>`。
   - 等 historical futures 完成后再调用 response callback。

第一版不要把 historical lookup 接入 `insertIfNotExists` 的二阶段插入逻辑。只允许普通 local KV lookup 使用现有 `insertIfNotExists` path。

## Step 6: 新增 HistoricalLakeLookupManager

新增 server 侧类，推荐位置：

- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalLakeLookupManager.java`

职责：

- 校验 historical lookup request。
- 创建并缓存 `LakeTableLookuper`。
- 调用 lake lookuper。
- 保持每个 bucket 中 key 的返回顺序。
- shutdown 时关闭所有 cached lookuper。

构造依赖：

- `Configuration conf`
- `PluginManager pluginManager`
- `TabletServerMetadataCache metadataCache`
- `ExecutorService ioExecutor`

由于 `ReplicaManager` 当前没有 `pluginManager`，PR 4 需要修改 `TabletServer` 创建 `ReplicaManager` 的接线，把 `pluginManager` 传入 `ReplicaManager` 或直接传入 `HistoricalLakeLookupManager`。

lake storage 创建：

1. 只支持 `DataLakeFormat.PAIMON`。
2. 从 cluster conf 中提取 lake properties：

   ```java
   LakeStorageUtils.extractLakeProperties(conf)
   ```

3. 通过 `LakeStoragePluginSetUp.fromDataLakeFormat(...)` 加载 plugin。
4. 创建 `LakeStorage`。
5. 创建 lookuper 时传入：

   ```java
   new LakeStorage.LookuperContext(conf.get(ConfigOptions.IO_TMP_DIR))
   ```

lookuper cache：

- 推荐 key：`TablePath` 或 `(tableId, TablePath)`。
- value：`LakeTableLookuper`。
- 用 `ConcurrentHashMap` lazy create。
- create 失败时不要缓存失败对象。
- `close()` 遍历关闭所有 lookuper。

## Step 7: Server 侧 historical validation

`HistoricalLakeLookupManager` 对每个 `LookupDataForBucket` 执行以下校验：

1. request 必须携带 `partitionName`。
2. `tableBucket.getPartitionId()` 必须非 null。
3. 通过 `metadataCache.getTablePath(tableId)` 找到 base `TablePath`。
4. 通过 `metadataCache.getPhysicalTablePath(partitionId)` 找到目标 partition path。
5. 通过 `metadataCache.getTableMetadata(tablePath)` 获取最新 `TableInfo`。
6. 目标 partition name 必须满足：

   ```java
   PartitionUtils.isHistoricalPartitionName(tableInfo, targetPartitionName)
   ```

7. request 中的 original partition name 必须能按 `tableInfo.getPartitionKeys()` 解析。
8. original partition name 必须满足：

   ```java
   PartitionUtils.isHistoricalLookupCandidatePartition(tableInfo, originalPartitionName, Instant.now())
   ```

9. 表必须是主键表。
10. 表必须是 Paimon lake-enabled auto-partitioned 表。
11. 如果普通 partition bucket 携带 `partitionName`，返回 bucket-level error，不能 fallback 到 local KV。
12. 如果 historical bucket 缺少 `partitionName`，返回 bucket-level error。

validation 成功后构造：

```java
LakeTableLookuper.LookupContext context =
        new LakeTableLookuper.LookupContext(
                ResolvedPartitionSpec.fromPartitionName(
                        tableInfo.getPartitionKeys(), originalPartitionName),
                tableBucket.getBucket(),
                (short) tableInfo.getSchemaInfo().getSchemaId(),
                tableInfo.getRowType());
```

然后按 input keys 顺序调用：

```java
lookuper.lookup(key, context)
```

返回 `LookupResultForBucket(tableBucket, values)`。

异常处理：

- validation failure 转成 bucket-level `ApiError`。
- lake lookup 抛异常也转成 bucket-level `ApiError`。
- 不要让单个 historical bucket 失败导致同一 request 中其他 bucket 无响应。

## Step 8: 生命周期和资源释放

修改：

- `ReplicaManager.shutdown()`

新增：

- `historicalLakeLookupManager.close()`。

要求：

- close 幂等。
- 关闭所有 cached `LakeTableLookuper`。
- 关闭异常记录日志，不影响 ReplicaManager 继续 shutdown。

如果 `ReplicaManager` 构造中创建 `HistoricalLakeLookupManager`，测试构造器也要同步补参数或提供 testing constructor 默认值。

## Step 9: Client 单测

建议新增：

- `fluss-client/src/test/java/org/apache/fluss/client/lookup/HistoricalPartitionResolverTest.java`
- `fluss-client/src/test/java/org/apache/fluss/client/lookup/PrimaryKeyLookuperTest.java`

覆盖：

- historical spec 构造：
  - 单分区键：`20240101 -> __historical__`
  - 多分区键：`region1$20240101 -> region1$__historical__`
- resolver 合并同一 original partition 的并发 resolve。
- create partition 失败后移除 in-flight future。
- 普通 partition lookup 仍走普通 partition id，不携带 `partitionName`。
- missing 但非 expired partition 仍返回 empty。
- expired partition 路由到 historical partition id，并携带 original partition name。
- `insertIfNotExists` 遇到 historical candidate 时返回明确 unsupported。
- kv format v2 且非默认 bucket key 时，historical lookup 发送 Paimon-encoded primary key，而不是 Fluss compacted primary key。

`LookupSenderTest` 已覆盖 PR2 batching 行为，PR4 只在新增 client routing 后补必要断言，不重复测试 sender 内部 packing。

## Step 10: Server 单测

建议新增或扩展：

- `fluss-server/src/test/java/org/apache/fluss/server/utils/ServerRpcMessageUtilsTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalLakeLookupManagerTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/replica/ReplicaManagerTest.java`
- `fluss-server/src/test/java/org/apache/fluss/server/tablet/TabletServiceITCase.java`

覆盖：

- `toLookupData` 保留 `partitionName`。
- 普通 partition bucket 携带 `partitionName` 时拒绝。
- historical bucket 缺少 `partitionName` 时拒绝。
- original partition name malformed 时拒绝。
- original partition 当前未 expired 时拒绝。
- 非 Paimon lake table 拒绝。
- 非主键表拒绝。
- 合法 historical lookup 调用 lake lookuper。
- 多 key lookup 保持返回顺序。
- mixed normal + historical lookup 在一个 request 中都能返回。
- `insertIfNotExists=true` 携带 `partitionName` 时拒绝。
- `ReplicaManager.shutdown()` 会关闭 cached lookuper。

测试中可以使用 fake `LakeTableLookuper`，避免 server unit test 依赖 Paimon。

## Step 11: Paimon 端到端测试

建议放在 Paimon lake module 中，因为该模块已有 Paimon 测试依赖：

- `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/lookup/HistoricalPartitionLookupITCase.java`

场景：

1. 启动 Fluss cluster，配置 Paimon lake。
2. 创建 auto-partitioned、Paimon lake-enabled 主键表。
3. 写入一个会进入 Paimon 的历史分区数据。
4. 等待/触发 tiering commit，确保 Paimon 中已有该行。
5. 让 original partition 按 retention 规则 expired，并从 Fluss metadata 中删除。
6. 对包含 original partition value 的 primary key 做 lookup。
7. 断言返回值等于写入行。
8. 断言 `listPartitionInfos(tablePath)` 包含 `__historical__` partition。
9. 断言普通 retained partition lookup 行为不变。

如果现有测试很难稳定等待 tiering，可以复用已有 lake tiering helper；不要在 PR4 中引入 sleep 型等待。

## Step 12: 验证命令

建议最小验证：

```bash
./mvnw test -pl fluss-client -Dtest=HistoricalPartitionResolverTest,PrimaryKeyLookuperTest
./mvnw test -pl fluss-server -Dtest=ServerRpcMessageUtilsTest,HistoricalLakeLookupManagerTest,ReplicaManagerTest
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=HistoricalPartitionLookupITCase
./mvnw spotless:check -pl fluss-client,fluss-server,fluss-lake/fluss-lake-paimon
```

如果修改了 RPC/proto generated code，需要额外跑：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

PR4 正常情况下不应再改 proto。

## 风险和注意事项

- Response 仍按 `TableBucket` 标识，因此 server 和 client 都要避免同一个 request 中出现重复 `TableBucket`。
- Client 必须发送 original partition name，server 不能信任该字段，必须重新解析和校验。
- Historical lookup 使用 Paimon key bytes；如果误用 Fluss compacted key bytes，会出现稳定 lookup miss。
- `insertIfNotExists` 不能进入 historical path，否则会引入 historical write 语义。
- `LakeTableLookuper` 缓存要随 `ReplicaManager` shutdown 关闭，否则 Paimon `IOManager` 临时目录会延迟清理。
- 第一版不 refresh Paimon snapshot，因此不支持在 lookuper 初始化后继续写入同一 historical partition 并立刻 lookup 到新数据；当前系统也不支持写 historical partition。

## 合并后的行为边界

合并后：

- primary-key lookup 对 expired auto partition 可以端到端从 Paimon 返回结果。
- 非 expired missing partition 仍返回 empty。
- 普通 partition lookup 行为不变。
- historical system partition 会在首次 eligible lookup 时按需创建。
- server 会拒绝非法或伪造的 historical lookup request。

仍不支持：

- historical write。
- `insertIfNotExists` historical lookup。
- prefix historical lookup。
- 非 Paimon lake format historical lookup。
- historical lookup 流控；metrics 另起后续。
