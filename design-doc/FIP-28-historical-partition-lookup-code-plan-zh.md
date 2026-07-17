# FIP-28 历史分区点查能力代码计划

## 目标

本文档描述 FIP-28 的第一个实现里程碑：支持对 Paimon lake-enabled 主键表的过期自动分区做 point lookup。

端到端行为如下：

1. 用户用主键 lookup 查询一个原始分区，该分区已经不在 Fluss 当前 metadata 中。
2. client 判断这个缺失分区是否是目标表的过期自动分区。
3. client 计算对应的 `__historical__` 系统分区，并通过现有 `createPartition` RPC 懒创建。第一版为了简化实现，coordinator 暂时信任该 historical spec 来自一次失败的普通分区解析，但仍由 coordinator 自己校验表类型和 historical system partition spec 的合法性。
4. client 将 lookup 请求发往 `__historical__` 分区的目标 bucket，并在 lookup RPC 中携带原始分区名。
5. tablet server 验证请求确实是合法历史查询，然后用 Paimon `LocalTableQuery` 查询 lake storage。
6. server 按普通 KV lookup response 的编码格式返回结果。

## 范围

本里程碑支持：

- 只支持主键表 point lookup。
- 只支持 auto-partitioned、Paimon lake-enabled 表。
- 对 client 判断为已过期且已从 Fluss metadata 删除的原始分区，从 Paimon lake storage 回查。
- 受控创建或解析对应的 `__historical__` 系统分区，因为 partitioned table 的 lookup 路由仍然需要一个可路由的 Fluss partition id。
- server 侧 lake lookup 使用 tablet server 现有 `ioExecutor` 隔离。

本里程碑不支持：

- 对过期分区写入。
- log table 历史写入。
- historical RocksDB、prewrite buffer、WAL replay，以及 late write 的 local-first 查询链路。
- 过期分区 prefix lookup。
- 过期分区的 `insertIfNotExists`。
- Iceberg、Lance、Hudi 的 historical lookup。
- client-side historical inflight ratio 和新 backoff 策略。第一版先复用现有 lookup retry 机制和 server `ioExecutor` 隔离。

## 参考分支说明

`history-partition-support` 分支可以参考 lake/tiering 相关风格，但不要直接照搬：

- lake 格式相关逻辑放在 lake module 中。
- server 侧通过 common SPI 调用 lake 能力，不在 server 逻辑中硬编码 Paimon。
- 复用现有 Paimon scan、partition conversion、row adapter 工具。

## 关键设计决策

### lookup 目标仍然是 `__historical__`

即使本里程碑只读 lake，client 仍然需要一个合法 `TableBucket` 将请求路由到 tablet-server leader。对 partitioned table 来说，这意味着必须存在一个 partition id。所以 client 在发送 lookup 前需要解析对应的 `__historical__` partition id。

对多分区键表，只替换 auto-partition key 对应的值，其他静态分区值保持不变。例如：

```text
partition keys: [region, dt]
auto key: dt
original partition: us$20200101
historical partition: us$__historical__
```

### historical partition 创建复用 `createPartition` RPC

historical lookup 是读操作。为了避免新增 RPC，第一版复用现有 public `createPartition` RPC，但在 coordinator 的 `createPartition` 实现中增加一个 historical system partition 分支。

普通 partition create 仍然要求 WRITE 权限。只有当 server 识别出请求创建的是合法 `__historical__` system partition 时，才走 historical 分支并使用 READ 权限授权。这个分支不能依赖 client 传入 `isSystem=true` 之类标记，必须由 server 根据 partition spec 自己判断。

第一版为了降低实现复杂度，client 负责根据 original partition name 计算 historical partition spec，并调用：

```java
admin.createPartition(tablePath, historicalSpec.toPartitionSpec(), true)
```

coordinator 暂时信任这个 historical spec 来自一次失败的普通 partition id resolution。也就是说，coordinator 不额外证明 original partition 当前不存在，也不证明它曾经存在过。

这个信任模型必须保留几个基本边界：

- ordinary partition create path 不变，仍然需要 WRITE 权限；
- historical 分支只对 server 识别出的 `__historical__` system partition 生效；
- historical 分支使用 READ 权限授权；
- historical 分支重新加载 table metadata；
- historical 分支校验表是 auto-partitioned；
- historical 分支校验表是 Paimon lake-enabled；
- historical 分支校验 partition spec 包含完整 partition keys；
- historical 分支校验只有 auto-partition key 的值是 `__historical__`；
- historical 分支校验非 auto partition values 符合普通 partition value 规则；
- historical 分支跳过 `validateAutoPartitionTime`；
- 推荐要求 `ignoreIfExists=true`，因为该路径用于 lookup 懒创建并需要处理并发。

这个取舍的风险是：多分区键表中，READ 用户理论上可以直接构造一个合法形态的 historical spec，例如 `fake-region$__historical__`，并通过 public `createPartition` 的 historical 分支创建它。第一版接受这个风险，后续可以通过 expired partition tombstone、drop registry、lake partition existence check 或 TTL drop 时预创建 historical partition 来收紧。

### 本里程碑不实现本地 historical state

FIP 最终 lookup 链路是：

```text
prewrite buffer -> historical RocksDB -> lake
```

本里程碑没有 historical write path，所以没有本地 historical state 可以查询。server 侧先实现为：

```text
historical lookup request -> lake lookup
```

后续实现 historical write 时，可以替换 server 内部查询链路，但 RPC 字段和 client 路由规则保持不变。

### `insertIfNotExists` 对过期分区不支持

`insertIfNotExists` 会把一次 missing lookup 变成写入。支持它需要 historical write path。本里程碑中，如果 lookup 被路由到 historical 且开启了 `insertIfNotExists`，应直接以明确异常失败，例如 `UnsupportedOperationException` 或 Fluss API exception。

## Step 1: 公共 historical partition 工具

在 `fluss-common` 增加公共工具，优先放在 `PartitionUtils`，或者放在相邻的小工具类中。

需要常量：

```java
public static final String HISTORICAL_PARTITION_VALUE = "__historical__";
```

需要 helper：

- `boolean isHistoricalPartitionName(TableInfo tableInfo, String partitionName)`
- `ResolvedPartitionSpec toHistoricalPartitionSpec(TableInfo tableInfo, String originalPartitionName)`
- `boolean isExpiredAutoPartition(TableInfo tableInfo, String partitionName, Instant now)`
- `Optional<Integer> getAutoPartitionKeyIndex(TableInfo tableInfo)`

`isExpiredAutoPartition` 必须按规则逐步判断，不要凭直觉判断：

1. 检查表是 partitioned 且启用了 auto partition：`tableInfo.isAutoPartitioned()`。
2. 检查表启用了 lake：`tableInfo.getTableConfig().isDataLakeEnabled()`。
3. 检查 lake format 是 Paimon：
   `tableInfo.getTableConfig().getDataLakeFormat().orElse(null) == DataLakeFormat.PAIMON`。
4. 按 `tableInfo.getPartitionKeys()` 严格解析 `partitionName`。解析时必须校验 partition values 数量和 partition keys 数量完全相等，不能只依赖当前 `String.split` 的宽松行为。
5. 定位 auto-partition key：如果配置了 `autoPartitionStrategy.key()`，用该 key；否则用第一个 partition key，和现有 `validateAutoPartitionTime` 行为一致。
6. 取出 auto-partition key 对应的值。
7. 检查该值符合配置的 auto-partition time format。
8. 按配置时区计算最早保留分区值：

   ```java
   ZonedDateTime current =
       ZonedDateTime.ofInstant(now, autoPartitionStrategy.timeZone().toZoneId());
   String earliestRetained =
       generateAutoPartitionTime(
           current,
           -autoPartitionStrategy.numToRetain(),
           autoPartitionStrategy.timeUnit());
   ```

9. 只有当下面条件成立时，该分区在时间维度上才是 expired：

   ```java
   earliestRetained.compareTo(autoPartitionValue) > 0
   ```

注意：metadata existence 不是 `isExpiredAutoPartition` 的职责。第一版中，client 只有在普通 partition id resolution 失败后才会尝试 historical lookup；coordinator/server 不额外做 authoritative original partition existence re-check。完整的 MVP 条件是：

1. partition name 合法且在时间上 expired；
2. client 的普通 partition id resolution 已经失败；
3. coordinator 端重新校验 table eligibility 和 computed historical partition spec 是否合法。

测试：

- 在 `PartitionUtilsTest` 增加单测。
- 覆盖单分区键和多分区键。
- 覆盖合法 expired partition。
- 覆盖非法 partition name。
- 覆盖 future/current retained partition。
- 覆盖非 Paimon lake format 和非 lake table。

## Step 2: 在 `createPartition` 中支持 historical system partition

现有 public coordinator create-partition 是 WRITE 操作，并且会通过 `validateAutoPartitionTime` 拒绝旧 auto partition，也会通过普通 identifier validation 拒绝 `__historical__`。第一版不新增 RPC，而是在现有 `createPartition` RPC 中增加 historical system partition 特殊分支。

涉及文件：

- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java`
- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/AutoPartitionManager.java`
- common partition utility tests

实现要求：

1. `CoordinatorService.createPartition` 先解析 request 中的 table path 和 partition spec，但不要立刻做 WRITE 授权。
2. 加一个 server-side predicate 判断该 request 是否是 historical system partition create。这个判断只能基于 server 解析出的 table metadata 和 partition spec，不能依赖 client 传入 flag。
3. 如果不是 historical system partition，保持现有行为：WRITE 授权、`validatePartitionSpec(..., true)`、`validateAutoPartitionTime(...)`。
4. 如果是 historical system partition，走特殊分支：
   - 使用 table READ 权限授权，不要求 WRITE；
   - 推荐要求 `ignoreIfExists=true`；
   - 不检查 original partition 当前是否仍存在，也不检查它是否曾经存在过。该取舍依赖 client 只在普通 partition id resolution 失败后调用 create；
   - 不做 `validateAutoPartitionTime`；
   - 创建 partition 时仍复用现有 metadata manager / replica assignment 机制。
5. historical system partition 判断和 validation 要求：
   - spec 包含完整 partition keys；
   - 只有 auto-partition key 的值是 `__historical__`；
   - 非 auto partition values 仍然使用普通 partition value 规则校验；
   - 表启用 auto partition；
   - 表启用 lake；
   - lake format 是 Paimon；
   - partition spec 中不能出现额外 key 或缺失 key；
   - `__historical__` 只能出现在 auto-partition key 上。
6. 如果 historical partition 不存在，则幂等创建。和其他 client 竞争创建时，将 already exists 当成成功。
7. `AutoPartitionManager.dropPartitions` 在 TTL 比较前跳过 historical partition，避免系统分区被自动过期删除。

测试：

- 普通 partition create 仍然需要 WRITE 权限。
- 合法 historical system partition create 只需要 READ 权限。
- `__historical__` 出现在非 auto-partition key 上时失败。
- non-lake 或 non-Paimon table 创建 historical system partition 失败。
- malformed / missing / extra partition keys 失败，且不创建 metadata。
- `ignoreIfExists=true` 下并发创建 historical partition 成功收敛。
- auto partition expiration 不会 drop `__historical__`。

## Step 3: 扩展 lookup RPC

在 lookup request 中携带原始 partition name。

文件：

- `fluss-rpc/src/main/proto/FlussApi.proto`

变更：

```protobuf
message PbLookupReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  repeated bytes keys = 3;
  optional string partition_name = 4;
}
```

然后重新生成 RPC classes：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

兼容性：

- 老 client 不发送 `partition_name`，新 server 当普通 lookup 处理。
- 新 client 只在 historical lookup 时发送 `partition_name`。

## Step 4: client batching 携带 historical metadata

当前 client batching 只按 `TableBucket` 分组，这不够。多个 expired original partitions 可以映射到同一个 `__historical__` bucket，而一个 `PbLookupReqForBucket` 只能携带一个 `partition_name`。

涉及文件：

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupQuery.java`
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/AbstractLookupQuery.java`
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupBatch.java`
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupSender.java`
- `fluss-client/src/main/java/org/apache/fluss/client/utils/ClientRpcMessageUtils.java`

实现：

1. 给 lookup query 增加 `@Nullable String partitionName`。该字段表示原始 partition name，不是 `__historical__` partition name。
2. 新增 `LookupBatchKey`，包含：
   - `TableBucket tableBucket`
   - `@Nullable String partitionName`
3. 普通 lookup 的 `partitionName` 为 null。
4. historical lookup 的 `partitionName` 是 expired original partition。
5. 普通 lookup 仍按 table bucket 分组。
6. historical lookup 按 `(historical table bucket, original partition name)` 分组。
7. `LookupSender` 的 historical dispatch 必须用 `LookupBatchKey`，不能只用 `TableBucket`。
8. 一个 `PbLookupReqForBucket` 最多只能设置一个 `partition_name`。
9. 同一个 `LookupRequest` 内不能出现相同 `TableBucket` 但不同 `partition_name` 的两个 bucket request。第一版直接拆成不同 RPC，保持现有 response bucket 只按 `TableBucket` 标识的格式可用。
10. 只有 batch key 的 original partition name 非 null 时，才设置 `PbLookupReqForBucket.partition_name`。

这个步骤避免如下错误：

```text
original partitions: 20200101, 20200102
historical partition: __historical__
same bucket id: 3
```

如果把两者混入同一个 bucket request，server 只能用一个 `partition_name` 查 lake，会把另一组 key 查错分区。

测试：

- `LookupSenderTest` 验证同一个 historical `TableBucket` 下，不同 original partition name 会被拆成不同 RPC。
- `LookupSenderTest` 验证不同 `TableBucket` 的 historical lookup 仍可 batch，且不丢 `partition_name`。
- `ClientRpcMessageUtilsTest` 验证只有 historical lookup batch 设置 `partition_name`。

## Step 5: client historical partition resolver

新增一个 connection-scoped 或 lookup-client-scoped resolver，不要每次 lookup 都创建。

推荐文件：

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/HistoricalPartitionResolver.java`

依赖：

- `MetadataUpdater`
- `Admin`

接入方式：

- `FlussConnection.getOrCreateLookupClient()` 创建 `LookupClient` 时传入 `HistoricalPartitionResolver`。
- `FlussTable.newLookup()` 继续使用 connection-scoped lookup client。
- `TableLookup` 和 `PrimaryKeyLookuper` 通过 `LookupClient` 或构造参数拿到 resolver。

resolver 行为：

1. 输入：`TableInfo tableInfo`、`String originalPartitionName`。
2. 计算 historical partition spec：`toHistoricalPartitionSpec(tableInfo, originalPartitionName)`。
3. 先从本地 metadata cache 查 historical partition id。
4. 如果缺失，调用 `metadataUpdater.checkAndUpdatePartitionMetadata(...)` 刷新。
5. 如果仍缺失，调用现有 create-partition RPC：

   ```java
   admin.createPartition(tablePath, historicalSpec.toPartitionSpec(), true)
   ```

   coordinator 会识别该 spec 是 historical system partition，并走 READ 授权特殊分支。
6. create 成功或 already exists 后，按 historical partition id 或 name 刷新 metadata。
7. cache key 必须带 table identity：

   ```text
   (table id, original partition name) -> historical partition id
   ```

   也可以增加第二层 cache：

   ```text
   (table id, historical partition name) -> historical partition id
   ```

   这样多个 original partitions 映射到同一个 static prefix historical partition 时，可以避免重复 metadata refresh。
8. 并发场景下，`PartitionAlreadyExistsException` 视为成功。
9. 不读取 `dynamicPartitionEnabled`。`__historical__` 是系统分区，不是用户动态分区。

并发：

- 用 `ConcurrentHashMap<HistoricalPartitionKey, CompletableFuture<Long>>` 合并同一个 table/original partition 的 in-flight resolve。
- failed future 要从 map 移除，便于下次重试。

## Step 6: `PrimaryKeyLookuper` 路由 historical lookup

文件：

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/PrimaryKeyLookuper.java`

当前行为：

```java
catch (PartitionNotExistException e) {
    return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
}
```

替换为：

1. 增加专门给 historical lookup 使用的 Paimon lake lookup key encoder。不要假设现有 `primaryKeyEncoder` 一定输出 Paimon bytes。kv format v2 且 bucket key 非默认时，现有 primary key encoder 会使用 Fluss compacted encoding 来保留 prefix lookup 支持，而 Paimon `LocalTableQuery` 需要 Paimon trimmed primary-key encoding。

   推荐：

   ```java
   KeyEncoder lakePrimaryKeyEncoder =
       KeyEncoder.ofBucketKeyEncoder(
           lookupRowType,
           tableInfo.getPhysicalPrimaryKeys(),
           DataLakeFormat.PAIMON);
   ```

   如果这个 factory 名字容易误导，可以新增显式的
   `KeyEncoder.ofLakePrimaryKeyEncoder(...)`。
2. 用 `partitionGetter.getPartition(lookupKey)` 从 lookup key 中提取 original partition name。
3. 先尝试普通 partition id 解析。
4. 如果普通 partition 存在，保持现有路径，发送普通 `pkBytes`。
5. 如果抛出 `PartitionNotExistException`：
   - 如果 `insertIfNotExists` 为 true，直接失败，因为本里程碑不支持 historical write；
   - 用 `Instant.now()` 计算 expired predicate；
   - 如果 predicate 为 false，对非 expired missing partition 保持旧行为，返回 empty lookup result；
   - 如果 predicate 为 true，通过 `HistoricalPartitionResolver` 解析 historical partition id。resolver 会调用现有 `createPartition` RPC 创建 historical system partition；coordinator 侧只校验目标 historical spec 的合法性，不重新校验 original partition 是否存在、曾经存在或时间上 expired。
6. bucket id 用现有 bucket key bytes 和 Paimon bucketing function 计算，保持 FIP 路由规则和 lake bucket 对齐。
7. 用 Paimon lake lookup key encoder 生成 `lakePkBytes`。
8. 发送 lookup 到：

   ```text
   TableBucket(tableId, historicalPartitionId, bucketId)
   ```

   并在 lookup query 中携带 original partition name。

重要约束：

- 普通 lookup 仍然发送 `pkBytes`。
- historical lake lookup 发送 `lakePkBytes`。
- 本里程碑不要把 partition name prepend 到 key bytes 中，original partition name 单独通过 `partition_name` 传输。

测试：

- 普通 partition lookup 路径不变。
- 缺失但非 expired partition 仍返回 empty。
- expired partition 路由到 historical partition id。
- original partition name 被传到 `LookupClient`。
- kv format v2 且非默认 bucket key 时，historical lookup 发送 Paimon-encoded key，不发送 Fluss compacted key。

## Step 7: TabletService 保留 `partition_name`

当前 server 转换会丢失 bucket-level request metadata，因为 `toLookupData` 返回 `Map<TableBucket, List<byte[]>>`。

涉及文件：

- `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java`
- `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

新增 request holder：

```java
public final class LookupDataForBucket {
    private final TableBucket tableBucket;
    private final List<byte[]> keys;
    @Nullable private final String partitionName;
}
```

实现：

1. lookup request parsing 返回有序 `List<LookupDataForBucket>`，或返回以 `(TableBucket, partitionName)` 为 key 的 composite map。historical lookup data 不能用 `Map<TableBucket, ...>`，否则会丢 original partition name，也可能覆盖同一个 historical bucket 的另一条 request entry。
2. `partitionName == null` 时保持普通 lookup 行为，但如果请求目标是 historical partition 且没有 `partitionName`，必须拒绝，因为 server 不知道该查哪个 lake partition。
3. `partitionName != null` 只是 client 给出的 historical lookup hint。server 仍必须验证目标 `TableBucket` 确实解析到 `__historical__` partition，才能走 lake lookup。
4. authorization 可以继续按 `TableBucket` 做，但传给 `ReplicaManager` 的数据必须保留 `partitionName`。
5. 本里程碑 response 不需要携带 `partition_name`，因为 client batching 规则禁止一个 `LookupRequest` 中出现相同 `TableBucket` 且不同 `partition_name` 的 request group。
6. 如果未来要在一个 RPC 中 batch 这种 duplicate historical bucket，必须增加 request-entry correlation key，或者在 `PbLookupRespForBucket` 中带回 `partition_name`。

兼容性：

- prefix lookup 不变。
- put-kv 本里程碑不变。

## Step 8: 增加 lake lookup SPI

在 lake SPI 中增加表级 point lookup 能力。

涉及文件：

- `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeStorage.java`
- 新文件：
  `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeTableLookuper.java`

推荐接口：

```java
public interface LakeTableLookuper extends AutoCloseable {

    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    final class LookupContext {
        private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final int schemaId;

        // constructor + getters
    }
}
```

在 `LakeStorage` 中增加 default method：

```java
default LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
    throw new UnsupportedOperationException(
            "Point lookup is not supported for this lake storage.");
}
```

本里程碑只有 Paimon override。

## Step 9: 实现 `PaimonLakeTableLookuper`

推荐文件：

- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/lookup/PaimonLakeTableLookuper.java`

使用 Paimon 1.3.1 API：

- `FileStoreTable.newLocalTableQuery()`
- `LocalTableQuery.lookup(BinaryRow partition, int bucket, InternalRow key)`
- `LocalTableQuery.refreshFiles(BinaryRow partition, int bucket, List<DataFileMeta> beforeFiles, List<DataFileMeta> dataFiles)`

实现 outline：

1. 懒加载：
   - Paimon catalog
   - `FileStoreTable`
   - `LocalTableQuery`
   - partition row converter
   - key row wrapper
   - Fluss value encoder
2. 将 `LookupContext.partitionSpec` 转成 Paimon partition `BinaryRow`。优先复用现有工具：
   - `PartitionUtils.toPartitionRow(...)`
   - `PaimonConversions.toFlussRowType(...)`
   - `FlussRowAsPaimonRow`
3. 直接把 incoming key bytes 包装成 Paimon `BinaryRow`。historical client path 必须发送 Paimon-encoded lake lookup key bytes，server 不做 decode/re-encode。
4. lookup 前按 `(partition, bucket)` 刷新文件。只有 latest snapshot 变化时才刷新。
   - 用 `fileStoreTable.newScan().withPartitionFilter(...).withBucket(bucket)` plan splits。
   - 对每个 `DataSplit` 收集 `beforeFiles()` 和 `dataFiles()`。
   - 调用 `refreshFiles` 前按 file name 去重。
5. 调用 `localTableQuery.lookup(partition, bucketId, keyRow)`。
6. Paimon 返回 null 时返回 null。
7. Paimon 返回 row 时：
   - 用 `PaimonRowAsFlussRow` 适配；
   - 用目标 Fluss table 在该 `schemaId` 下的 schema 构造 `CompactedRowEncoder`；
   - 排除 Paimon system fields，保持 Fluss physical field order；
   - 用 `ValueEncoder.encodeValue((short) schemaId, row)` 包装。

线程安全：

- `lookup` 加 `synchronized`，或用 lock 保护 Paimon query mutable state。多个 historical lookup task 可能从 `ioExecutor` 并发访问同一个 table lookuper。

生命周期：

- `close()` 中关闭 `LocalTableQuery` 和 catalog。
- `ReplicaManager` shutdown 时关闭 server-side manager 缓存的所有 lookupers。

测试：

- 可行时对 conversion 和 value encoding 写小型 Paimon table 单测。
- 端到端覆盖放在 Step 12。

## Step 10: server historical lookup manager

在 tablet-server/replica 侧增加一个小 manager，用 table id 或 table path 缓存 lake lookupers。

推荐文件：

- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPartitionLookupManager.java`

依赖：

- `Configuration`
- `PluginManager`
- `TabletServerMetadataCache` 或现有 replica metadata access

接入：

- `TabletServer` 将 `pluginManager` 传给 `ReplicaManager`。
- `ReplicaManager` 构造 `HistoricalPartitionLookupManager`。

manager 行为：

1. 第一次查询某张表时加载 lake storage：
   - 验证 table config 启用 lake；
   - 验证 lake format 是 Paimon；
   - 用 `LakeStoragePluginSetUp.fromDataLakeFormat(...)`；
   - 用 `LakeStorageUtils.extractLakeProperties(conf)` 创建 `LakeStorage`；
   - 调用 `createLakeTableLookuper(tablePath)`。
2. 按 table id 缓存 `LakeTableLookuper`。
3. schema id 变化时：
   - 要么让 lookuper 刷新内部 table resources；
   - 要么 evict 并重建 cached lookuper。
4. `ReplicaManager` close 时关闭缓存 lookupers。

动态配置：

- 第一版可以简单依赖进程重启加载新的 lake runtime config。
- 如果必须立即支持动态 lake config，可以参考 `LakeCatalogDynamicLoader`，在 `datalake.*` 配置变化时 evict lookupers。

## Step 11: `ReplicaManager` 执行 historical lookup

文件：

- `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

实现：

1. 先按 `partitionName` 是否存在把 lookup bucket data 分成普通候选和 historical 候选。这个分类只是候选，不是最终授权或校验结果。
2. 普通 lookup 按今天逻辑查询 local replica KV。
3. 对 historical lookup：
   - 用 `getReplicaOrException(tb)` 获取 hosted replica；
   - 验证目标 `TableBucket` 是 partitioned bucket；
   - 根据 partition id 解析 target partition name，并校验它满足 `isHistoricalPartitionName(tableInfo, targetPartitionName)`；
   - 如果普通 partition bucket 带了 `partitionName`，拒绝；
   - 如果 historical partition bucket 缺少 `partitionName`，拒绝；
   - 校验 PK table client version；
   - 校验 `replica.getTableInfo().hasPrimaryKey()`；
   - 严格解析 `partitionName` 为 `ResolvedPartitionSpec`；
   - server 侧重新计算 `isExpiredAutoPartition(replica.getTableInfo(), partitionName, now)`；
   - MVP 不在 server 侧重新检查 original partition 当前是否存在，依赖 client 只在普通 partition id resolution 失败后发送 historical lookup；
   - 将 lake lookup 提交到 `ioExecutor`；
   - 对每个 key 调用 `LakeTableLookuper.lookup(key, context)`；
   - 保持结果顺序。
4. normal lookup 和 historical futures 都完成后，才 complete 原始 response callback。
5. historical validation 或执行失败时，返回 bucket-level `ApiError`。validation failure 使用确定的 `ApiException` 和明确错误信息，不能 fallback 到普通 local lookup。

metrics：

- normal 和 historical bucket 都增加现有 total lookup requests。
- failed lookup requests 只对意外 server-side failure 增加，保持现有语义。
- 第一版不强制增加专门 historical lookup metric。

测试：

- `ReplicaManager` 或 tablet-service 测试：普通 partition bucket 携带 `partitionName` 时拒绝。
- historical bucket 缺少 `partitionName` 时拒绝。
- original partition name malformed/current/future/not expired 时拒绝。
- 合法 historical lookup 走 lake lookup 并保持结果顺序。

## Step 12: 集成测试

增加一个通过 public API 验证功能的端到端 ITCase。

推荐位置：

- `fluss-client/src/test/java/org/apache/fluss/client/table/LakeEnableTableITCase.java`
- 或者在现有 lake table tests 附近新增 historical lookup ITCase

测试场景：

1. 启动启用 Paimon lake storage 的 Fluss cluster。
2. 创建主键表：
   - 按 auto partition key 分区；
   - `table.datalake.enabled=true`；
   - `table.datalake.format=paimon`；
   - 使用较小 retention count。
3. 向一个当前合法分区写入一行。
4. 确保该行已 tier 到 Paimon。优先复用现有 tiering test helper，不要靠 sleep。
5. 通过 TTL 或测试可控 metadata 操作让原始分区过期并从 metadata 删除。
6. 构造包含原始 partition value 的 lookup key。
7. 调用 `table.newLookup().createLookuper().lookup(key)`。
8. 断言返回行等于写入行。
9. 断言 `listPartitionInfos` 包含生成的 `__historical__` partition。

负例：

- 非 lake table 上看起来 expired 的 partition 不路由到 `__historical__`。
- future/current missing partition 不路由到 `__historical__`。
- `enableInsertIfNotExists()` 查询过期分区时返回明确 unsupported 错误。

测试断言只使用 AssertJ。

## Step 13: 后续流控 PR

建议将 historical lookup 流控做成独立 PR，不放进基础功能 PR。基础功能 PR 只保证 historical lookup 能识别、能路由、能在 server 侧提交到 `ioExecutor`；流控 PR 在这个基础上限制 historical lookup 对实时 lookup 的影响。

目标：

- server 侧限制进入 lake lookup / `ioExecutor` 的 historical lookup 并发。
- overload 时返回明确 throttle 错误，并让 client 走 retry/backoff。
- 第一版不新增 client-side historical inflight ratio，也不增加 dedicated historical lookup metrics。

server 侧实现：

1. 新增 `netty.server.max-queued-historical-requests` 配置，作为 historical lookup request 的准入上限。
2. 在 `ReplicaManager` 或 `HistoricalPartitionLookupManager` 中增加 historical lookup semaphore，容量来自 `netty.server.max-queued-historical-requests`。
3. historical lookup 进入 lake lookup 前先 `tryAcquire()`。
4. 获取 permit 失败时，不提交到 `ioExecutor`，直接返回 bucket-level throttle `ApiError`。
5. 所有 historical futures 完成后释放 permit，异常路径也必须释放。
6. permit 粒度建议第一版按 bucket request 计算，而不是按 key 计算，避免大批量请求产生过多 acquire/release 开销。
7. normal lookup 不经过该 semaphore。

client 侧实现：

1. 不新增 `client.lookup.historical-inflight-ratio`。
2. 第一版不拆分 `LookupSender` 的 normal/historical inflight permits。
3. 收到 historical throttle error 后，复用现有 retry/backoff 机制；如现有路径不能表达延迟重试，只补最小的 historical throttle retry/backoff 处理。

推荐配置：

- `netty.server.max-queued-historical-requests`

该配置与 `netty.server.max-queued-requests` 分开，避免通过 ratio 隐式推导 historical 容量。默认值可以先设为保守值，例如 50；推荐不超过 `netty.server.max-queued-requests`。

推荐 metrics：

- 第一版暂不增加 dedicated historical lookup metrics。
- 继续复用现有 request/error 观测能力；如后续排查需要，再单独补 historical lookup total/throttled/latency/inflight metrics。

测试：

- server semaphore 满时，historical lookup 返回 throttle error，不进入 `ioExecutor`。
- normal lookup 不受 historical semaphore 影响。
- historical lookup 成功、失败、取消路径都释放 permit。
- `netty.server.max-queued-historical-requests` 控制 historical semaphore 容量；显式配置能覆盖默认值。
- client 收到 historical throttle error 后会 retry/backoff。

## 建议 PR 拆分

1. 公共工具和 coordinator 支持：
   - historical partition 常量；
   - expired partition predicate；
   - `createPartition` 中 READ-authorized historical system partition 分支；
   - historical system partition validation；
   - AutoPartitionManager 跳过 historical partition TTL；
   - 单测。
2. RPC 和 client 路由：
   - lookup `partition_name` proto field；
   - generated RPC classes；
   - lookup query metadata；
   - 按 original partition name batching；
   - historical partition resolver；
   - `PrimaryKeyLookuper` 路由修改；
   - client 单测。
3. Lake SPI 和 Paimon lookuper：
   - `LakeTableLookuper`；
   - `LakeStorage#createLakeTableLookuper`；
   - `PaimonLakeTableLookuper`；
   - focused Paimon tests。
4. Server 执行和 E2E：
   - server request parsing；
   - `ReplicaManager` historical lookup path；
   - server lookuper cache；
   - server-side validation 负例；
   - integration test。
5. 后续流控 PR：
   - server historical lookup semaphore；
   - `netty.server.max-queued-historical-requests` 配置；
   - historical throttle error 和 retry/backoff；
   - 流控单测。

## 验证命令

先跑 focused checks：

```bash
./mvnw test -pl fluss-common -Dtest=PartitionUtilsTest
./mvnw test -pl fluss-client -Dtest=LookupSenderTest,ClientRpcMessageUtilsTest
./mvnw test -pl fluss-server -Dtest=AutoPartitionManagerTest
./mvnw test -pl fluss-server -Dtest='*Historical*Lookup*Test'
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest='*Paimon*Lookup*Test'
```

再跑受影响模块：

```bash
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server,fluss-lake/fluss-lake-paimon
./mvnw spotless:check
```

如果包含 proto 变更，先生成代码：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

## 风险和后续工作

- historical lookup 和 normal lookup 第一版仍共用 client inflight semaphore，慢 lake lookup 仍可能占用 client lookup permits。如果服务端 throttle/backoff 不够，再单独评估 client normal/historical permits 拆分。
- 基础功能 PR 如果没有 server historical lookup semaphore，短时间大量 historical lookup 仍可能占满 `ioExecutor`。本文建议在后续流控 PR 中增加 server semaphore 和 throttle error。
- 本里程碑没有 historical write，所以 late write 到 `__historical__` 后立即可查的问题不处理。本里程碑只让已经 tier 到 lake 的历史数据可查。
- Paimon `LocalTableQuery.refreshFiles` 必须对 data files 去重，否则 Paimon level 构造可能失败。
- response dispatch 不能只依赖 `TableBucket` 来处理多个 original partitions 映射到同一个 historical bucket 的场景。第一版通过按 original partition name 拆 RPC 保证不歧义。
- `ResolvedPartitionSpec.fromPartitionName` 当前解析宽松，本功能需要 strict parser。
- MVP 不检查 original partition 当前是否存在，也不证明它曾经存在过。风险是 stale client 可能把仍存在的 old partition 误路由到 lake，或者 READ 用户在多分区键表中构造合法但无业务意义的静态前缀并创建 `fake-prefix$__historical__`。后续可通过 authoritative existence check、expired partition tombstone、drop registry 或 lake partition existence check 收紧。

## Done Criteria

- 普通主键 lookup 行为不变。
- eligible expired auto-partition 的 lookup 能从 Paimon lake storage 返回值。
- invalid、future、current、non-lake、non-Paimon partition 不路由到 `__historical__`。
- MVP 明确接受不校验 original partition 当前是否存在、也不校验 original partition 是否曾经存在。
- `__historical__` 不被 `AutoPartitionManager` 过期删除。
- historical lookup RPC 携带 original partition name，并且一个 bucket request 不混入多个 original partition names。
- lake lookup 不在 RPC thread 上执行。
- focused unit tests 和 Paimon historical lookup E2E test 通过。
- 后续流控 PR 完成后，historical lookup 过载不会占满 normal lookup permits，server 侧 historical lookup 超限会返回 throttle error。
