# FIP-28 Historical Partition Lookup PR 3 实施计划

## 目标

PR 3 增加 lake point lookup SPI，并实现 Paimon 版本的 table lookuper。

该 PR 只提供 server 后续执行 historical lookup 需要调用的 lake lookup 能力，不接入 `ReplicaManager`，不改变当前 lookup 请求处理路径，也不启用 client historical fallback。

## 前置依赖

该 PR 默认基于 PR 1 和 PR 2：

- PR 1 已提供 historical partition 的公共语义和 coordinator create historical partition 支持。
- PR 2 已让 lookup request 可以携带 original partition name。

PR 3 自身不依赖 client 一定发送 `partition_name`，也不依赖 server 已经识别 historical lookup。它只实现一个可被后续 server manager 调用的 lake lookup SPI。

## 现状

当前相关代码位置：

- `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeStorage.java`
  - 目前只有 `createLakeTieringFactory()`、`createLakeCatalog()` 和 `createLakeSource(TablePath)`。
  - 没有 point lookup 能力。
- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/PaimonLakeStorage.java`
  - 当前只创建 Paimon tiering factory、catalog 和 source。
- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/source/PaimonSplitPlanner.java`
  - 已有 Paimon catalog、`FileStoreTable`、`InnerTableScan` 的使用模式。
- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/RecordWriter.java`
  - `resolvePartition` 已有把 Fluss partition name 转成 Paimon `BinaryRow` 的写入侧逻辑。
  - 该方法是 private，且依赖 `TableWriteImpl#getPartition`，不能被 lookup 实现直接调用。
- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/utils/PaimonConversions.java`
  - 已有 Paimon/Fluss row type 和 partition value 转换工具。
- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/utils/PaimonRowAsFlussRow.java`
  - 已有 Paimon row 到 Fluss `InternalRow` 的 adapter，并会隐藏 Paimon system columns。
- `fluss-common/src/main/java/org/apache/fluss/row/encode/paimon/PaimonKeyEncoder.java`
  - 已有符合 Paimon BinaryRow 编码的 key encoder。
- `fluss-common/src/main/java/org/apache/fluss/row/encode/CompactedRowEncoder.java`
  - 可把 Fluss `InternalRow` 编码为 compacted row。
- `fluss-common/src/main/java/org/apache/fluss/row/encode/ValueEncoder.java`
  - 可把 compacted row 和 schema id 包装成 Fluss KV value bytes。

Paimon 版本为 1.3.1。该版本相关 API：

- `org.apache.paimon.table.FileStoreTable#newLocalTableQuery()`
- `org.apache.paimon.table.query.LocalTableQuery#lookup(BinaryRow, int, InternalRow)`
- `org.apache.paimon.table.query.LocalTableQuery#refreshFiles(BinaryRow, int, List<DataFileMeta>, List<DataFileMeta>)`
- `org.apache.paimon.table.source.InnerTableScan#withPartitionFilter(List<BinaryRow>)`
- `org.apache.paimon.table.source.InnerTableScan#withBucket(int)`

## 非目标

PR 3 不做以下事情：

- 不修改 `ReplicaManager` 的 lookup 执行路径。
- 不新增 `HistoricalPartitionLookupManager`。
- 不在 tablet server 中加载或缓存 lake lookupers。
- 不修改 `PrimaryKeyLookuper`。
- 不实现 expired partition fallback。
- 不新增 coordinator/client resolver。
- 不处理 `insertIfNotExists` 的 historical lookup。
- 不实现 Iceberg、Hudi、Lance 的 lookup。
- 不增加 historical lookup 流控和 metrics。
- 不做端到端 public API historical lookup ITCase。端到端覆盖放在 server 接入 PR。

## Step 1: 新增 Lake Lookup SPI

新增文件：

- `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeTableLookuper.java`

推荐接口：

```java
@PublicEvolving
public interface LakeTableLookuper extends AutoCloseable {

    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    final class LookupContext {
        private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final short schemaId;
        private final RowType valueRowType;

        // constructor + getters
    }
}
```

字段含义：

- `partitionSpec`
  - original partition 的 resolved spec。
  - 对 historical lookup 来说，它不是 `__historical__` partition spec。
  - Paimon lookuper 用它定位 lake table 中真实的数据 partition。
- `bucketId`
  - Paimon bucket id。
  - 后续 server 接入时，该值来自 request 的 historical `TableBucket#getBucket()`。
- `schemaId`
  - 返回给 Fluss client 的 value bytes 中携带的 schema id。
  - 类型建议使用 `short`，和 `ValueEncoder.encodeValue(short, BinaryRow)` 保持一致。
- `valueRowType`
  - 用于把 Paimon row 编码成 Fluss compacted row 的 Fluss row type。
  - 不建议只传 `schemaId`，否则 Paimon lookuper 需要依赖 server schema cache。

返回值语义：

- 返回 `null` 表示 lake table 中没有该 key。
- 返回非 null bytes 时，bytes 必须是 Fluss KV value 格式，即 `ValueEncoder.encodeValue(schemaId, compactedRow)` 的结果。
- SPI 不返回 Paimon row，也不把 Paimon 类型泄漏给 server lookup path。

## Step 2: 扩展 `LakeStorage`

修改：

- `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeStorage.java`

新增 default method：

```java
default LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
    throw new UnsupportedOperationException(
            "Point lookup is not supported for this lake storage.");
}
```

约束：

- 使用 default method，避免要求 Iceberg、Hudi、Lance 在 PR 3 中同步实现。
- 错误信息要明确，后续 server manager 可以把它转换成清晰的 bucket-level error。
- 不要让 default implementation 返回 `null`。返回 `null` 会把“不支持 lookup”和“lookup miss”混在一起。

## Step 3: PaimonLakeStorage 暴露 Lookuper

修改：

- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/PaimonLakeStorage.java`

新增 override：

```java
@Override
public LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
    return new PaimonLakeTableLookuper(paimonConfig, tablePath);
}
```

新增实现文件：

- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/lookup/PaimonLakeTableLookuper.java`

构造参数：

- `Configuration paimonConfig`
- `TablePath tablePath`

不要在 `PaimonLakeStorage` 中缓存 lookuper。缓存属于后续 server-side manager 的职责。

## Step 4: 实现 PaimonLakeTableLookuper 初始化

`PaimonLakeTableLookuper` 推荐按 lazy init 实现，避免创建对象时立即访问外部 catalog。

内部状态：

- `Catalog catalog`
- `FileStoreTable fileStoreTable`
- `LocalTableQuery localTableQuery`
- `int primaryKeyFieldCount`
- 可选：`Set<PaimonPartitionBucket> initializedBuckets`
- `Object lock` 或 `synchronized lookup`

初始化流程：

1. 使用当前 Paimon 配置创建 catalog：

   ```java
   CatalogFactory.createCatalog(
       CatalogContext.create(Options.fromMap(paimonConfig.toMap())))
   ```

2. 通过 `PaimonConversions.toPaimon(tablePath)` 获取 Paimon identifier。
3. 从 catalog 加载 table，并 cast 为 `FileStoreTable`。
4. 校验 table 是 primary-key table：
   - `fileStoreTable.primaryKeys()` 不能是空。
   - 如果为空，抛出 `UnsupportedOperationException` 或 Fluss 侧明确异常。
5. 创建 `LocalTableQuery`：

   ```java
   fileStoreTable.newLocalTableQuery()
   ```

6. 记录 `primaryKeyFieldCount = fileStoreTable.primaryKeys().size()`。

不要在 PR 3 中处理 server schema refresh。后续 server manager 如果发现 schema id 变化，可以选择 evict lookuper 或重新创建 context。

## Step 5: 转换 PartitionSpec 到 Paimon BinaryRow

Paimon lookup 需要 `BinaryRow partition`。不能假设 partition value 都是 string，也不能用 `BinaryRow.singleColumn(String)` 覆盖多列或非 string 分区。

不要让 lookup 实现直接调用 `RecordWriter#resolvePartition`：

- 该方法当前是 private。
- 它属于 `paimon.tiering` 写入路径。
- 它依赖 `TableWriteImpl#getPartition`，lookup path 不应该为了 partition conversion 创建 Paimon write object。

推荐把 `RecordWriter#resolvePartition` 中可复用的部分抽到 `PaimonConversions`，再让 `RecordWriter` 和 `PaimonLakeTableLookuper` 共同使用。

在 `PaimonConversions` 中新增 helper：

- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/utils/PaimonConversions.java`

推荐 helper 形态：

```java
public static BinaryRow toPaimonPartition(
        ResolvedPartitionSpec partitionSpec,
        RowType flussRowType,
        org.apache.paimon.types.RowType paimonRowType,
        Function<org.apache.paimon.data.InternalRow, BinaryRow> partitionExtractor)
```

实现步骤：

1. 从 `partitionSpec.getPartitionKeys()` 和 `partitionSpec.getPartitionValues()` 取出 partition key/value。
2. 用 `flussRowType` 查找每个 partition key 的字段位置和类型。
3. 用 `PartitionUtils.parseValueOfType(value, typeRoot)` 把 string partition value 转成 Fluss typed value。
4. 构造一个 Fluss `GenericRow`，field count 使用 `paimonRowType.getFieldCount()`，和 `FlussRowAsPaimonRow` 的字段布局保持一致。
5. 只设置 partition key 对应字段，其余字段保持 null。
6. 用 `new FlussRowAsPaimonRow(partitionRow, paimonRowType)` 适配成 Paimon row。
7. 调用传入的 `partitionExtractor` 生成 Paimon partition `BinaryRow`。

写入侧改造：

```java
return PaimonConversions.toPaimonPartition(
        spec, flussRowType, tableRowType, tableWrite::getPartition);
```

lookup 侧使用 Paimon 1.3.1 的 partition extractor，不创建 write object：

```java
RowPartitionKeyExtractor partitionExtractor =
        new RowPartitionKeyExtractor(fileStoreTable.schema());
BinaryRow partition =
        PaimonConversions.toPaimonPartition(
                context.partitionSpec(),
                context.valueRowType(),
                fileStoreTable.schema().logicalRowType(),
                partitionExtractor::partition);
```

这样做的好处：

- 复用 `RecordWriter#resolvePartition` 里的类型解析和 `FlussRowAsPaimonRow` 转换思路。
- 避免把 lookup path 依赖到 `paimon.tiering` 或 `TableWriteImpl`。
- 使用 Paimon 自己的 `RowPartitionKeyExtractor`，和写入侧 `TableWriteImpl#getPartition` 的 partition extraction 规则保持一致。
- 支持多 partition key。
- 支持非 string partition key。
- 避免在 writer 和 lookuper 中复制 partition conversion 逻辑。

## Step 6: 包装 Paimon Lookup Key

PR 3 的 SPI 假设传入的 `key` 已经是 Paimon primary-key BinaryRow bytes。后续 client historical routing PR 必须使用 `PaimonKeyEncoder(valueRowType, primaryKeys)` 生成这些 bytes。

Paimon lookuper 只负责包装，不负责解码再编码：

```java
private BinaryRow wrapLookupKey(byte[] key) {
    BinaryRow keyRow = new BinaryRow(primaryKeyFieldCount);
    keyRow.pointTo(MemorySegment.wrap(key), 0, key.length);
    return keyRow;
}
```

不要在 PR 3 中复用 normal lookup 的 compacted Fluss primary-key bytes。Paimon `LocalTableQuery` 需要 Paimon BinaryRow key。

## Step 7: 初始化 Paimon LocalTableQuery 文件缓存

`LocalTableQuery` 是本地文件查询对象，lookup 前需要让它知道目标 `(partition, bucket)` 的当前 data files。

推荐 helper：

```java
private void initializeFilesIfNeeded(BinaryRow partition, int bucketId) throws Exception
```

第一版采用保守策略：初始化后不再更新文件缓存。原因是当前 FIP-28 第一阶段只支持对历史分区做只读 lookup，不支持向 historical partition 写入数据。也就是说，historical lookup 读取的是已经过期并 tiered 到 Paimon 的 original partition 数据；在当前功能边界内，不会有新的写入继续改变这部分 lake files。

实现步骤：

1. 如果该 `(partition, bucket)` 已经初始化过，则直接返回。
2. 使用 `fileStoreTable.newScan()` 创建 `InnerTableScan`。
3. 调用：

   ```java
   scan.withPartitionFilter(Collections.singletonList(partition)).withBucket(bucketId)
   ```

4. 遍历 `scan.plan().splits()`，只处理 `DataSplit`。
5. 收集所有 `DataSplit#beforeFiles()` 和 `DataSplit#dataFiles()`。
6. 按 `DataFileMeta#fileName()` 去重，保持顺序。
7. 调用：

   ```java
   localTableQuery.refreshFiles(partition, bucketId, beforeFiles, dataFiles);
   ```

8. 初始化成功后记录该 `(partition, bucket)` 已初始化。
9. 在代码中留下 TODO：

   ```java
   // TODO: Refresh files when historical lookup needs to observe new lake snapshots.
   ```

注意：

- 这里的 `refreshFiles` 是 Paimon `LocalTableQuery` 的初始化入口。第一版仍需要调用一次，让 local query 看到初始 files。
- 不在每次 lookup 前检查 latest snapshot，也不维护 snapshot id。
- 后续如果支持向 historical partition 写入，或者允许补数据继续改变历史分区对应的 Paimon files，就必须补上 snapshot 变化检测和增量 refresh。否则 `LocalTableQuery` 只看到初始化时的 files，可能读不到后续写入的数据。
- 如果 plan 出来的 split 为空，lookup 应返回 `null`。不要把空文件列表当异常。

## Step 8: 执行 Lookup 并编码 Fluss Value

`lookup(byte[] key, LookupContext context)` 推荐流程：

1. lazy init。
2. 校验 `context.partitionSpec()`、`context.valueRowType()`、`key` 非 null。
3. 将 `context.partitionSpec()` 转成 Paimon partition `BinaryRow`。
4. 将 `key` 包装成 Paimon key `BinaryRow`。
5. 初始化目标 `(partition, bucket)` 的 files。
6. 调用：

   ```java
   InternalRow row = localTableQuery.lookup(partition, context.bucketId(), keyRow);
   ```

7. 如果 `row == null`，返回 `null`。
8. 如果 Paimon 返回 delete 语义的 row，返回 `null`。如果 Paimon 1.3.1 的 `LocalTableQuery` 已经把 delete merge 成 `null`，这里可以只加防御性判断。
9. 使用 `PaimonRowAsFlussRow` 适配返回 row。
10. 用 `CompactedRowEncoder` 和 `context.valueRowType()` 编码 compacted row：
    - field count 和 field types 必须来自 Fluss `valueRowType`；
    - 不要使用包含 Paimon system columns 的 Paimon row type；
    - 对每个字段用 `InternalRow.createFieldGetter(type, index)` 读取 value。
11. 调用：

    ```java
    ValueEncoder.encodeValue(context.schemaId(), compactedRow)
    ```

12. 返回 value bytes。

编码约束：

- 输出必须和 local KV lookup 返回的 value bytes 格式一致。
- 不要把 `schemaId` 写入 row body。schema id 只由 `ValueEncoder` 写入 value bytes 前缀。
- `PaimonRowAsFlussRow` 已隐藏 system columns，`CompactedRowEncoder` 只应看到 Fluss 业务列。

## Step 9: 并发边界和 Lifecycle

因为第一版不做持续 refresh，不需要为“每次 lookup 检查并刷新最新 snapshot”设计复杂并发控制。但单个 lookuper 后续仍可能被 server manager 缓存，并从 `ioExecutor` 并发调用，所以以下可变状态仍需要保护：

- lazy init 创建 `catalog`、`fileStoreTable` 和 `localTableQuery`；
- 第一次访问某个 `(partition, bucket)` 时调用 `LocalTableQuery#refreshFiles` 初始化文件缓存；
- `initializedBuckets` 的读写；
- `close()` 与正在执行的 lookup 之间的并发。

第一版建议使用简单方案：

```java
public synchronized @Nullable byte[] lookup(byte[] key, LookupContext context) throws Exception
```

这样可以保证 lazy init、首次 file initialization、`LocalTableQuery#lookup` 和 close 的状态访问顺序清晰。等后续需要更高并发时，再把锁缩小到 lazy init 和 `initializeFilesIfNeeded`，并单独确认 Paimon `LocalTableQuery` 的并发语义。

`close()` 行为：

1. 关闭 `localTableQuery`。
2. 关闭 `catalog`。
3. 多次 close 必须安全。
4. 使用 `IOUtils.closeQuietly` 或等价方式聚合关闭异常。

不要在 `close()` 中关闭外部传入的 `Configuration` 或 plugin manager。

## Step 10: 测试计划

### LakeStorage SPI 测试

可在 common 中增加一个小测试，验证默认方法语义：

- 一个 minimal `LakeStorage` implementation 不 override `createLakeTableLookuper`。
- 调用 default method 抛 `UnsupportedOperationException`。
- 错误信息包含 `Point lookup is not supported`。

如果觉得该测试价值不高，可以不加，重点放在 Paimon implementation。

### PaimonLakeTableLookuperTest

推荐新增：

- `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/lookup/PaimonLakeTableLookuperTest.java`

核心场景：

1. 创建临时 Paimon catalog/table。
2. 表必须是 primary-key table。
3. 覆盖 partitioned table：
   - partition key 为 string；
   - 写入一条数据到指定 partition 和 bucket；
   - 用 `PaimonKeyEncoder` 生成 primary-key bytes；
   - 构造 `LookupContext(originalPartitionSpec, bucketId, schemaId, valueRowType)`；
   - 调用 lookuper lookup；
   - 用 `ValueDecoder` 解码返回 bytes；
   - 断言 schema id 和 row 内容。
4. 覆盖 non-string partition key：
   - 例如 partition key 为 int/date；
   - 验证 `toPaimonPartition` 没有按 string-only 逻辑处理。
5. 覆盖 miss：
   - key 不存在时返回 `null`。
6. 覆盖 wrong bucket：
   - 同一 partition 下查错 bucket 返回 `null`。
7. 覆盖 non-primary-key table：
   - 创建 append-only table；
   - 创建 lookuper 后 lookup 时抛明确 unsupported 错误。
8. 如果测试成本可控，覆盖 update/delete：
   - update 后 lookup 返回最新值；
   - delete 后 lookup 返回 `null`。
9. 覆盖 schema evolution：
   - 先用旧 schema 写入一行并提交到 Paimon；
   - alter table 增加 nullable column；
   - 再用新 schema 写入或更新另一行；
   - 用当前最新 schema id/最新 `valueRowType` 构造 `LookupContext`；
   - lookup 旧数据时返回最新 schema id，新增 nullable 字段 padding 为 null；
   - lookup 新数据时返回最新 schema id，新增字段按新 schema 正确解码；
   - 这个 case 用来验证 lookuper 不试图从 Paimon row 推断旧 Fluss schema id，而是按 server 传入的 context schema 编码返回值。

测试实现建议：

- 复用 `PaimonTestUtils` 中的写入和 compact helper。
- 复用 `PaimonConversions.toPaimon(tablePath)`。
- 用 AssertJ 断言。
- 不依赖 sleep。

### 编码回归点

至少需要断言：

- 返回 value bytes 可以被 Fluss `ValueDecoder` 解码。
- 解码后的 schema id 等于 context schema id。
- 解码后的 row type 不包含 Paimon system columns。
- schema evolution 后，旧数据按最新 context schema 编码，新增 nullable 字段 padding 为 null。
- 使用 normal compacted Fluss key bytes 查不到数据，使用 Paimon key bytes 可以查到数据。这个断言可以防止后续误把 key contract 写错。

## Step 11: 验证命令

推荐在 PR 3 完成后运行：

```bash
./mvnw spotless:check -pl fluss-common,fluss-lake/fluss-lake-paimon
./mvnw test -pl fluss-lake/fluss-lake-paimon -am -Dtest=PaimonLakeTableLookuperTest -DfailIfNoTests=false
```

如果新增 common SPI 单测：

```bash
./mvnw test -pl fluss-common -Dtest=LakeStorageTest -DfailIfNoTests=false
```

如果 Paimon test 涉及更多 test utility 或集成依赖，再运行：

```bash
./mvnw test -pl fluss-lake/fluss-lake-paimon -am
```

## PR 3 完成标准

PR 3 合入后应满足：

- `LakeStorage` 提供 point lookup SPI，非 Paimon lake storage 默认明确不支持。
- `PaimonLakeStorage` 可以创建 `PaimonLakeTableLookuper`。
- Paimon lookuper 可以用 Paimon-encoded primary-key bytes 在指定 original partition 和 bucket 中查出一行。
- lookup miss 返回 `null`。
- 返回 value bytes 与 Fluss local KV lookup 的 value encoding 一致。
- lookuper 支持 close，并且单实例并发调用是安全的。
- 不改变当前线上 normal lookup 行为。

## 后续 PR 接入点

PR 4 或后续 server 接入 PR 可以基于本 PR 做：

1. 增加 `HistoricalPartitionLookupManager`，按 table id 缓存 `LakeTableLookuper`。
2. 在 `ReplicaManager` 中识别 historical lookup bucket。
3. server 侧校验 original partition name 和 historical target partition。
4. 对每个 historical lookup key 调用：

   ```java
   lakeTableLookuper.lookup(paimonKeyBytes, lookupContext)
   ```

5. 将返回 bytes 填入现有 lookup response。
6. 增加 historical lookup 端到端 ITCase。

PR 3 只保证第 4 步所需的 SPI 和 Paimon 实现可用。

## 风险和注意事项

- Paimon key bytes 和 Fluss compacted key bytes 不同。PR 3 的 Paimon lookuper 只接受 Paimon key bytes。
- Paimon table schema 中包含 Fluss system columns。value encoding 必须使用 Fluss `valueRowType`，不能直接用 Paimon row type。
- Partition conversion 必须按 partition key 的真实类型解析，不能只支持 string。
- `LocalTableQuery` 有本地文件缓存。第一版只初始化一次，是因为当前不支持写入 historical partition；如果后续支持写 historical partition，必须补上 refresh 逻辑，否则 lookup 可能读到旧文件视图。
- `LocalTableQuery` 是可变对象，不能无锁并发访问。
- SPI default method 必须明确报不支持，不能吞掉异常或返回 `null`。
