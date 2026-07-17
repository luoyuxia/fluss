# FIP-28 Historical Partition Lookup PR 1 实施计划

## 目标

PR 1 建立 historical partition 的公共语义，并让 coordinator 可以通过现有 `createPartition` RPC 创建合法的 `__historical__` system partition。

该 PR 只完成 common utility、coordinator create-partition 分支和 auto partition TTL 保护，不启用 client historical lookup fallback，也不接入 lake lookup。

## 现状

当前相关代码位置：

- `fluss-common/src/main/java/org/apache/fluss/utils/PartitionUtils.java`
  - 已有 `validatePartitionSpec`、`validatePartitionValues`、`validateAutoPartitionTime`、`generateAutoPartitionTime`。
  - `validateAutoPartitionTime` 会拒绝 out-of-date auto partition。
- `fluss-common/src/main/java/org/apache/fluss/metadata/ResolvedPartitionSpec.java`
  - `fromPartitionName` 当前使用 `partitionName.split("\\$")`，不适合作为 historical lookup 的严格 parser。
- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java`
  - `createPartition` 当前一开始就做 WRITE 授权。
  - 之后调用 `validatePartitionSpec(..., true)`，所以普通 create 会拒绝 `__historical__` 这类 `__` 前缀值。
  - 之后调用 `validateAutoPartitionTime`，所以旧 auto partition 会被拒绝。
- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/AutoPartitionManager.java`
  - `dropPartitions` 当前按 auto partition retention 直接删除过期分区，没有跳过 system partition。

## 非目标

PR 1 不做以下事情：

- 不修改 lookup RPC。
- 不修改 `PrimaryKeyLookuper`。
- 不新增 `HistoricalPartitionResolver`。
- 不新增 lake lookup SPI。
- 不实现 Paimon lookup。
- 不实现 historical lookup 流控。
- 不校验 original partition 当前是否存在。
- 不校验 original partition 是否曾经存在。

## Step 1: 增加 Common Historical Partition 工具

优先在 `PartitionUtils` 中新增常量和 helper，避免 coordinator、client、server 后续各自实现一套判断规则。

新增常量：

```java
public static final String HISTORICAL_PARTITION_VALUE = "__historical__";
```

新增 public helper：

- `boolean isHistoricalPartitionName(TableInfo tableInfo, String partitionName)`
- `ResolvedPartitionSpec toHistoricalPartitionSpec(TableInfo tableInfo, String originalPartitionName)`
- `boolean isExpiredAutoPartition(TableInfo tableInfo, String partitionName, Instant now)`
- `Optional<Integer> getAutoPartitionKeyIndex(TableInfo tableInfo)`

新增 private 或 package-private strict parser：

- 输入：`List<String> partitionKeys`、`String partitionName`
- 使用 `partitionName.split("\\$", -1)` 保留空值和尾部空 segment。
- 检查 partition values 数量必须和 partition keys 数量完全相等。
- 数量不一致时抛出 `InvalidPartitionException`，或在 boolean predicate 中捕获后返回 false。

不要直接复用 `ResolvedPartitionSpec.fromPartitionName` 作为严格 parser，因为它当前不能表达“数量必须完全匹配”这个约束。

### `getAutoPartitionKeyIndex`

按以下规则推导 auto partition key：

1. 读取 `tableInfo.getPartitionKeys()`。
2. 读取 `tableInfo.getTableConfig().getAutoPartitionStrategy()`。
3. 如果 `autoPartitionStrategy.key()` 非 null，使用该 key。
4. 如果 `autoPartitionStrategy.key()` 为 null，使用第一个 partition key，和现有 `validateAutoPartitionTime` 保持一致。
5. 如果 key 不在 partition keys 中，返回 `Optional.empty()`。
6. 否则返回该 key 在 partition keys 中的下标。

### `isHistoricalPartitionName`

按定义逐步判断，不凭名字后缀猜测：

1. 表必须是 partitioned 且 auto-partitioned。
2. partition name 必须能按 partition keys 严格解析。
3. 必须能定位 auto partition key index。
4. auto partition key 对应的 value 必须等于 `__historical__`。
5. 非 auto partition key 的 value 都不能等于 `__historical__`。
6. 以上条件全部满足才返回 true。

该方法只判断形态，不负责判断表是否启用 Paimon lake。

### `toHistoricalPartitionSpec`

按以下步骤从 original partition name 生成 historical spec：

1. 严格解析 original partition name。
2. 定位 auto partition key index。
3. 复制原始 partition values。
4. 只把 auto partition key 对应的 value 替换成 `__historical__`。
5. 非 auto partition values 保持不变。
6. 返回新的 `ResolvedPartitionSpec`。

示例：

```text
partition keys: [region, dt]
auto key: dt
original partition: us$20200101
historical partition: us$__historical__
```

### `isExpiredAutoPartition`

该方法是 client/server 后续判断 original partition 是否 eligible 的公共 predicate。PR 1 先实现和测试，PR 4 再接入 lookup path。

按以下定义逐步判断：

1. 表必须是 partitioned 且 auto-partitioned。
2. 表必须启用 data lake。
3. lake format 必须是 `DataLakeFormat.PAIMON`。
4. partition name 必须能按 partition keys 严格解析。
5. 必须能定位 auto partition key index。
6. 取出 auto partition key 对应的 value。
7. 该 value 必须符合 auto partition time format。
8. 用 `now` 和 auto partition timezone 计算当前时间。
9. 用 `generateAutoPartitionTime(current, -numToRetain, timeUnit)` 计算 earliest retained partition value。
10. 比较规则是 `earliestRetained.compareTo(autoPartitionValue) > 0`。
11. 只有第 10 步为 true 时，才返回 true。

注意边界：

- metadata existence 不是 `isExpiredAutoPartition` 的职责。
- invalid partition name 返回 false，或通过内部 parser 抛错后在该 predicate 内转换成 false。
- non-lake、non-Paimon、non-auto-partitioned 表都返回 false。

## Step 2: 增加 Coordinator Historical Create Validation

`CoordinatorService#createPartition` 需要从“先 WRITE 授权”改成“先解析 table 和 partition spec，再按分支授权”。

推荐结构：

```text
createPartition(request)
  tablePath = toTablePath(...)
  table = metadataManager.getTableRegistration(tablePath)
  if !table.isPartitioned(): throw TableNotPartitionedException
  partitionSpec = getPartitionSpec(...)
  if isHistoricalPartitionCreate(table, partitionSpec):
      authorizeTable(READ, tablePath)
      partitionToCreate = validateHistoricalPartitionCreate(...)
  else:
      authorizeTable(WRITE, tablePath)
      validatePartitionSpec(..., true)
      validateAutoPartitionTime(...)
      partitionToCreate = ResolvedPartitionSpec.fromPartitionSpec(...)
  create partition through existing assignment and metadataManager path
```

Coordinator 当前拿到的是 `TableRegistration`，不是完整 `TableInfo`。为了避免只为校验拉取 schema，coordinator 侧 helper 可以基于 `table.partitionKeys` 和 `table.getTableConfig()` 做同等校验；common utility 仍保留 `TableInfo` 版本供 client/server 后续复用。

### Historical Create 判定

该判定必须只依赖 server 解析出的 table metadata 和 request partition spec，不能依赖 client flag。

一个 request 只有满足以下条件，才进入 historical 分支：

1. 表是 partitioned。
2. 表启用了 auto partition。
3. 表启用了 data lake。
4. lake format 是 Paimon。
5. partition spec keys 和 table partition keys 完全一致，没有缺 key 或额外 key。
6. 能定位 auto partition key。
7. auto partition key 的值是 `__historical__`。
8. 非 auto partition key 的值都不是 `__historical__`。

如果 request 不是合法 historical create，就走普通 create 分支，并保持原有 WRITE 授权和普通校验。

### Historical Create 校验

historical 分支需要：

- 使用 READ 权限授权。
- 推荐要求 `ignoreIfExists=true`。如果 request 传入 false，返回明确的 `InvalidPartitionException`。
- 跳过 `validateAutoPartitionTime`。
- 不调用 `validatePartitionSpec(..., true)`，因为它会拒绝 `__historical__` 前缀。
- 仍校验非 auto partition values 的普通合法性。可以对非 auto values 调用 `validatePartitionValues(values, true)`，但不要把 `__historical__` 传进去。
- 仍复用现有 replica assignment、remote dir selection 和 `metadataManager.createPartition`。
- 并发创建时，如果 metadata manager 抛出 already-exists，且 request 是 historical create 或 `ignoreIfExists=true`，应收敛为成功。

### 普通 Create 行为保持不变

普通 create 必须保持以下行为：

- 仍需要 WRITE 权限。
- 仍调用 `validatePartitionSpec(..., true)`。
- 仍调用 `validateAutoPartitionTime`。
- 普通用户仍不能创建 `__historical__` 形态之外的 `__` 前缀 partition value。
- 旧 auto partition 仍被拒绝。

## Step 3: AutoPartitionManager 跳过 Historical Partition

在 `AutoPartitionManager#dropPartitions` 中，实际 drop 前判断当前 partition name 是否是 historical partition。

单分区键表：

```text
partitionName == "__historical__"
```

多分区键表：

```text
auto partition key 对应的 value == "__historical__"
```

实现时不要依赖 sorted map 的 key 是否进入 `headMap(lastRetainPartitionTime)`。如果 historical partition 因字符串顺序落入 headMap，也必须在 drop loop 中显式 skip。

推荐做法：

- 在 `AutoPartitionManager` 中复用 common helper，或增加基于 `partitionKeys + autoPartitionStrategy + partitionName` 的局部 helper。
- skip 时不要从 ZooKeeper 删除 partition。
- skip 后也不要从 `partitionsByTable` 中移除该 partition。
- 加一条 debug/info log，说明 historical partition 被 auto partition expiration 跳过。

## Step 4: 测试计划

### Common 单测

扩展 `PartitionUtilsTest`：

- `testIsHistoricalPartitionNameForSinglePartitionKey`
- `testIsHistoricalPartitionNameForMultiplePartitionKeys`
- `testHistoricalPartitionNameRejectsHistoricalValueOnNonAutoKey`
- `testToHistoricalPartitionSpecForSinglePartitionKey`
- `testToHistoricalPartitionSpecForMultiplePartitionKeys`
- `testIsExpiredAutoPartition`
- `testIsExpiredAutoPartitionRejectsMalformedPartitionName`
- `testIsExpiredAutoPartitionRejectsCurrentOrFuturePartition`
- `testIsExpiredAutoPartitionRejectsNonLakeTable`
- `testIsExpiredAutoPartitionRejectsNonPaimonLakeTable`

断言使用 AssertJ。

### Coordinator 测试

优先放在现有 admin/coordinator 集成测试附近；如果需要验证 ACL 行为，优先复用 `FlussAuthorizationITCase` 的 root/guest fixture。

覆盖：

- 普通 partition create 没有 WRITE 权限时仍失败。
- 合法 historical partition create 只有 READ 权限时成功。
- 合法 historical partition create 没有 READ 权限时失败。
- non-lake table 创建 `__historical__` 失败。
- non-Paimon lake table 创建 `__historical__` 失败。
- `__historical__` 出现在非 auto partition key 上时失败。
- missing partition key 失败且不创建 metadata。
- extra partition key 失败且不创建 metadata。
- `ignoreIfExists=true` 下重复创建 historical partition 成功收敛。
- `ignoreIfExists=false` 创建 historical partition 返回明确错误。

### AutoPartitionManager 测试

扩展 `AutoPartitionManagerTest`：

- 单分区键表中，手动加入 `__historical__` 后，推进 clock 触发 TTL drop，断言 `__historical__` 仍存在。
- 多分区键表中，手动加入 `us$__historical__` 或同等形态后，推进 clock 触发 TTL drop，断言 historical partition 仍存在。
- 同一批过期普通分区仍会被删除，避免 skip 逻辑误伤普通 TTL 行为。

## 建议实现顺序

1. 先实现 `PartitionUtils` 常量、strict parser 和 helper。
2. 补齐 `PartitionUtilsTest`，确认 historical name、historical spec 和 expired predicate 语义稳定。
3. 修改 `CoordinatorService#createPartition` 的授权和 validation 分支。
4. 增加 coordinator/admin 授权和 validation 测试。
5. 修改 `AutoPartitionManager#dropPartitions` 跳过 historical partition。
6. 增加 AutoPartitionManager TTL skip 测试。
7. 跑 focused tests 和 spotless。

## 验证命令

```bash
./mvnw test -pl fluss-common -Dtest=PartitionUtilsTest
./mvnw test -pl fluss-server -Dtest=AutoPartitionManagerTest
./mvnw test -pl fluss-client -Dtest=FlussAuthorizationITCase
./mvnw spotless:check
```

如果 coordinator validation 测试放在其他测试类中，替换第三条命令里的 `-Dtest`。

## Done Criteria

- `PartitionUtils` 有统一 historical partition 常量和 helper。
- expired predicate 按定义逐步判断，且不检查 metadata existence。
- historical partition create 使用 READ 权限。
- ordinary partition create 仍使用 WRITE 权限。
- legal `__historical__` system partition 可以通过 `createPartition` 创建。
- illegal `__historical__` 形态不能创建。
- non-lake、non-Paimon、non-auto-partitioned table 不能创建 historical system partition。
- `__historical__` 不会被 `AutoPartitionManager` TTL 删除。
- 所有新增测试使用 AssertJ。
- PR 1 合并后不会改变普通 lookup 行为。
