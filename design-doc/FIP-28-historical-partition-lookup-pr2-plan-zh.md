# FIP-28 Historical Partition Lookup PR 2 实施计划

## 目标

PR 2 为 historical lookup 增加 lookup RPC 字段和 client batching plumbing，让后续 PR 可以把 original partition name 安全传到 tablet server。

该 PR 只完成协议字段、client lookup query 数据结构、lookup batching 拆分和 request 序列化。不启用 expired partition fallback，不创建 `__historical__` partition，也不接入 lake lookup。

## 现状

当前相关代码位置：

- `fluss-rpc/src/main/proto/FlussApi.proto`
  - `PbLookupReqForBucket` 当前只包含 `partition_id`、`bucket_id` 和 `keys`。
  - `PbLookupRespForBucket` 只用 `partition_id` 和 `bucket_id` 标识 response bucket。
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/AbstractLookupQuery.java`
  - 当前 lookup query 只保存 `TablePath`、`TableBucket`、key bytes 和 retry count。
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupQuery.java`
  - 当前 primary-key lookup query 没有 partition name 字段。
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupClient.java`
  - 当前 public internal lookup 方法只接收 `TablePath`、`TableBucket`、key bytes 和 `insertIfNotExists`。
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupSender.java`
  - 当前 regular lookup batching 使用 `Map<TableBucket, LookupBatch>`。
  - response dispatch 也按 `TableBucket` 找回对应的 `LookupBatch`。
- `fluss-client/src/main/java/org/apache/fluss/client/utils/ClientRpcMessageUtils.java`
  - `makeLookupRequest` 当前按 `LookupBatch.tableBucket()` 生成 `PbLookupReqForBucket`。

## 非目标

PR 2 不做以下事情：

- 不修改 `PrimaryKeyLookuper` 的 missing partition 行为。
- 不新增或启用 `HistoricalPartitionResolver`。
- 不调用 `Admin#createPartition` 创建 `__historical__` partition。
- 不修改 tablet server lookup 执行语义。
- 不新增 lake lookup SPI。
- 不实现 Paimon lookup。
- 不处理 historical lookup 流控。
- 不让 prefix lookup 携带 `partition_name`。
- 不支持 `insertIfNotExists` 的 historical lookup。

## Step 1: 扩展 Lookup RPC

修改 `fluss-rpc/src/main/proto/FlussApi.proto`：

```protobuf
message PbLookupReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  repeated bytes keys = 3;
  optional string partition_name = 4;
}
```

`partition_name` 表示 original partition name，不是 `__historical__` system partition name。

生成 RPC classes：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

不要手动修改生成文件。

兼容性边界：

- 老 client 不发送 `partition_name`，新 server 按普通 lookup 处理。
- PR 2 合入后的新 client 仍不会主动发送 `partition_name`，除非后续 PR 显式调用带 partition name 的 lookup path。
- 新字段是 optional，不能改变旧 request 的编码和解析行为。

## Step 2: 扩展 Client Lookup Query

给 regular lookup query 增加 nullable partition name。

推荐改动：

- 在 `AbstractLookupQuery` 中增加：

```java
private final @Nullable String partitionName;
```

- 构造函数增加 `@Nullable String partitionName` 参数。
- 增加 getter：

```java
public @Nullable String partitionName() {
    return partitionName;
}
```

- 保留现有构造路径，让普通 lookup 默认传 `null`。

也可以只在 `LookupQuery` 中保存该字段。优先放在 `AbstractLookupQuery` 的理由是 `LookupSender` 统一处理 query 时可以直接读取，不需要把历史语义散落到多个 cast 之后。

`partitionName` 的定义必须写清楚：

- 普通 lookup：`partitionName == null`。
- 后续 historical lookup：`partitionName` 是 expired original partition name。
- `TableBucket.partitionId` 仍表示实际路由目标 partition id。historical lookup 中它会是 `__historical__` partition id。

## Step 3: 扩展 LookupClient API

保留现有方法：

```java
public CompletableFuture<byte[]> lookup(
        TablePath tablePath,
        TableBucket tableBucket,
        byte[] keyBytes,
        boolean insertIfNotExists)
```

新增 package/public internal overload：

```java
public CompletableFuture<byte[]> lookup(
        TablePath tablePath,
        TableBucket tableBucket,
        byte[] keyBytes,
        boolean insertIfNotExists,
        @Nullable String partitionName)
```

现有方法调用新 overload，并传 `null`。

PR 2 不从 `PrimaryKeyLookuper` 调用新 overload。这个 overload 是给 PR 4 的 historical routing 使用。

## Step 4: 新增 LookupBatchKey

新增 `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupBatchKey.java`，package-private 即可。

字段：

```java
private final TableBucket tableBucket;
private final @Nullable String partitionName;
```

方法：

- constructor
- `tableBucket()`
- `partitionName()`
- `equals()`
- `hashCode()`
- `toString()`

`equals` 和 `hashCode` 必须同时包含 `tableBucket` 和 `partitionName`。

这个 key 的含义是一个 client-side lookup batch 的最小安全分组。按定义逐步判断：

1. 一个 `PbLookupReqForBucket` 只有一个 `partition_name` 字段。
2. 一个 `PbLookupReqForBucket` 可以携带多个 keys。
3. 因此一个 bucket request 内所有 keys 必须属于同一个 original partition name。
4. normal lookup 没有 original partition name，所以用 `null` 参与分组。
5. historical lookup 必须按 `(historical TableBucket, original partition name)` 分组。

## Step 5: 扩展 LookupBatch

修改 `LookupBatch`，让它能暴露 `partitionName`。

推荐结构：

```java
private final LookupBatchKey lookupBatchKey;
```

并保留便捷方法：

```java
public TableBucket tableBucket() {
    return lookupBatchKey.tableBucket();
}

public @Nullable String partitionName() {
    return lookupBatchKey.partitionName();
}

LookupBatchKey lookupBatchKey() {
    return lookupBatchKey;
}
```

如果为了减少改动继续保存 `TableBucket tableBucket`，也必须额外保存 `@Nullable String partitionName`，并保证构造 `LookupBatch` 时不会丢失该字段。

## Step 6: 修改 LookupSender Regular Lookup Batching

当前 `sendLookupRequest` 使用：

```java
Map<Long, Map<TableBucket, LookupBatch>> lookupByTableId
```

需要调整为先按 `LookupBatchKey` 聚合：

```java
Map<Long, Map<LookupBatchKey, LookupBatch>> lookupByTableId
```

构造 key：

```java
LookupBatchKey batchKey =
        new LookupBatchKey(lookup.tableBucket(), lookup.partitionName());
```

prefix lookup 不改，仍按 `TableBucket` 聚合。

### Request Packing 规则

`LookupResponse` 的 bucket response 只包含 `partition_id` 和 `bucket_id`。它不返回 `partition_name`。因此发送端必须保证同一个 `LookupRequest` 中不会有相同 `TableBucket` 但不同 `partitionName` 的 bucket request。

按定义逐步判断冲突：

1. response bucket 由 `(table id, partition id, bucket id)` 还原为 `TableBucket`。
2. response bucket 不包含 original partition name。
3. 如果一个 request 中有两个 bucket request 使用相同 `TableBucket`，response dispatch 时只能找到一个 `LookupBatch`。
4. 如果这两个 bucket request 的 `partitionName` 不同，client 无法判断 response values 应该 complete 哪一组 futures。
5. 所以这种组合必须在发送前拆成不同 `LookupRequest`。

推荐实现：

- `sendLookupRequest` 先得到 `Map<Long, Map<LookupBatchKey, LookupBatch>>`。
- 对每个 table id，把 batch keys 分成多个 request group。
- 每个 request group 内用 `Set<TableBucket>` 保证不重复。
- 如果下一个 `LookupBatchKey.tableBucket()` 已经存在于当前 group，则开启新的 group。
- 每个 request group 再构造 `Map<TableBucket, LookupBatch>` 用于 response dispatch。

该实现保守但清晰。它满足以下行为：

- 同一个 `TableBucket` + 同一个 `partitionName` 会合并成一个 bucket request。
- 同一个 `TableBucket` + 不同 `partitionName` 会拆到不同 RPC。
- 不同 `TableBucket` 即使带不同 `partitionName`，也可以放在同一个 RPC。

注意普通 lookup 的 `partitionName` 为 null。相同普通 `TableBucket` 仍会像现在一样合并。

## Step 7: 修改 ClientRpcMessageUtils

修改 `makeLookupRequest`：

```java
if (batch.partitionName() != null) {
    pbLookupReqForBucket.setPartitionName(batch.partitionName());
}
```

约束：

- 只有非 null 时才设置 `partition_name`。
- 不要把 empty string 当成 special value。PR 4 的 historical validation 再决定是否接受 malformed partition name。
- `partition_id`、`bucket_id`、`keys` 的原有行为保持不变。
- `insertIfNotExists`、`acks`、`timeout_ms` 的原有行为保持不变。

## Step 8: 测试计划

### ClientRpcMessageUtilsTest

新增 focused tests：

- `testMakeLookupRequestWithoutPartitionName`
  - 构造普通 `LookupBatch`。
  - 断言 `PbLookupReqForBucket.hasPartitionName()` 为 false。
- `testMakeLookupRequestWithPartitionName`
  - 构造带 original partition name 的 `LookupBatch`。
  - 断言 `partition_name` 等于传入值。
  - 断言 `partition_id`、`bucket_id`、`keys` 未受影响。
- `testMakeLookupRequestSetsOnePartitionNamePerBucket`
  - 构造多个 batch。
  - 每个 bucket request 只检查自己的 `partition_name`。
  - 不在一个 bucket request 内混入多个 original partition name。

### LookupSenderTest

新增或扩展 tests：

- `testHistoricalLookupsWithSameBucketAndDifferentPartitionNamesSplitRequests`
  - 构造两个 `LookupQuery`，使用相同 `TableBucket`，分别传 `dt=20200101` 和 `dt=20200102`。
  - gateway 记录收到的 `LookupRequest`。
  - 断言收到两个 request，或至少两个 request group。
  - 每个 request 只有一个 bucket req。
  - 两个 bucket req 的 `partition_name` 分别正确。
  - 两个 query futures 都能被正确 complete。
- `testHistoricalLookupsWithDifferentBucketsCanBatch`
  - 构造两个 `LookupQuery`，使用不同 `TableBucket`，分别传不同 original partition name。
  - 断言可以进入同一个 `LookupRequest`。
  - 断言两个 bucket req 都保留自己的 `partition_name`。
- `testNormalLookupsKeepExistingBatching`
  - 构造两个普通 lookup，使用相同 `TableBucket`，partition name 为 null。
  - 断言仍合并成一个 bucket req。
  - 断言未设置 `partition_name`。
- `testLookupResponseDispatchForSplitHistoricalRequests`
  - gateway 针对拆分后的 requests 分别返回 values。
  - 断言每个 query future 收到对应 value。

测试中使用 AssertJ，不使用 JUnit assertions。

## 验证命令

建议至少运行：

```bash
./mvnw test -pl fluss-client -Dtest=LookupSenderTest,ClientRpcMessageUtilsTest
./mvnw spotless:check -pl fluss-client,fluss-rpc
```

如果 proto 生成或 RPC module 有改动风险，再运行：

```bash
./mvnw test -pl fluss-rpc
```

## 合入后的行为

合入 PR 2 后：

- lookup protobuf 已经能携带 bucket-level original partition name。
- client 内部已经能把 lookup query 按 `(TableBucket, partitionName)` 安全分组。
- client request 序列化只会在非 null 时设置 `partition_name`。
- 普通 lookup batching、retry 和 response dispatch 行为保持不变。
- 主键 lookup 遇到 missing partition 时仍返回 empty lookup result。
- server 不会因为该 PR 开始执行 historical lake lookup。

## 实现检查清单

- `PbLookupReqForBucket.partition_name` 是 optional field。
- generated RPC classes 来自 protogen，不是手写。
- `LookupBatchKey.equals/hashCode` 同时包含 `TableBucket` 和 nullable `partitionName`。
- `LookupSender` 不再用 `TableBucket` 作为 regular lookup 的唯一 batch key。
- 单个 `LookupRequest` 内没有相同 `TableBucket`、不同 `partitionName` 的 bucket req。
- `ClientRpcMessageUtils` 只在 non-null partition name 时设置 protobuf 字段。
- prefix lookup 没有被改动。
- `PrimaryKeyLookuper` 没有启用 historical fallback。
- 测试覆盖普通 lookup 兼容行为和 historical plumbing 行为。
