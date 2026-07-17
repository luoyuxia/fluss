# FIP-28 Historical Write PR 1 实施计划

## PR 标题

```text
[client] Add historical partition context to PK write batches
```

## 目标

本 PR 为 historical primary-key write 增加 RPC 字段、client record/batch 上下文、batch 隔离能力，以及供后续 processor 使用的 server decode holder。

本 PR 只建立 plumbing，不启用 expired partition write fallback。合并后，正常 writer 不会主动写入 `__historical__`，server 也不会进入 historical KV、lake old-value lookup、异步执行或 recovery 路径。

## 与总计划的关系

本 PR 对应 `FIP-28-historical-partition-write-pr-plan-zh.md` 中的：

```text
PR 1: PutKv RPC + Client Write Batching Plumbing
```

后续依赖：

- PR 3 使用本 PR 的 `PutKvDataForBucket` 及其 original partition name 构造 composite key 和 lake lookup context，并通过直接调用完成 processor tests。
- PR 4 使用本 PR 的 request 隔离结果接入 historical server dispatch。
- PR 6 在 client 识别 expired partition 后，才真正设置 historical write target 和 original partition name。

## 前置假设

- 当前 historical lookup PR 已将 lookup RPC 字段命名为 `original_partition_name`。
- historical PK write 使用同一个术语，避免 `partition_id` 和 `partition_name` 分别指向不同 partition 时产生歧义。
- `partition_id` 始终表示实际写入 target；普通写指向普通 partition，historical write 指向 `__historical__` partition。
- `original_partition_name` 表示 row 原本所属、已经过期的业务 partition。
- `original_partition_name` 是 bucket-request-level 字段，一个 `PbPutKvReqForBucket` 只能对应一个 original partition。
- 普通 PK write 的 original partition name 为 null。

## 非目标

本 PR 不实现：

- expired partition eligibility 判断。
- historical partition resolve/create。
- `WriterClient` 将 physical target 改写为 `__historical__`。
- server 根据 original partition name 选择 historical write pipeline。
- historical RocksDB、composite key、tombstone 或 lake fallback。
- historical write flow control、retry backoff、recovery 或 cleanup。
- log table historical routing。
- Paimon historical tiering。
- 修改普通 writer 的 idempotence、merge mode、target columns 或 retry 语义。

## 当前实现约束

### 1. RecordAccumulator 当前只按 physical path 和 bucket 建队列

当前结构是：

```text
PhysicalTablePath
    -> BucketAndWriteBatches
        -> bucketId
            -> Deque<WriteBatch>
```

不同 original partitions 会映射到相同 historical physical path 和 bucket。如果直接复用 deque 尾部的 `KvWriteBatch`，不同 original partitions 的 records 会被写进同一个 `KvRecordBatch`。

### 2. Idempotence 和发送顺序按 TableBucket 管理

`IdempotenceManager`、writer id、batch sequence 和 inflight state 都按 `TableBucket` 管理。为了 original partition batching 而创建多套并行 deque，会增加同一 historical bucket 下的排序和 sequence 协调复杂度。

### 3. Sender response 只通过 TableBucket 关联 batch

`PbPutKvRespForBucket` 不返回 original partition name。当前 `Sender` 使用：

```text
Map<TableBucket, ReadyWriteBatch> recordsByBucket
```

如果一个 `PutKvRequest` 中出现两个相同 `TableBucket`、不同 original partition name 的 bucket requests，后加入的 batch 会覆盖前一个，response 无法正确关联。

### 4. Server 当前 decode 会丢失 bucket-level context

`ServerRpcMessageUtils.getPutKvData()` 当前返回：

```java
Map<TableBucket, KvRecordBatch>
```

直接修改该方法的返回类型会连带影响 `TabletService` 和 `ReplicaManager` 的 normal PutKv path。PR 1 需要提供能够保留 original partition name 的 decode API，同时不应为了尚未启用的 historical request 改写正常请求热路径。

## 核心设计

### 1. RPC 字段使用 `original_partition_name`

扩展 `PbPutKvReqForBucket`：

```protobuf
message PbPutKvReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  required bytes records = 3;
  // The original partition name for historical PK writes.
  // It is unset for normal writes.
  optional string original_partition_name = 4;
}
```

保持 field number `4`，不修改已有字段编号。

字段含义：

```text
partition_id             = actual target partition id (__historical__)
original_partition_name  = expired original partition name
```

### 2. 保留 per-TableBucket 单一 deque

本 PR 不修改 `RecordAccumulator.writeBatches` 的顶层 key，也不把 original partition name 加入 `BucketAndWriteBatches.batches` 的 map key。

original partition context 只属于 PK/KV write。不要为了让 `Sender` 读取该字段而修改 `WriteBatch` 基类及所有 log batch constructors；`Sender` 在确认 batch 是 KV batch 后读取 `KvWriteBatch.getOriginalPartitionName()`。这样 `ProduceLog` plumbing 不受影响。

batch 隔离由 `KvWriteBatch` 自身保证：

- `KvWriteBatch` 在创建时固定 nullable `originalPartitionName`。
- `tryAppend()` 比较 batch 和 record 的 original partition name。
- 两者相同，record 可以继续 append。
- 两者不同，返回 `false`，让 `RecordAccumulator` 关闭当前 batch，并在同一个 deque 尾部创建新 batch。

示例：

```text
输入顺序: 2000/key1, 2000/key2, 2001/key3, 2000/key4

同一个 historical TableBucket deque:
    Batch(original=2000): key1, key2
    Batch(original=2001): key3
    Batch(original=2000): key4
```

这样同时满足：

- 单个 `KvWriteBatch` 只有一个 original partition。
- 同一个 historical bucket 的 arrival order 不变。
- writer id 和 batch sequence 继续按现有 `TableBucket` 顺序分配。
- `ready()`、`drain()`、re-enqueue 和 metadata readiness 不需要引入新的 bucket key 类型。

original partition 不同是正常的 batch boundary，不应抛异常。`KvWriteBatch.tryAppend()` 应返回 `false`，由 accumulator 执行现有的 close-and-create-next-batch 流程。

### 3. Sender 按 request group 隔离

在 `Sender` 中增加 focused grouping helper，例如：

```java
private List<List<ReadyWriteBatch>> packPutKvRequestGroups(
        List<ReadyWriteBatch> batches)
```

每个 PutKv request group 必须满足：

- 全部 batch 属于同一 table id。
- 全部是 normal PK batches，或者全部是 historical PK batches。
- 每个 `TableBucket` 在 group 中最多出现一次。
- historical batch 可以来自不同 `TableBucket`，每个 bucket request 携带自己的 original partition name。

遇到以下任一条件时开始新的 request group：

1. normal/historical write kind 发生变化。
2. 当前 group 已经包含相同 `TableBucket`。

这里以 `KvWriteBatch.getOriginalPartitionName() != null` 作为 historical batch 的内部判定。该判定只用于 client request grouping，不代表 server 已经启用 historical write。

拆分 request 之前，必须先在完整的 table-level batch list 上校验 `targetColumns` 和 `mergeMode` 一致。随后每个 group 调用 `makePutKvRequest()` 时保留现有校验。不能只在拆分后按 group 校验，否则原本应该因 table 内属性不一致而失败的 batches，可能因为恰好进入不同 groups 而绕过校验。

每个实际发送的 RPC 都创建自己的：

```java
Map<TableBucket, ReadyWriteBatch> recordsByBucket
```

不能让拆分后的多个 RPC 共享覆盖范围更大的 response correlation map，否则一个 RPC failure 可能错误 retry 或 complete 另一个 RPC 的 batch。

request groups 和各自的 correlation map 应在 gateway availability 分支之前构造。gateway 不存在时，也要逐 group 调用现有 failure handling，不能先把所有 batches 放进一个会按 `TableBucket` 覆盖的全局 map。

### 4. 新增 contextual server decoder，不替换 normal decoder

新增 immutable holder：

```java
public final class PutKvDataForBucket {
    private final TableBucket tableBucket;
    private final KvRecordBatch records;
    private final @Nullable String originalPartitionName;
}
```

命名和 getter style 参考现有 `LookupDataForBucket`。

在 `ServerRpcMessageUtils` 增加独立的 contextual decoder，例如：

```java
public static Map<TableBucket, PutKvDataForBucket> toPutKvDataForBuckets(
        PutKvRequest request)
```

该 helper 完整解析 `TableBucket`、`KvRecordBatch` 和 nullable original partition name。PR 3 的 historical processor 直接接收该 holder；PR 4 再让 online dispatch 调用此 decoder。

本 PR 保留现有 `getPutKvData()` 及其调用链，不修改 `TabletService.putKv()` 或 `ReplicaManager.putRecordsToKv()`。原因是 PR 1 的正常生产路径不会发送 original partition name；立即替换现有返回类型会让 normal-write hot path 在 PR 4 接入 online dispatch 前承担一次无消费者的 holder-to-records 转换。

contextual decoder 在本 PR 中通过 focused unit test 验证，但不做 target validation、不触发 historical dispatch。PR 3 只把 holder 作为 processor input，PR 4 再根据 online request context 选择 decoder 和执行路径。

## 详细实施步骤

### Step 1: 扩展 PutKv protobuf

修改：

```text
fluss-rpc/src/main/proto/FlussApi.proto
```

变更：

- 给 `PbPutKvReqForBucket` 增加 `optional string original_partition_name = 4`。
- 注释明确 target partition 与 original partition 的区别。
- 不修改 `PbPutKvRespForBucket`。
- 不新增 historical flag 或 enum，避免和 original partition name 形成重复状态。

重新生成并编译 RPC：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc -am
```

Rust compatibility：

```text
fluss-rust/crates/fluss/src/rpc/message/put_kv.rs
```

现阶段 Rust writer 仍只构造 normal write，新增字段固定为 `None`。本 PR 不在 Rust client 启用 historical write。

### Step 2: 在 WriteRecord 保存 original partition context

修改：

```text
fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java
```

增加：

```java
private final @Nullable String originalPartitionName;

public @Nullable String getOriginalPartitionName();
```

所有现有 factory 创建的 record 默认使用 null，因此普通 append/upsert/delete 行为不变。

为了让本 PR 的 tests 和 PR 6 能构造 historical write context，推荐增加 immutable copy 方法：

```java
public WriteRecord withOriginalPartitionContext(
        PhysicalTablePath physicalTablePath,
        @Nullable String originalPartitionName)
```

约束：

- non-null original partition name 只允许用于 KV write format。
- non-null original partition name 不能为空字符串。
- copy 保留 key、bucket key、row、schema、target columns、merge mode 和 estimated size。
- 原 `WriteRecord` 不修改。
- 本 PR 的正常生产路径不调用该方法。

### Step 3: 在 KvWriteBatch 固定 original partition

修改：

```text
fluss-client/src/main/java/org/apache/fluss/client/write/KvWriteBatch.java
fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java
```

`KvWriteBatch` constructor 增加：

```java
@Nullable String originalPartitionName
```

增加 getter：

```java
public @Nullable String getOriginalPartitionName();
```

`RecordAccumulator.createWriteBatch()` 使用创建 batch 的第一条 `WriteRecord` 初始化该字段。

`KvWriteBatch.tryAppend()` 在 schema、target columns 和 merge mode 校验前增加：

```java
if (!Objects.equals(originalPartitionName, writeRecord.getOriginalPartitionName())) {
    return false;
}
```

这项检查应在写入 `KvRecordBatchBuilder` 之前完成。

所有测试和 helper 中直接调用 `new KvWriteBatch(...)` 的位置都补 null，保持普通 batch 语义。

### Step 4: 序列化 bucket-level original partition name

修改：

```text
fluss-client/src/main/java/org/apache/fluss/client/utils/ClientRpcMessageUtils.java
```

`makePutKvRequest()` 对每个 `ReadyWriteBatch`：

1. cast 为 `KvWriteBatch`。
2. 正常设置 partition id、bucket id 和 records。
3. `getOriginalPartitionName() != null` 时设置 `setOriginalPartitionName(...)`。
4. null 时保持 protobuf field unset，不能写空字符串。

普通 PutKv request 的 wire payload 除 protobuf schema 增加 optional field 外保持不变。

### Step 5: 拆分 PutKv request groups

修改：

```text
fluss-client/src/main/java/org/apache/fluss/client/write/Sender.java
```

建议流程：

```text
drained batches for one destination
    -> group by table id
    -> log table: existing ProduceLog path
    -> PK table: validate targetColumns/mergeMode over the full table batch list
        -> packPutKvRequestGroups
            -> one PutKvRequest per group
            -> one recordsByBucket map per request
            -> existing response/retry handling
```

建议先把每个待发送单元表示为 request group 加其 `recordsByBucket`，再检查 gateway：

- gateway 可用时，逐单元发送并注册 response callback。
- gateway 不可用时，逐单元调用 `handleWriteRequestException()`。
- 不再在 table grouping 之前建立一个 destination-wide `recordsByBucket`。

不要修改：

- `IdempotenceManager` 的 key。
- `inFlightBatches` 的 key。
- batch sequence 分配方式。
- normal retry/backoff。
- ProduceLog batching。

`packPutKvRequestGroups` 使用 `LinkedHashMap` 或等价的 insertion-order collection，保证拆分后 batch 的提交顺序可预测。

如果抽取 table-level validation helper，它必须与 `ClientRpcMessageUtils.makePutKvRequest()` 使用相同的 `Arrays.equals(targetColumns, ...)` 和 `MergeMode` equality 规则，不增加新的兼容性语义。

### Step 6: 增加 server holder 和 contextual decoder

新增：

```text
fluss-server/src/main/java/org/apache/fluss/server/entity/PutKvDataForBucket.java
```

holder 保存：

- non-null `TableBucket`。
- non-null `KvRecordBatch`。
- nullable `originalPartitionName`。

修改：

```text
fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java
```

新增的 contextual decoder 对每个 `PbPutKvReqForBucket`：

1. 按当前逻辑构造 `TableBucket`。
2. 按当前逻辑把 records bytes 转为 `DefaultKvRecordBatch`。
3. field present 时保存 `getOriginalPartitionName()`；field absent 时保存 null。
4. 返回按 `TableBucket` 索引的 holder map。

本 PR 不在 decoder 内判断 target 是否为 `__historical__`，也不校验 original partition name 是否 expired。这些判断依赖 actual partition metadata，属于 PR 3。

保留现有 `getPutKvData()` 和 normal `TabletService.putKv()` 调用链。不要在两个 decoder 之间通过 serialize/deserialize 转换；允许 PR 1 暂时保留两段很短的 bucket decode，PR 4 接入 contextual decoder 后再删除重复路径。

### Step 7: 更新协议与代码注释

注释需要明确：

- `partitionId` 是 actual write target。
- `originalPartitionName` 只用于 historical PK context。
- null 表示 normal write。
- batch boundary 为什么必须包含 original partition identity。
- response 不携带 original partition name，因此 request 内不能重复 `TableBucket`。

不要在本 PR 修改用户文档或宣称 historical write 已可用。

## 预计文件范围

必须修改：

```text
fluss-rpc/src/main/proto/FlussApi.proto
fluss-rust/crates/fluss/src/rpc/message/put_kv.rs
fluss-client/src/main/java/org/apache/fluss/client/write/WriteRecord.java
fluss-client/src/main/java/org/apache/fluss/client/write/KvWriteBatch.java
fluss-client/src/main/java/org/apache/fluss/client/write/RecordAccumulator.java
fluss-client/src/main/java/org/apache/fluss/client/write/Sender.java
fluss-client/src/main/java/org/apache/fluss/client/utils/ClientRpcMessageUtils.java
fluss-server/src/main/java/org/apache/fluss/server/entity/PutKvDataForBucket.java
fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java
```

预计更新的测试：

```text
fluss-client/src/test/java/org/apache/fluss/client/write/KvWriteBatchTest.java
fluss-client/src/test/java/org/apache/fluss/client/write/RecordAccumulatorTest.java
fluss-client/src/test/java/org/apache/fluss/client/write/SenderTest.java
fluss-client/src/test/java/org/apache/fluss/client/utils/ClientRpcMessageUtilsTest.java
```

## 测试计划

### KvWriteBatchTest

覆盖：

- batch 和 record 都为 null original partition 时正常 append。
- batch 和 record original partition 相同时正常 append。
- batch 与 record original partition 不同时返回 false。
- 不同 original partition 不应抛异常。
- mismatch 后 batch 中已有 records 和 callbacks 不改变。

### RecordAccumulatorTest

构造同一个 historical physical path、同一个 bucket：

```text
record1 original=2000
record2 original=2000
record3 original=2001
record4 original=2000
```

验证 deque 中依次产生三个 batches：

```text
batch1 original=2000, recordCount=2
batch2 original=2001, recordCount=1
batch3 original=2000, recordCount=1
```

同时验证：

- 所有 batches 仍在同一个 TableBucket deque 中。
- drain 顺序与 append 顺序一致。
- normal records 仍按原逻辑合批。
- re-enqueue 不丢失 batch 的 original partition name。

### ClientRpcMessageUtilsTest

覆盖：

- normal `KvWriteBatch` 不设置 `original_partition_name`。
- historical batch 设置正确的 original partition name。
- 同一个 request 中不同 buckets 可以携带各自的 original partition name。
- field number 通过生成代码保持为 4。

### SenderTest

覆盖：

- normal 和 historical PK batches 被拆成不同 PutKv requests。
- 相同 historical `TableBucket` 的不同 original partitions 被拆成不同 requests。
- 不同 historical `TableBucket` 可以进入同一 request，并分别携带正确字段。
- 每个 request 使用独立 `recordsByBucket` correlation map。
- 一个 request 失败只 retry 该 request 中的 batches。
- gateway 不可用时，相同 `TableBucket` 的多个 groups 都得到失败处理，不因 map 覆盖遗漏 batch。
- response 正确 complete 对应 batch，不发生覆盖。
- 即使 batches 因 normal/historical 或重复 `TableBucket` 被拆组，table-level `targetColumns` 或 `mergeMode` 不一致仍按当前语义失败。
- ProduceLog request 数量和内容不受影响。

### Regression tests

继续运行现有：

- normal upsert/delete batching。
- partial update target columns。
- DEFAULT/OVERWRITE merge mode validation。
- idempotent writer batch sequence 和 retry。
- append-only ProduceLog path。

## 兼容性

### Old client -> new server

- old client 不发送 field 4。
- contextual decoder 将该字段解析为 null。
- normal PutKv path 保持不变。

### New client -> old server

- PR 1 合并后，正常生产路径仍不会设置 original partition name。
- normal write 与 old server 兼容。
- focused tests 可以构造带字段请求，但该能力在 PR 6 前不对用户开放。

### Rolling upgrade

- optional field 不改变普通 request 的 wire behavior。
- field number 4 必须固定，后续 PR 只能扩展语义，不能重新编号。
- Java generated API 和 Rust prost struct 必须在同一个 PR 更新。

## 本 PR 必须保证的行为

### 风险 1: 修改 accumulator key 破坏顺序

防护：保留单一 per-TableBucket deque，只通过 batch invariant 切分。

### 风险 2: original partition mismatch 被当成用户错误

防护：`KvWriteBatch.tryAppend()` 返回 false，不抛异常。

### 风险 3: response correlation 覆盖

防护：每个 request group 中 `TableBucket` 唯一，并为每个 RPC 创建独立 map；gateway unavailable 路径也逐 group 处理。

### 风险 4: request 拆分放宽原有校验

防护：在 pack 之前对完整 table-level list 校验 `targetColumns` 和 `mergeMode`，每个 request 内继续保留原校验。

### 风险 5: 普通写 payload 意外设置空字符串

防护：只有 non-null 时调用 protobuf setter；null 保持 field unset。

### 风险 6: PR 1 提前启用不完整 historical write

防护：不修改 `DynamicPartitionCreator`、`WriterClient.doSend()` 和 server dispatch。正常生产路径不会创建带 original partition name 的 record。

### 风险 7: 提前修改 server hot path

防护：PR 1 增加独立 contextual decoder，但不替换 `TabletService` 使用的 normal decoder。PR 3 只在 processor 层消费 holder，PR 4 再接入 online path。

## 验证命令

生成协议并编译依赖：

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc -am
```

运行 client focused tests：

```bash
./mvnw test -pl fluss-client \
  -Dtest=KvWriteBatchTest,RecordAccumulatorTest,SenderTest,ClientRpcMessageUtilsTest
```

验证 Rust：

```bash
cd fluss-rust
cargo fmt --check
cargo check -p fluss-rs
```

格式和模块验证：

```bash
./mvnw spotless:check -pl fluss-rpc,fluss-client,fluss-server
./mvnw verify -DskipITs -pl fluss-rpc,fluss-client,fluss-server -am
```

## 完成标准

以下条件全部满足后，PR 1 才算完成：

- `PbPutKvReqForBucket` 使用 `original_partition_name = 4`。
- normal PutKv request 不设置该字段。
- `WriteRecord` 和 `KvWriteBatch` 可以保存 original partition name。
- 同一个 `KvWriteBatch` 不会包含多个 original partitions。
- accumulator 仍保持一个 per-TableBucket FIFO deque。
- normal/historical PutKv requests 相互隔离。
- 单个 request 中不会出现重复 `TableBucket`。
- request 拆分前仍在完整 table-level batch list 上执行现有 target columns 和 merge mode 一致性校验。
- response 和 request-level failure 只处理对应 request group 的 batches。
- server contextual decoder 可以完整保留 bucket、records 和 nullable original partition name。
- server normal PutKv decode 和 dispatch 调用链保持不变。
- Rust client 可以在新增 protobuf 字段后正常编译。
- 不修改 expired partition routing，不启用 historical server pipeline。
- focused tests、Spotless 和受影响模块验证通过。

## 合并后的行为

PR 1 合并后：

- protocol 和 client batching 已经能够安全表达 historical PK write context。
- normal write 行为和 wire payload 保持不变。
- server 已具备保留 original partition context 的 contextual decoder，normal PutKv decode 和 dispatch 调用链保持不变。
- PR 3 使用 holder 实现 historical processor，PR 4 再把 contextual decoder 接入 online dispatch。
- 用户写 expired partition 的行为保持不变。
- historical write 的实际可用性仍依赖后续 storage、processor、recovery、tiering 和 client routing PR。
