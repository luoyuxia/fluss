# FIP-28 Historical Write PR 5 实施计划

## PR 标题

```text
[lake] Tier historical data to original Paimon partitions
```

## 目标

本 PR 让 Paimon tiering 正确处理 historical physical partition 中混合的多个 original partitions。

当前一个 normal Fluss tiering split 对应一个固定 Fluss partition，因此 Paimon writer 可以在构造时把 `WriterInitContext.partition()` 解析成固定的 Paimon `BinaryRow partition`。Historical split 不满足这个前提：

```text
Fluss physical partition:
    __historical__

rows in the same bucket/WAL:
    dt=20240101
    dt=20240102
    dt=20231231
```

如果继续使用 split-level fixed partition，所有 row 都会被写入错误的 Paimon `__historical__` partition，或者在把 `__historical__` 转换为 typed Paimon partition 时直接失败。

本 PR 完成后：

- normal split 继续使用构造期解析的 fixed Paimon partition，不增加 per-record partition extraction 开销。
- historical split 在每条 row 写入前，从 row partition columns 提取 actual Paimon partition。
- append-only 和 primary-key historical row 都写回各自的 original Paimon partition。
- Paimon write 继续使用当前 Fluss bucket，保持 online write、tiering 和 lake lookup 的 bucket 对齐。
- 不同 original partitions 中相同 primary key 不会跨 partition merge。
- historical Arrow log split 继续通过 `pollRecordBatch()` 批量读取，但在 Paimon writer 内逐行解析 partition 并写入。
- 一个 LakeWriter 产生的多个 Paimon commit messages 可以被完整序列化、传输和提交。
- normal Arrow direct-bundle fast path、normal fixed-partition writer 和普通 commit行为保持不变。
- 对符合FIP contract、row保留original partition values的historical WAL，Paimon metadata中不会产生`__historical__` partition。

本 PR 不启用 client expired-partition write fallback。PR 5 合并后，tiering service 已能处理手工构造或测试注入的 historical WAL；PR 6 再让正常 writer 主动产生 historical write。

## 与总计划的关系

本 PR 对应 `FIP-28-historical-partition-write-pr-plan-zh.md` 中的：

```text
PR 5: Paimon Historical Tiering
```

前置依赖：

- FIP-28 已定义 historical partition name、multi-level static prefix 和 row 保留 original partition columns 的语义。
- historical partition create/metadata 已能让 tiering split看到 `__historical__` physical partition。
- PR 1 已让测试或内部 client 构造 historical PK request并保留 original partition context。
- PR 3/PR 4 已保证 historical PK WAL row包含 tiering 所需的 full old/new row；key-only delete最终写入 WAL 的 DELETE row仍带 original partition columns。
- Paimon table schema已包含 Fluss business fields、partition fields和现有 system columns。

依赖关系：

- 核心 Paimon writer改造只依赖 historical partition公共语义，可与 PR 2 到 PR 4 并行开发。
- 按推荐合并顺序，PR 5 在 PR 4 之后合并，因此 focused ITCase可以通过 PR 1/PR 4 的内部入口产生真实 historical WAL。
- PR 6 是本 PR 的直接后续依赖。没有 PR 5 时，打开 client fallback会让 late data写入错误 Paimon partition。

本 PR 不反向修改 PR 1 到 PR 4 的协议、server storage或recovery设计。

## 前置假设

- `WriterInitContext.partition()` 表示当前 Fluss physical tiering split 的 partition name。
- normal split 的每条 row 都属于该 fixed physical partition。
- historical split 可以通过 `PartitionUtils.isHistoricalPartitionName(tableInfo, partitionName)` 可靠识别，不能只检查字符串是否以 `__historical__` 结尾。
- historical row payload 保留完整 original partition columns。append-only row天然满足；PK UPDATE/DELETE WAL由 shared KV processor保存 full before/after row。
- `FlussRecordAsPaimonRow` 在 `setFlussRecord()` 后可被 `TableWriteImpl.getPartition()` 用于提取 typed Paimon `BinaryRow partition`。
- Fluss table partition columns与Paimon table partition keys/schema一致，schema sync在 writer创建前已完成。
- Paimon partition key是primary key的一部分；`RowKeyExtractor.trimmedPrimaryKey()` 与动态 partition组合后可以隔离不同 original partitions中的相同业务 key。
- historical row的bucket key计算规则与normal write相同；tiering继续使用 `TableBucket.getBucket()`，不重新计算bucket。
- bucket-unaware append-only Paimon table仍要求写入bucket `0`，与Fluss physical bucket无关。
- 当前使用的Paimon版本会按touched `(partition, bucket)`产生commit messages；一个historical writer写多个original partitions时会返回多条`CommitMessage`。
- 对本PR支持的Paimon append/PK write path，一个实际写过record的LakeWriter必须产生至少一个commit message；zero-message不能被当成成功并推进tiered offset。
- `PaimonWriteResultSerializer`继续使用version 1，但payload直接替换为list layout，不支持旧version 1 singleton payload。
- `TieringSplitReader.pollRecordBatch()`按`TableBucket`返回batch；不同bucket各自创建LakeWriter，因此normal writer可以继续direct-bundle，historical writer可以在同一batch read path中逐行写入。
- `AppendOnlyArrowBatchHelper`创建的`ArrowBundleRecords`提供`Iterator<InternalRow>`；这些row在batch关闭前由Paimon writer同步消费，不跨`writeArrowBatch()`调用保存。
- record-batch path已在调用LakeWriter前按stopping offset截断batch，并使用consumed-up-to offset推进exclusive progress；historical逐行写不改变这层逻辑。
- PR 5的required coverage以直接构造historical `WriterInitContext`的writer/serde/committer focused tests为主；完整client -> server -> tiering historical WAL E2E由PR 6负责。

## 非目标

本 PR 不实现：

- client expired partition eligibility、metadata fallback或write target redirect；这些属于PR 6。
- historical PutKv dispatch、old-value lookup、RocksDB、recovery、flow control或cleanup；这些属于PR 2到PR 4。
- Iceberg、Lance、Hudi等其他lake format的historical tiering。
- 把historical physical split name当作Paimon business partition来创建或保留。
- 修改row partition columns、RowKind/change type、primary key或bucket key。
- 根据original partition name重新hash bucket。
- 为每个original partition创建独立的Fluss tiering split或LakeWriter。
- 为historical Arrow batch实现按partition group、slice或direct Parquet bundle write。
- 修改普通Arrow batch编码或generic `TieringSplitReader` poll策略。
- 改变normal split的fixed-partition fast path。
- 修改Paimon compaction、snapshot expiration或partition expiration策略。
- 修改Fluss lake snapshot中按`TableBucket`记录tiered offset的模型；一个historical bucket仍只有一个整体offset。
- 按original partition维护独立tiered offset或cleanup watermark。
- 新增server/client RPC字段。
- 让historical PK table使用Paimon snapshot split；historical PK state没有普通KvTablet snapshot，继续从WAL log split tiering。
- 优化per-row `tableWrite.getPartition()`的缓存；第一版先保证正确性。

## 当前实现约束

### 1. `RecordWriter` 把 split partition 固定为唯一 Paimon partition

当前 constructor：

```text
WriterInitContext.partition()
    -> ResolvedPartitionSpec.fromPartitionName()
    -> PaimonConversions.toPaimonPartition()
    -> final BinaryRow partition
```

之后所有record都复用该字段。

这对normal split正确且高效，但historical split的`WriterInitContext.partition()`只是physical routing identity，不是row应写入的lake partition。

PR 5需要在base writer中显式区分：

```text
normal split     -> fixedPartition
historical split -> partition extracted from current row
```

不能把所有partitioned table都改成per-row extraction，否则会给normal tiering hot path增加无意义开销。

### 2. `__historical__` 可能在constructor阶段就转换失败

auto partition column可能是DATE、INT或其他typed field。`__historical__`只是Fluss system partition name中的保留值，不是合法Paimon business value。

当前constructor尝试把split partition name转换成typed Fluss/Paimon row。Historical writer甚至可能在读取第一条WAL record之前就失败。

因此一旦识别为historical split，constructor不能调用fixed partition conversion；它必须延迟到有真实row后从row fields提取typed partition。

### 3. `AppendOnlyWriter` 始终向fixed partition写

当前row path：

```java
flussRecordAsPaimonRow.setFlussRecord(record);
tableWrite.getWrite().write(partition, writtenBucket, flussRecordAsPaimonRow);
```

`partition`来自split，不来自record。一个historical bucket中的不同original partitions会全部进入同一错误partition。

PR 5应保留bucket-unaware `bucket=0`规则，只把write调用的partition参数替换为当前record解析出的partition。

### 4. `MergeTreeWriter` 的partition错误会造成跨partition merge

当前merge-tree path先提取trimmed primary key，再调用：

```java
tableWrite.getWrite().write(partition, bucket, keyValue);
```

若两个original partitions有相同trimmed primary key，但两条record都使用fixed `__historical__` partition，它们会进入同一Paimon partition/bucket/key namespace，产生错误覆盖或merge。

PR 5必须让`partition + bucket + trimmedPrimaryKey`组合继续反映original row identity。

### 5. Arrow direct-bundle path只能接收一个fixed partition

`AppendOnlyArrowBatchHelper.writeArrowBatch()`接收整个Arrow batch和单个partition：

```text
one ArrowBatchData
    -> one BinaryRow partition
    -> direct Parquet write
```

Historical batch可能包含多个original partitions。直接把split-level `__historical__`传入helper是错误的。

但不需要因此退回`LogScanner.poll()`。`AppendOnlyArrowBatchHelper`已经完成以下工作：

```text
ArrowBatchData
    -> append __bucket / __offset / __timestamp vectors
    -> ArrowBundleRecords
    -> Iterator<InternalRow>
```

PR 5可以复用该`ArrowBundleRecords`：

- normal split继续调用`tableWrite.writeBundle(fixedPartition, bucket, records)`；
- historical split迭代其中的`InternalRow`，对每行调用`tableWrite.getPartition(row)`，再调用row write。

这样保留batch fetch和Arrow列式decode，只有historical Paimon落盘从direct-bundle改为row write，不需要把Arrow batch转换成Fluss `ScanRecord`。

### 6. Batch fetch与Paimon write方式可以分离

`TieringSplitReader.useRecordBatchPath()`决定scanner调用`pollRecordBatch()`还是`poll()`。它不要求所有LakeWriter必须以相同方式消费batch内部的rows。

`ArrowScanRecords`按`TableBucket`保存batch，`processLogRecords()`再为每个bucket取得独立LakeWriter。因此同一次poll中可以安全地执行：

```text
normal bucket     -> normal Paimon writer -> direct writeBundle
historical bucket -> historical writer    -> iterate rows and dynamic partition write
```

PR 5不修改`TieringSplitReader.useRecordBatchPath()`。stopping offset截断、consumed-up-to offset和batch资源释放继续使用现有实现。

### 7. `RecordWriter.complete()` 强制只有一个commit message

当前实现：

```java
List<CommitMessage> commitMessages = tableWrite.prepareCommit();
checkState(commitMessages.size() == 1, ...);
return commitMessages.get(0);
```

一个historical writer写多个Paimon partitions后，当前Paimon会返回多条commit messages。继续强制size=1会在数据已写入writer buffer后失败，tiering无法commit或推进offset。

PR 5必须让historical complete返回完整non-empty list，而不是任选第一条或把多条丢弃。Normal writer继续保留现有`size == 1`断言，避免无意放宽normal contract。

### 8. `PaimonWriteResult` 和committer都是singular contract

当前链路：

```text
RecordWriter.complete() -> CommitMessage
PaimonWriteResult       -> one CommitMessage
serializer              -> serialize one CommitMessage
PaimonLakeCommitter      -> add one file committable per result
```

只改`RecordWriter`不够。Result、serializer和committer必须一起升级为list contract，否则会在Flink shuffle/checkpoint或commit阶段丢message。

### 9. write result serializer保留version 1并切换payload layout

`PaimonWriteResultSerializer`当前version为1，payload直接是一个`CommitMessageSerializer`结果。

`TableBucketWriteResultSerializer`自身的version固定为1，反序列化nested write result时会把这个outer version原样传给`PaimonWriteResultSerializer.deserialize()`。如果只把Paimon serializer升级为version 2，而不修改generic wrapper，新数据也会因为收到version 1而反序列化失败。

按本PR不考虑旧payload兼容的取舍，最小方案是让`PaimonWriteResultSerializer`继续返回version 1，但直接把version 1 payload从singleton替换为list layout。

需要：

- `CURRENT_VERSION`保持1；
- serialize使用`CommitMessageSerializer.serializeList(...)`写入完整list；
- deserialize使用当前`CommitMessageSerializer.getVersion()`和`deserializeList(...)`读取完整list；
- unknown outer version继续明确失败。

新旧payload拥有相同outer version但layout不同，因此旧checkpoint/savepoint中的singleton write result不能由新代码恢复。部署时需要从没有这类旧状态的clean/drained job启动；本PR不增加layout标记或迁移代码。

`PaimonCommittableSerializer`不需要变化，因为committer在构造`ManifestCommittable`前已经把所有messages flatten进去。

### 10. offset/timestamp推进不能依赖partition数量

一个historical bucket仍对应一个Fluss `TableBucket`和一个tiering split。即使row被写到多个Paimon partitions：

- stopping offset仍是bucket级exclusive offset；
- tiered offset仍是scanner consumed-up-to bound；
- max timestamp仍是最后实际写入record的timestamp；
- commit时所有partition messages必须原子包含在同一个table snapshot中；
- 不能按partition分别提前报告bucket tiered offset。

PR 5主要改writer routing和commit payload，不应拆分`TableBucketWriteResult`。

### 11. normal path性能和行为必须保持稳定

normal tiering占绝大多数流量。若PR 5把normal writer也改成每条row调用`tableWrite.getPartition()`，会增加conversion/extractor开销，并改变当前fixed partition validation时机。

实现必须显式保存`historicalPartition`和nullable/fixed partition状态，使normal write仍走现有字段读取。

## 核心设计

### 1. 在`PaimonLakeWriter`构造时识别historical split

使用公共metadata语义：

```java
boolean historicalPartition =
        writerInitContext.partition() != null
                && isHistoricalPartitionName(
                        writerInitContext.tableInfo(), writerInitContext.partition());
```

然后把boolean传给`AppendOnlyWriter`或`MergeTreeWriter`。

不要：

- 仅比较`partition.equals("__historical__")`，这会漏掉multi-level static prefix。
- 使用`endsWith("__historical__")`，这会误判普通字符串值。
- 根据row内容猜测split类型；split metadata才决定是否允许dynamic partition。

### 2. Base writer同时支持fixed和dynamic partition

`RecordWriter`保留一个明确状态：

```java
protected final boolean historicalPartition;
protected final @Nullable BinaryRow fixedPartition;
```

构造语义：

```text
non-partitioned normal -> fixedPartition = BinaryRow.EMPTY_ROW
partitioned normal     -> eagerly resolve split partition
historical             -> fixedPartition = null, skip __historical__ conversion
```

提供一个小型protected primitive，例如：

```java
protected BinaryRow prepareRecordAndGetPartition(LogRecord record) {
    flussRecordAsPaimonRow.setFlussRecord(record);
    return historicalPartition
            ? tableWrite.getPartition(flussRecordAsPaimonRow)
            : checkNotNull(fixedPartition);
}
```

该方法确保：

- 每条record只调用一次`setFlussRecord()`。
- dynamic partition从已经包装好的Paimon row提取。
- normal path只读cached fixed partition。
- schema padding、RowKind和system columns继续由`FlussRecordAsPaimonRow`处理。

不为单个选择逻辑新增writer registry或per-partition writer map；一个`TableWriteImpl`本身支持multi-partition write。

### 3. Append-only dynamic partition write

row path：

```java
BinaryRow targetPartition = prepareRecordAndGetPartition(record);
int targetBucket =
        bucketMode == BUCKET_UNAWARE ? 0 : flussBucket;
tableWrite.getWrite().write(
        targetPartition, targetBucket, flussRecordAsPaimonRow);
```

保持：

- row内容不变；
- APPEND_ONLY RowKind不变；
- Fluss offset/timestamp system columns不变；
- aware bucket继续使用Fluss bucket；
- bucket-unaware继续强制bucket 0。

Arrow table仍调用`writeArrowBatch()`；historical Arrow的dynamic partition处理由`AppendOnlyArrowBatchHelper`完成，见下一节。

### 4. Merge-tree dynamic partition write

顺序：

```java
BinaryRow targetPartition = prepareRecordAndGetPartition(record);
rowKeyExtractor.setRecord(flussRecordAsPaimonRow);
keyValue.replace(
        trimmedPrimaryKey,
        UNKNOWN_SEQUENCE,
        toRowKind(record.getChangeType()),
        flussRecordAsPaimonRow);
tableWrite.getWrite().write(targetPartition, bucket, keyValue);
```

partition extraction必须在同一record已设置到adapter之后执行。RowKeyExtractor也读取同一个adapter，不能分别包装两次或在切换record前缓存partition row。

DELETE/UPDATE_BEFORE继续使用WAL里的full row，因此能提取original partition并向Paimon产生正确RowKind。

### 5. Historical Arrow保留batch fetch，在writer内逐行写

`TieringSplitReader`继续执行现有record-batch path：

```text
LogScanner.pollRecordBatch()
    -> ArrowScanRecords grouped by TableBucket
    -> stopping-offset truncate
    -> PaimonLakeWriter.write(ArrowRecordBatch)
```

`AppendOnlyArrowBatchHelper`继续为batch补齐system columns并创建`ArrowBundleRecords`。之后按writer类型分支：

```java
if (!historicalPartition) {
    tableWrite.writeBundle(fixedPartition, writtenBucket, arrowBundleRecords);
} else {
    for (InternalRow row : arrowBundleRecords) {
        BinaryRow targetPartition = tableWrite.getPartition(row);
        tableWrite.getWrite().write(targetPartition, writtenBucket, row);
    }
}
```

具体签名可让`AppendOnlyWriter`把`historicalPartition`和nullable `fixedPartition`传给helper，不新增新的generic LakeWriter SPI。

关键边界：

- normal Arrow仍走`writeBundle()` direct-Parquet fast path；
- historical Arrow仍按batch从scanner和网络读取，但在Paimon TableWrite层逐行写；
- 不把Arrow row再转换成Fluss `ScanRecord`；
- 不按partition group/slice batch；
- `__bucket`、`__offset`、`__timestamp`继续由现有system vectors生成；
- stopping offset的truncate和batch ownership/close继续由`TieringSplitReader`处理。

### 6. Multi-message `PaimonWriteResult`

把singular result改为immutable non-empty list：

```java
public final class PaimonWriteResult implements Serializable {
    private final List<CommitMessage> commitMessages;

    public PaimonWriteResult(List<CommitMessage> commitMessages) { ... }

    public List<CommitMessage> commitMessages() { ... }
}
```

设计要求：

- defensive copy；
- caller不能修改内部list；
- 空list拒绝，避免无commit却推进offset；
- normal writer自然得到singleton list；
- 不在result里增加partition map，Paimon `CommitMessage`已携带提交所需identity。

`RecordWriter.complete()`按split类型处理：

```text
normal split     -> 保留 commitMessages.size() == 1
historical split -> 要求 non-empty，返回全部messages
```

两条路径都把list交给`PaimonWriteResult`；`PaimonLakeWriter.complete()`不再抽取单条message。

### 7. Version 1 list serializer

serializer继续返回version 1，payload直接使用Paimon `CommitMessageSerializer`的list API：

```text
DataOutputSerializer output
CommitMessageSerializer.serializeList(commitMessages, output)
return output.getCopyOfBuffer()
```

反序列化：

```text
version 1 -> wrap payload as DataInputView
             call CommitMessageSerializer.deserializeList(
                 CommitMessageSerializer.getVersion(), input)
             validate result is non-empty

other     -> UnsupportedOperationException
```

不新增旧singleton payload识别、nested version header或migration branch。新serializer只需要保证当前list layout round-trip。

### 8. Committer flatten

`PaimonLakeCommitter.toCommittable()`：

```java
for (PaimonWriteResult result : results) {
    for (CommitMessage message : result.commitMessages()) {
        manifestCommittable.addFileCommittable(message);
    }
}
```

所有historical partitions的messages进入同一个table-level `ManifestCommittable`，由现有`TableCommitImpl.commit()`原子提交到一个Paimon snapshot。

不改变：

- snapshot properties；
- Fluss table bucket offset map；
- readable snapshot计算；
- abort对manifest file committables的处理；
- `PaimonCommittableSerializer`。

### 9. Normal behavior boundary

normal split：

- constructor仍解析fixed partition并尽早暴露invalid partition错误；
- row writer不调用`tableWrite.getPartition()`；
- normal Arrow table继续走record-batch path和direct `writeBundle()`；
- normal writer继续要求singleton commit message；
- non-partitioned table继续使用`BinaryRow.EMPTY_ROW`；
- bucket-unaware rule不变。

historical split：

- constructor不转换`__historical__`；
- 每row dynamic partition；
- Arrow继续batch fetch，但在Paimon writer内逐行dynamic partition write；
- complete允许multi-message；
- 对符合FIP contract、row保留original partition values的historical WAL，lake metadata中不产生historical partition。

### 10. PR 5测试注入路径

由于正常client fallback属于PR 6，PR 5 focused/IT tests不通过修改生产routing来制造historical data。

required focused test：

1. `PaimonTieringTest`直接构造historical `WriterInitContext`：
   - physical partition name设为`__historical__`或static-prefix historical name；
   - 写入row partition columns为多个original values的`LogRecord`；
   - 对Arrow append-only table额外构造包含多个original partitions的`ArrowBatchData`；
   - complete、serialize、commit并读取Paimon验证。

若现有低层test helper可以零production改动地把显式historical `WriteRecord`送入真实server，可额外在`PaimonTieringITCase`覆盖真实WAL；否则完整client/server/tiering E2E明确留给PR 6，不能为了PR 5测试新增production writer API。

## 详细实施步骤

### 步骤 1：在writer init时识别historical split

- 在`PaimonLakeWriter`读取`WriterInitContext.tableInfo()`和`partition()`。
- 使用`PartitionUtils.isHistoricalPartitionName()`计算boolean。
- 把boolean传给append-only/merge-tree writer。
- boolean只用于record writer的fixed/dynamic partition和complete contract。

验证：single-key、multi-level和normal partition detection。

### 步骤 2：改造`RecordWriter` partition选择

- 将当前固定`partition`字段改为语义明确的`fixedPartition`。
- historical constructor跳过split partition typed conversion。
- 增加`prepareRecordAndGetPartition()`小型shared primitive。
- normal path继续返回cached fixed partition。
- historical path调用`tableWrite.getPartition(flussRecordAsPaimonRow)`。
- 保留`resolvePartition()`作为normal constructor helper。

验证：normal conversion调用次数和historical dynamic结果。

### 步骤 3：接入append-only和merge-tree writer

- `AppendOnlyWriter.write()`使用per-record selected partition。
- 保留bucket-unaware `0`和bucket-aware Fluss bucket。
- `MergeTreeWriter.write()`使用per-record selected partition。
- 保留RowKeyExtractor、KeyValue reuse和RowKind mapping。
- 不修改row payload或key extractor。

验证：append/PK parameterized tests和same-key isolation。

### 步骤 4：让historical Arrow batch在Paimon writer内逐行写

- 保持`TieringSplitReader.pollRecordBatch()`和`handleArrowBatchRecords()`不变。
- `AppendOnlyWriter.writeArrowBatch()`把fixed/historical partition状态传给helper。
- `AppendOnlyArrowBatchHelper`复用已有enriched root和`ArrowBundleRecords`。
- normal branch继续调用`tableWrite.writeBundle()`。
- historical branch迭代`InternalRow`，逐行提取partition并调用row write。
- 保留bucket-unaware `0`和bucket-aware Fluss bucket。

验证：normal Arrow仍调用direct bundle；historical Arrow仍接收batch，但rows写入多个original partitions。

### 步骤 5：按split类型处理`RecordWriter.complete()`

- historical writer返回完整non-empty `List<CommitMessage>`。
- normal writer保留`size == 1` check并把singleton包装为list。
- 两条路径都保留zero-message防御。
- `PaimonLakeWriter.complete()`构造list-based result。

验证：一个historical writer写两个original partitions后返回两条messages；normal writer仍返回singleton。

### 步骤 6：升级write result和serializer

- `PaimonWriteResult`保存immutable list。
- `PaimonWriteResultSerializer.CURRENT_VERSION`保持1。
- version 1直接复用`CommitMessageSerializer.serializeList/deserializeList`。
- 不增加old singleton payload分支。
- unknown version继续失败。

验证：`getVersion() == 1`、v1 list singleton、v1 list multi-message和unknown version rejection。

### 步骤 7：升级committer aggregation

- `PaimonLakeCommitter.toCommittable()` flatten每个result的全部messages。
- 保持一个table round只产生一个ManifestCommittable和一个Paimon snapshot。
- abort/readable snapshot/stats逻辑不变。

验证：multi-result + multi-message都被commit，Paimon两个original partitions可读。

### 步骤 8：补focused Paimon tests

- 扩展`PaimonTieringTest`覆盖historical append/PK。
- 覆盖historical Arrow batch逐行dynamic partition write和normal Arrow direct-bundle regression。
- 新增serializer focused test，或在`PaimonTieringTest`中形成独立清晰section。
- 验证normal fixed partition regression。
- 验证Paimon partition list不存在historical value。

### 步骤 9：补historical Arrow writer tests

- 构造一个包含两个original partitions的`ArrowBatchData`。
- 通过historical `PaimonLakeWriter.write(ArrowRecordBatch)`写入。
- 验证两个original Paimon partitions分别可读。
- 验证offset按`baseOffset + rowIndex`生成，timestamp和bucket保持现有语义。
- normal Arrow case继续验证direct-bundle结果不变。
- 不为本变化新增`TieringSplitReader`分支测试；现有truncate、ownership和progress tests继续覆盖generic batch path。

### 步骤 10：按现有test plumbing决定是否补真实WAL ITCase

- required coverage由Paimon writer/Arrow/serde/committer focused tests完成。
- 若现有低层helper可以直接发送显式historical record，则补真实WAL tiering ITCase。
- 不为该ITCase增加production client/server接口。
- 完整client -> server -> tiering E2E由PR 6保证。

### 步骤 11：执行normal regression和新serializer验证

- Paimon normal partition/non-partitioned writer。
- multi-level normal partition。
- normal PK merge和append-only。
- normal Arrow fast path。
- write result serialization/committer。
- Flink tiering source/commit operator regression。
- Spotless/license checks。

## 预计文件范围

实际实现可按当前分支Paimon版本做小幅调整，但不应把改动扩散到其他lake plugin。

### fluss-lake-paimon main

修改：

```text
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/PaimonLakeWriter.java
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/RecordWriter.java
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/append/AppendOnlyWriter.java
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/append/AppendOnlyArrowBatchHelper.java
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/mergetree/MergeTreeWriter.java
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/PaimonWriteResult.java
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/PaimonWriteResultSerializer.java
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/PaimonLakeCommitter.java
```

预计不修改：

```text
PaimonCommittable.java
PaimonCommittableSerializer.java
PaimonLakeCatalog.java
PaimonLakeTableLookuper.java
```

### fluss-flink-common main

预计不修改：

```text
fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/TieringSplitReader.java
```

Historical Arrow继续复用现有record-batch poll、truncate、progress和resource ownership逻辑。

### tests

修改：

```text
fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/tiering/PaimonTieringTest.java
```

按需修改：

```text
fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/tiering/PaimonTieringITCase.java
```

只有现有test helper可以在不增加production plumbing的前提下直接注入historical WAL时，才补该ITCase。

建议新增：

```text
fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/tiering/PaimonWriteResultSerializerTest.java
```

若`PaimonTieringTest`已能清晰覆盖serializer新layout，可不新增单独test class；不要同时重复两套相同assertion。

### 不应修改

除非focused test暴露前序PR的明确bug，本PR不修改：

```text
fluss-rpc protobuf
fluss-client WriterClient / DynamicPartitionCreator
fluss-server historical KV/write/recovery classes
other lake plugins
generic tiering commit operator state model
```

## 测试计划

### 1. Normal fixed-partition writer regression

append-only和PK分别覆盖：

- non-partitioned table使用`BinaryRow.EMPTY_ROW`。
- normal single-key partition写入指定Paimon partition。
- normal multi-level partition正确typed conversion。
- normal writer不调用dynamic partition extractor。
- normal bucket-aware table使用Fluss bucket。
- normal bucket-unaware append-only table使用bucket 0。
- normal complete/commit和读取结果不变。

### 2. Historical append-only writer

在一个writer中依次写：

```text
row A -> original partition 20240101
row B -> original partition 20240102
row C -> original partition 20240101
```

验证：

- constructor接受`WriterInitContext.partition() = __historical__`。
- 每条row从自身partition column提取target。
- Paimon `20240101`含A/C，`20240102`含B。
- offset/timestamp system columns保持各record值。
- bucket system column与expected bucket一致。
- 对测试中符合FIP contract的rows，Paimon partition list不含`__historical__`。

分别覆盖bucket-aware和bucket-unaware模式，后者的Paimon bucket必须为0。

### 3. Historical primary-key writer

验证：

- 同一historical writer写多个original partitions。
- 不同partitions中相同trimmed primary key保存为两个独立row。
- 同一original partition中upsert/update按Paimon merge语义生效。
- UPDATE_BEFORE/UPDATE_AFTER RowKind保持正确。
- DELETE full row可提取original partition并删除正确partition中的key。
- 不会影响另一个partition的same key。

### 4. Multi-level historical partition

table partition keys例如`[region, dt]`，auto key为`dt`：

- physical split为`region=us$dt=__historical__`。
- rows可包含`us + 20240101`和`us + 20240102`。
- writer根据row写入两个`us + original dt` Paimon partitions。
- 不允许row被写到`us + __historical__`。

另一个physical split`region=eu$dt=__historical__`独立验证static prefix。PR 3已校验RPC original与actual static prefix，PR 5仍以row columns作为最终Paimon target。

### 5. Multi-message complete

- 一个writer写至少两个Paimon `(partition, bucket)` targets，当前Paimon应返回两条messages。
- `prepareCommit()`返回的全部messages保留在`PaimonWriteResult`。
- 不再触发size=1 exception。
- result list不可被caller修改。
- empty commit message list明确失败。
- normal singleton complete仍成功。
- normal writer若意外得到多条messages仍按原contract失败。

### 6. Serializer new layout

- `getVersion()`保持1。
- version 1 list singleton result serialize/deserialize。
- version 1 list multi-message result serialize/deserialize。
- 不测试或识别旧version 1 singleton payload。
- corrupted/zero-length list payload拒绝。
- truncated nested payload拒绝为`IOException`。
- unknown version拒绝。

### 7. Committer aggregation

输入：

```text
result 1 -> message A, message B
result 2 -> message C
```

验证：

- manifest committable包含A/B/C。
- commit只产生一个table snapshot。
- original partitions的数据全部可读。
- snapshot properties和Fluss bucket offset properties不丢失。
- abort覆盖所有file committables。

### 8. Historical Arrow batch逐行写

Paimon writer focused test覆盖：

- normal ARROW append-only split继续接收`ArrowBatchData`并调用direct `writeBundle()`。
- historical ARROW split继续接收`ArrowBatchData`，不回退到`LogScanner.poll()`。
- 一个batch包含两个original partitions时，helper迭代rows并写入两个Paimon partitions。
- normal和historical buckets同时存在时，各自writer独立选择direct-bundle或row write。
- Arrow batch中的system column值与现有normal batch语义一致。

### 9. Offset/timestamp semantics

historical Arrow row write验证：

- stopping offset是exclusive。
- offset等于stopping offset的record不写入。
- consumed-up-to超过stopping offset时tiered progress被cap。
- empty/filtered records仍按现有规则推进consumed offset。
- max tiered timestamp取最后实际写入record。
- 一个bucket写多个Paimon partitions后仍只报告一个bucket-level log end offset。

### 10. Optional historical WAL Paimon ITCase

如果现有低层test helper可直接复用：

append-only：

- Admin显式创建historical Fluss partition。
- test-only向其写入两个original partitions的rows。
- 运行真实tiering job。
- original Paimon partitions分别可读。
- 对符合FIP contract、row保留original partition values的测试数据，Paimon中不产生historical partition。

primary-key：

- 两个original partitions使用相同PK。
- historical WAL tiering后两个Paimon partitions状态隔离。
- update/delete落到正确original partition。
- Fluss historical bucket tiered offset推进到stopping offset。

该ITCase不调用PR 6 expired fallback resolver，也不新增production plumbing。若当前test infrastructure无法低成本注入historical WAL，则这些完整链路case移到PR 6；PR 5仍必须完成writer/serde/committer和historical Arrow focused coverage。

### 11. Existing regression

- `PaimonTieringTest` normal matrix。
- `PaimonTieringITCase` normal table/partition flows。
- normal multi-partition和three-partition tests。
- normal Arrow tiering。
- snapshot/partition expiration tests。
- `TieringSplitReaderTest` row path、empty batch、FIRST_ROW progress。
- historical lookup ITCase；PR 5不能破坏Paimon row adapter和snapshot可见性。

## 兼容性

### Fluss wire/state compatibility

- 不修改client/server RPC。
- 不修改Fluss WAL、KV snapshot、lake snapshot offset property或TieringSplit serializer。
- historical split仍以physical `TableBucket`为offset/recovery identity。

### Paimon data compatibility

- normal existing Paimon partitions/files继续可读。
- historical rows写入existing/original Paimon partition schema。
- 不创建新的Paimon system partition layout。
- commit仍使用当前commit user和single table snapshot。

### Write result serializer layout边界

- writer升级后仍输出version 1，但payload改为list layout。
- 新serializer不支持旧version 1 singleton payload；相同version号不表示layout兼容。
- 新旧serializer不能恢复彼此的in-flight write result；升级前应drain/clean stop tiering job，不能从包含旧singleton write result的checkpoint/savepoint恢复。
- `PaimonCommittableSerializer`保持version不变。

### Old tiering -> new server/client

- old tiering不理解historical multi-partition split。
- PR 6 client fallback不能在PR 5 tiering部署前打开。
- normal split继续工作。

### New tiering -> old client

- old client不会产生historical writes。
- new tiering对normal split保持fixed-partition和Arrow fast path。
- 可以先部署PR 5，为PR 6 rollout做准备。

## 本 PR 必须防住的风险

### 风险 1：把所有partitioned row都改成dynamic extraction

后果：normal tiering hot path增加per-record partition开销，fixed partition validation时机改变。

防护：constructor明确区分historical boolean；normal继续eager fixed partition。

### 风险 2：constructor仍尝试转换`__historical__`

后果：typed auto partition在第一条record前失败，historical tiering完全不可用。

防护：historical split设置`fixedPartition=null`并跳过`resolvePartition()`。

### 风险 3：dynamic partition在设置current record前提取

后果：`FlussRecordAsPaimonRow`仍指向上一条record或未初始化，row写入错误partition。

防护：base primitive固定执行`setFlussRecord(record)`后再`tableWrite.getPartition()`。

### 风险 4：historical PK same key跨partition merge

后果：一个original partition的update/delete覆盖另一个partition的数据。

防护：每条record使用row-derived partition + current bucket + trimmed PK；focused test使用same key跨两个partition。

### 风险 5：bucket-unaware append-only写到Fluss bucket

后果：违反Paimon bucket-unaware contract，产生错误layout或额外bucket。

防护：只替换partition选择，不改变现有bucket=0分支。

### 风险 6：historical Arrow仍调用fixed-partition `writeBundle()`

后果：整个batch使用split-level historical partition，数据写错或conversion失败。

防护：historical helper branch不调用`writeBundle()`，而是迭代`ArrowBundleRecords`，逐行提取partition后调用row write。

### 风险 7：Arrow逐行写丢失system columns或越过stopping offset

后果：Paimon row的bucket/offset/timestamp错误，或写入不属于当前split的rows。

防护：先复用helper现有enriched root生成system vectors，再迭代Paimon rows；继续由`TieringSplitReader`在调用writer前truncate batch，并增加focused test。

### 风险 8：只保留第一个commit message

后果：部分original partitions的files未commit，但Fluss tiered offset已推进，造成永久数据丢失。

防护：complete/result/serializer/committer端到端使用list并测试multi-message commit。

### 风险 9：单独把Paimon serializer升级为version 2

后果：`TableBucketWriteResultSerializer`反序列化时仍传入outer version 1，导致所有新Paimon write result反序列化失败。

防护：Paimon serializer保持version 1并直接替换payload layout；focused test显式断言`getVersion() == 1`和current-layout round-trip。

### 风险 10：commit messages分多个Paimon snapshot提交

后果：同一Fluss bucket offset对应部分可见lake状态，cleanup/lookup可能观察不一致。

防护：flatten到同一个ManifestCommittable并通过一次TableCommit提交。

### 风险 11：row partition columns被替换成historical value

后果：Paimon出现`__historical__` partition，用户数据partition identity丢失。

防护：writer只读取row partition fields，不修改adapter row；ITCase断言Paimon partition list。

### 风险 12：multi-level static prefix解析错误

后果：region/tenant之间数据串写。

防护：split detection使用完整table metadata，dynamic target直接从完整row partition fields提取；覆盖auto key非首列。

### 风险 13：zero commit message仍推进tiered offset

后果：Lake没有durable data但Fluss认为bucket已tiered并可能cleanup local state。

防护：有writer/record时complete要求non-empty messages；无record场景继续由TieringSplitReader返回null write result。

### 风险 14：PR 5测试意外提前打开client fallback

后果：PR边界扩大，review同时涉及client routing和lake correctness。

防护：test-only显式historical target或直接WriterInitContext注入；production WriterClient留给PR 6。

### 风险 15：normal Arrow也退化为逐行写

后果：normal Arrow tiering失去现有direct-Parquet batch性能。

防护：逐行逻辑只放在historical writer branch；normal branch继续调用现有`tableWrite.writeBundle()`，并保留normal Arrow regression test。

### 风险 16：Arrow row view在batch关闭后仍被使用

后果：`InternalRow`底层vectors已释放，commit时出现数据损坏或非法内存访问。

防护：在`writeArrowBatch()`调用内同步完成iterator遍历和`tableWrite.getWrite().write()`，不缓存row view；测试在关闭`ArrowRecordBatch`后再complete/commit并验证结果。

## 验证命令

先运行Paimon focused tests：

```bash
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=PaimonTieringTest
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=PaimonWriteResultSerializerTest
```

运行Paimon ITCases：

```bash
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=PaimonTieringITCase
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=HistoricalPartitionLookupITCase
```

运行affected modules：

```bash
./mvnw verify -pl fluss-lake/fluss-lake-paimon -am
```

格式与静态检查：

```bash
./mvnw spotless:check
```

如果historical WAL ITCase可以复用现有test plumbing并被单独新增，则把对应`-Dtest=...`加入上述命令。PR合并前必须完成Paimon writer/Arrow/serde/committer focused tests；完整historical WAL E2E最迟由PR 6完成。

## 完成标准

writer routing：

- [ ] historical split不会解析fixed `__historical__` Paimon partition。
- [ ] normal split继续使用cached fixed partition。
- [ ] historical append-only row按row fields写入original Paimon partition。
- [ ] historical PK row按row fields写入original Paimon partition。
- [ ] same PK跨original partitions不会merge。
- [ ] bucket-aware/bucket-unaware规则不变。
- [ ] row payload、RowKind、offset和timestamp不变。

Arrow：

- [ ] historical Arrow继续通过`pollRecordBatch()`读取。
- [ ] historical Arrow在Paimon writer内逐行提取dynamic partition。
- [ ] normal Arrow继续使用direct `writeBundle()` fast path。
- [ ] historical逐行写保持bucket/offset/timestamp和exclusive stopping-offset语义。

commit：

- [ ] historical `RecordWriter.complete()`保留全部non-empty commit messages，normal writer继续要求singleton。
- [ ] `PaimonWriteResult`保存immutable list。
- [ ] serializer保持version 1并支持list multi-message。
- [ ] 不增加旧singleton兼容分支，unknown version明确失败。
- [ ] committer flatten全部messages并单snapshot提交。
- [ ] `PaimonCommittableSerializer`无需变化。

tests：

- [ ] Paimon normal writer regression通过。
- [ ] historical append/PK focused tests通过。
- [ ] multi-level/same-key isolation通过。
- [ ] historical Arrow batch逐行写和normal direct-bundle tests通过。
- [ ] 对符合FIP contract的historical rows，Paimon metadata中不产生`__historical__` partition。
- [ ] 若PR 5未增加真实WAL ITCase，对应E2E已明确列入PR 6完成标准。
- [ ] affected modules verify通过。
- [ ] Spotless通过。

## 合并后的行为

PR 5合并后，Paimon tiering可以正确消费historical Fluss split：

```text
one historical Fluss TableBucket/WAL
    -> rows from multiple original partitions
    -> one Paimon TableWriteImpl
    -> dynamic Paimon partition per row
    -> multiple CommitMessages for multiple touched partitions
    -> one atomic Paimon table snapshot
    -> one Fluss bucket-level tiered offset
```

normal split继续使用fixed partition和Arrow direct-bundle fast path。

合并后仍不会发生：

- 正常client不会自动把expired write重定向到historical partition。
- PR 5不会创建historical KV state或处理server write。
- historical Arrow保留batch fetch，但不会使用single-partition direct-bundle write。
- tiered offset不会细分到original partition。

PR 6合并并rollout后，才会由正常AppendWriter/UpsertWriter端到端地产生这类historical WAL。
