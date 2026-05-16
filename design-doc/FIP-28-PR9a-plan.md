# PR 9a: `__historical__` 分区 Recovery — composite key WAL 回放 + 跳过快照

## Context

当前 `__historical__` 分区使用与普通分区完全相同的恢复路径（`Replica.initKvTablet()` → snapshot 下载 → `KvRecoverHelper.recover()` WAL 回放）。这存在两个关键问题：

### 问题 1: Recovery 时 key 格式不一致（正确性 bug）

正常写入时，`KvTablet.processKvRecords()` 对 `__historical__` 分区使用 `CompositeKeyEncoder.encode(partitionName, rawKeyBytes)` 编码 key，格式为 `[4-byte len][partitionName UTF-8][originalKey]`。

但 recovery 时，`KvRecoverHelper.applyLogRecordBatch()` 使用 `KeyEncoder.encodeKey(logRow)` 从 log row 重新编码 key，产生的是 **plain key**（不含 partition name 前缀）。

这导致 recovery 后 RocksDB 中的 key 是 plain 格式，而后续写入使用 composite 格式——两种格式共存会导致：
- lookup 命中不到 recovery 恢复的数据（用 composite key 查不到 plain key）
- 同一 PK 被当作两条不同记录（plain key 和 composite key 不碰撞），upsert 变成 insert

### 问题 2: 不必要的 snapshot 开销

FIP-28 设计文档 A.3.4 明确指出 historical RocksDB 应该是 **non-persistent**（无 snapshot/checkpoint），recovery 始终从 clean RocksDB + WAL replay 完成。但当前实现会：
- 对 `__historical__` 进行周期性 snapshot（`startPeriodicKvSnapshot()`）
- recovery 时尝试下载 snapshot（`getLatestSnapshot()` → `downloadKvSnapshots()`）

snapshot 中的 key 同样存在格式问题——如果 snapshot 在当前有 bug 的代码下生成，其中包含的 key 格式不正确。

**`__historical__` 分区不需要 snapshot 的根本原因**：它的 RocksDB 随时可以被 drop 掉（cleanup 后删除），recovery 只需从 tiered offset 开始 replay WAL 即可重建完整状态。replay 数据量 = `logEnd - tieredOffset`，只要 tiering 正常运行就是有界的小量。因此 snapshot 是纯开销——浪费 I/O、存储和恢复时间（下载 + 加载），却没有收益。

### 本 PR 目标

1. **修复 recovery key 编码**：WAL 回放时，对 `__historical__` 分区使用 composite key 编码
2. **禁用 snapshot**：`__historical__` 分区不创建 snapshot（不启动 `PeriodicSnapshotManager`），recovery 始终从 clean RocksDB + tiered offset WAL replay 恢复
3. **使用 tiered offset 作为回放起点**：减少 recovery 需要回放的数据量

---

## 现状分析

### Recovery 流程（当前）

```
Replica.initKvTablet()
  → getLatestSnapshot(tableBucket)              // 尝试找 snapshot
  → if snapshot: downloadKvSnapshots() + loadKv()  // 下载并加载
  → else: kvManager.getOrCreateKv()              // 创建空 RocksDB
  → recoverKvTablet(startOffset, ...)
      → KvRecoverHelper.recover()
          → Phase 1: replay [startOffset, HW) → kvBatchWriter.put(key, value)
          → Phase 2: replay [HW, logEnd)       → kvTablet.putToPreWriteBuffer()
          → key = keyEncoder.encodeKey(logRow)  // ❌ plain key, 无 composite 编码
```

### Recovery 流程（目标）

```
Replica.initKvTablet()
  → if isHistoricalPartition:
      → skip snapshot, always create fresh RocksDB
      → startOffset = max(tieredOffset, logStartOffset)  // 从 tiered offset 开始
      → recoverKvTablet(startOffset, ...)
          → KvRecoverHelper.recover()
              → 从 log row 提取 partitionName (分区列值)
              → compositeKey = CompositeKeyEncoder.encode(partitionName, plainKey)
              → kvBatchWriter.put(compositeKey, value)  // ✅ composite key
  → else:
      → 原有流程不变
```

### 关键已有基础设施

| 组件 | 状态 | 说明 |
|------|------|------|
| `KvTablet.isHistoricalPartition` | ✅ 已有 | 通过 `PartitionUtils.isHistoricalPartitionName()` 判断 |
| `CompositeKeyEncoder.encode()` | ✅ 已有 | `[4-byte len][partitionName][originalKey]` |
| `LogTablet.lakeLogEndOffset` | ✅ 已有 | tiered offset，recovery 可用作起点 |
| `Replica.startPeriodicKvSnapshot()` | ✅ 已有 | 需为 historical 分区跳过 |
| `KvRecoverHelper` | 需修改 | 增加 historical 分区的 composite key 编码 |

---

## 改动范围

| # | 文件 | 操作 | 说明 |
|---|------|------|------|
| 1 | `fluss-server/.../replica/Replica.java` | 修改 | `initKvTablet()` 为 historical 分区跳过 snapshot、使用 tiered offset；`startPeriodicKvSnapshot()` 跳过 historical |
| 2 | `fluss-server/.../kv/KvRecoverHelper.java` | 修改 | 增加 historical 模式：从 log row 提取 partitionName → composite key 编码 |
| 3 | `fluss-server/.../kv/KvTablet.java` | 修改 | 暴露 `isHistoricalPartition()` getter 供 `Replica` 使用 |
| 4 | `fluss-server/.../replica/Replica.java` | 修改 | `recoverKvTablet()` 传递 historical 信息给 `KvRecoverHelper` |
| 5 | `fluss-client/...test.../HistoricalPartitionTableITCase.java` | 修改 | 新增 recovery 测试 |

---

## 详细设计

### Step 1: `KvTablet` 暴露 `isHistoricalPartition()`

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`

```java
/** Returns true if this tablet belongs to a __historical__ partition. */
public boolean isHistoricalPartition() {
    return isHistoricalPartition;
}
```

### Step 2: `Replica.initKvTablet()` — historical 分区特殊处理

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`

在 `initKvTablet()` 中增加分支：

```java
private Optional<CompletedSnapshot> initKvTablet() {
    checkNotNull(kvManager);
    long startTime = clock.milliseconds();

    boolean isHistorical = PartitionUtils.isHistoricalPartitionName(
            physicalPath.getPartitionName());

    long restoreStartOffset = 0;
    Optional<CompletedSnapshot> optCompletedSnapshot = Optional.empty();

    try {
        Long rowCount;
        AutoIncIDRange autoIncIDRange;

        if (isHistorical) {
            // Historical partition: always start from clean RocksDB, no snapshot.
            // Per FIP-28 A.3.4: "delete the historical RocksDB data directory if
            // it exists, then create a fresh empty RocksDB instance."
            LOG.info("Historical partition {} — skipping snapshot, creating fresh RocksDB.",
                    tableBucket);
            kvTablet = kvManager.getOrCreateKv(
                    physicalPath, tableBucket, logTablet,
                    tableConfig.getKvFormat(), schemaGetter,
                    tableConfig, arrowCompressionInfo);

            // Use tiered offset as recovery start to minimize replay volume.
            // tieredOffset = lakeLogEndOffset (exclusive next-offset semantics).
            long tieredOffset = logTablet.getLakeLogEndOffset();
            if (tieredOffset > 0) {
                restoreStartOffset = tieredOffset;
            }
            rowCount = null;  // historical partition doesn't track row count
            autoIncIDRange = null;
        } else if (optCompletedSnapshot.isPresent()) {
            // ... 原有 snapshot 恢复逻辑 ...
        } else {
            // ... 原有无 snapshot 恢复逻辑 ...
        }

        // Set lake table lookuper (unchanged)
        if (lakeTableLookuper != null) {
            kvTablet.setLakeTableLookuper(lakeTableLookuper, tableInfo.getPartitionKeys());
        }

        logTablet.updateMinRetainOffset(restoreStartOffset);
        recoverKvTablet(restoreStartOffset, rowCount, autoIncIDRange);
    } catch (Exception e) { ... }

    return optCompletedSnapshot;
}
```

**注意**：需要重新组织 `if-else` 分支，将 `optCompletedSnapshot = getLatestSnapshot()` 的调用移到非 historical 分支中。

### Step 3: 禁用 `__historical__` 的周期性 snapshot

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`

`__historical__` 的 RocksDB 随时可以被 drop（cleanup），recovery 从 tiered offset replay WAL 即可，不需要 snapshot。直接不启动 `PeriodicSnapshotManager`：

```java
private void createKv(...) {
    // ...
    snapshotUsed = initKvTablet();

    // Historical partitions: RocksDB can be dropped at any time (cleanup after tiering),
    // recovery replays WAL from tiered offset. No snapshot needed — skip entirely.
    if (!PartitionUtils.isHistoricalPartitionName(physicalPath.getPartitionName())) {
        startPeriodicKvSnapshot(snapshotUsed.orElse(null));
    }
}
```

### Step 4: `KvRecoverHelper` — 支持 historical composite key 编码

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/kv/KvRecoverHelper.java`

核心改动：增加 historical 模式参数，在 `applyLogRecordBatch()` 中对 key 做 composite 编码。

#### 4a. 构造函数增加 historical 参数

```java
public KvRecoverHelper(
        KvTablet kvTablet,
        LogTablet logTablet,
        long recoverPointOffset,
        @Nullable Long recoverPointRowCount,
        @Nullable AutoIncIDRange autoIncRange,
        KvRecoverContext recoverContext,
        KvFormat kvFormat,
        LogFormat logFormat,
        SchemaGetter schemaGetter,
        RemoteLogFetcher remoteLogFetcher,
        boolean isHistoricalPartition,          // 新增
        @Nullable List<String> partitionKeys)   // 新增：分区列名列表
```

新增字段：
```java
private final boolean isHistoricalPartition;
@Nullable private final List<String> partitionKeys;
// 延迟初始化：分区列在 schema rowType 中的索引
private int partitionColumnIndex = -1;
```

#### 4b. `initSchema()` 中初始化分区列索引

```java
private void initSchema(int schemaId) throws Exception {
    // ... 原有逻辑 ...

    if (isHistoricalPartition && partitionKeys != null && !partitionKeys.isEmpty()) {
        // 找到分区列在 rowType 中的索引
        String partitionColumn = partitionKeys.get(0);  // 目前只支持单分区列
        List<String> fieldNames = currentRowType.getFieldNames();
        partitionColumnIndex = fieldNames.indexOf(partitionColumn);
        if (partitionColumnIndex < 0) {
            throw new IllegalStateException(
                    "Partition column '" + partitionColumn + "' not found in row type: "
                    + currentRowType);
        }
    }
}
```

#### 4c. `applyLogRecordBatch()` 中 composite key 编码

```java
private long applyLogRecordBatch(...) throws Exception {
    try (CloseableIterator<LogRecord> logRecordIter = logRecordBatch.records(readContext)) {
        while (logRecordIter.hasNext()) {
            LogRecord logRecord = logRecordIter.next();
            ChangeType changeType = logRecord.getChangeType();
            rowCountUpdater.applyChange(changeType);

            if (changeType != ChangeType.UPDATE_BEFORE) {
                InternalRow logRow = logRecord.getRow();
                byte[] key = keyEncoder.encodeKey(logRow);

                // Historical partition: encode as composite key with partition name prefix
                if (isHistoricalPartition && partitionColumnIndex >= 0) {
                    String partitionName = logRow.getString(partitionColumnIndex).toString();
                    key = CompositeKeyEncoder.encode(partitionName, key);
                }

                byte[] value = null;
                if (changeType != ChangeType.DELETE) {
                    BinaryRow row = toKvRow(logRow);
                    value = ValueEncoder.encodeValue(currentSchemaId.shortValue(), row);
                }
                resumeRecordConsumer.accept(
                        new KeyValueAndLogOffset(changeType, key, value, logRecord.logOffset()));

                autoIncIdRangeUpdater.applyRecord(changeType, logRow);
            }
        }
    }
    return logRecordBatch.nextLogOffset();
}
```

### Step 5: `Replica.recoverKvTablet()` — 传递 historical 信息

**文件**: `fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`

```java
private void recoverKvTablet(
        long startRecoverLogOffset,
        @Nullable Long rowCount,
        @Nullable AutoIncIDRange autoIncIDRange) {
    // ...
    boolean isHistorical = kvTablet.isHistoricalPartition();
    List<String> partitionKeys = isHistorical ? tableInfo.getPartitionKeys() : null;

    KvRecoverHelper kvRecoverHelper =
            new KvRecoverHelper(
                    kvTablet, logTablet,
                    startRecoverLogOffset, rowCount, autoIncIDRange,
                    recoverContext,
                    tableConfig.getKvFormat(),
                    tableConfig.getLogFormat(),
                    schemaGetter, remoteLogFetcher,
                    isHistorical,     // 新增
                    partitionKeys);   // 新增
    kvRecoverHelper.recover();
    // ...
}
```

---

## 不需要修改的组件

| 组件 | 原因 |
|------|------|
| `HistoricalKvManager.java` | 当前为死代码（未在生产代码中使用），本 PR 不集成它。`__historical__` 继续使用标准 `KvTablet` 路径，只是跳过 snapshot 和修复 key 编码。后续如需切换到 non-persistent RocksDB 可作为独立 PR。 |
| `HistoricalPartitionHandler.java` | 仅负责 flow control 和任务调度，不涉及 recovery |
| `CompositeKeyEncoder.java` | 已有，直接复用 |
| `KvManager.java` | `getOrCreateKv()` 已支持创建空 RocksDB |
| `ReplicaManager.java` | recovery 路径不经过 `ReplicaManager`，在 `Replica` 层处理 |

---

## 测试

### 单元测试: `KvRecoverHelperTest`

1. **testHistoricalRecoveryUsesCompositeKeys**: 写入含不同分区列值的 log records → recovery → 验证 RocksDB 中存的是 composite key
2. **testNormalRecoveryUnchanged**: 普通分区 recovery 不受影响，仍使用 plain key

### 集成测试: `KvHistoricalPartitionReplicaRestoreITCase`

在 `fluss-server/src/test/java/org/apache/fluss/server/replica/KvHistoricalPartitionReplicaRestoreITCase.java` 中新增独立的 historical 分区 recovery 测试类（使用单独的 `FlussClusterExtension`，配置 `DATALAKE_FORMAT=PAIMON`，避免影响 `KvReplicaRestoreITCase` 中已有的非 datalake 测试）：

1. **testHistoricalPartitionRecovery**:
   - 写入过期分区 → 数据进入 `__historical__` RocksDB
   - 触发 leader 切换 / `makeLeader` 重新初始化
   - recovery 完成后 lookup → 返回正确值（验证 composite key 格式一致）

2. **testHistoricalRecoveryFromTieredOffset**:
   - 写入 → tiering 完成 → tieredOffset 推进
   - 重启 → recovery 从 tieredOffset 开始，不回放已 tiered 的数据
   - lookup 确认数据正确

3. **testHistoricalRecoverySkipsSnapshot**:
   - 验证 `__historical__` 分区不创建 snapshot、recovery 不尝试下载 snapshot

---

## 验证步骤

```bash
# 1. 格式化
./mvnw spotless:apply -pl fluss-server -q

# 2. 编译
./mvnw test-compile -pl fluss-server -am -q

# 3. 运行 recovery helper 单元测试
./mvnw test -Dtest=KvRecoverHelperTest -pl fluss-server -am

# 4. 运行 recovery 集成测试
./mvnw test -Dtest=KvHistoricalPartitionReplicaRestoreITCase -pl fluss-server -am

# 5. 运行已有的 KvReplicaRestoreITCase 确认无回归
./mvnw test -Dtest="KvReplicaRestoreITCase#testRestore+testRowCountRecoveryAfterFailover" -pl fluss-server -am
```

---

## 风险与开放问题

1. **已生成的错误 snapshot**: 如果在本 PR 之前已经对 `__historical__` 分区生成了 snapshot（含 plain key），需要确保本 PR 跳过这些 snapshot 而不是尝试加载它们。方案：直接在 `initKvTablet()` 中判断 `isHistorical` 时不调用 `getLatestSnapshot()`，即使存在旧 snapshot 也忽略。

2. **`lakeLogEndOffset` 可用性**: 如果 `__historical__` 分区刚创建、尚未完成首次 tiering，`lakeLogEndOffset` 可能为 `-1` 或 `0`。此时 `restoreStartOffset` 应退回到 `0`（回放完整 WAL），这是安全的。

3. **多分区列**: 当前假设单分区列。如果 Fluss 未来支持多分区列，`CompositeKeyEncoder` 需要编码所有分区列值的组合。当前先按单列实现，留注释说明。

4. **DELETE 记录的 partitionName**: `ChangeType.DELETE` 的 log record 可能不含完整 row（只有 key）。需确认 DELETE record 中是否包含分区列值。如果不包含，需要从 key 中推断或从上下文获取。但根据当前 `__historical__` 写入路径，changelog 是完整 row，所以 DELETE record 也会有分区列。需确认。
