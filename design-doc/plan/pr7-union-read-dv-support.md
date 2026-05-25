# PR 7: Union Read DV 支持

## 目标

在 Flink union read 中应用三层 DV（Paimon/Iceberg DV + LakeDv + LogDv），使查询结果正确排除已删除/已更新的行。

**设计文档参考**：`fluss-deletion-vector-design-v3-en.md` §6

---

## 背景：Union Read 数据流

Union Read = 读取 lake snapshot（Paimon/Iceberg 历史数据）+ 读取 Fluss changelog（实时增量数据），合并为完整视图。

三层 DV 在 union read 中的作用：
1. **Lake DV（Paimon DV / Iceberg DV）**：lake 层面的物理 DV（compaction 产生），由 LakeSource 内部透明应用
2. **LakeDv**：Fluss TabletServer 维护的逻辑 DV，标记 lake 数据文件中已被后续 -U/-D 删除的行（file_id + row_position）
3. **LogDv**：Fluss TabletServer 维护的逻辑 DV，标记 changelog 中已被后续操作覆盖的记录（log offset）

当前 union read（非 DV 表）的执行路径：
```
FlinkSourceEnumerator
  └─ LakeSplitGenerator.generateHybridLakeFlussSplits()
       └─ 为每个 bucket 生成 LakeSnapshotAndFlussLogSplit（lake splits + log offset range）

FlinkSourceSplitReader
  ├─ 读 lake splits → SeekableLakeSnapshotSplitScanner / LakeSnapshotAndLogSplitScanner
  └─ 读 log splits → LogScanner
```

DV 表的 union read 需要在上述路径中注入 LakeDv 和 LogDv 过滤。

---

## 整体设计

### 关键设计决策

1. **DV 数据获取方式**：**FlinkSourceEnumerator 侧获取**，DV 数据直接携带在 split 中。
   - Enumerator 在 `LakeSplitGenerator.generateHybridLakeFlussSplits()` 中已获取 `LakeSnapshot`（含 `snapshotId`），顺带从各 bucket 的 leader TabletServer 获取 DV 快照
   - DV 数据（LakeDv + LogDv + logEndOffset + snapshotStartOffset）作为字段附加到 `LakeSnapshotAndFlussLogSplit`
   - Reader 直接从 split 中取 DV 数据进行过滤，无需额外 RPC
   - 主要用于 **batch read**，不需要考虑 split state 序列化

2. **对比旧方案（Reader 侧获取）的优势**：
   - 无需在 Reader 中处理 stale snapshot 重试逻辑
   - 一致性更好——Enumerator 在同一时刻获取 LakeSnapshot + DV 数据
   - 无需额外的 `DvSnapshotClient`
   - Reader 代码更简单，只需读 split 字段

3. **重试策略**（Enumerator 侧）：
   - `requestedSnapshotId > currentSnapshotId`（TabletServer 尚未完成 Switch）：backoff 重试 getDvSnapshot
   - `requestedSnapshotId < currentSnapshotId`（snapshot 已被新版本取代）：刷新 LakeSnapshot，重新 plan，从头重试
   - 连接/leader 异常：RPC 框架层面自动重试

4. **过滤位置**：LakeDv 过滤在 lake split scanner 中，LogDv 过滤在 log scanner 中，各自独立。

### Union Read 流程

```
FlinkSourceEnumerator (Batch Mode)
  └─ LakeSplitGenerator.generateHybridLakeFlussSplits()
       ├─ [1] flussAdmin.getReadableLakeSnapshot() → LakeSnapshot(snapshotId, bucketOffsets)
       ├─ [2] lakeSource.createPlanner().plan() → List<LakeSplit>
       ├─ [3] if dvEnabled: fetchDvForAllBuckets(snapshotId, bucketIds)
       │    └─ for each bucket: admin.getDvSnapshot(tableId, bucketId, snapshotId)
       │         → DvSnapshotInfo { lakeDv(Map<String,byte[]>), logDv(byte[]),
       │                            logEndOffset, snapshotStartOffset }
       │    [retry: not-ready → backoff; superseded → refresh from step 1]
       └─ [4] 生成 LakeSnapshotAndFlussLogSplit(lakeSplits, dvSnapshot, ...)

FlinkSourceSplitReader
  ├─ 读 lake splits → 应用 LakeDv（从 split.getDvSnapshot().getLakeDv()）
  │    按 (filePath, rowPosition) 过滤已删除行
  └─ 读 log [snapshotStartOffset, logEndOffset] → 应用 LogDv
       按 offset 过滤已覆盖记录
     读 log (logEndOffset, stoppingOffset] → 无 DV 过滤
```

---

## Step 1: Proto 消息定义 — GetDvSnapshot RPC

**文件**: `fluss-rpc/src/main/proto/FlussApi.proto`

新增 DV 快照获取 RPC：

```proto
message GetDvSnapshotRequest {
  required int64 table_id = 1;
  required int32 bucket_id = 2;
  required int64 readable_snapshot_id = 3;
}

message GetDvSnapshotResponse {
  // LakeDv: per-file deleted position bitmaps (file_path as key, resolved via FileDict)
  repeated PbLakeDvEntry lake_dv_entries = 1;
  // LogDv: deleted log offsets bitmap (serialized Roaring64Bitmap)
  optional bytes log_dv_bitmap = 2;
  // The log end offset at snapshot time
  required int64 log_end_offset = 3;
  // The log start offset for this snapshot (snapshotStartLogOffset)
  required int64 snapshot_start_offset = 4;
}

message PbLakeDvEntry {
  required string file_path = 1;
  // Serialized Roaring64Bitmap of deleted row positions
  required bytes deleted_positions_bitmap = 2;
}
```

**文件**: `fluss-rpc/.../protocol/ApiKeys.java`
- 新增 `GET_DV_SNAPSHOT(1063, 0, 0, PRIVATE)`

**文件**: `fluss-rpc/.../gateway/TabletServerGateway.java`
- 新增方法：
```java
@RPC(api = ApiKeys.GET_DV_SNAPSHOT)
CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(GetDvSnapshotRequest request);
```

Regenerate: `./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`

---

## Step 2: TabletServer DV 快照服务

### 2a. DvManager 新增 union read 接口

**文件**: `fluss-server/.../kv/dv/DvManager.java`

新增方法（在 DvRWLock 读锁下调用）：

```java
/**
 * Get DV snapshot for union read. Must be called under DvRWLock read lock.
 * DvManager converts file_id → file_path via FileDict.
 */
public DvSnapshot getDvForUnionRead(long requestedSnapshotId) throws IOException {
    // 1. Verify snapshot consistency
    if (readableSnapshotId != requestedSnapshotId) {
        throw new StaleSnapshotException(readableSnapshotId, requestedSnapshotId);
    }
    // 2. Snapshot LakeDv: read all entries, convert file_id → file_path
    Map<Integer, byte[]> rawLakeDv = lakeDv.getAllSerialized();
    Map<String, byte[]> resolvedLakeDv = new HashMap<>();
    for (Map.Entry<Integer, byte[]> entry : rawLakeDv.entrySet()) {
        String filePath = fileDict.getFilePath(entry.getKey());
        if (filePath != null) {
            resolvedLakeDv.put(filePath, entry.getValue());
        }
    }
    // 3. Snapshot LogDv: range [snapshotStartLogOffset, logEndOffset)
    long logEndOffset = getLogEndOffset();
    byte[] logDvBitmap = logDv.snapshotSerialized(snapshotStartLogOffset, logEndOffset);
    // 4. Return
    return new DvSnapshot(resolvedLakeDv, logDvBitmap, logEndOffset, snapshotStartLogOffset);
}
```

### 2b. DvSnapshot 数据类（NEW）

**文件**: `fluss-server/.../kv/dv/DvSnapshot.java`

```java
@Internal
public class DvSnapshot {
    private final Map<String, byte[]> lakeDvEntries;  // filePath → serialized Roaring64Bitmap
    @Nullable private final byte[] logDvBitmap;       // serialized Roaring64Bitmap
    private final long logEndOffset;
    private final long snapshotStartOffset;
    // constructor, getters
}
```

### 2c. LogDv snapshot serialization

**文件**: `fluss-server/.../kv/dv/LogDv.java`

已有 `snapshot(fromOffset, toOffset)` 返回 `Roaring64Bitmap`。新增 serialized 版本：

```java
public byte[] snapshotSerialized(long fromOffset, long toOffset) throws IOException {
    Roaring64Bitmap bitmap = snapshot(fromOffset, toOffset);
    if (bitmap.isEmpty()) {
        return null;
    }
    return RoaringBitmapUtils.serializeRoaring64(bitmap);
}
```

### 2d. LakeDv serialized getAll

**文件**: `fluss-server/.../kv/dv/LakeDv.java`

新增方法：

```java
/** Returns all LakeDv entries with serialized bitmaps. */
public Map<Integer, byte[]> getAllSerialized() throws IOException {
    Map<Integer, byte[]> result = new HashMap<>();
    // iterate RocksDB CF, for each fileId -> bitmap bytes (already stored as serialized)
    return result;
}
```

### 2e. ReplicaManager / TabletService 处理

**文件**: `fluss-server/.../replica/ReplicaManager.java`

```java
public GetDvSnapshotResponse getDvSnapshot(GetDvSnapshotRequest request) {
    TableBucket tb = new TableBucket(request.getTableId(), request.getBucketId());
    KvTablet kvTablet = getKvTablet(tb);
    Preconditions.checkNotNull(kvTablet, "KvTablet not found for %s", tb);
    DvManager dvManager = kvTablet.getDvManager();
    Preconditions.checkNotNull(dvManager, "DvManager not found for %s", tb);

    dvManager.getDvRWLock().readLock();
    try {
        DvSnapshot snapshot = dvManager.getDvForUnionRead(request.getReadableSnapshotId());
        return ServerRpcMessageUtils.buildGetDvSnapshotResponse(snapshot);
    } finally {
        dvManager.getDvRWLock().readUnlock();
    }
}
```

**文件**: `fluss-server/.../tablet/TabletService.java`

```java
@Override
public CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(GetDvSnapshotRequest request) {
    CompletableFuture<GetDvSnapshotResponse> response = new CompletableFuture<>();
    try {
        response.complete(replicaManager.getDvSnapshot(request));
    } catch (Exception e) {
        response.completeExceptionally(e);
    }
    return response;
}
```

---

## Step 3: Admin API — getDvSnapshot

不再需要独立的 `DvSnapshotClient`。改为在 `Admin` 接口中新增 `getDvSnapshot` 方法，内部路由到 bucket leader 的 TabletServer。

### 3a. Admin 接口扩展

**文件**: `fluss-client/.../admin/Admin.java`

```java
/**
 * Get DV snapshot for a specific bucket. Routes to the bucket's leader TabletServer.
 */
CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
        long tableId, int bucketId, long readableSnapshotId);
```

### 3b. FlussAdmin 实现

**文件**: `fluss-client/.../admin/FlussAdmin.java`

```java
@Override
public CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
        long tableId, int bucketId, long readableSnapshotId) {
    GetDvSnapshotRequest request = new GetDvSnapshotRequest()
            .setTableId(tableId)
            .setBucketId(bucketId)
            .setReadableSnapshotId(readableSnapshotId);
    // Route to the bucket's leader TabletServer
    TabletServerGateway gateway = metadataUpdater.newTabletServerClientForBucket(
            new TableBucket(tableId, bucketId));
    return gateway.getDvSnapshot(request);
}
```

注意：需要确认 `MetadataUpdater` 是否有按 bucket 找 leader 的方法。如果没有，可以使用 `newRandomTabletServerClient()` 让服务端自行路由，或在 `MetadataUpdater` 中新增 helper。参考 `FlussAdmin.getTableStats()` 的实现模式（先获取 bucket leader mapping，再分发请求）。

---

## Step 4: StaleSnapshotException

**文件**: `fluss-common/.../exception/StaleSnapshotException.java`（NEW）

```java
@Internal
public class StaleSnapshotException extends FlussException {
    private final long currentSnapshotId;
    private final long requestedSnapshotId;

    public StaleSnapshotException(long currentSnapshotId, long requestedSnapshotId) {
        super(String.format(
                "Stale snapshot: requested %d, current %d",
                requestedSnapshotId, currentSnapshotId));
        this.currentSnapshotId = currentSnapshotId;
        this.requestedSnapshotId = requestedSnapshotId;
    }

    public long getCurrentSnapshotId() { return currentSnapshotId; }
    public long getRequestedSnapshotId() { return requestedSnapshotId; }
}
```

需要在 proto 的 `PbError` 中注册对应的 error code，使其可通过 RPC 传输。

---

## Step 5: LakeSplitGenerator — DV 获取 + 重试

### 5a. DvSnapshotInfo 数据类（NEW）

**文件**: `fluss-flink/fluss-flink-common/.../lake/DvSnapshotInfo.java`

在 Flink 侧携带 DV 快照数据的轻量类（不需要序列化）：

```java
@Internal
public class DvSnapshotInfo {
    /** filePath → serialized Roaring64Bitmap of deleted row positions. */
    private final Map<String, byte[]> lakeDv;
    /** Serialized Roaring64Bitmap of deleted log offsets. Null if empty. */
    @Nullable private final byte[] logDvBitmap;
    /** Log offset up to which LogDv applies. */
    private final long logEndOffset;
    /** Log start offset of the DV snapshot. */
    private final long snapshotStartOffset;
    // constructor, getters
}
```

### 5b. LakeSplitGenerator 扩展

**文件**: `fluss-flink/fluss-flink-common/.../lake/LakeSplitGenerator.java`

在 `generateHybridLakeFlussSplits()` 中，获取 LakeSnapshot 后判断 DV 是否启用，如果启用则获取 DV 数据。

新增构造参数：
- `boolean dvEnabled` — 从 `tableInfo.getTableConfig()` 读取

修改 `generateHybridLakeFlussSplits()`：

```java
@Nullable
public List<SourceSplitBase> generateHybridLakeFlussSplits() throws Exception {
    int maxOuterRetries = 3;
    for (int outerRetry = 0; outerRetry < maxOuterRetries; outerRetry++) {
        LakeSnapshot lakeSnapshotInfo;
        try {
            lakeSnapshotInfo = flussAdmin.getReadableLakeSnapshot(
                    tableInfo.getTablePath()).get();
        } catch (Exception exception) {
            if (ExceptionUtils.stripExecutionException(exception)
                    instanceof LakeTableSnapshotNotExistException) {
                return null;
            }
            throw exception;
        }

        long snapshotId = lakeSnapshotInfo.getSnapshotId();

        // Plan lake splits
        List<LakeSplit> lakeSplits = lakeSource
                .createPlanner((LakeSource.PlannerContext) () -> snapshotId)
                .plan();

        // Fetch DV data if enabled
        Map<Integer, DvSnapshotInfo> bucketDvSnapshots = null;
        if (dvEnabled) {
            try {
                bucketDvSnapshots = fetchDvForAllBuckets(
                        tableInfo.getTableId(), snapshotId,
                        collectBucketIds(lakeSnapshotInfo));
            } catch (Exception e) {
                Throwable cause = ExceptionUtils.stripExecutionException(e);
                if (cause instanceof StaleSnapshotException) {
                    StaleSnapshotException stale = (StaleSnapshotException) cause;
                    if (stale.getRequestedSnapshotId() > stale.getCurrentSnapshotId()) {
                        // Should not happen at outer level (inner loop handles this)
                        throw e;
                    }
                    // Snapshot superseded, refresh and retry
                    LOG.info("DV snapshot {} superseded (current: {}), refreshing.",
                            snapshotId, stale.getCurrentSnapshotId());
                    continue;
                }
                throw e;
            }
        }

        // Generate splits with DV data attached
        return generateSplitsWithDv(lakeSnapshotInfo, lakeSplits, bucketDvSnapshots);
    }
    throw new FlussException("Failed to fetch DV snapshots after retries");
}
```

### 5c. DV 获取 + 内层重试

新增 helper 方法：

```java
private static final int MAX_DV_FETCH_RETRIES = 10;
private static final long INITIAL_BACKOFF_MS = 500;
private static final long MAX_BACKOFF_MS = 10000;

/**
 * Fetch DV snapshot for all buckets. Per-bucket retry for "not ready" errors.
 * Throws StaleSnapshotException (superseded) to caller for outer retry.
 */
private Map<Integer, DvSnapshotInfo> fetchDvForAllBuckets(
        long tableId, long snapshotId, Set<Integer> bucketIds) throws Exception {
    Map<Integer, DvSnapshotInfo> results = new HashMap<>();
    for (int bucketId : bucketIds) {
        results.put(bucketId,
                fetchDvForBucketWithRetry(tableId, bucketId, snapshotId));
    }
    return results;
}

private DvSnapshotInfo fetchDvForBucketWithRetry(
        long tableId, int bucketId, long snapshotId) throws Exception {
    long backoffMs = INITIAL_BACKOFF_MS;
    for (int attempt = 0; attempt < MAX_DV_FETCH_RETRIES; attempt++) {
        try {
            GetDvSnapshotResponse resp =
                    flussAdmin.getDvSnapshot(tableId, bucketId, snapshotId).get();
            return toDvSnapshotInfo(resp);
        } catch (Exception e) {
            Throwable cause = ExceptionUtils.stripExecutionException(e);
            if (cause instanceof StaleSnapshotException) {
                StaleSnapshotException stale = (StaleSnapshotException) cause;
                if (stale.getRequestedSnapshotId() > stale.getCurrentSnapshotId()) {
                    // Server not ready yet, backoff and retry
                    LOG.debug("Bucket {} not ready for snapshot {} (current: {}), "
                            + "retry {}/{}",
                            bucketId, snapshotId,
                            stale.getCurrentSnapshotId(),
                            attempt + 1, MAX_DV_FETCH_RETRIES);
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                    continue;
                }
                // Snapshot superseded — re-throw to trigger outer retry
                throw e;
            }
            throw e;
        }
    }
    throw new FlussException(String.format(
            "Failed to fetch DV snapshot for bucket %d after %d retries",
            bucketId, MAX_DV_FETCH_RETRIES));
}

private static DvSnapshotInfo toDvSnapshotInfo(GetDvSnapshotResponse resp) {
    Map<String, byte[]> lakeDv = new HashMap<>();
    for (int i = 0; i < resp.getLakeDvEntriesCount(); i++) {
        PbLakeDvEntry entry = resp.getLakeDvEntriesAt(i);
        lakeDv.put(entry.getFilePath(), entry.getDeletedPositionsBitmap());
    }
    byte[] logDvBitmap = resp.hasLogDvBitmap() ? resp.getLogDvBitmap() : null;
    return new DvSnapshotInfo(
            lakeDv, logDvBitmap, resp.getLogEndOffset(), resp.getSnapshotStartOffset());
}
```

### 5d. 传递 DV 数据到 split

在 `generateSplitForPrimaryKeyTableBucket()` 中，为 `LakeSnapshotAndFlussLogSplit` 附加 DV 数据：

```java
private SourceSplitBase generateSplitForPrimaryKeyTableBucket(
        @Nullable List<LakeSplit> lakeSplits,
        TableBucket tableBucket,
        @Nullable String partitionName,
        @Nullable Long snapshotLogOffset,
        long stoppingOffset,
        @Nullable DvSnapshotInfo dvSnapshot) {
    // ... existing logic ...
    return new LakeSnapshotAndFlussLogSplit(
            tableBucket, partitionName, lakeSplits,
            snapshotLogOffset, stoppingOffset, dvSnapshot);
}
```

---

## Step 6: Split 扩展 — 携带 DV 数据

### 6a. LakeSnapshotAndFlussLogSplit 扩展

**文件**: `fluss-flink/fluss-flink-common/.../lake/split/LakeSnapshotAndFlussLogSplit.java`

新增字段：

```java
// DV snapshot data (nullable - only for DV-enabled tables)
@Nullable private final DvSnapshotInfo dvSnapshot;
```

新增构造函数（保留旧构造函数向后兼容，旧构造函数 dvSnapshot 传 null）：

```java
public LakeSnapshotAndFlussLogSplit(
        TableBucket tableBucket,
        @Nullable String partitionName,
        @Nullable List<LakeSplit> snapshotSplits,
        long startingOffset,
        long stoppingOffset,
        @Nullable DvSnapshotInfo dvSnapshot) {
    this(tableBucket, partitionName, snapshotSplits,
         startingOffset, stoppingOffset, 0, 0,
         snapshotSplits == null, dvSnapshot);
}
```

新增 getter：

```java
@Nullable
public DvSnapshotInfo getDvSnapshot() {
    return dvSnapshot;
}
```

### 6b. LakeSplitSerializer 更新

**文件**: `fluss-flink/fluss-flink-common/.../lake/LakeSplitSerializer.java`

Enumerator 将 split 分配给 Reader 需要走序列化（Enumerator 在 JobManager，Reader 在 TaskManager，不同 JVM）。因此 `DvSnapshotInfo` 必须序列化。

**序列化**：在 `LakeSnapshotAndFlussLogSplit` 的现有字段之后追加 DV 数据：

```java
// 在现有 isLakeSplitFinished 之后追加
DvSnapshotInfo dvSnapshot = lakeSnapshotAndFlussLogSplit.getDvSnapshot();
if (dvSnapshot == null) {
    out.writeBoolean(false);
} else {
    out.writeBoolean(true);
    // LakeDv: Map<String, byte[]>
    Map<String, byte[]> lakeDv = dvSnapshot.getLakeDv();
    out.writeInt(lakeDv.size());
    for (Map.Entry<String, byte[]> entry : lakeDv.entrySet()) {
        out.writeUTF(entry.getKey());           // filePath
        out.writeInt(entry.getValue().length);   // bitmap length
        out.write(entry.getValue());             // bitmap bytes
    }
    // LogDv: byte[]
    byte[] logDvBitmap = dvSnapshot.getLogDvBitmap();
    if (logDvBitmap == null) {
        out.writeBoolean(false);
    } else {
        out.writeBoolean(true);
        out.writeInt(logDvBitmap.length);
        out.write(logDvBitmap);
    }
    // offsets
    out.writeLong(dvSnapshot.getLogEndOffset());
    out.writeLong(dvSnapshot.getSnapshotStartOffset());
}
```

**反序列化**：在 `isLakeSplitFinished` 之后，用 `input.available() > 0` 做向后兼容（旧版 split 没有 DV 字段）：

```java
DvSnapshotInfo dvSnapshot = null;
if (input.available() > 0 && input.readBoolean()) {
    // LakeDv
    int lakeDvSize = input.readInt();
    Map<String, byte[]> lakeDv = new HashMap<>(lakeDvSize);
    for (int i = 0; i < lakeDvSize; i++) {
        String filePath = input.readUTF();
        byte[] bitmap = new byte[input.readInt()];
        input.read(bitmap);
        lakeDv.put(filePath, bitmap);
    }
    // LogDv
    byte[] logDvBitmap = null;
    if (input.readBoolean()) {
        logDvBitmap = new byte[input.readInt()];
        input.read(logDvBitmap);
    }
    long logEndOffset = input.readLong();
    long snapshotStartOffset = input.readLong();
    dvSnapshot = new DvSnapshotInfo(lakeDv, logDvBitmap, logEndOffset, snapshotStartOffset);
}
return new LakeSnapshotAndFlussLogSplit(
        tableBucket, partition, lakeSplits,
        startingOffset, stoppingOffset,
        recordsToSkip, splitIndex, isLakeSplitFinished, dvSnapshot);
```

### 6c. DvSnapshotInfo 序列化说明

`DvSnapshotInfo` 本身不实现 `Serializable`，序列化/反序列化完全由 `LakeSplitSerializer` 控制。数据结构：

| 字段 | 类型 | 序列化方式 |
|------|------|-----------|
| lakeDv | `Map<String, byte[]>` | writeInt(size) + 每个 entry: writeUTF(filePath) + writeInt(len) + write(bytes) |
| logDvBitmap | `byte[]` | writeBoolean(hasValue) + writeInt(len) + write(bytes) |
| logEndOffset | `long` | writeLong |
| snapshotStartOffset | `long` | writeLong |

---

## Step 7: Reader 侧 DV 过滤

### 7a. Lake Split Scanner DV 过滤

读取 lake 数据文件时，需要按 `(filePath, rowPosition)` 过滤。

**关键**：`LakeSplit` 接口只有 `bucket()` 和 `partition()`，不直接暴露 file path。具体的 file path 取决于 lake 格式实现（Paimon/Iceberg）。

**方案**：在 `LakeSnapshotAndLogSplitScanner`（batch 模式下的 scanner）或其 wrapper 中注入 DV 过滤：

```java
// 在 LakeSplitReaderGenerator.getBoundedSplitScanner() 中
if (split instanceof LakeSnapshotAndFlussLogSplit) {
    LakeSnapshotAndFlussLogSplit lakeSplit = (LakeSnapshotAndFlussLogSplit) split;
    BatchScanner scanner = getBatchScanner(lakeSplit);
    DvSnapshotInfo dvSnapshot = lakeSplit.getDvSnapshot();
    if (dvSnapshot != null) {
        scanner = new DvAwareBatchScanner(scanner, dvSnapshot);
    }
    return new BoundedSplitReader(scanner, lakeSplit.getRecordsToSkip());
}
```

`DvAwareBatchScanner` 在 lake 部分按 filePath + rowPosition 过滤，在 log 部分按 offset 过滤。

**LakeDv 过滤逻辑**：

```java
// DvAwareBatchScanner wraps the underlying scanner
// For lake records: track rowPosition per file, check lakeDv bitmap
Roaring64Bitmap deletedPositions = lakeDv.get(currentFilePath);
if (deletedPositions != null && deletedPositions.contains(rowPosition)) {
    // skip this row
}
rowPosition++;
```

Lake format 实现需要提供获取当前 file path 的能力。如果 `LakeSplit` 接口不暴露 file path，可以在 lake format 层面（如 Paimon 的 `PaimonSplit`）提供。

### 7b. Log Scanner DV 过滤

**文件**: `fluss-flink/fluss-flink-common/.../source/reader/FlinkSourceSplitReader.java`

在 `forLogRecords()` 中，对于有 DV 的 split，过滤 `[snapshotStartOffset, logEndOffset]` 范围内的已删除 offset：

```java
// FlinkSourceSplitReader 需要持有当前 bucket 的 DV 信息
// 在 handleSplitsChanges() 中，当接收到带 DV 的 LakeSnapshotAndFlussLogSplit 时，
// 缓存该 bucket 的 LogDv + logEndOffset

// 在 forLogRecords() 中过滤
for (ScanRecord record : bucketScanRecords) {
    long offset = record.logOffset();
    if (logDv != null
            && offset >= snapshotStartOffset
            && offset < logEndOffset
            && logDv.contains(offset)) {
        // skip this record
        continue;
    }
    // emit record
}
```

**LogDv 反序列化时机**：当 `handleSplitsChanges()` 接收到带 DV 的 split 时，反序列化 `logDvBitmap` 为 `Roaring64Bitmap`。需要在 `fluss-flink-common` 模块添加 `roaringbitmap` 依赖。

### 7c. logEndOffset 与 stoppingOffset 的关系

对于 batch union read：
- 读 lake splits（应用 LakeDv 过滤）
- 读 log `[snapshotStartOffset, stoppingOffset]`：
  - 在 `[snapshotStartOffset, logEndOffset)` 范围内应用 LogDv 过滤
  - `[logEndOffset, stoppingOffset]` 范围内不过滤（DV 快照之后到达的新数据）

---

## Step 8: 依赖管理

### 8a. roaringbitmap 依赖

`fluss-flink-common` 模块需要添加 `roaringbitmap` 依赖以反序列化 DV bitmaps：

**文件**: `fluss-flink/fluss-flink-common/pom.xml`

```xml
<dependency>
    <groupId>org.roaringbitmap</groupId>
    <artifactId>RoaringBitmap</artifactId>
</dependency>
```

需要确认版本是否在根 pom 的 `dependencyManagement` 中已定义。

### 8b. RoaringBitmapUtils

`RoaringBitmapUtils` 目前在 `fluss-server` 模块。Reader 侧需要反序列化能力。两个选项：
1. 将 `RoaringBitmapUtils` 移到 `fluss-common`（推荐，如果 `fluss-common` 可以依赖 roaringbitmap）
2. 在 `fluss-flink-common` 中直接使用 `Roaring64Bitmap.deserialize()` API

---

## Step 9: ServerRpcMessageUtils 扩展

**文件**: `fluss-server/.../utils/ServerRpcMessageUtils.java`

新增 helper 方法：

```java
/** Build GetDvSnapshotResponse from DvSnapshot data. */
public static GetDvSnapshotResponse buildGetDvSnapshotResponse(DvSnapshot snapshot) {
    GetDvSnapshotResponse resp = new GetDvSnapshotResponse()
            .setLogEndOffset(snapshot.getLogEndOffset())
            .setSnapshotStartOffset(snapshot.getSnapshotStartOffset());
    if (snapshot.getLogDvBitmap() != null) {
        resp.setLogDvBitmap(snapshot.getLogDvBitmap());
    }
    for (Map.Entry<String, byte[]> entry : snapshot.getLakeDvEntries().entrySet()) {
        resp.addLakeDvEntry()
                .setFilePath(entry.getKey())
                .setDeletedPositionsBitmap(entry.getValue());
    }
    return resp;
}
```

---

## Step 10: 测试

### 10a. Proto 消息测试（NEW）

**文件**: `fluss-rpc/src/test/.../messages/GetDvSnapshotMessageTest.java`
- GetDvSnapshotRequest/Response 序列化/反序列化 round-trip
- PbLakeDvEntry 多条记录
- 空 lake_dv_entries、null log_dv_bitmap 的边界场景

### 10b. DvManager union read 测试（NEW）

**文件**: `fluss-server/src/test/.../kv/dv/DvManagerUnionReadTest.java`
- `getDvForUnionRead` 返回正确的 LakeDv + LogDv 快照
- Snapshot 一致性校验：requestedSnapshotId != readableSnapshotId → StaleSnapshotException
- FileDict file_id → file_path 解析正确性
- 空 DV 场景（无 LakeDv、无 LogDv）
- 在 DvRWLock 读锁下获取快照的并发安全性

### 10c. Split 序列化测试（EXTEND）

**文件**: `fluss-flink/fluss-flink-common/src/test/.../lake/LakeSplitSerializerTest.java`
- `LakeSnapshotAndFlussLogSplit` 带 DvSnapshotInfo 的序列化/反序列化 round-trip
- DvSnapshotInfo 为 null 的兼容性（旧格式 split 反序列化不报错）
- 边界场景：空 lakeDv map、null logDvBitmap

### 10d. LakeSplitGenerator DV 获取测试（NEW）

**文件**: `fluss-flink/fluss-flink-common/src/test/.../lake/LakeSplitGeneratorDvTest.java`
- Mock Admin.getDvSnapshot()
- 验证 DV 数据正确附加到 split
- 重试逻辑：模拟 "not ready" → backoff → 成功
- 重试逻辑：模拟 "superseded" → 刷新 LakeSnapshot → 成功
- DV 未启用时不获取 DV 数据

### 10e. DV 过滤测试

- Lake 读取 + LakeDv 过滤：验证标记为已删除的行被跳过
- Log 读取 + LogDv 过滤：验证标记的 offset 被跳过
- LogDv 过滤范围：只在 `[snapshotStartOffset, logEndOffset)` 内过滤

### 10f. 集成测试

**文件**: `fluss-lake/fluss-lake-paimon/src/test/.../FlinkUnionReadDvTableITCase.java`
- 端到端验证：写入 → tiering → 更新/删除 → union read 结果正确
- 三层 DV 协作：Paimon DV + LakeDv + LogDv 同时生效
- 边界场景：空 DV、全部删除

---

## 关键文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-rpc/.../proto/FlussApi.proto` | MODIFY | 新增 GetDvSnapshot RPC messages |
| `fluss-rpc/.../protocol/ApiKeys.java` | MODIFY | 新增 GET_DV_SNAPSHOT |
| `fluss-rpc/.../gateway/TabletServerGateway.java` | MODIFY | 新增 getDvSnapshot() |
| `fluss-common/.../exception/StaleSnapshotException.java` | NEW | Stale snapshot 异常 |
| `fluss-server/.../kv/dv/DvManager.java` | MODIFY | 新增 getDvForUnionRead() |
| `fluss-server/.../kv/dv/DvSnapshot.java` | NEW | DV 快照数据类 |
| `fluss-server/.../kv/dv/LogDv.java` | MODIFY | 新增 snapshotSerialized() |
| `fluss-server/.../kv/dv/LakeDv.java` | MODIFY | 新增 getAllSerialized() |
| `fluss-server/.../replica/ReplicaManager.java` | MODIFY | 新增 getDvSnapshot() |
| `fluss-server/.../tablet/TabletService.java` | MODIFY | 新增 getDvSnapshot handler |
| `fluss-server/.../utils/ServerRpcMessageUtils.java` | MODIFY | buildGetDvSnapshotResponse() |
| `fluss-client/.../admin/Admin.java` | MODIFY | 新增 getDvSnapshot() |
| `fluss-client/.../admin/FlussAdmin.java` | MODIFY | getDvSnapshot() 实现 |
| `fluss-flink/.../lake/DvSnapshotInfo.java` | NEW | DV 快照数据载体 |
| `fluss-flink/.../lake/LakeSplitGenerator.java` | MODIFY | DV 获取 + 重试逻辑 |
| `fluss-flink/.../lake/split/LakeSnapshotAndFlussLogSplit.java` | MODIFY | 新增 dvSnapshot 字段 |
| `fluss-flink/.../lake/LakeSplitSerializer.java` | MODIFY | DvSnapshotInfo 序列化/反序列化 |
| `fluss-flink/.../lake/LakeSplitReaderGenerator.java` | MODIFY | 注入 DV 过滤 |
| `fluss-flink/.../source/reader/FlinkSourceSplitReader.java` | MODIFY | LogDv 过滤 |

---

## 前置依赖

- PR 1: DvRocksDB + 核心数据结构（LakeDv, LogDv, FileDict）
- PR 3: DvManager（getDvForUnionRead 依赖 DvManager 状态）
- PR 5: Protocol 扩展（CoordinatorEventProcessor 中的 readable snapshot 管理）
- PR 6: TabletServer Prepare + Readable Switch（readableSnapshotId 和 snapshotStartLogOffset 的维护）

---

## 验证步骤

```bash
# 1. Proto 重新生成
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc

# 2. 编译
./mvnw compile -pl fluss-server,fluss-client -am -DskipTests
./mvnw compile -pl fluss-flink/fluss-flink-common -am -DskipTests

# 3. 格式化
./mvnw spotless:apply -pl fluss-server,fluss-client,fluss-rpc
./mvnw spotless:apply -pl fluss-flink/fluss-flink-common

# 4. 单元测试
./mvnw test -pl fluss-rpc -Dtest=GetDvSnapshotMessageTest
./mvnw test -pl fluss-server -Dtest=DvManagerUnionReadTest
./mvnw test -pl fluss-flink/fluss-flink-common -Dtest=LakeSplitGeneratorDvTest

# 5. 集成测试
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=FlinkUnionReadDvTableITCase
```
