# Bootstrap Upgrade Code Design

## 1. Overview

Bootstrap upgrade enables migrating an existing Paimon (lake) table's historical data into Fluss as a PrimaryKey table. The system reads from a lake snapshot, generates RocksDB SST files, uploads them to remote storage, registers `CompletedSnapshot` metadata, and activates the bootstrap partition — all while the tiering service continues normal operation for other tables.

The end-to-end flow:

```
Coordinator                          Tiering Service (Flink)                     Remote Storage
    |                                        |                                        |
    |-- dispatch bootstrap task ------------>|                                        |
    |   (holdPartition, snapshotId)          |                                        |
    |                                        |-- TieringSplitGenerator                |
    |                                        |   creates TieringBootstrapSplit        |
    |                                        |                                        |
    |                                        |-- TieringSplitReader                   |
    |                                        |   forBootstrapSplit()                  |
    |                                        |     |                                  |
    |                                        |     |-- BootstrapSstWriter             |
    |                                        |     |   write(record) -> tempRocksDB   |
    |                                        |     |   flush() -> checkpoint          |
    |                                        |     |   upload SSTs --------------->   |
    |                                        |                                        |
    |                                        |-- TieringCommitOperator                |
    |                                        |   separate bootstrap results           |
    |                                        |   commitBootstrapArtifacts() --------->|
    |                                        |                                        |
    |<-- CommitBootstrapArtifactsRequest ----|                                        |
    |                                        |                                        |
    |-- maybeCompleteBootstrapUpgrade        |                                        |
    |   registerCompletedSnapshots           |                                        |
    |   markComplete (ZK)                    |                                        |
    |   activateBootstrapPartition           |                                        |
    |   (CreatePartitionEvent)               |                                        |
```

---

## 2. Key Domain Classes

### 2.1 BootstrapUpgradeState (ZK-persisted state)

**File**: `fluss-server/.../zk/data/BootstrapUpgradeState.java`

Persisted in ZooKeeper per table. Tracks the lifecycle of a bootstrap upgrade.

```java
public class BootstrapUpgradeState {
    private final BootstrapUpgradeStatus status;   // IN_PROGRESS | COMPLETE
    private final String holdPartition;            // partition name to bootstrap (non-null)
    private final @Nullable Long holdPartitionId;  // assigned after early partition creation
}
```

**Status transitions**:
```
(table create / alter enable datalake)
    --> IN_PROGRESS(holdPartition, holdPartitionId=null)
        --> IN_PROGRESS(holdPartition, holdPartitionId=assigned)   // after early partition creation
            --> COMPLETE(holdPartition, holdPartitionId)            // after SST commit
                --> (deleted on datalake disable / table drop)
```

### 2.2 BootstrapUpgradeStateManager (coordinator-side state access)

**File**: `fluss-server/.../coordinator/BootstrapUpgradeStateManager.java`

Coordinator-side wrapper for ZK operations on `BootstrapUpgradeState`:

| Method                                                          | Description                                  |
|-----------------------------------------------------------------|----------------------------------------------|
| `initializeInProgress(tableId, holdPartition)`                  | Create IN_PROGRESS state (idempotent)        |
| `initializeInProgress(tableId, holdPartition, holdPartitionId)` | Create IN_PROGRESS state with partition ID   |
| `updateHoldPartitionId(tableId, holdPartitionId)`               | Store assigned partition ID                  |
| `markComplete(tableId)`                                         | Transition to COMPLETE                       |
| `get(tableId)`                                                  | Load current state                           |
| `delete(tableId)`                                               | Remove state                                 |
| `deleteIfPresent(tableId)`                                      | Clean up on table drop / datalake disable    |

### 2.3 BootstrapArtifact (server-side domain)

**File**: `fluss-server/.../entity/BootstrapArtifact.java`

Metadata about a bootstrap SST artifact for a single bucket:

```java
public class BootstrapArtifact {
    private final TableBucket tableBucket;
    @Nullable private final String partitionName;
    private final long sstSizeBytes;
    private final long rowCount;
    @Nullable private final String snapshotPath;  // remote snapshot dir where _METADATA was written
}
```

### 2.4 CommitBootstrapArtifactsEvent (coordinator event)

**File**: `fluss-server/.../coordinator/event/CommitBootstrapArtifactsEvent.java`

Event posted into the coordinator event queue when a `CommitBootstrapArtifactsRequest` RPC arrives:

```java
public class CommitBootstrapArtifactsEvent implements CoordinatorEvent {
    private final Map<Long, Map<TableBucket, BootstrapArtifact>> bootstrapArtifactsByTableId;
    private final CompletableFuture<CommitBootstrapArtifactsResponse> respCallback;
}
```

---

## 3. Split Hierarchy & Serialization

### 3.1 TieringSplit class hierarchy

**File**: `fluss-flink/.../source/split/TieringSplit.java`

```
TieringSplit (abstract)
├── TieringLogSplit          // normal log tiering (bounded offset range)
├── TieringSnapshotSplit     // PK table KV snapshot tiering
└── TieringBootstrapSplit    // bootstrap-upgrade: read lake → generate SSTs
```

Base class fields:
```java
public abstract class TieringSplit implements SourceSplit {
    static final byte TIERING_SNAPSHOT_SPLIT_FLAG = 1;
    static final byte TIERING_LOG_SPLIT_FLAG = 2;
    static final byte TIERING_BOOTSTRAP_SPLIT_FLAG = 3;

    protected final TablePath tablePath;
    protected final TableBucket tableBucket;
    @Nullable protected final String partitionName;
    protected final int numberOfSplits;
    protected final LakeTieringTaskType taskType;
    protected final long tieringEpoch;
    @Nullable protected final String remoteDataDir;
    protected boolean skipCurrentRound;
}
```

### 3.2 TieringBootstrapSplit

**File**: `fluss-flink/.../source/split/TieringBootstrapSplit.java`

Bootstrap-specific split containing the lake snapshot ID to read from:

```java
public class TieringBootstrapSplit extends TieringSplit {
    private final long snapshotId;  // lake snapshot ID to read from

    // Constructor sets taskType = LakeTieringTaskType.BOOTSTRAP_UPGRADE
}
```

### 3.3 TieringSplitSerializer

**File**: `fluss-flink/.../source/split/TieringSplitSerializer.java`

- Current version: `VERSION_4`
- Serializes `splitKind` byte to distinguish split types
- For `TIERING_BOOTSTRAP_SPLIT_FLAG`: serializes `snapshotId` (long)
- Version 3+ adds `remoteDataDir` (optional string)
- Version 1+ adds `taskType` (int code)
- Version 2+ adds `tieringEpoch` (long)

---

## 4. Split Generation (TieringSplitGenerator)

**File**: `fluss-flink/.../source/split/TieringSplitGenerator.java`

When `taskType == BOOTSTRAP_UPGRADE` and the bucket has never been tiered but has a lake snapshot:

```java
// generateSplitForPrimaryKeyTableBucket()
if (taskType == LakeTieringTaskType.BOOTSTRAP_UPGRADE) {
    return Optional.of(
        new TieringBootstrapSplit(
            tablePath, tableBucket, partitionName,
            latestSnapshotId, 0, false, -1L));
}
```

### Partition resolution (`resolveTargetPartitions`)

When a `holdPartition` is specified:
- If the partition already exists in Fluss → use it
- If no partitions exist yet → create a **synthetic placeholder** entry `(null, holdPartition)` so splits can still be generated
- If partitions exist but holdPartition is missing → throw error

---

## 5. SST Generation (BootstrapSstWriter)

**File**: `fluss-flink/.../source/BootstrapSstWriter.java`

Uses a temporary RocksDB instance as a disk-backed write buffer to avoid OOM. Employs **incremental background upload** to overlap CPU work (reading lake data + writing to RocksDB) with I/O (uploading to remote storage).

> Detailed design: see `design_doc/bootstrap-sst-writer-design.md`

### 5.1 Architecture overview

During the write phase, a background `ScheduledExecutorService` monitor thread periodically (every 5s) detects newly created SST files via `RocksDB.getLiveFilesMetaData()` and uploads them asynchronously using a 4-thread upload pool. L0 files are **skipped** (high compaction probability); deeper levels are uploaded first (more stable). On `flush()`, a checkpoint is created and **diffed** against already-uploaded files:

- **New SSTs** (in checkpoint but not uploaded, or size mismatch) → uploaded from checkpoint dir
- **Stale SSTs** (uploaded but not in checkpoint, compacted away) → deleted from remote (best-effort)

### 5.2 Key fields

```java
// Remote paths (computed eagerly in constructor)
@Nullable private final FsPath remoteKvSharedDir;   // {tablet}/shared/
@Nullable private final FsPath remoteSnapshotDir;   // {tablet}/snap-1/

// Thread pools
private final ExecutorService uploadExecutor;               // 4 threads
private final ScheduledExecutorService monitorExecutor;     // 1 thread, 5s interval

// Thread-safe state
private final Map<String, SnapshotFileInfo> uploadedSstFiles;  // ConcurrentHashMap
private final ConcurrentLinkedQueue<CompletableFuture<?>> pendingUploadFutures;
```

### 5.3 Constructor

```java
BootstrapSstWriter(TableInfo tableInfo, TieringBootstrapSplit split) {
    // 1. Validate PK table
    // 2. Initialize RowSerializer, KeyEncoder, schemaId
    // 3. Open temp RocksDB (WAL disabled, compaction enabled by default)
    RocksDB.loadLibrary();
    tempDbDir = Files.createTempDirectory("fluss-bootstrap-rocksdb-");
    tempDbOptions = new Options().setCreateIfMissing(true);
    tempWriteOptions = new WriteOptions().setDisableWAL(true);
    tempDb = RocksDB.open(tempDbOptions, tempDbDir.toString());

    // 4. Eagerly compute remote paths for background upload
    remoteKvSharedDir = FlussPaths.remoteKvSharedDir(remoteKvTabletDir);
    remoteSnapshotDir = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, 1L);

    // 5. Start background SST monitor
    uploadExecutor = Executors.newFixedThreadPool(4);
    monitorExecutor = Executors.newSingleThreadScheduledExecutor(...);
    monitorExecutor.scheduleWithFixedDelay(this::checkAndUploadNewSsts, 5s, 5s, MILLISECONDS);
}
```

### 5.4 write()

```java
void write(LogRecord record) {
    byte[] keyBytes = primaryKeyEncoder.encodeKey(record.getRow());
    if (record.getChangeType() == ChangeType.DELETE) {
        tempDb.delete(tempWriteOptions, keyBytes);   // tombstone
        rowCount--;
    } else {
        BinaryRow binaryRow = rowSerializer.toBinaryRow(record.getRow());
        byte[] valueBytes = ValueEncoder.encodeValue(schemaId, binaryRow);
        tempDb.put(tempWriteOptions, keyBytes, valueBytes);  // upsert
        rowCount++;
    }
}
```

Key properties:
- **Deduplication**: Later puts win (RocksDB's default behavior)
- **Memory safety**: RocksDB auto-flushes memtable to disk when it reaches configured limits
- **Meanwhile**: Background monitor detects and uploads L1+ SST files in parallel

### 5.5 Background monitor (checkAndUploadNewSsts)

```java
private void checkAndUploadNewSsts() {
    List<LiveFileMetaData> liveFiles = tempDb.getLiveFilesMetaData();
    liveFiles.sort(Comparator.comparingInt(LiveFileMetaData::level).reversed());
    for (LiveFileMetaData file : liveFiles) {
        if (file.level() == 0) continue;                  // skip L0
        if (uploadedSstFiles.containsKey(origName)) continue;  // already submitted
        submitSstUpload(origName, file.size());            // async upload
    }
}
```

Level-aware strategy: L0 skipped (high compaction probability), deeper levels uploaded first.

### 5.6 flush()

```java
@Nullable
String flush() {
    // 1. tempDb.flush()              — flush final memtable
    // 2. monitorExecutor.shutdown()   — stop background monitor
    // 3. waitForPendingUploads()      — wait for in-flight uploads (best-effort)
    // 4. Checkpoint.create(tempDb).createCheckpoint(checkpointDir)
    // 5. Categorize checkpoint files: *.sst vs metadata (MANIFEST, CURRENT, OPTIONS)
    // 6. Diff: new SSTs (in checkpoint but not uploaded / size mismatch)
    // 7. Diff: stale SSTs (uploaded but not in checkpoint)
    // 8. Upload new SSTs from checkpoint dir (parallel, 4 threads)
    // 9. Delete stale remote SSTs (best-effort)
    // 10. Upload metadata files to snap-1/
    // 11. Collect shared + private file info
    // 12. Build & upload _METADATA JSON to snap-1/_METADATA
    return remoteSnapshotDir.toString();  // snapshot location path
}
```

### 5.7 Upload paths

SST files are uploaded to:
```
{remoteDataDir}/kv/{db}/{table}-{tableId}/{partition}/{bucket}/shared/{uuid}.sst
```

Metadata files are uploaded to:
```
{remoteDataDir}/kv/{db}/{table}-{tableId}/{partition}/{bucket}/snap-1/{MANIFEST-xxx, CURRENT, OPTIONS-xxx}
```

`_METADATA` JSON file is uploaded to:
```
{remoteDataDir}/kv/{db}/{table}-{tableId}/{partition}/{bucket}/snap-1/_METADATA
```

This follows the existing `FlussPaths` convention so that replicas can find and download the files using the standard `CompletedSnapshotHandle.retrieveCompleteSnapshot()` path.

### 5.8 _METADATA JSON format

Strictly matches `CompletedSnapshotJsonSerde`:
```json
{
  "version": 1,
  "table_id": 12345, "partition_id": 67890, "bucket_id": 0,
  "snapshot_id": 1,
  "snapshot_location": "s3://bucket/fluss/kv/.../snap-1",
  "kv_snapshot_handle": {
    "shared_file_handles": [
      { "kv_file_handle": { "path": "s3://.../shared/550e8400.sst", "size": 1048576 },
        "local_path": "000005.sst" }
    ],
    "private_file_handles": [
      { "kv_file_handle": { "path": "s3://.../snap-1/MANIFEST-000003", "size": 256 },
        "local_path": "MANIFEST-000003" }
    ],
    "snapshot_incremental_size": 1048832
  },
  "log_offset": 0,
  "row_count": 50000
}
```

### 5.9 close()

Implements `Closeable` — shuts down `monitorExecutor`, `uploadExecutor`, closes temp RocksDB (db, options), and deletes temp directory.

---

## 6. Source Reader (TieringSplitReader)

**File**: `fluss-flink/.../source/TieringSplitReader.java`

### 6.1 forBootstrapSplit()

```java
private TableBucketWriteResultWithSplitIds forBootstrapSplit(
        TieringBootstrapSplit bootstrapSplit) throws IOException {
    LakeSource<LakeSplit> lakeSource = createBootstrapLakeSource();
    try (BootstrapSstWriter bootstrapSstWriter =
            new BootstrapSstWriter(currentTable.getTableInfo(), bootstrapSplit)) {
        // Plan lake splits
        Planner<LakeSplit> planner = lakeSource.createPlanner(bootstrapSplit::getSnapshotId);
        List<LakeSplit> lakeSplits = planner.plan();

        // Read matching bucket's data
        for (LakeSplit lakeSplit : lakeSplits) {
            if (!matchesBootstrapBucket(bootstrapSplit, lakeSplit)) continue;
            RecordReader recordReader = lakeSource.createRecordReader(() -> lakeSplit);
            try (CloseableIterator<LogRecord> it = recordReader.read()) {
                while (it.hasNext()) {
                    bootstrapSstWriter.write(it.next());
                }
            }
        }

        // Flush and finish
        String artifact = bootstrapSstWriter.flush();
        return finishCurrentBootstrapSplit(bootstrapSplit, artifact);
    }
}
```

### 6.2 matchesBootstrapBucket()

Filters lake splits to match the target bucket and partition:
```java
private boolean matchesBootstrapBucket(TieringBootstrapSplit bootstrapSplit, LakeSplit lakeSplit) {
    if (lakeSplit.bucket() != bootstrapSplit.getTableBucket().getBucket()) return false;
    String targetPartitionName = bootstrapSplit.getPartitionName();
    if (targetPartitionName == null) return lakeSplit.partition().isEmpty();
    return targetPartitionName.equals(String.join("$", lakeSplit.partition()));
}
```

### 6.3 finishCurrentBootstrapSplit()

Creates a `TableBucketWriteResult` with `bootstrap=true`, `writeResult=null`, and the `bootstrapSnapshotPath` from `BootstrapSstWriter.flush()`:

```java
private TableBucketWriteResultWithSplitIds finishCurrentBootstrapSplit(
        TieringBootstrapSplit bootstrapSplit, @Nullable String bootstrapSnapshotPath) {
    TableBucketWriteResult<WriteResult> writeResult = new TableBucketWriteResult<>(
        bootstrapSplit.getTablePath(),
        tableBucket,
        bootstrapSplit.getPartitionName(),
        null,                              // no lake write result
        UNKNOWN_BUCKET_OFFSET,
        UNKNOWN_BUCKET_TIMESTAMP,
        currentTableNumberOfSplits,
        true,                              // bootstrap = true
        bootstrapSnapshotPath);            // remote snapshot dir path (e.g. hdfs://.../snap-1)
}
```

---

## 7. Write Result & Serialization

### 7.1 TableBucketWriteResult

**File**: `fluss-flink/.../source/TableBucketWriteResult.java`

Added `bootstrap` boolean flag and `bootstrapSnapshotPath` to distinguish bootstrap results from normal tiering:

```java
public class TableBucketWriteResult<WriteResult> implements Serializable {
    // ... existing fields ...
    private final boolean bootstrap;                       // true if bootstrap-upgrade result
    @Nullable private final String bootstrapSnapshotPath;  // remote snapshot dir path

    public boolean isBootstrap() { return bootstrap; }
    @Nullable public String bootstrapSnapshotPath() { return bootstrapSnapshotPath; }
}
```

### 7.2 TableBucketWriteResultSerializer

**File**: `fluss-flink/.../source/TableBucketWriteResultSerializer.java`

- Current version: `VERSION_5`
- VERSION_4 added: `bootstrap` boolean flag
- VERSION_5 added: `bootstrapSnapshotPath` optional string
- Deserialization: handles backward compatibility for VERSION_2/3 (legacy fields), VERSION_4+ reads `bootstrap` flag, VERSION_5+ reads `bootstrapSnapshotPath`

```java
// Serialize (VERSION_5)
out.writeBoolean(tableBucketWriteResult.isBootstrap());
String snapshotPath = tableBucketWriteResult.bootstrapSnapshotPath();
if (snapshotPath != null) {
    out.writeBoolean(true);
    out.writeUTF(snapshotPath);
} else {
    out.writeBoolean(false);
}

// Deserialize (backward compatible)
if (version >= VERSION_2 && version < VERSION_4) {
    if (in.readBoolean()) in.readUTF();     // skip legacy bootstrapArtifactPath
}
if (version == VERSION_3) {
    in.readLong();                           // skip legacy tieringEpoch
}
boolean bootstrap = version >= VERSION_4 && in.readBoolean();
String bootstrapSnapshotPath = null;
if (version >= VERSION_5 && in.readBoolean()) {
    bootstrapSnapshotPath = in.readUTF();
}
```

---

## 8. Commit Pipeline (TieringCommitOperator)

**File**: `fluss-flink/.../committer/TieringCommitOperator.java`

### 8.1 commitWriteResults() — bootstrap separation

When all bucket results for a table are collected, separates bootstrap results from normal tiering results:

```java
private Committable commitWriteResults(long tableId, TablePath tablePath,
        List<TableBucketWriteResult<WriteResult>> committableWriteResults) {

    // 1. Separate bootstrap results
    List<...> bootstrapResults = committableWriteResults.stream()
        .filter(TableBucketWriteResult::isBootstrap)
        .collect(toList());

    // 2. Filter normal results (non-null writeResult)
    committableWriteResults = committableWriteResults.stream()
        .filter(r -> r.writeResult() != null)
        .collect(toList());

    // 3. Commit bootstrap artifacts first
    if (!bootstrapResults.isEmpty()) {
        Map<TableBucket, String> partitionNames = bootstrapResults.stream()
            .collect(toMap(
                TableBucketWriteResult::tableBucket,
                r -> r.partitionName(),
                (left, right) -> right));
        Map<TableBucket, String> snapshotPaths = bootstrapResults.stream()
            .filter(r -> r.bootstrapSnapshotPath() != null)
            .collect(toMap(
                TableBucketWriteResult::tableBucket,
                TableBucketWriteResult::bootstrapSnapshotPath,
                (left, right) -> right));
        flussTableLakeSnapshotCommitter.commitBootstrapArtifacts(
            tableId, partitionNames, snapshotPaths);
    }

    // 4. If only bootstrap results, return null (no lake commit needed)
    if (committableWriteResults.isEmpty()) return null;

    // 5. Otherwise, proceed with normal lake commit...
}
```

### 8.2 FlussTableLakeSnapshotCommitter.commitBootstrapArtifacts()

**File**: `fluss-flink/.../committer/FlussTableLakeSnapshotCommitter.java`

Sends `CommitBootstrapArtifactsRequest` to coordinator with per-bucket snapshot paths:

```java
void commitBootstrapArtifacts(
        long tableId,
        Map<TableBucket, String> partitionNames,
        Map<TableBucket, String> snapshotPaths) {
    CommitBootstrapArtifactsRequest request = new CommitBootstrapArtifactsRequest();
    for (Map.Entry<TableBucket, String> entry : partitionNames.entrySet()) {
        TableBucket tableBucket = entry.getKey();
        PbBootstrapArtifactMetadata metadata = request.addBootstrapArtifactMetadata();
        metadata.setTableId(tableId);
        if (tableBucket.getPartitionId() != null) {
            metadata.setPartitionId(tableBucket.getPartitionId());
        }
        metadata.setBucketId(tableBucket.getBucket());
        if (entry.getValue() != null) {
            metadata.setPartitionName(entry.getValue());
        }
        // Set the snapshot path where _METADATA JSON was written.
        String snapshotPath = snapshotPaths.get(tableBucket);
        if (snapshotPath != null) {
            metadata.setSnapshotPath(snapshotPath);
        }
    }
    coordinatorGateway.commitBootstrapArtifacts(request).get();
}
```

---

## 9. RPC Protocol

### 9.1 Protobuf Messages

**File**: `fluss-rpc/src/main/proto/FlussApi.proto`

```protobuf
message CommitBootstrapArtifactsRequest {
  repeated PbBootstrapArtifactMetadata bootstrap_artifact_metadata = 1;
}

message CommitBootstrapArtifactsResponse {
  repeated PbCommitBootstrapArtifactsRespForTable table_resp = 1;
}

message PbCommitBootstrapArtifactsRespForTable {
  optional int32 error_code = 1;
  optional string error_message = 2;
  required int64 table_id = 3;
}

message PbBootstrapArtifactMetadata {
  required int64 table_id = 1;
  optional int64 partition_id = 3;
  required int32 bucket_id = 4;
  optional string partition_name = 6;
  optional int64 sst_size_bytes = 7;
  optional int64 row_count = 8;
  optional string snapshot_path = 10;  // remote snapshot dir where _METADATA JSON was written
}
```

### 9.2 API Key

**File**: `fluss-rpc/.../protocol/ApiKeys.java`

```java
COMMIT_BOOTSTRAP_ARTIFACTS(1060, 0, 0, PRIVATE);
```

### 9.3 CoordinatorGateway

**File**: `fluss-rpc/.../gateway/CoordinatorGateway.java`

```java
@RPC(api = ApiKeys.COMMIT_BOOTSTRAP_ARTIFACTS)
CompletableFuture<CommitBootstrapArtifactsResponse> commitBootstrapArtifacts(
        CommitBootstrapArtifactsRequest request);
```

---

## 10. Coordinator Processing

### 10.1 CoordinatorService

**File**: `fluss-server/.../coordinator/CoordinatorService.java`

RPC handler — deserializes the request and posts a `CommitBootstrapArtifactsEvent`:

```java
public CompletableFuture<CommitBootstrapArtifactsResponse> commitBootstrapArtifacts(
        CommitBootstrapArtifactsRequest request) {
    CompletableFuture<CommitBootstrapArtifactsResponse> response = new CompletableFuture<>();
    eventManagerSupplier.get().put(
        new CommitBootstrapArtifactsEvent(getBootstrapArtifactsData(request), response));
    return response;
}
```

### 10.2 CoordinatorEventProcessor — handleCommitBootstrapArtifacts()

**File**: `fluss-server/.../coordinator/CoordinatorEventProcessor.java`

Processes the event on the IO executor:

```java
private void handleCommitBootstrapArtifacts(
        CommitBootstrapArtifactsEvent event,
        CompletableFuture<CommitBootstrapArtifactsResponse> callback) {
    ioExecutor.execute(() -> {
        CommitBootstrapArtifactsResponse response = new CommitBootstrapArtifactsResponse();
        for (Map.Entry<Long, Map<TableBucket, BootstrapArtifact>> entry :
                event.getBootstrapArtifactsByTableId().entrySet()) {
            long tableId = entry.getKey();
            PbCommitBootstrapArtifactsRespForTable tableResp = response.addTableResp();
            tableResp.setTableId(tableId);
            try {
                maybeCompleteBootstrapUpgrade(tableId, entry.getValue());
            } catch (Exception e) {
                tableResp.setError(ApiError.fromThrowable(e).error().code(), ...);
            }
        }
        callback.complete(response);
    });
}
```

### 10.3 maybeCompleteBootstrapUpgrade()

The core completion logic (two overloads):

**With artifacts** (called from `handleCommitBootstrapArtifacts`):
```java
private void maybeCompleteBootstrapUpgrade(
        long tableId, Map<TableBucket, BootstrapArtifact> artifacts) {
    // 1. Check state is IN_PROGRESS
    BootstrapUpgradeState state = bootstrapUpgradeStateManager.get(tableId).orElse(null);
    if (state == null || state.getStatus() != IN_PROGRESS) return;

    // 2. Register CompletedSnapshot for each bucket
    //    (reads _METADATA JSON from snapshot path written by BootstrapSstWriter)
    if (tableInfo.isPartitioned() && holdPartitionId != null) {
        registerBootstrapCompletedSnapshots(tableId, ..., artifacts);
    }

    // 3. Mark COMPLETE (before activation to avoid re-dispatch)
    bootstrapUpgradeStateManager.markComplete(tableId);

    // 4. Activate the hold partition (fire CreatePartitionEvent)
    if (tableInfo.isPartitioned() && holdPartitionId != null) {
        activateBootstrapPartition(tableId, ...);
    }
}
```

**Without artifacts** (called from the regular `commitLakeTableSnapshot` path):
```java
private void maybeCompleteBootstrapUpgrade(long tableId) {
    // Only marks COMPLETE; no snapshot registration or partition activation.
    // CompletedSnapshot registration is handled by the artifacts path.
    bootstrapUpgradeStateManager.markComplete(tableId);
}
```

### 10.4 registerBootstrapCompletedSnapshots()

For each bucket, **reads** the `CompletedSnapshot` from the `_METADATA` JSON file that was already written by `BootstrapSstWriter` at the snapshot path:

```java
private void registerBootstrapCompletedSnapshots(
        long tableId, TablePath tablePath, TableInfo tableInfo,
        String holdPartition, long holdPartitionId,
        Map<TableBucket, BootstrapArtifact> artifacts) {
    for (Map.Entry<TableBucket, BootstrapArtifact> entry : artifacts.entrySet()) {
        TableBucket tableBucket = entry.getKey();
        String snapshotPath = entry.getValue().getSnapshotPath();
        if (snapshotPath == null) continue;

        // Read the CompletedSnapshot from _METADATA written by BootstrapSstWriter
        FsPath snapshotLocation = new FsPath(snapshotPath);
        FsPath metadataFilePath = CompletedSnapshot.getMetadataFilePath(snapshotLocation);
        CompletedSnapshotHandle handle = new CompletedSnapshotHandle(1L, metadataFilePath, 0L);
        CompletedSnapshot completedSnapshot = handle.retrieveCompleteSnapshot();

        completedSnapshotStoreManager
            .getOrCreateCompletedSnapshotStore(tablePath, tableBucket)
            .add(completedSnapshot);
    }
}
```

This approach avoids manual `CompletedSnapshot` construction — the coordinator simply reads the full snapshot metadata from remote storage, ensuring consistency with the standard snapshot restore path.

### 10.5 activateBootstrapPartition()

Fires a `CreatePartitionEvent` which triggers the standard partition creation state machine (leader election, `notifyLeaderAndIsr` to replicas):

```java
private void activateBootstrapPartition(long tableId, TablePath tablePath,
        String holdPartition, long holdPartitionId, TableInfo tableInfo) {
    // Read partition assignment from ZK (stored during early creation)
    PartitionAssignment assignment = zooKeeperClient.getPartitionAssignment(holdPartitionId).get();

    // Fire event to trigger standard partition activation
    coordinatorEventManager.put(
        new CreatePartitionEvent(tablePath, tableId, holdPartitionId, partitionName, assignment));
}
```

---

## 11. File Index

| Component | File |
|-----------|------|
| **ZK State** | `fluss-server/.../zk/data/BootstrapUpgradeState.java` |
| **ZK Serde** | `fluss-server/.../zk/data/BootstrapUpgradeStateJsonSerde.java` |
| **State Manager** | `fluss-server/.../coordinator/BootstrapUpgradeStateManager.java` |
| **Artifact Domain** | `fluss-server/.../entity/BootstrapArtifact.java` |
| **Coordinator Event** | `fluss-server/.../coordinator/event/CommitBootstrapArtifactsEvent.java` |
| **Event Processor** | `fluss-server/.../coordinator/CoordinatorEventProcessor.java` |
| **Coordinator RPC** | `fluss-server/.../coordinator/CoordinatorService.java` |
| **Split Base** | `fluss-flink/.../source/split/TieringSplit.java` |
| **Bootstrap Split** | `fluss-flink/.../source/split/TieringBootstrapSplit.java` |
| **Split Generator** | `fluss-flink/.../source/split/TieringSplitGenerator.java` |
| **Split Serializer** | `fluss-flink/.../source/split/TieringSplitSerializer.java` |
| **SST Writer** | `fluss-flink/.../source/BootstrapSstWriter.java` |
| **SST Writer Design** | `design_doc/bootstrap-sst-writer-design.md` |
| **Source Reader** | `fluss-flink/.../source/TieringSplitReader.java` |
| **Write Result** | `fluss-flink/.../source/TableBucketWriteResult.java` |
| **Result Serializer** | `fluss-flink/.../source/TableBucketWriteResultSerializer.java` |
| **Commit Operator** | `fluss-flink/.../committer/TieringCommitOperator.java` |
| **Snapshot Committer** | `fluss-flink/.../committer/FlussTableLakeSnapshotCommitter.java` |
| **Protobuf** | `fluss-rpc/.../proto/FlussApi.proto` |
| **API Key** | `fluss-rpc/.../protocol/ApiKeys.java` |
| **Gateway** | `fluss-rpc/.../gateway/CoordinatorGateway.java` |
| **IT Test** | `fluss-server/.../coordinator/BootstrapUpgradeLifecycleITCase.java` |
