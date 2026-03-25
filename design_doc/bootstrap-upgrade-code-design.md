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
    |                                        |     |   flush() -> checkpoint           |
    |                                        |     |   upload SSTs --------------->    |
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

| Method                                                  | Description                               |
|---------------------------------------------------------|-------------------------------------------|
| `initializeInProgress(tableId, holdPartition)`          | Create IN_PROGRESS state (idempotent)     |
| `updateHoldPartitionId(tableId, holdPartitionId)`       | Store assigned partition ID               |
| `markComplete(tableId, holdPartition, holdPartitionId)` | Transition to COMPLETE                    |
| `get(tableId)`                                          | Load current state                        |
| `deleteIfPresent(tableId)`                              | Clean up on table drop / datalake disable |

### 2.3 BootstrapArtifact (server-side domain)

**File**: `fluss-server/.../entity/BootstrapArtifact.java`

Metadata about a bootstrap SST artifact for a single bucket:

```java
public class BootstrapArtifact {
    private final TableBucket tableBucket;
    @Nullable private final String partitionName;
    private final long sstSizeBytes;
    private final long rowCount;
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

Uses a temporary RocksDB instance as a disk-backed write buffer to avoid OOM. On `flush()`, creates a RocksDB Checkpoint and uploads the resulting SST files.

### 5.1 Constructor

```java
BootstrapSstWriter(TableInfo tableInfo, TieringBootstrapSplit split) {
    // Validate PK table
    // Initialize RowSerializer, KeyEncoder, schemaId
    // Open temp RocksDB (WAL disabled)
    RocksDB.loadLibrary();
    this.tempDbDir = Files.createTempDirectory("fluss-bootstrap-rocksdb-");
    this.tempDbOptions = new Options().setCreateIfMissing(true);
    this.tempWriteOptions = new WriteOptions().setDisableWAL(true);
    this.tempDb = RocksDB.open(tempDbOptions, tempDbDir.toString());
}
```

### 5.2 write()

```java
void write(LogRecord record) {
    byte[] keyBytes = primaryKeyEncoder.encodeKey(record.getRow());
    if (record.getChangeType() == ChangeType.DELETE) {
        tempDb.delete(tempWriteOptions, keyBytes);   // tombstone
    } else {
        BinaryRow binaryRow = rowSerializer.toBinaryRow(record.getRow());
        byte[] valueBytes = ValueEncoder.encodeValue(schemaId, binaryRow);
        tempDb.put(tempWriteOptions, keyBytes, valueBytes);  // upsert
    }
}
```

Key properties:
- **Deduplication**: Later puts win (RocksDB's default behavior)
- **Sorting**: RocksDB's default `BytewiseComparator` = unsigned byte-by-byte lexicographic = matches `Arrays.compareUnsigned`
- **Memory safety**: RocksDB auto-flushes memtable to disk when it reaches configured limits

### 5.3 flush()

```java
@Nullable
String flush() {
    // 1. Flush memtable to ensure all data is in SST files
    tempDb.flush(new FlushOptions().setWaitForFlush(true));

    // 2. Create a consistent checkpoint
    Path checkpointDir = Files.createTempDirectory("fluss-bootstrap-checkpoint-");
    Checkpoint.create(tempDb).createCheckpoint(checkpointDir.toString());

    // 3. Collect .sst files from checkpoint directory
    List<Path> sstFiles = Files.list(checkpointDir)
        .filter(p -> p.getFileName().toString().endsWith(".sst"))
        .collect(toList());

    // 4. Upload SST files to remote storage
    if (split.getRemoteDataDir() != null) {
        for (Path sstFile : sstFiles) {
            uploadToRemoteStorage(sstFile, split.getRemoteDataDir());
        }
    }

    // 5. Cleanup checkpoint dir
    FileUtils.deleteDirectoryQuietly(checkpointDir.toFile());
    return "uploaded " + sstFiles.size() + " SST files";
}
```

### 5.4 Upload path

Each SST file is uploaded to:
```
{remoteDataDir}/kv/{db}/{table}/{partition}/{tableId}-{bucketId}/shared/{uuid}.sst
```

This follows the existing `FlussPaths.remoteKvSharedDir()` convention so that replicas can find and download the SST files using the standard snapshot restore path.

### 5.5 close()

Implements `Closeable` — cleans up temp RocksDB (db, options, temp directory).

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

Creates a `TableBucketWriteResult` with `bootstrap=true` and `writeResult=null` (no lake write result for bootstrap):

```java
TableBucketWriteResult<WriteResult> writeResult = new TableBucketWriteResult<>(
    bootstrapSplit.getTablePath(),
    tableBucket,
    bootstrapSplit.getPartitionName(),
    null,                              // no lake write result
    UNKNOWN_BUCKET_OFFSET,
    UNKNOWN_BUCKET_TIMESTAMP,
    currentTableNumberOfSplits,
    true);                             // bootstrap = true
```

---

## 7. Write Result & Serialization

### 7.1 TableBucketWriteResult

**File**: `fluss-flink/.../source/TableBucketWriteResult.java`

Added `bootstrap` boolean flag to distinguish bootstrap results from normal tiering:

```java
public class TableBucketWriteResult<WriteResult> implements Serializable {
    // ... existing fields ...
    private final boolean bootstrap;  // true if this is a bootstrap-upgrade result

    public boolean isBootstrap() { return bootstrap; }
}
```

### 7.2 TableBucketWriteResultSerializer

**File**: `fluss-flink/.../source/TableBucketWriteResultSerializer.java`

- Current version: `VERSION_4`
- Serialization: writes `bootstrap` as boolean after `numberOfWriteResults`
- Deserialization: handles backward compatibility for VERSION_2 (skips legacy `bootstrapArtifactPath`), VERSION_3 (skips legacy `tieringEpoch`), VERSION_4+ reads `bootstrap` flag

```java
// Serialize (VERSION_4)
out.writeBoolean(tableBucketWriteResult.isBootstrap());

// Deserialize (backward compatible)
if (version >= VERSION_2 && version < VERSION_4) {
    if (in.readBoolean()) in.readUTF();     // skip bootstrapArtifactPath
}
if (version == VERSION_3) {
    in.readLong();                           // skip tieringEpoch
}
boolean bootstrap = version >= VERSION_4 && in.readBoolean();
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
        flussTableLakeSnapshotCommitter.commitBootstrapArtifacts(tableId, partitionNames);
    }

    // 4. If only bootstrap results, return null (no lake commit needed)
    if (committableWriteResults.isEmpty()) return null;

    // 5. Otherwise, proceed with normal lake commit...
}
```

### 8.2 FlussTableLakeSnapshotCommitter.commitBootstrapArtifacts()

**File**: `fluss-flink/.../committer/FlussTableLakeSnapshotCommitter.java`

Sends `CommitBootstrapArtifactsRequest` to coordinator:

```java
void commitBootstrapArtifacts(long tableId, Map<TableBucket, String> partitionNames) {
    CommitBootstrapArtifactsRequest request = new CommitBootstrapArtifactsRequest();
    for (Map.Entry<TableBucket, String> entry : partitionNames.entrySet()) {
        PbBootstrapArtifactMetadata metadata = request.addBootstrapArtifactMetadata();
        metadata.setTableId(tableId);
        if (entry.getKey().getPartitionId() != null) {
            metadata.setPartitionId(entry.getKey().getPartitionId());
        }
        metadata.setBucketId(entry.getKey().getBucket());
        if (entry.getValue() != null) {
            metadata.setPartitionName(entry.getValue());
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

The core completion logic:

```java
private void maybeCompleteBootstrapUpgrade(
        long tableId, Map<TableBucket, BootstrapArtifact> artifacts) {
    // 1. Check state is IN_PROGRESS
    BootstrapUpgradeState state = bootstrapUpgradeStateManager.get(tableId).orElse(null);
    if (state == null || state.getStatus() != IN_PROGRESS) return;

    // 2. Register CompletedSnapshot for each bucket
    //    (so replicas can find the bootstrap SST)
    if (tableInfo.isPartitioned() && holdPartitionId != null) {
        registerBootstrapCompletedSnapshots(tableId, ...);
    }

    // 3. Mark COMPLETE (before activation to avoid re-dispatch)
    bootstrapUpgradeStateManager.markComplete(tableId, holdPartition, holdPartitionId);

    // 4. Activate the hold partition (fire CreatePartitionEvent)
    if (tableInfo.isPartitioned() && holdPartitionId != null) {
        activateBootstrapPartition(tableId, ...);
    }
}
```

### 10.4 registerBootstrapCompletedSnapshots()

For each bucket, creates a `CompletedSnapshot` with a `KvSnapshotHandle` pointing to the remote SST:

```java
// Snapshot ID = 1 (initial bootstrap snapshot)
long snapshotId = 1L;

KvFileHandle sstFileHandle = new KvFileHandle(remoteSstPath, sstSizeBytes);
KvFileHandleAndLocalPath sharedFile = KvFileHandleAndLocalPath.of(sstFileHandle, sstFileName);
KvSnapshotHandle kvSnapshotHandle = new KvSnapshotHandle(
    Collections.singletonList(sharedFile), Collections.emptyList(), sstSizeBytes);

CompletedSnapshot completedSnapshot = new CompletedSnapshot(
    tableBucket, snapshotId, snapshotLocation, kvSnapshotHandle, 0L, rowCount, null);

completedSnapshotStoreManager
    .getOrCreateCompletedSnapshotStore(tablePath, tableBucket)
    .add(completedSnapshot);
```

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
| **Source Reader** | `fluss-flink/.../source/TieringSplitReader.java` |
| **Write Result** | `fluss-flink/.../source/TableBucketWriteResult.java` |
| **Result Serializer** | `fluss-flink/.../source/TableBucketWriteResultSerializer.java` |
| **Commit Operator** | `fluss-flink/.../committer/TieringCommitOperator.java` |
| **Snapshot Committer** | `fluss-flink/.../committer/FlussTableLakeSnapshotCommitter.java` |
| **Protobuf** | `fluss-rpc/.../proto/FlussApi.proto` |
| **API Key** | `fluss-rpc/.../protocol/ApiKeys.java` |
| **Gateway** | `fluss-rpc/.../gateway/CoordinatorGateway.java` |
| **IT Test** | `fluss-server/.../coordinator/BootstrapUpgradeLifecycleITCase.java` |
