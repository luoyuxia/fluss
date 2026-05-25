# PR 8: Paimon Write Path DV Adaptation (__rowid + Write Logic)

## Context

PRs 0-7 implemented shared DV infrastructure (DvRocksDB, DvManager, KV State RowId, changelog format, SST generation, protocol extensions, union-read DV support). PR 8 enters **Phase 2 (Paimon-specific)**: adapting MergeTreeWriter for DV mode.

**Problem**: Currently, MergeTreeWriter writes all changelog types (-U, -D, +I, +U) to Paimon with `UNKNOWN_SEQUENCE` and no `__rowid` column. For DV to work, Paimon needs:
1. A `__rowid` system column storing the RowId (= logOffset) so compaction can rebuild RowId→FilePos mappings
2. DV-aware write semantics: skip -U, write -D as DELETE, use logOffset as sequence number
3. LogDv filtering to avoid writing superseded +I/+U records

**Design doc ref**: `design-doc/plan/paimon-dv-implementation-plan.md` §PR 8, §5.2 Phase A1, §10.3

---

## Step 1: `__rowid` System Column in Paimon Schema

### 1a. Add constant in `PaimonLakeCatalog`
**File**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/PaimonLakeCatalog.java`

Add constant (next to existing SYSTEM_COLUMNS):
```java
public static final String ROWID_COLUMN_NAME = "__rowid";
```

Keep `SYSTEM_COLUMNS` unchanged (3 entries: __bucket, __offset, __timestamp). The `__rowid` column is DV-specific and added conditionally.

### 1b. Conditionally add `__rowid` in `toPaimonSchema()`
**File**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/utils/PaimonConversions.java`

After the existing loop that adds SYSTEM_COLUMNS (line 214-216), add:

```java
// Add __rowid system column for DV-enabled tables
if (options.get(CoreOptions.DELETION_VECTORS_ENABLED)) {
    schemaBuilder.column(PaimonLakeCatalog.ROWID_COLUMN_NAME, DataTypes.BIGINT());
}
```

This must be placed **after** the DV option sync (line 235-245) so `DELETION_VECTORS_ENABLED` is already set. Move the `__rowid` addition to after line 245 (after DV options are resolved).

### 1c. Validate `__rowid` not used as user column name
In the column name conflict check loop (line 198-206), also check `ROWID_COLUMN_NAME`:
```java
if (SYSTEM_COLUMNS.containsKey(columnName)
        || columnName.equals(PaimonLakeCatalog.ROWID_COLUMN_NAME)) {
    throw new InvalidTableException(...);
}
```

### 1d. Handle `__rowid` in schema change (AddColumn positioning)
In `toPaimonSchemaChanges()` (line 152), new business columns are inserted before the first system column. The `firstSystemColumnName` is currently `__bucket`. This remains correct because `__rowid` is added after the 3 standard system columns, so new business columns still go before `__bucket`.

---

## Step 2: `FlussRecordAsPaimonRow` DV Support

**File**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/FlussRecordAsPaimonRow.java`

### 2a. Add `dvEnabled` constructor parameter
```java
private final boolean dvEnabled;
private final int rowidFieldIndex;  // -1 if DV disabled

public FlussRecordAsPaimonRow(int bucket, RowType tableRowType, boolean dvEnabled) {
    super(tableRowType);
    this.bucket = bucket;
    this.dvEnabled = dvEnabled;
    int systemColumnCount = dvEnabled ? 4 : 3;  // +1 for __rowid
    this.businessFieldCount = tableRowType.getFieldCount() - systemColumnCount;
    this.bucketFieldIndex = businessFieldCount;
    this.offsetFieldIndex = businessFieldCount + 1;
    this.timestampFieldIndex = businessFieldCount + 2;
    this.rowidFieldIndex = dvEnabled ? businessFieldCount + 3 : -1;
}
```

Keep the old 2-arg constructor for backward compatibility (calls `this(bucket, tableRowType, false)`).

### 2b. Handle `__rowid` in `getLong(int pos)`
Add to `getLong()` method, after the timestamp check:
```java
if (dvEnabled && pos == rowidFieldIndex) {
    // __rowid system column = logOffset (RowId)
    return logRecord.logOffset();
}
```

### 2c. Handle `__rowid` in `isNullAt(int pos)`
The existing logic checks `pos < businessFieldCount` for padding and then returns false for system fields. With `__rowid`, the "last N system fields" comment changes from 3 to 4 when DV is enabled. The existing code already handles this correctly since `getFieldCount()` returns `tableRowType.getFieldCount()` which includes `__rowid`.

---

## Step 3: `RecordWriter` Passes `dvEnabled`

**File**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/RecordWriter.java`

### 3a. Add `dvEnabled` parameter to constructor
Add `boolean dvEnabled` parameter to the constructor (line 49-55). Pass it to `FlussRecordAsPaimonRow`:

```java
this.flussRecordAsPaimonRow =
        new FlussRecordAsPaimonRow(tableBucket.getBucket(), tableRowType, dvEnabled);
```

### 3b. Store dvEnabled for subclass access
```java
protected final boolean dvEnabled;
```

---

## Step 4: `MergeTreeWriter` DV Mode

**File**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/mergetree/MergeTreeWriter.java`

### 4a. Add `logDvBitmap` field
```java
@Nullable private final Roaring64Bitmap logDvBitmap;
```

Add `@Nullable byte[] logDvBitmapBytes` parameter to constructor. Deserialize to `Roaring64Bitmap` if non-null:
```java
if (logDvBitmapBytes != null) {
    this.logDvBitmap = new Roaring64Bitmap();
    RoaringBitmapUtils.deserializeRoaringBitmap64(this.logDvBitmap, logDvBitmapBytes);
} else {
    this.logDvBitmap = null;
}
```

### 4b. Rewrite `write()` method for DV mode
```java
@Override
public void write(LogRecord record) throws Exception {
    if (dvEnabled) {
        writeDvMode(record);
    } else {
        writeNormalMode(record);
    }
}

private void writeNormalMode(LogRecord record) throws Exception {
    // Existing logic (current write() body)
    flussRecordAsPaimonRow.setFlussRecord(record);
    rowKeyExtractor.setRecord(flussRecordAsPaimonRow);
    keyValue.replace(
            rowKeyExtractor.trimmedPrimaryKey(),
            KeyValue.UNKNOWN_SEQUENCE,
            toRowKind(record.getChangeType()),
            flussRecordAsPaimonRow);
    tableWrite.getWrite().write(partition, bucket, keyValue);
}

private void writeDvMode(LogRecord record) throws Exception {
    ChangeType changeType = record.getChangeType();

    // Skip -U (UPDATE_BEFORE): not written to Paimon in DV mode
    if (changeType == ChangeType.UPDATE_BEFORE) {
        return;
    }

    // For +I/+U: check LogDv filter
    if (changeType == ChangeType.INSERT || changeType == ChangeType.UPDATE_AFTER) {
        if (logDvBitmap != null && logDvBitmap.contains(record.logOffset())) {
            // This record has been superseded by a later -U/-D, skip
            return;
        }
    }

    flussRecordAsPaimonRow.setFlussRecord(record);
    rowKeyExtractor.setRecord(flussRecordAsPaimonRow);

    // Use logOffset as sequence number for DV ordering
    long seq = record.logOffset();
    RowKind rowKind;
    if (changeType == ChangeType.DELETE) {
        rowKind = RowKind.DELETE;
    } else {
        rowKind = RowKind.INSERT;  // Both +I and +U written as INSERT
    }

    keyValue.replace(
            rowKeyExtractor.trimmedPrimaryKey(),
            seq,
            rowKind,
            flussRecordAsPaimonRow);
    tableWrite.getWrite().write(partition, bucket, keyValue);
}
```

### 4c. Update constructor chain
Both public constructor and package-private constructor need `@Nullable byte[] logDvBitmapBytes`. Pass through to the chain.

### 4d. Add dependency for `ChangeType`
Import `org.apache.fluss.record.ChangeType`.

---

## Step 5: `PaimonLakeWriter` Wiring

**File**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/PaimonLakeWriter.java`

### 5a. Determine `dvEnabled` from table info
```java
boolean dvEnabled = writerInitContext.tableInfo().isDeletionVectorsEnabled();
byte[] logDvBitmap = writerInitContext.logDvBitmap();
```

### 5b. Pass to MergeTreeWriter
```java
this.recordWriter =
        fileStoreTable.primaryKeys().isEmpty()
                ? new AppendOnlyWriter(
                        fileStoreTable,
                        writerInitContext.tableBucket(),
                        writerInitContext.partition(),
                        partitionKeys,
                        flussRowType)
                : new MergeTreeWriter(
                        fileStoreTable,
                        writerInitContext.tableBucket(),
                        writerInitContext.partition(),
                        partitionKeys,
                        flussRowType,
                        dvEnabled,
                        logDvBitmap);
```

Note: `AppendOnlyWriter` hardcodes `dvEnabled=false` — append-only tables never use DV.

---

## Step 6: `WriterInitContext` Extension

**File**: `fluss-common/src/main/java/org/apache/fluss/lake/writer/WriterInitContext.java`

Add default method:
```java
/**
 * Returns the serialized LogDv bitmap for filtering superseded records during DV tiering.
 * Returns null if DV is not enabled or LogDv is not available.
 */
@Nullable
default byte[] logDvBitmap() {
    return null;
}
```

---

## Step 7: `TieringWriterInitContext` Extension

**File**: `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/TieringWriterInitContext.java`

Add `@Nullable byte[] logDvBitmap` field, constructor parameter, and getter override:
```java
@Nullable private final byte[] logDvBitmap;

public TieringWriterInitContext(
        TablePath tablePath,
        TableBucket tableBucket,
        @Nullable String partition,
        TableInfo tableInfo,
        @Nullable byte[] logDvBitmap) {
    // ... existing fields ...
    this.logDvBitmap = logDvBitmap;
}

@Nullable
@Override
public byte[] logDvBitmap() {
    return logDvBitmap;
}
```

Keep backward-compatible 4-arg constructor that passes `null` for logDvBitmap.

---

## Step 8: LogDv Fetching in Tiering Path

**Design**: Combine `endOffset` and `logDvBitmap` into a single `getDvSnapshot` RPC call to reduce round trips.
For DV-enabled PK tables, the split generator calls `getDvSnapshot(fromOffset)` per bucket. The server
determines `toOffset` from the current log end, computes LogDv bitmap for `[fromOffset, logEndOffset)`, and
returns both `logEndOffset` (used as the split's `stoppingOffset`) and `logDvBitmap` in one response.
The `fromOffset` is the last committed offset from the lake snapshot (i.e., the previous tiering's end offset).

### 8a. Extend `GetDvSnapshotRequest` proto
**File**: `fluss-rpc/src/main/proto/FlussApi.proto`

Add optional field to `GetDvSnapshotRequest`:
```proto
message GetDvSnapshotRequest {
  required int64 table_id = 1;
  required int32 bucket_id = 2;
  required int64 readable_snapshot_id = 3;
  optional int64 partition_id = 4;
  // When set, returns LogDv bitmap for [log_dv_from_offset, logEndOffset)
  // along with logEndOffset. Used by tiering to get both stoppingOffset
  // and logDvBitmap in a single RPC.
  optional int64 log_dv_from_offset = 5;
}
```

### 8b. Add `DvManager.getLogDvSnapshot()`
**File**: `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvManager.java`

```java
/**
 * Returns a serialized LogDv bitmap for the given offset range [fromOffset, toOffset).
 * Used by tiering to filter superseded records.
 */
@Nullable
public byte[] getLogDvSnapshot(long fromOffset, long toOffset) throws IOException {
    Roaring64Bitmap bitmap = dvRocksDB.logDv().snapshot(fromOffset, toOffset);
    if (bitmap.isEmpty()) {
        return null;
    }
    return RoaringBitmapUtils.serializeRoaringBitmap64(bitmap);
}
```

### 8c. Handle in `ReplicaManager`
**File**: `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

Add `getLogDvSnapshot()` method: when `logDvFromOffset` is provided, use current log end offset
as `toOffset`, call `dvManager.getLogDvSnapshot(fromOffset, logEndOffset)`, and return a response
with `logDvBitmap` and `logEndOffset`. No LakeDv, no snapshot validation.

### 8d. Handle in `TabletService.getDvSnapshot()`
**File**: `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`

If request has `logDvFromOffset`, route to `replicaManager.getLogDvSnapshot()`.

### 8e. Add `Admin.getLogDvBitmap()` method
**File**: `fluss-client/src/main/java/org/apache/fluss/client/admin/Admin.java`

```java
/** Returns logEndOffset and logDvBitmap for [fromOffset, logEndOffset) in a single RPC. */
CompletableFuture<GetDvSnapshotResponse> getLogDvBitmap(
        TablePath tablePath,
        long tableId,
        @Nullable Long partitionId,
        int bucketId,
        long fromOffset);
```

### 8f. Implement in `FlussAdmin`
**File**: `fluss-client/src/main/java/org/apache/fluss/client/admin/FlussAdmin.java`

Build `GetDvSnapshotRequest` with `log_dv_from_offset` set, `readable_snapshot_id` = -1 (unused).
Server determines `toOffset` from current log end.

### 8g. Add `logDvBitmap` to `TieringLogSplit`
**File**: `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/split/TieringLogSplit.java`

Add field to carry the bitmap through the split:
```java
@Nullable private final byte[] logDvBitmap;
```

Add 8-arg constructor with `logDvBitmap`. Keep existing constructors (pass `null`). Add getter and `withLogDvBitmap()`.
Update `copy()`, `equals()`, `hashCode()`, `toString()`.

### 8h. Serialize `logDvBitmap` in `TieringSplitSerializer`
**File**: `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/split/TieringSplitSerializer.java`

Serialize: `boolean hasLogDv + int length + byte[] data`. Deserialize: read back and pass to 8-arg constructor.

### 8i. Preserve `logDvBitmap` in `TieringSplitState`
**File**: `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/state/TieringSplitState.java`

`toSourceSplit()` must use the 8-arg constructor to preserve `logDvBitmap` and `skipCurrentRound`.

### 8j. Fetch LogDv + endOffset in `TieringSplitGenerator`
**File**: `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/split/TieringSplitGenerator.java`

For DV-enabled PK tables, replace the separate `latestBucketsOffset` fetch + post-enrichment
with a single `getLogDvBitmap(fromOffset)` call per bucket that returns both `logEndOffset`
(as `stoppingOffset`) and `logDvBitmap`:

```java
// In generateTableSplit(), for DV-enabled PK tables:
if (tableInfo.hasPrimaryKey() && tableInfo.isDeletionVectorsEnabled()) {
    for (int bucket = 0; bucket < tableInfo.getNumBuckets(); bucket++) {
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, bucket);
        Long lastCommittedBucketOffset = lakeSnapshotInfo != null
                ? lakeSnapshotInfo.getTableBucketsOffset().get(tableBucket) : null;
        long fromOffset = lastCommittedBucketOffset != null
                ? lastCommittedBucketOffset : EARLIEST_OFFSET;

        // Single RPC: get both logEndOffset and logDvBitmap
        GetDvSnapshotResponse resp = flussAdmin.getLogDvBitmap(
                tableInfo.getTablePath(), tableInfo.getTableId(),
                partitionId, bucket, fromOffset).get();

        long stoppingOffset = resp.getLogEndOffset();
        byte[] logDvBitmap = resp.hasLogDvBitmap() ? resp.getLogDvBitmap() : null;

        if (fromOffset < stoppingOffset) {
            splits.add(new TieringLogSplit(
                    tableInfo.getTablePath(), tableBucket, partitionName,
                    fromOffset, stoppingOffset, 0, false, logDvBitmap));
        }
    }
}
```

This replaces the existing `generateSplitForPrimaryKeyTableBucket` + `enrichSplitsWithLogDvBitmap`
flow for DV-enabled tables. Non-DV PK tables continue to use the existing flow unchanged.

### 8k. Read LogDv from split in `TieringSplitReader`
**File**: `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/TieringSplitReader.java`

In `getOrCreateLakeWriter()`, get the bitmap from the split:
```java
byte[] logDvBitmap = null;
TieringSplit split = currentTableSplitsByBucket.get(bucket);
if (split != null && split.isTieringLogSplit()) {
    logDvBitmap = split.asTieringLogSplit().getLogDvBitmap();
}
```

No RPC needed — the reader reads what the enumerator already fetched.

---

## Step 9: `AppendOnlyWriter` Update

**File**: `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/append/AppendOnlyWriter.java`

No `dvEnabled` parameter needed — append-only tables never use DV. Remove the `boolean dvEnabled` constructor parameter and hardcode `false` in the `super()` call. The write logic remains unchanged.

---

## Step 10: RoaringBitmap Dependency

**File**: `fluss-lake/fluss-lake-paimon/pom.xml`

Add RoaringBitmap dependency (needed for MergeTreeWriter's LogDv deserialization):
```xml
<dependency>
    <groupId>org.roaringbitmap</groupId>
    <artifactId>RoaringBitmap</artifactId>
</dependency>
```

Check if it's already in the parent POM's `<dependencyManagement>`. If not, add version there too.

Also add `RoaringBitmapUtils` usage — check if it's in `fluss-server` only or available in `fluss-common`.

**Alternative**: If RoaringBitmap dependency is problematic for `fluss-lake-paimon`, keep `logDvBitmap` as `byte[]` in MergeTreeWriter and use a simple helper to check membership. But the `Roaring64Bitmap` approach is more efficient.

---

## Step 11: Tests

### 11a. `MergeTreeWriterDvTest` (NEW)
**File**: `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/tiering/mergetree/MergeTreeWriterDvTest.java`

Test cases:
- `testDvModeSkipsUpdateBefore`: -U records not written to Paimon
- `testDvModeDeleteWrittenAsDelete`: -D records written with RowKind.DELETE and seq=logOffset
- `testDvModeInsertWrittenWithSeqAndRowid`: +I records have seq=logOffset, __rowid populated
- `testDvModeUpdateAfterWrittenAsInsert`: +U records written as RowKind.INSERT
- `testDvModeLogDvFilterSkipsSupersededRecords`: +I/+U with offset in LogDv bitmap are skipped
- `testDvModeLogDvFilterNull`: when logDvBitmap is null, all records written
- `testNonDvModeUnchanged`: existing behavior preserved when dvEnabled=false

### 11b. `FlussRecordAsPaimonRowDvTest` (NEW)
**File**: `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/tiering/FlussRecordAsPaimonRowDvTest.java`

Test cases:
- `testRowidFieldPopulated`: __rowid returns logOffset
- `testFieldCountWithDv`: getFieldCount() includes __rowid
- `testBusinessFieldCountWithDv`: business fields computed correctly with 4 system columns
- `testBackwardCompatNonDv`: 2-arg constructor works as before

### 11c. `PaimonConversionsTest` (EXTEND)
**File**: `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/utils/PaimonConversionsTest.java`

Test cases:
- `testToPaimonSchemaDvEnabled`: schema includes `__rowid` column after system columns
- `testToPaimonSchemaDvDisabled`: schema has no `__rowid` column
- `testToPaimonSchemaRowidColumnConflict`: user column named `__rowid` throws InvalidTableException

### 11d. `DvManagerTest` (EXTEND)
**File**: `fluss-server/src/test/java/org/apache/fluss/server/kv/dv/DvManagerTest.java`

Test cases:
- `testGetLogDvSnapshot`: returns correct bitmap for given offset range
- `testGetLogDvSnapshotEmpty`: empty range returns null

---

## Critical Files Summary

| File | Op | Notes |
|------|----|-------|
| `fluss-lake/fluss-lake-paimon/.../PaimonLakeCatalog.java` | MODIFY | `ROWID_COLUMN_NAME` constant |
| `fluss-lake/fluss-lake-paimon/.../utils/PaimonConversions.java` | MODIFY | Conditional `__rowid` in schema |
| `fluss-lake/fluss-lake-paimon/.../tiering/FlussRecordAsPaimonRow.java` | MODIFY | dvEnabled, __rowid field |
| `fluss-lake/fluss-lake-paimon/.../tiering/RecordWriter.java` | MODIFY | Pass dvEnabled |
| `fluss-lake/fluss-lake-paimon/.../tiering/mergetree/MergeTreeWriter.java` | MODIFY | DV mode write logic |
| `fluss-lake/fluss-lake-paimon/.../tiering/PaimonLakeWriter.java` | MODIFY | Wire dvEnabled + logDvBitmap |
| `fluss-lake/fluss-lake-paimon/.../tiering/append/AppendOnlyWriter.java` | MODIFY | Pass dvEnabled to super |
| `fluss-common/.../lake/writer/WriterInitContext.java` | MODIFY | Add `logDvBitmap()` default |
| `fluss-flink/fluss-flink-common/.../tiering/source/TieringWriterInitContext.java` | MODIFY | Add logDvBitmap field |
| `fluss-flink/fluss-flink-common/.../tiering/source/split/TieringLogSplit.java` | MODIFY | Add `logDvBitmap` field |
| `fluss-flink/fluss-flink-common/.../tiering/source/split/TieringSplitSerializer.java` | MODIFY | Serialize `logDvBitmap` |
| `fluss-flink/fluss-flink-common/.../tiering/source/split/TieringSplitGenerator.java` | MODIFY | Combined RPC: fetch endOffset + LogDv |
| `fluss-flink/fluss-flink-common/.../tiering/source/state/TieringSplitState.java` | MODIFY | Preserve `logDvBitmap` in `toSourceSplit()` |
| `fluss-flink/fluss-flink-common/.../tiering/source/TieringSplitReader.java` | MODIFY | Read LogDv from split |
| `fluss-rpc/.../proto/FlussApi.proto` | MODIFY | Add optional offset range fields |
| `fluss-server/.../kv/dv/DvManager.java` | MODIFY | `getLogDvSnapshot()` |
| `fluss-server/.../replica/ReplicaManager.java` | MODIFY | Handle LogDv-only mode |
| `fluss-server/.../tablet/TabletService.java` | MODIFY | Pass new fields |
| `fluss-client/.../admin/Admin.java` | MODIFY | `getLogDvBitmap()` |
| `fluss-client/.../admin/FlussAdmin.java` | MODIFY | Implement getLogDvBitmap |

## Existing Utilities to Reuse

- `TableInfo.isDeletionVectorsEnabled()` — check DV flag (`fluss-common/.../metadata/TableInfo.java:247`)
- `RoaringBitmapUtils.serializeRoaringBitmap64()` / `deserializeRoaringBitmap64()` — bitmap serde (`fluss-server/.../utils/RoaringBitmapUtils.java`)
- `LogDv.snapshot(from, to)` — range bitmap snapshot (`fluss-server/.../kv/dv/LogDv.java:85`)
- `Connection.getAdmin()` — get Admin from connection (`fluss-client/.../Connection.java:54`)
- `ConfigOptions.TABLE_DELETION_VECTORS_ENABLED` — DV config option (`fluss-common/.../config/ConfigOptions.java:1604`)
- `PaimonConversions.toRowKind()` — ChangeType to RowKind (`fluss-lake/fluss-lake-paimon/.../utils/PaimonConversions.java:78`)

---

## Verification

1. **Regenerate proto**: `./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`
2. **Compile**: `./mvnw compile -pl fluss-lake/fluss-lake-paimon,fluss-flink/fluss-flink-common,fluss-server,fluss-client -am -DskipTests`
3. **Format**: `./mvnw spotless:apply -pl fluss-lake/fluss-lake-paimon,fluss-flink/fluss-flink-common,fluss-server,fluss-client,fluss-rpc,fluss-common`
4. **Run Paimon tests**: `./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=MergeTreeWriterDvTest,FlussRecordAsPaimonRowDvTest,PaimonConversionsTest`
5. **Run DvManager tests**: `./mvnw test -pl fluss-server -Dtest=DvManagerTest`
6. **Run existing Paimon tests**: `./mvnw test -pl fluss-lake/fluss-lake-paimon` (regression check)
