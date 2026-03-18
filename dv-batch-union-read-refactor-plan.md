# DV-Enabled PK Table Batch Union Read Refactor Plan

## Background

Current batch union read for primary-key tables uses `LakeSnapshotAndFlussLogSplit` to combine:

- Lake snapshot splits from the readable lake snapshot
- A bounded Fluss log range from the snapshot log offset to the query stopping offset

Then `LakeSnapshotAndLogSplitScanner` loads both sides and performs a PK sort-merge in memory.

This is correct for the generic PK-table path, but it is unnecessarily expensive for **DV-enabled** tables.

For DV-enabled tables, we already maintain two logical deletion layers:

- **LakeDv**: removes rows in the readable lake snapshot that were logically deleted or superseded after tiering
- **LogDv**: removes stale records inside the incremental Fluss log range

If both layers are applied during query-time reading, the remaining rows from lake and log are already non-overlapping in key space, so a PK sort-merge is no longer necessary.

## Goal

For **primary-key tables with deletion vector enabled**, replace the current batch union-read sort-merge path with a DV-aware path:

- Read lake snapshot splits and apply `LakeDv`
- Read bounded log splits and apply `LogDv`
- Concatenate both streams directly
- Avoid in-memory PK sort-merge

## Key Observation

The correctness invariant for the new path is:

1. **Lake side** only returns rows that are still valid in the requested readable snapshot after applying `LakeDv`
2. **Log side** only returns rows that are still valid in `(readableSnapshotTieredOffset, logEndOffset]` after applying `LogDv`
3. The two result sets do not overlap logically for the same PK at the chosen consistency point

If this invariant holds, sort-merge can be removed for this branch.

## Why the Current `LakeSnapshotAndFlussLogSplit` Path Is Wasteful

Today the batch query path is:

1. `FlinkSourceEnumerator` enters batch mode and generates hybrid lake + Fluss splits
2. `LakeSplitGenerator` creates `LakeSnapshotAndFlussLogSplit` for PK tables
3. `LakeSplitReaderGenerator` creates `LakeSnapshotAndLogSplitScanner`
4. `LakeSnapshotAndLogSplitScanner`:
   - reads all lake snapshot records
   - reads all bounded log records
   - stores log rows in a tree map
   - merges both sides via `SortMergeReader`

This has several costs:

- extra buffering and sorting work for the bounded log side
- extra merge cost proportional to snapshot size + incremental log size
- complicated projected-key handling just for merge
- duplicated correctness machinery, because DV already encodes staleness

## Proposed Refactor Direction

### High-level strategy

Keep the existing generic union-read path intact for:

- non-PK tables
- PK tables without DV
- fallback scenarios

Add a **new DV-aware batch union-read path** for:

- `tableInfo.hasPrimaryKey() == true`
- `tableInfo.getTableConfig().isDvEnabled() == true`

This new path should be selected during split generation / split reading and should not use sort-merge.

---

## Proposed New Types

### 1. New split class

Add a dedicated split for DV-aware batch union read.

Suggested name:

- `DvAwareLakeSnapshotAndFlussLogSplit`

Suggested location:

- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/split/`

### Suggested fields

Basic identity / bounds:

- `TableBucket tableBucket`
- `@Nullable String partitionName`
- `@Nullable List<LakeSplit> lakeSnapshotSplits`
- `long startingOffset`
- `long stoppingOffset`

DV-consistency context:

- `long requestedReadableSnapshotId`
- `Map<String, byte[]> lakeDvSnapshot`
- `Map<Long, byte[]> logDvSnapshot`
- `boolean stale`
- `long currentReadableSnapshotId`

Reader progress state:

- `long recordsToSkip`
- `int currentLakeSplitIndex`
- `boolean lakePhaseFinished`
- `boolean logPhaseFinished`

### Why a new split class instead of mutating `LakeSnapshotAndFlussLogSplit`

A new class keeps concerns clean:

- generic hybrid merge path remains stable
- DV-aware branch can evolve independently
- serialization / state handling are explicit
- rollback is easy if needed

---

## Consistency Source of Truth

### Use `getDvForUnionRead` as the single query-time handshake

Do **not** compose query consistency from multiple APIs like:

- readable lake snapshot API
- `getLakeDvSnapshot`
- `getLogDvSnapshot`

Instead, use `getDvForUnionRead` as the single authoritative union-read DV handshake.

### Why

`getDvForUnionRead` already returns a consistency bundle:

- `lakeDv`
- `logDv`
- `logEndOffset`
- `isStale`
- `currentReadableSnapshot`

That makes it the correct place to bind:

- the requested readable snapshot id
- the lake data files referenced by that snapshot
- the bounded log range for this query

### Required extension in split generation

When building DV-aware batch splits for a bucket:

1. obtain readable lake snapshot metadata
2. plan lake splits for that snapshot
3. collect the referenced lake data files for that bucket
4. call `getDvForUnionRead(requestedSnapshotId, dataFiles)`
5. build a `DvAwareLakeSnapshotAndFlussLogSplit`

If the response is stale:

- either regenerate against `currentReadableSnapshot`
- or fallback to the generic path for the current query attempt

Recommended first implementation:

- regenerate once against `currentReadableSnapshot`
- if still stale, fallback to the generic merge path

---

## Reader-Side Architecture

### 2. New scanner class

Add a dedicated batch scanner.

Suggested name:

- `DvAwareLakeSnapshotAndLogSplitScanner`

Suggested location:

- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/reader/`

### Responsibilities

This scanner should execute in two phases.

#### Phase A: lake phase

- sequentially read planned lake snapshot splits
- apply `LakeDv` filtering while reading
- emit surviving rows directly
- no sort-merge buffer

#### Phase B: log phase

- read bounded Fluss log records from `startingOffset` to `stoppingOffset`
- apply `LogDv` filtering while reading
- for PK changelog semantics, emit only the records that survive DV filtering
- no sort-merge tree map

### Expected output behavior

For batch `select * from table`:

- lake phase emits surviving historical rows as inserts
- log phase emits surviving latest rows as inserts
- no delete/update_before records should be emitted in final batch result

---

## How to Apply `LogDv`

This part is straightforward and can reuse existing concepts.

### Implementation suggestion

Introduce a small utility class:

- `LogDvFilter`

Suggested responsibilities:

- deserialize `Map<Long, byte[]>` into `Map<Long, RoaringBitmap>`
- expose `boolean isDeleted(long logOffset)`

This logic can reuse the same idea already present in:

- `DvTaskWriter.isDeletedInSplitLogDv(...)`

### Log filtering rules

For each scanned log record in the bounded range:

- if `logOffset` is marked deleted by `LogDv`, skip it
- if `changeType` is `DELETE` or `UPDATE_BEFORE`, skip it in batch final result
- if `changeType` is `INSERT` or `UPDATE_AFTER` and not deleted by `LogDv`, emit it

This means the log side becomes a simple bounded “latest surviving rows” stream.

---

## How to Apply `LakeDv`

This is the most important implementation challenge.

### Required information

`LakeDv` is keyed by:

- `file_path`
- `row_position`

So query-time lake filtering needs access to both values while reading lake rows.

### Current limitation

The current Iceberg reader path:

- `IcebergLakeSource#createRecordReader(...)`
- `IcebergRecordReader`
- `IcebergRecordAsFlussRecordIterator`

currently converts Iceberg records into Fluss `LogRecord` but does **not** expose:

- source file path
- row position inside the data file

This means the current reader cannot directly apply `LakeDv` yet.

### Proposed solution

Add a DV-aware Iceberg reader path instead of trying to retrofit filtering externally.

Suggested new classes:

- `IcebergDvAwareRecordReader`
- `IcebergDvAwareRecordIterator`

Suggested location:

- `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/source/`

### Reader behavior

For each `FileScanTask`:

- know the task file path from `fileScanTask.file().location()`
- maintain the current row position while iterating records
- consult `LakeDv` bitmap for the file path
- skip rows whose row position is marked deleted
- emit only surviving rows

### Important technical risk

We need to confirm the underlying Iceberg reader preserves row order exactly matching position indexing for the file scan task.

This is likely true for standard data file scanning, but it should be validated explicitly because correctness depends on row-position alignment with Puffin DV semantics.

### Recommended validation

Add a focused Iceberg source-level unit/integration test:

- build a file with known rows and positions
- inject a synthetic `LakeDv` bitmap
- verify the reader skips exactly those positions

---

## Split Generator Changes

### 3. Add a DV-aware branch in `LakeSplitGenerator`

For PK tables in batch mode, the current generator returns `LakeSnapshotAndFlussLogSplit`.

Change the flow to:

- if PK table and DV disabled: keep existing `LakeSnapshotAndFlussLogSplit`
- if PK table and DV enabled: generate `DvAwareLakeSnapshotAndFlussLogSplit`

### Pseudocode

```java
if (!tableInfo.hasPrimaryKey()) {
    // existing log-table path
} else if (!tableInfo.getTableConfig().isDvEnabled()) {
    // existing PK sort-merge path
    return new LakeSnapshotAndFlussLogSplit(...);
} else {
    // new DV-aware path
    long requestedSnapshotId = readableSnapshot.snapshotId();
    List<LakeSplit> lakeSplits = ...;
    List<String> dataFiles = collectReferencedDataFiles(lakeSplits);
    DvForUnionReadResult dv = fetchDvForUnionRead(requestedSnapshotId, tableBucket, dataFiles);
    if (dv.isStale()) {
        // regenerate or fallback
    }
    return new DvAwareLakeSnapshotAndFlussLogSplit(...);
}
```

### Utility needed

Add a helper to extract file paths from planned lake splits.

For Iceberg this is straightforward because the split contains `FileScanTask`.

---

## Reader Generator Changes

### 4. Extend `LakeSplitReaderGenerator`

Today it supports:

- `LakeSnapshotSplit`
- `LakeSnapshotAndFlussLogSplit`

Add support for:

- `DvAwareLakeSnapshotAndFlussLogSplit`

Behavior:

- create `BoundedSplitReader(new DvAwareLakeSnapshotAndLogSplitScanner(...), recordsToSkip)`
- always treat this split as a bounded split in batch mode

No sort-merge should be used in this branch.

---

## Split Serializer and State

### 5. Add serializer / state support for the new split

Files to update:

- `LakeSplitSerializer`
- `LakeSplitStateInitializer`
- new state class `DvAwareLakeSnapshotAndFlussLogSplitState`

### State requirements

The state needs to preserve:

- current lake split index
- lake records skipped in the current split
- current next log offset
- whether lake phase has finished
- whether log phase has finished
- the DV snapshots carried in the split

This preserves failover behavior without forcing a full restart of the split.

---

## Fallback Strategy

This optimization should be guarded.

### Use DV-aware path only when all conditions hold

- table has primary key
- DV is enabled
- readable lake snapshot exists
- `getDvForUnionRead` succeeds and is not stale after at most one regeneration
- lake source implementation supports query-time `LakeDv` filtering

### Otherwise fallback to existing path

Fallback target:

- existing `LakeSnapshotAndFlussLogSplit`
- existing `LakeSnapshotAndLogSplitScanner`

This keeps the refactor low-risk and incremental.

---

## Suggested Implementation Steps

### Phase 1: split and protocol plumbing

1. Add `DvAwareLakeSnapshotAndFlussLogSplit`
2. Extend `LakeSplitSerializer`
3. Extend split state / restore support
4. Add `fetchDvForUnionRead(...)` helper in the Flink-side generator path
5. Generate DV-aware splits for batch PK+DV tables

### Phase 2: log-side optimization

1. Add `LogDvFilter`
2. Implement `DvAwareLakeSnapshotAndLogSplitScanner` with only the log phase working initially
3. Keep lake phase temporarily delegated to existing reader if needed for scaffolding

### Phase 3: lake-side DV filtering

1. Add `IcebergDvAwareRecordReader`
2. Add file-path + row-position-based filtering
3. Integrate with `DvAwareLakeSnapshotAndLogSplitScanner`

### Phase 4: full switch-over

1. Route DV-enabled batch PK union read to the new scanner
2. Keep old sort-merge path as fallback only
3. Add dedicated tests

---

## Tests to Add

### A. Split generation tests

Verify for PK + DV tables in batch mode:

- generator creates DV-aware splits
- split carries requested snapshot id
- split carries bounded log end offset
- stale DV response triggers regeneration or fallback

### B. LogDv filtering tests

Verify bounded log read:

- `INSERT` / `UPDATE_AFTER` survive when not marked in `LogDv`
- `DELETE` / `UPDATE_BEFORE` are not emitted
- rows marked by `LogDv` are skipped

### C. LakeDv filtering tests

Verify Iceberg lake read:

- rows whose `(file_path, row_position)` are marked in `LakeDv` are skipped
- rows not marked are returned

### D. End-to-end batch union read tests

Create a PK + DV table and verify:

1. first round tiering creates lake snapshot
2. second round creates Puffin DV
3. third round leaves incremental updates/deletes only in Fluss
4. batch `select * from table` returns final expected rows
5. no sort-merge-only behavior is required for correctness

### E. Failover / restore tests

Verify recovery when:

- lake phase is half read
- log phase is half read
- split state restores correctly with DV context intact

---

## Risks and Open Questions

### 1. Row-position access in Iceberg reader

This is the biggest open item.

We must ensure the query-time Iceberg reader can reliably map each emitted row to the exact row position used by Puffin DV semantics.

### 2. Snapshot staleness during planning

`getDvForUnionRead` may report stale if the readable snapshot changed between:

- planning lake splits
- collecting referenced files
- requesting DV snapshots

This is why the new path must include a bounded regeneration or fallback rule.

### 3. Planner/source API generality

The DV-aware query path is currently Iceberg-specific because `LakeDv` is file-path + row-position based.

If other lake backends later support query-time DV, we may want to abstract:

- row-position-aware lake readers
- DV filter application hooks

For now, keep the change Iceberg-focused.

### 4. Memory behavior

The new path should significantly reduce memory pressure because it avoids buffering log rows into a `TreeMap` for merge.

Still, care is needed to ensure the log phase remains streaming and does not accidentally rebuild an in-memory latest-row index.

---

## Recommended Class Additions

### Flink common

- `DvAwareLakeSnapshotAndFlussLogSplit`
- `DvAwareLakeSnapshotAndFlussLogSplitState`
- `DvAwareLakeSnapshotAndLogSplitScanner`
- `LogDvFilter`
- `UnionReadDvContext` (optional helper object to hold DV snapshots and consistency metadata)

### Iceberg module

- `IcebergDvAwareRecordReader`
- `IcebergDvAwareRecordIterator`
- optional `LakeDvFilter`

---

## Recommended Minimal First Cut

If we want the smallest safe first implementation:

1. **Add a new split class** rather than mutating the old one
2. **Use `getDvForUnionRead`** as the only DV query handshake
3. **Implement log-side DV filtering first**
4. **Implement Iceberg row-position-aware lake filtering second**
5. **Keep full fallback to existing sort-merge path** until lake-side DV filtering is validated

This gives us an incremental rollout path while still moving toward the desired architecture.

---

## Bottom Line

Yes, for **DV-enabled PK tables**, batch union read can be redesigned to avoid sort-merge.

But to do that safely, we should not merely tweak `LakeSnapshotAndFlussLogSplitScanner`.
We should introduce a **new DV-aware split and scanner path** built around query-time `getDvForUnionRead`, with explicit support for:

- consistency pinning
- `LakeDv` application in the Iceberg reader
- `LogDv` application in the bounded log reader
- bounded fallback to the existing merge path

That is the cleanest refactor with the best correctness boundary.

---

## Detailed Blueprint

This section turns the refactor direction into a patch-by-patch implementation blueprint.

## Design Principles

1. **Do not break the generic path**
   - Existing `LakeSnapshotAndFlussLogSplit` + `LakeSnapshotAndLogSplitScanner` remains the fallback.
   - The new path is opt-in for PK + DV batch reads only.

2. **Keep consistency explicit in the split**
   - Once a query split is generated, it should already contain the exact DV context it needs.
   - Reader logic should not make best-effort live RPC calls during scan execution.

3. **Keep reader logic sequential and bounded**
   - Lake phase first
   - Log phase second
   - No key-indexed merge structure
   - No full-materialization of either side unless absolutely necessary

4. **Prefer additive refactor over invasive mutation**
   - New split class
   - New scanner class
   - New Iceberg reader class
   - Minimal branching in existing factories/generators

---

## Concrete Problem Statement

### Current batch path

For PK tables in batch mode, the current system effectively does:

```text
readable lake snapshot
+ bounded Fluss log after snapshot
=> sort by PK and merge
```

### Desired DV-aware batch path

For PK + DV tables, we want:

```text
readable lake snapshot - LakeDv
+ bounded Fluss log - LogDv - (DELETE / UPDATE_BEFORE)
=> concatenate
```

### Why concatenation is enough

At a fixed readable snapshot boundary:

- any historical lake row invalidated by later upsert/delete is masked by `LakeDv`
- any stale log record inside the incremental range is masked by `LogDv`
- remaining lake rows and remaining log rows represent disjoint valid states

So the final batch result does not require PK-level conflict resolution.

---

## Proposed Class Graph

### Existing classes kept as-is

- `LakeSnapshotAndFlussLogSplit`
- `LakeSnapshotAndFlussLogSplitState`
- `LakeSnapshotAndLogSplitScanner`
- `LakeSplitGenerator` generic logic for non-DV tables
- `LakeSplitReaderGenerator` generic logic for old split kinds

### New classes to add

#### Flink common

- `DvAwareLakeSnapshotAndFlussLogSplit`
- `DvAwareLakeSnapshotAndFlussLogSplitState`
- `DvAwareLakeSnapshotAndLogSplitScanner`
- `UnionReadDvContext`
- `LogDvFilter`

#### Iceberg module

- `IcebergDvAwareRecordReader`
- `IcebergDvAwareRecordIterator`
- `IcebergLakeDvFilter` or static utility in `IcebergDvAwareRecordReader`

### Existing classes to extend

- `LakeSplitGenerator`
- `LakeSplitSerializer`
- `LakeSplitStateInitializer`
- `LakeSplitReaderGenerator`
- `IcebergLakeSource`
- optionally `LakeSource.ReaderContext` if we need to pass DV filter state through a typed channel

---

## New Data Structures

### `UnionReadDvContext`

Purpose:

- aggregate query-time DV consistency metadata into one object
- avoid long parameter lists across split/scanner/reader construction

Suggested shape:

```java
public class UnionReadDvContext {
    private final long requestedReadableSnapshotId;
    private final long logEndOffset;
    private final Map<String, byte[]> lakeDvSnapshot;
    private final Map<Long, byte[]> logDvSnapshot;
    private final boolean stale;
    private final long currentReadableSnapshotId;
}
```

### Why separate context from split fields

Two viable options:

- inline all fields in the split
- keep a nested context object in the split

Recommendation:

- keep split fields flattened for serialization simplicity
- optionally expose a helper `toUnionReadDvContext()` on the split

---

## New Split Definition

### `DvAwareLakeSnapshotAndFlussLogSplit`

Suggested semantics:

- represents one bucket-scoped, batch-only, DV-aware union-read unit
- contains both the readable lake snapshot slice and the bounded Fluss log range
- carries all DV filters needed to avoid sort-merge

### Suggested fields

```java
@Nullable private final List<LakeSplit> lakeSnapshotSplits;
private long startingOffset;
private final long stoppingOffset;
private final long requestedReadableSnapshotId;
private final long logEndOffset;
private final Map<String, byte[]> lakeDvSnapshot;
private final Map<Long, byte[]> logDvSnapshot;
private long recordsToSkip;
private int currentLakeSplitIndex;
private boolean lakePhaseFinished;
private boolean logPhaseFinished;
```

### Notes on fields

- `startingOffset` is mutable through state restore, same as current hybrid split
- `stoppingOffset` should be pinned to the `logEndOffset` returned by `getDvForUnionRead`
- `logEndOffset` can be redundant with `stoppingOffset`; if so, store only one to reduce duplication
- `lakePhaseFinished` and `logPhaseFinished` let restore resume without ambiguity

### Split id convention

Suggested prefix:

- `lake-dv-aware-snapshot-log-`

This avoids confusion with the old hybrid split.

---

## Serializer Changes

### `LakeSplitSerializer`

Add a new split kind constant, for example:

```java
public static final byte DV_AWARE_LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND = -3;
```

### Serialization payload

Order recommendation:

1. split kind
2. table bucket / partition metadata
3. lake splits list
4. starting offset
5. stopping offset
6. requested readable snapshot id
7. lake DV snapshot map
8. log DV snapshot map
9. recordsToSkip
10. currentLakeSplitIndex
11. lakePhaseFinished
12. logPhaseFinished

### Helper methods to add

- `writeStringByteMap(...)`
- `readStringByteMap(...)`
- `writeLongByteMap(...)`
- `readLongByteMap(...)`

Reuse the same style as the `TieringSplitSerializer` changes you just made.

### Compatibility strategy

Because this is an additive new split kind:

- no compatibility risk for old serialized `LakeSnapshotAndFlussLogSplit`
- new serializer only needs to understand both old and new split kinds

---

## Split State Changes

### `DvAwareLakeSnapshotAndFlussLogSplitState`

Suggested fields:

```java
private long recordsToSkip;
private int currentLakeSplitIndex;
private long nextLogOffset;
private boolean lakePhaseFinished;
private boolean logPhaseFinished;
private final DvAwareLakeSnapshotAndFlussLogSplit split;
```

### State transitions

#### During lake phase

- `recordsToSkip` advances within the current lake split
- `currentLakeSplitIndex` advances when a lake split is exhausted
- once all lake splits finish, set `lakePhaseFinished = true`

#### During log phase

- `nextLogOffset` advances to the next unread offset
- once bounded log finishes, set `logPhaseFinished = true`

### `toSourceSplit()` contract

Must return a split that resumes from the exact phase/offset/skip state.

This should mirror the pattern already used by `LakeSnapshotAndFlussLogSplitState`.

---

## Generator Refactor

### `LakeSplitGenerator`

Add a new branch in `generateSplitForPrimaryKeyTableBucket(...)`.

### Existing logic

Today:

```java
if (snapshotLogOffset == null || snapshotLogOffset < 0) {
    return new LakeSnapshotAndFlussLogSplit(...);
}
return new LakeSnapshotAndFlussLogSplit(...);
```

### New logic

Pseudo-flow:

```java
if (!tableInfo.getTableConfig().isDvEnabled()) {
    return oldHybridSplit(...);
}

long requestedReadableSnapshotId = readableSnapshot.snapshotId();
List<LakeSplit> lakeSplits = plannedLakeSplitsForBucket(...);
List<String> dataFiles = collectDataFiles(lakeSplits);
DvResponse dv = fetchDvForUnionRead(tableBucket, requestedReadableSnapshotId, dataFiles);

if (dv.isStale()) {
    // regenerate against dv.currentReadableSnapshotId() or fallback
}

return new DvAwareLakeSnapshotAndFlussLogSplit(
    tableBucket,
    partitionName,
    lakeSplits,
    snapshotLogOffsetOrEarliest,
    dv.getLogEndOffset(),
    requestedReadableSnapshotId,
    dv.getLakeDvSnapshot(),
    dv.getLogDvSnapshot(),
    ...state defaults...
);
```

### Helper methods to add

#### `collectReferencedDataFiles(...)`

Input:

- `@Nullable List<LakeSplit> lakeSplits`

Output:

- `List<String>` of file paths for this bucket’s planned readable snapshot files

For Iceberg, this likely reads from the underlying `IcebergSplit.fileScanTask().file().location()`.

#### `fetchDvForUnionRead(...)`

Input:

- `TableBucket`
- `requestedSnapshotId`
- `List<String> dataFiles`

Output:

- a small DTO wrapping the RPC response

### Where to locate the helper DTO

Suggested local private static class inside `LakeSplitGenerator`, unless it grows large.

---

## Enumerator Interaction

### No large enumerator redesign needed

`FlinkSourceEnumerator.startInBatchMode()` already uses `generateHybridLakeFlussSplits()`.
That call chain can continue to work.

The main change is:

- `LakeSplitGenerator` may now return either
  - `LakeSnapshotAndFlussLogSplit`
  - `DvAwareLakeSnapshotAndFlussLogSplit`

No behavior change is needed at the enumerator level except ensuring the new split kind can flow through checkpoint/restore.

---

## Reader Generator Refactor

### `LakeSplitReaderGenerator.addSplit(...)`

Add branch:

```java
if (split instanceof DvAwareLakeSnapshotAndFlussLogSplit) {
    boundedSplits.add(split);
}
```

Since this path is batch-only, always treat it as a bounded split.

### `LakeSplitReaderGenerator.getBoundedSplitScanner(...)`

Add branch:

```java
if (split instanceof DvAwareLakeSnapshotAndFlussLogSplit) {
    return new BoundedSplitReader(
        new DvAwareLakeSnapshotAndLogSplitScanner(...),
        split.getRecordsToSkip());
}
```

### Important note

Do not reuse `LakeSnapshotAndLogSplitScanner` and just “toggle off merge”.
The state machine and data model are different enough that a dedicated scanner is cleaner.

---

## Scanner Refactor

### `DvAwareLakeSnapshotAndLogSplitScanner`

### Responsibilities

1. Initialize lake-side DV filter and log-side DV filter
2. Run lake phase
3. Run log phase
4. Emit final batch rows only
5. Preserve progress for state restore

### Internal fields

Suggested:

```java
private final DvAwareLakeSnapshotAndFlussLogSplit split;
private final LakeSource<LakeSplit> lakeSource;
private final Table table;
private final LogScanner logScanner;
private final long stoppingOffset;
private final LogDvFilter logDvFilter;
private final Map<String, RoaringBitmap> lakeDvBitmaps;

private int currentLakeSplitIndex;
private boolean lakePhaseFinished;
private boolean logPhaseFinished;
private @Nullable CloseableIterator<LogRecord> currentLakeIterator;
```

### Phase machine

#### `pollBatch(timeout)`

Recommended behavior:

- if `!lakePhaseFinished`, try to read from current/next lake split
- if a lake iterator yields rows, return them immediately
- if no lake rows remain, transition to log phase
- if `!logPhaseFinished`, poll bounded log and return surviving rows
- if both phases are finished, return `null`

This matches the existing `BoundedSplitReader` protocol.

### Why not combine both sides in one iterator

Keeping explicit phases simplifies:

- restore semantics
- metrics
- debugging
- future parallelization of lake-side split scheduling

---

## Log Scanner Filtering Details

### Recommended helper

```java
final class LogDvFilter {
    private final Map<Long, RoaringBitmap> deletedBitmaps;

    boolean isDeleted(long logOffset) { ... }
}
```

### Filtering algorithm

For each `ScanRecord`:

1. if `scanRecord.logOffset() >= stoppingOffset`, finish the log phase after processing boundary semantics
2. if `logDvFilter.isDeleted(scanRecord.logOffset())`, skip
3. if `changeType` is `DELETE` or `UPDATE_BEFORE`, skip
4. if `changeType` is `INSERT` or `UPDATE_AFTER`, emit as final batch row

### Why `DELETE` / `UPDATE_BEFORE` can be skipped

In the DV-aware batch path they are not final-state rows. Their effect is already reflected in DV.

---

## Iceberg Reader Refactor

### Problem

Current `IcebergRecordReader` emits plain Fluss `LogRecord` and does not expose:

- file path
- row position

That prevents query-time `LakeDv` filtering.

### Recommended approach

Add a reader dedicated to DV-aware lake filtering.

### `IcebergDvAwareRecordReader`

Suggested constructor:

```java
public IcebergDvAwareRecordReader(
        FileScanTask fileScanTask,
        Table table,
        @Nullable int[][] project,
        @Nullable RoaringBitmap deletedPositions)
```

### `IcebergDvAwareRecordIterator`

Suggested behavior:

- wraps the same Iceberg generic reader
- keeps a `long currentRowPosition`
- before emitting each row:
  - check whether `deletedPositions.contains((int) currentRowPosition)`
  - if yes, skip
  - else emit
- increment row position after each physical row consumed

### File-path lookup

The `LakeDv` map is keyed by file path.
For each `FileScanTask`, compute:

```java
String filePath = fileScanTask.file().location();
RoaringBitmap deletedPositions = lakeDvByFilePath.get(filePath);
```

### Integration into `IcebergLakeSource`

Two possible approaches:

#### Option A: extend `ReaderContext`

Add optional union-read DV metadata to `LakeSource.ReaderContext`.

Pros:

- generic and explicit

Cons:

- broader interface churn across lake source implementations

#### Option B: create a dedicated Iceberg-side reader entrypoint for DV-aware scanner

Example:

```java
RecordReader createDvAwareRecordReader(
    IcebergSplit split,
    @Nullable int[][] project,
    @Nullable RoaringBitmap deletedPositions)
```

Recommendation for first cut:

- **Option B**
- keep this optimization Iceberg-specific first
- avoid widening all lake-source interfaces prematurely

---

## Proposed Minimal API Additions

### In `IcebergLakeSource`

Add:

```java
public RecordReader createDvAwareRecordReader(
        IcebergSplit split,
        @Nullable int[][] project,
        @Nullable byte[] serializedLakeDv)
```

or better:

```java
public RecordReader createDvAwareRecordReader(
        IcebergSplit split,
        @Nullable int[][] project,
        @Nullable RoaringBitmap deletedPositions)
```

### In `DvAwareLakeSnapshotAndLogSplitScanner`

If `lakeSource` is not Iceberg, either:

- throw unsupported for this optimization branch
- or fallback to old merge path

Recommendation:

- fallback to old path if the concrete `LakeSource` does not support DV-aware lake reading

---

## Rollout Strategy

### Patch 1: structure only

Files:

- add `DvAwareLakeSnapshotAndFlussLogSplit`
- add serializer support
- add state support
- add split kind handling in reader generator

No behavior switch yet.

### Patch 2: planning only

Files:

- extend `LakeSplitGenerator`
- add `fetchDvForUnionRead(...)`
- add `collectReferencedDataFiles(...)`

Still allowed to fallback immediately to the old path if scanner is not ready.

### Patch 3: log-side DV-aware scanner

Files:

- add `LogDvFilter`
- add `DvAwareLakeSnapshotAndLogSplitScanner`

Initially this scanner can still delegate lake reads to the existing non-DV lake path if necessary, but should already avoid log-side sort-merge.

### Patch 4: Iceberg lake-side DV filtering

Files:

- add `IcebergDvAwareRecordReader`
- wire scanner to it

This is the patch that completes the no-sort-merge design.

### Patch 5: switch primary path

- enable DV-aware batch union read by default for PK + DV + Iceberg
- keep fallback branch guarded

### Patch 6: cleanup

- add metrics
- tighten docs/comments
- consider whether the old hybrid merge path needs a warning for DV-enabled tables

---

## Fallback Matrix

| Condition | Path |
|---|---|
| Non-PK table | Existing log/lake path |
| PK table, DV disabled | Existing `LakeSnapshotAndFlussLogSplit` + sort-merge |
| PK table, DV enabled, Iceberg source supports DV-aware read | New DV-aware path |
| PK table, DV enabled, stale DV response after one regeneration | Fallback to existing path |
| PK table, DV enabled, unsupported lake source | Fallback to existing path |
| PK table, DV enabled, lake-side row-position filtering unavailable | Fallback to existing path |

---

## Metrics to Add

To prove the refactor works and is worthwhile, add metrics at the scanner level.

Suggested metrics:

- `dvAwareLakeRowsRead`
- `dvAwareLakeRowsSkippedByLakeDv`
- `dvAwareLogRowsRead`
- `dvAwareLogRowsSkippedByLogDv`
- `dvAwareLogRowsSkippedByDeleteSemantics`
- `dvAwareUnionReadFallbackCount`
- `dvAwareUnionReadStaleRetryCount`

These can be no-op in the first cut, but the design should leave room for them.

---

## Test Matrix

### Unit tests

#### Split / serializer

- serialize/deserialize `DvAwareLakeSnapshotAndFlussLogSplit`
- state restore round-trip
- empty lake DV map
- empty log DV map
- no lake splits

#### `LogDvFilter`

- exact base-offset bucket hit
- missing base-offset bucket
- empty bitmap
- multiple bitmap buckets

#### Generator helpers

- collect data files from planned Iceberg splits
- stale DV response -> regenerate
- stale after regenerate -> fallback marker

### Integration tests

#### Iceberg reader DV filtering

- one data file, skip one row position
- multiple data files, per-file bitmap correctness
- empty bitmap leaves all rows untouched

#### Batch union read DV path

- readable snapshot only, no log tail
- readable snapshot + insert tail
- readable snapshot + update tail
- readable snapshot + delete tail
- readable snapshot + multiple updates to same key in log tail
- readable snapshot + lake DV + log DV together

#### Fallback tests

- force stale response and verify fallback still returns correct result
- disable DV and verify old path remains unchanged

---

## Exact Files Likely to Change

### Flink common

- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/LakeSplitGenerator.java`
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/LakeSplitReaderGenerator.java`
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/LakeSplitSerializer.java`
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/LakeSplitStateInitializer.java`
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/split/DvAwareLakeSnapshotAndFlussLogSplit.java`
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/state/DvAwareLakeSnapshotAndFlussLogSplitState.java`
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/reader/DvAwareLakeSnapshotAndLogSplitScanner.java`
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/reader/LogDvFilter.java`

### Iceberg module

- `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/source/IcebergLakeSource.java`
- `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/source/IcebergRecordReader.java`
- `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/source/IcebergDvAwareRecordReader.java`

### Optional RPC / DTO helper additions

Only if a Flink-side response wrapper is desired:

- no proto change required if we reuse existing `GetDvForUnionReadResponse`

---

## Recommended First Coding Task

If we start implementing immediately, the best first coding task is:

1. add `DvAwareLakeSnapshotAndFlussLogSplit`
2. add serializer + state support
3. add `LakeSplitGenerator.fetchDvForUnionRead(...)`
4. make generator produce the new split for PK + DV batch mode
5. keep scanner fallback to the old path temporarily

This yields an early, reviewable patch with clear boundaries.

---

## Recommended Second Coding Task

Then implement the log-side filtering branch:

1. add `LogDvFilter`
2. add `DvAwareLakeSnapshotAndLogSplitScanner`
3. make log phase emit only surviving `INSERT` / `UPDATE_AFTER`
4. keep lake phase temporarily delegated if needed

This isolates the easier half before tackling Iceberg row-position filtering.

---

## Recommended Third Coding Task

Finally tackle the hardest piece:

1. add `IcebergDvAwareRecordReader`
2. validate row-position correctness against `LakeDv`
3. wire the scanner to use the new reader
4. enable the full no-sort-merge path by default

---

## Final Recommendation

Do the refactor as an **additive DV-aware fast path**, not as a rewrite of the current hybrid merge path.

That means:

- new split
- new state
- new scanner
- new Iceberg DV-aware reader
- generator-side consistency pinning via `getDvForUnionRead`
- explicit fallback to the existing path

This keeps the change reviewable, testable, and safe while still moving directly toward the architecture you want.

---

## Immediate Actionable Patch Split

### Patch A: Planning and split plumbing

Scope:

- new split kind
- new split class
- serializer / state support
- generator branch for PK + DV tables
- `getDvForUnionRead` RPC consumption

Expected outcome:

- query planning can produce DV-aware batch splits
- runtime can carry them through checkpoint/restore
- execution can still fallback to old path if scanner not finished

### Patch B: DV-aware scanner skeleton

Scope:

- new scanner class
- explicit lake phase / log phase state machine
- log-side `LogDv` filtering
- fallback lake reading behavior if DV-aware Iceberg reader is not ready

Expected outcome:

- log-side sort-merge cost is already reduced
- scanner lifecycle and restore semantics are validated early

### Patch C: Iceberg row-position filtering

Scope:

- `IcebergDvAwareRecordReader`
- per-file `LakeDv` filtering by row position
- full end-to-end batch union read tests

Expected outcome:

- complete no-sort-merge DV-aware batch union read path
- old hybrid merge path remains as guarded fallback
