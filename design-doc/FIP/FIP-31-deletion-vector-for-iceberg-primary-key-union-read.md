# FIP-31: Introduce Deletion Vectors to support Primary Key Table Union Read over Iceberg

> Full design detail: `design-doc/fluss-deletion-vector-design-v3-en.md`. This FIP is the proposal-level summary. The Paimon variant is tracked separately in `FIP-xx-deletion-vector-for-paimon-primary-key-union-read.md`; the two share the same Fluss-side data model and differ only in the lake-side materialization (see §5 / Rejected Alternatives).

## Motivation

Under the lakehouse (Streamhouse) architecture, Fluss serves as the real-time layer while Iceberg serves as the historical layer. Fluss continuously tiers real-time data into Iceberg, and provides **union read** — combining hot-layer incremental data not yet tiered with historical data already in Iceberg, presenting a single, complete table with exactly-once semantics.

Today union read is supported only for **log tables** (append-only, no deduplication needed). It is **not** supported for **primary key tables** backed by Iceberg. This FIP closes that gap, and at the same time removes the read-side cost of the current equality-delete-based tiering.

### Problem 1: Cross-Layer Deduplication for Union Read

For primary key tables, updates and deletes first arrive at Fluss, but older versions of the same row may already have been tiered into Iceberg. During union read, the system must precisely mask rows in Iceberg that have been updated or deleted on the Fluss side; otherwise stale rows resurface from the historical layer, violating exactly-once semantics.

Consider a row `(key1, v1)` already tiered to Iceberg. The user then issues `UPDATE key1 → v2`, which arrives at Fluss as `-U(key1, v1)` + `+U(key1, v2)`. `v2` is still in the Fluss hot layer while `v1` is already in an Iceberg data file. A union read at this moment must return exactly `(key1, v2)` — read `v2` from the changelog **and** mask `v1` in Iceberg. There is currently no mechanism to do that masking in real time.

The two existing fallbacks are both unacceptable: either the client reads stale rows from Iceberg (data duplication / wrong results), or it performs an in-memory full merge of the entire table on every read (cost scales with table size, not the delete delta). This is precisely why Fluss does not yet support union read for primary key tables.

### Problem 2: Equality Delete Degradation

Current tiering writes `DELETE` / `UPDATE_BEFORE` to Iceberg via Iceberg v2 **equality delete**, which suffers from:

- **Small-file accumulation**: each tiering round produces equality delete files that pile up over time.
- **Read amplification**: query engines must apply equality deletes against all historical data files, degrading read performance continuously.
- **Metadata bloat**: manifest entries grow linearly with the number of delete files.

### Goal

Introduce a **three-layer Deletion Vector (DV)** mechanism that solves both problems simultaneously:

1. **Enables primary key union read on Iceberg** by maintaining lightweight logical delete markers (Lake DV + Log DV) on the Fluss TabletServer side, so union read can **instantly** mask rows in Iceberg (and the hot-layer changelog) that have been updated or deleted, without waiting for the next tiering commit — achieving exactly-once union read semantics.
2. **Replaces equality delete** by materializing deletes as Iceberg v3 **position deletes** (RoaringBitmap in Puffin files) at tiering time, eliminating small-file accumulation and read-amplification.

---

## Public Interfaces

### New Table Configuration

| Option                           | Type    | Default | Description |
|----------------------------------|---------|---------|-------------|
| `table.deletion-vectors.enabled` | Boolean | `false` | Master switch for the Fluss three-layer DV architecture. Must be set at table creation and is **immutable** afterwards. |

Enabling DV requires (validated at creation; creation is rejected otherwise):

- a **primary key** table;
- `table.datalake.enabled = true` (the table must be tiered to a lake);
- **FULL** changelog mode (`table.changelog.image = FULL`) — only FULL mode emits `-U` / `-D` carrying the old version, which is required to locate the old row in Iceberg. Under LOOKUP mode, updates emit only `+U` without `-U`, so the old version's RowId is unknown and the old Iceberg row cannot be masked.

`ALTER TABLE` attempts to change `table.deletion-vectors.enabled` are rejected (not in the alterable-properties whitelist): the option changes the persisted KV-state and changelog byte layout and cannot be retrofitted onto existing data.

### Iceberg Format Version

DV-enabled tables default to **Iceberg v3** (Puffin deletion vectors) to replace equality delete. Users may explicitly set `format-version = 2` to fall back to v2 position deletes.

- **Default (v3)**: deletes are materialized as Deletion Vectors (RoaringBitmap in Puffin files) — compact, mergeable, no small-file accumulation.
- **Fallback (v2)**: deletes are materialized as v2 position delete files (Parquet `(file_path, position)` pairs). Still eliminates equality delete but retains per-operation delete-file accumulation.

The fallback only affects the materialization format in the TieringService; the upstream data model (LakeDv / RowPosIndex / PendingDeletes) is format-agnostic.

### New Iceberg System Columns

For DV-enabled tables, two system columns are added to Iceberg data files:

- **`__rowid`** (BIGINT): the changelog log offset of the originating `+I` / `+U` record. After external compaction rewrites rows into new files, Fluss scans this column to rebuild the RowId → file-position mapping.
- **`__bucket`** (INT): the Fluss bucket id. External compaction may merge files **across buckets** into a single file; `__bucket` lets Fluss attribute each rewritten row to its bucket without recomputing the hash from the primary key.

External engines performing compaction / rewrite on Fluss-managed Iceberg tables **must preserve both columns and their values**, and **must not directly INSERT** into these tables (externally inserted rows are invisible to Fluss's changelog / KV state and break upsert and union-read consistency).

### On-Disk / On-Wire Format Changes (Internal)

`@Internal` formats whose byte layout changes for DV-enabled tables:

- **KV state value**: `[RowId(8B)][schemaId(2B)][BinaryRow]` (RowId prepended at the head so the old RowId can be read without parsing the variable-length BinaryRow).
- **Changelog value**: all four record types carry an 8-byte RowId at the value header. `+I` / `+U` carry their own log offset; `-U` / `-D` carry the old version's RowId, copied from the old KV-state value header.

### New / Extended Internal RPC

- **Tiering split request (extended)**: when the TieringService requests a split, the TabletServer additionally returns, under one consistent snapshot, the split's `offset_range`, the `lakeDvSnapshot` (`{file_path → bitmap}`), and the `logDvSnapshot` (deleted RowIds within the range). Used by §4.2.
- **Tiering commit report (extended), TieringService → CoordinatorServer**: after committing S_new to Iceberg, the report carries `indexUuid`, `actualSnapshotId`, `newFileDictEntries`, `materializedLakeDv`, and `currentTieredOffset`. Used by §4.2 step 6.
- **Prepare notification + ready ack (new), CoordinatorServer → TabletServer**: carries `indexUuid`, `materializedLakeDv`, `currentTieredOffset`, `actualSnapshotId`, `newFileDictEntries`; the bucket replies with a ready ack after SST prefetch + FileDict write. Used by §4.3.
- **Readable-switch notification + switched ack (new), CoordinatorServer → TabletServer**: triggers the local Ingest + batch-resolve + cleanup; the bucket replies with a switched ack. Used by §4.4.
- **Union-read response (extended)**: for a request carrying `requestedSnapshotId`, returns `{lakeDv, logDv, logEndOffset}` (or a stale-snapshot error with `currentReadableSnapshot`). Used by §6.

All of the above are `@Internal`.

### User-Visible Behavior Change

Union read on **Iceberg-backed primary key tables** becomes supported (previously unsupported). No new client read API is introduced; the existing union-read path is extended to apply the three-layer DV.

---

## Proposed Changes

### 1. Architecture: Three-Layer Deletion Vector

```
Fluss (hot layer)                            Iceberg (cold layer)
┌───────────────────────────────┐            ┌──────────────────────────┐
│ Changelog                     │            │ Data files (Parquet)     │
│ Log DV   (hot→hot tracking)   │            │ Iceberg DV (Puffin)      │
│ Lake DV  (hot→cold tracking)  │ ─────────▶ │   materialized by Fluss  │
└───────────────────────────────┘            └──────────────────────────┘
                  (Lake DV markers take effect immediately for union read;
                   physically materialized as Puffin DV at the next tiering commit)
```

During union read, the query engine applies **all three layers** so that UPDATE produces the latest value and DELETE fully removes the row, regardless of which layer the original data resides in.

- **Iceberg Deletion Vector** (cold, physical): standard Iceberg v3 DV. When the Fluss TieringService writes to Iceberg, it materializes delete operations as **Puffin files** (RoaringBitmaps pointing at deleted row positions), completely replacing equality delete. **Fluss is the writer of these Puffin DV files** — this is the key difference from the Paimon variant, where Paimon's own compaction maintains the DV.
- **Log Deletion Vector** (hot→hot, logical): tracks deletes / updates within the Fluss changelog not yet tiered, so union read does not surface stale versions still in the hot layer.
- **Lake Deletion Vector** (hot→cold, logical): the bridge between layers. When Fluss receives a delete / update for a row already tiered to Iceberg, the TabletServer records a logical delete marker (`file_id → deleted row positions bitmap`). It takes effect **immediately** for union read and is materialized into Puffin DV at the next tiering commit, then cleaned by **bitmap diff** (see §5).

### 2. Data Model & Storage

#### 2.1 RowId

A RowId uniquely identifies a **specific version** of a KV record (not the primary key). Different versions of the same key have different RowIds. Its value is the **log offset** of the corresponding `+I` / `+U` changelog record.

| KV operation     | Changelog record(s)                            | RowId                                  |
|------------------|------------------------------------------------|----------------------------------------|
| `PUT (key1, v1)` | `+I (offset=0, key1, v1)`                      | RowId = 0 (first version)              |
| `PUT (key1, v2)` | `-U (offset=1, key1, v1)` + `+U (offset=2, v2)`| `-U` references RowId 0; new RowId = 2 |
| `DELETE (key1)`  | `-D (offset=3, key1, v2)`                      | references RowId 2 (version to delete) |

- `+I` / `+U`: RowId = the record's own log offset, set at write time.
- `-U` / `-D`: RowId = the deleted version's log offset, extracted from the old KV-state value header (first 8 bytes).
- KV state (RocksDB): RowId = the current version's log offset, in the value header.

RowId is 8 bytes and ties directly to the Iceberg `__rowid` column.

#### 2.2 FilePos

Locates a row's physical position in Iceberg:

- **file_id** (int): dictionary-encoded id of the Iceberg data file (not the raw path). 4 bytes ≈ 4 billion files.
- **row_position** (long): 0-based row number within the file (Iceberg defines position as a 64-bit integer).

Both fields use unsigned varint (LEB128) encoding; in the common case (file_id < thousands, row_position < millions) a FilePos occupies 3–5 bytes. RocksDB tracks per-entry value length, so variable-width values work natively.

#### 2.3 DvRocksDB

A dedicated RocksDB instance per bucket, **independent** from the KvTablet RocksDB (independent checkpoint / recovery, lifecycle bound to Iceberg snapshots, independently tunable). Five column families:

| Column Family     | Key                 | Value                              | Description |
|-------------------|---------------------|------------------------------------|-------------|
| **RowPosIndex**   | RowId (8B)          | FilePos (varint)                   | Position in the current readable snapshot. Updated only at readable switch (SST Ingest). |
| **LogDv**         | offset_range        | del_bitmap                         | Deleted offsets within each changelog range. |
| **LakeDv**        | file_id (4B)        | del_bitmap (RoaringPositionBitmap) | Unmaterialized logical deletes for Iceberg files. Same 64-bit-position layout as the Iceberg DV. |
| **FileDict**      | file_path ↔ file_id | (bidirectional)                    | Dictionary encoding for Iceberg file paths (forward + reverse). |
| **PendingDeletes**| RowId (8B)          | FilePos (varint) or `pending`      | Unmaterialized dead-row log; resolves timing gaps and compaction-rewrite position changes at readable switch. |

> **Why RowPosIndex is a single CF**: between prepare and readable switch, union read still serves the old snapshot and reads the old RowPosIndex; the new positions are needed only at the actual switch, where RowPosIndex is atomically updated via SST Ingest under the write lock. There is no window requiring both old and new positions simultaneously.

#### 2.4 Concurrency: DvRWLock

A reader-writer lock. All DV write paths take the write lock and are serialized; union read takes the read lock (concurrent among readers, mutually exclusive with writers).

| Lock holder              | Lock type   | Operations |
|--------------------------|-------------|------------|
| `-U`/`-D` processing     | write lock  | RowPosIndex point-get, PendingDeletes write, LakeDv update, LogDv update |
| Prepare (Phase B)        | write lock  | FileDict write, store SST path (no Ingest), resolve `materializedLakeDv` to file_id |
| Readable switch (Phase C)| write lock  | Ingest SST → RowPosIndex, batch resolve, bitmap-diff LakeDv cleanup, LogDv/PendingDeletes cleanup |
| Union read               | read lock   | read `readableSnapshotId`, clone LakeDv bitmap subset, range-read LogDv |

The write critical sections are minimal (point-gets, bitmap ops); the read critical section only clones bitmap subsets and range-reads — serialization and network I/O happen after lock release.

> **Consistency invariant**: `-U`/`-D` processing acquires the DvRWLock write lock **inside** the KvTablet write lock, completes the DV updates, releases the DvRWLock write lock, and only then advances `log_hw`. Union read (under KvTablet read lock) therefore never sees a state where `log_hw` has advanced but the DV has not.

### 3. Write Path

#### 3.1 Real-Time Write (`+I` / `+U`)

Identical to today's path, with one addition: the RowId (= the about-to-be-assigned log offset) is prepended to both the KV-state value and the changelog value. No DV-specific work.

- **New key**: emit `+I(value, rowId)`, write to PrewriteBuffer + changelog, write `[RowId][schemaId][BinaryRow]` to KV state.
- **Existing key (PUT)**: extract `oldRowId` from the old value header; emit `-U(oldValue, oldRowId)` + `+U(newValue, newRowId)`; update KV state to the new version.
- **Existing key (DELETE)**: emit `-D(oldValue, oldRowId)`; delete the key from KV state.

#### 3.2 Deletion Processing (`-U` / `-D`)

After the changelog is synced to all replicas, under the KvTablet write lock:

1. Flush PrewriteBuffer to RocksDB.
2. Acquire the **DvRWLock write lock**.
3. For each `-U` / `-D` entry, point-get `RowPosIndex[oldRowId]`:
   - **Hit `(file_id, row_position)`** — the row's Iceberg position is known: mark `row_position` in `LakeDv[file_id]`, delete the RowPosIndex entry, and write `PendingDeletes[oldRowId] = (file_id, row_position)`.
   - **Miss** — the row is currently in the tiering pipeline (prepare not yet received): write `PendingDeletes[oldRowId] = pending`. The next readable switch's batch resolve fills in the LakeDv marker after SST Ingest.
   - Update `LogDv`: mark `offset = oldRowId` as deleted in the corresponding changelog range.
4. Release the DvRWLock write lock.
5. Advance `log_hw`; release the KvTablet write lock.

> **Ordering constraint**: DV must be updated and the DvRWLock released **before** `log_hw` advances. Otherwise union read could observe a larger `logEndOffset` while LakeDv is not yet updated, surfacing deleted data.

> **Why PendingDeletes also records the hit position**: it serves a dual role — a fallback for misses, and a **reverse index** for readable switch. When external compaction rewrites a row to a new file, batch resolve detects the new position via a RowPosIndex point-get and patches LakeDv for the new file, without scanning every SST entry.

### 4. Tiering Pipeline

Because Fluss writes data files **directly** to Iceberg, it knows each row's `(file, row_position)` at write time — no compaction-output scan is needed for the normal path (only external compaction requires scanning, see §7). **CoordinatorServer is the single orchestrator**; the TieringService (Flink job) writes / commits data and reports results.

#### 4.1 End-to-End Timeline

```
TieringService (Flink job)        CoordinatorServer            TabletServer (per bucket)
        │                                 │                            │
  A: write data files + Puffin DV         │                            │
     generate SST + upload (pre-commit)   │                            │
     commit S_new to Iceberg              │                            │
     report ───────────────────────────▶ │                            │
        │                          B: prepare ──────────────────────▶ download SST (no lock)
        │                                 │                            write FileDict, store path,
        │                                 │                            resolve materializedLakeDv (write lock)
        │                                 │◀───── ready ack ───────────┤
        │                          [barrier: all ready acks]           │
        │                          C: mark S_new DV-readable (ZK)       │
        │                             readable switch ───────────────▶ Ingest SST → RowPosIndex
        │                                 │                            batch resolve PendingDeletes
        │                                 │                            bitmap-diff LakeDv cleanup
        │                                 │                            advance readableSnapshotId
        │                                 │◀───── switched ack ────────┤
        │                          [barrier: all switched acks → round complete]
```

Two-phase ack: the **ready ack** gates publishing (pre-publish liveness check + SST prefetch, so the post-publish stale window is local-only); the **switched ack** is observability only (ordering is guaranteed by CoordinatorServer).

#### 4.2 Phase A: Split Generation, Write & Commit

**Split generation (TabletServer)** — under the KvTablet read lock (so LakeDv, LogDv, and `log_hw` are consistent):

1. Read `log_hw` as `latest_offset`.
2. Snapshot the entire LakeDv, resolving `file_id → file_path` via FileDict → `lakeDvSnapshot: {file_path → bitmap}`.
3. Snapshot LogDv for the split range `(last_tiered_offset, latest_offset]` → `logDvSnapshot` (deleted RowIds within the round).
4. Generate split `{offset_range, lakeDvSnapshot, logDvSnapshot}`.

**TieringService processing**:

1. Read changelog `(last_tiered_offset, latest_offset]`.
2. For each `+I` / `+U`: if its RowId is in `logDvSnapshot`, the row was written-then-deleted within this round → **skip**; otherwise write to an Iceberg data file (with `__rowid` / `__bucket`), recording `(RowId, file, row_position)`. `-U` / `-D` do not directly generate DV (cross-split deletes are captured by `lakeDvSnapshot`; intra-split ones by `logDvSnapshot`).
3. **Materialize `lakeDvSnapshot` into Puffin DV**: read the current Iceberg file set; retain only `lakeDvSnapshot` entries whose files still exist (**stale-file protection**) → this filtered result is **`materializedLakeDv`**; for each file, generate (or merge with the existing Puffin DV via RoaringBitmap OR) a Puffin DV file.
4. **Pre-commit: generate RowPosIndex SST and upload**: allocate `file_id`s via `FileDictAllocator`; write a sorted SST (`key=RowId`, `value=fileId+row_position`); upload per bucket to `{remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`; write a per-bucket manifest and a cross-bucket index at `{indexUuid}`.
5. **Commit** via Iceberg `RowDelta`: `validateFromSnapshot(baseSnapshotId)` + `validateDataFilesExist(lakeDvReferencedFiles)` + `addRows(dataFiles)` + `addDeletes(dvFiles)`. On `ValidationException` (external compaction replaced a referenced file): **abort**; the next round retries. The Iceberg snapshot property records `indexUuid` and `fluss.nextFileId`.
6. **Report** to CoordinatorServer: `indexUuid`, `actualSnapshotId`, `newFileDictEntries`, `materializedLakeDv` (pre-filtered in step 3), `currentTieredOffset`.

> **FileDictAllocator** is stateless: `nextFileId` is recovered from the Iceberg snapshot property `fluss.nextFileId`; the in-memory `pathToFileId` map is for batch dedup only. After restart, the same path may get a different `file_id`, which remains correct since each bucket's RowPosIndex and FileDict are self-consistent.

#### 4.3 Phase B: Prepare

CoordinatorServer sends a prepare notification carrying `indexUuid`, `materializedLakeDv`, `currentTieredOffset`, `actualSnapshotId`, `newFileDictEntries`.

- **Phase 1 (no lock — pure remote I/O)**: locate the SST via `indexUuid → cross-bucket index → sstDir`; download the manifest and SST files locally.
- **Phase 2 (DvRWLock write lock — lightweight)**: write `newFileDictEntries` to FileDict (idempotent; a `file_id` mapping to a different path is a bug → fail-fast); store the local SST path as `pendingSstPath` (**no Ingest**); resolve `materializedLakeDv` keys from file_path to file_id.
- Send the bucket's **ready ack** (only after `materializedLakeDv` is resolved).

Prepare modifies no DV state beyond FileDict and stored paths, so rollback on failure is trivial. Processing is idempotent.

#### 4.4 Phase C: Publish + Readable Switch

After all ready acks, CoordinatorServer marks S_new DV-readable (updates LakeTableZNode), then notifies TabletServers to switch. Under the DvRWLock write lock, each bucket:

1. **Ingest SST → RowPosIndex** (`IngestExternalFile`). RowPosIndex now reflects S_new. Rewritten rows' new positions overwrite old ones (higher sequence number); rows tombstoned by §3.2 are "resurrected" with the new position and handled by step 2.
2. **Batch resolve PendingDeletes** — for each `(R, v)`, `hit = RowPosIndex.get(R)`:
   - **Hit** (timing gap / external compaction rewrite / zombie): `LakeDv[hit.fileId] |= {hit.pos}`; delete `RowPosIndex[R]`; update `PendingDeletes[R] = hit`.
   - **Miss** and `R < currentTieredOffset`: orphan (row covered by tiering but never written to a data file — filtered by logDvSnapshot) → delete `PendingDeletes[R]`.
   - **Miss** and `R ≥ currentTieredOffset`: row still being processed → keep for next round.
3. **Cleanup oldFiles** (from external compaction, §7): drop their LakeDv entries; cleanup PendingDeletes entries pointing at old files. No-op if no external compaction.
4. **Bitmap-diff LakeDv cleanup**: for each file_id in `materializedLakeDv`, `LakeDv[file_id] = LakeDv[file_id] AND NOT materializedLakeDv[file_id]`; remove the entry if empty. Clear `materializedLakeDv`.
5. **Cleanup PendingDeletes** whose position was materialized this round; **cleanup expired LogDv** (range end < new `snapshotStartLogOffset`).
6. **Advance** `readableSnapshotId` and `snapshotStartLogOffset`.
7. Clear `pendingSstPath`; release the lock; send the **switched ack**.

> **Why bitmap diff and not direct clear**: between split generation and readable switch, new `-U/-D` may append bits to the same file's bitmap. `AND NOT materializedLakeDv` removes only the bits that were actually materialized this round, preserving post-snapshot unmaterialized bits (otherwise stale rows resurface). See the design doc's Appendix B for the proof.

#### 4.5 First-Time Bootstrap

Write data files → generate SST + commit S1 → report → prepare (download SST; `materializedLakeDv` empty) → publish → readable switch (Ingest SST; PendingDeletes empty so batch resolve is a no-op). RowPosIndex now reflects S1; LakeDv empty.

### 5. LakeDv Materialization & Cleanup: Bitmap Diff

This is the central difference from the Paimon variant.

| Dimension          | Iceberg variant (this FIP)                        | Paimon variant                            |
|--------------------|---------------------------------------------------|-------------------------------------------|
| Who writes the DV  | **Fluss** materializes Puffin DV at tiering commit| **Paimon compaction** maintains the DV    |
| Cleanup trigger    | after each tiering commit                         | after each compaction that replaces files |
| Cleanup method     | bitmap diff (`LakeDv AND NOT materializedLakeDv`) | drop the old file's LakeDv entry          |
| Required info       | `materializedLakeDv` (reported)                   | `oldFiles` (reported)                     |

Materialization reads any existing Puffin DV for an affected file, merges (RoaringBitmap OR) with the LakeDv snapshot, and writes a new Puffin DV, committed alongside data files via `RowDelta`. Cleanup happens **after** the new snapshot becomes DV-readable (not at commit), so union read on the old readable snapshot is never left without masking. `materializedLakeDv` is pre-filtered by stale-file protection (§4.2 step 3) so the diff never removes entries for files that were not actually materialized.

### 6. Union Read

1. The client obtains the latest DV-readable `snapshotId` (`requestedSnapshotId`) and sends a union read request carrying it.
2. Fluss lists the data files under that snapshot.
3. Under KvTablet read lock + DvRWLock read lock, perform the **snapshot consistency check**: `readableSnapshotId == requestedSnapshotId`?
   - `requested < current`: this TabletServer already switched to a newer snapshot → client refreshes to the newer id and retries.
   - `requested > current`: a newer target was published but this bucket has not switched yet → client **keeps the same id** and retries with backoff (must not fall back to an older snapshot).
4. Read `logEndOffset`; clone the **LakeDv bitmap subset** for the requested files (only query-relevant files); range-read **LogDv** from the snapshot's start offset to `logEndOffset`.
5. Release locks; serialize and return `{lakeDv, logDv, logEndOffset}` (outside locks).

**Client-side processing**:

1. Apply the **Iceberg DV** (physical Puffin DV) on the Iceberg snapshot.
2. Apply the returned **LakeDv** (logical) to mask not-yet-materialized deletes.
3. Read surviving Iceberg rows.
4. Fetch `[snapshotStartLogOffset, logEndOffset]` changelog, apply **LogDv**, skip deleted records.
5. Merge for the complete, exactly-once result.

The single-snapshot consistency rule (all buckets serve the same snapshot per request) trades a brief stale window — between publish and per-bucket switch, handled transparently by client retry — for a single-snapshot / single-LakeDv / single-RowPosIndex architecture.

### 7. External Compaction

External engines (Spark, Trino, …) may compact Fluss-managed Iceberg tables, merging old files into new ones. Fluss does not control the timing but must handle the resulting file changes.

- **Detection**: at the next tiering commit, TieringService traverses Iceberg snapshot history since the last known snapshot and identifies snapshots **without** the `fluss.tiering` property — these are external compaction snapshots. From them it derives `externalNewFiles` / `externalOldFiles` (cheaper than diffing full file sets).
- **Re-establish positions**: scan `externalNewFiles`' `__rowid` and `__bucket` columns, group by bucket, and merge `(RowId, file, row_position)` into the §4.2 SST pipeline. `externalOldFiles` is reported as `oldFiles` for cleanup at readable switch (§4.4 step 3).
- **Physically deleted rows** (excluded from new files by existing Iceberg DV) leave no residue: alive rows are re-Ingested to RowPosIndex; rows previously deleted by §3.2 are patched onto the new file via batch resolve.
- **Operational constraint**: Iceberg snapshot expiration must preserve the current Fluss readable snapshot and all files it references (`history.expire.min-snapshots-to-keep`, or mark the readable snapshot in table properties).

### 8. Failure Handling & Recovery

#### 8.1 TieringService (stateless)

Recovery keys off the pre-commit / post-commit boundary (SST upload precedes Iceberg commit):

| Failure point                          | Iceberg state | SST state          | Recovery |
|----------------------------------------|---------------|--------------------|----------|
| before SST upload                      | not committed | missing / partial  | full retry (new UUID; `nextFileId` from last snapshot property) |
| SST uploaded, before commit            | not committed | complete           | full retry (old UUID paths become orphans, cleaned periodically) |
| commit succeeded, before / failed report| committed     | complete           | **metadata-only reconcile**: re-report from Iceberg snapshot + manifests; **must not re-commit** (would duplicate alive rows) |

#### 8.2 TabletServer

DvRocksDB periodically checkpoints (independent from KvTablet snapshots), recording `restoreSnapshot`, `snapshotStartLogOffset`, and `checkpointLogHw`. Recovery:

1. Load the checkpoint; RowPosIndex reflects `restoreSnapshot`.
2. Replay changelog from `checkpointLogHw + 1` — **deletes only** (replay never adds RowPosIndex entries; it only deletes entries and updates LakeDv / LogDv / PendingDeletes).
3. For post-checkpoint readable-switched snapshots: query CoordinatorServer for the current DV-readable snapshot; download and **Ingest the intermediate snapshots' remote SSTs in commit order** (in-order Ingest ensures later snapshots win for the same RowId), then replay deletes and batch-resolve PendingDeletes.
4. **Skip bitmap-diff cleanup** during recovery (no `materializedLakeDv`). Redundant already-materialized LakeDv entries may remain — harmless, since union read double-marking is idempotent — and are precisely eliminated in the next normal tiering round (design doc Appendix C).

> RowPosIndex recovery depends entirely on the checkpoint plus re-Ingesting remote SSTs; it cannot be rebuilt from changelog replay.

#### 8.3 CoordinatorServer

Orchestration is reconstructed entirely from LakeTableZNode, with two ZK checkpoints: (1) round metadata persisted (committed, not yet DV-readable); (2) S_new published as DV-readable.

- **Metadata present, not DV-readable**: re-send prepare (idempotent) → collect ready acks → publish → readable switch.
- **DV-readable**: re-send readable switch (buckets already switched skip; others have `pendingSstPath` ready).
- **No pending round**: no action; TieringService re-reports or retries.

#### 8.4 Ordering & Idempotency

CoordinatorServer is the single orchestrator and does not start round N+1's prepare before round N's readable switch completes; TabletServer needs no local ordering check. Single-flight per split; retry only after CoordinatorServer explicitly declares the attempt failed; a cancelled attempt sends no further report / ack. Prepare and readable switch are idempotent.

### 9. Data Format & Protocol Changes (summary)

- **KV state value**: `[RowId(8B)][schemaId(2B)][BinaryRow]`.
- **Changelog value**: all four record types carry an 8-byte RowId header; `-U` / `-D` carry the old version's RowId.
- **Iceberg data columns**: add `__rowid` (BIGINT) and `__bucket` (INT) for DV tables; external compaction must preserve them.
- **Iceberg format version**: default v3 (Puffin DV); fallback v2 position deletes.
- **Commit**: `IcebergLakeCommitter` upgraded from no validation to `validateFromSnapshot` + `validateDataFilesExist`.
- **RPC**: tiering split request returns `lakeDvSnapshot` / `logDvSnapshot`; tiering commit report carries `indexUuid` / `materializedLakeDv` / `currentTieredOffset` / `newFileDictEntries`; prepare / readable-switch notifications + acks; union read returns `{lakeDv, logDv, logEndOffset}`.

---

## Compatibility, Deprecation, and Migration Plan

- **Opt-in, off by default**: gated by `table.deletion-vectors.enabled = false`. Existing tables and behavior are unaffected unless a table opts in at creation.
- **No migration of existing tables**: the switch is immutable post-creation; existing primary key tables keep their current (equality-delete) tiering and remain without primary key union read. Dynamic enabling is impossible — pre-existing KV state has no embedded RowId and pre-existing Iceberg files have no `__rowid` / `__bucket` columns, so historical positions cannot be rebuilt.
- **Prerequisite enforcement**: creating a DV table without a primary key, without datalake enabled, or without FULL changelog mode is rejected at creation.
- **Iceberg version**: DV tables default to v3; users may pin `format-version = 2` to keep v2 position deletes.
- **Storage / write overhead** (the reason DV is opt-in): an 8-byte RowId per KV-state value and per changelog record (typically <10% of payload); a dedicated DvRocksDB instance per bucket; Puffin DV materialization + SST upload/download + multi-phase coordination per round; extra LakeDv / LogDv filtering at read time.
- **MergeEngine matrix** — all compatible under FULL changelog mode:

  | MergeEngine    | Notes                                                  |
  |----------------|--------------------------------------------------------|
  | DEDUPLICATE    | standard upsert; `-U`/`-D` carry oldRowId              |
  | FIRST_ROW      | duplicate keys ignored (no `-U`); DELETE still emits `-D` |
  | PARTIAL_UPDATE | requires FULL changelog mode                           |
  | AGGREGATE      | requires FULL changelog mode                           |

- **External writer constraint**: external engines must not INSERT into Fluss-managed Iceberg tables and must preserve `__rowid` / `__bucket` during compaction (Fluss-sole-writer is an existing constraint; DV adds the column-preservation requirement).

---

## Test Plan

- **Union read correctness** (system / integration): end-to-end primary key union read across the hot/cold boundary; exactly-once results for INSERT / UPDATE / DELETE spanning tiering rounds; the design doc's Appendix A walkthrough end-to-end; three-layer cooperation (Iceberg DV + LakeDv + LogDv).
- **Tiering write path**: `+I`/`+U` written with `__rowid`/`__bucket` and recorded positions; intra-split write-then-delete filtered by `logDvSnapshot`; LakeDv materialized to Puffin DV (and merged with existing DV); stale-file protection filters `materializedLakeDv`; SST generated/uploaded pre-commit; `RowDelta` validation (`validateFromSnapshot` / `validateDataFilesExist`) and abort-on-conflict. Schema conversion adds `__rowid`/`__bucket` only when DV is enabled and rejects user columns of those names.
- **Tiering pipeline**: two-phase ack barriers; deferred Ingest at readable switch; bitmap-diff LakeDv cleanup and PendingDeletes cleanup ordering (after batch resolve); timing-gap and zombie PendingDeletes resolution; orphan cleanup using `currentTieredOffset`.
- **External compaction**: snapshot-history detection via the `fluss.tiering` property; `__rowid`/`__bucket` scan + group-by-bucket merge into the SST pipeline; oldFiles cleanup at readable switch; physically-deleted-row handling; snapshot-expiration protection.
- **Iceberg format**: v3 Puffin DV default; v2 position-delete fallback.
- **Failure & recovery**: TieringService failover at each pre-/post-commit boundary (idempotent re-write, no duplicate alive rows; `nextFileId` from snapshot property; metadata-only reconcile); TabletServer recovery from checkpoint + changelog replay + in-order SST re-Ingest, including post-recovery redundant-LakeDv elimination; CoordinatorServer failover at each ZK checkpoint (idempotent prepare / switch).
- **Concurrency**: DvRWLock serialization across `-U/-D`, prepare, readable switch, and union read; union read never observes an advanced `log_hw` with stale DV.
- **Config & validation**: prerequisite rejection at creation; immutability under `ALTER TABLE`.

---

## Rejected Alternatives

- **In-memory full merge at read time (no DV)**: read all hot changelog plus all Iceberg data and deduplicate by primary key on the client. Correct but prohibitively expensive — cost scales with full table size, not the delete delta, defeating the purpose of tiering. Rejected.

- **Keep equality-delete tiering**: neither solves real-time cross-layer masking (deletes only visible after the next commit) nor the small-file / read-amplification / metadata-bloat problems. Rejected in favor of Iceberg v3 position deletes materialized from LakeDv.

- **Serve two snapshots simultaneously to avoid the stale window**: requires maintaining two LakeDv and two RowPosIndex states, doubling write-path point-get cost and complexity (the old snapshot's file positions may already be gone from RowPosIndex after external compaction). We instead accept a brief (tens-of-ms) stale window handled by client retry, keeping a single-snapshot architecture. Rejected.

- **Single-phase publish-then-switch (skip prepare)**: would include remote SST download in the post-publish stale window and lose the pre-publish liveness check. The two-phase prepare front-loads remote I/O and gates publish on ready acks. Rejected.

- **Eager SST Ingest during prepare**: would require dual position column families (one per snapshot) to keep union read on the old snapshot correct between prepare and switch. Deferring Ingest to readable switch keeps a single RowPosIndex CF. Rejected.

- **Direct-clear LakeDv cleanup (instead of bitmap diff)**: between snapshotting LakeDv and cleanup, new `-U/-D` append bits to the same file's bitmap; a direct clear would drop these unmaterialized bits and resurrect stale rows. Bitmap diff (`AND NOT materializedLakeDv`) removes only materialized bits. Rejected.

- **Reverse-engineer cleanup from the committed Puffin DV**: theoretically feasible but requires TabletServer remote file I/O, violating the "lightweight local operations only" principle for readable switch. Carrying `materializedLakeDv` in the prepare notification is cheaper. Rejected.

- **Omit the `__bucket` column (as in the Paimon variant)**: in Iceberg, external compaction may merge files **across buckets**, so the bucket cannot be recovered from file metadata alone. `__bucket` avoids recomputing the hash from primary key columns during external-compaction scans. Required here (unlike Paimon, where compaction is intra-bucket). Rejected.

- **Build positions purely from writer reports, no external-compaction scan**: external engines may rewrite files outside Fluss's control; without scanning `externalNewFiles` the RowId → position mapping would go stale after compaction. The scan path is the fallback that keeps RowPosIndex correct. Rejected.
