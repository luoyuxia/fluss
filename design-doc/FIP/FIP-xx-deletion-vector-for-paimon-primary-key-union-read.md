# FIP-xx: Deletion Vector for Paimon Primary Key Union Read

## Motivation

Under the lakehouse (Streamhouse) architecture, Fluss serves as the real-time layer while Paimon serves as the historical layer. Fluss continuously tiers real-time data into Paimon, and provides **union read** — combining hot-layer incremental data not yet tiered with historical data already in Paimon, presenting a single, complete table with exactly-once semantics.

Today union read is supported only for **log tables** (append-only, no deduplication needed). It is **not** supported for **primary key tables** backed by Paimon. This FIP closes that gap.

### Problem 1: Cross-Layer Deduplication for Union Read

For primary key tables, updates and deletes first arrive at Fluss, but older versions of the same row may already have been tiered into Paimon. During union read, the system must precisely mask rows in Paimon that have been updated or deleted on the Fluss side; otherwise stale rows resurface from the historical layer, violating exactly-once semantics.

Consider a row `(key1, v1)` that has been tiered to Paimon. The user then issues `UPDATE key1 → v2`. This update arrives at Fluss as `-U(key1, v1)` + `+U(key1, v2)`; `v2` is still in the Fluss hot layer, while `v1` is already in a Paimon data file. A union read at this moment must return exactly `(key1, v2)` — it must read `v2` from the changelog **and** mask `v1` in Paimon. There is currently no mechanism to do the masking in real time.

The two existing fallbacks are both unacceptable: either the client reads stale deleted rows from Paimon (data duplication / wrong results), or it performs an in-memory full merge of the entire table on every read (cost scales with table size, not the delete delta). This is precisely why Fluss does not yet support union read for primary key tables.

### Problem 2: Equality Delete Degradation

Current tiering handles `DELETE` / `UPDATE_BEFORE` in a way that, for query engines, behaves like equality delete:

- **Small file accumulation**: each tiering round produces delete entries that pile up over time.
- **Read amplification**: query engines must apply deletes against all historical data files, degrading read performance continuously.
- **Metadata bloat**: manifest entries grow linearly with the number of delete files.

Paimon already has a native, more efficient mechanism — Deletion Vectors produced by compaction — that this design leverages instead of carrying equality deletes.

### Goal

Introduce a **three-layer Deletion Vector (DV)** mechanism, integrated with Paimon's native LSM / compaction / DV machinery, that:

1. **Enables primary key union read on Paimon** by maintaining lightweight logical delete markers on the Fluss TabletServer side, so union read can **instantly** mask rows in Paimon (and the hot-layer changelog) that have been updated or deleted, without waiting for the next compaction — achieving exactly-once union read semantics.
2. **Leverages Paimon's native deletion vectors** instead of equality delete: deletes are written as Paimon `DELETE` records and resolved by Paimon compaction, eliminating Fluss-generated delete files entirely.

---

## Public Interfaces

### New Table Configuration

| Option                           | Type    | Default | Description |
|----------------------------------|---------|---------|-------------|
| `table.deletion-vectors.enabled` | Boolean | `false` | Master switch for the Fluss three-layer DV architecture. Must be set at table creation time and is **immutable** afterwards. |

Enabling DV requires (validated at table creation; creation is rejected otherwise):

- a **primary key** table;
- `table.datalake.enabled = true` (the table must be tiered to a lake);
- **FULL** changelog mode (`table.changelog.image = FULL`) — only FULL mode emits `-U` / `-D` carrying the old version, which is required to locate the old row in Paimon. Under LOOKUP mode, updates emit only `+U` without `-U`, so the old version's RowId is unknown and the old Paimon row cannot be masked.

`ALTER TABLE` attempts to change `table.deletion-vectors.enabled` are rejected (it is not in the alterable-properties whitelist), because the option changes the persisted KV-state and changelog byte layout and cannot be retrofitted onto existing data.

The Paimon-side switch `paimon.deletion-vectors.enabled` is kept consistent automatically:

- Fluss DV on + Paimon switch unset → Paimon switch auto-enabled.
- Fluss DV on + Paimon switch already on → no conflict.
- Fluss DV off + Paimon switch on → **rejected** (Paimon DV files depend on the Fluss three-layer architecture).

Users cannot toggle the Paimon-side switch independently.

### Behavior Differences (DV off vs on)

| Component         | `table.deletion-vectors.enabled = false` | `table.deletion-vectors.enabled = true`                    |
|-------------------|------------------------------------------|------------------------------------------------------------|
| KV state value    | no RowId                                 | RowId (8B) prepended                                        |
| Changelog         | no RowId                                 | all records carry RowId; `-U`/`-D` carry old version's RowId|
| TabletServer      | no DvRocksDB                             | maintains DvRocksDB + DV write/read paths                  |
| Tiering write     | full write to Paimon                     | `__rowid` embedded; `-U` skipped; `-D` written as DELETE   |
| Tiering pipeline  | write completes the round                | Phase A1 → A2 → A3 → Prepare → Readable Switch              |
| Readable snapshot | every snapshot is readable               | only `COMPACT` snapshots are DV-readable                    |
| Union read (PK)   | unsupported                              | supported (three-layer DV applied)                         |

### New Paimon System Column

For DV-enabled tables, one system column is added to Paimon data files:

- **`__rowid`** (BIGINT): the changelog log offset of the originating `+I` / `+U` record. After Paimon compaction rewrites rows into new files, Fluss scans this column to rebuild the RowId → file-position mapping.

Paimon compaction **must preserve `__rowid` and its values** when rewriting files. Unlike the Iceberg variant of this design, **no `__bucket` column is needed** — Paimon compaction runs within a single partition-bucket and never merges files across buckets, so the bucket is derivable from file metadata. (`__rowid` is independent from Paimon's own `_ROW_ID` field; the two coexist with different semantics.)

External engines must not directly INSERT into Fluss-managed Paimon tables; they may only run compaction / rewrite, preserving `__rowid`.

### On-Disk / On-Wire Format Changes (Internal)

`@Internal` formats whose byte layout changes for DV-enabled tables:

- **KV state value**: `[RowId(8B)][schemaId(2B)][BinaryRow]` (RowId prepended at the head so the old RowId can be read without parsing the variable-length BinaryRow).
- **Changelog value**: all four record types carry an 8-byte RowId at the value header. `+I` / `+U` carry their own log offset; `-U` / `-D` carry the old version's RowId, copied from the old KV-state value header.

### New / Extended Internal RPC

- An extended `GetDvSnapshot` RPC: given a `from` offset, returns both the bucket's `logEndOffset` (used as the tiering split's stopping offset) and the serialized **LogDv bitmap** for `[from, logEndOffset)` in a single round trip.
- New CoordinatorServer → TabletServer **prepare** and **readable-switch** notifications (each with an ack) to orchestrate a tiering round's DV state transition.

All of the above are `@Internal`.

### User-Visible Behavior Change

Union read on **Paimon-backed primary key tables** becomes supported (previously unsupported). No new client read API is introduced; the existing union read path is extended to apply the three-layer DV.

---

## Proposed Changes

### 1. Architecture: Three-Layer Deletion Vector

```
Fluss (hot layer)                           Paimon (cold layer)
┌───────────────────────────────┐           ┌──────────────────────────┐
│ Changelog                     │           │ Data files (Parquet/ORC) │
│ Log DV   (hot→hot tracking)   │           │ Paimon DV (by compaction)│
│ Lake DV  (hot→cold tracking)  │ ───────▶  │   cleaned by file lifecyc│
└───────────────────────────────┘           └──────────────────────────┘
                  (Lake DV markers take effect immediately for union read;
                   physically resolved by Paimon compaction next round)
```

During union read, the query engine applies **all three layers** so that UPDATE produces the latest value and DELETE fully removes the row, regardless of which layer the original data resides in.

- **Paimon Deletion Vector** (cold, physical): produced by Paimon's **native** compaction. Fluss writes `-D` as Paimon `DELETE` records; Paimon's merge tree and compaction resolve data merging and deletion, maintaining the Paimon DV files. **Fluss does not generate any DV file itself** — this is the key difference from the Iceberg variant, where Fluss must produce Puffin DV files.
- **Log Deletion Vector** (hot→hot, logical): tracks deletes / updates within the Fluss changelog that has not yet been tiered, so union read does not surface stale versions still in the hot layer.
- **Lake Deletion Vector** (hot→cold, logical): the bridge between the layers. When Fluss receives a delete / update for a row already tiered to Paimon, the TabletServer records a logical delete marker (`file_id → deleted row positions bitmap`). It takes effect **immediately** for union read and is physically resolved when Paimon compaction next replaces the file. LakeDv entries are cleaned by **file lifecycle** (see §5).

### 2. Data Model & Storage

#### 2.1 RowId

A RowId uniquely identifies a **specific version** of a KV record (not the primary key). Different versions of the same key have different RowIds. Its value is the **log offset** of the corresponding `+I` / `+U` changelog record.

| KV operation     | Changelog record(s)                           | RowId                                  |
|------------------|-----------------------------------------------|----------------------------------------|
| `PUT (key1, v1)` | `+I (offset=0, key1, v1)`                     | RowId = 0 (first version)              |
| `PUT (key1, v2)` | `-U (offset=1, key1, v1)` + `+U (offset=2,v2)`| `-U` references RowId 0; new RowId = 2 |
| `DELETE (key1)`  | `-D (offset=3, key1, v2)`                      | references RowId 2 (version to delete) |

- `+I` / `+U`: RowId = the record's own log offset, set at write time.
- `-U` / `-D`: RowId = the deleted version's log offset, extracted from the old KV-state value header (first 8 bytes).
- KV state (RocksDB): RowId = current version's log offset, in the value header.

RowId is 8 bytes and ties directly to the Paimon `__rowid` column.

#### 2.2 FilePos

Locates a row's physical position in Paimon:

- **file_id** (int): dictionary-encoded id of the Paimon data file (not the raw path). 4 bytes ≈ 4 billion files.
- **row_position** (long): 0-based row number within the file.

Both fields use unsigned varint (LEB128) encoding; in the common case (file_id < thousands, row_position < millions) a FilePos occupies 3–5 bytes. RocksDB tracks per-entry value length, so variable-width values work natively.

#### 2.3 DvRocksDB

A dedicated RocksDB instance per bucket, **independent** from the KvTablet RocksDB (independent checkpoint / recovery, independent lifecycle bound to Paimon snapshots, independently tunable). Five column families:

| Column Family    | Key                  | Value                            | Description |
|------------------|----------------------|----------------------------------|-------------|
| **RowPosIndex**  | RowId (8B)           | FilePos (varint)                 | Position in the current readable snapshot. Updated only at readable switch (SST Ingest). |
| **LogDv**        | offset_range         | del_bitmap                       | Deleted offsets within each changelog range. |
| **LakeDv**       | file_id (4B)         | del_bitmap (RoaringPositionBitmap)| Unmaterialized logical deletes for Paimon files. |
| **FileDict**     | file_path ↔ file_id  | (bidirectional)                  | Dictionary encoding for Paimon file paths (forward + reverse). |
| **PendingDeletes**| RowId (8B)          | FilePos (varint) or `pending`    | Unmaterialized dead-row log; resolves timing gaps and compaction-rewrite position changes at readable switch. |

> **Why RowPosIndex is a single CF**: between prepare and readable switch, union read still serves the old snapshot and reads the old RowPosIndex; the new positions are needed only at the actual switch, where RowPosIndex is atomically updated via SST Ingest under the write lock. There is no window requiring both old and new positions simultaneously.

#### 2.4 Concurrency: DvRWLock

A reader-writer lock. All DV write paths take the write lock and are serialized; union read takes the read lock (concurrent among readers, mutually exclusive with writers).

| Lock holder            | Lock type   | Operations |
|------------------------|-------------|------------|
| `-U`/`-D` processing   | write lock  | RowPosIndex point-get, PendingDeletes write, LakeDv update, LogDv update |
| Prepare (Phase B)      | write lock  | FileDict write, store SST path (no Ingest), resolve oldFiles |
| Readable switch (Phase C)| write lock| Ingest SST → RowPosIndex, batch resolve, oldFiles cleanup, LogDv/PendingDeletes cleanup |
| Union read             | read lock   | read `readableSnapshotId`, clone LakeDv bitmap subset, range-read LogDv |

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
   - **Hit `(file_id, row_position)`** — the row's Paimon position is known: mark `row_position` in `LakeDv[file_id]`, delete the RowPosIndex entry, and write `PendingDeletes[oldRowId] = (file_id, row_position)`.
   - **Miss** — the row is currently in the tiering / compaction pipeline (SST not yet Ingested): write `PendingDeletes[oldRowId] = pending`. The next readable switch's batch resolve fills in the LakeDv marker after Ingest.
   - Update `LogDv`: mark `offset = oldRowId` as deleted in the corresponding changelog range.
4. Release the DvRWLock write lock.
5. Advance `log_hw`; release the KvTablet write lock.

> **Ordering constraint**: DV must be updated and the DvRWLock released **before** `log_hw` advances. Otherwise union read could observe a larger `logEndOffset` while LakeDv is not yet updated, surfacing deleted data.

> **Why PendingDeletes also records the hit position**: it serves a dual role — a fallback for misses, and a **reverse index** for readable switch. When compaction rewrites a row to a new file, batch resolve detects the new position via a RowPosIndex point-get and patches LakeDv for the new file, without scanning every SST entry.

### 4. Tiering Pipeline (Paimon-specific)

Because Paimon is an LSM store, two facts reshape the pipeline relative to a direct-write lake:

1. **Position is unknown at write time** — newly written rows land in L0; their stable physical position is only known **after compaction** merges them into lower levels. Fluss must **scan the compaction output files** to build RowId → FilePos.
2. **Only `COMPACT` snapshots are DV-readable** — an `APPEND` snapshot contains only L0 files (unstable positions). The prepare → publish → readable-switch flow is triggered **only when a new `COMPACT` snapshot appears**, not on every write.

**CoordinatorServer is the single orchestrator**; the TieringService (Flink job) writes / commits data, optionally triggers compaction, scans, and reports.

#### 4.1 End-to-End Timeline

```
TieringService (Flink job)        CoordinatorServer            TabletServer (per bucket)
        │                                 │                            │
  A1: write changelog → Paimon L0         │                            │
      commit APPEND snapshot (no trigger) │                            │
        │                                 │                            │
  A2: trigger/await compaction            │                            │
      → COMPACT snapshot                  │                            │
        │                                 │                            │
  A3: detect new COMPACT snapshot         │                            │
      compute per-bucket readableOffset   │                            │
      scan __rowid → build SST, upload    │                            │
      report ───────────────────────────▶│                            │
        │                          B: prepare ──────────────────────▶ download SST (no lock)
        │                                 │                            write FileDict, store path,
        │                                 │                            resolve oldFiles (write lock)
        │                                 │◀───── ready ack ───────────┤
        │                          [barrier: all ready acks]           │
        │                          C: mark COMPACT DV-readable (ZK)     │
        │                             readable switch ───────────────▶ Ingest SST → RowPosIndex
        │                                 │                            batch resolve PendingDeletes
        │                                 │                            cleanup oldFiles / LogDv
        │                                 │                            advance readableSnapshotId
        │                                 │◀───── switched ack ────────┤
        │                          [barrier: all switched acks → round complete]
```

Two-phase ack: the **ready ack** gates publishing (pre-publish liveness check + SST prefetch, so the post-publish stale window is local-only); the **switched ack** is observability only (ordering is guaranteed by CoordinatorServer).

#### 4.2 Key Concept: tieredOffset vs readableOffset (per-bucket)

```
tieredOffset[bucket]   = max log offset whose data has been written to Paimon (latest APPEND)
readableOffset[bucket] = max log offset whose data has been compacted into base files (per-bucket)

readableOffset[bucket] ≤ tieredOffset[bucket]
```

Because compaction is **per-bucket**, one `COMPACT` snapshot may flush only some buckets' L0; other buckets still have L0 and a lower readable offset. Example:

```
APPEND  S1: bucket0 writes offset 0–3, bucket1 writes offset 0–5
COMPACT S2: flushes only bucket0's L0 → L1
APPEND  S3: bucket0 writes offset 4–7

With S2 as the readable snapshot:
  tieredOffsets   = {bucket0: 3, bucket1: 5}        (from S1)
  readableOffsets = {bucket0: 3,  bucket1: < 5}     (bucket1 still has L0 → must back off)
```

**Per-bucket readable offset algorithm** (Phase A3):

1. Classify buckets in the COMPACT snapshot into *no-L0* and *has-L0*.
2. **No-L0 bucket**: all data is in base files → `readableOffset = latestTieredOffset`.
3. **Has-L0 bucket**: walk COMPACT snapshots backwards to find the most recent one that flushed this bucket's L0; from the snapshot that "exactly held" those L0 files, take the preceding APPEND snapshot's registered offset as `readableOffset`. (All data up to that offset is settled in base files.) If no flush record is found, return null and skip this round's advance for the bucket.

> **Why back off to the L0 source's preceding APPEND offset**: an L0 file's rows have unstable positions (a later compaction will move them). The safe readable boundary is the offset just before those L0 files were written. Everything before it is in base files with stable positions.

#### 4.3 Phase A1: Write to Paimon

**Split generation (TabletServer)** — unlike the Iceberg variant, **no `lakeDvSnapshot` is needed** (deletes go to Paimon as DELETE records, not Fluss-generated DV files):

1. Under KvTablet read lock, read `log_hw` as `latest_offset`.
2. Snapshot LogDv for the split range → `logDvSnapshot` (deleted RowIds within the round).
3. Generate split `{offset_range: (last_tiered_offset, latest_offset], logDvSnapshot}`.

**TieringService write** — for each changelog record:

| Record               | Action in Paimon                                                              |
|----------------------|-------------------------------------------------------------------------------|
| `+I` / `+U` (not in logDvSnapshot) | write as `KeyValue(key, seq=logOffset, ADD, value)`, with `__rowid = RowId` embedded |
| `+I` / `+U` (in logDvSnapshot)     | **skip** — written-then-deleted within this round                          |
| `-D`                 | write as `KeyValue(key, seq=logOffset, DELETE, null)` so compaction removes the old data |
| `-U`                 | **skip** — the corresponding `+U` (higher seq) supersedes the old version via Paimon's DEDUPLICATE merge |

Commit an `APPEND` snapshot tagged with the `fluss.tiering` property (distinguishes Fluss-produced snapshots from external compaction snapshots). **This APPEND snapshot does not trigger the prepare / switch flow.**

> **Why `-U` is not written**: Paimon's DEDUPLICATE merge keeps the highest sequence number per key. Since `+U`'s seq (its log offset) exceeds the old version's, compaction naturally overwrites the old version — `-U`'s semantics are handled implicitly.

> **Why `-D` must be written**: if a key was tiered in a previous round and is deleted this round, the DELETE marker must reach Paimon, otherwise the key persists forever in Paimon's data files.

#### 4.4 Phase A2: Compaction

Compaction merges L0 into lower levels, producing stable files and a `COMPACT` snapshot. The executor is **flexible**:

- **TieringService-triggered**: after writing, trigger `fullCompaction = true` and block until done. Simple; TieringService controls the timeline.
- **External compact job**: an independent Flink compact job / periodic task performs compaction. TieringService only writes L0 and **detects** completion in Phase A3.

Both are transparent to downstream processing — Phase A3 uses Paimon snapshot diff to capture file changes regardless of source. **Unfinished L0 must not be skipped**: RowPosIndex must contain all tiered rows' positions, so Phase A3 waits until all of this round's L0 files are consumed by some COMPACT snapshot (full compaction, or continued polling in external mode with a configurable timeout / fallback trigger).

#### 4.5 Phase A3: Detect + Scan + SST

Triggered on detecting a new, **unregistered** COMPACT snapshot (checked after each APPEND commit):

1. **Detect**: find the latest COMPACT snapshot ≤ current tiered snapshot; if none, or already registered, skip. (Multiple APPEND snapshots may follow one COMPACT snapshot.)
2. **Compute per-bucket readableOffset** (§4.2 algorithm) and `tieredOffsets`.
3. **Collect file changes** across all COMPACT snapshots between the last readable snapshot and the current one: `allNewFiles`, `allOldFiles`. (Captures both this round's compaction and any concurrent external/background compaction.)
4. **Scan** each new file's `__rowid` column (projection pushdown — a single long column) to build `RowId → (file_id, row_position)`. Scope is limited to compaction output, not the whole table.
5. **Generate SST + upload**: allocate `file_id`s via `FileDictAllocator`; write a sorted SST (`key=RowId`, `value=fileId+row_position`); upload per bucket to `{remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`; write a cross-bucket index at `{indexUuid}`.
6. **Report** to CoordinatorServer: `indexUuid`, `readableSnapshotId` (= COMPACT snapshot id), per-bucket `tieredOffsets` / `readableOffsets`, `newFileDictEntries`, `oldFiles` (= `allOldFiles`), `earliestSnapshotIdToKeep`.

> **FileDictAllocator** is stateless: `nextFileId` is recovered from the Paimon snapshot property `fluss.nextFileId`; the in-memory `pathToFileId` map is for batch dedup only. After restart, the same path may get a different `file_id`, which remains correct since each bucket's RowPosIndex and FileDict are self-consistent.

> Compared to the Iceberg variant: the report carries **`oldFiles` and per-bucket offsets** instead of `materializedLakeDv` — because LakeDv cleanup is file-lifecycle-based, not bitmap-diff-based.

#### 4.6 Phase B: Prepare

CoordinatorServer sends a prepare notification to all relevant buckets carrying `indexUuid`, `readableSnapshotId`, per-bucket `tieredOffsets` / `readableOffsets`, `newFileDictEntries`, and `oldFiles`.

- **Phase 1 (no lock — pure remote I/O)**: locate the SST via `indexUuid → cross-bucket index → sstDir`; download the manifest and SST files locally.
- **Phase 2 (DvRWLock write lock — lightweight)**: write `newFileDictEntries` to FileDict (idempotent; a `file_id` mapping to a different path is a bug → fail-fast); store the local SST path as `pendingSstPath` (**no Ingest**); resolve `oldFiles` to `file_id`s as `pendingOldFileIds`.
- Send the bucket's **ready ack**.

Prepare modifies no DV state beyond FileDict and stored paths, so rollback on failure is trivial (clear the stored path). Processing is idempotent.

#### 4.7 Phase C: Publish + Readable Switch

After all ready acks, CoordinatorServer marks the COMPACT snapshot DV-readable (updates LakeTableZNode), then notifies TabletServers to switch. Under the DvRWLock write lock, each bucket:

1. **Ingest SST → RowPosIndex** (`IngestExternalFile`). The SST contains all compaction-output rows, so RowPosIndex now reflects post-compaction positions. Rewritten rows' new positions overwrite old ones (higher sequence number); rows tombstoned by §3.2 are "resurrected" with the new position and handled by step 2.
2. **Batch resolve PendingDeletes** — for each `(R, v)`, `hit = RowPosIndex.get(R)`:
   - **Hit** (timing gap / compaction rewrite / zombie): `LakeDv[hit.fileId] |= {hit.pos}`; delete `RowPosIndex[R]`; update `PendingDeletes[R] = hit`.
   - **Miss** and `R < readableOffset[bucket]`: orphan (row covered by tiering+compaction but absent from base files — eliminated by a DELETE merge or by logDvSnapshot) → delete `PendingDeletes[R]`.
   - **Miss** and `R ≥ readableOffset[bucket]`: row still in uncompacted L0 → keep for next round.
3. **Cleanup oldFiles (file-lifecycle)** — for each `fileId` in `pendingOldFileIds`: delete `LakeDv[fileId]`; delete PendingDeletes entries whose value points to an old file. **Must run after step 2** so migrated positions are carried over first.
4. **Cleanup expired LogDv** (range end < the new `snapshotStartLogOffset`).
5. **Advance** `readableSnapshotId` and per-bucket `snapshotStartLogOffset = readableOffset`.
6. Clear `pendingSstPath` / `pendingOldFileIds`; release the lock; send the **switched ack**.

> **Why `snapshotStartLogOffset = readableOffset` (not tieredOffset)**: union read fetches changelog from `snapshotStartLogOffset` to supply untiered increments. Using tieredOffset would skip L0 rows not yet visible in the Paimon readable snapshot, dropping data. readableOffset ensures only base-file-visible data is skipped.

#### 4.8 First-Time Bootstrap

Write to L0 → await compaction → scan + SST + report → prepare (download SST, oldFiles empty) → publish → readable switch (Ingest SST; PendingDeletes empty so batch resolve is a no-op; oldFiles empty). RowPosIndex now reflects S1; LakeDv empty.

### 5. LakeDv Cleanup: File Lifecycle (not bitmap diff)

This is the central difference from the Iceberg variant.

| Dimension          | Iceberg variant                              | Paimon variant                                |
|--------------------|----------------------------------------------|-----------------------------------------------|
| Cleanup trigger    | after each tiering commit                    | after each compaction that replaces files     |
| Cleanup method     | bitmap diff (`LakeDv AND NOT materializedLakeDv`) | drop the old file's LakeDv entry          |
| Required info      | `materializedLakeDv` (reported)              | `oldFiles` (reported)                         |
| Incremental safety | diff preserves post-snapshot bits            | naturally safe — new bits point at new files  |

**Correctness** — when compaction replaces `file_A` (e.g. rewriting it into `file_B`):

1. **Deleted rows in `file_A`** (pos0, pos2): their DELETE markers were written to Paimon; compaction removes them, so they do not appear in `file_B`. Dropping `LakeDv[file_A]` is correct.
2. **Surviving rows migrated to `file_B`**: the SST scan captured `RowId → (file_B, new_pos)`. A later delete on them is correctly marked on `LakeDv[file_B]` by §3.2.
3. **A new delete arrives after compaction but before readable switch**: §3.2 point-gets RowPosIndex (still old position, SST not Ingested) → marks `LakeDv[file_A] += {pos1}` and writes PendingDeletes. At readable switch, batch resolve finds the new position `(file_B, pos_x)` (post-Ingest) → marks `LakeDv[file_B] += {pos_x}`; then oldFiles cleanup drops `LakeDv[file_A]` (including the now-stale pos1). Result: the delete is correctly carried to the new position.
4. **Compaction does not touch a file**: its LakeDv entry is unchanged; surviving rows keep their positions and markers stay effective; cleaned when a future compaction replaces it.

Because cleanup is whole-file, the "redundant already-materialized entries" problem of the bitmap-diff approach does not arise here.

### 6. Union Read

1. The client obtains the latest DV-readable `snapshotId` (`requestedSnapshotId`) and sends a union read request carrying it.
2. Fluss lists the data files under that snapshot.
3. Under KvTablet read lock + DvRWLock read lock, perform the **snapshot consistency check**: `readableSnapshotId == requestedSnapshotId`?
   - `requested < current`: this TabletServer already switched to a newer snapshot → client refreshes to the newer id and retries.
   - `requested > current`: a newer target was published but this bucket has not switched yet → client **keeps the same id** and retries with backoff (must not fall back to an older snapshot).
4. Read `logEndOffset`; clone the **LakeDv bitmap subset** for the requested files (only query-relevant files); range-read **LogDv** from the snapshot's start offset to `logEndOffset`.
5. Release locks; serialize and return `{lakeDv, logDv, logEndOffset}` (outside locks).

**Client-side processing**:

1. Apply the **Paimon DV** (physical, from compaction) on the Paimon snapshot.
2. Apply the returned **LakeDv** (logical) to mask not-yet-compacted deletes.
3. Read surviving Paimon rows.
4. Fetch `[snapshotStartLogOffset, logEndOffset]` changelog, apply **LogDv**, skip deleted records.
5. Merge for the complete, exactly-once result.

The single-snapshot consistency rule (all buckets serve the same snapshot per request) trades a brief stale window — between publish and per-bucket switch, handled transparently by client retry — for a single-snapshot / single-LakeDv / single-RowPosIndex architecture.

### 7. Compaction Scenarios

In the Paimon variant, compaction is a **core path**, not an external exception. Phase A3's snapshot diff handles all sources uniformly:

- **TieringService self-triggered**: explicit full compaction + wait; newFiles / oldFiles taken directly from the compaction result. Concurrent external compaction is still captured by the snapshot diff.
- **External compact job**: TieringService polls Paimon snapshots (configurable interval, e.g. 1–5 s) and proceeds once this round's L0 files appear in some COMPACT snapshot's `removedFiles`; configurable timeout with optional self-trigger fallback.
- **Mixed mode**: both run; the snapshot diff collects all COMPACT file changes between the last readable snapshot and the current one, agnostic to source.

Paimon snapshot expiration must preserve the current readable snapshot and all files it references.

### 8. Failure Handling & Recovery

#### 8.1 TieringService (stateless)

Recovery keys off the A1 / A2 / A3 boundary; Paimon's `commitIdentifier` makes re-writes idempotent.

| Failure point                     | Paimon state        | SST state | Recovery |
|-----------------------------------|---------------------|-----------|----------|
| before / during A1 write          | none / partial L0   | none      | full retry (idempotent re-write via `commitIdentifier`) |
| after A1 commit, before compaction| L0 committed        | none      | resume from A2 (trigger / await compaction) |
| after compaction, before SST      | compacted           | none      | resume from A3 (deterministic snapshot diff re-detects new files) |
| after SST upload, before report   | compacted           | uploaded  | metadata reconcile: rebuild report from Paimon snapshot + manifests; do not re-commit |
| report failed                     | compacted           | uploaded  | metadata reconcile (same) |

`nextFileId` is always recovered from the latest Paimon snapshot property.

#### 8.2 TabletServer

DvRocksDB periodically checkpoints (independent from KvTablet snapshots), recording `restoreSnapshot`, `snapshotStartLogOffset`, and `checkpointLogHw`. Recovery:

1. Load the checkpoint; RowPosIndex reflects `restoreSnapshot`.
2. Replay changelog from `checkpointLogHw + 1` — **deletes only** (replay never adds RowPosIndex entries; it only deletes entries and updates LakeDv / LogDv / PendingDeletes).
3. For post-checkpoint readable-switched snapshots: query CoordinatorServer for the current DV-readable snapshot; download and **Ingest the intermediate snapshots' remote SSTs in commit order** (in-order Ingest ensures later snapshots win for the same RowId), rebuilding RowPosIndex; then replay deletes and batch-resolve PendingDeletes.
4. **Skip oldFiles LakeDv cleanup** during recovery (no `pendingOldFileIds`). Redundant LakeDv entries (pointing at already-replaced files) may remain — harmless, since union read double-marking is idempotent — and are eliminated in the next normal tiering round's oldFiles cleanup.

> RowPosIndex recovery depends entirely on the checkpoint plus re-Ingesting remote SSTs; it cannot be rebuilt from changelog replay.

#### 8.3 CoordinatorServer

Orchestration is reconstructed entirely from LakeTableZNode, with two ZK checkpoints: (1) round metadata persisted (committed, not yet DV-readable); (2) S_new published as DV-readable.

- **Metadata present, not DV-readable**: re-send prepare (idempotent) using persisted metadata → collect ready acks → publish → readable switch.
- **DV-readable**: re-send readable switch (buckets already switched skip; others have `pendingSstPath` ready).
- **No pending round**: no action; TieringService re-reports or retries.

#### 8.4 Ordering & Idempotency

CoordinatorServer is the single orchestrator and does not start round N+1's prepare before round N's readable switch completes; TabletServer needs no local ordering check. Single-flight per split; retry only after CoordinatorServer explicitly declares the attempt failed; a cancelled attempt sends no further report / ack. Prepare and readable switch are idempotent.

### 9. Data Format & Protocol Changes (summary)

- **KV state value**: `[RowId(8B)][schemaId(2B)][BinaryRow]`.
- **Changelog value**: all four record types carry an 8-byte RowId header; `-U` / `-D` carry the old version's RowId.
- **Paimon data column**: add `__rowid` (BIGINT) for DV tables; compaction must preserve it. No `__bucket` column (Paimon compaction is intra-bucket).
- **Paimon table config**: `DEDUPLICATE` merge engine; `paimon.deletion-vectors.enabled` kept consistent with the Fluss switch.
- **RPC**: extended `GetDvSnapshot` (returns `logEndOffset` + LogDv bitmap in one call); prepare / readable-switch notifications + acks.

---

## Compatibility, Deprecation, and Migration Plan

- **Opt-in, off by default**: gated by `table.deletion-vectors.enabled = false`. Existing tables and behavior are unaffected unless a table opts in at creation.
- **No migration of existing tables**: the switch is immutable post-creation; existing primary key tables keep their current tiering and remain without primary key union read. Dynamic enabling is impossible — pre-existing KV state has no embedded RowId, and pre-existing Paimon files have no `__rowid` column, so historical positions cannot be rebuilt.
- **Prerequisite enforcement**: creating a DV table without a primary key, without datalake enabled, or without FULL changelog mode is rejected at creation; `paimon.deletion-vectors.enabled` is kept consistent automatically.
- **Storage / write overhead** (the reason DV is opt-in): an 8-byte RowId per KV-state value and per changelog record (typically <10% of payload); a dedicated DvRocksDB instance per bucket; post-compaction `__rowid` scan + SST upload/download + multi-phase coordination per round; extra LakeDv / LogDv filtering at read time.
- **MergeEngine matrix** — all compatible under FULL changelog mode:

  | MergeEngine    | Notes                                                  |
  |----------------|--------------------------------------------------------|
  | DEDUPLICATE    | standard upsert; `-U`/`-D` carry oldRowId              |
  | FIRST_ROW      | duplicate keys ignored (no `-U`); DELETE still emits `-D` |
  | PARTIAL_UPDATE | requires FULL changelog mode                           |
  | AGGREGATE      | requires FULL changelog mode                           |

- **External writer constraint**: external engines must not INSERT into Fluss-managed Paimon tables and must preserve `__rowid` during compaction (Fluss-sole-writer is an existing constraint; DV adds the column-preservation requirement).

---

## Test Plan

- **Union read correctness** (system / integration): end-to-end primary key union read across the hot/cold boundary; exactly-once results for INSERT / UPDATE / DELETE spanning tiering rounds; the Appendix walkthrough scenario end-to-end.
- **Paimon write path** (`MergeTreeWriter` DV mode): `-U` skipped; `-D` written as `DELETE` with seq=logOffset; `+I`/`+U` written as `INSERT` with seq=logOffset and `__rowid` populated; LogDv filter skips superseded `+I`/`+U`; non-DV mode unchanged. Schema conversion adds `__rowid` only when DV is enabled and rejects a user column named `__rowid`.
- **Per-bucket offset & compaction**: readableOffset computation when some buckets still hold L0; TieringService-triggered vs external vs mixed compaction; partial/unfinished L0 (must wait for full L0 consumption); only COMPACT snapshots trigger prepare/switch.
- **Tiering pipeline**: two-phase ack barriers; deferred Ingest at readable switch; file-lifecycle LakeDv/PendingDeletes cleanup ordering (after batch resolve); timing-gap and zombie PendingDeletes resolution; orphan cleanup using readableOffset.
- **Failure & recovery**: TieringService failover at each A1/A2/A3 boundary (idempotent re-write, no duplicate alive rows; `nextFileId` from snapshot property); TabletServer recovery from checkpoint + changelog replay + in-order SST re-Ingest, including post-recovery redundant-LakeDv elimination; CoordinatorServer failover at each ZK checkpoint (idempotent prepare / switch).
- **Concurrency**: DvRWLock serialization across `-U/-D`, prepare, readable switch, and union read; union read never observes an advanced `log_hw` with stale DV.
- **Config & validation**: prerequisite rejection at creation; immutability under `ALTER TABLE`; Paimon-side switch consistency.

---

## Rejected Alternatives

- **In-memory full merge at read time (no DV)**: read all hot changelog plus all Paimon data and deduplicate by primary key on the client. Correct but prohibitively expensive — cost scales with full table size, not the delete delta, defeating the purpose of tiering. Rejected.

- **Keep equality-delete-style tiering**: neither solves real-time cross-layer masking (deletes only visible after the next commit) nor the small-file / read-amplification / metadata-bloat problems, and ignores Paimon's native compaction DV. Rejected.

- **Build positions at write time (treat Paimon like a direct-write lake)**: infeasible — rows in L0 have no stable position until compaction merges them down, and compaction may be performed by an external job outside Fluss's control. Hence positions are built by scanning compaction output (`__rowid`) in Phase A3. Rejected.

- **Bitmap-diff LakeDv cleanup (the Iceberg variant's approach)**: in Paimon, deletes are not materialized into a Fluss-controlled DV file whose contents we know precisely; compaction replaces whole files. File-lifecycle cleanup matches Paimon's model exactly, avoids tracking `materializedLakeDv`, and is naturally incremental-safe. Rejected in favor of file-lifecycle cleanup.

- **Add a `__bucket` column (as in the Iceberg variant)**: unnecessary — Paimon compaction never merges files across partition-buckets, so the bucket is recoverable from file metadata. The column would only inflate storage. Rejected.

- **Treat every snapshot as readable**: an APPEND snapshot exposes only L0 files with unstable positions; serving it yields wrong or shifting positions. Only COMPACT snapshots, whose data is settled in base files, are made DV-readable. Rejected.

- **Use tiered offset (not per-bucket readable offset) as the changelog start for union read**: would skip rows still in uncompacted L0 that are invisible in the Paimon readable snapshot, dropping data. The per-bucket readable offset is the safe boundary. Rejected.

- **Serve two snapshots simultaneously to avoid the stale window**: requires maintaining two LakeDv and two RowPosIndex states, doubling write-path point-get cost and complexity. We instead accept a brief (tens-of-ms) stale window handled by client retry, keeping a single-snapshot architecture. Rejected.

- **Single-phase publish-then-switch (skip prepare)**: would include remote SST download in the post-publish stale window and lose the pre-publish liveness check. The two-phase prepare front-loads remote I/O and gates publish on ready acks. Rejected.

- **Eager SST Ingest during prepare**: would require dual position column families (one per snapshot) to keep union read on the old snapshot correct between prepare and switch. Deferring Ingest to readable switch keeps a single RowPosIndex CF. Rejected.

---

## Appendix A: End-to-End Walkthrough

Initial state: Paimon empty; RowPosIndex / LakeDv / LogDv / PendingDeletes empty; no readable snapshot.

**Step 1 — Write 3 records**: `PUT key1,key2,key3` → `+I` at offsets 0,1,2 → RowIds 0,1,2. KV state stores RowId per value.

**Step 2 — First tiering**: split `[0,2]`, logDvSnapshot empty. A1 writes 3 `ADD` rows (with `__rowid`) to L0. A2 compacts → `file_A` at pos0/1/2. A3 scans `__rowid`, SST `{0→(A,0),1→(A,1),2→(A,2)}`, reports. Prepare downloads SST. Publish S1. Readable switch Ingests SST → RowPosIndex `{0→(A,0),1→(A,1),2→(A,2)}`; LakeDv empty; `readableSnapshotId=S1`.

**Step 3 — Update key1**: `-U(offset=3, oldRowId=0)` + `+U(offset=4)`. §3.2 on `-U`: RowPosIndex[0] hit `(A,0)` → `LakeDv[A]={0}`; delete RowPosIndex[0]; `PendingDeletes[0]=(A,0)`; `LogDv` marks offset 0.

**Step 4 — Union read (S1)**: server returns `lakeDv={A:{0}}, logDv={0}, logEndOffset=4`. Client: Paimon `file_A` minus pos0 → `key2,key3`; changelog `[3,4]` → `+U(key1,v4)`. Result `(key1,v4),(key2,v2),(key3,v3)` ✓.

**Step 5 — Delete key3**: `-D(offset=5, oldRowId=2)` → `LakeDv[A]={0,2}`; delete RowPosIndex[2]; `PendingDeletes[2]=(A,2)`; LogDv marks offset 2.

**Step 6 — Second tiering**: split `[3,5]`. A1 writes `+U(key1,v4)` as ADD to L0, `-D(key3)` as DELETE; `-U(offset=3)` skipped. A2 compacts → `file_B` holds `(key1,v4)` at pos0; `file_A` rewritten/replaced (key3 deleted, key1's old version removed by the `+U` higher seq, key2 migrated). A3 scans new files, SST includes `{4→(B,0)}` and key2's new position; `oldFiles={file_A}`; reports per-bucket readable offset. Readable switch: Ingest SST; batch resolve PendingDeletes `0` and `2` (now in `oldFiles`/eliminated) ; **oldFiles cleanup drops `LakeDv[file_A]`**; advance `readableSnapshotId=S_compact`. LakeDv empty, PendingDeletes cleaned ✓.

**Step 7 — New writes + union read** demonstrate three-layer cooperation: Paimon DV (compaction-materialized historical deletes) + LakeDv (new unmaterialized delete) + LogDv (untiered increment) jointly produce the exactly-once result.

---

## Appendix B: File Path Conventions

```
{remoteLakeTableSnapshotDir}/
├── metadata/
│   └── {UUID}.offsets              ← existing
└── rowPos/
    ├── {bucketId}/{uuid}/          ← per-bucket SST directory
    │   ├── manifest                ← SST file names, newFileDictEntries, offsets
    │   └── sst_0.sst
    └── {indexUuid}                 ← cross-bucket index (bucketId → sstDir)
```

`{remoteLakeTableSnapshotDir}` = `{remote.data.dir}/lake/{databaseName}/{tableName}-{tableId}`.
