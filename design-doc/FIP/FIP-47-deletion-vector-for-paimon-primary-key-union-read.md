# FIP-47: Introduce Deletion Vectors to accelerate Primary Key Table Union Read over Paimon

## Motivation

Under the lakehouse (Streamhouse) architecture, Fluss serves as the real-time layer while Paimon serves as the historical layer. Fluss continuously tiers real-time data into Paimon, and provides **union read** — combining hot-layer incremental data not yet tiered with historical data already in Paimon, presenting a single, complete table with exactly-once semantics.

Union read works for both log tables and primary key tables today, but at very different cost. Log tables are append-only and need no deduplication. Primary key tables require **cross-layer deduplication** on every read, which Fluss currently performs as a full **sort-merge** between the hot changelog and historical Paimon data. The result is correct, but the cost scales with the table size rather than with the rows changed since the last tiering round. This FIP moves that work out of the read-time full merge and replaces it with lightweight deletion vectors.

### Problem 1: Cross-Layer Deduplication for Union Read

For primary key tables, updates and deletes first arrive at Fluss, but older versions of the same row may already have been tiered into Paimon. During union read, the system must precisely mask rows in Paimon that have been updated or deleted on the Fluss side; otherwise stale rows resurface from the historical layer, violating exactly-once semantics.

Consider a row `(key1, v1)` that has been tiered to Paimon. The user then issues `UPDATE key1 → v2`. This update arrives at Fluss as `-U(key1, v1)` + `+U(key1, v2)`; `v2` is still in the Fluss hot layer, while `v1` is already in a Paimon data file. A union read at this moment must return exactly `(key1, v2)` — it must read `v2` from the changelog **and** mask `v1` in Paimon. Today the masking is achieved only by sort-merging the changelog against the full Paimon dataset on every read.

This sort-merge is correct but expensive: it re-reads and re-sorts data proportional to the **full table size** on every query, even when only a handful of rows were updated or deleted since the last tiering. The cost lands entirely on the read path and scales with the table size, not with the delete delta — exactly what a lightweight delete marker should avoid.

### Problem 2: Merge-on-Read Cost on the Paimon Side

Even within the Paimon historical layer alone, resolving updates and deletes on a primary key table requires **merge-on-read (MOR)**: a read must merge overlapping LSM runs and apply the deletes / updates on the fly. As more updates and deletes accumulate between compactions, this merge cost grows and read latency degrades.

Paimon already ships a native mechanism for exactly this — **Deletion Vectors produced during compaction**, which let readers skip deleted rows directly instead of merging runs. This design enables and relies on Paimon's native DV for the cold (historical) layer, so the Paimon-side portion of a union read avoids MOR.

### Goal

Introduce a **three-layer Deletion Vector (DV)** mechanism, integrated with Paimon's native LSM / compaction / DV machinery:

1. **Accelerate primary key union read over Paimon**: Fluss keeps logical LakeDv and LogDv markers on the TabletServer side, so a union read can mask stale rows in Paimon and in the hot changelog by bitmap lookup instead of running a full sort-merge.
2. **Use Paimon's native DV for the cold layer**: Fluss writes deletes as Paimon `DELETE` records. Paimon compaction resolves them into Paimon-managed deletion vectors, so historical reads skip deleted rows without merge-on-read. Fluss does not generate Paimon delete files.

The read path becomes proportional to the queried files and the delete delta, while preserving the same exactly-once union read result.

---

## Public Interfaces

### New Table Configuration

| Option                           | Type    | Default | Description |
|----------------------------------|---------|---------|-------------|
| `table.datalake.deletion-vectors.enabled` | Boolean | `false` | Master switch for the Fluss three-layer DV architecture. Must be set at table creation time and is **immutable** afterwards. |

Enabling DV requires (validated at table creation; creation is rejected otherwise):

- a **primary key** table;
- `table.datalake.enabled = true` (the table must be tiered to a lake);
- **FULL** changelog mode (`table.changelog.image = FULL`) — only FULL mode emits `-U` / `-D` carrying the old version, which is required to locate the old row in Paimon. Under LOOKUP mode, updates emit only `+U` without `-U`, so the old version's RowId is unknown and the old Paimon row cannot be masked.

`ALTER TABLE` attempts to change `table.datalake.deletion-vectors.enabled` are rejected (it is not in the alterable-properties whitelist), because the option changes the persisted KV-state and changelog byte layout and cannot be retrofitted onto existing data.

The Fluss DV architecture relies on Paimon's native DV for the cold layer, so when Fluss DV is on the Paimon-side switch `paimon.deletion-vectors.enabled` is forced consistent:

- Fluss DV on + Paimon switch unset → Paimon switch auto-enabled.
- Fluss DV on + Paimon switch already on → no conflict.
- Fluss DV on + Paimon switch explicitly off → **rejected** (Fluss DV depends on Paimon native DV).

When **Fluss DV is off**, the Paimon-side switch is left to the user, exactly as today (Fluss DV off + Paimon DV on is the current behavior).

### New Paimon System Column

For DV-enabled tables, one system column is added to Paimon data files:

- **`__rowid`** (BIGINT): the changelog log offset of the originating `+I` / `+U` record. After Paimon compaction rewrites rows into new files, Fluss scans this column to rebuild the RowId → file-position mapping.

Paimon compaction **must preserve `__rowid` and its values** when rewriting files. (`__rowid` is independent from Paimon's own `_ROW_ID` field; the two coexist with different semantics.)

External engines must not directly INSERT into Fluss-managed Paimon tables; they may only run compaction / rewrite, preserving `__rowid`.

### On-Disk / On-Wire Format Changes (Internal)

`@Internal` formats whose byte layout changes for DV-enabled tables:

- **KV state value**: `[RowId(8B)][schemaId(2B)][BinaryRow]` (RowId prepended at the head so the old RowId can be read without parsing the variable-length BinaryRow).
- **Changelog value**: all four record types carry an 8-byte RowId at the value header. `+I` / `+U` carry their own log offset; `-U` / `-D` carry the old version's RowId, copied from the old KV-state value header.

### New / Extended RPC

**Two RPCs are new** (`GetLakeDvSnapshot`, `DvReadableSwitch`); **two existing RPCs are extended** (`NotifyLakeTableOffset` gains per-bucket `readable_snapshot_id` + `readable_offset` for prepare; the lake-snapshot commit gains `earliest_snapshot_id_to_keep`).

These RPCs carry only control-plane information. Bulk per-bucket metadata (`newFileDictEntries`, `oldFiles`) is stored in the remote `index.json`, and per-bucket `readableOffset` is stored in the existing `readable_bucket_offsets_file_path`; neither is sent inline. Definitions below follow `fluss-rpc/src/main/proto/FlussApi.proto` (where this RPC is renamed from the existing `GetDvSnapshot*` to `GetLakeDvSnapshot*` to avoid future naming conflicts).

#### `GetLakeDvSnapshot` *(new; TabletServer; §4.2 split generation + §6 union read)*

A single RPC serves two callers. **Union read** gets the LakeDv (per-file deleted-position bitmaps) + LogDv + offsets for the requested snapshot. **Tiering split generation** sets `log_dv_from_offset`, which additionally returns the LogDv bitmap so `log_end_offset` (the split stopping offset) and `logDvSnapshot` come back in one round trip.

```protobuf
message GetLakeDvSnapshotRequest {
  required int64 table_id = 1;
  required int32 bucket_id = 2;
  required int64 readable_snapshot_id = 3;
  optional int64 partition_id = 4;
  // When set, returns LogDv bitmap for [log_dv_from_offset, logEndOffset)
  // along with logEndOffset. Used by tiering to get both stoppingOffset
  // and logDvBitmap in a single RPC.
  optional int64 log_dv_from_offset = 5;
}

message GetLakeDvSnapshotResponse {
  // LakeDv: per-file deleted position bitmaps (file_path as key, resolved via FileDict)
  repeated PbLakeDvEntry lake_dv_entries = 1;
  // LogDv: deleted log offsets bitmap (serialized Roaring64Bitmap)
  optional bytes log_dv_bitmap = 2;
  // The log end offset at snapshot time
  required int64 log_end_offset = 3;
  // The log start offset for this snapshot (snapshotStartLogOffset)
  required int64 snapshot_start_offset = 4;
}
```

#### Lake-snapshot commit — `+ earliest_snapshot_id_to_keep` *(extended; §4.4 commit)*

No DV-specific bulk payload is needed in the commit RPC. Per-bucket `readableOffset` travels through the **existing** `readable_bucket_offsets_file_path` (the file of per-bucket readable log-end offsets, symmetric with `tiered_bucket_offsets_file_path`) — TieringService writes it and references it in the commit; the coordinator reads it to relay in prepare. The commit RPC's DV addition is `earliest_snapshot_id_to_keep`. (`newFileDictEntries` / `oldFiles` live in `index.json`.)

```protobuf
message PbLakeTableSnapshotMetadata {
  required int64 table_id = 1;
  required int64 snapshot_id = 2;
  required string tiered_bucket_offsets_file_path = 3;
  optional string readable_bucket_offsets_file_path = 4;   // per-bucket readableOffset (DV uses this)
  // NEW (DV): the earliest snapshot id to retain (snapshot retention policy).
  optional int64 earliest_snapshot_id_to_keep = 5;
}
```

The persisted ZK metadata for each lake snapshot is extended with an explicit DV state marker:

```java
class LakeSnapshotMetadata {
    long snapshotId;
    FsPath tieredOffsetsFilePath;
    // Persisted even while dvPendingReadable is true, so coordinator failover
    // can reconstruct prepare.
    @Nullable FsPath readableOffsetsFilePath;
    boolean dvPendingReadable;
}
```

For a DV-enabled table, the coordinator registers the committed lake snapshot with `dvPendingReadable = true` and persists the `readableOffsetsFilePath` from the commit metadata. After all bucket prepare ready acks are collected, `markLakeTableSnapshotReadable` flips `dvPendingReadable` to `false` (and may rewrite the same `readableOffsetsFilePath` idempotently). For a non-DV table, `dvPendingReadable` is always `false`.

`readableOffsetsFilePath == null` alone is not a valid pending marker: non-DV snapshots may also have a null `readableOffsetsFilePath`, while a DV snapshot needs the readable-offsets path available during recovery even before it becomes DV-readable. The explicit `dvPendingReadable` bit makes coordinator recovery self-contained in ZK.

#### `NotifyLakeTableOffset` — `+ readable_offset` per bucket *(extended; §4.5 prepare)*

The prepare phase reuses the existing `NotifyLakeTableOffsetRequest`; each bucket entry gains a `readable_offset`. **No DV sub-message or flag is added** — a TabletServer recognizes a DV prepare from the table being DV-enabled plus the per-bucket `snapshot_id` / `readable_offset`, and then fetches the round's bulk metadata (SST files, `newFileDictEntries`, `oldFiles`) from the remote `index.json` itself (§4.5). So the prepare RPC stays tiny.

```protobuf
message NotifyLakeTableOffsetRequest {
  required int32 coordinator_epoch = 1;
  repeated PbNotifyLakeTableOffsetReqForBucket notify_buckets_req = 2;
}

message PbNotifyLakeTableOffsetReqForBucket {
  required int64 table_id = 1;
  optional int64 partition_id = 2;
  required int32 bucket_id = 3;
  required int64 snapshot_id = 4;   // the tiered lake snapshot (existing meaning; may be an APPEND)
  optional int64 log_start_offset = 5;
  optional int64 log_end_offset = 6;
  optional int64 max_timestamp = 7;
  // NEW (DV): the COMPACT snapshot to make DV-readable; locates rowPos/{id}/index.json
  // and becomes the bucket-local readableSnapshotId at the switch.
  optional int64 readable_snapshot_id = 8;
  // NEW (DV): base-file coverage; becomes snapshotStartLogOffset (the union-read changelog start).
  optional int64 readable_offset = 9;
}
```

#### `DvReadableSwitch` *(new; CoordinatorServer → TabletServer; §4.6)*

After the snapshot is published as DV-readable, the coordinator tells each bucket to switch: Ingest the SST into RowPosIndex, batch-resolve PendingDeletes, drop `oldFiles`' LakeDv entries, expire stale LogDv, advance the bucket-local `readableSnapshotId`. The prepare phase (§4.5) already had each bucket download its SST and read its `index.json` metadata, so the switch only needs to name the buckets.

```protobuf
message DvReadableSwitchRequest {
  required int32 coordinator_epoch = 1;
  required int64 table_id = 2;
  required int64 readable_snapshot_id = 3;
  repeated int32 bucket_ids = 4;
  repeated int64 partition_ids = 5;
}

message DvReadableSwitchResponse {
}
```

#### Shared DV sub-message

Union read returns `PbLakeDvEntry`. (Per-bucket `readableOffset` travels via the readable-offsets file and the prepare's `readable_offset` field, not a sub-message; `newFileDictEntries` / `oldFiles` live in `index.json`.)

```protobuf
message PbLakeDvEntry {
  required string file_path = 1;
  // Serialized Roaring64Bitmap of deleted row positions.
  required bytes deleted_positions_bitmap = 2;
}
```

### RowPos Index Files (remote lake storage)

For DV-enabled tables, each tiering round persists the `RowId → position` records under the table's remote lake-snapshot directory so any TabletServer can (re)build its RowPosIndex (§4.4; path layout in Appendix B). These are durable on-storage formats — a cross-version contract, since recovery reads back files written by earlier versions.

**`index.json`** (`RowPosSstIndex`) — one per snapshot at `rowPos/{snapshotId}/[{partitionId}/]index.json`, mapping each participating bucket to its SST file list, the `file_id → file_path` entries (`fileDict`) allocated that round, and the `oldFiles` replaced this round (the `fileDict` and `oldFiles` are added by this FIP so both prepare and recovery read all per-bucket bulk metadata from here rather than over RPC — §4.5 / §8.2):

```json
{
  "version": 1,
  "buckets": {
    "0": {
      "files": [ { "name": "sst_0.sst", "size": 12345 } ],
      "fileDict": [ { "fileId": 7, "path": "bucket-0/data-abc.parquet" } ],
      "oldFiles": [ "bucket-0/data-old.parquet" ]
    }
  }
}
```

**RowPos SST** — RocksDB SST files at `rowPos/{snapshotId}/[{partitionId}/]{bucketId}/sst_{N}.sst`, holding sorted `RowId → FilePos` entries:

- **Key** — RowId as an **8-byte big-endian** integer (fixed width, so RocksDB's lexicographic key order equals RowId order, which IngestExternalFile requires).
- **Value** — **FilePos** = `varint(file_id) || varint(row_position)`: two unsigned LEB128 varints, typically 3–5 bytes total. `file_id` is the FileDict-encoded id of a Paimon data file; `row_position` is the 0-based row index within that file.
- Entries are sorted by RowId ascending and split at 1,000,000 entries per file (`sst_0.sst`, `sst_1.sst`, …).

The SST value carries only `file_id`; the `file_id → file_path` mapping is held in the per-bucket **FileDict** (a DvRocksDB column family: forward `0x00 + path → BE(file_id)`, reverse `0x01 + BE(file_id) → path`). FileDict is populated by reading each snapshot's `index.json` `fileDict` — during **prepare** for the live round, and during **recovery** for post-checkpoint rounds (§4.5 / §8.2) — and persisted with the DvRocksDB checkpoint. The entries do **not** travel over RPC. A reader resolves a `file_id` (from an SST or a `LakeDv[file_id]`) to a Paimon path via FileDict's reverse lookup; union read does this server-side, so `PbLakeDvEntry` is keyed by `file_path`.

### User-Visible Behavior Change

Union read on **Paimon-backed primary key tables** becomes significantly cheaper: the per-read full sort-merge is replaced by applying the three-layer DV. No new client read API is introduced and results are unchanged; only the read cost drops (from table-size-proportional to delete-delta-proportional).

---

## Proposed Changes

### 1. Architecture: Three-Layer Deletion Vector

```
Fluss (hot layer)                           Paimon (cold layer)
┌───────────────────────────────┐           ┌──────────────────────────┐
│ Changelog                     │           │ Data files (Parquet/ORC) │
│ Log DV   (hot→hot tracking)   │           │ Paimon DV (by compaction)│
│ Lake DV  (hot→cold tracking)  │ ───────▶  │ cleaned by file lifecycle│
└───────────────────────────────┘           └──────────────────────────┘
                  (Lake DV markers take effect immediately for union read;
                   physically resolved by Paimon compaction next round)
```

During union read, the query engine applies **all three layers** so that UPDATE produces the latest value and DELETE fully removes the row, regardless of which layer the original data resides in.

- **Paimon Deletion Vector** (cold, physical): produced by Paimon's **native** compaction. Fluss writes `-D` as Paimon `DELETE` records; Paimon's merge tree and compaction resolve data merging and deletion, maintaining the Paimon DV files. **Fluss does not generate any DV file itself** — Paimon's compaction is the sole producer of physical DV files.
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
| **RowPosIndex**  | RowId (8B big-endian)| FilePos (varint)                 | Position in the current DV-readable snapshot. Updated only at readable switch (SST Ingest). |
| **LogDv**        | range start (8B BE long) | 32-bit RoaringBitmap         | Partition-style: offsets are split into fixed ranges (default 1024). Key = `floor(offset / 1024) * 1024`; value = bitmap of offsets **relative** to the range start (always < 1024). |
| **LakeDv**       | file_id (4B big-endian) | serialized `Roaring64Bitmap` | Unmaterialized logical deletes for Paimon files: per-file bitmap of deleted row positions. |
| **FileDict**     | `0x00`+path / `0x01`+BE(file_id) | BE(file_id) / path bytes | Bidirectional path↔id dictionary in one CF; a 1-byte prefix selects the direction (`0x00` forward path→id, `0x01` reverse id→path). |
| **PendingDeletes**| RowId (8B)          | FilePos (varint) or `pending`    | Unmaterialized dead-row log; resolves timing gaps and compaction-rewrite position changes at readable switch. |

#### 2.4 Concurrency: DvRWLock

A reader-writer lock protects DvRocksDB state. All DV mutations take the write lock; union read takes the read lock.

| Lock holder              | Lock type   | Operations |
|--------------------------|-------------|------------|
| `-U` / `-D` processing   | write lock  | RowPosIndex point-get, PendingDeletes write, LakeDv update, LogDv update |
| Prepare                  | write lock  | FileDict write, store pending SST path, resolve `oldFiles` to file ids |
| Readable switch          | write lock  | SST Ingest, PendingDeletes batch resolve, old-file cleanup, LogDv cleanup, readable-snapshot advance |
| Union read               | read lock   | snapshot consistency check, LakeDv subset clone, LogDv range read |

Remote I/O is kept outside the lock. Prepare downloads SSTs before taking the write lock, and union read serializes the response after releasing the locks.

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

**CoordinatorServer is the single orchestrator**; the TieringService (Flink job) writes / commits data, waits for external compaction, scans, and reports.

#### 4.1 End-to-End Timeline

```
TieringService (Flink job)        CoordinatorServer            TabletServer (per bucket)
        │                                 │                            │
  A1: write changelog → Paimon L0         │                            │
      commit APPEND snapshot (no trigger) │                            │
        │                                 │                            │
      (external compaction)               │                            │
      → COMPACT snapshot                  │                            │
        │                                 │                            │
  A2: detect new COMPACT snapshot         │                            │
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

#### 4.2 Phase A1: Write to Paimon

**Split generation (TabletServer)** — **no `lakeDvSnapshot` is needed** (deletes go to Paimon as DELETE records, not Fluss-generated DV files):

1. Under KvTablet read lock, read `log_hw` as `latest_offset`.
2. Snapshot LogDv for the split range → `logDvSnapshot` (deleted RowIds within the round).
3. Generate split `{offset_range: (last_tiered_offset, latest_offset], logDvSnapshot}`.

**TieringService write** — for each changelog record:

| Record               | Action in Paimon                                                              |
|----------------------|-------------------------------------------------------------------------------|
| `+I` / `+U` (not in logDvSnapshot) | write as `KeyValue(key, seq=logOffset, ADD, value)`, with `__rowid = RowId` embedded |
| `+I` / `+U` (in logDvSnapshot)     | **skip** — written-then-deleted within this round                          |
| `-D` (`oldRowId` not in logDvSnapshot) | write as `KeyValue(key, seq=logOffset, DELETE, null)` — deletes a version tiered in a **previous** round |
| `-D` (`oldRowId` in logDvSnapshot)     | **skip** — the deleted version was created and skipped within this round; nothing exists in Paimon to delete |
| `-U`                 | **skip** — the corresponding `+U` (higher seq) supersedes the old version via Paimon's DEDUPLICATE merge |

Commit an `APPEND` snapshot tagged with the `fluss.tiering` property (distinguishes Fluss-produced snapshots from external compaction snapshots). **This APPEND snapshot does not trigger the prepare / switch flow.**

> **Why `-U` is not written**: Paimon's DEDUPLICATE merge keeps the highest sequence number per key. Since `+U`'s seq (its log offset) exceeds the old version's, compaction naturally overwrites the old version — `-U`'s semantics are handled implicitly.

> **When `-D` is written vs skipped**: `-D` deletes a specific version (`oldRowId`). If that version was tiered in a previous round (`oldRowId` ≤ `last_tiered_offset`, i.e. **not** in this round's `logDvSnapshot`), the DELETE must reach Paimon, otherwise the row persists forever in Paimon's data files. If the version was created **and** deleted within this round (`oldRowId` in `logDvSnapshot`), its `+I`/`+U` was already skipped — there is nothing in Paimon to delete, so the `-D` is skipped too. This mirrors the `+I`/`+U` skip: a key whose whole lifecycle is contained in one round produces no Paimon writes at all.

#### 4.3 Compaction (external)

Compaction merges L0 into lower levels, producing stable files and a `COMPACT` snapshot. It is performed **externally** — by an independent compact job / periodic task (or Paimon's own background compaction), not by TieringService. It is not a TieringService phase; TieringService only writes L0 (A1) and **detects** the resulting COMPACT snapshot in Phase A2.

#### 4.4 Phase A2: Detect + Scan + SST

Triggered on detecting a new, **unregistered** COMPACT snapshot (checked after each APPEND commit):

1. **Detect**: find the latest unregistered COMPACT snapshot after the last DV-readable snapshot; if none exists, skip and poll again. The COMPACT snapshot may be produced after one or more APPEND snapshots, and multiple APPEND snapshots may be covered by one COMPACT snapshot.
2. **Compute per-bucket readableOffset** (max log offset settled in compacted base files; `≤ tieredOffset`) and `tieredOffsets`.
3. **Collect file changes** across all COMPACT snapshots between the last DV-readable snapshot and the detected COMPACT snapshot: `allNewFiles`, `allOldFiles`. This captures both this round's compaction and any concurrent external/background compaction.
4. **Scan** each new file's `__rowid` column (projection pushdown — a single long column) to build `RowId → (file_id, row_position)`. Scope is limited to compaction output, not the whole table.
5. **Generate SST + upload**: allocate `file_id`s via `FileDictAllocator`; write a sorted SST (`key=RowId`, `value=fileId+row_position`) per bucket; upload each bucket's SSTs to `{remoteLakeTableSnapshotDir}/rowPos/{snapshotId}/[{partitionId}/]{bucketId}/{fileName}`, and write one `index.json` per snapshot at `{remoteLakeTableSnapshotDir}/rowPos/{snapshotId}/[{partitionId}/]index.json` (`RowPosSstIndex`: maps each participating `bucketId` to its SST file names + sizes, that round's `newFileDictEntries`, and `oldFiles`). The TabletServer reads this `index.json` for **all** bulk per-bucket metadata in both prepare (§4.5) and recovery (§8.2), so none of it is sent over RPC. This single unified index replaces any per-bucket manifest or cross-bucket UUID file — the SSTs are located purely by the `{snapshotId}` / `{partitionId}` / `{bucketId}` path convention.
6. **Commit the lake-table snapshot** to CoordinatorServer (`CommitLakeTableSnapshotRequest` → `PbLakeTableSnapshotMetadata`): `snapshotId` (= the Paimon snapshot id, which also locates the SSTs / `index.json`), `earliestSnapshotIdToKeep`, and `readable_bucket_offsets_file_path` (the file of per-bucket `readableOffset`, written in this step). `newFileDictEntries` / `oldFiles` are not sent — they went to `index.json`.

> **FileDictAllocator** is stateless: `nextFileId` is recovered from the Paimon snapshot property `fluss.nextFileId`; the in-memory `pathToFileId` map is for batch dedup only. After restart, the same path may get a different `file_id`, which remains correct since each bucket's RowPosIndex and FileDict are self-consistent.

#### 4.5 Phase B: Prepare

**CoordinatorServer** — on receiving the commit (`CommitLakeTableSnapshotRequest`) from TieringService:
1. **Registers the committed lake-table snapshot in ZooKeeper** (so the round survives a coordinator failover). For DV-enabled tables, the persisted `LakeSnapshotMetadata` is written with `dvPendingReadable = true` and the commit's `readableOffsetsFilePath`: the Paimon/lake snapshot is registered, but it is not DV-readable for union read yet.
2. Sends a `NotifyLakeTableOffsetRequest` to the TabletServers hosting the relevant buckets, carrying per bucket `readable_snapshot_id` (the COMPACT snapshot to make readable) and `readable_offset`.
3. Waits for all buckets' **ready acks** — it does **not** publish yet (publish is §4.6).

**TabletServer** — on receiving the notification, for each of its targeted buckets:
1. **Phase 1 (no lock — pure remote I/O)**: read `rowPos/{snapshotId}/[{partitionId}/]index.json` to get the bucket's SST file names, `newFileDictEntries`, and `oldFiles`; download the SSTs from `rowPos/{snapshotId}/[{partitionId}/]{bucketId}/` to a local dir.
2. **Phase 2 (DvRWLock write lock — lightweight)**: write the `newFileDictEntries` to FileDict (idempotent; a `file_id` mapping to a different path is a bug → fail-fast); store the local SST path as `pendingSstPath` (**no Ingest**); resolve `oldFiles` to `file_id`s as `pendingOldFileIds`.
3. Reply the **ready ack** to CoordinatorServer.

Prepare modifies no DV state beyond FileDict and stored paths, so rollback on failure is trivial (clear the stored path). Processing is idempotent.

> **Why a separate prepare phase** (instead of just publish-then-switch in one step): the costly part of a switch is the **remote SST download** (potentially hundreds of ms). Two phases buy two things:
> 1. **Short stale window** — the download is front-loaded *before* publish, so once published, the readable switch is purely local (Ingest + batch-resolve + cleanup) and the window where clients get stale-snapshot errors shrinks to tens of ms. A single-phase design would put the remote download *inside* that window.
> 2. **Pre-publish liveness check** — CoordinatorServer publishes only after **all** buckets' ready acks. If any bucket is down or cannot fetch its SST, it does not publish, and every bucket keeps serving the old snapshot. A single-phase design would discover the dead bucket only after publish, leaving clients stuck on a snapshot one bucket can't serve.
>
> Rollback is cheap precisely because prepare touches no DV state (no Ingest), as noted above.

#### 4.6 Phase C: Publish + Readable Switch

**CoordinatorServer** — after collecting all buckets' ready acks:
1. Marks the COMPACT snapshot **DV-readable** (sets `dvPendingReadable = false` in LakeTableZNode). Clients may now target it for DV union read.
2. Sends a `DvReadableSwitch` notification to the relevant TabletServers.

**TabletServer** — on receiving the switch notification, under the DvRWLock write lock, for each of its targeted buckets:

1. **Ingest SST → RowPosIndex** (`IngestExternalFile`). The SST contains all compaction-output rows, so RowPosIndex now reflects post-compaction positions. Rewritten rows' new positions overwrite old ones (higher sequence number); rows tombstoned by §3.2 are "resurrected" with the new position and handled by step 2.
2. **Batch resolve PendingDeletes** — for each `(R, v)`, `hit = RowPosIndex.get(R)`:
   - **Hit** (timing gap / compaction rewrite / zombie): `LakeDv[hit.fileId] |= {hit.pos}`; delete `RowPosIndex[R]`; update `PendingDeletes[R] = hit`.
   - **Miss** and `R < readableOffset[bucket]`: orphan (row covered by tiering+compaction but absent from base files — eliminated by a DELETE merge or by logDvSnapshot) → delete `PendingDeletes[R]`.
   - **Miss** and `R ≥ readableOffset[bucket]`: row still in uncompacted L0 → keep for next round.
3. **Cleanup oldFiles (file-lifecycle)** — for each `fileId` in `pendingOldFileIds`: delete `LakeDv[fileId]`; delete PendingDeletes entries whose value points to an old file. **Must run after step 2** so migrated positions are carried over first.
4. **Cleanup expired LogDv** (range end < the new `snapshotStartLogOffset`).
5. **Advance** `readableSnapshotId` and per-bucket `snapshotStartLogOffset = readableOffset`.
6. Clear `pendingSstPath` / `pendingOldFileIds`; release the lock; send the **switched ack** to CoordinatorServer.

> **Why `snapshotStartLogOffset = readableOffset` (not tieredOffset)**: union read fetches changelog from `snapshotStartLogOffset` to supply untiered increments. Using tieredOffset would skip L0 rows not yet visible in the Paimon readable snapshot, dropping data. readableOffset ensures only base-file-visible data is skipped.

### 5. LakeDv Cleanup: File Lifecycle

LakeDv entries are cleaned by **file lifecycle**: when compaction replaces a file, that file's LakeDv entry is dropped as a whole.

- **Trigger**: after each compaction that replaces files (the `oldFiles` reported in §4.4).
- **Method**: for each replaced file, drop its `LakeDv[file_id]` entry outright — no per-bit bookkeeping.
- **Incremental safety**: naturally safe — any delete that arrived after the snapshot points at a *new* file, so dropping the *old* file's entry never loses it.

Because cleanup is whole-file, there is no per-bit bookkeeping to maintain and no risk of leftover redundant entries.

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

Compaction is performed externally (an independent compact job / periodic task, or Paimon's background compaction). TieringService writes L0 and **detects** the result: it polls Paimon snapshots (configurable interval, e.g. 1–5 s) and proceeds once this round's L0 files appear in some COMPACT snapshot's `removedFiles` (configurable timeout). Phase A2's snapshot diff then collects all COMPACT file changes between the last DV-readable snapshot and the detected one, agnostic to which job produced them.

Paimon snapshot expiration must preserve the current DV-readable snapshot and all files it references.

### 8. Failure Handling & Recovery

#### 8.1 TieringService (stateless)

Recovery hinges on a single boundary: **has the A1 APPEND committed?** (§4.2). Compaction is external, and A2 (detect + scan + SST + report, §4.4) is deterministic and idempotent, so every failure after the A1 commit collapses into one "resume A2" case.

| Failure point | Recovery |
|---------------|----------|
| before / during A1 commit | **Full retry.** Re-write and re-commit the APPEND; Paimon's `commitIdentifier` dedups the re-commit, so no duplicate L0. |
| after A1 commit (compaction pending or done; SST and/or report incomplete) | **Resume A2.** Wait for the COMPACT snapshot, then scan → generate + upload SST → report. Idempotent throughout: the snapshot diff is deterministic, SST paths are fixed (`rowPos/{snapshotId}/`), and the coordinator dedups a re-sent report. **Never re-commit the A1 APPEND.** |

`nextFileId` is always recovered from the latest Paimon snapshot property.

#### 8.2 TabletServer

**DvRocksDB checkpoint** (independent from KvTablet snapshots; recommended after each readable switch so the captured state is consistent with a DV-readable snapshot). Each checkpoint records:

- `restoreSnapshot` — the DV-readable snapshot id at checkpoint time;
- `snapshotStartLogOffset` — that snapshot's changelog start offset;
- `checkpointLogHw` — `log_hw` at checkpoint time. Replay must resume from `checkpointLogHw + 1` so already-processed `-U`/`-D` are not reapplied.

**RowPosIndex is built only by SST Ingest, never by changelog replay** — replay only *deletes* RowPosIndex entries (processing `-U`/`-D`) and updates LakeDv / LogDv / PendingDeletes. So RowPosIndex recovery depends entirely on the checkpoint plus re-Ingesting remote SSTs.

**Recovery steps:**

1. **Load checkpoint**: pull DvRocksDB SSTs from remote and open DvRocksDB. RowPosIndex, FileDict, LakeDv, LogDv, and PendingDeletes now reflect `restoreSnapshot`.

2. **Replay changelog from `checkpointLogHw + 1` (deletes only)**: for each `-U`/`-D`, point-get `RowPosIndex[oldRowId]` — hit → mark LakeDv, delete the entry, write `PendingDeletes[oldRowId] = hit`; miss → write `PendingDeletes[oldRowId] = pending`. Update LogDv only when `oldRowId ≥ snapshotStartLogOffset` (older deletes are already covered by the lake snapshot).

3. **Advance to the current DV-readable snapshot** (post-checkpoint rounds): read the current DV-readable snapshot `S_readable` directly from ZooKeeper (LakeTableZNode). If it is newer than `restoreSnapshot`, query Paimon for the committed COMPACT snapshots between them in commit order `S_1 … S_n = S_readable`; for each `S_i` **in order**:

   - read `rowPos/{S_i}/[{partitionId}/]index.json` and, for this bucket, get its SST file names, `newFileDictEntries`, and `oldFiles`;
   - write `newFileDictEntries` to FileDict (idempotent);
   - download the bucket's SSTs and **Ingest into RowPosIndex in commit order** (later snapshots win for the same RowId via higher sequence numbers — handles compaction rewrites).

   After all SSTs are ingested, set `readableSnapshotId = S_readable` and the corresponding `snapshotStartLogOffset`, then **batch-resolve PendingDeletes** (§4.6 step 2). The changelog replay in step 2 is not repeated; replay is based on `checkpointLogHw`, while the readable snapshot advance is based on the remote RowPos SSTs.

4. **Apply file-lifecycle cleanup**: because each recovered `index.json` carries `oldFiles`, recovery resolves them through FileDict and drops their LakeDv entries after batch resolve, using the same ordering as the normal readable switch. This keeps recovered DvRocksDB state aligned with normal operation and avoids carrying redundant LakeDv entries until the next round.

#### 8.3 CoordinatorServer

The orchestration is reconstructed from LakeTableZNode. For DV-enabled tables, each registered `LakeSnapshotMetadata` carries an explicit `dvPendingReadable` marker:

- `dvPendingReadable = true`: the Paimon/lake snapshot is registered, but the DV prepare barrier has not durably completed;
- `dvPendingReadable = false`: the snapshot is not pending DV readability. For DV snapshots, this means the coordinator has already marked it DV-readable; for non-DV snapshots, the flag is always false.

Ready acks are not recovered across coordinator failover. Passing the prepare barrier becomes durable only when `markLakeTableSnapshotReadable` updates the ZK metadata by flipping `dvPendingReadable` from `true` to `false`. That ZK update is the commit point for DV-readability: if recovery sees `dvPendingReadable = true`, the new coordinator re-runs prepare and collects ready acks again; if recovery sees `dvPendingReadable = false`, it does not infer any pending prepare from `readableOffsetsFilePath`.

On coordinator recovery, the new coordinator scans the registered lake-table snapshots in ZK:

- **`dvPendingReadable = true`**: re-run prepare (idempotent) → mark readable (`dvPendingReadable = false`; `readableOffsetsFilePath` was already persisted at registration) → switch. The per-bucket payload is reconstructed from remote — `readableOffset` from the readable-offsets file, `newFileDictEntries` / `oldFiles` from `index.json` — so the lost in-memory orchestration state is not needed.
- **`dvPendingReadable = false` and readable switch may not have completed**: re-send the readable switch for the latest DV snapshot. Buckets that already switched skip (idempotent); others still hold their `pendingSstPath`.

If multiple snapshots have `dvPendingReadable = true`, the coordinator re-drives the smallest snapshot id first to preserve snapshot order.

> For active re-drive, `readable_bucket_offsets_file_path` must be persisted in ZK at registration, even while `dvPendingReadable = true`; otherwise the new coordinator cannot locate the round's `readableOffset`s.

#### 8.4 Ordering & Idempotency

CoordinatorServer is the single orchestrator and does not start round N+1's prepare before round N's readable switch completes; TabletServer needs no local ordering check. Single-flight per split; retry only after CoordinatorServer explicitly declares the attempt failed; a cancelled attempt sends no further report / ack. Prepare and readable switch are idempotent.

### 9. Data Format & Protocol Changes (summary)

- **KV state value**: `[RowId(8B)][schemaId(2B)][BinaryRow]`.
- **Changelog value**: all four record types carry an 8-byte RowId header; `-U` / `-D` carry the old version's RowId.
- **Paimon data column**: add `__rowid` (BIGINT) for DV tables; compaction must preserve it.
- **Paimon table config**: the underlying Paimon table uses the `DEDUPLICATE` merge engine (Fluss resolves its own MergeEngine semantics and writes resolved full rows before tiering — see the compatibility matrix); `paimon.deletion-vectors.enabled` forced on when Fluss DV is on, independent otherwise.
- **RPC / ZK metadata**: new `GetLakeDvSnapshot` (union read + tiering's LogDv fetch in one call) and `DvReadableSwitch`; prepare reuses `NotifyLakeTableOffsetRequest` with a per-bucket `readable_snapshot_id` + `readable_offset`; commit adds only `earliest_snapshot_id_to_keep` (per-bucket `readableOffset` rides the existing `readable_bucket_offsets_file_path`). ZK `LakeSnapshotMetadata` adds `dvPendingReadable` so coordinator recovery can distinguish "DV prepare pending" from normal non-DV snapshots. Bulk per-bucket metadata (`newFileDictEntries`, `oldFiles`) is not sent over RPC — it lives in `index.json`.

---

## Compatibility, Deprecation, and Migration Plan

- **Opt-in, off by default**: gated by `table.datalake.deletion-vectors.enabled = false`. Existing tables and behavior are unaffected unless a table opts in at creation.
- **No migration of existing tables**: the switch is immutable post-creation; existing primary key tables keep their current tiering and remain on the full sort-merge union read path (correct, just not DV-accelerated). Dynamic enabling is impossible — pre-existing KV state has no embedded RowId, and pre-existing Paimon files have no `__rowid` column, so historical positions cannot be rebuilt.
- **Prerequisite enforcement**: creating a DV table without a primary key, without datalake enabled, or without FULL changelog mode is rejected at creation. When Fluss DV is on, `paimon.deletion-vectors.enabled` is forced on (and rejected if explicitly set off); when Fluss DV is off, the Paimon-side switch is left to the user, exactly as today.
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
- **Per-bucket offset & compaction**: readableOffset computation when some buckets still hold L0; detection of external compaction via snapshot diff; partial/unfinished L0 (must wait for full L0 consumption); only COMPACT snapshots trigger prepare/switch.
- **Tiering pipeline**: two-phase ack barriers; deferred Ingest at readable switch; file-lifecycle LakeDv/PendingDeletes cleanup ordering (after batch resolve); timing-gap and zombie PendingDeletes resolution; orphan cleanup using readableOffset.
- **Failure & recovery**: TieringService failover at the A1/A2 boundary (idempotent re-write, no duplicate alive rows; `nextFileId` from snapshot property); TabletServer recovery from checkpoint + changelog replay + in-order SST re-Ingest, including post-recovery redundant-LakeDv elimination; CoordinatorServer failover at each ZK checkpoint (idempotent prepare / switch).
- **Concurrency**: DvRWLock serialization across `-U/-D`, prepare, readable switch, and union read; union read never observes an advanced `log_hw` with stale DV.
- **Config & validation**: prerequisite rejection at creation; immutability under `ALTER TABLE`; Paimon-side switch forced on only when Fluss DV is on (independent otherwise).

---

## Rejected Alternatives

- **Keep the current full sort-merge at read time (status quo, no DV)**: read all hot changelog plus all Paimon data and deduplicate by primary key on every read. Correct — and what Fluss does today — but expensive: cost scales with full table size, not the delete delta, so read latency degrades as the table grows. Replacing it with DV is the whole point of this FIP. Rejected.

- **Tier without enabling Paimon's native DV (rely on merge-on-read only)**: leaves the Paimon-side historical read paying MOR cost for every delete / update, and still does nothing for real-time cross-layer masking (deletes only visible after the next compaction). Enabling Paimon native DV for the cold layer plus Fluss logical DV for the hot layer is precisely what removes both costs. Rejected.

- **Build positions at write time (treat Paimon like a direct-write lake)**: infeasible — rows in L0 have no stable position until compaction merges them down, and compaction may be performed by an external job outside Fluss's control. Hence positions are built by scanning compaction output (`__rowid`) in Phase A2. Rejected.

- **Bitmap-diff LakeDv cleanup** (track a snapshot of materialized bits and clean via `LakeDv AND NOT materialized`): in Paimon, deletes are not materialized into a Fluss-controlled DV file whose contents we know precisely — compaction replaces whole files. File-lifecycle cleanup (drop the replaced file's entry) matches Paimon's model exactly, needs no per-bit bookkeeping, and is naturally incremental-safe. Rejected in favor of file-lifecycle cleanup.

- **Treat every snapshot as readable**: an APPEND snapshot exposes only L0 files with unstable positions; serving it yields wrong or shifting positions. Only COMPACT snapshots, whose data is settled in base files, are made DV-readable. Rejected.

- **Use tiered offset (not per-bucket readable offset) as the changelog start for union read**: would skip rows still in uncompacted L0 that are invisible in the Paimon readable snapshot, dropping data. The per-bucket readable offset is the safe boundary. Rejected.

- **Serve two snapshots simultaneously to avoid the stale window**: requires maintaining two LakeDv and two RowPosIndex states, doubling write-path point-get cost and complexity. We instead accept a brief (tens-of-ms) stale window handled by client retry, keeping a single-snapshot architecture. Rejected.

- **Single-phase publish-then-switch (skip prepare)**: would include remote SST download in the post-publish stale window and lose the pre-publish liveness check. The two-phase prepare front-loads remote I/O and gates publish on ready acks. Rejected.

- **Eager SST Ingest during prepare**: would require dual position column families (one per snapshot) to keep union read on the old snapshot correct between prepare and switch. Deferring Ingest to readable switch keeps a single RowPosIndex CF. Rejected.

---

## Appendix A: End-to-End Walkthrough

Initial state: Paimon empty; RowPosIndex / LakeDv / LogDv / PendingDeletes empty; no readable snapshot.

**Step 1 — Write 3 records**: `PUT key1,key2,key3` → `+I` at offsets 0,1,2 → RowIds 0,1,2. KV state stores RowId per value.

**Step 2 — First tiering**: split `[0,2]`, logDvSnapshot empty. A1 writes 3 `ADD` rows (with `__rowid`) to L0. External compaction → `file_A` at pos0/1/2. A2 scans `__rowid`, SST `{0→(A,0),1→(A,1),2→(A,2)}`, reports. Prepare downloads SST. Publish S1. Readable switch Ingests SST → RowPosIndex `{0→(A,0),1→(A,1),2→(A,2)}`; LakeDv empty; `readableSnapshotId=S1`.

**Step 3 — Update key1**: `-U(offset=3, oldRowId=0)` + `+U(offset=4)`. §3.2 on `-U`: RowPosIndex[0] hit `(A,0)` → `LakeDv[A]={0}`; delete RowPosIndex[0]; `PendingDeletes[0]=(A,0)`; `LogDv` marks offset 0.

**Step 4 — Union read (S1)**: server returns `lakeDv={A:{0}}, logDv={0}, logEndOffset=4`. Client: Paimon `file_A` minus pos0 → `key2,key3`; changelog `[3,4]` → `+U(key1,v4)`. Result `(key1,v4),(key2,v2),(key3,v3)` ✓.

**Step 5 — Delete key3**: `-D(offset=5, oldRowId=2)` → `LakeDv[A]={0,2}`; delete RowPosIndex[2]; `PendingDeletes[2]=(A,2)`; LogDv marks offset 2.

**Step 6 — Second tiering**: split `[3,5]`. A1 writes `+U(key1,v4)` as ADD to L0, `-D(key3)` as DELETE; `-U(offset=3)` skipped. External compaction → `file_B` holds `(key1,v4)` at pos0; `file_A` rewritten/replaced (key3 deleted, key1's old version removed by the `+U` higher seq, key2 migrated). A2 scans new files, SST includes `{4→(B,0)}` and key2's new position; `oldFiles={file_A}`; reports per-bucket readable offset. Readable switch: Ingest SST; batch resolve PendingDeletes `0` and `2` (now in `oldFiles`/eliminated) ; **oldFiles cleanup drops `LakeDv[file_A]`**; advance `readableSnapshotId=S_compact`. LakeDv empty, PendingDeletes cleaned ✓.

**Step 7 — New writes + union read** demonstrate three-layer cooperation: Paimon DV (compaction-materialized historical deletes) + LakeDv (new unmaterialized delete) + LogDv (untiered increment) jointly produce the exactly-once result.

---

## Appendix B: File Path Conventions

```
{remoteLakeTableSnapshotDir}/
├── metadata/
│   └── {UUID}.offsets                          ← existing
└── rowPos/
    └── {snapshotId}/[{partitionId}/]           ← one directory per (snapshot[, partition])
        ├── index.json                          ← RowPosSstIndex: bucketId → SST [{name, size}] + newFileDictEntries + oldFiles
        └── {bucketId}/                         ← per-bucket SST files
            └── {fileName}.sst
```

`{remoteLakeTableSnapshotDir}` = `{remote.data.dir}/lake/{databaseName}/{tableName}-{tableId}`.

Bulk per-bucket metadata (`newFileDictEntries`, `oldFiles`) lives **only** in `index.json` — the TabletServer reads it from there in both prepare and recovery, so it does not travel over RPC. The RPCs carry only the target DV-readable snapshot id + per-bucket `readableOffset`. SSTs are located purely by the `{snapshotId}` / `{partitionId}` / `{bucketId}` path convention.
