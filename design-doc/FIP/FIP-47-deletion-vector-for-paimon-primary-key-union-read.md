# FIP-47: Introduce Deletion Vectors to accelerate Primary Key Table Union Read over Paimon

## Motivation

Under the lakehouse (Streamhouse) architecture, Fluss serves as the real-time layer while Paimon serves as the historical layer. Fluss continuously tiers real-time data into Paimon, and provides **union read** — combining hot-layer incremental data not yet tiered with historical data already in Paimon, presenting a single, complete table with exactly-once semantics.

Union read works for both log tables and primary key tables today, but at very different cost. Log tables are append-only and need no deduplication. Primary key tables require **cross-layer deduplication** on every read, which Fluss currently performs as a full **sort-merge** between the hot changelog and historical Paimon data — correct, but its cost scales with the table size rather than the delete delta. This FIP accelerates primary key union read by replacing the per-read sort-merge with lightweight deletion vectors.

### Problem 1: Cross-Layer Deduplication for Union Read

For primary key tables, updates and deletes first arrive at Fluss, but older versions of the same row may already have been tiered into Paimon. During union read, the system must precisely mask rows in Paimon that have been updated or deleted on the Fluss side; otherwise stale rows resurface from the historical layer, violating exactly-once semantics.

Consider a row `(key1, v1)` that has been tiered to Paimon. The user then issues `UPDATE key1 → v2`. This update arrives at Fluss as `-U(key1, v1)` + `+U(key1, v2)`; `v2` is still in the Fluss hot layer, while `v1` is already in a Paimon data file. A union read at this moment must return exactly `(key1, v2)` — it must read `v2` from the changelog **and** mask `v1` in Paimon. Today the masking is achieved only by sort-merging the changelog against the full Paimon dataset on every read.

This sort-merge is correct but expensive: it re-reads and re-sorts data proportional to the **full table size** on every query, even when only a handful of rows were updated or deleted since the last tiering. The cost lands entirely on the read path and scales with the table size, not with the delete delta — exactly what a lightweight delete marker should avoid.

### Problem 2: Merge-on-Read Cost on the Paimon Side

Even within the Paimon historical layer alone, resolving updates and deletes on a primary key table requires **merge-on-read (MOR)**: a read must merge overlapping LSM runs and apply the deletes / updates on the fly. As more updates and deletes accumulate between compactions, this merge cost grows and read latency degrades.

Paimon already ships a native mechanism for exactly this — **Deletion Vectors produced during compaction**, which let readers skip deleted rows directly instead of merging runs. This design enables and relies on Paimon's native DV for the cold (historical) layer, so the Paimon-side portion of a union read avoids MOR.

### Goal

Introduce a **three-layer Deletion Vector (DV)** mechanism, integrated with Paimon's native LSM / compaction / DV machinery, that:

1. **Accelerates primary key union read on Paimon** by maintaining lightweight logical delete markers on the Fluss TabletServer side, so union read can **instantly** mask rows in Paimon (and the hot-layer changelog) that have been updated or deleted, without waiting for the next compaction — replacing the per-read full sort-merge with O(delete-delta) bitmap lookups, while preserving exactly-once union read semantics.
2. **Leverages Paimon's native deletion vectors** for the cold layer: deletes are written as Paimon `DELETE` records and resolved by Paimon compaction into deletion vectors, so historical reads skip deleted rows without merge-on-read — and Fluss generates no delete files of its own.

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

#### Shared messages

```protobuf
message PbBucket {
  required int32 bucket_id = 1;
  optional int64 partition_id = 2;
}

message PbLakeDvEntry {
  required string file_path = 1;
  // Serialized Roaring64Bitmap of deleted row positions.
  required bytes deleted_positions_bitmap = 2;
}
```

#### `GetLakeDvSnapshot` *(new; TabletServer; §6 union read)*

Union read gets the LakeDv (per-file deleted-position bitmaps) + LogDv + offsets for the requested snapshot.

```protobuf
message GetLakeDvSnapshotRequest {
  required int64 table_id = 1;
  required int32 bucket_id = 2;
  required int64 readable_snapshot_id = 3;
  optional int64 partition_id = 4;
}

message GetLakeDvSnapshotResponse {
  // LakeDv: per-file deleted position bitmaps (file_path as key, resolved via FileId2Name)
  repeated PbLakeDvEntry lake_dv_entries = 1;
  // LogDv: deleted log offsets bitmap (serialized Roaring64Bitmap)
  optional bytes log_dv_bitmap = 2;
  // The log end offset at snapshot time
  required int64 log_end_offset = 3;
  // The log start offset for this snapshot (snapshotStartLogOffset)
  required int64 snapshot_start_offset = 4;
}
```

DV-enabled tables use a two-phase protocol to advance the readable snapshot: the coordinator first sends `NotifyLakeTableOffset` to let each bucket prepare (download RowPos SSTs), then after all buckets are ready, sends `DvReadableSwitch` to perform the actual switch. Details in §4.3–§4.4.

#### `NotifyLakeTableOffset` — `+ readable_offset` per bucket *(extended; §4.3 prepare)*

The prepare phase reuses the existing `NotifyLakeTableOffsetRequest`; each bucket entry gains a `readable_snapshot_id` and `readable_offset`.

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
  // NEW (DV): the COMPACT snapshot to make DV-readable; locates rowPos/{id}/rowpos.manifest
  // and is set as readableSnapshotId at the switch.
  optional int64 readable_snapshot_id = 8;
  // NEW (DV): base-file coverage; becomes snapshotStartLogOffset (the union-read changelog start).
  optional int64 readable_offset = 9;
}
```

#### `DvReadableSwitch` *(new; CoordinatorServer → TabletServer; §4.4)*

After the coordinator marks a snapshot as DV-readable, it sends this RPC to notify each bucket to perform the readable switch (§4.4).

```protobuf
message DvReadableSwitchRequest {
  required int32 coordinator_epoch = 1;
  required int64 table_id = 2;
  required int64 readable_snapshot_id = 3;
  repeated PbBucket buckets = 4;
}

message DvReadableSwitchResponse {
}
```

### RowPos Index Files (remote lake storage)

For DV-enabled tables, each tiering round persists the `RowId → position` records under the table's remote lake-snapshot directory so any TabletServer can (re)build its RowPosIndex (§4.2). These are durable on-storage formats — a cross-version contract, since recovery reads back files written by earlier versions.

**Path layout:**

```
{remoteLakeTableSnapshotDir}/
├── metadata/
│   └── {UUID}.offsets                          ← existing
└── rowPos/
    └── {snapshotId}/[{partitionId}/]           ← one directory per (snapshot[, partition])
        ├── rowpos.manifest                          ← RowPosSstIndex: bucketId → SST [{name, size}] + newFileId2Name + replacedFiles
        └── {bucketId}/                         ← per-bucket SST files
            └── {fileName}.sst
```

`{remoteLakeTableSnapshotDir}` = `{remote.data.dir}/lake/{databaseName}/{tableName}-{tableId}`.

**`rowpos.manifest`** (`RowPosSstIndex`) — one per snapshot at `rowPos/{snapshotId}/[{partitionId}/]rowpos.manifest`, mapping each participating bucket to its SST file list, the `file_id → file_path` entries (`fileId2Name`) allocated that round, and the `replacedFiles` this round (the `fileId2Name` and `replacedFiles` are added by this FIP so both prepare and recovery read all per-bucket bulk metadata from here rather than over RPC — §4.3 / §8.2):

```json
{
  "version": 1,
  "buckets": {
    "0": {
      "rowPosSstFiles": [ { "name": "sst_0.sst", "size": 12345 } ],
      "fileId2Name": [ { "fileId": 7, "name": "data-abc.parquet" } ],
      "replacedFiles": [ "data-old.parquet" ]
    }
  }
}
```

**RowPos SST** — RocksDB SST files at `rowPos/{snapshotId}/[{partitionId}/]{bucketId}/sst_{N}.sst`, holding sorted `RowId → FilePos` entries:

- **Key** — RowId as an **8-byte big-endian** integer (fixed width, so RocksDB's lexicographic key order equals RowId order, which IngestExternalFile requires).
- **Value** — **FilePos** = `varint(file_id) || varint(row_position)`: two unsigned LEB128 varints, typically 3–5 bytes total. `file_id` is the FileId2Name-encoded id of a Paimon data file; `row_position` is the 0-based row index within that file.
- Entries are sorted by RowId ascending and split across multiple files based on RocksDB's recommended SST size (`sst_0.sst`, `sst_1.sst`, …).

The SST value carries only `file_id`; the `file_id → file_path` mapping is held in the per-bucket **FileId2Name** (a DvRocksDB column family: forward `0x00 + path → BE(file_id)`, reverse `0x01 + BE(file_id) → path`). FileId2Name is populated by reading each snapshot's `rowpos.manifest` `fileId2Name` — during **prepare** for the live round, and during **recovery** for post-checkpoint rounds (§4.3 / §8.2) — and persisted with the DvRocksDB checkpoint. The entries do **not** travel over RPC. A reader resolves a `file_id` (from an SST or a `LakeDv[file_id]`) to a Paimon path via FileId2Name's reverse lookup; union read does this server-side, so `PbLakeDvEntry` is keyed by `file_path`.

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
│ Lake DV  (hot→cold tracking)  │ ───────▶  │   cleaned by file lifecyc│
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
| **RowPosIndex**  | RowId (8B big-endian)| FilePos (varint)                 | Position in the current readable snapshot. Updated only at readable switch (SST Ingest). |
| **LogDv**        | range start (8B BE long) | 32-bit RoaringBitmap         | Time-based partitioning: a new range is opened every `datalake.freshness / N` interval (e.g. N=100). Key = the first offset in that time window; value = bitmap of offsets relative to the range start. Expired ranges (range end < `snapshotStartLogOffset`) are dropped as a whole at readable switch. |
| **LakeDv**       | file_id (4B big-endian) | serialized `Roaring64Bitmap` | Unmaterialized logical deletes for Paimon files: per-file bitmap of deleted row positions. |
| **FileId2Name**     | `0x00`+path / `0x01`+BE(file_id) | BE(file_id) / path bytes | Bidirectional path↔id dictionary in one CF; a 1-byte prefix selects the direction (`0x00` forward path→id, `0x01` reverse id→path). |
| **PendingDeletes**| deleteOffset (8B BE long) | oldRowId (8B)              | Records that a `-U`/`-D` at `deleteOffset` was processed for `oldRowId`. Orphan cleanup is a range delete `[0, readableOffset)`. |

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
3. For each `-U` / `-D` entry (at log offset `deleteOffset`), point-get `RowPosIndex[oldRowId]`:
    - **Hit `(file_id, row_position)`** — the row's Paimon position is known: mark `row_position` in `LakeDv[file_id]` and delete the RowPosIndex entry.
    - **Miss** — the row is currently in the tiering / compaction pipeline (SST not yet Ingested). The next readable switch's batch resolve fills in the LakeDv marker after Ingest.
    - In both cases, write `PendingDeletes[deleteOffset] = oldRowId` and update `LogDv`: mark `offset = oldRowId` as deleted.
4. Release the DvRWLock write lock.
5. Advance `log_hw`; release the KvTablet write lock.

> **Ordering constraint**: DV must be updated and the DvRWLock released **before** `log_hw` advances. Otherwise union read could observe a larger `logEndOffset` while LakeDv is not yet updated, surfacing deleted data.

### 4. Tiering Pipeline (Paimon-specific)

Because Paimon is an LSM store, two facts reshape the pipeline relative to a direct-write lake:

1. **Position is unknown at write time** — newly written rows land in L0; their stable physical position is only known **after compaction** merges them into lower levels. Fluss must **scan the compaction output files** to build RowId → FilePos.
2. **Only `COMPACT` snapshots are DV-readable** — an `APPEND` snapshot contains only L0 files (unstable positions). The prepare → publish → readable-switch flow is triggered **only when a new `COMPACT` snapshot appears**, not on every write.

**CoordinatorServer is the single orchestrator**; the TieringService (Flink job) writes / commits data, waits for external compaction, scans, and reports.

#### 4.1 End-to-End Timeline

```
TieringService (Flink job)        CoordinatorServer            TabletServer (per bucket)
        │                                 │                            │
  A: write changelog → Paimon (buffered)  │                            │
     check for COMPACT snapshot           │                            │
     if found:                            │                            │
       scan __rowid → build RowPos index  │                            │
         files (RocksDB SST), upload      │                            │
         to remote                        │                            │
       commit readable snapshot ────────▶│                            │
     commit APPEND → Paimon & Fluss       │                            │
        │                          B: prepare ──────────────────────▶ download RowPos SST (no lock)
        │                                 │                            write FileId2Name, store path,
        │                                 │                            resolve replacedFiles (write lock)
        │                                 │◀───── ready ack ───────────┤
        │                          [barrier: all ready acks]           │
        │                          C: mark COMPACT DV-readable (ZK)     │
        │                             readable switch ───────────────▶ Ingest SST → RowPosIndex
        │                                 │                            batch resolve PendingDeletes
        │                                 │                            cleanup replacedFiles / LogDv
        │                                 │                            advance readableSnapshotId
        │                                 │◀───── switched ack ────────┤
        │                          [barrier: all switched acks → round complete]
```

Two-phase ack: the **ready ack** gates publishing (pre-publish liveness check + SST prefetch, so the post-publish stale window is local-only); the **switched ack** is observability only (ordering is guaranteed by CoordinatorServer).

#### 4.2 Phase A: Write + Commit

**Split generation (TabletServer)**:

1. Under KvTablet read lock, read `log_hw` as `latest_offset`.
2. Generate split `{offset_range: (last_tiered_offset, latest_offset]}`.

**TieringService write** — for each changelog record:

| Record               | Action in Paimon                                                              |
|----------------------|-------------------------------------------------------------------------------|
| `+I` / `+U`         | write as `KeyValue(key, seq=logOffset, ADD, value)`, with `__rowid = RowId` embedded |
| `-D`                 | write as `KeyValue(key, seq=logOffset, DELETE, null)` |
| `-U`                 | write as `KeyValue(key, seq=logOffset, UPDATE_BEFORE, value)` |

**Before committing**, check for a new COMPACT snapshot:

1. **Detect**: find the latest COMPACT snapshot that covers this round's L0 files; if none, skip to the APPEND commit.
2. **Compute per-bucket readableOffset** (max log offset settled in compacted base files; `≤ tieredOffset`) and `tieredOffsets`.
3. **Collect file changes** across all COMPACT snapshots between the last readable snapshot and the current one: `allNewFiles`, `allReplacedFiles`. (Captures both this round's compaction and any concurrent external/background compaction.)
4. **Scan** each new file's `__rowid` column (projection pushdown — a single long column) to build `RowId → (file_id, row_position)`. Scope is limited to compaction output, not the whole table.
5. **Generate SST + upload**: allocate `file_id`s via `FileIdAllocator`; write a sorted SST (`key=RowId`, `value=fileId+row_position`) per bucket; upload each bucket's SSTs to `{remoteLakeTableSnapshotDir}/rowPos/{snapshotId}/[{partitionId}/]{bucketId}/{fileName}`, and write one `rowpos.manifest` per snapshot at `{remoteLakeTableSnapshotDir}/rowPos/{snapshotId}/[{partitionId}/]rowpos.manifest` (`RowPosSstIndex`: maps each participating `bucketId` to its SST file names + sizes, that round's `newFileId2Name`, and `replacedFiles`).
6. **Commit readable snapshot** to CoordinatorServer: `readableSnapshotId` (= COMPACT snapshot id, which also locates the SSTs / `rowpos.manifest`) and `readable_bucket_offsets_file_path` (the file of per-bucket `readableOffset`).

**Commit APPEND snapshot** to Paimon (tagged with `fluss.tiering` property) and to Fluss (lake-table snapshot with tiered offsets). The APPEND commit happens after the readable snapshot commit, so that recovery can distinguish between the two.

> **FileIdAllocator** is stateless: `nextFileId` is recovered from the Paimon snapshot property `fluss.nextFileId`; the in-memory `pathToFileId` map is for batch dedup only. After restart, the same path may get a different `file_id`, which remains correct since each bucket's RowPosIndex and FileId2Name are self-consistent.

#### 4.3 Phase B: Prepare

**CoordinatorServer** — on receiving the commit (`CommitLakeTableSnapshotRequest`) from TieringService:
1. **Registers the committed lake-table snapshot in ZooKeeper** (so the round survives a coordinator failover).
2. Sends a `NotifyLakeTableOffsetRequest` to the TabletServers hosting the relevant buckets, carrying per bucket `readable_snapshot_id` (the COMPACT snapshot to make readable) and `readable_offset`.
3. Waits for all buckets' **ready acks** — it does **not** mark the snapshot as DV-readable yet (that is §4.4).

**TabletServer** — on receiving the notification, for each of its targeted buckets:
1. **Phase 1 (no lock — pure remote I/O)**: read `rowPos/{snapshotId}/[{partitionId}/]rowpos.manifest` to get the bucket's SST file names, `newFileId2Name`, and `replacedFiles`; download the SSTs from `rowPos/{snapshotId}/[{partitionId}/]{bucketId}/` to a local dir.
2. **Phase 2 (DvRWLock write lock — lightweight)**: write the `newFileId2Name` to FileId2Name (idempotent; a `file_id` mapping to a different path is a bug → fail-fast); store the local SST path as `pendingSstPath` (**no Ingest**); resolve `replacedFiles` to `file_id`s as `pendingOldFileIds`.
3. Reply the **ready ack** to CoordinatorServer.

Prepare modifies no DV state beyond FileId2Name and stored paths, so rollback on failure is trivial (clear the stored path). Processing is idempotent.

> **Why a separate prepare phase** (instead of just publish-then-switch in one step): the costly part of a switch is the **remote SST download** (potentially hundreds of ms). Two phases buy two things:
> 1. **Short stale window** — the download is front-loaded *before* publish, so once published, the readable switch is purely local (Ingest + batch-resolve + cleanup) and the window where clients get stale-snapshot errors shrinks to tens of ms. A single-phase design would put the remote download *inside* that window.
> 2. **Pre-publish liveness check** — CoordinatorServer publishes only after **all** buckets' ready acks. If any bucket is down or cannot fetch its SST, it does not publish, and every bucket keeps serving the old snapshot. A single-phase design would discover the dead bucket only after publish, leaving clients stuck on a snapshot one bucket can't serve.
>
> Rollback is cheap precisely because prepare touches no DV state (no Ingest), as noted above.

#### 4.4 Phase C: Publish + Readable Switch

**CoordinatorServer** — after collecting all buckets' ready acks:
1. Marks the COMPACT snapshot **DV-readable** (updates LakeTableZNode). Clients may now target it.
2. Sends a `DvReadableSwitch` notification to the relevant TabletServers.

**TabletServer** — on receiving the switch notification, under the DvRWLock write lock, for each of its targeted buckets:

1. **Ingest SST → RowPosIndex** (`IngestExternalFile`). The SST contains all compaction-output rows, so RowPosIndex now reflects post-compaction positions. Rewritten rows' new positions overwrite old ones (higher sequence number); rows tombstoned by §3.2 are "resurrected" with the new position and handled by step 2.
2. **Batch resolve PendingDeletes**:
    - **Orphan cleanup**: range delete `PendingDeletes[0, readableOffset)` — all entries with `deleteOffset < readableOffset` are in base files and eliminated by DEDUPLICATE merge.
    - **Resolve remaining**: seek to `readableOffset`, iterate each `(deleteOffset, oldRowId)`:
        - `hit = RowPosIndex.get(oldRowId)`:
            - **Hit** (timing gap / compaction rewrite): `LakeDv[hit.fileId] |= {hit.pos}`; delete `RowPosIndex[oldRowId]`.
            - **Miss**: keep for next round.
3. **Cleanup replacedFiles (file-lifecycle)** — for each `fileId` in `pendingOldFileIds`: delete `LakeDv[fileId]`.
4. **Cleanup expired LogDv** (range end < the new `snapshotStartLogOffset`).
5. **Advance** `readableSnapshotId` and per-bucket `snapshotStartLogOffset = readableOffset`.
6. Clear `pendingSstPath` / `pendingOldFileIds`; release the lock; send the **switched ack** to CoordinatorServer.

> **Why `snapshotStartLogOffset = readableOffset` (not tieredOffset)**: union read fetches changelog from `snapshotStartLogOffset` to supply untiered increments. Using tieredOffset would skip L0 rows not yet visible in the Paimon readable snapshot, dropping data. readableOffset ensures only base-file-visible data is skipped.

### 5. LakeDv Cleanup: File Lifecycle

LakeDv entries are cleaned by **file lifecycle**: when compaction replaces a file, that file's LakeDv entry is dropped as a whole.

- **Trigger**: after each compaction that replaces files (the `replacedFiles` reported in §4.2).
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

Compaction is performed externally (an independent compact job / periodic task, or Paimon's background compaction). TieringService detects the result before committing its APPEND snapshot: if a new COMPACT snapshot has appeared, TieringService processes it (scan → build RowPos → commit readable snapshot to Fluss) before committing the APPEND. The snapshot diff collects all COMPACT file changes between the last readable snapshot and the current one, agnostic to which job produced them.

Paimon snapshot expiration must preserve the current readable snapshot and all files it references.

### 8. Failure Handling & Recovery

#### 8.1 TieringService (stateless)

Recovery hinges on a single boundary: **has the APPEND been committed?** (§4.2). The readable snapshot commit (if a COMPACT was found) happens before the APPEND commit, so both are retried together on failure.

| Failure point | Recovery |
|---------------|----------|
| before APPEND commit (including readable snapshot commit) | **Full retry.** Re-write, re-detect COMPACT, re-build RowPos if needed, re-commit readable snapshot, re-commit APPEND. Paimon's `commitIdentifier` dedups the re-commit; RowPos SST paths are deterministic; the coordinator dedups a re-sent readable snapshot commit. |
| after APPEND commit | **No action needed.** The readable snapshot (if any) was already committed before the APPEND. |

`nextFileId` is always recovered from the latest Paimon snapshot property.

#### 8.2 TabletServer

**DvRocksDB checkpoint** (independent from KvTablet snapshots; recommended after each readable switch so the captured state is consistent with a readable snapshot). Each checkpoint records:

- `restoreSnapshot` — the DV-readable snapshot id at checkpoint time;
- `snapshotStartLogOffset` — that snapshot's changelog start offset;
- `checkpointLogHw` — `log_hw` at checkpoint time. Replay must resume from `checkpointLogHw + 1` so already-processed `-U`/`-D` are not reapplied.

**RowPosIndex is built only by SST Ingest, never by changelog replay** — replay only *deletes* RowPosIndex entries (processing `-U`/`-D`) and updates LakeDv / LogDv / PendingDeletes. So RowPosIndex recovery depends entirely on the checkpoint plus re-Ingesting remote SSTs.

**Recovery steps:**

1. **Load checkpoint**: pull DvRocksDB SSTs from remote and open DvRocksDB. RowPosIndex, FileId2Name, LakeDv, LogDv, and PendingDeletes now reflect `restoreSnapshot`.

2. **Replay changelog from `checkpointLogHw + 1` (deletes only)**: for each `-U`/`-D` at log offset `deleteOffset`, point-get `RowPosIndex[oldRowId]` — hit → mark LakeDv, delete the entry; miss → no-op. In both cases, write `PendingDeletes[deleteOffset] = oldRowId`. Update LogDv only when `oldRowId ≥ snapshotStartLogOffset` (older deletes are already covered by the lake snapshot).

3. **Advance to the current readable snapshot** (post-checkpoint rounds): read the current DV-readable snapshot `S_readable` directly from ZooKeeper (LakeTableZNode). If it is newer than `restoreSnapshot`, query Paimon for the committed COMPACT snapshots between them in commit order `S_1 … S_n = S_readable`; for each `S_i` **in order**:

    - read `rowPos/{S_i}/[{partitionId}/]rowpos.manifest` and, for this bucket, get both its SST file names **and** its `newFileId2Name`;
    - write `newFileId2Name` to FileId2Name (idempotent);
    - download the bucket's SSTs and **Ingest into RowPosIndex in commit order** (later snapshots win for the same RowId via higher sequence numbers — handles compaction rewrites).

   Then set `readableSnapshotId = S_readable` and the corresponding `snapshotStartLogOffset`, replay `-U`/`-D` from `S_n`'s `readableOffset + 1`, and **batch-resolve PendingDeletes** (§4.4 step 2).

4. **Skip replacedFiles LakeDv cleanup** during recovery (no `replacedFiles` payload is delivered). Redundant LakeDv entries pointing at already-replaced files may remain — harmless, since union-read double-marking is idempotent — and are dropped by the next normal round's file-lifecycle cleanup.

#### 8.3 CoordinatorServer

The orchestration is reconstructed from LakeTableZNode, which gives two durable checkpoints per round: **(1)** the snapshot is registered (committed) at commit time as **not yet DV-readable**; **(2)** it is marked **DV-readable** only after all prepare ready acks (§4.4). The crash point relative to these two checkpoints determines recovery:

- **Registered, not yet DV-readable** (crashed any time between commit and mark-readable — e.g. before/during prepare, or just before mark-readable): the round is safe in ZK as a committed-but-not-readable snapshot. The new coordinator finds the not-readable snapshot in ZK (identified by `dvPendingReadable = true`, see below) and re-runs prepare (idempotent) → mark-readable → switch. The per-bucket payload is reconstructed from remote — `readableOffset` from the readable-offsets file, `newFileId2Name` / `replacedFiles` from `rowpos.manifest` — so the lost in-memory orchestration state is not needed.
- **DV-readable** (crashed after mark-readable, before/during switch): the new coordinator reads the `readableOffsetsFilePath` from `LakeSnapshotMetadata` to reconstruct the bucket set (`TableBucket` keys in the offsets file), then re-sends the readable switch — buckets that already switched skip (idempotent); others still hold their `pendingSstPath`.
- **No pending round**: no action; TieringService re-reports or retries.

**`LakeSnapshotMetadata` requires an explicit `dvPendingReadable` flag.** A `null` `readableOffsetsFilePath` is ambiguous: for non-DV tables it is the normal state (the field is not applicable), while for DV tables it means the snapshot is stuck between commit and mark-readable. To let the coordinator identify incomplete DV rounds from ZK data alone — without consulting the table config — `LakeSnapshotMetadata` carries a `dvPendingReadable` boolean:

- DV table registration: `dvPendingReadable = true`, `readableOffsetsFilePath` set (persisted at registration so a new coordinator can reconstruct prepare after failover).
- `markLakeTableSnapshotReadable`: `dvPendingReadable = false`, `readableOffsetsFilePath` unchanged.
- Non-DV table registration: `dvPendingReadable = false` (default).

On startup, the coordinator scans each table's `LakeSnapshotMetadata` list; any entry with `dvPendingReadable = true` triggers active re-drive.

**Latest-readable query must exclude pending snapshots.** The existing `getOrReadLatestReadableTableSnapshot()` uses `readableOffsetsFilePath != null` to find the latest readable snapshot. Since DV pending snapshots now also carry a non-null `readableOffsetsFilePath`, this condition must be tightened to `readableOffsetsFilePath != null && !dvPendingReadable`. Non-DV tables are unaffected (`dvPendingReadable` defaults to `false`).

#### 8.4 System-Level Invariant: Round-Loss Safety

Regardless of which component fails or how the coordinator recovers, a single lost round never causes data loss. This is a structural property of the tiering pipeline, not a recovery mechanism of any specific component:

- Phase A always scans relative to the **last DV-readable snapshot** (§4.2), not the last committed snapshot. If a round is lost (never becomes DV-readable), the baseline does not advance, so the next round naturally re-scans the same range and absorbs the skipped round's data at their current positions.
- Any `-U`/`-D` that arrived during the gap sit in PendingDeletes as `pending` and are resolved at the next switch's batch-resolve.
- The skipped round's `rowPos/{snapshotId}/` files become orphans, reclaimed by GC.

The only cost is deferred readability: data from the lost round is not DV-readable until the next round completes (one tiering cycle).

#### 8.5 Ordering & Idempotency

CoordinatorServer is the single orchestrator and does not start round N+1's prepare before round N's readable switch completes; TabletServer needs no local ordering check. Single-flight per split; retry only after CoordinatorServer explicitly declares the attempt failed; a cancelled attempt sends no further report / ack. Prepare and readable switch are idempotent.

### 9. Data Format & Protocol Changes (summary)

- **KV state value**: `[RowId(8B)][schemaId(2B)][BinaryRow]`.
- **Changelog value**: all four record types carry an 8-byte RowId header; `-U` / `-D` carry the old version's RowId.
- **Paimon data column**: add `__rowid` (BIGINT) for DV tables; compaction must preserve it.
- **Paimon table config**: the underlying Paimon table uses the `DEDUPLICATE` merge engine (Fluss resolves its own MergeEngine semantics and writes resolved full rows before tiering — see the compatibility matrix); `paimon.deletion-vectors.enabled` forced on when Fluss DV is on, independent otherwise.
- **RPC**: new `GetLakeDvSnapshot` (union read) and `DvReadableSwitch`; new shared message `PbBucket`; `NotifyLakeTableOffsetRequest` extended with per-bucket `readable_snapshot_id` + `readable_offset`.

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
- **Paimon write path** (`MergeTreeWriter` DV mode): all four record types written to Paimon (`+I`/`+U` as `ADD` with `__rowid`, `-U` as `UPDATE_BEFORE`, `-D` as `DELETE`, all with seq=logOffset); non-DV mode unchanged. Schema conversion adds `__rowid` only when DV is enabled and rejects a user column named `__rowid`.
- **Per-bucket offset & compaction**: readableOffset computation when some buckets still hold L0; detection of external compaction via snapshot diff; partial/unfinished L0 (must wait for full L0 consumption); only COMPACT snapshots trigger prepare/switch.
- **Tiering pipeline**: two-phase ack barriers; deferred Ingest at readable switch; file-lifecycle LakeDv cleanup; PendingDeletes orphan detection via `deleteOffset < readableOffset`; timing-gap resolution.
- **Failure & recovery**: TieringService failover at the A1/A2 boundary (idempotent re-write, no duplicate alive rows; `nextFileId` from snapshot property); TabletServer recovery from checkpoint + changelog replay + in-order SST re-Ingest, including post-recovery redundant-LakeDv elimination; CoordinatorServer failover at each ZK checkpoint (idempotent prepare / switch).
- **Concurrency**: DvRWLock serialization across `-U/-D`, prepare, readable switch, and union read; union read never observes an advanced `log_hw` with stale DV.
- **Config & validation**: prerequisite rejection at creation; immutability under `ALTER TABLE`; Paimon-side switch forced on only when Fluss DV is on (independent otherwise).

---

## Rejected Alternatives

- **Keep the current full sort-merge at read time (status quo, no DV)**: read all hot changelog plus all Paimon data and deduplicate by primary key on every read. Correct — and what Fluss does today — but expensive: cost scales with full table size, not the delete delta, so read latency degrades as the table grows. Replacing it with DV is the whole point of this FIP. Rejected.

- **Tier without enabling Paimon's native DV (rely on merge-on-read only)**: leaves the Paimon-side historical read paying MOR cost for every delete / update, and still does nothing for real-time cross-layer masking (deletes only visible after the next compaction). Enabling Paimon native DV for the cold layer plus Fluss logical DV for the hot layer is precisely what removes both costs. Rejected.

- **Build positions at write time (treat Paimon like a direct-write lake)**: infeasible — rows in L0 have no stable position until compaction merges them down, and compaction may be performed by an external job outside Fluss's control. Hence positions are built by scanning compaction output (`__rowid`) in Phase A. Rejected.

- **Serve two snapshots simultaneously to avoid the stale window**: requires maintaining two LakeDv and two RowPosIndex states, doubling write-path point-get cost and complexity. We instead accept a brief (tens-of-ms) stale window handled by client retry, keeping a single-snapshot architecture. Rejected.

- **Single-phase publish-then-switch (skip prepare)**: would include remote SST download in the post-publish stale window and lose the pre-publish liveness check. The two-phase prepare front-loads remote I/O and gates publish on ready acks. Rejected.

- **Eager SST Ingest during prepare**: would require dual position column families (one per snapshot) to keep union read on the old snapshot correct between prepare and switch. Deferring Ingest to readable switch keeps a single RowPosIndex CF. Rejected.

---

## Appendix A: End-to-End Walkthrough

This walkthrough traces a primary key table through its full DV lifecycle: initial writes → first tiering + compaction → updates/deletes triggering LakeDv → union read with three-layer DV → second tiering with compaction and cleanup. Each step shows only the logical state changes; implementation details (SST paths, manifests) are covered in §4.2.

### Initial State

| Component | State |
|-----------|-------|
| Paimon | No data files |
| RowPosIndex | empty |
| LakeDv / LogDv / PendingDeletes | empty |
| readableSnapshotId | none |

---

### Step 1: Write 3 Records

```
PUT (key1, v1)  → +I (offset=0)  → RowId=0
PUT (key2, v2)  → +I (offset=1)  → RowId=1
PUT (key3, v3)  → +I (offset=2)  → RowId=2
```

KV state now stores RowId in each value: `key1→[rowId=0][v1]`, `key2→[rowId=1][v2]`, `key3→[rowId=2][v3]`.

DV state unchanged — no deletes, no tiering yet.

---

### Step 2: First Tiering Round

**Split generation**: `offset_range: [0, 2]`.

**TieringService** writes 3 `+I` records to Paimon L0:

| Paimon L0 | RowId (`__rowid`) | Key | Value |
|-----------|-------------------|-----|-------|
| row0 | 0 | key1 | v1 |
| row1 | 1 | key2 | v2 |
| row2 | 2 | key3 | v3 |

Before committing, TieringService checks for a COMPACT snapshot — none exists yet (first round, no prior data to compact). No readable snapshot to commit.

**Commit APPEND snapshot** to Paimon (tagged `fluss.tiering`) and to Fluss. No prepare/switch flow triggered — the DV-readable snapshot remains `none`.

| Component | After Step 2 |
|-----------|-------------|
| Paimon | L0 files with 3 rows (not yet compacted) |
| RowPosIndex | empty — no COMPACT snapshot yet |
| readableSnapshotId | none |

*This step demonstrates: initial write — data lands in Paimon L0, but DV is not yet active because no compaction has occurred.*

---

### Step 3: Update key1

```
PUT (key1, v4)  → -U (offset=3, oldRowId=0) + +U (offset=4)  → new RowId=4
```

At this point, RowPosIndex is still empty (no readable snapshot yet), so the `-U` processing hits a **miss**:
- Point-get `RowPosIndex[0]` → miss
- **PendingDeletes**: write `deleteOffset=3 → oldRowId=0`
- **LogDv**: mark offset=0 as deleted

| Component | State |
|-----------|-------|
| RowPosIndex | empty |
| LakeDv | empty |
| PendingDeletes | `3 → oldRowId=0` |
| LogDv | offset 0 marked deleted |

*This step demonstrates: RowPosIndex miss — the row is in the tiering pipeline (L0, not yet compacted), so PendingDeletes records it for later resolution.*

---

### Step 4: Second Tiering Round (detect compaction)

Between Step 2 and now, an external compact job has run, merging L0 into base files → COMPACT snapshot **S1** with `file_A`:

| Paimon file_A | row position | RowId (`__rowid`) | Key | Value |
|---------------|-------------|-------------------|-----|-------|
| pos0 | 0 | 0 | key1 | v1 |
| pos1 | 1 | 1 | key2 | v2 |
| pos2 | 2 | 2 | key3 | v3 |

**Split generation**: `offset_range: [3, 4]`.

**TieringService** writes the update records to Paimon L0:
- offset=3: `-U(key1,v1)` → written as UPDATE_BEFORE
- offset=4: `+U(key1,v4)` → written as ADD with `__rowid=4`

Before committing, TieringService checks for a COMPACT snapshot — finds **S1**:

1. **Scan** `file_A`'s `__rowid` column → build `RowId → FilePos` mapping.
2. **Generate RowPos SST**: `{0→(A,pos0), 1→(A,pos1), 2→(A,pos2)}`, allocate `file_A → fileId=1` in FileId2Name.
3. **Upload** SST + `rowpos.manifest` to remote.
4. **Commit readable snapshot** (S1) to CoordinatorServer.

**Commit APPEND snapshot** to Paimon & Fluss.

**Prepare** (§4.3): CoordinatorServer sends `NotifyLakeTableOffset`. TabletServer downloads RowPos SST, writes FileId2Name, stores SST path. Sends ready ack.

**Publish + Readable switch** (§4.4): CoordinatorServer marks S1 DV-readable. TabletServer:
1. **Ingest SST** → RowPosIndex: `{0→(A,pos0), 1→(A,pos1), 2→(A,pos2)}`.
2. **Batch resolve PendingDeletes**: entry (deleteOffset=3, oldRowId=0). deleteOffset=3 ≥ readableOffset (the `-U` is not yet in base files) → check RowPosIndex. Hit `(file_A, pos0)` → `LakeDv[file_A] |= {0}`; delete `RowPosIndex[0]`.
3. Advance `readableSnapshotId = S1`.

| Component | After readable switch |
|-----------|----------------------|
| RowPosIndex | `1→(file_A,pos1)`, `2→(file_A,pos2)` — entry 0 resolved by batch resolve |
| LakeDv | `file_A → {0}` — pos0 marked by batch resolve |
| PendingDeletes | `3→oldRowId=0` — unchanged, awaiting orphan |
| readableSnapshotId | S1 |

*This step demonstrates: tiering with compaction detection — new data is written while compaction is detected in the same round. Batch resolve at readable switch handles the timing gap: the `-U` arrived before RowPosIndex existed, so PendingDeletes recorded it for later; after SST Ingest populates RowPosIndex, batch resolve finds the position and marks LakeDv.*

---

### Step 5: Union Read (snapshot S1)

Client requests union read targeting S1.

**TabletServer returns**: `lakeDv = {file_A: {0}}`, `logDv = {offset 0 deleted}`, `logEndOffset = 4`.

**Client-side**:

| Source | Data | DV applied | Result |
|--------|------|-----------|--------|
| Paimon file_A | pos0(key1,v1), pos1(key2,v2), pos2(key3,v3) | Paimon DV: none; LakeDv masks pos0 | key2=v2, key3=v3 |
| Changelog [snapshotStartOffset, 4] | offset=3: `-U`, offset=4: `+U(key1,v4)` | LogDv: offset 0 not in range | -U retracted, key1=v4 output |

**Final result**: `(key1, v4), (key2, v2), (key3, v3)` ✓

*This step demonstrates: union read with two-layer DV — LakeDv masks deleted Paimon rows, changelog provides untiered updates.*

---

### Step 6: Delete key3

```
DELETE (key3)  → -D (offset=5, oldRowId=2)
```

**Deletion processing** (§3.2):
- Point-get `RowPosIndex[2]` → hit `(file_A, pos2)`
- **LakeDv**: `file_A → {0, 2}` (pos2 added)
- **Delete** `RowPosIndex[2]`
- **PendingDeletes**: write `deleteOffset=5 → oldRowId=2`
- **LogDv**: mark offset=2 as deleted

| Component | State |
|-----------|-------|
| RowPosIndex | `1→(file_A,pos1)` — only key2 remains |
| LakeDv | `file_A → {0, 2}` — pos0 and pos2 deleted |
| PendingDeletes | `3→oldRowId=0`, `5→oldRowId=2` |
| LogDv | offset 0, 2 marked deleted |

---

### Step 7: Third Tiering Round (with compaction)

**Split generation**: `offset_range: [5, 5]`.

**TieringService** writes to Paimon L0:
- offset=5: `-D(key3,v3)` → written as DELETE

Before committing, TieringService checks for a COMPACT snapshot. External compaction has run on the previous L0 (Step 4's `-U` and `+U` at offsets 3-4), producing COMPACT snapshot **S2** with `file_B`. Note: the `-D` at offset=5 was just written to L0 in this round and is **not** part of this compaction — key3 is still in the compaction output:

| Paimon file_B | row position | RowId (`__rowid`) | Key | Value |
|---------------|-------------|-------------------|-----|-------|
| pos0 | 0 | 4 | key1 | v4 |
| pos1 | 1 | 1 | key2 | v2 |
| pos2 | 2 | 2 | key3 | v3 |

`file_A` is replaced by compaction (key1's old version superseded by higher-seq `+U`; key2 and key3 migrated). `replacedFiles = {file_A}`. `readableOffset = 4` (offset 5 is still in L0).

1. **Scan** `file_B`'s `__rowid` column → `{4→(B,pos0), 1→(B,pos1), 2→(B,pos2)}`.
2. **Generate RowPos SST**, allocate `file_B → fileId=2`.
3. **Upload** SST + `rowpos.manifest` (with `replacedFiles = [file_A]`).
4. **Commit readable snapshot** (S2) to CoordinatorServer.

**Commit APPEND snapshot** to Paimon & Fluss.

**Prepare** (§4.3): TabletServer downloads RowPos SST, writes `file_B → fileId=2` to FileId2Name, resolves `replacedFiles` → `pendingOldFileIds = {fileId=1}`. Sends ready ack.

| Component | After prepare (before readable switch) |
|-----------|------|
| RowPosIndex | `1→(file_A,pos1)` (unchanged — SST not Ingested) |
| LakeDv | `file_A → {0, 2}` (unchanged) |
| PendingDeletes | `0→deleteOffset=3`, `2→deleteOffset=5` (unchanged) |

**Publish + Readable switch** (§4.4):

1. **Ingest SST** → RowPosIndex: `4→(file_B,pos0)`, `1→(file_B,pos1)`, `2→(file_B,pos2)` added; `1` overwrites old entry (higher seq).
2. **Batch resolve PendingDeletes**:
    - Range delete `PendingDeletes[0, 4)` → deletes entry (3→oldRowId=0). ✓
    - Seek to readableOffset=4, iterate: entry (5→oldRowId=2). deleteOffset=5 ≥ readableOffset=4 → check RowPosIndex. Hit `(file_B, pos2)` → `LakeDv[file_B] |= {2}`; delete `RowPosIndex[2]`.
3. **File-lifecycle cleanup**: `replacedFiles = {file_A}` → delete `LakeDv[file_A]` entirely. ✓
4. **Cleanup expired LogDv** (ranges before new `snapshotStartLogOffset`).
5. **Advance** `readableSnapshotId = S2`.

| Component | After readable switch |
|-----------|----------------------|
| RowPosIndex | `1→(file_B,pos1)`, `4→(file_B,pos0)` — entry 2 deleted by batch resolve |
| LakeDv | `file_B → {2}` — key3 masked by LakeDv (the `-D` is not yet in base files) |
| PendingDeletes | `5→oldRowId=2` — kept, awaiting next compaction |
| LogDv | expired entries cleaned |

*This step demonstrates: compaction rewrite with partial coverage — the `-D` at offset=5 is still in L0 (not compacted), so readableOffset=4 doesn't cover it. Batch resolve correctly keeps PendingDeletes for key3 (deleteOffset ≥ readableOffset), marks LakeDv[file_B] for the migrated position, and cleans only the orphan entry (key1, deleteOffset < readableOffset). LakeDv masks key3 in file_B until the next compaction processes the `-D`.*

---

### Step 8: Union Read (snapshot S2)

Client requests union read targeting S2.

**TabletServer returns**: `lakeDv = {file_B: {2}}`, `logDv = {offset 0, 2 deleted}`, `logEndOffset = 5`.

**Client-side**:

| Source | Data | DV applied | Result |
|--------|------|-----------|--------|
| Paimon file_B | pos0(key1,v4), pos1(key2,v2), pos2(key3,v3) | Paimon DV: none; LakeDv masks pos2 | key1=v4, key2=v2 |
| Changelog [snapshotStartOffset, 5] | offset=5: `-D(key3)` | LogDv: offset 0,2 not in range | -D retracted (key3 already masked by LakeDv) |

**Final result**: `(key1, v4), (key2, v2)` ✓

*This step demonstrates: LakeDv masks key3 in Paimon even though the `-D` hasn't been compacted yet. The changelog `-D` is redundant with the LakeDv mask — key3 is correctly absent from the result.*

---

### Step 9: New Writes + Union Read (S2)

New writes:
```
UPDATE key2 → -U (offset=6, oldRowId=1) + +U (offset=7, key2, v5)  → new RowId=7
INSERT key4 → +I (offset=8, key4, v6)  → RowId=8
```

**Deletion processing** (§3.2) for offset=6 `-U(oldRowId=1)`:
- Point-get `RowPosIndex[1]` → hit `(file_B, pos1)`
- **LakeDv**: `file_B → {1, 2}` (pos1 added to existing {2})
- **Delete** `RowPosIndex[1]`
- **PendingDeletes**: write `deleteOffset=6 → oldRowId=1`
- **LogDv**: mark offset=1 as deleted

| Component | State |
|-----------|-------|
| RowPosIndex | `4→(file_B,pos0)` — only key1 remains |
| LakeDv | `file_B → {1, 2}` — pos1 (key2) and pos2 (key3) both masked |
| PendingDeletes | `5→oldRowId=2`, `6→oldRowId=1` |
| LogDv | offset 1 marked deleted |

**Client union read (snapshot S2)**:

TabletServer returns: `lakeDv = {file_B: {1, 2}}`, `logDv = {offset 1 deleted}`, `logEndOffset = 8`.

| Source | Data | DV applied | Result |
|--------|------|-----------|--------|
| Paimon file_B | pos0(key1,v4), pos1(key2,v2), pos2(key3,v3) | Paimon DV: none; LakeDv masks pos1 and pos2 | key1=v4 |
| Changelog [snapshotStartOffset, 8] | offset=5: `-D`, offset=6: `-U`, offset=7: `+U(key2,v5)`, offset=8: `+I(key4,v6)` | LogDv: offset 1 not in range | -D retracted, -U retracted, key2=v5 and key4=v6 output |

**Final result**: `(key1, v4), (key2, v5), (key4, v6)` ✓

*This step demonstrates: **three-layer DV cooperation** — LakeDv masks file_B's pos1 (key2, new delete) and pos2 (key3, carried from Step 7), changelog + LogDv provides untiered incremental data including the new key4.*

---
