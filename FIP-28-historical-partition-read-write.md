# FIP: Historical Partition Read/Write Support with Historical Partition and Lake Lookup

## Motivation

For auto-partitioned partitioned tables (Primary Key tables and Log tables) with lake tiering enabled, Fluss will expire old partitions from metadata.
After expiration, users face two gaps:

1. **Lookup gap (Primary Key tables)**: point lookup on expired partitions returns `null` in current behavior, because once Fluss determines the partition does not exist, the row is treated as not found, even though data may still exist in lake storage.
2. **Write gap**: late-arriving updates/inserts for expired partitions are rejected.

This is confusing for users because batch/lake reads can still observe historical data while Fluss online paths cannot.

This FIP proposes a unified solution for both read and write:

- read historical partition data through server-side lake lookup fallback (Primary Key tables),
- write historical partition data through one dedicated special Fluss partition (`__historical__`) for both Primary Key and Log tables,
- allow downstream consumers to consume late records of expired original partitions from this partition.

Scope and eligibility:

- This FIP only supports tables with Paimon-enabled lake storage.
- Reason: Primary Key tables require point lookup for expired partitions; non-datalake-enabled tables cannot provide this capability.
- Although Log-table historical writes can technically work without lake lookup, we still do not support non-datalake-enabled tables in this FIP, to keep behavior consistent across table types.

Note: This FIP primarily focuses on Paimon as the lake storage backend, because Paimon provides efficient point lookup capabilities via its `LocalTableQuery` API. Other lake formats such as Iceberg and Lance do not currently offer comparable point query performance, so support for them can be considered in future FIPs as their point lookup capabilities mature.

## Public Interfaces

### New RPC API

Add a lake lookup RPC for server-side fallback:

```protobuf
message LakeLookupRequest {
  required int64 table_id = 1;
  repeated PbLakeLookupReqForBucket buckets_req = 2;
}

message LakeLookupResponse {
  repeated PbLakeLookupRespForBucket buckets_resp = 1;
}

message PbLakeLookupReqForBucket {
  required string partition_name = 1;
  required int32 bucket_id = 2;
  repeated bytes keys = 3;
}

message PbLakeLookupRespForBucket {
  optional int32 error_code = 1;
  optional string error_message = 2;
  required string partition_name = 3;
  required int32 bucket_id = 4;
  repeated PbValue values = 5;
}
```

Why introduce `LakeLookupRequest` instead of reusing `LookupRequest`:

- **Different execution profile**: normal `LookupRequest` targets local RocksDB with predictable latency, while lake lookup is remote/file-based I/O with much higher latency variance and different timeout/backpressure needs.
- **Different routing key**: lake lookup is naturally scoped by `(originalPartition, bucket)` for expired partitions and historical routing; forcing it into normal lookup shape would add ambiguous semantics.
- **Protocol evolution safety**: keeping a dedicated request/response pair allows future lake-specific fields without coupling to regular lookup protocol compatibility.

Extend put-kv RPC for deterministic historical delete routing:

```protobuf
message PbPutKvReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  required bytes records = 3;
  optional string partition_name = 4;
}
```

- `partition_name` carries the original partition before redirecting to `__historical__`.
- This allows server-side deterministic routing for key-only delete (`row == null`) in `__historical__` path.
- Field is optional for backward compatibility and only needed when partition cannot be derived from row payload.

### New SPI Interface

Expose table-level lake point lookup through `LakeStorage`:

```java
public interface LakeStorage {
    // existing methods...

    /**
     * Create a {@link LakeTableLookuper} for the given table to perform point lookups
     * against lake storage for expired partitions.
     */
    default LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
        throw new UnsupportedOperationException(
                "Point lookup is not supported for this lake storage.");
    }
}

/**
 * An interface for performing point lookups against lake storage for expired partitions.
 *
 * <p>Each instance is bound to a specific table and caches per-table resources
 * (e.g., catalog connections, table metadata) for efficient repeated lookups.
 *
 * <p>The key bytes passed to {@link #lookup} are already encoded in the lake storage's
 * native format (e.g., Paimon BinaryRow format) by the client-side encoder, so
 * implementations can use them directly without re-encoding.
 */
public interface LakeTableLookuper {
    /**
     * Lookup a single key from lake storage for an expired partition.
     *
     * <p>The key bytes are already encoded in the lake storage's native key format
     * by the client-side encoder (e.g., Paimon BinaryRow format). Implementations
     * can wrap them directly as the lake storage's key type without decode/re-encode.
     *
     * <p>The returned value bytes should be encoded with a schema ID prefix
     * so the client can correctly decode them.
     *
     * @param key the encoded key bytes to lookup, in the lake storage's native key format
     * @param context the lookup context containing partition and bucket information
     * @return the encoded value bytes, or null if the key is not found
     * @throws Exception if the lookup fails
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    class LookupContext {
        /** Original partition spec resolved from Fluss. */
        ResolvedPartitionSpec partitionSpec;
        /** Bucket id used by lake lookup for this table partition. */
        int bucketId;
        /** Schema id used to encode value bytes returned to Fluss-compatible format. */
        int schemaId;
    }
}
```

### New Lookup Type

Add `LookupType.LAKE_LOOKUP` to distinguish this path from normal local lookup.

## Proposed Changes

### A. Historical Partition Write Path (Log + Primary Key Table)

#### A.1 Historical Partition

For each eligible table, maintain one special partition (name `__historical__`) as the write target for the all expired partitions. In this FIP we call it **Historical Partition**.

- always writable,
- not auto-expired,
- normal replication/WAL behavior.

When a write targets an expired original partition, client redirects it to the historical partition; original partition identity remains in row partition columns. This applies to both Primary Key writes and Log appends.

#### A.2 Log-table write path (client -> server)

Client-side handling:

1. Producer resolves target partition from record partition columns.
2. If target partition exists, follow normal log write path.
3. If target partition is expired, client rewrites destination partition to `__historical__` partition while keeping row data unchanged.
   - Detection mechanism: client checks partition existence through metadata/update path; missing partition is surfaced as `PartitionNotExistException`, then redirected.
4. Client computes the destination bucket within `__historical__` partition:
   - **Sticky strategy (no bucket key):** reuse normal sticky behavior; keep writing to current sticky bucket and rotate to next bucket when sticky window is switched.
   - **Bucket-key strategy:** compute bucket directly from `bucketKeyBytes` (reuse already encoded bytes).
5. Record is sent as a normal log append request to the selected `__historical__` bucket leader.

Concrete bucket-key routing code:

```java
byte[] bucketKeyBytes = record.getBucketKey(); // already encoded by existing bucket-key encoder
int bucketId = bucketAssigner.assignBucket(bucketKeyBytes, cluster);
```

Rationale: for Log tables, the main reason is consistency with Primary Key bucket strategy in `__historical__` (and existing bucket-key semantics). Primary Key tables need to bucket by bucket key; the detailed reason is described in the Primary Key section below. Trade-off: hotspots may increase for same bucket keys across different original partitions.

Server-side handling:

1. `__historical__` partition leader appends the record to `__historical__` log with normal replication and ACK semantics.
2. Row payload still contains original partition columns, so no extra envelope field is required.
3. Downstream consumers subscribe to `__historical__` buckets and consume these records through standard log consumption path.
4. Consumer can recover original partition identity from row partition columns.

This keeps log-table historical writes fully compatible with existing producer/consumer protocol and preserves partition semantics in data payload.

Offset Continuity:

When a partition expires, both its metadata and log data are deleted, so the original partition's offsets are gone. However, **offset continuity between the original partition and the `__historical__` partition is not required**.

**Key assumption**: By the time a write arrives for an expired partition and is redirected to the `__historical__` partition, all data from the original partition has already been fully consumed by subscribers (either from Fluss before expiration or from the lake after tiering). The `__historical__` partition only captures NEW late-arriving data after the partition has expired.

**Therefore**:
- Consumers subscribe to `__historical__` buckets and start from offset 0.
- The original partition is derived from the partition columns in row data.
- There is no need to track or map offsets between the original partition and the `__historical__` partition.


#### A.3 Primary Key-table write path (client -> server)

#### A.3.1 Historical Partition Write Flow

Client-side handling:

1. Upsert writer / delete writer resolves target partition from row partition columns.
2. If target partition exists, follow normal Primary Key write path.
3. If target partition is expired, client rewrites destination partition to `__historical__` and keeps row payload unchanged.
   - Detection mechanism: client checks partition existence through metadata/update path; missing partition is surfaced as `PartitionNotExistException`, then redirected.
4. Client computes bucket:
   - compute destination `__historical__` bucket directly from `bucketKeyBytes`.
5. Record is sent to the selected `__historical__` bucket leader.
6. For redirected PK writes, client carries `partition_name` in put-kv bucket request metadata. This is required for deterministic routing of key-only deletes (`row == null`).

Concrete bucket computation:

```java
byte[] bucketKeyBytes = record.getBucketKey(); // already encoded by existing bucket-key encoder
int bucketId = bucketAssigner.assignBucket(bucketKeyBytes, cluster);
```

Rationale: this preserves bucket-key alignment between online writes and union-read planning for Primary Key tables. Using `originalPartitionName + bucketKey` composite routing would improve hotspot distribution, but would force wider fan-out when union reading historical data from specific Paimon buckets.

Server-side handling:

1. `__historical__` partition leader receives write and enters normal PK write pipeline.
2. For Primary Key tables, PK encoding excludes partition columns. Different original partitions can produce identical key bytes:
   - `dt=2020,id=1` -> `encode(id=1)`
   - `dt=2019,id=1` -> `encode(id=1)`
3. To avoid collision and wrong old-value resolution, `__historical__` partition stores KV state per original partition:
   - one `KvPreWriteBuffer` per original partition,
   - one RocksDB CF per original partition (`{partitionName}`).
4. Here, a **per-partition write context** means one isolated write-state unit per original partition inside `__historical__`:
   `originalPartition -> {in-memory write buffer + partition-scoped rocksdb CF}`.
   This is the core mechanism that prevents cross-partition key collision and wrong old-value resolution.
5. Extract original partition from row partition columns.
6. Resolve per-partition write context:
   - if context exists: reuse existing `{buffer, CF}`,
   - if context does not exist: create partition-scoped buffer and rocksdb CF, then register it.
7. Use that partition context as the active write target:
   - upsert -> merge and write to partition buffer,
   - delete -> delete from partition buffer,
   - flush -> partition buffer flushes into its own CF.
8. For Primary Key historical writes, when old-value fallback needs lake lookup, execute that remote lookup on a dedicated IO executor (instead of the write/apply thread) and then continue write processing with returned result.
Note: unlike normal non-historical put flow, when local old value is absent this path can fallback to point lookup from underlying lake storage; detailed lookup behavior is described in Section B.

Flow sketch:

```text
Incoming PK record on __historical__
        |
        v
Extract original partition (from row partition columns)
        |
        v
Resolve or create partition-scoped write context
        |
        +--> partition in-memory KvPreWriteBuffer
        +--> partition local RocksDB CF
        |
        v
Process upsert/delete with partition-scoped state
        |
        v
Old-value lookup: partition in-memory state -> partition local state -> lake
```

Old-value resolution chain:

1. partition-specific prewrite buffer,
2. partition-specific RocksDB CF,
3. lake fallback.

Execution note:

- Step 3 (lake fallback) runs on a dedicated IO executor for Primary Key historical writes, so unpredictable remote lake I/O latency does not block write/apply threads.

Delete special case:

- if delete record has `row == null`, partition is unknown in payload;
- in `__historical__` path this is unsafe if no partition hint is provided (same PK bytes may exist in multiple original partitions);
- key-only delete is supported when client carries `partition_name` in put-kv request so server can route to one deterministic partition context.

#### A.3.2 Why per-partition CF instead of encoding partition into key

Two options were considered for Primary Key writes in `__historical__`:

1. keep one shared CF and encode partition information into storage key,
2. keep key encoding unchanged and isolate state by per-partition CF.

This FIP chooses per-partition CF for the write path because:

- **Lower migration risk**: key codec remains unchanged, avoiding cross-cutting changes in put/get/delete/replay/snapshot compatibility.
- **Partition lifecycle management**: per-partition CF can be dropped independently after tiering completion.
- **Conceptual alignment**: routing unit is original partition, so local state isolation follows the same unit.

Trade-off:
- CF cardinality can grow with active historical partitions and requires operational guardrails.

#### A.3.3 Per-partition cleanup after tiering sync

When tiering service has fully synchronized one original partition's data from `__historical__`, we can clean its local write state immediately:

- drop the partition's in-memory write buffer,
- drop the partition's RocksDB CF.

Cleanup decision uses a per-partition end offset:

1. maintain `partitionEndOffset[partition]` that tracks the latest log offset written for that partition in `__historical__`,
2. tiering reports/observes synced log offset,
3. when `tieredOffset >= partitionEndOffset[partition]`, the partition is eligible for cleanup.

This keeps local state bounded and avoids retaining obsolete per-partition contexts after data is durably available in lake storage.

#### A.3.4 `__historical__` partition recovery flow

1. RocksDB opens and restores existing per-partition dynamic CF handles.
2. `KvRecoverHelper` first replays from recover offset to **high watermark** (`FetchIsolation.HIGH_WATERMARK`) and applies records directly to persistent KV state:
   - route by original partition and write into that partition's CF.
3. After high-watermark replay, update tablet flushed offset (and row count when enabled), so durable KV state is aligned with acknowledged log.
4. `KvRecoverHelper` then replays from high watermark to **log end** (`FetchIsolation.LOG_END`) into prewrite buffer state:
   - route by original partition and write into that partition's prewrite buffer.
5. During replay, each record carries original partition routing information so partition isolation is preserved end-to-end; auto-increment range is also advanced consistently with replayed records.

This preserves per-partition isolation and ensures restart consistency for old-value resolution and changelog generation.

#### A.3.5 Ordering caveat and assumption for TTL scenarios

- There is a potential changelog gap if a job has not finished consuming an original partition's changelog when that partition is TTL-expired, and new writes are then redirected to `__historical__`.
- In that case, the job may start consuming `__historical__` as a newly discovered partition while still missing the tail of the expired partition's old changelog.

Current assumption (explicitly accepted in this FIP):

- For TTL-expired partition scenarios, jobs are expected to have consumed historical partition changelog before new writes are redirected to `__historical__`.
- In other words, `__historical__` is treated as the post-expiration append path, not as a strict continuity stream with pre-expiration partition offsets.


### B. Historical Partition Point Lookup Path (Primary Key)

#### B.1 Client-side fallback

When `PrimaryKeyLookuper` cannot resolve partition (partition expired):

- compute `bucketId` by following the same `__historical__` write bucketing strategy (bucket-key based),
- route lookup request to the leader of `__historical__` partition + `bucketId`,
- build lake lookup query with `(tablePath, originalPartition, bucketId, keyBytes)`.
- `LookupSender` groups `LakeLookupQuery` instances by destination server and table path into `LakeLookupBatch`. Each batch is sent as a single `LakeLookupRequest` containing multiple `PbLakeLookupReqForBucket` entries, reducing RPC overhead.

#### B.2 Server-Side: Asynchronous Lake Lookup Execution

`ReplicaManager.lakeLookups()` handles incoming requests:

- Validates that lake storage is configured (throws `IllegalStateException` if not)
- Obtains (or caches) a per-table `LakeTableLookuper`
- Submits the actual lookup work to a dedicated IO executor thread pool to prevent blocking the RPC thread

**Why a dedicated IO executor?** Unlike regular lookups in Fluss which operate against local RocksDB and are fast with predictable latency, lake lookups involve remote file I/O operations against the lake storage (e.g., reading Paimon data files from remote storage, loading file indexes). These operations can have unpredictable latency due to disk access, file cache misses, or first-time file loading. If executed directly on the RPC thread, slow lake lookups would block the processing of other RPC requests (including regular lookups, writes, etc.), degrading overall cluster performance. By offloading lake lookups to a separate IO executor, we isolate their latency impact and keep the RPC threads responsive for normal operations.

#### B.3 Paimon Implementation: `PaimonLakeTableLookuper`

The Paimon-specific implementation uses Paimon's `LocalTableQuery` for efficient point lookups:

**Lazy initialization:**
- Resources (Catalog, FileStoreTable, LocalTableQuery, etc.) are lazily initialized on first lookup

**Per-(partition, bucket) file refresh caching:**
- Maintains a `Set<Tuple2<String, Integer>> refreshedBuckets` to track which (partitionName, bucketId) pairs have had their files loaded
- On first lookup for a new (partition, bucket), scans Paimon for the data files and calls `tableQuery.refreshFiles()`
- Subsequent lookups for the same (partition, bucket) skip the scan

**Lookup flow:**
1. The key bytes are already encoded in Paimon's BinaryRow format by the client-side `PaimonKeyEncoder`, so directly wrap them as a Paimon `BinaryRow` via `keyRow.pointTo(MemorySegment.wrap(key), 0, key.length)` — no decode/re-encode needed
2. Call `tableQuery.lookup(partition, bucketId, keyRow)`
3. If found, wrap Paimon result as Fluss `InternalRow` via `PaimonRowAsFlussRow` adapter
4. Encode value using `CompactedRowEncoder` + `ValueEncoder.encodeValue(binaryRow)`

## Compatibility, Deprecation, and Migration Plan

- Additive change for eligible auto-partitioned tables with Paimon lake storage enabled.
- No behavior change for non-expired partition read/write paths.
- No behavior change for tables that never hit `__historical__` path.
- Lake lookup fallback only triggers for Primary Key tables when partition metadata is absent.
- Existing `LakeStorage` implementations remain compatible via default `UnsupportedOperationException` for unsupported point lookup.
- No deprecation is introduced in this FIP.
- No data migration is required; this is a forward-compatible protocol and behavior extension.

Rolling upgrade behavior:

- **Old client -> New server**
  - Request compatibility: old client does not send `PbPutKvReqForBucket.partition_name`; new server accepts it because the field is optional.
  - Behavior: normal writes and non-historical deletes are unchanged.
  - Limitation: for writes on `__historical__` path, if `partition_name` is not set, new server rejects the request to avoid ambiguous partition routing.

- **New client -> Old server**
  - Protocol compatibility: unknown protobuf field `partition_name` is ignored by old server.
  - Lake lookup path: no impact; behavior stays the same.
  - Write `__historical__` path: not allowed. Old server does not have the new Primary Key historical write behavior (including correct old-value lookup/changelog `-U/+U` semantics), so writing historical partition data to old server can produce incorrect results.
  - Client requirement: new client must perform server capability gating (e.g., API version check) and must NOT send any `__historical__` write to old servers.
  - If capability is not available, client should fail fast and block the historical write path.

## Test Plan

- **Integration tests**
  - write to expired partition redirected to `__historical__` partition.
  - update on historical key generates `UPDATE_BEFORE + UPDATE_AFTER` by retrieving old value from lake.
  - log-table write to expired partition is redirected to `__historical__` partition and can be consumed downstream from `__historical__` buckets.
  - look up from an expired partition can still get the value correctly
  - restart/recovery preserves behavior.

## Rejected Alternatives

N/A
