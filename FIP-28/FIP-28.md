# FIP-28: Support Point Lookup on Lake Storage for Expired Partitions

## Motivation

Currently, for Fluss auto-partitioned primary key tables with lake tiering (e.g., Paimon), when Fluss partitions expire, point queries (lookup) only query data from the Fluss cluster. Even though the expired partition data still exists in the lake storage (Paimon), point queries cannot find it. Users see inconsistent results between point queries and batch reads, which is extremely confusing.

This FIP proposes to support point lookup fallback to lake storage for expired partitions. When a partition has expired in Fluss but its data has been tiered to the lake, point queries should be able to retrieve data from the lake storage.

This FIP primarily focuses on Paimon as the lake storage backend, because Paimon provides efficient point lookup capabilities via its `LocalTableQuery` API. Other lake formats such as Iceberg and Lance do not currently offer comparable point query performance, so support for them can be considered in future FIPs as their point lookup capabilities mature.

**Note on writes to expired partitions**: When a partition is expired by the auto-partition mechanism, data cannot be written to it through Fluss anymore. For late-arriving data that targets an expired partition, we recommend users write directly to the corresponding partition in the lake storage (e.g., Paimon) itself.

## Public Interfaces

### New RPC API

A new `LAKE_LOOKUP` API is introduced for lake storage point lookups:

```protobuf
// Lake Lookup request and response
message LakeLookupRequest {
  required int64 table_id = 1;
  repeated PbLakeLookupReqForBucket buckets_req = 2;
}

message LakeLookupResponse {
  repeated PbLakeLookupRespForBucket buckets_resp = 1;
}

message PbLakeLookupReqForBucket {
  optional string partition_name = 1;
  required int32 bucket_id = 2;
  repeated bytes keys = 3;
}

message PbLakeLookupRespForBucket {
  optional int32 error_code = 1;
  optional string error_message = 2;
  optional string partition_name = 3;
  required int32 bucket_id = 4;
  repeated PbValue values = 5;
}
```

### New SPI Interface

`LakeTableLookuper` is added to the `LakeStorage` SPI for performing point lookups against lake storage for expired partitions.

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
     * <p>The returned value bytes should be encoded so the client
     * can correctly decode them.
     *
     * @param key the encoded key bytes to lookup, in the lake storage's native key format
     * @param context the lookup context containing partition and bucket information
     * @return the encoded value bytes, or null if the key is not found
     * @throws Exception if the lookup fails
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;
    
    class LookupContext {
        String partitionName;
        int bucketId;
    }
}
```

### New Lookup Type

`LookupType.LAKE_LOOKUP` is added to distinguish lake lookups from regular lookups.

## Proposed Changes

### 1. Client-Side: Partition-Not-Exist Detection and Lake Fallback

When `PrimaryKeyLookuper.lookup()` encounters a `PartitionNotExistException` during partition ID resolution, it checks whether the table qualifies for lake lookup fallback:
- Datalake is enabled
- Lake format is Paimon (currently the only supported format for lake point lookup; Iceberg support may be added in future as its point lookup capabilities improve)
- Auto-partitioning is enabled

If all conditions are met, it creates a `LakeLookupQuery` instead of returning an empty result. The `LakeLookupQuery` carries `tablePath`, `partitionName`, `bucketId`, and the encoded primary key bytes.

### 2. Client-Side: Request Routing via Hash-Based Server Selection

Since expired partitions have their metadata deleted from Fluss to release cluster metadata pressure, there is no leader for these partitions. The client uses a hash-based routing algorithm to select a tablet server for the lake lookup request:

```
hash = tablePath.hashCode()
hash = hash * 31 + (partitionName == null ? 0 : partitionName.hashCode())
hash = hash * 31 + bucketId
index = (hash & 0x7FFFFFFF) % serverCount
```

This ensures:
- Deterministic routing: the same (table, partition, bucket) always goes to the same server, enabling server-side caching
- Even load distribution across tablet servers

### 3. Client-Side: Request Batching

`LookupSender` groups `LakeLookupQuery` instances by destination server and table path into `LakeLookupBatch`. Each batch is sent as a single `LakeLookupRequest` containing multiple `PbLakeLookupReqForBucket` entries, reducing RPC overhead.

### 4. Server-Side: Asynchronous Lake Lookup Execution

`ReplicaManager.lakeLookups()` handles incoming requests:
- Validates that lake storage is configured (throws `IllegalStateException` if not)
- Obtains (or caches) a per-table `LakeTableLookuper`
- Submits the actual lookup work to a dedicated IO executor thread pool to prevent blocking the RPC thread

**Why a dedicated IO executor?** Unlike regular lookups in Fluss which operate against local RocksDB and are fast with predictable latency, lake lookups involve remote file I/O operations against the lake storage (e.g., reading Paimon data files from remote storage, loading file indexes). These operations can have unpredictable latency due to disk access, file cache misses, or first-time file loading. If executed directly on the RPC thread, slow lake lookups would block the processing of other RPC requests (including regular lookups, writes, etc.), degrading overall cluster performance. By offloading lake lookups to a separate IO executor, we isolate their latency impact and keep the RPC threads responsive for normal operations.

### 5. Paimon Implementation: `PaimonLakeTableLookuper`

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

### 6. Error Handling

- Per-bucket error isolation: if one bucket's lookup fails, other buckets in the same request still succeed
- Response carries per-bucket `error_code` and `error_message`
- Client-side retries follow the same pattern as regular lookups

## Compatibility, Deprecation, and Migration Plan

- This is a new feature with no impact on existing behavior
- Regular (non-expired) partition lookups continue to go through the existing code path
- Lake lookup only activates when a partition is not found in Fluss metadata AND the table has datalake enabled with Paimon format
- The `LakeStorage.createLakeTableLookuper()` has a default implementation that throws `UnsupportedOperationException`, so existing lake storage implementations are unaffected
- No configuration changes are needed; the feature is automatically available for eligible tables

## Test Plan

- Unit tests for `PaimonLakeTableLookuper`: verify point lookup correctness against a local Paimon table
- Unit tests for hash-based routing: verify deterministic server selection and even distribution
- Integration tests: end-to-end test with an auto-partitioned PK table where partitions are expired, verifying that point queries correctly fall back to Paimon and return the expected data

## Rejected Alternatives

### 1. Client-Side Direct Lake Lookup

We considered having the client directly query Paimon lake storage without going through the tablet server. This was rejected because:
- It would require the client to bundle Paimon dependencies, making the client heavier and harder to maintain. As more lake formats are supported in the future, the client would need to bundle dependencies for each format, which does not scale well
- Each client would need to independently load Paimon data files and maintain its own local table query cache, leading to duplicated resource consumption across multiple clients
- By routing through the tablet server, we leverage deterministic hash-based routing to ensure the same (table, partition, bucket) always hits the same server, enabling effective server-side caching of Paimon file indexes and data. Multiple clients benefit from a shared cache on the server
- Server-side execution enables centralized monitoring and observability for lake lookups (e.g., latency, throughput, error rates). With client-side lookups, these metrics would be scattered across all clients, making it much harder to monitor and troubleshoot
