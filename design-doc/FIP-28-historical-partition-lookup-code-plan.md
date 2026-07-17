# FIP-28 Historical Partition Lookup Code Plan

## Goal

This plan implements the first FIP-28 milestone: point lookup on expired
auto-partitions for Paimon lake-enabled primary-key tables.

The end-to-end behavior is:

1. A primary-key lookup targets an original partition name that is no longer in
   Fluss metadata.
2. The client verifies that the missing partition is an expired auto-partition
   of an eligible table.
3. The client asks the coordinator to resolve or lazily create the corresponding
   `__historical__` system partition through a read-authorized internal path.
4. The client sends the lookup to the `__historical__` partition bucket and
   includes the original partition name in the lookup RPC.
5. The tablet server detects the historical lookup and uses Paimon
   `LocalTableQuery` to query lake storage.
6. The server returns the value in the same encoded format used by normal KV
   lookup responses.

## Scope

In scope:

- Primary-key table lookup only.
- Auto-partitioned, Paimon lake-enabled tables only.
- Expired original partition lookup fallback to Paimon lake storage.
- Controlled lazy creation of the corresponding `__historical__` system
  partition because the lookup still needs a routable Fluss bucket.
- Server-side offload of lake lookup work to the existing tablet-server
  `ioExecutor`.

Out of scope for this milestone:

- Historical writes to expired partitions.
- Log table historical writes.
- Local historical RocksDB, tombstones, WAL replay, and local-first lookup for
  not-yet-tiered late writes.
- Prefix lookup on expired partitions.
- `insertIfNotExists` for expired historical partitions.
- Historical lookup support for Iceberg, Lance, or Hudi.
- Client-side historical inflight ratio and new backoff policies. The first
  version uses the existing lookup retry path plus server `ioExecutor`
  isolation.

## Reference Branch Notes

The `history-partition-support` branch is not a direct implementation of this
FIP milestone. Its useful reference points are the existing lake/tiering style:

- Keep lake-format-specific code under the lake modules.
- Expose lake behavior through common SPI instead of hard-coding Paimon in
  server logic.
- Reuse current Paimon scan, partition conversion, and row adapter utilities
  where possible.

The plan below follows those patterns but does not copy the branch structure.

## Design Decisions For The First Milestone

### Keep The Lookup Target As `__historical__`

Even if this milestone only reads from lake storage, the client still needs a
valid `TableBucket` to route the request to a tablet-server leader. For a
partitioned table, that means a partition id must exist. Therefore the client
resolves the `__historical__` partition before sending the lookup.

For multi-key partitions, replace the auto-partition key value with
`__historical__` and keep other partition values unchanged. Example:

```text
partition keys: [region, dt]
auto key: dt
original partition: us$20200101
historical partition: us$__historical__
```

### Create Historical Partitions Through A Read Path

Historical lookup is a read operation. Do not route its first lookup through
the public `Admin.createPartition` API, because that API is intentionally
guarded by table WRITE permission. A user who is allowed to read a table should
not need WRITE permission just because the system partition has not been
materialized yet.

Instead, add a coordinator-owned historical partition resolver RPC or equivalent
internal coordinator path. It accepts the original partition name, validates
that it is an expired auto-partition of an eligible table, computes the
corresponding `__historical__` partition spec, creates that system partition
idempotently if missing, and returns the partition id. This path is authorized
with table READ permission and must not accept arbitrary user-provided partition
specs.

### Do Not Implement Local Historical State Yet

The FIP's final lookup chain is:

```text
prewrite buffer -> historical RocksDB -> lake
```

This milestone has no historical write path, so there is no local historical
state to check. Implement the server path as:

```text
historical lookup request -> lake lookup
```

When historical writes are implemented later, replace the direct lake lookup
call with the full local-first chain while keeping the RPC field and client
routing unchanged.

### Treat `insertIfNotExists` As Unsupported For Expired Partitions

`insertIfNotExists` turns a missing lookup into a write. Supporting it on an
expired partition requires the historical write path. In this milestone, if a
lookup is routed to historical and `insertIfNotExists` is enabled, complete the
lookup exceptionally with `UnsupportedOperationException` or a Fluss API
exception with an explicit message.

## Step 1: Common Historical Partition Utilities

Add shared utilities in `fluss-common`, preferably in `PartitionUtils` or a new
small utility class next to it.

Required constants:

```java
public static final String HISTORICAL_PARTITION_VALUE = "__historical__";
```

Required helpers:

- `boolean isHistoricalPartitionName(TableInfo tableInfo, String partitionName)`
- `ResolvedPartitionSpec toHistoricalPartitionSpec(TableInfo tableInfo, String originalPartitionName)`
- `boolean isExpiredAutoPartition(TableInfo tableInfo, String partitionName, Instant now)`
- `Optional<Integer> getAutoPartitionKeyIndex(TableInfo tableInfo)`

The expired partition predicate must be evaluated from the rule, not by
intuition:

1. Check the table is partitioned and auto-partition is enabled:
   `tableInfo.isAutoPartitioned()`.
2. Check the table is lake-enabled:
   `tableInfo.getTableConfig().isDataLakeEnabled()`.
3. Check the lake format is Paimon:
   `tableInfo.getTableConfig().getDataLakeFormat().orElse(null) == DataLakeFormat.PAIMON`.
4. Parse `partitionName` against `tableInfo.getPartitionKeys()`.
   The parser should validate that the number of partition values exactly
   matches the number of partition keys. Do not rely on the current permissive
   `String.split` behavior alone.
5. Locate the auto-partition key:
   use `autoPartitionStrategy.key()` when set, otherwise use the first
   partition key, matching existing `validateAutoPartitionTime` behavior.
6. Extract the auto-partition value at that key index.
7. Check that the value matches the configured auto-partition time format.
8. Compute the earliest retained partition value using:

   ```java
   ZonedDateTime current =
       ZonedDateTime.ofInstant(now, autoPartitionStrategy.timeZone().toZoneId());
   String earliestRetained =
       generateAutoPartitionTime(
           current,
           -autoPartitionStrategy.numToRetain(),
           autoPartitionStrategy.timeUnit());
   ```

9. The partition is expired only if:

   ```java
   earliestRetained.compareTo(autoPartitionValue) > 0
   ```

The metadata existence condition is checked by the caller. The normal flow is:
try to resolve the original partition; only after `PartitionNotExistException`
run this predicate.

Tests:

- Add unit tests in `PartitionUtilsTest`.
- Cover single partition key and multi partition key.
- Cover eligible expired partition.
- Cover invalid partition name.
- Cover future/current retained partitions.
- Cover non-Paimon lake format and non-lake tables.

## Step 2: Add A Controlled Historical Partition Resolver Path

The existing public coordinator create-partition path is a WRITE operation. It
also rejects old auto partitions through `validateAutoPartitionTime` and rejects
`__historical__` through normal identifier validation. Do not use that public
path for lookup-time historical partition creation.

Add a coordinator-owned resolver path for historical lookup. It is internal to
the Fluss client/server implementation, not a new public `Admin` API.

Files:

- `fluss-rpc/src/main/proto/FlussApi.proto`
- coordinator gateway/message wiring for the new internal RPC
- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorService.java`
- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/AutoPartitionManager.java`
- common partition utility tests

Recommended RPC shape:

```protobuf
message ResolveHistoricalPartitionRequest {
  required PbTablePath table_path = 1;
  required string original_partition_name = 2;
}

message ResolveHistoricalPartitionResponse {
  required int64 partition_id = 1;
  required string historical_partition_name = 2;
  required PbPartitionSpec historical_partition_spec = 3;
}
```

Implementation:

1. Add an internal `resolveHistoricalPartition` coordinator RPC and call it
   from the client historical partition resolver.
2. Authorize this RPC with table READ permission, not WRITE permission.
3. Do not accept a user-provided target partition spec. The request contains
   only the original partition name.
4. Load the table metadata and evaluate
   `isExpiredAutoPartition(tableInfo, originalPartitionName, now)` before any
   metadata mutation.
5. Compute the historical partition spec with
   `toHistoricalPartitionSpec(tableInfo, originalPartitionName)`.
6. Validate the computed system partition spec with a system-aware branch:
   - it contains exactly the table partition keys,
   - only the auto-partition key value is `__historical__`,
   - ordinary non-auto partition values pass normal partition value rules,
   - the table is auto-partition enabled,
   - the table is lake-enabled,
   - the lake format is Paimon,
   - `validateAutoPartitionTime` is skipped for the computed system partition.
7. Create the historical partition idempotently if it is missing. Treat
   concurrent creation by another client as success.
8. Return the historical partition id and spec. The client refreshes metadata
   after receiving the response.
9. Keep the public `CoordinatorService.createPartition` path unchanged unless
   the implementation needs an explicit guard that rejects `__historical__`
   from normal user create-partition requests.
10. In `AutoPartitionManager.dropPartitions`, skip historical partition names
   before applying TTL comparison. This is defensive for system-created
   historical partitions.

Tests:

- Coordinator unit or integration test that resolving an eligible expired
  partition creates the corresponding `__historical__` partition with READ
  permission.
- Test that the resolver fails for a non-lake or non-Paimon table.
- Test that the resolver fails for current, future, malformed, or non-auto
  partition names without creating metadata.
- Test that normal public create-partition still requires WRITE permission.
- Test that auto partition expiration does not drop `__historical__`.

## Step 3: Extend Lookup RPC

Add the original partition name to lookup requests.

File:

- `fluss-rpc/src/main/proto/FlussApi.proto`

Change:

```protobuf
message PbLookupReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  repeated bytes keys = 3;
  optional string partition_name = 4;
}
```

Then regenerate RPC classes:

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

Compatibility:

- Old clients omit `partition_name`; new servers treat the request as a normal
  lookup.
- New clients send `partition_name` only for historical lookups.

## Step 4: Carry Historical Lookup Metadata Through Client Batching

Current client batching is keyed by `TableBucket`. That is not enough because
multiple expired original partitions can map to the same `__historical__`
bucket. A single `PbLookupReqForBucket` can carry only one `partition_name`.

Files:

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupQuery.java`
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/AbstractLookupQuery.java`
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupBatch.java`
- `fluss-client/src/main/java/org/apache/fluss/client/lookup/LookupSender.java`
- `fluss-client/src/main/java/org/apache/fluss/client/utils/ClientRpcMessageUtils.java`

Implementation:

1. Add `@Nullable String partitionName` to lookup queries.
   This field is the original partition name, not the `__historical__`
   partition name.
2. Add a `LookupBatchKey` containing:
   - `TableBucket tableBucket`,
   - `@Nullable String partitionName`.
3. For normal lookups, `partitionName` is null.
4. For historical lookups, `partitionName` is the expired original partition.
5. Batch normal lookups by table bucket as before.
6. Batch historical lookups by `(historical table bucket, original partition name)`.
7. Change `LookupSender` so historical lookup dispatch is keyed by
   `LookupBatchKey`, not by `TableBucket` alone.
8. Ensure one generated `PbLookupReqForBucket` has at most one
   `partition_name`.
9. Do not send two bucket request entries for the same `TableBucket` but
   different `partition_name` in the same `LookupRequest`. The first
   implementation should split those historical groups into separate RPCs.
   This keeps the existing response format valid because response buckets are
   identified only by `TableBucket`.
10. Set `PbLookupReqForBucket.partition_name` only when the batch key has a
   non-null original partition name.

This step prevents this bug:

```text
original partitions: 20200101, 20200102
historical partition: __historical__
same bucket id: 3
```

If both were mixed into one bucket request, the server would apply one
`partition_name` to keys from both original partitions and return incorrect
results.

Tests:

- Extend `LookupSenderTest` to assert historical lookups for different original
  partition names and the same historical `TableBucket` are emitted as separate
  RPCs.
- Extend `LookupSenderTest` to assert historical lookups with different
  `TableBucket`s can still be batched without losing their `partition_name`.
- Extend `ClientRpcMessageUtilsTest` to assert `partition_name` is set only for
  historical lookup batches.

## Step 5: Client Historical Partition Resolver

Add a small resolver used by primary-key lookup. It should be connection-scoped
or lookup-client-scoped, not constructed per lookup.

Recommended location:

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/HistoricalPartitionResolver.java`

Recommended dependencies:

- `MetadataUpdater`
- coordinator gateway/client used by `MetadataUpdater`

Recommended wiring:

- `FlussConnection.getOrCreateLookupClient()` should construct `LookupClient`
  with a `HistoricalPartitionResolver`.
- `FlussTable.newLookup()` passes the existing connection-scoped lookup client.
- `TableLookup` and `PrimaryKeyLookuper` receive the resolver through
  `LookupClient` or constructor injection.

Resolver behavior:

1. Input: `TableInfo tableInfo`, `String originalPartitionName`.
2. Compute the historical partition spec with
   `toHistoricalPartitionSpec(tableInfo, originalPartitionName)`.
3. Check the local metadata cache for the historical partition id.
4. If missing, call `metadataUpdater.checkAndUpdatePartitionMetadata(...)`.
5. If still missing, call the coordinator
   `resolveHistoricalPartition(tablePath, originalPartitionName)` internal RPC.
   Do not call public `Admin.createPartition`; it requires WRITE permission and
   is not part of the lookup read path.
6. After the resolver RPC returns, refresh metadata for the historical
   partition id or historical partition name.
7. Cache the mappings with table-scoped keys:

   ```text
   (table id, original partition name) -> historical partition id
   ```

   For multi-key partitions this remains correct because different static
   prefixes generate different historical partition names. It is also useful to
   keep a second cache:

   ```text
   (table id, historical partition name) -> historical partition id
   ```

   to avoid duplicate metadata refreshes across many original partitions with
   the same static prefix.
8. Treat a resolver response for an already-created partition as success when
   racing with another client.
9. Do not honor `dynamicPartitionEnabled`; this is a system partition.

Concurrency:

- Use a `ConcurrentHashMap<HistoricalPartitionKey, CompletableFuture<Long>>`
  for in-flight historical partition resolution. The key must include the table
  identity and original partition name.
- Remove failed futures from the map so the next lookup can retry.

## Step 6: Route `PrimaryKeyLookuper` To Historical Lookup

File:

- `fluss-client/src/main/java/org/apache/fluss/client/lookup/PrimaryKeyLookuper.java`

Current behavior:

```java
catch (PartitionNotExistException e) {
    return CompletableFuture.completedFuture(new LookupResult(Collections.emptyList()));
}
```

Replace it with:

1. Add a dedicated Paimon lake lookup key encoder for historical lookups.
   Do not assume the existing `primaryKeyEncoder` produces Paimon bytes. For
   kv format v2 tables with a non-default bucket key, the existing primary key
   encoder uses Fluss compacted encoding to preserve prefix lookup support.
   Paimon `LocalTableQuery` needs Paimon's trimmed primary-key encoding.
   Recommended approach:

   ```java
   KeyEncoder lakePrimaryKeyEncoder =
       KeyEncoder.ofBucketKeyEncoder(
           lookupRowType,
           tableInfo.getPhysicalPrimaryKeys(),
           DataLakeFormat.PAIMON);
   ```

   If this feels too indirect, add an explicit
   `KeyEncoder.ofLakePrimaryKeyEncoder(...)` factory and use that.
2. Extract the original partition name from the lookup key with
   `partitionGetter.getPartition(lookupKey)`.
3. Try normal partition id resolution.
4. If normal partition exists, keep the existing path and send the normal
   `pkBytes`.
5. If `PartitionNotExistException` is thrown:
   - if `insertIfNotExists` is true, fail fast because this milestone does not
     support historical writes;
   - evaluate the expired partition predicate with `Instant.now()`;
   - if the predicate is false, preserve the old behavior for non-expired
     missing partitions: complete with an empty lookup result;
   - if the predicate is true, resolve the historical partition id with
     `HistoricalPartitionResolver`.
6. Compute bucket id using the existing bucket key bytes and Paimon bucketing
   function. This matches the FIP routing rule and existing lake bucket
   alignment.
7. Encode `lakePkBytes` with the dedicated Paimon lake lookup key encoder.
8. Send the lookup to:

   ```text
   TableBucket(tableId, historicalPartitionId, bucketId)
   ```

   and include the original partition name in the lookup query.

Important: normal lookup requests keep using `pkBytes`. Historical lake lookup
requests use `lakePkBytes`. Do not prepend the partition name in this
milestone; the original partition name is carried separately in
`partition_name`.

Tests:

- Add or extend lookup tests to verify:
  - normal partition lookup path is unchanged,
  - missing non-expired partition still returns empty,
  - expired partition routes to historical partition id,
  - original partition name is carried to `LookupClient`,
  - kv format v2 with non-default bucket key sends Paimon-encoded historical
    lookup keys, not compacted Fluss keys.

## Step 7: Carry `partition_name` Through Tablet Service

The current server conversion loses bucket-level request metadata because
`toLookupData` returns `Map<TableBucket, List<byte[]>>`.

Files:

- `fluss-server/src/main/java/org/apache/fluss/server/utils/ServerRpcMessageUtils.java`
- `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`
- `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

Add a request data holder:

```java
public final class LookupDataForBucket {
    private final TableBucket tableBucket;
    private final List<byte[]> keys;
    @Nullable private final String partitionName;
}
```

Implementation:

1. Change lookup request parsing to return an ordered
   `List<LookupDataForBucket>` or a map keyed by a composite
   `(TableBucket, partitionName)` key. Do not use `Map<TableBucket, ...>` for
   historical lookup data because it loses the original partition name and can
   overwrite another request entry with the same historical bucket.
2. Keep normal lookup behavior unchanged when `partitionName == null`, except
   that a request targeting a historical partition without `partitionName` must
   be rejected because the server cannot know which lake partition to query.
3. Treat `partitionName != null` only as a historical lookup hint from the
   client. The server must still validate that the target `TableBucket`
   resolves to a `__historical__` partition before using the lake lookup path.
4. Authorization can still be evaluated by `TableBucket`, but the data passed
   into `ReplicaManager` must retain `partitionName` for historical entries.
5. The response format does not need `partition_name` in this milestone because
   the client batching rule forbids ambiguous request groups: one
   `LookupRequest` cannot contain the same `TableBucket` with multiple
   `partition_name` values.
6. If a future optimization wants to batch such duplicate historical buckets in
   one RPC, it must also add a request-entry correlation key or include
   `partition_name` in `PbLookupRespForBucket`.

Compatibility:

- Prefix lookup remains unchanged.
- Put-kv remains unchanged in this milestone.

## Step 8: Add Lake Lookup SPI

Add table-level point lookup to the lake SPI.

Files:

- `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeStorage.java`
- New file:
  `fluss-common/src/main/java/org/apache/fluss/lake/lakestorage/LakeTableLookuper.java`

Recommended interface:

```java
public interface LakeTableLookuper extends AutoCloseable {

    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    final class LookupContext {
        private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final int schemaId;

        // constructor + getters
    }
}
```

Add a default method to `LakeStorage`:

```java
default LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
    throw new UnsupportedOperationException(
            "Point lookup is not supported for this lake storage.");
}
```

Only Paimon overrides it in this milestone.

## Step 9: Implement `PaimonLakeTableLookuper`

Recommended file:

- `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/lookup/PaimonLakeTableLookuper.java`

Use Paimon 1.3.1 APIs:

- `FileStoreTable.newLocalTableQuery()`
- `LocalTableQuery.lookup(BinaryRow partition, int bucket, InternalRow key)`
- `LocalTableQuery.refreshFiles(BinaryRow partition, int bucket, List<DataFileMeta> beforeFiles, List<DataFileMeta> dataFiles)`

Implementation outline:

1. Lazily create:
   - Paimon catalog,
   - `FileStoreTable`,
   - `LocalTableQuery`,
   - partition row converter,
   - key row wrapper,
   - Fluss value encoder.
2. Convert `LookupContext.partitionSpec` into a Paimon partition `BinaryRow`.
   Reuse existing conversion utilities where possible:
   - `PartitionUtils.toPartitionRow(...)`
   - `PaimonConversions.toFlussRowType(...)`
   - `FlussRowAsPaimonRow`
3. Wrap incoming key bytes directly as a Paimon `BinaryRow`.
   The historical client path must send Paimon-encoded lake lookup key bytes.
   The server must not decode and re-encode them.
4. Before lookup, refresh files for the `(partition, bucket)` pair when the
   latest snapshot changes.
   - Use `fileStoreTable.newScan().withPartitionFilter(...).withBucket(bucket)`
     to plan splits.
   - For each `DataSplit`, collect `beforeFiles()` and `dataFiles()`.
   - Deduplicate files by file name before calling `refreshFiles`.
5. Call `localTableQuery.lookup(partition, bucketId, keyRow)`.
6. If Paimon returns null, return null.
7. If Paimon returns a row:
   - adapt it with `PaimonRowAsFlussRow`,
   - encode it into Fluss compacted row with `CompactedRowEncoder` built from
     the target Fluss table schema for the returned `schemaId`,
   - exclude Paimon system fields and keep the Fluss physical field order,
   - wrap it with `ValueEncoder.encodeValue((short) schemaId, row)`.

Thread safety:

- Make `lookup` synchronized or guard the mutable Paimon query state with a
  lock. Multiple historical lookup tasks can target the same table lookuper
  concurrently from `ioExecutor`.

Lifecycle:

- Close `LocalTableQuery` and catalog in `close()`.
- The server-side manager should close all cached lookupers when
  `ReplicaManager` shuts down.

Tests:

- Unit test the conversion and value encoding with a small Paimon table if
  practical.
- Integration coverage can be in the end-to-end test in Step 12.

## Step 10: Server Historical Lookup Manager

Add a small manager inside tablet-server/replica code to cache lake lookupers by
table id or table path.

Recommended file:

- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalPartitionLookupManager.java`

Dependencies:

- `Configuration`
- `PluginManager`
- `TabletServerMetadataCache` or existing replica metadata access

Wiring:

- Pass `pluginManager` from `TabletServer` into `ReplicaManager`.
- `ReplicaManager` constructs `HistoricalPartitionLookupManager`.

Manager behavior:

1. On first historical lookup for a table, load the table's lake storage:
   - verify table config is lake-enabled,
   - verify lake format is Paimon,
   - use `LakeStoragePluginSetUp.fromDataLakeFormat(...)`,
   - use `LakeStorageUtils.extractLakeProperties(conf)` to create the
     `LakeStorage`,
   - call `createLakeTableLookuper(tablePath)`.
2. Cache the `LakeTableLookuper` by table id.
3. On schema id change, either:
   - let the lookuper refresh its internal table resources, or
   - evict and recreate the cached lookuper.
4. Close cached lookupers on `ReplicaManager` close.

Dynamic config:

- The first milestone can keep this simple and rely on process restart for
  lake runtime config changes.
- If dynamic lake config support is required immediately, mirror
  `LakeCatalogDynamicLoader` and evict cached lookupers when `datalake.*`
  configuration changes.

## Step 11: Execute Historical Lookups In `ReplicaManager`

File:

- `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`

Implementation:

1. Split incoming lookup bucket data into normal and historical candidates by
   the presence of `partitionName`. This split is only a candidate
   classification; it is not sufficient authorization or validation.
2. Process normal lookups exactly as today with local replica KV lookup.
3. For historical lookups:
   - get the hosted replica with `getReplicaOrException(tb)`;
   - verify the target `TableBucket` is a partitioned bucket and the
     partition id resolves to a partition name that satisfies
     `isHistoricalPartitionName(tableInfo, targetPartitionName)`;
   - reject requests that set `partitionName` on a normal partition bucket;
   - reject requests that target a historical partition bucket without
     `partitionName`;
   - validate client version for PK table as today;
   - verify `replica.getTableInfo().hasPrimaryKey()`;
   - parse `partitionName` into `ResolvedPartitionSpec`;
   - re-evaluate
     `isExpiredAutoPartition(replica.getTableInfo(), partitionName, now)` on
     the server side before lake lookup;
   - submit lake lookup work to `ioExecutor`;
   - for every key, call `LakeTableLookuper.lookup(key, context)`;
   - preserve result order.
4. Complete the original response callback only after all historical futures and
   normal lookup processing finish.
5. If historical lookup validation or execution fails, return a bucket-level
   `ApiError` as the current lookup path does. Validation failures should use a
   deterministic `ApiException` with an explicit message, not fall back to
   normal local lookup.

Metrics:

- Increment existing total lookup requests for both normal and historical
  buckets.
- Increment failed lookup requests only for unexpected server-side failures,
  following current semantics.
- A dedicated historical lookup metric can be added later; it is not required
  for the first milestone.

Tests:

- Add `ReplicaManager` or tablet-service tests that reject a lookup request
  with `partitionName` when the target bucket is a normal partition.
- Add a test that rejects a lookup request targeting a `__historical__` bucket
  when `partitionName` is missing.
- Add a test that rejects a historical lookup when the supplied original
  partition name is malformed, current, future, or otherwise not expired.

## Step 12: Integration Test

Add an end-to-end ITCase that proves the feature works through public APIs.

Recommended location:

- `fluss-client/src/test/java/org/apache/fluss/client/table/LakeEnableTableITCase.java`
  or a new historical lookup ITCase near existing lake table tests.

Test scenario:

1. Start a Fluss cluster with Paimon lake storage enabled.
2. Create a primary-key table:
   - partitioned by an auto partition key,
   - `table.datalake.enabled=true`,
   - `table.datalake.format=paimon`,
   - small retention count for the auto-partition strategy.
3. Insert a row into a partition that is initially valid.
4. Ensure the row is tiered to Paimon. Reuse existing tiering test helpers
   rather than adding sleeps when possible.
5. Expire/drop the original partition through existing TTL or direct metadata
   operation suitable for the test.
6. Build a lookup key containing the original partition value.
7. Call `table.newLookup().createLookuper().lookup(key)`.
8. Assert the returned row equals the inserted row.
9. Assert `listPartitionInfos` includes the generated `__historical__`
   partition.

Negative cases:

- Expired-looking partition on a non-lake table returns the previous empty
  result or fails as before.
- Missing future partition does not route to `__historical__`.
- `enableInsertIfNotExists()` on an expired partition fails with an explicit
  unsupported message.

Use AssertJ assertions only.

## Suggested PR Breakdown

1. Common utilities and coordinator support:
   - historical partition constants,
   - expired partition predicate,
   - read-authorized historical partition resolver RPC,
   - system partition validation inside the resolver path,
   - auto partition manager exclusion,
   - unit tests.
2. RPC and client routing:
   - resolver RPC messages,
   - lookup `partition_name` proto field,
   - generated RPC classes,
   - lookup query metadata,
   - batching by original partition name,
   - historical partition resolver,
   - `PrimaryKeyLookuper` route change,
   - client unit tests.
3. Lake SPI and Paimon lookuper:
   - `LakeTableLookuper`,
   - `LakeStorage#createLakeTableLookuper`,
   - `PaimonLakeTableLookuper`,
   - focused Paimon tests.
4. Server execution and E2E test:
   - server request parsing,
   - `ReplicaManager` historical lookup path,
   - server lookuper cache,
   - integration test.

## Verification Commands

Run focused checks first:

```bash
./mvnw test -pl fluss-common -Dtest=PartitionUtilsTest
./mvnw test -pl fluss-client -Dtest=LookupSenderTest,ClientRpcMessageUtilsTest
./mvnw test -pl fluss-server -Dtest=AutoPartitionManagerTest
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest='*Paimon*Lookup*Test'
```

Then run affected module verification:

```bash
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server,fluss-lake/fluss-lake-paimon
./mvnw spotless:check
```

If proto changes are included, run generation before tests:

```bash
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc
```

## Risks And Follow-up Work

- If historical lookup and normal lookup share the same client inflight
  semaphore, slow lake lookups can still occupy client lookup permits. The FIP
  proposes a separate historical semaphore; defer it until the basic feature is
  stable unless tests show visible interference.
- Without historical writes, a late write redirected to `__historical__` is not
  visible because that path is not implemented yet. This milestone only makes
  already-tiered historical lake data queryable.
- Paimon `LocalTableQuery.refreshFiles` must deduplicate data files. Passing
  duplicates can fail inside Paimon level construction.
- The response path must not rely only on `TableBucket` when multiple original
  partitions map to the same historical bucket. Keep batching split by original
  partition name.
- `ResolvedPartitionSpec.fromPartitionName` is permissive. Add strict parsing
  for this feature to avoid accepting malformed historical routing names.

## Done Criteria

- Normal primary-key lookup behavior is unchanged.
- Lookup on an eligible expired auto-partition returns a value from Paimon lake
  storage.
- Missing invalid, future, non-lake, or non-Paimon partitions are not routed to
  `__historical__`.
- `__historical__` is lazily created and not expired by `AutoPartitionManager`.
- Historical lookup RPCs carry the original partition name and never mix
  different original partition names in one bucket request.
- Lake lookup runs off the RPC thread.
- Focused unit tests and the end-to-end Paimon historical lookup test pass.
