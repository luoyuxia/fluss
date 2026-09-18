<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# **FIP-25: Support In-Place Promotion of Existing Paimon Tables to Datalake-Enabled Fluss Tables**

| Item | Value |
| --- | --- |
| Author | Yuxia Luo |
| Last updated | September 11, 2026 |
| Status | Discussion |
| Discussion thread | [dev@fluss.apache.org](https://lists.apache.org/list.html?dev@fluss.apache.org) |
| Vote thread | TBD |
| Issue | [Apache Fluss Issues](https://github.com/apache/incubator-fluss/issues) |
| Target version | TBD |

> Related discussions should take place on the Fluss developer mailing list rather than in lengthy
> Wiki comment threads.

## Background and Motivation

Fluss provides unified lake and stream storage. Users can process real-time data with Fluss and
persist it to Lake Storage systems such as Paimon.

Some users already have Paimon tables and historical data before adopting Fluss. To add Fluss
real-time read and write capabilities to these tables, users currently need to perform the
following steps manually:

1. Read the columns, primary keys, partition keys, and table properties from the Paimon table.
2. Create a table with the same schema in Fluss.
3. Configure the Fluss table to reuse the existing Paimon table as Lake Storage.
4. For a primary-key table with historical data, start a Bulk Load job to initialize the Fluss
   real-time serving layer.
5. Register the initial lake snapshot.
6. Enable datalake after initialization, and then start new real-time reads and writes.

The Bulk Load job reads the required data from a fixed Paimon snapshot, groups the final state
of every primary key by Fluss bucket, builds KV snapshots, and publishes them to the corresponding
replicas through a server-side transaction. It initializes the Fluss real-time KV storage without
rewriting historical data to Paimon. After initialization, Fluss can look up the current value of an
existing primary key, correctly process subsequent upserts and deletes based on that state, and
generate changelog records.

## Goals

This FIP supports the in-place promotion of an existing Paimon table to a datalake-enabled Fluss
table. "In-place" means that Fluss reuses the existing Paimon table and its historical data without
creating a replacement Paimon table or rewriting the historical data to Paimon. Fluss creates the
corresponding table metadata and, when necessary, initializes its real-time KV storage through Bulk
Load. Users complete the promotion with a single `CALL sys.enable_fluss_on_lake_table(...)`
statement. The procedure determines the Bulk Load scope from the table type and the optional
partition selection supplied by the user.

## Public Interfaces

### Flink Procedure

Register the `sys.enable_fluss_on_lake_table` procedure in the Fluss Catalog:

```sql
-- Use the default properties derived from the Paimon table
CALL sys.enable_fluss_on_lake_table('my_db.my_table');

-- Override or supplement Fluss table properties
CALL sys.enable_fluss_on_lake_table(
    'my_db.my_table',
    'bucket.num=16,table.log.format=compacted'
);

-- Load specific partitions
CALL sys.enable_fluss_on_lake_table(
    table => 'my_db.my_partitioned_table',
    partitions => '2026-09-07,2026-09-08,2026-09-09'
);

-- Wait until the entire promotion completes
SET 'table.dml-sync' = 'true';
CALL sys.enable_fluss_on_lake_table('my_db.my_table');
```

#### Input Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `table` | STRING | Yes | Fully qualified name of the existing Paimon table in `database.table` format. The Fluss table uses the same database and table name. |
| `properties` | STRING | No | Comma-separated Fluss table properties in `key=value` format. These properties override or supplement properties derived from the Paimon table. |
| `partitions` | STRING | No | Comma-separated partition names to Bulk Load for a partitioned primary-key table. See [Bulk Load Partition Selection](#bulk-load-partition-selection) for the defaults and restrictions. |

#### Result

The procedure returns one row:

| Output column | Type | Description |
| --- | --- | --- |
| `message` | STRING | `JobID=<flink-job-id>` after asynchronous job submission, or a completion message such as `Successfully enabled Fluss on Paimon table 'my_db.my_table'.` after promotion completes. |

When Bulk Load is required, the procedure submits the job asynchronously by default
(`table.dml-sync=false`) and returns its Job ID. The job performs Bulk Load, registers the initial
lake snapshot, and enables datalake. Setting `table.dml-sync=true` makes the procedure wait until
all these steps complete.

### Admin API

Add the following method to `Admin`:

```java
/**
 * Creates a Fluss table on an existing lake table asynchronously.
 *
 * @param tablePath path of the existing lake table
 * @param properties properties that override or supplement values derived from the lake table
 * @return a future containing the created table metadata
 */
CompletableFuture<TableInfo> createTableOnLake(
        TablePath tablePath, Map<String, String> properties);
```

The Coordinator derives the schema, primary keys, partition keys, and buckets from the existing
Paimon table, merges `properties`, and creates a lake-disabled Fluss table. When the Paimon
partition definition can be mapped to Fluss auto partitioning, it also derives the Fluss
auto-partition configuration.
The returned future completes with the actual `TableInfo` after the Fluss table metadata has been
created. If a Fluss table with the same name already exists, the future fails with
`TableAlreadyExistException`.
`createTableOnLake()` rejects `table.datalake.enabled` and
`table.datalake.historical-partition.enabled` in `properties` because the promotion flow manages
these options.

Completion of this future does not mean that the initial lake snapshot has been registered,
historical data has been loaded, or datalake has been enabled. The caller is responsible for these
steps.

### RPC Protocol

Add the public `CREATE_TABLE_ON_LAKE` RPC with API key `1065`. Its minimum and maximum protocol
versions are both `0`:

```protobuf
message CreateTableOnLakeRequest {
  required PbTablePath table_path = 1;
  repeated PbKeyValue properties = 2;
}

message CreateTableOnLakeResponse {
  required int64 table_id = 1;
  required int32 schema_id = 2;
  required bytes table_json = 3;
  required int64 created_time = 4;
  required int64 modified_time = 5;
  optional string remote_data_dir = 6;
}
```

The request identifies the existing Paimon table through `table_path` and passes Fluss table
properties to override or supplement through `properties`. The response returns the table ID,
schema ID, table descriptor, timestamps, and optional remote data directory required to construct
`TableInfo`. A successful RPC means only that the Fluss table metadata has been created. It does not
include Bulk Load, initial snapshot registration, or datalake enablement.

### LakeCatalog API

Add the following method to `LakeCatalog` to retrieve the current snapshot ID of a Paimon table:

```java
/**
 * Get the latest snapshot ID of a lake table.
 *
 * @param tablePath path of the lake table
 * @return the latest snapshot ID, or empty if the table has no snapshot
 * @throws TableNotExistException if the lake table does not exist
 * @throws UnsupportedOperationException if reading snapshots is not supported
 */
default Optional<Long> getLatestSnapshotId(TablePath tablePath) throws TableNotExistException {
    throw new UnsupportedOperationException(
            "Reading snapshots is not supported by this lake catalog.");
}
```

`PaimonLakeCatalog` implements this method. The procedure uses it to fix the Bulk Load source
snapshot. The job verifies that the source snapshot has not changed before registering the initial
lake snapshot. The Coordinator uses this method to retrieve the current Paimon snapshot before
enabling datalake.

## Proposed Changes

### Partition Modeling

The Coordinator models a Paimon table as auto-partitioned when its partition definition identifies
one time partition key and its timestamp pattern or formatter can be mapped unambiguously to Fluss.
It derives `table.auto-partition.enabled`, the time key, format, unit, and time zone. Paimon
partition expiration is not mapped to Fluss retention because the lake table and Fluss real-time
storage have independent lifecycle policies. The caller configures Fluss retention through
`properties`, or the Fluss default applies.

Callers do not need to set `table.auto-partition.enabled=true`. Explicit values for derived options
must match the inferred values. A partition definition that cannot be mapped to one compatible time
key remains a regular partition definition. Regular partitioned tables may have multiple partition
keys. Because historical partition access requires exactly one partition key, this FIP limits
auto-partitioned primary-key tables to one time-based partition key.

A regular partitioned table is appropriate when Fluss should not automatically manage the
partition lifecycle. An auto-partitioned table is appropriate when Fluss should maintain a rolling
window of real-time partitions.

For an auto-partitioned primary-key table, promotion also sets
`table.datalake.historical-partition.enabled` to `true` when it enables datalake. Past Paimon
partitions that are not Bulk Loaded are not created as regular Fluss partitions. Subsequent writes
and primary-key point lookups for them use historical partition access to retrieve existing state
from Lake Storage.

### Bulk Load Partition Selection

For an auto-partitioned table, the initial Bulk Load scope is not derived from
`table.auto-partition.num-retention`. Retention controls how long Fluss keeps regular real-time
partitions; the promotion does not need to initialize that entire window.

| Table type | Bulk Load scope |
| --- | --- |
| Non-partitioned primary-key table | Load the whole table from the source snapshot. |
| Regular partitioned primary-key table | By default, load all existing Paimon partitions from the source snapshot. If `partitions` is specified, load only those partitions. |
| Auto-partitioned primary-key table | By default, load the current time partition and the immediately preceding time partition if they exist. If `partitions` is specified, load those partition values instead. |
| Append-only log table | Do not run Bulk Load; register only the initial lake snapshot. |

`partitions` specifies the partitions to initialize in Fluss. Every explicitly specified partition
must exist in the source Paimon snapshot. For a regular partitioned primary-key table, unloaded
partitions do not support Fluss point lookups or writes, and users must ensure that no writes are
issued to them. For an auto-partitioned primary-key table, the current and immediately preceding
time partitions are loaded by default; unloaded past partitions are accessed through Lake Storage.

### Architecture and Component Boundaries

```text
CALL sys.enable_fluss_on_lake_table(...)
  │
  └─ EnableFlussOnLakeTableProcedure
       │
       └─ createTableOnLake()
            │
            ├─ TableAlreadyExistException
            │    └─ skip initialization → validated enablement → return success
            │
            └─ created → fix snapshot and scope
                 │
                 ├─ no Bulk Load required
                 │    └─ register snapshot if present → enable → return success
                 │
                 └─ Bulk Load required → submit Flink job
                      │
                      ├─ Job: Bulk Load → register snapshot → enable → FINISHED
                      │
                      └─ Procedure response:
                           table.dml-sync=false → return JobID
                           table.dml-sync=true  → wait for FINISHED → return success
```

The component boundaries are as follows:

| Capability | Location | Responsibility |
| --- | --- | --- |
| Paimon schema, primary-key, partition, and bucket mapping | Coordinator/Paimon lake plugin | Read the Paimon table and perform authoritative compatibility validation. |
| Create a lake-disabled Fluss table | `Admin.createTableOnLake()` | Attempt to create the Fluss metadata before job submission; return `TableAlreadyExistException` if a table with the same name already exists. |
| Retrieve the latest Paimon snapshot ID | `LakeCatalog.getLatestSnapshotId()` | Fix the Bulk Load source snapshot, validate the requested load scope, and validate consistency before enabling datalake. |
| Register the initial snapshot for a log table | `FlussTableLakeSnapshotCommitter` | If a snapshot exists, register it in the `lakeTable` ZooKeeper node with empty offsets. |
| Bulk Load data job | `load_lake_data_to_fluss` | Initialize a primary-key table over the selected load scope, register its initial snapshot, and enable datalake. |
| Historical partition routing | Fluss lookup and write clients | Route a missing past auto partition to Lake Storage rather than dynamically creating an empty regular partition. |
| Enable datalake | `Admin.alterTable()` | Called by the job, or directly by the procedure when no Bulk Load is needed, after the Coordinator validates the schema and snapshots; also enable historical partition access for an auto-partitioned primary-key table. |
| Submit the Flink job | `EnableFlussOnLakeTableProcedure` | Submit the job asynchronously by default or wait for completion when `table.dml-sync=true`. |

### Detailed Flow

The procedure executes the following steps:

1. Retrieve the Fluss and Paimon Catalog configurations from the current Fluss Catalog.
2. Call `Admin.createTableOnLake()`. The Coordinator maps the Paimon schema through
   `LakeCatalog.getTableDescriptor()`, derives the auto-partition configuration when applicable,
   and creates a lake-disabled Fluss table.
3. If the server returns `TableAlreadyExistException`, skip table creation and Bulk Load and
   continue with step 6. Other exceptions are propagated to the caller.
4. After creating the table, fix the latest Paimon snapshot as the source snapshot and validate the
   requested Bulk Load scope, including every explicitly selected partition.
5. Select the following initialization path:
   - For a log table, if a snapshot exists, use `FlussTableLakeSnapshotCommitter` to register the
     snapshot directly in the `lakeTable` ZooKeeper node with empty offsets.
   - For an empty primary-key table with no snapshot, do not execute Bulk Load or register a
     snapshot.
   - For a primary-key table with a snapshot, use the snapshot ID as the source snapshot and submit
     the `load_lake_data_to_fluss` batch job with the scope defined in
     [Bulk Load Partition Selection](#bulk-load-partition-selection). The job performs Bulk Load,
     validates and registers the initial lake snapshot, and enables datalake as described below.
     Return the Job ID by default, or wait for completion when `table.dml-sync=true`.
6. When no Bulk Load job is needed, the procedure calls `Admin.alterTable()` directly to enable
   datalake with server-side validation and returns the success message. For an auto-partitioned
   primary-key table, the same alteration also enables
   `table.datalake.historical-partition.enabled`.

### Server-Side Validation Before Enabling Datalake

When `table.datalake.enabled` changes from `false` to `true`, the Coordinator first checks whether a
Paimon table with the same name exists. If it does not exist, the existing flow creates the Paimon
table. If it already exists, the Coordinator performs the following validation:

1. Read the Paimon table schema and verify that it is compatible with the current Fluss table.
2. Read the current Paimon snapshot ID through `LakeCatalog.getLatestSnapshotId()`.
3. Read the registered snapshot ID from the Fluss lake table metadata.
4. Allow the enablement if both snapshot IDs are equal or neither side has a snapshot. Reject it
   with `InvalidAlterTableException` if only one side has a snapshot or the two IDs differ.

This validation applies to `Admin.alterTable()` invoked by the job or procedure and to an
`ALTER TABLE` statement issued directly by a user. Setting the property to `true` again on a table
that already has datalake enabled does not change the property and is handled as an idempotent
operation.

Users must not write directly to Paimon while datalake is disabled. Such data does not enter the
Fluss real-time serving layer, and any change to the Paimon snapshot causes validation to fail when
datalake is enabled again. Validation cannot prevent a Paimon write that commits concurrently with
the check, so stopping native Paimon writers remains a prerequisite for re-enablement.

### Initializing the Real-Time Serving Layer with Bulk Load

Historical data in a Paimon primary-key table exists only in Lake Storage. After
`createTableOnLake()` creates the Fluss metadata, the corresponding Fluss KV storage is still empty.
If real-time writes are accepted at this point, Fluss cannot read the current value of an existing
primary key or generate correct changelog records for updates and deletes to that key. Before
accepting real-time reads and writes, Fluss therefore needs to load a specific Paimon data version
into its KV storage as the initial primary-key state.

The `load_lake_data_to_fluss` batch job submitted by the procedure completes initialization and
datalake enablement:

1. The procedure fixes the latest Paimon snapshot observed after table creation as the source
   snapshot. Subsequent reads and initial lake snapshot registration use the same ID and do not
   select a new latest snapshot while the job is running.
2. The job starts a transaction through Bulk Load Begin and verifies that the target Fluss table or
   partition does not contain user data.
3. The Bulk Load job applies the scope defined in
   [Bulk Load Partition Selection](#bulk-load-partition-selection). It loads the whole table for a
   non-partitioned table, loads all or explicitly selected partitions for a regular partitioned
   table, or loads the default or explicitly selected time partitions for an auto-partitioned
   table.
4. The job reads the data to initialize and divides it by Fluss bucket. Each bucket task builds
   RocksDB, SST, and Remote Log files outside the TabletServer and uploads them to remote storage.
5. After all buckets have been built, Bulk Load Commit publishes the KV snapshot and Remote Log
   metadata together through a server-side transaction. Once replicas load the published snapshot,
   the historical primary-key state becomes readable in Fluss.
6. Within the job's commit hook, read the latest Paimon snapshot ID again. Register the source
   snapshot and the per-bucket log end offsets produced by Bulk Load only if the ID still equals
   the source snapshot ID.
7. After registration succeeds, the job calls `Admin.alterTable()` to enable datalake. For an
   auto-partitioned primary-key table, it also enables historical partition access in the same
   alteration. The job completes successfully only after datalake is enabled.

After initialization, the Fluss real-time serving layer contains the final primary-key state of the
loaded partitions at the source snapshot. Fluss can use this state for Lookup, correctly process
subsequent Upsert and Delete operations, and generate changelog records for downstream consumers.
The initial lake snapshot and bucket offsets define the handoff boundary between the historical
data in Lake Storage and subsequent Fluss Log records, preventing later tiering from duplicating or
omitting data. For a regular partitioned table, only loaded partitions support Fluss point lookups
and writes. For an auto-partitioned table, historical partition access retrieves existing state
from Lake Storage for past partitions that were not loaded and supports subsequent point lookups,
upserts, and deletes.

Bulk Load does not rewrite historical data to Paimon because the data already exists in the source
snapshot. It initializes the Fluss real-time KV storage and its correspondence with that Paimon
snapshot. The primary-key table Bulk Load capability is tracked by
[[Umbrella] Support production-ready bulk loading for primary-key tables](https://github.com/apache/fluss/issues/4222).
The issue and its subtasks define the data reading, file construction, transaction commit, and
failure recovery mechanisms.

### Failure Handling

- If datalake is being enabled when a Paimon table with the same name already exists and the current
  Paimon snapshot does not match the snapshot registered in Fluss, `alterTable()` returns an error
  and the table remains lake-disabled.
- If datalake is being enabled when a Paimon table with the same name already exists and the current
  Fluss table schema is incompatible with the Paimon table schema, `alterTable()` returns an error
  and the table remains lake-disabled.
- If job submission fails, the procedure throws an exception. After submission, job failures are
  reported through the Flink job status, or by the procedure when synchronous waiting is enabled.

### User-Visible Constraints

All tables must meet the following conditions:

- The specified table exists in the Paimon Catalog.
- Every Paimon column type can be fully mapped to a Fluss type.
- The user-supplied Fluss table properties are valid.
- When datalake is enabled and a Paimon table with the same name already exists, the current Paimon
  snapshot must match the snapshot registered in Fluss, and the server-side schema compatibility
  validation must pass.

The source table definition must be mappable to Fluss according to
[Appendix A](#appendix-a-paimon-to-fluss-table-definition-mapping-matrix).
The appendix covers table types, schemas, bucket modes and functions, merge engines, deletion
semantics, TTL, and partition expiration. Partitioned tables may have multiple partition keys, but
auto-partitioned primary-key tables are limited to one time-based partition key because this flow
enables historical partition access.

### Write Ownership Handoff

Before executing `CALL`, users must stop native Paimon writers and direct schema changes, and keep
them stopped throughout initialization. Setting `table.datalake.enabled=false` does not prevent
normal Fluss writes, so users must also wait for promotion to complete before writing to the
new Fluss table. After datalake is enabled, all application writes must go through Fluss. Data and
schema changes must no longer be committed directly through native Paimon write paths. Fluss Lake
Tiering may continue to commit data to Paimon through the existing protocol.

Fluss cannot forcibly stop external Paimon writers, so users must satisfy this prerequisite. If the
write ownership handoff is incomplete, native Paimon writes or schema changes bypass Fluss and can
make the data or schema state in the Fluss real-time serving layer inconsistent with Paimon,
affecting data correctness.

## Compatibility

Compatibility and supported Paimon table definitions are documented in
[Appendix A](#appendix-a-paimon-to-fluss-table-definition-mapping-matrix).

## Test Plan

- Verify that Paimon append-only tables and empty tables complete table creation, snapshot
  registration, and datalake enablement without executing Bulk Load.
- Verify that a Paimon primary-key table with historical data uses a fixed snapshot for Bulk Load.
  When the snapshot ID remains unchanged before and after the job, verify that the result is
  registered and datalake is enabled; when it changes, verify that the process fails.
- Verify that a regular partitioned primary-key table loads every existing Paimon partition by
  default and loads only the requested partitions when `partitions` is specified. Verify that an
  unloaded partition is not initialized in Fluss.
- Verify that an auto-partitioned primary-key table loads the current and immediately preceding time
  partitions by default. Verify that missing partitions are skipped without substituting older
  partitions and that an explicit `partitions` value selects the requested existing partitions.
- Verify that `partitions` supports single-key and multi-key partition names and rejects invalid
  formats, duplicates, missing or ineligible partitions, and use with a non-partitioned or
  append-only table. For an auto-partitioned table, also verify that current or future data
  omissions are rejected.
- Verify that time partition definitions automatically derive the auto-partition key, format, unit,
  and time zone. Verify that Paimon partition expiration is not used to derive Fluss retention.
  Verify that missing, ambiguous, and incompatible time mappings fall back to regular partition
  modeling or are rejected when required by an explicit property.
- Verify that historical partition access is enabled automatically for an auto-partitioned
  primary-key table. Verify that point lookups, upserts, and deletes for an unloaded past partition
  use Lake Storage and do not dynamically create an empty regular Fluss partition.
- Verify multi-key regular partitioned tables and auto-partitioned log tables, and reject multi-key
  auto-partitioned primary-key tables because historical partition access requires one key.
- Verify that repeated procedure calls with only `table` specified are idempotent.
- Verify that enabling datalake through `ALTER TABLE` creates the Paimon table through the existing
  flow when a table with the same name does not exist. When one already exists, verify that
  enablement succeeds when the snapshot IDs match or neither side has a snapshot, and is rejected
  when only one side has a snapshot or the snapshot IDs differ.
- Verify that incompatible schemas for a table with the same name, process failures, bucket modes,
  time-based partition validation, and user property validation produce the expected results.

## Rejected Alternatives

### Load Data into Fluss with the Tiering Service

The Tiering Service continuously writes Fluss data to Lake Storage. Loading historical data from a
Paimon table into Fluss is a one-time reverse initialization task that scans a specified Paimon
snapshot, builds RocksDB and SST files, and publishes them to Fluss through a Bulk Load transaction.
Running this task in the Tiering Service would make it responsible for data flows in both directions
and for two separate task lifecycles, expanding its scheduling, resource management, and failure
recovery responsibilities.

Bulk Load also consumes substantial compute, memory, network, and remote storage I/O resources. If
it shares execution and recovery paths with continuous tiering jobs, its resource contention,
backpressure, or failure retries may delay normal tiering and affect the stability of the Tiering
Service. This FIP therefore uses a separate Flink batch job to isolate Bulk Load resources and
failures. The procedure submits the job asynchronously by default.

## Appendix A: Paimon to Fluss Table Definition Mapping Matrix

The complete support matrix is maintained in
[Paimon to Fluss Table Definition Mapping Matrix](paimon-fluss-promotion-support-matrix-en.md). It is
part of this FIP and defines the detailed table-definition mappings and compatibility limitations
for promotion.
