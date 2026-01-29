# FIP-XXX: Historical Partition Support for Fluss Datalake-Enabled Tables

## Motivation

Currently, for Fluss auto-partitioned tables with lake tiering (e.g., Paimon), when Fluss partitions expire, several issues arise:

### Problem 1: Data Loss for Writes to Expired Partitions

When data arrives late and is written to an already-expired partition:
- The data is written to Fluss successfully
- Fluss automatically cleans up the expired partition
- Although the partition exists in Paimon, the data cannot be tiered to Paimon
- This results in silent data loss

### Problem 2: Point Query Inconsistency for Primary Key Tables

For primary key tables with auto-partitioning:
- Point queries (lookup) only query data from Fluss cluster
- When a partition expires on Fluss, even though the partition data exists in Paimon, point queries cannot find it
- Users see inconsistent results between point queries and batch reads
- This is extremely confusing for users who expect the data to be available

### Problem 3: Union Read Incomplete Data

For union read scenarios:
- Historical partitions that have been tiered to Paimon but expired on Fluss are not accessible
- Users cannot query the full history of their data through Fluss

### Proposed Solution

This FIP proposes to make Paimon historical partitions truly serve as the data source for Fluss auto-partitioned tables after the corresponding Fluss partitions expire. Specifically:

1. **Late data writes** to expired partitions should still be tiered to Paimon
2. **Union read** should include historical partitions from Paimon
3. **Point queries** for PK tables should fallback to Paimon for expired partitions

## Public Interfaces

### 1. Automatic Enablement (No Explicit Configuration)

Historical partition support is **automatically enabled** when both conditions are met:
- `datalake.enabled = true` (lake table)
- Table has auto-partitioning enabled

No explicit configuration is needed. The partition lifecycle automatically follows the data lake's expiration policy.

```sql
-- Example: This table automatically gets historical partition support
CREATE TABLE orders (
    order_id BIGINT,
    order_time TIMESTAMP,
    ...
    PRIMARY KEY (order_id) NOT ENFORCED
) PARTITIONED BY (DATE_FORMAT(order_time, 'yyyy-MM-dd'))
WITH (
    'datalake.enabled' = 'true',
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-interval' = '1 d'
);

-- Partition lifecycle:
-- 1. Fluss partition expires (based on Fluss auto-partition config)
--    → metadata retained, Fluss data cleaned
-- 2. Paimon partition expires (based on Paimon partition.expiration-time)
--    → metadata cleaned, lake data cleaned
```

### 2. New Metadata Structure

A new metadata structure to track historical partitions:

```java
public class HistoricalPartitionInfo {
    private final String partitionName;
    private final long flussExpireTimestamp;    // When the partition expired in Fluss
    private final long lakeSnapshotId;          // Latest lake snapshot containing this partition
}
```

**Lifecycle**: When Paimon partition expires (controlled by Paimon's `partition.expiration-time`), Fluss periodically syncs with Paimon and removes the corresponding `HistoricalPartitionInfo`.

### 3. Extended Client API

```java
public interface Table {
    // Existing method
    List<PartitionInfo> listPartitions();
    
    // New method to include historical partitions from lake
    List<PartitionInfo> listPartitions(boolean includeHistorical);
}
```

### 5. Lake Integration (Paimon-Specific)

Currently, only Paimon supports partition TTL (`partition.expiration-time`). We hardcode Paimon's TTL detection logic instead of abstracting it into a generic interface.

**Rationale**: Avoid over-engineering when there's only one implementation. If other lake formats (Iceberg, Hudi) need support in the future, we can introduce abstraction then.

```java
/**
 * Paimon-specific historical partition handler.
 * Directly uses Paimon APIs for partition expiration detection.
 */
public class PaimonHistoricalPartitionHandler {
    
    private final Table paimonTable;
    
    /**
     * Check if a partition has expired in Paimon.
     * Uses Paimon's partition.expiration-time configuration.
     */
    public boolean isPartitionExpiredInPaimon(String partitionName) {
        // Directly use Paimon's partition expiration logic
        // No abstraction needed for now
    }
    
    /**
     * Lookup a key from Paimon for changelog generation.
     */
    @Nullable
    public InternalRow lookup(String partitionName, int bucket, InternalRow key) {
        // Direct Paimon lookup
    }
    
    /**
     * Scan historical partition data from Paimon.
     */
    public CloseableIterator<InternalRow> scan(String partitionName, int bucket) {
        // Direct Paimon scan
    }
}
```

**Future Extensibility**: If Iceberg/Hudi support is needed later, extract a `LakeHistoricalPartitionHandler` interface at that time.

## Proposed Changes

### Phase 1: Historical Partition Metadata Management

#### 1.1 Partition Expiration Handling

When a Fluss partition expires:

```
Before (Current):
┌─────────────────┐
│  Fluss Partition │ ──expire──> Partition Deleted (metadata + data)
│     (active)     │
└─────────────────┘

After (Proposed):
┌─────────────────┐              ┌──────────────────────┐
│  Fluss Partition │ ──expire──> │  Historical Partition │
│     (active)     │             │   metadata: retained  │
└─────────────────┘              │   data: cleaned up    │
                                 └──────────────────────┘
```

**Key Points**:
- **Metadata retained**: Partition metadata is kept so we know this partition exists as a historical partition
- **Data cleaned up**: Fluss data (logs/KV) is cleaned up as before to free storage
- **Lake data available**: Data that was already tiered to Paimon remains accessible

#### 1.2 Historical Partition Data Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Historical Partition Lifecycle                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Fluss Partition    expire     Historical Partition    Paimon expires   │
│    (active)      ──────────>    (metadata only)      ───────────────>   │
│  - has data                    - metadata retained     Fully Cleaned    │
│  - being tiered                - Fluss data cleaned    - metadata gone  │
│                                - lake data available   - lake data gone │
│                                - can receive late data                   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 1.3 Historical Partition Data Cleanup

Historical partitions can still receive late data, which will be tiered to Paimon. To prevent unbounded data growth, the cleanup is governed by **Paimon's partition expiration policy**:

- Paimon's `partition.expiration-time` controls how long partition data is retained in the lake
- When Paimon expires a partition, Fluss detects this and cleans up the corresponding historical partition metadata
- This ensures both Fluss metadata and Paimon data are cleaned up in a coordinated manner

#### 1.4 Coordinator Server Changes

- Maintain a `HistoricalPartitionManager` to track expired partitions with lake data
- Store historical partition metadata in Zookeeper/metadata store
- Provide APIs for querying historical partition information
- **Periodically sync with Paimon** to detect expired partitions in lake and clean up corresponding Fluss metadata

### Phase 2: Write Path Enhancement

#### 2.1 Late Data Write Handling (Log Tables)

For log tables (append-only), late data writes go through the normal Fluss write path:

```
Write Request (partition=dt-2024-01-01, data)
           │
           ▼
┌──────────────────────────┐
│  Check Partition Status   │
└──────────────────────────┘
           │
           ├── Active Partition ──────────> Normal Write Path
           │
           └── Historical Partition ─────────> Normal Write Path
                                                    │
                                                    ▼
                                           ┌─────────────────┐
                                           │ Write to Fluss  │
                                           │ (same as normal)│
                                           └─────────────────┘
                                                    │
                                                    ▼
                                           ┌─────────────────┐
                                           │ Tiering Service │
                                           │ writes to Paimon│
                                           └─────────────────┘
```

**Key Design Decision**: Historical partitions receive writes through the same path as active partitions. The tiering service then handles tiering the data to Paimon. This simplifies the implementation and ensures consistency.

#### 2.2 Late Data Write Handling (Primary Key Tables)

For PK tables, writes to historical partitions follow the same path as active partitions:

```
PK Table Write (partition=dt-2024-01-01, key=K, value=V)
           │
           ▼
┌──────────────────────────┐
│  Check Partition Status   │
└──────────────────────────┘
           │
           ├── Active Partition ──────> Normal Write Path (Fluss KV + Log)
           │
           └── Historical Partition ───> Normal Write Path (Fluss KV + Log)
                     │
                     ▼
           ┌─────────────────────────┐
           │  Tiering Service         │
           │  - Reads from Fluss log  │
           │  - Writes to Paimon      │
           │  - Changelog already     │
           │    generated by Fluss KV │
           └─────────────────────────┘
```

**Key Design Decision**: For historical partitions, writes go through the normal Fluss KV path:
1. Fluss KV generates changelog by looking up existing values in Fluss KV store
2. Data is written to Fluss log
3. Tiering service then writes the data to Paimon

This approach:
- Avoids the complexity of querying Paimon for changelog generation
- Reuses existing write path infrastructure
- Maintains consistency with active partition behavior

**Note**: The Fluss KV store for historical partitions may be empty initially (data cleaned up). In this case:
- All writes are treated as INSERTs initially
- This is acceptable because the tiering service will reconcile with existing Paimon data

#### 2.2.1 Historical Partition Lookup Performance

Considering that **point queries to historical partitions are relatively infrequent**, we can directly query data from Paimon. The latency (50-200ms) is acceptable for this use case.

**Optional Optimizations** (can be implemented incrementally based on actual usage patterns):

1. **LRU Cache**: Cache recently accessed keys in Tablet Server memory to speed up repeated lookups
2. **Preload / Warm-up**: Preload hot keys when partition transitions from active to historical
3. **Paimon File Index**: Enable bloom filter for PK columns to speed up key existence check

```properties
# Optional configuration (future optimization)
fluss.historical.cache.enabled = false  # Enable LRU cache
fluss.historical.cache.size = 256mb
fluss.historical.warmup.enabled = false # Enable preload
```

#### 2.3 Tablet Server Changes

- Accept writes to historical partitions through the normal write path
- No special routing needed - writes go through standard Fluss KV/Log path
- Tiering service handles writing data to Paimon (same as active partitions)

### Phase 3: Read Path Enhancement

#### Design Principle: Server-Side Paimon Abstraction

**All Paimon lookup/read operations are encapsulated within Fluss cluster**. Clients (Flink connector, Java client, etc.) do not need to know about Paimon or implement any fallback logic.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Client View (Simple)                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Flink Connector / Java Client                                          │
│         │                                                                │
│         │  lookup(partition, key) / scan(partition)                     │
│         ▼                                                                │
│   ┌─────────────────┐                                                   │
│   │  Fluss Cluster  │  ← Single entry point, transparent to clients     │
│   └─────────────────┘                                                   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    Server View (Handles Complexity)                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Fluss Tablet Server                                                    │
│         │                                                                │
│         ├── Active Partition ────────> Fluss KV / Log                   │
│         │                                                                │
│         └── Historical Partition ────> Paimon (encapsulated)            │
│                                                                          │
│   Benefits:                                                              │
│   • Clients stay simple, no Paimon dependency                           │
│   • Consistent behavior across all clients                               │
│   • Server-side caching and optimization                                 │
│   • Easier to evolve Paimon integration                                  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.1 Union Read Enhancement

```
Union Read Query
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│                    Partition Planning                     │
├─────────────────────────────────────────────────────────┤
│  Active Partitions    │  Historical Partitions           │
│  (from Fluss)         │  (from Paimon)                   │
│                       │                                   │
│  ┌─────────────────┐  │  ┌─────────────────────────────┐ │
│  │ Fluss Log +      │  │  │ Paimon Snapshot (full data) │ │
│  │ Paimon Snapshot  │  │  └─────────────────────────────┘ │
│  └─────────────────┘  │                                   │
└─────────────────────────────────────────────────────────┘
```

#### 3.2 Point Query Enhancement for PK Tables

The Fluss server handles the routing internally - clients simply call `lookup(partition, key)`:

```
Client: lookup(partition=dt-2024-01-01, key=xxx)
           │
           ▼
┌──────────────────────────────────────────────────────────────┐
│              Fluss Tablet Server (Internal Routing)          │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   Check Partition Status                                     │
│         │                                                    │
│         ├── Active Partition                                │
│         │         │                                          │
│         │         ▼                                          │
│         │   ┌─────────────────┐                              │
│         │   │ Query Fluss KV │                              │
│         │   └─────────────────┘                              │
│         │                                                    │
│         └── Historical Partition                             │
│                   │                                          │
│                   ▼                                          │
│             ┌────────────────────────────────────┐          │
│             │ Query Paimon (server-side)     │          │
│             │ - Use file index if available  │          │
│             │ - Or bucket scan               │          │
│             │ - Results cached on server     │          │
│             └────────────────────────────────────┘          │
│                                                              │
└──────────────────────────────────────────────────────────────┘
           │
           ▼
    Return result to client (transparent)
```

#### 3.3 Flink Connector Changes

Since Paimon fallback is handled server-side, Flink connector changes are minimal:

- `FlinkLookupFunction`: No changes needed - server handles historical partition lookup
- `FlinkTableSource`: No changes needed for union read - server returns combined results
- Partition metadata APIs may need to include historical partition info for planning

### Phase 4: Tiering Service Enhancement

#### 4.1 Historical Partition Tiering

- Support tiering data directly to Paimon for expired partitions
- Ensure data consistency between late writes and existing Paimon data
- Handle concurrent writes to the same historical partition

## Compatibility, Deprecation, and Migration Plan

### Backward Compatibility

1. **Automatic enablement**: Historical partition support is automatically enabled for tables with `datalake.enabled = true` AND auto-partitioning. No configuration changes needed.
2. **Metadata Compatibility**: New historical partition metadata is additive; old clients will ignore it
3. **Wire Protocol**: New APIs are additive; existing clients continue to work

### Migration Plan

#### For Existing Tables

Existing datalake-enabled auto-partitioned tables will automatically get historical partition support after cluster upgrade. No manual migration needed.

#### For New Tables

Historical partition support is enabled by default for new tables created with:
- `datalake.enabled = true`
- Auto-partitioning enabled

### Deprecation

No existing features are deprecated.

## Test Plan

### Unit Tests

1. **HistoricalPartitionManager Tests**
   - Test partition expiration with lake data tracking
   - Test historical partition metadata persistence and recovery
   - Test metadata cleanup when Paimon partition expires (sync with lake)

2. **Write Path Tests**
   - Test late data writes to expired partitions
   - Test concurrent writes to historical partitions
   - Test tiering of historical partition data

3. **Read Path Tests**
   - Test union read including historical partitions
   - Test point query fallback to Paimon
   - Test partition pruning with historical partitions

### Integration Tests

1. **End-to-End Tiering Tests**
   - Create auto-partitioned table with Paimon tiering
   - Write data, let partitions expire
   - Write late data to expired partitions
   - Verify data is tiered to Paimon correctly

2. **Query Tests**
   - Verify union read returns data from both Fluss and Paimon
   - Verify point queries return correct results for expired partitions
   - Verify Flink SQL queries work correctly with historical partitions

3. **Failure Recovery Tests**
   - Test coordinator failover with historical partition metadata
   - Test tablet server failover during historical partition writes
   - Test tiering service recovery for historical partitions

### Performance Tests

1. **Lookup Latency**
   - Measure point query latency for expired partitions (Paimon fallback)
   - Compare with active partition lookup latency

2. **Write Throughput**
   - Measure throughput for late data writes to expired partitions
   - Ensure no significant impact on active partition writes

### Compatibility Tests

1. **Rolling Upgrade Test**
   - Upgrade cluster from old version to new version
   - Verify existing tables continue to work
   - Verify new features work after enabling

2. **Client Compatibility Test**
   - Test old clients with new server
   - Test new clients with old server

## Implementation Status

### Core Framework (Completed)

The following components have been implemented as part of the core framework:

1. **PartitionStatus Enum** (`fluss-common/src/main/java/org/apache/fluss/metadata/PartitionStatus.java`)
   - Defines `ACTIVE` and `HISTORICAL` partition states
   - Used to track partition lifecycle

2. **HistoricalPartitionException** (`fluss-common/src/main/java/org/apache/fluss/exception/HistoricalPartitionException.java`)
   - Runtime exception thrown when operations target historical partitions
   - Contains partition ID and name for routing to lake

3. **PartitionMetadata Enhancement** (`fluss-server/src/main/java/org/apache/fluss/server/metadata/PartitionMetadata.java`)
   - Added `PartitionStatus` field to track partition state
   - Added `isHistorical()` and `withStatus()` methods

4. **TabletServerMetadataCache Enhancement** (`fluss-server/src/main/java/org/apache/fluss/server/metadata/TabletServerMetadataCache.java`)
   - Added partition status tracking map
   - Added `isHistoricalPartition()`, `getPartitionStatus()`, `updatePartitionStatus()` methods

5. **ServerMetadataCache Interface** (`fluss-server/src/main/java/org/apache/fluss/server/metadata/ServerMetadataCache.java`)
   - Added `isHistoricalPartition()` method with default implementation

6. **HistoricalPartitionManager** (`fluss-server/src/main/java/org/apache/fluss/server/coordinator/HistoricalPartitionManager.java`)
   - Manager for tracking historical partitions on Coordinator
   - Handles partition status transitions and lake sync

7. **PaimonHistoricalPartitionHandler** (`fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/historical/PaimonHistoricalPartitionHandler.java`)
   - Paimon-specific handler for historical partition operations
   - Provides lookup, scan, and partition expiration check methods

8. **Replica Read/Write Path** (`fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java`)
   - Added `isHistoricalPartition()` method to check partition status
   - Modified `lookups()` to throw `HistoricalPartitionException` for historical partitions
   - Modified `putRecordsToLeader()` to throw `HistoricalPartitionException` for historical partitions

9. **AutoPartitionManager Integration** (`fluss-server/src/main/java/org/apache/fluss/server/coordinator/AutoPartitionManager.java`)
   - Modified to mark partitions as historical for lake tables instead of dropping

10. **Error Protocol** (`fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/Errors.java`)
    - Added `HISTORICAL_PARTITION_EXCEPTION` error code (63) for client-side handling

11. **Lake Handler Interface** (`fluss-server/src/main/java/org/apache/fluss/server/lake/`)
    - Created `LakeHistoricalPartitionReader` interface for historical partition lookups
    - Created `LakeHistoricalPartitionReaderFactory` interface for reader creation

### Remaining Work

1. **Partition Status Sync**: Implement coordinator-to-tablet-server partition status synchronization via UpdateMetadataRequest
2. **Lake Handler Implementation**: Implement `PaimonHistoricalPartitionReader` that uses `PaimonHistoricalPartitionHandler`
3. **Client-Side Handling**: Handle `HISTORICAL_PARTITION_EXCEPTION` in Flink connector to route lookups to lake
4. **Lake Expiration Sync**: Implement periodic sync with Paimon to detect expired partitions
5. **Integration Tests**: End-to-end tests for the feature

### Implementation Notes

**Current Status (2026-01-29)**:
- Core framework complete: exception handling, error codes, partition status tracking
- Write path: Historical partitions allow writes through normal Fluss path, tiering service handles Paimon
- Read path: Lookups to historical partitions throw `HistoricalPartitionException` (error code 63)
- FIP document updated to reflect simplified write path design

**Key Files Modified**:
- `fluss-common/.../exception/HistoricalPartitionException.java` - extends ApiException
- `fluss-common/.../metadata/PartitionStatus.java` - ACTIVE/HISTORICAL enum
- `fluss-rpc/.../protocol/Errors.java` - added HISTORICAL_PARTITION_EXCEPTION (63)
- `fluss-server/.../replica/Replica.java` - isHistoricalPartition(), lookups() throws exception
- `fluss-server/.../metadata/TabletServerMetadataCache.java` - partition status tracking
- `fluss-server/.../coordinator/HistoricalPartitionManager.java` - coordinator-side management
- `fluss-server/.../coordinator/AutoPartitionManager.java` - mark as historical instead of drop
- `fluss-server/.../lake/LakeHistoricalPartitionReader.java` - interface for lake lookups
- `fluss-lake/.../historical/PaimonHistoricalPartitionHandler.java` - Paimon-specific handler
