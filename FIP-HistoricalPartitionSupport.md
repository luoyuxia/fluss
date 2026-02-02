# FIP: Improve Historical Partition Support for Fluss Auto-Partitioned Tables with integrating with DataLake

## Motivation
Currently, for Fluss auto-partitioned tables with data lake (e.g., Paimon), when Fluss partitions expire but still exist in the data lake, several issues arise:

### Problem 1: Data Loss for Writes to Expired Partitions
When data arrives late and is written to an already-expired partition:
- The data is written to Fluss successfully
- Fluss automatically cleans up the partition data but retains the partition metadata
- Although the partition exists in Paimon, the data cannot be tiered to Paimon
- This results in silent data loss

### Problem 2: Point Query Inconsistency for Primary Key Tables
For primary key tables with auto-partitioning:
- Point queries (lookup) only query data from Fluss cluster
- When a partition expires on Fluss, even though the partition data exists in Paimon, point queries cannot find it
- Users see inconsistent results between point queries and batch reads
- This is extremely confusing for users who expect the data to be available

### Problem 3: ZooKeeper Pressure from Historical Partition Metadata
Maintaining all historical partition bucket's ZooKeeper nodes for extended periods creates significant pressure on ZooKeeper:
- Each historical partition requires dedicated ZooKeeper metadata nodes
- As the number of historical partitions grows, ZooKeeper becomes a bottleneck
- Long-term retention of historical partition metadata increases operational overhead
- This approach is not scalable for systems with many auto-partitioned tables

This FIP proposes to make Paimon historical partitions truly serve as the data source for Fluss auto-partitioned tables after the corresponding Fluss partitions expire. Specifically:
- Writes to expired partitions by auto partition mechanism should still be tiered to data lake (e.g., Paimon)
- Point queries for PK tables should fallback to Paimon for expired partitions
- Address ZooKeeper pressure by optimizing historical partition metadata retention strategies

## Public Interfaces

todo: Introduce a new lookup interface to lookup row from data-lake via key

## Proposed Changes

## Basic Approach

The core idea of this FIP is to introduce a new `DATALAKE_HISTORICAL` partition state that enables seamless integration between Fluss and its connected data lake (e.g., Paimon).
When partitions expire in Fluss, they transition to the `DATALAKE_HISTORICAL` state with data deleted in Fluss, allowing the data lake to serve as the authoritative source for historical partition data. This approach addresses three critical issues:

1. **Data Loss Prevention**: Prevents silent data loss when writes occur to partitions that have expired in Fluss but still exist in the lake
2. **Query Consistency**: Ensures consistent query results by routing operations targeting expired partitions to the data lake where the data actually resides
3. **ZooKeeper Scalability**: Reduces ZooKeeper pressure by implementing efficient metadata management for historical partitions, with configurable retention policies to balance between operational needs and resource utilization

The solution maintains partition metadata in Fluss while routing operations to the data lake for historical partitions, ensuring seamless user experience while preserving data integrity. The system checks both Fluss and data lake TTL configurations before fully deleting partitions.


### 1. Coordinator Server Modifications

#### Partition Lifecycle Management
The coordinator continuously checks whether partitions on Fluss should be automatically TTL'd according to Fluss's own partition TTL rules. If a partition should be TTL'd but it's a data lake enabled table, the system checks whether the partition should be TTL'd on the data lake side:

- For Paimon, the system uses the user-defined partition TTL rules (e.g., `partition.expiration-time` = '7 d', `partition.expiration-check-interval` = '1 d', `partition.timestamp-formatter` = 'yyyyMMdd') to determine if it should be TTL'd. Note that Iceberg does not support this natively, so in the future we may consider introducing a procedure to force partition TTL.

- If the partition should be TTL'd on the data lake side, the corresponding partition is directly deleted and its data is removed from ZooKeeper as well.

- Otherwise, the partition state is set to `DATALAKE_HISTORICAL` and the corresponding TabletServer is notified, allowing the TabletServer to clean up the local data.

This approach introduces a new `DATALAKE_HISTORICAL` partition state alongside the existing `ACTIVE` state. When partitions expire in Fluss but still exist in the data lake (Paimon), the partition data in Fluss is cleaned up but the partition metadata remains in Fluss and it transitions to `DATALAKE_HISTORICAL` state. The system checks the data lake's TTL configuration and only fully deletes partitions when they have expired in both Fluss and the data lake. This ensures the data lake serves as the authoritative source for historical partition data.

Additionally, to address ZooKeeper pressure concerns from maintaining historical partition metadata, the system implements configurable retention policies for DATALAKE_HISTORICAL partitions in ZooKeeper. These policies include:

- Automatic cleanup of historical partition metadata after configurable time periods
- Configurable thresholds for the number of historical partitions maintained in ZooKeeper
- Lazy loading mechanisms to temporarily load historical partition metadata when needed
- Efficient aggregation of historical partition metadata to reduce the total number of ZooKeeper nodes required

These mechanisms allow administrators to tune the balance between operational requirements and resource utilization based on their specific deployment requirements.


-- todo: may update the state to zk, and mark the state in update metadata request

### 2. Tablet Server Modifications

#### 2.1 Log Table Handling
For log tables (append-only tables without primary keys), when a partition is marked as DATALAKE_HISTORICAL:
- **Notification Handling**: The Tablet Server receives notification that the partition has been marked as DATALAKE_HISTORICAL
- **Log TTL Logic**: If the corresponding log's endOffset has already been synced to the data lake, the Tablet Server can directly TTL the corresponding log segments
- **Low-Frequency Write Handling**: If new data is subsequently written to this partition, the data is written normally, but considering the lower frequency of data writes, if the log's endOffset has been synced to the data lake, the Tablet Server can also TTL the corresponding log segments
- **Change Log Generation**: To handle PUT requests and generate complete change logs, the system needs to probe data from Paimon to generate -U data, and then generate +U change logs
- **Tiering**: The tiering service continues to move log segments from Fluss to the data lake (Paimon)
- **Query Operations**: Both streaming and batch queries can access data from historical partitions through the data lake
- **Segment Management**: Historical partitions with low update frequency will have their inactive log segments periodically uploaded to remote storage and rely on remote TTL for cleanup

### 2.2. Primary Key Table Handling
For primary-key tables with KV full data + change log, when a partition is marked as DATALAKE_HISTORICAL:
- **Notification Handling**: The Tablet Server receives notification that the partition has been marked as DATALAKE_HISTORICAL
- **Log TTL Logic**: If the corresponding log's endOffset has already been synced to the data lake, the Tablet Server can directly TTL the corresponding log segments
- **Change Log Generation**: If new data is subsequently written to this partition, the data is written normally，to handle PUT/DELETE requests and generate complete change logs, the system needs to probe data from Paimon to generate -U data, and then generate +U change logs
- **Memory State Maintenance**: The system maintains this part of data in memory because subsequent PUT requests may not be in Paimon but in this change log
- **Tiering Service Coordination**: When the tiering service syncs this part of data, the system needs to remove the corresponding offset data from memory
- **Recovery Process**: During recovery, apply change logs starting from the latest synced offset to Paimon until reaching the latest change log to rebuild the part of kv data in memory
- **Query Routing**: When point queries target historical partitions, route them to the data lake to maintain consistency

## Compatibility, Deprecation, and Migration Plan

### Backward Compatibility
- Existing applications will continue to work without changes
- Only when partitions become historical will the new behavior take effect, and still no any backward compatibility issue
- All existing APIs remain compatible

### Migration Strategy
- No manual migration required
- Existing data in expired partitions will be preserved in Paimon
- New writes to historical partitions will follow the new behavior automatically
- Applications can gradually adapt to the improved consistency model

## Test Plan

### Integration Tests
- End-to-end tests for write operations to historical partitions
- Point query routing from Fluss to Paimon for historical partitions

## Rejected Alternatives

### Alternative 1: Immediate Partition Deletion
写老分区就直接 reject 写入，但是对于 lookup，如果对应的分区不存在，且是湖表，尝试直接在 client 侧进行 lookup


湖表原地升级：
- 非分区表
    - 非主键表，直接创建对应的 replica 即可
    - 主键表，需要 scan 出底层的数据，然后 apply 到 rocksdb

- 分区表
    - 非主键表，直接创建对应的 partition +  replica 即可
    - 主键表
        - 支持所有类型分区，需要 scan 所有的 partition & apply 数据，成本会比较高
        - 只支持时间分区（partition.expiration-strategy）：，只 scan 最新partition & apply 数据成本会比较高，历史分区不支持写入