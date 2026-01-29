# Design Review: Lake Tiering Status System Table

## Overview

This document reviews the proposed design for implementing a `sys.lake_tiering_status` system table to expose lake tiering status information via Flink SQL and Admin API.

---

## Summary

**Overall Assessment**: The design is well-structured and addresses a real operational visibility gap. The approach is pragmatic and leverages existing patterns in the codebase. Below are detailed observations organized by category.

---

## Strengths

### 1. Clear Problem Statement
The motivation clearly identifies the information loss in the current error path:
```
FailedTieringEvent { failReason } → TieringSourceEnumerator (logged only) → Heartbeat (no error field)
```

### 2. Consistent with Existing Patterns
- The `sys` virtual database approach aligns with the existing `$changelog` virtual table pattern in [FlinkCatalog.java](file:///Users/yuxia/Projects/fluss/fluss/fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/catalog/FlinkCatalog.java)
- Using `AdminReadOnlyGateway` for the new API is correct since this is a read-only status query
- Error message truncation (2k-4k chars) follows the pattern in [ApiError.java](file:///Users/yuxia/Projects/fluss/fluss/fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/ApiError.java#L36) which uses `MAX_ERROR_MESSAGE_LENGTH = 2048`

### 3. Backward Compatibility
Adding `error_message` as an optional protobuf field in `PbHeartbeatReqForTable` is the correct approach for backward compatibility.

---

## Architectural Concerns

### 1. State Persistence and Coordinator Failover

**Issue**: The design stores error state in memory (`tieringFailMessages`, `tieringFailTimes` Maps in `LakeTableTieringManager`). What happens when the Coordinator restarts?

**Current Behavior Analysis**: Looking at [LakeTableTieringManager.java](file:///Users/yuxia/Projects/fluss/fluss/fluss-server/src/main/java/org/apache/fluss/server/coordinator/LakeTableTieringManager.java), the `initWithLakeTables()` method reconstructs state from persistent metadata but doesn't preserve historical error information.

**Recommendation**: 
- Consider persisting last error state to ZooKeeper or the metadata store, similar to how `tableLastTieredTime` is persisted
- Alternatively, document this as a known limitation (error info lost on coordinator restart)

### 2. Thread Safety with Multiple Maps

**Issue**: The design proposes adding two new Maps (`tieringFailMessages`, `tieringFailTimes`) alongside existing Maps. The current implementation uses `@GuardedBy("lock")` annotation and `inLock()` pattern.

**Recommendation**: Ensure all new Maps are:
1. Declared with `@GuardedBy("lock")` annotation
2. All accesses are wrapped in `inLock()` calls
3. Properly cleaned up in `removeLakeTable()` method (already mentioned in design)

### 3. Error State Lifecycle Clarification

The design mentions "cleared when finishTableTiering() succeeds" - this needs clarification:

**Question**: Should `last_error` and `last_error_time` be cleared when tiering succeeds, or should they preserve the last error until the next failure?

**Recommendation**: Consider preserving the last error info even after success. This provides valuable debugging information:
- "Table X is healthy now, but last failed 2 hours ago due to auth error"
- Users can then correlate with when they fixed the issue

---

## API Design Feedback

### 1. `ListTieringStatusesRequest` Table Path Filter

```protobuf
message ListTieringStatusesRequest {
  optional PbTablePath table_path = 1;  // query all if empty, or specific table if specified
}
```

**Recommendation**: Consider adding database-level filtering:
```protobuf
message ListTieringStatusesRequest {
  optional string database_name = 1;  // filter by database
  optional PbTablePath table_path = 2; // specific table (overrides database filter)
}
```

This allows `SELECT * FROM sys.lake_tiering_status WHERE database_name = 'my_db'` to be pushed down.

### 2. Status Enum vs String

The schema shows `status` as STRING type. Consider whether the status values should be documented as an enum in the protobuf for type safety:

```protobuf
enum TieringStatus {
  NEW = 0;
  INITIALIZED = 1;
  SCHEDULED = 2;
  PENDING = 3;
  TIERING = 4;
  TIERED = 5;
  FAILED = 6;
}
```

### 3. API Key Numbering

The design proposes `LIST_TIERING_STATUSES(1053, 0, 0, PUBLIC)`. Looking at [ApiKeys.java](file:///Users/yuxia/Projects/fluss/fluss/fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/ApiKeys.java#L83), `PREPARE_LAKE_TABLE_SNAPSHOT(1052)` is indeed the last one, so 1053 is correct.

---

## Implementation Details

### 1. TieringSourceEnumerator Changes

The proposed change from:
```java
Map<Long, Long> failedTableEpochs
```
to:
```java
Map<Long, Tuple2<Long, String>> failedTableEpochs  // (epoch, errorMessage)
```

**Alternative Consideration**: Creating a small `FailedTableInfo` class might be cleaner than `Tuple2`:
```java
private static class FailedTableInfo {
    final long epoch;
    final String errorMessage;
}
```

This improves readability and allows future extensions.

### 2. TabletService Default Implementation

The design mentions TabletService will forward requests to Coordinator. Looking at [AdminReadOnlyGateway.java](file:///Users/yuxia/Projects/fluss/fluss/fluss-rpc/src/main/java/org/apache/fluss/rpc/gateway/AdminReadOnlyGateway.java), this is consistent with the pattern where TabletServer forwards admin operations.

**Recommendation**: The forwarding logic should handle:
- Coordinator unavailability gracefully
- Return appropriate error response if forwarding fails

### 3. Error Message Truncation Location

The design mentions truncating to 2k-4k characters in TieringSourceEnumerator before sending to Coordinator.

**Recommendation**: Truncate at the server-side (CoordinatorService) rather than client-side:
1. Ensures consistent truncation regardless of client version
2. Allows server to control policy centrally
3. Follow the pattern in `ApiError.fromThrowable()` which truncates on server

---

## FlinkCatalog Virtual Database Implementation

### 1. sys Database Handling

The design proposes handling `sys` database virtually. Looking at [FlinkCatalog.java](file:///Users/yuxia/Projects/fluss/fluss/fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/catalog/FlinkCatalog.java), you'll need to modify:

```java
@Override
public List<String> listDatabases() {
    // Add "sys" to the list
}

@Override
public boolean databaseExists(String databaseName) {
    if ("sys".equals(databaseName)) {
        return true;
    }
    // existing logic
}

@Override
public List<String> listTables(String databaseName) {
    if ("sys".equals(databaseName)) {
        return Arrays.asList("lake_tiering_status");
    }
    // existing logic
}
```

### 2. System Table Implementation Pattern

For `getTable()` on `sys.lake_tiering_status`, consider:
1. Return a `CatalogTable` with the defined schema
2. The table source will be implemented via `DynamicTableSourceFactory` (SPI)
3. The source calls `Admin.listTieringStatuses()` internally

---

## Testing Recommendations

### 1. Unit Tests
- `LakeTableTieringManagerTest`: Add tests for error state storage and retrieval
- Verify error state is cleared on table removal
- Verify error state behavior after tiering success

### 2. Integration Tests
- End-to-end test: Trigger tiering failure → Query `sys.lake_tiering_status` → Verify error visible
- Backward compatibility: Old Tiering Service without error field should still work
- Coordinator restart: Document expected behavior for error state

### 3. Test Scenarios
- Authentication failure during tiering
- Network timeout during tiering
- Schema mismatch errors
- Multiple tables with different statuses

---

## Minor Suggestions

### 1. Column Naming Convention
Consider using consistent naming:
- `last_error` → `last_error_message` (more explicit)
- Or follow existing patterns in the codebase

### 2. System Table Future Extensibility
The `sys` database approach allows future system tables:
- `sys.tablet_server_status`
- `sys.replication_status`
- etc.

Document this as a pattern for future features.

### 3. Metrics Integration
Consider adding a gauge metric for tables in FAILED state. This enables alerting via Prometheus without SQL queries.

---

## Conclusion

The design is solid and well-thought-out. The main areas requiring clarification or enhancement are:

1. **Coordinator failover handling** - clarify if error state persists
2. **Error state lifecycle** - decide if errors are cleared on success
3. **Database-level filtering** - consider adding to the API
4. **Truncation location** - recommend server-side truncation

The implementation path is clear and follows established patterns in the codebase.
