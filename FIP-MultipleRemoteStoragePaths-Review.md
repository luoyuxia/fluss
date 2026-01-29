# FIP Review: Multiple Remote Storage Paths Support

## Overview

This FIP proposes support for multiple remote storage paths to overcome single-path throughput limitations (e.g., OSS's 20 Gbit/s internal / 10 Gbit/s public bandwidth limit per account).

---

## Strengths

1. **Clear Motivation**: The throughput bottleneck problem is well-articulated with concrete numbers.
2. **Backward Compatibility**: Maintains `remote.data.dir` as fallback when `remote.data.dirs` is not configured.
3. **Flexible Strategies**: Round Robin and Weighted Round Robin cover common distribution needs.
4. **Per-Bucket Authentication**: Allows different credentials for different storage accounts/buckets.
5. **Immutable Path Assignment**: Once a table/partition is assigned a path, it doesn't change - this simplifies data management.

---

## Concerns & Suggestions

### 1. Configuration Naming Inconsistency

**Issue**: Mixed singular/plural naming patterns:
- `remote.data.dir` (existing, singular)
- `remote.data.dirs` (new, plural)
- `remote.data.dirs.weights` vs `remote.data.dir.weights` (typo in description?)

**Suggestion**: Ensure consistent naming. The description mentions `remote.data.dir.weights` but the config is `remote.data.dirs.weights`.

---

### 2. Weight Validation & Error Handling

**Issue**: The FIP states weights must match the size of `remote.data.dirs`, but doesn't specify:
- What happens if sizes don't match?
- What if weights contain zero or negative values?
- What if `remote.data.dirs` is empty but weights are configured?

**Suggestion**: Add explicit validation rules:
```java
// Validation should include:
// 1. weights.size() == dirs.size() when WEIGHTED_ROUND_ROBIN
// 2. All weights > 0
// 3. At least one dir configured when strategy is not default
```

---

### 3. Dynamic Reconfiguration Risks

**Issue**: `RemoteDirDynamicLoader` supports runtime reconfiguration, but:
- What happens to in-flight operations during reconfiguration?
- Can paths be removed? If a path is removed but tables still reference it, data becomes inaccessible.
- How does the selector state (e.g., round-robin counter) get preserved or reset?

**Suggestion**: 
- **Only allow adding new paths**, not removing existing ones (or require migration first)
- Document the behavior during reconfiguration
- Consider atomic swapping of the selector with proper synchronization

---

### 4. Missing Failover & Health Check

**Issue**: No mention of:
- What happens if a remote storage path becomes unavailable?
- How to detect unhealthy paths and skip them in selection?
- Retry strategy when writing to a selected path fails?

**Suggestion**: Consider adding:
```java
public interface RemoteDirSelector {
    FsPath nextDataDir();
    
    // Future enhancement
    void markUnhealthy(FsPath path, Duration duration);
    boolean isHealthy(FsPath path);
}
```

---

### 5. Security Token Management Complexity

**Issue**: The `SecurityTokenReceiver` now manages credentials for multiple paths with `Map<FSKey, Credentials>`. Concerns:
- Token refresh timing may differ per path
- Memory overhead with many paths
- Cache eviction policy not specified

**Suggestion**: 
- Define maximum supported paths
- Implement LRU or TTL-based cache eviction
- Document token refresh behavior per path

---

### 6. Table Path in GetFileSystemSecurityTokenRequest

**Issue**: Adding `table_path` to `GetFileSystemSecurityTokenRequest`:
- For partitioned tables, is it the table path or partition path?
- What about operations that span multiple tables (e.g., tiering service)?

**Suggestion**: Clarify:
- Whether this should be `table_path` or `physical_path` (the actual remote storage path)
- How batch operations across tables should request tokens

---

### 7. Metadata Storage & Migration

**Issue**: Remote path stored in ZooKeeper as table/partition metadata:
- How large can this metadata grow with many partitions?
- Is there a migration plan for existing tables (they have no explicit path stored)?

**Suggestion**: 
- For existing tables without explicit path, default to `remote.data.dir`
- Consider storing path index/reference instead of full path string to save space

---

### 8. Load Balancing Granularity

**Issue**: Path selection happens at:
- Table creation (non-partitioned)
- Partition creation (partitioned)

This means all buckets of a table/partition go to the same path.

**Question**: Should bucket-level distribution be considered for better load balancing?

**Trade-off**: Bucket-level would provide finer load balancing but complicates metadata and file management.

---

### 9. Monitoring & Observability

**Issue**: No mention of metrics for:
- Distribution of tables/partitions across paths
- Per-path throughput/latency
- Path health status

**Suggestion**: Add metrics:
```
fluss_remote_storage_path_tables_total{path="oss://bucket1/..."} 
fluss_remote_storage_path_bytes_written_total{path="oss://bucket1/..."}
fluss_remote_storage_path_selector_invocations_total{strategy="round_robin"}
```

---

### 10. Documentation for `RemoteDirSelector.nextDataDir()`

**Issue**: The JavaDoc is incomplete (cut off in the FIP).

**Suggestion**: Complete the documentation:
```java
/**
 * Returns the next remote data directory path to use.
 *
 * <p>This method should implement the selection strategy (e.g., round-robin, weighted
 * round-robin) to choose from the available remote data directories.
 *
 * <p>If {@code remote.data.dirs} is not configured, returns {@code remote.data.dir}.
 *
 * @return the next FsPath to use for storing remote data
 */
FsPath nextDataDir();
```

---

## Questions for Clarification

1. **Cross-region paths**: Are paths expected to be in the same region? Different regions have latency implications.

2. **Path format validation**: Should paths be validated for scheme consistency (all OSS, all S3, or mixed)?

3. **Tiering service integration**: How does the tiering service (Lake integration) work with multiple paths? Does it need path-aware configuration?

4. **Client-side caching**: Should clients cache the path for a table to avoid repeated metadata lookups?

---

## Summary

| Aspect | Assessment |
|--------|------------|
| **Design** | Good overall architecture with clear separation of concerns |
| **Backward Compatibility** | Well handled with fallback to `remote.data.dir` |
| **Completeness** | Missing failover, monitoring, and some edge case handling |
| **Security** | Per-bucket auth is good; token management needs more detail |
| **Operability** | Dynamic reconfiguration needs safety constraints |

**Recommendation**: Address the concerns around **dynamic reconfiguration safety** (item 3) and **failover handling** (item 4) before implementation, as these could cause production issues.
