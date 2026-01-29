# Reply: FIP - Multiple Remote Storage Paths

Hi,

Thanks for the detailed FIP. I have a few questions and concerns:

---

## 1. Mixed Scheme Support

Does `remote.data.dirs` support paths with different schemes? For example:

```
remote.data.dirs: oss://bucket1/fluss-data, s3://bucket2/fluss-data
```

If mixed schemes are supported, this significantly increases complexity for authentication and file system handling. If not, should we add validation to enforce consistent schemes?

---

## 2. Client-Side Token Management & Request Structure

A few points regarding the token request/response changes:

### 2.1 Should `GetFileSystemSecurityTokenRequest` include partition?

The FIP adds `table_path` to the request, but since different partitions of the same table may reside on different remote paths (and thus require different tokens), should the request also include partition information?

```protobuf
message GetFileSystemSecurityTokenRequest {
  optional PbTablePath table_path = 1;
  optional string partition_name = 2;  // Should this be added?
}
```

### 2.2 Heads-up: Client-side complexity increase

Just a reminder that the client's `DefaultSecurityTokenManager` will become more complex. Previously, it only sent a fixed token request with no content. Now it needs to dynamically construct different token requests based on table/partition.

This is not a blocker, but worth noting during implementation:  
- Client needs to know which partitions it will access
- Token caching strategy (per remote path? per partition?)
- Potential increase in token request frequency

---

## 3. Remote Dir Assignment for Partitioned Tables

I want to confirm my understanding: For a partitioned table, does the table itself have a remote dir, AND each partition also has its own remote dir?

Or is it:
- Non-partitioned table → table-level remote dir
- Partitioned table → only partition-level remote dirs (no table-level)?

Please clarify the metadata structure.

---

## 4. Backward Compatibility with Old Clients

Can old clients (without table path in token request) still read data from new clusters?

My suggestion: For RPCs without table information, the server returns a token for the default `remote.data.dir`. This should cover most backward compatibility cases since:
- Existing tables created before this feature would use the default path
- Old clients typically access existing tables

Does this align with your backward compatibility plan?

---

## 5. "Unified Authentication" Clarification

The FIP mentions:
> "if unified authentication is adopted, the existing configuration parameters can be reused"

What exactly does "unified token" mean here? How can the server return a single unified token that works for different OSS buckets (e.g., `bucket1` and `bucket2`) when they may have:
- Different access keys
- Different endpoints
- Different regions

I assume "unified" means all paths share the same credentials, but this seems like a rare production scenario. Could you clarify this?

---

Thanks,
[Your Name]
