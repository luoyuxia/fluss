# Lake Lookup Implementation for Cold Partitions

## Overview

This implementation enables point lookups on expired/cold partitions where data has been tiered to lake storage (e.g., Paimon). When a partition doesn't exist in Fluss anymore but data is available in the lake, the client can fallback to querying the lake storage.

## Problem Statement

For auto-partitioned tables, when a partition expires and is dropped from Fluss, the partition's metadata (partitionId, bucket, leader info) no longer exists in the cluster. This makes it impossible for clients to route lookup requests using the normal path. However, the data is still available in the lake storage (Paimon) after tiering.

## Solution Design

### 1. Routing Strategy

Since expired partitions have no metadata in Fluss, we use **hash-based tablet server selection**:
- Hash key: `database_name.table_name[.partition_name]:bucket_id`
- Consistently routes requests to the same tablet server for the same table/partition/bucket combination

### 2. Request Separation

Lake lookup requests are separated from regular Fluss lookups:
- Different `LookupType` (LAKE_LOOKUP)
- Processed in separate batches
- Server uses independent thread pool (to be implemented) to avoid blocking regular lookups

## Changes Summary

### RPC Layer (fluss-rpc)

| File | Change |
|------|--------|
| `ApiKeys.java` | Added `LAKE_LOOKUP(1060, 0, 0, PUBLIC)` API key |
| `FlussApi.proto` | Added `LakeLookupRequest`, `LakeLookupResponse`, `PbLakeLookupReqForBucket`, `PbLakeLookupRespForBucket` messages |
| `TabletServerGateway.java` | Added `lakeLookup(LakeLookupRequest)` method |

### Client Layer (fluss-client)

| File | Change |
|------|--------|
| `LookupType.java` | Contains `LAKE_LOOKUP` enum value |
| `LakeLookupQuery.java` | New query class with `partitionName`, `bucketId`, `key` fields |
| `LookupClient.java` | Added `lakeLookup()` method |
| `LookupSender.java` | - Modified `groupByLeaderAndType()` for hash-based routing<br>- Added `sendLakeLookupRequest()`<br>- Added response/error handlers<br>- Added `LakeLookupBatch` helper class |
| `MetadataUpdater.java` | Added `getTabletServerForLakeLookup()` method for hash-based tablet server selection |
| `PrimaryKeyLookuper.java` | Contains `shouldFallbackToLakeLookup()` and `performLakeLookup()` methods |

### Server Layer (fluss-server)

| File | Change |
|------|--------|
| `TabletService.java` | Added `lakeLookup()` method with authorization |
| `ServerRpcMessageUtils.java` | Added `toLakeLookupData()`, `makeLakeLookupResponse()`, `makeLakeLookupErrorResponse()` |
| `ReplicaManager.java` | Added `lakeLookups()` method (placeholder) |

## Request Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT SIDE                               │
├─────────────────────────────────────────────────────────────────┤
│ 1. PrimaryKeyLookuper.lookup()                                  │
│    └─> PartitionNotExistException caught                        │
│    └─> shouldFallbackToLakeLookup() checks:                     │
│        - datalake enabled                                        │
│        - format is Paimon                                        │
│    └─> performLakeLookup() creates LakeLookupQuery               │
│                                                                  │
│ 2. LookupSender.groupByLeaderAndType()                          │
│    └─> For LAKE_LOOKUP: use hash-based tablet server selection  │
│        hash("db.table[.partition]:bucketId") % serverCount       │
│                                                                  │
│ 3. sendLakeLookupRequest()                                       │
│    └─> Build LakeLookupRequest with tablePath, partitionName    │
│    └─> Send to selected tablet server                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        SERVER SIDE                               │
├─────────────────────────────────────────────────────────────────┤
│ 1. TabletService.lakeLookup()                                   │
│    └─> Authorize READ permission                                │
│    └─> Forward to ReplicaManager                                │
│                                                                  │
│ 2. ReplicaManager.lakeLookups()                                 │
│    └─> TODO: Implement Paimon lookup                            │
│    └─> Return results (currently returns null for all keys)     │
└─────────────────────────────────────────────────────────────────┘
```

## Protocol Messages

### LakeLookupRequest

```protobuf
message LakeLookupRequest {
  required PbTablePath table_path = 1;
  optional string partition_name = 2;      // null for non-partitioned tables
  repeated PbLakeLookupReqForBucket buckets_req = 3;
}

message PbLakeLookupReqForBucket {
  required int32 bucket_id = 1;
  repeated bytes keys = 2;
}
```

### LakeLookupResponse

```protobuf
message LakeLookupResponse {
  repeated PbLakeLookupRespForBucket buckets_resp = 1;
}

message PbLakeLookupRespForBucket {
  required int32 bucket_id = 1;
  optional int32 error_code = 2;
  optional string error_message = 3;
  repeated PbValue values = 4;             // null value means key not found
}
```

## Configuration Requirements

For lake lookup to work, the table must have:
- `table.datalake.enabled = true`
- `table.datalake.format = paimon` (only Paimon is supported for now)

## Future Work

The current implementation provides complete infrastructure. To enable actual Paimon lookups:

1. **ReplicaManager Enhancement**
   - Get lake storage configuration from cluster config
   - Create `LakeStorage` instance using Paimon plugin
   - Implement key-based lookup against Paimon table

2. **Thread Pool Isolation**
   - Create dedicated thread pool for lake lookups
   - Prevent slow lake I/O from blocking regular Fluss lookups

3. **Caching**
   - Add local cache for lake lookup results
   - Reduce latency for repeated lookups

4. **Metrics**
   - Add metrics for lake lookup latency, success rate
   - Track fallback rate from regular to lake lookup

## Testing

Unit tests and integration tests should cover:
- Hash-based routing consistency
- Request/response serialization
- Fallback logic when partition doesn't exist
- Error handling for various failure scenarios
