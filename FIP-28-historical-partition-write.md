# Historical Partition Write Support via Overflow Partition

## Motivation

With FIP-28, reading from historical cold partitions tiered to lake storage is well-supported. However, there are scenarios where late-arriving data needs to be written to an already-expired partition. Currently, once a partition expires and its metadata is deleted, writes to it are rejected.

A naive approach would be to retain metadata for all historical partitions, but this defeats the purpose of partition expiration — releasing cluster metadata pressure.

This proposal introduces a **single special "overflow" partition** that serves as the write target for all historical partition data, keeping metadata overhead to a minimum (only one extra partition) while still supporting writes, subscriptions, and partition-aware consumption.

## Design Overview

### 1. Overflow Partition

Introduce a single special partition (e.g., named `__overflow__`) per auto-partitioned primary key table with lake tiering enabled:

- **Always active**: This partition never expires and is always writable
- **Single instance**: Only one overflow partition exists per table, regardless of how many historical partitions have expired
- **Metadata lightweight**: Only adds one partition's metadata to the cluster, avoiding the pressure of retaining all historical partitions
- **Behaves like a regular partition**: The overflow partition uses standard bucketing (hash on primary key), standard write path, standard replication — no special routing or format changes needed
- **Partition identity from row data**: The partition column is already part of the table schema and stored in every row. Consumers and the tiering service can determine the original partition from the row data itself

### 2. Write Path

When a client writes data targeting an expired partition:

1. Client detects the target partition no longer exists
2. Client redirects the write to the overflow partition
3. The row already contains the original partition value in the partition column — no extra metadata needed

```
Write(partition="2024-01", key, value)
  → partition "2024-01" expired
  → write to __overflow__ partition (row already contains partition column = "2024-01")
```

### 3. Subscription (Log Consumption)

Subscribers can consume the overflow partition like any normal partition:

- Standard subscription by bucket with independent offsets
- The original partition is determined from the partition column in the row data

### 4. Tiering

The overflow partition is tiered to lake storage like any normal partition:

- The tiering service handles the overflow partition the same way as regular partitions
- When tiering to Paimon, records are written to their **original partition** in the lake table (based on the carried partition metadata), not to an "overflow" partition in Paimon
- This ensures the lake table maintains the correct partition layout for batch queries

### 5. Key Design Decisions

| Decision | Options | Recommendation |
|----------|---------|----------------|
| Overflow partition naming | `__overflow__` vs. `__historical__` | TBD |
| Client redirect behavior | Automatic vs. explicit error | Automatic redirect — transparent to user |
| Overflow partition creation timing | Table creation vs. first write to expired partition | Lazy creation on first write to expired partition (avoid overhead for tables that never need it) |
| Tiering target partition resolution | Based on partition column in row | Records routed to original partitions in lake |

## Open Questions

### 1. Offset Continuity

When a partition expires, both its metadata and log data are deleted, so the original partition's offsets are gone. However, **offset continuity between the original partition and the overflow partition is not required**.

**Key assumption**: By the time a write arrives for an expired partition and is redirected to the overflow partition, all data from the original partition has already been fully consumed by subscribers (either from Fluss before expiration or from the lake after tiering). The overflow partition only captures NEW late-arriving data after the partition has expired.

**Therefore**:
- Consumers subscribe to overflow buckets and start from offset 0
- The original partition is derived from the partition column in the row data
- There is no need to track or map offsets between the original partition and the overflow partition

### 2. Interaction with lake lookup

After overflow partition data is tiered to the original partition in Paimon, lake lookups (FIP-28) should be able to find this data. Need to verify the tiering correctly places data in the right Paimon partition.
