# KV Overflow Partition Support — Implementation Plan

## Current Status (2026-03-09)

**Branch**: `support-write-lookup-historial-partition`  
**Latest commit**: `08dd4f9eb` — "support put for overflow partition"  
**Base**: `upstream/main` at `15f838288`

### Completed (9/10 todos done):
- ✅ All core implementation (RocksDB CF, WriteBatch, KvBatchWriter, KvPreWriteBuffer, OverflowWriteContext, KvTablet, Replica, ReplicaManager)
- ✅ Paimon fallback for historical data lookups
- ✅ IT case `OverflowPartitionChangelogITCase` created (compiles, NOT yet run)
- ⬜ `tiering-cleanup` — lower priority optimization (drop CFs after tiering)

### Uncommitted Changes:
1. **Modified** (tracked, unstaged):
   - `fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/PaimonLakeTableLookuper.java` — minor formatting
   - `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/OverflowPartitionWriteITCase.java` — test adjustments
   - `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/flink/FlinkOverflowPartitionLogTableITCase.java` — test adjustments
   - `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/testutils/FlinkPaimonTieringTestBase.java` — test adjustments
2. **New** (untracked):
   - `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/OverflowPartitionChangelogITCase.java` — 387 lines

### Build / Test Status:
- `mvn compile -pl fluss-server -am` ✅ passes
- `mvn compile test-compile -pl fluss-lake/fluss-lake-paimon -am -Dcheckstyle.skip=true` ✅ passes
- Unit tests (RocksDBKvTest, KvPreWriteBufferTest, KvTabletTest, ReplicaTest, ReplicaManagerTest) ✅ all pass
- IT case NOT yet run (needs full cluster env): `mvn test -pl fluss-lake/fluss-lake-paimon -Dtest=OverflowPartitionChangelogITCase -Dcheckstyle.skip=true`
- Note: `-Dcheckstyle.skip=true` needed due to pre-existing fluss-client issue (`UnusedImports: PartitionNotExistException` in `PrimaryKeyLookuper.java`)

---

## Problem
When overflow records from multiple original partitions land in a single overflow tablet, the in-memory `KvPreWriteBuffer` uses raw pk bytes as keys. Two records from different original partitions (e.g., `dt=2020, id=1` and `dt=2019, id=1`) produce identical pk bytes, causing key collisions.

## Approach
Maintain a **separate `KvPreWriteBuffer` + RocksDB column family per original partition**. An `OverflowWriteContext` manages this mapping. During `processKvRecords`, the original partition name is extracted from `BinaryRow` and used to route to the correct `(buffer, CF)` pair. When data is not found in buffer or RocksDB, fall back to Paimon (data lake) via `LakeTableLookuper`.

```
Lookup chain: buffer → RocksDB CF → Paimon (via LakeTableLookuper)

OverflowWriteContext:
  partitionBuffers:
    "2020" → KvPreWriteBuffer + CF handle
    "2019" → KvPreWriteBuffer + CF handle
  lakeLookuper → PaimonLakeTableLookuper (for historical data)

RocksDB:
  CF "overflow_2020":  id=1 → value
  CF "overflow_2019":  id=1 → value
```

---

## All Changed Files (Complete List)

### Files committed in `08dd4f9eb` (already in git):

| File | Type | What Changed |
|------|------|--------------|
| `fluss-server/.../kv/rocksdb/RocksDBWriteBatchWrapper.java` | Modified | CF-aware `put(cf, key, value)` and `delete(cf, key)` methods |
| `fluss-server/.../kv/KvBatchWriter.java` | Modified | Default CF-aware `put` and `delete` methods in interface |
| `fluss-server/.../kv/rocksdb/RocksDBKv.java` | Modified | `Map<String, ColumnFamilyHandle>` cache, `getOrCreateColumnFamily`, CF-aware get/put/delete, `dropColumnFamily`, updated `close()` |
| `fluss-server/.../kv/prewrite/KvPreWriteBuffer.java` | Modified | Optional `@Nullable ColumnFamilyHandle`, new constructor overload, CF-aware flush routing |
| `fluss-server/.../kv/overflow/OverflowWriteContext.java` | **NEW** | Per-partition `{KvPreWriteBuffer, ColumnFamilyHandle}` management, partition name extraction, Paimon fallback |
| `fluss-server/.../kv/KvTablet.java` | Modified | Major: `overflowContext` field, 8+ method signatures changed to pass `activeBuffer`/`partitionName`/`schemaId`, Paimon fallback in `getFromBufferOrKv`, `processOverflowDeletion`, flush/close/truncation for overflow |
| `fluss-server/.../replica/Replica.java` | Modified | `@Nullable LakeStorage lakeStorage` field, `maySetupOverflowContext()` |
| `fluss-server/.../replica/ReplicaManager.java` | Modified | Passes `lakeStorage` to Replica constructor |
| `fluss-server/src/test/.../replica/ReplicaTestBase.java` | Modified | Updated Replica constructor call with `null` for lakeStorage |
| `fluss-server/.../RpcServiceBase.java` | Modified | (Part of broader branch changes) |
| `fluss-server/.../metadata/TabletServerMetadataCache.java` | Modified | (Part of broader branch changes) |
| `fluss-server/.../utils/ServerRpcMessageUtils.java` | Modified | (Part of broader branch changes) |
| `fluss-flink/.../lake/LakeSplitGenerator.java` | Modified | (Part of broader branch changes) |

### Files in earlier commits (also in git, part of `support-write-lookup-historial-partition` branch):

| File | Commit | What |
|------|--------|------|
| `fluss-client/.../write/DynamicPartitionCreator.java` | f536328, 97c9621 | Historical partition write support |
| `fluss-client/.../write/Sender.java` | f536328 | Historical partition write support |
| `fluss-client/.../write/WriteRecord.java` | f536328 | Historical partition write support |
| `fluss-client/.../write/WriterClient.java` | f536328, 97c9621 | Historical partition write support |
| `fluss-common/.../metadata/PhysicalTablePath.java` | f536328 | `OVERFLOW_PARTITION_NAME` constant |
| `FIP-28-historical-partition-write.md` | f536328 | FIP document |

### Files NOT yet committed (uncommitted/untracked):

| File | Status | What |
|------|--------|------|
| `fluss-lake/.../PaimonLakeTableLookuper.java` | Modified | Minor formatting fix |
| `fluss-lake/.../OverflowPartitionWriteITCase.java` | Modified | Test adjustments |
| `fluss-lake/.../FlinkOverflowPartitionLogTableITCase.java` | Modified | Test adjustments |
| `fluss-lake/.../testutils/FlinkPaimonTieringTestBase.java` | Modified | Test adjustments |
| `fluss-lake/.../OverflowPartitionChangelogITCase.java` | **NEW (untracked)** | 387-line IT case for overflow changelog with Paimon fallback |

---

## Key Architecture & Technical Details

### Core Design Decisions
1. **Per-partition buffer approach**: Each original partition gets its own `KvPreWriteBuffer` + RocksDB CF, avoiding pk byte prefix encoding
2. **CF naming convention**: `"overflow_" + partitionName` (e.g., `overflow_2020`)
3. **Overflow partition constant**: `PhysicalTablePath.OVERFLOW_PARTITION_NAME` = `"overflow"` (NOT `"__overflow__"`)
4. **Partition name format**: Values joined by `$` separator (`ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR`). Single: `"2020"`, multiple: `"us$2020"`
5. **Context passed through method chain**: `KvPreWriteBuffer activeBuffer`, `@Nullable String partitionName`, `short schemaId` replace direct field access
6. **Lazy CF creation**: `getOrCreateColumnFamily` creates on first write to a partition

### Paimon Fallback
- **Key encoding**: Fluss pk bytes (via `PaimonKeyEncoder`) = Paimon BinaryRow format → directly compatible, no re-encoding
- **Value encoding**: `PaimonLakeTableLookuper.lookup()` returns `ValueEncoder.encodeValue(schemaId, row)` format → `ValueDecoder.decodeValue()` compatible
- **Bucket ID**: Fluss bucket ID = Paimon bucket ID (no translation needed)
- **Lookup chain**: buffer → RocksDB → Paimon (via `OverflowWriteContext.lookupInLake`)
- **LakeTableLookuper**: Created per-table in `Replica.maySetupOverflowContext()`, takes `LookupContext(partitionName, bucketId, schemaId)`

### Delete in Overflow
- When `row == null` (delete), partition is unknown
- `processOverflowDeletion` searches all partition contexts (buffer → RocksDB → Paimon per partition)
- O(n) over partitions but deletes in overflow are rare

### Changelog Mode
- Default `ChangelogImage` is `FULL`, generating `-U` + `+U` pairs for updates
- The IT case relies on FULL mode (default) to verify UPDATE_BEFORE/UPDATE_AFTER pairs

### Known Limitations / Out of Scope
- **Recovery path**: `putToPreWriteBuffer()` in `KvRecoverHelper` still uses default buffer; overflow recovery needs separate work
- **Pre-existing checkstyle issue**: fluss-client has `UnusedImports: PartitionNotExistException` in `PrimaryKeyLookuper.java` — use `-Dcheckstyle.skip=true`

---

## IT Case: OverflowPartitionChangelogITCase

**Location**: `fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/OverflowPartitionChangelogITCase.java`

**Test flow**:
1. Create auto-partitioned PK table (FULL changelog image, partition by `dt`) + start tiering job
2. Write initial data (`id=1 Alice`, `id=2 Bob`, `id=3 Charlie`) to partition `"2026"`, wait for Paimon sync
3. Drop partition `"2026"` (simulating expiration)
4. Upsert `id=1 Alice_v2`, `id=2 Bob_v2` (updates) and `id=10 NewRecord` (insert) → redirected to overflow
5. Read changelog from overflow partition and verify:
   - `UPDATE_BEFORE(1, "Alice")` + `UPDATE_AFTER(1, "Alice_v2")` — old value from Paimon
   - `UPDATE_BEFORE(2, "Bob")` + `UPDATE_AFTER(2, "Bob_v2")` — old value from Paimon
   - `INSERT(10, "NewRecord")` — new key, no old value

**Run command**: `mvn test -pl fluss-lake/fluss-lake-paimon -Dtest=OverflowPartitionChangelogITCase -Dcheckstyle.skip=true`

---

## Remaining Work

### `tiering-cleanup` (pending, lower priority)
Drop per-partition CFs after tiering completes (`tieredOffset >= currentLogEndOffset`). This is an optimization; CFs can persist without correctness issues.

### Next Steps
1. **Run the IT case** in full cluster environment to verify end-to-end behavior
2. If IT case fails, debug the overflow write path (partition detection → overflow redirect → Paimon lookup → changelog generation)
3. Implement `tiering-cleanup` if needed
4. Recovery path for overflow tablets (separate future work)

---

## Useful Commands
```bash
# Compile server module
cd /Users/yuxia/Projects/fluss/fluss
mvn compile -pl fluss-server -am -q

# Compile lake-paimon (needs checkstyle skip)
mvn compile test-compile -pl fluss-lake/fluss-lake-paimon -am -Dcheckstyle.skip=true -q

# Run unit tests
mvn test -pl fluss-server -Dtest="RocksDBKvTest,KvPreWriteBufferTest,KvTabletTest" -DfailIfNoTests=false -q

# Run IT case
mvn test -pl fluss-lake/fluss-lake-paimon -Dtest=OverflowPartitionChangelogITCase -Dcheckstyle.skip=true

# Apply formatting
mvn spotless:apply -pl fluss-server,fluss-lake/fluss-lake-paimon -q
```
