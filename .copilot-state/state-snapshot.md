# State Snapshot — 2026-03-09

## Quick Resume Guide

Read `plan.md` first — it has the full context, architecture, all changed files, and next steps.

## Key File Paths (for quick navigation)

### Core implementation (all in fluss-server):
```
fluss-server/src/main/java/org/apache/fluss/server/kv/overflow/OverflowWriteContext.java  (NEW)
fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java                      (heavily modified)
fluss-server/src/main/java/org/apache/fluss/server/kv/rocksdb/RocksDBKv.java              (CF support added)
fluss-server/src/main/java/org/apache/fluss/server/kv/prewrite/KvPreWriteBuffer.java      (CF-aware flushing)
fluss-server/src/main/java/org/apache/fluss/server/kv/KvBatchWriter.java                  (interface extended)
fluss-server/src/main/java/org/apache/fluss/server/kv/rocksdb/RocksDBWriteBatchWrapper.java (CF write methods)
fluss-server/src/main/java/org/apache/fluss/server/replica/Replica.java                   (LakeStorage + overflow)
fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java            (passes lakeStorage)
```

### IT Case (untracked, needs git add):
```
fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/OverflowPartitionChangelogITCase.java
```

### Reference files (existing, for understanding patterns):
```
fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/PaimonLakeLookupITCase.java
fluss-lake/fluss-lake-paimon/src/test/java/org/apache/fluss/lake/paimon/OverflowPartitionWriteITCase.java
fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/PaimonLakeTableLookuper.java
```

## What to Do Next
1. Run IT case: `mvn test -pl fluss-lake/fluss-lake-paimon -Dtest=OverflowPartitionChangelogITCase -Dcheckstyle.skip=true`
2. Debug if it fails — likely areas: partition detection, overflow redirect, Paimon lookup chain, changelog generation
3. Consider implementing `tiering-cleanup` todo
