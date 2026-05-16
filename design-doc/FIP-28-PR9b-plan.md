# PR 9b: `__historical__` 分区 Cleanup

## Context

PR 9a 解决了 `__historical__` 分区的 recovery 问题（composite key WAL 回放 + 跳过 snapshot）。但当前 `__historical__` 分区的 RocksDB 数据会无限增长——即使数据已经完全 tiered 到 lake，composite keys 仍然留在 RocksDB 中，占用磁盘空间。

### 设计目标（FIP-28 A.3.3）

当 `tieredOffset >= logEndOffset`（所有 `__historical__` 数据已 tiered 到 lake），清空 RocksDB 释放磁盘空间。

### 核心思路

**Cleanup = `dropKv()` + `initKvTablet()`**：tiering 完成后关闭旧 RocksDB、删除目录，立即创建一个空的新 RocksDB。

- `kvTablet` 始终非 null（leader 状态下），不引入任何新状态
- lookup、write 路径零改动
- cleanup 在 per-bucket serial executor 内执行，与写入天然串行
- `dropKv()` + `initKvTablet()` 在 `leaderIsrUpdateLock` 写锁内执行，与 lookup 互斥

### 与之前方案对比

| | 最初方案 | 当前方案 |
|---|---|---|
| **cleanup 做什么** | 关闭删除 RocksDB + `cleanedUp` flag | `dropKv()` + `initKvTablet()`（重建空 RocksDB） |
| **KvTablet 改动** | reference counting、`cleanedUp` flag | 无 |
| **Lookup 适配** | 检查 `cleanedUp` flag → `lookupAllFromLake()` | 无（RocksDB 始终存在） |
| **Write 适配** | 多种方案未定 | 无（RocksDB 始终存在） |
| **并发问题** | `dropKv()` + `createKv()` 之间 lookup 可能 NPE | 无（`leaderIsrUpdateLock` 写锁保护） |
| **新状态/flag** | `cleanedUp`、`readReferenceCount` | 无 |

---

## 现有基础设施

| 组件 | 状态 |
|------|------|
| `HistoricalPartitionHandler.submitWrite()` | ✅ per-bucket serial executor |
| `LogTablet.lakeLogEndOffset` / `logEndOffset()` | ✅ 已有 |
| `ReplicaManager.notifyLakeTableOffset()` | ✅ coordinator 通知 tiered offset 更新 |
| `Replica.dropKv()` | ✅ 关闭 RocksDB + 删除目录 + `kvTablet = null` |
| `Replica.initKvTablet()` | ✅ PR9a 已有 historical 分支（创建 fresh RocksDB + replay WAL） |

---

## 改动范围

| # | 文件 | 操作 | 说明 |
|---|------|------|------|
| 1 | `Replica.java` | 修改 | 新增 `cleanupHistoricalKv()`：re-check 条件 → `dropKv()` + `initKvTablet()` |
| 2 | `ReplicaManager.java` | 修改 | `notifyLakeTableOffset()` 中对 historical 分区提交 cleanup 任务到 serial executor |
| 3 | 测试 | 新增/修改 | 验证 cleanup 触发、重建、lookup 正确性 |

**不需要修改的组件**：`KvTablet`、`HistoricalPartitionHandler`、`KvRecoverHelper`、`CompositeKeyEncoder`、`createKv()`、`lookups()`、写入路径

---

## 详细设计

### Step 1: `Replica.cleanupHistoricalKv()`

```java
/**
 * Cleans up the historical partition's RocksDB after tiering completes, then
 * immediately re-creates an empty RocksDB so that kvTablet is never null.
 * Must be called within the per-bucket serial executor to serialize with writes.
 *
 * <p>The dropKv() + initKvTablet() pair is protected by leaderIsrUpdateLock
 * write lock to prevent concurrent lookups from seeing kvTablet == null.
 * This mirrors onBecomeNewLeader() which also calls dropKv() + createKv()
 * under the write lock.
 *
 * <p>When tieredOffset >= logEndOffset, all data has been tiered to lake.
 * dropKv() removes old data, initKvTablet() creates fresh RocksDB (replay
 * from new tieredOffset will find 0 records to replay).
 */
public void cleanupHistoricalKv() {
    inWriteLock(leaderIsrUpdateLock, () -> {
        if (kvTablet == null) {
            return;
        }

        long tieredOffset = logTablet.getLakeLogEndOffset();
        long logEnd = logTablet.logEndOffset();

        if (tieredOffset < logEnd) {
            LOG.debug(
                    "Skipping historical KV cleanup for {}: tieredOffset={} < logEndOffset={}",
                    tableBucket, tieredOffset, logEnd);
            return;
        }

        LOG.info(
                "Cleaning up historical KV for {} (tieredOffset={} >= logEndOffset={}).",
                tableBucket, tieredOffset, logEnd);
        dropKv();
        initKvTablet();
    });
}
```

**并发安全**：`dropKv()` + `initKvTablet()` 在 `leaderIsrUpdateLock` 写锁内执行。lookup 持有读锁，会被阻塞直到 cleanup 完成。与 `onBecomeNewLeader()` 中 `dropKv()` + `createKv()` 的模式一致。

`initKvTablet()` 的 historical 分支（PR9a）会：创建 fresh RocksDB → 从 `tieredOffset` replay WAL。由于 `tieredOffset >= logEndOffset`，replay 0 条记录，写锁持有时间极短。

### Step 2: `ReplicaManager.notifyLakeTableOffset()` — 触发 cleanup

```java
// 在 notifyLakeTableOffset() 的 offset 更新循环内，每个 bucket 更新后追加：
maybeScheduleHistoricalCleanup(tb);

private void maybeScheduleHistoricalCleanup(TableBucket tb) {
    Replica replica = getReplicaOrException(tb);
    if (!PartitionUtils.isHistoricalPartitionName(
            replica.getPhysicalTablePath().getPartitionName())) {
        return;
    }
    historicalPartitionHandler.submitWrite(tb, replica::cleanupHistoricalKv);
}
```

### 流程图

```
=== Leader 初始化（不变，PR9a 行为）===

onBecomeNewLeader()
  → dropKv() + createKv()
      → initKvTablet() → 创建 RocksDB + replay WAL from tieredOffset
      → 跳过 periodic snapshot（PR9a）

=== Cleanup 流程 ===

notifyLakeTableOffset(tieredOffset)
  → logTablet.updateLakeLogEndOffset(tieredOffset)
  → maybeScheduleHistoricalCleanup(tb)
      → historicalPartitionHandler.submitWrite(tb, replica::cleanupHistoricalKv)
          ┌─ (per-bucket serial executor, serialized with writes) ────┐
          │  inWriteLock(leaderIsrUpdateLock):                        │
          │    kvTablet == null? → return                             │
          │    tieredOffset < logEndOffset? → return (有新数据)       │
          │    dropKv() → 删除旧 RocksDB                             │
          │    initKvTablet() → 创建空 RocksDB (replay 0 条)         │
          └──────────────────────────────────────────────────────────┘

=== Cleanup 后 lookup ===

lookup(key, partitionName="2000")
  → kvTablet 非 null（空 RocksDB）
  → composite key 在 RocksDB 中找不到
  → lake fallback → 返回正确值

=== Cleanup 后新写入 ===

submitWrite(records)
  → kvTablet 非 null → 正常写入
```

---

## 测试

### 集成测试

1. **testHistoricalCleanupAfterTiering**:
   - 写入过期分区 → 数据进入 `__historical__` RocksDB
   - 模拟 tiering 完成（`tieredOffset >= logEndOffset`）
   - 触发 cleanup
   - 验证旧数据已清空（lookup 返回 null）
   - 验证 `kvTablet` 仍非 null（新的空 RocksDB）

2. **testWriteAfterCleanup**:
   - 写入 → tiering → cleanup
   - 再次写入新数据
   - lookup 新数据 → 从 RocksDB 返回正确值

3. **testCleanupSkippedWhenNewWriteArrives**:
   - 写入 → tiering 完成 → 但 cleanup 执行时已有新写入
   - 验证 cleanup 被跳过（`tieredOffset < logEndOffset`）

---

## 验证步骤

```bash
./mvnw spotless:apply -pl fluss-server -q
./mvnw test-compile -pl fluss-server -am -q
./mvnw test -Dtest=KvHistoricalPartitionReplicaRestoreITCase -pl fluss-server -am
```

---

## 备注

1. **`kvTablet` 始终非 null**：cleanup 后立即 `initKvTablet()` 重建空 RocksDB。lookup、write 路径零改动。

2. **空 RocksDB 开销**：cleanup 后 RocksDB 为空，磁盘开销极小（几个元数据文件）。内存开销也很小（block cache 是共享的）。下次 cleanup 会再次清空。

3. **并发安全**：cleanup 在 per-bucket serial executor 内执行，与写入串行。`dropKv()` + `initKvTablet()` 在 `leaderIsrUpdateLock` 写锁内执行，与 lookup（持有读锁）互斥。与 `onBecomeNewLeader()` 的模式一致。

4. **依赖 PR9a**：`initKvTablet()` 中 historical 分区的处理（跳过 snapshot、从 tieredOffset replay）由 PR9a 实现。
