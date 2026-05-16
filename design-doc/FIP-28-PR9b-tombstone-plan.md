# Historical Partition DELETE Tombstone 修复

## 问题

Historical 分区的 lookup 链路：prewrite buffer → RocksDB → lake fallback。当 DELETE 记录被 flush 到 RocksDB 后（RocksDB native delete），key 从 RocksDB 中消失，与 "从未写入过" 无法区分，导致 lake fallback 返回被删除的旧数据。

### 复现场景

```
+I(k1, v1) → offset 0 → tiered 到 lake
-D(k1)     → offset 1 → 未 tiered

Failover → recovery 从 tieredOffset=1 开始:
  replay -D(k1) → 空 RocksDB 上 delete() 是 no-op
  lookup k1 → RocksDB 没有 → lake fallback → 返回 v1 → 错误！
```

正常写入也有这个窗口（DELETE flush 到 RocksDB 后、tiering 到 lake 前）：

```
+I(k1, v1) → offset 0 → RocksDB 有 k1, tiered 到 lake
-D(k1)     → offset 1 → prewrite buffer 存 null
HW 推进 → flush → RocksDB delete(k1) → RocksDB 没有 k1
lookup k1 → RocksDB 没有 → lake fallback → 返回 v1 → 错误！
```

### 根因

RocksDB `get()` 对 "key 被删除" 和 "key 从未写入" 都返回 null，无法区分。`getOldValue()` 见到 null 就走 lake fallback。

## 修复方案：Tombstone

对 historical 分区，DELETE 不调用 `rocksdb.delete(key)`，改为 `rocksdb.put(key, TOMBSTONE)`。Lookup 时遇到 tombstone 返回 null，不走 lake fallback。

**Tombstone 值**：`new byte[0]`（空字节数组）。正常 value 至少 2 字节（`[schemaId][compactedRow]`），空数组无歧义。

### Tombstone 生命周期

- 写入：DELETE flush 到 RocksDB 或 recovery replay DELETE 时写入
- 覆盖：同 key 的新 INSERT 会用正常 value 覆盖 tombstone
- 清理：PR9b cleanup（`dropKv()` + `initKvTablet()`）清除整个 RocksDB，tombstone 随之消失

---

## 改动范围

| # | 文件 | 改动 |
|---|------|------|
| 1 | `KvTablet.java` | 定义 `TOMBSTONE_VALUE`；`getOldValue()` 识别 tombstone 返回 null 不走 lake |
| 2 | `KvRecoverHelper.java` | recovery phase 1：historical DELETE 写 tombstone 而非 delete |
| 3 | `KvPreWriteBuffer.java` | `flush()`：historical DELETE 写 tombstone 而非 delete |

---

## 详细设计

### Step 1: `KvTablet.java` — tombstone 定义 + getOldValue 识别

```java
/** Tombstone marker for historical partition deletes in RocksDB. */
static final byte[] TOMBSTONE_VALUE = new byte[0];

static boolean isTombstone(@Nullable byte[] value) {
    return value != null && value.length == 0;
}
```

`getOldValue()` 修改：

```java
// 2. RocksDB
byte[] rocksResult = rocksDBKv.get(key.get());
if (rocksResult != null) {
    // Historical partition tombstone: key was explicitly deleted.
    // Return null without lake fallback.
    if (isTombstone(rocksResult)) {
        return null;
    }
    return rocksResult;
}
// 3. lake fallback ...
```

### Step 2: `KvRecoverHelper.java` — recovery 写 tombstone

Recovery phase 1（到 HW，直接写 RocksDB）中，historical 分区的 DELETE 改为写 tombstone：

```java
// 当前代码：
if (resumeRecord.value == null) {
    kvBatchWriter.delete(resumeRecord.key);
} else {
    kvBatchWriter.put(resumeRecord.key, resumeRecord.value);
}

// 改为：
if (resumeRecord.value == null) {
    if (isHistoricalPartition) {
        kvBatchWriter.put(resumeRecord.key, KvTablet.TOMBSTONE_VALUE);
    } else {
        kvBatchWriter.delete(resumeRecord.key);
    }
} else {
    kvBatchWriter.put(resumeRecord.key, resumeRecord.value);
}
```

Recovery phase 2（HW 到 log end，写 prewrite buffer）不需要改——prewrite buffer 用 `Value.of(null)` 标记 delete，`getOldValue()` 能正确识别（`value != null` 但 `value.get() == null` → 返回 null 不走 lake）。

### Step 3: `KvPreWriteBuffer.java` — flush 写 tombstone

`flush()` 中 DELETE 条目刷盘时，historical 分区写 tombstone 而非 delete：

```java
// 当前代码：
if (value.value != null) {
    kvBatchWriter.put(entry.getKey().key, value.value);
} else {
    kvBatchWriter.delete(entry.getKey().key);
}

// 改为：
if (value.value != null) {
    kvBatchWriter.put(entry.getKey().key, value.value);
} else {
    if (isHistoricalPartition) {
        kvBatchWriter.put(entry.getKey().key, KvTablet.TOMBSTONE_VALUE);
    } else {
        kvBatchWriter.delete(entry.getKey().key);
    }
}
```

需要将 `isHistoricalPartition` 传递给 `KvPreWriteBuffer`（构造函数加参数）。

---

## 正确性验证

| 场景 | 预期行为 |
|------|---------|
| DELETE in prewrite buffer → lookup | prewrite buffer 返回 null → 不走 lake → 正确 |
| DELETE flush 到 RocksDB → lookup | RocksDB 返回 tombstone → `getOldValue()` 返回 null → 不走 lake → 正确 |
| Recovery replay DELETE → lookup | RocksDB 有 tombstone → 同上 → 正确 |
| DELETE 后新 INSERT 同 key | INSERT 的 value 覆盖 tombstone → lookup 返回新 value → 正确 |
| DELETE 已 tiered 的 key，old value 从 lake 获取 | `getOldValue()` 中 RocksDB 无 tombstone 也无值 → lake fallback 获取 old value → 用于 CDC → DELETE flush 后写 tombstone → 后续 lookup 不走 lake → 正确 |
| Cleanup 后（PR9b）| `dropKv()` 清除所有 tombstone，`initKvTablet()` 创建空 RocksDB → 正确 |

---

## 测试

在 `KvHistoricalPartitionReplicaRestoreITCase` 中新增：

1. **testDeleteTombstoneAfterRecovery**:
   - 写入 +I(k1, v1) → 模拟 tiering → 写入 -D(k1)
   - Failover → recovery 从 tieredOffset 开始
   - lookup k1 → 应返回 null（不应返回被删除的 v1）

2. **testDeleteTombstoneInNormalOperation**:
   - 写入 +I(k1, v1) → 模拟 tiering → 写入 -D(k1)
   - 等待 flush 到 RocksDB
   - lookup k1 → 应返回 null

3. **testInsertAfterDelete**:
   - 写入 +I(k1, v1) → -D(k1) → +I(k1, v2)
   - lookup k1 → 应返回 v2（tombstone 被新值覆盖）

---

## 验证步骤

```bash
./mvnw spotless:apply -pl fluss-server -q
./mvnw test-compile -pl fluss-server -am -q
./mvnw test -Dtest=KvHistoricalPartitionReplicaRestoreITCase -pl fluss-server -am
```
