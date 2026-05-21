# PR 3: DvManager + KvTablet DV 写入路径

## 目标

实现 DV 写入路径的核心状态机。当 KvTablet 写入路径产生 `-U`（update-before）或 `-D`（delete）记录时：
1. 在 **LogDv** 中标记旧记录的 changelog offset（使 tiering/union-read 跳过已替代的记录）
2. 点查 **RowPosIndex** 看旧记录的 lake 位置是否已知
3. 已知：标记 **LakeDv** + 写入已解析的 **PendingDeletes** + 删除 RowPosIndex 条目
4. 未知：写入 pending 标记的 **PendingDeletes**，等待未来 Readable Switch 解析

本 PR 是 DV 实现计划的第 3 个 PR。PR 1（DvRocksDB + 数据结构）和 PR 2（RowId + changelog 格式）已提交。

## 设计文档参考

- [fluss-paimon-deletion-vector-design.md](../fluss-paimon-deletion-vector-design.md) §4.1, §4.2
- [paimon-dv-implementation-plan.md](paimon-dv-implementation-plan.md) PR 3

---

## 改动清单

### 1. 新增类：`DvEntry`
**文件**：`fluss-server/.../kv/dv/DvEntry.java`（新建）

简单 POJO，承载一个 -U/-D 事件的信息：

```java
public class DvEntry {
    private final long oldRowId;  // 被替代的 +I/+U 记录的 RowId
    // 构造方法、getter
}
```

`oldRowId` 是旧 +I/+U 记录的 changelog offset（在 PR 2 的 applyInsert/applyUpdate 中设为 RowId）。三重用途：LogDv 删除标记、RowPosIndex 查找键、PendingDeletes 键。

---

### 2. 新增类：`DvManager`
**文件**：`fluss-server/.../kv/dv/DvManager.java`（新建）

DV 写入的核心状态机。持有 DvRocksDB 和 DvRWLock 的引用。

```java
public class DvManager implements Closeable {
    private final DvRocksDB dvRocksDB;
    private final DvRWLock dvRWLock;

    // 主入口：changelog append 成功后处理收集的 -U/-D 条目
    public void handleChangelogSynced(List<DvEntry> entries) throws IOException {
        dvRWLock.writeLock();
        try {
            for (DvEntry entry : entries) {
                long oldRowId = entry.getOldRowId();

                // 1. 标记 LogDv：旧 +I/+U offset 已被替代
                dvRocksDB.logDv().markDeleted(oldRowId);

                // 2. 点查 RowPosIndex
                FilePos filePos = dvRocksDB.rowPosIndex().get(oldRowId);

                if (filePos != null) {
                    // 命中：旧记录的 lake 位置已知
                    dvRocksDB.lakeDv().markDeleted(filePos.fileId(), filePos.rowPosition());
                    dvRocksDB.rowPosIndex().delete(oldRowId);
                    dvRocksDB.pendingDeletes().put(oldRowId, filePos);
                } else {
                    // 未命中：位置未知（数据尚未 tiered 或 compacted）
                    dvRocksDB.pendingDeletes().putPending(oldRowId);
                }
            }
        } finally {
            dvRWLock.writeUnlock();
        }
    }

    @Override
    public void close() {
        // DvRocksDB 生命周期由 KvTablet 管理
    }
}
```

---

### 3. KvTablet 集成
**文件**：`fluss-server/.../kv/KvTablet.java`（修改）

#### 3a. 新增字段
```java
@Nullable private final DvRocksDB dvRocksDB;     // !dvEnabled 时为 null
@Nullable private final DvManager dvManager;       // !dvEnabled 时为 null
```

#### 3b. 构造方法 + create()
- 构造方法：接受 `@Nullable DvRocksDB` 和 `@Nullable DvManager`
- `create()`：当 `dvEnabled` 时，在 `new File(kvTabletDir, "dv")` 打开 DvRocksDB，创建 DvRWLock 和 DvManager

#### 3c. dvEnabled 时禁用 WAL 优化
`processUpsert()` 中的 WAL 优化路径会跳过获取旧值（传 `NO_ROW_ID`）。dvEnabled 时必须获取旧值以得到 oldRowId。在条件中加入 `&& !dvEnabled`：
```java
if (changelogImage == ChangelogImage.WAL
        && !dvEnabled                          // <-- 新增
        && !autoIncrementUpdater.hasAutoIncrement()
        && currentMerger instanceof DefaultRowMerger) {
```

#### 3d. 在 processKvRecords 中收集 DvEntries
- 将 `processKvRecords` 返回类型从 `void` 改为 `List<DvEntry>`
- 添加 `List<DvEntry> dvEntries` 局部变量（dvEnabled 时 `new ArrayList<>()`，否则 `Collections.emptyList()`）
- 将 `dvEntries` 列表传递给 `processDeletion()` 和 `processUpsert()`
- 在 `processDeletion()` 中：`applyDelete()` 或 `applyUpdate()` 之后，若 dvEnabled 且 `oldRowId != NO_ROW_ID`，添加 `new DvEntry(oldRowId)`
- 在 `processUpsert()` 中：`applyUpdate()`（存在旧值时）之后，若 dvEnabled 且 `oldRowId != NO_ROW_ID`，添加 `new DvEntry(oldRowId)`

#### 3e. 在 putAsLeader 中调用 DvManager
`logTablet.appendAsLeader()` 成功且 batch 非重复后：
```java
LogAppendInfo logAppendInfo = logTablet.appendAsLeader(walBuilder.build());
if (logAppendInfo.duplicated()) {
    kvPreWriteBuffer.truncateTo(logEndOffsetOfPrevBatch, TruncateReason.DUPLICATED);
} else if (dvManager != null && !dvEntries.isEmpty()) {
    dvManager.handleChangelogSynced(dvEntries);
}
return logAppendInfo;
```

#### 3f. close() 和 drop()
- `close()`：关闭 KV RocksDB 后关闭 DvRocksDB（若非 null）
- `drop()`：DV 目录是 kvTabletDir 的子目录，`FileUtils.deleteDirectory(kvTabletDir)` 已覆盖

---

## 涉及文件列表

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-server/.../kv/dv/DvEntry.java` | 新建 | -U/-D 事件载体 POJO |
| `fluss-server/.../kv/dv/DvManager.java` | 新建 | DV 写入核心状态机 |
| `fluss-server/.../kv/KvTablet.java` | 修改 | DvRocksDB/DvManager 集成 |
| `fluss-server/.../kv/dv/DvManagerTest.java` | 新建 | DvManager 单元测试 |

## 复用的现有工具

| 工具 | 用途 |
|------|------|
| `DvRocksDB` | 打开/关闭/写入 DV 数据 |
| `DvRWLock` | 读写锁并发控制 |
| `RowPosIndex` | get/put/delete 行位置 |
| `LogDv` | markDeleted 标记 changelog offset |
| `LakeDv` | markDeleted 标记文件内行位置 |
| `PendingDeletes` | put/putPending 写入待解析删除 |
| `FilePos` | 行位置编解码 |

---

## 测试

### DvManagerTest
- **testRowPosIndexMiss**：RowPosIndex 无映射 → PendingDeletes 有 pending 标记，LogDv 有删除标记
- **testRowPosIndexHit**：预填 RowPosIndex → LakeDv 标记，RowPosIndex 删除，PendingDeletes 已解析，LogDv 标记
- **testMixedHitAndMiss**：batch 中同时有命中和未命中条目
- **testEmptyEntries**：空列表 → 无操作
- **testMultipleEntriesForSameFile**：同一文件不同位置的两行 → LakeDv bitmap 包含两个位置
- **testMultipleBatches**：多次 handleChangelogSynced 调用维持正确状态

### 现有测试验证
- `KvTabletTest`（26 个测试）：验证 dvEnabled=false 路径不受影响
- `KvTabletMergeModeTest`（9 个测试）：验证合并模式不受影响
- `KvTabletSchemaEvolutionTest`（2 个测试）：验证 schema 演化不受影响

---

## 前置依赖

- PR 1（DvRocksDB + 核心数据结构）
- PR 2（KV State RowId + Changelog 格式）

---

## 验证

1. 编译：`mvn compile -pl fluss-server -am -DskipTests`
2. 格式化：`mvn spotless:apply -pl fluss-server`
3. 运行 DvManager 测试：`mvn test -pl fluss-server -Dtest=DvManagerTest`
4. 运行 KvTablet 测试：`mvn test -pl fluss-server -Dtest=KvTabletTest`
