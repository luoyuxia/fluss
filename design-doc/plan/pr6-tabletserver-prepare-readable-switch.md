# PR 6: TabletServer Prepare + Readable Switch

## 目标

实现 TabletServer 侧的 Prepare 阶段（下载 SST + 写 FileDict）和 Readable Switch 阶段（Ingest SST + batch resolve + cleanup），将 PR 5 中的 TODO stub 填充为完整实现。

**设计文档参考**：`fluss-deletion-vector-design-v3-en.md` §5.3（Phase B: Prepare）、§5.4（Phase C: Readable Switch）

---

## 背景

PR 5 在 `ReplicaManager` 中留下了两个 stub：

```java
private void handleDvPrepare(DvPrepareData dvPrepare) {
    // TODO: PR 6 - Download SST, write FileDict, resolve pending deletes
}

private void handleDvReadableSwitch(DvReadableSwitchData data) {
    // TODO: PR 6 - Ingest SST, batch resolve, set readable offset
}
```

DvPrepare 通过 `NotifyLakeTableOffsetRequest` 中的可选字段到达 TabletServer，在 `notifyLakeTableOffset()` 中被提取并路由到 `handleDvPrepare()`。

DvReadableSwitch 通过独立的 `DvReadableSwitchRequest` RPC 到达，在 `dvReadableSwitch()` 中路由到 `handleDvReadableSwitch()`。

---

## 已有基础设施

| 组件 | 位置 | 用途 |
|------|------|------|
| `DvRocksDB` | `server/kv/dv/DvRocksDB.java` | 5 个 CF：RowPosIndex, LogDv, LakeDv, FileDict, PendingDeletes |
| `RowPosIndex` | `server/kv/dv/RowPosIndex.java` | `get(rowId)`, `delete(rowId)`, `ingestExternalFile(sstPaths)` |
| `LakeDv` | `server/kv/dv/LakeDv.java` | `markDeleted(fileId, pos)`, `delete(fileId)`, `getAll()` |
| `LogDv` | `server/kv/dv/LogDv.java` | `cleanup(offset)` |
| `FileDict` | `server/kv/dv/FileDict.java` | `put(fileId, filePath)`, `getFileId(path)`, `getFilePath(id)` |
| `PendingDeletes` | `server/kv/dv/PendingDeletes.java` | `put(rowId, filePos)`, `putPending(rowId)`, `get(rowId)`, `delete(rowId)`, `iterator()` |
| `DvRWLock` | `server/kv/dv/DvRWLock.java` | `writeLock()` / `writeUnlock()` / `readLock()` / `readUnlock()` |
| `DvManager` | `server/kv/dv/DvManager.java` | `handleChangelogSynced()` 写路径 |
| `RowPosSstDownloader` | `server/kv/dv/RowPosSstDownloader.java` | `downloadBucketSst(snapshotId, bucketId, localDir)`, `readIndex(snapshotId)` |
| `DvPrepareData` | `server/entity/DvPrepareData.java` | `tableId`, `readableSnapshotId`, `bucketOffsets` (per-bucket) |
| `DvPositionReportData.DvBucketOffset` | `server/entity/DvPositionReportData.java` | `readableOffset`, `newFileDictEntries`, `oldFiles` |
| `DvReadableSwitchData` | `server/entity/DvReadableSwitchData.java` | `coordinatorEpoch`, `tableId`, `readableSnapshotId` |
| `KvTablet` | `server/kv/KvTablet.java` | `dvEnabled`, `dvRocksDB`, `dvManager` 字段 |
| `FlussPaths.remoteLakeTableSnapshotDir()` | `common/utils/FlussPaths.java` | 计算远端 lake 快照目录 |

---

## Step 1: DvManager 扩展 — 新增状态字段和 Prepare/Switch 方法

**文件**: `fluss-server/.../kv/dv/DvManager.java`

### 1a. 新增状态字段

```java
// 已完成 Prepare 的 snapshot ID（Prepare 最后一步更新，用于幂等检查）
private long preparedSnapshotId = -1;

// 当前 DV-readable snapshot ID（Readable Switch 最后一步更新，用于幂等检查）
private long readableSnapshotId = -1;

// 当前 readable snapshot 的 log 起始 offset（Readable Switch 时更新）
private long snapshotStartLogOffset = -1;

// Prepare 阶段下载的 SST 文件路径（per-bucket，Readable Switch 时消费）
private final Map<Integer, List<String>> pendingSstPaths = new HashMap<>();

// Prepare 阶段缓存的 oldFiles fileId 列表（per-bucket，Readable Switch 时消费）
private final Map<Integer, List<Integer>> pendingOldFileIds = new HashMap<>();
```

### 1b. Prepare 方法

```java
/**
 * Handles DV Prepare phase for this tablet's bucket.
 *
 * <p>No DvRWLock needed: FileDict writes are RocksDB thread-safe and no other
 * operation concurrently modifies FileDict; in-memory pending maps are only
 * accessed sequentially by Prepare/Switch (coordinator orchestration).
 *
 * @param bucketId this tablet's bucket ID
 * @param bucketOffset per-bucket DV data (readableOffset, newFileDictEntries, oldFiles)
 * @param downloader SST downloader configured with remote dir
 * @param snapshotId the readable snapshot ID for locating SST files
 * @param localTempDir local directory for downloaded SST files
 */
public void handlePrepare(
        int bucketId,
        DvPositionReportData.DvBucketOffset bucketOffset,
        RowPosSstDownloader downloader,
        long snapshotId,
        String localTempDir) throws IOException {

    // Idempotency: preparedSnapshotId is updated as the last step,
    // so if it already matches, the entire Prepare has completed before.
    if (this.preparedSnapshotId == snapshotId) {
        LOG.info("DV Prepare already completed for snapshot {}, skipping", snapshotId);
        return;
    }

    // Download SST from remote storage
    List<String> sstPaths = downloader.downloadBucketSst(snapshotId, bucketId, localTempDir);

    // Write newFileDictEntries to FileDict CF
    Map<Integer, String> newEntries = bucketOffset.getNewFileDictEntries();
    for (Map.Entry<Integer, String> entry : newEntries.entrySet()) {
        int fileId = entry.getKey();
        String filePath = entry.getValue();
        dvRocksDB.fileDict().put(fileId, filePath);
    }

    // Resolve oldFiles: convert file_path to file_id
    List<Integer> oldFileIds = new ArrayList<>();
    for (String oldFilePath : bucketOffset.getOldFiles()) {
        int fileId = dvRocksDB.fileDict().getFileId(oldFilePath);
        if (fileId >= 0) {
            oldFileIds.add(fileId);
        }
    }

    // Store SST paths and old file IDs for Readable Switch
    if (!sstPaths.isEmpty()) {
        pendingSstPaths.put(bucketId, sstPaths);
    }
    if (!oldFileIds.isEmpty()) {
        pendingOldFileIds.put(bucketId, oldFileIds);
    }

    // Mark Prepare as completed (must be last step for idempotency)
    this.preparedSnapshotId = snapshotId;
}
```

### 1c. Readable Switch 方法

```java
/**
 * Handles DV Readable Switch for this tablet's bucket.
 *
 * <p>All steps under DvRWLock write lock:
 * <ol>
 *   <li>Ingest SST into RowPosIndex
 *   <li>Batch resolve PendingDeletes
 *   <li>Cleanup oldFiles from LakeDv and PendingDeletes
 *   <li>Cleanup expired LogDv
 *   <li>Update readableSnapshotId and snapshotStartLogOffset
 * </ol>
 *
 * @param bucketId this tablet's bucket ID
 * @param readableSnapshotId the new readable snapshot ID
 * @param readableOffset the per-bucket readable offset (= snapshotStartLogOffset)
 */
public void handleReadableSwitch(
        int bucketId,
        long readableSnapshotId,
        long readableOffset) throws IOException {

    // Idempotency: readableSnapshotId is updated as the last step,
    // so if it already matches, the entire Switch has completed before.
    if (this.readableSnapshotId == readableSnapshotId) {
        LOG.info("DV ReadableSwitch already completed for snapshot {}, skipping",
                readableSnapshotId);
        return;
    }

    dvRWLock.writeLock();
    try {
        // Step 1: Ingest SST into RowPosIndex
        List<String> sstPaths = pendingSstPaths.remove(bucketId);
        if (sstPaths != null && !sstPaths.isEmpty()) {
            dvRocksDB.rowPosIndex().ingestExternalFile(sstPaths);
        }

        // Step 2: Batch resolve PendingDeletes
        batchResolvePendingDeletes(readableOffset);

        // Step 3: Cleanup oldFiles
        List<Integer> oldFileIds = pendingOldFileIds.remove(bucketId);
        if (oldFileIds != null) {
            cleanupOldFiles(oldFileIds);
        }

        // Step 4: Cleanup expired LogDv
        dvRocksDB.logDv().cleanup(readableOffset);

        // Step 5: Update state
        this.readableSnapshotId = readableSnapshotId;
        this.snapshotStartLogOffset = readableOffset;
    } finally {
        dvRWLock.writeUnlock();
    }
}
```

### 1d. Batch Resolve PendingDeletes

```java
/**
 * Batch resolves PendingDeletes against RowPosIndex.
 *
 * <p>For each entry in PendingDeletes:
 * <ul>
 *   <li>RowPosIndex hit → mark LakeDv, delete RowPosIndex, update PendingDeletes
 *   <li>RowPosIndex miss + R < readableOffset → orphan, delete PendingDeletes
 *   <li>RowPosIndex miss + R >= readableOffset → keep for next round
 * </ul>
 */
private void batchResolvePendingDeletes(long readableOffset) throws IOException {
    try (PendingDeletes.PendingDeleteIterator iter = dvRocksDB.pendingDeletes().iterator()) {
        while (iter.hasNext()) {
            PendingDeletes.PendingDeleteEntry entry = iter.next();
            long rowId = entry.getRowId();

            FilePos hit = dvRocksDB.rowPosIndex().get(rowId);
            if (hit != null) {
                // Case A/B/C: position now known
                dvRocksDB.lakeDv().markDeleted(hit.fileId(), hit.rowPosition());
                dvRocksDB.rowPosIndex().delete(rowId);
                dvRocksDB.pendingDeletes().put(rowId, hit);
            } else if (rowId < readableOffset) {
                // Orphan: row was tiered but never in a data file
                dvRocksDB.pendingDeletes().delete(rowId);
            }
            // else: rowId >= readableOffset, keep for next round
        }
    }
}
```

### 1e. Cleanup oldFiles

```java
/**
 * Cleans up LakeDv and PendingDeletes entries for old files removed by compaction.
 */
private void cleanupOldFiles(List<Integer> oldFileIds) throws IOException {
    Set<Integer> oldFileIdSet = new HashSet<>(oldFileIds);

    // Remove LakeDv entries for old files
    for (int fileId : oldFileIds) {
        dvRocksDB.lakeDv().delete(fileId);
    }

    // Remove PendingDeletes entries pointing to old files
    try (PendingDeletes.PendingDeleteIterator iter = dvRocksDB.pendingDeletes().iterator()) {
        while (iter.hasNext()) {
            PendingDeletes.PendingDeleteEntry entry = iter.next();
            if (!entry.isPending()) {
                FilePos fp = entry.getFilePos();
                if (oldFileIdSet.contains(fp.fileId())) {
                    dvRocksDB.pendingDeletes().delete(entry.getRowId());
                }
            }
        }
    }
}
```

### 1f. Getter 方法

```java
public long getReadableSnapshotId() {
    return readableSnapshotId;
}

public long getSnapshotStartLogOffset() {
    return snapshotStartLogOffset;
}

public DvRWLock getDvRWLock() {
    return dvRWLock;
}
```

---

## Step 2: KvTablet 扩展

**文件**: `fluss-server/.../kv/KvTablet.java`

### 2a. 新增 DvManager accessor

```java
@Nullable
public DvManager getDvManager() {
    return dvManager;
}

public boolean isDvEnabled() {
    return dvEnabled;
}
```

### 2b. 暴露 tablePath 和 tableId（如果尚未暴露）

`ReplicaManager.handleDvPrepare()` 需要通过 `FlussPaths.remoteLakeTableSnapshotDir()` 构造远端路径，因此需要从 KvTablet 或 Replica 获取 `tablePath` 和 `tableId`。

检查 `Replica` 类是否已提供这些信息。如果已有，直接使用；如果没有，从 `TableBucket` 和 `PhysicalTablePath` 中获取。

---

## Step 3: ReplicaManager 实现

**文件**: `fluss-server/.../replica/ReplicaManager.java`

### 3a. 实现 handleDvPrepare

```java
private void handleDvPrepare(DvPrepareData dvPrepare) {
    long tableId = dvPrepare.getTableId();
    long snapshotId = dvPrepare.getReadableSnapshotId();

    for (Map.Entry<Integer, DvPositionReportData.DvBucketOffset> entry
            : dvPrepare.getBucketOffsets().entrySet()) {
        int bucketId = entry.getKey();
        DvPositionReportData.DvBucketOffset bucketOffset = entry.getValue();

        TableBucket tb = new TableBucket(tableId, bucketId);
        try {
            Replica replica = getReplicaOrException(tb);
            KvTablet kvTablet = replica.getKvTablet();
            Preconditions.checkNotNull(kvTablet,
                    "KvTablet not available for %s", tb);
            Preconditions.checkState(kvTablet.isDvEnabled(),
                    "DV not enabled for %s", tb);
            DvManager dvManager = kvTablet.getDvManager();

            // Construct remote dir and local temp dir
            PhysicalTablePath tablePath = replica.getPhysicalTablePath();
            FsPath remoteLakeDir = FlussPaths.remoteLakeTableSnapshotDir(
                    serverConf.getString(ConfigOptions.REMOTE_DATA_DIR),
                    tablePath.getTablePath(),
                    tableId);
            RowPosSstDownloader downloader = new RowPosSstDownloader(remoteLakeDir);

            String localTempDir = createLocalTempDir(tb, snapshotId);

            dvManager.handlePrepare(
                    bucketId, bucketOffset, downloader, snapshotId, localTempDir);

            LOG.info("DV Prepare completed for {} snapshot {}", tb, snapshotId);
        } catch (Exception e) {
            LOG.error("DV Prepare failed for {} snapshot {}", tb, snapshotId, e);
            throw new FlussRuntimeException(
                    "DV Prepare failed for " + tb + " snapshot " + snapshotId, e);
        }
    }
}
```

### 3b. 实现 handleDvReadableSwitch

```java
private void handleDvReadableSwitch(DvReadableSwitchData data) {
    long tableId = data.getTableId();
    long snapshotId = data.getReadableSnapshotId();

    for (int bucketId : data.getBucketIds()) {
        TableBucket tb = new TableBucket(tableId, bucketId);
        try {
            Replica replica = getReplicaOrException(tb);
            KvTablet kvTablet = replica.getKvTablet();
            Preconditions.checkNotNull(kvTablet,
                    "KvTablet not available for %s", tb);
            Preconditions.checkState(kvTablet.isDvEnabled(),
                    "DV not enabled for %s", tb);
            DvManager dvManager = kvTablet.getDvManager();

            long readableOffset = dvManager.getPendingReadableOffset(bucketId);
            dvManager.handleReadableSwitch(bucketId, snapshotId, readableOffset);
            LOG.info("DV ReadableSwitch completed for {} snapshot {}", tb, snapshotId);
        } catch (Exception e) {
            LOG.error("DV ReadableSwitch failed for {} snapshot {}", tb, snapshotId, e);
            throw new FlussRuntimeException(
                    "DV ReadableSwitch failed for " + tb + " snapshot " + snapshotId, e);
        }
    }
}
```

### 3c. Proto / Data Class 扩展 — 添加 bucket_ids

**Proto** (`FlussApi.proto`):
```proto
message DvReadableSwitchRequest {
  required int32 coordinator_epoch = 1;
  required int64 table_id = 2;
  required int64 readable_snapshot_id = 3;
  repeated int32 bucket_ids = 4;
}
```

**Data Class** (`DvReadableSwitchData.java`): 添加 `Set<Integer> bucketIds` 字段和 getter。

**ServerRpcMessageUtils**: `getDvReadableSwitchData()` 提取 `bucket_ids`。

**Coordinator 侧** (`processDvSwitchEvent`): 现有逻辑只收集 `Set<Integer> targetServerIds`，需要改为 `Map<Integer, Set<Integer>> serverBucketMap`（serverId → bucketIds），给每个 server 发包含该 server 对应 bucket IDs 的请求：

```java
Map<Integer, Set<Integer>> serverBucketMap = new HashMap<>();
for (Integer bucketId : event.getBucketIds()) {
    TableBucket tb = new TableBucket(tableId, bucketId);
    List<Integer> assignment = coordinatorContext.getAssignment(tb);
    for (Integer serverId : assignment) {
        if (serverId >= 0) {
            serverBucketMap.computeIfAbsent(serverId, k -> new HashSet<>()).add(bucketId);
        }
    }
}

for (Map.Entry<Integer, Set<Integer>> entry : serverBucketMap.entrySet()) {
    DvReadableSwitchRequest request = new DvReadableSwitchRequest()
            .setCoordinatorEpoch(coordinatorEpoch)
            .setTableId(tableId)
            .setReadableSnapshotId(snapshotId);
    for (int bucketId : entry.getValue()) {
        request.addBucketIds(bucketId);
    }
    // send to entry.getKey()
}
```

### 3d. readableOffset 传递

readableOffset 在 Prepare 阶段通过 `DvBucketOffset.getReadableOffset()` 传入，缓存在 DvManager 中：

```java
// DvManager 新增字段
private final Map<Integer, Long> pendingReadableOffsets = new HashMap<>();

// handlePrepare 中缓存
pendingReadableOffsets.put(bucketId, bucketOffset.getReadableOffset());

// 提供 getter
public long getPendingReadableOffset(int bucketId) {
    Long offset = pendingReadableOffsets.get(bucketId);
    return offset != null ? offset : -1;
}

// handleReadableSwitch 中消费并清理
pendingReadableOffsets.remove(bucketId);
```

### 3d. 本地临时目录

```java
private String createLocalTempDir(TableBucket tb, long snapshotId) {
    // 使用 KvTablet 的本地目录创建临时子目录
    File tempDir = new File(
            localDataDir,
            "dv-prepare/" + tb.getTableId() + "/" + tb.getBucket() + "/" + snapshotId);
    tempDir.mkdirs();
    return tempDir.getAbsolutePath();
}
```

---

## Step 4: PendingDeletes iterator 确认

**文件**: `fluss-server/.../kv/dv/PendingDeletes.java`

确认 `iterator()` 返回的迭代器可以在遍历过程中安全删除/修改条目。RocksDB 的 iterator 是快照隔离的，遍历过程中写入不影响迭代。但需要注意：

- 遍历时收集需要修改的条目
- 遍历结束后一次性通过 WriteBatch 提交所有变更

如果当前 `iterator()` 不支持这种模式，需要改为两阶段：先遍历收集，再批量写入。

### 4a. 优化：BatchResolve 使用 WriteBatch

```java
private void batchResolvePendingDeletes(long readableOffset) throws IOException {
    // Phase 1: Iterate and collect changes
    List<BatchResolveAction> actions = new ArrayList<>();
    try (PendingDeletes.PendingDeleteIterator iter = dvRocksDB.pendingDeletes().iterator()) {
        while (iter.hasNext()) {
            PendingDeletes.PendingDeleteEntry entry = iter.next();
            long rowId = entry.getRowId();

            FilePos hit = dvRocksDB.rowPosIndex().get(rowId);
            if (hit != null) {
                actions.add(new BatchResolveAction(rowId, hit, BatchResolveAction.Type.RESOLVE));
            } else if (rowId < readableOffset) {
                actions.add(new BatchResolveAction(rowId, null, BatchResolveAction.Type.ORPHAN));
            }
        }
    }

    // Phase 2: Apply all changes
    for (BatchResolveAction action : actions) {
        if (action.type == BatchResolveAction.Type.RESOLVE) {
            dvRocksDB.lakeDv().markDeleted(action.filePos.fileId(), action.filePos.rowPosition());
            dvRocksDB.rowPosIndex().delete(action.rowId);
            dvRocksDB.pendingDeletes().put(action.rowId, action.filePos);
        } else {
            dvRocksDB.pendingDeletes().delete(action.rowId);
        }
    }
}
```

---

## Step 5: 测试

### 5a. DvManager Prepare 测试

**文件**: `fluss-server/src/test/.../kv/dv/DvManagerPrepareTest.java`（NEW）

- Prepare 下载 SST + FileDict 写入
- FileDict 幂等性：重复相同映射 → 跳过
- FileDict 冲突检测：不同映射 → 抛异常
- Prepare 重置：重复 prepare 清除旧的 pending 状态
- oldFiles → pendingOldFileIds 解析

### 5b. DvManager Readable Switch 测试

**文件**: `fluss-server/src/test/.../kv/dv/DvManagerReadableSwitchTest.java`（NEW）

- SST Ingest 后 RowPosIndex 包含新条目
- Batch resolve 各场景：
  - Hit（position known）→ markLakeDv + deleteRowPosIndex + updatePendingDeletes
  - Miss + orphan（R < readableOffset）→ deletePendingDeletes
  - Miss + keep（R >= readableOffset）→ 保留
- Zombie 条目处理：§4.2 删除 RowPosIndex[R]，Ingest 写回 R，batch resolve 再次标记 LakeDv
- oldFiles 清理：LakeDv 删除 + PendingDeletes 指向旧文件的条目清理
- LogDv 清理：过期 range 删除
- readableSnapshotId 和 snapshotStartLogOffset 更新
- 空 SST / 空 PendingDeletes 场景

### 5c. 集成测试

**文件**: `fluss-server/src/test/.../kv/dv/DvPrepareAndSwitchIntegrationTest.java`（NEW）

- 端到端流程：写入 DvEntry → handleChangelogSynced → handlePrepare → handleReadableSwitch → 验证状态
- 并发安全：写路径（handleChangelogSynced）与 Prepare/Switch 的锁互斥
- First-time bootstrap：空 RowPosIndex + 空 PendingDeletes → Ingest → batch resolve 是 no-op

---

## 关键文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-server/.../kv/dv/DvManager.java` | MODIFY | 新增 handlePrepare(), handleReadableSwitch(), batch resolve, cleanup 逻辑 |
| `fluss-server/.../kv/KvTablet.java` | MODIFY | 新增 getDvManager(), isDvEnabled() |
| `fluss-server/.../replica/ReplicaManager.java` | MODIFY | 实现 handleDvPrepare(), handleDvReadableSwitch() |
| `fluss-server/src/test/.../kv/dv/DvManagerPrepareTest.java` | NEW | Prepare 阶段测试 |
| `fluss-server/src/test/.../kv/dv/DvManagerReadableSwitchTest.java` | NEW | Readable Switch 阶段测试 |
| `fluss-server/src/test/.../kv/dv/DvPrepareAndSwitchIntegrationTest.java` | NEW | 端到端集成测试 |

---

## 设计要点

### Prepare 无需加锁

Prepare 不持有 DvRWLock。FileDict 写入走 RocksDB 线程安全接口，`handleChangelogSynced` 不碰 FileDict，union read 读到新 entry 也无影响（对应的 LakeDv 条目要到 Switch 才产生）。内存 pending maps 只有 Prepare/Switch 串行访问。

### Readable Switch 的原子性

所有 DV 状态变更（Ingest、batch resolve、cleanup、更新 readableSnapshotId）在同一个 DvRWLock 写锁内完成，保证与写路径（`handleChangelogSynced`）和读路径（union read）的互斥。

### 幂等性

- **Prepare**：开头检查 `preparedSnapshotId == snapshotId`，如果已完成则直接跳过，避免重复下载 SST。`preparedSnapshotId` 在 Prepare 最后一步（write lock 内）才设置
- **Readable Switch**：开头检查 `readableSnapshotId == snapshotId`，如果已完成则直接跳过。`readableSnapshotId` 是 Switch 的最后一步才更新的，因此该检查可靠区分"已完成"和"未完成/部分失败"。Switch 内部全是本地 RocksDB 操作、都在 write lock 下，部分失败意味着 RocksDB 出了严重问题，不需要额外处理

### Batch Resolve 的正确性（设计文档 §5.4）

SST 条目分两类：
- **(A) 新写入行**：本轮 tiering 的 +I/+U，RowId 在 split offset 范围内，不在 PendingDeletes 中。若在 Ingest 前被删除，§4.2 写 pending 到 PendingDeletes，batch resolve 命中后修补 LakeDv。
- **(B) 外部 compaction 行**：RowId 来自早期 tiering，若被 -U/-D 删除，§4.2 已写 PendingDeletes。batch resolve 精确命中并修补 LakeDv。

### 与 Coordinator 编排的配合

```
Coordinator                  TabletServer
    │                            │
    ├─ NotifyLakeTableOffset ──→ │ handleDvPrepare()
    │   (+ DvPrepare)            │   ├─ Phase 1: download SST (no lock)
    │                            │   └─ Phase 2: write FileDict (write lock)
    │ ←── response (Ready ACK) ──┤
    │                            │
    ├─ Mark readable in ZK       │
    │                            │
    ├─ DvReadableSwitchRequest ─→│ handleDvReadableSwitch()
    │                            │   └─ write lock: Ingest + resolve + cleanup
    │ ←── response (Switch ACK) ─┤
```

---

## 前置依赖

- PR 1: DvRocksDB + 核心数据结构
- PR 3: DvManager + KvTablet DV 写路径
- PR 4: SST 基础设施（RowPosSstDownloader）
- PR 5: Protocol 扩展 + Coordinator DV 编排（DvPrepareData、ReplicaManager stubs）

---

## 验证步骤

```bash
# 1. 编译
./mvnw compile -pl fluss-server -am -DskipTests

# 2. 格式化
./mvnw spotless:apply -pl fluss-server

# 3. 单元测试
./mvnw test -pl fluss-server -Dtest="DvManagerPrepareTest,DvManagerReadableSwitchTest"

# 4. 集成测试
./mvnw test -pl fluss-server -Dtest="DvPrepareAndSwitchIntegrationTest"

# 5. 已有测试不受影响
./mvnw test -pl fluss-server -Dtest="CoordinatorEventProcessorTest"
```
