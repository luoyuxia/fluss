# PR 7: Union Read DV 支持

## 目标

在 Flink union read 中应用三层 DV（Paimon/Iceberg DV + LakeDv + LogDv），使查询结果正确排除已删除/已更新的行。

**设计文档参考**：`fluss-deletion-vector-design-v3-en.md` §6

---

## 背景：Union Read 数据流

Union Read = 读取 lake snapshot（Paimon/Iceberg 历史数据）+ 读取 Fluss changelog（实时增量数据），合并为完整视图。

三层 DV 在 union read 中的作用：
1. **Lake DV（Paimon DV / Iceberg DV）**：lake 层面的物理 DV（compaction 产生），由 LakeSource 内部透明应用
2. **LakeDv**：Fluss TabletServer 维护的逻辑 DV，标记 lake 数据文件中已被后续 -U/-D 删除的行（file_id + row_position）
3. **LogDv**：Fluss TabletServer 维护的逻辑 DV，标记 changelog 中已被后续操作覆盖的记录（log offset）

当前 union read（非 DV 表）的执行路径：
```
FlinkSourceEnumerator
  └─ LakeSplitGenerator.generateHybridLakeFlussSplits()
       └─ 为每个 bucket 生成 LakeSnapshotAndFlussLogSplit（lake splits + log offset range）

FlinkSourceSplitReader
  ├─ 读 lake splits → SeekableLakeSnapshotSplitScanner → LakeSnapshotScanner
  └─ 读 log splits → LogScanner
```

DV 表的 union read 需要在上述路径中注入 LakeDv 和 LogDv 过滤。

---

## 整体设计

### Union Read 流程（设计文档 §6）

1. Client 获取最新 DV-readable snapshot ID（`requestedSnapshotId`）
2. Client 列出该 snapshot 下的数据文件
3. **TabletServer 侧**：加 DvRWLock 读锁，校验 snapshot 一致性，快照 LakeDv + LogDv，返回 `{lakeDv, logDv, logEndOffset}`
4. **Client 侧**：
   - 读 lake 数据文件，应用 Lake DV（物理）+ LakeDv（逻辑），过滤已删除行
   - 读 `[snapshotStartOffset, logEndOffset]` 范围的 changelog，应用 LogDv，跳过已覆盖记录
   - 合并结果

### 关键设计决策

1. **DV 数据获取方式**：通过新 RPC `GetDvSnapshot` 从 TabletServer 获取 LakeDv + LogDv，而不是在 split 中携带。原因：
   - DV 快照需要在 DvRWLock 读锁下获取，保证一致性
   - Bitmap 数据可能较大，不适合序列化到 split checkpoint
   - 每个 reader 直接从 TabletServer 获取自己 bucket 的 DV

2. **Snapshot 一致性**：每次 union read 绑定一个 `readableSnapshotId`，所有 bucket 使用同一个 snapshot。如果 TabletServer 已切换到更新的 snapshot，返回 stale error，client 刷新后重试。

3. **过滤位置**：LakeDv 过滤在 lake split scanner 中，LogDv 过滤在 log scanner 中，各自独立。

---

## Step 1: Proto 消息定义 — GetDvSnapshot RPC

**文件**: `fluss-rpc/src/main/proto/FlussApi.proto`

新增 DV 快照获取 RPC：

```proto
message GetDvSnapshotRequest {
  required int64 table_id = 1;
  required int32 bucket_id = 2;
  required int64 readable_snapshot_id = 3;
}

message GetDvSnapshotResponse {
  // LakeDv: per-file deleted position bitmaps
  repeated PbLakeDvEntry lake_dv_entries = 1;
  // LogDv: deleted log offsets bitmap (serialized Roaring64Bitmap)
  optional bytes log_dv_bitmap = 2;
  // The log end offset at snapshot time
  required int64 log_end_offset = 3;
  // The log start offset for this snapshot (snapshotStartLogOffset)
  required int64 snapshot_start_offset = 4;
}

message PbLakeDvEntry {
  required int32 file_id = 1;
  // Serialized Roaring64Bitmap of deleted row positions
  required bytes deleted_positions_bitmap = 2;
}
```

**文件**: `fluss-rpc/.../protocol/ApiKeys.java`
- 新增 `GET_DV_SNAPSHOT(1063, 0, 0, PRIVATE)`

**文件**: `fluss-rpc/.../gateway/TabletServerGateway.java`
- 新增 `getDvSnapshot(GetDvSnapshotRequest)` 方法

Regenerate: `./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`

---

## Step 2: TabletServer DV 快照服务

### 2a. DvManager 新增 union read 接口

**文件**: `fluss-server/.../kv/dv/DvManager.java`

新增方法：

```java
/**
 * Get DV snapshot for union read. Must be called under DvRWLock read lock.
 *
 * @param requestedSnapshotId the readable snapshot ID requested by client
 * @return DvSnapshot containing lakeDv, logDv, logEndOffset
 * @throws StaleSnapshotException if requestedSnapshotId doesn't match current
 */
public DvSnapshot getDvForUnionRead(long requestedSnapshotId) {
    // 1. Verify snapshot consistency
    if (readableSnapshotId != requestedSnapshotId) {
        throw new StaleSnapshotException(readableSnapshotId, requestedSnapshotId);
    }
    // 2. Snapshot LakeDv: clone all bitmaps (under read lock, no concurrent writes)
    Map<Integer, byte[]> lakeDvEntries = lakeDv.getAllSerialized();
    // 3. Snapshot LogDv: range [snapshotStartLogOffset, logEndOffset)
    long logEndOffset = getLogEndOffset();
    byte[] logDvBitmap = logDv.snapshotSerialized(snapshotStartLogOffset, logEndOffset);
    // 4. Return
    return new DvSnapshot(lakeDvEntries, logDvBitmap, logEndOffset, snapshotStartLogOffset);
}
```

新增 `DvSnapshot` 数据类（放在 `fluss-server/.../kv/dv/DvSnapshot.java`）：
- `Map<Integer, byte[]> lakeDvEntries` — fileId → serialized Roaring64Bitmap
- `byte[] logDvBitmap` — serialized Roaring64Bitmap（可能为空）
- `long logEndOffset`
- `long snapshotStartOffset`

### 2b. DvManager 状态跟踪

DvManager 需要维护以下状态（部分在之前 PR 中已有 stub，需要正式实现）：
- `readableSnapshotId`：当前 DV-readable snapshot ID（Readable Switch 时更新）
- `snapshotStartLogOffset`：当前 readable snapshot 的 log 起始 offset（Readable Switch 时更新）

### 2c. LogDv snapshot 和 serialization

**文件**: `fluss-server/.../kv/dv/LogDv.java`

已有 `snapshot(fromOffset, toOffset)` 返回 `Roaring64Bitmap`。新增 serialized 版本避免中间对象：

```java
public byte[] snapshotSerialized(long fromOffset, long toOffset) {
    Roaring64Bitmap bitmap = snapshot(fromOffset, toOffset);
    return RoaringBitmapUtils.serializeRoaringBitmap64(bitmap);
}
```

### 2d. LakeDv serialized getAll

**文件**: `fluss-server/.../kv/dv/LakeDv.java`

新增方法：

```java
/** Returns all LakeDv entries with serialized bitmaps (for network transport). */
public Map<Integer, byte[]> getAllSerialized() {
    Map<Integer, byte[]> result = new HashMap<>();
    // iterate CF, for each fileId -> bitmap bytes (already stored as serialized bytes)
    // ...
    return result;
}
```

### 2e. ReplicaManager / TabletService 处理

**文件**: `fluss-server/.../replica/ReplicaManager.java`

```java
public GetDvSnapshotResponse getDvSnapshot(GetDvSnapshotRequest request) {
    TableBucket tb = new TableBucket(request.getTableId(), request.getBucketId());
    KvTablet kvTablet = getKvTabletOrThrow(tb);
    DvManager dvManager = kvTablet.getDvManager();

    // Acquire DvRWLock read lock
    dvManager.getDvRWLock().readLock().lock();
    try {
        DvSnapshot snapshot = dvManager.getDvForUnionRead(request.getReadableSnapshotId());
        return buildGetDvSnapshotResponse(snapshot);
    } finally {
        dvManager.getDvRWLock().readLock().unlock();
    }
}
```

**文件**: `fluss-server/.../tablet/TabletService.java`

```java
@Override
public CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(GetDvSnapshotRequest request) {
    CompletableFuture<GetDvSnapshotResponse> response = new CompletableFuture<>();
    try {
        response.complete(replicaManager.getDvSnapshot(request));
    } catch (Exception e) {
        response.completeExceptionally(e);
    }
    return response;
}
```

---

## Step 3: Client 侧 DV 数据获取

### 3a. DvSnapshotClient

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/lookup/DvSnapshotClient.java`（NEW）

提供获取 DV 快照的 client API：

```java
@Internal
public class DvSnapshotClient {
    private final RpcClient rpcClient;
    private final MetadataUpdater metadataUpdater;

    /**
     * Fetch DV snapshot for a specific bucket from its leader TabletServer.
     */
    public CompletableFuture<DvSnapshotData> getDvSnapshot(
            long tableId, int bucketId, long readableSnapshotId) {
        // 1. Find leader server for this bucket
        // 2. Send GetDvSnapshotRequest
        // 3. Parse response into DvSnapshotData
    }
}
```

### 3b. DvSnapshotData

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/lookup/DvSnapshotData.java`（NEW）

```java
@Internal
public class DvSnapshotData {
    private final Map<Integer, Roaring64Bitmap> lakeDv;  // fileId → deleted positions
    private final Roaring64Bitmap logDv;                  // deleted log offsets
    private final long logEndOffset;
    private final long snapshotStartOffset;
    // constructor, getters
}
```

注意：`Roaring64Bitmap` 依赖 `org.roaringbitmap` 库。需要确认该库在 `fluss-client` 或 `fluss-common` 模块是否可用，或者是否需要在 `fluss-flink-common` 中处理反序列化。

**替代方案**：如果 `Roaring64Bitmap` 不适合放在 `fluss-client`，可以将 DV 数据保持为 `byte[]` 形式在 client 传输，在 Flink 侧反序列化。

---

## Step 4: Flink Source 获取 readable snapshot ID

### 4a. LakeSnapshot 扩展

**文件**: `fluss-client/src/main/java/org/apache/fluss/client/metadata/LakeSnapshot.java`

当前 `LakeSnapshot` 包含 `snapshotId` 和 `tableBucketsOffset`。对于 DV 表，`snapshotId` 即是 `readableSnapshotId`（Coordinator 只有在标记 readable 后才会通知）。

无需修改——`LakeSnapshot.getSnapshotId()` 已可作为 `readableSnapshotId` 使用。

### 4b. LakeSplitGenerator 传递 readableSnapshotId

**文件**: `fluss-flink/fluss-flink-common/.../lake/LakeSplitGenerator.java`

`generateHybridLakeFlussSplits()` 已经获取 `LakeSnapshot`，其中包含 `snapshotId`。需要将 `readableSnapshotId` 传递到生成的 split 中。

---

## Step 5: Split 扩展 — 携带 DV 元数据

### 5a. LakeSnapshotAndFlussLogSplit 扩展

**文件**: `fluss-flink/fluss-flink-common/.../lake/split/LakeSnapshotAndFlussLogSplit.java`

新增字段：

```java
// DV fields (nullable - only for DV-enabled tables)
@Nullable private final Long readableSnapshotId;
private final boolean dvEnabled;
```

- `readableSnapshotId`：用于向 TabletServer 请求 DV 快照
- `dvEnabled`：标记是否需要 DV 过滤

更新序列化/反序列化以包含这些字段（向后兼容：旧 split 没有这些字段时默认 `dvEnabled = false`）。

### 5b. Split Serializer 更新

更新 `LakeSnapshotAndFlussLogSplitSerializer`（或相关 serializer），序列化/反序列化新增的 DV 字段。

---

## Step 6: Flink Source Reader DV 过滤

### 6a. DV 数据获取时机

在 `FlinkSourceSplitReader` 开始读取一个 `LakeSnapshotAndFlussLogSplit` 时：

1. 检查 `dvEnabled`
2. 如果启用，通过 `DvSnapshotClient` 向该 bucket 的 leader TabletServer 发送 `GetDvSnapshotRequest(tableId, bucketId, readableSnapshotId)`
3. 获取 `DvSnapshotData`（lakeDv + logDv + logEndOffset + snapshotStartOffset）
4. 将 DV 数据传递给 lake scanner 和 log scanner

### 6b. Lake Split Scanner DV 过滤

**文件**: `fluss-flink/fluss-flink-common/.../lake/reader/LakeSnapshotScanner.java`（MODIFY）

或新建 wrapper：

在读取 lake 数据文件时，需要按 `(file_id, row_position)` 过滤。关键问题：如何获取当前记录的 file_id 和 row_position？

**方案**：依赖 LakeSource 提供的元数据。Lake 层面的 split 已经关联了特定的数据文件，需要：
- 从 `LakeSplit` 获取关联的 file path
- 通过 FileDict 映射 file_path → file_id（FileDict 信息需要从 TabletServer 获取，或者在 GetDvSnapshotResponse 中附带）
- row_position 按文件内行号递增

**简化方案**：在 `GetDvSnapshotResponse` 中直接用 `file_path` 替代 `file_id` 作为 LakeDv 的 key，避免 client 侧做 FileDict 映射。TabletServer 在快照 LakeDv 时将 file_id 通过 FileDict 转换为 file_path。

```proto
message PbLakeDvEntry {
  required string file_path = 1;  // 改用 file_path
  required bytes deleted_positions_bitmap = 2;
}
```

这样 Flink reader 可以直接用 split 的 file path 查找对应的 LakeDv bitmap。

**过滤逻辑**：

```java
// 在 LakeSnapshotScanner 或新建 DvAwareLakeSnapshotScanner 中
Roaring64Bitmap deletedPositions = lakeDv.get(currentFilePath);
if (deletedPositions != null) {
    // 跳过 deletedPositions 中标记的行
    while (iterator.hasNext()) {
        LogRecord record = iterator.next();
        if (!deletedPositions.contains(rowPosition)) {
            emit(record);
        }
        rowPosition++;
    }
}
```

### 6c. Log Split Scanner DV 过滤

**文件**: `fluss-flink/fluss-flink-common/.../source/reader/FlinkSourceSplitReader.java`（MODIFY）

在读取 changelog 时，需要按 log offset 过滤。LogDv 是一个 `Roaring64Bitmap`，包含所有被覆盖的 offset。

**过滤逻辑**：

```java
// 在 log 读取路径中
ScanRecord record = logScanner.poll();
if (logDv != null && logDv.contains(record.getOffset())) {
    // 跳过已被覆盖的记录
    continue;
}
```

**Log 读取范围**：从 `snapshotStartOffset`（lake snapshot 的 offset）到 `logEndOffset`（DV 快照时的 log end），超出 `logEndOffset` 的记录不需要过滤（它们是 DV 快照之后到达的新数据）。

### 6d. logEndOffset 与 stoppingOffset 的关系

对于 streaming union read：
- 读 lake splits（应用 LakeDv 过滤）
- 读 log `[snapshotStartOffset, logEndOffset]`（应用 LogDv 过滤）
- 读 log `(logEndOffset, +∞)`（无 DV 过滤，正常 streaming 读取）

对于 batch union read：
- stoppingOffset 已经由 split 定义，LogDv 只需应用在 `[snapshotStartOffset, min(logEndOffset, stoppingOffset)]` 范围内

---

## Step 7: FileDict 映射处理

TabletServer 返回 DV 快照时，需要将 LakeDv 的 file_id 转换为 file_path，以便 client 侧直接匹配 lake split 中的文件路径。

**文件**: `fluss-server/.../kv/dv/DvManager.java`

在 `getDvForUnionRead` 中，LakeDv 快照使用 FileDict 将 file_id 解析为 file_path：

```java
Map<Integer, byte[]> rawLakeDv = lakeDv.getAllSerialized();
Map<String, byte[]> resolvedLakeDv = new HashMap<>();
for (Map.Entry<Integer, byte[]> entry : rawLakeDv.entrySet()) {
    String filePath = fileDict.getFilePath(entry.getKey());
    if (filePath != null) {
        resolvedLakeDv.put(filePath, entry.getValue());
    }
}
```

---

## Step 8: 异常处理 — Stale Snapshot

### 8a. StaleSnapshotException

**文件**: `fluss-common/.../exception/StaleSnapshotException.java`（NEW）

```java
public class StaleSnapshotException extends FlussException {
    private final long currentSnapshotId;
    private final long requestedSnapshotId;
    // ...
}
```

### 8b. Client 重试逻辑

在 `FlinkSourceSplitReader` 获取 DV 快照时，处理 stale snapshot：

- `requestedSnapshotId < currentSnapshotId`：TabletServer 已切换到更新 snapshot。Client 刷新 `readableSnapshotId` 为 `currentSnapshotId`，重新获取 lake splits，重试。
- `requestedSnapshotId > currentSnapshotId`：TabletServer 尚未完成 switch。Client 保持 `requestedSnapshotId` 不变，backoff 重试。

---

## Step 9: 测试

### 9a. 单元测试

**文件**: `fluss-server/src/test/.../kv/dv/DvManagerUnionReadTest.java`（NEW）
- `getDvForUnionRead` 返回正确的 LakeDv + LogDv 快照
- Snapshot 一致性校验：requestedSnapshotId != readableSnapshotId → 抛异常
- 空 DV 场景
- FileDict file_id → file_path 解析

**文件**: `fluss-rpc/src/test/.../messages/GetDvSnapshotMessageTest.java`（NEW）
- GetDvSnapshotRequest/Response 序列化/反序列化
- PbLakeDvEntry bitmap 正确传输

### 9b. 集成测试

**文件**: `fluss-lake/fluss-lake-paimon/src/test/.../FlinkUnionReadDvTableITCase.java`（已存在）
- 验证三层 DV 协作：Paimon DV + LakeDv + LogDv 同时生效
- 数据正确性端到端验证
- 边界场景：空 DV、全部删除、split 跨多文件

---

## 关键文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-rpc/.../proto/FlussApi.proto` | MODIFY | 新增 GetDvSnapshot RPC messages |
| `fluss-rpc/.../protocol/ApiKeys.java` | MODIFY | 新增 GET_DV_SNAPSHOT |
| `fluss-rpc/.../gateway/TabletServerGateway.java` | MODIFY | 新增 getDvSnapshot() |
| `fluss-server/.../kv/dv/DvManager.java` | MODIFY | 新增 getDvForUnionRead() |
| `fluss-server/.../kv/dv/DvSnapshot.java` | NEW | DV 快照数据类 |
| `fluss-server/.../kv/dv/LogDv.java` | MODIFY | 新增 snapshotSerialized() |
| `fluss-server/.../kv/dv/LakeDv.java` | MODIFY | 新增 getAllSerialized() |
| `fluss-server/.../replica/ReplicaManager.java` | MODIFY | 新增 getDvSnapshot() |
| `fluss-server/.../tablet/TabletService.java` | MODIFY | 新增 getDvSnapshot handler |
| `fluss-common/.../exception/StaleSnapshotException.java` | NEW | Stale snapshot 异常 |
| `fluss-flink/.../lake/split/LakeSnapshotAndFlussLogSplit.java` | MODIFY | 新增 readableSnapshotId, dvEnabled |
| `fluss-flink/.../lake/LakeSplitGenerator.java` | MODIFY | 传递 readableSnapshotId |
| `fluss-flink/.../source/reader/FlinkSourceSplitReader.java` | MODIFY | DV 获取 + LogDv 过滤 |
| `fluss-flink/.../lake/reader/LakeSnapshotScanner.java` | MODIFY | LakeDv 过滤 |

---

## 前置依赖

- PR 1: DvRocksDB + 核心数据结构（LakeDv, LogDv, FileDict）
- PR 3: DvManager（getDvForUnionRead 依赖 DvManager 状态）
- PR 5: Protocol 扩展（CoordinatorEventProcessor 中的 readable snapshot 管理）
- PR 6: TabletServer Prepare + Readable Switch（readableSnapshotId 和 snapshotStartLogOffset 的维护）

---

## 验证步骤

```bash
# 1. Proto 重新生成
./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc

# 2. 编译
./mvnw compile -pl fluss-server,fluss-client -am -DskipTests
./mvnw compile -pl fluss-flink/fluss-flink-common -am -DskipTests

# 3. 格式化
./mvnw spotless:apply -pl fluss-server,fluss-client,fluss-rpc
./mvnw spotless:apply -pl fluss-flink/fluss-flink-common

# 4. 单元测试
./mvnw test -pl fluss-rpc -Dtest=GetDvSnapshotMessageTest
./mvnw test -pl fluss-server -Dtest=DvManagerUnionReadTest

# 5. 集成测试
./mvnw test -pl fluss-lake/fluss-lake-paimon -Dtest=FlinkUnionReadDvTableITCase
```
