# PR 5: Protocol 扩展 + Coordinator DV 编排

## 目标

扩展现有 RPC 协议以支持 DV 编排流程（Position Report / Prepare / Publish / Readable Switch），在 CoordinatorServer 中实现 DV 编排状态机。

**核心设计原则**：最大程度复用现有 RPC，减少新增接口。仅新增 1 个 RPC（`DvReadableSwitch`），其余通过在现有 `CommitLakeTableSnapshot` 和 `NotifyLakeTableOffset` 消息上挂载独立 DV message 实现。

## 设计文档参考

- [fluss-paimon-deletion-vector-design.md](../fluss-paimon-deletion-vector-design.md) §5.2.2 Step 6, §5.3, §5.4
- [paimon-dv-implementation-plan.md](paimon-dv-implementation-plan.md) PR 5

---

## RPC 编排全景

```
Phase A3 完成后:

TieringService ──CommitLakeTableSnapshot(+PbDvPositionReport)──→ Coordinator       (复用)
                                                                    │
                                                            ┌───────┴────────┐
                                                            │ 1. Parse DV    │
                                                            │    Position    │
                                                            │    Report      │
                                                            └───────┬────────┘
                                                                    │
Coordinator ────NotifyLakeTableOffset(+PbDvPrepare)────────────────→ TabletServer    (复用, 同步)
                                                                    │
                                                            ┌───────┴────────┐
                                                            │ 2. Download    │
                                                            │    SST +       │
                                                            │    Write       │
                                                            │    FileDict    │
                                                            └───────┬────────┘
                                                                    │
TabletServer ──────── Response (Ready ACK) ────────────────────────→ Coordinator
                                                                    │
                                                            ┌───────┴────────┐
                                                            │ 3. Publish     │
                                                            │    (ZK update) │
                                                            └───────┬────────┘
                                                                    │
Coordinator ────DvReadableSwitchRequest────────────────────────────→ TabletServer    (新增, 同步)
                                                                    │
                                                            ┌───────┴────────┐
                                                            │ 4. Ingest SST  │
                                                            │    + Batch     │
                                                            │    Resolve     │
                                                            └───────┬────────┘
                                                                    │
TabletServer ──────── Response (Switched ACK) ─────────────────────→ Coordinator
```

---

## 改动清单

### 1. Proto 消息扩展

**文件**：`fluss-rpc/src/main/proto/FlussApi.proto`（修改）

#### 1a. 新增 DV 独立 Message 类型

```proto
// ============================================================
// Deletion Vector (DV) Messages
// ============================================================

// DV Position Report: TieringService 完成 Phase A3 后报告的数据
// 挂载在 PbLakeTableSnapshotMetadata 上
// 注：snapshot_id 由父 message PbLakeTableSnapshotMetadata.snapshot_id 提供，
// 远程存储路径为 rowPos/{snapshotId}/
message PbDvPositionReport {
  repeated PbFileDictEntry new_file_dict_entries = 1;     // 本轮新分配的 fileId -> filePath 映射
  repeated string old_files = 2;                          // compaction 中被替换的旧文件路径列表
  repeated PbDvBucketOffset bucket_offsets = 3;           // per-bucket 的 readableOffset
}

// Per-bucket readable offset（tieredOffset 已由现有 log_end_offset 传递，无需重复）
message PbDvBucketOffset {
  required int32 bucket_id = 1;
  required int64 readable_offset = 2;                     // compaction 输出覆盖到的最大 offset，用于 batch resolve 孤儿判断
}

// FileDict 条目: fileId <-> filePath 的映射
message PbFileDictEntry {
  required int32 file_id = 1;
  required string file_path = 2;
}

// DV Prepare: Coordinator 向 TabletServer 发送的 Prepare 数据
// 挂载在 NotifyLakeTableOffsetRequest 上
// readable_snapshot_id 同时作为远程存储路径标识: rowPos/{readableSnapshotId}/
message PbDvPrepare {
  required int64 table_id = 1;                            // 表 ID
  required int64 readable_snapshot_id = 2;                // readable snapshot ID，同时用于定位远程 SST 目录
  repeated PbFileDictEntry new_file_dict_entries = 3;     // FileDict 新条目（需写入本地 DvRocksDB）
  repeated string old_files = 4;                          // 被替换的旧文件路径
  repeated PbDvBucketOffset bucket_offsets = 5;           // per-bucket offset
}
```

#### 1b. 扩展现有 Message

**PbLakeTableSnapshotMetadata**（已有字段 1-5，新增字段 6）：

```proto
message PbLakeTableSnapshotMetadata {
  required int64 table_id = 1;
  required int64 snapshot_id = 2;
  required string tiered_bucket_offsets_file_path = 3;
  optional string readable_bucket_offsets_file_path = 4;
  optional int64 earliest_snapshot_id_to_keep = 5;
  optional PbDvPositionReport dv_position_report = 6;     // <-- 新增
}
```

**NotifyLakeTableOffsetRequest**（已有字段 1-2，新增字段 3）：

```proto
message NotifyLakeTableOffsetRequest {
  required int32 coordinator_epoch = 1;
  repeated PbNotifyLakeTableOffsetReqForBucket notify_buckets_req = 2;
  optional PbDvPrepare dv_prepare = 3;                    // <-- 新增
}
```

#### 1c. 新增 RPC：DvReadableSwitch

```proto
message DvReadableSwitchRequest {
  required int32 coordinator_epoch = 1;
  required int64 table_id = 2;
  required int64 readable_snapshot_id = 3;                // 本轮 Publish 的 readable snapshot ID
}

message DvReadableSwitchResponse {
  // 空 message 即 Switched ACK
}
```

#### 1d. 注册新 RPC 到 Service

在 `ApiMessageType` / Service 定义中注册 `DvReadableSwitchRequest` → `DvReadableSwitchResponse` RPC。

参考已有 RPC 注册方式：
- `fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/ApiKeys.java` 中新增 `DV_READABLE_SWITCH` API Key
- `fluss-rpc/src/main/java/org/apache/fluss/rpc/messages/` 下生成对应 Message 类
- 需要重新执行 `./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`

---

### 2. Server 侧数据类扩展

#### 2a. DvPositionReportData（新建）
**文件**：`fluss-server/.../entity/DvPositionReportData.java`（新建）

```java
@Internal
public class DvPositionReportData {
    private final Map<Integer, String> newFileDictEntries;  // fileId -> filePath
    private final List<String> oldFiles;
    private final Map<Integer, DvBucketOffset> bucketOffsets;  // bucketId -> offset

    // 构造方法、getters
    // 注：snapshotId 由外层 LakeSnapshotMetadata 提供，不重复存储

    public static class DvBucketOffset {
        private final long readableOffset;
        // 构造方法、getters
    }
}
```

#### 2b. DvPrepareData（新建）
**文件**：`fluss-server/.../entity/DvPrepareData.java`（新建）

```java
@Internal
public class DvPrepareData {
    private final long tableId;
    private final long readableSnapshotId;                   // 同时用于定位远程 SST: rowPos/{readableSnapshotId}/
    private final Map<Integer, String> newFileDictEntries;  // fileId -> filePath
    private final List<String> oldFiles;
    private final Map<Integer, DvPositionReportData.DvBucketOffset> bucketOffsets;
    // 构造方法、getters
}
```

#### 2c. DvReadableSwitchData（新建）
**文件**：`fluss-server/.../entity/DvReadableSwitchData.java`（新建）

```java
@Internal
public class DvReadableSwitchData {
    private final int coordinatorEpoch;
    private final long tableId;
    private final long readableSnapshotId;
    // 构造方法、getters
}
```

#### 2d. 扩展 CommitLakeTableSnapshotsData（修改）
**文件**：`fluss-server/.../entity/CommitLakeTableSnapshotsData.java`（修改）

在 `CommitLakeTableSnapshot` 内部类新增字段：

```java
public static class CommitLakeTableSnapshot {
    // ... 已有字段 ...
    @Nullable private final DvPositionReportData dvPositionReport;  // <-- 新增

    // 更新构造方法和 getter
}
```

#### 2e. 扩展 NotifyLakeTableOffsetData（修改）
**文件**：`fluss-server/.../entity/NotifyLakeTableOffsetData.java`（修改）

```java
public class NotifyLakeTableOffsetData {
    private final int coordinatorEpoch;
    private final Map<TableBucket, LakeBucketOffset> lakeBucketOffsets;
    @Nullable private final DvPrepareData dvPrepare;  // <-- 新增

    // 更新构造方法和 getter
}
```

---

### 3. ServerRpcMessageUtils 扩展

**文件**：`fluss-server/.../utils/ServerRpcMessageUtils.java`（修改）

#### 3a. Proto → Data 转换（反序列化方向）

**扩展 `getCommitLakeTableSnapshotData()`**：

在处理 V2 格式的 `PbLakeTableSnapshotMetadata` 时，检查 `has_dv_position_report`：

```java
// 在已有的 V2 循环中
DvPositionReportData dvPositionReport = null;
if (pbMetadata.hasDvPositionReport()) {
    PbDvPositionReport pbReport = pbMetadata.getDvPositionReport();
    dvPositionReport = parseDvPositionReport(pbReport);
}
// 传入 builder.addTableSnapshot(..., dvPositionReport)
```

新增 helper 方法：

```java
private static DvPositionReportData parseDvPositionReport(PbDvPositionReport pb) {
    Map<Integer, String> newFileDictEntries = new HashMap<>();
    for (PbFileDictEntry entry : pb.getNewFileDictEntriesList()) {
        newFileDictEntries.put(entry.getFileId(), entry.getFilePath());
    }

    List<String> oldFiles = new ArrayList<>(pb.getOldFilesList());

    Map<Integer, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
    for (PbDvBucketOffset offset : pb.getBucketOffsetsList()) {
        bucketOffsets.put(offset.getBucketId(),
                new DvPositionReportData.DvBucketOffset(offset.getReadableOffset()));
    }

    return new DvPositionReportData(newFileDictEntries, oldFiles, bucketOffsets);
}
```

**扩展 `getNotifyLakeTableOffset()`**：

```java
DvPrepareData dvPrepare = null;
if (request.hasDvPrepare()) {
    PbDvPrepare pb = request.getDvPrepare();
    dvPrepare = parseDvPrepare(pb);
}
return new NotifyLakeTableOffsetData(coordinatorEpoch, lakeBucketOffsetMap, dvPrepare);
```

**新增 `getDvReadableSwitchData()`**：

```java
public static DvReadableSwitchData getDvReadableSwitchData(DvReadableSwitchRequest request) {
    return new DvReadableSwitchData(
            request.getCoordinatorEpoch(),
            request.getTableId(),
            request.getReadableSnapshotId());
}
```

#### 3b. Data → Proto 转换（序列化方向，用于 Coordinator 构建请求）

**新增 `buildDvPrepareMessage()`**：

```java
public static PbDvPrepare buildDvPrepareMessage(DvPrepareData data) {
    PbDvPrepare.Builder builder = PbDvPrepare.newBuilder()
            .setTableId(data.getTableId())
            .setReadableSnapshotId(data.getReadableSnapshotId());

    for (Map.Entry<Integer, String> entry : data.getNewFileDictEntries().entrySet()) {
        builder.addNewFileDictEntries(
                PbFileDictEntry.newBuilder()
                        .setFileId(entry.getKey())
                        .setFilePath(entry.getValue())
                        .build());
    }
    for (String oldFile : data.getOldFiles()) {
        builder.addOldFiles(oldFile);
    }
    for (Map.Entry<Integer, DvPositionReportData.DvBucketOffset> entry :
            data.getBucketOffsets().entrySet()) {
        builder.addBucketOffsets(
                PbDvBucketOffset.newBuilder()
                        .setBucketId(entry.getKey())
                        .setReadableOffset(entry.getValue().getReadableOffset())
                        .build());
    }

    return builder.build();
}
```

> 注：由于 Fluss 使用 fluss-protogen 生成 proto 代码（非标准 protobuf-java），实际构建方式可能使用 setter 链而非 builder 模式。参考已有代码的 setter 用法：`new PbDvPrepare().setTableId(...).setReadableSnapshotId(...)`。

---

### 4. CoordinatorServer DV 编排

#### 4a. 新增 DvOrchestrationEvent（新建）
**文件**：`fluss-server/.../coordinator/event/DvOrchestrationEvent.java`（新建）

```java
@Internal
public class DvOrchestrationEvent implements CoordinatorEvent {
    private final long tableId;
    private final DvPositionReportData dvPositionReport;
    private final LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata;
    @Nullable private final Long earliestSnapshotIdToKeep;
    private final CompletableFuture<Void> callback;

    // 构造方法、getters
}
```

#### 4b. 扩展 CoordinatorEventProcessor（修改）
**文件**：`fluss-server/.../coordinator/CoordinatorEventProcessor.java`（修改）

**修改 `handleCommitLakeTableSnapshotV2()`**：

在已有的 V2 处理循环中，当 `dvPositionReport != null` 时，不直接触发 `notifyLakeTableOffsets()`，而是发起 DV 编排流程：

```java
// 在已有的 for 循环中，注册完 snapshot 后
CommitLakeTableSnapshotsData.CommitLakeTableSnapshot snapshot = entry.getValue();

if (snapshot.getDvPositionReport() != null) {
    // DV 表: 触发 DV 编排流程（Prepare → Publish → Switch）
    processDvOrchestration(
            tableId,
            snapshot.getDvPositionReport(),
            snapshot.getLakeSnapshotMetadata(),
            snapshot.getEarliestSnapshotIDToKeep());
} else {
    // 非 DV 表: 走原有 notifyLakeTableOffsets 路径
    // ... 保持不变 ...
}
```

**新增 `processDvOrchestration()` 方法**：

```java
private void processDvOrchestration(
        long tableId,
        DvPositionReportData dvReport,
        LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata,
        @Nullable Long earliestSnapshotIdToKeep) {

    // Step 1: Prepare —— 向所有相关 TabletServer 发送 NotifyLakeTableOffset + PbDvPrepare
    DvPrepareData dvPrepare = new DvPrepareData(
            tableId,
            lakeSnapshotMetadata.getSnapshotId(),  // readableSnapshotId，同时用于定位 rowPos/{id}/
            dvReport.getNewFileDictEntries(),
            dvReport.getOldFiles(),
            dvReport.getBucketOffsets());

    // 找到该表所有 bucket 的 TabletServer
    Set<Integer> tabletServerIds = collectTabletServersForTable(tableId, dvReport);

    // 构建并发送 Prepare 请求（同步等待所有 Ready ACK）
    List<CompletableFuture<Void>> prepareFutures = new ArrayList<>();
    for (int serverId : tabletServerIds) {
        CompletableFuture<Void> future = sendDvPrepareRequest(serverId, dvPrepare);
        prepareFutures.add(future);
    }

    FutureUtils.completeAll(prepareFutures).thenAccept(ignored -> {
        // Step 2: Publish —— 所有 TabletServer Ready 后，更新 ZK 注册 readable snapshot
        try {
            lakeTableHelper.registerLakeTableSnapshotV2(
                    tableId, lakeSnapshotMetadata, earliestSnapshotIdToKeep);
        } catch (Exception e) {
            LOG.error("Failed to publish readable snapshot for table {}", tableId, e);
            return;
        }

        // Step 3: Readable Switch —— 向所有 TabletServer 发送 Switch 请求
        List<CompletableFuture<Void>> switchFutures = new ArrayList<>();
        for (int serverId : tabletServerIds) {
            CompletableFuture<Void> future = sendDvReadableSwitchRequest(
                    serverId, tableId, lakeSnapshotMetadata.getSnapshotId());
            switchFutures.add(future);
        }

        FutureUtils.completeAll(switchFutures).thenAccept(ignored2 -> {
            LOG.info("DV orchestration completed for table {}, snapshot {}",
                    tableId, lakeSnapshotMetadata.getSnapshotId());

            // Step 4: 发送普通的 notifyLakeTableOffsets 通知
            // (通知 log offset 更新，与 DV 无关的部分)
            notifyLakeTableOffsetsForDvTable(tableId, lakeSnapshotMetadata);
        });
    }).exceptionally(t -> {
        LOG.error("DV orchestration failed for table {}", tableId, t);
        return null;
    });
}
```

> **注**：以上是概念性伪代码。实际实现中需要考虑：
> - Coordinator 的事件驱动模型——可能需要将 Prepare/Publish/Switch 拆成多个 Event
> - 错误处理和超时
> - Coordinator failover 后的幂等性
> - `ioExecutor` 与 `eventExecutor` 的线程分工

#### 4c. sendDvPrepareRequest 方法

复用已有的 `NotifyLakeTableOffsetRequest` 通道：

```java
private CompletableFuture<Void> sendDvPrepareRequest(int serverId, DvPrepareData dvPrepare) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    NotifyLakeTableOffsetRequest request = new NotifyLakeTableOffsetRequest()
            .setCoordinatorEpoch(coordinatorContext.getCoordinatorEpoch())
            .setDvPrepare(ServerRpcMessageUtils.buildDvPrepareMessage(dvPrepare));
    // 注意：notify_buckets_req 可以为空（DV Prepare 不需要 per-bucket offset 通知）
    // 或者同时携带正常的 per-bucket 通知

    coordinatorChannelManager.sendNotifyLakeTableOffsetRequest(
            serverId,
            request,
            (response, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                } else {
                    future.complete(null);  // Ready ACK
                }
            });

    return future;
}
```

#### 4d. sendDvReadableSwitchRequest 方法

使用新增的 `DvReadableSwitchRequest` RPC：

```java
private CompletableFuture<Void> sendDvReadableSwitchRequest(
        int serverId, long tableId, long readableSnapshotId) {
    CompletableFuture<Void> future = new CompletableFuture<>();

    DvReadableSwitchRequest request = new DvReadableSwitchRequest()
            .setCoordinatorEpoch(coordinatorContext.getCoordinatorEpoch())
            .setTableId(tableId)
            .setReadableSnapshotId(readableSnapshotId);

    coordinatorChannelManager.sendDvReadableSwitchRequest(
            serverId,
            request,
            (response, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                } else {
                    future.complete(null);  // Switched ACK
                }
            });

    return future;
}
```

#### 4e. collectTabletServersForTable 方法

```java
private Set<Integer> collectTabletServersForTable(
        long tableId, DvPositionReportData dvReport) {
    Set<Integer> serverIds = new HashSet<>();
    for (int bucketId : dvReport.getBucketOffsets().keySet()) {
        TableBucket tb = new TableBucket(tableId, bucketId);
        coordinatorContext.getBucketLeaderAndIsr(tb).ifPresent(leaderAndIsr -> {
            // 收集所有 replica 所在的 TabletServer
            List<Integer> assignment = coordinatorContext.getAssignment(tb);
            assignment.stream().filter(s -> s >= 0).forEach(serverIds::add);
        });
    }
    return serverIds;
}
```

---

### 5. TabletServer 侧 RPC 处理框架

> **注**：TabletServer 侧的实际 Prepare 和 Switch 逻辑在 PR 6 中实现。本 PR 仅搭建 RPC 处理框架。

#### 5a. 扩展 ReplicaManager（修改）
**文件**：`fluss-server/.../replica/ReplicaManager.java`（修改）

**修改 `notifyLakeTableOffset()`**：

```java
public void notifyLakeTableOffset(
        NotifyLakeTableOffsetData notifyLakeTableOffsetData,
        Consumer<NotifyLakeTableOffsetResponse> responseCallback) {

    inLock(replicaStateChangeLock, () -> {
        validateAndApplyCoordinatorEpoch(
                notifyLakeTableOffsetData.getCoordinatorEpoch(),
                "notifyLakeTableOffset");

        // 处理 DV Prepare（如果存在）
        DvPrepareData dvPrepare = notifyLakeTableOffsetData.getDvPrepare();
        if (dvPrepare != null) {
            handleDvPrepare(dvPrepare);  // PR 6 实现
        }

        // 处理正常的 per-bucket offset 通知（保持不变）
        Map<TableBucket, LakeBucketOffset> lakeBucketOffsets =
                notifyLakeTableOffsetData.getLakeBucketOffsets();
        for (Map.Entry<TableBucket, LakeBucketOffset> entry : lakeBucketOffsets.entrySet()) {
            // ... 已有逻辑不变 ...
        }

        responseCallback.accept(new NotifyLakeTableOffsetResponse());
    });
}
```

**新增 `dvReadableSwitch()` 方法**：

```java
public void dvReadableSwitch(
        DvReadableSwitchData dvReadableSwitchData,
        Consumer<DvReadableSwitchResponse> responseCallback) {

    inLock(replicaStateChangeLock, () -> {
        validateAndApplyCoordinatorEpoch(
                dvReadableSwitchData.getCoordinatorEpoch(),
                "dvReadableSwitch");

        handleDvReadableSwitch(dvReadableSwitchData);  // PR 6 实现

        responseCallback.accept(new DvReadableSwitchResponse());
    });
}
```

#### 5b. 扩展 TabletService（修改）
**文件**：`fluss-server/.../tablet/TabletService.java`（修改）

注册 `DvReadableSwitch` RPC handler：

```java
@Override
public CompletableFuture<DvReadableSwitchResponse> dvReadableSwitch(
        DvReadableSwitchRequest request) {
    CompletableFuture<DvReadableSwitchResponse> response = new CompletableFuture<>();
    DvReadableSwitchData data = ServerRpcMessageUtils.getDvReadableSwitchData(request);
    replicaManager.dvReadableSwitch(data, response::complete);
    return response;
}
```

#### 5c. 扩展 CoordinatorChannelManager（修改）
**文件**：`fluss-server/.../coordinator/CoordinatorChannelManager.java`（修改）

新增发送 `DvReadableSwitchRequest` 的方法：

```java
public void sendDvReadableSwitchRequest(
        int serverId,
        DvReadableSwitchRequest request,
        BiConsumer<DvReadableSwitchResponse, Throwable> callback) {
    // 类似已有的 sendNotifyLakeTableOffsetRequest 实现
    sendRequest(serverId, request, callback);
}
```

---

### 6. ApiKeys 注册

**文件**：`fluss-rpc/.../protocol/ApiKeys.java`（修改）

新增 API Key：

```java
DV_READABLE_SWITCH(
    <next_id>,
    "DvReadableSwitch",
    DvReadableSwitchRequest.class,
    DvReadableSwitchResponse.class)
```

---

## 涉及文件列表

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-rpc/.../proto/FlussApi.proto` | 修改 | 新增 DV message + 扩展现有 message + DvReadableSwitch RPC |
| `fluss-rpc/.../protocol/ApiKeys.java` | 修改 | 注册 DV_READABLE_SWITCH API Key |
| `fluss-server/.../entity/DvPositionReportData.java` | 新建 | DV Position Report 服务端数据类 |
| `fluss-server/.../entity/DvPrepareData.java` | 新建 | DV Prepare 服务端数据类 |
| `fluss-server/.../entity/DvReadableSwitchData.java` | 新建 | DV Readable Switch 服务端数据类 |
| `fluss-server/.../entity/CommitLakeTableSnapshotsData.java` | 修改 | 新增 dvPositionReport 字段 |
| `fluss-server/.../entity/NotifyLakeTableOffsetData.java` | 修改 | 新增 dvPrepare 字段 |
| `fluss-server/.../utils/ServerRpcMessageUtils.java` | 修改 | Proto ↔ Data 转换（DV 相关字段） |
| `fluss-server/.../coordinator/CoordinatorEventProcessor.java` | 修改 | DV 编排状态机（Prepare → Publish → Switch） |
| `fluss-server/.../coordinator/CoordinatorChannelManager.java` | 修改 | 新增 sendDvReadableSwitchRequest |
| `fluss-server/.../coordinator/CoordinatorService.java` | 修改 | 注册 dvReadableSwitch RPC handler（Coordinator 侧） |
| `fluss-server/.../replica/ReplicaManager.java` | 修改 | 扩展 notifyLakeTableOffset + 新增 dvReadableSwitch |
| `fluss-server/.../tablet/TabletService.java` | 修改 | 注册 dvReadableSwitch RPC handler（TabletServer 侧） |

---

## 复用的现有组件

| 组件 | 用途 |
|------|------|
| `CommitLakeTableSnapshotRequest/Response` | TieringService → Coordinator 报告（复用，挂载 PbDvPositionReport） |
| `NotifyLakeTableOffsetRequest/Response` | Coordinator → TabletServer 通知（复用，挂载 PbDvPrepare） |
| `CoordinatorEventProcessor.handleCommitLakeTableSnapshotV2()` | 已有的 V2 处理入口 |
| `CoordinatorRequestBatch` | 已有的请求批处理基础设施 |
| `CoordinatorChannelManager` | Coordinator 与 TabletServer 的通信通道 |
| `LakeTableHelper.registerLakeTableSnapshotV2()` | ZK 注册 lake snapshot（Publish 步骤） |
| `FutureUtils.completeAll()` | 异步 Future 聚合 |
| `LakeTable.LakeSnapshotMetadata` | Lake snapshot 元数据存储 |

---

## 关键设计决策

### 为什么复用 CommitLakeTableSnapshot？
- TieringService 完成一轮 compaction 后，需要同时报告 lake snapshot metadata 和 DV position report
- 这两者是同一事件的不同维度，放在同一 RPC 中减少交互次数
- `PbDvPositionReport` 作为 `PbLakeTableSnapshotMetadata` 的可选字段，非 DV 表不受影响

### 为什么复用 NotifyLakeTableOffset？
- Coordinator → TabletServer 的通知通道已有，且 Prepare 阶段本质上也是通知 TabletServer "有新的 lake 数据需要处理"
- 同步等待 response 即为 Ready ACK，无需额外 RPC
- `PbDvPrepare` 作为 `NotifyLakeTableOffsetRequest` 的可选字段，正常通知不受影响

### 为什么 DvReadableSwitch 需要新 RPC？
- Readable Switch 必须在 Publish（ZK 更新）完成后才能执行
- 不能复用 NotifyLakeTableOffset，因为该 RPC 用于 Prepare 阶段（Publish 之前）
- 独立 RPC 使语义清晰：Prepare ≠ Switch

### 为什么同步等待？
- Prepare 阶段：同步等所有 TabletServer Ready 后才能 Publish，否则部分 TabletServer 未下载 SST 就切换会导致查询不到数据
- Switch 阶段：同步等所有 TabletServer Switched 后才能确认本轮完成，方便清理该批次的远程目录

---

## 测试

### 5a. Proto 消息序列化测试
**文件**：`fluss-rpc/src/test/.../rpc/messages/DvMessageTest.java`（新建）

- **testDvPositionReportSerde**：PbDvPositionReport 序列化/反序列化一致性
- **testDvPrepareSerde**：PbDvPrepare 序列化/反序列化一致性
- **testDvReadableSwitchSerde**：DvReadableSwitchRequest/Response 序列化一致性
- **testLakeTableSnapshotMetadataWithDvReport**：PbLakeTableSnapshotMetadata 包含 dv_position_report 的编解码
- **testNotifyLakeTableOffsetWithDvPrepare**：NotifyLakeTableOffsetRequest 包含 dv_prepare 的编解码
- **testBackwardCompatibility**：不含 DV 字段的消息可正常解析（optional 字段缺失不报错）

### 5b. ServerRpcMessageUtils 转换测试
**文件**：`fluss-server/src/test/.../utils/ServerRpcMessageUtilsTest.java`（修改或扩展）

- **testGetCommitLakeTableSnapshotDataWithDvReport**：解析含 DV 字段的 CommitLakeTableSnapshotRequest
- **testGetNotifyLakeTableOffsetWithDvPrepare**：解析含 DV 字段的 NotifyLakeTableOffsetRequest
- **testGetDvReadableSwitchData**：DvReadableSwitchRequest → DvReadableSwitchData
- **testBuildDvPrepareMessage**：DvPrepareData → PbDvPrepare

### 5c. Coordinator 编排测试
**文件**：`fluss-server/src/test/.../coordinator/CoordinatorDvOrchestrationTest.java`（新建）

- **testDvOrchestrationFlow**：完整 Prepare → Publish → Switch 流程
  - Mock TabletServer 端：Prepare 返回 Ready ACK，Switch 返回 Switched ACK
  - 验证 ZK 注册在 Prepare 之后、Switch 之前
  - 验证所有 TabletServer 都收到了请求
- **testNonDvTableUnaffected**：无 dvPositionReport 时走原有路径
- **testPrepareFailure**：部分 TabletServer Prepare 失败 → 不执行 Publish 和 Switch
- **testSwitchFailure**：Publish 成功后部分 TabletServer Switch 失败 → 错误处理

### 5d. 数据类测试
**文件**：`fluss-server/src/test/.../entity/DvDataClassesTest.java`（新建）

- **testDvPositionReportData**：构造和字段访问
- **testDvPrepareData**：构造和字段访问
- **testDvReadableSwitchData**：构造和字段访问

---

## 前置依赖

- 无硬依赖（Proto 定义和 Coordinator 编排框架可独立开发）
- 但逻辑上依赖 PR 1（DvRocksDB）、PR 3（DvManager）、PR 4（SST 基础设施）的概念

---

## 验证

1. 重新生成 Proto：`./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`
2. 编译：`./mvnw compile -pl fluss-server -am -DskipTests`
3. 格式化：`./mvnw spotless:apply -pl fluss-server,fluss-rpc`
4. Proto 消息测试：`./mvnw test -pl fluss-rpc -Dtest=DvMessageTest`
5. Coordinator 编排测试：`./mvnw test -pl fluss-server -Dtest=CoordinatorDvOrchestrationTest`
6. 数据类测试：`./mvnw test -pl fluss-server -Dtest=DvDataClassesTest`
7. 现有测试无回归：`./mvnw test -pl fluss-server -Dtest=CoordinatorEventProcessorTest`

---

## 实现注意事项

1. **Proto 代码生成**：Fluss 使用自定义的 `fluss-protogen` 而非标准 protobuf-java。生成的 Message 类使用 setter 链（`new Msg().setField1(...).setField2(...)`）而非 Builder 模式。修改 proto 后需要执行 `./mvnw clean install -DskipTests -pl fluss-protogen,fluss-rpc`。

2. **Coordinator 事件模型**：Coordinator 使用单线程事件循环（`CoordinatorEventProcessor`）。DV 编排的异步等待（Prepare / Switch 的 Future）需要在 `ioExecutor` 上执行，避免阻塞事件循环。

3. **线程安全**：
   - `handleCommitLakeTableSnapshotV2()` 已经在 `ioExecutor` 上执行
   - DV 编排的 Future 回调也应在 `ioExecutor` 上
   - ZK 操作（Publish）在 IO 线程上

4. **向后兼容性**：
   - `PbDvPositionReport` 是 `PbLakeTableSnapshotMetadata` 的 optional 字段 6 → 旧版本 TieringService 不发送此字段，Coordinator 正常处理
   - `PbDvPrepare` 是 `NotifyLakeTableOffsetRequest` 的 optional 字段 3 → 旧版本 Coordinator 不发送此字段，TabletServer 忽略
   - `DvReadableSwitchRequest` 是新 RPC → 旧版本 TabletServer 不支持，需在 Coordinator 侧处理 RPC not found 错误
