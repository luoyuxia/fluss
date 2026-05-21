# Fluss + Paimon Deletion Vector 实现计划

基于 [fluss-paimon-deletion-vector-design.md](../fluss-paimon-deletion-vector-design.md) 设计文档。

## 现状分析

### 已实现的组件

| 组件 | 状态 | 说明 |
|------|------|------|
| DvManager | ✅ 已实现 | 核心 DV 状态管理（handleChangelogSynced, handlePositionReport, handleReadableSwitch 等） |
| DvRocksDB | ✅ 已实现 | 6 个 CF（RowPosIndex, PendingRowPos, LogDv, LakeDv, FileDict, PendingDeletes） |
| RowPosIndex / LakeDv / LogDv / FileDict / PendingDeletes | ✅ 已实现 | 核心数据结构 |
| KvTablet DV 集成 | ✅ 已实现 | 写入路径（§4.1, §4.2）已集成 DvManager |
| Protocol 定义 | ✅ 已实现 | FlussApi.proto 中已定义 DV 相关 RPC |
| TabletService RPC | ✅ 已实现 | reportPosition, notifyKvSnapshotOffset 等 |
| Flink Union Read DV 支持 | ✅ 已实现 | DvAwareLakeSnapshotSplitScanner, DvAwareFlussLogSplitScanner 等 |
| DvTableReadableSnapshotRetriever | ✅ 已实现 | 计算 per-bucket readable offset |
| PaimonDvTableUtils | ✅ 已实现 | findLatestSnapshotExactlyHoldingL0Files 等 |
| PaimonLakeCommitter DV 逻辑 | ✅ 部分实现 | 调用 DvTableReadableSnapshotRetriever 计算 readable offset |
| MergeTreeWriter | ✅ 部分实现 | 写 Paimon，但不写 __rowid |

### 关键缺失

| 缺失项 | 设计文档章节 | 说明 |
|--------|------------|------|
| __rowid 写入 Paimon | §5.2 Phase A1, §10.3 | MergeTreeWriter 未将 RowId（log offset）写入 Paimon 数据文件 |
| 写入路径 DV 调整 | §5.2 Phase A1 | -U 不写 Paimon、-D 写 DELETE、seq 用 RowId 等 |
| Compaction 输出文件扫描 | §5.2.2 Phase A3 Step 4 | 扫描 compaction 后的新文件，提取 __rowid → FilePos |
| SST 生成 | §5.2.2 Phase A3 Step 5 | 从 RowId→FilePos 映射生成 SST 文件 |
| Position Report（Paimon DV） | §5.2.2 Phase A3 Step 6 | PaimonWriteResult 未实现 PositionReportableWriteResult；per-bucket offset 和 oldFiles 未报告 |
| Prepare Phase oldFiles 处理 | §5.3 | 解析 oldFiles → pendingOldFileIds |
| Readable Switch oldFiles 清理 | §5.4 | 基于文件生命周期清理 LakeDv 和 PendingDeletes |
| Batch Resolve per-bucket readableOffset | §5.4 | 孤儿判断使用 readableOffset 而非 tieredOffset |
| 集成测试 | - | 端到端 Paimon DV 流程测试 |

---

## PR 拆分计划

### PR 1: 在 Paimon 数据文件中写入 __rowid 系统列

**目标**：为后续 compaction 扫描建立基础——让 Paimon 数据文件中包含 Fluss RowId，compaction 重写文件时保留该值。

**设计文档参考**：§10.3, §5.2 Phase A1 Step 2

**改动范围**：

1. **定义 __rowid 系统列**
   - 在 Paimon 表 schema 中增加 `__rowid` 列（BIGINT 类型）
   - 该列应作为 Paimon 的 system field 或 hidden column，对用户不可见
   - 确保 Paimon compaction 重写文件时**保留** __rowid 值（不丢失、不重新计算）

2. **修改 Paimon 表创建逻辑**
   - DV 表创建时自动添加 __rowid 列到 Paimon schema
   - 相关文件：Paimon 表创建相关代码（`PaimonLakeTableCreator` 或类似类）

3. **修改 MergeTreeWriter**
   - `MergeTreeWriter.write()` 中，对 `+I`/`+U` 记录填充 `__rowid = logOffset`
   - 修改 `FlussRecordAsPaimonRow`（或对应的 Row adapter）以携带 __rowid 值
   - 文件：`fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/tiering/mergetree/MergeTreeWriter.java`

4. **测试**
   - 单元测试：写入带 __rowid 的记录到 Paimon，读取验证 __rowid 值正确
   - 单元测试：写入 → compaction → 读取，验证 __rowid 在 compaction 后保留

**前置依赖**：无

---

### PR 2: Tiering 写入路径适配 Paimon DV 表

**目标**：调整 MergeTreeWriter 的写入逻辑，使其符合设计文档 §5.2 Phase A1 的行为——正确处理 `-U`/`-D` 记录、使用 RowId 作为 sequence number。

**设计文档参考**：§5.2 Phase A1

**改动范围**：

1. **调整 MergeTreeWriter 写入逻辑（DV 模式下）**
   - `+I`/`+U`（logDvSnapshot 未命中）：写入 ADD，`seq = RowId`（log offset），`value` 包含 `__rowid`
   - `+I`/`+U`（logDvSnapshot 命中）：跳过
   - `-D`：写入 DELETE(`key, seq=logOffset, DELETE, null`)
   - `-U`：跳过（不写入 Paimon）
   - 文件：`MergeTreeWriter.java`

2. **调整 Tiering Split 生成**
   - DV 表的 tiering split 不再需要 `lakeDvSnapshot`
   - 仍需 `logDvSnapshot`（用于过滤本轮内已删除的 +I/+U）
   - 相关文件：`TieringSplitGenerator` 或对应的 split 生成逻辑

3. **Paimon Snapshot 属性标记**
   - 在 Paimon commit 时设置 snapshot property `fluss.tiering = true`
   - 用于区分 Fluss 产生的 snapshot 和外部 compaction 产生的 snapshot

4. **测试**
   - 验证 -U 不写入 Paimon
   - 验证 -D 写入 DELETE 记录
   - 验证 +I/+U 使用 RowId 作为 sequence number
   - 验证 logDvSnapshot 过滤生效

**前置依赖**：PR 1

---

### PR 3: Compaction 输出文件扫描与 RowId→FilePos 映射构建

**目标**：实现 Phase A3 的核心逻辑——检测 COMPACT snapshot、收集文件变更、扫描 compaction 输出文件建立 RowId→FilePos 映射。

**设计文档参考**：§5.2.2 Phase A3 Step 1/2/3/4

**改动范围**：

1. **COMPACT Snapshot 检测**
   - 每次 APPEND snapshot 提交后检查是否有新的 COMPACT snapshot
   - 基于 `DvTableReadableSnapshotRetriever` 已有的逻辑扩展
   - 避免重复处理已注册的 COMPACT snapshot

2. **文件变更收集**
   - 收集从 lastReadableSnapshot 到当前 COMPACT snapshot 之间的所有文件变更
   - 遍历中间的 COMPACT snapshot，聚合 allNewFiles 和 allOldFiles
   - 利用 Paimon 的 `ManifestEntry`（FileKind.ADD / FileKind.DELETE）

3. **Compaction 输出文件扫描**
   - 对 allNewFiles 中的每个文件，projection pushdown 仅读取 `__rowid` 列
   - 构建 `List<(RowId, FilePos)>` 映射
   - 利用 Paimon 的 `FileStoreTable.newRead()` API 或直接读取 Parquet/ORC 文件

4. **Per-Bucket Readable Offset 计算**
   - 复用 `DvTableReadableSnapshotRetriever.getReadableSnapshotAndOffsets()` 获取 per-bucket 的 readableOffsets 和 tieredOffsets
   - 确保返回值包含所有 bucket 的 offset 信息

5. **测试**
   - 写入 → compaction → 扫描新文件，验证 RowId→FilePos 映射正确
   - 多 bucket 场景：部分 bucket 有 L0、部分无 L0，验证 per-bucket readableOffset 正确
   - 多轮 compaction：验证 allNewFiles/allOldFiles 收集完整

**前置依赖**：PR 1, PR 2

---

### PR 4: SST 生成、上传与 Position Report

**目标**：将 RowId→FilePos 映射生成 SST 文件，上传到远程存储，并向 CoordinatorServer 报告（含 per-bucket offset 和 oldFiles）。

**设计文档参考**：§5.2.2 Phase A3 Step 5/6, §5.2 FileDictAllocator

**改动范围**：

1. **FileDictAllocator 适配 Paimon**
   - 为 Paimon data file 路径分配 fileId
   - `nextFileId` 通过 Paimon snapshot property 持久化/恢复
   - 生成 `newFileDictEntries`（fileId → file_path 映射）

2. **SST 生成**
   - 将 `List<(RowId, FilePos)>` 按 RowId 排序
   - 使用 SstFileWriter 生成 SST（key=RowId, value=fileId+row_position）
   - 每个 bucket 独立生成 SST
   - 上传到 `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`
   - 写入跨 bucket 索引文件

3. **Position Report 协议扩展**
   - 修改 `PaimonWriteResult` 或新建结果类，携带 DV 报告信息：
     - `indexUuid`
     - `newFileDictEntries`
     - `tieredOffsets`（per-bucket）
     - `readableOffsets`（per-bucket）
     - `oldFiles`
     - `readableSnapshotId`（COMPACT snapshot id）
     - `earliestSnapshotIdToKeep`
   - 修改 `TieringCommitOperator.reportPositionIfNeeded()` 使其能处理 Paimon DV 报告
   - 确保 report 发送到 CoordinatorServer

4. **CoordinatorServer 接收 Report**
   - 处理 Paimon DV 报告，存储 per-bucket offset 和 oldFiles 信息
   - 触发 Phase B Prepare 流程

5. **测试**
   - SST 生成正确性（key 排序、value 编码）
   - FileDictAllocator 分配和恢复
   - Position Report 端到端（TieringService → CoordinatorServer）

**前置依赖**：PR 3

---

### PR 5: Prepare Phase 支持 oldFiles

**目标**：CoordinatorServer 向 TabletServer 发送 prepare 通知时携带 oldFiles，TabletServer 在 prepare 阶段解析 oldFiles 为 pendingOldFileIds。

**设计文档参考**：§5.3 Phase B

**改动范围**：

1. **Prepare 通知协议扩展**
   - 在 prepare 通知中增加 `oldFiles` 字段（List<String>，file_path 列表）
   - 增加 `readableOffsets`（per-bucket）字段
   - 可能需要修改 proto 定义

2. **TabletServer Prepare 处理**
   - Phase 1（无锁）：下载 SST，与 Iceberg 方案相同
   - Phase 2（DvRWLock 写锁）：
     - 写入 newFileDictEntries 到 FileDict CF
     - 存储 SST 路径
     - **新增**：将 oldFiles 中的 file_path 通过 FileDict 转换为 file_id，存储为 `pendingOldFileIds`
   - 发送 ready ack

3. **DvManager 扩展**
   - 在 prepare 处理中增加 oldFiles → pendingOldFileIds 的解析逻辑
   - 存储 pendingOldFileIds 供 readable switch 使用

4. **测试**
   - Prepare 通知携带 oldFiles
   - pendingOldFileIds 正确解析和存储

**前置依赖**：PR 4

---

### PR 6: Readable Switch 适配 Paimon DV（oldFiles 清理 + per-bucket offset）

**目标**：实现 Phase C 的 Paimon 特有逻辑——基于文件生命周期清理 LakeDv/PendingDeletes，batch resolve 使用 per-bucket readableOffset。

**设计文档参考**：§5.4 Phase C

**改动范围**：

1. **Ingest SST → RowPosIndex**
   - 与 Iceberg 方案基本相同
   - Paimon SST 包含所有 compaction 输出文件的行映射（包括被重写的历史行），Ingest 后 RowPosIndex 反映 compaction 后的最新位置

2. **Batch Resolve PendingDeletes 调整**
   - 孤儿判断条件从 `R < currentTieredOffset` 改为 `R < readableOffset[this_bucket]`
   - 修改 `DvManager.handleReadableSwitch()` 或相应方法接收 per-bucket readableOffset
   - 当 `R >= readableOffset` 但 `R < tieredOffset` 时，保留到下一轮（行在 L0 中未被 compact）

3. **oldFiles LakeDv 清理**（新增逻辑，替代 Iceberg 的 bitmap diff）
   - 遍历 `pendingOldFileIds`，删除对应的 LakeDv 条目
   - 遍历 PendingDeletes，删除指向 oldFiles 的条目
   - **顺序要求**：先执行 batch resolve（步骤 2），再执行 oldFiles 清理（步骤 3）

4. **更新 snapshotStartLogOffset**
   - `snapshotStartLogOffset` = 本 bucket 的 `readableOffset`（per-bucket）
   - 不再使用全局 tieredOffset

5. **清理 pendingSstPath 和 pendingOldFileIds**

6. **测试**
   - Batch resolve 使用 readableOffset 的正确性
   - oldFiles LakeDv 清理：compaction 替换文件后，旧文件的 LakeDv 被删除
   - PendingDeletes 清理：指向旧文件的条目被删除
   - 顺序正确性：先 batch resolve 再 oldFiles 清理
   - per-bucket snapshotStartLogOffset 正确

**前置依赖**：PR 5

---

### PR 7: Compaction 模式支持（自行触发 + 外部 compaction）

**目标**：支持两种 compaction 模式——TieringService 自行触发 full compaction 和等待外部 compact job 完成。

**设计文档参考**：§5.2.1 Phase A2, §8

**改动范围**：

1. **自行触发模式**
   - 写入 L0 后显式触发 full compaction
   - 通过 Paimon CompactManager / compact API 触发
   - 等待 compaction 完成后继续 Phase A3

2. **外部 compaction 等待模式**
   - 写入 L0 后不触发 compaction
   - 以可配置间隔轮询 Paimon snapshot，检测是否有新的 COMPACT snapshot 消费了本轮 L0
   - 通过检查 L0 文件是否出现在 COMPACT snapshot 的 removedFiles 中判断
   - 超时处理：记录警告 + 可选 fallback 自行触发

3. **配置项**
   - `fluss.lake.paimon.compaction.mode`: `self` / `external`（默认 `self`）
   - `fluss.lake.paimon.compaction.external.poll-interval`: 轮询间隔（默认 5s）
   - `fluss.lake.paimon.compaction.external.timeout`: 超时时间（默认 10min）

4. **测试**
   - 自行触发模式：写入 → compaction → 验证 COMPACT snapshot
   - 外部 compaction 模式：写入 → 外部 compaction → 检测到 COMPACT snapshot
   - 超时处理

**前置依赖**：PR 3（需要 COMPACT snapshot 检测逻辑）

---

### PR 8: 故障恢复

**目标**：实现 §9 中描述的故障恢复逻辑，确保各故障点的正确恢复。

**设计文档参考**：§9

**改动范围**：

1. **TieringService 故障恢复**
   - A1 故障（写入前/写入后/提交前）：利用 Paimon commitIdentifier 幂等性完全重试
   - A2 故障（compaction 前/后）：通过 Paimon snapshot diff 判断 compaction 状态，从 A2 或 A3 恢复
   - A3 故障（SST 生成前/上传后/报告前）：通过 Paimon snapshot diff 重建，重新扫描生成 SST

2. **TabletServer 故障恢复**
   - 从 DvRocksDB checkpoint 恢复
   - 重放 changelog（从 checkpointLogHw + 1）
   - 处理 checkpoint 后已完成的 readable switch：查询 CoordinatorServer 获取当前 DV-readable snapshot，下载和 Ingest 中间 SST
   - 恢复阶段跳过 oldFiles LakeDv 清理（冗余条目下一轮消除）

3. **测试**
   - 各故障点的恢复测试（模拟 TieringService 在不同阶段失败后重启）
   - TabletServer 恢复后 RowPosIndex/LakeDv 状态正确性
   - 冗余 LakeDv 条目在下一轮被清理

**前置依赖**：PR 6

---

### PR 9: 端到端集成测试

**目标**：覆盖完整的 Paimon DV 流程，验证设计文档 Appendix A 中的 walkthrough 场景和各种边界情况。

**设计文档参考**：Appendix A, Appendix B

**改动范围**：

1. **端到端 Walkthrough 测试**（Appendix A）
   - 首轮 Tiering：写入 → compaction → 扫描 → SST → prepare → switch
   - 更新后 Union Read：LakeDv + LogDv 正确屏蔽
   - 第二轮 Tiering：oldFiles 清理、PendingDeletes 清理
   - 新写入 + Union Read：三层 DV 协作

2. **Compaction 位置变更测试**（Appendix B）
   - Case 1：行存活，位置从 file_A 迁移到 file_B
   - Case 2：行在 compaction 前被删除，batch resolve 正确迁移 LakeDv
   - Case 3：行的 DELETE 已写入 Paimon，compaction 物理删除
   - Case 4：时间差——compaction 后、Ingest 前的新删除

3. **多 Bucket 场景**
   - 不同 bucket 的 compaction 状态不同
   - Per-bucket readableOffset 正确性
   - 部分 bucket 有 L0、部分无 L0

4. **外部 Compaction 场景**
   - 外部 compact job 处理了部分 L0
   - 混合模式：自行触发 + 外部同时 compaction

5. **Union Read 正确性**
   - Paimon DV + LakeDv + LogDv 三层协作
   - 读取结果的一致性

**前置依赖**：PR 6, PR 7, PR 8

---

## PR 依赖图

```
PR 1 (写入 __rowid)
  │
  ▼
PR 2 (写入路径 DV 调整)
  │
  ▼
PR 3 (Compaction 扫描 + RowId→FilePos)
  │
  ├──────────────────┐
  ▼                  ▼
PR 4 (SST + Report)  PR 7 (Compaction 模式)
  │
  ▼
PR 5 (Prepare oldFiles)
  │
  ▼
PR 6 (Readable Switch)
  │
  ▼
PR 8 (故障恢复)
  │
  ▼
PR 9 (端到端集成测试)
```

## 关键文件索引

| 文件 | 涉及的 PR |
|------|----------|
| `fluss-lake/fluss-lake-paimon/.../tiering/mergetree/MergeTreeWriter.java` | PR 1, PR 2 |
| `fluss-lake/fluss-lake-paimon/.../tiering/PaimonWriteResult.java` | PR 4 |
| `fluss-lake/fluss-lake-paimon/.../tiering/PaimonLakeCommitter.java` | PR 3, PR 4 |
| `fluss-lake/fluss-lake-paimon/.../utils/DvTableReadableSnapshotRetriever.java` | PR 3 |
| `fluss-lake/fluss-lake-paimon/.../utils/PaimonDvTableUtils.java` | PR 3 |
| `fluss-server/.../kv/dv/DvManager.java` | PR 5, PR 6 |
| `fluss-server/.../kv/dv/LakeDv.java` | PR 6 |
| `fluss-server/.../replica/ReplicaManager.java` | PR 5, PR 6 |
| `fluss-flink/.../tiering/committer/TieringCommitOperator.java` | PR 4 |
| `fluss-common/.../rpc/protocol/FlussApi.proto` | PR 4, PR 5 |
