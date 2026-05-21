# Fluss + Paimon Deletion Vector 实现计划

基于 [fluss-paimon-deletion-vector-design.md](../fluss-paimon-deletion-vector-design.md) 设计文档。

---

## 现状分析

### main 分支上已实现的组件

| 组件 | 状态 | 说明 |
|------|------|------|
| DvTableReadableSnapshotRetriever | ✅ | 计算 per-bucket readable offset，含 ReadableSnapshotResult |
| PaimonDvTableUtils | ✅ | findLatestSnapshotExactlyHoldingL0Files 等 |
| LakeCommitResult.ReadableSnapshot | ✅ | 支持 tieredOffsets + readableOffsets per-bucket |
| PaimonLakeCommitter DV 分支 | ✅ | DV 表调用 DvTableReadableSnapshotRetriever |
| FlussTableLakeSnapshotCommitter | ✅ | 两阶段 prepare + commit 协议 |
| Lake Snapshot V2 存储 | ✅ | LakeTable, LakeTableHelper, LakeSnapshotMetadata |
| MergeTreeWriter 基本写入 | ✅ | 写入 Paimon L0，但不写 __rowid，不处理 DV 特殊逻辑 |
| System Columns (BUCKET, OFFSET, TIMESTAMP) | ✅ | OFFSET 列存储 log offset (= RowId) |
| Coordinator Lake Snapshot 处理 | ✅ | notifyLakeTableOffsets 通知 TabletServer |

### main 分支上缺失的组件

**核心 DV 基础设施（设计文档 §3, §4 — "与 Iceberg 方案相同" 的部分）**：

| 缺失组件 | 设计文档章节 | 说明 |
|----------|------------|------|
| DvRocksDB | §3.3 | 5 个 CF：RowPosIndex, LogDv, LakeDv, FileDict, PendingDeletes |
| DvManager | §4.1, §4.2 | 核心 DV 状态机（handleChangelogSynced, handleReadableSwitch 等） |
| DvRWLock | §3.4 | DV 读写并发控制 |
| KV State RowId | §3.1, §10.1 | ValueEncoder 中嵌入 RowId |
| Changelog RowId 扩展 | §10.2 | changelog record 携带 oldRowId |
| KvTablet DV 集成 | §4.1, §4.2 | 写入路径中调用 DvManager |
| SST 生成基础设施 | §5.2.2 Step 5 | SstFileWriter, FileDictAllocator, SST 上传/下载 |
| Protocol DV 扩展 | §5.3, §5.4 | prepare / readable switch RPC 消息 |
| Coordinator DV 编排 | §5.3, §5.4 | Prepare → Publish → Switch 状态机 |
| TabletServer Prepare | §5.3 | SST 下载，FileDict 写入，ready ack |
| TabletServer Readable Switch | §5.4 | SST Ingest，batch resolve，LogDv 清理 |
| Union Read DV 支持 | §6 | LakeDv + LogDv 三层 DV 应用 |

**Paimon 特有组件（设计文档中与 Iceberg 方案不同的部分）**：

| 缺失组件 | 设计文档章节 | 说明 |
|----------|------------|------|
| __rowid 写入 Paimon | §10.3, §5.2 A1 | MergeTreeWriter 写入 __rowid 系统列 |
| 写入路径 DV 调整 | §5.2 A1 | 跳过 -U，写 -D DELETE，RowId 作 seq |
| Compaction 输出文件扫描 | §5.2.2 A3 Step 3-4 | 扫描新文件 __rowid → FilePos |
| Phase A3 SST 生成 | §5.2.2 A3 Step 5-6 | Paimon 场景的 SST 生成 + 报告 |
| oldFiles 清理 | §5.4 Step 3 | 文件生命周期清理 LakeDv/PendingDeletes |
| Batch resolve per-bucket readableOffset | §5.4 Step 2 | 孤儿判断用 readableOffset |
| Compaction 模式 | §5.2.1, §8 | 自行触发 / 外部等待 |
| Tiering Split DV 快照 | §5.2 A1 | split 携带 logDvSnapshot（不需要 lakeDvSnapshot） |
| 故障恢复 Paimon 适配 | §9 | A1/A2/A3 各故障点恢复 |

---

## PR 拆分计划

计划分为三个阶段：

- **阶段零（PR 0）**：DV 开关配置 `table.deletion-vectors.enabled`。所有后续 PR 的 DV 逻辑均受此开关控制。
- **阶段一（PR 1-7）**：共享 DV 基础设施。这些组件与 Iceberg DV 方案共用，设计文档中标注为"与 Iceberg 方案相同"的部分。
- **阶段二（PR 8-13）**：Paimon 特有实现。设计文档中 Paimon 与 Iceberg 不同的部分。

> **注**：如果 Iceberg DV 的实现已经合入 main，则阶段一的 PR 可以跳过或简化，直接进入阶段二。

---

## 阶段零：DV 开关

### PR 0: `table.deletion-vectors.enabled` 配置项 + 前置校验

**目标**：引入 `table.deletion-vectors.enabled` 表级配置项，作为所有 DV 功能的总开关。该配置必须在建表时设置，不支持动态开启。

**设计文档参考**：§10.5

**改动范围**：

1. **新增配置项**
   - 在 `ConfigOptions` 中新增：
     ```java
     public static final ConfigOption<Boolean> TABLE_DELETION_VECTORS_ENABLED =
             key("table.deletion-vectors.enabled")
                     .booleanType()
                     .defaultValue(false)
                     .withDescription(
                             "Whether to enable Deletion Vector support for the table. "
                             + "Must be set at table creation time and cannot be changed afterwards. "
                             + "When enabled, Fluss maintains a three-layer DV architecture "
                             + "(Paimon DV + Lake DV + Log DV) for instant cross-layer deduplication "
                             + "during union reads. Disabled by default as it introduces additional "
                             + "storage, write path, and tiering overhead.");
     ```

2. **建表时不可变属性**
   - `table.deletion-vectors.enabled` 是建表时不可变属性
   - 建表后不允许通过 ALTER TABLE 修改（因为开启 DV 会改变 KV State Value 格式和 Changelog 格式，已有数据无法回溯补全 RowId）
   - ALTER TABLE 尝试修改此配置时抛出明确的错误信息

3. **建表时前置条件校验**
   - 必须是主键表（有 primary key）
   - 必须开启 datalake（`table.datalake.enabled = true`）
   - 必须使用 FULL changelog mode
   - 校验失败时抛出明确的错误信息

4. **运行时判断**
   - 提供工具方法 `isDeletionVectorsEnabled(TableDescriptor)` / `isDeletionVectorsEnabled(TableInfo)` 供后续 PR 使用
   - 各组件（KvTablet, MergeTreeWriter, TieringCommitOperator, DvManager 等）在初始化时检查此配置
   - DV 关闭时走原有路径，无任何额外开销

5. **与 `paimon.deletion-vectors.enabled` 的关系**
   - `table.deletion-vectors.enabled` 是 Fluss 侧的配置，控制 Fluss 三层 DV 架构
   - `paimon.deletion-vectors.enabled` 是 Paimon 侧的配置（通过 customProperty 传递），控制 Paimon compaction 是否生成 DV 文件
   - 当 `table.deletion-vectors.enabled = true` 时，建议同时设置 `paimon.deletion-vectors.enabled = true`
   - 可在校验中增加 warning 或自动设置

**测试**：
- 配置解析和默认值
- 建表校验：非主键表 → 失败，未开启 datalake → 失败
- ALTER TABLE 修改 → 失败（不可变属性）
- `isDeletionVectorsEnabled()` 工具方法
- DV 关闭时各组件走原有路径（无额外开销）

**前置依赖**：无

---

## 阶段一：共享 DV 基础设施

### PR 1: DvRocksDB + 核心数据结构

**目标**：构建 DV 的存储层——带 5 个 Column Family 的 RocksDB 实例及其数据结构封装。

**设计文档参考**：§3.2, §3.3

**改动范围**：

1. **DvRocksDB**
   - 创建专用 RocksDB 实例，包含 5 个 CF：
     - `RowPosIndex`：key = RowId (8B long)，value = FilePos (unsigned varint)
     - `LogDv`：key = offset_range，value = del_bitmap
     - `LakeDv`：key = file_id (4B int)，value = RoaringPositionBitmap
     - `FileDict`：双向映射 file_path (string) ↔ file_id (int)
     - `PendingDeletes`：key = RowId (8B)，value = FilePos 或 `pending` 标记
   - 支持 checkpoint / restore
   - 支持 `IngestExternalFile` (用于 SST Ingest)

2. **数据结构封装**
   - `RowPosIndex`：get(rowId) → FilePos，delete(rowId)，IngestExternalFile
   - `LakeDv`：get(fileId) → bitmap，put(fileId, bitmap)，delete(fileId)，getDvForUnionRead()
   - `LogDv`：分区式 bitmap (per 1000 offset range)，snapshot()，cleanup(offset)
   - `FileDict`：getOrAssign(filePath) → fileId，getFilePath(fileId) → string
   - `PendingDeletes`：put(rowId, filePos/pending)，get(rowId)，delete(rowId)，iterate()

3. **FilePos 编解码**
   - `FilePos`：(file_id: int, row_position: long)
   - 编码：unsigned varint / LEB128

4. **DvRWLock**
   - 读写锁：写路径串行化，union read 并发读

**测试**：
- 各 CF 的 CRUD 操作
- IngestExternalFile 到 RowPosIndex
- DvRocksDB checkpoint & restore
- FilePos 编解码

**前置依赖**：无

---

### PR 2: KV State RowId + Changelog 格式扩展

**目标**：在 KV state value 中嵌入 RowId，在 changelog 中携带 oldRowId，为 DV 写入路径提供数据基础。

**设计文档参考**：§3.1, §10.1, §10.2

**改动范围**：

1. **KV State Value 格式**
   - `ValueEncoder` 在 value 中追加 RowId 字段（= +I/+U 的 log offset）
   - PUT 操作时：`encode(value, rowId=logOffset)`
   - GET 操作时：`decode(value) → (原始 value, rowId)`

2. **Changelog 格式扩展**
   - `-U` / `-D` changelog record 携带 `oldRowId`（被删除行的 RowId）
   - `oldRowId` 从 KV state 中 GET 旧值时获得
   - 格式向后兼容（通过 version byte 区分）

3. **KvTablet 写入路径调整**
   - PUT 写入时：分配 RowId = logOffset，编码到 value
   - DELETE 写入时：先 GET 旧值获取 oldRowId，编码到 changelog

**测试**：
- ValueEncoder RowId 编解码
- Changelog record 携带 oldRowId
- 向后兼容性测试（旧格式数据可读）

**前置依赖**：无

---

### PR 3: DvManager + KvTablet DV 写入路径

**目标**：实现 DV 写入路径的核心状态机——当 changelog 同步成功后，DvManager 处理 -U/-D 记录，维护 RowPosIndex / LakeDv / LogDv / PendingDeletes。

**设计文档参考**：§4.1, §4.2

**改动范围**：

1. **DvManager**
   - `handleChangelogSynced(List<ChangelogEntry> entries)`：
     - 对每个 `-U` / `-D` 条目：
       - Point-get `RowPosIndex[oldRowId]`
       - Hit → 标记 `LakeDv[fileId] |= {pos}`，删除 `RowPosIndex[oldRowId]`，写 `PendingDeletes[oldRowId] = filePos`
       - Miss → 写 `PendingDeletes[oldRowId] = pending`
       - 更新 `LogDv`
   - `snapshotLogDv(offsetRange)` → LogDvSnapshot
   - `snapshotLakeDv()` → LakeDvSnapshot
   - `getDvForUnionRead()` → (lakeDv, logDv)

2. **KvTablet 集成**
   - 在 `appendKvBatchAsLeader()` 中调用 `DvManager.handleChangelogSynced()`
   - 锁顺序：KvTablet 写锁 → flush prewrite buffer → DvRWLock 写锁 → DvManager → 释放

3. **DvManager 生命周期**
   - 随 KvTablet 创建/关闭
   - 持有 DvRocksDB 引用

**测试**：
- handleChangelogSynced：RowPosIndex hit 场景 → LakeDv 标记 + PendingDeletes
- handleChangelogSynced：RowPosIndex miss 场景 → PendingDeletes pending
- getDvForUnionRead 返回正确的 DV 快照
- 并发测试：写路径 + union read 的锁正确性

**前置依赖**：PR 1, PR 2

---

### PR 4: SST 生成基础设施 + FileDictAllocator

**目标**：构建 SST 文件的生成、上传、下载基础设施。这些在 Phase A3（TieringService 侧）和 Phase B（TabletServer 侧）中共同使用。

**设计文档参考**：§5.2.2 Step 5, §5.2 FileDictAllocator, Appendix C

**改动范围**：

1. **SstFileWriter**
   - 输入：`List<(RowId, FilePos)>`，按 RowId 排序
   - 输出：RocksDB SST 文件（key = RowId BigEndian 8B，value = FilePos varint）
   - 支持分 bucket 生成

2. **SST 上传/下载**
   - 上传到 `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`
   - 下载到 TabletServer 本地临时目录
   - 生成 manifest 文件（列出 SST 文件名和大小）

3. **跨 Bucket 索引文件**
   - 写入 `{indexUuid}` 文件：bucket_id → SST 目录的 UUID 映射
   - 读取索引文件定位指定 bucket 的 SST

4. **FileDictAllocator**
   - 维护 `nextFileId` 计数器
   - `allocate(filePath) → fileId`
   - `getNewEntries() → Map<fileId, filePath>`（本轮新分配的条目）
   - `nextFileId` 通过 lake snapshot property 持久化/恢复

**测试**：
- SST 生成 + 读取验证
- 上传/下载往返一致性
- 跨 bucket 索引文件读写
- FileDictAllocator 分配唯一性 + 恢复

**前置依赖**：PR 1

---

### PR 5: Protocol 扩展 + Coordinator DV 编排

**目标**：扩展 RPC 协议以支持 DV 的 Position Report / Prepare / Publish / Readable Switch，在 CoordinatorServer 中实现 DV 编排状态机。

**设计文档参考**：§5.2.2 Step 6, §5.3, §5.4

**改动范围**：

1. **Proto 消息扩展**
   - **Position Report**（TieringService → Coordinator）：
     - `indexUuid`，`newFileDictEntries`
     - `tieredOffsets` (per-bucket)，`readableOffsets` (per-bucket)
     - `oldFiles` (List<String>)
     - `readableSnapshotId`，`earliestSnapshotIdToKeep`
   - **Prepare 通知**（Coordinator → TabletServer）：
     - `indexUuid`，`readableSnapshotId`
     - `newFileDictEntries`
     - `tieredOffsets` (per-bucket)，`readableOffsets` (per-bucket)
     - `oldFiles` (List<String>)
   - **Ready ACK**（TabletServer → Coordinator）
   - **Readable Switch 通知**（Coordinator → TabletServer）
   - **Switched ACK**（TabletServer → Coordinator）

2. **CoordinatorServer DV 状态机**
   - 接收 Position Report → 存储报告信息
   - 发送 Prepare 通知到所有相关 bucket 的 TabletServer
   - 收集 Ready ACK → 全部就绪后进入 Publish
   - 更新 LakeTableZNode → 标记 readable snapshot
   - 发送 Readable Switch 通知
   - 收集 Switched ACK

3. **CoordinatorEventProcessor 扩展**
   - 新增 DV 相关 Event 类型
   - 处理 Position Report / Ready ACK / Switched ACK

**测试**：
- Proto 消息序列化/反序列化
- Coordinator 状态机状态转换
- 超时和重试

**前置依赖**：无（协议定义可独立）

---

### PR 6: TabletServer Prepare + Readable Switch（基础版）

**目标**：TabletServer 侧实现 Prepare 阶段（下载 SST + 写 FileDict）和 Readable Switch 阶段（Ingest SST + batch resolve + LogDv 清理）。

**设计文档参考**：§5.3, §5.4

**改动范围**：

1. **Prepare 阶段**（Phase B）
   - Phase 1（无锁）：通过 `indexUuid` 读取跨 bucket 索引 → 下载 SST 到本地
   - Phase 2（DvRWLock 写锁）：写 newFileDictEntries 到 FileDict CF → 存储 SST 路径 → ready ack

2. **Readable Switch 阶段**（Phase C 基础版）
   - **Step 1**：`IngestExternalFile(pendingSstPath, RowPosIndex)`
   - **Step 2**：Batch resolve PendingDeletes（基础版，使用全局 tieredOffset 判断孤儿）：
     - `RowPosIndex.get(R)` hit → `LakeDv[fileId] |= {pos}`，删除 RowPosIndex[R]，更新 PendingDeletes
     - miss + R < tieredOffset → 孤儿，删除
     - miss + R >= tieredOffset → 保留
   - **Step 3**：清理过期 LogDv
   - **Step 4**：更新 readableSnapshotId 和 snapshotStartLogOffset
   - **Step 5**：清理 pendingSstPath，释放锁，发送 switched ack

3. **ReplicaManager 集成**
   - 接收 Prepare / Readable Switch 通知
   - 路由到对应 KvTablet 的 DvManager

**测试**：
- Prepare 下载 + FileDict 写入
- Readable Switch SST Ingest
- Batch resolve 各场景（hit、miss-orphan、miss-pending）
- LogDv 清理

**前置依赖**：PR 1, PR 3, PR 4, PR 5

---

### PR 7: Union Read DV 支持

**目标**：在 Flink union read 中应用三层 DV（Paimon DV + LakeDv + LogDv），使查询结果正确排除已删除/已更新的行。

**设计文档参考**：§6

**改动范围**：

1. **TabletServer 返回 DV 数据**
   - `getDvForUnionRead(readableSnapshotId)` 返回：
     - `lakeDv`：`Map<fileId, deletedPositionBitmap>`
     - `logDv`：`deletedOffsetBitmap`（指定 offset range 内）
     - `logEndOffset`

2. **Flink Source DV-Aware Scanner**
   - `DvAwareLakeSnapshotSplitScanner`：读 Paimon data file 时应用 LakeDv bitmap 过滤
   - `DvAwareFlussLogSplitScanner`：读 changelog 时应用 LogDv bitmap 过滤
   - `LakeDvFilterIterator`：按 file_id + row_position 过滤
   - `LogDvFilter`：按 offset 过滤

3. **Split 扩展**
   - `DvAwareLakeSnapshotSplit`：携带 lakeDv 数据
   - `DvAwareFlussLogSplit`：携带 logDv 数据 + logEndOffset

4. **DV 序列化**
   - `DvBitmapUtils`：bitmap 序列化/反序列化，用于 TabletServer → Flink Source 传输

**测试**：
- LakeDv 过滤：屏蔽指定 file_id + position 的行
- LogDv 过滤：屏蔽指定 offset 的 changelog 记录
- 三层 DV 协作：Paimon DV + LakeDv + LogDv 同时生效
- 边界场景：空 DV、全部删除、split 跨多文件

**前置依赖**：PR 3, PR 6

---

## 阶段二：Paimon 特有实现

### PR 8: Paimon 写入路径 DV 适配（__rowid + 写入逻辑调整）

**目标**：让 MergeTreeWriter 在 DV 模式下正确写入 Paimon——包含 __rowid 系统列、跳过 -U、写 -D 为 DELETE、使用 RowId 作为 sequence number。

**设计文档参考**：§5.2 Phase A1, §10.3

**改动范围**：

1. **__rowid 系统列**
   - 在 Paimon 表 schema 中增加 `__rowid` 列（BIGINT 类型）
   - 修改 `PaimonLakeCatalog` 的系统列定义，DV 表额外添加 `__rowid`
   - 确保 Paimon compaction 重写文件时**保留** `__rowid` 值（作为普通数据列参与 merge）
   - 注意 `__rowid` 不同于 Paimon 自身的 `_ROW_ID`（§10.3）

2. **MergeTreeWriter DV 模式**
   - `+I` / `+U`（logDvSnapshot 未命中）→ 写入 `KeyValue(key, seq=logOffset, ADD, value_with___rowid)`
   - `+I` / `+U`（logDvSnapshot 命中）→ 跳过
   - `-D` → 写入 `KeyValue(key, seq=logOffset, DELETE, null)`
   - `-U` → 跳过（不写入 Paimon）
   - 修改 `FlussRecordAsPaimonRow` 以携带 `__rowid` 值

3. **Tiering Split 调整**
   - DV 表的 split 携带 `logDvSnapshot`（从 DvManager.snapshotLogDv 获取）
   - 不携带 `lakeDvSnapshot`（Paimon 方案不需要）

4. **Paimon Snapshot 属性标记**
   - commit 时设置 `fluss.tiering = true` snapshot property

**测试**：
- 写入 +I/+U 后 Paimon 文件中包含 __rowid 列且值正确
- -U 不写入 Paimon
- -D 写入 DELETE record
- logDvSnapshot 过滤生效
- compaction 后 __rowid 值保留

**前置依赖**：PR 3（DvManager 提供 logDvSnapshot）

---

### PR 9: Phase A3 — Compaction 扫描 + SST 生成 + Position Report

**目标**：实现 Phase A3 的完整流程——检测 COMPACT snapshot、收集文件变更、扫描 compaction 输出文件、生成 SST、报告 CoordinatorServer。

**设计文档参考**：§5.2.2 Phase A3 全部步骤

**改动范围**：

1. **COMPACT Snapshot 检测**（Step 1）
   - 复用 `DvTableReadableSnapshotRetriever` 已有的逻辑
   - 每次 APPEND snapshot 提交后检查新 COMPACT snapshot
   - 跳过已注册的 COMPACT snapshot

2. **Per-Bucket Readable Offset 计算**（Step 2）
   - 复用 `DvTableReadableSnapshotRetriever.getReadableSnapshotAndOffsets()`
   - 获取 `ReadableSnapshotResult`（tieredOffsets, readableOffsets, readableSnapshotId, earliestSnapshotIdToKeep）

3. **文件变更收集**（Step 3）
   - 收集从 lastReadableSnapshot 到当前 COMPACT snapshot 之间所有 COMPACT snapshot 的文件变更
   - 利用 Paimon `ManifestEntry`（FileKind.ADD / DELETE）
   - 输出：allNewFiles, allOldFiles

4. **Compaction 输出文件扫描**（Step 4）
   - 对 allNewFiles 中的每个文件，projection pushdown 仅读取 `__rowid` 列
   - 构建 `List<(RowId, FilePos)>` 映射
   - 利用 Paimon 的 `FileStoreTable.newRead()` API

5. **SST 生成 + 上传**（Step 5）
   - 使用 PR 4 的 SstFileWriter 和 FileDictAllocator
   - 每个 bucket 独立生成 SST
   - 上传到远程存储 + 写入跨 bucket 索引文件

6. **Position Report**（Step 6）
   - 向 CoordinatorServer 报告：
     - indexUuid, newFileDictEntries
     - tieredOffsets, readableOffsets (per-bucket)
     - oldFiles, readableSnapshotId, earliestSnapshotIdToKeep
   - 触发 Phase B Prepare 流程

7. **集成到 TieringCommitOperator**
   - 在 Paimon DV 表的 commit 流程中，commit 之后检测 COMPACT snapshot 并执行 Phase A3
   - 修改 `PaimonLakeCommitter` 或新建 `PaimonDvPositionReporter`

**测试**：
- 写入 → compaction → 扫描新文件 → RowId→FilePos 映射正确
- 多 bucket：部分 bucket 有 L0、部分无 L0 → per-bucket readableOffset 正确
- 多轮 compaction：allNewFiles/allOldFiles 收集完整
- SST 生成 → 上传 → 下载验证
- Position Report 端到端

**前置依赖**：PR 4, PR 5, PR 8

---

### PR 10: Prepare + Readable Switch Paimon 适配（oldFiles 清理 + per-bucket readableOffset）

**目标**：在 PR 6 的基础上，增加 Paimon 特有的 oldFiles 清理和 per-bucket readableOffset batch resolve 逻辑。

**设计文档参考**：§5.3, §5.4

**改动范围**：

1. **Prepare 阶段增加 oldFiles 处理**
   - 将 `oldFiles` 中的 file_path 通过 FileDict 转换为 file_id
   - 存储为 `pendingOldFileIds` 供 readable switch 使用

2. **Batch Resolve 使用 per-bucket readableOffset**
   - 修改 PR 6 中的 batch resolve 逻辑
   - 孤儿判断条件：`R < readableOffset[this_bucket]`（而非 `R < tieredOffset`）
   - 当 `R >= readableOffset` 但 `R < tieredOffset`：行在 L0 中未被 compact，保留到下一轮

3. **oldFiles LakeDv 清理**（§5.4 Step 3）
   - 遍历 `pendingOldFileIds`，删除对应的 LakeDv 条目：`LakeDv.delete(fileId)`
   - 遍历 PendingDeletes，删除指向 oldFiles 的条目
   - **顺序要求**：先 batch resolve（Step 2），再 oldFiles 清理（Step 3）

4. **snapshotStartLogOffset 使用 readableOffset**
   - `snapshotStartLogOffset = readableOffset[this_bucket]`（per-bucket）
   - 不使用 tieredOffset（避免跳过 L0 中未 compact 的数据）

5. **清理 pendingOldFileIds**

**测试**：
- oldFiles → pendingOldFileIds 解析正确
- Batch resolve 用 readableOffset：R 在 (readableOffset, tieredOffset) 之间 → 保留
- oldFiles 清理后 LakeDv 不含旧文件条目
- PendingDeletes 指向旧文件的条目被清理
- 顺序正确性：先 batch resolve 后 oldFiles 清理
- per-bucket snapshotStartLogOffset 正确

**前置依赖**：PR 6, PR 9

---

### PR 11: Compaction 模式支持

**目标**：支持两种 compaction 模式——TieringService 自行触发 full compaction 和等待外部 compact job 完成。

**设计文档参考**：§5.2.1 Phase A2, §8

**改动范围**：

1. **自行触发模式**（§8.1）
   - 写入 L0 提交后，通过 Paimon compact API 触发 full compaction
   - 阻塞等待 compaction 完成
   - 直接从 COMPACT snapshot 继续 Phase A3

2. **外部 compaction 等待模式**（§8.2）
   - 写入 L0 后不触发 compaction
   - 以可配置间隔轮询 Paimon snapshot，检测 COMPACT snapshot
   - 通过检查 L0 文件是否出现在 COMPACT snapshot 的 removedFiles 中判断完成
   - 超时处理：记录警告 + 可选 fallback 自行触发

3. **混合模式支持**（§8.3）
   - Phase A3 的 snapshot diff 天然支持——收集所有 COMPACT snapshot 的文件变更，不区分 compaction 来源

4. **未完成 L0 Compaction 处理**（§8.4）
   - 自行触发：使用 `fullCompaction = true`
   - 外部：持续等待直到所有 L0 被消费

5. **配置项**
   - `fluss.lake.paimon.dv.compaction.mode`：`self` / `external`（默认 `self`）
   - `fluss.lake.paimon.dv.compaction.external.poll-interval`：轮询间隔（默认 5s）
   - `fluss.lake.paimon.dv.compaction.external.timeout`：超时（默认 10min）

**测试**：
- 自行触发模式：写入 → compaction → COMPACT snapshot
- 外部等待模式：写入 → 模拟外部 compaction → 检测到 COMPACT snapshot
- 超时处理
- partial compaction 场景

**前置依赖**：PR 9

---

### PR 12: 故障恢复

**目标**：实现各故障点的正确恢复逻辑。

**设计文档参考**：§9

**改动范围**：

1. **TieringService 故障恢复**（§9.1）
   - A1 故障：利用 Paimon `commitIdentifier` 幂等性完全重试
   - A2 故障：通过 snapshot diff 判断 compaction 状态，从 A2 或 A3 恢复
   - A3 故障：通过 snapshot diff 重建，重新扫描生成 SST

2. **TabletServer 故障恢复**（§9.2）
   - DvRocksDB checkpoint 恢复：记录 `restoreSnapshot`、`snapshotStartLogOffset`、`checkpointLogHw`
   - 从 `checkpointLogHw + 1` 重放 changelog
   - 查询 Coordinator 获取当前 DV-readable snapshot，下载和 Ingest 中间 SST
   - **Paimon 特有**：恢复阶段跳过 oldFiles LakeDv 清理（无 `pendingOldFileIds` 信息），冗余条目下一轮消除

3. **CoordinatorServer 故障恢复**（§9.3）
   - 由 LakeTableZNode 状态决定恢复点

4. **幂等性保证**（§9.4）
   - Prepare 和 readable switch 均幂等

**测试**：
- 模拟 TieringService 在 A1/A2/A3 各阶段失败后重启恢复
- TabletServer 恢复后 RowPosIndex/LakeDv 状态正确
- 冗余 LakeDv 条目在下一轮被清理

**前置依赖**：PR 10

---

### PR 13: 端到端集成测试

**目标**：覆盖完整的 Paimon DV 流程，验证 Appendix A/B 中的场景。

**设计文档参考**：Appendix A, Appendix B

**改动范围**：

1. **端到端 Walkthrough**（Appendix A）
   - Step 1-2：首轮写入 → tiering → compaction → SST → prepare → switch
   - Step 3-4：更新 key → LakeDv 标记 → union read 验证
   - Step 5-6：删除 key → 第二轮 tiering → oldFiles 清理 → PendingDeletes 清理
   - Step 7：新写入 + union read 三层 DV 协作

2. **Compaction 位置变更正确性**（Appendix B）
   - Case 1：行存活，位置迁移 file_A → file_B
   - Case 2：行在 compaction 前被删除，batch resolve 正确迁移 LakeDv
   - Case 3：DELETE 已写入 Paimon，compaction 物理删除行
   - Case 4：时间差——compaction 后、Ingest 前的新删除

3. **多 Bucket 场景**
   - 不同 bucket compaction 状态不同
   - per-bucket readableOffset 正确性
   - 部分 bucket 有 L0、部分无 L0

4. **外部 Compaction 场景**
   - 外部 compact job 处理部分/全部 L0
   - 混合模式

5. **Union Read 一致性**
   - Paimon DV + LakeDv + LogDv 三层协作
   - 数据正确性端到端验证

**前置依赖**：PR 7, PR 10, PR 11, PR 12

---

## PR 依赖图

```
阶段零：DV 开关

PR 0 (table.deletion-vectors.enabled 配置 + 校验)
  │
  ▼

阶段一：共享 DV 基础设施

PR 1 (DvRocksDB)    PR 2 (RowId/Changelog)    PR 5 (Protocol/Coordinator)
  │                    │
  ├────────────────────┤
  │                    │
  ▼                    ▼
PR 3 (DvManager + KvTablet)     PR 4 (SST 基础设施)
  │                                │
  ├────────────────────────────────┤
  │                                │
  ▼                                ▼
PR 6 (TabletServer Prepare/Switch 基础版)
  │
  ▼
PR 7 (Union Read DV)


阶段二：Paimon 特有实现

PR 3 ──► PR 8 (Paimon 写入 __rowid + DV 调整)
              │
PR 4,5,8 ──► PR 9 (Phase A3 扫描 + SST + Report)
              │
PR 6,9 ────► PR 10 (Prepare/Switch Paimon 适配: oldFiles + per-bucket offset)
              │
PR 9 ──────► PR 11 (Compaction 模式)
              │
PR 10 ─────► PR 12 (故障恢复)
              │
PR 7,10,11,12 ► PR 13 (端到端集成测试)
```

**关键路径**：PR 0 → PR 1 → PR 3 → PR 8 → PR 9 → PR 10 → PR 12 → PR 13

**可并行开发**：
- PR 0 最先完成（其他 PR 均依赖 DV 开关判断）
- PR 1 和 PR 2 并行
- PR 4 和 PR 5 可与 PR 3 并行
- PR 7 可在 PR 6 之后独立开发
- PR 11 可与 PR 10 并行

---

## 关键文件索引

| 文件 | 涉及的 PR | 操作 |
|------|----------|------|
| `fluss-common/.../config/ConfigOptions.java` | PR 0 | 修改（新增 TABLE_DELETION_VECTORS_ENABLED） |
| `fluss-server/.../kv/dv/DvRocksDB.java` | PR 1 | 新建 |
| `fluss-server/.../kv/dv/RowPosIndex.java` | PR 1 | 新建 |
| `fluss-server/.../kv/dv/LakeDv.java` | PR 1 | 新建 |
| `fluss-server/.../kv/dv/LogDv.java` | PR 1 | 新建 |
| `fluss-server/.../kv/dv/FileDict.java` | PR 1 | 新建 |
| `fluss-server/.../kv/dv/PendingDeletes.java` | PR 1 | 新建 |
| `fluss-server/.../kv/dv/DvRWLock.java` | PR 1 | 新建 |
| `fluss-server/.../kv/dv/FilePos.java` | PR 1 | 新建 |
| `fluss-common/.../kv/ValueEncoder.java` | PR 2 | 修改 |
| `fluss-server/.../kv/dv/DvManager.java` | PR 3 | 新建 |
| `fluss-server/.../kv/KvTablet.java` | PR 3 | 修改 |
| `fluss-server/.../kv/dv/SstFileWriter.java` | PR 4 | 新建 |
| `fluss-server/.../kv/dv/FileDictAllocator.java` | PR 4 | 新建 |
| `fluss-rpc/.../proto/FlussApi.proto` | PR 5 | 修改 |
| `fluss-server/.../coordinator/CoordinatorEventProcessor.java` | PR 5 | 修改 |
| `fluss-server/.../replica/ReplicaManager.java` | PR 6 | 修改 |
| `fluss-flink/.../source/DvAwareLakeSnapshotSplitScanner.java` | PR 7 | 新建 |
| `fluss-flink/.../source/DvAwareFlussLogSplitScanner.java` | PR 7 | 新建 |
| `fluss-lake/fluss-lake-paimon/.../tiering/mergetree/MergeTreeWriter.java` | PR 8 | 修改 |
| `fluss-lake/fluss-lake-paimon/.../tiering/FlussRecordAsPaimonRow.java` | PR 8 | 修改 |
| `fluss-lake/fluss-lake-paimon/.../PaimonLakeCatalog.java` | PR 8 | 修改 |
| `fluss-lake/fluss-lake-paimon/.../tiering/PaimonLakeCommitter.java` | PR 9 | 修改 |
| `fluss-lake/fluss-lake-paimon/.../utils/DvTableReadableSnapshotRetriever.java` | PR 9 | 复用 |
| `fluss-flink/.../tiering/committer/TieringCommitOperator.java` | PR 9 | 修改 |
