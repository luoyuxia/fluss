# Fluss Deletion Vector — Agent Coding 子任务拆解（无 Checkpoint / 无 Compaction 版）

> 基于 `fluss-deletion-vector-design-v2.md` 拆解，每个 subtask 可直接交给 code agent 执行。
> 标注 `🔀 可并行` 的 subtask 之间无依赖，可同时开 sub-agent 执行。
>
> **本文档范围**：**不包含** Checkpoint 恢复（原 Phase 6）和外部 Compaction 交互（原 Phase 7）。
> 这两个功能作为后续迭代独立实现。

---

## Phase 0：基础数据格式变更 `🔀 与 Phase 1 可并行`

> Phase 0 改数据格式，Phase 1 建 DvRocksDB 存储层，两者互不依赖，可同时启动。
> Phase 2（写入流程改造）同时依赖 Phase 0 + Phase 1。

### Subtask 0-1：KV State Value 格式变更 — 首部插入 RowId（8 bytes）

**设计参考**：§4.1

**目标**：在 KV state 的 value 格式最前面插入 8 bytes 的 RowId（即 `+I`/`+U` 对应的 log offset）。

**变更前格式**：`[schemaId (2 bytes)][BinaryRow (变长)]`
**变更后格式**：`[RowId (8 bytes)][schemaId (2 bytes)][BinaryRow (变长)]`

**涉及文件**：
- `fluss-common/src/main/java/org/apache/fluss/row/encode/ValueEncoder.java` — 修改 `encodeValue()` 方法，在 schemaId 前写入 8 bytes RowId
- `fluss-common/src/main/java/org/apache/fluss/record/DefaultValueRecord.java` — 适配新格式
- `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java` — 写入 KV state 时传入 RowId（log offset）
- `fluss-server/src/main/java/org/apache/fluss/server/kv/prewrite/KvPreWriteBuffer.java` — 缓存 KV 数据时需要包含 RowId
- `fluss-server/src/main/java/org/apache/fluss/server/kv/KvRecoverHelper.java` — 恢复时适配新格式

**实现要点**：
1. `ValueEncoder` 新增方法 `encodeValueWithRowId(long rowId, short schemaId, BinaryRow row)`，在最前面写入 8 bytes rowId
2. 新增方法 `extractRowId(byte[] value)` — 从 value 首部直接读取前 8 bytes 作为 RowId
3. 新增方法 `extractSchemaIdFromDvValue(byte[] value)` — 跳过前 8 bytes 再读 schemaId
4. KvTablet 写入时：PUT 操作获取即将分配的 log offset 作为 RowId，写入 `[RowId][schemaId][BinaryRow]`
5. KvTablet 读取旧值时：从 value 首部提取 `oldRowId`（前 8 bytes）
6. KvRecoverHelper 恢复时使用新格式编码
7. 需要考虑向后兼容：是否需要支持读取旧格式的 value（无 RowId 前缀）

**验证**：单元测试验证编码/解码 roundtrip，验证 oldRowId 提取正确。

---

### Subtask 0-2：Changelog 格式扩展 — `-U`/`-D` 携带 oldRowId

**设计参考**：§4.2

**依赖**：Subtask 0-1（`-U`/`-D` 的 value 直接复用 KV state 旧 value，而 KV state 格式由 0-1 改造）

**目标**：`-U` 和 `-D` 记录的 value 中携带被删除版本的 RowId。

**变更前 `-U`/`-D` value**：`[schemaId][BinaryRow(旧值)]`
**变更后 `-U`/`-D` value**：`[RowId(8 bytes)][schemaId][BinaryRow(旧值)]`

**注意**：`+I` 和 `+U` 的 value 格式不变，它们的 RowId 就是自身的 log offset。

**涉及文件**：
- `fluss-common/src/main/java/org/apache/fluss/record/ChangeType.java` — 无需修改，仅参考
- `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java` — 生成 `-U`/`-D` changelog 时，直接将 KV state 中读出的旧 value（已包含首部 RowId）原样写入 `-U`/`-D` 的 value
- `fluss-common/src/main/java/org/apache/fluss/record/LogRecord.java` — 可能需要新增 RowId 解析方法
- `fluss-common/src/main/java/org/apache/fluss/row/encode/ValueDecoder.java` 或 `ValueEncoder.java` — 新增 `extractOldRowId(byte[] value)` 工具方法供 Tiering Writer / TabletServer 使用

**实现要点**：
1. 生成 `-U` changelog 时：从 KV state 读出旧 value（格式已是 `[RowId][schemaId][BinaryRow]`），直接作为 `-U` 的 value 写入 changelog，无需额外拼接
2. 生成 `-D` changelog 时：同上，旧 value 直接作为 `-D` 的 value
3. Tiering Writer 读 `-U`/`-D` 时需要能从 value 首部提取 oldRowId（前 8 bytes）
4. TabletServer changelog 同步成功后处理 `-U`/`-D` 时，从 value 首部提取 RowId
5. **关键**：此 subtask 不做独立的格式编码 —— `-U`/`-D` value 实际上就是 KV state 旧 value 的透传。核心工作是确保各消费方（Tiering Writer、TabletServer DV 更新逻辑）能正确解析新格式

**验证**：测试 changelog 记录编解码，验证 `-U`/`-D` value 首部 8 bytes 为正确的 oldRowId。

---

### 🔀 Subtask 0-3：Iceberg 版本升级与 `__bucket` 列写入

**设计参考**：§4.3, §4.4

**目标**：
1. 新建 Iceberg 表时使用 format-version=3（替代默认 v2）
2. 存量 v2 表自动升级到 v3
3. 确保 `__bucket` 列已写入（当前代码已有，确认即可）
4. 新增 table property `fluss.system-columns=__offset,__bucket`

**涉及文件**：
- `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/IcebergLakeCatalog.java` — `createTable()` 时设置 `format-version=3`，新增 `fluss.system-columns` property
- `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/tiering/FlussRecordAsIcebergRecord.java` — 确认 `__bucket` 列已正确写入（当前代码已有 `__bucket` 在 `originRowFieldCount` 位置）

**实现要点**：
1. `IcebergLakeCatalog.createTable()`：在 `buildTableProperties()` 中添加 `TableProperties.FORMAT_VERSION = "3"`
2. 新增升级逻辑：启用 DV 功能时检测现有表版本，若为 v2 则执行 `table.updateProperties().set("format-version", "3").commit()`
3. 添加 table property `fluss.system-columns=__offset,__bucket`
4. 确认 `FlussRecordAsIcebergRecord` 中 `__bucket` 和 `__offset` 列都正确写入

**验证**：集成测试验证新表创建为 v3，存量 v2 表升级成功。

---

### 🔀 Subtask 0-4：FULL Changelog 模式校验

**设计参考**：§4.5

**依赖**：Subtask 0-5（DV 功能开关）

**目标**：创建主键表时，如果启用了 DV 功能，校验 changelog 模式为 FULL，否则拒绝创建。

**涉及文件**：
- `fluss-common/src/main/java/org/apache/fluss/metadata/ChangelogImage.java` — 参考 FULL/WAL 定义
- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/` 下表创建处理逻辑 — 增加校验
- `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java` 或类似配置类 — 引用 DV 功能开关

**实现要点**：
1. 在表创建逻辑中增加校验：如果是主键表且启用 DV，检查 `ChangelogImage` 是否为 `FULL`
2. 如果不是 FULL 模式，返回明确的错误信息：`"Deletion Vector requires FULL changelog mode for primary key tables"`

---

### 🔀 Subtask 0-5：DV 功能开关与配置（新增）

**设计参考**：设计文档中多处提及"启用 DV 功能时"

**依赖**：无

**目标**：新增 DV 功能开关的 table property / 系统配置项，作为所有 DV 逻辑的 feature gate。

**涉及文件**：
- `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java` — 新增配置项
- `fluss-common/src/main/java/org/apache/fluss/metadata/TableDescriptor.java` 或表属性相关类 — 支持 table-level DV 开关

**实现要点**：
1. 新增 table property `table.dv.enabled`（默认 false）
2. 提供工具方法 `isDvEnabled(TableDescriptor)` 供各模块判断
3. 只有主键表可启用 DV（append-only 表不需要）
4. 此配置项在 Subtask 0-3（Iceberg v3 升级）、0-4（FULL 校验）、Phase 2（写入流程）等处作为 gate 使用

---

## Phase 1：DvRocksDB 存储层 `🔀 与 Phase 0 可并行`

> Phase 1 构建 DvRocksDB 独立存储，不依赖 Phase 0 的格式变更。两者可同时启动。
> Phase 1 内部 subtask 1-1 ~ 1-5 可并行；1-6 依赖 1-1。
> **前置准备**：确认 pom.xml 中是否已引入 `org.roaringbitmap:RoaringBitmap` 依赖，若无需在 fluss-server 的 pom.xml 中添加。

### 🔀 Subtask 1-1：DvRocksDB 实例创建与列族定义

**设计参考**：§5.1

**目标**：创建独立于 KvTablet RocksDB 的 DvRocksDB 实例，包含 6 个 Column Family。

**涉及文件（新建）**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvRocksDB.java` — DvRocksDB 封装类

**参考现有实现**：
- `fluss-common/src/main/java/org/apache/fluss/rocksdb/RocksDBHandle.java` — RocksDB 创建模式
- `fluss-server/src/main/java/org/apache/fluss/server/kv/rocksdb/RocksDBKvBuilder.java` — Builder 模式参考

**实现要点**：
1. 定义 6 个 Column Family：
   - `CF_ROW_POS_INDEX`：`RowId (8 bytes) → FilePos (8 bytes)`
   - `CF_PENDING_ROW_POS`：`RowId (8 bytes) → FilePos (8 bytes)`
   - `CF_LOG_DV`：`offset_range_key → del_bitmap (RoaringBitmap serialized)`
   - `CF_LAKE_DV`：`file_id (4 bytes) → del_bitmap (RoaringBitmap serialized)`
   - `CF_FILE_DICT`：`file_path (string) ↔ file_id (int)`（需要正向和反向映射，可用前缀区分）
   - `CF_PENDING_DELETES`：`RowId (8 bytes) → empty`
2. 提供 `open()`、`close()` 方法
3. 支持 WriteBatch 原子写入（参考 `RocksDBWriteBatchWrapper`）
4. DvRocksDB 文件路径与 KvTablet RocksDB 路径分离

> **注意**：本阶段不实现 `createCheckpoint()` 方法。Checkpoint 机制作为后续迭代实现。

**验证**：单元测试验证 DvRocksDB 创建、列族访问、WriteBatch 原子写入。

---

### 🔀 Subtask 1-2：核心数据结构 — RowId、FilePos、RowPosIndex

**设计参考**：§3.1, §3.2, §3.3

**依赖**：Subtask 1-1（需要 DvRocksDB 列族）；但可先开发数据类型（FilePos），后集成 RocksDB 操作

**涉及文件（新建）**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/RowPosIndex.java` — RowPosIndex 操作封装
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/FilePos.java` — FilePos 值对象（file_id + row_position 合并为 8 bytes）
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/FileDict.java` — 文件路径字典编码

**实现要点**：
1. `FilePos`：高 4 bytes = file_id，低 4 bytes = row_position。提供编解码方法。
2. `RowPosIndex`：
   - `get(long rowId) → FilePos`：查 RowPosIndex CF
   - `put(long rowId, FilePos filePos)`：写 RowPosIndex CF
   - `delete(long rowId)`：删 RowPosIndex CF
3. `FileDict`：
   - `getOrCreateFileId(String filePath) → int`：查找或创建 file_id
   - `getFilePath(int fileId) → String`：反向查找
   - 使用自增 int 作为 file_id
4. PendingRowPos 操作：
   - `getPendingRowPos(long rowId) → FilePos`
   - `putPendingRowPos(long rowId, FilePos filePos)`
   - `deletePendingRowPos(long rowId)`
   - `iteratePendingRowPos()` — 用于 readable 切换时的迁移
   - `clearPendingRowPos()` — 清空所有条目

**验证**：单元测试验证 FilePos 编解码、RowPosIndex CRUD、FileDict 自增 ID。

---

### 🔀 Subtask 1-3：核心数据结构 — LogDv

**设计参考**：§3.4

**目标**：实现 Log Deletion Vector 的读写操作。

**涉及文件（新建）**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/LogDv.java` — LogDv 操作封装

**依赖库**：RoaringBitmap（需确认 pom.xml 是否已引入，若无需添加）

**实现要点**：
1. Key 设计：offset range 按固定间隔分段（如每 1000 条一段），key = `range_start_offset / RANGE_SIZE`，RANGE_SIZE 应可配置
2. Value：RoaringBitmap 序列化后的 bytes
3. **Bitmap 索引规则**：bitmap 中的位置 = `(offset - range_start_offset)`（0-indexed）。设计文档示例中 offset=0 在 range 0~9 中 bitmap={1} 使用的是 1-indexed，实现时建议统一用 0-indexed（即 offset=0 → bit 0）以符合 RoaringBitmap 惯例，但需确保 client 和 server 端一致
4. 操作：
   - `markDeleted(long offset)`：找到 offset 所在的 range，更新该 range 的 bitmap
   - `getDeletedBitmap(long startOffset, long endOffset) → Map<Long, RoaringBitmap>`：获取指定范围内的 LogDv
   - `cleanup(long startLogOffset)`：清理 **range 结束 offset** < startLogOffset 的条目（注意：按 range 结束 offset 判断，不是起始 offset，见设计文档 §3.4 生命周期管理）
5. 返回格式：`List<{base_offset, del_bits}>`，供 union read client 使用

**验证**：单元测试验证 bitmap 标记、范围查询、cleanup 边界条件。

---

### 🔀 Subtask 1-4：核心数据结构 — LakeDv

**设计参考**：§3.5

**目标**：实现 Lake Deletion Vector 的读写操作。

**涉及文件（新建）**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/LakeDv.java` — LakeDv 操作封装

**实现要点**：
1. Key = file_id (4 bytes)，Value = RoaringBitmap (del_bitmap)
2. 操作：
   - `markDeleted(int fileId, int rowPosition)`：将 rowPosition 加入 fileId 对应的 bitmap
   - `getDeletedBitmap(int fileId) → RoaringBitmap`
   - `getAllEntries() → Map<Integer, RoaringBitmap>`：获取全部 LakeDv（用于快照）
   - `snapshot() → Map<Integer, RoaringBitmap>`：深拷贝当前状态
   - `deleteFile(int fileId)`：删除某个文件的 LakeDv 条目
   - `applyBitmapDiff(Map<Integer, RoaringBitmap> snapshotBitmap)`：执行 `当前 bitmap AND NOT snapshotBitmap` 差集清理
3. 增量存储：只保存尚未物化到 Iceberg DV 的删除

**验证**：单元测试验证 bitmap 操作、快照深拷贝、差集清理。

---

### 🔀 Subtask 1-5：PendingDeletes 操作

**设计参考**：§5.1 PendingDeletes 列族

**目标**：实现 PendingDeletes 的读写操作。

**涉及文件**：
- 可集成到 `DvRocksDB.java` 或新建 `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/PendingDeletes.java`

**实现要点**：
1. Key = RowId (8 bytes)，Value = empty
2. 操作：
   - `add(long rowId)`：将 rowId 加入 PendingDeletes
   - `contains(long rowId) → boolean`：检查 rowId 是否存在
   - `remove(long rowId)`：移除指定 rowId
   - `cleanupRange(long maxRowId)`：`deleteRange(0, maxRowId + 1)`，用于 readable 切换时统一清理
3. 在 WriteBatch 中与其他 CF 操作原子提交

**验证**：单元测试验证增删查、range delete。

---

### Subtask 1-6：dvLock 并发控制 + DvManager 骨架

**设计参考**：§5.1 并发控制

**依赖**：Subtask 1-1

**目标**：创建 DvManager 作为所有 DV 操作的统一入口，内置 dvLock 并发控制，管理 snapshotBitmap 生命周期。

**涉及文件（新建）**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvManager.java` — DV 管理器

**实现要点**：
1. `dvLock = new ReentrantReadWriteLock()`
2. Write lock 使用场景（三条写路径互斥）：
   - Changelog 同步成功（§6.2 步骤 3-5）
   - Position 上报（§7.3 步骤 1-7）
   - Readable 切换（§8.2 Step 3）
3. Read lock 使用场景：
   - Union Read（§10 步骤 4）
4. 锁顺序：需要同时持有两把锁时，先 KvTablet 锁，再 dvLock
5. **DvManager 持有以下状态**（后续 subtask 逐步填充实现）：
   - `DvRocksDB dvRocksDB` — 底层存储
   - `RowPosIndex rowPosIndex` — RowId → FilePos 映射
   - `LakeDv lakeDv` — Lake 层逻辑删除标记
   - `LogDv logDv` — Log 层逻辑删除标记
   - `PendingDeletes pendingDeletes` — 待处理的删除
   - `FileDict fileDict` — 文件路径字典编码
   - `Map<Integer, RoaringBitmap> snapshotBitmap` — LakeDv 快照副本（内存，非持久化），用于 §13.3 差集清理。由 §7.1 步骤 3 填充，§7.3 步骤 8 过滤，§8.2 Step 3 执行差集后清空。任何时刻最多一份。
   - `long currentReadableSnapshotId` — 当前 DV-readable snapshot ID
   - `long currentReadableSnapshotTieredOffset` — 当前 readable snapshot 的 tiered offset
6. **骨架 API**（方法签名，实现在后续 subtask 填充）：
   - `handleChangelogSynced(List<DvEntry> entries)` → Subtask 2-2
   - `handlePositionReport(...)` → Subtask 3-4
   - `handleReadableSwitch(...)` → Subtask 4-2
   - `getDvForUnionRead(long requestedSnapshotId, ...) → DvReadResult` → Subtask 5-1
   - `snapshotLakeDv() → Map<String, RoaringBitmap>` → Subtask 3-1

> **注意**：本阶段不包含 `recover(...)` 方法。恢复流程（Checkpoint 恢复）作为后续迭代实现。
> 同样不包含 `knownFiles` 字段。外部 Compaction 文件追踪作为后续迭代实现。

---

## Phase 2：写入流程改造

> 依赖 Phase 0 + Phase 1。Phase 2 的两个 subtask 有顺序依赖。

### Subtask 2-1：实时数据写入流程改造（§6.1）

**设计参考**：§6.1

**依赖**：Subtask 0-1, 0-2

**目标**：改造 KvTablet 的 PUT/DELETE 写入流程，生成携带 RowId 的 changelog。

**涉及文件**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java`

**实现要点**：
1. **RowId 分配时机**：RowId = 即将分配的 log offset。当前 KvTablet 写入时，log offset 是 PrewriteBuffer 内顺序递增分配的（参考 `KvPreWriteBuffer` 中的 log sequence number）。写入 `+I`/`+U` 时可预知该记录的 log offset，作为 RowId。如果 offset 在 changelog 写入后才确定，则需要调整：先分配 offset 再写 KV state。
2. **新 key（查不到旧值）**：
   - 生成 `+I(value, rowId)`，rowId = 即将分配的 log offset
   - 写入 PrewriteBuffer + changelog
   - KV state value = `[RowId][schemaId][BinaryRow]`
3. **已有 key（查到旧值）**：
   - 从旧 value 首部提取 `oldRowId`（前 8 bytes）
   - **PUT**：生成 `-U(oldValue含oldRowId)` + `+U(newValue, newRowId)`
   - **DELETE**：生成 `-D(oldValue含oldRowId)`
   - `-U`/`-D` 的 value 直接使用从 KV state 读出的完整旧 value（已含 RowId 前缀）
4. 更新 KV state：`[newRowId][schemaId][BinaryRow(newValue)]`
5. **注意**：`+I`/`+U` 的 changelog value 格式不变（仍为 `[schemaId][BinaryRow]`），因为它们的 RowId = 自身 log offset，无需在 value 中冗余

**验证**：集成测试验证 PUT/DELETE 生成的 changelog 格式正确，RowId 正确。

---

### Subtask 2-2：Changelog 同步成功后的 DV 更新（§6.2）

**设计参考**：§6.2

**依赖**：Subtask 1-1 ~ 1-6, Subtask 2-1

**目标**：changelog 同步成功后，处理 `-U`/`-D` 记录，更新 LakeDv、LogDv、PendingDeletes。

**涉及文件**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java` — 新增 DV 更新逻辑
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvManager.java` — DV 操作入口

**实现要点**：
1. 获取 KvTablet 写锁
2. Flush PrewriteBuffer 到 RocksDB
3. **获取 dvLock.writeLock()**
4. 遍历 flush 下去的 entry，**仅处理 `-U`/`-D` 类型**（跳过 `+I`/`+U`）：
   - a. 用 `oldRowId` 查 RowPosIndex **和** pendingRowPos：
     - RowPosIndex 命中 → LakeDv 标记删除 + 从 RowPosIndex 删除
     - pendingRowPos 命中 → LakeDv 标记删除 + 从 pendingRowPos 删除
     - 都没命中 → 加入 PendingDeletes
   - b. 用 `oldRowId` 更新 LogDv：标记对应 offset 为已删除
5. **释放 dvLock.writeLock()**
6. 更新 `log_hw`（先 DV 后 hw，保证 union read 一致性）
7. 释放 KvTablet 写锁

**验证**：测试 changelog 同步后 LakeDv/LogDv 正确更新，PendingDeletes 在 position 未到达时正确填充。

---

## Phase 3：Tiering 流程改造

> 依赖 Phase 2。内部部分 subtask 可并行。

### Subtask 3-1：Tiering Split 生成改造 — 携带 LakeDv 快照（§7.1）

**设计参考**：§7.1

**依赖**：Phase 1 (LakeDv, FileDict)

**目标**：生成 tiering split 时同步快照 LakeDv，随 split 下发。

**涉及文件**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvManager.java` — 提供 LakeDv 快照 API（server 侧）
- `fluss-server/` 下 RPC handler — 新增或扩展 RPC，供 TieringSplitGenerator 调用获取 LakeDv 快照
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/split/TieringSplit.java` — 扩展 split 结构，增加 `lakeDvSnapshot` 字段
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/split/TieringSplitGenerator.java` — 生成 split 时通过 RPC 获取 LakeDv 快照
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/tiering/source/split/TieringSplitSerializer.java` — 适配 split 序列化
- `fluss-rpc/src/main/proto/FlussApi.proto` — 新增获取 LakeDv 快照的 RPC 消息（或扩展现有 split 获取接口）

**实现要点**：
1. **Server 侧（DvManager）**：
   - 在 KvTablet 读锁保护下：
     - 读取 `log_hw` 作为 `latest_offset`
     - 快照 LakeDv：遍历所有条目，通过 FileDict 将 file_id → file_path
     - 将快照副本保存在 DvManager 的内存变量 `snapshotBitmap`（`Map<Integer, RoaringBitmap>`，使用 file_id 作为 key）中
   - 返回 `Map<String, byte[]>`（file_path → bitmap serialized）
2. **Client 侧（TieringSplitGenerator）**：
   - 通过 RPC 调用 TabletServer 获取 LakeDv 快照
   - TieringSplit 新增字段：`lakeDvSnapshot: Map<String, byte[]>`（file_path → bitmap bytes）
3. Split 序列化/反序列化适配（TieringSplitSerializer）

**验证**：测试 split 生成时 LakeDv 快照正确，file_id 到 file_path 转换正确。

---

### Subtask 3-2：DvTaskWriter 实现（§9.2）

**设计参考**：§9.1, §9.2, §7.2

**依赖**：Subtask 3-1（需要 lakeDvSnapshot 在 split 中）

**目标**：新建 `DvTaskWriter` 替代 `DeltaTaskWriter`，实现 DV 模式的 tiering 写入。

**涉及文件（新建 + 修改）**：
- 新建 `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/tiering/writer/DvTaskWriter.java`
- 新建或修改 `IcebergWriteResult` / `IcebergCommittable` — 扩展 result 包含 positionReport、locallyDeletedRowIds、materializedDvFiles
- 修改 `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/tiering/IcebergLakeWriter.java` — 根据 DV 配置选择 DvTaskWriter 或 DeltaTaskWriter
- 修改 `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/tiering/FlussRecordAsIcebergRecord.java` — 确保 `__offset` 列值正确（即 RowId）

**实现要点**：
1. `DvTaskWriter` 只做 append 写入，不生成 equality delete file
2. 处理逻辑：
   - `+I`/`+U` → 写入 data file + 记录 `(RowId, file, row_position)` 到 positionReport
   - `-U`/`-D` → 提取 `oldRowId`：
     - `oldRowId > last_tiered_offset`（同 split 内先写后删）→ 从 positionReport 查 position → 加入 localDv
     - `oldRowId <= last_tiered_offset`（跨 split 删除）→ 跳过（已在 LakeDv 快照中）
3. 生成 Puffin DV 文件：
   - 读 Iceberg table state 获取 currentFiles + baseSnapshotId
   - 过滤 lakeDvSnapshot：仅保留 currentFiles 中存在的文件
   - 将过滤后的 lakeDvSnapshot + localDv 合并生成 Puffin DV 文件
4. WriteResult 包含：`{dataFiles, dvFiles, positionReport, locallyDeletedRowIds, materializedDvFiles}`
5. **Puffin DV 文件生成**：使用 Iceberg v3 的 `ContentFile` API 创建 position delete 文件。Iceberg v3 中 position delete 以 Puffin 格式存储 RoaringBitmap。参考 Iceberg 的 `PositionDeleteWriter` 和 `DVFileWriter` API。需要为每个有删除标记的 data file 生成一个对应的 DV 文件。
6. **IcebergWriteResult 扩展**：当前 `IcebergWriteResult` 只有 `dataFiles` + `deleteFiles`。需新增字段：`positionReport: Map<String, List<(long, int)>>`、`locallyDeletedRowIds: List<Long>`、`materializedDvFiles: List<String>`、`baseSnapshotId: long`

**验证**：单元测试验证同 split 内先写后删的 localDv 生成，lakeDvSnapshot 过滤逻辑。

---

### Subtask 3-3：IcebergLakeCommitter 改造（§9.3）

**设计参考**：§9.3, §7.2 Commit 部分

**依赖**：Subtask 3-2（需要新的 WriteResult/Committable 结构）

**目标**：改造 RowDelta commit 逻辑，增加 `validateFromSnapshot` 防护。

**涉及文件**：
- `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/tiering/IcebergLakeCommitter.java`

**实现要点**：
1. 改造 RowDelta commit（DV 模式下）：
   ```java
   RowDelta rowDelta = icebergTable.newRowDelta();
   rowDelta.validateFromSnapshot(baseSnapshotId);
   committable.getDataFiles().forEach(rowDelta::addRows);
   committable.getDvFiles().forEach(rowDelta::addDeletes);  // Puffin DV files, 不再是 equality delete
   ```
2. `baseSnapshotId`：读取 table state 时的 snapshot id
3. WriteResult 扩展：包含 `positionReport`、`locallyDeletedRowIds`、`materializedDvFiles`、`actualSnapshotId`

> **注意**：本阶段不实现 `validateDataFilesExist` 校验（该校验用于防护外部 Compaction 删除文件的场景）。
> 外部 Compaction 交互作为后续迭代实现。

**验证**：集成测试验证 RowDelta commit 成功，DV 文件正确提交。

---

### Subtask 3-4：Position 上报处理（§7.3）

**设计参考**：§7.3

**依赖**：Subtask 1-1 ~ 1-6, Subtask 2-2

**目标**：TabletServer 接收 Tiering Writer 的 position 上报并更新 DV 元数据。

**涉及文件**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvManager.java` — 新增 position report 处理方法
- RPC 定义（需新增 position report RPC）

**实现要点**：
1. 接收参数：`positionReport`, `locallyDeletedRowIds`, `splitOffsetRange`, `materializedDvFiles`, `actualSnapshotId`
2. **步骤 0**：结构性过期检查 — 若 `splitOffsetRange.latest_offset <= currentReadableSnapshotTieredOffset`，拒绝
3. **步骤 1**：获取 `dvLock.writeLock()`
4. **步骤 2**：对每个 `(RowId, row_position)`，在 FileDict 中查找或创建 file_id
5. **步骤 3**：按 RowId 范围统一处理：
   - RowId 在 PendingDeletes 中 → 标记 LakeDv，不写 pendingRowPos，不移除 PendingDeletes
   - RowId ∈ splitOffsetRange（新写入行）→ 写 pendingRowPos
   - RowId ∉ splitOffsetRange（重写行）→ 查 RowPosIndex/pendingRowPos：
     - 找到 → 写 pendingRowPos（覆盖旧位置）
     - 都找不到 → 标记 LakeDv
6. **步骤 4**：遍历 `locallyDeletedRowIds`，从 PendingDeletes 移除
7. **步骤 5**：WriteBatch 原子提交步骤 2-4 的所有修改
8. **步骤 6**：释放 `dvLock.writeLock()`
9. **步骤 7**（在 dvLock 外执行）：用 `materializedDvFiles` 过滤 DvManager 内存中的 `snapshotBitmap`，仅保留已物化的文件
10. **步骤 8**：发送 DV 完成通知（**必须在步骤 7 成功之后**，若步骤 7 失败则不发送通知，记录错误日志，下轮 tiering 重试）

> **注意**：相比完整版，本阶段不包含 `knownFiles` 追踪逻辑（用于外部 Compaction 文件区分），作为后续迭代实现。

**验证**：单元测试验证各分支逻辑（PendingDeletes 命中、新写入行、重写行、已删除行）。

---

### Subtask 3-5：Position Report RPC 定义

**设计参考**：§7.3

**依赖**：无（可与 Subtask 3-4 并行，但 3-4 需要此 RPC）

**目标**：在 FlussApi.proto 中定义 position report 相关的 RPC 消息。

**涉及文件**：
- `fluss-rpc/src/main/proto/FlussApi.proto` — 新增消息定义
- RPC 处理类 — 新增处理逻辑

**实现要点**：
1. 新增 protobuf 消息（按文件分组以提高传输效率）：
   ```protobuf
   message ReportPositionRequest {
     required int64 table_id = 1;
     optional int64 partition_id = 2;
     required int32 bucket_id = 3;
     repeated FilePositionEntries file_position_entries = 4;  // 按文件分组
     repeated int64 locally_deleted_row_ids = 5;
     required int64 split_last_tiered_offset = 6;
     required int64 split_latest_offset = 7;
     repeated string materialized_dv_files = 8;
     required int64 actual_snapshot_id = 9;
   }
   message FilePositionEntries {
     required string file_path = 1;
     repeated RowPosition row_positions = 2;
   }
   message RowPosition {
     required int64 row_id = 1;
     required int32 row_position = 2;
   }
   message ReportPositionResponse {}
   ```
2. 在 TabletService 中注册 RPC handler
3. Handler 调用 DvManager 的 position report 处理方法

---

## Phase 4：Snapshot 处理与 Readable 切换

> 依赖 Phase 3。

### Subtask 4-1：DV 完成通知与 DV-Readable 标记（§8.2 Step 2）

**设计参考**：§3.6, §8.2 Step 2

**依赖**：Subtask 3-4

**目标**：实现 CoordinatorServer 收集所有 bucket DV 完成通知后标记 snapshot 为 DV-readable。

**涉及文件**：
- `fluss-server/src/main/java/org/apache/fluss/server/coordinator/CoordinatorServer.java` — 新增 DV 完成通知处理
- ZooKeeper/元数据存储 — 更新 LakeTableZNode

**实现要点**：
1. CoordinatorServer 维护 per-snapshot 的 bucket 完成状态：`Map<snapshotId, Set<bucketId>>`
2. 收到某个 bucket 的 DV 完成通知后，加入该 snapshot 的 ready set
3. 所有 bucket 都 ready 后：
   - 更新 LakeTableZNode，标记该 snapshot 为 DV-readable
   - 通知所有 TabletServer 执行 readable 切换
4. **新增 RPC 定义**（FlussApi.proto）：
   ```protobuf
   // TabletServer → CoordinatorServer
   message NotifyDvReadyRequest {
     required int64 table_id = 1;
     optional int64 partition_id = 2;
     required int32 bucket_id = 3;
     required int64 snapshot_id = 4;
   }
   message NotifyDvReadyResponse {}
   
   // CoordinatorServer → TabletServer
   message NotifyReadableSwitchRequest {
     required int64 table_id = 1;
     optional int64 partition_id = 2;
     required int64 new_readable_snapshot_id = 3;
     required int64 new_readable_tiered_offset = 4;
   }
   message NotifyReadableSwitchResponse {}
   ```
5. LakeTableZNode 增加 `dvReadableSnapshotId` 字段

---

### Subtask 4-2：Readable 切换执行（§8.2 Step 3）

**设计参考**：§8.2 Step 3

**依赖**：Subtask 4-1, Subtask 1-1 ~ 1-6

**目标**：TabletServer 收到 readable 切换通知后，原子执行 pendingRowPos 迁移、oldFiles 清理、PendingDeletes 清理、LakeDv 差集清理。

**涉及文件**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvManager.java` — 新增 readable 切换方法

**实现要点**：
1. 计算 `oldFiles = snapshot_files(S_old) - snapshot_files(S_new)`。注意对比基准是前后两个 **readable snapshot**，不是相邻 commit snapshot。TabletServer 需要通过 Iceberg API 或从 Coordinator 通知中获取两个 snapshot 的文件列表。
2. **在 dvLock.writeLock() 下原子执行**：
   - a. 迁移 pendingRowPos → RowPosIndex（遍历 pendingRowPos，写入 RowPosIndex，覆盖旧值）
   - b. 清空 pendingRowPos
   - c. 对 oldFiles：删除 LakeDv 条目
   - d. PendingDeletes cleanup：`deleteRange(0, S_new_tiered_offset + 1)`
3. 执行 bitmap 差集清理（§13.3）：用 snapshotBitmap 执行 `当前 bitmap AND NOT snapshotBitmap`，清空 snapshotBitmap
4. 更新 `currentReadableSnapshot`

> **注意**：相比完整版，不包含 `knownFiles` 的移除操作（外部 Compaction 追踪功能在后续迭代实现）。
> 同样不触发 DvRocksDB Checkpoint（Checkpoint 机制在后续迭代实现）。

**验证**：测试 pendingRowPos 到 RowPosIndex 的迁移、oldFiles 清理、差集清理。

---

### Subtask 4-3：初始构建处理（§8.3）

**设计参考**：§8.3

**依赖**：Subtask 3-4

**目标**：处理首次 tiering 完成后的 RowPosIndex 构建。

**涉及文件**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvManager.java`

**实现要点**：
1. 判断条件：首次 tiering（无旧 readable snapshot）
2. 将所有上报的 `(RowId, file, row_position)` 直接写入 RowPosIndex（不经过 pendingRowPos）
3. 该 snapshot 立即成为首个 DV-readable snapshot

---

## Phase 5：Union Read 改造

> 依赖 Phase 2 + Phase 4。

### Subtask 5-1：TabletServer 侧 Union Read 接口（§10）

**设计参考**：§10

**依赖**：Phase 1 (LakeDv, LogDv), Phase 4 (readable snapshot)

**目标**：实现 TabletServer 侧的 union read DV 数据返回。

**涉及文件**：
- `fluss-server/src/main/java/org/apache/fluss/server/kv/KvTablet.java` — 新增 union read DV 获取方法
- `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/DvManager.java`
- RPC 定义 — 新增 union read DV 请求/响应

**实现要点**：
1. Client 发送请求携带 `requestedSnapshotId`
2. 获取 KvTablet 读锁
3. **获取 dvLock.readLock()**
4. **Snapshot 一致性校验**：检查 `requestedSnapshotId == currentReadableSnapshot`，不匹配则返回 stale snapshot error
5. 获取 `logEndOffset`
6. 从 LakeDv 获取 datafile list 对应的删除 bitmap（LakeDv 内部使用 file_id，需通过 FileDict 转换为 file_path 返回给 client）
7. 从 LogDv 获取 `[snapshot_start_offset, logEndOffset]` 范围的删除 bitmap
8. **释放 dvLock.readLock()**
9. **释放 KvTablet 读锁**
10. 返回 `{lakeDv, logDv, logEndOffset}`

**验证**：测试 union read 返回正确的 LakeDv + LogDv，snapshot 一致性校验生效。

---

### Subtask 5-2：Union Read RPC 定义

**设计参考**：§10

**涉及文件**：
- `fluss-rpc/src/main/proto/FlussApi.proto`

**实现要点**：
```protobuf
message GetDvForUnionReadRequest {
  required int64 table_id = 1;
  optional int64 partition_id = 2;
  required int32 bucket_id = 3;
  required int64 requested_snapshot_id = 4;
  repeated string data_files = 5;  // Iceberg snapshot 中的 data file 列表
}
message GetDvForUnionReadResponse {
  repeated LakeDvEntry lake_dv = 1;
  repeated LogDvEntry log_dv = 2;
  required int64 log_end_offset = 3;
  optional int64 current_readable_snapshot = 4;  // 用于 stale 时返回
  optional bool is_stale = 5;
}
message LakeDvEntry {
  required string file_path = 1;
  required bytes del_bitmap = 2;  // RoaringBitmap serialized
}
message LogDvEntry {
  required int64 base_offset = 1;
  required bytes del_bits = 2;
}
```

---

### 🔀 Subtask 5-3：Client 侧 Union Read 适配

**设计参考**：§10 Client 侧处理

**依赖**：Subtask 5-1, 5-2

**目标**：改造 Flink connector 的 union read，支持 DV 过滤。

**涉及文件**：
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/reader/LakeSnapshotAndLogSplitScanner.java` — 改造 merge 逻辑
- `fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lake/reader/LakeSnapshotScanner.java`
- `fluss-lake/fluss-lake-iceberg/src/main/java/org/apache/fluss/lake/iceberg/source/IcebergRecordReader.java`

**实现要点**：
1. **Iceberg 读取改造**：读 Iceberg snapshot 时先 apply Iceberg 物理 DV（Puffin 文件，Iceberg v3 原生支持——`IcebergRecordReader` 使用的 FileScanTask 自动包含 DV 信息），再 apply LakeDv（从 TabletServer 获取的逻辑 DV）过滤额外的行
2. **LakeDv 过滤实现**：在 `IcebergRecordReader` 层包装一个 filter iterator，读到行时检查该行的 `(file_path, row_position)` 是否在 LakeDv 中，若在则跳过
3. **Changelog 读取改造**：Fetch `[snapshot_start_offset, logEndOffset]` 这段 changelog 时 apply LogDv。在 `LakeSnapshotAndLogSplitScanner` 的 log 读取路径中增加 LogDv 过滤——读到某个 offset 的记录时，检查该 offset 是否在 LogDv 的 bitmap 中，若在则跳过
4. **替代 SortMergeReader**：当前 `LakeSnapshotAndLogSplitScanner` 使用 `SortMergeReader` 按主键 merge 去重。有了 DV 后，不再需要 merge 去重——直接串行读取：先输出存活的 Iceberg 行，再输出 changelog 中未被 LogDv 屏蔽的 `+I`/`+U` 行（`-U`/`-D` 类型的 retract 记录不输出）。`SortMergeReader` 可以在 DV 模式下跳过。
5. **Stale snapshot 重试**：收到 stale error 后，使用新 snapshotId 重试。在 split reader 层处理此异常。
6. **DV 数据获取时机**：在开始读取 Iceberg data 之前，先通过 RPC 获取 LakeDv + LogDv + logEndOffset，确保读取过程中使用一致的 DV 视图

**验证**：端到端测试验证 union read 正确去重，无需 SortMerge 也能正确输出。

---

## Phase 6：LakeDv 物化完整流程

> 这是 §13 描述的完整物化流程，涉及 Phase 3 和 Phase 4 的协调。主要是集成验证。

### Subtask 6-1：LakeDv 物化端到端集成（§13）

**设计参考**：§13.1, §13.2, §13.3

**依赖**：Phase 3 + Phase 4

**目标**：验证 LakeDv 从逻辑删除到物化为 Puffin DV 再到差集清理的完整流程。

**实现要点**：
1. 验证触发时机：每轮 tiering commit
2. 验证物化流程：
   - TabletServer 快照 LakeDv + FileDict 解析 → 随 split 下发
   - Tiering Writer 生成 Puffin DV → commit
3. 验证清理时机：新 snapshot 成为 DV-readable 后
4. 验证 bitmap 差集清理：快照后新增的 bit 被保留
5. 验证 snapshotBitmap 与 materializedDvFiles 的过滤对齐

---

## Phase 7：端到端集成测试

> 依赖所有前置 Phase。

### Subtask 7-1：端到端测试 — 设计文档 §14 示例场景

**设计参考**：§14

**目标**：按 §14 的完整示例编写端到端集成测试。

**测试场景覆盖**：
1. 写入 3 条数据 → 第一轮 tiering → RowPosIndex 构建
2. 更新 key1 → LakeDv + LogDv 更新
3. Union read 验证（DV 过滤后结果正确）
4. 删除 key3 → LakeDv 累加
5. 第二轮 tiering → Puffin DV 物化 + pendingRowPos 迁移 + 差集清理
6. Readable 切换后 union read 验证

### 🔀 Subtask 7-2：并发场景测试

**目标**：测试 dvLock 在并发场景下的正确性。

**测试场景**：
1. Changelog 同步 + Position 上报并发
2. Union Read + Readable 切换并发
3. PendingDeletes 在时序间隙中正确处理

---

## 依赖关系总览

```
Phase 0 (数据格式)  🔀  Phase 1 (DvRocksDB 存储)
  ├── 0-1 KV Value 格式           ├── 1-1 DvRocksDB 实例  🔀
  ├── 0-2 Changelog 格式          ├── 1-2 RowPosIndex     🔀
  │     └─ 依赖 0-1               ├── 1-3 LogDv           🔀
  ├── 0-3 Iceberg 版本    🔀      ├── 1-4 LakeDv          🔀
  ├── 0-4 FULL 校验       🔀      ├── 1-5 PendingDeletes  🔀
  └── 0-5 DV 功能开关     🔀      └── 1-6 DvManager 骨架
          │                                  │
          └──────────── 同时依赖 ─────────────┘
                         ▼
Phase 2 (写入流程)
  ├── 2-1 写入流程改造
  └── 2-2 Changelog 同步后 DV 更新（依赖 2-1）
                         ▼
Phase 3 (Tiering 改造)
  ├── 3-1 Tiering Split + LakeDv 快照
  ├── 3-2 DvTaskWriter（依赖 3-1）
  ├── 3-3 IcebergLakeCommitter（依赖 3-2）
  ├── 3-4 Position 上报处理（依赖 2-2）
  └── 3-5 Position Report RPC（🔀 与 3-1/3-2 并行）
                         ▼
Phase 4 (Snapshot 处理)
  ├── 4-1 DV 完成通知 + Readable 标记
  ├── 4-2 Readable 切换执行（依赖 4-1）
  └── 4-3 初始构建处理
                         ▼
Phase 5 (Union Read)
  ├── 5-1 Server 接口
  ├── 5-2 RPC 🔀
  └── 5-3 Client 适配
                         ▼
Phase 6 (LakeDv 物化集成验证)
                         ▼
Phase 7 (端到端测试)
  ├── 7-1 §14 示例  🔀
  └── 7-2 并发测试  🔀
```

**关键并行点**：
- Phase 0 与 Phase 1 **完全并行**（两者无任何依赖）
- Phase 7 的 2 个测试 subtask **可并行**

---

## 与完整版的差异说明

本文档相比 `deletion-vector-subtasks.md` 的完整版，**裁剪了以下内容**：

| 裁剪项 | 完整版对应 | 影响范围 |
|--------|-----------|---------|
| **DvRocksDB Checkpoint** | Phase 6 Subtask 6-1 | DvRocksDB 不实现 `createCheckpoint()`；readable 切换后不触发 checkpoint |
| **DvRocksDB 恢复流程** | Phase 6 Subtask 6-2 | DvManager 不实现 `recover()` 方法；TabletServer 重启后 DV 状态需全量重建（从 Iceberg snapshot + changelog 重放） |
| **外部 Compaction 检测** | Phase 7 Subtask 7-1 | Tiering commit 不检测外部 compaction 文件；不扫描外部新文件 |
| **Snapshot 过期策略** | Phase 7 Subtask 7-2 | 不配置 Iceberg snapshot 过期保护；不新增 compaction metric |
| **`knownFiles` 追踪** | Subtask 1-6, 3-4, 4-2 | DvManager 不维护 `knownFiles` 集合 |
| **`validateDataFilesExist`** | Subtask 3-3 | IcebergLakeCommitter 不校验 data file 是否仍存在 |
| **恢复场景测试** | Phase 9 Subtask 9-2 | 不包含 failover 恢复测试 |
| **Compaction 场景测试** | Phase 9 Subtask 9-3 | 不包含外部 compaction 测试 |

**后续迭代计划**：
- Checkpoint / 恢复流程将在 DV 基本功能验证通过后独立实现
- 外部 Compaction 交互将在与 Spark/Trino compaction 集成时实现

---

## Sub-Agent 执行建议

### 第一批（同时开 8 个 sub-agent —— Phase 0 + Phase 1 完全并行）
- Agent A → Subtask 0-1（KV Value 格式）→ Subtask 0-2（Changelog 格式，依赖 0-1）
- Agent B → Subtask 0-3（Iceberg 版本）+ Subtask 0-5（DV 功能开关）
- Agent C → Subtask 0-4（FULL 校验，等 0-5 完成后执行）
- Agent D → Subtask 1-1（DvRocksDB 实例）
- Agent E → Subtask 1-2（RowPosIndex + FilePos + FileDict）
- Agent F → Subtask 1-3（LogDv）
- Agent G → Subtask 1-4（LakeDv）
- Agent H → Subtask 1-5（PendingDeletes）+ Subtask 1-6（DvManager 骨架）

### 第二批（Phase 2，顺序执行）
- Agent I → Subtask 2-1 → Subtask 2-2（写入流程改造 + DV 更新）

### 第三批（Phase 3，同时开 2 个 sub-agent）
- Agent J → Subtask 3-5（RPC 定义）+ Subtask 3-1（Tiering Split）→ Subtask 3-2（DvTaskWriter）→ Subtask 3-3（IcebergLakeCommitter）
- Agent K → Subtask 3-4（Position 上报处理）

### 第四批（Phase 4，顺序执行）
- Agent L → Subtask 4-1 + 4-2 + 4-3（Snapshot 处理全流程）

### 第五批（Phase 5，顺序执行）
- Agent M → Subtask 5-2（Union Read RPC）→ Subtask 5-1 + 5-3（Server + Client）

### 第六批
- Agent N → Subtask 6-1（LakeDv 物化端到端集成验证）

### 第七批（同时开 2 个 sub-agent）
- Agent O → Subtask 7-1（§14 端到端测试）
- Agent P → Subtask 7-2（并发场景测试）

> **总计 16 个 sub-agent，分 7 批执行。第一批最大化并行度（8 个），总体开发效率最高。**
> 相比完整版减少 4 个 sub-agent（省去恢复和 Compaction 相关任务）。
