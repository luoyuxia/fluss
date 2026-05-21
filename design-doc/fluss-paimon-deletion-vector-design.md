# Fluss + Paimon Deletion Vector 集成设计文档

## 1. 背景与动机

在 Streamhouse 架构下，Fluss 作为实时层，Paimon 作为历史层（替代 Iceberg 的角色）。Fluss 持续将实时数据下沉到 Paimon，并提供 **union read** —— 将尚未下沉的热层增量数据与 Paimon 中的历史数据合并，呈现一张完整的、具有 exactly-once 语义的表。

本文档基于 [Fluss Deletion Vector 设计文档（Iceberg 版）](fluss-deletion-vector-design-v3-en.md)，针对 Paimon 作为历史层的场景进行适配设计。核心动机与 Iceberg 版相同：

1. **解决 Union Read 跨层去重**：维护轻量级的逻辑删除标记（Lake DV + Log DV），使 union read 能**即时**屏蔽 Paimon 中已被更新或删除的行。
2. **替代 Equality Delete**：利用 Paimon 原生的 compaction 机制处理删除，而非依赖 equality delete 文件。

### 与 Iceberg 方案的核心差异

Paimon 与 Iceberg 在架构上有本质区别：Paimon 是一个具有 LSM Tree 结构的表存储引擎，拥有原生的 compaction、merge 和 deletion vector 机制。这导致两个关键差异：

**差异一：不需要显式写 Deletion Vector**

在 Iceberg 方案中，Fluss TieringService 需要将 LakeDv 快照生成 Puffin DV 文件并提交到 Iceberg。而在 Paimon 方案中：

- Fluss 将所有 changelog 记录（包括 `-D` 删除记录）写入 Paimon
- Paimon 的 merge tree 和 compaction 机制原生处理数据合并和删除
- 无需 Fluss 显式生成任何 DV 文件

**差异二：写入时无法得知 FilePos**

在 Iceberg 方案中，TieringService 直接写 data file，因此在写入时就知道每条记录的精确位置 `(file_id, row_position)`。而在 Paimon 方案中：

- TieringService 写入的数据进入 Paimon L0 层文件
- L0 文件会被 Paimon compaction 合并到更低层级（L1, L2, ...）
- 在 compaction 完成前，数据的最终物理位置是未知的
- 必须在 compaction 完成后扫描输出文件，才能建立 RowId → FilePos 映射
- **compaction 不一定由 TieringService 自身触发** —— 可能由独立的 Paimon compact job 或其他外部进程完成

这两个差异从根本上改变了 Tiering Pipeline 的设计：写入与位置构建被解耦，TieringService 需要在写入后**等待并检测** compaction 完成，而非自行控制全流程。

---

## 2. 整体架构：三层 Deletion Vector

```
┌──────────────────────────────────────────┐
│             Fluss (Hot Layer)            │
│  ┌──────────┐  ┌──────────┐  ┌────────┐ │
│  │Changelog │  │ Log DV   │  │Lake DV │ │
│  └──────────┘  └──────────┘  └────────┘ │
└──────────────────────┬───────────────────┘
                       │ compaction 清理
                       ▼
┌──────────────────────────────────────────┐
│           Paimon (Cold Layer)            │
│  ┌──────────────┐  ┌──────────────────┐  │
│  │ Data Files   │  │ Paimon DV        │  │
│  │ (Parquet/ORC)│  │ (by compaction)  │  │
│  └──────────────┘  └──────────────────┘  │
└──────────────────────────────────────────┘
```

### Layer 1：Paimon Deletion Vector（由 compaction 生成）

Paimon 原生支持 Deletion Vector（`BitmapDeletionVector` / `Bitmap64DeletionVector`）。在 compaction 过程中，Paimon 会：

- 合并 L0 层的新写入（包括 DELETE 标记）与底层数据
- 在底层 compaction 中消除已删除的行
- 自动管理 DV 文件的生命周期

Fluss 不需要直接参与 Paimon DV 的管理。

### Layer 2：Log Deletion Vector（与 Iceberg 方案相同）

追踪 Fluss 实时 changelog 内部的删除和更新。仅适用于尚未下沉到 Paimon 的热层数据。详见 Iceberg 版设计文档 §2 Layer 2。

### Layer 3：Lake Deletion Vector（概念相同，清理机制不同）

热层与历史层之间的桥梁。当 Fluss 收到针对已下沉到 Paimon 的行的删除或更新时：

- TabletServer 在 LakeDv 中记录逻辑删除标记 `(file_id → deleted row position bitmap)`
- 该逻辑删除在 union read 期间**立即生效**
- 这些逻辑删除在后续的 Paimon compaction 替换相关文件时被清理

**与 Iceberg 方案的关键区别**：Iceberg 方案通过 bitmap diff 清理已物化的 LakeDv 条目；Paimon 方案通过**文件生命周期**清理 —— 当 compaction 替换旧文件时，旧文件的 LakeDv 条目被删除。

### Union Read 语义

Union Read 期间，查询引擎同时应用三层 DV：

- **Paimon DV**：屏蔽 Paimon 中已被物理删除的行（由 compaction 生成）
- **Lake DV**：屏蔽 Paimon 中已被 Fluss 逻辑删除但尚未被 compaction 处理的行
- **Log DV**：屏蔽 Fluss 热层中已被后续操作覆盖或删除的行

---

## 3. 数据模型与存储

### 3.1 RowId

与 Iceberg 方案完全相同。RowId 唯一标识 KV 记录的一个**特定版本**，其值为对应 `+I` / `+U` changelog 记录的 log offset。详见 Iceberg 版设计文档 §3.1。

### 3.2 FilePos

定位一行在 Paimon 中的物理位置，由两部分组成：

- **file_id**：数据文件的字典编码 ID（int 类型），通过 FileDict 映射到 Paimon data file 名称
- **row_position**：文件内的行号（0-based，long 类型）

编码方式与 Iceberg 方案相同（unsigned varint / LEB128）。

**与 Iceberg 方案的区别**：Paimon 的 data file 路径格式不同（如 `data-{uuid}-{seq}.parquet`），但 FileDict 的字典编码机制完全适用。

### 3.3 DvRocksDB

与 Iceberg 方案基本相同，包含五个 Column Family：

| Column Family | Key | Value | 描述 |
|---|---|---|---|
| **RowPosIndex** | RowId (8B) | FilePos (varint) | 当前可读快照中的位置。仅在 readable switch 时更新（SST Ingest）。|
| **LogDv** | offset_range | del_bitmap | changelog 范围内的已删除 offset |
| **LakeDv** | file_id (4B) | del_bitmap (RoaringPositionBitmap) | 未物化的逻辑删除标记 |
| **FileDict** | file_path (string) ↔ file_id (int) | (双向) | 文件路径的字典编码 |
| **PendingDeletes** | RowId (8B) | FilePos (varint) 或 `pending` 标记 | 未物化的死行日志 |

**与 Iceberg 方案的区别**：

- **LakeDv 清理方式不同**：不再通过 `materializedLakeDv` 做 bitmap diff，而是通过文件生命周期清理（compaction 替换文件时删除对应的 LakeDv 条目）
- **PendingDeletes 的清理方式不同**：通过 oldFiles（被 compaction 替换的文件）清理指向旧文件的条目

### 3.4 并发控制：DvRWLock

与 Iceberg 方案相同。所有写路径获取写锁并串行化；union read 获取读锁。详见 Iceberg 版设计文档 §3.4。

---

## 4. 写入路径

### 4.1 实时写入 (+I/+U)

与 Iceberg 方案完全相同。详见 Iceberg 版设计文档 §4.1。

### 4.2 删除处理 (-U/-D)

与 Iceberg 方案完全相同。Changelog 成功同步后：

1. 获取 KvTablet 写锁
2. Flush PrewriteBuffer 到 RocksDB
3. 获取 DvRWLock 写锁
4. 对每个 `-U` / `-D` 条目：
   - Point-get `RowPosIndex` for `oldRowId`
   - Hit：标记 LakeDv，删除 RowPosIndex 条目，写 PendingDeletes
   - Miss：写 PendingDeletes 为 `pending`
   - 更新 LogDv
5. 释放 DvRWLock 写锁
6. 更新 `log_hw`
7. 释放 KvTablet 写锁

详见 Iceberg 版设计文档 §4.2。

---

## 5. Tiering Pipeline（核心差异）

这是与 Iceberg 方案差异最大的部分。由于 Paimon 的 LSM Tree 架构，Tiering Pipeline 需要重新设计。

### 5.1 端到端概览

与 Iceberg 方案的关键区别：

1. **SST 生成发生在 compaction 之后，而非写入时**
2. **写入与 compaction 解耦** —— compaction 可能由 TieringService 自身触发，也可能由外部 compact job 完成
3. **只有 COMPACT snapshot 才是 readable 的** —— Paimon DV 表中，APPEND snapshot 仅包含 L0 文件（位置不稳定），只有 COMPACT snapshot 将 L0 合并到底层后，数据才是 readable 的。因此，Prepare → Publish → Readable Switch 流程**只在检测到新的 COMPACT snapshot 时触发**，而非每次写入都触发（这与 Iceberg 方案不同，Iceberg 中每个 snapshot 都触发该流程）。
4. **Readable offset 是 per-bucket 的，且不等于 tiered offset** —— 因为 compaction 是 per-bucket 的，不同 bucket 的 L0 可能在不同时机被 flush 到底层。每个 bucket 的 readable offset 取决于该 bucket 的 L0 是否已被 compaction 消费。

Phase A 被拆分为三个子阶段（A1/A2/A3）：

```
┌─────────────────────────────────────────────────────────────┐
│  Phase A1: 写入 Paimon (TieringService)                      │
│    1. 读取 changelog                                        │
│    2. 写入 +I/+U 为 ADD 记录到 Paimon L0                     │
│    3. 写入 -D 为 DELETE 记录到 Paimon L0                      │
│    4. 提交 Paimon APPEND snapshot                             │
│                                                             │
│    ⚠ APPEND snapshot 不触发后续流程                            │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase A2: Compaction (TieringService 或 外部 compact job)    │
│                                                             │
│    方式一：TieringService 自行触发 full compaction 并等待       │
│    方式二：外部 compact job 独立执行 compaction                 │
│                                                             │
│    产生 Paimon COMPACT snapshot                               │
│    ✓ COMPACT snapshot 触发 Phase A3                           │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase A3: 扫描 + SST 生成 + Readable Offset 计算             │
│    (TieringService / DvTableReadableSnapshotRetriever)       │
│                                                             │
│    1. 检测到新 COMPACT snapshot                               │
│    2. 计算 per-bucket readable offset（见 §5.2.3）            │
│    3. 扫描 compaction 输出文件，提取 __rowid → FilePos         │
│    4. 生成 SST 并上传到远程存储                                │
│    5. 向 CoordinatorServer 报告（含 per-bucket 的              │
│       tieredOffsets 和 readableOffsets）                      │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase B: Prepare (SST 预取)                                 │
│                                                             │
│  CoordinatorServer → TabletServer:                          │
│    1. 下载 SST（无锁，纯远程 I/O）                            │
│    2. 写 FileDict，存储 SST 路径（写锁，轻量）                 │
│    3. 发送 ready ack                                        │
│  CoordinatorServer: 等待所有 bucket 的 ready ack              │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase C: Publish + Readable Switch                          │
│                                                             │
│  CoordinatorServer:                                         │
│    更新 LakeTableZNode，标记 COMPACT snapshot 为 DV-readable  │
│  TabletServer (per-bucket):                                 │
│    1. Ingest SST → RowPosIndex                              │
│    2. Batch resolve PendingDeletes                          │
│    3. 清理 oldFiles 的 LakeDv 和 PendingDeletes              │
│    4. 清理过期 LogDv                                         │
│    5. 更新 readableSnapshotId 和 per-bucket 的               │
│       snapshotStartLogOffset（= readableOffset）             │
└─────────────────────────────────────────────────────────────┘
```

**与 Iceberg 方案的时间线对比**：

```
Iceberg:  每个 snapshot → 写入 → SST → 提交 → 报告 → Prepare → Switch
Paimon:   APPEND snapshot → 仅写入 L0（不触发后续流程）
          COMPACT snapshot → 扫描 → SST → 报告 → Prepare → Switch
```

**关键概念：tieredOffset vs readableOffset**：

```
tieredOffset[bucket]    = 数据已写入到 Paimon 的最大 log offset（来自最新 APPEND snapshot）
readableOffset[bucket]  = 数据已被 compaction 到底层文件的最大 log offset（per-bucket）

tieredOffset >= readableOffset（对每个 bucket）
```

- `tieredOffset` 回答："数据写到了哪里？"
- `readableOffset` 回答："数据可读到哪里？"（只有 compaction 后的底层文件才是 readable 的）

**举例**：
```
APPEND S1: bucket0 写入 offset 0-3, bucket1 写入 offset 0-5
COMPACT S2: 仅 compact 了 bucket0 的 L0 → L1
APPEND S3: bucket0 写入 offset 4-7

此时 S2 是 readable snapshot:
  tieredOffset  = {bucket0: 3, bucket1: 5}  （S1 写入的最远位置）
  readableOffset = {bucket0: 3, bucket1: ?}
  // bucket0 已经没有 L0 → readableOffset = tieredOffset = 3
  // bucket1 仍有 L0 → readableOffset 需要回溯到更早的 COMPACT 找到其 L0 flush 点
```

**两阶段 ack 语义**：与 Iceberg 方案相同（ready ack + switched ack）。

### 5.2 Phase A1：写入 Paimon

#### Split 生成（TabletServer 侧）

与 Iceberg 方案类似，但**不再需要 `lakeDvSnapshot`** 用于生成 DV 文件。

1. **获取 KvTablet 读锁**
2. 读取当前 `log_hw` 作为 `latest_offset`
3. 快照 LogDv（split 范围内的已删除 RowId），生成 `logDvSnapshot`
4. **释放读锁**
5. 生成 tiering split：`{offset_range: (last_tiered_offset, latest_offset], logDvSnapshot}`

> **为什么不再需要 `lakeDvSnapshot`**：在 Iceberg 方案中，`lakeDvSnapshot` 用于让 TieringService 生成 Puffin DV 文件。在 Paimon 方案中，删除操作以 DELETE 记录的形式写入 Paimon，由 Paimon compaction 处理，因此不需要导出 LakeDv 快照。

#### TieringService 写入

**Step 1. 读取 changelog** `(last_tiered_offset, latest_offset]`

**Step 2. 写入 Paimon**：

对 changelog 中的每条记录：

- **`+I` / `+U`（logDvSnapshot 未命中）**：写入 Paimon 作为 `KeyValue(key, seq, ADD, value)`
  - `seq` 使用 RowId（log offset）作为 sequence number
  - value 中包含 `__rowid` 系统列（= RowId），用于后续 compaction 后的位置扫描
- **`+I` / `+U`（logDvSnapshot 命中）**：跳过，该行在本轮内已被删除
- **`-D`**：写入 Paimon 作为 `KeyValue(key, seq, DELETE, null)`
  - 这使得 Paimon compaction 在合并时删除该 key 对应的旧数据
  - `seq` 使用该 `-D` 记录的 log offset
- **`-U`**：跳过，不写入 Paimon
  - 对应的 `+U` 已经作为 ADD 写入，Paimon 的 merge 机制（按 key + sequence number 去重）会自动用新版本覆盖旧版本

> **为什么 `-U` 不需要写入 Paimon**：Paimon 的 DEDUPLICATE merge engine 按 primary key 去重，保留 sequence number 最大的版本。当 `+U` 写入后，其 sequence number 大于旧版本，compaction 时自然覆盖旧版本。`-U` 的语义已由 Paimon merge 隐式处理。

> **为什么 `-D` 需要写入 Paimon**：如果某个 key 在前一轮 tiering 中已写入 Paimon，且在本轮 changelog 中被删除（`-D`），我们必须将 DELETE 标记写入 Paimon，否则该 key 会永久留在 Paimon 的数据文件中。

**Step 3. 提交到 Paimon**：调用 Paimon writer 的 `prepareCommit()` + `commit()`，产生 Paimon snapshot `S_write`。此时数据在 L0 层。Snapshot property 中标记 `fluss.tiering = true`，用于区分 Fluss 产生的 snapshot 和外部 compaction 产生的 snapshot。

### 5.2.1 Phase A2：Compaction

Compaction 将 L0 层数据合并到更低层级（L1, L2, ...），产生稳定的数据文件。**Compaction 的执行方是灵活的**：

#### 方式一：TieringService 自行触发

TieringService 在写入提交后显式触发 compaction：

```java
// 通过 CompactManager 触发 full compaction
compactManager.triggerCompaction(true /* fullCompaction */);
CompactResult result = compactManager.getCompactionResult(true /* blocking */);
```

等待 compaction 完成后，Paimon 产生新 snapshot `S_compact`（commitKind = COMPACT）。

**适用场景**：TieringService 对 Paimon 表有独占写入权限，compaction 由 TieringService 统一管理。

#### 方式二：外部 Compact Job 执行

一个独立的 Paimon compact job（例如 Flink compact job 或周期性 compact 任务）负责 compaction：

- TieringService 写入 L0 后不触发 compaction
- 外部 compact job 周期性或按策略执行 compaction，将 L0 合并到底层
- TieringService 在 Phase A3 中**检测**外部 compaction 是否已处理本轮写入的 L0 数据

**适用场景**：生产环境中 compaction 资源需要独立管理，或多个写入方共享同一 Paimon 表的 compaction 任务。

> **两种方式对后续流程透明**：无论 compaction 由谁执行，Phase A3 的处理逻辑完全相同 —— 通过 Paimon snapshot diff 检测 compaction 产生的文件变更，扫描新文件建立位置映射。

### 5.2.2 Phase A3：COMPACT Snapshot 检测 + Readable Offset 计算 + 扫描 + SST 生成

Phase A3 在检测到新的 COMPACT snapshot 时触发。无论 compaction 来源（自行触发或外部），处理逻辑完全相同。

#### Step 1. 检测新的 COMPACT Snapshot

每次 APPEND snapshot 提交后，TieringService 检查是否存在新的 COMPACT snapshot（参见 `DvTableReadableSnapshotRetriever`）：

```python
# 在当前 tiered snapshot 之前（含），找到最新的 COMPACT snapshot
latestCompactedSnapshot = findPreviousSnapshot(tieredSnapshotId, COMPACT)

if latestCompactedSnapshot is None:
    # 没有 compaction 发生过，跳过
    return None

if latestCompactedSnapshot already registered in Fluss:
    # 这个 COMPACT snapshot 已经处理过，跳过（避免重复工作）
    # 多个 APPEND snapshot 可能跟在同一个 COMPACT snapshot 后面
    return None
```

> **何时触发**：每次 APPEND snapshot 提交后都会检查。但只有当检测到**尚未注册**的新 COMPACT snapshot 时才继续执行后续步骤。
>
> **如果 TieringService 自行触发 compaction**：compaction 完成后立即产生 COMPACT snapshot，检测逻辑会在下一次检查时发现它。
>
> **如果依赖外部 compact job**：外部 compact job 产生 COMPACT snapshot 后，TieringService 在下一次 APPEND 提交后的检查中发现它。

#### Step 2. 计算 Per-Bucket Readable Offset

**核心问题**：COMPACT snapshot 的 readable offset **不等于** tiered offset。原因是 compaction 是 per-bucket 的 —— 一次 COMPACT snapshot 可能只 flush 了部分 bucket 的 L0 文件，其他 bucket 的 L0 仍然存在。

**算法**（参见 `DvTableReadableSnapshotRetriever.getReadableSnapshotAndOffsets()`）：

1. 在 COMPACT snapshot 中，分类所有 bucket：
   - **无 L0 文件的 bucket**：该 bucket 的所有数据都在 base file（L1+），可以使用最新 tiered offset
   - **有 L0 文件的 bucket**：该 bucket 的 L0 尚未被 flush，需要回溯找到安全的 readable offset

2. 对于**无 L0 文件**的 bucket：`readableOffset[bucket] = latestTieredOffset[bucket]`

3. 对于**有 L0 文件**的 bucket，从 latestCompactedSnapshot 向前遍历 COMPACT snapshot：
   - 找到最近一次 flush 了该 bucket L0 的 COMPACT snapshot
   - 找到该 COMPACT snapshot flush 的 L0 文件的来源 snapshot（"exactly holding" 这些 L0 文件的最新 snapshot）
   - 找到该来源 snapshot 之前的 APPEND snapshot
   - `readableOffset[bucket]` = 该 APPEND snapshot 在 Fluss 中注册的 offset

```python
readableOffsets = {}
bucketsWithoutL0, bucketsWithL0 = classifyBuckets(latestCompactedSnapshot)

# 无 L0 的 bucket：直接用最新 tiered offset
for bucket in bucketsWithoutL0:
    readableOffsets[bucket] = latestTieredSnapshot.offset[bucket]

# 有 L0 的 bucket：回溯找安全 offset
for compactSnapshot in compactSnapshots_descending():
    flushedBuckets = getBucketsWithFlushedL0(compactSnapshot)
    for bucket in flushedBuckets:
        if bucket not in readableOffsets:
            sourceSnapshot = findLatestSnapshotExactlyHoldingL0Files(compactSnapshot)
            previousAppendSnapshot = findPreviousSnapshot(sourceSnapshot, APPEND)
            readableOffsets[bucket] = fluss.getLakeSnapshot(previousAppendSnapshot).offset[bucket]
```

4. `tieredOffsets` = COMPACT snapshot 之前最近 APPEND snapshot 在 Fluss 中注册的 offset（per-bucket）

> **为什么 readableOffset 要回溯到 L0 来源的 APPEND snapshot**：COMPACT snapshot 中有 L0 的 bucket，其 L0 文件包含了某些 APPEND snapshot 写入的数据。这些 L0 数据的位置不稳定（会被后续 compaction 改变）。安全的 readable offset 是这些 L0 文件写入之前的 offset —— 即 L0 来源 snapshot 的前一个 APPEND snapshot 的 offset。在这个 offset 之前的所有数据都在 base file 中，位置稳定。

> **示例**：
> ```
> APPEND S1: bucket0 写 offset 0-3, bucket1 写 offset 0-5
> COMPACT S2: flush bucket0 的 L0 → L1, 未 flush bucket1
> APPEND S3: bucket0 写 offset 4-7
>
> COMPACT S2 作为 readable snapshot:
>   tieredOffsets     = S1 的 offsets = {bucket0: 3, bucket1: 5}
>   readableOffsets:
>     bucket0: 无 L0 → readableOffset = tieredOffset = 3
>     bucket1: 有 L0 → 回溯... 假设 bucket1 从未被 compact → 无法 advance
>       (实际实现中，如果找不到 flush 记录，返回 null，跳过本次 advance)
> ```

#### Step 3. 收集文件变更

收集从 **上一轮 readable snapshot** 到**当前 COMPACT snapshot** 之间的所有 COMPACT snapshot 的文件变更：

```python
allNewFiles = set()
allOldFiles = set()

for snapshot in paimon_snapshots_between(lastReadableSnapshot, latestCompactedSnapshot):
    if snapshot.commitKind == COMPACT:
        allNewFiles += snapshot.addedDataFiles()
        allOldFiles += snapshot.removedDataFiles()
```

> **为什么从 lastReadableSnapshot 开始**：在两次 readable switch 之间，可能有多次 COMPACT snapshot（包括外部 compact job 对历史数据的合并）。这些 compaction 改变了文件位置，必须全部捕获。

#### Step 4. 扫描 compaction 输出文件，建立 RowId → FilePos 映射

对每个 newFile（compaction 产生的新文件），读取其中的 `__rowid` 列（projection pushdown，仅读取该列）：

```python
for file in allNewFiles:
    reader = createReader(file, projection=["__rowid"])
    for row_position, row in enumerate(reader):
        rowId = row.__rowid
        filePos = (file_id, row_position)
        sst_entries.append((rowId, filePos))
```

> **扫描效率**：仅读取 `__rowid` 这一个 long 类型的列，利用列式存储的 projection pushdown，I/O 开销很小。扫描范围限于 compaction 输出的新文件（allNewFiles），而非全表。

> **统一处理**：无论文件变更来自本轮写入的 compaction 还是外部后台 compaction，扫描逻辑完全相同。SST 中包含所有新文件中的行映射 —— 既包括本轮新写入的行，也包括被 compaction 重写的历史行。

#### Step 5. 生成 SST 并上传（与 Iceberg 方案相同）

- 为每个 `file_path` 分配 `fileId`（通过 FileDictAllocator）
- 收集新分配的 `(fileId → file_path)` 条目作为 `newFileDictEntries`
- SstFileWriter 生成 SST（`key=RowId` 排序，`value=fileId+row_position`）
- 上传 SST 到远程存储 `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`
- 写入跨 bucket 索引文件

#### Step 6. 向 CoordinatorServer 报告

- `indexUuid` —— SST 位置的跨 bucket 索引 UUID
- `newFileDictEntries` —— 新 fileId → file_path 映射
- `tieredOffsets` —— per-bucket 的 tiered offset（来自 COMPACT snapshot 前最近 APPEND 在 Fluss 注册的 offset）
- `readableOffsets` —— per-bucket 的 readable offset（由 Step 2 计算得到）
- `oldFiles` —— `allOldFiles`，所有被 compaction 替换的旧文件列表（用于 readable switch 时清理 LakeDv）
- `readableSnapshotId` —— COMPACT snapshot id（作为新的 readable snapshot）
- `earliestSnapshotIdToKeep` —— 计算 readable offset 过程中访问的最早 snapshot id（用于 Fluss 侧 snapshot 清理）

> **per-bucket offset 而非全局 currentTieredOffset**：Iceberg 方案中每个 snapshot 对所有 bucket 统一推进 offset。Paimon 方案中，由于 compaction 是 per-bucket 的，不同 bucket 的 readable offset 可能不同。
>
> **不再报告 `materializedLakeDv`**：Iceberg 方案中 TieringService 报告哪些 LakeDv 条目已被物化为 Puffin DV。Paimon 方案中不需要此信息 —— LakeDv 的清理基于文件生命周期（oldFiles），而非 bitmap diff。
>
> **`oldFiles` 包含所有来源的 compaction**：oldFiles 列表不仅包含本轮写入触发的 compaction 所替换的文件，也包含外部后台 compaction 所替换的文件。TabletServer 在 readable switch 时统一清理这些文件的 LakeDv 和 PendingDeletes 条目。

#### FileDictAllocator

与 Iceberg 方案相同。`nextFileId` 通过 Paimon snapshot property 恢复。详见 Iceberg 版设计文档 §5.2 FileDictAllocator。

### 5.3 Phase B：Prepare（CoordinatorServer → TabletServer）

CoordinatorServer 接收 TieringService 的报告后，发送 **prepare 通知** 给所有相关 bucket 的 TabletServer，携带 `indexUuid`、`readableSnapshotId`、`tieredOffsets`、`readableOffsets`（per-bucket）、`newFileDictEntries` 和 **`oldFiles`**。

#### 处理流程

**Step 0：重置 pending 状态**（与 Iceberg 方案相同）

**Phase 1（无锁 —— 纯远程 I/O）**：

Step 1：**定位 SST**：通过 `indexUuid` 读取跨 bucket 索引文件

Step 2：**下载 SST**：下载 manifest 和 SST 文件到本地

**Phase 2（获取 DvRWLock 写锁）**：

Step 3：获取 DvRWLock 写锁

Step 4：**写入 newFileDictEntries 到 FileDict CF**（与 Iceberg 方案相同）

Step 5：**存储 SST 路径**（不 Ingest）

Step 6：**解析 `oldFiles`**：将 `oldFiles` 中的 file_path 通过 FileDict 转换为 file_id，存储为 `pendingOldFileIds`，供 readable switch 时使用。

Step 7：释放 DvRWLock 写锁

Step 8：发送 **ready ack**

> **与 Iceberg 方案的区别**：不再需要解析 `materializedLakeDv`（步骤 6）。取而代之的是解析 `oldFiles` 为 `pendingOldFileIds`。

### 5.4 Phase C：Publish & Readable Switch

#### Publish DV-Readable

与 Iceberg 方案相同。CoordinatorServer 收集所有 bucket 的 ready ack 后，更新 LakeTableZNode。

#### Readable Switch（TabletServer）

收到 readable switch 通知后，TabletServer 在 DvRWLock 写锁下执行：

**1. Ingest SST → RowPosIndex**

`IngestExternalFile(pendingSstPath, RowPosIndex)`。

与 Iceberg 方案不同的是，Paimon 的 SST 包含**所有** compaction 输出文件中的行映射，因此 Ingest 后 RowPosIndex 完整反映了 compaction 后的最新位置状态。对于被 compaction 重写的行（位置从旧文件移到新文件），新位置自然覆盖旧位置。

**2. Batch resolve PendingDeletes**

与 Iceberg 方案相同的逻辑：

```python
for (R, v) in PendingDeletes:
    hit = RowPosIndex.get(R)
    if hit is not None:
        # Case A: 时间差 —— §4.2 未命中（v == pending），现在位置已知
        # Case B: Compaction 重写 —— §4.2 命中旧位置，SST 包含新位置
        # Case C: "僵尸" —— §4.2 删除了 RowPosIndex[R]，Ingest 又写回
        LakeDv[hit.fileId] |= { hit.pos }
        RowPosIndex.delete(R)
        PendingDeletes.put(R, {hit.fileId, hit.pos})
    else:
        if R < readableOffset[this_bucket]:
            # 孤儿：行已被 tiered 且 compacted，但不在数据文件中
            # （被 logDvSnapshot 过滤，或被 Paimon compaction 中的 DELETE 标记消除）
            PendingDeletes.delete(R)
        else:
            # 行仍在 L0 中未被 compact，或仍在处理中，保留到下一轮
            pass
```

> **使用 readableOffset 而非 tieredOffset 判断孤儿**：如果 `R < tieredOffset` 但 `R >= readableOffset`，说明该行已写入 Paimon 但仍在 L0 中（尚未被 compact 到 base file）。此时该行的位置不稳定，不能判定为孤儿。只有 `R < readableOffset` 才说明该行应该在 base file 中 —— 如果 RowPosIndex 中找不到它，才是真正的孤儿（被 DELETE 消除或被 logDvSnapshot 过滤）。

**3. 清理 oldFiles 的 LakeDv 和 PendingDeletes**

这是 Paimon 方案的核心清理步骤，替代了 Iceberg 方案中的 bitmap diff：

```python
for fileId in pendingOldFileIds:
    # 删除旧文件的 LakeDv 条目
    LakeDv.delete(fileId)

# 清理 PendingDeletes 中指向旧文件的条目
for (R, v) in PendingDeletes:
    if v != pending and v.fileId in pendingOldFileIds:
        PendingDeletes.delete(R)
```

> **为什么基于文件生命周期清理是正确的**：当 compaction 替换旧文件时，旧文件中的所有行要么被迁移到新文件（位置变更），要么被删除（DELETE 标记合并）。
> - **迁移的行**：batch resolve（步骤 2）已经通过 RowPosIndex 发现了新位置，并为新文件创建了 LakeDv 条目。旧文件的 LakeDv 条目可以安全删除。
> - **被删除的行**：行不再存在于任何文件中，旧文件的 LakeDv 条目自然可以删除。

> **顺序很重要**：必须先执行 batch resolve（步骤 2），再执行 oldFiles 清理（步骤 3）。batch resolve 会将旧文件位置的删除标记迁移到新文件位置。如果先清理旧文件，batch resolve 中 PendingDeletes 引用的旧位置信息会丢失。

**4. 清理过期 LogDv**

与 Iceberg 方案相同。

**5. 更新 readableSnapshotId 和 snapshotStartLogOffset**

- `readableSnapshotId` = 报告中的 `readableSnapshotId`（COMPACT snapshot id）
- `snapshotStartLogOffset` = 本 bucket 的 `readableOffset`（per-bucket，由 Phase A3 Step 2 计算）

> **snapshotStartLogOffset 使用 readableOffset 而非 tieredOffset**：union read 时，客户端从 `snapshotStartLogOffset` 开始读取 changelog 以获取未下沉的增量数据。如果使用 tieredOffset，会跳过 L0 中尚未 compact 的数据（这些数据在 Paimon readable snapshot 中不可见），导致数据丢失。使用 readableOffset 确保只跳过已经在 base file 中可见的数据。

**6. 清理 pendingSstPath，清空 pendingOldFileIds。释放 DvRWLock 写锁。发送 switched ack**。

### 5.5 首次引导

与 Iceberg 方案类似：

- TieringService 写入数据到 Paimon L0（Phase A1）
- 等待 compaction 完成（Phase A2，无论自行触发或外部完成）
- 扫描 compaction 输出文件，生成 SST，上传，报告（Phase A3）
- CoordinatorServer 发送 prepare，TabletServer 下载 SST
- CoordinatorServer 发布 S1，发送 readable switch
- TabletServer Ingest SST → RowPosIndex。PendingDeletes 为空，batch resolve 为空操作
- oldFiles 为空（首次 tiering 无旧文件需要清理）

---

## 6. Union Read

与 Iceberg 方案完全相同。详见 Iceberg 版设计文档 §6。

客户端处理流程：

1. 应用 Paimon DV（compaction 产生的物理 DV）
2. 应用 LakeDv（TabletServer 返回的逻辑 DV）
3. 读取 Paimon 中存活的行
4. 获取 `[snapshot_start_offset, logEndOffset]` changelog，应用 LogDv
5. 合并结果

---

## 7. LakeDv 清理机制

### 与 Iceberg 方案的对比

| 维度 | Iceberg 方案 | Paimon 方案 |
|------|-------------|-------------|
| **清理触发** | 每轮 tiering 提交后 | 每轮 compaction 替换文件后 |
| **清理方法** | bitmap diff (`LakeDv AND NOT materializedLakeDv`) | 文件生命周期（删除 oldFiles 的 LakeDv 条目） |
| **需要的信息** | `materializedLakeDv`（从 TieringService 报告） | `oldFiles`（从 TieringService 报告） |
| **增量安全性** | bitmap diff 保留 snapshot 后新增的 bit | 文件级删除天然安全 —— 新增的 bit 指向新文件，不受旧文件删除影响 |

### 正确性论证

**场景**：`file_A` 有 `LakeDv = {pos0, pos2}`，compaction 将 `file_A` 重写为 `file_B`。

1. **file_A 的 pos0, pos2 是已删除行**：这些行的 DELETE 标记已写入 Paimon。compaction 合并后，这些行不出现在 `file_B` 中。`LakeDv[file_A]` 被删除，正确。

2. **file_A 中存活行移到 file_B**：这些行在 `file_B` 中有新位置。SST 扫描捕获了 `RowId → (file_B, new_pos)` 映射。如果后续这些行被删除，§4.2 会正确地标记 `LakeDv[file_B]`。

3. **在 compaction 后、readable switch 前，新删除到达**：
   - 新删除的 §4.2 点查 RowPosIndex → 旧位置（SST 未 Ingest）
   - 标记 `LakeDv[file_A] += {pos1}`
   - Readable switch 时：batch resolve 发现 `RowPosIndex[rowId] = (file_B, pos_x)`（Ingest 后）→ 标记 `LakeDv[file_B] += {pos_x}`
   - 随后清理 `LakeDv[file_A]`（包括 pos1）
   - 结果：`LakeDv[file_B]` 正确包含新位置的删除标记 ✓

4. **compaction 不涉及某文件**：该文件的 LakeDv 条目保持不变。存活行的位置不变，删除标记继续对 union read 生效。未来 compaction 涉及该文件时再清理。

### 冗余 LakeDv 条目

Paimon 方案中，LakeDv 不会产生 Iceberg 方案中 Appendix C 描述的"冗余已物化条目"问题。因为：

- Iceberg 方案：LakeDv 物化为 Puffin DV 后，如果物化条目未被 bitmap diff 清理，就会冗余
- Paimon 方案：LakeDv 清理是文件级别的 —— 一旦文件被 compaction 替换，其所有 LakeDv 条目（无论是否"物化"）都被删除，不存在部分清理的问题

---

## 8. Compaction 场景分析

在 Paimon 方案中，compaction 不是"外部操作"（如 Iceberg 方案的 §8），而是**核心路径**。并且 compaction 的执行方是灵活的 —— 可能由 TieringService 自行触发，也可能由独立的外部 compact job 完成。本节分析不同场景的处理方式。

### 8.1 场景一：TieringService 自行触发 Compaction

TieringService 写入 L0 后显式触发 full compaction 并等待完成。

**流程**：§5.2 Phase A1 → TieringService 触发 compaction → 从 `CompactResult` 直接获取 newFiles/oldFiles → §5.2.2 Phase A3 扫描 + SST 生成。

**优点**：流程简单，TieringService 完全控制时间线，无需轮询等待。

**注意**：即使 TieringService 自行触发 compaction，也可能同时存在外部 compact job 对历史数据的 compaction。Phase A3 的 snapshot diff（§5.2.2 Step 2）会统一捕获所有文件变更，包括这些并发的外部 compaction。

### 8.2 场景二：外部 Compact Job 执行 Compaction

一个独立的 Paimon compact job（例如 Flink compact job、周期性 compact 任务、或 Paimon 的 auto-compaction 机制）负责所有 compaction 工作。TieringService 仅负责写入 L0。

**流程**：

1. TieringService 执行 §5.2 Phase A1，写入 L0，提交 Paimon snapshot
2. 外部 compact job 按其策略执行 compaction（可能在写入后的若干秒到若干分钟内完成）
3. TieringService 轮询检测 compaction 完成（§5.2.2 Phase A3 Step 1）
4. 一旦确认本轮 L0 文件已被 compaction 消费，执行 §5.2.2 Phase A3 后续步骤

**等待策略**：TieringService 以可配置的间隔（默认建议 1-5 秒）轮询 Paimon snapshot。通过检查写入时产生的 L0 文件是否出现在后续 COMPACT snapshot 的 removedFiles 中来判断 compaction 是否完成。

**超时处理**：如果超过配置的最大等待时间（例如 10 分钟），TieringService 记录警告并可选择：
- 继续等待
- 自行触发一次 compaction 作为 fallback

### 8.3 场景三：混合模式

实际生产环境中可能出现混合场景：

- TieringService 触发了 compaction，但同时外部 compact job 也在执行 compaction
- 外部 compact job 处理了 TieringService 写入的部分 L0 文件
- TieringService 触发的 compaction 处理了剩余的 L0 文件

**处理**：Phase A3 的 snapshot diff 机制天然支持混合模式 —— 它收集从 lastReadableSnapshot 到当前 snapshot 之间的**所有** COMPACT snapshot 的文件变更，不区分 compaction 来源。

### 8.4 未完成的 L0 Compaction

在某些情况下，compaction 可能未处理所有 L0 文件（partial compaction）。

**问题**：如果本轮写入的 L0 文件未被 compaction 消费，这些行的位置未知，无法建立完整的 RowId → FilePos 映射。

**处理策略**：

1. **自行触发模式**：使用 `fullCompaction = true`，确保所有 L0 文件被处理。如果 full compaction 后仍有 L0 文件存在（理论上不应发生），重试 compaction。
2. **外部 compaction 模式**：持续等待直到本轮 L0 文件全部出现在某个 COMPACT snapshot 的 removedFiles 中。

> **不允许跳过未 compact 的 L0 文件**：RowPosIndex 必须包含所有已 tiered 行的位置信息。如果跳过某些 L0 行，后续这些行被 `-U/-D` 删除时，§4.2 无法在 RowPosIndex 中找到位置，只能写 `pending` 到 PendingDeletes。当 compaction 最终处理这些 L0 文件时，batch resolve 可以补救，但这会引入额外的延迟和复杂性。因此，Phase A3 必须等待所有 L0 文件被 compaction 消费后才继续。

### 8.5 Paimon Snapshot 过期

与 Iceberg 方案的 §8.4 类似。Paimon snapshot 过期策略必须保留当前 readable snapshot 及其引用的所有数据文件。

---

## 9. 故障恢复

### 9.1 TieringService 故障

由于 Paimon 方案的 Phase A 被拆分为三个子阶段（A1 写入 / A2 compaction / A3 扫描+SST），故障恢复需要根据故障点区分：

| 故障点 | Phase | Paimon 状态 | SST 状态 | 恢复策略 |
|--------|-------|------------|---------|---------|
| 写入 Paimon 前 | A1 | 无变化 | 无 | **完全重试**：从头执行 A1 |
| 写入后，提交前 | A1 | 部分 L0 文件 | 无 | **完全重试**：Paimon 通过 `commitIdentifier` 去重，重复写入幂等安全 |
| 提交后，compaction 前 | A2 | L0 文件已提交 | 无 | **从 A2 恢复**：触发 compaction（或等待外部 compaction），然后继续 A3 |
| Compaction 后，SST 生成前 | A3 | 数据已 compact | 无 | **从 A3 恢复**：通过 Paimon snapshot diff 重新检测 compaction 结果，扫描新文件生成 SST |
| SST 上传后，报告前 | A3 | 数据已 compact | 已上传 | **元数据调和**：读取 Paimon snapshot，重建报告信息 |
| 报告成功后 | - | 数据已 compact | 已上传 | CoordinatorServer 驱动 Prepare → Switch |

> **Paimon 写入的幂等性**：Paimon 的 `ManifestCommittable` 使用 `commitIdentifier` 进行去重。相同的 `commitIdentifier` 重复提交是幂等的。TieringService 重启后使用相同的 `commitIdentifier` 重新写入是安全的。

> **Compaction 后的 SST 重建**：如果 compaction 已完成但 SST 未生成，TieringService 可以通过 Paimon snapshot diff 确定 compaction 产生的新文件，重新扫描生成 SST。snapshot diff 是确定性的，重建结果与首次生成一致。

> **外部 compaction 模式下的恢复**：如果 TieringService 在等待外部 compaction 期间故障，重启后重新检测 Paimon snapshot。如果外部 compaction 已完成，直接从 A3 继续；如果未完成，继续等待。

### 9.2 TabletServer 故障

#### DvRocksDB Checkpoint

与 Iceberg 方案相同。DvRocksDB 定期 checkpoint，记录 `restoreSnapshot`、`snapshotStartLogOffset`、`checkpointLogHw`。

#### 恢复步骤

1. 从远程存储拉取 SST 文件，加载 DvRocksDB。RowPosIndex 反映 `restoreSnapshot` 状态。

2. 从 `checkpointLogHw + 1` 开始重放 changelog。

3. 对每个 `-U` / `-D` 记录，处理逻辑与 Iceberg 方案 §9.2 相同。

4. **处理 checkpoint 后已完成 readable switch 的 snapshot**：

   与 Iceberg 方案相同：查询 CoordinatorServer 获取当前 DV-readable snapshot，按序下载和 Ingest 中间 snapshot 的远程 SST，重建 RowPosIndex。

   **关键区别**：在恢复阶段跳过 oldFiles LakeDv 清理（因为没有 `pendingOldFileIds` 信息）。LakeDv 可能保留冗余条目（指向已被 compaction 替换的旧文件）。这不影响正确性 —— union read 中双重标记是幂等的。冗余条目在下一轮正常 tiering 时被清理。

   > **冗余条目的消除**：下一轮 tiering 的 compaction 会再次替换这些文件（或文件已不存在）。readable switch 的 oldFiles 清理会删除这些冗余的 LakeDv 条目。

### 9.3 CoordinatorServer 故障

与 Iceberg 方案完全相同。CoordinatorServer 的恢复完全由 LakeTableZNode 的状态决定。详见 Iceberg 版设计文档 §9.3。

### 9.4 顺序与幂等性

与 Iceberg 方案相同。Prepare 和 readable switch 都是幂等的。详见 Iceberg 版设计文档 §9.4。

---

## 10. 数据格式与协议变更

### 10.1 KV State Value 格式

与 Iceberg 方案相同。详见 Iceberg 版设计文档 §10.1。

### 10.2 Changelog 格式扩展

与 Iceberg 方案相同。详见 Iceberg 版设计文档 §10.2。

### 10.3 Paimon 数据列扩展

当 tiering 写入 Paimon 数据文件时，需要在用户列之外包含以下系统列：

- **`__rowid`**：Fluss RowId（`+I`/`+U` 的 log offset）。Compaction 后扫描此列建立 RowId → FilePos 映射。Paimon compaction 在重写文件时**必须保留此列及其值**。

> **不需要 `__bucket` 列**：与 Iceberg 不同，Paimon 的 compaction 在单个 partition-bucket 内执行，不会跨 bucket 合并文件。因此 bucket 信息可以从文件元数据（路径或 ManifestEntry）中获取，无需额外存储 `__bucket` 列。

> **`__rowid` 与 Paimon 的 `_ROW_ID`**：Paimon 自身有 `_ROW_ID` 系统字段（`SpecialFields.ROW_ID`），用于 row tracking。Fluss 的 `__rowid` 是独立的，语义不同（Fluss RowId = changelog log offset，Paimon RowId = 文件内累计行号）。两者需要共存。

### 10.4 Paimon 表配置

- **Merge Engine**：使用 `DEDUPLICATE` merge engine（默认），按 primary key 去重
- **Changelog Producer**：TieringService 仅需写入 `ADD` 和 `DELETE` 记录，Paimon 的 changelog producer 设置不影响 DV 逻辑
- **Compaction 策略**：
  - **自行 compaction 模式**：TieringService 在写入后触发 full compaction，确保 L0 数据立即合并到底层
  - **外部 compaction 模式**：配置独立的 compact job，确保其执行频率与 tiering 频率匹配，避免 L0 文件积压导致 TieringService 长时间等待

### 10.5 前置条件：FULL Changelog 模式

与 Iceberg 方案相同。DV 要求主键表使用 **FULL changelog mode**。详见 Iceberg 版设计文档 §10.5。

---

## 11. 总结

| 维度 | Paimon 方案 | 与 Iceberg 方案的差异 |
|------|------------|---------------------|
| **RowId** | 使用 `+I`/`+U` log offset | 无变化 |
| **RowPosIndex** | SST 由 TieringService 在 compaction 后扫描输出文件生成；compaction 可自行触发或由外部 compact job 完成 | Iceberg 在写入时即知位置；Paimon 需等待 compaction |
| **LakeDv** | 基于文件生命周期清理（oldFiles 删除） | Iceberg 使用 bitmap diff（materializedLakeDv） |
| **LogDv** | 范围式 bitmap | 无变化 |
| **DV 物化** | 不需要显式写 DV 文件；写 DELETE 记录到 Paimon，由 compaction 处理 | Iceberg 需生成 Puffin DV 文件 |
| **Tiering Split** | 不含 lakeDvSnapshot | Iceberg split 包含 lakeDvSnapshot |
| **Readable Snapshot** | 仅 COMPACT snapshot 才是 readable 的；APPEND snapshot 不触发 prepare/switch | Iceberg 每个 snapshot 都触发 |
| **Offset 语义** | per-bucket 的 tieredOffset 和 readableOffset（readableOffset ≤ tieredOffset） | 全局 currentTieredOffset |
| **SST 内容** | 包含所有 compaction 输出文件中的行映射 | Iceberg SST 仅包含本轮新写入行 |
| **Compaction 处理** | 核心路径；支持 TieringService 自行触发或外部 compact job 执行；Phase A3 统一检测和处理 | Iceberg 中仅处理外部 compaction（例外情况） |
| **oldFiles 清理** | 每轮 readable switch 核心步骤 | Iceberg 中仅在外部 compaction 时需要 |
| **报告内容** | 包含 oldFiles、per-bucket readableOffsets、tieredOffsets，不含 materializedLakeDv | Iceberg 包含 materializedLakeDv，不含 oldFiles |
| **`__bucket` 列** | 不需要（Paimon compaction 不跨 bucket） | Iceberg 需要（外部 compaction 可能跨 bucket） |
| **Paimon 表配置** | DEDUPLICATE merge engine；compaction 可自行触发或外部管理 | N/A |
| **存储** | DvRocksDB 五个 CF（与 Iceberg 相同） | 无变化 |
| **并发控制** | DvRWLock 读写锁 | 无变化 |
| **Union Read** | 三层 DV 应用 | 无变化 |
| **Recovery** | 恢复时跳过 oldFiles LakeDv 清理；冗余条目下一轮消除 | Iceberg 恢复时跳过 bitmap diff；冗余条目下一轮消除 |
| **前置条件** | FULL changelog 模式 | 无变化 |

---

## Appendix A：端到端 Walkthrough

### 初始状态

| 组件 | 状态 |
|------|------|
| Paimon | 无数据文件 |
| RowPosIndex | 空 |
| LakeDv / LogDv / PendingDeletes | 空 |
| readableSnapshotId | 无 |

---

### Step 1：写入 3 条记录

```
PUT (key1, v1)  → +I (offset=0)  → RowId=0
PUT (key2, v2)  → +I (offset=1)  → RowId=1
PUT (key3, v3)  → +I (offset=2)  → RowId=2
```

KV state 存储 RowId：`key1→[rowId=0][v1]`，`key2→[rowId=1][v2]`，`key3→[rowId=2][v3]`。

DV 状态无变化。

---

### Step 2：首轮 Tiering

**Split 生成**：`offset_range: [0, 2]`，`logDvSnapshot: empty`

**Phase A1（写入）**：

1. 读取 changelog，写入 Paimon L0：
   - `+I(key1, v1, __rowid=0)` → ADD
   - `+I(key2, v2, __rowid=1)` → ADD
   - `+I(key3, v3, __rowid=2)` → ADD

2. 提交 Paimon snapshot S_write

**Phase A2（compaction）**：full compaction 完成（无论自行触发或外部完成）→ Paimon snapshot S_compact

**Phase A3（扫描 + SST）**：

Compaction 结果：L0 文件 → file_A（L1）

   | file_A | row_pos | __rowid | key | value |
   |--------|---------|---------|-----|-------|
   | pos0 | 0 | 0 | key1 | v1 |
   | pos1 | 1 | 1 | key2 | v2 |
   | pos2 | 2 | 2 | key3 | v3 |

5. 扫描 file_A → SST: `{0→(1,pos0), 1→(1,pos1), 2→(1,pos2)}`（fileId=1 对应 file_A）

6. 上传 SST，报告：`readableSnapshotId=S_compact`, `readableOffsets={bucket0: 2}`, `oldFiles=[]`
   - 全表只有一个 bucket，compaction 后无 L0 → readableOffset = tieredOffset = 2

**Prepare → Publish → Readable Switch**：

- TabletServer 下载 SST，Ingest → RowPosIndex
- PendingDeletes 空，batch resolve 无操作
- oldFiles 空，无清理
- 更新 readableSnapshotId = S_compact，snapshotStartLogOffset = readableOffset = 2

| 组件 | Readable Switch 后 |
|------|-------------------|
| RowPosIndex | `0→(file_A,pos0)`, `1→(file_A,pos1)`, `2→(file_A,pos2)` |
| LakeDv | 空 |
| PendingDeletes | 空 |

---

### Step 3：更新 key1

```
PUT (key1, v4)  → -U (offset=3, oldRowId=0) + +U (offset=4)  → new RowId=4
```

**§4.2 删除处理**：
- `RowPosIndex[0]` → hit `(file_A, pos0)`
- LakeDv: `file_A → {0}`
- 删除 `RowPosIndex[0]`
- PendingDeletes: `0 → (fileId=1, pos=0)`
- LogDv: offset=0 标记为已删除

| 组件 | 状态 |
|------|------|
| RowPosIndex | `1→(file_A,pos1)`, `2→(file_A,pos2)` |
| LakeDv | `file_A → {0}` |
| PendingDeletes | `0 → (1, pos0)` |

---

### Step 4：Union Read（snapshot S1）

TabletServer 返回：`lakeDv = {file_A: {0}}`，`logDv = {offset 0 deleted}`，`logEndOffset = 4`

| 来源 | 数据 | DV 应用 | 结果 |
|------|------|---------|------|
| Paimon file_A | pos0(key1,v1), pos1(key2,v2), pos2(key3,v3) | lakeDv 屏蔽 pos0 | key2=v2, key3=v3 |
| Changelog [3,4] | offset=3: `-U`, offset=4: `+U(key1,v4)` | logDv: offset 0 不在范围内 | key1=v4 |

**最终结果**：`(key1, v4), (key2, v2), (key3, v3)` ✓

---

### Step 5：删除 key3

```
DELETE (key3)  → -D (offset=5, oldRowId=2)
```

**§4.2**：
- `RowPosIndex[2]` → hit `(file_A, pos2)`
- LakeDv: `file_A → {0, 2}`
- PendingDeletes: `2 → (1, pos2)`

---

### Step 6：第二轮 Tiering

**Split 生成**：`offset_range: [3, 5]`，`logDvSnapshot: empty`

**Phase A1（写入）**：

1. 读取 changelog，写入 Paimon L0：
   - offset=3: `-U` → 跳过（-U 不写 Paimon）
   - offset=4: `+U(key1, v4, __rowid=4)` → ADD(key1, v4)
   - offset=5: `-D(key3)` → DELETE(key3)

**Phase A2（compaction）**：full compaction 完成（自行触发或外部完成）：
   - L0（包含 ADD(key1,v4) 和 DELETE(key3)）+ file_A 合并
   - key1: L0 的 ADD(seq=4) > file_A 的 ADD(seq=0) → 保留 v4
   - key2: 无新版本 → 保留 v2
   - key3: DELETE(seq=5) > ADD(seq=2) → 删除
   - 输出：file_B

   | file_B | row_pos | __rowid | key | value |
   |--------|---------|---------|-----|-------|
   | pos0 | 0 | 4 | key1 | v4 |
   | pos1 | 1 | 1 | key2 | v2 |

**Phase A3（扫描 + SST）**：

3. oldFiles = [file_A]，newFiles = [file_B]

4. 扫描 file_B → SST: `{4→(2,pos0), 1→(2,pos1)}`（fileId=2 对应 file_B）

5. 报告：`indexUuid`, `oldFiles=[file_A]`

**Readable Switch**：

- Ingest SST → RowPosIndex: `{4→(file_B,pos0), 1→(file_B,pos1)}`
- Batch resolve PendingDeletes:
  - `PendingDeletes[0] = (1, pos0)` → `RowPosIndex.get(0)` = miss → `0 < currentTieredOffset(5)` → 孤儿 → 删除
  - `PendingDeletes[2] = (1, pos2)` → `RowPosIndex.get(2)` = miss → `2 < currentTieredOffset(5)` → 孤儿 → 删除
- 清理 oldFiles：`LakeDv.delete(file_A)` → LakeDv 变为空 ✓
- 清理 PendingDeletes：PendingDeletes[0] 和 [2] 已在 batch resolve 中删除

| 组件 | Readable Switch 后 |
|------|-------------------|
| RowPosIndex | `4→(file_B,pos0)`, `1→(file_B,pos1)` |
| LakeDv | 空 ✓ |
| PendingDeletes | 空 ✓ |

---

### Step 7：新写入 + Union Read（S2）

```
UPDATE key2 → -U (offset=6, oldRowId=1) + +U (offset=7, key2, v5)
INSERT key4 → +I (offset=8, key4, v6)
```

**§4.2 处理 offset=6 的 -U(oldRowId=1)**：
- `RowPosIndex[1]` → hit `(file_B, pos1)`
- LakeDv: `file_B → {1}`
- 删除 `RowPosIndex[1]`
- PendingDeletes: `1 → (fileId=2, pos=1)`

**Union Read（snapshot S2）**：

TabletServer 返回：`lakeDv = {file_B: {1}}`，`logDv = {offset 1 deleted}`，`logEndOffset = 8`

| 来源 | 数据 | DV 应用 | 结果 |
|------|------|---------|------|
| Paimon file_B | pos0(key1,v4), pos1(key2,v2) | Paimon DV：无；lakeDv 屏蔽 pos1 | key1=v4 |
| Changelog [6,8] | offset=6: `-U`, offset=7: `+U(key2,v5)`, offset=8: `+I(key4,v6)` | logDv: offset 1 不在范围内 | key2=v5, key4=v6 |

**最终结果**：`(key1, v4), (key2, v5), (key4, v6)` ✓

*三层 DV 协作：Paimon 物理 DV（本例中无）处理历史删除，LakeDv 屏蔽 file_B 中未物化的新删除，changelog + LogDv 提供未下沉的增量数据。*

---

## Appendix B：Compaction 位置变更的正确性论证

### 问题

当 Paimon compaction 将 file_A 重写为 file_B 时，file_A 中行的位置发生了变化。Fluss 必须正确追踪这些位置变更，确保 LakeDv 指向正确的文件和位置。

### 证明

考虑 file_A 中的一行，RowId=R，位于 pos=P。

**Case 1：行存活（未被删除）**

- file_A compaction 后，行出现在 file_B 的 pos=Q
- SST 扫描：`R → (file_B, Q)`
- Ingest 后 RowPosIndex：`R → (file_B, Q)`
- 如果后续 `-U/-D` 删除该行，§4.2 正确查到 `(file_B, Q)` 并标记 `LakeDv[file_B]`

**Case 2：行在 §4.2 中被删除，compaction 前**

- §4.2：`RowPosIndex[R]` = `(file_A, P)` → hit → `LakeDv[file_A] += {P}`，删除 `RowPosIndex[R]`，`PendingDeletes[R] = (file_A, P)`
- Compaction 后，行仍出现在 file_B（因为 DELETE 尚未写入 Paimon）
- SST 扫描：`R → (file_B, Q)`
- Ingest：`RowPosIndex[R] = (file_B, Q)`（"僵尸"恢复）
- Batch resolve：`PendingDeletes[R]` 存在 + `RowPosIndex.get(R)` = `(file_B, Q)` → hit
  - `LakeDv[file_B] += {Q}` ✓
  - 删除 `RowPosIndex[R]`
- 清理 oldFiles：`LakeDv[file_A]` 被删除（包含旧的 {P}）
- **结果**：LakeDv 正确地从 `file_A:P` 迁移到 `file_B:Q` ✓

**Case 3：行在 §4.2 中被删除，DELETE 已写入 Paimon**

- §4.2：同 Case 2
- 本轮 tiering 写入 DELETE(key) 到 Paimon
- Compaction 合并 DELETE → 行**不**出现在 file_B
- SST 扫描：不含 R
- Ingest：RowPosIndex 无 R 条目
- Batch resolve：`PendingDeletes[R]` 存在 + `RowPosIndex.get(R)` = miss → `R < currentTieredOffset` → 孤儿 → 删除
- 清理 oldFiles：`LakeDv[file_A]` 被删除
- **结果**：行已从 Paimon 中物理删除，LakeDv 也被清理 ✓

**Case 4：行在 compaction 后、Ingest 前被 §4.2 删除（时间差）**

- SST 已生成，包含 `R → (file_B, Q)`，但 SST 未 Ingest
- §4.2：`RowPosIndex[R]` = miss（之前已删除或旧条目）→ `PendingDeletes[R] = pending`
- Ingest：`RowPosIndex[R] = (file_B, Q)`
- Batch resolve：`PendingDeletes[R] = pending` + `RowPosIndex.get(R)` = `(file_B, Q)` → hit
  - `LakeDv[file_B] += {Q}` ✓
- **结果**：时间差通过 PendingDeletes 的 pending 标记 + batch resolve 正确处理 ✓

---

## Appendix C：文件路径约定

```
{$remoteLakeTableSnapshotDir}/
├── metadata/
│   └── {UUID}.offsets              ← 现有
└── rowPos/
    ├── {bucketId}/{uuid}/          ← 每 bucket 的 SST 目录
    │   ├── manifest
    │   ├── sst_0.sst
    │   └── sst_1.sst
    └── {indexUuid}                 ← 跨 bucket 索引文件
```

与 Iceberg 方案相同。
