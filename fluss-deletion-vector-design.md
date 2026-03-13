# Fluss Deletion Vector 详细设计文档

## 1. 背景与动机

Streamhouse 架构下，Fluss 作为实时层，Iceberg 作为历史层。Fluss 通过 lake tiering 将实时数据持续同步到 Iceberg，并提供 **union read** 能力——将 Fluss 热层中尚未 tiering 的增量数据与 Iceberg 中的历史数据联合查询，对外呈现为一张完整的、数据不重不丢的表。

本方案要解决两个问题：

### 问题 1：Union Read 的跨层数据去重

对于主键表，更新和删除首先到达 Fluss，但同一行的旧版本可能已经 tiering 到 Iceberg 中。联合查询时，系统必须精确屏蔽 Iceberg 中那些已在 Fluss 侧被更新或删除的行，否则旧行会从历史层重新出现，破坏 exactly-once 语义。

当前没有这种跨层实时去重的机制。两轮 tiering 之间到达的删除和更新，无法实时反映到 union read 中——要么读到已删除的旧行（数据重复），要么需要 client 在内存中全量 merge（性能差）。

### 问题 2：Iceberg Equality Delete 的性能劣化

当前 tiering 写入 Iceberg 时，DELETE 和 UPDATE_BEFORE 通过 Iceberg v2 的 **equality delete** 处理。这个方案存在以下缺陷：

- **小文件累积**：每轮 tiering 都会产生 equality delete 文件，随时间不断堆积。
- **读取合并开销大**：查询引擎需要将 equality delete 应用到所有历史 data file 上，读取性能持续劣化。
- **元数据膨胀**：manifest 条目随 delete 文件数量线性增长。

### 本方案

引入三层 **Deletion Vector** 同时解决以上两个问题：

1. **解决 union read 去重**：在 Fluss TabletServer 侧维护轻量的逻辑删除标记（Lake Deletion Vector + Log Deletion Vector），使 union read 能够**实时**屏蔽 Iceberg 和 WAL 中已被更新或删除的行，无需等待下一轮 tiering commit，实现 exactly-once 联合查询语义。
2. **替代 equality delete**：tiering 写入 Iceberg 时，使用 Iceberg v3 的 position delete 机制（Puffin 文件中的 RoaringBitmap，精确标记被删除行的 row position）完全替代 equality delete，消除小文件累积和读取性能劣化问题。

---

## 2. 整体架构：三层 Deletion Vector

```
              Fluss (实时层)                    Iceberg (历史层)
  ┌───────────────────────────────┐     ┌──────────────────────────┐
  │  WAL (changelog)              │     │  Data Files (Parquet)    │
  │  ┌─────────────────────────┐  │     │                          │
  │  │  Log Deletion Vector    │  │     │  ┌────────────────────┐  │
  │  │  (热层内的删除/更新追踪) │ │  │     │  Iceberg Deletion     │  │
  │  └─────────────────────────┘  │     │  │ Vector (Puffin)    │  │
  │                               │     │  └────────────────────┘  │
  │  ┌─────────────────────────┐  │     │                          │
  │  │  Lake Deletion Vector   │──┼────►│  下一轮 tiering 时物化     │
  │  │  (跨层逻辑删除标记)        │  │     │                          │
  │  └─────────────────────────┘  │     │                          │
  └───────────────────────────────┘     └──────────────────────────┘
```

### 2.1 Iceberg Deletion Vector（第一层）

标准的 Iceberg v3 deletion vector。Fluss Tiering Writer 写入 Iceberg 时，将删除操作物化为 **Puffin 文件**，其中包含 RoaringBitmap，精确指向 data file 中被删除行的 row position。完全替代 equality delete。

### 2.2 Log Deletion Vector（第二层）

追踪 Fluss 实时 changelog（WAL）中的删除和更新。仅作用于仍在热层中、尚未 tiering 到 Iceberg 的数据。

当一轮 tiering 完成后，新到达的 DELETE 和 UPDATE 记录会持续写入 WAL。这些变更对应的旧行可能存在于两个位置：同在 WAL 中的更早记录，或者已经 tiering 到 Iceberg 的历史数据。Log Deletion Vector 负责前者——标记 WAL 内部已被后续操作覆盖或删除的行，确保联合查询时不会读到 WAL 中已过时的版本。后者（旧行已在 Iceberg 中）则由 Lake Deletion Vector 负责。

### 2.3 Lake Deletion Vector（第三层）

连接实时层与历史层的桥梁。当 Fluss 收到一条针对已 tiering 到 Iceberg 的行的删除或更新时：

- TabletServer 在 LakeDv 中记录逻辑删除标记（datafile → 被删除的 row position bitmap）。
- 该逻辑删除在联合查询（union read）时**立即生效**，无需等待下一次 Iceberg snapshot 写入。
- 这些逻辑删除会在下一轮 tiering commit 时，由 Tiering Writer 物化为 Iceberg 中的物理 deletion vector（Puffin 文件）。

### 2.4 联合查询语义

联合查询（Fluss 热数据 + Iceberg 历史数据）时，查询引擎同时应用三层 deletion vector：

- **Iceberg Deletion Vector**：屏蔽 Iceberg 中已物化的删除行。
- **Lake Deletion Vector**：屏蔽 Iceberg 中已在 Fluss 侧逻辑删除但尚未物化的行。
- **Log Deletion Vector**：屏蔽热层 WAL 中已被后续操作覆盖或删除的行。

三层协作确保正确的 upsert 语义：UPDATE 产生最新值，DELETE 彻底移除该行，无论原始数据位于哪一层。

---

## 3. 核心概念定义

### 3.1 RowId

**定义**：RowId 唯一标识一条 KV 数据的**某个版本**，而不是 primary key。同一个 key 的不同版本有不同的 RowId。

**取值**：使用该数据对应的 changelog 中 `INSERT (+I)` 或 `UPDATE_AFTER (+U)` 记录的 **log offset** 作为 RowId。

**示例**：

```
------KV------                ------LOG------
PUT (key1, v1)    ==>   +I  (offset=0, key1, v1)           => RowId = 0  （第一个版本）
PUT (key1, v2)    ==>   -U  (offset=1, key1, v1)           => 引用 RowId = 0（要删除的旧版本）
                        +U  (offset=2, key1, v2)           => RowId = 2  （第二个版本）
DELETE (key1)     ==>   -D  (offset=3, key1, v2)           => 引用 RowId = 2（要删除的旧版本）
```

**各组件中 RowId 的对应关系**：

- **`+I`/`+U` changelog**：RowId = 该记录自身的 log offset，写入时自动确定。
- **`-U`/`-D` changelog**：RowId = 被删除版本的 log offset，从 KV state 旧 value 尾部提取。
- **KV state (RocksDB)**：RowId = 当前版本的 log offset，写入时追加到 value 首部（8 bytes）。

### 3.2 FilePos

标记一条数据在 Iceberg 中的物理位置，由两部分组成：

- **file_id**：data file 的字典编码 ID（int 类型，非原始文件路径）
- **row_position**：数据在该文件中的行号（从 0 开始）

两者合并为一个 8 bytes 的值：高 4 bytes 为 file_id，低 4 bytes 为 row_position。

### 3.3 RowPosIndex

RowId 到 FilePos 的映射，用于根据 RowId 快速定位一行数据在 Iceberg 最新快照中的物理位置。

**关键设计决策：只存最新快照的 FilePos。**

| RowId  | FilePos           |
|--------|-------------------|
| rowId1 | `{file_B, pos3}`  |
| rowId2 | `{file_C, pos10}` |

之所以只存最新快照而不存多个快照的位置，原因如下：

1. Tiering commit 时，changelog 中的删除已经物化为 Iceberg DV（Puffin 文件）。任何快照中，早于 tiered offset 的删除已由 Iceberg 自身处理。
2. LakeDv 只覆盖 tiered offset 之后的新删除——这些新删除针对的是最新快照中的文件。
3. Union read 读的是最新的 DV-readable snapshot，不需要为老快照维护 LakeDv。

因此每行只需存一个 FilePos（8 bytes），而不是一个 FilePosList。

**存储方案**：

- 保存在 DvRocksDB 中（独立于 KvTablet 的 RocksDB）。
- data file 文件名包含 UUID，是很长的字符串，因此需要进行 **dictionary 编码**，将文件名转成 int 类型。编码映射关系记录在 DvRocksDB 的另一个列族中。

### 3.4 LogDv

标记 Fluss changelog（WAL）中已被删除或覆盖的记录。在 union read 场景中，client 需要读出 `[log_startOffset, log_endOffset]` 这段 changelog 数据。通过 LogDv，client 可以知道这段数据中哪些记录可以被跳过，避免将整段数据全部读出再在内存中 merge。

**数据结构**：

| offset_range        | del_bitmap  |
|---------------------|-------------|
| offset0 ~ offset9   | `bin{1}`    |
| offset10 ~ offset20 | `bin{2, 5}` |
| offset21 ~ offset30 | `bin{1, 4}` |

Key 是 changelog offset 的 range（固定间隔），value 是该 range 内被删除记录的 bitmap。

**示例**：

1. INSERT 一条数据 `(key1, v1)`，RowId = 0，offset = 0
2. 之后 append 了一些数据，changelog 的 endOffset 为 5
3. 用 `(key1, v2)` 更新了这条数据，RowId = 6（`+U` 的 offset），offset = 6
4. 处理 `-U(key1, v1)` 时，发现被删除数据的 RowId 为 0，offset 也是 0，找到 offset = 0 所在的 range `offset0 ~ offset9`，更新 bitmap 为 `{1}`，表示该 range 中第一条数据被删掉了

Client 读取时：收到 LogDv `{offset0 ~ offset9: {1}}`，从 offset 0 读到 offset 6，读到 offset 0 时发现其在 LogDv 中，直接跳过。

**生命周期管理**：当数据湖 snapshot advance 后，所有小于数据湖最新 snapshot 对应的 `start_logOffset` 的 `offset_range` 条目都可以清理掉。

### 3.5 LakeDv

标记 Iceberg data file 中已在 Fluss 侧逻辑删除但尚未物化的行。

**数据结构**：

| file_name  | del_bitmap   |
|------------|--------------|
| data_file1 | `bin{3}`     |
| data_file2 | `bin{2, 10}` |

**增量存储**：LakeDv 只保存自上次 tiering commit 以来的新增删除，不存全量。每轮 tiering commit 将 LakeDv 物化为 Iceberg DV（Puffin 文件）后，清空 LakeDv，重新从空开始积累。

```
Tiering commit S2 完成（offset 100）
    │
    │  新的 -D/-U 到达 → LakeDv 逐步积累
    │  （通常只有几分钟的增量，很小）
    │
Tiering commit S3 完成（offset 120）
    │  → S3 的 Iceberg DV 已包含 offset 101~120 的删除
    │  → 清空 LakeDv，重新从空开始
```

由于每轮 tiering 间隔通常只有几分钟，LakeDv 积累的删除量很小，不存在全量 DV 过大的问题。历史删除已物化到 Iceberg DV 中，不需要 TabletServer 维护。

---

## 4. 数据格式变更

### 4.1 KV State Value 格式

在现有的 value 格式最前面插入 8 bytes 的 RowId：

```
之前：[schemaId (2 bytes)][BinaryRow (变长)]
之后：[RowId (8 bytes)][schemaId (2 bytes)][BinaryRow (变长)]
```

RowId 就是写入该条 KV 数据时，对应的 `+I` 或 `+U` changelog 记录的 log offset。

**RowId 放在首部的原因**：当同一个 key 被更新或删除时，需要从旧 value 中提取 RowId。放在首部可以直接读取前 8 bytes，无需解析变长的 BinaryRow 来确定 RowId 的偏移位置。

**写入时机**：KV 数据写入 RocksDB 时，将 RowId 写入 value 首部。

**读取时机**：当同一个 key 被更新或删除时，先读出旧 value，从首部提取旧版本的 RowId，用于：
- 生成 `-U`/`-D` changelog 时携带旧 RowId
- 查 RowPosIndex 定位该行在 Iceberg 中的物理位置
- 更新 LakeDv 和 LogDv

### 4.2 Changelog 格式扩展

`-U` 和 `-D` 记录的 value 中需要携带被删除版本的 RowId。这个 RowId 在 KV state 的旧 value 首部已经存在，生成 changelog 时直接提取即可。

```
之前的 -U value：[schemaId][BinaryRow(旧值)]
之后的 -U value：[RowId(8 bytes)][schemaId][BinaryRow(旧值)]

之前的 -D value：[schemaId][BinaryRow(旧值)]
之后的 -D value：[RowId(8 bytes)][schemaId][BinaryRow(旧值)]
```

RowId 同样放在首部，与 KV state value 格式保持一致。生成 changelog 时，直接将 KV state 中读出的旧 value（已包含首部 RowId）原样写入 `-U`/`-D` 的 value，无需额外拼接。

**使用方**：

- **TabletServer**：changelog 同步成功后处理 `-U`/`-D` 时，从 value 首部提取 RowId，查 RowPosIndex 更新 LakeDv 和 LogDv。
- **Tiering Writer**：读到 `-U`/`-D` 记录时，从 value 首部提取 RowId，批量 RPC 查 TabletServer 的 RowPosIndex，获取 `(file, row_position)`，生成 Puffin DV 文件。

> **注意**：`+I` 和 `+U` 记录的 value 格式不变，仍为 `[schemaId][BinaryRow]`。它们的 RowId 就是自身的 log offset，无需在 value 中额外携带。

### 4.3 Iceberg 版本

从 Iceberg v2 直接切换到 v3。不保留 equality delete 的兼容性。

---

## 5. 存储架构

### 5.1 DvRocksDB

RowPosIndex、LogDv、LakeDv 作为三个不同的列族（Column Family），保存在一个独立的 RocksDB 实例中，记为 **DvRocksDB**。文件路径字典编码保存在第四个列族中。

```
DvRocksDB
├── CF: RowPosIndex    — RowId (8 bytes) → FilePos (8 bytes)
├── CF: LogDv          — offset_range → del_bitmap
├── CF: LakeDv         — file_id (4 bytes) → del_bitmap (RoaringBitmap)
└── CF: FileDict       — file_path (string) → file_id (int)
                         file_id (int) → file_path (string)（反向映射）
```

**与 KvTablet RocksDB 分离的原因**：
- DV 的 checkpoint/恢复流程与 KV 数据的 checkpoint 互相独立，不会互相干扰。
- DV 的生命周期与 KV 数据不同（DV 与 Iceberg snapshot 绑定）。
- 可以独立调优 DV RocksDB 的参数（如 compaction 策略、block cache 大小）。

### 5.2 TabletServer 与 Tiering Writer 的职责分工

```
TabletServer (轻量元数据维护)              Tiering Writer (Flink, 重 I/O)
┌─────────────────────────┐               ┌──────────────────────────────┐
│                         │               │                              │
│  KV 写入时:              │               │  读 changelog                │
│    -U/-D 到达            │               │                              │
│    → 本地查 RowPosIndex  │               │  +I/+U → 写 data file        │
│    → 更新 LakeDv         │               │         → 记录 position      │
│    → 更新 LogDv          │               │                              │
│                         │               │  -U/-D → 批量查 RowPosIndex  │
│  Union Read 时:          │               │         → 生成 Puffin DV     │
│    → 返回 LakeDv+LogDv   │               │                              │
│    → 立即生效            │               │  Commit → 物化到 Iceberg      │
│                         │◄── 上报 positions ──│                         │
└─────────────────────────┘               └──────────────────────────────┘
```

**TabletServer 侧（轻量操作）**：

- KV 写入时，`-U/-D` 到达后，本地查 DvRocksDB 中的 RowPosIndex，更新 LakeDv 和 LogDv。这些都是本地 RocksDB 读写，开销可控。
- 为 union read 提供实时可见的逻辑删除标记（LakeDv + LogDv），不需要等待下一轮 tiering commit。

**Tiering Writer 侧（重 I/O 操作）**：

- 读 changelog，将 `+I`/`+U` 写入 Iceberg data file。
- 遇到 `-U`/`-D` 时，从 changelog value 中提取 RowId，批量 RPC 查 TabletServer 的 RowPosIndex，获取 `(file, row_position)`，批量生成 Puffin DV 文件。
- Commit 到 Iceberg 后，上报新写入行的 position 映射给 TabletServer。

**这样分工的好处**：

1. **Union read 实时生效**：TabletServer 侧的 LakeDv 在 `-U/-D` 到达时立即更新，union read 不需要等待下一轮 tiering commit 就能跳过 Iceberg 中已删除的行。
2. **Iceberg 写入不在 Fluss Cluster 关键路径上**：Puffin 文件生成和 Iceberg commit 这类重 I/O 操作由 Tiering Writer（Flink job）执行，TabletServer 只做轻量的本地 RocksDB 读写。

---

## 6. 写入流程

### 6.1 实时数据写入 Fluss

一条 KV 数据进入 Fluss 时的处理流程：

1. **获取 KvTablet 写锁**
2. 用 key 反查 KvTablet 的 RocksDB：
   - **查不到（新 key）**：
     - 生成 `+I(value, rowId)`，其中 `rowId` = 即将分配的 log offset
     - 写入 PrewriteBuffer
     - 写入 changelog
     - 将 `[schemaId][BinaryRow][rowId]` 写入 KV state
   - **查到了（已有 key）**：
     - 从旧 value 尾部提取 `oldRowId`
     - **PUT 操作**：
       - 生成 `-U(oldValue, oldRowId)` 和 `+U(newValue, newRowId)`
       - 写入 PrewriteBuffer，写入 changelog
       - 更新 KV state 为 `[schemaId][BinaryRow(newValue)][newRowId]`
     - **DELETE 操作**：
       - 生成 `-D(oldValue, oldRowId)`
       - 写入 PrewriteBuffer，写入 changelog
       - 从 KV state 删除该 key
3. **释放 KvTablet 写锁**，等待 changelog 同步成功

### 6.2 Changelog 同步成功

Changelog 成功同步到所有副本后的处理流程：

1. **获取 KvTablet 写锁**
2. Flush PrewriteBuffer 数据到 RocksDB
3. **获取 LakeDv 写锁**
4. 遍历 PrewriteBuffer flush 下去的每一行 entry，如果是 `-U` / `-D`：
   - a. 用对应 `oldRowId` 从 RowPosIndex 查 FilePos：
     - **查到了** `{file_id, row_position}`：在 LakeDv 中找到 `file_id` 对应的 del_bitmap，将 `row_position` 加入 bitmap。如果 LakeDv 中没有该 `file_id` 的条目，说明该文件对应的删除已在上一轮 tiering 中物化，忽略即可。
     - **查不到**：说明该行从未 tiering 到 Iceberg（仍在 WAL 中），不需要更新 LakeDv。
   - b. 在 RowPosIndex 中删除 `oldRowId`
   - c. 用 `oldRowId` 更新 LogDv：将 `offset = oldRowId` 对应的 changelog 标记为已删除
5. **释放 LakeDv 写锁**
6. 更新 `log_hw`（high watermark）
7. **释放 KvTablet 写锁**

> **关于步骤顺序的说明**：必须先更新 DV 再更新 `log_hw`。如果先更新 `log_hw`，union read 可能看到更大的 `logEndOffset`，但 DV 还没更新到对应位置，导致重复读出已被删除的数据。

> **关于加锁的说明**：KvTablet 写锁的持有时间被 DV 更新操作拉长。在高吞吐场景下，可以考虑先在 flush 阶段收集 `(oldRowId, change_type)` 列表，释放写锁后再异步批量更新 DV，但需要额外处理一致性。

---

## 7. Snapshot 处理流程

### 7.1 RowPosIndex 的构建策略：Writer 上报 + 外部 Compaction 兜底

RowPosIndex 的核心问题是：如何知道每行数据在 Iceberg data file 中的 row position。

**方案**：默认走 Tiering Writer 同步上报（高效），检测到外部未知文件时回退扫描（兜底）。

**Tiering Writer 上报**：Tiering Writer 在写入 data file 的过程中，天然知道每行的 row position（因为是 writer 自己按顺序写入的）。写入完成后，将 `(RowId, file, row_position)` 映射作为 tiering 结果的一部分上报给 TabletServer。

**外部 Compaction 兜底**：TabletServer 维护一个 `knownFiles` 集合（由 writer 上报时 add）。当新 snapshot 到达时，对 `newFiles` 中每个文件检查是否在 `knownFiles` 中：

| 情况                    | 判断条件                | 处理方式                                   |
|-----------------------|---------------------|----------------------------------------|  
| **Fluss 自己写的**        | 文件在 `knownFiles` 中  | 直接用已上报的 position 更新 RowPosIndex，零扫描    |
| **外部 compaction 产生的** | 文件不在 `knownFiles` 中 | 回退扫描该文件，读取 `__offset` 列重建 position 映射  |

### 7.2 处理新 Snapshot

LakeTieringService 生成新的数据湖快照后，通知 CoordinatorServer，CoordinatorServer 再通知 TabletServer。

已有 s2，新来 s3，则：

```
newFiles = snapshot_files(s3) - snapshot_files(s2)
oldFiles = snapshot_files(s2) - snapshot_files(s3)
```

**处理流程**：

```
新 snapshot 到达，计算 newFiles / oldFiles
        │
        ├── 对每个 newFile:
        │       │
        │       ├── knownFiles 中存在？
        │       │       ├── YES → 用已上报的 position 更新 RowPosIndex（快）
        │       │       └── NO  → 扫描文件读 __offset 列，重建 position（慢，仅针对外部 compaction 文件）
        │       │
        │       └── 获取 LakeDv 写锁
        │           → 对于每个 RowId，检查是否已在 RowPosIndex 中被标记为已删除（即 RowPosIndex 中不存在）
        │             → 如果不存在：该行已被删除，将 row_position 加入 LakeDv 的 del_bitmap
        │             → 如果存在：该行存活，用新的 FilePos 覆盖 RowPosIndex 中的旧值
        │           释放 LakeDv 写锁
        │
        └── 对每个 oldFile:
                ├── 从 RowPosIndex 清理指向该文件的 FilePos（这些已被 newFile 的新 FilePos 覆盖）
                ├── 从 LakeDv 删除该文件条目（该文件的删除已物化到新 snapshot 的 Iceberg DV 中）
                └── 从 knownFiles 移除
```

**Step 1 — 处理 newFiles**：

1. **获取 LakeDv 写锁**
2. 对该文件中的每个 RowId：
   - 从 RowPosIndex 反查：
     - **查不到**：说明该 RowId 已经在 changelog 同步成功流程中被删除，该行实际已不存在。将该 row_position 加入 LakeDv 中该文件的 `del_bitmap`。
     - **查到了**：该行存活，用新的 `{file_id, row_position}` 覆盖 RowPosIndex 中旧的 FilePos。
3. 将 `file:del_bitmap` 写入 LakeDv
4. **释放 LakeDv 写锁**

> **加锁原因**：如果不加锁，可能出现以下竞态——changelog 同步成功流程正在处理 `-D(rowId1)`，RowPosIndex 还没删除 rowId1。同时 Step 1 从 RowPosIndex 中查到了 rowId1，认为它存活，不在 LakeDv 中标记删除。结果该行在 LakeDv 中遗漏了。
>
> 加锁后，两种执行顺序都正确：
> - changelog 同步先执行：RowPosIndex 已删除 rowId1，LakeDv 已标记。Step 1 查不到 rowId1，再次标记（幂等）。
> - Step 1 先执行：RowPosIndex 记录了 rowId1 的新 FilePos。后续 changelog 同步时，查到新 FilePos，在 LakeDv 中标记正确的位置。

**Step 2 — 通知 CoordinatorServer**：

CoordinatorServer 收齐所有 bucket 的 DV 完成通知后，将 s3 设置为 DV 可读（更新 LakeTableZNode）。Client 即可在读 s3 时使用 DV。

**Step 3 — 清理旧 snapshot**：

假设 s2 要被清理，对 oldFiles 中的每个文件：
- 从 LakeDv 中删除该文件的条目
- 从 RowPosIndex 中清理指向该文件的 FilePos
- 从 `knownFiles` 中移除

### 7.3 初始构建

第一次 tiering 完成后，RowPosIndex 为空。此时的处理逻辑：

- Tiering Writer 上报新写入行的 `(RowId, file, row_position)` 映射。
- TabletServer 直接将所有上报的映射写入 RowPosIndex。
- 此时 LakeDv 为空（没有新的删除需要标记）。

如果不是通过 writer 上报，而是扫描文件，则第一次 snapshot 的所有行应**全部写入 RowPosIndex**。不能反查 RowPosIndex（因为此时为空，反查全部 miss 会错误地认为所有行都被删除了）。

---

## 8. Tiering Writer 改造

### 8.1 当前实现

当前 tiering 使用 `DeltaTaskWriter`（具体是 `GenericRecordDeltaWriter`），处理逻辑：

- `+I`/`+U` → 写入 data file
- `-U`/`-D` → 写入 equality delete file

### 8.2 改造后实现

引入新的 `DvTaskWriter`，替代 `DeltaTaskWriter`：

| 组件                  | 当前                                          | 改造后                                          |
|---------------------|---------------------------------------------|----------------------------------------------|
| **Writer 类**        | `GenericRecordDeltaWriter` (equality delta) |  新的 `DvTaskWriter`，只做 append + DV 生成         |
| **DELETE 输出**       | Equality delete file                        | Puffin deletion vector file                  |
| **-U/-D 的 RowId**   | changelog 不携带                               | changelog value 中附带 RowId                    |
| **跨批 DELETE**       | 按 key 匹配（equality delete）                   | 用 RowId 批量 RPC 查 TabletServer RowPosIndex    |
| **WriteResult**     | `{dataFiles, deleteFiles}`                  | `{dataFiles, dvFiles, positionReport}`       |
| **Commit**          | `RowDelta.addDeletes(eqDeleteFile)`         | `RowDelta.addDeletes(dvFile)` + 上报 positions |
| **Iceberg 版本**      | v2                                          | v3                                           |

### 8.3 DvTaskWriter 处理流程

```
1. 读 changelog 记录
2. 按记录类型分流：
   ├── +I/+U:
   │     ├── 写入 Iceberg data file（Parquet）
   │     └── 记录 (RowId, file, row_position) 到内存中的 positionReport
   │
   └── -U/-D:
         ├── 从 changelog value 尾部提取 oldRowId
         ├── 累积到批次中，定期批量 RPC 查 TabletServer RowPosIndex
         │     → 获得 (oldRowId → {file_id, row_position}) 映射
         └── 按 file_id 分组，构建 RoaringBitmap → 生成 Puffin DV 文件

3. Commit:
   ├── RowDelta.addRows(dataFiles)
   ├── RowDelta.addDeletes(dvFiles)
   └── 上报 positionReport 给 TabletServer → 更新 RowPosIndex
```

### 8.4 Position 上报

Tiering Writer commit 成功后，通过 RPC 将 `positionReport` 上报给 TabletServer。`positionReport` 的结构：

```
positionReport = Map<file_path, List<(RowId, row_position)>>
```

TabletServer 收到后：
1. 将 `file_path` 加入 `knownFiles` 集合
2. 对每个 `(RowId, row_position)`，在 DvRocksDB 的 FileDict 列族中查找或创建 `file_id`
3. 写入 RowPosIndex：`RowId → {file_id, row_position}`

---

## 9. Union Read 流程

Client 通过 DV 进行 union read 的完整流程：

1. Client 获得 DV 可见的最新 snapshot id，发送 union read 请求
2. Fluss list 该 snapshot 下的 datafile list
3. **获取 KvTablet 读锁**
4. 获取当前 `logEndOffset`
5. 从 LakeDv 中获取 datafile list 对应的 lakeDv
6. 从 LogDv 中获取当前 snapshot 的 start offset 到 `logEndOffset` 的 logDv
7. **释放读锁**
8. 返回给 client：`{lakeDv, logDv, logEndOffset}`

> **加读锁的原因**：为了保证 LakeDv、LogDv 和 logEndOffset 的一致性快照。如果不加锁，可能出现：LogDv 对应的是 `logOffset [0, 10]`，但返回 `logEndOffset = 12`，其中包含 `-U[key1, v1]` 和 `+U[key1, v2]`。Client 读到 logEndOffset = 12 时，`+U[key1, v2]` 不在 LogDv 中会被读出，但 `-U` 对应的旧行也没有在 LakeDv 中标记删除——导致新旧两个版本都被读出，数据重复。

**Client 侧处理**：

1. 在 Iceberg snapshot 上 apply Iceberg DV（Puffin 文件中的物理 DV）
2. 再 apply lakeDv（TabletServer 返回的逻辑 DV）
3. 读出存活的 Iceberg 行
4. Fetch `[snapshot_start_offset, logEndOffset]` 这段 changelog，apply logDv，跳过已删除的记录
5. 合并结果，得到完整数据

**LogDv 返回格式**：

```json
{
  "logDv": [
    { "base_offset": "offset1",  "del_bits": "xxxx" },
    { "base_offset": "offset10", "del_bits": "xxxx" },
    { "base_offset": "offset20", "del_bits": "xxxx" }
  ]
}
```

---

## 10. 恢复流程

### 10.1 DvRocksDB Checkpoint

DvRocksDB 定期做 checkpoint，将 SST 文件上传到远程存储。做 checkpoint 时记录：

- `restoreSnapshot`：当前数据湖 snapshot 的 ID
- `snapshotStartLogOffset`：该 snapshot 对应的 changelog start offset

### 10.2 恢复步骤

1. 从远程存储拉取 SST 文件到本地，加载 DvRocksDB
2. 从 `snapshotStartLogOffset` 开始重放 changelog
3. 对于每条 `-U`/`-D` 记录，提取 `oldRowId`：
   - 在 RowPosIndex 中查找 `oldRowId` 对应的 FilePos：
     - **找到了** `{file_id, row_position}`：在 LakeDv 中找到 `file_id` 的 del_bitmap，将 `row_position` 加入。如果 LakeDv 中没有该 `file_id`，说明该文件在已过期的 snapshot 中，忽略。
     - **找不到**：该行不在 Iceberg 中（仍在 WAL 中或已被更早的操作删除），不需要更新 LakeDv。
   - 在 RowPosIndex 中删除 `oldRowId`
   - 比较 `oldRowId` 和 `snapshotStartLogOffset`：
     - **oldRowId < snapshotStartLogOffset**：不需要更新 LogDv。要删除的行对应的 changelog 已在湖上 snapshot 覆盖的范围内，union read 的 delta log 不会读到这条记录。
     - **oldRowId >= snapshotStartLogOffset**：更新 LogDv，将 `offset = oldRowId` 对应的 changelog 标记为删除。
4. 恢复出来的 RowPosIndex、LogDv、LakeDv 都是针对 `restoreSnapshot` 的。如果存在比 `restoreSnapshot` 更新的 snapshot（`newSnapshot`），则按照 §7.2 的流程处理增量。

### 10.3 Checkpoint 策略建议

- **触发频率**：建议在每次 Iceberg snapshot advance 后触发一次 DvRocksDB checkpoint，确保恢复时需要重放的 changelog 量最小。
- **降级策略**：如果 checkpoint 失败，记录日志并在下一次 snapshot advance 时重试。不影响正常写入和查询。

---

## 11. 与 Iceberg Compaction 的交互

### 11.1 Fluss 内部 Compaction（IcebergRewriteDataFiles）

`IcebergRewriteDataFiles` 执行 compaction 时，旧文件被合并为新文件。由于 compaction 由 Fluss 自己执行，writer 天然知道每行的新 position。处理方式：

1. Compaction writer 在重写文件时，记录 `(RowId, new_file, new_row_position)` 映射
2. Compaction commit 后，上报 position 映射给 TabletServer
3. TabletServer 更新 RowPosIndex（用新 FilePos 覆盖旧 FilePos）
4. 从 LakeDv 中删除旧文件的条目
5. 将新文件加入 `knownFiles`，旧文件从 `knownFiles` 移除

### 11.2 外部 Compaction（Spark 等）

如果外部引擎对 Fluss 管理的 Iceberg 表执行了 compaction：

- 外部产生的新文件不在 TabletServer 的 `knownFiles` 中
- 新 snapshot 到达时，按照 §7.1 的兜底逻辑，自动扫描这些未知文件，读取 `__offset` 列重建 position 映射
- 性能代价只在外部 compaction 发生时支付

**可观测性**：检测到外部 compaction 文件时，打日志或上报 metric（如 `external_compaction_files_scanned`），让运维感知到有外部引擎在修改 Fluss 管理的 Iceberg 表。

---

## 12. LakeDv 物化流程

LakeDv 从 TabletServer 的逻辑删除标记物化为 Iceberg 中的物理 Deletion Vector（Puffin 文件）的流程：

### 12.1 触发时机

每轮 tiering commit 时执行。

### 12.2 流程

1. Tiering Writer 读 changelog 中的 `-U`/`-D` 记录
2. 从 changelog value 提取 `oldRowId`
3. 批量 RPC 查 TabletServer RowPosIndex，获取 `(file_id, row_position)`
4. 按 `file_id` 分组，构建 RoaringBitmap
5. 将 RoaringBitmap 序列化为 Puffin 文件
6. 通过 Iceberg `RowDelta` API 将 Puffin DV 文件 commit 到 Iceberg

### 12.3 物化后清理

Tiering commit 成功后，TabletServer 清空 LakeDv（因为这些逻辑删除已物化到 Iceberg DV 中）。从新的空状态开始积累后续的逻辑删除。

---

## 13. 端到端示例

以下通过一个完整的端到端示例，展示所有组件如何协作。

### 初始状态

- Iceberg 中无数据
- RowPosIndex、LakeDv、LogDv 均为空

### Step 1：写入 3 条数据

```
PUT (key1, v1)  → +I (offset=0, key1, v1)  → RowId=0
PUT (key2, v2)  → +I (offset=1, key2, v2)  → RowId=1
PUT (key3, v3)  → +I (offset=2, key3, v3)  → RowId=2
```

KV State:
```
key1 → [schemaId][v1][rowId=0]
key2 → [schemaId][v2][rowId=1]
key3 → [schemaId][v3][rowId=2]
```

DV 状态：全部为空（还没有删除操作，还没有 tiering）。

### Step 2：第一轮 Tiering

Tiering Writer 读 changelog offset 0~2，将 `+I` 记录写入 Iceberg data file：

```
data_file_A:
  pos0 → (key1, v1, __offset=0)
  pos1 → (key2, v2, __offset=1)
  pos2 → (key3, v3, __offset=2)
```

Tiering Writer 上报 position：
```
positionReport = {
  data_file_A: [(RowId=0, pos=0), (RowId=1, pos=1), (RowId=2, pos=2)]
}
```

Iceberg commit snapshot S1（tiered offset = 2）。

TabletServer 收到上报后，更新 RowPosIndex：
```
RowPosIndex:
  0 → {file_A, pos0}
  1 → {file_A, pos1}
  2 → {file_A, pos2}
```

LakeDv、LogDv 仍为空。`knownFiles = {file_A}`。

### Step 3：更新 key1

```
PUT (key1, v4)  → -U (offset=3, key1, v1, oldRowId=0)
                   +U (offset=4, key1, v4)  → RowId=4
```

KV State:
```
key1 → [schemaId][v4][rowId=4]
key2 → [schemaId][v2][rowId=1]
key3 → [schemaId][v3][rowId=2]
```

Changelog 同步成功后：
- 查 RowPosIndex：`oldRowId=0 → {file_A, pos0}` ✓ 找到
- LakeDv 中检查 file_A：
  - 此时 LakeDv 为空，但 file_A 是最新 snapshot 的文件，应该初始化条目
  - 更新 LakeDv：`file_A → del_bitmap{0}` （pos0 被删除）
- 从 RowPosIndex 中删除 RowId=0
- 更新 LogDv：offset=0 在 range `offset0~offset9` 中，bitmap = `{1}`

DV 状态：
```
RowPosIndex:
  1 → {file_A, pos1}
  2 → {file_A, pos2}

LakeDv:
  file_A → {0}

LogDv:
  offset0~offset9 → {1}
```

### Step 4：Union Read

Client 请求 union read（snapshot S1）：

1. 获取 KvTablet 读锁
2. `logEndOffset = 4`
3. lakeDv = `{file_A: {0}}`
4. logDv = `{offset0~offset9: {1}}`
5. 释放读锁
6. 返回 `{lakeDv, logDv, logEndOffset=4}`

Client 侧处理：
- 读 Iceberg snapshot S1 的 data_file_A，apply lakeDv → 跳过 pos0（key1, v1）
- 读出 pos1（key2, v2）和 pos2（key3, v3）
- 读 changelog `[S1_start=0, logEndOffset=4]`：
  - offset=0：在 LogDv 中 → 跳过
  - offset=1：在 LogDv 中但未标记删除 → 但这是 `+I`，在 Iceberg 中已有，通过 `start_offset=3` 跳过（实际 delta log 从 tiered offset 之后开始读）
  - offset=3：`-U` → 不输出给用户（retract 类型）
  - offset=4：`+U (key1, v4)` → 输出

最终结果：`(key1, v4), (key2, v2), (key3, v3)` ✓ 正确

### Step 5：删除 key3

```
DELETE (key3)  → -D (offset=5, key3, v3, oldRowId=2)
```

Changelog 同步成功后：
- 查 RowPosIndex：`oldRowId=2 → {file_A, pos2}` ✓ 找到
- 更新 LakeDv：`file_A → del_bitmap{0, 2}` （pos0 和 pos2 被删除）
- 从 RowPosIndex 中删除 RowId=2
- 更新 LogDv：offset=2 在 range `offset0~offset9` 中，bitmap = `{1, 3}`（第1条和第3条被删除）

### Step 6：第二轮 Tiering

Tiering Writer 读 changelog offset 3~5：
- offset=3：`-U(key1, v1, oldRowId=0)` → 查 RowPosIndex 获取 `{file_A, pos0}` → 生成 DV
- offset=4：`+U(key1, v4)` → 写入新 data file
- offset=5：`-D(key3, v3, oldRowId=2)` → 查 RowPosIndex 获取 `{file_A, pos2}` → 生成 DV

生成：
```
data_file_B:
  pos0 → (key1, v4, __offset=4)

Puffin DV file:
  file_A → {0, 2}  (pos0 和 pos2 被删除)
```

Iceberg commit snapshot S2（tiered offset = 5）。上报 position：
```
positionReport = {
  data_file_B: [(RowId=4, pos=0)]
}
```

TabletServer 收到新 snapshot S2：
- `newFiles = {file_B}`，`oldFiles = {}`（file_A 仍在 S2 中）
- file_B 在 `knownFiles` 中 → 用上报的 position 更新 RowPosIndex
- 清空 LakeDv（已物化到 Iceberg DV）

DV 状态：
```
RowPosIndex:
  1 → {file_A, pos1}
  4 → {file_B, pos0}

LakeDv: 空（已物化）

LogDv: 清理 offset < S2_start_offset 的条目
```

---

## 14. 总结

| 维度               | 设计决策                                              |
|------------------|---------------------------------------------------|
| **RowId**        | 使用 `+I`/`+U` 的 log offset，天然唯一递增，与 `__offset` 列一致 |
| **RowPosIndex**  | 只存最新快照的单个 FilePos（8 bytes/行），dictionary 编码文件路径    |
| **LakeDv**       | 增量存储，每轮 tiering commit 后清空                        |
| **LogDv**        | Range-based bitmap，按固定 offset 间隔分段                |
| **存储**           | DvRocksDB 独立于 KvTablet RocksDB，四个列族               |
| **架构分工**         | TabletServer 维护轻量元数据，Tiering Writer 生成物理 DV 文件    |
| **Position 构建**  | Writer 上报（默认）+ 外部 compaction 扫描（兜底）               |
| **Changelog 格式** | `-U`/`-D` 的 value 携带 oldRowId（8 bytes）            |
| **KV State 格式**  | 尾部追加 RowId（8 bytes）                               |
| **Iceberg 版本**   | 直接切换到 v3，不保留 equality delete 兼容                   |
| **恢复**           | 从 DvRocksDB checkpoint 加载，重放 changelog 增量         |
