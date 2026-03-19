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
  │  │  (热层内的删除/更新追踪)   │  │     │   │ Iceberg Deletion   │  │
  │  └─────────────────────────┘  │     │  │ Vector (Puffin)    │  │
  │                               │     │  └────────────────────┘  │
  │  ┌─────────────────────────┐  │     │                          │
  │  │  Lake Deletion Vector   │──┼────►│  下一轮 tiering 时物化     │
  │  │  (跨层逻辑删除标记)        │  │    │                           │
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
- **`-U`/`-D` changelog**：RowId = 被删除版本的 log offset，从 KV state 旧 value 首部提取。
- **KV state (RocksDB)**：RowId = 当前版本的 log offset，写入时追加到 value 首部（8 bytes）。

### 3.2 FilePos

标记一条数据在 Iceberg 中的物理位置，由两部分组成：

- **file_id**：data file 的字典编码 ID（int 类型，非原始文件路径）
- **row_position**：数据在该文件中的行号（从 0 开始）

两者合并为一个 8 bytes 的值：高 4 bytes 为 file_id，低 4 bytes 为 row_position。

### 3.3 RowPosIndex

RowId 到 FilePos 的映射，用于根据 RowId 快速定位一行数据在 Iceberg 中的物理位置。

**关键设计决策：RowPosIndex 始终表示当前 readable snapshot 中的位置。**

| RowId  | FilePos           |
|--------|-------------------|
| rowId1 | `{file_B, pos3}`  |
| rowId2 | `{file_C, pos10}` |

每行只需存一个 FilePos（8 bytes）：

1. Tiering commit 时，changelog 中的删除已经物化为 Iceberg DV（Puffin 文件）。任何快照中，早于 tiered offset 的删除已由 Iceberg 自身处理。
2. LakeDv 只覆盖 tiered offset 之后的新删除——这些新删除针对的是 readable snapshot 中的文件。
3. Union read 读的是最新的 DV-readable snapshot，§6.2 处理删除时查 RowPosIndex 得到的位置必须与 union read 读取的 snapshot 一致。

**pendingRowPos**：

新 snapshot 提交后、成为 DV-readable 之前，新文件中行的位置不能直接写入 RowPosIndex（否则 §6.2 会将删除标记打到新文件而非当前 readable snapshot 的文件上，见下方说明）。这些位置暂存在 **pendingRowPos** 中，等待 readable snapshot 前移时再原子迁移到 RowPosIndex（见 §8.2 步骤 3）。

pendingRowPos 是一个扁平结构（`RowId → FilePos`），不按 snapshotId 分组。这依赖于以下保证：**S_{n+1} 的 position report 不会在 S_n 的 readable 切换完成之前到达**。因此在任何时刻，pendingRowPos 中的条目都属于同一个 snapshot——当前已提交但尚未 readable 的最新 snapshot。同一个 RowId 在新 snapshot 到达时直接覆盖旧值即可。

> **为什么 RowPosIndex 不能提前写入新 snapshot 的位置**：假设 readable snapshot S_old 中 rowId=R 位于 file_A:pos5。新 snapshot S_new 到达但尚未 readable，§8.2 步骤 1 将 RowPosIndex[R] 覆盖为 file_B:pos7。此时对 R 来了一个 delete，§6.2 查 RowPosIndex 命中 file_B:pos7，在 LakeDv 中标记 file_B 而非 file_A。但 union read 仍然读 S_old（只扫 file_A），file_A:pos5 没有任何屏蔽标记——旧行重新暴露，删除失效。

**存储方案**：

- RowPosIndex 保存在 DvRocksDB 中（独立于 KvTablet 的 RocksDB）。
- pendingRowPos 保存在 DvRocksDB 的独立列族中（key = `RowId(8 bytes)`，value = `FilePos(8 bytes)`）。
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

**生命周期管理**：当数据湖 readable snapshot advance 后，只有 **range 的结束 offset < `start_logOffset`** 的整段 `offset_range` 条目才可以清理。

> **为什么不能按 range 起始 offset 判断**：LogDv 使用固定大小的 offset range（如 0\~9、10\~19）。如果按"range 起始 offset < start_logOffset"清理，当 `start_logOffset` 落在某个 range 中间时（如 `start_logOffset = 5`，range 0\~9），该 range 会被整段删除，但其中 offset 5\~9 的 delete bit 仍然是 union read 需要的——union read 读取 `[snapshot_start_offset, logEndOffset]` 这段 changelog（见 §10），offset 5\~9 中的已删除记录如果没有 LogDv 屏蔽，旧版本会被错误读出。
>
> 按 range 结束 offset 判断，确保横跨边界的那段 range 被完整保留。其中 offset < `start_logOffset` 的 bit 不会被 union read 用到（union read 从 `start_logOffset` 开始读），但保留它们不影响正确性，仅多占少量存储（一个 range 的 bitmap）。

### 3.5 LakeDv

标记 Iceberg data file 中已在 Fluss 侧逻辑删除但尚未物化的行。

**数据结构**：

| file_name  | del_bitmap   |
|------------|--------------|
| data_file1 | `bin{3}`     |
| data_file2 | `bin{2, 10}` |

**增量存储**：LakeDv 只保存尚未物化到 Iceberg DV 的删除。
每轮 tiering 生成 split 时快照当前 LakeDv，Tiering Writer 将快照物化为 Puffin DV 文件。
新 snapshot 成为 DV-readable 后，通过 bitmap 差集清理已物化的条目——在快照到清理之间新到达的 `-U/-D` 产生的 bit 会被保留，不会丢失。

```
Tiering commit S2 完成（offset 100）
    │
    │  新的 -D/-U 到达 → LakeDv 逐步积累
    │  （通常只有几分钟的增量，很小）
    │
Tiering commit S3 完成（offset 120）
    │  → S3 的 Iceberg DV 已包含 offset 101~120 的删除
    │  → 等待 S3 成为 DV-readable 后，通过 bitmap 差集清理已物化的条目
```

由于每轮 tiering 间隔通常只有几分钟，LakeDv 积累的删除量很小，不存在全量 DV 过大的问题。历史删除已物化到 Iceberg DV 中，不需要 TabletServer 维护。

### 3.6 DV-Readable Snapshot

并非每个 Iceberg snapshot 都可以立即用于 union read。本文中的 **DV-readable snapshot** 指的是：CoordinatorServer 已对外发布、允许 client 发起 union read 的**目标 snapshot**。注意，这不等价于“所有 TabletServer 都已经完成本地 readable switch”。在短暂窗口内，CoordinatorServer 可能已经对外发布 `S_new`，但部分 TabletServer 仍停留在 `S_old`；client 仍然应该以 `S_new` 作为目标 snapshot 继续重试，直到这些 TabletServer 完成切换。

流程：Tiering Writer 提交新 snapshot 后，TieringService 先通知各 TabletServer 处理本 bucket 的 DV 元数据。各 TabletServer 处理完成后，向 TieringService 发送 **ready ack**。TieringService 收齐所有 bucket 的 ready ack 后，向 CoordinatorServer 提交“该 snapshot 可发布为 DV-readable”的通知；CoordinatorServer 将该 snapshot 标记为 DV-readable（更新 LakeTableZNode），并对外发布。
此时 client 可以开始以该 snapshot 作为目标 snapshot 发起 union read。随后 CoordinatorServer 再通知各 TabletServer 执行 readable switch。各 TabletServer 完成 readable switch 后，向 TieringService 发送 **switched ack**。
只有当 CoordinatorServer 收齐所有 bucket 的 switched ack 后，才允许生成下一轮 tiering split。

在 snapshot 从“已提交”到“所有 TabletServer 都完成 readable switch”的窗口内，部分 TabletServer 仍可能返回旧的 `currentReadableSnapshot`。这不会影响正确性：client 不回退到旧 snapshot，而是继续对目标 snapshot 重试；
TabletServer 完成切换后，请求自然收敛成功。在此窗口内，TabletServer 必须保留旧 snapshot 对应的 LakeDv，直到本地完成 readable switch 后才能清理。

**CoordinatorServer barrier 机制**：

- **Phase 1 / ready**：TieringService 先发起本轮 bucket 级 DV 元数据处理。TabletServer 完成 §7.3 的 position report 处理、`snapshotBitmap` 过滤后，向 TieringService 发送 ready ack，表示"本 bucket 的 DV 元数据已就绪，但尚未完成 readable switch"。
- **Phase 2 / publish + switch**：TieringService 收齐 ready ack 后，向 CoordinatorServer 提交发布请求；CoordinatorServer 将该 snapshot 标记为 DV-readable 并对外发布。随后 CoordinatorServer 通知所有 TabletServer 执行 §8.2 步骤 3 的 readable switch。TabletServer 完成 `pendingRowPos → RowPosIndex` 迁移、oldFiles 清理、PendingDeletes 清理和 LakeDv 差集清理后，返回 switched ack 给 TieringService。
- **Phase 3 / next split gate**：TieringService 只有在收齐所有 bucket 的 switched ack 后，才允许生成下一轮 split。

**单飞 / 强取消语义**：

- **单飞约束**：同一 tiering split 在任意时刻最多只允许一个有效 attempt。
- **显式失败后才重试**：retry 只能在 CoordinatorServer **明确宣告**当前 attempt 失败后启动；超时、网络抖动或短暂无响应都不能直接触发新的 attempt。
- **强取消语义**：被 CoordinatorServer 宣告失败的旧 attempt 必须被强制取消；取消后不得再向任何 TabletServer 发送 `positionReport`、ready ack 或 switched ack 相关请求。
- **设计取舍**：系统依赖上述协议保证，因此 TabletServer 不额外基于 `actualSnapshotId` 做 attempt 校验；对 position report 的拒绝主要依赖结构性过期检查。`actualSnapshotId` 保留用于 ready ack / switched ack 关联和排障。

**时序保证**：

1. S_{n+1} 的 position report 不会在 S_n 的 readable 切换完成之前到达 TabletServer。这一保证由 CoordinatorServer 的两阶段 ack barrier 提供，使得 pendingRowPos 可以采用扁平结构（不需要按 snapshotId 分组）。
2. Split n+1 的生成不会在 readable switch n 之前发生。这一保证同样由 CoordinatorServer 的两阶段 ack barrier 提供，使得 LakeDv 差集清理所用的 `snapshotBitmap` 只需保留一份（不需要按 snapshotId 分组）。

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

> **注意**：`+I` 和 `+U` 记录的 value 格式不变，仍为 `[schemaId][BinaryRow]`。它们的 RowId 就是自身的 log offset，无需在 value 中额外携带。

### 4.3 Iceberg 数据列扩展

Tiering 写入 Iceberg data file 时，除了用户数据列外，还写入以下系统列：

- **`__offset`**：该行对应的 `+I`/`+U` changelog log offset，即 RowId。已有列，用于 union read 和 DV 定位。
- **`__bucket`**：该行所属的 Fluss bucket id（int 类型）。**新增列**，用于外部 compaction 后识别行的 bucket 归属（见 §12），避免通过主键哈希反算 bucket。

> **约束**：`__offset` 和 `__bucket` 是 DV 正确性的基础。外部引擎对 Fluss 管理的 Iceberg 表执行 compaction 或 rewrite 时，**必须保留这两列及其值**。如果这两列被丢弃或篡改，Fluss 将无法重建 position 映射或正确路由 bucket，导致删除标记失效、数据复活。
>
> **强制措施**：
> - Fluss 在创建 Iceberg 表时设置 table property `fluss.system-columns=__offset,__bucket`，通过文档告知外部引擎不得删除这些列。
> - Tiering Service 扫描外部 compaction 文件时（§12.2），**校验 `__offset` 和 `__bucket` 列是否存在且类型正确**。如果缺失或类型不匹配：
>
> **这是 correctness 问题，不仅是性能问题。** 如果仅拒绝处理而允许 readable snapshot 继续前移，旧文件从 snapshot 中消失后，RowPosIndex 中指向旧文件的条目变成悬空引用。后续对这些行的更新/删除会查到悬空的 FilePos，尝试在已不存在的文件上标记 LakeDv——标记无效，活行从新文件中漏删。
>
> 因此：**阻止该表的 DV-readable snapshot 前移**，直到问题解决。具体措施：
> 1. 上报 metric `external_compaction_invalid_files`，日志中明确报错原因。
> 2. 该 bucket 的 ready ack / switched ack **都不发送给 TieringService**，从而阻止 TieringService 向 CoordinatorServer 提交 DV-readable 发布请求。
> 3. Union read 继续使用旧的 readable snapshot（旧 LakeDv 覆盖旧文件，数据正确但陈旧）。
> 4. 运维修复后（重新执行保留系统列的 compaction，或回滚到包含系统列的 snapshot），Tiering Service 重新扫描并上报，DV-readable 恢复前移。

### 4.4 Iceberg 版本

从 Iceberg v2 切换到 v3，使用 position delete（Puffin DV）替代 equality delete。

**新表**：启用 DV 功能时，`IcebergLakeCatalog` 创建 Iceberg 表时设置 `format-version=3`。当前代码未显式设置 format-version（默认为 v2），需要在 `createTable` 时增加 `TableProperties.FORMAT_VERSION = "3"` 的设置。

**存量 v2 表**：已有 v2 表通过 Iceberg 的 `table.updateProperties().set("format-version", "3").commit()` 原地升级到 v3。升级后 v2 中已有的 equality delete 文件仍然有效，查询引擎可以同时处理 equality delete 和 position delete。后续 compaction 会逐步消化旧的 equality delete 文件。Fluss 在启用 DV 功能时自动检测并触发升级，无需用户手动操作。

### 4.5 前置要求：FULL Changelog 模式

DV 功能要求主键表使用 **FULL changelog 模式**（即更新时同时写 `-U` 和 `+U`）。WAL changelog 模式下，更新只写 `+U` 不写 `-U`，无法获知被覆盖的旧版本 RowId，因此无法定位 Iceberg 中的旧行进行删除标记。

创建主键表时，如果启用了 DV 功能，系统应校验 changelog 模式为 FULL，否则拒绝创建。

---

## 5. 存储架构

### 5.1 DvRocksDB

RowPosIndex、LogDv、LakeDv 作为三个不同的列族（Column Family），保存在一个独立的 RocksDB 实例中，记为 **DvRocksDB**。文件路径字典编码保存在第四个列族中。

```
DvRocksDB
├── CF: RowPosIndex    — RowId (8 bytes) → FilePos (8 bytes)
│                        始终表示当前 readable snapshot 中的位置
├── CF: PendingRowPos  — RowId (8 bytes) → FilePos (8 bytes)
│                        尚未 readable 的 snapshot 中的位置（扁平结构）
├── CF: LogDv          — offset_range → del_bitmap
├── CF: LakeDv         — file_id (4 bytes) → del_bitmap (RoaringBitmap)
├── CF: FileDict       — file_path (string) → file_id (int)
│                        file_id (int) → file_path (string)（反向映射）
└── CF: PendingDeletes — RowId (8 bytes) → empty
```

**与 KvTablet RocksDB 分离的原因**：
- DV 的 checkpoint/恢复流程与 KV 数据的 checkpoint 互相独立，不会互相干扰。
- DV 的生命周期与 KV 数据不同（DV 与 Iceberg snapshot 绑定）。
- 可以独立调优 DV RocksDB 的参数（如 compaction 策略、block cache 大小）。

**PendingDeletes 列族**：

用于解决 position report 与 `-U/-D` 处理之间的时序间隙。当 `-U/-D` 到达时，被删除行可能正在被 tiering（position report 尚未到达），RowPosIndex 和 pendingRowPos 中都没有该行的条目，无法更新 LakeDv。此时将 `oldRowId` 记入 PendingDeletes。后续 position report 到达时，检查 PendingDeletes，补齐 LakeDv。详细流程见 §6.2 和 §7.3。

**并发控制：dvLock（ReadWriteLock）**：

DvRocksDB 包含一个读写锁（记为 **dvLock**），用于保护 PendingDeletes、RowPosIndex、LakeDv 的并发访问：

- **dvLock.writeLock()**：序列化以下三条写路径，确保任意时刻只有一条路径在修改 DV 元数据：

| 持锁路径                         | 章节                      | 加锁步骤            | 操作                                                                            |
|------------------------------|-------------------------|-----------------|-------------------------------------------------------------------------------|
| Changelog 同步成功               | §6.2 步骤 3-5             | 步骤 3 获取，步骤 5 释放 | 读写 RowPosIndex、pendingRowPos、PendingDeletes、LakeDv                            |
| Position 上报（含外部 compaction） | §7.3                     | 整段在 writeLock 下执行   | 读写 pendingRowPos、PendingDeletes（只读不删）、RowPosIndex（只读）、LakeDv；WriteBatch 原子提交 |
| Readable 切换                     | §8.2 步骤 3              | 整段在 writeLock 下执行   | 读写 RowPosIndex、pendingRowPos、PendingDeletes、LakeDv                            |

- **dvLock.readLock()**：Union Read（§10 步骤 4）获取读锁，确保读取 LakeDv 时不会与上述写路径并发。多个 union read 可以同时持有读锁。

不加写锁的竞态示例：positionReport 检查 PendingDeletes 未命中（准备写 pendingRowPos），同时 `-D` 到达、查 RowPosIndex 也未命中（写入 PendingDeletes），两者交错后 pendingRowPos 出现残留条目，已删除行复活。

不加读锁的竞态示例：union read 在 KvTablet 读锁保护下读取 LakeDv，但 position report（仅持 dvLock.writeLock，不持 KvTablet 锁）同时修改 LakeDv——union read 可能读到与当前 readable snapshot 不一致的 bitmap，导致漏屏蔽已删除行或错屏蔽存活行。

**锁顺序**：需要同时持有两把锁时，必须先获取 KvTablet 锁，再获取 dvLock，避免死锁。§6.2 遵循此顺序（KvTablet.writeLock → dvLock.writeLock）；§10 遵循此顺序（KvTablet.readLock → dvLock.readLock）。§7.3 和 §8.2 仅获取 dvLock.writeLock，无锁顺序问题。

**幂等机制：天然幂等 + 结构性过期检查**：

Position 上报（§7.3）的处理过程是**天然幂等**的——所有操作（pendingRowPos 写入、LakeDv bitmap set、PendingDeletes 检查）在重复执行时产生相同结果。关键在于 §7.3 步骤 4 **不移除** PendingDeletes 条目：重试时条目仍在，做出相同判断。PendingDeletes 的清理推迟到 readable 切换时统一执行（见 §8.2 步骤 3）。

Position 上报（§7.3 步骤 0）通过**结构性过期检查**拦截过期请求：如果 `splitOffsetRange.latest_offset <= currentReadableSnapshotTieredOffset`，说明 readable snapshot 已前移到该 split 之后，该 split 的 position 数据已被后续 snapshot 处理（§8.2）覆盖或取代，直接拒绝。此检查仅依赖当前 readable snapshot 的 tiered offset，无需额外持久化标记。

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
│                         │               │  -U/-D → 区分处理：          │
│                         │               │    oldRowId在本轮内 → 本地DV │
│                         │               │    oldRowId在旧数据 → 跳过   │
│  生成 Tiering Split 时:  │               │                              │
│    → 快照 LakeDv         │               │  Commit 时:                  │
│    → 随 split 下发       │               │    → 用 LakeDv 快照生成       │
│                         │               │      Puffin DV 文件           │
│  Union Read 时:          │               │                              │
│    → 返回 LakeDv+LogDv   │               │  上报 positions              │
│    → 立即生效            │               │    → 更新 RowPosIndex         │
└─────────────────────────┘               └──────────────────────────────┘
```

**TabletServer 侧（轻量操作）**：

- KV 写入时，`-U/-D` 到达后，本地查 DvRocksDB 中的 RowPosIndex，更新 LakeDv 和 LogDv。这些都是本地 RocksDB 读写，开销可控。
- 生成 tiering split 时，快照当前 LakeDv，随 split 一起下发给 Tiering Writer。
- 为 union read 提供实时可见的逻辑删除标记（LakeDv + LogDv），不需要等待下一轮 tiering commit。

**Tiering Writer 侧（重 I/O 操作）**：

- 读 changelog，先用 split-scoped `logDvSnapshot` 过滤本轮已被删除的 RowId，再将存活的 `+I`/`+U` 写入 Iceberg data file，并记录每行的 position。
- `-U`/`-D` 不再由 Tiering Writer 自己根据 `oldRowId` 推导 position；跨 split 的删除已经沉淀在 LakeDv 快照里，同 split 内先写后删的行也已经在 `logDvSnapshot` 过滤阶段被跳过。
- Commit 时，只需将 LakeDv 快照物化为 Puffin DV 文件，与 data file 一起提交到 Iceberg。
- Commit 成功后，上报新写入行的 position 映射给 TabletServer。

**这样分工的好处**：

1. **Union read 实时生效**：TabletServer 侧的 LakeDv 在 `-U/-D` 到达时立即更新，union read 不需要等待下一轮 tiering commit 就能跳过 Iceberg 中已删除的行。
2. **Iceberg 写入不在 Fluss Cluster 关键路径上**：Puffin 文件生成和 Iceberg commit 这类重 I/O 操作由 Tiering Writer（Flink job）执行，TabletServer 只做轻量的本地 RocksDB 读写。
3. **Tiering Writer 无需 RPC 查 RowPosIndex**：跨 split 的删除信息通过 LakeDv 快照随 split 下发；同 split 内先写后删的行通过 `logDvSnapshot` 在写前直接过滤。整个 writer 路径都不需要 RPC 反查 TabletServer。

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
     - 将 `[RowId][schemaId][BinaryRow]` 写入 KV state
   - **查到了（已有 key）**：
     - 从旧 value 首部提取 `oldRowId`
     - **PUT 操作**：
       - 生成 `-U(oldValue, oldRowId)` 和 `+U(newValue, newRowId)`
       - 写入 PrewriteBuffer，写入 changelog
       - 更新 KV state 为 `[newRowId][schemaId][BinaryRow(newValue)]`
     - **DELETE 操作**：
       - 生成 `-D(oldValue, oldRowId)`
       - 写入 PrewriteBuffer，写入 changelog
       - 从 KV state 删除该 key
3. **释放 KvTablet 写锁**，等待 changelog 同步成功

### 6.2 Changelog 同步成功

Changelog 成功同步到所有副本后的处理流程：

1. **获取 KvTablet 写锁**
2. Flush PrewriteBuffer 数据到 RocksDB
3. **获取 dvLock.writeLock()**（见 §5.1 并发控制）
4. 遍历 PrewriteBuffer flush 下去的每一行 entry，如果是 `-U` / `-D`：
   - a. 用对应 `oldRowId` 在 RowPosIndex **和** pendingRowPos 中查找 FilePos：
     - **RowPosIndex 中查到了** `{file_id, row_position}`：在 LakeDv 中将 `row_position` 加入 `file_id` 对应的 del_bitmap。从 RowPosIndex 中删除 `oldRowId`。
     - **pendingRowPos 中查到了** `{file_id, row_position}`：在 LakeDv 中将 `row_position` 加入 `file_id` 对应的 del_bitmap。从 pendingRowPos 中删除 `oldRowId`。
     - **两者都查不到**：该行可能正在被 tiering（position report 尚未到达），将 `oldRowId` 加入 **PendingDeletes**。后续 position report 到达时会检查 PendingDeletes 并补齐 LakeDv（见 §7.3）。
   - b. 用 `oldRowId` 更新 LogDv：将 `offset = oldRowId` 对应的 changelog 标记为已删除
5. **释放 dvLock.writeLock()**
6. 更新 `log_hw`（high watermark）
7. **释放 KvTablet 写锁**

> **关于步骤顺序的说明**：必须先更新 DV 再更新 `log_hw`。如果先更新 `log_hw`，union read 可能看到更大的 `logEndOffset`，但 DV 还没更新到对应位置，导致重复读出已被删除的数据。

> **关于加锁的说明**：KvTablet 写锁的持有时间被 DV 更新操作拉长。在高吞吐场景下，可以考虑先在 flush 阶段收集 `(oldRowId, change_type)` 列表，释放写锁后再异步批量更新 DV，但需要额外处理一致性。

---

## 7. Tiering 流程

### 7.1 生成 Tiering Split

Tiering split 定义了本轮 tiering 需要处理的 changelog 范围：`(last_tiered_offset, latest_offset]`。

- `last_tiered_offset`：上一轮 tiering 成功处理的**最后一条** changelog 的 offset（含义是"已完成到此"）。
- 当前 split 从 `last_tiered_offset + 1` 开始（左开），到 `latest_offset` 结束（右闭）。
- 首次 tiering 时，`last_tiered_offset = -1`，split 从 offset 0 开始。

生成 tiering split 时，**同时快照 LakeDv**：

1. **获取 KvTablet 读锁**（保证 LakeDv 与 `log_hw` 一致）
2. 读取当前 `log_hw` 作为 `latest_offset`
3. 快照当前 LakeDv 的全部内容，并通过 FileDict 将 `file_id` 反向映射为 `file_path`。同时将快照的 `Map<file_id, bitmap>` 副本保存在内存变量 `snapshotBitmap` 中（用于后续 §13.3 的差集清理）。由于保证 split n+1 的生成不会在 readable switch n 之前发生，`snapshotBitmap` 在任何时刻最多只有一份，直接覆盖即可。
4. **释放读锁**
5. 生成 tiering split：`{offset_range: (last_tiered_offset, latest_offset], lakeDvSnapshot: {file_path → bitmap, ...}}`

> **LakeDv 快照使用 file_path 而非 file_id**：`file_id` 是 TabletServer 内部 DvRocksDB 的字典编码，Tiering Writer 无法解析。TabletServer 在生成快照时利用本地 FileDict 将 `file_id` 解析为 `file_path`，随 split 下发的是 `{file_path → bitmap}`。Tiering Writer 直接用 `file_path` 生成 Puffin DV 文件，无需访问 FileDict。

**为什么 LakeDv 快照与 tiering split 天然对齐**：

- LakeDv 积累的是自上次 tiering commit 以来**尚未被 bitmap 差集清理**的删除。通过 §13.3 的清理机制，已物化到 Iceberg DV 的条目在 readable snapshot 前移后被差集移除，因此 LakeDv 中的内容实际覆盖的是"上次清理以来的所有新增删除"。
- `log_hw` 是 LakeDv 已经处理到的位置（changelog 同步成功流程中，先更新 LakeDv 再更新 `log_hw`）。
- 因此在读锁保护下，LakeDv 的内容精确覆盖了所有尚未物化到 Iceberg DV 的逻辑删除，与 tiering split 的 changelog 范围对齐。

### 7.2 Tiering Writer 处理

Tiering Writer 收到 tiering split 后的处理流程：

```
1. 读 changelog (last_tiered_offset, latest_offset]
2. 先 apply split-scoped `logDvSnapshot`：
   ├── `+I/+U` 的 RowId 如果命中 `logDvSnapshot`，说明该行已在本轮 changelog 中被删除 → 直接跳过，不写 data file
   └── 未命中的 `+I/+U` 才写入 Iceberg data file（Parquet），并记录 `(RowId, file, row_position)` 到内存中的 `positionReport`

3. `-U/-D` 不直接生成本地 DV：
   ├── 跨 split 的删除已经体现在 `lakeDvSnapshot` 中
   └── 同 split 内先写后删的情况已经由步骤 2 的 `logDvSnapshot` 过滤掉

4. 生成 Puffin DV 文件：
   ├── 读取当前 Iceberg table state，获取 currentFiles 集合和 baseSnapshotId
   ├── 过滤 lakeDvSnapshot：仅保留 currentFiles 中仍存在的文件（见下方 lakeDvSnapshot 过时保护）
   └── 将过滤后的 lakeDvSnapshot 序列化为 Puffin 文件（覆盖 Iceberg 中旧行的删除）

5. Commit（见下方 Commit 验证与冲突处理）:
   ├── RowDelta rowDelta = table.newRowDelta()
   ├── rowDelta.validateFromSnapshot(baseSnapshotId)
   ├── rowDelta.validateDataFilesExist(lakeDvReferencedFiles)  // LakeDv 引用的已有文件
   ├── rowDelta.addRows(dataFiles)
   ├── rowDelta.addDeletes(dvFiles)
   ├── rowDelta.commit()   // 失败则 abort，见冲突处理
   └── 上报给 TabletServer：
         ├── positionReport → 更新 RowPosIndex
         ├── materializedDvFiles → 实际物化的 DV 文件列表（过滤后的 lakeDvSnapshot keys）
         └── 不需要额外上报同 split 内删除列表
```

> **同 split 内先写后删的处理**：当同一轮 tiering split 中，一行数据先被 `+I`/`+U` 写入，随后又被 `-U`/`-D` 删除时，Tiering Writer 不需要再根据 `oldRowId` 判断该删除是否属于当前 split。只要 split 下发的 `logDvSnapshot` 已经覆盖这轮 changelog 中的删除，writer 在写 `+I`/`+U` 之前先检查其 RowId 是否命中 `logDvSnapshot`；命中则直接跳过。这样最终写入 Iceberg 的天然就是“apply 过本轮 log DV 后的存活数据”，不会再遇到“oldRowId 是否在本轮 split 范围内”的判断问题。

> **lakeDvSnapshot 过时保护**：从 split 生成到 commit 之间可能发生外部 compaction，导致 lakeDvSnapshot 中引用的文件已被替换或删除。Tiering Writer 在生成 Puffin DV 前，**必须读取当前 Iceberg table state 的文件集合，过滤 lakeDvSnapshot**，仅为当前仍存在的文件生成 Puffin DV。
>
> 被过滤的文件对应的逻辑删除仍保留在 TabletServer 的 LakeDv 中（不会被差集清理——因为 `materializedDvFiles` 不包含这些文件，见 §13.3）。后续 §8.2 处理新 snapshot 时，外部 compaction 产出的替代文件作为 newFile 被处理，§8.2 会为其中已删除的行重建 LakeDv 条目。这些删除将在下一轮 tiering 中被物化。

> **Commit 验证与冲突处理——IcebergLakeCommitter 改造要点**：
>
> 当前 `IcebergLakeCommitter.java:115-123` 的注释明确声明：*"Position deletes committed to the table in this path are used only to delete rows from data files that are being added in this commit. There is no way for data files added along with the delete files to be concurrently removed, so there is no need to validate the files referenced by the position delete files."* 因此当前 RowDelta 不做任何文件存在性校验。
>
> 引入 LakeDv 物化后，这一假设不再成立：Puffin DV 中的 position delete 会指向 **历史 snapshot 中的已有 data file**（而非本次 commit 新增的文件），这些文件可能在 split 生成到 commit 之间被外部 compaction 替换。改造后的 commit 逻辑必须：
>
> 1. **`validateFromSnapshot(baseSnapshotId)`**：以 step 3 读取 table state 时的 snapshot 为基准，检测 commit 时是否有并发修改（TOCTOU 安全网）。
> 2. **`validateDataFilesExist(lakeDvReferencedFiles)`**：显式校验 Puffin DV 引用的已有 data file 在提交时仍存在。`lakeDvReferencedFiles` 是过滤后的 lakeDvSnapshot 中引用的文件集合（不包括本次 commit 新增的 data file；本次新增文件只承载步骤 2 过滤后仍存活的数据）。
> 3. **冲突处理**：如果 commit 因 `ValidationException` 失败（外部 compaction 在 step 3 读 table state 之后又替换了被引用的文件），当前 tiering 任务 **abort**——清理已生成的 Puffin DV 和 data file。下一轮 tiering 重新生成 split（包含新的 lakeDvSnapshot，自然覆盖此轮失败的删除）。**LakeDv 中的逻辑删除标记不受影响**，union read 在下一轮物化前仍能通过 LakeDv 正确屏蔽旧行，不会出现数据重复。
>
> 过滤（step 3）作为**优化**减少无效 commit 的概率，`validateDataFilesExist` 作为**安全网**兜底过滤后到 commit 之间的 TOCTOU 竞态。两者配合确保不会向 Iceberg 提交指向已不存在文件的 position delete。

### 7.3 Position 上报

Tiering Writer commit 成功后，通过 RPC 将 `positionReport` 和 `materializedDvFiles` 上报给 TabletServer。

```
positionReport       = Map<file_path, List<(RowId, row_position)>>
                       // 包含本轮 tiering 新写入的行 + 外部 compaction 重写的行（见 §12.2）
splitOffsetRange     = (last_tiered_offset, latest_offset]  // 用于结构性过期检查 + RowId 范围区分
materializedDvFiles  = List<file_path>  // 实际物化了 DV 的文件（过滤后的 lakeDvSnapshot keys）
actualSnapshotId     = long  // Iceberg commit 返回的实际 snapshot id（用于 ready ack / switched ack 关联与排障）
```

TabletServer 收到后：

0. **结构性过期检查**（见 §5.1 幂等机制）：如果 `splitOffsetRange.latest_offset <= currentReadableSnapshotTieredOffset`，说明 readable snapshot 已前移到该 split 之后，直接拒绝。
1. **获取 dvLock.writeLock()**（见 §5.1 并发控制，与 §6.2 步骤 3 使用同一把锁）
2. 将 `file_path` 加入 `knownFiles` 集合
3. 对每个 `(RowId, row_position)`，在 DvRocksDB 的 FileDict 列族中查找或创建 `file_id`
4. 统一处理本轮 tiering 新写入行和外部 compaction 重写行（使用 RowId 范围区分）：
   - **RowId 在 PendingDeletes 中**：该行在 position report 到达之前已被删除。将 `{file_id, row_position}` 加入 LakeDv 的 del_bitmap（如无条目则创建）。**不从 PendingDeletes 中移除**（保留条目使重试时做出相同判断，见 §5.1 幂等机制）。**不写入 pendingRowPos**。
   - **RowId 不在 PendingDeletes 中**，按 RowId 范围区分：
     - **RowId ∈ splitOffsetRange**（本轮 tiering 新写入的行）：该行是本轮刚写入 Iceberg 的新数据，直接写入 **pendingRowPos**：`RowId → {file_id, row_position}`。无需查 RowPosIndex——该行之前不存在于 Iceberg 中。
     - **RowId ∉ splitOffsetRange**（外部 compaction 重写的已有行，见 §12）：查 RowPosIndex 和 pendingRowPos：
       - **查到了**（RowPosIndex 或 pendingRowPos 中）：该行存活，将新的 `{file_id, row_position}` 写入 **pendingRowPos**（覆盖旧位置）。
       - **都查不到**：该行已在 changelog 同步成功流程中被删除（§6.2 已从 RowPosIndex/pendingRowPos 中移除），实际已不存在。将 `row_position` 加入 LakeDv 中该文件的 `del_bitmap`。
   RowPosIndex 始终不变——它表示当前 readable snapshot 的位置，待 readable 切换时再迁移（见 §8.2 步骤 3）。
5. **将步骤 2-4 的所有 DvRocksDB 修改（pendingRowPos、LakeDv）通过同一个 RocksDB WriteBatch 原子提交**
6. **释放 dvLock.writeLock()**
7. 用 `materializedDvFiles` 过滤 `snapshotBitmap`：仅保留 `materializedDvFiles` 中包含的文件条目（见 §13.3），移除未物化的文件。
8. **步骤 7 完成后**，才可发送该 bucket 的 ready ack（见 §8.2 步骤 2）。

> **实现约束：ready ack 必须在步骤 7 之后发送**。如果在步骤 7 之前就通知 CoordinatorServer，CoordinatorServer 可能过早将 snapshot 标记为 DV-readable 并触发 §13.3 差集清理。此时 `snapshotBitmap` 中尚未过滤未物化文件，差集运算会错误地清除 LakeDv 中尚未物化的删除标记——不影响正确性（多屏蔽不会导致旧行复活），但浪费存储且可能干扰后续 union read 的性能。
>
> **步骤 8 失败策略**：如果步骤 8 失败，**不得发送 ready ack**。实现上应原地重试；若重试仍失败，记录错误日志并等待 CoordinatorServer **显式宣告当前 attempt 失败** 后再触发新的 retry。该 bucket 的 ready ack 缺失会阻止 CoordinatorServer 将新 snapshot 标记为 DV-readable，union read 继续使用旧的 readable snapshot——数据正确但陈旧，不会导致旧行复活。

> **为什么用 RowId 范围区分新写入行和重写行**：RowId 就是 `+I`/`+U` 的 log offset。`splitOffsetRange = (last_tiered_offset, latest_offset]` 恰好是本轮 tiering 处理的 changelog 范围。如果 RowId ∈ splitOffsetRange，说明该行的 `+I`/`+U` 在本轮 split 中，是新写入 Iceberg 的数据，此前从未出现在任何 Iceberg 快照中——"都查不到"是正常的（首次入 Iceberg），应写入 pendingRowPos。如果 RowId ∉ splitOffsetRange，说明该行来自更早的 tiering，是外部 compaction 从旧文件重写到新文件的已有行——"都查不到"意味着该行已被 §6.2 删除，应标记 LakeDv。
>
> 这一统一逻辑也覆盖了外部 compaction 场景（原 §8.2 步骤 1），无需单独处理路径。

> **外部 compaction 行的并发正确性**：dvLock.writeLock() 保证外部 compaction 行的处理与 §6.2（changelog 同步）串行化。两种执行顺序都正确：
> - §6.2 先执行：RowPosIndex/pendingRowPos 已删除 rowId，LakeDv 已标记旧文件。§7.3 查不到 rowId（"都查不到"分支），在 LakeDv 中标记新文件（幂等）。
> - §7.3 先执行：pendingRowPos 记录了 rowId 的新 FilePos。后续 §6.2 查到 pendingRowPos 中的新 FilePos，在 LakeDv 中标记新文件位置；同时若 RowPosIndex 中有旧 FilePos，也标记旧文件。新旧文件的删除标记都正确。

---

## 8. Snapshot 处理流程

### 8.1 RowPosIndex 的构建策略

RowPosIndex 的核心问题是：如何知道每行数据在 Iceberg data file 中的 row position。

**默认路径：Tiering Writer 同步上报**

Tiering Writer 在写入 data file 的过程中，天然知道每行的 row position（因为是 writer 自己按顺序写入的）。写入完成后，将 `(RowId, file, row_position)` 映射作为 tiering 结果的一部分上报给 TabletServer。

**外部 Compaction 路径：Tiering Service 扫描并分发**

TabletServer 维护一个 `knownFiles` 集合（由 writer 上报时 add）。Tiering Service 在 commit 时检测到外部 compaction 产生的未知文件后，扫描这些文件读取 `__offset` 和 `__bucket` 列，按 bucket 分组，将 position 信息连同本轮 tiering 的 positionReport 一起上报给对应 bucket 的 TabletServer（详见 §12）。

| 情况 | 判断条件 | 处理方式 |
|------|----------|----------|
| **Fluss 自己写的** | 文件在 `knownFiles` 中 | 直接用已上报的 position 写入 pendingRowPos，零扫描 |
| **外部 compaction 产生的** | 文件不在 `knownFiles` 中 | 由 Tiering Service 扫描文件并上报 position，TabletServer 不做文件 I/O |

两条路径的 position 合并在同一个 positionReport 中上报，由 §7.3 统一处理。TabletServer 通过 RowId 范围（splitOffsetRange）自动区分新写入行和外部 compaction 重写行（见 §7.3 步骤 4）。

### 8.2 处理新 Snapshot

LakeTieringService 生成新的数据湖快照后，通知 CoordinatorServer，CoordinatorServer 再通知 TabletServer。

已有 s2，新来 s3，则：

```
newFiles = snapshot_files(s3) - snapshot_files(s2)
oldFiles = snapshot_files(s2) - snapshot_files(s3)
```

**处理流程**：

newFiles 中每行的 position 由 Tiering Service 扫描并上报（外部 compaction 文件见 §12.2），与本轮 tiering 的 positionReport 合并后通过 §7.3 统一处理。TabletServer 在 §7.3 步骤 4 中使用 RowId 范围（splitOffsetRange）自动区分本轮新写入行和外部 compaction 重写行，无需单独的处理步骤。

```
新 snapshot 到达
        │
        ├── newFiles 处理：
        │     由 Tiering Service 扫描并通过 §7.3 统一上报处理
        │     （RowId ∈ splitOffsetRange → 新行，写 pendingRowPos）
        │     （RowId ∉ splitOffsetRange → 重写行，检查存活状态）
        │
        └── readable 切换时（步骤 3）：
              原子迁移 pendingRowPos → RowPosIndex
              + 清理 oldFiles + 清理 PendingDeletes
```

**步骤 1 — newFiles 处理（由 §7.3 统一执行）**：

newFiles 中的行与本轮 tiering 新写入的行通过同一个 positionReport RPC 上报给 TabletServer，由 §7.3 的统一逻辑处理。§7.3 步骤 4 使用 RowId 范围自动区分两种行：

- **RowId ∈ splitOffsetRange**（新写入行）：直接写入 pendingRowPos。
- **RowId ∉ splitOffsetRange**（外部 compaction 重写行）：检查 PendingDeletes → 检查 RowPosIndex/pendingRowPos → "都查不到"则标记 LakeDv。

并发正确性由 §7.3 的 dvLock.writeLock() 保证（见 §5.1 及 §7.3 末尾的并发分析）。

**步骤 2 — ready ack 到 CoordinatorServer**：

该 bucket 的 ready ack **必须在 §7.3 步骤 8 完成之后发送给 TieringService**——即 `snapshotBitmap` 已完成对未物化文件的过滤。

TieringService 收齐所有 bucket 的 ready ack 后，向 CoordinatorServer 提交将 s3 发布为 DV-readable 的请求；CoordinatorServer 更新 LakeTableZNode，并向所有相关 TabletServer 下发 readable switch 通知。此后 client 可以开始以 s3 作为目标 snapshot 发起 union read；尚未完成 switch 的 TabletServer 可能暂时返回 stale snapshot error，client 按 §10 的规则对同一个 s3 重试即可。

**步骤 3 — Readable 切换（在 readable snapshot 前移时执行）**：

oldFiles 的清理和 RowPosIndex 的更新**不在新 snapshot 到达时执行**，而是推迟到 CoordinatorServer 将新 snapshot 标记为 DV-readable、并由 CoordinatorServer 下发 readable switch 通知之后。TabletServer 完成 readable switch 后，必须向 TieringService 返回 switched ack；TieringService 只有在收齐所有 switched ack 后，才允许下一轮 split 生成。清理操作在 dvLock.writeLock() 下执行，与 union read 的 dvLock.readLock() 互斥。union read 在 readLock 临界区内校验 `requestedSnapshotId == currentReadableSnapshot`（见 §10 步骤 5），确保不会读到已被清理的旧 LakeDv。

当 readable snapshot 从 S_old 前移到 S_new 时：

```
oldFiles = snapshot_files(S_old) - snapshot_files(S_new)
```

注意对比基准是**前后两个 readable snapshot**（不是相邻的任意两个 commit snapshot）。中间可能经历了多个 snapshot（tiering 产生的、外部 compaction 产生的），但只要 readable 没前移，旧文件的 LakeDv 就必须保留。

**在 dvLock.writeLock() 保护下原子执行以下操作**：

1. **迁移 pendingRowPos → RowPosIndex**：遍历 pendingRowPos 中的每个 `(RowId → FilePos)`。若该条目仍存在（未被 §6.2 删除），则将 FilePos 写入 RowPosIndex，覆盖旧值。已被 §6.2 删除的条目不会出现在遍历中——LakeDv 已在 §6.2 中同时标记了新旧文件的删除，无需额外处理。
2. 清除 pendingRowPos 的全部条目。
3. 对 oldFiles 中的每个文件：从 LakeDv 中删除该文件条目，从 `knownFiles` 中移除。
4. **清理 PendingDeletes**：`deleteRange(PendingDeletes, 0, S_new_tiered_offset + 1)`。oldRowId ≤ S_new 的 tiered offset 意味着对应行的 position report 已处理过（LakeDv 已标记），条目已失效。RocksDB range delete，高效。

> **为什么不能跳过 pendingRowPos 直接写 RowPosIndex**：readable 切换可能跨过中间的 commit snapshot。例如 S_old（当前 readable）→ S2（commit）→ S3（commit，成为新 readable）。假设行 R 在 S2 中从 file_A 重写到 file_B（`pendingRowPos[R] = file_B:pos7`），但 R 在 S3 中文件未再变化（file_B 不是 S3 的 newFile）。此时 S3 到达时不会再更新 R 的条目。若跳过 pendingRowPos 直接写 RowPosIndex，RowPosIndex[R] 会在 S2 阶段被覆盖为 file_B，但此时 union read 仍在读 S_old（使用 file_A），§6.2 会将删除打到 file_B 而非 file_A——删除失效。

同时执行 §13.3 的 bitmap 差集清理——使用 `snapshotBitmap` 执行差集运算，清除 LakeDv 中已物化的标记，然后清空 `snapshotBitmap`。

> **为什么必须在 readable 切换时才迁移 RowPosIndex**：RowPosIndex 是 §6.2 处理 `-U/-D` 时查找行物理位置的主要来源。如果 RowPosIndex 提前更新到新 snapshot 的位置，但 union read 仍在读旧 readable snapshot 的文件，§6.2 会将删除标记打到新文件上而非旧文件上——旧 readable snapshot 中的行漏删，重新暴露。推迟到 readable 切换时迁移，确保 RowPosIndex 始终与 union read 读取的 snapshot 一致。
>
> **窗口期内的删除**：在步骤 1（处理 newFiles）到步骤 3（readable 切换）之间，如果有 `-U/-D` 到达，§6.2 会同时检查 RowPosIndex（旧 readable snapshot 位置）和 pendingRowPos（新 snapshot 位置），在 LakeDv 中同时标记两个文件的删除。这保证了：
> - union read 在窗口期内读 S_old → LakeDv 中有 S_old 文件的标记 ✓
> - readable 切换后 union read 读 S_new → LakeDv 中有 S_new 文件的标记 ✓

### 8.3 初始构建

第一次 tiering 完成后，RowPosIndex 为空，且不存在旧的 readable snapshot。此时的处理逻辑：

- Tiering Writer 上报新写入行的 `(RowId, file, row_position)` 映射。
- TabletServer 将所有上报的映射**直接写入 RowPosIndex**（而非 pendingRowPos）。因为这是第一个 snapshot，commit 成功后它将立即成为首个 DV-readable snapshot，不存在"旧 readable snapshot 与新 snapshot 不一致"的窗口。
- 此时 LakeDv 为空（没有新的删除需要标记）。

如果不是通过 writer 上报，而是扫描文件，则第一次 snapshot 的所有行应**全部写入 RowPosIndex**。不能反查 RowPosIndex（因为此时为空，反查全部 miss 会错误地认为所有行都被删除了）。

---

## 9. Tiering Writer 改造

### 9.1 当前实现

当前 tiering 使用 `DeltaTaskWriter`（具体是 `GenericRecordDeltaWriter`），处理逻辑：

- `+I`/`+U` → 写入 data file
- `-U`/`-D` → 写入 equality delete file

### 9.2 改造后实现

引入新的 `DvTaskWriter`，替代 `DeltaTaskWriter`：

| 组件 | 当前 | 改造后 |
|------|------|--------|
| **Writer 类** | `GenericRecordDeltaWriter` (equality delta) | 新的 `DvTaskWriter`，只做 append |
| **DELETE 输出** | Equality delete file | Puffin DV file（来自 LakeDv 快照） |
| **DV 信息来源** | Writer 自己处理 `-U`/`-D` | LakeDv 快照 + split-scoped `logDvSnapshot` 过滤 |
| **WriteResult** | `{dataFiles, deleteFiles}` | `{dataFiles, dvFiles, positionReport, materializedDvFiles}` |
| **Commit** | `RowDelta.addDeletes(eqDeleteFile)` 无校验 | `RowDelta` + `validateFromSnapshot` + `validateDataFilesExist`（见 §7.2） |
| **Iceberg 版本** | v2 | v3 |

### 9.3 IcebergLakeCommitter 改造

当前 `IcebergLakeCommitter.java` 的 `commit()` 方法中，RowDelta 不配置任何校验（`validateFromSnapshot`、`validateDataFilesExist` 均未调用），因为当前 position delete 只引用同一 commit 中新增的 data file（见源码注释 L115-L123）。

引入 LakeDv 物化后，position delete 会引用历史 snapshot 中的已有 data file。`IcebergLakeCommitter` 必须改造：

```java
// 改造前（当前实现）：
RowDelta rowDelta = icebergTable.newRowDelta();
committable.getDataFiles().forEach(rowDelta::addRows);
committable.getDeleteFiles().forEach(rowDelta::addDeletes);
// 无校验，直接 commit

// 改造后：
RowDelta rowDelta = icebergTable.newRowDelta();
rowDelta.validateFromSnapshot(baseSnapshotId);               // 检测并发修改
rowDelta.validateDataFilesExist(lakeDvReferencedDataFiles);   // 校验引用的已有文件仍存在
committable.getDataFiles().forEach(rowDelta::addRows);
committable.getDeleteFiles().forEach(rowDelta::addDeletes);
// commit，ValidationException 时 abort 并重试
```

其中 `lakeDvReferencedDataFiles` 是 LakeDv 快照过滤后引用的已有 data file 集合（不含本次 commit 新增的 data file）。`baseSnapshotId` 是 step 3 读取 table state 时的 snapshot id。

### 9.4 Tiering Writer 不查 RowPosIndex

Tiering Writer 不通过 RPC 查 TabletServer 的 RowPosIndex。DV 相关信息来自两个来源：

1. **LakeDv 快照**：承载跨 split 的删除。TabletServer 在 changelog 同步成功时已查过 RowPosIndex 并将结果沉淀到 LakeDv 中。生成 tiering split 时快照 LakeDv，随 split 下发。Tiering Writer 直接将快照序列化为 Puffin DV 文件。
2. **split-scoped `logDvSnapshot`**：承载本轮 split 内已经发生的删除。Tiering Writer 在写 `+I`/`+U` 前先 apply 这份快照，命中的 RowId 直接跳过，因此最终写入的数据天然已经扣除了同 split 内先写后删的行。

整个 writer 路径都不需要 RPC 反查 TabletServer。

---

## 10. Union Read 流程

Client 通过 DV 进行 union read 的完整流程：

1. Client 获得 DV 可见的最新 snapshot id（记为 `requestedSnapshotId`），发送 union read 请求（**请求中携带 `requestedSnapshotId`**）
2. Fluss list 该 snapshot 下的 datafile list
3. **获取 KvTablet 读锁**
4. **获取 dvLock.readLock()**（见 §5.1，保证 LakeDv 不会被 §7.3/§8.2 并发修改）
5. **Snapshot 一致性校验**：检查 `requestedSnapshotId == currentReadableSnapshot`。如果不匹配，释放两把锁，返回 **stale snapshot error**（附带 `currentReadableSnapshot`）。
   - 若 `requestedSnapshotId < currentReadableSnapshot`：说明 TabletServer 已切到更新的 readable snapshot，client 刷新到更新 snapshotId 后重试。
   - 若 `requestedSnapshotId > currentReadableSnapshot`：说明 CoordinatorServer 已对外发布了更新的目标 snapshot，但该 TabletServer 尚未完成 readable switch。client **保持原来的 `requestedSnapshotId` 不变**，对同一个目标 snapshot 做退避重试，**不得回退到旧 snapshot**。
6. 获取当前 `logEndOffset`
7. 从 LakeDv 中获取 datafile list 对应的 lakeDv
8. 从 LogDv 中获取当前 snapshot 的 start offset 到 `logEndOffset` 的 logDv
9. **释放 dvLock.readLock()**
10. **释放 KvTablet 读锁**
11. 返回给 client：`{lakeDv, logDv, logEndOffset}`

> **加锁的原因**：
> - **KvTablet 读锁**：与 §6.2（changelog 同步成功）互斥，保证 LakeDv/LogDv 与 logEndOffset 的一致性。§6.2 在 KvTablet 写锁内先更新 DV 再更新 log_hw，读锁确保 union read 不会看到 log_hw 已更新但 DV 尚未更新的中间状态。
> - **dvLock.readLock()**：与 §7.3（position 上报）、§8.2 步骤 1（处理新 snapshot）和 §8.2 步骤 3（readable 切换 + 清理）互斥。这些路径持 dvLock.writeLock()，因此 readLock 保证 union read 在读取 LakeDv 期间，不会有并发的 LakeDv 修改或清理。

> **Snapshot 一致性校验的必要性**：Client 获取 `requestedSnapshotId`（步骤 1）和 TabletServer 读取 LakeDv（步骤 7）之间存在 TOCTOU 窗口。在此窗口内，可能出现两种方向的偏差：
> - Coordinator 已发布 `S_new`，但该 TabletServer 还停留在 `S_old`；
> - 该 TabletServer 已切到 `S_new`，但 client 仍拿着更旧的 snapshotId。
>   如果不做校验，TabletServer 可能返回与目标 snapshot 不一致的 LakeDv：要么 client 按 `S_old` 读取时拿到了已经为 `S_new` 清理过的 LakeDv，要么 client 按 `S_new` 读取时却拿到了仍对应 `S_old` 的 LakeDv，都会破坏屏蔽语义。
>
> 将校验放在 dvLock.readLock() 临界区内确保：一旦校验通过，步骤 3 的 cleanup 无法并发执行（需要 writeLock），LakeDv 在整个读取过程中保持与 `requestedSnapshotId` 一致。校验失败时，client 按上面的双向规则收敛：要么刷新到更新 snapshotId，要么保持当前目标 snapshotId 重试；无论哪种情况，都不回退到更旧的目标 snapshot。

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

## 11. 恢复流程

### 11.1 DvRocksDB Checkpoint

DvRocksDB 定期做 checkpoint，将 SST 文件上传到远程存储。做 checkpoint 时记录：

- `restoreSnapshot`：当前 DV-readable snapshot 的 ID
- `snapshotStartLogOffset`：该 snapshot 对应的 changelog start offset
- `checkpointLogHw`：checkpoint 时刻的 `log_hw`

> **为什么需要 `checkpointLogHw`**：checkpoint 保存的是"某个运行时刻的增量状态"，包含了 `snapshotStartLogOffset` 到 `checkpointLogHw` 之间所有 `-U/-D` 的处理结果（RowPosIndex 已删除、LakeDv 已更新、LogDv 已更新）。恢复时如果从 `snapshotStartLogOffset` 重放，会重复应用这些操作。必须从 `checkpointLogHw + 1` 开始重放。

> **RowPosIndex 与 checkpoint 的关系**：RowPosIndex 的数据来源是 Tiering Service 的 position report（§7.3），不在 changelog 中。因此 **changelog 重放无法为 RowPosIndex 新增条目**——它只能删除条目（处理 `-U/-D` 时）和更新 LakeDv/LogDv/PendingDeletes。RowPosIndex 的恢复完全依赖 DvRocksDB checkpoint 中保存的状态。对于 checkpoint 之后到达的 position report（已丢失），需要通过重新扫描 Iceberg 文件来恢复（见 §11.2 步骤 4）。

### 11.2 恢复步骤

1. 从远程存储拉取 SST 文件到本地，加载 DvRocksDB。此时 RowPosIndex 反映 `restoreSnapshot` 的状态。
2. 从 **`checkpointLogHw + 1`** 开始重放 changelog（跳过 checkpoint 已包含的部分）
3. 对于每条 `-U`/`-D` 记录，提取 `oldRowId`（**仅处理删除，不新增 RowPosIndex 条目**）：
   - 在 RowPosIndex 中查找 `oldRowId` 对应的 FilePos：
     - **找到了** `{file_id, row_position}`：在 LakeDv 中找到 `file_id` 对应的 del_bitmap，将 `row_position` 加入 bitmap。如果该 `file_id` 在 LakeDv 中尚无条目，则创建新条目。
     - **找不到**：将 `oldRowId` 加入 PendingDeletes（与 §6.2 逻辑一致）。后续 position report 到达时会检查 PendingDeletes 补齐 LakeDv。
   - 在 RowPosIndex 中删除 `oldRowId`
   - 比较 `oldRowId` 和 `snapshotStartLogOffset`：
     - **oldRowId < snapshotStartLogOffset**：不需要更新 LogDv。要删除的行对应的 changelog 已在湖上 snapshot 覆盖的范围内，union read 的 delta log 不会读到这条记录。
     - **oldRowId >= snapshotStartLogOffset**：更新 LogDv，将 `offset = oldRowId` 对应的 changelog 标记为删除。
4. **处理 checkpoint 之后的新 snapshot**：恢复出来的 RowPosIndex、LogDv、LakeDv 都是针对 `restoreSnapshot` 的。如果当前存在比 `restoreSnapshot` 更新的目标 snapshot（记为 `targetSnapshot`），需要重建本 bucket 对 `targetSnapshot` 的位置状态。

   **重建位置状态**（不复用 §7.3 在线路径——恢复场景缺少 `splitOffsetRange`、`materializedDvFiles` 等在线上下文）：

   a. 计算 `newFiles = snapshot_files(targetSnapshot) - snapshot_files(restoreSnapshot)`
   b. 扫描 newFiles 中属于本 bucket 的行（读 `__offset` 和 `__bucket` 列），得到 `(RowId, file, row_position)` 列表
   c. 令 `restoreTieredOffset = snapshotStartLogOffset - 1`（即 `restoreSnapshot` 包含的最后一条 tiered log offset）。对每个 `(RowId, file, row_position)`：
      - **RowId 在 PendingDeletes 中**：该行已被删除。标记 LakeDv，不写 pendingRowPos。
      - **RowId > restoreTieredOffset**（`restoreSnapshot` 之后新 tiering 写入的行）：该行不存在于 `restoreSnapshot` 中，RowPosIndex 中查不到是正常的。写入 pendingRowPos。
      - **RowId ≤ restoreTieredOffset**（`restoreSnapshot` 中的已有行，被后续 tiering 或 compaction 重写到新文件）：
        - 查 RowPosIndex：**找到** → 行存活，写入 pendingRowPos（覆盖旧位置）。
        - **找不到** → 行已被删除（changelog replay 已从 RowPosIndex 中移除），标记 LakeDv。
   d. 重建完成后，向 TieringService 发送本 bucket 对 `targetSnapshot` 的 **ready ack**。
   e. **不在本地执行 readable 切换**。只有 TieringService 收齐所有 bucket 的 ready ack、并由 CoordinatorServer 对外发布该 targetSnapshot 为 DV-readable 后，CoordinatorServer 才触发全局 readable switch（§8.2 步骤 3），各 TabletServer 收到通知后执行 pendingRowPos → RowPosIndex 迁移和 oldFiles 清理。
   f. **snapshotBitmap 处理**：恢复场景下 `snapshotBitmap` 未被填充（正常流程中由 §7.1 步骤 3 快照、§7.3 步骤 8 过滤），readable 切换时**跳过 §13.3 的 bitmap 差集清理**。LakeDv 中可能残留已物化到 Iceberg DV 的冗余条目，但不影响正确性——union read 同时 apply Iceberg DV 和 LakeDv，重复标记是幂等的。冗余条目在下一轮正常 tiering 中消除：§7.1 步骤 3 快照 LakeDv 时会完整捕获冗余 bits 到 `snapshotBitmap`，Tiering Writer 物化后，§8.2 步骤 3 的差集运算（`当前 bitmap AND NOT snapshotBitmap`）精确移除这些 bits。

### 11.3 Checkpoint 策略建议

- **触发时机**：建议在每次 readable snapshot 前移（§8.2 步骤 3）完成后触发一次 DvRocksDB checkpoint。此时 RowPosIndex 已通过 pendingRowPos 迁移反映最新 readable snapshot 的状态，checkpoint 保存的 RowPosIndex 是一致的。这也确保恢复时需要重放的 changelog 量最小、需要重新扫描的 Iceberg 文件最少。
- **降级策略**：如果 checkpoint 失败，记录日志并在下一次 readable snapshot 前移时重试。不影响正常写入和查询。恢复时会从更早的 checkpoint 开始，重放更多 changelog 并可能需要扫描更多 Iceberg 文件，但不影响正确性。

---

## 12. 与外部 Compaction 的交互

外部引擎（如 Spark）可能对 Fluss 管理的 Iceberg 表执行 compaction，合并旧文件为新文件。Fluss 不控制外部 compaction 的时机，但必须正确处理其产生的文件变化。

### 12.1 感知时机

Fluss 不实时监听 Iceberg snapshot 变化。外部 compaction 产生的新 snapshot（如 S3）对 Fluss 是不可见的，直到 Fluss 自己的 Tiering Writer 进行下一次 commit（如 S4）。此时 Tiering Service 对比上次已知 snapshot（S2）和当前 Iceberg table state，发现其中包含了外部 compaction 的变化。

### 12.2 检测与处理

Tiering Service 在 commit 时执行以下检测：

```
本轮 tiering 自己写的文件 = tieringNewFiles
当前 Iceberg table 所有文件 = currentFiles
上次已知 snapshot 的文件 = lastKnownFiles

externalNewFiles = (currentFiles - lastKnownFiles) - tieringNewFiles
externalOldFiles = lastKnownFiles - currentFiles
```

如果 `externalNewFiles` 非空，说明发生了外部 compaction。Tiering Service 执行：

1. **扫描外部新文件**：读取 `externalNewFiles` 中每个文件的 `__offset` 和 `__bucket` 列。`__offset` 即 RowId，`__bucket` 标识行所属的 Fluss bucket。
2. **按 bucket 分组**：将 `(RowId, file, row_position)` 按 `__bucket` 值分组。
3. **上报 position**：每个 bucket 的 position 信息连同本轮 tiering 的 `positionReport` 一起上报给对应 bucket 的 TabletServer。
4. **上报旧文件列表**：将 `externalOldFiles` 也通知 TabletServer，用于后续 readable snapshot 前移时清理。

TabletServer 收到后通过 §7.3 的统一逻辑处理。外部 compaction 重写行的 RowId ∉ splitOffsetRange，§7.3 步骤 4 自动区分：检查 PendingDeletes 和 RowPosIndex/pendingRowPos 状态，存活行写入 pendingRowPos，已删除行标记 LakeDv。外部新文件加入 `knownFiles`。

### 12.3 被 compaction 物理删除的行

外部 compaction 会应用已有的 Iceberg DV（Puffin 文件），将已物理删除的行排除在新文件之外。这些行不会出现在 `externalNewFiles` 的扫描结果中。

这些行在 RowPosIndex 中**不会残留**：

- **存活行**：新文件中存活行的 RowId 与旧文件中相同，通过 §7.3 上报后会覆盖 RowPosIndex/pendingRowPos 中旧文件的条目（见 §7.3 步骤 4 的 `RowId ∉ splitOffsetRange` 分支）。
- **被物理删除的行**：这些行被删除时，§6.2 已将其 RowPosIndex 条目删除（或放入 PendingDeletes 后由 §7.3 处理时不写入 pendingRowPos）。到 compaction 应用 Iceberg DV 时，RowPosIndex 中已无该行的条目，不存在残留。

### 12.4 运维约束：Snapshot 过期策略

外部 compaction 产生的新 snapshot 对 Fluss 不可见，直到下一次 Fluss tiering commit 时才被吸收。在此期间：

- Fluss 的 readable snapshot（如 S1）仍在被 union read 使用，其引用的 data files 不能被删除。
- 外部 compaction 产生的中间 snapshot（如 S3）中，旧文件可能已被标记为不需要，但 Fluss 侧仍依赖这些文件的 LakeDv 条目提供逻辑删除屏蔽。

**约束**：Iceberg 表的 snapshot expiration 策略必须保留 Fluss 当前 readable snapshot 及其引用的所有 data files。建议：

- 将 Iceberg 表的 `history.expire.min-snapshots-to-keep` 设置为足够大的值，覆盖 tiering 间隔内可能产生的 snapshot 数量。
- 或由 Fluss 在 table property 中标记当前 readable snapshot id，外部 expiration 工具跳过该 snapshot 及其之前的依赖。

如果 readable snapshot 被过早 expire 导致 data files 被物理删除，union read 会失败（读不到文件）。

### 12.5 可观测性

检测到外部 compaction 文件时，打日志或上报 metric（如 `external_compaction_files_scanned`），让运维感知到有外部引擎在修改 Fluss 管理的 Iceberg 表。

---

## 13. LakeDv 物化流程

LakeDv 从 TabletServer 的逻辑删除标记物化为 Iceberg 中的物理 Deletion Vector（Puffin 文件）的完整流程：

### 13.1 触发时机

每轮 tiering commit 时执行。

### 13.2 流程

1. 生成 tiering split 时，TabletServer 在读锁保护下快照当前 LakeDv，并通过 FileDict 将 `file_id` 解析为 `file_path`
2. LakeDv 快照（`{file_path → bitmap}`）随 tiering split 下发给 Tiering Writer
3. Tiering Writer 直接用 `file_path` 和 bitmap 生成 Puffin DV 文件（无需额外的字典查找）
4. 通过 Iceberg `RowDelta` API 将 Puffin DV 文件与 data file 一起 commit 到 Iceberg

### 13.3 物化后清理

**清理时机：新 snapshot 成为 DV-readable 之后**，而不是 tiering commit 成功时。

> **为什么不能在 commit 成功时就清理**：tiering commit 成功后，新 snapshot（如 S2）的 Puffin DV 已包含了 LakeDv 快照中的删除。但此时 S2 还没有被 CoordinatorServer 标记为 DV-readable（需要等收齐所有 bucket 的通知）。在这个窗口内，union read 客户端拿到的仍是旧的 readable snapshot S1。如果 TabletServer 已经清理了 LakeDv，那么 S1 中被删除的行既没有物理 DV（S1 本身没有 Puffin DV），也没有逻辑 LakeDv 屏蔽——旧行会重新暴露出来，查询结果错误。

**清理流程**：TieringService 收齐所有 bucket 的 ready ack 后，向 CoordinatorServer 提交将新 snapshot 发布为 DV-readable 的请求；CoordinatorServer 完成对外发布后，通知各 TabletServer 执行 readable switch 与 LakeDv 清理；待 TieringService 收齐 switched ack 后，才放行下一轮 split。

**清理方式：bitmap 差集**。

在快照 LakeDv 到新 snapshot 成为 DV-readable 之间，可能有新的 `-U/-D` 到达并往同一个 file 的 bitmap 中追加了新的 bit。不能直接清空整个 file 的 bitmap，否则会丢失快照之后新增的删除。

对 LakeDv 中每个 file_id：

```
清理后的 bitmap = 当前 bitmap AND NOT 快照时的 bitmap
```

- 如果结果为空 bitmap，删除该 file_id 的条目。
- 如果结果非空，用差集 bitmap 替换当前 bitmap。

实现上，TabletServer 维护一个 `snapshotBitmap`（`Map<file_id, bitmap>`）。生成 split 时保存 LakeDv 快照副本（§7.1 步骤 3），position report 到达后过滤未物化的文件（§7.3 步骤 8）。收到 DV-readable 通知后，用 `snapshotBitmap` 执行差集运算，然后清空。由于保证 split n+1 的生成不会在 readable switch n 之前发生，`snapshotBitmap` 在任何时刻最多只有一份，不需要按 snapshotId 分组。

> **snapshotBitmap 与实际物化结果的对齐**：如果 Tiering Writer 因外部 compaction 过滤了 lakeDvSnapshot 中的部分文件（见 §7.2 lakeDvSnapshot 过时保护），这些文件的 DV 未被物化到 Iceberg。TabletServer 收到 Tiering Writer 上报的 `materializedDvFiles` 后，必须从 `snapshotBitmap` 中移除未物化的文件（见 §7.3 步骤 8）。否则，差集清理会错误地清除 LakeDv 中尚未物化的删除标记。

---

## 14. 端到端示例

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
key1 → [rowId=0][schemaId][v1]
key2 → [rowId=1][schemaId][v2]
key3 → [rowId=2][schemaId][v3]
```

DV 状态：全部为空（还没有删除操作，还没有 tiering）。

### Step 2：第一轮 Tiering

生成 tiering split：`{offset_range: [0, 2], lakeDvSnapshot: 空}`。

Tiering Writer 读 changelog offset 0~2，将 `+I` 记录写入 Iceberg data file：

```
data_file_A:
  pos0 → (key1, v1, __offset=0)
  pos1 → (key2, v2, __offset=1)
  pos2 → (key3, v3, __offset=2)
```

LakeDv 快照为空 → 无 Puffin DV 文件。

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
key1 → [rowId=4][schemaId][v4]
key2 → [rowId=1][schemaId][v2]
key3 → [rowId=2][schemaId][v3]
```

Changelog 同步成功后：
- 查 RowPosIndex：`oldRowId=0 → {file_A, pos0}` ✓ 找到
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
- 读 changelog `[tiered_offset+1=3, logEndOffset=4]`：
  - offset=3：`-U` → retract 类型，不输出
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

生成 tiering split：
1. 获取读锁
2. `log_hw = 5`，`latest_offset = 5`
3. 快照 LakeDv = `{file_A: {0, 2}}`
4. 释放读锁
5. tiering split = `{offset_range: [3, 5], lakeDvSnapshot: {file_A: {0, 2}}}`

Tiering Writer 处理：
- offset=3：`-U(oldRowId=0)` → `oldRowId=0 < last_tiered_offset=3` → 跳过（已在 LakeDv 快照中）
- offset=4：`+U(key1, v4)` → 写入新 data file，记录 positionReport: `(RowId=4, file_B, pos0)`
- offset=5：`-D(oldRowId=2)` → `oldRowId=2 < last_tiered_offset=3` → 跳过（已在 LakeDv 快照中）

生成：
```
data_file_B:
  pos0 → (key1, v4, __offset=4)

Puffin DV file（来自 LakeDv 快照）:
  file_A → {0, 2}  (pos0 和 pos2 被删除)
```

上报 position：
```
positionReport = {
  data_file_B: [(RowId=4, pos=0)]
}
```

Iceberg commit snapshot S2（tiered offset = 5）。

TabletServer 收到 commit 成功通知后：
- 用上报的 position 写入 **pendingRowPos**：`4 → {file_B, pos0}`
- **暂不更新 RowPosIndex，暂不清理 LakeDv**（S2 尚未成为 DV-readable）

TieringService 收齐所有 bucket 的 ready ack 后，向 CoordinatorServer 提交将 S2 发布为 DV-readable 的请求；CoordinatorServer 完成对外发布。

TabletServer 收到 DV-readable 通知后（§8.2 步骤 3）：
- **迁移 pendingRowPos → RowPosIndex**：`4 → {file_B, pos0}` 写入 RowPosIndex
- 对 LakeDv 执行 bitmap 差集清理（`file_A: {0, 2}` 已物化到 S2 的 Iceberg DV）
- 此时 union read 已切换到 S2，S2 自带物理 DV，LakeDv 清理安全

DV 状态：
```
RowPosIndex:
  1 → {file_A, pos1}
  4 → {file_B, pos0}

LakeDv: 空（S2 已成为 DV-readable 后清理）

LogDv: 清理 offset < S2_start_offset 的条目
```

---

## 15. 总结

| 维度 | 设计决策 |
|------|----------|
| **RowId** | 使用 `+I`/`+U` 的 log offset，天然唯一递增，与 `__offset` 列一致 |
| **RowPosIndex** | 始终表示当前 readable snapshot 的位置（8 bytes/行）；新 snapshot 的位置暂存 pendingRowPos（扁平结构，`RowId → FilePos`），readable 切换时原子迁移到 RowPosIndex 并清空；dictionary 编码文件路径 |
| **LakeDv** | 增量存储，每轮 tiering commit 后通过 bitmap 差集清理已物化的条目 |
| **LogDv** | Range-based bitmap，按固定 offset 间隔分段 |
| **存储** | DvRocksDB 独立于 KvTablet RocksDB，六个列族（RowPosIndex、PendingRowPos、LogDv、LakeDv、FileDict、PendingDeletes）；dvLock（ReadWriteLock）序列化写路径 + 保护读路径；position 上报天然幂等（PendingDeletes 不在处理时移除）+ 结构性过期检查拦截过期请求；PendingDeletes 在 readable 切换时统一清理 |
| **架构分工** | TabletServer 维护轻量元数据 + 快照 LakeDv；Tiering Writer 写 data file + 物化 Puffin DV |
| **DV 物化** | LakeDv 快照覆盖跨 split 删除；同 split 内先写后删通过 `logDvSnapshot` 写前过滤；commit 前过滤已被外部 compaction 替换的文件 + `validateDataFilesExist` 兜底；未物化的删除由 LakeDv 保底 |
| **Commit 验证** | IcebergLakeCommitter 从无校验改为 `validateFromSnapshot` + `validateDataFilesExist`；冲突时 abort 下轮重试 |
| **Position 构建** | Writer 上报（默认）+ Tiering Service 扫描外部 compaction 文件（兜底）；§7.3 统一处理路径（RowId 范围区分新写入行 vs 重写行）；PendingDeletes 解决 position report 与删除操作的时序间隙 |
| **Changelog 格式** | `-U`/`-D` 的 value 首部携带 oldRowId（8 bytes） |
| **KV State 格式** | 首部插入 RowId（8 bytes） |
| **Iceberg 数据列** | 新增 `__bucket` 列，用于外部 compaction 后识别行的 bucket 归属 |
| **Iceberg 版本** | 切换到 v3；新表强制 v3，存量 v2 表原地升级，历史 equality delete 仍有效 |
| **外部 Compaction** | Tiering Service 检测并扫描外部新文件，按 `__bucket` 分发 position report；oldFiles 清理推迟到 readable snapshot 前移 |
| **恢复** | 从 DvRocksDB checkpoint 加载，重放 changelog 增量 |
| **前置要求** | 主键表必须使用 FULL changelog 模式 |

---

## 附录 A：snapshotBitmap（LakeDv 差集清理）的作用与必要性

### 问题背景

LakeDv 记录的是"Iceberg 中哪些行被删了，但 Iceberg 自己还不知道"的逻辑删除标记。当 tiering commit 把 LakeDv 快照物化成 Puffin DV 文件后，这些删除 Iceberg 已经知道了——LakeDv 中对应的条目变成冗余，应该清理。

但清理方式有讲究：

- **不清理**：LakeDv 无限膨胀，每轮 tiering 重复物化已有删除到 Puffin DV，union read 多扫描冗余 bitmap。不影响正确性，但浪费存储和性能。
- **直接清空**：从快照 LakeDv 到清理之间（可能几分钟），新的 `-U/-D` 会往同一个文件的 bitmap 里追加新 bit。直接清空会把这些新 bit 一起丢掉——这些删除还没物化到 Iceberg，丢了就意味着旧行复活。**正确性问题**。

### 解决方案：bitmap 差集

```
清理后的 bitmap = 当前 bitmap AND NOT 快照时的 bitmap
```

只移除快照时已有的 bit（已物化），保留快照之后新增的 bit（未物化）。

`snapshotBitmap` 就是保存"快照时的 bitmap 副本"，用于在清理时做差集运算的右操作数。由于保证 split n+1 的生成不会在 readable switch n 之前发生，`snapshotBitmap` 在任何时刻最多只有一份，不需要按 snapshotId 分组。

### 具体示例

```
时刻 T1: 生成 split，快照 LakeDv = {file_A: {0, 2}}
         保存为 snapshotBitmap

时刻 T2: 新 -D 到达，LakeDv 变为 {file_A: {0, 2, 5}}     ← bit 5 是新增的

时刻 T3: S2 commit 成功，S2 的 Puffin DV 包含 {file_A: {0, 2}}

时刻 T4: S2 成为 DV-readable，执行差集清理：
         {0, 2, 5} AND NOT {0, 2} = {5}
         LakeDv 变为 {file_A: {5}}     ← bit 5 保留 ✓
         清空 snapshotBitmap
```

如果没有 `snapshotBitmap`（不知道快照时的 bitmap 是什么），要么不清理（膨胀），要么清空（bit 5 丢失，旧行复活）。

### 没有 snapshotBitmap 的替代方案

| 方案 | 可行性 | 问题 |
|------|--------|------|
| 不清理 LakeDv | 正确但低效 | LakeDv 无限膨胀，重复物化，union read 性能劣化 |
| 直接清空 LakeDv | **不可行** | 快照后新增的删除丢失，旧行复活 |
| 从 Iceberg 读 Puffin DV 反推已物化内容 | 理论可行 | TabletServer 需做远程文件 I/O，违背"只做轻量本地操作"的设计原则 |
| **保存快照副本做差集（当前方案）** | **正确且高效** | 一份本地内存副本，通常只有几分钟增量，成本很低 |

---

## 附录 B：恢复后冗余 LakeDv 条目的消除推演

### 问题

恢复场景下 `snapshotBitmap` 未被填充（§11.2 Step 4f），readable 切换时跳过差集清理。这导致 LakeDv 中残留已物化到 Iceberg DV 的冗余条目。本附录通过具体场景推演，证明这些冗余条目在下一轮正常 tiering 中被精确消除。

### 场景设定

**初始状态**：

- S2 为 readable snapshot（tiered offset = 50）
- DvRocksDB checkpoint 在 S2 readable switch 后触发
- checkpoint 时：RowPosIndex = `{10→file_A:pos0, 20→file_A:pos1, 30→file_A:pos2}`，LakeDv = `{}`，checkpointLogHw = 55

### checkpoint 之后、failover 之前发生的事

```
offset=56: DELETE key(RowId=10) → §6.2: LakeDv[file_A] += {0}, 删 RowPosIndex[10]
offset=57: DELETE key(RowId=20) → §6.2: LakeDv[file_A] += {1}, 删 RowPosIndex[20]

→ 生成 tiering split (50, 60]
→ snapshotBitmap = {file_A: {0, 1}}（§7.1 步骤 3）
→ lakeDvSnapshot = {file_A: {0, 1}} 随 split 下发

offset=58: +I(key4) → RowId=58
offset=59: DELETE key(RowId=30) → §6.2: LakeDv[file_A] += {2}, 删 RowPosIndex[30]
                                   ← bit {2} 在快照之后新增

→ Tiering Writer 处理 split:
  - +I 写入 file_B:pos0
  - lakeDvSnapshot {file_A: {0,1}} 物化为 Puffin DV
  - commit S3 (tiered offset = 60, Iceberg DV: file_A → {0,1})

→ position report 到达，§7.3 处理完成
→ snapshotBitmap 过滤后 = {file_A: {0,1}}

★ FAILOVER（readable switch 到 S3 之前）
  snapshotBitmap 丢失（内存），pendingRowPos 丢失（未 checkpoint）
```

此刻真实状态：
- LakeDv 应有 `{file_A: {0, 1, 2}}`，其中 {0,1} 已物化到 S3 的 Iceberg DV，{2} 未物化
- RowPosIndex 应为 `{}`（三条全删了）

### 恢复流程

**步骤 1**：加载 checkpoint

```
RowPosIndex = {10→file_A:pos0, 20→file_A:pos1, 30→file_A:pos2}
LakeDv = {}
snapshotBitmap = empty
```

**步骤 2-3**：从 offset=56 重放 changelog

```
offset=56: -D(oldRowId=10) → RowPosIndex[10]=file_A:pos0 ✓
           → LakeDv[file_A] += {0}, 删 RowPosIndex[10]
offset=57: -D(oldRowId=20) → RowPosIndex[20]=file_A:pos1 ✓
           → LakeDv[file_A] += {1}, 删 RowPosIndex[20]
offset=58: +I → 跳过（非 -U/-D）
offset=59: -D(oldRowId=30) → RowPosIndex[30]=file_A:pos2 ✓
           → LakeDv[file_A] += {2}, 删 RowPosIndex[30]
```

恢复后：

```
RowPosIndex = {}
LakeDv = {file_A: {0, 1, 2}}   ← bits {0,1} 冗余（已在 S3 Iceberg DV 中）
                                   bit {2} 有效（未物化）
snapshotBitmap = empty
```

**步骤 4**：targetSnapshot = S3

```
newFiles = {file_B}（S3 新增的 data file）
restoreTieredOffset = snapshotStartLogOffset - 1 = 50

扫描 file_B: RowId=58 at pos0
  → RowId=58 > 50 → 新行 → pendingRowPos[58] = file_B:pos0

上报 ready → CoordinatorServer
```

**Readable switch 到 S3**（CoordinatorServer 触发）：

```
§8.2 步骤 3:
  1. 迁移 pendingRowPos → RowPosIndex: {58→file_B:pos0}
  2. 清空 pendingRowPos
  3. oldFiles = {} （file_A 在 S2 和 S3 中都存在）
  4. PendingDeletes cleanup → 无
  5. snapshotBitmap = empty → 跳过差集清理（Step 4f）

结果：
  RowPosIndex = {58→file_B:pos0}
  LakeDv = {file_A: {0, 1, 2}}   ← bits {0,1} 仍冗余
```

### 下一轮正常 tiering（冗余消除）

**§7.1**：生成 split (60, 70]

```
快照 LakeDv → snapshotBitmap = {file_A: {0, 1, 2}}   ← 完整捕获，含冗余 bits
lakeDvSnapshot = {file_A: {0, 1, 2}} 随 split 下发
```

假设快照之后又来了一条删除：

```
offset=65: DELETE key(RowId=58) → §6.2: LakeDv[file_B] += {0}

此刻 LakeDv = {file_A: {0, 1, 2}, file_B: {0}}
                                    ↑ file_B 在快照之后新增
```

**Tiering Writer**：

```
lakeDvSnapshot = {file_A: {0, 1, 2}}
过滤 currentFiles: file_A 存在 → 保留
生成 Puffin DV for file_A: {0, 1, 2}
  （S3 已有 file_A 的 Puffin DV: {0,1}，新 DV 是超集，含冗余 bits，幂等安全）

commit S4
materializedDvFiles = [file_A]
```

**§7.3 步骤 8**：

```
用 materializedDvFiles 过滤 snapshotBitmap:
  file_A ∈ materializedDvFiles → 保留
snapshotBitmap = {file_A: {0, 1, 2}}
```

**Readable switch 到 S4**（§8.2 步骤 3）：

```
差集清理：
  当前 LakeDv[file_A] = {0, 1, 2}     （快照后没有新 bit 加到 file_A）
  snapshotBitmap[file_A] = {0, 1, 2}
  → {0, 1, 2} AND NOT {0, 1, 2} = {}  → 删除 file_A 条目 ✓

  file_B 不在 snapshotBitmap 中 → 不受影响
  LakeDv[file_B] = {0}                 → 保留（未物化）✓

清空 snapshotBitmap

结果：
  LakeDv = {file_B: {0}}   ← 冗余 bits {0,1} 已消除 ✓
                               有效 bit {2} 也已消除（已物化到 S4）✓
                               file_B:{0} 保留（下轮物化）✓
```

### 结论

冗余条目在下一轮正常 tiering 中被精确消除。关键路径：

1. **`snapshotBitmap` 完整捕获**：§7.1 快照 LakeDv 时，冗余 bits 和有效 bits 一视同仁，全部进入 `snapshotBitmap`。
2. **物化幂等安全**：Tiering Writer 物化 `lakeDvSnapshot` 时，冗余 bits 的 Puffin DV 是已有 Iceberg DV 的超集，Iceberg 处理时幂等。
3. **差集精确清除**：§8.2 步骤 3 的 `当前 bitmap AND NOT snapshotBitmap` 运算精确移除所有已物化 bits（含冗余），保留快照之后新增的未物化 bits。
