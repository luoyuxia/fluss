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

当一轮 tiering 完成后，新到达的 DELETE 和 UPDATE 记录会持续写入 Fluss。这些变更对应的旧行可能存在于两个位置：同在 Fluss 中的更早记录，或者已经 tiering 到 Iceberg 的历史数据。Log Deletion Vector 负责前者——标记 FLuss 内部已被后续操作覆盖或删除的行，确保联合查询时不会读到 WAL 中已过时的版本。后者（旧行已在 Iceberg 中）则由 Lake Deletion Vector 负责。

### 2.3 Lake Deletion Vector（第三层）

连接实时层与历史层的桥梁。当 Fluss 收到一条针对已 tiering 到 Iceberg 的行的删除或更新时：

- TabletServer 在 LakeDv 中记录逻辑删除标记（datafile → 被删除的 row position bitmap）。
- 该逻辑删除在联合查询（union read）时**立即生效**，无需等待下一次 Iceberg snapshot 写入。
- 这些逻辑删除会在下一轮 tiering commit 时，由 Tiering Writer 物化为 Iceberg 中的物理 deletion vector（Puffin 文件）。

### 2.4 联合查询语义

联合查询（Fluss 热数据 + Iceberg 历史数据）时，查询引擎同时应用三层 deletion vector：

- **Iceberg Deletion Vector**：屏蔽 Iceberg 中已物化的删除行。
- **Lake Deletion Vector**：屏蔽 Iceberg 中已在 Fluss 侧逻辑删除但尚未物化的行。
- **Log Deletion Vector**：屏蔽热层 Fluss 中已被后续操作覆盖或删除的行。

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

**关键设计决策：双 CF `RowPosIndex` + `pendingRowPos`，hard-link SST 复用消除迁移数据拷贝。**

| CF              | Key (RowId)   | Value (FilePos)    | 含义                                           |
|-----------------|---------------|--------------------|-----------------------------------------------|
| `RowPosIndex`   | `rowId1`      | `{file_A, pos5}`   | 当前 readable snapshot 的 position             |
| `pendingRowPos` | `rowId1`      | `{file_B, pos7}`   | 尚未 readable 的新 position（下次切换后合并）     |

Key 编码：8 bytes RowId（大端）。Value：8 bytes FilePos（4 bytes file_id + 4 bytes row_position）。

**设计动机——为什么需要两个 CF**：

假设 readable snapshot S_old 中 rowId=R 位于 file_A:pos5。新 snapshot S_new 到达但尚未 readable，若直接覆盖 RowPosIndex[R] 为 file_B:pos7，此时对 R 来了一个 delete，§6.2 查 RowPosIndex 命中 file_B:pos7，在 LakeDv 中标记 file_B 而非 file_A。但 union read 仍然读 S_old（只扫 file_A），file_A:pos5 没有任何屏蔽标记——旧行重新暴露，删除失效。

引入 `pendingRowPos`：§7.3.1 将新 position 写入 `pendingRowPos`（不动 RowPosIndex）。§6.2 遍历 `RowPosIndex` 和 `pendingRowPos` 各做一次 point get，**同时标记新旧两个文件**——无论 union read 当前读哪个 snapshot 都安全。readable switch 时将 `pendingRowPos` 的全部 entry 搬到 `RowPosIndex`。

**存储方案**：

- `RowPosIndex` 和 `pendingRowPos` 各占 DvRocksDB 一个列族，Key/Value 编码相同。
- data file 文件名包含 UUID，是很长的字符串，因此需要进行 **dictionary 编码**，将文件名转成 int 类型。编码映射关系记录在 DvRocksDB 的 FileDict 列族中。
- **fileId 由 Tiering Service 的全局 FileDictAllocator 统一分配**（见 §7.2.4），保证同一 `file_path` 在所有 TabletServer 的 FileDict CF 中具有一致的 `fileId`。

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

**生命周期管理**：当数据湖 readable snapshot advance 后， **range 的结束 offset < `start_logOffset`** 的整段 `offset_range` 条目才可以清理。


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

流程：Tiering Writer 提交新 snapshot 后，TieringService 先通知各 TabletServer 处理本 bucket 的 DV 元数据。各 TabletServer 处理完成后，向 TieringService 响应 **ready ack**。TieringService 收齐所有 bucket 的 ready ack 后，向 CoordinatorServer 提交“该 snapshot 可发布为 DV-readable”的通知；CoordinatorServer 将该 snapshot 标记为 DV-readable（更新 LakeTableZNode），并对外发布。
此时 client 可以开始以该 snapshot 作为目标 snapshot 发起 union read。随后 CoordinatorServer 再通知各 TabletServer 执行 readable switch。各 TabletServer 完成 readable switch 后，向 CoordinatorServer 发送 **switched ack**。
只有当 CoordinatorServer 收齐所有 bucket 的 switched ack 后，才允许生成下一轮 tiering split。

在 snapshot 从“已提交”到“所有 TabletServer 都完成 readable switch”的窗口内，部分 TabletServer 仍可能返回旧的 `currentReadableSnapshot`。这不会影响正确性：client 不回退到旧 snapshot，而是继续对目标 snapshot 重试；
TabletServer 完成切换后，请求自然收敛成功。在此窗口内，TabletServer 必须保留旧 snapshot 对应的 LakeDv，直到本地完成 readable switch 后才能清理。

**CoordinatorServer barrier 机制**：

- **Phase 1 / ready**：TieringService 先发起本轮 bucket 级 DV 元数据处理。TabletServer 完成 §7.3.1 的 position report 处理、`snapshotBitmap` 过滤后，向 TieringService 发送 ready ack，表示"本 bucket 的 DV 元数据已就绪，但尚未完成 readable switch"。
- **Phase 2 / publish + switch**：TieringService 收齐 ready ack 后，向 CoordinatorServer 提交发布请求；CoordinatorServer 将该 snapshot 标记为 DV-readable 并对外发布。随后 CoordinatorServer 通知所有 TabletServer 执行 §7.3.3 的 readable switch。TabletServer 完成 oldFiles 清理、PendingDeletes 清理、LakeDv 差集清理和过期 LogDv 清理后，返回 switched ack 给 TieringService。
- **Phase 3 / next split gate**：CoordinatorServer 只有在收齐所有 bucket 的 switched ack 后，才允许生成下一轮 split。

**单飞 / 强取消语义**：

- **单飞约束**：同一 tiering split 在任意时刻最多只允许一个有效 attempt。
- **显式失败后才重试**：retry 只能在 CoordinatorServer **明确宣告**当前 attempt 失败后启动；超时、网络抖动或短暂无响应都不能直接触发新的 attempt。
- **强取消语义**：被 CoordinatorServer 宣告失败的旧 attempt 必须被强制取消；取消后不得再向任何 TabletServer 发送 `positionReport`、ready ack 或 switched ack 相关请求。
- **attemptEpoch（本地硬校验）**：仅靠上述协议保证无法防御网络延迟导致的跨 attempt 乱序。若旧 attempt A 的 positionReport 因网络延迟在新 attempt B 之后到达，A 的 FilePos 会覆盖 B 在 pendingRowPos 中的正确 entry（RocksDB 更高 sequence number），导致 §6.2 为错误文件补打 LakeDv、B 的文件中死行复活。因此引入 `attemptEpoch`——CoordinatorServer 为每个 bucket 维护**全局单调递增**的 epoch 计数器（跨 split 不重置），每次发起新 attempt（无论是同一 split 的 retry 还是新 split 的首次 attempt）都 epoch +1，写入 positionReport。TabletServer 维护 `pendingAttemptEpoch` 做本地三路校验（见 §7.3.1 步骤 0）：epoch 过小则拒绝（旧 attempt 或旧 split 延迟到达），epoch 更大则重置所有 pending 状态后继续，epoch 相等则幂等重试。由于 epoch 全局单调递增，此单一检查同时覆盖"旧 split 延迟到达"和"同 split 旧 attempt 延迟到达"两种场景，无需额外的结构性过期检查。`actualSnapshotId` 保留用于 ready ack / switched ack 关联和排障。

**时序保证**：

1. S_{n+1} 的 position report 不会在 S_n 的 readable 切换完成之前到达 TabletServer。这一保证由 CoordinatorServer 的两阶段 ack barrier 提供，使得 `pendingRowPos` 在 readable switch 时可以一次性合并到 `RowPosIndex` 并清空，不会跨 snapshot 堆积。
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

**统一规则**：所有四种 changelog 记录（`+I`、`+U`、`-U`、`-D`）的 value 首部均携带 8 字节 RowId，与 KV state value 格式保持一致。

```
之前的 +I value：[schemaId][BinaryRow]
之后的 +I value：[RowId(8 bytes)][schemaId][BinaryRow]

之前的 +U value：[schemaId][BinaryRow(新值)]
之后的 +U value：[RowId(8 bytes)][schemaId][BinaryRow(新值)]

之前的 -U value：[schemaId][BinaryRow(旧值)]
之后的 -U value：[RowId(8 bytes)][schemaId][BinaryRow(旧值)]

之前的 -D value：[schemaId][BinaryRow(旧值)]
之后的 -D value：[RowId(8 bytes)][schemaId][BinaryRow(旧值)]
```

- **`+I`/`+U`**：RowId 等于**本条记录自身的 log offset**，由 writer 在写入时填入。
- **`-U`/`-D`**：RowId 是**被覆盖/删除的旧版本的 RowId**，从 KV state 的旧 value 首部直接读出后原样写入 changelog value，无需额外拼接。

> **为什么 `+I`/`+U` 也记录 RowId 而不是按需从 offset 推导**：虽然 `+I`/`+U` 的 RowId 在语义上等于自身 log offset、理论上可省略存储，但统一所有四种记录的 value 格式带来的收益更大：(1) 消费方（TabletServer、union read client、恢复流程）无需按 record type 分支处理，读 value 就能拿到 RowId；(2) 避免"RowId = log offset"这一隐式约束在所有消费路径上被重复实现，降低 bug 概率；(3) 未来若需要让 RowId 与 log offset 解耦（例如支持非 log-offset 编码），格式已就位，无需再做 on-wire 变更。单条记录多 8 字节的存储开销相对于整体 payload（key + BinaryRow + 元信息）通常 < 10%，可接受。

**使用方**：

- **TabletServer**：
  - 处理 `-U`/`-D`（changelog 同步成功流程，§6.2）：从 value 首部提取 RowId（即被删除版本的 RowId），查 RowPosIndex 更新 LakeDv 和 LogDv。
  - 处理 `+I`/`+U`（KV state 写入时）：将 RowId（= 当前 log offset）写入 value 首部，随 KV state 一起落盘；随后生成 changelog 时复用 value 首部，无需再计算。
- **Tiering Writer**：从 `+I`/`+U` value 首部提取 RowId 写入 Iceberg `__rowid` 列。

### 4.3 Iceberg 数据列扩展

Tiering 写入 Iceberg data file 时，除了用户数据列外，还写入以下系统列：

- **`__rowid`**：该行对应的 `+I`/`+U` changelog log offset，即 RowId。已有列，用于外部 compaction 后识别行的 rowid 
- **`__bucket`**：该行所属的 Fluss bucket id（int 类型）。**新增列**，用于外部 compaction 后识别行的 bucket 归属（见 §11），避免通过主键哈希反算 bucket。

> **约束**：`__rowid` 和 `__bucket` 是 DV 正确性的基础。外部引擎对 Fluss 管理的 Iceberg 表执行 compaction 或 rewrite 时，**必须保留这两列及其值**。如果这两列被丢弃或篡改，Fluss 将无法重建 position 映射或正确路由 bucket，导致删除标记失效、数据复活。

### 4.4 Iceberg 版本

从 Iceberg v2 切换到 v3，使用 position delete（Puffin DV）替代 equality delete。

**新表**：启用 DV 功能时，`IcebergLakeCatalog` 创建 Iceberg 表时设置 `format-version=3`。当前代码未显式设置 format-version（默认为 v2），需要在 `createTable` 时增加 `TableProperties.FORMAT_VERSION = "3"` 的设置。

### 4.5 前置要求：FULL Changelog 模式

DV 功能要求主键表使用 **FULL changelog 模式**（即更新时同时写 `-U` 和 `+U`）。WAL changelog 模式下，更新只写 `+U` 不写 `-U`，无法获知被覆盖的旧版本 RowId，因此无法定位 Iceberg 中的旧行进行删除标记。

创建主键表时，如果启用了 DV 功能，系统应校验 changelog 模式为 FULL，否则拒绝创建。

---

## 5. 存储架构

### 5.1 DvRocksDB

RowPosIndex、LogDv、LakeDv 作为不同的列族（Column Family），保存在一个独立的 RocksDB 实例中，记为 **DvRocksDB**。文件路径字典编码保存在另一个列族中。

```
DvRocksDB
├── CF: RowPosIndex     — RowId (8 bytes) → FilePos (8 bytes)
│                        当前 readable snapshot 的 position
├── CF: pendingRowPos   — RowId (8 bytes) → FilePos (8 bytes)
│                        尚未 readable 的新 position（readable switch 时合并到 RowPosIndex）
├── CF: LogDv           — offset_range → del_bitmap
├── CF: LakeDv          — file_id (4 bytes) → del_bitmap (RoaringBitmap)
├── CF: FileDict        — file_path (string) → file_id (int)
│                        file_id (int) → file_path (string)（反向映射）
└── CF: PendingDeletes  — RowId (8 bytes) → FilePos (8 bytes)
                         FilePos 编码为 {fileId (4B), pos (4B)}
                         sentinel {0, 0} 表示"未知位置"（-U/-D 到达时 RowPosIndex 和 pendingRowPos 均未命中）
                         非 sentinel 值表示该 RowId 当前在 LakeDv 中未物化的"最新位置"
```

**与 KvTablet RocksDB 分离的原因**：
- DV 的 checkpoint/恢复流程与 KV 数据的 checkpoint 互相独立，不会互相干扰。
- DV 的生命周期与 KV 数据不同（DV 与 Iceberg snapshot 绑定）。
- 可以独立调优 DV RocksDB 的参数（如 compaction 策略、block cache 大小）。

**PendingDeletes 列族**：

PendingDeletes 是**完整的"未物化死行日志"**——追踪所有已经被 §6.2 / §10.2 changelog replay 处理过但其对应的 LakeDv 删除标记尚未物化到 Iceberg DV 的 RowId。它有两个核心用途：

1. **时序兜底（Case Y）**：当 `-U/-D` 到达时，被删除行可能正在被 tiering（position report 尚未到达），`RowPosIndex` 和 `pendingRowPos` 均未命中，无法更新 LakeDv。此时将 `oldRowId` 记入 PendingDeletes（值为 sentinel `{0, 0}`），后续 position report 到达时由 §7.3.1 的**反向扫描**补齐 LakeDv。

2. **外部 compaction 检测（Case X）**：`-U/-D` 命中 CF 时也写入 PendingDeletes（值为命中的 `{fileId, pos}`）。后续 position report 到达时，§7.3.1 反向扫 PendingDeletes 查 `pendingRowPos`，一旦发现 `pendingRowPos[R]` 被新 SST Ingest（意味着外部 compaction 将 R 重写到新文件且 tiering 捕获了其 position），就能精确识别并为新位置补打 LakeDv 标记，而无需在 SST 的每个 entry 上做 alive check。

PendingDeletes 的值（当前位置 `{fileId, pos}`）在反向扫描命中时会被**更新**为最新的 `pendingRowPos` 返回值，以支持多跳外部 compaction；在 §7.3.3 readable switch 时，若该位置已被物化（`snapshotBitmap` 包含该 `{fileId, pos}` bit），对应的 PendingDeletes 条目被清理。详细流程见 §6.2、§7.3.1、§7.3.3。

**并发控制：DvRWLock（读写锁）**：

DvRocksDB 的并发控制采用一把 **DvRWLock（全局读写锁）**：所有写路径（§6.2 / §7.3.1 / §7.3.3）获取**写锁**互相串行化，union read（§9）获取**读锁**与写路径互斥、读路径之间并行。

| 持锁路径                           | 章节           | 锁类型                 | 操作                                                              |
|--------------------------------|--------------|---------------------|------------------------------------------------------------------|
| Changelog 同步成功                 | §6.2 步骤 3    | DvRWLock 写锁，整批 `-U/-D` 处理期间持有 | RowPosIndex + pendingRowPos 各 point get + delete、**统一写 PendingDeletes（命中时记 `{fileId, pos}`，未命中时记 sentinel `{0,0}`）**、LakeDv、LogDv |
| Position 上报（含外部 compaction）    | §7.3.1 Phase 2 | DvRWLock 写锁，Ingest + 善后处理期间持有 | 写入 newFileDictEntries 到 FileDict、Ingest SST → pendingRowPos CF、记录 hard-link SST 副本、**反向扫 PendingDeletes + 查 pendingRowPos**（替代原先对 SST 每个 entry 的正向 alive check） |
| Readable 切换                    | §7.3.3         | DvRWLock 写锁          | Ingest pendingSstFiles → RowPosIndex、重建 pendingRowPos CF、清理 oldFiles LakeDv、**基于 `snapshotBitmap` 清理 PendingDeletes（删除值已被物化的条目）**、bitmap 差集、清理过期 LogDv、更新 `readableSnapshotId` |
| Union Read                     | §9         | DvRWLock 读锁          | 读 `readableSnapshotId`、按查询涉及的 fileId 从 LakeDv clone 出 bitmap 子集、读 LogDv 范围 |

**为什么读写锁就够**：
- §6.2 已在 KvTablet 写锁内串行处理所有 `-U/-D`，§7.3.1 是低频 RPC（每轮 tiering 仅一次），§7.3.3 频率更低（仅在 readable snapshot 前移时）。三者都持 DvRWLock 写锁，实际并发度极低，互斥开销可忽略。
- Union read 持 DvRWLock 读锁，期间对 LakeDv 的读取和 bitmap 子集 clone 在锁内完成；**序列化和网络发送放在释放锁之后**，锁的临界区保持在 ms 级。
- 写路径的临界区同样很短：§6.2 的 DV 修改、§7.3.1 Phase 2 的 hard-link + Ingest + 反向扫 PendingDeletes（O(|PendingDeletes|)）、§7.3.3 的 Ingest + DropCF + 差集，均为 O(1)~O(|PendingDeletes|) 的元数据操作。读写锁下的读-写相互阻塞可忽略。
- 如果未来读 QPS 变大到被写路径的写锁压出长尾延迟，可以升级为 CoW 快照（volatile 引用一个不可变 `DvReadableState` 对象，union read 通过 volatile read 获取）——此优化与正确性无关，仅用于降低读延迟。

**锁顺序**：§6.2 遵循 `KvTablet.writeLock → DvRWLock.writeLock`；§9 遵循 `KvTablet.readLock → DvRWLock.readLock`；§7.3.1 和 §7.3.3 只获取 `DvRWLock.writeLock`，无锁顺序问题。

**§6.2 的一致性关键**：§6.2 在 KvTablet 写锁内先获取 DvRWLock 写锁，完成 LakeDv / LogDv / PendingDeletes 修改后释放 DvRWLock 写锁，最后才更新 `log_hw` 并释放 KvTablet 写锁。union read 在 KvTablet 读锁保护下看不到 "`log_hw` 已更新但 DV 尚未更新" 的中间状态，读到的 `logEndOffset` 与 DvRocksDB 当前内容天然一致。§7.3.1 / §7.3.3 不持有 KvTablet 锁，仅持 DvRWLock 写锁，union read 通过 DvRWLock 读锁与其互斥。

**幂等机制：天然幂等 + attemptEpoch 校验**：

Position 上报（§7.3.1）的处理过程是**天然幂等**的——所有操作（pendingRowPos 写入、LakeDv bitmap set、反向扫 PendingDeletes、FileDict 写入）在重复执行时产生相同结果。关键在于 §7.3.1 步骤 7 **不移除** PendingDeletes 条目（仅更新其值为最新位置）：重试时条目仍在，反向扫再次走同样的路径，LakeDv bit-set 幂等，pendingRowPos 删除幂等，PendingDeletes 值覆盖为相同值也幂等。PendingDeletes 的清理推迟到 readable 切换时统一执行（见 §7.3.3 步骤 3）。另外 SST 由 Tiering Service 不可变产出（见 §7.2.4），同一 attempt 重试复用相同 `sstDir`，TabletServer 的 Ingest 对同一 SST 幂等。

Position 上报（§7.3.1 步骤 0）通过 **attemptEpoch 三路校验**拦截过期和乱序请求。`attemptEpoch` 由 CoordinatorServer 为每个 bucket 全局单调递增分配（跨 split 不重置），positionReport 携带该 epoch。TabletServer 维护 `pendingAttemptEpoch`，对到达的 epoch 做三路比较：小于则拒绝（旧 split 或旧 attempt 延迟到达）、大于则重置 pending 状态后继续（新 attempt 或新 split 取代旧状态）、等于则幂等重试。由于 epoch 全局单调递增，此单一检查同时覆盖"旧 split 延迟到达"和"同 split 旧 attempt 延迟到达"两种场景。详见 §3.6 和 §7.3.1 步骤 0。

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
3. **获取 DvRWLock 写锁**（见 §5.1 并发控制）
4. 遍历 PrewriteBuffer flush 下去的每一行 entry，如果是 `-U` / `-D`：
   - a. 查 `RowPosIndex` 和 `pendingRowPos` 各做一次 point get（`get(oldRowId)`），收集所有命中的 FilePos：
     - **至少命中一条（Case X）**：对每条命中的 `{file_id, row_position}`，在 LakeDv 中将 `row_position` 加入 `file_id` 对应的 del_bitmap，并从对应 CF 中删除该 entry。两个 CF 都可能命中（外部 compaction 窗口期：`RowPosIndex` 有旧 position，`pendingRowPos` 有新 position），需要同时标记新旧两个文件。**若 `pendingRowPos` 命中，还需 `pendingDeletedRowIds.add(oldRowId)`**（`pendingDeletedRowIds` 是内存集合，收集窗口期内从 pendingRowPos 删除的 RowId，供 §7.3.3 readable switch 时清除 RowPosIndex 中因 hard-link Ingest 复活的孤儿 entry——hard-link SST 不含 pendingRowPos CF 的 tombstone，直接 Ingest 会把已删除的 entry 重新灌入 RowPosIndex）。**同时写入 PendingDeletes[oldRowId] = {fileId, pos}**，值取 `pendingRowPos` 命中项（若其命中）或 `RowPosIndex` 命中项（优先取更"新"的位置——pendingRowPos 对应最近 tiering 提交到 Iceberg 的新文件，更可能在未来被外部 compaction 影响）。
     - **全部未命中（Case Y）**：该行可能正在被 tiering（position report 尚未到达），**将 `oldRowId` 加入 PendingDeletes，值为 sentinel `{0, 0}`**。后续 position report 到达时，§7.3.1 反向扫 PendingDeletes 并用 `pendingRowPos` 查到新 Ingest 的位置，补齐 LakeDv 并把 sentinel 更新为实际位置。
   - b. 用 `oldRowId` 更新 LogDv：将 `offset = oldRowId` 对应的 changelog 标记为已删除
5. **释放 DvRWLock 写锁**
6. 更新 `log_hw`（high watermark）
7. **释放 KvTablet 写锁**

> **关于步骤顺序的说明**：必须先更新 DV、释放 DvRWLock 写锁、最后更新 `log_hw`。如果先更新 `log_hw`，union read 可能看到更大的 `logEndOffset`，但 LakeDv 还没更新到对应位置，导致重复读出已被删除的数据。union read 获取 DvRWLock 读锁与 §6.2 的写锁互斥，因此只会看到"DV 已更新 + log_hw 已更新"的一致状态。

> **关于加锁的说明**：DvRWLock 写锁在 KvTablet 写锁内获取，整批 `-U/-D` 处理期间持有。由于 §7.3.1 频率极低（每轮 tiering 仅一次），DvRWLock 写锁几乎不会与 §7.3.1/§7.3.3 产生竞争；与 union read 的读锁之间会短暂互斥，但 union read 临界区仅做范围读和 bitmap 子集 clone（见 §9），通常在 ms 级完成。

> **为什么 Case X 也写 PendingDeletes**：PendingDeletes 承担双重角色——不仅是 Case Y 的时序兜底，也是 §7.3.1 "外部 compaction alive check" 的**反向索引**。§7.3.1 Phase 2 只需反向扫 PendingDeletes（`|PendingDeletes|` 远小于本批 SST 的 entry 数量）并对每个 RowId 查一次 `pendingRowPos`，即可精确处理所有因外部 compaction 而被重写的死行，无需对 SST 里每一行都做 `RowPosIndex.get()` alive check。关键点：SST 里的外部 compaction 重写行，其 RowId 若已被 `-U/-D` 删除，必然已经在 PendingDeletes 中——因为 §6.2 处理 `-U/-D` 时无论是否命中 CF 都会写入 PendingDeletes。详细证明见 §7.3.1 步骤 7。

---

## 7. Tiering 与 Snapshot 处理

### 7.1 端到端总览

一轮 tiering 完成后，readable snapshot 从 S_old 前移到 S_new 的端到端流程涉及三个组件，通过**两阶段 ack barrier** 协调：

```
newFiles = snapshot_files(S_new) - snapshot_files(S_old)   // S_new 新增的文件
oldFiles = snapshot_files(S_old) - snapshot_files(S_new)   // S_old 中已被替换或删除的文件
```

**端到端时序**

```
 TieringService               CoordinatorServer            TabletServer (每个 bucket)
 (Flink Job)                  (Fluss 元数据中心)             (本地执行)
─────────────────────────────────────────────────────────────────────────────────────

 Phase A: Position Report 分发
 ┃
 ┃  commit S_new 到 Iceberg
 ┃  生成 SST + positionReport
 ┃──── positionReport RPC ─────────────────────────────→  收到 positionReport
 ┃                                                        执行 §7.3.1 全流程：
 ┃                                                          步骤 0: attemptEpoch 校验
 ┃                                                          Phase 1: 下载 SST
 ┃                                                          Phase 2: Ingest → 反向扫 PendingDeletes
 ┃                                                          过滤 snapshotBitmap (步骤 9)
 ┃←─────────── ready ack ──────────────────────────────  发送 ready ack
 ┃
 ┃  ── barrier: 等待所有 bucket 的 ready ack ──
 ┃
 Phase B: 发布 DV-Readable
 ┃
 ┃── 请求发布 S_new ──→  更新 LakeTableZNode
 ┃                       标记 S_new 为 DV-readable
 ┃                       (client 可开始以 S_new 做 union read)
 ┃                       ── readable switch 通知 ──→  执行 readable switch：
 ┃                                                      1. pendingRowPos → RowPosIndex
 ┃                                                      2. 清理 oldFiles
 ┃                                                      3. snapshotBitmap 清理 PendingDeletes
 ┃                                                      4. bitmap 差集清理 LakeDv
 ┃                                                      5. 清理过期 LogDv
 ┃                                                      6. 更新 readableSnapshotId
 ┃←─────────── switched ack ───────────────────────  发送 switched ack
 ┃
 ┃  ── barrier: 等待所有 bucket 的 switched ack ──
 ┃
 允许生成下一轮 split
```

**两阶段 ack 的作用**

| | ready ack | switched ack |
|--|-----------|-------------|
| **含义** | "我的 DV 元数据已就绪，client 来问我可以正确回答" | "我已完成本地切换，旧 snapshot 相关状态已清理" |
| **卡住什么** | 卡住 CoordinatorServer 发布 S_new 为 DV-readable | 卡住下一轮 split 生成 |
| **为什么需要** | 防止 client 查到 S_new 时 TabletServer 还没准备好 | 保证 `snapshotBitmap` 最多一份——S_new+1 的 split 不能在 S_new switch 之前生成 |

**为什么不能在 positionReport 阶段就完成 readable switch**：

一种看似更简单的设计是：TabletServer 收到 positionReport 后一次性完成所有操作（Ingest SST → 合并到 RowPosIndex → 清理 oldFiles LakeDv → diff 清理 → 更新 readableSnapshotId），然后 TieringService 收齐 ack 后通知 CoordinatorServer 发布。这样可以省掉 pendingRowPos、hard-link、两阶段 ack 等机制。但这个方案有两个致命问题：

1. **部分成功导致状态撕裂**：若 5 个 bucket 中 bucket 0-3 的 TabletServer 已完成 switch（readableSnapshotId = S_new），bucket 4 失败，则：bucket 0-3 只能服务 S_new（LakeDv 已清理，无法服务 S_old），bucket 4 只能服务 S_old（未切换），CoordinatorServer 未发布（未收齐 ack）。client 无论用 S_old 还是 S_new 都无法完成查询——整张表不可用，直到 bucket 4 恢复。当前设计中，positionReport 阶段不修改 RowPosIndex、不清理 LakeDv，所有 bucket 始终可用 S_old 服务，部分 bucket 失败不影响整体可用性。

2. **attempt 失败无法干净回滚**：当前设计中 attempt 失败时，通过 `DropColumnFamily(pendingRowPos)` 即可 O(1) 清除所有新 attempt 的数据，RowPosIndex 零污染。若已合并到 RowPosIndex，S_new 的 entry 与 S_old 的 entry 混在一起，无法区分和剔除——RowPosIndex 被永久污染，后续 retry 的新 attempt 数据与残留旧 attempt 数据叠加，结果不可预测。

因此 `pendingRowPos` 作为独立 CF 的设计是必要的：它既保证 §6.2 的 dual-check 能同时标记新旧两个 snapshot 的文件（见 §3.3 设计动机），又提供原子回滚能力（DropColumnFamily），使得 positionReport 阶段的所有操作对 RowPosIndex 完全无侵入。

**可优化的点——消除 publish 后的 stale error 窗口**：

当前设计中，CoordinatorServer 发布 S_new 后到 TabletServer 完成 readable switch 之间，client 用 S_new 请求会收到 stale error（`requestedSnapshotId > readableSnapshotId`）。但实际上，TabletServer 在 ready ack 时已具备服务 S_new 的能力——union read 不依赖 RowPosIndex（只用 LakeDv + LogDv），而 LakeDv 在 diff 清理之前是 S_old 和 S_new 的超集，对两个 snapshot 都正确（S_new 的 client 会收到一些已物化的冗余 LakeDv 条目，多屏蔽不丢数据）。可通过在 positionReport 完成后记录 `pendingReadableSnapshotId = S_new`，将 snapshot 一致性校验放宽为同时接受 `readableSnapshotId` 和 `pendingReadableSnapshotId`，使 publish 后所有 TabletServer 立即可服务 S_new，readable switch 变为不影响可用性的后台清理。

### 7.2 Tiering 流程 (TieringService 侧)

#### 7.2.1 生成 Tiering Split

Tiering split 定义了本轮 tiering 需要处理的 changelog 范围：`(last_tiered_offset, latest_offset]`。

- `last_tiered_offset`：上一轮 tiering 成功处理的**最后一条** changelog 的 offset（含义是"已完成到此"）。
- 当前 split 从 `last_tiered_offset + 1` 开始（左开），到 `latest_offset` 结束（右闭）。
- 首次 tiering 时，`last_tiered_offset = -1`，split 从 offset 0 开始。

生成 tiering split 时，**同时快照 LakeDv**：

1. **获取 KvTablet 读锁**（保证 LakeDv 与 `log_hw` 一致）
2. 读取当前 `log_hw` 作为 `latest_offset`
3. 快照当前 LakeDv 的全部内容，并通过 FileDict 将 `file_id` 反向映射为 `file_path`。同时将快照的 `Map<file_id, bitmap>` 副本保存在内存变量 `snapshotBitmap` 中（用于后续 §12.3 的差集清理），并保留一份原始拷贝 `snapshotBitmapRaw`（用于 §7.3.1 步骤 0 attemptEpoch 重置时恢复——§7.3.1 步骤 9 会就地过滤 `snapshotBitmap`，attempt 重试需要未过滤的版本）。由于保证 split n+1 的生成不会在 readable switch n 之前发生，`snapshotBitmap` 在任何时刻最多只有一份，直接覆盖即可。
4. **释放读锁**
5. 生成 tiering split：`{offset_range: (last_tiered_offset, latest_offset], lakeDvSnapshot: {file_path → bitmap, ...}}`

> **LakeDv 快照使用 file_path 而非 file_id**：`file_id` 是 TabletServer 内部 DvRocksDB 的字典编码，Tiering Writer 无法解析。TabletServer 在生成快照时利用本地 FileDict 将 `file_id` 解析为 `file_path`，随 split 下发的是 `{file_path → bitmap}`。Tiering Writer 直接用 `file_path` 生成 Puffin DV 文件，无需访问 FileDict。

**为什么 LakeDv 快照与 tiering split 天然对齐**：

- LakeDv 积累的是自上次 tiering commit 以来**尚未被 bitmap 差集清理**的删除。通过 §12.3 的清理机制，已物化到 Iceberg DV 的条目在 readable snapshot 前移后被差集移除，因此 LakeDv 中的内容实际覆盖的是"上次清理以来的所有新增删除"。
- `log_hw` 是 LakeDv 已经处理到的位置（changelog 同步成功流程中，先更新 LakeDv 再更新 `log_hw`）。
- 因此在读锁保护下，LakeDv 的内容精确覆盖了所有尚未物化到 Iceberg DV 的逻辑删除，与 tiering split 的 changelog 范围对齐。

#### 7.2.2 Tiering Writer 处理

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

5. Pre-commit：生成 RowPosIndex SST 并上传远程（见 §7.2.4 FileDictAllocator）：
   ├── 对 positionReport 中每个 file_path，向 FileDictAllocator 查找/分配 fileId
   ├── 对新分配的 (fileId → file_path) 条目收集为 `newFileDictEntries`
   ├── 收集 `materializedDvFiles`（= 步骤 4 过滤后实际生成 Puffin DV 的文件列表）
   ├── 每个 bucket 生成一个随机 UUID
   ├── SstFileWriter 生成 SST（key=RowId 排序，value=fileId+row_position）到本地临时路径
   ├── 上传 SST 到远程 `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/sst_0.sst`（可能产生多个 SST）
   │   其中 `$remoteLakeTableSnapshotDir` = `FlussPaths.remoteLakeTableSnapshotDir()`
   │   与现有 `{$remoteLakeTableSnapshotDir}/metadata/{UUID}.offsets` 平级
   ├── 写入 per-bucket manifest 文件到 `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/manifest`，记录：
   │     本次所有 SST 文件名、`newFileDictEntries`、`splitOffsetRange`、`materializedDvFiles`
   └── 写入 cross-bucket index 文件到 `{$remoteLakeTableSnapshotDir}/rowPos/{indexUuid}`，记录：
         每个 bucketId 对应的 sstDir（= `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`）
         index 在 commit 前写入，保证 committed → index + SST + manifest 均已存在

6. Commit（见下方 Commit 验证与冲突处理）：
   ├── RowDelta rowDelta = table.newRowDelta()
   ├── rowDelta.validateFromSnapshot(baseSnapshotId)
   ├── rowDelta.validateDataFilesExist(lakeDvReferencedFiles)  // LakeDv 引用的已有文件
   ├── rowDelta.addRows(dataFiles)
   ├── rowDelta.addDeletes(dvFiles)
   └── rowDelta.commit()   // 失败则 abort，见冲突处理
   commit 时远程 SST + manifest + index 已存在，保证 committed → position metadata 可恢复。
   Iceberg snapshot property 中记录 `indexUuid`（用于 post-commit reconcile 和恢复定位 SST，见 §7.2.4、§10.2）和 `fluss.nextFileId`（用于 Tiering Service 重启后恢复 FileDictAllocator 计数器）。

7. positionReport RPC 上报给 TabletServer：
         ├── sstDir（= `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`）→ 下载 manifest 获取 SST 列表，逐个 Ingest 到 pendingRowPos
         ├── newFileDictEntries → 更新 TabletServer 本地 FileDict（保证全局一致）
         ├── materializedDvFiles → 实际物化的 DV 文件列表
         ├── splitOffsetRange → 用于 §7.3.3 stale sentinel 清理（`currentTieredOffset`），其中 tieredOffset = splitOffsetRange.latest_offset
         ├── actualSnapshotId → Iceberg commit 返回的实际 snapshot id
         └── attemptEpoch → 见 §3.6
```

> **同 split 内先写后删的处理**：当同一轮 tiering split 中，一行数据先被 `+I`/`+U` 写入，随后又被 `-U`/`-D` 删除时，Tiering Writer 不需要再根据 `oldRowId` 判断该删除是否属于当前 split。只要 split 下发的 `logDvSnapshot` 已经覆盖这轮 changelog 中的删除，writer 在写 `+I`/`+U` 之前先检查其 RowId 是否命中 `logDvSnapshot`；命中则直接跳过。这样最终写入 Iceberg 的天然就是“apply 过本轮 log DV 后的存活数据”，不会再遇到“oldRowId 是否在本轮 split 范围内”的判断问题。

> **lakeDvSnapshot 过时保护**：从 split 生成到 commit 之间可能发生外部 compaction，导致 lakeDvSnapshot 中引用的文件已被替换或删除。Tiering Writer 在生成 Puffin DV 前，**必须读取当前 Iceberg table state 的文件集合，过滤 lakeDvSnapshot**，仅为当前仍存在的文件生成 Puffin DV。
>
> 被过滤的文件对应的逻辑删除仍保留在 TabletServer 的 LakeDv 中（不会被差集清理——因为 `materializedDvFiles` 不包含这些文件，见 §12.3）。同一轮 commit 中 Tiering Service 检测到外部 compaction 产出的替代文件（§11），将其 position 合并进 SST 上报；§7.3.1 步骤 7 的反向扫 PendingDeletes 命中替代文件中已删除行的新位置，补打 LakeDv 条目。这些删除将在下一轮 tiering 中被物化。

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

#### 7.2.3 RowPosIndex 的构建策略

RowPosIndex 的核心问题是：如何知道每行数据在 Iceberg data file 中的 row position。**本版本中 SST 由 Tiering Service 生成并上传，TabletServer 仅下载 + Ingest**（见 §7.2.2 / §7.3.1）。

**默认路径：Tiering Writer 同步上报**

Tiering Writer 在写入 data file 的过程中，天然知道每行的 row position（因为是 writer 自己按顺序写入的）。写入完成后，Tiering Service 将 `(RowId, file, row_position)` 在内存中聚合，用全局 FileDictAllocator 分配 fileId（见 §7.2.4），生成排序的 SST 并上传远程。

**外部 Compaction 路径：Tiering Service 扫描并分发**

TabletServer 维护一个 `knownFiles` 集合（从 `newFileDictEntries` 逐步填充）。Tiering Service 在 commit 时检测到外部 compaction 产生的未知文件后，扫描这些文件读取 `__offset` 和 `__bucket` 列，按 bucket 分组，将 position 信息与本轮 tiering 的新行合并生成每个 bucket 的 SST（详见 §11）。

| 情况                    | 判断条件                            | 处理方式                                                     |
|-----------------------|---------------------------------|----------------------------------------------------------|
| **Fluss 自己写的**        | Tiering Writer 直接产出的 position   | 进入 Tiering Service 聚合管道，零扫描                              |
| **外部 compaction 产生的** | Tiering Service commit 时发现的未知文件 | 由 Tiering Service 扫描文件并聚合 position，TabletServer 不做文件 I/O |

两条路径的 position 合并在同一个 SST 中上报，由 §7.3.1 统一处理。新方案下 TabletServer 无需对 SST 里的每一行做存活判定——由反向扫 PendingDeletes + 查 `pendingRowPos` 精确处理所有死行（见 §7.3.1 步骤 7 的正确性论证）。

#### 7.2.4 FileDictAllocator（Tiering Service 全局 fileId 分配器）

**角色**：每张 Fluss 启用 DV 的主键表，在 Tiering Service 侧持有一个**内存级 FileDictAllocator**，负责为所有新出现的 `file_path` 分配全局唯一的 `fileId`。所有 TabletServer 的本地 FileDict CF 是该全局映射的子集——仅包含本 bucket 实际涉及的文件条目。

**状态**：

```
FileDictAllocator {
    nextFileId   : int            // 单调递增分配计数器
    pathToFileId : Map<String, int>  // 当前 batch 内已分配的 file_path → fileId（纯内存，不持久化）
}
```

**分配时机**：§7.2.2 步骤 5（commit 之前，生成 SST 时）。对 positionReport 中每个 `file_path`：

```
fileId = pathToFileId.computeIfAbsent(path, _ -> nextFileId++);
if (新分配)
    newFileDictEntries.put(fileId, path);
```

**无状态设计**：Allocator **不依赖 Flink state backend**，Tiering Service 保持无状态。`nextFileId` 的恢复通过 Iceberg snapshot property 实现——每次 Iceberg commit 时将当前 `nextFileId` 写入 snapshot property（字段 `fluss.nextFileId`，与 `indexUuid` 一同写入）。`pathToFileId` 仅在内存中维护当前 batch 的去重，不做跨 batch 持久化。

**重启恢复**：Tiering Service 启动时，从最新 Iceberg committed snapshot 的 `fluss.nextFileId` property 读取计数器值，`pathToFileId` 初始化为空。重启后同一 `file_path` 可能被分配新的 `fileId`（跨 batch 去重丢失），但功能正确——每个 bucket 的 RowPosIndex entry 与本地 FileDict entry 自洽即可，跨 bucket 是否共享同一 fileId 不影响查询。代价仅为少量 fileId 空间浪费，int 40 亿空间远不至于耗尽。

**故障恢复——Pre-commit vs Post-commit 分治**：

由于 §7.2.2 将 SST 生成/上传（步骤 5）安排在 Iceberg commit（步骤 6）**之前**，故障恢复可按 commit 边界精确分治：

| 故障点 | 远程 SST 状态 | Iceberg 状态 | 恢复策略 |
|--------|-------------|-------------|---------|
| SST 上传前 crash | 不存在或不完整 | 未 commit | **全量 retry**：重新执行步骤 1-7。`nextFileId` 从上次 committed snapshot property 恢复，本次已分配但未 commit 的 fileId 不存在于任何地方，不会冲突。新 UUID 生成新路径。 |
| SST 已上传、commit 前 crash | 完整 | 未 commit | **全量 retry**：同上。旧 UUID 路径下的远程 SST + index 成为孤儿（由定期清理回收）。 |
| commit 成功、positionReport RPC 前 crash | 完整 | 已 commit | **Post-commit Metadata Reconcile**（见下方）：不得重新 commit，仅补齐 Fluss 注册。`nextFileId` 已随本次 commit 写入 snapshot property。 |
| positionReport RPC 失败 | 完整 | 已 commit | **Post-commit Metadata Reconcile**：同上。 |
| Tiering Service 整体 failover | 取决于故障点 | 取决于故障点 | 从最新 Iceberg snapshot property 恢复 `nextFileId`；TieringService 启动时检测 committed-but-unregistered snapshot，按需走 reconcile。 |

**Post-commit Metadata Reconcile**：

一旦 Iceberg `rowDelta.commit()` 成功，该 split 的数据已持久化到 Iceberg（data files + Puffin DV）。此时**不得对同一 `splitOffsetRange` 重新执行 writer commit**——Tiering Writer 的写入是 append 型的，重新 commit 会将同一批存活行再次写入新 data files，而现有 DV 机制只能屏蔽已删除行，无法消除重复 append 的活行。

恢复路径是 **metadata-only reconcile**——远程 SST + manifest + index 已在步骤 5 上传完毕（commit 前保证存在），Iceberg snapshot 已是数据结果的 source of truth，Fluss 缺失的仅是对该 snapshot 的 position metadata 注册。

**检测**：TieringService 启动（或当前 attempt 被宣告失败后准备 retry）时，对比 Iceberg committed snapshots 与 Fluss 已注册的 tiering metadata。Iceberg snapshot property 中记录了 `indexUuid`，若发现存在"由 Fluss tiering 产生、已 commit、但未在 Fluss 完成注册"的 snapshot，进入 reconcile。

**reconcile 流程**：

1. 从 Iceberg snapshot property 中读取 `indexUuid`。
2. 下载 cross-bucket index 文件 `{$remoteLakeTableSnapshotDir}/rowPos/{indexUuid}`，获取每个 bucket 的 `sstDir`。
3. 对每个未完成注册的 bucket，从 `sstDir` 下载 manifest，恢复：SST 文件名、`newFileDictEntries`、`splitOffsetRange`、`materializedDvFiles`。
4. 从 Iceberg snapshot 获取 `actualSnapshotId`。
5. CoordinatorServer 为各 bucket 分配新的 `attemptEpoch`（epoch > 上一次 attempt），向 TabletServer 重新发送 positionReport（与正常 §7.3.1 流程完全相同）。
6. TabletServer 收到后，attemptEpoch 校验走 `epoch > pendingAttemptEpoch` 分支，自动重置旧 attempt 的残留状态，正常处理。
7. 收敛所有 bucket 的 ready ack → CoordinatorServer 发布 DV-readable → 各 TabletServer 完成 readable switch。

**正确性保证**：同一 committed snapshot 的 reconcile 只补 metadata 不重提数据。远程 SST 是 commit 前的不可变产物，reconcile 下载的 SST 内容与正常流程完全一致。index 文件同样是 commit 前写入的不可变产物，保证 committed → index 可定位所有 bucket 的 SST。positionReport 幂等（§5.1），reconcile 多次执行也安全。

**fileId 空间大小**：int（4 字节），40 亿上限。对于单表数据 file 数量极大的场景（如长期运行多年），建议：
- 周期性（例如每次大 compaction 之后）执行 **fileId 重映射**：扫描当前 Iceberg 表中仍活跃的 file_path，生成新的连续 fileId 映射，TabletServer 通过全量 checkpoint 切换。这属于运维工具范畴，本文不展开；
- 如确有必要，未来可扩展至 long。

**为什么放在 Tiering Service 而非 CoordinatorServer**：

1. fileId 分配紧耦合 Iceberg commit 流程（分配后立即生成 SST、随 commit 持久化 `nextFileId`），放在 Tiering Service 减少跨进程 RPC；
2. CoordinatorServer 做 Allocator 需要额外的持久化和分配 RPC，引入更多故障面。

### 7.3 Server 处理流程 (TabletServer 侧)

#### 7.3.1 Position Report 处理

Tiering Writer commit 成功后（SST 已在 §7.2.2 步骤 5 上传），通过 RPC 将 positionReport 上报给 TabletServer。

```
sstDir               = 远程 SST 目录路径（`{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`），
                       其中 uuid 为 Pre-commit 阶段生成的随机标识，目录下包含 manifest 文件和一或多个 SST 文件
newFileDictEntries   = Map<fileId, file_path>
                       // 本 split 中 Tiering Service 新分配的 fileId → file_path 映射
                       // TabletServer 据此更新本地 FileDict CF
splitOffsetRange     = (last_tiered_offset, latest_offset]  // 用于 §7.3.3 stale sentinel 清理
materializedDvFiles  = List<file_path>  // 实际物化了 DV 的文件（过滤后的 lakeDvSnapshot keys）
actualSnapshotId     = long  // Iceberg commit 返回的实际 snapshot id（用于 ready ack / switched ack 关联与排障）
attemptEpoch         = long  // CoordinatorServer 为该 bucket 分配的单调递增 attempt epoch
                       // 每次 retry 新 attempt 时 epoch +1，用于 TabletServer 本地硬校验（见 §3.6）
```

> **为什么不再需要解析 SST 的每个 entry**：旧方案需要遍历本批 SST 中所有 `(RowId, fileId, row_position)` 对每个 RowId 做 alive check（查 PendingDeletes + 查 RowPosIndex）。新方案改为**反向扫** PendingDeletes，仅对每个 PendingDeletes entry 做一次 `pendingRowPos.get(RowId)`——PendingDeletes 大小为"未物化死行数"，远小于 SST 里的行数（SST 包含所有新写入行 + 所有外部 compaction 重写行，绝大多数是存活行）。TabletServer 不再需要解析 SST 的每个 entry，Phase 1 仅做下载 + 校验。

TabletServer 收到后：

0. **attemptEpoch 校验**（见 §3.6、§5.1 幂等机制）：
   - `epoch < pendingAttemptEpoch`：旧 attempt 或旧 split 的延迟报告，**直接拒绝**。
   - `epoch > pendingAttemptEpoch`：新 attempt 到达（可能是同一 split 的 retry，也可能是新 split 的首次 attempt）。**重置所有 pending 状态**：`DropColumnFamily(pendingRowPos)` + 重建空 CF、清空 `pendingSstFiles`、清空 `pendingDeletedRowIds`、将 `snapshotBitmap` 从 `snapshotBitmapRaw` 恢复（撤销旧 attempt 步骤 9 的过滤）。更新 `pendingAttemptEpoch = epoch`，继续正常处理。
   - `epoch == pendingAttemptEpoch`：同一 attempt 的幂等重试，继续正常处理。
1. 将 `newFileDictEntries` 中的 `file_path` 加入 `knownFiles` 集合。

**Phase 1（无锁——纯远程 I/O，不读写 DvRocksDB 的 pendingRowPos / PendingDeletes）**：

2. **下载 SST**：从 `sstDir` 下载 `manifest` 文件，解析获取所有 SST 文件名；逐个下载 SST 到本地临时路径。新方案下 TabletServer 无需解析 SST 的每个 entry——Phase 2 仅用 `pendingRowPos` 做点查即可（见步骤 7）。

**Phase 2（获取 DvRWLock 写锁——写 FileDict + hard-link + Ingest + 反向扫 PendingDeletes）**：

3. **获取 DvRWLock 写锁**（见 §5.1 并发控制）。
4. **创建 hard-link**：`link(sstPath, sstCopyPath)`——O(1) 文件系统操作，两个路径共享同一 inode。将 `sstCopyPath` 追加到 `pendingSstFiles` 列表（供 readable switch 时 Ingest 到 RowPosIndex，见 §7.3.3）。
5. **写入 newFileDictEntries 到 FileDict CF**：WriteBatch 批量写入本轮新分配的 `fileId → file_path`（以及反向 `file_path → fileId`）。
   - 如果某 `fileId` 在本地 FileDict 中已存在且映射到**相同** `file_path`：幂等重试场景，跳过；
   - 如果映射到**不同** `file_path`：全局 Allocator 不变式被破坏，必然是 bug——立即 fail-fast 并报警（见下方"newFileDictEntries 的幂等与异常分支"）。
6. **Ingest SST 到 pendingRowPos CF**：通过 `IngestExternalFile(sstPath, pendingRowPos)` 原子导入。Ingest 完成后，所有新 entry 立即对 §6.2 可见（§6.2 会同时查 `RowPosIndex` 和 `pendingRowPos`），同时也对本次 Phase 2 的反向扫步骤 7 可见。
7. **反向扫 PendingDeletes 补打 LakeDv**（替代旧方案"遍历 SST 每行做 alive check"）：遍历 PendingDeletes CF 的全部条目 `(R, v)`：
   ```
   for (R, v) in PendingDeletes:
       hit = pendingRowPos.get(R)
       if hit is not None:
           # R 已被 -U/-D 删除（§6.2 写入了 PendingDeletes），现在 position report 又把 R
           # 重新写入了 pendingRowPos——必然意味着外部 compaction 将 R 从旧文件
           # 重写到了新文件 hit.{fileId, pos}。对新文件补打 LakeDv 并立即移除
           # pendingRowPos 中的"复活" entry。
           LakeDv[hit.fileId] |= { hit.pos }
           pendingRowPos.delete(R)
           pendingDeletedRowIds.add(R)   # 供 §7.3.3 清除 hard-link Ingest 复活的孤儿
           # 更新 PendingDeletes[R] 为最新位置，使得未来多跳 compaction 仍能正确识别
           PendingDeletes.put(R, {hit.fileId, hit.pos})
       else:
           # R 在本批 SST 中不存在（无外部 compaction 或外部 compaction 的新文件
           # 不包含 R）——不做任何修改，R 继续保留在 PendingDeletes 中等待后续 tiering。
           pass
   ```
   所有上述 LakeDv 更新、pendingRowPos 删除、PendingDeletes 值更新通过单个 WriteBatch 原子提交。**不从 PendingDeletes 中移除条目**——PendingDeletes 的清理推迟到 readable 切换（见 §7.3.3）。
8. **释放 DvRWLock 写锁**。
9. 用 `materializedDvFiles` 过滤 `snapshotBitmap`：仅保留 `materializedDvFiles` 中包含的文件条目（见 §12.3），移除未物化的文件。
10. **步骤 9 完成后**，才可发送该 bucket 的 ready ack（见 §3.6 Phase 1）。

> **实现约束：ready ack 必须在步骤 9 之后发送**。如果在步骤 9 之前就通知 CoordinatorServer，CoordinatorServer 可能过早将 snapshot 标记为 DV-readable 并触发 §12.3 差集清理。此时 `snapshotBitmap` 中尚未过滤未物化文件，差集运算会错误地清除 LakeDv 中尚未物化的删除标记——不影响正确性（多屏蔽不会导致旧行复活），但浪费存储且可能干扰后续 union read 的性能。
>
> **步骤 10 失败策略**：如果步骤 10 失败，**不得发送 ready ack**。实现上应原地重试；若重试仍失败，记录错误日志并等待 CoordinatorServer **显式宣告当前 attempt 失败** 后再触发新的 retry。该 bucket 的 ready ack 缺失会阻止 CoordinatorServer 将新 snapshot 标记为 DV-readable，union read 继续使用旧的 readable snapshot——数据正确但陈旧，不会导致旧行复活。

> **SST 下载失败的处理**：Phase 1 的下载若失败，TabletServer 直接返回 RPC 错误给 Tiering Service，Tiering Service 根据 §3.6 的单飞/强取消语义决定是否重试。由于 SST 是 Tiering Service 侧不可变产物，同一 attempt 内重试直接复用 `sstDir` 即可。若 attempt 失败后触发新 attempt：(a) Iceberg commit 尚未成功——全量 retry，生成新的 SST 和 `newFileDictEntries`（fileId 可能与旧 attempt 不同，但 `epoch > pendingAttemptEpoch` 分支会重置所有 pending 状态，新 attempt 的 fileId 自洽即可）；(b) Iceberg commit 已成功——走 Post-commit Metadata Reconcile（§7.2.4），复用远程已存在的 SST + manifest，仅补发 positionReport。

> **`newFileDictEntries` 的幂等与异常分支**：同一 attempt 内，SST 和 `newFileDictEntries` 是一体生成的不可变产物，重试直接复用，天然幂等。跨 attempt 时，由于 `epoch > pendingAttemptEpoch` 分支会重置所有 pending 状态（`DropColumnFamily(pendingRowPos)` + 重建空 CF、清空 `pendingSstFiles` 等），新 attempt 的 fileId 分配与旧 attempt 无关，不存在跨 attempt 的 fileId 冲突。若检测到同一 attempt 内 `fileId → 不同 path` 的冲突，属于路由 bug——必须 fail-fast，不得静默覆盖。

> **为什么反向扫 PendingDeletes 能覆盖所有死行（正确性论证）**：SST 中的 entry 可以分成两类——(A) 本轮 tiering 新写入的行（`RowId ∈ splitOffsetRange`，即 Fluss 自己产出的 `+I/+U`）和 (B) 外部 compaction 把已有行从旧文件重写到新文件（`RowId ∉ splitOffsetRange`）。两类的"死行"判定逻辑各自独立：
>
> - **(A) 新写入行**：其 RowId 是本轮 Fluss 写入的 `+I/+U` 的 log offset，此前从未出现在 Iceberg 中，也从未被任何 `-U/-D` 引用过——因此**一定不在 PendingDeletes 中**。若这些行在本轮 split 内就被删除了，Tiering Writer 已用 split-scoped `logDvSnapshot` 在写入前过滤，根本不会出现在 SST 中。若其在本轮 tiering commit 之后被删除，§6.2 会查到 `pendingRowPos`（本轮 Ingest 的新 entry）并同步标记 LakeDv + 写入 PendingDeletes——反向扫下一轮再处理即可。**结论：(A) 类 entry 在本次反向扫中不需要任何操作——Ingest 本身就是正确结果**。
> - **(B) 外部 compaction 重写行**：其 RowId 是更早 tiering 产生的。如果该行已被 `-U/-D` 删除，§6.2 处理 `-U/-D` 时必然写入过 PendingDeletes（无论命中或未命中 CF，Case X / Case Y 都写 PendingDeletes——见 §6.2 步骤 4a）。因此**所有需要补打 LakeDv 的死行都能通过反向扫 PendingDeletes + 查 pendingRowPos 精确命中**。
>
> 这一等价性使得新方案可以**完全省略**原方案的 `RowPosIndex.get()` alive check——只要 §6.2 守住"任何 `-U/-D` 都写 PendingDeletes"这个不变式，反向扫 PendingDeletes 就是完备的。

> **为什么 Phase 1 不需要锁**：Phase 1 仅做远程下载（只读文件系统，不访问 DvRocksDB），与 §6.2 无竞态。Phase 2 获取 DvRWLock 写锁后先写 FileDict，再 hard-link + Ingest，此时 §6.2 被阻塞，Ingest 完成后所有 entry 立即可见。后续 §6.2 获取 DvRWLock 写锁时，能看到完整的 Ingest 结果和新的 FileDict 条目。

> **外部 compaction 行的并发正确性**：DvRWLock 写锁保证 §7.3.1 Phase 2 与 §6.2 互斥。两种执行顺序都正确：
> - §6.2 先执行：`RowPosIndex` 中 rowId 的旧 entry 已被删除，LakeDv 已标记旧文件，**PendingDeletes[rowId] 已写入**（值为旧位置）。§7.3.1 Phase 2 Ingest 新 entry 后，反向扫命中 `pendingRowPos[rowId] = 新位置`：为新文件补打 LakeDv，删除 pendingRowPos 中的新 entry，更新 PendingDeletes[rowId] 为新位置。
> - §7.3.1 先执行：Ingest 写入 `pendingRowPos` 的新 entry，`RowPosIndex` 中旧 entry 不动。反向扫阶段 PendingDeletes 中还没有该 rowId 的条目，不做处理。后续 §6.2 同时查 `RowPosIndex`（命中旧 entry）和 `pendingRowPos`（命中新 entry），在 LakeDv 中标记新旧两个文件位置，从两个 CF 分别删除 entry，**并写入 PendingDeletes[rowId] = 新位置**。新旧文件的删除标记都正确。

#### 7.3.2 发布 DV-Readable（TieringService + CoordinatorServer 执行）

TieringService 收齐所有 bucket 的 ready ack 后，向 CoordinatorServer 提交将 S_new 发布为 DV-readable 的请求。CoordinatorServer 更新 LakeTableZNode，并向所有相关 TabletServer 下发 readable switch 通知。

此后 client 可以开始以 S_new 作为目标 snapshot 发起 union read。尚未完成 switch 的 TabletServer 可能暂时返回 stale snapshot error，client 按 §9 的规则对同一个 S_new 重试即可。

#### 7.3.3 Readable 切换（TabletServer 执行）

TabletServer 收到 CoordinatorServer 下发的 readable switch 通知后，执行本地切换。完成后向 TieringService 返回 switched ack。

注意 `oldFiles` 的对比基准是**前后两个 readable snapshot**（不是相邻的任意两个 commit snapshot）。中间可能经历了多个 snapshot（tiering 产生的、外部 compaction 产生的），但只要 readable 没前移，旧文件的 LakeDv 就必须保留。

**获取 DvRWLock 写锁后，执行以下操作**（临界区仅包含 Ingest、DropColumnFamily 等 O(1) 元数据操作）：

1. **迁移 pendingRowPos → RowPosIndex**：
   - `IngestExternalFile(pendingSstFiles, RowPosIndex)`——将 §7.3.1 累积的 hard-link SST 副本一次性 Ingest 到 RowPosIndex CF。对同一 RowId，后 Ingest 的 SST 通过 sequence number 自然 shadow 先 Ingest 的，保证覆盖语义。
   - **清除窗口期孤儿 entry**：遍历 `pendingDeletedRowIds`，对每个 R 执行 `RowPosIndex.delete(R)`，然后清空 `pendingDeletedRowIds`。hard-link SST 是原始 SST 的副本，不含 pendingRowPos CF 在窗口期内产生的 delete tombstone；若不补打 delete，这些已死的 RowId 会因 Ingest 被"复活"进 RowPosIndex 且无任何清理机制，造成 RowPosIndex 持续膨胀。
   - `DropColumnFamily(pendingRowPos)` + 重建空 pendingRowPos CF——原 CF 中的 SST 文件被删除（hard-link 副本已在 RowPosIndex，磁盘数据不丢）。
   - 清空 `pendingSstFiles` 列表。
2. **清理 oldFiles**：对 oldFiles 中的每个文件，从 LakeDv 中删除该文件条目，从 `knownFiles` 中移除。**同时清理 PendingDeletes 中 `value.fileId` 指向 oldFiles 的条目**：这些 RowId 的最新未物化位置对应的文件已从 Iceberg 中消失（说明中间被外部 compaction 重写过且新位置已由反向扫更新过 PendingDeletes.value，此处只可能是遗留指向旧文件的 entry；即便不是，LakeDv[oldFile] 被整体移除后此 PendingDeletes entry 的值也已失效）。
3. **基于 snapshotBitmap 清理 PendingDeletes**（替代旧方案的 `deleteRange`）：遍历 PendingDeletes，对每个 `(R, v)`：
   - 若 `v == sentinel {0, 0}` 且 `R < currentTieredOffset`（`currentTieredOffset` = 本轮新 readable snapshot 的 `splitOffsetRange.latest_offset`）：该行已被 tiering 覆盖但从未写入 data file（被 logDvSnapshot 过滤），sentinel 永远不会被解决，**从 PendingDeletes 中删除 R**。
   - 若 `v == sentinel {0, 0}` 且 `R >= currentTieredOffset`：行尚在 tiering 处理中，保留等待后续反向扫。
   - 若 `v = {fileId, pos}` 且 `snapshotBitmap[fileId]` 存在且包含 `pos`：说明该位置已随本轮 tiering 物化到 Iceberg DV，PendingDeletes 条目已失效，**从 PendingDeletes 中删除 R**。
   - 否则：PendingDeletes.value 指向未物化位置（可能是跨 snapshot 残留的 Case X 条目，或外部 compaction 新位置尚未被后续 tiering 物化），保留等待下一轮清理。
4. **bitmap 差集清理 LakeDv**：执行 §12.3 的差集运算——使用 `snapshotBitmap` 执行 `当前 bitmap AND NOT snapshotBitmap`，清除 LakeDv 中已物化的标记，然后清空 `snapshotBitmap`。
5. **清理过期 LogDv**：删除 LogDv 中 `range 结束 offset < snapshotStartLogOffset`（新 readable snapshot 对应的 start offset）的整段 `offset_range` 条目（见 §3.4 生命周期管理）。这些 range 覆盖的 changelog 已被湖上 snapshot 包含，union read 的 delta log 不再读取该范围。
6. 更新 `readableSnapshotId` 和 `snapshotStartLogOffset` 为本轮 readable snapshot 对应的值。
7. **释放 DvRWLock 写锁**。

> **Readable 切换的正确性不变式**：任何 RowId R 只要存在尚未物化到 Iceberg DV 的 LakeDv bit 且 R 对应的 Iceberg 行可能被后续外部 compaction 重写，PendingDeletes 中必须保留 R 的条目。清理只发生在"PendingDeletes.value 已物化"时——此后 Iceberg DV 已知道该行死，后续外部 compaction 是 DV-aware 的（§10.3），不会再把 R 写入新文件，因此 PendingDeletes 条目不再需要。对于持有多个未物化位置的 R（罕见情况，通常发生在 §6.2 同时命中 RowPosIndex 和 pendingRowPos 的外部 compaction 窗口期），PendingDeletes.value 记录的是较新的 pendingRowPos 位置，它更可能被后续 compaction 影响；较旧的 RowPosIndex 位置指向已在 Iceberg 中的旧文件，与本轮 tiering 同步物化，差集清理与 oldFiles 扫除共同保证其 LakeDv bit 正确消除。

> **迁移为什么轻量**：SST 由 Tiering Service 侧生成（见 §7.2.2 步骤 5），TabletServer 在 §7.3.1 Phase 1 已完成下载和 hard-link，本步骤只需 Ingest hard-link 副本到 RowPosIndex CF——Ingest 是 O(1) 元数据操作（更新 MANIFEST + fsync），与 SST 中 entry 数量无关。随后的 `pendingDeletedRowIds` delete pass 开销为 O(|pendingDeletedRowIds|)——即窗口期内从 pendingRowPos 删除的 RowId 数量，通常远小于 SST 行数（两轮 tiering 之间分钟级的 `-U/-D` 量），不改变临界区量级。Hard-link 使磁盘上 SST 被两个 CF 同时引用，readable switch 不再产生数据拷贝。整个临界区毫秒级。
>
> **窗口期内的删除**：在 Phase A（处理 positionReport）到 Phase C（readable 切换）之间，如果有 `-U/-D` 到达，§6.2 同时查 `RowPosIndex`（命中旧 snapshot 的 entry）和 `pendingRowPos`（命中新 snapshot 的 entry），在 LakeDv 中同时标记两个文件的删除。这保证了：
> - union read 在窗口期内读 S_old → LakeDv 中有 S_old 文件的标记 ✓
> - readable 切换后 union read 读 S_new → LakeDv 中有 S_new 文件的标记 ✓
>
> RowPosIndex 侧的保护由 `pendingDeletedRowIds` 提供：§6.2 / §7.3.1 步骤 7 每次从 pendingRowPos 删除 entry 时，同步将 RowId 加入该内存集合；§7.3.3 步骤 1 在 Ingest(pendingSstFiles → RowPosIndex) 之后、Drop(pendingRowPos) 之前，遍历集合补打 `RowPosIndex.delete(R)`，消除 hard-link SST 因不含 tombstone 而复活的孤儿 entry。`pendingDeletedRowIds` 无需持久化——若 readable switch 之前崩溃，switch 未发生，孤儿不会产生；§10.2 恢复流程直接 Ingest → RowPosIndex（不经 pendingRowPos），不存在此问题。

#### 7.3.4 初始构建

第一次 tiering 完成后，RowPosIndex 和 pendingRowPos 均为空，且不存在旧的 readable snapshot。此时的处理逻辑：

- Tiering Writer 上报新写入行的 `(RowId, file, row_position)` 映射。
- Tiering Service 按 §7.2.2 步骤 5 用 FileDictAllocator 分配 fileId，生成 SST，上传远程。
- TabletServer 按 §7.3.1 流程处理：下载 SST → hard-link → 写 FileDict → Ingest 到 pendingRowPos。因为这是第一个 snapshot，commit 成功后它将立即成为首个 DV-readable snapshot——随后的 readable switch 会将 pendingRowPos 的 SST Ingest 到 RowPosIndex。
- 此时 LakeDv 为空（没有新的删除需要标记）。

如果不是通过 writer 上报，而是扫描文件，则第一次 snapshot 的所有行应**全部写入 pendingRowPos**（后续 readable switch 迁移到 RowPosIndex）。不能反查 RowPosIndex（因为此时为空，反查全部 miss 会错误地认为所有行都被删除了）。

---

## 8. Tiering Writer 改造

### 8.1 当前实现

当前 tiering 使用 `DeltaTaskWriter`（具体是 `GenericRecordDeltaWriter`），处理逻辑：

- `+I`/`+U` → 写入 data file
- `-U`/`-D` → 写入 equality delete file

### 8.2 改造后实现

引入新的 `DvTaskWriter`，替代 `DeltaTaskWriter`：

| 组件 | 当前 | 改造后 |
|------|------|--------|
| **Writer 类** | `GenericRecordDeltaWriter` (equality delta) | 新的 `DvTaskWriter`，只做 append |
| **DELETE 输出** | Equality delete file | Puffin DV file（来自 LakeDv 快照） |
| **DV 信息来源** | Writer 自己处理 `-U`/`-D` | LakeDv 快照 + split-scoped `logDvSnapshot` 过滤 |
| **WriteResult** | `{dataFiles, deleteFiles}` | `{dataFiles, dvFiles, positionReport, materializedDvFiles}` |
| **Commit** | `RowDelta.addDeletes(eqDeleteFile)` 无校验 | `RowDelta` + `validateFromSnapshot` + `validateDataFilesExist`（见 §7.2.2） |
| **Iceberg 版本** | v2 | v3 |

### 8.3 IcebergLakeCommitter 改造

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

### 8.4 Tiering Writer 不查 RowPosIndex

Tiering Writer 不通过 RPC 查 TabletServer 的 RowPosIndex。DV 相关信息来自两个来源：

1. **LakeDv 快照**：承载跨 split 的删除。TabletServer 在 changelog 同步成功时已查过 RowPosIndex 并将结果沉淀到 LakeDv 中。生成 tiering split 时快照 LakeDv，随 split 下发。Tiering Writer 直接将快照序列化为 Puffin DV 文件。
2. **split-scoped `logDvSnapshot`**：承载本轮 split 内已经发生的删除。Tiering Writer 在写 `+I`/`+U` 前先 apply 这份快照，命中的 RowId 直接跳过，因此最终写入的数据天然已经扣除了同 split 内先写后删的行。

整个 writer 路径都不需要 RPC 反查 TabletServer。

---

## 9. Union Read 流程

Client 通过 DV 进行 union read 的完整流程：

1. Client 获得 DV 可见的最新 snapshot id（记为 `requestedSnapshotId`），发送 union read 请求（**请求中携带 `requestedSnapshotId`**）
2. Fluss list 该 snapshot 下的 datafile list
3. **获取 KvTablet 读锁**
4. **获取 DvRWLock 读锁**（见 §5.1 并发控制）
5. **Snapshot 一致性校验**：读取当前 `readableSnapshotId`，检查 `readableSnapshotId == requestedSnapshotId`。如果不匹配，释放 DvRWLock 读锁和 KvTablet 读锁，返回 **stale snapshot error**（附带 `currentReadableSnapshot`）。
   - 若 `requestedSnapshotId < currentReadableSnapshot`：说明 TabletServer 已切到更新的 readable snapshot，client 刷新到更新 snapshotId 后重试。
   - 若 `requestedSnapshotId > currentReadableSnapshot`：说明 CoordinatorServer 已对外发布了更新的目标 snapshot，但该 TabletServer 尚未完成 readable switch。client **保持原来的 `requestedSnapshotId` 不变**，对同一个目标 snapshot 做退避重试，**不得回退到旧 snapshot**。
6. 获取当前 `logEndOffset`
7. 从 LakeDv 中**按 datafile list 对应的 fileId 子集 clone bitmap**，构造返回给 client 的 `{fileId → bitmap}` 视图。clone 只针对查询涉及的文件（通常远少于 LakeDv 全量），临界区内完成。
8. 从 LogDv 中获取当前 snapshot 的 start offset 到 `logEndOffset` 的 logDv（此步读取的是范围数据，也在 DvRWLock 读锁保护下完成）。
9. **释放 DvRWLock 读锁**
10. **释放 KvTablet 读锁**
11. **在锁外**进行序列化和网络发送，返回给 client：`{lakeDv, logDv, logEndOffset}`

> **并发安全说明**：
> - **KvTablet 读锁**：与 §6.2（changelog 同步成功）互斥，保证 `log_hw`（即 `logEndOffset`）的读取与 DV 视图一致。§6.2 在 KvTablet 写锁内先完成 DV 修改（持有 DvRWLock 写锁）再更新 `log_hw`，读锁确保 union read 不会看到 `log_hw` 已更新但 DV 尚未更新的中间状态。
> - **DvRWLock 读锁**：与 §6.2 / §7.3.1 / §7.3.3 的 DvRWLock 写锁互斥，保证读取 `readableSnapshotId` 和 LakeDv bitmap 子集时没有并发写入。bitmap 子集 clone 在锁内完成——一旦锁释放，即便 §7.3.3 后续修改 LakeDv，已 clone 出的 bitmap 独立于原数据，不受影响。
> - **序列化放在锁外**：步骤 11 的序列化和网络发送不在 DvRWLock 读锁内，临界区保持在 ms 级，不会阻塞写路径。

> **Snapshot 一致性校验的必要性**：Client 获取 `requestedSnapshotId`（步骤 1）和 TabletServer 读取 `readableSnapshotId`（步骤 5）之间存在 TOCTOU 窗口。在此窗口内，可能出现两种方向的偏差：
> - Coordinator 已发布 `S_new`，但该 TabletServer 还停留在 `S_old`；
> - 该 TabletServer 已切到 `S_new`，但 client 仍拿着更旧的 snapshotId。
>   如果不做校验，TabletServer 可能返回与目标 snapshot 不一致的 LakeDv，破坏屏蔽语义。
>
> DvRWLock 读锁保证 `readableSnapshotId` 和 LakeDv 的读取原子一致——即使 §7.3.3 此刻希望执行 readable 切换，它必须等待 union read 释放读锁才能获取写锁；union read 在锁内完成 bitmap 子集 clone 后即可释放读锁，clone 出的 bitmap 副本与当时的 `readableSnapshotId` 自然一致。校验失败时，client 按上面的双向规则收敛：要么刷新到更新 snapshotId，要么保持当前目标 snapshotId 重试；无论哪种情况，都不回退到更旧的目标 snapshot。

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

- `restoreSnapshot`：当前 DV-readable snapshot 的 ID
- `snapshotStartLogOffset`：该 snapshot 对应的 changelog start offset
- `checkpointLogHw`：checkpoint 时刻的 `log_hw`

> **为什么需要 `checkpointLogHw`**：checkpoint 保存的是"某个运行时刻的增量状态"，包含了 `snapshotStartLogOffset` 到 `checkpointLogHw` 之间所有 `-U/-D` 的处理结果（RowPosIndex 已删除、LakeDv 已更新、LogDv 已更新）。恢复时如果从 `snapshotStartLogOffset` 重放，会重复应用这些操作。必须从 `checkpointLogHw + 1` 开始重放。

> **RowPosIndex 与 checkpoint 的关系**：RowPosIndex 的数据来源是 Tiering Service 的 position report（§7.3.1），不在 changelog 中。因此 **changelog 重放无法为 RowPosIndex 新增条目**——它只能删除条目（处理 `-U/-D` 时）和更新 LakeDv/LogDv/PendingDeletes。RowPosIndex 的恢复完全依赖 DvRocksDB checkpoint 中保存的状态。对于 checkpoint 之后到达的 position report（已丢失），需要通过下载远程 SST 来恢复（见 §10.2 步骤 4）。

### 10.2 恢复步骤

1. 从远程存储拉取 SST 文件到本地，加载 DvRocksDB。此时 RowPosIndex 反映 `restoreSnapshot` 的状态（简单 Key `RowId → FilePos`）。pendingRowPos CF 为空（checkpoint 建议在 readable switch 完成后触发，此时 pendingRowPos 已在 §7.3.3 中被迁移清空）。
2. 从 **`checkpointLogHw + 1`** 开始重放 changelog（跳过 checkpoint 已包含的部分）
3. 对于每条 `-U`/`-D` 记录，提取 `oldRowId`（**仅处理删除，不新增 RowPosIndex 条目**；写入 PendingDeletes 的规则与 §6.2 一致）：
   - 查 `RowPosIndex` 和 `pendingRowPos` 各做一次 point get（与 §6.2 逻辑一致）：
     - **至少命中一条（Case X）**：对每条命中的 `{file_id, row_position}`，在 LakeDv 中将 `row_position` 加入 `file_id` 对应的 del_bitmap，并从对应 CF 中删除该 entry。**写入 PendingDeletes[oldRowId] = 命中位置**（优先 `pendingRowPos`，其次 `RowPosIndex`）。
     - **全部未命中（Case Y）**：**将 `oldRowId` 加入 PendingDeletes，值为 sentinel `{0, 0}`**。后续 position report 到达时由 §7.3.1 反向扫补齐 LakeDv。
   - 比较 `oldRowId` 和 `snapshotStartLogOffset`：
     - **oldRowId < snapshotStartLogOffset**：不需要更新 LogDv。要删除的行对应的 changelog 已在湖上 snapshot 覆盖的范围内，union read 的 delta log 不会读到这条记录。
     - **oldRowId >= snapshotStartLogOffset**：更新 LogDv，将 `offset = oldRowId` 对应的 changelog 标记为删除。
4. **处理 checkpoint 之后已完成 readable switch 的 snapshot**：恢复出来的 RowPosIndex、LogDv、LakeDv 都是针对 `restoreSnapshot` 的。如果 checkpoint 不是在每轮 readable switch 后都成功触发（§10.3 降级策略），`restoreSnapshot` 可能落后于当前 DV-readable snapshot。TabletServer 向 CoordinatorServer 查询当前 DV-readable snapshot（记为 `S_readable`）。若 `S_readable` 比 `restoreSnapshot` 更新，需要将 RowPosIndex 推进到 `S_readable` 的状态。

   通过 LakeStorage 接口查询 `restoreSnapshot` 到 `S_readable` 之间所有已提交的 snapshot（LakeStorage 需提供 `listSnapshotsBetween(restoreSnapshotId, readableSnapshotId)` 方法，返回按提交顺序排列的 snapshot 列表；Iceberg 实现基于 snapshot log / `parent-snapshot-id` 链，不能依赖 snapshotId 大小），记为 `S_1, S_2, ..., S_n`（其中 `S_n = S_readable`）。这些 snapshot 都已完成 readable switch（两阶段 ack barrier 保证：下一轮 split 只有在上一轮 readable switch 完成后才能生成，因此 `S_readable` 之前的所有 snapshot 必然已完成 readable switch）。

   > **为什么不恢复 `S_readable` 之后的 snapshot**：`S_readable` 之后至多有一个 pending snapshot（尚未完成 readable switch）。该 snapshot 的 positionReport 在崩溃中丢失，但 TieringService 会在 CoordinatorServer 宣告当前 attempt 失败后重试（§3.6 单飞/强取消语义），通过正常的 §7.3.1 流程重新下发——无需在恢复阶段特殊处理。

   **按序重建位置状态**——利用 Iceberg snapshot property 中记录的 `indexUuid`，通过 cross-bucket index 文件定位各 bucket 的远程 SST 目录，直接下载 SST 恢复，无需扫描 Iceberg data file。对每个 `S_i`（从 `S_1` 到 `S_n`，**必须按时间顺序**）执行：

   a. 从 `S_i` 的 Iceberg snapshot property 中读取 `indexUuid`，下载 index 文件 `{$remoteLakeTableSnapshotDir}/rowPos/{indexUuid}`，获取本 bucket 的 `sstDir`（= `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`）。从 `sstDir` 下载 `manifest` 文件获取所有 SST 文件名及 `newFileDictEntries`。
   b. **逐个下载 SST**——与 §7.3.1 Phase 1 相同，SST 中已包含完整的 `RowId → {fileId, row_position}` 映射，无需扫描 Iceberg data file 读取 `__offset`/`__bucket` 列。
   c. **从 manifest 中读取 `newFileDictEntries`**，写入本地 FileDict CF（幂等）。fileId 必须与 SST 中一致（由 Tiering Service 在生成 SST 时通过 Allocator 分配并写入 manifest）。
   d. **Ingest SST → RowPosIndex**。IngestExternalFile 会为 SST 分配高于当前 DB 最大值的 sequence number，保证后续 snapshot 的 entry 覆盖前一个 snapshot 的同 RowId entry。

   > **为什么必须按序 Ingest**：IngestExternalFile 按调用顺序递增分配 sequence number。假设 `S_1` 和 `S_2` 都包含 RowId=100 的映射（因为 `S_2` 的 compaction 重写了该行），按序 Ingest 保证 `S_2` 的 entry 有更高的 sequence number，RocksDB 读取时自动返回最新值。如果顺序颠倒，`S_1` 的过期 entry 反而胜出。

   > **为什么 Ingest 到 RowPosIndex 而非 pendingRowPos**：`S_1` 到 `S_n` 都已完成 readable switch，它们的 position 数据属于当前 readable snapshot 的一部分。Ingest 到 RowPosIndex 保持了"RowPosIndex 反映当前 readableSnapshot"的不变式。

   **全部 Ingest 完成后**，执行删除恢复：

   e. **更新 `readableSnapshotId = S_readable`** 和对应的 `snapshotStartLogOffset`。
   f. **从 `S_n.tieredOffset + 1`（即 `splitOffsetRange.latest_offset + 1`，`latest_offset` 本身属于已消费的 tiered 范围右闭端，不重放）开始重放 changelog 中的 `-U`/`-D`**，处理 Ingest 之后新暴露的删除：对每个 oldRowId 查 RowPosIndex（此时已包含 `S_readable` 的位置数据），命中则标记 LakeDv、删除 RowPosIndex entry、写入 PendingDeletes；未命中则写入 PendingDeletes sentinel `{0, 0}`。LogDv 无需更新——步骤 3 已为所有 `-U`/`-D` 处理了 LogDv（此处重放的是步骤 3 的子集，LogDv bitmap set 幂等）。
   g. **反向扫 PendingDeletes 补打 LakeDv**（与 §7.3.1 步骤 7 逻辑一致）：遍历 PendingDeletes 中每个 `(R, v)`，执行 `hit = RowPosIndex.get(R)`：
      - 命中：在 LakeDv 中为 `{hit.fileId, hit.pos}` 设置 bit，从 RowPosIndex 中删除 R，更新 `PendingDeletes[R] = {hit.fileId, hit.pos}`。
      - 未命中：保留 PendingDeletes[R]。
      > 此步骤覆盖两类 step f 无法处理的场景：(1) tiered 范围**内**的 `-U`/`-D` 删除同范围内的行（offset ≤ tieredOffset，step f 不会重放）；(2) 外部 compaction 将已删除行重写到新文件（step 3 已设 LakeDv 旧位置，但 Ingest 后 RowPosIndex 出现新位置，需补标）。

   > **为什么只需从 `S_n.tieredOffset + 1` 重放，而非每个 `S_i` 的 tieredOffset**：`S_n` 是最后一个 snapshot，其 tieredOffset 是所有 snapshot 中最大的。而 step g 的反向扫 PendingDeletes 会覆盖所有早期 snapshot 范围内的删除（PendingDeletes 在步骤 3 中已收集了所有 `-U`/`-D`，反向扫只需检查 RowPosIndex 是否命中即可）。因此无需对 `S_1` 到 `S_{n-1}` 逐一重放 changelog。

   h. **snapshotBitmap 处理**：恢复场景下 `snapshotBitmap` 未被填充（正常流程中由 §7.2.1 步骤 3 快照、§7.3.1 步骤 9 过滤）。恢复完成后正常运行，下一轮 tiering 的 §7.3.3 readable switch 时**跳过 §12.3 的 bitmap 差集清理**。LakeDv 中可能残留已物化到 Iceberg DV 的冗余条目，但不影响正确性——union read 同时 apply Iceberg DV 和 LakeDv，重复标记是幂等的。冗余条目在下一轮正常 tiering 中消除：§7.2.1 步骤 3 快照 LakeDv 时会完整捕获冗余 bits 到 `snapshotBitmap`，Tiering Writer 物化后，§7.3.3 的差集运算（`当前 bitmap AND NOT snapshotBitmap`）精确移除这些 bits。

   > **恢复期间的 PendingDeletes 清理**：恢复完成后，下一轮 §7.3.3 readable switch 时，若 `snapshotBitmap` 为空（恢复场景），snapshotBitmap 相关的清理（`v = {fileId, pos}` 匹配 snapshotBitmap）被跳过，但 **stale sentinel 清理（`R < currentTieredOffset`）仍然执行**——它不依赖 snapshotBitmap。PendingDeletes 中可能残留"值已物化但未被清理"的非 sentinel 冗余条目，与冗余 LakeDv bits 一样，会在下一轮正常 tiering 的 §7.3.3 清理中被精确消除（反向扫后 PendingDeletes.value 反映最新位置，下轮 tiering 物化该位置对应的 bit 后 PendingDeletes 条目被 §7.3.3 步骤 3 清理）。

### 10.3 Checkpoint 策略建议

- **触发时机**：建议在每次 readable snapshot 前移（§7.3.3）完成后触发一次 DvRocksDB checkpoint。此时 pendingRowPos 刚被迁移清空、RowPosIndex 已反映最新 readable snapshot 的位置，checkpoint 保存的状态是一致的。这也确保恢复时需要重放的 changelog 量最小、需要重新下载的 SST 最少。
- **降级策略**：如果 checkpoint 失败，记录日志并在下一次 readable snapshot 前移时重试。不影响正常写入和查询。恢复时会从更早的 checkpoint 开始，重放更多 changelog 并可能需要下载更多 SST，但不影响正确性。
- **远程 SST 清理**：DvRocksDB checkpoint 完成后，该 checkpoint 所覆盖的所有 snapshot 对应的远程 SST 目录（per-bucket `{$remoteLakeTableSnapshotDir}/rowPos/{bucketId}/{uuid}/`）及 cross-bucket index 文件（`{$remoteLakeTableSnapshotDir}/rowPos/{indexUuid}`）可以安全删除。因为 checkpoint 已将这些 SST 中的 `RowId → {fileId, row_position}` 映射持久化到本地 RowPosIndex，恢复时只需从 checkpoint 恢复、无需重新下载这些 SST。具体而言：若 checkpoint 覆盖到 `S_n`，则 `S_1` 到 `S_n` 各自对应的 per-bucket `{uuid}/` 目录（含 manifest 和所有 SST 文件）及对应的 index 文件均可清理。

---

## 11. 与外部 Compaction 的交互

外部引擎（如 Spark）可能对 Fluss 管理的 Iceberg 表执行 compaction，合并旧文件为新文件。Fluss 不控制外部 compaction 的时机，但必须正确处理其产生的文件变化。

### 11.1 感知时机

Fluss 不实时监听 Iceberg snapshot 变化。外部 compaction 产生的新 snapshot（如 S3）对 Fluss 是不可见的，直到 Fluss 自己的 Tiering Writer 进行下一次 commit（如 S4）。此时 Tiering Service 对比上次已知 snapshot（S2）和当前 Iceberg table state，发现其中包含了外部 compaction 的变化。

### 11.2 检测与处理

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
3. **合并到 SST 生成管道**：每个 bucket 的外部 compaction position 条目与本轮 tiering 新写入行一起进入 §7.2.2 步骤 5 的 SST 生成流程（Allocator 分配 fileId、SstFileWriter 生成 SST、上传远程）。最终每个 bucket 得到一个包含新写入行 + 外部重写行的 SST，通过 positionReport RPC 上报。
4. **上报旧文件列表**：将 `externalOldFiles` 也通知 TabletServer，用于后续 readable snapshot 前移时清理。

TabletServer 收到后通过 §7.3.1 的统一逻辑处理。Phase 2 Ingest SST 完毕后，**反向扫 PendingDeletes** 并对每个 `R ∈ PendingDeletes` 查一次 `pendingRowPos.get(R)`：命中即表示外部 compaction 将已删除的 R 重写到新文件，为新位置补打 LakeDv 并删除该 pendingRowPos entry；未命中则是存活行，直接保留等待 readable switch 合并到 `RowPosIndex`。外部新文件的 `file_path → fileId` 已随 `newFileDictEntries` 写入本地 FileDict。

### 11.3 被 compaction 物理删除的行

外部 compaction 会应用已有的 Iceberg DV（Puffin 文件），将已物理删除的行排除在新文件之外。这些行不会出现在 `externalNewFiles` 的扫描结果中。

这些行在 RowPosIndex / pendingRowPos 中**不会残留**：

- **存活行**：新文件中存活行的 RowId 与旧文件中相同，通过 §7.3.1 上报后 Ingest 到 pendingRowPos。新方案下此类 RowId 不在 PendingDeletes 中（从未被 `-U/-D`），反向扫不触碰这些 entry——Ingest 结果直接保留，下次 readable switch 合并时覆盖 `RowPosIndex` 中的旧 entry。
- **被物理删除的行**：这些行被删除时，§6.2 已将其从 `RowPosIndex` 和 `pendingRowPos` 中删除，并写入 PendingDeletes（值为命中位置或 sentinel）。若随后又被外部 compaction 重写到新文件并纳入 SST，§7.3.1 反向扫 PendingDeletes 会精确命中 pendingRowPos 的新 entry，为新位置补打 LakeDv 并删除 pendingRowPos entry——两个 CF 中均无残留。若 Iceberg DV 已经把该行物理排除在新文件之外，两个 CF 和 PendingDeletes 从一开始就不会被 Ingest 触及。

### 11.4 运维约束：Snapshot 过期策略

外部 compaction 产生的新 snapshot 对 Fluss 不可见，直到下一次 Fluss tiering commit 时才被吸收。在此期间：

- Fluss 的 readable snapshot（如 S1）仍在被 union read 使用，其引用的 data files 不能被删除。
- 外部 compaction 产生的中间 snapshot（如 S3）中，旧文件可能已被标记为不需要，但 Fluss 侧仍依赖这些文件的 LakeDv 条目提供逻辑删除屏蔽。

**约束**：Iceberg 表的 snapshot expiration 策略必须保留 Fluss 当前 readable snapshot 及其引用的所有 data files。建议：

- 将 Iceberg 表的 `history.expire.min-snapshots-to-keep` 设置为足够大的值，覆盖 tiering 间隔内可能产生的 snapshot 数量。
- 或由 Fluss 在 table property 中标记当前 readable snapshot id，外部 expiration 工具跳过该 snapshot 及其之前的依赖。

如果 readable snapshot 被过早 expire 导致 data files 被物理删除，union read 会失败（读不到文件）。

### 11.5 可观测性

检测到外部 compaction 文件时，打日志或上报 metric（如 `external_compaction_files_scanned`），让运维感知到有外部引擎在修改 Fluss 管理的 Iceberg 表。

---

## 12. LakeDv 物化流程

LakeDv 从 TabletServer 的逻辑删除标记物化为 Iceberg 中的物理 Deletion Vector（Puffin 文件）的完整流程：

### 12.1 触发时机

每轮 tiering commit 时执行。

### 12.2 流程

1. 生成 tiering split 时，TabletServer 在读锁保护下快照当前 LakeDv，并通过 FileDict 将 `file_id` 解析为 `file_path`
2. LakeDv 快照（`{file_path → bitmap}`）随 tiering split 下发给 Tiering Writer
3. Tiering Writer 直接用 `file_path` 和 bitmap 生成 Puffin DV 文件（无需额外的字典查找）
4. 通过 Iceberg `RowDelta` API 将 Puffin DV 文件与 data file 一起 commit 到 Iceberg

### 12.3 物化后清理

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

实现上，TabletServer 维护一个 `snapshotBitmap`（`Map<file_id, bitmap>`）。生成 split 时保存 LakeDv 快照副本（§7.2.1 步骤 3），position report 到达后过滤未物化的文件（§7.3.1 步骤 9）。收到 DV-readable 通知后，用 `snapshotBitmap` 执行差集运算，然后清空。由于保证 split n+1 的生成不会在 readable switch n 之前发生，`snapshotBitmap` 在任何时刻最多只有一份，不需要按 snapshotId 分组。

> **snapshotBitmap 与实际物化结果的对齐**：如果 Tiering Writer 因外部 compaction 过滤了 lakeDvSnapshot 中的部分文件（见 §7.2.2 lakeDvSnapshot 过时保护），这些文件的 DV 未被物化到 Iceberg。TabletServer 收到 Tiering Writer 上报的 `materializedDvFiles` 后，必须从 `snapshotBitmap` 中移除未物化的文件（见 §7.3.1 步骤 9）。否则，差集清理会错误地清除 LakeDv 中尚未物化的删除标记。

---

## 13. 端到端示例

> 以下示例中 `{$lakeSnapshotDir}` 为 `FlussPaths.remoteLakeTableSnapshotDir()` 的缩写。

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

Tiering Writer 内部产出 position entries：`file_A:pos0/1/2 对应 RowId 0/1/2`。

Tiering Service 侧（§7.2.2 步骤 5-7）：
- FileDictAllocator 分配 `file_A → fileId=1`（首次分配）；`newFileDictEntries = {1 → file_A}`
- 生成 UUID=`a1b2c3`
- SstFileWriter 生成 SST（key=RowId，value={fileId=1, pos}）到本地
- 上传 SST 到远程 `{$lakeSnapshotDir}/rowPos/B0/a1b2c3/sst_0.sst`，写入 manifest 到 `{$lakeSnapshotDir}/rowPos/B0/a1b2c3/manifest`
- 写入 index 文件到 `{$lakeSnapshotDir}/rowPos/{indexUuid1}`，记录 `{B0: .../rowPos/B0/a1b2c3/}`
- Iceberg commit snapshot S1（snapshot property 记录 indexUuid1）
- 发送 positionReport RPC：`{sstDir={$lakeSnapshotDir}/rowPos/B0/a1b2c3/, newFileDictEntries={1→file_A}, splitOffsetRange=(-1, 2], materializedDvFiles=[], actualSnapshotId=S1}`

TabletServer 收到后（§7.3.1）：
- Phase 1（无锁）：下载 manifest + SST 到 /tmp/sst_1.sst
- Phase 2（DvRWLock 写锁）：hard-link 到 /tmp/sst_1_copy.sst；WriteBatch 写 FileDict `{1→file_A, file_A→1}`；Ingest /tmp/sst_1.sst → pendingRowPos；pendingSstFiles = [/tmp/sst_1_copy.sst]；**反向扫 PendingDeletes 为空**（首轮 tiering，无任何历史 `-U/-D`）

```
RowPosIndex: 空
pendingRowPos:
  0 → {file_A, pos0}
  1 → {file_A, pos1}
  2 → {file_A, pos2}
pendingSstFiles: [/tmp/sst_1_copy.sst]
```

此时 S1 尚未成为 DV-readable（等待 ready ack + CoordinatorServer 发布）。

假设随后 S1 被发布为 DV-readable，TabletServer 执行 readable switch（§7.3.3）：
- IngestExternalFile(pendingSstFiles, RowPosIndex)，DropColumnFamily(pendingRowPos) + 重建

```
RowPosIndex:
  0 → {file_A, pos0}
  1 → {file_A, pos1}
  2 → {file_A, pos2}
pendingRowPos: 空
pendingSstFiles: []
```

LakeDv、LogDv 仍为空。`knownFiles = {file_A}`。readableSnapshotId = S1。

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

Changelog 同步成功后（§6.2，获取 DvRWLock 写锁）：
- 查 `RowPosIndex` 和 `pendingRowPos` point get(0)：`RowPosIndex` 命中 {file_A, pos0}，`pendingRowPos` 未命中
- 更新 LakeDv：`file_A → del_bitmap{0}` （pos0 被删除）
- 从 `RowPosIndex` 中删除 `0`
- **写入 PendingDeletes：`0 → {fileId=1, pos=0}`**（Case X，记录命中位置）
- 更新 LogDv：offset=0 在 range `offset0~offset9` 中，bitmap = `{1}`
- 释放 DvRWLock 写锁，更新 log_hw

DV 状态：
```
RowPosIndex:
  1 → {file_A, pos1}
  2 → {file_A, pos2}
pendingRowPos: 空

LakeDv:
  file_A → {0}

LogDv:
  offset0~offset9 → {1}

PendingDeletes:
  0 → {fileId=1, pos=0}   ← 待下一轮 tiering 物化后清理
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

Changelog 同步成功后（§6.2，获取 DvRWLock 写锁）：
- 查 `RowPosIndex` 和 `pendingRowPos` point get(2)：`RowPosIndex` 命中 {file_A, pos2}，`pendingRowPos` 未命中
- 更新 LakeDv：`file_A → del_bitmap{0, 2}` （pos0 和 pos2 被删除）
- 从 `RowPosIndex` 中删除 `2`
- **写入 PendingDeletes：`2 → {fileId=1, pos=2}`**
- 更新 LogDv：offset=2 在 range `offset0~offset9` 中，bitmap = `{1, 3}`（第1条和第3条被删除）
- 释放 DvRWLock 写锁，更新 log_hw

PendingDeletes 当前：`{0 → {1, 0}, 2 → {1, 2}}`（均为 Case X，等待下一轮 tiering 物化 file_A 后在 §7.3.3 清理）

### Step 6：第二轮 Tiering

生成 tiering split（§7.2.1）：
1. 获取读锁
2. `log_hw = 5`，`latest_offset = 5`
3. 快照 LakeDv = `{file_A: {0, 2}}`，同时保存 `snapshotBitmap = {file_A: {0, 2}}`（§7.2.1 步骤 3，用于后续 §12.3 差集清理）
4. 释放读锁
5. tiering split = `{offset_range: [3, 5], lakeDvSnapshot: {file_A: {0, 2}}}`

Tiering Writer 处理（§7.2.2）：
- offset=3：`-U(oldRowId=0)` → `-U/-D` 不写入 data file（oldRowId=0 的删除已在 logDv 中）
- offset=4：`+U(key1, v4)` → logDvSnapshot 未命中 → 写入新 data file，记录 positionReport: `(RowId=4, file_B, pos0)`
- offset=5：`-D(oldRowId=2)` → `-U/-D` 不写入 data file（oldRowId=2 的删除已在 logDv 中）

生成：
```
data_file_B:
  pos0 → (key1, v4, __offset=4)

Puffin DV file（来自 LakeDv 快照）:
  file_A → {0, 2}  (pos0 和 pos2 被删除)
```

Tiering Writer 内部产出 `file_B:pos0 对应 RowId 4`。

Tiering Service 侧（§7.2.2 步骤 5-7）：
- FileDictAllocator 查/分配 `file_B → fileId=2`；`newFileDictEntries = {2 → file_B}`
- 生成 UUID=`d4e5f6`
- SstFileWriter 生成 SST（4 → {fileId=2, pos=0}）到本地
- 上传 SST 到远程 `{$lakeSnapshotDir}/rowPos/B0/d4e5f6/sst_0.sst`，写入 manifest 到 `{$lakeSnapshotDir}/rowPos/B0/d4e5f6/manifest`
- 写入 index 文件到 `{$lakeSnapshotDir}/rowPos/{indexUuid2}`，记录 `{B0: .../rowPos/B0/d4e5f6/}`
- Iceberg commit snapshot S2（snapshot property 记录 indexUuid2）
- 发送 positionReport：`{sstDir={$lakeSnapshotDir}/rowPos/B0/d4e5f6/, newFileDictEntries={2→file_B}, splitOffsetRange=(2, 5], materializedDvFiles=[file_A], actualSnapshotId=S2}`

TabletServer 收到后（§7.3.1）：
- Phase 1（无锁）：下载并校验 SST 到 /tmp/sst_2.sst
- Phase 2（DvRWLock 写锁）：hard-link 到 /tmp/sst_2_copy.sst；WriteBatch 写 FileDict `{2→file_B, file_B→2}`；Ingest /tmp/sst_2.sst → pendingRowPos；pendingSstFiles = [/tmp/sst_2_copy.sst]
- **反向扫 PendingDeletes**：
  - `R=0, v={1, 0}`：`pendingRowPos.get(0)` 未命中（file_A 已物化且 RowId=0 不在本轮 SST 中——外部 compaction 未发生）→ 不做操作
  - `R=2, v={1, 2}`：`pendingRowPos.get(2)` 未命中 → 不做操作
  - 两条 PendingDeletes 条目保留等待 §7.3.3 清理
- **步骤 9**：用 `materializedDvFiles = [file_A]` 过滤 `snapshotBitmap`——file_A 在 materializedDvFiles 中，保留；过滤后 `snapshotBitmap = {file_A: {0, 2}}`（不变）
- 发送 ready ack

```
RowPosIndex:
  1 → {file_A, pos1}
pendingRowPos:
  4 → {file_B, pos0}
pendingSstFiles: [/tmp/sst_2_copy.sst]
PendingDeletes:
  0 → {fileId=1, pos=0}
  2 → {fileId=1, pos=2}
```

TieringService 收齐所有 bucket 的 ready ack 后，向 CoordinatorServer 提交将 S2 发布为 DV-readable 的请求；CoordinatorServer 完成对外发布。

TabletServer 收到 DV-readable 通知后（§7.3.3）：
- `IngestExternalFile(pendingSstFiles, RowPosIndex)`：将 `pendingRowPos` 的 entry 合并到 RowPosIndex
- `DropColumnFamily(pendingRowPos)` + 重建空 CF，清空 pendingSstFiles
- **bitmap 差集清理 LakeDv**（§7.3.3 步骤 4，§12.3）：对 LakeDv 中每个 file_id，执行 `当前 bitmap AND NOT snapshotBitmap`：
  - file_A：当前 bitmap = `{0, 2}`，snapshotBitmap[file_A] = `{0, 2}` → `{0, 2} AND NOT {0, 2}` = `{}` → 结果为空，移除 file_A 条目
  - 清空 `snapshotBitmap`
  - 清理后 LakeDv = 空（file_A 的 pos0、pos2 已物化到 S2 的 Iceberg DV，LakeDv 中不再需要保留）
- **清理 PendingDeletes**：`snapshotBitmap = {file_A: {0, 2}}`，遍历 PendingDeletes：
  - `0 → {1, 0}`：fileId=1 对应 file_A，snapshotBitmap[file_A] 包含 pos=0 → **删除 PendingDeletes[0]**
  - `2 → {1, 2}`：fileId=1 对应 file_A，snapshotBitmap[file_A] 包含 pos=2 → **删除 PendingDeletes[2]**
- 更新 `readableSnapshotId = S2` 和 `snapshotStartLogOffset`
- 释放 DvRWLock 写锁
- 此时 union read 已切换到 S2，S2 自带物理 DV，LakeDv / PendingDeletes 清理安全

DV 状态：
```
RowPosIndex:
  1 → {file_A, pos1}
  4 → {file_B, pos0}
pendingRowPos: 空
pendingSstFiles: []

LakeDv: 空（S2 已成为 DV-readable 后差集清理）
PendingDeletes: 空（0 和 2 对应的 bit 已物化，条目同步清理）

LogDv: 清理 offset < S2_start_offset 的条目
```

### Step 7：新写入 + Union Read（S2）

S2 已成为 DV-readable（tiered_offset=5）。此时再写入新数据：

```
UPDATE key2 → -U (offset=6, key2, v2, oldRowId=1) + +U (offset=7, key2, v5)
INSERT key4  → +I (offset=8, key4, v6)
```

Changelog 同步成功后（§6.2，获取 DvRWLock 写锁）：

**offset=6：`-U(oldRowId=1)`**
- 查 `RowPosIndex` point get(1)：命中 `{file_A, pos1}`；`pendingRowPos` point get(1)：未命中
- 更新 LakeDv：`file_A → {1}`
- 从 `RowPosIndex` 中删除 `1`
- 写入 PendingDeletes：`1 → {fileId=1, pos=1}`
- 更新 LogDv：offset=1 在 range 中标记为已删除

**offset=7、8**：`+U`、`+I` 正常写入 changelog，更新 `log_hw = 8`

DV 状态：
```
RowPosIndex:
  4 → {file_B, pos0}       ← key1=v4
pendingRowPos: 空

LakeDv: {file_A: {1}}       ← key2 在 file_A:pos1 的删除，尚未物化
PendingDeletes:
  1 → {fileId=1, pos=1}
LogDv: offset=1 标记为已删除
```

**Client 请求 union read（snapshot S2）**：

1. 获取 KvTablet 读锁
2. `logEndOffset = 8`
3. 获取 DvRWLock 读锁
4. `readableSnapshotId = S2`
5. lakeDv = `{file_A: {1}}`（clone bitmap 子集）
6. logDv = `{...: offset=1 已删除}`
7. 释放 DvRWLock 读锁
8. 释放 KvTablet 读锁
9. 返回 `{lakeDv, logDv, logEndOffset=8}`

Client 侧处理：
- 读 Iceberg snapshot S2：
  - file_A：apply Iceberg DV `{0, 2}` → 跳过 pos0（key1, v1）和 pos2（key3, v3）；apply lakeDv `{1}` → 跳过 pos1（key2, v2）→ file_A 无存活行
  - file_B：无 DV → 读出 pos0（key1, v4）
- 读 changelog `[tiered_offset+1=6, logEndOffset=8]`，apply logDv 过滤：
  - offset=6：`-U` → retract 类型，不输出
  - offset=7：`+U(key2, v5)` → 输出
  - offset=8：`+I(key4, v6)` → 输出

最终结果：`(key1, v4), (key2, v5), (key4, v6)` ✓ 正确（key2 更新为 v5，key4 新插入）

> **三层 DV 协作**：本次 union read 同时使用了全部三层——Iceberg DV 过滤 file_A 的 pos0/pos2（已物化的历史删除），LakeDv 过滤 file_A 的 pos1（尚未物化的新删除），changelog + LogDv 提供未 tiered 的增量数据。三层各司其职，共同保证查询结果的正确性。

---

## 14. 总结

| 维度 | 设计决策 |
|------|----------|
| **RowId** | 使用 `+I`/`+U` 的 log offset，天然唯一递增，与 `__offset` 列一致 |
| **RowPosIndex** | 双 CF 架构：`RowPosIndex` 存当前 readable snapshot 的 `RowId → FilePos`，`pendingRowPos` 存尚未 readable 的新 position；§6.2 两个 CF 各 point get 一次（固定 2 次）；SST 由 Tiering Service 侧生成并上传远程（FileDictAllocator 统一分配 fileId），TabletServer 只做下载 + hard-link + Ingest；通过 hard-link 让 readable switch 的迁移为纯 O(1) Ingest（物理数据不拷贝）；dictionary 编码文件路径 |
| **LakeDv** | 增量存储，每轮 tiering commit 后通过 bitmap 差集清理已物化的条目 |
| **LogDv** | Range-based bitmap，按固定 offset 间隔分段 |
| **存储** | DvRocksDB 独立于 KvTablet RocksDB，六个列族（RowPosIndex、pendingRowPos、LogDv、LakeDv、FileDict、PendingDeletes）；PendingDeletes 升级为"完整未物化死行日志"（value = `{fileId, pos}` 或 sentinel `{0, 0}`），作为 §7.3.1 反向扫的唯一索引；DvRWLock（全局读写锁）序列化 §6.2/§7.3.1/§7.3.3 写路径（写锁），union read 持读锁并 clone 出查询涉及文件的 bitmap 子集后立即释放；position 上报天然幂等（反向扫仅更新 PendingDeletes.value 不删除）+ attemptEpoch 三路校验拦截过期和乱序请求；PendingDeletes 在 readable 切换时基于 `snapshotBitmap` 精确清理（PendingDeletes.value 已物化的条目被删除） |
| **架构分工** | TabletServer 维护轻量元数据 + 快照 LakeDv，SST 下载 + Ingest；Tiering Writer 写 data file + 物化 Puffin DV；Tiering Service 持有内存级 FileDictAllocator（无状态，`nextFileId` 通过 Iceberg snapshot property 恢复）、**commit 前**在 `{$remoteLakeTableSnapshotDir}/rowPos/` 下生成 per-bucket SST（UUID 子目录）+ cross-bucket index 并上传远程（保证 committed → index + SST + manifest 均可恢复）；post-commit failure 走 metadata-only reconcile（§7.2.4）；snapshot property 记录 indexUuid + nextFileId，恢复时 snapshotId → indexUuid → index → per-bucket sstDir |
| **DV 物化** | LakeDv 快照覆盖跨 split 删除；同 split 内先写后删通过 `logDvSnapshot` 写前过滤；commit 前过滤已被外部 compaction 替换的文件 + `validateDataFilesExist` 兜底；未物化的删除由 LakeDv 保底 |
| **Commit 验证** | IcebergLakeCommitter 从无校验改为 `validateFromSnapshot` + `validateDataFilesExist`；冲突时 abort 下轮重试 |
| **Position 构建** | Writer 上报（默认）+ Tiering Service 扫描外部 compaction 文件（兜底）；两条路径合并进入 Tiering Service 的 SST 生成管道；§7.3.1 反向扫 PendingDeletes 统一处理所有死行（复杂度 O(\|PendingDeletes\|) 而非 O(\|SST\|)），无需对 SST 的每一行做 `RowPosIndex.get()` alive check；PendingDeletes 既解决 position report 与删除操作的时序间隙，也作为外部 compaction 死行的反向索引 |
| **Changelog 格式** | `-U`/`-D` 的 value 首部携带 oldRowId（8 bytes） |
| **KV State 格式** | 首部插入 RowId（8 bytes） |
| **Iceberg 数据列** | 新增 `__bucket` 列，用于外部 compaction 后识别行的 bucket 归属 |
| **Iceberg 版本** | 切换到 v3；新表强制 v3，存量 v2 表原地升级，历史 equality delete 仍有效 |
| **外部 Compaction** | Tiering Service 检测并扫描外部新文件，按 `__bucket` 合并进入 SST 生成管道；oldFiles 清理推迟到 readable snapshot 前移 |
| **恢复** | TabletServer：从 DvRocksDB checkpoint 加载，重放 changelog 增量；通过 snapshot property 中的 `indexUuid` 定位 cross-bucket index → per-bucket sstDir，按序下载远程 SST Ingest → RowPosIndex，从 `tieredOffset + 1` 重放 changelog + 反向扫 PendingDeletes 补齐 LakeDv。TieringService（无状态）：从最新 Iceberg snapshot property 恢复 `nextFileId`；检测 committed-but-unregistered snapshot，走 Post-commit Metadata Reconcile（§7.2.4）补齐 Fluss 注册，不重新 commit |
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

恢复场景下 `snapshotBitmap` 未被填充（§10.2 步骤 4h），下一轮 readable 切换时跳过差集清理。这导致 LakeDv 中残留已物化到 Iceberg DV 的冗余条目。本附录通过具体场景推演，证明这些冗余条目在下一轮正常 tiering 中被精确消除。

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
→ snapshotBitmap = {file_A: {0, 1}}（§7.2.1 步骤 3）
→ lakeDvSnapshot = {file_A: {0, 1}} 随 split 下发

offset=58: +I(key4) → RowId=58
offset=59: DELETE key(RowId=30) → §6.2: LakeDv[file_A] += {2}, 删 RowPosIndex[30]
                                   ← bit {2} 在快照之后新增

→ Tiering Writer 处理 split:
  - +I 写入 file_B:pos0
  - lakeDvSnapshot {file_A: {0,1}} 物化为 Puffin DV
  - commit S3 (tiered offset = 60, Iceberg DV: file_A → {0,1})

→ position report 到达，§7.3.1 处理完成
→ snapshotBitmap 过滤后 = {file_A: {0,1}}

★ FAILOVER（readable switch 到 S3 之前）
  snapshotBitmap 丢失（内存），S3 的 RowPosIndex entry 丢失（未 checkpoint）
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

**步骤 4**：向 CoordinatorServer 查询当前 DV-readable snapshot → S3。通过 LakeStorage 查询 restoreSnapshot(S2) 到 S3 之间的 snapshot → [S3]

```
读取 S3 的 snapshot property → indexUuid → 下载 index 文件 → 获取 B0 的 sstDir：{$lakeSnapshotDir}/rowPos/B0/{uuid3}/
下载远程 SST 目录 {$lakeSnapshotDir}/rowPos/B0/{uuid3}/：
  manifest → SST 文件名=[sst_0.sst], newFileDictEntries={fileId_B → file_B}, splitOffsetRange=(50, 60]
  sst_0.sst → 包含 {RowId=58 → {fileId_B, pos0}}

写入 FileDict：{fileId_B → file_B, file_B → fileId_B}
Ingest sst_0.sst → RowPosIndex（S3 已完成 readable switch，Ingest 到 RowPosIndex 保持不变式）

步骤 e: 更新 readableSnapshotId = S3
步骤 f: 从 S3 的 tieredOffset=60 开始重放 changelog → 无新的 -U/-D（checkpointLogHw+1=56 到当前已在步骤 3 处理）
步骤 g: 反向扫 PendingDeletes：
  R=10: RowPosIndex.get(10) 未命中 → 保留
  R=20: RowPosIndex.get(20) 未命中 → 保留
  R=30: RowPosIndex.get(30) 未命中 → 保留
  （三行均已在步骤 3 中从 RowPosIndex 删除，SST 中不包含这些 RowId）

恢复完成，不发送 ready ack（S3 已是 DV-readable）
```

```
结果：
  readableSnapshotId = S3
  RowPosIndex = {58 → {fileId_B, pos0}}
  pendingRowPos: 空
  LakeDv = {file_A: {0, 1, 2}}   ← bits {0,1} 冗余（已在 S3 Iceberg DV 中）
                                     bit {2} 有效（未物化）
```

### 下一轮正常 tiering（冗余消除）

**§7.2.1**：生成 split (60, 70]

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

**§7.3.1 步骤 9**：

```
用 materializedDvFiles 过滤 snapshotBitmap:
  file_A ∈ materializedDvFiles → 保留
snapshotBitmap = {file_A: {0, 1, 2}}
```

**Readable switch 到 S4**（§7.3.3）：

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

1. **`snapshotBitmap` 完整捕获**：§7.2.1 快照 LakeDv 时，冗余 bits 和有效 bits 一视同仁，全部进入 `snapshotBitmap`。
2. **物化幂等安全**：Tiering Writer 物化 `lakeDvSnapshot` 时，冗余 bits 的 Puffin DV 是已有 Iceberg DV 的超集，Iceberg 处理时幂等。
3. **差集精确清除**：§7.3.3 步骤 4 的 `当前 bitmap AND NOT snapshotBitmap` 运算精确移除所有已物化 bits（含冗余），保留快照之后新增的未物化 bits。
