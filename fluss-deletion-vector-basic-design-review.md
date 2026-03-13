# Deletion Vector 基础设计方案评审

本文档基于 `fluss-deletion-vector.md` 中定义的三层 DV 架构，对 `fluss-deletion-vector-basic-design.md` 中的具体设计方案进行评审。

---

## 一、方案优点

### 1. RowId 设计简洁

用 INSERT/UPDATE_AFTER 的 log offset 作为 RowId，天然唯一、单调递增，且与 Fluss 已有的 `__offset` 系统列完全一致——当前 tiering 写入 Iceberg 时，只有 INSERT 和 UPDATE_AFTER 记录会写入 data file，其 `__offset` 列值就是 RowId。无需引入额外的 ID 生成机制。

需要明确的是，RowId 标识的是**一行数据的某个版本**，不是 primary key。同一个 key 的不同版本有不同的 RowId：

```
PUT (key1, v1)    → +I (offset=0, key1, v1)   → RowId = 0   （第一个版本）
PUT (key1, v2)    → -U (offset=1, key1, v1)   → 引用 RowId = 0（要删除的版本）
                     +U (offset=2, key1, v2)   → RowId = 2   （第二个版本）
DELETE (key1)     → -D (offset=3, key1, v2)   → 引用 RowId = 2（要删除的版本）
```

RowId 在各个组件中的对应关系：

| 场景 | RowId 的值 | 来源 |
|------|-----------|------|
| `+I`/`+U` changelog | 该记录自身的 log offset | 写入时自动确定 |
| `-U`/`-D` changelog | 被删除版本的 log offset | 从 KV state 旧 value 中提取 |
| Iceberg data file | `__offset` 列值 | tiering 写入时带入 |
| KV state (RocksDB) | 当前版本的 log offset | 写入时存入 value 尾部，格式为 `[schemaId][BinaryRow][rowId(8 bytes)]` |
| RowPosIndex | 索引 key | 用于查 (file, row_position) |

这要求 **changelog 格式做一处扩展**：`-U`/`-D` 记录的 value 中需要携带被删除版本的 RowId。这个 RowId 在 KV state 的旧 value 中已经存在，生成 changelog 时直接提取即可。

### 2. 三层存储职责清晰

RowPosIndex、LogDv、LakeDv 各自职责明确，通过不同的 RocksDB 列族隔离，与 KvTablet 的 RocksDB 解耦。这避免了 DV 的 checkpoint/恢复流程与 KV 数据的 checkpoint 互相干扰。

### 3. 并发正确性有明确分析

文档对加锁原因做了详细推演（changelog 同步成功 vs 处理数据湖 snapshot 的竞争），并论证了加锁后两种执行顺序都能保证 LakeDv 正确性。这是方案中质量较高的部分。

### 4. Union Read 一致性考虑到位

通过 KvTablet 读锁保证 LakeDv、LogDv 与 logEndOffset 的一致性快照，避免 DV 和 log 位点不一致导致的重复读。问题识别准确。

---

## 二、关键问题

### 问题 1：RowPosIndex 存储开销巨大

RowPosIndex 为**每一条活跃行**维护一个条目。对于一张 10 亿行的表：

- Key（RowId，8 bytes）+ Value（FilePosList，每个 FilePos 8 bytes，假设平均 2 个快照）= 约 24 bytes/行
- 总开销：约 **24 GB**

这还不包括 RocksDB 的 block index、bloom filter 等元数据开销。对于 TabletServer 来说，这个额外存储负担很重，尤其是每个 TabletServer 可能承载多张表的多个 bucket。

**建议**：评估是否可以用 key（primary key bytes）替代 RowId 作为索引 key。这样 RowPosIndex 的条目数等于去重后的行数（而非 changelog 条目数），且可以直接复用 KvTablet 已有的 key 编码。

### 问题 2：处理新 Snapshot 需要全量扫描 Data Files

"处理数据湖的 snapshot" Step 1 要求：对 newFiles 中的每个文件，**遍历文件的每个 RowId，反查 RowPosIndex**。

这意味着：
- 需要读取每个新 data file 的全部内容（至少读 `__offset` 列）来获取 RowId
- 对于大表，一轮 tiering 可能产生几百个 data file，每个文件几百 MB
- 这些 I/O 发生在 TabletServer 上，与正常的写入/查询争抢资源

这是整个方案中**最大的性能瓶颈**。Moonlink 的做法是在写入 data file 时同步构建索引（因为 writer 天然知道每行的 position），而本方案将索引构建延迟到 snapshot 处理阶段，导致需要反向扫描文件。

**建议**：在 tiering writer 写入 data file 的过程中同步记录 `(RowId, file, position)` 映射，随 tiering 结果一起上报给 TabletServer，避免事后扫描。

### 问题 3：写入路径的锁竞争

Changelog 同步成功流程中，需要同时持有 **KvTablet 写锁** 和 **LakeDv 写锁**：

```
获取 KvTablet 写锁 → flush → 获取 LakeDv 写锁 → 遍历更新 DV → 释放 LakeDv 写锁 → 更新 log_hw → 释放 KvTablet 写锁
```

问题：
- KvTablet 写锁的持有时间被 DV 更新操作拉长。对于每条 `-U/-D` 记录，都需要查 RowPosIndex + 查/更新 LakeDv + 更新 LogDv，涉及多次 RocksDB 读写。
- "处理数据湖的 snapshot" 也需要 LakeDv 写锁，会与 changelog flush 互相阻塞。
- 高吞吐写入场景下，DV 更新成为写入路径上的性能瓶颈。

**建议**：考虑将 DV 更新从 KvTablet 写锁的临界区中解耦。例如：先在 flush 阶段收集需要更新的 `(rowId, change_type)` 列表，释放 KvTablet 写锁后再异步批量更新 DV。需要额外处理一致性，但可以显著降低写入路径的延迟。

### 问题 4：FilePosList 多快照膨胀——实际上只需存最新快照

设计指出"相同的一条数据会在不同快照的不同 datafile 中出现"，因此 FilePosList 包含多个 FilePos。但这个多快照设计是不必要的。

**原因分析**：文档没有解释为什么需要跨快照存储，只说"因为相同的一条数据会在不同快照的不同 datafile 中出现"。表面看，如果保留多个快照且 client 可能读任意一个快照，LakeDv 就需要覆盖所有快照中的文件，因此 RowPosIndex 需要记录每行在所有快照中的位置。

**但实际上只需要最新快照**，逻辑如下：

1. Tiering commit 时，changelog 中的删除已经物化为 Iceberg DV（Puffin 文件）。任何快照中，早于 tiered offset 的删除已由 Iceberg 自身处理。
2. LakeDv 只覆盖 tiered offset 之后的新删除——这些新删除针对的是最新快照中的文件。
3. Union read 读的是最新的 DV-readable snapshot，不需要为老快照维护 LakeDv。

**因此 RowPosIndex 每行只需存一个 FilePos（最新快照中的位置）**：

```
之前：rowId → [{file_A, pos5}, {file_B, pos3}, {file_C, pos10}]   // 跨快照，N * 8 bytes
之后：rowId → {file_B, pos3}                                       // 最新快照，8 bytes
```

**简化后的收益**：

- **存储减半以上**：每行从 `N * 8 bytes` 降到 `8 bytes`
- **去掉 FilePosList 过期清理逻辑**：不再需要"检查 LakeDv 中是否有这个文件来判断是否过期"的间接判断
- **新 snapshot 到达时逻辑简化**：直接用新文件的 position 覆盖旧的，而非 merge 到 list 中

**新 snapshot 到达时的处理**：

```
1. 新文件的 position 覆盖 RowPosIndex（来自 writer 上报或扫描）
2. 被替换的旧文件从 RowPosIndex 清除
3. LakeDv 中旧文件的 bitmap 清除（这些删除已物化到新 snapshot 的 Iceberg DV 中）
```

### 问题 5：RowPosIndex 初始构建流程缺失

文档描述了 RowPosIndex 在"处理数据湖的 snapshot"时如何更新，但没有说明**第一次 tiering 后如何构建** RowPosIndex。

- 第一次 tiering 写入一批 data files 后，RowPosIndex 为空
- 随后的"处理数据湖的 snapshot"会遍历 newFiles，对每个 RowId 反查 RowPosIndex
- 全部查不到 → 全部认为"被删除了" → 全部标记为 delete_bits

这显然不对。初始构建流程需要特殊处理：第一次 snapshot 的所有行应全部写入 RowPosIndex，而非反查。

### 问题 6：恢复流程的日志量风险

恢复时需要从 `snapshotStartLogOffset` 重放 changelog。如果 DvRocksDB checkpoint 的频率不够高，或者两次 checkpoint 之间的日志量很大，恢复时间会很长。

文档没有指定：
- DvRocksDB checkpoint 的触发频率
- 最大可容忍的恢复日志量
- checkpoint 失败时的降级策略

---

## 三、设计缺失

### 缺失 1：Iceberg Deletion Vector 的生成流程

`fluss-deletion-vector.md` 定义了三层 DV 中的第一层——Iceberg Deletion Vector（标准 Iceberg v3 Puffin 文件）。但基础设计文档完全没有描述这一层是**如何生成**的：

- Tiering 时是否直接将 LakeDv 物化为 Iceberg deletion vector？
- 物化后 LakeDv 中对应的条目是否清理？
- Iceberg v3 format version 的设置在哪里？

这是三层架构中从"逻辑删除"到"物理删除"的关键衔接，需要补充。

### 缺失 2：Tiering Writer 的改造

当前 tiering 使用 `DeltaTaskWriter`，INSERT/UPDATE_AFTER 写 data file，DELETE/UPDATE_BEFORE 写 equality delete file。引入 DV 后需要明确改造方案。

**推荐架构：TabletServer 维护元数据，Tiering Writer 生成物理 DV 文件。**

两者职责分明：

- **TabletServer（轻量元数据维护）**：KV 写入时，`-U/-D` 到达后本地查 RowPosIndex、更新 LakeDv 和 LogDv。为 union read 提供实时可见的逻辑删除标记。
- **Tiering Writer（重 I/O 的物理文件生成）**：读 changelog，`-U/-D` 记录携带 rowId，批量 RPC 查 TabletServer 的 RowPosIndex 获取 `(file, row_position)`，生成 Puffin DV 文件，commit 到 Iceberg。同时上报新写入行的 position。

```
TabletServer                          Tiering Writer (Flink)
┌─────────────────────┐               ┌──────────────────────────────┐
│                     │               │                              │
│  KV 写入时:          │               │  读 changelog                │
│    -U/-D 到达        │               │                              │
│    → 本地查 RowPosIndex│              │  +I/+U → 写 data file       │
│    → 更新 LakeDv     │               │         → 记录 position      │
│    → 更新 LogDv      │               │                              │
│                     │               │  -U/-D → 批量查 RowPosIndex  │
│  Union Read 时:      │               │         → 生成 Puffin DV     │
│    → 返回 LakeDv+LogDv│              │                              │
│    → 立即生效        │               │  Commit → 物化到 Iceberg     │
│                     │◄── 上报 positions ──│                         │
└─────────────────────┘               └──────────────────────────────┘
```

这样的分工有两个关键好处：

1. **Union read 实时生效**：TabletServer 侧的 LakeDv 在 `-U/-D` 到达时立即更新，union read 不需要等待下一轮 tiering commit 就能跳过 Iceberg 中已删除的行。
2. **Iceberg 写入不在 Fluss Cluster 关键路径上**：Puffin 文件生成和 Iceberg commit 这类重 I/O 操作由 Tiering Writer（Flink job）执行，TabletServer 只做轻量的本地 RocksDB 读写。

**Tiering Writer 具体改造点**：

| 组件 | 当前 | 改造后 |
|------|------|--------|
| **Writer 类** | `GenericRecordDeltaWriter` (equality delta) | 新的 `DvTaskWriter`，只做 append + DV 生成 |
| **DELETE 输出** | Equality delete file | Puffin deletion vector file |
| **-U/-D 的 rowId** | changelog 不携带 | changelog value 中附带 rowId（`+I/+U` 的 log offset） |
| **跨批 DELETE** | 按 key 匹配（equality delete） | 用 rowId 批量 RPC 查 TabletServer RowPosIndex |
| **WriteResult** | `{dataFiles, deleteFiles}` | `{dataFiles, dvFiles, positionReport}` |
| **Commit** | `RowDelta.addDeletes(eqDeleteFile)` | `RowDelta.addDeletes(dvFile)` + 上报 positions |
| **Iceberg 版本** | v2 | v3 |

### 缺失 3：增量 DV 的存储方式

文档中有一个 TODO 提到"对于超大表，需要考虑增量 DV"，但没有给出方案。

实际上 LakeDv 天然应该以增量方式存储：**只保存自上次 tiering commit 以来的新增删除，不存全量**。每轮 tiering commit 将 LakeDv 物化为 Iceberg DV（Puffin 文件）后，清空 LakeDv，重新从空开始积累。

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

由于每轮 tiering 间隔通常只有几分钟，LakeDv 积累的删除量很小，不存在全量 DV 过大的问题。历史删除已物化到 Iceberg DV 中，不需要 server 维护。基础设计文档中应明确这一点，而不是留作 TODO。

### 缺失 4：与 Iceberg Compaction 的交互

当 `IcebergRewriteDataFiles` 执行 compaction 时：
- 旧文件被合并为新文件
- RowPosIndex 中所有指向旧文件的 FilePos 都失效了
- LakeDv 中旧文件的 bitmap 也失效了

文档没有描述 compaction 后如何更新 RowPosIndex 和 LakeDv。

---

## 四、总结

| 维度 | 评价 |
|------|------|
| **架构方向** | 三层 DV 分层合理，职责边界清晰 |
| **RowId 设计** | 简洁，与现有 `__offset` 一致 |
| **存储开销** | RowPosIndex 对大表开销过大，需要优化 |
| **写入性能** | 锁竞争是主要风险，DV 更新在写入关键路径上 |
| **Snapshot 处理** | 全量扫描 data file 不可接受，应在 tiering 写入时同步构建索引 |
| **恢复** | 基本框架可行，但缺少频率/降级策略 |
| **完整性** | 缺少 Iceberg DV 物化、tiering writer 改造、增量 DV、compaction 交互四个关键流程 |

**核心建议**：将 RowPosIndex 的构建从"snapshot 处理阶段扫描 data file"改为"tiering 写入时同步上报 position"，可以同时解决全量扫描的性能问题和初始构建的正确性问题。这也是 Moonlink 采用的思路——writer 天然知道每行的 position，无需事后反查。

---

## 五、改进方案：Writer 上报 + 外部 Compaction 兜底

纯 Moonlink 方式（writer 同步上报）的问题是无法处理外部 compaction：如果 Spark 等外部引擎对 Fluss 管理的 Iceberg 表执行了 compaction，旧文件被合并为新文件，行的 row position 发生变化，但 Fluss 的索引仍指向旧文件——索引失效，deletion vector 指向错误位置，数据正确性被破坏。

而基础设计中"新 snapshot 到达时全量扫描 newFiles"的方式虽然天然兼容外部 compaction，但性能代价不可接受。

建议采用**混合方案**：默认走 writer 上报（高效），检测到外部未知文件时回退扫描（兜底）。

### 判断逻辑

当新 snapshot 到达时，计算 `newFiles = snapshot_files(s_new) - snapshot_files(s_old)`。TabletServer 维护一个 `knownFiles` 集合（由 writer 上报时 add），对 newFiles 中的每个文件区分处理：

| 情况 | 判断条件 | 处理方式 |
|------|----------|----------|
| **Fluss 自己写的** | 文件在 `knownFiles` 中 | 直接用已上报的 position 更新 RowPosIndex，零扫描 |
| **外部 compaction 产生的** | 文件不在 `knownFiles` 中 | 回退扫描该文件，读取 `__offset` 列重建 position 映射 |

### 处理流程

```
新 snapshot 到达，计算 newFiles / oldFiles
        │
        ├── 对每个 newFile:
        │       │
        │       ├── knownFiles 中存在？
        │       │       ├── YES → 用已上报的 position 更新 RowPosIndex（快）
        │       │       └── NO  → 扫描文件读 __offset 列，重建 position（慢，仅针对外部 compaction 文件）
        │       │
        │       └── 更新 LakeDv（检查哪些行已被删除）
        │
        └── 对每个 oldFile:
                ├── 从 RowPosIndex 清理指向该文件的 FilePos
                ├── 从 LakeDv 删除该文件条目
                └── 从 knownFiles 移除
```

### 优势

- **常规路径零扫描**：Fluss 自身 tiering 和 compaction（`IcebergRewriteDataFiles`）产生的文件全部由 writer 上报 position，无需扫描。
- **外部 compaction 可兜底**：当检测到未知文件时自动回退扫描，保证正确性。性能代价只在外部 compaction 发生时支付。
- **可观测性**：检测到外部 compaction 文件时可打日志或上报 metric（如 `external_compaction_files_scanned`），让运维感知到有外部引擎在修改 Fluss 管理的 Iceberg 表。
