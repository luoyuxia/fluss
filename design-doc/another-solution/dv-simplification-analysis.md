# Fluss DV 简化方案分析

## 1. 当前方案复杂度根源

当前方案的核心复杂度集中在：**Fluss 实时层必须实时维护 RowId -> FilePos 映射**。由此派生出整条复杂度链：

1. **RowPosIndex + pendingRowPos 双 CF**：解决 snapshot 切换期间不能覆盖旧位置的问题
2. **PendingDeletes + sentinel {0,0}**：解决 tiering 管道中的时序间隙（Case Y）
3. **reverse-scan PendingDeletes**：解决外部 compaction 重写文件后的位置追踪
4. **两阶段 ack barrier**：确保所有 bucket ready 后才能 publish，避免 torn state
5. **SST 生成/上传/下载/Ingest**：TieringService 生成位置映射 SST，TabletServer 下载并 Ingest
6. **DvRWLock + 锁排序约束**：保证 4.2 / 5.3 / 5.4 / 6 之间的并发正确性
7. **snapshotBitmap diff cleanup**：bitmap 差集清理避免丢失增量 LakeDv
8. **恢复流程**：需按 snapshot 顺序下载远程 SST 重建 RowPosIndex，再 replay changelog

这些机制环环相扣，任何一个步骤的证明都依赖上下文的多个不变量。导致 safety proof、工程实现和运行时排查三重困难。

---

## 2. 方案一：DeletedRowIdSet + 异步物化

### 2.1 核心思路

Fluss 实时层**完全不感知**湖上的逻辑位置到物理位置映射。删除/更新到来时，只记录被删除的 RowId（即 `oldRowId`）到一个集合 `DeletedRowIdSet` 中。位置解析（RowId -> FilePos）推迟到 tiering 物化阶段批量完成。

### 2.2 数据结构简化

| 当前方案 | 方案一 |
|---------|--------|
| RowPosIndex CF | **删除** |
| pendingRowPos CF | **删除** |
| PendingDeletes CF | **删除** |
| FileDict CF | **删除**（或大幅简化） |
| LakeDv CF（file_id -> bitmap） | 替换为 DeletedRowIdSet（RowId 集合） |
| LogDv CF | **保留**（changelog 范围内的删除标记） |

DvRocksDB 从 6 个 CF 减少到 2 个（DeletedRowIdSet + LogDv）。

### 2.3 写入路径简化

```
-U/-D 到达，提取 oldRowId:
  -> DeletedRowIdSet.add(oldRowId)     // 始终添加，不区分 row 在 Iceberg 还是 changelog
  -> 若 oldRowId >= snapshotStartLogOffset: LogDv.mark(oldRowId)
  -> 结束
```

不需要任何 point-get、不需要区分 Case X / Case Y、不需要写 PendingDeletes。写入路径从 O(2 point-get + conditional writes) 简化为 O(1 set add)。

### 2.4 Union Read

```
客户端获取：
  - DeletedRowIdSet（未物化的删除）
  - LogDv（changelog 范围内的删除）
  - logEndOffset

Iceberg 文件过滤：
  1. 应用 Iceberg DV（已物化的 position delete）
  2. 利用文件级 __rowid 列统计信息（min/max）做快速裁剪：
     - 若文件的 [min_rowid, max_rowid] 与 DeletedRowIdSet 无交集 -> 跳过（O(1)）
     - 若有交集 -> 投影 __rowid 列，按 DeletedRowIdSet 过滤
  3. 读取存活行

Changelog 过滤：
  - 使用 LogDv（与当前方案相同）
```

**性能影响评估**：

方案一的 union read 核心问题是：知道某个 RowId 需要删除，但**不知道这个 RowId 在哪个文件的哪个位置**。无法像 position delete 那样零 I/O 跳行，必须读取 `__rowid` 列逐行匹配。

**与 position delete 的开销对比**（假设 3 分钟一次 tiering，~100 delete/s，一个区间内约 2 万条 delete，分布在 2-3 个文件，每文件 100 万行）：

| 方式                        | 额外 I/O                                                        |
|---------------------------|---------------------------------------------------------------|
| position delete（当前方案）     | ~几 KB bitmap，零列读取                                             |
| RowId delete（方案一）         | 2-3 文件 x 100 万行 x 8B = ~16-24MB（压缩前），delta encoding 后约 ~4-6MB |

对于 128MB/文件的典型场景，`__rowid` 列约占 3-5% 额外 I/O。若查询本身只投影少量列或有强谓词下推，这个比例会更大。且这是**每次 union read 都要付的代价**。

**文件级裁剪的局限性**：

上述开销估算基于一个乐观假设：文件级 `__rowid` min/max 裁剪有效、受影响文件少。这只在 **Fluss 原始写入的文件**上成立（`__rowid` 单调递增，文件间区间不重叠）。**外部 compaction 之后这个假设不再成立**：

- Compaction 通常按 primary key 排序合并，不按 `__rowid`
- 合并后文件内 `__rowid` 乱序散布
- 文件的 `__rowid` min/max 区间变得极宽（如 min=0, max=100 万）
- 多个 compacted 文件区间大面积重叠
- 文件级裁剪基本失效，几乎所有 compacted 文件都"可能包含"被删 RowId
- 退化为：**对所有 compacted 文件扫描 `__rowid` 列做匹配** — 本质上就是 equality delete 的读放大问题

**引擎接入成本**：

方案一将删除语义从标准的 position delete 变为基于 `__rowid` 的 equality 匹配。这意味着所有接入 union read 的查询引擎（Flink、Spark、Trino、StarRocks 等）都需要：

- 理解 `__rowid` 这个 Fluss 私有语义列
- 在 scan 时额外投影 `__rowid` 列
- 实现 DeletedRowIdSet 的匹配过滤逻辑
- 处理 DeletedRowIdSet + Iceberg DV 两套删除机制的叠加

相比之下，当前方案的 LakeDv 按文件 pos 进行过滤，方案一对引擎侧的理解成本增加。

### 2.5 Tiering 物化

```
split 生成时 snapshot DeletedRowIdSet -> deletedRowIdSetSnapshot

Tiering Writer:
  1. logDvSnapshot 过滤 intra-split write-then-delete（与当前方案相同）
  2. 对 deletedRowIdSetSnapshot 做位置解析：
     a. 读 Iceberg 文件级统计（__rowid min/max）定位候选文件
     b. 扫描候选文件的 __rowid 列，匹配 RowId -> 获取 (file, row_position)
     c. 生成 Puffin DV
  3. 提交 Iceberg snapshot

清理（readable switch 时）：
  - set diff: DeletedRowIdSet -= snapshot 中已物化的 RowId
  - 孤儿条目（row 被 logDvSnapshot 过滤、从未写入 Iceberg）：
    若 RowId < currentTieredOffset 且物化扫描未命中 -> 安全移除
```

**物化阶段的核心问题**：

步骤 2 的位置解析本质上是：**拿着一个标识（RowId）去 Iceberg 文件中反查它在哪个文件的哪个位置**。这和直接拿 primary key 去 Iceberg 做点查定位没有本质区别——只是把匹配键从 PK 换成了 `__rowid`。

目前所有写 Iceberg 的引擎（Flink Iceberg Sink、Spark 等）都不会在写入时做这种"先查位置再写 position delete"的操作，原因就是**成本太高**：需要读取候选文件的列数据做匹配。这也是为什么现有方案普遍选择 equality delete（直接记录"删 PK=X"，让读端去匹配）或者 copy-on-write（全量重写文件）。

方案一把当前方案中 TabletServer 实时维护映射的复杂度，搬到了 tiering 物化阶段做批量反查。虽然是批量操作、不在实时路径上，但每轮 tiering 都要扫描 Iceberg 文件做位置解析，随着表规模增大和 compaction 导致 `__rowid` 分布散乱，扫描成本会持续增长。

### 2.6 方案一的风险点总结

1. **Union Read 性能**：必须读 `__rowid` 列逐行匹配，无法像 position delete 那样零 I/O 跳行。外部 compaction 后文件级裁剪失效，退化为 equality delete 级别的读放大。
2. **Tiering 物化成本**：每轮 tiering 需扫描 Iceberg 文件做 RowId -> position 反查，本质上和拿 PK 去 Iceberg 点查无异，随表规模和 compaction 程度增长。
3. **引擎接入成本**：所有查询引擎需理解 `__rowid` 私有语义、实现 DeletedRowIdSet 匹配逻辑、处理两套删除机制叠加，不利于生态接入。

---

## 3. 方案二：映射作为 Snapshot 附属

### 3.1 核心思路

RowId -> FilePos 映射仍然存在，但不在 Fluss 实时层维护。映射作为 Iceberg snapshot 的附属文件（sidecar）存储。

关键设计决策：**不要求外部 compaction 更新映射**。我们无法控制外部引擎的行为，映射只在 Fluss 自己写 snapshot 的时候更新，忽略 compaction 产生的 snapshot。

### 3.2 映射生成与更新

映射只在 Fluss 自己写 snapshot 的时候生成/更新，忽略外部 compaction 产生的 snapshot：

- **Fluss tiering commit**：TieringService 写数据到 Iceberg 时自然知道每行的 (RowId, file, position)，生成映射作为 snapshot 附属存储
- **外部 compaction**：Fluss 不感知、不处理。下一次 Fluss tiering 时检测到外部 compaction（与当前方案 §8.2 类似），扫描被重写文件的 `__rowid` 列重建受影响部分的映射
- **TabletServer**：readable switch 时从 snapshot 附属加载新映射，替换旧缓存

### 3.3 Union Read 流程

方案二的 LakeDv 仍然是 **position-based**（`{file_id -> position bitmap}`），和当前方案一样。区别在于位置的来源：当前方案从 RowPosIndex（DvRocksDB 内实时增量维护）获取，方案二从 snapshot 附属的映射文件加载到 TabletServer 本地缓存。

**删除路径**：

```
-U/-D 到达，提取 oldRowId:
  -> 查 TabletServer 本地缓存的映射: oldRowId -> (file_id, position)
  -> 命中: LakeDv[file_id] |= {position}    // position-based，和当前方案一样
  -> 未命中: row 可能在 tiering 管道中，记录 pending
```

**Union read**：返回 LakeDv（position bitmap），查询引擎直接按位置跳行，不需要读 `__rowid` 列，不需要任何额外适配。

### 3.4 DV 生成（物化）

和当前方案完全一样，因为 LakeDv 已经是 position-based：

1. Split 生成时 snapshot LakeDv（已经是 position bitmap）
2. Tiering Writer 直接从 LakeDv snapshot 生成 Puffin DV
3. 提交 Iceberg
4. Readable switch 后 bitmap diff cleanup

映射在 TabletServer 侧已经完成了 RowId -> position 的解析，LakeDv 存的就是物理位置，物化是直接的，无需反查 Iceberg 文件。

### 3.5 优势

1. **TabletServer 状态简化**：不需要 RowPosIndex/pendingRowPos 的增量维护协议
2. **Position-based deletion 保持高效**：union read 仍可使用位置位图，不需要读 `__rowid` 列
3. **映射生命周期与 snapshot 绑定**：切换 snapshot 时原子替换映射，无需双 CF
4. **不耦合外部引擎**：外部引擎做标准 Iceberg compaction 即可，无需理解 Fluss 私有协议
5. **DV 生成与当前方案一致**：LakeDv 已是 position-based，物化流程无需改变

### 3.6 核心问题

**问题一：映射规模与全量膨胀**

映射包含所有曾 tier 过且尚未过期的 row 的 RowId -> FilePos 条目。对于大表（亿级行），映射规模可达 GB 级：

- 1 亿行 x 16B/entry = 1.6GB
- TabletServer 需要加载/缓存映射用于实时删除路径

更关键的问题是：映射作为 snapshot 附属，**每个 snapshot 都需要一份全量映射**。当前方案的双 CF（RowPosIndex + pendingRowPos）虽然复杂，但本质上是单份数据做增量覆盖——新位置直接覆盖旧位置，存储开销 = 存活行数。而 snapshot 附属模式下：

- 每轮 tiering 产生一个新 snapshot，需要生成一份**完整的**映射文件
- 映射无法像 RocksDB CF 那样做 in-place 覆盖，每次都是全量写出
- 若保留多个 snapshot（Iceberg 默认行为），就有多份全量映射存储
- 生成全量映射的开销也随表规模线性增长（每轮 tiering 都要写 GB 级文件）

双 CF 的复杂度换来的是增量更新能力（Ingest SST 只写新增/变更条目）。snapshot 附属消除了双 CF，但丢失了增量能力，代价是全量膨胀。

**增量附属的变体**：如果改为每个 snapshot 只存 delta（新增/变更条目），TabletServer 加载时合并增量，可以解决全量膨胀问题。但这实际上在向当前方案收敛：

- 当前方案：每轮 tiering 生成 SST（增量映射），TabletServer 通过 RocksDB Ingest 合并，RocksDB 自动处理多版本合并、compaction、point-get
- 增量附属：每轮 tiering 生成 delta 文件（增量映射），TabletServer 下载 delta 合并到本地缓存，需自己管理多版本合并、累积 delta 的定期压缩、恢复时按顺序回放

两者本质上是同一件事——当前方案的 SST + Ingest **就是**"增量映射作为 snapshot 附属，由 RocksDB 自动合并"。当前方案真正的复杂度来源不是 SST Ingest 本身，而是 pendingRowPos（双 CF）和 PendingDeletes（timing gap），这两个问题在增量附属方案里同样存在。所以增量附属 = 当前方案的 SST 机制 - RocksDB 的自动合并能力 + 手动合并实现，并没有减少复杂度，反而丢掉了 RocksDB 提供的基础设施。

**问题二：Pending Delete 仍然不可避免**

映射来自 DV-readable snapshot 的附属，只包含该 snapshot 已 tier 过的 row 的位置。对于正在 tiering 管道中的 row（已写入 changelog 但 TieringService 尚未报告位置），映射中没有对应条目。此时到达的删除会 miss：

```
DV-readable snapshot S_old 覆盖到 offset=40
当前 tiering 区间 [40, 60]
offset=50 的 row 正在被 TieringService 写入 Iceberg，位置还不知道

DELETE(oldRowId=50) 到达:
  -> 查映射（对应 S_old，只有 offset<40 的条目）-> miss
  -> 位置未知，无法标记 LakeDv
  -> 需要记录 pending，等新映射到达后再补标记
```

这和当前方案的 Case Y 本质相同：**只要映射不是实时更新的（无论存储在 RocksDB 还是 snapshot 附属），映射更新之前到达的删除就会 miss，就需要 pending 机制**。

方案二仍需某种形式的 PendingDeletes 来跟踪这些 miss 的删除，等新映射到达时补充 LakeDv 标记。

**问题三：外部 compaction 的 readable switch 重定向**

外部 compaction 期间，union read 基于 DV-readable snapshot（Fluss 的 snapshot），映射与之一致，**不影响正确性**。但在 Fluss 下一次 tiering 检测到外部 compaction 并切换到新 snapshot 时，存在一个重定向问题：

```
S_old 映射: RowId=10 -> file_A:pos5

外部 compaction 把 file_A 重写为 file_C（RowId=10 现在在 file_C:pos3）
Fluss 还没感知，TabletServer 缓存仍是 S_old 的映射

DELETE(RowId=10) 到达:
  -> 查映射: RowId=10 -> file_A:pos5
  -> LakeDv[file_A] |= {pos5}       // 标记在旧文件上
  -> union read 基于 S_old，file_A 存在 -> 正确

下一次 Fluss tiering，检测到外部 compaction，重建映射:
  -> 新映射: RowId=10 -> file_C:pos3
  -> readable switch 到新 snapshot

问题：LakeDv 标记的是 file_A:pos5，但新 snapshot 里 file_A 已不存在
      file_C:pos3 没有被标记 -> RowId=10 在新 snapshot 中复活
```

解决问题二和问题三都需要 TabletServer 维护一个"已删除但未物化的 RowId 集合"：
- 问题二：映射 miss 时记录 RowId，新映射到达后解析位置补标记 LakeDv
- 问题三：readable switch 时用新映射重新解析已删除 RowId，把 LakeDv 标记从旧文件转移到新文件

这个跟踪机制本质上就是当前方案 PendingDeletes 的变体。方案二消除了双 CF（RowPosIndex + pendingRowPos），但 PendingDeletes 类似的机制仍然需要。

---

## 4. 对比总结

| 维度 | 当前方案 | 方案一（DeletedRowIdSet） | 方案二（Snapshot 附属） |
|------|---------|--------------------------|------------------------|
| Fluss 实时层复杂度 | 高（6 CF, 双 CF 协议, PendingDeletes） | **低（2 CF, 简单 set add）** | 中（无双 CF，但仍需 PendingDeletes 类似机制） |
| Safety Proof | 困难（多不变量交织） | **简单（单一集合语义）** | 中（映射与 snapshot 绑定，不变量较少） |
| 外部 Compaction | 复杂（detection + scan + SST rebuild） | **透明（无感知）** | 中（Fluss tiering 时重建受影响映射） |
| Union Read 性能 | **最优（position bitmap）** | 低（需读 `__rowid` 列，compaction 后退化为 equality delete） | **最优（position bitmap）** |
| Tiering 物化开销 | 低（位置已知） | 高（需扫描 Iceberg 文件做 RowId 反查） | 低（位置已知） |
| 引擎接入成本 | **低（标准 position delete）** | 高（引擎需理解 `__rowid` + DeletedRowIdSet） | **低（标准 position delete）** |
| 映射存储开销 | 中（RocksDB local） | **无** | 高（snapshot 附属，全量映射） |

---

## 5. 方案三：当前方案的协议简化（Deferred Ingest）

方案一和方案二都尝试从根本上改变映射的存储方式，但分析表明各有硬伤。回到当前方案本身，其复杂度的**核心根因**是：在 position report 阶段就 Ingest SST 到 pendingRowPos，导致 RowPosIndex 和 pendingRowPos 必须共存，进而引发双 CF point-get、hard-link 复制、pendingDeletedRowIds 追踪等一系列复杂度。

**关键观察**：position report 到 readable switch 之间，union read 仍然使用旧 snapshot。§4.2 的删除只需查 RowPosIndex（旧位置），标记的 LakeDv 对旧 snapshot 完全正确。**不需要同时看到新旧两个位置**——因为读端还没切到新 snapshot。

因此可以**把 SST Ingest 推迟到 readable switch 时才做**，消除 pendingRowPos。

### 5.1 数据结构变更

DvRocksDB 从 6 个 CF 简化为 5 个：

| CF | Key | Value | 变更 |
|---|---|---|---|
| RowPosIndex | RowId (8B) | FilePos (8B) | **保留**，始终反映当前 readable snapshot |
| ~~pendingRowPos~~ | | | **删除** |
| DeletedRowIdSet | RowId (8B) | FilePos (8B) 或空 | **替代 PendingDeletes**，语义更简单 |
| LakeDv | file_id (4B) | del_bitmap | 不变 |
| LogDv | offset_range | del_bitmap | 不变 |
| FileDict | file_path <-> file_id | bidirectional | 不变 |

### 5.2 写入路径简化（§4.2）

```
-U/-D 到达，提取 oldRowId:

1. Acquire DvRWLock write lock
2. Point-get RowPosIndex[oldRowId]:       // 单次 point-get，不再查 pendingRowPos
   - Hit (file_id, pos):
       LakeDv[file_id] |= {pos}
       delete RowPosIndex[oldRowId]
       DeletedRowIdSet[oldRowId] = (file_id, pos)
   - Miss:
       DeletedRowIdSet[oldRowId] = pending   // 等 readable switch 时解析
3. Update LogDv
4. Release DvRWLock write lock
```

对比当前方案：
- **1 次 point-get**（当前方案 2 次：RowPosIndex + pendingRowPos）
- **无 Case X / Case Y 区分**（统一处理）
- **无 sentinel {0,0} 语义**

### 5.3 Position Report 简化（§5.3）

```
Phase 1（无锁）：
  - 下载 SST 到本地临时路径

Phase 2（DvRWLock write lock）：
  - 写 newFileDictEntries 到 FileDict CF
  - 存储 SST 路径（不 Ingest，不 hard-link）
  - 构建 snapshotBitmap
  - Release DvRWLock write lock

发送 ready ack
```

对比当前方案，**整个 Phase 2 删除了**：
- ~~hard-link SST~~
- ~~Ingest SST 到 pendingRowPos~~
- ~~reverse-scan PendingDeletes~~（移至 readable switch 作为 batch resolve，操作本质相同）
- ~~pendingDeletedRowIds 追踪~~

Position report 变成了纯粹的"下载 + 存储"，几乎无锁竞争。

### 5.4 Readable Switch 简化（§5.4）

```
Acquire DvRWLock write lock

1. Ingest SST 到 RowPosIndex          // 新位置覆盖旧位置（此时无人读旧 snapshot 了）

2. Batch 解析 DeletedRowIdSet:
   for each (R, v) in DeletedRowIdSet:
     hit = RowPosIndex.get(R)
     if hit:
       // Case Y（timing gap）或 外部 compaction 重写了已删除的 row
       // SST 写入了新位置 -> 需要标记 LakeDv
       LakeDv[hit.file_id] |= {hit.pos}
       delete RowPosIndex[R]                // 清除"僵尸"条目
       DeletedRowIdSet[R] = (hit.file_id, hit.pos)  // 更新为实际位置
     else:
       // R 不在本轮 SST 中
       if v == pending && R < currentTieredOffset:
         // 孤儿：row 被 logDvSnapshot 过滤，从未写入 Iceberg
         delete DeletedRowIdSet[R]

3. Cleanup oldFiles 对应的 LakeDv 条目
4. Bitmap diff cleanup LakeDv（使用 snapshotBitmap）
5. Cleanup DeletedRowIdSet 中已物化的条目（position 在 snapshotBitmap 中）
6. Cleanup 过期 LogDv
7. 更新 readableSnapshotId

Release DvRWLock write lock
发送 switched ack
```

步骤 2 的 batch 解析统一处理了当前方案的两个难题：
- **Case Y（timing gap）**：row 在 tiering 管道中，§4.2 miss。Ingest 后 RowPosIndex 有了新位置，batch lookup 命中，补标记 LakeDv。
- **外部 compaction 重写**：§4.2 标记了旧文件位置。SST 包含新文件位置，Ingest 覆盖后，batch lookup 命中新位置，补标记 LakeDv。

复杂度 O(|DeletedRowIdSet|) point-get，与当前方案的 reverse-scan O(|PendingDeletes|) 相同。

### 5.5 正确性验证

**场景 1：正常删除**

```
RowPosIndex: {0->(A,pos0), 1->(A,pos1)}

DELETE(oldRowId=0):
  RowPosIndex[0] hit -> LakeDv[A] |= {0}, delete RowPosIndex[0]
  DeletedRowIdSet[0] = (A, pos0)

Position report: SST = {4->(B,pos0)}，存储，不 Ingest

Readable switch:
  1. Ingest SST -> RowPosIndex: {1->(A,pos1), 4->(B,pos0)}
  2. DeletedRowIdSet[0]: RowPosIndex[0] miss -> no action
  3. Bitmap diff cleanup...

结果正确 ✓
```

**场景 2：Case Y（timing gap）**

```
Tiering 区间 [40, 60]，offset=50 的 row 在管道中

DELETE(oldRowId=50):
  RowPosIndex[50] miss -> DeletedRowIdSet[50] = pending

Position report: SST = {50->(B,pos0)}，存储

Readable switch:
  1. Ingest SST -> RowPosIndex 新增 {50->(B,pos0)}
  2. DeletedRowIdSet[50]: RowPosIndex[50] = (B,pos0) hit!
     -> LakeDv[B] |= {pos0}, delete RowPosIndex[50]

结果正确 ✓  无需 sentinel {0,0}，遍历解析移至 readable switch
```

**场景 3：外部 compaction**

```
RowPosIndex: {10->(A,pos0), 20->(A,pos1)}
外部 compaction: file_A -> file_C

DELETE(oldRowId=10):
  RowPosIndex[10] = (A,pos0) hit -> LakeDv[A] |= {pos0}
  delete RowPosIndex[10], DeletedRowIdSet[10] = (A,pos0)

TieringService 检测 compaction，SST = {10->(C,pos0), 20->(C,pos1), ...}

Readable switch:
  1. Ingest SST -> RowPosIndex: {10->(C,pos0), 20->(C,pos1)}
     （RowId=10 被 §4.2 删了，但 Ingest 重新写入了新位置 -> "僵尸"条目）
  2. DeletedRowIdSet[10]: RowPosIndex[10] = (C,pos0) hit!
     -> LakeDv[C] |= {pos0}, delete RowPosIndex[10]
  3. Cleanup oldFiles: 清除 LakeDv[A]

结果：LakeDv[C] = {pos0}，旧文件 A 已清理
union read 新 snapshot -> file_C:pos0 被 LakeDv 遮蔽 ✓
```

**场景 4：position report 到 readable switch 期间的并发删除**

```
RowPosIndex: {10->(A,pos0)}
Position report 已到达，SST 已存储（未 Ingest）

DELETE(oldRowId=10):
  RowPosIndex[10] = (A,pos0) hit -> LakeDv[A] |= {pos0}  // 旧位置，对旧 snapshot 正确
  delete RowPosIndex[10], DeletedRowIdSet[10] = (A,pos0)

union read（旧 snapshot）: file_A, LakeDv[A] 遮蔽 pos0 -> 正确 ✓

Readable switch:
  1. Ingest SST -> 若 SST 含 RowId=10（外部 compaction），RowPosIndex 新增
  2. DeletedRowIdSet[10]: 若 RowPosIndex hit -> 补标记新位置
  3. 若 RowPosIndex miss -> 不处理（row 没被重写，旧标记已物化）

结果正确 ✓  无需双 CF 即可正确处理
```

### 5.6 消除与简化的对比

| 项目 | 当前方案 | 简化后 |
|------|---------|-------|
| DvRocksDB CF 数 | 6 | **5**（删除 pendingRowPos） |
| §4.2 point-get 次数 | 2（RowPosIndex + pendingRowPos） | **1**（仅 RowPosIndex） |
| §4.2 Case 区分 | Case X / Case Y，不同处理逻辑 | **统一**：hit 标记 + 始终写 DeletedRowIdSet |
| §5.3 Phase 2 操作 | hard-link + Ingest + reverse-scan + pendingDeletedRowIds | **无**（仅存储 SST 路径） |
| PendingDeletes 语义 | sentinel {0,0} + filePos 值 + reverse-scan 解析 | **简化**：pending / filePos，batch lookup 解析（遍历操作本身仍在，移至 readable switch） |
| readable switch | Ingest hardlink + pendingDeletedRowIds 清理 + DropCF | **Ingest SST + batch 解析 DeletedRowIdSet** |
| 并发不变量 | §4.2 看 2 个 CF，§5.3 修改 pendingRowPos + PendingDeletes | §4.2 看 1 个 CF，§5.4 做 Ingest + batch 解析 |

**保留不变的**：LakeDv（position bitmap）、LogDv、FileDict、DV 物化流程、两阶段 ack barrier、snapshotBitmap diff cleanup、union read 协议。

### 5.7 为什么这个简化是安全的

核心不变量：**在 readable switch 之前，RowPosIndex 始终反映当前 DV-readable snapshot 的位置**。

- §4.2 查 RowPosIndex，命中则标记旧 snapshot 对应文件的 LakeDv -> 对旧 snapshot 正确
- Union read 使用旧 snapshot，LakeDv 与旧 snapshot 一致 -> 正确
- Readable switch 时才 Ingest，原子地将 RowPosIndex 切换到新 snapshot
- Ingest 后 batch 解析 DeletedRowIdSet，处理所有遗留的 timing gap 和外部 compaction 重写
- 解析在 DvRWLock write lock 内完成，与 §4.2 和 union read 互斥 -> 无并发问题

当前方案的 pendingRowPos 存在的原因是"§4.2 需要同时看到新旧位置"。但这个需求的前提是 SST 在 position report 阶段就 Ingest 了。**推迟 Ingest 消除了这个前提**，也就消除了 pendingRowPos 的存在必要。

### 5.8 Trade-off：工作分布与可用性窗口

Deferred Ingest 的总工作量与当前方案相同，但工作分布不同：

| 阶段 | 当前方案 | Deferred Ingest |
|------|---------|-----------------|
| Position report | **重**：Ingest + reverse-scan PendingDeletes + hard-link（DvRWLock write lock） | **轻**：下载 SST + 存储路径 |
| Readable switch | **轻**：Ingest hard-link + DropCF + 轻量清理 | **重**：Ingest SST + batch 解析 DeletedRowIdSet O(\|DeletedRowIdSet\|) point-gets |

当前方案在 position report 阶段完成重活后，TabletServer 已具备服务 S_new 的能力（pendingReadableSnapshotId 优化）。Publish 到 readable switch 之间的窗口内，客户端可以无感知地使用 S_new。

Deferred Ingest 在 readable switch 完成前无法服务 S_new。Publish 到 readable switch 之间的窗口内，请求 S_new 的客户端会收到 stale error 需重试。

**窗口大小**：Ingest 是 O(1) 元数据操作；batch resolve 在 3 分钟 tiering、~100 delete/s 的场景下约 1.8 万条 point-get，RocksDB 内存 point-get 微秒级，总计约几十毫秒。窗口很短，但确实是一个可用性退化。

**如果需要恢复 pendingReadableSnapshotId 优化**：可在 position report 阶段用 SstFileReader 读取 SST（不 Ingest）做 batch resolve，提前 patch LakeDv。这样 TabletServer 在 position report 后即可服务 S_new，同时仍然不需要双 CF（SST 推迟到 readable switch 才 Ingest）。但 position report 会变重（增加 batch resolve），部分抵消了简化收益。

**总结**：Deferred Ingest 用"publish 到 switch 之间几十毫秒的可用性窗口"换取"消除双 CF + reverse-scan 的协议复杂度"。

---

## 6. 结论

**方案一（DeletedRowIdSet）**写入路径极简，但存在三个硬伤：
1. Union Read 必须读 `__rowid` 列做匹配，compaction 后退化为 equality delete 级别读放大
2. Tiering 物化需扫描 Iceberg 文件做 RowId -> position 反查，本质和拿 PK 点查无异
3. 引擎接入需理解 `__rowid` 私有语义和 DeletedRowIdSet 匹配逻辑

**方案二（Snapshot 附属）**保留了 position-based deletion 的读性能，但全量映射存储开销大（每 snapshot GB 级），增量附属则收敛回当前方案的 SST 机制，且 PendingDeletes 仍不可避免。

**方案三（Deferred Ingest）**在当前方案基础上做协议简化，消除了 pendingRowPos（双 CF），保留了所有核心优势（position-based deletion、增量 SST、RocksDB 基础设施）。PendingDeletes 本身没有消除（重命名为 DeletedRowIdSet，数据结构基本相同），但其解析协议从 reverse-scan + pendingRowPos lookup 简化为 readable switch 时的 batch RowPosIndex lookup。复杂度从"3 种数据结构 + 双 CF 协议 + reverse-scan"降到"2 种数据结构（RowPosIndex + DeletedRowIdSet）+ 单 CF + batch lookup"。代价是 publish 到 readable switch 之间存在几十毫秒的可用性窗口（丢失 pendingReadableSnapshotId 优化）。
