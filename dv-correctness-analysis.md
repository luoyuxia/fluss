# Fluss Deletion Vector 数据正确性全面分析

基于设计文档（含 PendingDeletes 修复方案）的完整设计，系统性分析所有可能的数据正确性问题。

---

## 分析维度

1. 写入路径正确性
2. DV 更新正确性（LakeDv / LogDv / RowPosIndex）
3. Position Report 与 PendingDeletes 交互正确性
4. Tiering 流程正确性
5. Snapshot 处理正确性
6. Union Read 正确性
7. 恢复流程正确性
8. Compaction 交互正确性
9. LakeDv 物化与清理正确性

---

## 正确性确认项（无问题）

### 1. RowId 唯一性 ✅

RowId = `+I`/`+U` 的 log offset，在单个 bucket 内严格单调递增且唯一。不同 bucket 有独立的 DvRocksDB，不会冲突。

### 2. KV State 中 RowId 的一致性 ✅

PUT 时，先从旧 value 提取 oldRowId，再用 newRowId 覆盖 KV State。整个操作在 KvTablet 写锁内完成（§6.1），不会有并发写入同一个 key 的情况。

### 3. LakeDv 更新时机与 log_hw 的顺序 ✅

§6.2 中，先更新 DV（步骤 3-5），再更新 log_hw（步骤 6）。如果顺序反过来，union read 可能看到更大的 logEndOffset 但 DV 还没更新，导致已删除的行被读出。当前设计正确。

### 4. LakeDv 与 RowPosIndex 的原子性 ✅

§6.2 步骤 4 中，查 RowPosIndex → 更新 LakeDv → 删除 RowPosIndex，这三步在 LakeDv 写锁内完成。§8.2 Step 1 也需要获取 LakeDv 写锁，两者互斥。

### 5. 同一个 key 被快速连续更新两次 ✅

第一次更新的 oldRowId 在 RowPosIndex 中（已 tiering），正常处理。第二次更新的 oldRowId 不在 RowPosIndex 中（还没 tiering），写入 PendingDeletes。后续 tiering 时 locallyDeletedRowIds 会正确清理。

### 6. LakeDv 快照与 split 范围的对齐 ✅

§7.1 中，在读锁内同时读取 log_hw 和快照 LakeDv。LakeDv 精确覆盖 `[last_tiered_offset, log_hw]` 范围内所有对 Iceberg 的删除。重复物化是幂等的。

### 7. Union Read 一致性 ✅

§10 中，在 KvTablet 读锁内同时获取 logEndOffset、LakeDv、LogDv，与 §6.2 的写锁互斥，保证三者的一致性快照。

### 8. Bitmap 差集清理 ✅

§13.3 中，`当前 bitmap AND NOT 快照时的 bitmap` 正确保留了新增的删除，清理了已物化的删除。

---

## 发现的问题

---

### 问题 A：Position Report 重试导致 RowPosIndex 残留

**严重程度**：中

**触发条件**：Tiering Writer commit 成功后上报 positionReport，RPC 超时导致 Writer 重试上报。

**详细 Workflow**：

```
前置状态：
  - RowId=80 对应的行已被 DELETE 处理
  - PendingDeletes 中有 RowId=80
  - RowPosIndex 中无 RowId=80

Step 1：Tiering Writer commit 成功，上报 positionReport
  positionReport = {file_X: [(RowId=80, pos=3)]}

Step 2：TabletServer 第一次收到 positionReport（§7.3）
  → 检查 PendingDeletes：RowId=80 存在
  → 在 LakeDv 中标记 file_X:pos3 为已删除 ✅
  → 从 PendingDeletes 中移除 RowId=80
  → 不写入 RowPosIndex

Step 3：RPC 超时，Tiering Writer 重试上报相同的 positionReport

Step 4：TabletServer 第二次收到 positionReport（§7.3）
  → 检查 PendingDeletes：RowId=80 不存在（已在 Step 2 移除）
  → 认为该行存活
  → 写入 RowPosIndex：RowId=80 → {file_X, pos3}  ← 错误！

结果：
  - RowPosIndex 中出现了不该存在的残留条目
  - LakeDv 已正确标记（Step 2），所以 Iceberg 层面的删除是正确的
  - 但 RowPosIndex 残留会影响后续 §8.2 的判断
    （§8.2 查 RowPosIndex 发现 RowId=80 存在，认为该行存活）
```

**影响链**：

```
RowPosIndex 残留 RowId=80
        │
        ▼
§8.2 处理新 snapshot 时，查 RowPosIndex 发现 RowId=80 存在
        │
        ▼
认为该行存活，不在 LakeDv 中标记新文件的对应 position
        │
        ▼
如果 compaction 将 file_X 重写为 file_Y，
新文件 file_Y 中该行没有 LakeDv 标记
        │
        ▼
旧的 LakeDv 标记（file_X:pos3）在 §8.2 Step 3 被清理
        │
        ▼
该行在新 snapshot 中既无物理 DV 也无逻辑 LakeDv
        │
        ▼
Union read 中该行重新暴露 ← 数据错误
```

**修复建议**：

方案 1：让 position report 处理具有去重能力——TabletServer 记录已处理过的 tiering commit id，重复的 report 直接忽略。

方案 2：在 §7.3 写入 RowPosIndex 前，额外检查 LakeDv 中该 file_id:row_position 是否已被标记删除。如果已标记，跳过写入。

方案 3：§7.3 中不从 PendingDeletes 中移除条目，而是在 LakeDv 物化清理时（§13.3）统一清理。但这会增加 PendingDeletes 的生命周期。

---

### 问题 B：§6.2 和 §7.3 之间缺乏原子性保护

**严重程度**：高

**触发条件**：DELETE 处理（§6.2）和 position report 处理（§7.3）并发执行。

**详细 Workflow**：

```
前置状态：
  - Tiering 正在进行，RowId=80 的 +I 已被写入 file_X:pos3
  - RowPosIndex 中无 RowId=80（position report 尚未到达）
  - PendingDeletes 中无 RowId=80

并发执行：

  线程 A（§6.2 changelog 同步）         线程 B（§7.3 position report）
  ─────────────────────────────         ──────────────────────────────
  获取 LakeDv 写锁
  查 RowPosIndex：RowId=80 → 找不到
  准备写 PendingDeletes{80}
  释放 LakeDv 写锁
                                        检查 PendingDeletes：RowId=80 不存在
                                        写入 RowPosIndex：RowId=80 → {file_X, pos3}
  写入 PendingDeletes{80}

结果：
  - RowPosIndex 中有 RowId=80 → {file_X, pos3}（线程 B 写入）
  - PendingDeletes 中有 RowId=80（线程 A 写入）
  - LakeDv 中没有 file_X:pos3 的删除标记

后续影响：
  1. 下一次 §8.2 处理 snapshot 时，PendingDeletes 中有 RowId=80
     → LakeDv 标记 file_X 的新 position 删除（如果 file_X 在 newFiles 中）
     → 但如果 file_X 不在 newFiles 中（没有 compaction），PendingDeletes 不会被触发
  2. 下一次 position report 到达时（如果有的话），PendingDeletes 中有 RowId=80
     → LakeDv 标记删除 → 但此时 RowPosIndex 也有 RowId=80，不会被清理
  3. RowPosIndex 残留 + LakeDv 可能遗漏 → 与问题 A 相同的影响链
```

**根因**：§7.3 的 position report 处理没有在 LakeDv 写锁内执行，导致与 §6.2 的检查-写入操作不是原子的。

**修复建议**：

§7.3 的 position report 处理必须在 LakeDv 写锁内执行。具体来说，以下操作必须在同一个 LakeDv 写锁内原子完成：

```
获取 LakeDv 写锁
  对每个 (RowId, file_path, row_position):
    1. 检查 PendingDeletes
    2. 如果在 PendingDeletes 中 → 标记 LakeDv，移除 PendingDeletes，不写 RowPosIndex
    3. 如果不在 PendingDeletes 中 → 写入 RowPosIndex
释放 LakeDv 写锁
```

这样，§6.2 和 §7.3 通过 LakeDv 写锁互斥，不会出现上述竞态。

---

### 问题 C：边界条件 `oldRowId == last_tiered_offset` 处理可能错误

**严重程度**：高

**触发条件**：一条 `-U`/`-D` 的 `oldRowId` 恰好等于 `last_tiered_offset`。

**详细 Workflow**：

```
前置状态：
  - 上一轮 tiering split 范围：[30, 50]（包含 offset=50）
  - 上一轮 tiering 将 offset=50 的 +I(key1, v1) 写入 file_A:pos7
  - last_tiered_offset = 50
  - 当前轮 tiering split 范围：[50, 100]（或 (50, 100]？语义不明确）

Step 1：当前 split 中有 -U(offset=70, oldRowId=50)

Step 2：Tiering Writer 判断 oldRowId=50 vs last_tiered_offset=50
  → 当前设计：oldRowId >= last_tiered_offset → 认为是本轮内部删除
  → 从 positionReport 中查找 RowId=50

Step 3：positionReport 中没有 RowId=50
  （因为 offset=50 的 +I 是在上一轮 tiering 中写入的，不在当前 split 的 positionReport 中）

Step 4：查找失败
  → 该删除被遗漏
  → file_A:pos7 没有被任何 DV 覆盖

Step 5：LakeDv 快照中是否包含这条删除？
  → 取决于 DELETE 到达的时机：
    a. 如果 DELETE 在 LakeDv 快照之前到达：
       - §6.2 查 RowPosIndex 找到 file_A:pos7 → LakeDv 标记 → LakeDv 快照包含 → ✅ 正确
       - 但 Tiering Writer 仍然会尝试从 positionReport 查找（因为 oldRowId >= last_tiered_offset）
       - 查找失败 → Tiering Writer 如何处理？如果报错则中断 tiering；如果跳过则依赖 LakeDv 快照
    b. 如果 DELETE 在 LakeDv 快照之后到达：
       - LakeDv 快照不包含这条删除
       - Tiering Writer 从 positionReport 查找失败
       - 该删除被完全遗漏 ← 数据错误

结果（情况 b）：
  - file_A:pos7 没有 Puffin DV
  - LakeDv 中可能有标记（如果 §6.2 在 LakeDv 快照之后处理了 DELETE）
  - 但 LakeDv 标记不会被物化到当前轮的 Puffin DV 中（快照已拍）
  - 需要等到下一轮 tiering 才能物化
  - 在此期间 union read 依赖 LakeDv 逻辑标记 → 如果 LakeDv 已标记则 union read 正确
  - 但如果 DELETE 在 LakeDv 快照之后、且在 union read 之前到达 → union read 正确
  - 如果 DELETE 在 union read 之后到达 → union read 可能读到旧行（但这是正常的时序行为）
```

**根因**：split 范围的开闭语义不明确，导致 `oldRowId == last_tiered_offset` 的判断可能错误。

**修复建议**：

1. 明确 split 范围的开闭语义。推荐使用**左开右闭** `(last_tiered_offset, latest_offset]`，即当前 split 不包含 `last_tiered_offset` 本身（它已在上一轮处理）。
2. 将 Tiering Writer 中的判断条件从 `oldRowId >= last_tiered_offset` 改为 `oldRowId > last_tiered_offset`。
3. 在文档中显式说明 split 范围语义和判断条件。

---

### 问题 D：§8.2 Step 3 清理 oldFile 的 LakeDv 时的锁保护

**严重程度**：中

**触发条件**：§8.2 Step 1 和 Step 3 之间，§6.2 为 oldFile 添加了新的 LakeDv 条目。

**详细 Workflow**：

```
前置状态：
  - Iceberg 有 snapshot S2，包含 file_A
  - Compaction 将 file_A 重写为 file_B，产生 snapshot S3
  - newFiles = {file_B}，oldFiles = {file_A}

并发执行：

  §8.2 处理 S3                          §6.2 changelog 同步
  ──────────────                        ──────────────────
  Step 1：获取 LakeDv 写锁
    处理 file_B 的 RowId 映射
    释放 LakeDv 写锁
                                        获取 LakeDv 写锁
                                        -D(oldRowId=X) 到达
                                        查 RowPosIndex：RowId=X → {file_A, pos5}
                                          （§8.2 Step 1 可能还没更新 RowPosIndex，
                                            或者 RowId=X 在 file_A 中但不在 file_B 中）
                                        LakeDv 标记 file_A:pos5 删除
                                        释放 LakeDv 写锁
  Step 3：清理 oldFiles
    获取 LakeDv 写锁（如果有的话）
    删除 LakeDv 中 file_A 的所有条目
      → 包括 §6.2 刚添加的 file_A:pos5 标记 ← 被错误清理
    释放 LakeDv 写锁

结果：
  - file_A 的 LakeDv 标记被清理（file_A 不在 S3 中，清理本身合理）
  - 但 file_B 中对应的行没有被标记删除
  - 如果 RowId=X 在 file_B 中存在（compaction 保留了该行），
    则该行在 S3 中既无物理 DV 也无逻辑 LakeDv → 数据错误
```

**但需要进一步分析**：

如果 §8.2 Step 1 在处理 file_B 时，发现 RowId=X 不在 RowPosIndex 中（因为 §6.2 已经删除了），会在 LakeDv 中标记 file_B 的对应 position 删除。这种情况下是正确的。

关键在于 §8.2 Step 1 和 §6.2 的执行顺序：

- 如果 §6.2 先完成（删除 RowPosIndex 中的 RowId=X）→ §8.2 Step 1 查 RowPosIndex 找不到 → LakeDv 标记 file_B 删除 → ✅ 正确
- 如果 §8.2 Step 1 先完成（RowPosIndex 更新为 file_B 的 FilePos）→ §6.2 查 RowPosIndex 找到 file_B 的 FilePos → LakeDv 标记 file_B 删除 → ✅ 正确
- 如果 §6.2 在 §8.2 Step 1 之后、Step 3 之前执行，且 RowPosIndex 仍指向 file_A（§8.2 Step 1 没有更新该 RowId，因为 RowId=X 可能不在 file_B 中）→ §6.2 标记 file_A → §8.2 Step 3 清理 file_A → **file_B 中没有标记** → ⚠️ 可能有问题

**但如果 RowId=X 不在 file_B 中**，说明 compaction 时该行已经被 Iceberg DV 过滤掉了，不需要额外标记。所以这种情况是正确的。

**如果 RowId=X 在 file_B 中**，§8.2 Step 1 应该已经处理了 RowId=X（从 file_B 中扫描到），并更新了 RowPosIndex。所以 §6.2 查到的应该是 file_B 的 FilePos。

**结论**：在 LakeDv 写锁保护下，§6.2 和 §8.2 Step 1 是互斥的。但 §8.2 Step 3 是否也在 LakeDv 写锁内？

**修复建议**：确保 §8.2 的 Step 1 和 Step 3 在同一个 LakeDv 写锁内执行，或者 Step 3 单独获取 LakeDv 写锁。文档应明确说明。

---

### 问题 E：恢复后 PendingDeletes 中的条目可能永远无法被清理

**严重程度**：低

**触发条件**：Checkpoint 后 position report 到达并被处理，随后 crash 恢复。

**详细 Workflow**：

```
Step 1：正常运行
  - DELETE(key1) 到达，oldRowId=80
  - RowPosIndex 查不到 → PendingDeletes{80}
  - DvRocksDB checkpoint（PendingDeletes 包含 RowId=80）
  - checkpointLogHw = 120

Step 2：Checkpoint 之后
  - Position report 到达，RowId=80 匹配 PendingDeletes
  - LakeDv 标记 file_X:pos3 删除
  - PendingDeletes 移除 RowId=80
  - 后续正常运行...

Step 3：Crash 发生

Step 4：恢复
  - 从 checkpoint 加载 DvRocksDB
  - PendingDeletes 恢复到 checkpoint 时的状态：包含 RowId=80
  - 从 checkpointLogHw + 1 = 121 开始重放 changelog
  - DELETE(key1, oldRowId=80) 的 offset < 121，不会被重放
  - PendingDeletes 中的 RowId=80 残留

Step 5：后续运行
  - 如果后续有新的 position report 包含 RowId=80 → PendingDeletes 被清理 ✅
  - 如果没有新的 position report（Tiering Writer 已完成该轮 tiering）
    → PendingDeletes{80} 永远残留
  - §8.2 处理新 snapshot 时，如果 newFile 中有 RowId=80
    → PendingDeletes 触发 LakeDv 标记（幂等，正确）
  - 如果 newFile 中没有 RowId=80（该行已被 compaction 过滤）
    → PendingDeletes{80} 永远无法被清理

结果：
  - 不会导致数据错误（PendingDeletes 只会导致 LakeDv 多标记删除，不会导致遗漏）
  - 但会导致 PendingDeletes 空间泄漏
  - 长期运行后，PendingDeletes 可能积累大量无法清理的条目
```

**修复建议**：

方案 1：在恢复流程中增加 PendingDeletes 的清理逻辑。对于 PendingDeletes 中的每个 RowId，检查 KV State 中对应的 key 是否仍然存在：
- 如果 key 不存在（已被删除），保留 PendingDeletes 条目等待 position report
- 如果 key 存在且 KV State 中的 RowId ≠ PendingDeletes 中的 RowId（已被更新为新版本），说明 PendingDeletes 中的条目是过时的，可以安全清理

方案 2：在 §8.2 处理 snapshot 时，增加清理逻辑。遍历 PendingDeletes，对于不在任何当前 snapshot 文件中、也不在 RowPosIndex 中的 RowId，清理其 PendingDeletes 条目。

方案 3：为 PendingDeletes 条目设置 TTL。如果条目存在时间超过 N 轮 tiering 周期仍未被清理，自动移除。

---

### 问题 F：外部 Compaction 可能丢失 `__offset` 列

**严重程度**：高

**触发条件**：外部引擎（如 Spark）对 Fluss 管理的 Iceberg 表执行 compaction，且不保留 `__offset` 列。

**详细 Workflow**：

```
Step 1：Fluss tiering 写入 Iceberg
  data_file_A:
    pos0 → (key1, v1, __offset=0)
    pos1 → (key2, v2, __offset=1)
    pos2 → (key3, v3, __offset=2)

Step 2：外部 Spark job 执行 compaction
  读取 file_A，重写为 file_B
  如果 Spark 不知道 __offset 列的特殊含义：
    - 可能丢弃 __offset 列（如果配置了列裁剪）
    - 可能重新排序行（改变 row position）

Step 3：新 snapshot S3 到达 TabletServer
  newFiles = {file_B}，oldFiles = {file_A}
  file_B 不在 knownFiles 中 → 回退扫描

Step 4：扫描 file_B 读取 __offset 列
  → __offset 列不存在
  → 无法重建 RowId → position 映射
  → RowPosIndex 无法更新

Step 5：后续 DELETE 到达
  - 查 RowPosIndex：RowId 指向旧文件 file_A 的 FilePos（已被 §8.2 Step 3 清理）
  - 或者 RowPosIndex 中根本没有该 RowId 的条目
  - LakeDv 无法正确标记
  → 数据错误
```

**修复建议**：

1. 在 Iceberg 表的 schema 中将 `__offset` 列标记为 required/non-null，确保 compaction 不会丢弃它。
2. 在文档中明确说明：外部 compaction 必须保留 `__offset` 列，否则 DV 功能会失效。
3. 在 §8.1 的兜底扫描中，如果发现 `__offset` 列缺失，应该报错并告警，而不是静默失败。
4. 考虑将 `__offset` 列设为 Iceberg 的 metadata column 或 hidden column，减少外部引擎误操作的风险。

---

## 额外发现的潜在问题

### 问题 G：§6.2 中 PendingDeletes 写入与 RowPosIndex 删除的顺序

**严重程度**：低

**分析**：§6.2 步骤 4 中，当 RowPosIndex 查不到 oldRowId 时：
- a. 写入 PendingDeletes{oldRowId}
- b. 在 RowPosIndex 中删除 oldRowId（无操作，本来就没有）

但如果 RowPosIndex 查到了 oldRowId：
- a. 更新 LakeDv
- b. 在 RowPosIndex 中删除 oldRowId

步骤 b 的 "删除 RowPosIndex" 在两种情况下都执行。如果查不到时也执行删除操作，虽然是无操作（delete non-existent key），但不会有正确性问题。

**结论**：✅ 无问题，但代码实现时可以优化为只在查到时才删除。

### 问题 H：多个 `-U`/`-D` 引用同一个 oldRowId

**严重程度**：无（不可能发生）

**分析**：同一个 RowId 只会被一次 `-U` 或 `-D` 引用，因为 RowId 对应的是 KV State 中的当前版本。一旦被更新或删除，KV State 中的 RowId 就变了。不可能有两条 `-U`/`-D` 引用同一个 oldRowId。

**结论**：✅ 不可能发生。

### 问题 I：Tiering Writer 处理 `-U`/`-D` 时 positionReport 中找不到 oldRowId（同 split 内）

**严重程度**：低

**触发条件**：同 split 内，`-U`/`-D` 的 oldRowId 对应的 `+I`/`+U` 在 changelog 中存在，但 Tiering Writer 的 positionReport 中没有记录。

**分析**：这不应该发生，因为 changelog 是有序的，`+I`/`+U` 一定在 `-U`/`-D` 之前被处理。除非 Tiering Writer 的处理逻辑有 bug（如并行处理 changelog 导致乱序）。

**建议**：在 Tiering Writer 中添加断言：如果 `oldRowId >= last_tiered_offset` 但 positionReport 中找不到，应该抛出异常而不是静默跳过。

---

## 问题汇总

| 编号 | 问题 | 严重程度 | 是否导致数据错误 | 修复复杂度 |
|------|------|----------|------------------|------------|
| **A** | Position Report 重试导致 RowPosIndex 残留 | 中 | 可能（通过影响链传播） | 低 |
| **B** | §6.2 和 §7.3 之间缺乏原子性保护 | 高 | 可能（RowPosIndex 残留 + LakeDv 遗漏） | 低（加锁即可） |
| **C** | 边界条件 `oldRowId == last_tiered_offset` | 高 | 可能（删除遗漏） | 低（改判断条件） |
| **D** | §8.2 Step 3 清理 oldFile 的锁保护 | 中 | 可能（LakeDv 标记被错误清理） | 低（确认锁范围） |
| **E** | 恢复后 PendingDeletes 残留 | 低 | 否（空间泄漏） | 中 |
| **F** | 外部 Compaction 丢失 `__offset` 列 | 高 | 是（DV 功能完全失效） | 中 |

### 优先修复顺序

1. **问题 B**（最高优先级）：§7.3 加 LakeDv 写锁，一行代码级别的修复
2. **问题 C**（高优先级）：明确 split 范围语义，改判断条件
3. **问题 F**（高优先级）：确保 `__offset` 列不可丢弃
4. **问题 A**（中优先级）：position report 去重
5. **问题 D**（中优先级）：确认 §8.2 Step 3 的锁范围
6. **问题 E**（低优先级）：PendingDeletes 清理策略
