# Fluss Deletion Vector 设计文档 v2 — 数据正确性全面分析

本文档对设计文档 v2（含 PendingDeletes 修复）进行逐场景的数据正确性分析。分析方法：枚举所有可能的事件交错顺序，验证在每种交错下，union read 和 Iceberg 物化后的数据是否正确。

正确性定义：
- 不重：已删除/已更新的旧版本不会被读出
- 不丢：存活的最新版本一定能被读出

---

## 分析 1：正常路径（无竞态）

场景：写入 → tiering → position report → DELETE → 下一轮 tiering

这是设计文档 §14 端到端示例覆盖的路径。DELETE 到达时 RowPosIndex 已有条目，直接更新 LakeDv。

结论：**正确**。文档已充分论证。

---

## 分析 2：DELETE 在 tiering 期间到达（PendingDeletes 场景）

这是之前发现的竞态条件，已通过 PendingDeletes 修复。

时序：+I(offset=80) → 生成 split [50,100] → Tiering Writer 写入 file_X:pos3 → DELETE(offset=105, oldRowId=80) 到达 → position report 到达

验证修复后的流程：

1. DELETE 到达时，RowPosIndex 查不到 RowId=80，oldRowId=80 >= pendingTieringStartOffset=50 → 写入 PendingDeletes
2. Position report 到达时，检查 PendingDeletes：RowId=80 存在 → 标记 LakeDv(file_X, pos3)，不写入 RowPosIndex，从 PendingDeletes 移除
3. 下一轮 tiering split 的 LakeDv 快照包含 file_X:pos3 → 物化为 Puffin DV

结论：**正确**。

---

## 分析 3：Position report 先于 DELETE 到达

时序：+I(offset=80) → 生成 split [50,100] → Tiering Writer 写入 → position report 到达 → DELETE(offset=105, oldRowId=80) 到达

1. Position report 到达时，PendingDeletes 中无 RowId=80 → 正常写入 RowPosIndex：80 → {file_X, pos3}
2. DELETE 到达时，RowPosIndex 查到 80 → {file_X, pos3} → 更新 LakeDv(file_X, pos3)，删除 RowPosIndex 中 RowId=80

结论：**正确**。这是正常路径。

---

## 分析 4：同 split 内先写后删

时序：+I(offset=80) → -D(offset=90, oldRowId=80)，两者都在 split [50,100] 内

Tiering Writer 处理：
1. 读到 +I(offset=80)，写入 file_X:pos3，记录 positionReport
2. 读到 -D(offset=90, oldRowId=80)，oldRowId=80 >= last_tiered=50 → 从 positionReport 查到 pos3 → 加入 localDv
3. Commit 时：file_X:pos3 有 Puffin DV 覆盖
4. 上报：positionReport 不包含 RowId=80（被 localDv 处理的行不上报），locallyDeletedRowIds 包含 RowId=80

TabletServer 侧：
- §6.2 处理 -D(oldRowId=80) 时，RowPosIndex 查不到（还没 tiering 完），写入 PendingDeletes
- §7.3 收到 locallyDeletedRowIds 包含 RowId=80 → 从 PendingDeletes 移除

结论：**正确**。Iceberg 中 file_X:pos3 有 DV 覆盖，PendingDeletes 被正确清理。

---

## 分析 5：同一个 key 在 split 期间被多次更新

时序：
- +I(key1, v1, offset=80) 在 split [50,100] 内
- PUT(key1, v2, offset=105) 在 split 外 → -U(offset=105, oldRowId=80) + +U(offset=106, key1, v2)
- PUT(key1, v3, offset=110) 在 split 外 → -U(offset=110, oldRowId=106) + +U(offset=111, key1, v3)

处理 -U(oldRowId=80)：
- RowPosIndex 查不到 → PendingDeletes 加入 RowId=80 ✓

处理 -U(oldRowId=106)：
- RowPosIndex 查不到（106 还没 tiering）→ 但 oldRowId=106 >= pendingTieringStartOffset？
- 这里有个问题：offset=106 是在 split [50,100] 之外的，pendingTieringStartOffset=50，106 >= 50 为 true
- 但 RowId=106 不在当前 tiering split 范围内，position report 不会包含 RowId=106
- PendingDeletes 中会残留 RowId=106

等等，RowId=106 对应的 +U(offset=106) 还没被 tiering，它还在 WAL 中。这条 -U(oldRowId=106) 删除的是 WAL 中的数据，不是 Iceberg 中的数据。所以不需要更新 LakeDv，只需要更新 LogDv。

问题在于：pendingTieringStartOffset=50，oldRowId=106 >= 50，所以会被写入 PendingDeletes。但 RowId=106 永远不会出现在当前 split 的 position report 中（因为 106 > 100 = split 的 latest_offset）。

这条 PendingDeletes 条目会在什么时候被清理？
- 下一轮 tiering split [101, ...] 会包含 +U(offset=106)
- 但 -U(offset=110, oldRowId=106) 也在这个 split 内
- Tiering Writer 处理时：oldRowId=106 >= last_tiered=101 → 同 split 内先写后删 → localDv 处理
- locallyDeletedRowIds 包含 RowId=106 → §7.3 从 PendingDeletes 移除

但如果 -U(offset=110) 在下一轮 split 之前就到达了 TabletServer（即 split [101, ...] 还没生成），那么 §6.2 处理 -U(oldRowId=106) 时：
- RowPosIndex 查不到 → pendingTieringStartOffset 此时是什么？
  - 如果上一轮 position report 已处理完，pendingTieringStartOffset 已清除（=-1）
  - oldRowId=106 >= -1 为 true → 又写入 PendingDeletes

**这里暴露了一个问题：pendingTieringStartOffset 清除后（=-1），所有查不到 RowPosIndex 的 oldRowId 都会被写入 PendingDeletes，即使该行确实还在 WAL 中、从未被 tiering 过。**

让我重新审视这个判断条件...

### 问题 A：PendingDeletes 的写入条件过于宽松

**问题描述**：

设计文档 v2 的 §6.2 中，当 RowPosIndex 查不到 oldRowId 时，将其加入 PendingDeletes。但这个条件没有区分两种情况：

1. 该行正在被 tiering，position report 还没回来（需要 PendingDeletes）
2. 该行从未被 tiering 过，仍在 WAL 中（不需要 PendingDeletes）

如果不加区分地写入 PendingDeletes，会导致：
- PendingDeletes 中积累大量不必要的条目（所有 WAL 内部的删除都会写入）
- 这些条目需要等到对应的 +I/+U 被 tiering 后才能清理
- 在此之前，PendingDeletes 会持续膨胀

**但这是否会导致数据正确性问题？**

分析：假设 RowId=106 被错误地写入 PendingDeletes。

情况 a：下一轮 tiering 包含 +U(offset=106) 和 -U(offset=110, oldRowId=106)
- Tiering Writer 同 split 内处理，locallyDeletedRowIds 包含 106 → PendingDeletes 清理 ✓
- 数据正确

情况 b：下一轮 tiering 只包含 +U(offset=106)，-U(offset=110) 在更后面
- Position report 包含 RowId=106
- §7.3 检查 PendingDeletes：RowId=106 存在 → 标记 LakeDv(file_Y, posN)，不写入 RowPosIndex
- **但 RowId=106 对应的行此时是存活的！-U(offset=110) 还没到达！**
- **这会导致 Iceberg 中一条存活的行被错误地标记为删除 → 数据丢失！**

**这是一个严重的正确性 bug。**

**根因**：PendingDeletes 的写入条件应该是"该行正在被 tiering 且已被删除"，但实际上只检查了"RowPosIndex 查不到"。对于从未 tiering 过的行，RowPosIndex 也查不到，导致误判。

**修复方案**：

需要精确区分"正在被 tiering 但 position report 未到"和"从未被 tiering 过"。

方案 1：使用 lastTieredOffset 作为判断边界

```
§6.2 中 RowPosIndex 查不到时：
  if oldRowId >= lastTieredOffset AND oldRowId <= currentTieringSplitEndOffset:
      → 写入 PendingDeletes（该行在当前 tiering split 范围内）
  else:
      → 跳过（该行要么从未 tiering 过，要么已经在更早的 snapshot 中被处理）
```

这里 `lastTieredOffset` 是上一轮 tiering 完成的 offset，`currentTieringSplitEndOffset` 是当前正在进行的 tiering split 的结束 offset。

只有 oldRowId 落在 `[lastTieredOffset, currentTieringSplitEndOffset]` 范围内时，才说明该行正在被 tiering。

但还有一个边界情况：如果没有正在进行的 tiering（pendingTieringStartOffset = -1），那么所有查不到的 oldRowId 都应该跳过。

修正后的判断：

```
§6.2 中 RowPosIndex 查不到时：
  if pendingTieringStartOffset != -1 
     AND oldRowId >= pendingTieringStartOffset 
     AND oldRowId <= pendingTieringSplitEndOffset:
      → 写入 PendingDeletes
  else:
      → 跳过（该行不在当前 tiering 范围内）
```

需要同时记录 `pendingTieringSplitEndOffset`（即 tiering split 的 latest_offset）。

**验证修复后的分析 5**：

- 处理 -U(oldRowId=80)：pendingTieringStartOffset=50, pendingTieringSplitEndOffset=100, 80 在 [50,100] 内 → PendingDeletes ✓
- 处理 -U(oldRowId=106)：106 > pendingTieringSplitEndOffset=100 → 跳过 ✓（106 不在当前 split 范围内，还在 WAL 中）

---

## 分析 6：DELETE 在 tiering 完成后、下一轮 tiering 开始前到达

时序：
- Tiering split [50,100] 完成，position report 已处理，pendingTieringStartOffset 已清除
- +I(key1, v1, offset=80) 已 tiering 到 file_X:pos3，RowPosIndex 有 80 → {file_X, pos3}
- DELETE(key1, offset=105, oldRowId=80) 到达

处理：RowPosIndex 查到 80 → {file_X, pos3} → 更新 LakeDv → 删除 RowPosIndex 中 80

结论：**正确**。这是正常路径。

---

## 分析 7：Compaction 与 DELETE 的交互

时序：
- RowPosIndex: 80 → {file_A, pos3}
- Compaction 将 file_A 重写为 file_B，RowId=80 在 file_B:pos1
- Compaction commit，上报新 position
- DELETE(key1, offset=105, oldRowId=80) 到达

§12.1 中 compaction commit 后：
- TabletServer 更新 RowPosIndex：80 → {file_B, pos1}（覆盖旧值）
- 从 LakeDv 删除 file_A 的条目

DELETE 到达时：
- RowPosIndex 查到 80 → {file_B, pos1} → 更新 LakeDv(file_B, pos1) ✓

结论：**正确**。

但如果 DELETE 在 compaction commit 之前到达呢？

时序变体：
- RowPosIndex: 80 → {file_A, pos3}
- Compaction 正在进行
- DELETE(key1, offset=105, oldRowId=80) 到达
- RowPosIndex 查到 80 → {file_A, pos3} → 更新 LakeDv(file_A, pos3)，删除 RowPosIndex 中 80
- Compaction commit，上报新 position：RowId=80 在 file_B:pos1

§7.3 position report 处理（compaction 上报）：
- 检查 PendingDeletes：RowId=80 不在（因为 DELETE 时 RowPosIndex 查到了，没写 PendingDeletes）
- 写入 RowPosIndex：80 → {file_B, pos1}

**问题：RowId=80 已经被 DELETE 了，但 compaction 的 position report 又把它写回了 RowPosIndex！**

这和之前的 tiering position report 竞态是同一类问题，但发生在 compaction 场景。

**这是否会导致数据错误？**

分析后续流程：
- LakeDv 中有 file_A:pos3 的删除标记
- RowPosIndex 中有 80 → {file_B, pos1}（残留）
- 下一轮 tiering split 的 LakeDv 快照包含 file_A:pos3
- Tiering Writer 物化 Puffin DV：file_A:pos3 被标记删除
- 但 file_B:pos1 没有被标记删除！

§8.2 处理新 snapshot 时：
- file_B 是 compaction 产生的新文件
- 遍历 file_B 的每个 RowId，反查 RowPosIndex
- RowId=80 → 查到了（残留条目）→ 认为存活 → 不标记 LakeDv

**最终：file_B:pos1 没有 DV 覆盖，已删除的行重新暴露。数据错误！**

### 问题 B：Compaction position report 与 DELETE 的竞态

**问题描述**：与 tiering position report 的竞态完全同构。DELETE 先到达并从 RowPosIndex 删除了条目，但 compaction 的 position report 随后盲目写入了新的 FilePos，导致 RowPosIndex 残留。

**修复方案**：

Compaction 的 position report 处理也需要检查 PendingDeletes，与 §7.3 的逻辑一致。

但这里有一个额外的复杂性：compaction 不像 tiering 有明确的 offset 范围。Compaction 重写的是已有的 Iceberg 文件，涉及的 RowId 可能跨越多个历史 tiering split。

PendingDeletes 的写入条件需要扩展：

```
§6.2 中 RowPosIndex 查不到时：
  if (pendingTieringStartOffset != -1 
      AND oldRowId >= pendingTieringStartOffset 
      AND oldRowId <= pendingTieringSplitEndOffset):
      → 写入 PendingDeletes（tiering 场景）
  else if pendingCompaction == true:
      → 写入 PendingDeletes（compaction 场景）
  else:
      → 跳过
```

但这个条件仍然不够精确。更好的方案是：

**统一方案：DELETE 处理时，如果 RowPosIndex 查不到，始终检查该 RowId 是否"应该在 RowPosIndex 中"。**

判断依据：如果 oldRowId < lastTieredOffset（即该行对应的 +I/+U 已经被 tiering 过），那么该行应该在 RowPosIndex 中。查不到说明要么 position report 还没到，要么 compaction 正在进行。此时写入 PendingDeletes。

如果 oldRowId >= lastTieredOffset 且在当前 tiering split 范围内，同样写入 PendingDeletes。

如果 oldRowId > 所有已知的 tiering/compaction 范围，说明该行确实还在 WAL 中，跳过。

简化后的判断：

```
§6.2 中 RowPosIndex 查不到时：
  if oldRowId < lastCommittedTieredOffset:
      → 该行应该已经在 Iceberg 中，但 RowPosIndex 查不到
      → 可能是 compaction 正在进行，position report 还没到
      → 写入 PendingDeletes
  else if pendingTieringStartOffset != -1 
          AND oldRowId >= pendingTieringStartOffset 
          AND oldRowId <= pendingTieringSplitEndOffset:
      → 该行正在被 tiering，position report 还没到
      → 写入 PendingDeletes
  else:
      → 该行还在 WAL 中，从未被 tiering 过
      → 跳过
```

这里 `lastCommittedTieredOffset` 是最近一次 tiering commit 成功的 offset。如果 oldRowId < lastCommittedTieredOffset，说明该行的 +I/+U 已经被 tiering 过，理应在 RowPosIndex 中。

**但还有一个问题**：如果该行已经被之前的 DELETE 从 RowPosIndex 中删除了（正常路径），此时又收到一条对同一个 oldRowId 的删除（不可能，因为 KV State 中 key 已经不存在了，不会再生成 -D）。所以这种情况不会发生。

但 compaction 场景下，RowPosIndex 中的条目可能因为 compaction 正在进行而暂时不存在（旧文件的条目已清理，新文件的条目还没写入）。

等等，§12.1 的流程是：compaction commit 后才上报 position。在 commit 之前，RowPosIndex 中仍然保留旧文件的 FilePos。所以 DELETE 到达时应该能查到旧的 FilePos。

让我重新分析 compaction 的时序：

1. RowPosIndex: 80 → {file_A, pos3}
2. Compaction 开始（异步，不影响 RowPosIndex）
3. DELETE(oldRowId=80) 到达 → RowPosIndex 查到 {file_A, pos3} → LakeDv(file_A, pos3) → 删除 RowPosIndex 中 80
4. Compaction commit → 上报 position：80 → {file_B, pos1}
5. §7.3 处理：PendingDeletes 中无 80 → 写入 RowPosIndex：80 → {file_B, pos1} ← 残留

问题确认：步骤 3 中 DELETE 确实能查到旧的 FilePos（compaction 还没 commit），所以不会写入 PendingDeletes。但步骤 5 中 compaction 的 position report 又写回了。

**修复方案（针对 compaction）**：

compaction 的 position report 处理也需要检查 PendingDeletes。但问题是步骤 3 中没有写入 PendingDeletes（因为 RowPosIndex 查到了）。

所以 PendingDeletes 方案无法覆盖这个场景。需要另一种机制。

**方案：compaction position report 处理时，检查 RowId 是否仍在 RowPosIndex 中指向旧文件。**

不对，RowPosIndex 中 RowId=80 已经被删除了（步骤 3）。

**方案：compaction position report 处理时，检查 RowId 是否仍在 RowPosIndex 中（无论指向哪个文件）。如果不在，说明该行已被删除，不写入新的 FilePos，而是标记 LakeDv。**

具体流程：

```
Compaction position report 处理：
对每个 (RowId, new_file, new_row_position)：
  1. 检查 RowPosIndex 中是否存在 RowId：
     - 存在（指向旧文件）：用新 FilePos 覆盖 → 正常更新
     - 不存在：该行已被删除 → 标记 LakeDv(new_file, new_row_position)，不写入 RowPosIndex
  2. 检查 PendingDeletes：
     - 存在：该行已被删除 → 标记 LakeDv，不写入 RowPosIndex，从 PendingDeletes 移除
     - 不存在：继续步骤 1 的结果
```

这样，步骤 5 中处理 RowId=80 时：
- RowPosIndex 中不存在 80（步骤 3 已删除）→ 标记 LakeDv(file_B, pos1) ✓
- 不写入 RowPosIndex ✓

**但这引入了一个新问题**：compaction 重写文件时，旧文件的 RowPosIndex 条目什么时候清理？

§12.1 说"TabletServer 更新 RowPosIndex（用新 FilePos 覆盖旧 FilePos）"。如果 compaction position report 处理时发现 RowId 不在 RowPosIndex 中，就不写入。那旧文件的条目已经在步骤 3 被 DELETE 清理了，新文件的条目也不写入。这是正确的。

但如果 compaction 涉及的某些行没有被删除呢？

正常情况：RowPosIndex 中有 RowId=81 → {file_A, pos4}，compaction 后 81 → {file_B, pos2}。
- Compaction position report：RowPosIndex 中存在 81 → 用 {file_B, pos2} 覆盖 ✓

结论：**compaction position report 处理时，需要先检查 RowPosIndex 中是否存在该 RowId。不存在则标记 LakeDv 而非写入 RowPosIndex。**

---

## 分析 8：§8.2 处理新 snapshot 与 DELETE 的竞态

§8.2 Step 1 中，处理 newFiles 时需要获取 LakeDv 写锁。文档已分析了与 changelog 同步的竞态，并通过加锁解决。

但让我验证一个更细的场景：

时序：
1. §8.2 获取 LakeDv 写锁
2. 遍历 file_B 的 RowId=80，从 RowPosIndex 查到 → 认为存活，写入新 FilePos
3. 释放 LakeDv 写锁
4. DELETE(oldRowId=80) 到达，§6.2 获取 LakeDv 写锁
5. RowPosIndex 查到 80 → {file_B, new_pos} → 更新 LakeDv(file_B, new_pos)
6. 删除 RowPosIndex 中 80

这个顺序是正确的：步骤 2 写入了新 FilePos，步骤 5 用新 FilePos 更新了 LakeDv。

反过来：
1. DELETE(oldRowId=80) 到达，§6.2 获取 LakeDv 写锁
2. RowPosIndex 查到 80 → {file_A, old_pos} → 更新 LakeDv(file_A, old_pos)
3. 删除 RowPosIndex 中 80
4. 释放 LakeDv 写锁
5. §8.2 获取 LakeDv 写锁
6. 遍历 file_B 的 RowId=80，从 RowPosIndex 查不到 → 标记 LakeDv(file_B, new_pos)
7. 释放 LakeDv 写锁

步骤 2 标记了 file_A:old_pos（旧文件的位置），步骤 6 标记了 file_B:new_pos（新文件的位置）。两个都标记了，是正确的。

但 file_A 是旧文件（在 oldFiles 中），§8.2 Step 3 会清理 file_A 的 LakeDv 条目。这也是正确的，因为 file_A 已经不在新 snapshot 中了。

结论：**正确**。LakeDv 写锁保证了两种执行顺序都正确。

---

## 分析 9：Union Read 一致性快照

§10 中 union read 在读锁保护下获取 LakeDv、LogDv 和 logEndOffset 的一致性快照。

场景：union read 获取快照时，一条 -U 正在被处理。

时序 A（-U 先完成）：
1. §6.2 处理 -U(oldRowId=X)：更新 LakeDv、LogDv
2. 更新 log_hw = N
3. Union read 获取读锁：logEndOffset=N，LakeDv 和 LogDv 都包含了 -U 的效果
4. Client 读 changelog [start, N]：+U 在范围内，-U 对应的旧行在 LakeDv/LogDv 中被屏蔽 ✓

时序 B（union read 先获取快照）：
1. Union read 获取读锁：logEndOffset=M（-U 还没处理完）
2. LakeDv 和 LogDv 不包含 -U 的效果
3. Client 读 changelog [start, M]：-U 和 +U 都不在范围内（offset > M）
4. Iceberg 中旧行没有被 LakeDv 屏蔽，但 +U 也没被读出 → 读到旧版本

这是否正确？取决于语义：union read 读到的是 logEndOffset=M 时刻的一致性快照。此时 -U 还没处理完，旧版本确实是当时的最新版本。

结论：**正确**。读锁保证了快照一致性。

但有一个微妙的问题：§6.2 中先更新 DV 再更新 log_hw。如果 union read 在"DV 已更新但 log_hw 还没更新"的窗口获取快照：

1. §6.2 更新 LakeDv：标记 file_A:pos0 删除
2. §6.2 更新 LogDv：标记 offset=0 删除
3. （此时 log_hw 还没更新）
4. Union read 获取读锁：logEndOffset = 旧的 log_hw = M
5. LakeDv 已包含新的删除标记，LogDv 也已包含

Client 处理：
- Iceberg 中 file_A:pos0 被 LakeDv 屏蔽（旧行被删除）
- Changelog [start, M] 中 offset=0 被 LogDv 屏蔽
- 但 +U 的 offset > M，不在 changelog 范围内 → 新值没被读出
- **结果：旧行被删除了，但新行还没出现 → 数据丢失！**

等等，这不对。让我重新看 §6.2 的流程：

§6.2 的步骤是：
1. 获取 KvTablet 写锁
2. Flush PrewriteBuffer
3. 获取 LakeDv 写锁
4. 更新 DV（LakeDv、LogDv、RowPosIndex）
5. 释放 LakeDv 写锁
6. 更新 log_hw
7. 释放 KvTablet 写锁

Union read 需要获取 KvTablet 读锁（§10 步骤 3）。由于 §6.2 持有写锁，union read 会被阻塞，直到 §6.2 释放写锁（步骤 7）。此时 log_hw 已经更新。

所以上述窗口不存在——KvTablet 写锁保证了 DV 更新和 log_hw 更新的原子性。

结论：**正确**。KvTablet 读写锁保证了 union read 不会看到"DV 已更新但 log_hw 未更新"的中间状态。

---

## 分析 10：LogDv 与 changelog 的对齐

LogDv 标记的是 WAL 中已被删除的记录。Client 读 changelog 时 apply LogDv 跳过这些记录。

场景：+I(key1, v1, offset=0) → +I(key2, v2, offset=1) → PUT(key1, v3) → -U(offset=2, oldRowId=0) + +U(offset=3, key1, v3)

LogDv 标记 offset=0 已删除。

Client 读 changelog [0, 3]：
- offset=0：+I(key1, v1) → LogDv 标记删除 → 跳过 ✓
- offset=1：+I(key2, v2) → 不在 LogDv 中 → 输出 ✓
- offset=2：-U(key1, v1) → retract 类型 → 不输出（client 只输出 +I/+U）
- offset=3：+U(key1, v3) → 不在 LogDv 中 → 输出 ✓

最终：(key1, v3), (key2, v2) ✓

但如果 client 只读 changelog 中的 +I/+U 记录（跳过 -U/-D），那 LogDv 只需要标记 +I/+U 记录的删除。-U/-D 记录本身就会被 client 跳过。

文档中 LogDv 标记的是 `offset = oldRowId` 对应的 changelog。oldRowId 就是 +I/+U 的 offset，所以 LogDv 确实只标记 +I/+U 记录。

结论：**正确**。

但有一个边界情况：如果 +I 和 -U 的 offset 相邻（比如 +I 在 offset=0，-U 在 offset=1），LogDv 标记 offset=0。Client 读到 offset=0 时跳过，读到 offset=1 时是 -U 类型也跳过。没问题。

---

## 分析 11：LakeDv bitmap 差集清理的正确性

§13.3 中，LakeDv 清理使用 bitmap 差集：`当前 bitmap AND NOT 快照时的 bitmap`。

场景：
- 快照时 LakeDv: file_A → {0, 2}
- 快照后新到达 -D(oldRowId=X) → LakeDv: file_A → {0, 2, 5}
- DV-readable 通知到达，执行清理：{0, 2, 5} AND NOT {0, 2} = {5}
- 清理后 LakeDv: file_A → {5}

pos5 的删除是快照后新增的，保留了。pos0 和 pos2 已物化到 Puffin DV，清理了。

结论：**正确**。

但如果在清理执行的同时，又有新的 -D 到达呢？

时序：
1. 清理线程读取当前 bitmap = {0, 2, 5}
2. 新的 -D 到达，LakeDv: file_A → {0, 2, 5, 7}
3. 清理线程计算差集：{0, 2, 5} AND NOT {0, 2} = {5}
4. 清理线程写入 LakeDv: file_A → {5}
5. **pos7 的删除丢失了！**

这需要在清理时持有 LakeDv 写锁。§13.3 没有明确说明清理时的加锁策略。

如果清理时持有 LakeDv 写锁：
1. 获取 LakeDv 写锁
2. 读取当前 bitmap = {0, 2, 5, 7}（步骤 2 的 -D 已经写入）
3. 计算差集：{0, 2, 5, 7} AND NOT {0, 2} = {5, 7}
4. 写入 LakeDv: file_A → {5, 7}
5. 释放 LakeDv 写锁

结论：**需要在 LakeDv 清理时持有 LakeDv 写锁**，否则可能丢失并发写入的删除标记。文档应明确这一点。

### 问题 C：LakeDv 清理需要持有写锁

**问题描述**：§13.3 的 bitmap 差集清理如果不在 LakeDv 写锁保护下执行，可能丢失并发写入的删除标记。

**修复**：清理流程应在 LakeDv 写锁保护下执行读取-计算-写入的完整操作。

---

## 分析 12：恢复流程的正确性

§11.2 恢复步骤从 `checkpointLogHw + 1` 开始重放 changelog。

场景：checkpoint 时 log_hw=100，restoreSnapshot=S2（tiered offset=80）。

恢复时重放 changelog [101, ...]。

对于 -D(offset=105, oldRowId=50)：
- oldRowId=50 < tiered offset=80 → 该行应该在 Iceberg 中
- RowPosIndex 查找 50：
  - 如果 checkpoint 时 RowPosIndex 中有 50 的条目（DELETE 在 checkpoint 之后到达）→ 找到 → 更新 LakeDv ✓
  - 如果 checkpoint 时 RowPosIndex 中没有 50 的条目（可能之前已被删除）→ 找不到 → 写入 PendingDeletes

PendingDeletes 在恢复后如何消费？需要等 position report 到达。但恢复后可能没有正在进行的 tiering。

如果 RowId=50 对应的行已经在之前的 DELETE 中被处理过（RowPosIndex 已删除，LakeDv 已标记），那么 checkpoint 中 RowPosIndex 确实没有 50 的条目，LakeDv 中已有标记。重放时再次写入 PendingDeletes 是多余的，但不会导致错误——PendingDeletes 中的残留条目不会影响数据正确性，只是占用空间。

但如果 RowId=50 对应的行正在被 compaction（checkpoint 时旧文件的条目已清理，新文件的条目还没写入），那么恢复后 RowPosIndex 中确实没有 50 的条目。此时写入 PendingDeletes 是正确的——后续 compaction 的 position report 到达时会消费。

**但恢复后，之前正在进行的 compaction 可能已经失败了。** 如果 compaction 不会重试，PendingDeletes 中的条目永远不会被消费。

这是否会导致数据错误？PendingDeletes 中的残留条目本身不会导致错误——它只是一个"待处理"标记。如果后续有新的 tiering 或 compaction 涉及该 RowId，position report 到达时会消费。如果没有，该条目只是占用空间。

但如果 §8.2 处理新 snapshot 时检查 PendingDeletes（如文档所述），那么 PendingDeletes 中的残留条目会导致 §8.2 将该行标记为已删除。如果该行实际上是存活的（比如 checkpoint 时 RowPosIndex 中没有条目是因为 compaction 正在进行，而不是因为被删除），那么 §8.2 会错误地标记 LakeDv。

**但等等**：如果 RowPosIndex 中没有 RowId=50 的条目，且 PendingDeletes 中有 RowId=50，那么 §8.2 处理 newFile 时：
- 先检查 PendingDeletes：RowId=50 存在 → 标记 LakeDv
- 但该行可能是存活的（compaction 场景）

**这是一个潜在的正确性问题。**

不过，让我重新思考：恢复时重放的是 changelog [101, ...]。如果 -D(offset=105, oldRowId=50) 在重放范围内，说明 key 确实被删除了。所以 RowId=50 对应的行确实应该被标记为已删除。PendingDeletes 中的条目是正确的。

问题在于：恢复时重放 -D(oldRowId=50)，RowPosIndex 查不到 50。这可能是因为：
1. checkpoint 之前已经处理过这条 -D（但 checkpointLogHw=100，-D 在 offset=105，不可能在 checkpoint 之前处理过）
2. Compaction 正在进行，旧条目已清理

情况 2 不可能：如果 compaction 在 checkpoint 之前完成，新条目已写入 RowPosIndex，checkpoint 中会包含。如果 compaction 在 checkpoint 之后才完成，那么 checkpoint 中 RowPosIndex 仍有旧条目。

所以恢复时 RowPosIndex 查不到 50 的唯一原因是：该行从未被 tiering 过（不可能，因为 oldRowId=50 < tiered offset=80），或者之前已被删除（不可能，因为 -D 在 checkpointLogHw 之后）。

等等，还有一种可能：checkpoint 时 RowPosIndex 中有 50 的条目，但恢复时加载的 checkpoint 数据中确实有这个条目。那么重放 -D(oldRowId=50) 时应该能查到。

结论：恢复流程在正常情况下是正确的。PendingDeletes 在恢复场景中的行为与正常运行时一致。

---

## 分析 13：Leader 切换（Failover）场景

TabletServer 发生 failover，新 leader 从 checkpoint 恢复。

关键问题：正在进行的 tiering split 会怎样？

1. 旧 leader 生成了 tiering split [50, 100]，Tiering Writer 正在处理
2. 旧 leader crash
3. 新 leader 从 checkpoint 恢复（checkpointLogHw=80）
4. 新 leader 重放 changelog [81, ...]
5. Tiering Writer 可能 commit 成功或失败

情况 A：Tiering Writer commit 失败
- Position report 不会到达
- 新 leader 重新生成 tiering split
- PendingDeletes 中可能有旧 leader 写入的条目，但新的 tiering 会覆盖

情况 B：Tiering Writer commit 成功，position report 到达新 leader
- 新 leader 的 RowPosIndex 可能与旧 leader 不同（因为从 checkpoint 恢复，可能丢失了 checkpointLogHw 到 crash 之间的更新）
- 但新 leader 会重放 changelog [81, ...]，重建这些更新
- Position report 到达时，§7.3 的处理逻辑与正常运行时一致

**但有一个问题**：新 leader 重放 changelog 时，pendingTieringStartOffset 是什么？

恢复后 pendingTieringStartOffset 需要从 checkpoint 中恢复。如果 checkpoint 中没有保存这个值，恢复后默认为 -1。

如果 pendingTieringStartOffset = -1，且重放 changelog 中有 -D(oldRowId=80)，RowPosIndex 查不到 80（因为 position report 还没到）：
- 按照分析 5 的修复方案，pendingTieringStartOffset=-1 且 oldRowId=80 不满足任何条件 → 跳过 PendingDeletes
- 后续 position report 到达时，盲目写入 RowPosIndex → 残留

**这是一个恢复场景下的正确性问题。**

### 问题 D：Failover 后 pendingTieringStartOffset 丢失

**问题描述**：如果 checkpoint 中没有保存 pendingTieringStartOffset 和 pendingTieringSplitEndOffset，恢复后这两个值为默认值（-1），导致 PendingDeletes 的写入条件失效。

**修复方案**：
1. 将 pendingTieringStartOffset 和 pendingTieringSplitEndOffset 保存在 DvRocksDB checkpoint 的元数据中
2. 或者，恢复后如果检测到有正在进行的 tiering（通过查询 Tiering Service 的状态），重新设置这两个值

---

## 分析 14：多轮 tiering 快速连续执行

场景：第一轮 tiering split [50,100] 的 position report 还没回来，第二轮 tiering split [101,150] 就要生成了。

文档假设 tiering 是串行的（下一轮需要知道 last_tiered_offset）。但如果 position report 是异步的，可能出现：

1. 第一轮 tiering commit 成功（last_tiered_offset 更新为 100）
2. Position report 还在传输中
3. 第二轮 tiering split [101, 150] 生成
4. 第一轮 position report 到达

这个时序下，pendingTieringStartOffset 应该是什么？

- 第一轮生成 split 时设置 pendingTieringStartOffset=50
- 第二轮生成 split 时，第一轮的 position report 还没到，pendingTieringStartOffset 应该保持 50 还是更新为 101？

如果更新为 101，那么 [50,100] 范围内的 RowId 就不在 pending 范围内了，但 position report 还没到。

**修复方案**：pendingTieringStartOffset 应该在 position report 处理完成后才清除/更新，而不是在新 split 生成时覆盖。或者维护一个 pending ranges 列表而非单个值。

但这增加了复杂性。更简单的方案是：**不允许在 position report 未处理完时生成新的 tiering split**。即 tiering split 的生成需要等待上一轮的 position report 处理完成。

---

## 分析 15：RowPosIndex 只存最新快照的 FilePos — Compaction 场景

§3.3 说 RowPosIndex 只存最新快照的 FilePos。但 compaction 可能产生新的 snapshot，其中文件发生了变化。

场景：
- Snapshot S2：file_A（包含 RowId=80 在 pos3）
- Compaction 将 file_A 重写为 file_B，commit snapshot S3
- RowPosIndex 更新：80 → {file_B, pos1}
- LakeDv 中 file_A 的条目被清理

但如果 union read 客户端还在读 S2（S3 还没成为 DV-readable）：
- Client 请求 union read（snapshot S2）
- TabletServer 返回 LakeDv（针对 S2 的文件）
- 但 LakeDv 中 file_A 的条目已被清理（§12.1 步骤 4）

**问题：如果 S3 还没成为 DV-readable，union read 仍在读 S2，但 LakeDv 中 file_A 的条目已被清理。**

等等，§12.1 说"从 LakeDv 中删除旧文件的条目"。但这应该在 S3 成为 DV-readable 之后才执行，与 §13.3 的清理时机一致。

文档 §12.1 没有明确说明清理时机。如果 compaction commit 后立即清理 LakeDv 中旧文件的条目，而 S3 还没成为 DV-readable，union read 仍在读 S2，那么 S2 中 file_A 的已删除行会重新暴露。

### 问题 E：Compaction 后 LakeDv 清理时机

**问题描述**：§12.1 中"从 LakeDv 中删除旧文件的条目"的时机不明确。如果在 compaction commit 后立即清理，而新 snapshot 还没成为 DV-readable，union read 仍在读旧 snapshot，会导致旧文件中已删除的行重新暴露。

**修复方案**：Compaction 产生的新 snapshot 也需要经过 DV-readable 流程。LakeDv 中旧文件条目的清理应在新 snapshot 成为 DV-readable 之后执行，与 §13.3 的清理时机保持一致。

具体来说：compaction commit 后，TabletServer 更新 RowPosIndex（用新 FilePos 覆盖旧 FilePos），但不立即清理 LakeDv 中旧文件的条目。等新 snapshot 成为 DV-readable 后，再清理旧文件的 LakeDv 条目。

在此期间，LakeDv 中同时存在旧文件和新文件的条目。Union read 读旧 snapshot 时使用旧文件的 LakeDv 条目，读新 snapshot 时使用新文件的 LakeDv 条目（如果有的话）。

---

## 分析 16：外部 Compaction 与 RowPosIndex 的一致性

§12.2 中，外部 compaction 产生的新文件通过扫描 `__offset` 列重建 position 映射。

场景：
- RowPosIndex: 80 → {file_A, pos3}
- 外部 Spark compaction 将 file_A 重写为 file_C
- 新 snapshot S3 到达，file_C 不在 knownFiles 中
- §8.2 扫描 file_C，读取 __offset 列，发现 RowId=80 在 pos1

§8.2 Step 1 处理 file_C：
- RowId=80，从 RowPosIndex 查到 80 → {file_A, pos3}（旧的 FilePos）
- 该行存活，用新的 {file_C, pos1} 覆盖 RowPosIndex

§8.2 Step 3 处理 oldFile file_A：
- 从 RowPosIndex 清理指向 file_A 的 FilePos → 但 RowId=80 已经指向 file_C 了，不需要清理

结论：**正确**。

但如果在扫描 file_C 的过程中，DELETE(oldRowId=80) 到达呢？

时序：
1. §8.2 开始处理 file_C（获取 LakeDv 写锁）
2. 扫描 file_C，发现 RowId=80
3. 从 RowPosIndex 查到 80 → {file_A, pos3} → 存活，覆盖为 {file_C, pos1}
4. 释放 LakeDv 写锁
5. DELETE(oldRowId=80) 到达，§6.2 获取 LakeDv 写锁
6. RowPosIndex 查到 80 → {file_C, pos1} → 更新 LakeDv(file_C, pos1) ✓

结论：**正确**。LakeDv 写锁保证了顺序。

反过来：
1. DELETE(oldRowId=80) 到达，§6.2 获取 LakeDv 写锁
2. RowPosIndex 查到 80 → {file_A, pos3} → 更新 LakeDv(file_A, pos3)
3. 删除 RowPosIndex 中 80
4. 释放 LakeDv 写锁
5. §8.2 获取 LakeDv 写锁
6. 扫描 file_C，发现 RowId=80
7. 先检查 PendingDeletes：不在（因为步骤 2 查到了 RowPosIndex）
8. 从 RowPosIndex 查不到 80 → 标记 LakeDv(file_C, pos1) ✓

结论：**正确**。file_A:pos3 和 file_C:pos1 都被标记了。file_A 的条目后续会被清理。

---

## 分析 17：LogDv 生命周期管理

§3.4 说"当数据湖 snapshot advance 后，所有小于数据湖最新 snapshot 对应的 start_logOffset 的 offset_range 条目都可以清理掉"。

场景：
- Snapshot S2 的 start_logOffset = 50
- LogDv 中有 offset_range [0,9], [10,19], ..., [40,49], [50,59], ...
- S2 成为 DV-readable 后，清理 [0,9] 到 [40,49]

但 union read 读的是 changelog [S2_start_offset, logEndOffset] = [50, logEndOffset]。LogDv 中 [50,59] 及之后的条目仍然需要。

如果在 S2 成为 DV-readable 之前就清理了 [0,49] 的 LogDv 条目，而 union read 仍在读 S1（start_offset=0），那么 [0,49] 范围内的已删除记录会被重新读出。

### 问题 F：LogDv 清理时机需要与 DV-readable 对齐

**问题描述**：LogDv 的清理应该在新 snapshot 成为 DV-readable 之后执行，而不是 snapshot advance 时立即执行。否则 union read 仍在读旧 snapshot 时，旧 snapshot 对应的 LogDv 条目已被清理。

**修复方案**：LogDv 清理时机与 LakeDv 清理时机一致——在新 snapshot 成为 DV-readable 后，清理小于新 snapshot 的 start_logOffset 的 LogDv 条目。

---

## 分析 18：Union Read Client 处理 -U/-D 记录

§10 Client 侧处理步骤 4："Fetch [snapshot_start_offset, logEndOffset] 这段 changelog，apply logDv，跳过已删除的记录"。

Client 读 changelog 时，会遇到 +I、+U、-U、-D 四种记录类型。

对于 union read（读最新快照），client 应该如何处理这些记录？

- +I/+U：输出（这是新数据或更新后的数据）
- -U/-D：不输出（这是 retract 记录，对应的旧行已在 Iceberg 中被 LakeDv 屏蔽或在 LogDv 中被标记）

但 LogDv 标记的是被删除的 +I/+U 记录（通过 oldRowId = +I/+U 的 offset）。-U/-D 记录本身不在 LogDv 中。

Client 的处理逻辑应该是：
1. 读 changelog [start, end]
2. 对每条记录：
   - 如果是 -U/-D：跳过（retract 记录不输出）
   - 如果是 +I/+U：检查 LogDv，如果被标记则跳过，否则输出

这个逻辑是正确的。但文档没有明确说明 client 如何区分记录类型。如果 client 不区分类型，直接 apply LogDv，那么 -U/-D 记录不在 LogDv 中，会被"输出"。

**建议**：文档应明确 client 的处理逻辑——先按记录类型过滤（只保留 +I/+U），再 apply LogDv。

---

## 分析 19：RowId 唯一性

§3.1 定义 RowId = +I/+U 的 log offset。Log offset 在单个 partition 内是唯一递增的。

但如果 TabletServer 发生 failover，新 leader 的 log offset 是否会与旧 leader 的 offset 冲突？

在 Fluss 的设计中，changelog 是持久化的（WAL），log offset 是全局唯一的（在单个 bucket 内）。Failover 后新 leader 从 WAL 的最新 offset 继续分配。所以 RowId 的唯一性是有保证的。

结论：**正确**。

---

## 分析 20：FilePos 的 row_position 用 4 bytes 的限制

§3.2 中 row_position 用 4 bytes（int），最大约 21 亿行。单个 Iceberg data file 通常不会超过这个限制（Parquet 文件通常在几百 MB 到几 GB，行数远小于 21 亿）。

结论：**不是问题**。

---

## 分析 21：PendingDeletes 与 §8.2 的交互

§8.2 Step 1 处理 newFiles 时，先检查 PendingDeletes，再检查 RowPosIndex。

场景：
- PendingDeletes 中有 RowId=80
- §8.2 处理 newFile file_X，发现 RowId=80 在 pos3
- PendingDeletes 中有 80 → 标记 LakeDv(file_X, pos3)
- 文档说"不从 PendingDeletes 中移除"

后续 position report 到达时：
- §7.3 检查 PendingDeletes：RowId=80 存在 → 标记 LakeDv(file_X, pos3)（幂等），从 PendingDeletes 移除

这是正确的。但如果 position report 永远不到达（Tiering Writer crash）呢？

PendingDeletes 中 RowId=80 会残留。但 §8.2 已经标记了 LakeDv(file_X, pos3)，所以数据正确性不受影响。残留的 PendingDeletes 条目只是占用空间。

后续如果有新的 tiering 重试，新的 position report 到达时会清理。

结论：**正确**。

---

## 分析 22：同一个 key 的快速连续操作

场景：在一个 batch 中，同一个 key 被多次操作：

```
PUT(key1, v1)  → +I(offset=0)
PUT(key1, v2)  → -U(offset=1, oldRowId=0) + +U(offset=2)
PUT(key1, v3)  → -U(offset=3, oldRowId=2) + +U(offset=4)
DELETE(key1)   → -D(offset=5, oldRowId=4)
```

如果这些操作在同一个 PrewriteBuffer batch 中：

§6.2 处理时：
1. -U(oldRowId=0)：RowPosIndex 查不到（还没 tiering）→ 根据条件判断是否写入 PendingDeletes
2. -U(oldRowId=2)：RowPosIndex 查不到 → 同上
3. -D(oldRowId=4)：RowPosIndex 查不到 → 同上

LogDv 标记 offset=0, 2, 4 已删除。

如果这些都在 tiering 之前发生（pendingTieringStartOffset=-1），则都跳过 PendingDeletes。

后续 tiering 时，Tiering Writer 处理：
- +I(offset=0)：写入 file，记录 position
- -U(offset=1, oldRowId=0)：oldRowId=0 >= last_tiered → 同 split 内 → localDv
- +U(offset=2)：写入 file，记录 position
- -U(offset=3, oldRowId=2)：oldRowId=2 >= last_tiered → 同 split 内 → localDv
- +U(offset=4)：写入 file，记录 position
- -D(offset=5, oldRowId=4)：oldRowId=4 >= last_tiered → 同 split 内 → localDv

所有中间版本都被 localDv 处理，只有最终状态（key1 被删除）反映在 Iceberg 中。

结论：**正确**。

---

## 分析 23：DvRocksDB checkpoint 与 KvTablet checkpoint 的一致性

DvRocksDB 和 KvTablet RocksDB 是独立的。它们的 checkpoint 时机可能不同。

场景：
- KvTablet checkpoint 在 log_hw=100 时完成
- DvRocksDB checkpoint 在 log_hw=95 时完成（稍早）
- Failover 后恢复

KvTablet 从 log_hw=100 恢复，DvRocksDB 从 log_hw=95 恢复。

DvRocksDB 需要重放 changelog [96, ...]，但 KvTablet 已经包含了 [96, 100] 的 KV 数据。

这会导致不一致吗？

DvRocksDB 重放 [96, ...] 时，处理 -U/-D 记录，更新 RowPosIndex、LakeDv、LogDv。这些操作是幂等的（bitmap set 是幂等的，RowPosIndex 删除也是幂等的）。

但 KvTablet 已经处理了 [96, 100] 的写入，KV State 已经是 log_hw=100 的状态。如果 DvRocksDB 重放 [96, 100] 时需要从 KV State 读取旧 value 来提取 oldRowId，但 KV State 已经是更新后的状态，可能读到错误的 oldRowId。

等等，DvRocksDB 重放时不需要从 KV State 读取。重放的是 changelog 记录，-U/-D 记录的 value 中已经携带了 oldRowId（§4.2）。所以 DvRocksDB 重放只需要读 changelog，不需要访问 KV State。

结论：**正确**。DvRocksDB 和 KvTablet 的 checkpoint 可以独立进行，因为 DvRocksDB 重放只依赖 changelog，不依赖 KV State。

但 §11.1 建议"在每次 Iceberg snapshot advance 后触发一次 DvRocksDB checkpoint"。如果 KvTablet 的 checkpoint 频率不同，两者的 checkpointLogHw 可能不同。恢复时需要分别从各自的 checkpointLogHw 开始重放。

**建议**：文档应明确 DvRocksDB 和 KvTablet 的 checkpoint 是独立的，恢复时各自从自己的 checkpointLogHw 开始重放。

---

## 分析 24：Iceberg RowDelta 的原子性

§7.2 中 Tiering Writer 通过 `RowDelta.addRows(dataFiles)` 和 `RowDelta.addDeletes(dvFiles)` 提交。

Iceberg 的 RowDelta 是原子操作——要么 data files 和 DV files 一起提交成功，要么都不提交。所以不会出现 data files 提交了但 DV files 没提交的情况。

结论：**正确**。Iceberg 的事务语义保证了原子性。

---

## 分析 25：LakeDv 快照与 Tiering Writer 的 localDv 合并

§7.2 步骤 3 中，将 lakeDvSnapshot 和 localDv 合并生成 Puffin DV 文件。

如果 lakeDvSnapshot 和 localDv 涉及同一个文件（比如 file_A 在 LakeDv 快照中有 {0, 2}，localDv 中也有 file_A 的条目），需要合并 bitmap。

这种情况是否可能？

- LakeDv 快照中的文件是之前 snapshot 中的文件
- localDv 中的文件是本轮 tiering 新写入的文件

两者不应该有交集——LakeDv 快照中的文件是旧文件，localDv 中的文件是新文件。

但如果本轮 tiering 写入的 data file 恰好与旧文件同名？不可能，Iceberg data file 名包含 UUID，不会重复。

结论：**正确**。LakeDv 快照和 localDv 涉及的文件不会重叠。

---

## 分析 26：Union Read 读到的 Iceberg snapshot 与 LakeDv 的对齐

§10 中 client 获取 DV-readable 的最新 snapshot id，然后请求 union read。

TabletServer 返回的 LakeDv 是针对当前最新 snapshot 的（因为 RowPosIndex 只存最新 snapshot 的 FilePos）。

但如果 client 获取 snapshot id 和 TabletServer 处理请求之间，新的 snapshot 成为 DV-readable 了呢？

时序：
1. Client 获取 DV-readable snapshot = S2
2. 新 snapshot S3 成为 DV-readable
3. TabletServer 处理 union read 请求，此时 LakeDv 已经被清理（S2 的 LakeDv 条目已通过 bitmap 差集清理）

**问题：client 请求的是 S2，但 TabletServer 的 LakeDv 已经是针对 S3 的了。**

S2 中被删除的行：
- 已物化到 S2 的 Iceberg DV（Puffin 文件）→ client 读 S2 时会 apply 这些 DV ✓
- S2 之后、S3 之前的删除：已物化到 S3 的 Iceberg DV，但 S2 中没有 → 需要 LakeDv 覆盖

但 LakeDv 已经清理了 S2 快照中的条目（S3 成为 DV-readable 后清理）。S3 之后的新删除在 LakeDv 中，但 S2 到 S3 之间的删除既不在 S2 的 Iceberg DV 中，也不在当前 LakeDv 中。

**这是一个正确性问题吗？**

等等，让我重新理解 LakeDv 的语义。

LakeDv 积累的是"自上次 tiering commit 以来的删除"。当 S3 commit 后，LakeDv 快照被物化到 S3 的 Puffin DV 中。S3 成为 DV-readable 后，LakeDv 通过 bitmap 差集清理已物化的条目。

如果 client 读的是 S2，那么 S2 到 S3 之间的删除应该在哪里？
- 这些删除已经物化到 S3 的 Puffin DV 中
- 但 client 读的是 S2，S2 没有这些 Puffin DV

所以 client 应该读 S3 而不是 S2。一旦 S3 成为 DV-readable，client 应该切换到 S3。

**关键假设：client 获取 DV-readable snapshot 后，在整个 union read 过程中使用同一个 snapshot。如果中途有新 snapshot 成为 DV-readable，client 不会切换。**

但 TabletServer 侧的 LakeDv 已经被清理了。如果 client 请求的是 S2，TabletServer 应该返回针对 S2 的 LakeDv。但 S2 的 LakeDv 条目已被清理。

### 问题 G：Snapshot 切换时 LakeDv 的可见性窗口

**问题描述**：当新 snapshot S3 成为 DV-readable 后，TabletServer 清理了 S2 对应的 LakeDv 条目。但如果有 client 仍在使用 S2 进行 union read，它获取到的 LakeDv 不完整。

**分析**：这个问题的严重程度取决于 client 获取 snapshot 和请求 LakeDv 之间的时间窗口。

如果 client 在同一个 RPC 中获取 snapshot id 和 LakeDv（即 §10 的流程是原子的），那么：
- Client 请求时，TabletServer 返回当前 DV-readable snapshot 和对应的 LakeDv
- 如果 S3 已成为 DV-readable，TabletServer 返回 S3 和 S3 对应的 LakeDv
- Client 不会拿到 S2 的 snapshot id 和 S3 的 LakeDv

但如果 client 先获取 snapshot id（S2），然后在另一个 RPC 中请求 LakeDv，中间 S3 成为 DV-readable 并清理了 LakeDv，那么 client 拿到的 LakeDv 是不完整的。

**修复方案**：
1. 确保 client 在同一个 RPC 中获取 snapshot id 和 LakeDv（§10 的流程已经是这样的）
2. TabletServer 在返回 LakeDv 时，验证请求的 snapshot id 是否仍是当前 DV-readable snapshot。如果不是，返回错误让 client 重试
3. 或者，TabletServer 保留上一个 DV-readable snapshot 的 LakeDv 副本，直到确认没有 client 在使用

方案 2 最简单，且 §10 的流程已经在同一个读锁保护下获取所有信息。只需要确保 LakeDv 清理不会在 union read 的读锁持有期间发生（LakeDv 清理需要写锁，与读锁互斥）。

实际上，§10 步骤 3 获取 KvTablet 读锁，步骤 5 读取 LakeDv。LakeDv 清理需要 LakeDv 写锁（分析 11 中已确认）。但 §10 获取的是 KvTablet 读锁，不是 LakeDv 写锁。

如果 LakeDv 清理只需要 LakeDv 写锁（不需要 KvTablet 写锁），那么 §10 的 KvTablet 读锁不能阻止 LakeDv 清理。

**需要确认**：LakeDv 清理是否在 KvTablet 写锁保护下执行？如果是，§10 的读锁可以阻止清理。如果不是，需要额外的同步机制。

**建议**：LakeDv 清理应在 KvTablet 写锁保护下执行，或者 §10 在读取 LakeDv 时也获取 LakeDv 读锁，确保清理不会并发执行。

---

## 问题汇总

| 编号 | 问题 | 严重程度 | 影响 |
|------|------|----------|------|
| A | PendingDeletes 写入条件过于宽松，可能将 WAL 中未 tiering 的行误写入 PendingDeletes，导致 position report 到达时错误标记 LakeDv | **严重（数据丢失）** | 存活的行被错误标记为已删除 |
| B | Compaction position report 与 DELETE 的竞态，compaction 的 position report 盲目写入 RowPosIndex，不检查行是否已被删除 | **严重（数据重现）** | 已删除的行在 Iceberg 中重新暴露 |
| C | LakeDv bitmap 差集清理未明确要求持有写锁 | **严重（数据重现）** | 并发写入的删除标记可能丢失 |
| D | Failover 后 pendingTieringStartOffset 丢失，导致 PendingDeletes 写入条件失效 | **中等** | 恢复后可能出现 position report 竞态 |
| E | Compaction 后 LakeDv 旧文件条目清理时机不明确 | **严重（数据重现）** | 旧 snapshot 的已删除行可能重新暴露 |
| F | LogDv 清理时机需要与 DV-readable 对齐 | **中等** | 旧 snapshot 的 union read 可能读到已删除的 WAL 记录 |
| G | Snapshot 切换时 LakeDv 的可见性窗口 | **中等** | Client 可能拿到不完整的 LakeDv |

---

## 修复建议汇总

### 问题 A 修复：精确化 PendingDeletes 写入条件

```
§6.2 中 RowPosIndex 查不到 oldRowId 时：

if oldRowId < lastCommittedTieredOffset:
    // 该行已被 tiering 过，应该在 RowPosIndex 中
    // 查不到说明 compaction 正在进行或 position report 延迟
    → 写入 PendingDeletes
else if pendingTieringSplitRange 存在
        AND oldRowId >= pendingTieringSplitRange.start
        AND oldRowId <= pendingTieringSplitRange.end:
    // 该行正在被当前 tiering split 处理
    → 写入 PendingDeletes
else:
    // 该行还在 WAL 中，从未被 tiering 过
    → 跳过（只更新 LogDv）
```

### 问题 B 修复：统一 position report 处理逻辑

所有 position report（tiering 和 compaction）处理时，统一检查：

```
对每个 (RowId, file, row_position)：
  1. 检查 PendingDeletes：
     - 存在 → 标记 LakeDv，从 PendingDeletes 移除，不写入 RowPosIndex
  2. 检查 RowPosIndex 中是否已存在该 RowId：
     - 不存在（已被 DELETE 清理）→ 标记 LakeDv，不写入 RowPosIndex
     - 存在 → 用新 FilePos 覆盖（compaction 场景）或写入新条目（tiering 场景）
```

对于 compaction 场景，步骤 2 的"不存在"检查是关键——它捕获了 DELETE 先于 compaction position report 到达的情况。

### 问题 C 修复：明确 LakeDv 清理的加锁要求

§13.3 的清理流程应明确：

```
1. 获取 LakeDv 写锁
2. 对每个 file_id in snapshotBitmaps：
   a. 读取当前 bitmap
   b. 计算差集：当前 bitmap AND NOT 快照 bitmap
   c. 写入差集结果（或删除空条目）
3. 释放 LakeDv 写锁
```

### 问题 D 修复：持久化 pending tiering 状态

在 DvRocksDB checkpoint 元数据中增加：
- `pendingTieringSplitStart`：当前正在进行的 tiering split 的起始 offset（-1 表示无）
- `pendingTieringSplitEnd`：当前正在进行的 tiering split 的结束 offset

恢复时从 checkpoint 元数据中读取这两个值。

### 问题 E 修复：统一清理时机

所有 LakeDv 条目的清理（包括 tiering 和 compaction 产生的新 snapshot）都应在新 snapshot 成为 DV-readable 之后执行。§12.1 的步骤 4 应改为：

```
4. 暂不清理 LakeDv 中旧文件的条目
5. 等新 snapshot 成为 DV-readable 后，再清理旧文件的 LakeDv 条目
```

### 问题 F 修复：LogDv 清理与 DV-readable 对齐

LogDv 清理时机改为：在新 snapshot 成为 DV-readable 后，清理小于新 snapshot 的 start_logOffset 的 LogDv 条目。

### 问题 G 修复：确保 union read 的 snapshot 一致性

方案 1（推荐）：§10 的 union read 流程中，TabletServer 在返回结果时附带当前 DV-readable snapshot id。如果与 client 请求的 snapshot id 不一致，返回新的 snapshot id 让 client 使用新 snapshot。

方案 2：LakeDv 清理在 KvTablet 写锁保护下执行，确保 §10 的读锁可以阻止并发清理。

---

## 未覆盖的场景（需要进一步分析）

1. **网络分区**：TabletServer 与 CoordinatorServer 之间的网络分区可能导致 DV-readable 通知延迟或丢失。需要超时重试机制。

2. **Tiering Writer 重试**：如果 Tiering Writer commit 失败后重试，可能产生重复的 position report。§7.3 的处理需要是幂等的（RowPosIndex 的 put 操作天然幂等）。

3. **并发 Compaction**：如果多个 compaction 同时进行，涉及同一个文件的不同行，position report 的处理顺序可能影响 RowPosIndex 的最终状态。需要确保 compaction 是串行的或者 position report 处理是原子的。

4. **Schema Evolution**：如果表 schema 发生变化，KV State 中的 value 格式可能不同。RowId 在 value 首部的位置不受 schema 变化影响（始终是前 8 bytes），但 BinaryRow 的解析可能受影响。需要确认 schema evolution 不影响 RowId 的提取。

5. **Bucket 数量变化**：如果表的 bucket 数量发生变化（repartition），RowId 到 bucket 的映射可能改变。需要确认 repartition 场景下 DV 的正确性。
