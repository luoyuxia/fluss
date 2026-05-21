# Deletion Vector 性能优化方案

本文档针对 [dv-performance-review-checklist.md](dv-performance-review-checklist.md) 中提出的性能风险，逐一给出设计层面的优化方案或运营兜底策略。

---

## 1. §7.3.1 Phase 2 拆锁

**对应风险**：锁竞争（checklist §1）、PendingDeletes 扫描成本（checklist §2）

### 问题

当前 §7.3.1 Phase 2 将 Ingest + 反向扫 PendingDeletes 捆在同一次写锁中，写锁持有时间 = O(Ingest + |PendingDeletes|)，是全系统最长的临界区。期间 §6.2 delete 处理和 union read 全部阻塞。

### 方案：拆为两次短锁 + 中间无锁扫描

```
// 第一次写锁（快，O(1) Ingest + FileDict write）
lock()
  hard-link + Ingest → pendingRowPos
  write FileDict
unlock()

// 无锁阶段：反向扫 PendingDeletes（慢，但不阻塞 §6.2 和 union read）
batch = []
for (R, v) in PendingDeletes:
    hit = pendingRowPos.get(R)   // RocksDB 并发读安全
    if hit: batch.add(R, hit)

// 第二次写锁（快，只 apply 收集到的结果）
lock()
  for (R, hit) in batch:
      if pendingRowPos.get(R) == hit:   // 验证：§6.2 可能已在无锁阶段处理了 R
          LakeDv[hit.fileId] |= {hit.pos}
          pendingRowPos.delete(R)
          pendingDeletedRowIds.add(R)
          PendingDeletes.put(R, hit)
unlock()
```

### 正确性论证

无锁阶段 §6.2 可能处理了某些 RowId（从 pendingRowPos 删除 + 更新 PendingDeletes + 标记 LakeDv）。第二次加锁时通过 `pendingRowPos.get(R) == hit` 验证：

- **R 已被 §6.2 处理**：pendingRowPos 中 R 已不存在（或值已变），验证失败 → 跳过。不丢不重。
- **R 未被 §6.2 处理**：验证通过，正常 apply。

漏掉的条目 = §6.2 已正确处理的，无遗漏风险。

### 效果

写锁持有时间从 O(Ingest + |PendingDeletes|) 降到 max(O(Ingest), O(|batch_hits|))。batch_hits 通常远小于 PendingDeletes 总条目数（只有外部 compaction 重写行才命中），正常场景下接近 O(Ingest)。

### 优先级：最高

改动范围小（仅 §7.3.1 Phase 2 的锁边界），效果最大（解锁最长临界区）。

---

## 2. LakeDv COW 版本化

**对应风险**���Union Read 锁内工作量（checklist §3）、LakeDv 快照成本（checklist §4）

### 问题

- union read 在读锁内 clone 查询涉及文件的 bitmap 子集。大查询 + 多文件命中时 clone 成本高。
- §7.2.1 生成 split 时要在读锁内快照 LakeDv 全量。LakeDv 大时快照慢，阻塞写路径。

### 方案：不可变版本链 + 原子引用

LakeDv 改为 copy-on-write 版本化结构。每次写操作产生新版本（仅修改的 bitmap 换新引用，其余共享），读操作直接获取当前版本引用。

```java
// 写路径（§6.2，仍在写锁内）
LakeDv newVersion = currentVersion.withUpdated(fileId, newBitmap);
currentVersionRef.set(newVersion);  // atomic reference swap

// union read（无锁）
LakeDv snapshot = currentVersionRef.get();  // atomic, O(1)
// 直接使用 snapshot 中涉及文件的 bitmap，无需 clone
// GC 自动回收无引用的旧版本

// §7.2.1 快照（无锁）
LakeDv snapshotForSplit = currentVersionRef.get();  // atomic, O(1)
// 不可变，安全传递给 Tiering Service
```

### 效果

- union read 完全无锁（消除读写互斥），延迟不再受写路径影响。
- LakeDv 快照从 O(|LakeDv|) clone 降为 O(1) 引用获取。
- 写路径的 `withUpdated()` 开销 ≈ 创建新 Map entry + 新 bitmap（仅涉及修改的文件），远小于全量 clone。

### 注意

`withUpdated()` 需要保证 bitmap 本身不可变。如果使用 Roaring Bitmap，每次修改需 `bitmap.clone()` 后再 set bit。单个 bitmap 的 clone 开销远小于全量 LakeDv clone。

### 优先级：高

同时解决 union read 和 LakeDv 快照两个瓶颈。

---

## 3. §6.2 批量化

**对应风险**：锁竞争（checklist §1）

### 问题

当前每条 `-U`/`-D` 独立取写锁（2 次 point get + 1 WriteBatch），高频 delete 时锁获取/释放开销显著。

### 方案：微批攒写

```
// 攒一批 delete（例如 100 条或 1ms 窗口内的 delete）
batch = collectDeletes(maxSize=100, maxWait=1ms)

// 一次写锁，批量处理
lock()
  for each delete in batch:
      // 原 §6.2 逻辑：point get RowPosIndex + pendingRowPos → mark LakeDv → WriteBatch
  writeBatch.commit()  // 所有 delete 合并为一个 WriteBatch
unlock()
```

### 效果

- 锁获取/释放次数从 N 次降为 1 次（N = batch size）
- WriteBatch 合并减少 RocksDB WAL fsync 次数
- 在低延迟要求场景下 maxWait 可设为 0（纯 size-based batching）

### 优先级：中

高频 delete 场景收益明显，低频场景无感知。

---

## 4. PendingDeletes 迭代优化

**对应风险**：PendingDeletes 扫描成本（checklist §2）

### 问题

§7.3.1 反向扫和 §7.3.3 清理都需要遍历 PendingDeletes。delete-heavy 时条目数膨胀，扫描成本线性增长。

### 方案 A：Stale Sentinel 清理用 Seek 代替全扫

PendingDeletes 的 key = RowId（单调递增 offset）。§7.3.3 的 stale sentinel 清理条件是 `R < currentTieredOffset`。利用 RocksDB 有���性：

```
iterator.seek(FIRST)
while iterator.valid() && iterator.key() < currentTieredOffset:
    delete(iterator.key())
    iterator.next()
// 只扫描 stale 部分，不触碰新条目
```

### 方案 B：反向扫 Bloom Filter 加速

反向扫的 `pendingRowPos.get(R)` 绝大多数是 miss（只有外部 compaction 重写行才命中）。RocksDB 的 per-CF bloom filter 使 miss 的成本接近零（无磁盘 I/O）。

确保 pendingRowPos CF 配置了 bloom filter：

```
columnFamilyOptions.setTableFormatConfig(
    new BlockBasedTableConfig().setFilterPolicy(new BloomFilter(10))
);
```

### 方案 C：PendingDeletes 分区

如果 PendingDeletes 条目数达到百万级，考虑按 offset 范围分区（例如每 100K offset 一个前缀），扫描时只扫描相关分区。但这增加了实现复杂度，建议仅在压测验证必要时引��。

### 效果

- 方案 A：stale sentinel 清理从 O(|PendingDeletes|) 降为 O(|stale entries|)
- 方案 B：反向扫中 miss 路径从 O(point get) 降为 O(bloom check)，实测通常 < 1μs
- 两者组合可显著降低 PendingDeletes 的实际扫描成本

### ���先级：中

改动小（配置级别），在 delete-heavy 场景下收益明显。

---

## 5. 自适应 Tiering 触发

**对应���险**：Delete-Heavy 工作负载（checklist §9）、PendingDeletes 扫描成本（checklist §2）

### 问题

delete-heavy 场景下存在恶性循环：

```
高 delete 比例
  → PendingDeletes 膨胀 → §7.3.1 反向扫变慢 → Phase 2 持锁时间长
  → LakeDv 膨胀 → union read clone/apply 变慢 �� §7.2.1 快照变慢
  �� LogDv 膨��� → union read 过滤成本增加
  → 三者同时膨胀 → tiering 周期拉长 → 下一轮积累更多 delete → 恶性循环
```

### 方案：多条件触发

```
触发 tiering = any(
    定时触发（固���间隔，保底）,
    PendingDeletes 条目数 > threshold_pd,
    LakeDv 总 bit 数 > threshold_lake,
    changelog 积压量 > threshold_log
)
```

���一条件满足即触发，避免 delete 累积超过系统消化能���。

### 阈值参考

通过压测确定稳态边界：在给定硬件配置下，单轮 tiering 能处理的最大 PendingDeletes/LakeDv 规模是多少？阈值设为该上限的 50-70%，留出安全余量。

### 效果

从"被动等时间"变为"主动按压力触发"，打破恶性循环。

### 优先级：高

防止系统在 delete-heavy 场景下失控。不是优化某个环节，而是控制整体节奏。

---

## 6. 外部 Compaction 应对

**对应风险**：外部 Compaction 干扰（checklist §5）

### 问题

外部 compaction 后，Tiering Service 需扫描 external new files 的 `__offset` + `__bucket` 列，大 compaction 可能产生大量/大体积文件。

### 方案

**A. 列投影**（已在设计中）：只读 `__offset` 和 `__bucket` 列，跳过数据列。Parquet 列式存储下 I/O 量大幅减少。

**B. 并行扫描**：多线程扫描 external new files。各文件之间无依赖，天然可并行。

**C. 单轮限速**：如果外部 compaction 产出大量文件，限制单轮 tiering 处理的外部文件数（例如最多 N 个），剩余推迟到下轮。避免单轮 tiering 被外部 compaction 拖垮。

```
externalFiles = detectExternalNewFiles()
if externalFiles.size() > MAX_EXTERNAL_PER_ROUND:
    thisRound = externalFiles.subList(0, MAX_EXTERNAL_PER_ROUND)
    // 剩余文件下轮处理
```

**D. 监控告警**：外��� compaction 文件数/扫描耗时超过阈值时告警，提示用户调整外部 compaction 策略。

### 优先级：中

Tiering Service 侧执行，不影响 TabletServer。本质是外部引擎的成本转嫁，Fluss 只能尽量低开销地适配。

---

## 7. RocksDB Ingest / CF 重建调优

**对应风险**：RocksDB Ingest / CF 重建开销（checklist §6）

### 问题

`DropColumnFamily` + `CreateColumnFamily` 可能引发 MANIFEST 膨胀、后台 compaction 抖动。高频 readable switch 时效应累积。

### 方案

**A. RocksDB 配置调优**：

```
// 减少 Ingest 触发的 compaction
options.setLevel0FileNumCompactionTrigger(8);  // 默认 4，适当放大
options.setMaxBackgroundCompactions(4);

// 控制 MANIFEST 增长
options.setMaxManifestFileSize(128 * 1024 * 1024);  // 128MB
```

**B. 监控 MANIFEST 增长**：每次 Drop/Create CF 后记录 MANIFEST 文件大小。如果增长过快，考虑降低 readable switch 频率或触发 MANIFEST compaction。

**C. 替代方案评估**：如果 DropColumnFamily 的 MANIFEST 副作用在压测中确认严重，可考虑改为 `DeleteRange` + 后台 `CompactRange` 清理 pendingRowPos（避免 CF 级操作），但需权衡清理延迟。

### 优先级：低

IngestExternalFile 本身是 O(1) metadata 操作，实际性能通常优于预期。建议压测验证后再决定是否需要额外调优。

---

## 8. Recovery 开销控制

**对应风险**：Reconcile / Recovery 开销（checklist §8）

### 问题

checkpoint 失败或落后时，恢复需要下载多个 snapshot 的 SST + replay changelog，耗时可能失控。

### 方案

**A. 保证 checkpoint 频率**（§10.3 已建议）：每次 readable switch 后立即触发 DvRocksDB checkpoint。如果 checkpoint 可靠，恢复最多处理一个 pending snapshot → 开销极小。

**B. 并行 SST 下载**：恢复多个 snapshot 时，SST 下载可以并行（各 snapshot 的 SST 相互独立），Ingest 仍需按序执行。

```
// 并行下载
List<Future<SstFiles>> downloads = snapshots.stream()
    .map(s -> executor.submit(() -> downloadSst(s)))
    .collect(toList());

// 按序 Ingest
for (Future<SstFiles> f : downloads):
    ingest(f.get(), RowPosIndex)
```

**C. checkpoint 失败重试 + 告警**：checkpoint 连续失败 N 次后告警（而不是静默等到恢复时才暴露问题）。

### 效果

- 方案 A 使正常场景的恢复 = O(1 snapshot)
- 方案 B 使降级场景（多 snapshot）的恢复从串行下载改为并行下载，瓶颈变为 Ingest 速度

### 优先级：中

正常运行时不触发；降级场景靠方案 B 兜底。

---

## 9. 远��对象存储 GC

**对应风险**：远程对象存储压力（checklist §7）

### 问题

UUID 路径方案下，pre-commit crash 后 retry 会生成新 UUID，旧路径成为孤儿对象。长时间运行后孤儿累积。

### 方案

**后台 GC 任务**：

```
1. 扫描 {$remoteLakeTableSnapshotDir}/rowPos/ 下所有对象路径
2. 收集 Iceberg 当前所有 snapshot property 中引用的 indexUuid + per-bucket uuid 集合
3. 未被引用的 = 孤儿候选
4. 加安全窗口（例如创建时间 > 1h 前），排除正在上传中的对象
5. 删除确认为孤儿的对象
```

**触发时机**：

- 定期执行（例如每小时）
- 或在 DvRocksDB checkpoint 成功后附带执行（checkpoint 覆盖的 snapshot 对应的远程对象可以安全删除，见 §10.3）

### 优先级：低

孤儿产生频率 = retry 频率，通常很低。长期运行需要 GC 兜底，但不是上线前的阻塞项。

---

## 10. 大查询三层 DV Apply

**对应风险**：大查询场景（checklist §10）

### 问题

client 需要同时 apply Iceberg DV、LakeDv、LogDv 三层过滤，大范围扫描时 DV 处理成本可能超过数据读取本身。

### 方案

**A. DV Bitmap 缓存**：对热点文件的 DV bitmap 做 client 侧缓存。同一文件的 DV bitmap 在短时间内不变（只有 tiering commit 时才变），多次查询可复用。

**B. 延迟 apply**：不预加载所有文件的 DV，改为按需加载。只有当 scan 实际读到某个文件时才 apply 该文件的 DV。避免"查询涉及 1000 个文件但只扫描 10 个"时的无效 DV 加载。

**C. 三层合并下推**：如果查询引擎支持，将 Iceberg DV + LakeDv 合并为一个 bitmap 后下推（两者都是 position-level bitmap，可以直接 OR 合并），减少 apply 次数。

### 优先级：低

DV apply 在 client 侧执行，不影响 TabletServer。属于查询引擎优化范畴。

---

## 固有代价（无法根本消除）

| 关注项 | 为什么是固有的 | 最佳兜底 |
|--------|--------------|---------|
| PendingDeletes 两次全扫（§7.3.1 + §7.3.3） | 设计选择：反向扫替代 per-entry alive check | 拆锁后非阻塞扫描 + bloom filter 加速 miss |
| 单飞吞吐上界（上一轮完成前下一轮不能开始） | 两阶段 ack barrier 保证正确性 | 自适应 tiering 频率 + 监控告警 |
| 三层 DV apply（client 侧） | union read 语义要求 | bitmap 缓存 + 延迟 apply + 合并下推 |
| 外部 compaction 文件扫描 | 外部引擎不可控 | 列投影 + 并行扫描 + 单轮限速 |

---

## 优先级总览

| 优先级 | 方案 | 效果 | 改动范围 |
|--------|------|------|---------|
| **最高** | §7.3.1 Phase 2 拆锁 | 解锁最长临界区 | §7.3.1 锁边界 |
| **高** | 自适应 tiering 触发 | 防止 delete-heavy 恶性循环 | TieringService 触发逻辑 |
| **高** | LakeDv COW 版本化 | union read 无锁 + 快照 O(1) | LakeDv 数据结构 |
| **中** | §6.2 批量化 | 减少锁竞争频率 | §6.2 写路径 |
| **中** | PendingDeletes 迭代优化 | stale 清理提速 + miss 接近零成本 | RocksDB 配置 + 迭代逻辑 |
| **中** | Recovery 并行下载 | 降级场景恢复提速 | §10.2 恢复流程 |
| **中** | 外部 compaction 并行/限速 | 避免单轮 tiering 被拖垮 | §11 扫描逻辑 |
| **低** | RocksDB 调优 | 减少 Ingest/CF 重建副作用 | RocksDB 配置 |
| **低** | 远程对象 GC | 清理 UUID 孤儿 | 后台任务 |
| **低** | 大查询 DV 优化 | client 侧查询提速 | 查询引擎 |
