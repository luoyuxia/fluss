# Fluss Deletion Vector 设计文档 Review

## 整体评价

这是一份**非常高质量的设计文档**。三层 DV 架构设计清晰，职责分明，端到端示例详尽，锁机制和一致性分析到位。以下是详细的 review 意见。

---

## 设计优点 ✅

- **三层 DV 架构分层清晰**：Iceberg DV（物理层）、LakeDv（跨层桥梁）、LogDv（热层内部）各司其职，职责边界明确。
- **RowId = log offset 的设计非常巧妙**：天然唯一递增，与 Iceberg `__offset` 列一致，避免了额外的 ID 生成器。
- **TabletServer 轻量 + Tiering Writer 重 IO 的分工合理**：TabletServer 只做本地 RocksDB 读写，重 IO 操作全部下推到 Tiering Writer。
- **Tiering Writer 无需 RPC 反查 RowPosIndex**：通过 LakeDv 快照（跨 split）+ 本地 positionReport（同 split 内）两条路径覆盖，设计优雅。
- **LakeDv 增量存储 + bitmap 差集清理**：避免了全量 DV 膨胀，清理逻辑正确处理了快照与清理之间的时间窗口。
- **DV-readable 机制**：不在 tiering commit 成功时立即清理 LakeDv，而是等 CoordinatorServer 收齐所有 bucket 通知后再清理，避免了查询结果错误。
- **端到端示例（Section 14）非常好**：完整覆盖了写入、tiering、union read、删除、第二轮 tiering 的全流程，便于理解和验证。

---

## 问题与疑虑

### 1. 写锁持有时间被 DV 更新拉长，可能成为性能瓶颈 ⚠️

Section 6.2 中：

> KvTablet 写锁的持有时间被 DV 更新操作拉长。在高吞吐场景下，可以考虑先在 flush 阶段收集 `(oldRowId, change_type)` 列表，释放写锁后再异步批量更新 DV，但需要额外处理一致性。

文档提到了这个问题但没有给出明确的方案。这是一个**关键的性能问题**：

- 每次 changelog 同步成功后，需要在写锁内遍历所有 `-U/-D` 记录，逐条查 RowPosIndex（RocksDB 读）、更新 LakeDv（RocksDB 写）、更新 LogDv（RocksDB 写）。
- 在高吞吐场景下，一批 flush 可能包含大量 `-U/-D` 记录，写锁持有时间可能显著增加。

**建议**：
- 明确是否在第一版实现中采用同步方案（简单但可能有性能问题），还是直接采用异步方案。
- 如果采用同步方案，建议给出预期的锁持有时间量级估算（如：每条 DV 更新的 RocksDB 读写延迟 × 批量大小）。
- 如果采用异步方案，需要详细设计一致性保证（如何确保 DV 更新在 `log_hw` 更新之前完成）。

### 2. RowPosIndex 的空间开销需要量化 ⚠️

RowPosIndex 为每行存活数据存储 `RowId (8 bytes) → FilePos (8 bytes)`，加上 RocksDB 的 key/value 开销。

- 对于一个 10 亿行的表，RowPosIndex 的存储量约为 `10^9 × (8+8) bytes ≈ 16 GB`（不含 RocksDB 开销）。
- 加上 RocksDB 的 block index、bloom filter 等元数据，实际内存和磁盘占用可能更大。

**建议**：
- 补充空间开销的量化分析。
- 讨论是否需要对 RowPosIndex 做 compaction 优化（如 prefix bloom filter）。
- 考虑是否需要为 RowPosIndex 设置独立的 block cache 大小限制。

### 3. FileDict 的并发安全和容量管理 ⚠️

FileDict 将 `file_path (string) → file_id (int)` 做字典编码。

- **file_id 溢出**：`file_id` 是 `int` 类型（4 bytes），最大 ~21 亿。对于长期运行的表，如果 compaction 频繁产生新文件，file_id 是否会溢出？旧文件被清理后，file_id 是否可以回收？
- **FileDict 清理**：当旧文件从 Iceberg 中删除后，FileDict 中对应的条目是否会被清理？如果不清理，FileDict 会持续膨胀。
- **并发访问**：FileDict 在多个流程中被访问（changelog 同步、tiering split 生成、snapshot 处理），是否需要额外的并发控制？

**建议**：补充 FileDict 的生命周期管理和容量规划。

### 4. Union Read 的读锁与写入的互斥影响

Section 10 中 union read 需要获取 KvTablet 读锁，Section 6.2 中 changelog 同步需要获取 KvTablet 写锁。

- 如果 union read 频繁，读锁会与写锁竞争。
- 特别是 union read 需要从 LakeDv 和 LogDv 中读取数据，如果数据量大，读锁持有时间可能较长。

**建议**：
- 评估读写锁竞争的影响。
- 考虑是否可以用 snapshot 机制（如 RocksDB snapshot）替代读锁，减少锁竞争。

### 5. LogDv 的 range 大小选择未讨论

Section 3.4 中 LogDv 使用 "固定间隔" 的 offset range 作为 key，但没有讨论这个间隔应该设为多大。

- **间隔太小**：RocksDB 中的 key 数量多，查询时需要扫描更多 key。
- **间隔太大**：每个 bitmap 可能很大，且更新时需要读取-修改-写回整个 bitmap。
- 间隔大小还影响 LogDv 返回给 client 的数据量。

**建议**：讨论 range 大小的选择策略，给出推荐值和依据。

### 6. 恢复流程的幂等性保证需要显式说明

Section 11.2 恢复步骤中，从 `checkpointLogHw + 1` 开始重放 changelog。如果同一个 key 在 checkpoint 之后被更新了两次：

1. 第一次更新：`-U(oldRowId=A)` → 删除 RowPosIndex[A]，更新 LakeDv
2. 第二次更新：`-U(oldRowId=B)` → B 是第一次更新的 `+U` 的 offset，但 B 还没有被 tiering，所以 RowPosIndex 中没有 B

恢复时重放这两条记录，第一次更新会正确处理（RowPosIndex 中有 A），第二次更新也会正确处理（RowPosIndex 中没有 B，跳过 LakeDv 更新）。**这是正确的**。

但建议在文档中**显式说明这个幂等性保证**，让 reviewer 更容易验证正确性。

### 7. Snapshot 处理中 newFiles 的 RowId 来源不够清晰

Section 8.2 Step 1 中：

> 对该文件中的每个 RowId：从 RowPosIndex 反查...

这里的 "该文件中的每个 RowId" 从哪里获取？

- 如果是 Fluss 自己写的文件（在 `knownFiles` 中），RowId 来自 positionReport，这是清楚的。
- 如果是外部 compaction 产生的文件，需要扫描文件读取 `__offset` 列获取 RowId。

但文档的描述把这两种情况混在了一起，建议分开描述，让流程更清晰。

### 8. LakeDv 快照与 Tiering Split 的原子性

Section 7.1 中：

> 1. 获取 KvTablet 读锁
> 2. 读取当前 `log_hw` 作为 `latest_offset`
> 3. 快照当前 LakeDv
> 4. 释放读锁

**问题**：如果 LakeDv 的数据量很大（虽然文档说通常很小），快照操作可能耗时较长，读锁持有时间增加。

**建议**：考虑是否可以用 RocksDB 的 snapshot 功能来实现 LakeDv 的快照，而不是在读锁内做全量拷贝。

### 9. 缺少对分区表的讨论

整个文档没有提到分区表的场景。对于分区表：

- 每个分区是否有独立的 DvRocksDB？
- RowPosIndex、LakeDv、LogDv 是否按分区隔离？
- Tiering 是按分区独立进行的，DV 的生命周期是否也按分区独立管理？

**建议**：补充分区表场景的设计说明。

### 10. 缺少监控指标和可观测性设计

建议补充以下监控指标：

- RowPosIndex 的条目数量和存储大小
- LakeDv 的条目数量（活跃的 file 数 × 平均 bitmap 大小）
- LogDv 的条目数量
- DV 更新延迟（从 `-U/-D` 到达到 DV 更新完成的时间）
- Union read 中 DV apply 的耗时
- FileDict 的条目数量
- 外部 compaction 文件扫描次数（`external_compaction_files_scanned` 已提到，很好）

---

## 小问题

- **Section 6.2 步骤编号**：步骤 3 "获取 LakeDv 写锁" 和步骤 1 "获取 KvTablet 写锁" 是两把不同的锁，但文档中没有明确说明它们的关系。是否 LakeDv 写锁是 KvTablet 写锁的子锁？还是独立的锁？建议明确锁的层级关系。
- **Section 8.2 中 "已有 s2，新来 s3"**：这里的 s2、s3 是 snapshot id，建议首次出现时用全称 "snapshot S2"、"snapshot S3"。
- **Section 7.2 中 "被 localDv 标记删除的行不需要上报"**：这个优化很好，但建议补充说明原因——因为这些行已经被 DV 标记删除，后续不会被 union read 读到，RowPosIndex 中也不需要维护它们的位置。
- **Section 4.3 中 v2 → v3 升级**：`table.updateProperties().set("format-version", "3").commit()` 这个 API 是否真的支持原地升级？Iceberg 的 format version 升级通常是通过 `table.updateProperties()` 还是 `TableOperations`？建议确认 API 的正确性。

---

## 总结

这是一份设计非常扎实的文档，三层 DV 的架构设计、一致性保证、锁机制分析都很到位。主要需要关注的点：

1. **写锁持有时间的性能影响**——需要明确同步 vs 异步方案
2. **RowPosIndex 的空间开销量化**——10 亿行级别的表需要评估
3. **FileDict 的生命周期管理**——file_id 回收和条目清理
4. **LogDv range 大小的选择**——需要给出推荐值
5. **分区表场景**——需要补充设计说明
6. **监控指标**——需要补充可观测性设计
