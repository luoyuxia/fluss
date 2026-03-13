这是一份非常详尽的设计文档，整体架构清晰、考虑周全。以下是我的 review 意见：

整体评价
三层 DV 的分层设计很合理，职责划分清晰：Iceberg DV 管物化后的删除，LakeDv 管跨层实时删除，LogDv 管 WAL 内部去重。TabletServer 做轻量元数据维护、Tiering Writer 做重 I/O 的分工也很好，避免了 RPC 反查的开销。

需要关注的问题
KvTablet 写锁持有时间变长
§6.2 中 changelog 同步成功后，在 KvTablet 写锁内做了 RowPosIndex 查询 + LakeDv 更新 + LogDv 更新。文档提到了"可以考虑异步批量更新"，但这块对写入吞吐影响较大，建议在设计阶段就给出明确的方案选择，而不是留作"可以考虑"。特别是高吞吐场景下，每条 -U/-D 都要做一次 RocksDB point lookup（RowPosIndex），这个延迟会直接叠加到写入路径上。

建议：可以考虑在 flush 阶段收集 (oldRowId, change_type) 列表，释放写锁后批量更新 DV，用一个轻量的 pending queue + 版本号来保证一致性。

LakeDv 快照的内存开销
§7.1 中快照 LakeDv 时需要将 file_id 反向映射为 file_path。如果两轮 tiering 之间有大量删除（比如批量 delete 场景），LakeDv 快照可能包含大量 {file_path → bitmap} 条目。file_path 包含 UUID，字符串较长。

建议：评估一下极端场景下 LakeDv 快照的内存上限，考虑是否需要分批下发或压缩传输。

LogDv 的 range 粒度未定义
§3.4 提到 LogDv 的 key 是"固定间隔"的 offset range，但没有给出具体的间隔大小。这个值直接影响：

bitmap 的大小和查询效率
range 条目的数量
内存占用
建议：给出推荐的 range 大小（比如 1024 或 4096），以及选择依据。

恢复流程中 LogDv 的重建
§11.2 步骤 3 中，恢复时对 -U/-D 需要判断 oldRowId 与 snapshotStartLogOffset 的关系来决定是否更新 LogDv。但如果 checkpoint 时 LogDv 已经包含了部分条目，重放时再次写入相同的 offset 到 LogDv，bitmap 操作是幂等的（set bit），所以正确性没问题。但文档没有明确说明这一点，建议补充说明幂等性保证。

外部 Compaction 的 __offset 列依赖
§8.1 和 §12.2 中，外部 compaction 兜底方案依赖扫描 __offset 列。但如果外部引擎（如 Spark）在 compaction 时没有保留 __offset 列，或者重新排序了数据，这个方案就会失败。

建议：明确 __offset 列是 Fluss 管理的 Iceberg 表的必要约束，并在文档中说明如果外部 compaction 不保留该列会怎样（是否需要禁止、告警、或降级处理）。

Snapshot 处理中 newFiles 的并发安全
§8.2 Step 1 中，处理 newFiles 时需要获取 LakeDv 写锁。但如果 newFiles 数量很多（比如外部 compaction 产生了大量新文件），扫描文件 + 更新 RowPosIndex + 更新 LakeDv 的时间可能很长，会阻塞正常的写入路径（因为写入路径也需要 LakeDv 写锁）。

建议：考虑将 newFiles 的处理拆分为"扫描 + 准备"（不持锁）和"批量写入"（短暂持锁）两个阶段。

FileDict 的 file_id 分配
§5.1 中 FileDict 将 file_path 映射为 int 类型的 file_id，但没有说明 file_id 的分配策略。需要考虑：

是否使用自增 ID？如果是，恢复后如何保证不冲突？
file_id 用 4 bytes（int），最大约 21 亿，对于长期运行的系统是否足够？
旧文件清理后，file_id 是否可以回收？
建议：补充 file_id 的分配和回收策略。

DV-readable 窗口期的一致性
§13.3 中提到 tiering commit 成功到 S2 成为 DV-readable 之间有一个窗口期。在这个窗口内，LakeDv 同时包含已物化到 S2 的删除和新增的删除。如果这个窗口期很长（比如某个 bucket 的 DV 处理很慢），LakeDv 会持续膨胀。

建议：考虑对这个窗口期设置超时告警，以及 LakeDv 大小的监控指标。

小问题
§3.4 的 LogDv 示例中，"bitmap = {1}，表示该 range 中第一条数据被删掉了"——这里用的是 1-based index 还是 0-based？如果 offset=0 是 range 中的第一条，bitmap 应该是 {0} 才对（0-based），或者需要明确说明 bitmap 中存的是 range 内的相对偏移。
§14 Step 5 中，LogDv 更新为 {1, 3}，表示第 1 条和第 3 条被删除。但 offset=0 是 range 内第 1 条（index 0），offset=2 是第 3 条（index 2）。如果是 0-based 应该是 {0, 2}，如果是 1-based 应该是 {1, 3}。需要统一并明确说明。
§4.3 中提到存量 v2 表通过 table.updateProperties() 升级到 v3，但 Iceberg 的 format version 升级通常不是通过 updateProperties 而是通过专门的 table.updateProperties().set("format-version", "3") 或 TableOperations。建议确认 Iceberg API 的具体调用方式，不同 Iceberg 版本的 API 可能不同。
总结
整体设计质量很高，三层 DV 的分层、TabletServer/Tiering Writer 的职责分工、LakeDv 的增量存储 + bitmap 差集清理都是很好的设计决策。主要需要关注的是写锁持有时间对吞吐的影响、LogDv range 粒度的具体定义、以及外部 compaction 场景下 __offset 列的约束保证。建议在实现前先对写入路径的锁竞争做一轮 benchmark。