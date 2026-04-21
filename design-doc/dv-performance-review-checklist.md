# Deletion Vector 性能 Review 清单

本文档整理 Deletion Vector 方案的通用性能 review 检查项，适用于设计评审、压测规划和上线前风险排查。

## 1. 锁竞争

### 风险点

- 全局 `DvRWLock` 串行化了所有写路径，并与 union read 读路径互斥。

### 关注问题

- `§6.2` 高频 delete/update 时，写锁是否成为热点。
- `§7.3.1` 和 `§7.3.3` 在锁内做 Ingest、扫描、清理，是否拉长持锁时间。
- union read 在高 QPS 下是否压住写路径，或反过来被写路径拉高尾延迟。

### 建议指标

- 读锁/写锁平均持有时长、P95、P99。
- 读等待时间、写等待时间。
- tiering / positionReport / readable switch 期间 union read 延迟分布。

## 2. `PendingDeletes` 扫描成本

### 风险点

- 设计将“扫描 SST 每行”优化为“全扫 `PendingDeletes`”。

### 关注问题

- 在 delete-heavy workload 下，`PendingDeletes` 是否持续膨胀。
- `§7.3.1` 的反向扫是否从“增量成本”退化成“历史状态成本”。
- `§7.3.3` 再次全扫清理时是否形成双倍放大。

### 建议指标

- `PendingDeletes` 条目数。
- 单轮 positionReport 处理耗时 vs `PendingDeletes` 大小。
- readable switch 耗时 vs `PendingDeletes` 大小。
- 每次扫描命中率（扫描多少、真正命中多少）。

## 3. Union Read 锁内工作量

### 风险点

- union read 在锁内 clone `LakeDv` 子集并读取 `LogDv`。

### 关注问题

- 大查询、宽扫描、多文件命中时，锁内 clone 是否过重。
- bitmap clone 大小时延是否线性增长。
- 查询规模扩大时是否明显阻塞写路径。

### 建议指标

- 单次 union read 涉及的 file 数。
- 返回的 `LakeDv` bitmap 总字节数。
- 锁内 clone 时间。
- union read P95 / P99 延迟。

## 4. LakeDv 快照成本

### 风险点

- 生成 split 时要快照当前 LakeDv 全量内容。

### 关注问题

- delete 积累较多时，split 生成是否明显变慢。
- 内存复制、bitmap clone、字典解析是否成为热点。
- split 生成是否随着 LakeDv 文件数或 bit 数线性恶化。

### 建议指标

- split 生成耗时。
- `snapshotBitmap` 大小。
- LakeDv 中 file 数、总 bit 数、总字节数。
- 生成 split 时额外内存峰值。

## 5. 外部 Compaction 干扰

### 风险点

- 外部 compaction 后，TieringService 需要扫描 `externalNewFiles`。

### 关注问题

- 大 compaction 是否显著拉长下一轮 tiering。
- 外部引擎产生的大文件/大量文件是否造成不可预测抖动。
- 扫描 `__offset` / `__bucket` 的 I/O 和 CPU 成本是否可接受。

### 建议指标

- 单轮 external compaction 扫描文件数。
- 扫描总字节数。
- 外部 compaction 触发后的 tiering latency 增量。
- 外部 compaction 场景下的 end-to-end commit 时间。

## 6. RocksDB Ingest / CF 重建开销

### 风险点

- `pendingRowPos -> RowPosIndex` 依赖 Ingest + `DropColumnFamily` + Recreate。

### 关注问题

- 实际 RocksDB 行为是否真的接近 O(1)。
- Drop/Recreate CF 是否引发 stall、MANIFEST 放大、后台 compaction 抖动。
- 高频 snapshot 前移时，是否出现非预期系统开销。

### 建议指标

- 单次 Ingest 耗时。
- Drop/Recreate CF 耗时。
- RocksDB stall 次数。
- MANIFEST 增长速度、后台 compaction 指标。

## 7. 远程对象存储压力

### 风险点

- 每个 snapshot、每个 bucket 都要写 SST/manifest，再加 cross-bucket index。

### 关注问题

- 对象数增长是否过快。
- PUT/GET/List 开销是否影响 tiering 周期。
- 清理策略是否跟得上对象增长速度。

### 建议指标

- 每轮 tiering 产生的对象数。
- 单表累计远程对象数。
- 单次 reconcile / recovery 的 GET 数量。
- 远程存储 API 调用耗时和失败率。

## 8. Reconcile / Recovery 开销

### 风险点

- checkpoint 失败或落后时，恢复需要下载 index / manifest / SST 并 replay。

### 关注问题

- 在多 snapshot、长时间无 checkpoint 的情况下，恢复时间是否失控。
- 恢复流量是否可接受。
- 恢复期间对正常服务是否有明显影响。

### 建议指标

- 恢复总耗时。
- 下载 SST 总量。
- 重放 changelog 数量。
- 恢复后第一次 tiering / union read 延迟。

## 9. Delete-Heavy 工作负载

### 风险点

- 整个方案很多关键假设都建立在“未物化删除通常较小”上。

### 关注问题

- 当 update/delete 比例高时，LakeDv、LogDv、PendingDeletes 是否同时膨胀。
- tiering 周期是否因此显著拉长。
- union read 返回的逻辑 DV 是否过大。

### 建议指标

- delete/update 比例。
- LakeDv 总字节数。
- LogDv 条目数、bitmap 大小。
- PendingDeletes 条目数。
- end-to-end tiering latency。

## 10. 大查询场景

### 风险点

- client 需要同时 apply Iceberg DV、LakeDv、LogDv。

### 关注问题

- 查询引擎是否能高效处理三层过滤。
- 大范围扫描时 DV 应用成本是否明显超过预期。
- 是否出现“读结果不大，但 DV 处理很重”的情况。

### 建议指标

- 查询扫描文件数。
- Iceberg DV 应用耗时。
- LakeDv 应用耗时。
- 增量 log 读取与 LogDv 过滤耗时。
- 整体查询端到端耗时。

## 建议重点压测场景

### 场景 A：高 delete/update 比例

- 验证 `PendingDeletes`、LakeDv、LogDv 的增长曲线。

### 场景 B：大范围 union read

- 验证锁竞争和 bitmap clone 成本。

### 场景 C：外部 compaction 干扰

- 验证下一轮 tiering 延迟抖动。

### 场景 D：checkpoint 连续失败后的恢复

- 验证下载 SST + replay 的上界。

### 场景 E：长时间运行

- 验证 file 数、远程对象数、RocksDB 状态规模的长期增长。

## 优先级最高的关注项

- `PendingDeletes` 扫描成本。
- 全局 `DvRWLock` 竞争。
- 外部 compaction 扫描成本。
- 大查询下 union read 锁内 clone 成本。
- 恢复路径耗时上界。

## 总结

- 这套方案的性能风险主要集中在：
  - 状态规模增长。
  - 全局锁竞争。
  - 扫描型操作。
  - 恢复与外部 compaction 的长尾成本。
- 设计上不是不能接受，但必须靠专项压测验证，而不能只依赖“通常很小”“一般是 ms 级”这类假设。
