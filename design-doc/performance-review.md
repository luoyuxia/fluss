结论

- 我反复对照后，确认这份设计里有 5 个明确的性能热点，不是“可能会慢”，而是规模一上来就会放大；另有 1 个很确定的小优化点。
- 我没有把 RowId 多 8 bytes、LogDv 固定 range 之类列成问题，因为它们更像设计 tradeoff，不像这里这几处这么确定。

确认有问题

- Major：dvLock.writeLock() 持锁范围过大。 positionReport 整段在写锁下处理，readable switch 也整段在写锁下执行；而 union
  read 读 LakeDv 时还要拿 dvLock.readLock()。这意味着一次大 split / 外部 compaction / switch，会同时阻塞 -U/-D 路径和 union
  read，尾延迟会被直接拉高。见 design-doc/fluss-deletion-vector-design-v2.md:343, design-doc/fluss-deletion-vector-design-
  v2.md:549, design-doc/fluss-deletion-vector-design-v2.md:655, design-doc/fluss-deletion-vector-design-v2.md:748
- Major：positionReport 设计成“全量内存构建 + 单次 RPC + 单个 WriteBatch 原子提交”，峰值内存和停顿都不受控。 Writer 先把所
  有 (RowId, file, row_position) 放进内存，再一次性上报；TabletServer 再一次性做 WriteBatch。当 split 很大或 compaction 重
  写很多行时，内存、网络包体、批写停顿都会按“行数”线性放大。见 design-doc/fluss-deletion-vector-design-v2.md:492, design-
  doc/fluss-deletion-vector-design-v2.md:539, design-doc/fluss-deletion-vector-design-v2.md:560, design-doc/fluss-deletion-
  vector-design-v2.md:866
- Major：每轮 split 都全量快照 LakeDv，并在锁内做 bitmap 深拷贝和 file_id -> file_path 反查。 这一步不是按本轮 delta，而是
  按“当前全部未物化删除”做；同时还要保留一份 snapshotBitmap 副本，等于一次 split 至少两份 bitmap 生命周期重叠。删除分散到很
  多文件时，这会明显放大 CPU、内存和加锁时间。见 design-doc/fluss-deletion-vector-design-v2.md:470, design-doc/fluss-
  deletion-vector-design-v2.md:472, design-doc/fluss-deletion-vector-design-v2.md:474
- Major：外部 compaction 和恢复都落成“逐行重扫新文件”的重路径。 文档要求对 newFiles / externalNewFiles 逐行读 __offset、
  __bucket 来重建位置；大 compaction 一次就可能把很多文件全重写，恢复时也同理。这会把 commit 延迟和恢复时间直接绑到“重写了
  多少行”，而不是“这轮有多少真实变更”。见 design-doc/fluss-deletion-vector-design-v2.md:823, design-doc/fluss-deletion-
  vector-design-v2.md:864
- Major：union read 的锁范围也偏大。 它在同时持有 KvTablet 读锁和 dvLock.readLock() 时，才去校验 snapshot、取
  logEndOffset、按 datafile list 取 LakeDv、按 offset range 取 LogDv；如果 snapshot 文件很多，读取 bitmap 本身就会拖长锁持
  有时间，进一步压住写入和切换。见 design-doc/fluss-deletion-vector-design-v2.md:747, design-doc/fluss-deletion-vector-
  design-v2.md:756

可以优化

- 把大部分计算移出锁外。 positionReport 先在锁外完成文件分组、file_id 解析、存活性初判，再在锁内只做最终 apply；更进一步可
  以拆成 RowPosIndex/PendingDeletes 与 LakeDv 两类锁，避免一个大写锁串行化所有路径。
- 把 positionReport 改成分块/流式协议。 例如按 file 或按固定条数 chunk，上报 chunk_seq + final_marker；TabletServer 幂等落
  盘，全部 chunk 完成后再 ready ack。这样能同时解决 Writer 内存、RPC 包体、WriteBatch 峰值。
- LakeDv 用增量快照或 COW。 只快照“自上轮 split 以来 dirty 的文件”，或让 snapshotBitmap 复用不可变 bitmap 引用，避免每轮
  deep copy 全量 RoaringBitmap。
- 给外部 compaction / 恢复补 side metadata。 最理想的是把 RowId -> position 的 sidecar/index 随 tiering 产出，或要求外部
  compaction 产出等价映射；否则这两条路径都会长期是 O(重写行数)。
- 缩短 union read 临界区。 可以先拿版本号/指针，再读不可变快照；或者维护一份只读 DV 视图，让 union read 不必在锁内逐文件从
  RocksDB 组装 bitmap。
- 小优化：file_id 应按文件解析一次，不要按行解析。 现在 positionReport 已经是 Map<file_path, List<...>>，但文档写的是“对每
  个 row 去 FileDict 查/建 file_id”，这是纯额外开销。见 design-doc/fluss-deletion-vector-design-v2.md:539, design-doc/
  fluss-deletion-vector-design-v2.md:551

我最建议优先改的 3 处

- 先改 positionReport 的 分块协议；
- 再改 dvLock 的 锁粒度/锁范围；
- 最后补 LakeDv 增量快照，不再每轮全量拷贝。