# FIP-28 历史分区写 PR 6 Review 记录

## 1. 记录范围

本文记录 PR 6（Enable End-to-End Historical Write）在实现和多轮 review 中发现的主要问题、最终采用的解决方案，以及明确决定暂不处理的风险。重点是客户端从已删除的原分区切换到 `__historical__` 分区时的路由、顺序、幂等和 metadata 恢复语义。

文中使用以下符号：

- `O`：已经过期并被删除的原分区（original partition）。
- `H`：承载历史写入的 `__historical__` 系统分区。
- original queue：以原 `PhysicalTablePath` 为 key 的 accumulator queue。
- actual target：请求实际发送到的 `TableBucket`，可能从 `O` 切换为 `H`。

最终实现保留 original queue，不迁移 `WriteBatch` 到另一套 queue。queue 的逻辑身份继续表示“原分区 + bucket”，actual target 则由 queue 中独立的 historical target metadata 表示。

## 2. 最终需要满足的语义

1. 相同 original partition、相同 bucket 的 batch 顺序必须保持不变。
2. 不同 original partition 可以使用不同 queue，即使它们最终映射到同一个 historical `TableBucket`。
3. writer ID 沿用当前 writer 的全局状态，batch sequence 则始终按请求实际发送到的 `TableBucket` 分配。多个 original queue 共享同一个 `H/bucket` 时，也共享该 actual target 的 sequence 状态。
4. 从 `O` 切换到 `H` 前，必须等待已经使用 `O` sequence 的旧 attempt 返回，避免新 historical batch 超车。
5. reroute 不完成、不丢弃用户 batch。`flush()` 仍通过 incomplete batches 等待 normal 和 historical write 的最终结果。
6. historical KV RPC 携带 `originalPartitionName`，用于区分同一个 historical KV 中不同原分区的 key space。ProduceLog RPC 不增加该字段；log record 自身已有原分区列，客户端保存的 `originalPartitionName` 只用于 reroute 和 metadata 管理。

## 3. 已修复的问题

### 3.1 Abort 后迁移到新 queue 会破坏原分区内顺序

**问题**

最初考虑在 `O` 不存在时 abort pending batch，再将重试数据放入 historical queue。假设同一 original partition、同一 bucket 中有 `b1` 和 `b2`，如果 `b1` 因 `O` 不存在而被迁移，而 `b2` 已经可以从另一条发送路径继续执行，两者可能在 `H` 上反序。abort 还会改变 callback、buffer 和 incomplete batch 的生命周期。

**解决方案**

采用原地 reroute：

- `RecordAccumulator.writeBatches` 仍按 original `PhysicalTablePath` 保存 queue。
- batch 不迁移到另一条 queue，只修改 actual partition ID，并补上 `originalPartitionName`。
- `reroutedWriteBatches` 只为 drain 补充 original queue 到 historical leader 的映射。
- `flush()` 继续等待原来的 batch future，不需要额外拼接 source/target queue 的完成条件。

主要代码：`RecordAccumulator.rerouteToHistorical()`、`addReroutedBucketsInCurrentNode()`。

### 3.2 已分配的 O sequence 被直接复用到 H，可能静默丢数据

**问题**

batch 在 `O` 上获得 writer ID 和 sequence 后，如果仅修改 partition ID 就发送到 `H`，`H` 可能已经使用过相同 writer ID/sequence。server 会把新写入判断为 duplicate；client 对 duplicate sequence 按成功处理，从而出现数据没有写入但 callback 成功的情况。即使 `H` 为空，client 端 `H` 的 next sequence 也没有随该 batch 正确推进，后续 batch 仍可能重复使用 sequence。

**解决方案**

reroute 一个带 sequence 的 batch 时：

1. 从旧 actual `TableBucket` 的 `IdempotenceBucketEntry` 移除该 batch。
2. 将 batch 的 writer ID 和 sequence 重置为未分配状态。
3. 保持 batch 在原 deque 中。
4. 下一次按 `H` drain 时，使用 `H/bucket` 的幂等状态重新分配 sequence 并登记 in-flight batch。

主要代码：`RecordAccumulator.rerouteBatchToHistorical()`、`IdempotenceManager.removeInFlightBatch()`、各类 `WriteBatch.resetWriterState()`。

### 3.3 较晚返回的 O in-flight batch 会绕过已经完成的 reroute

**问题**

默认一个 bucket 可以有多个 in-flight request。例如 `b0` 和 `b1` 都已经发送到 `O`：

1. `b0` 先返回 `UnknownTableOrBucket`，queue 被切换到 `H`。
2. `b1` 随后返回同样的错误并重新入队。
3. 此时 queue 已经保存 `H` 的 partition ID，但 `b1` 仍带 `O` 的 sequence，并且可能没有 historical context。

如果直接重新入队，KV write 会被 server 当作 normal write 发送到 historical replica，或者复用错误的 sequence。

**解决方案**

`RecordAccumulator.reEnqueue()` 比较 response 对应的 actual `TableBucket` 和 queue 当前 partition ID。发现这是较晚返回的旧 target attempt 时，立即：

- 使用 queue 已保存的 `originalPartitionName` 标记该 batch；
- 从旧幂等 entry 移除并重置 writer state；
- 将 batch 放回同一个 original deque，等待按当前 `H` target 重新分配 sequence。

### 3.4 新 H batch 可能超越仍在 O 上的旧 in-flight batch

**问题**

如果第一个收到 `O` 不存在响应的 batch 立即把 queue 切到 `H`，其他更早或相邻的 batch 可能仍在 `O` 上 in-flight。此时 queue 中的新 batch 可以先发到 `H`，旧 batch 稍后才返回并 reroute，破坏同一 original partition、同一 bucket 的顺序。

**解决方案**

增加 reroute barrier：

- `pendingHistoricalTarget` 先保存待切换的 `H`，此时 `partitionId` 仍表示旧 actual target。
- `getReadyDeque()` 在 barrier 存在时隐藏整条 queue，禁止继续 drain。
- `tryCompleteHistoricalReroute()` 检查该 original queue 在旧 actual `TableBucket` 下登记过 sequence 的 batch 是否都已返回 deque。
- response 乱序返回时，`reEnqueue()` 先按旧 sequence 恢复 deque 顺序。
- 所有旧 attempt 返回后，统一清除旧 sequence，再一次性发布 `H` 的 partition ID。

这样 `H` 上重新分配 sequence 的顺序与 `O` 上原来的逻辑顺序一致。

### 3.5 多个 original queue 共享 H 时，barrier 会互相等待

**问题**

两个原分区可以映射到同一个 historical `TableBucket`：

```text
Q1 -> H/bucket-0 -> b1
Q2 -> H/bucket-0 -> b2
```

幂等 entry 按 actual `TableBucket` 统一管理，因此 entry 中同时包含 `b1` 和 `b2`。如果 Q1 的 barrier 要求 entry 中所有 batch 都必须位于 Q1 deque，Q1 会等待 Q2 的 `b2`；Q2 同样会等待 Q1 的 `b1`，形成固定死锁。

**解决方案**

幂等状态仍按 actual `TableBucket` 统一分配 sequence，但 barrier 检查增加 original physical path 过滤：

- Q1 只等待 entry 中属于 Q1 的 batch 返回 Q1 deque。
- Q2 只等待属于 Q2 的 batch。
- 两条 queue 切换到新 target 后，仍由同一个 `H/bucket` entry 分配全局不重复的 sequence。

主要代码：`IdempotenceBucketEntry.hasInflightBatchNotIn()`、`IdempotenceManager.hasInflightBatchNotIn()`。

### 3.6 ProduceLog request 中重复 TableBucket 会覆盖 batch

**问题**

两个 original log partition 可能映射到相同的 `H/bucket`。如果它们进入同一个 ProduceLog RPC：

- client 的 response correlation map 以 `TableBucket` 为 key，前一个 batch 会被覆盖；
- server 的 `getProduceLogData()` 也以 `TableBucket` 为 key，前一份 records 会再次被覆盖；
- 被覆盖 batch 的数据不会写入，callback/future 可能无法完成，进而使 `flush()` 永久等待。

**解决方案**

- `Sender.packProduceLogRequestGroups()` 保持 drain 顺序拆分 request，保证一个 RPC 内同一个 `TableBucket` 最多出现一次。
- client 构造 response correlation map 时拒绝重复 key。
- server 的 `ServerRpcMessageUtils.getProduceLogData()` 也拒绝包含重复 `TableBucket` 的非法 request，避免静默覆盖。

PutKV 使用相同原则：normal/historical batch 分组，一个 request 内 `TableBucket` 唯一。

### 3.7 Historical target metadata 失效后可能永久等待

**问题**

historical batch 写 `H` 时收到 `NotLeader` 或其他 `InvalidMetadataException` 后，batch 回到 original queue。如果继续按 original path `O` 刷新 metadata，就无法获取 actual target `H` 的新 leader。此时不会发出 write RPC，attempts 不再增加，batch future 和 `flush()` 都可能一直等待。

**解决方案**

- `ReadyWriteBatch` 同时保存 queue 的 original path 和实际的 `targetPhysicalTablePath`。
- normal 和 historical batch 收到 invalid metadata 时，都只失效 actual target 的 bucket metadata。
- `ReadyCheckResult.unknownLeaderTables` 保存 actual target path：normal write 保存 `O`，historical write 保存 `H`。
- Sender 直接刷新 `unknownLeaderTables`，不再维护单独的 `metadataRefreshTables`。

### 3.8 Normal queue 固定第一次记录的 partition ID

**问题**

为支持 historical target，queue 内增加了独立 `partitionId`。如果 normal queue 也永久使用该字段，会引入 normal path 回归：原分区 O1 的 batch 写完后，分区被删除并以相同 path 重建为 O2，新 record 会复用旧 queue 并继续查询 O1。metadata 按 path 虽能刷新到 O2，但 queue 不更新，最终永久无法发送。

**解决方案**

- normal queue 每次执行 `bucketReady()` 时继续从当前 `Cluster` 按 path 获取最新 partition ID，保持修改前的行为。
- 只有 historical queue 保留单独解析得到的 actual target ID，因为它的 queue key 仍是 original path，不能用 original path 的 metadata 覆盖 `H`。

### 3.9 Historical routing 每条 record 都强制检查原分区 metadata

**问题**

原分区第一次被确认不存在后，如果后续每次 append 都调用 `forceCheckPartitionExist()`，批量回填会退化为接近每条 record 一次 metadata RPC。

**解决方案**

`DynamicPartitionCreator.confirmedHistoricalPartitions` 缓存已经确认需要 historical routing 的 original path：

- 首次仍强制检查 `O`，只有确认不存在且满足 historical eligibility 才缓存。
- 后续 record 跳过 `O` 的强制 metadata RPC，但仍重新 resolve historical target，以刷新失效的 `H` metadata。
- 如果 metadata cache 再次发现原分区存在，立即清除该 historical routing cache，恢复 normal write。

### 3.10 Tiering 完成并清理本地 KV 后，lake lookup 可能读旧 snapshot

**问题**

historical KV 是可恢复的本地 cache。数据 tiering 到 Paimon 后，本地 KV 会被清理；如果 `HistoricalLakeLookupManager` 继续复用 cleanup 前打开的 `LakeTableLookuper`，fallback lookup 可能看不到刚完成 tiering 的 snapshot，表现为刚写入的数据短暂不可见。

**解决方案**

- `ReplicaManager` 在确认 lake offset 已覆盖 local end、提交 historical KV cleanup 前，记录该 table 必须可见的最新 snapshot ID。
- `HistoricalLakeLookupManager` 以 table 为粒度保存最大 required snapshot ID。
- lookup 只有在 schema 一致且 cached lookuper 已满足 required snapshot 时才复用；否则惰性创建新 lookuper。
- 新 lookuper 先执行本次 lookup，成功后才替换旧 lookuper；refresh 失败时关闭新实例并保留旧实例，便于后续重试。

### 3.11 Lookup 与 write 各自维护 HistoricalPartitionResolver

**问题**

historical lookup 已经有“查找或创建 `__historical__` 分区并获得 partition ID”的逻辑。write 再维护一份会产生行为差异，尤其是并发 create、metadata refresh 和失败重试。

**解决方案**

- 将 `HistoricalPartitionResolver` 从 lookup package 移到 client metadata package。
- lookup 和 write 共用同一实现。
- 相同 table/original partition 的并发 resolve 共用一个 in-flight future；失败后移除 entry，允许下一次重新 resolve。
- 多级分区继续通过 `toHistoricalPartitionSpec()` 保留静态分区前缀。

## 4. 明确暂不处理的风险

### 4.1 Append 与 reroute 的低概率并发窗口

写线程可能先根据旧 metadata 判断为 normal，然后在进入 accumulator append 前暂停；Sender 在此期间完成 queue reroute；写线程恢复后仍可能 append 一个没有 historical context 的 record。

彻底关闭该窗口需要 append 与 reroute 共享 routing state/lock，并补确定性的 latch 并发测试，会扩大当前 PR 的并发改动范围。当前决定先不处理。现有实现已经覆盖 append 之前直接解析为 historical，以及已发送 batch 返回后的 reroute；这里保留为后续并发加固项。

### 4.2 `abortBatches(path)` 仍会覆盖同一路径下的 in-flight batch

`IncompleteBatches` 同时包含 deque 中和已经发送的 batch，`abortBatches(path)` 会对两者执行 abort。它可能使 callback、pooled buffer 与 Sender in-flight 清理的生命周期交错。

该行为与现有 normal dynamic-partition 失败路径保持一致，本 PR 不单独改变 abort 语义。historical reroute 遇到 `RetriableException` 时不会 abort，而是保留 batch 等待重试；只有非 retriable 的 resolve/reroute 失败才进入现有 abort 路径。

### 4.3 Historical routing 索引不会主动缩减

`confirmedHistoricalPartitions` 和 `reroutedWriteBatches` 会按曾经写入的 original partition 增长。`reroutedWriteBatches` 使用 `CopyOnWriteMap`，插入会复制 map；drain 也会遍历已经登记的 rerouted queues。大量、长期持续的历史分区回填可能让 Sender 热路径逐渐变慢。

当前使用场景是偶尔补写少量历史分区，因此决定不在 PR 6 增加 active queue 回收状态。后续若扩展为大规模 backfill，应在 deque 全部为空时移除 active reroute index，并在新 append 到来时重新登记。

### 4.4 Resolver 调用 Admin 时的同步异常窗口

`admin.createPartition(...)` 通常返回一个异常完成的 future，`whenComplete` 会把错误传给调用方。理论上它也可能在返回 future 之前同步抛出 `RuntimeException`；当前这次调用不在 `try` 内，会使外层 coalesced result future 无法完成。

已知实现中常见的 RPC、鉴权和服务端错误都通过 future 异步返回，同步抛出的概率较低，当前决定不为此扩大异常处理代码。若后续观察到 writer 无法完成且栈指向 Admin 调用前置校验，应将 `admin.createPartition()` 本身纳入 `try/catch` 并异常完成 result future。

### 4.5 Metadata 瞬时失败导致错误判定 O 不存在

如果强制 metadata 检查遇到瞬时异常但上层把它表现为“未找到 partition”，理论上仍可能把实际存在的 `O` 错误路由到 `H`。当前 historical routing 还要求 table 为 auto-partitioned Paimon lake table，并且 partition time 已早于 retention 边界，因此只有同时满足这些条件的旧分区才可能进入该分支。

当前选择保持实现简单，不引入额外的 metadata 结果状态。后续如果 metadata API 能明确区分“确认不存在”和“刷新暂时失败”，应只在前者设置 confirmed historical route。

### 4.6 重复执行 Paimon E2E 时 ZooKeeper metadata callback 偶发不返回

提交前验证中，`HistoricalPartitionWriteITCase` 的第一次完整执行通过。使用 `-Dtest` 同时命中 Surefire 的 unit 和 integration execution、在同一 Maven 生命周期内重复执行该用例时，第二次执行卡在首次 historical upsert 的 `flush()`：

- client Sender 等待 original partition 的 metadata RPC；
- TabletServer metadata worker 等待 ZooKeeper 批量 `getData` future；
- 缺失 partition 对应的 ZooKeeper callback 没有返回，因此 client 收不到 `PartitionNotExistException`，也无法进入正常 reroute 分支。

client metadata RPC 有 30 秒超时，超时后会继续刷新，但如果 server 侧 callback 持续缺失，pending batch 仍可能长时间等待。此前已决定不在本 PR 修改 server 的 ZooKeeper batch 请求逻辑，因此该现象没有被标记为已解决。当前确定的是标准单次 E2E 路径可以通过；是否只由嵌入式集群重复启动触发，仍需单独排查和稳定复现。

## 5. 测试覆盖

本轮增加或扩展的测试主要覆盖：

- historical partition resolver 的 cache、refresh、create、并发合并、失败重试和多级分区。
- dynamic partition creator 对 normal、historical、dynamic-create-disabled 和 confirmed historical cache 的处理。
- pending batch 原地 reroute、旧 sequence 重置、晚返回 attempt、乱序 response 恢复和 barrier。
- 多个 original queue 共享一个 historical bucket。
- normal partition O1 重建为 O2 后继续写入。
- ProduceLog/PutKV request 分组和重复 `TableBucket` 隔离。
- normal/historical invalid metadata 的不同失效目标，以及 historical leader 更新。
- required lake snapshot 的惰性 lookuper refresh 和 refresh 失败回退。
- Fluss lake table 的 expired log write，以及 Paimon historical PK update/delete 端到端路径。

提交前单次执行 `HistoricalPartitionWriteITCase` 通过；同一 Maven 生命周期内强制重复执行时的已知卡点见 4.6。

## 6. 最终实现取舍总结

最终方案以最小化 queue 结构变更为原则：original queue 保序，actual `TableBucket` 负责发送和幂等，`pendingHistoricalTarget` 只在 target 切换期间充当 barrier。这样复用了现有 append、drain、retry、incomplete batch 和 flush 生命周期，同时针对跨 target 重试增加必要的 sequence 重置与 metadata reconciliation。

保留下来的风险集中在低概率并发窗口、既有 abort 语义、大规模历史分区回填性能和重复 E2E 中观察到的 ZooKeeper metadata callback 卡点。它们没有被当作已解决问题，后续若使用规模或线上现象发生变化，可以按第 4 节的触发条件继续加固。
