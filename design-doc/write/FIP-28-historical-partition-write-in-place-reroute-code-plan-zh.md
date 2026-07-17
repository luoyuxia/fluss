# FIP-28 Historical Write 原地重路由代码计划

## 1. 目标

处理下面的竞态：record 已进入 original partition 对应的 `RecordAccumulator` queue，Sender
发送前该 partition 被 retention 删除。此时 pending batch 应改投 `__historical__`，同时保持：

1. 同一个 original partition、同一个 bucket 内的 batch 顺序不变；
2. 不通过 abort callback 复制并重新 append record；
3. 多个 original partitions 指向同一个 historical `TableBucket` 时，共享 writer id、batch
   sequence、in-flight 和 ACK 状态；
4. normal write 的现有 queue 和 drain 行为不变。

## 2. 最小实现

不为 accumulator 增加 logical queue、logical table path 或新的 route 对象；复用
`DynamicPartitionCreator` 已返回的 `ResolvedWriteTarget`。

`RecordAccumulator.writeBatches` 继续以 record 原来的 `PhysicalTablePath` 为 key。每个
`BucketAndWriteBatches` 已经保存 nullable `partitionId`，重路由只修改：

```text
BucketAndWriteBatches.partitionId = historicalPartitionId
BucketAndWriteBatches.originalPartitionName = originalPartitionName（PK only）
KvWriteBatch.originalPartitionName = originalPartitionName
```

deque 不换 key，不移动 batch，不改变 callback、request future 和 encoded records。
queue 上保存 original name 是为了让与 reroute 并发、但已经完成 normal resolve 的 PK append
补上同样的 RPC context；log write 不使用该字段。

## 3. 调用流程

### 3.1 append 前已经解析为 historical

`WriterClient` 仍在 bucket assignment 前解析 historical partition：

1. 使用 historical physical path 创建或复用 `BucketAssigner`；
2. record 中保留 original physical path；
3. historical PK record 补充 `originalPartitionName`；
4. 按原有签名调用 `RecordAccumulator.append()`；
5. append 完成后，accumulator 使用 original physical path 找到原 queue，原地设置其
   partition id。

log write 不需要 `originalPartitionName`，只更新 partition id。

### 3.2 append 后 original partition 消失

Sender 的 metadata batch 请求返回 `PartitionNotExistException` 后：

1. 对 batch 中的各 path 调用 `DynamicPartitionCreator` 重新解析；该方法内部会用 singleton
   metadata 请求确认 partition 是否确实缺失；
2. historical resolver 刷新或创建 historical partition，拿到 partition id；
3. accumulator 原地设置 historical partition id；
4. 对 queue 中的 KV batches 补充 original partition name；
5. resolver 临时失败时保留 batch 等待下一轮，只有不可重试异常才 abort 该 path。

## 4. Drain 与 idempotence

cluster 中可见的是 historical physical bucket，而 accumulator queue 的 key 仍是 original
path。Drain 遍历实际 bucket metadata 时，用下面的条件找到对应的原 queues：

```text
tablePath 相同
BucketAndWriteBatches.partitionId == TableBucket.partitionId
bucketId 相同
```

每个匹配项继续调用原有的 `getReadyDeque(originalPath, bucketId)`，所以不需要新增 deque
选择器，也不改变单个 original partition queue 内的顺序。多个 original partitions 可以在
同一轮 drain 中使用同一个 historical `TableBucket`。

sequence 统一分配按现有定义推导：

1. drain 使用 historical `tableId + partitionId + bucketId` 构造 `TableBucket`；
2. 指向同一 historical bucket 的 queues 得到相等的 `TableBucket`；
3. `IdempotenceManager` 以 `TableBucket` 为 key 保存状态；
4. 因此这些 queues 调用同一个 `nextSequence(tableBucket)`，sequence 不会分别计数。

## 5. 并发边界

Sender 只在 original partition 没有 leader、metadata 更新确认其不存在时执行 reroute，此时
对应 batch 仍在 deque 中。Append 不获取 queue state lock，只在对应 deque lock 内读取
volatile `originalPartitionName`，判断这条 record 是否需要按 historical write append。
Reroute 先发布 original partition name，再逐个持有 deque lock 更新 pending batches：

1. append 先获得 deque lock 时，它可能仍按 normal record append，随后 reroute 在同一把
   deque lock 内将该 batch 补成 historical context；
2. reroute 先发布 original partition name 时，后续 append 会在 deque lock 内读到它，
   直接按 historical record append；
3. reroute 在所有 deque 更新完成后才发布 historical partition id，Sender 不会提前将
   缺少 historical context 的 batch drain 到 historical bucket。

因此不需要迁移 batch，也不会在并发 append 时产生一个缺少 historical RPC context 的新
batch。

## 6. 代码改动

- `WriterClient`
  - historical record 保留 original physical path；
  - 沿用现有 append 签名，append 后原地更新 queue 的 partition id。
- `RecordAccumulator`
  - 原地更新 `BucketAndWriteBatches.partitionId`；
  - drain 根据最终 partition id 选择现有 deque；
  - 不增加 logical queue 或 logical path 建模。
- `Sender`
  - 确认 original partition 缺失后重新解析并更新 accumulator；
  - metadata invalidation 仍使用实际发送的 historical physical path。
- `KvWriteBatch`
  - 允许 pending normal batch补充 `originalPartitionName`。
- `ReadyWriteBatch`
  - 保存当前发送使用的 physical path，供 metadata invalidation 使用。

## 7. 测试

- pending batch reroute 后仍保留 original path 对应的 queue；
- 与 reroute 并发的旧 metadata append 继承 historical context；
- 两个 original partition queues 指向同一 historical `TableBucket` 时 sequence 连续；
- existing normal writer、Sender 和 DynamicPartitionCreator tests 全部通过。

验证命令：

```bash
./mvnw -o -pl fluss-client -Dfast \
  -Dtest=RecordAccumulatorTest,SenderTest,DynamicPartitionCreatorTest test
./mvnw -o -pl fluss-client -Dfast test
./mvnw -o -pl fluss-client -DskipTests spotless:check
```
