# FIP-28 Historical Partition Write Sender Retry 流程

## 1. 文档目的

本文整理当前 historical partition write 在 client Sender 中的发送、失败、metadata 刷新、
reroute 和重试流程。重点回答以下问题：

- 一个 batch 为什么可能先发往 original partition，随后改发 historical partition；
- RPC 失败后 batch 如何重新进入原队列；
- `unknownLeaderTables` 为什么保存 RPC actual target path；
- original target `O` 如何切换到 historical target `H`；
- reroute 时为什么需要等待旧 in-flight batch，并重新分配 sequence；
- retry budget、metadata retry 与 `flush()` 分别如何工作。

本文描述当前代码，不描述早期“abort batch 后迁移到新 queue”的方案。

## 2. 术语与核心状态

文中使用以下符号：

- `O`：record 原本所属的 original partition，例如 `dt=20240101`；
- `H`：当前 historical system partition，例如 `dt=__historical__`；
- original queue：`RecordAccumulator.writeBatches` 中以 original `PhysicalTablePath` 为 key
  的 queue；
- actual target：RPC 实际发送到的 `TableBucket`，可能是 `O` 或 `H`。
- reroute barrier：queue 切换 actual target 前的顺序保护。它暂停该 queue 的 drain，等待所有
  已经分配旧 target sequence 的 batch 回到原 deque；随后清除旧 sequence，再发布新 target，
  防止较新的 batch 先发往新 target。

一个 historical batch 同时保留以下几类身份：

| 状态 | 含义 | normal write | historical write |
| --- | --- | --- | --- |
| `WriteBatch.physicalTablePath()` | accumulator queue 的 key | normal path | original path `O` |
| `ReadyWriteBatch.targetPhysicalTablePath()` | 本次 RPC 实际 target 的 metadata path | normal path | historical path `H` |
| `ReadyWriteBatch.tableBucket()` | 本次 RPC 实际使用的 table/partition/bucket ID | `O/bucket` | `H/bucket` |
| `WriteBatch.originalPartitionName` | 标记 historical context | `null` | original partition name |

queue 始终留在 original path 下。Reroute 只修改 actual target state，不迁移 batch，不替换
callback/future，也不重新 append record。

对于 PK/KV write，`originalPartitionName` 会进入 PutKv RPC，用于区分 historical KV 中不同
original partition 的 key space。对于 log write，ProduceLog RPC 不携带该字段，但 client batch
仍保存它，用于识别 historical retry 和刷新正确的 metadata target。

## 3. 总体流程

```text
WriterClient.doSend(record)
        |
        v
DynamicPartitionCreator.resolveWriteTarget(O)
        |
        +-- O 存在 ------------------------------> append 为 normal batch
        |
        +-- O 缺失且符合 historical 条件 --------> resolve/create H
                                                     |
                                                     v
                                              append 到 original queue
                                              queue actual target = H
                                                     |
                                                     v
RecordAccumulator.ready(cluster)
        |
        +-- target leader 已知 --------------------> drain
        |
        +-- target leader 未知 --------------------> metadata refresh
                                                     |
                                                     v
Sender.sendWriteRequest()
        |
        +-- success -------------------------------> complete batch
        |
        +-- retriable RPC error -------------------> re-enqueue
        |
        +-- invalid metadata ----------------------> re-enqueue + invalidate target metadata
        |
        +-- non-retriable / retries exhausted -----> fail batch
```

此外存在第二条进入 historical routing 的路径：WriterClient append 时使用了旧 metadata，认为
`O` 仍存在；batch 发往 `O` 后，server 才返回 `UnknownTableOrBucket`。Sender 随后确认 `O`
已经不存在，并将同一个 original queue 原地切换到 `H`。

## 4. Historical write 的两个入口

### 4.1 Append 前已经确认 O 不存在

`WriterClient.doSend()` 在 bucket assignment 前调用：

```java
dynamicPartitionCreator.resolveWriteTarget(originalPath, tableInfo)
```

解析过程为：

1. 先查 metadata cache 中是否存在 `O`；
2. cache miss 时强制刷新一次 `O`；
3. 刷新后仍不存在，才检查该 partition 是否符合 historical write 条件；
4. 符合条件时解析或创建 `H`，取得 historical partition ID；
5. bucket assignment 使用 `H` 的 metadata；
6. record 仍以 original path 进入 accumulator；
7. `RecordAccumulator.rerouteToHistorical()` 记录待切换的 `H`；如果没有旧 target 的
   in-flight batch，则立即完成切换。

这条路径中的 resolve/create 在调用 `accumulator.append()` 前完成。如果 resolve 失败，record
还没有进入 accumulator，由当前 `send()` 调用直接收到异常，不进入 Sender RPC retry。

### 4.2 Batch 已发往 O，随后发现 O 不存在

该路径发生在 append 使用的 metadata 与 server 当前状态不一致时：

```text
append 时：client cache 中 O 存在
发送时：   O 已被 retention 删除
```

完整流程如下：

1. batch 从 original queue drain，actual target 为 `O`；
2. server 返回 `UnknownTableOrBucket` 或对应的 invalid metadata error；
3. `handleWriteBatchException()` 判断该错误可重试；
4. `reEnqueueBatch()` 将 batch 放回原 deque；
5. batch 此时仍是 normal context，因此只失效 `O` 的 bucket metadata；
6. 下一轮 `ready()` 找不到 `O` 的 leader，把 `O` 加入 `unknownLeaderTables`；
7. `sendWriteData()` 强制刷新 `O`，收到 `PartitionNotExistException`；
8. Sender 失效 `O` 的 bucket 和 partition-ID metadata；
9. `rerouteBatches(originalPath)` 重新调用 `resolveWriteTarget()`；
10. 如果 `O` 已经重新出现，resolver 返回 normal target，queue 下一轮恢复 normal write；否则
    resolver 解析或创建 `H`；
11. `RecordAccumulator.rerouteToHistorical()` 记录待切换的 historical target；
12. barrier 完成后，queue 的 actual target 从 `O` 切换为 `H`；
13. 下一轮 drain 按 `H` 重新分配 sequence 并发送。

这里的 barrier 由 `pendingHistoricalTarget` 状态和
`tryCompleteHistoricalReroute()` 的检查共同实现。只要还有已经发往 `O`、尚未回到原 deque
的 batch，整个 queue 就不会继续 drain。“barrier 完成”表示这些旧 attempt 已全部回到 deque
并恢复原顺序；此时可以清除它们在 `O` 下的 sequence，再让 queue 开始向 `H` 发送。

## 5. RPC response 的统一分类

Normal 和 historical request 共用 `Sender.handleWriteBatchException()`。判断顺序如下。

### 5.1 Duplicate sequence

```java
error == DUPLICATE_SEQUENCE_EXCEPTION
```

沿用现有 writer 语义，将 batch 按成功完成。

### 5.2 Out-of-order，但本地已确认提交

开启 idempotence 且该 sequence 不大于 `lastAckedBatchSequence` 时，说明 server 可能已经写入，
只是成功 response 丢失。Sender 将 batch 按成功完成，避免无限重试。

### 5.3 可重试错误

`canRetry()` 要求同时满足：

```text
batch.attempts < configured retries
batch 尚未完成
错误属于 RetriableException，或 IdempotenceManager 明确允许重试
```

满足条件后：

1. 根据错误类型设置可选 backoff；
2. 将 batch 重新放回 deque；
3. 从 Sender 的当前 in-flight map 中移除本次 attempt；
4. 如果是 invalid metadata，再失效对应 actual target metadata；
5. 后续由正常 `ready()`、metadata refresh 和 drain 流程再次发送。

`WriteBatch.reEnqueued()` 每次执行时将 `attempts` 加一。因此 RPC response 驱动的 retry 受
`client.writer.retries` 限制。

开启 idempotence 时还要求 batch 的 writer ID 等于当前 writer ID。writer ID 已经切换时，
Sender 不再 re-enqueue 该旧 batch，而是以 `UnknownWriterIdException` 将其失败。

### 5.4 不可重试或 retry budget 耗尽

Sender 调用 `failBatch()`：

- 完成 batch future 和用户 callback；
- 更新 idempotence state；
- 从 Sender in-flight map 中移除 batch；
- 归还 accumulator buffer。

## 6. Historical target 上的几类 retry

### 6.1 普通 retriable error

例如暂时性的 server 或 RPC failure，但错误不属于 `InvalidMetadataException`：

1. batch 重新进入 original deque；
2. actual target 仍是当前 `H`；
3. metadata cache 不失效；
4. 后续 drain 使用原 sequence 重试同一个 `H/bucket`。

重试已有 sequence 的 batch 时不会重新分配 sequence，因为前一次 attempt 可能已经在 server
执行成功。复用同一个 sequence 才能利用 server idempotence 去重。

### 6.2 Historical KV throttling

Historical KV write 收到 `HistoricalPartitionThrottledException` 时，Sender 使用指数 backoff：

```text
initial = 100 ms
multiplier = 2
max = 5000 ms
jitter = 0.2
```

backoff deadline 保存在 batch 的 `nextRetryTimeMs`。`RecordAccumulator.ready()` 看到 deque
头部 batch 仍处于 backoff 时，不把该 bucket 标记为 ready，并使用剩余 backoff 更新下一次
ready check delay。

当前特殊 backoff 条件要求 batch 是 historical `KvWriteBatch`。Historical log write 即使收到
retriable error，也只走普通 re-enqueue，不应用这段 KV throttle backoff。

### 6.3 H 的 leader 变化，但 partition ID 不变

假设 batch 已经发送到 `H`，随后收到 `NotLeader`、`LeaderNotAvailable` 等
`InvalidMetadataException`：

1. batch 重新进入 original deque；
2. `ReadyWriteBatch.targetPhysicalTablePath()` 指向 historical path；
3. Sender 失效 historical target 的 bucket metadata，保留其稳定的 partition ID；
4. 下一轮 `ready()` 仍用 original path 定位 queue，但把 historical path `H` 放入
   `unknownLeaderTables`；
5. metadata refresh 得到 `H` 的新 leader；
6. 下一轮 `ready()` 找到新 leader，batch 使用原 `H` sequence 重试。

## 7. `unknownLeaderTables` 的 path 语义

`ReadyCheckResult.unknownLeaderTables` 保存需要刷新 leader metadata 的 RPC actual target path。
Normal write 的 queue path 和 actual target 相同，因此保存 `O`：

```text
normal queue path = O
actual target     = O
unknownLeaderTables = {O}
```

Historical write 的 queue 仍以 `O` 为 key，但 actual target 是 `H`，因此保存 `H`：

```text
historical queue path = O
actual target         = H
unknownLeaderTables   = {H}
```

Sender 直接使用该集合刷新 metadata。多个 original queues 共享同一个 `H` 时，Set 会把 `H`
去重，只发送一次 metadata refresh。

当前 metadata update 是批量请求，异常本身不标明集合中的哪一个 partition 不存在。因此只要
批量刷新返回 `PartitionNotExistException`，Sender 会失效集合中的 actual targets，并尝试对同名
queue 执行 reroute。Normal target `O` 同时也是 queue key，因此仍可完成 `O -> H`；historical
target `H` 不是 original queue key，reroute 会直接返回。当前实现不支持删除或重建 historical
system partition。

## 8. Re-enqueue 与顺序恢复

### 8.1 Normal fast path

一个 queue 从未进入 historical routing 时，`reEnqueue()` 只获取 deque lock，沿用 normal
writer 的逻辑：

- idempotence 开启：按旧 sequence 插回正确位置；
- idempotence 关闭：放到 deque 头部。

为避免“第一次检查后立即开始 reroute”的竞态，代码在获取 deque lock 后再次检查
`pendingHistoricalTarget` 和 `originalPartitionName`。只有两次检查都确认没有 historical
routing，才使用 normal fast path。

### 8.2 Historical routing path

queue 已经开始或完成 historical routing 时，`reEnqueue()` 使用
`BucketAndWriteBatches -> deque` 的锁顺序，使 batch 回队列和 target 切换互斥。

如果 response 对应的 partition ID 与 queue 当前 actual target ID 不同，说明这是一个较晚
返回的旧 target attempt。例如 queue 已经从 `O` 切到 `H`，但该 batch 的 response 仍来自
`O`。此时会：

1. 从旧 target 的 idempotence entry 移除 batch；
2. 清除旧 writer ID 和 sequence；
3. 写入 `originalPartitionName`；
4. 将 batch 放回 original deque；
5. 等待下一次 drain 从当前 target 重新分配 sequence。

## 9. Reroute barrier 与 sequence 重新分配

`tryCompleteHistoricalReroute()` 负责完成 `O -> H` 的 target switch。

假设同一 original partition、同一 bucket 中：

```text
b0 在 b1 之前 append
O 上分配：b0.sequence = 3，b1.sequence = 4
```

即使 RPC response 按 `b1 -> b0` 的顺序返回，idempotent re-enqueue 也会根据旧 sequence 恢复：

```text
deque = [b0(sequence=3), b1(sequence=4)]
```

Barrier 的完成步骤为：

1. `pendingHistoricalTarget` 保存 `H`，但此时不发布 `H` 的 partition ID；
2. `bucketReady()` 在 barrier 未完成时隐藏整条 queue；
3. 对每个 bucket 检查旧 actual `TableBucket` 的 idempotence entry；
4. 只要仍有属于当前 original path、但尚未回到当前 deque 的 batch，就返回 `false`；
5. 所有旧 attempt 回队后，按 deque 当前顺序遍历 batch；
6. 从旧 target entry 移除旧 sequence，并执行
   `resetWriterState(NO_WRITER_ID, NO_BATCH_SEQUENCE)`；
7. 发布 historical target ID 和 path；
8. 下一次 drain 从 deque 头部依次取 batch；
9. 使用新 actual `TableBucket` 的 `nextSequence` 重新分配。

因此根据上述步骤：

```text
reroute 前：b0.sequence(O) < b1.sequence(O)
deque 顺序：[b0, b1]
reroute 后：b0.sequence(H) < b1.sequence(H)
```

例如 `H/bucket` 当前 `nextSequence = 10`：

```text
b0 -> H.sequence = 10
b1 -> H.sequence = 11
```

多个 original queues 可以映射到同一个 historical `TableBucket`。它们保留各自 deque，但
`IdempotenceManager` 以 actual `TableBucket` 为 key，因此共享同一个 sequence state。不同
original partition 之间没有顺序要求；每个 original partition、每个 bucket 内部的 deque
顺序保持不变。

## 10. Request grouping 对 retry correlation 的影响

多个 original queues 可能同时映射到同一个 `H/bucket`。PutKv 和 ProduceLog response 都使用
`TableBucket` 关联 request batch。如果一个 RPC 中出现两个相同 `TableBucket`，client
correlation map 会覆盖其中一个 batch。

Sender 因此在发送前拆分 request：

- 一个 request group 内同一个 `TableBucket` 最多出现一次；
- PutKv 还会把 normal 与 historical batch 分到不同 group；
- group 保持 drain 输入顺序。

这样 retry response 能精确找到对应的 `ReadyWriteBatch`，不会因为多个 original queues 共享
historical bucket 而丢失 callback。

## 11. RPC retry budget 与 metadata retry 的区别

### 11.1 RPC retry

RPC response 进入 `reEnqueueBatch()` 时：

```java
batch.reEnqueued();
```

该调用增加 `attempts`，因此受 `client.writer.retries` 限制。

### 11.2 Metadata/resolve retry

`sendWriteData()` 在 metadata refresh 确认 target path 不存在后调用 `rerouteBatches()`。
当前 `rerouteBatches()` 不单独捕获异常：

```text
resolver 抛异常
    -> sendWriteData() 抛出
    -> Sender.run() 记录日志
    -> 下一轮 runOnce() 再次执行 ready/metadata/reroute
```

这个阶段没有新的 RPC response，也没有再次调用 `reEnqueueBatch()`，所以不会增加 batch
`attempts`。其语义与现有 normal path 的 metadata 恢复一致：暂时性错误恢复后继续发送；永久性
错误可能持续重试。

这意味着 `client.writer.retries` 只限制 write RPC attempts，不限制 metadata/target resolve
循环。

## 12. Flush 语义

`flush()` 等待 accumulator 中所有 incomplete batch 的 request future。Reroute 过程中：

- batch 没有迁移到新 queue；
- callback 和 request future 没有替换；
- buffer 没有提前归还；
- barrier 未完成时 batch 仍属于 incomplete 集合。

因此 successful retry 最终会完成原 future，`flush()` 会等待 historical write 真正写入或由
RPC failure path 最终失败。

需要同时注意 metadata/resolve retry 的当前语义：`rerouteBatches()` 的异常交给 Sender 主循环
继续尝试，不会主动 fail batch。如果错误永久存在，batch 会一直留在 incomplete 集合，
`flush()` 也会持续等待。该行为与当前 normal metadata recovery 保持一致。

## 13. Idempotence 关闭时的边界

上述严格顺序恢复依赖旧 sequence 和 idempotence in-flight tracking。关闭 idempotence 后：

- re-enqueue 没有旧 sequence 可用于恢复乱序 response；
- reroute barrier 不等待旧 target 的 sequenced in-flight batch；
- 多个 in-flight retry 可能出现新 batch 先到 `H`、旧 batch 后到 `H` 的情况。

因此当前保证可以准确表述为：默认开启 idempotence 时，同一 original partition、同一 bucket
内的顺序通过旧 sequence、deque 和 reroute barrier 保持；关闭 idempotence 后，retry 顺序
与现有 normal writer 一样不提供同等级保证。

## 14. 关键方法索引

| 类 | 方法 | 职责 |
| --- | --- | --- |
| `WriterClient` | `doSend()` | append 前解析 initial target，选择 bucket assigner |
| `DynamicPartitionCreator` | `resolveWriteTarget()` | 判断 O 是否存在，解析 normal 或 historical target |
| `HistoricalPartitionResolver` | `resolveHistoricalPartitionId()` | 查询、刷新或创建 H，并返回 partition ID |
| `Sender` | `sendWriteData()` | ready、metadata refresh、reroute、drain 和 send |
| `Sender` | `handleWriteBatchException()` | response 分类、retry、metadata invalidation、fail |
| `Sender` | `rerouteBatches()` | 根据 original path 重新解析 historical target，并交给 accumulator 记录为待切换目标 |
| `RecordAccumulator` | `reEnqueue()` | batch 回原 deque，恢复旧 sequence 顺序 |
| `RecordAccumulator` | `rerouteToHistorical()` | 记录 pending target，尝试完成 barrier |
| `RecordAccumulator` | `tryCompleteHistoricalReroute()` | 等待旧 attempt、清除旧 sequence、发布新 target |
| `RecordAccumulator` | `bucketReady()` | 隐藏 pending queue，区分 queue path 与 metadata target |
| `IdempotenceManager` | `nextSequence()` | 按 actual `TableBucket` 统一分配 sequence |

## 15. 快速排查指南

### Batch 一直没有再次发送

依次检查：

1. deque 头部是否仍处于 `nextRetryTimeMs` backoff；
2. `pendingHistoricalTarget` 是否仍在等待旧 target in-flight batch；
3. `unknownLeaderTables` 是否保存 actual target path；
4. resolver 是否持续在 metadata/create 阶段抛异常。

### Batch 收到 duplicate，但数据似乎没有写入

检查 reroute 前是否执行了：

```text
removeInFlightBatch(old TableBucket)
resetWriterState(NO_WRITER_ID, NO_BATCH_SEQUENCE)
```

如果直接把 O sequence 带到 H，H 可能将该 batch 误判为 duplicate。

### Flush 持续等待

区分两种情况：

- write RPC 持续失败：检查 `attempts`、`client.writer.retries` 和最终 `failBatch()`；
- metadata/resolve 持续失败：该阶段不增加 `attempts`，当前行为是由 Sender 主循环持续重试。

## 16. 一句话模型

Historical retry 始终保留 original queue；Sender 使用 actual target path 刷新 metadata，使用
original path 找回 queue，等旧 target attempt 按旧 sequence 全部归队后，再按同一 deque
顺序为新 historical `TableBucket` 分配 sequence 并重发。
