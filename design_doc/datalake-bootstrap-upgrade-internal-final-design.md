# Data Lake Bootstrap Upgrade 内部最终设计稿

## 1. 目标

本文档定义一套最小可行的内部机制，用于在创建 Fluss 表后，将已有 data lake 表的数据 bootstrap 到 Fluss 中。

第一版实现只处理一个 hold 分区，并复用现有 tiering 框架，不引入独立的 bootstrap service。

## 2. 核心思路

bootstrap 被建模为一种特殊的 `TieringTask`：

- 正常 tiering：Fluss -> Paimon
- bootstrap tiering：Paimon -> Fluss

系统只在 ZooKeeper 中保存最小化的表级 bootstrap 状态；bucket 级执行细节全部留在 `tiering service` 内部处理。

## 3. 端到端流程

1. 用户创建一张带 `table.datalake.bootstrap.*` 配置的 Fluss 表。
2. 系统创建 Fluss 表并初始化 bootstrap 元数据。
3. 系统自动触发 bootstrap upgrade。
4. 系统按默认规则推导 `holdPartition`。
5. coordinator 创建 bootstrap znode，并写入：
   - `status = IN_PROGRESS`
   - `holdPartition = ...`
6. coordinator 将该表放入 `LakeTableTieringManager`，将其视为一种特殊的 tiering：从 Paimon tiering 到 Fluss。
7. `tiering service` 通过 heartbeat 请求 `TieringTask`。
8. coordinator 从统一的 `pendingTieringTables` 中选择表，并按表状态决定返回哪一种 `TieringTask`。
9. 如果该表处于 bootstrap 生命周期，`tiering service` 读取 Paimon 数据，识别需要 bootstrap 的 bucket，并将数据转换为可被 Fluss 加载的 SST 文件。
10. `tiering service` 向 coordinator 上报转换写入成功。
11. coordinator 创建对应的 partition 和 bucket，并通知目标 Tablet Server 创建 bucket、下载 SST 文件。
12. replica 完成加载后，对应 partition / bucket 即可开始支持写入。
13. coordinator 将 bootstrap 状态更新为 `COMPLETE`。
14. 该表后续进入正常 tiering 生命周期：从 Fluss 读取新增数据并同步回 Paimon。

## 4. Bootstrap 状态

bootstrap 状态保存在 ZooKeeper 中：

- 路径：`/tabletservers/tables/[tableId]/bootstrap-upgrade`

状态结构如下：

```text
BootstrapUpgradeState {
  status: IN_PROGRESS | COMPLETE
  holdPartition: String
}
```

状态语义：

- `IN_PROGRESS`：该表已经进入 bootstrap 生命周期，可能处于待分配、执行中或等待重试等中间状态
- `COMPLETE`：bootstrap 已完成，后续不再调度

说明：

- 建表完成并通过 bootstrap 初始化后，znode 立即创建并写入 `IN_PROGRESS`
- `znode 不存在` 不是正常业务状态，只视为异常或未初始化完成
- 第一版不持久化 bucket 级进度

## 5. 统一任务模型

bootstrap 不再被视为独立的调度体系，而是 `TieringTask` 的一种类型。

建议的任务结构如下：

```text
TieringTask {
  tableId
  tablePath
  holdPartition
  taskType
  taskEpoch
}
```

其中：

- `taskType = NORMAL_TIERING`：正常 tiering
- `taskType = BOOTSTRAP_UPGRADE`：bootstrap tiering

说明：

- `holdPartition` 从 ZooKeeper 中的 bootstrap 状态读取，并直接放入 task payload
- `taskEpoch` 用于 fencing，避免旧任务误上报成功或失败
- 正常 tiering 与 bootstrap 共享同一套 heartbeat、dispatch、timeout 和完成回调框架

## 6. 调度设计

### 6.1 设计原则

coordinator 通过统一调度入口调度所有 `TieringTask`。

不同类型的 `TieringTask` 都从统一的待调度表集合中产生；bootstrap 不是单独的一套调度系统，而只是某张表当前对应的一种特殊 task type。

### 6.2 Coordinator 内部状态

coordinator 只维护统一的待调度表集合：

- `pendingTieringTables`

### 6.3 分配逻辑

伪代码如下：

```java
Task requestTask() {
    TableId tableId = pickNextTableFromPendingTieringTables();
    if (tableId == null) {
        return null;
    }

    if (isBootstrapInProgress(tableId)) {
        return buildBootstrapUpgradeTask(tableId);
    }

    return buildNormalTieringTask(tableId);
}
```

该设计具有以下特性：

- 所有 tiering 工作共用一个调度入口
- bootstrap 被视为特殊的 tiering task type，而不是独立队列
- task type 由表状态决定，而不是由两套队列分别决定

## 7. Coordinator 职责

### 7.1 初始化 bootstrap

建表完成并通过 bootstrap 校验后，coordinator：

1. 计算 `holdPartition`
2. 创建 bootstrap znode，并写入 `IN_PROGRESS`
3. 将该表加入 `pendingTieringTables`

### 7.2 分配任务

当 coordinator 从 `pendingTieringTables` 中选中一张表时：

1. 读取该表的 bootstrap znode（如果存在）
2. 若 bootstrap 状态为 `IN_PROGRESS`，则返回 `taskType = BOOTSTRAP_UPGRADE` 的 `TieringTask`
3. 若 bootstrap 状态为 `COMPLETE`，则返回该表的正常 tiering task
4. 若不存在 bootstrap znode，则返回该表的正常 tiering task

### 7.3 成功处理

当 `tiering service` 上报 bootstrap 成功时，coordinator：

1. 校验 `taskEpoch`
2. 将 znode 状态更新为 `COMPLETE`
3. 将该表切换到正常 tiering 生命周期

### 7.4 失败或超时处理

当 bootstrap 失败或超时时，coordinator：

1. 校验 `taskEpoch`（如果适用）
2. 保持 znode 状态为 `IN_PROGRESS`
3. 将该表重新放回 `pendingTieringTables`

这样做的好处是：

- 不需要在 ZooKeeper 中引入 `FAILED`、`PENDING` 等额外状态
- 恢复语义简单
- bucket 级恢复复杂度被推迟到未来版本

## 8. Tiering Service 职责

当 `tiering service` 收到一个 `taskType = BOOTSTRAP_UPGRADE` 的 `TieringTask` 时：

1. 从 task payload 中读取 `holdPartition`
2. 识别该 hold 分区下需要 bootstrap 的 bucket
3. 从 Paimon 读取源数据
4. 将源数据转换为 Fluss 可加载的 SST 文件
5. 持久化 SST / Imported Base 产物
6. 向 coordinator 上报转换写入成功
7. 持续 heartbeat，直到该任务最终完成

第一版中，`tiering service` 不在 cluster 元数据中持久化 bucket 级 bootstrap 进度。

## 9. Partition 与 Bucket 激活流程

当转换成功后，coordinator 驱动 Fluss 侧接入流程：

1. 创建目标 partition
2. 创建并分配目标 bucket / replica
3. 通知对应 Tablet Server 下载并加载 SST 文件
4. replica 加载完成后，对应 partition / bucket 开始支持写入

这一过程必须仍然经过 coordinator；bootstrap 不会绕过 Fluss 现有的 partition / replica 管理流程。

## 10. 恢复与幂等性

第一版采用粗粒度恢复策略。

基本假设：

- 同一个 hold 分区的 bootstrap 可能被重复执行
- Imported Base / SST 生成需要支持安全重试
- 旧任务的完成回调必须通过 `taskEpoch` 做 fencing

恢复模型：

- ZooKeeper 中的状态在 bootstrap 完成前始终保持 `IN_PROGRESS`
- 失败或超时后，通过重新调度另一个 `BOOTSTRAP_UPGRADE` 任务继续处理
- 第一版不持久化 bucket 级 checkpoint

## 11. 范围与非目标

第一版包含：

- 建表后自动触发 bootstrap
- 只处理一个 hold 分区
- 只记录 ZooKeeper 表级状态
- 统一的 `TieringTask` 调度模型
- bootstrap 完成后自动进入正常 tiering 生命周期

第一版不包含：

- 多目标分区 bootstrap
- bucket 级进度持久化
- 分区内断点续跑
- 增量追平 bootstrap
- 独立的 bootstrap 执行服务

## 12. 后续演进方向

后续可以逐步扩展：

- 支持多个目标分区
- 持久化 bucket 级 bootstrap 进度
- 支持分区内可恢复执行
- 增加更丰富的可观测字段
- 引入更复杂的资源隔离与调度策略

## 13. 最终结论

bootstrap 是一种特殊类型的 `TieringTask`，由现有 tiering 框架统一调度与执行。

ZooKeeper 中只保留最小化的表级 bootstrap 状态：

- `status = IN_PROGRESS | COMPLETE`
- `holdPartition`

`pendingTieringTables` 是唯一的统一待调度表集合。coordinator 根据表状态决定返回 `BOOTSTRAP_UPGRADE` 还是正常 tiering task；`tiering service` 负责把 Paimon 数据转换成 SST 文件，coordinator 负责驱动 Fluss 的 partition / bucket / replica 接入流程。bootstrap 完成后，该表进入正常的 Fluss -> Paimon tiering 生命周期。
