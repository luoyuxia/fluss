# 基于 Data Lake Bootstrap 的 Paimon 表升级为 Fluss 表最小内部设计

## 1. 文档目标

本文档是内部实现设计稿，目标是基于用户设计文档中的术语，给出一版更极简、可落地的内部设计。

本文档采用如下口径：

- 用户通过 `CREATE TABLE ... WITH (...)` 声明表需要从 data lake bootstrap
- 建表成功后，系统自动开始升级
- 系统默认 hold 当天分区
- `tiering service` 负责把 hold 分区的数据转换为 Imported Base，并接入 Fluss

本文档重点回答三个内部实现问题：

- bootstrap 元数据如何以极简方式建模
- 如何复用现有 `tiering service` 的 heartbeat 拉任务模型
- 如何把对普通表同步的影响控制在较低水平

## 2. 范围与前提

### 2.1 范围

本设计仅覆盖以下范围：

- Paimon 表通过 Data Lake Bootstrap 接入 Fluss
- 建表成功后自动触发的数据基线导入
- hold 分区的数据转换与接入
- Imported Base 准备完成后再接入 Fluss 分区服务流程
- 复用现有 `tiering service` 作为执行器

### 2.2 前提

当前最小设计基于如下前提：

- 需要导入的是建表当天对应的目标分区
- 这个目标分区在导入期间可以视为稳定数据
- 现有 `tiering service` 已经支持通过 heartbeat 向 coordinator 拉任务
- 分区是否正式接入 Fluss，仍然由 coordinator 决定

### 2.3 非目标

本设计暂不覆盖：

- 连续追踪 data lake 新增数据的增量同步
- bucket 级更细粒度的切分与并行
- 完整资源隔离或独立 bootstrap service
- 用户可见的复杂调度和重试配置

## 3. 术语对齐

为避免用户设计和内部实现使用两套词汇，本文统一使用以下术语：

- **Data Lake Bootstrap**：用户声明目标 Fluss 表需要从 data lake 接入数据
- **自动开始升级**：系统在建表成功并完成初始化后自动触发 bootstrap upgrade
- **hold partition**：系统按默认规则自动推导出的当天目标分区，也是当前唯一需要导入的分区
- **Imported Base**：系统为目标分区生成的可被 Fluss 加载的基础数据，如 SST
- **bootstrap status / upgrade status**：系统内部维护的升级状态

说明：

- 用户侧入口统一为 `table.datalake.bootstrap.*`
- 内部执行器仍然复用现有 `tiering service`
- 但它执行的是 `taskType = BOOTSTRAP_UPGRADE` 的 `TieringTask`

## 4. 用户入口与系统行为

### 4.1 用户入口

用户通过 `CREATE TABLE ... WITH (...)` 声明 Data Lake Bootstrap 需求，例如：

- `table.datalake.format = 'paimon'`
- `table.datalake.bootstrap.enabled = 'true'`
- `table.auto-partition.enabled = 'true'`
- `table.auto-partition.time-unit = 'day'`
- `table.auto-partition.num-retention = '2'`

### 4.2 建表后的行为

当用户创建带 `table.datalake.bootstrap.*` 配置的 Fluss 表时，系统执行：

- 创建目标 Fluss 表
- 记录 bootstrap 元数据
- 校验源表、schema、主键、分区定义兼容性
- 初始化 upgrade 状态

建表成功后：

- 自动开始升级
- 系统在 bootstrap 元数据初始化完成后自动进入 upgrade 调度

### 4.3 自动启动升级后的行为

当建表成功且 bootstrap 元数据初始化完成后，系统按默认规则自动确定 hold 分区。

例如按天分区，若建表当天是 `2026-03-23`，则系统默认目标分区为 `dt=2026-03-23`。

随后由 `tiering service` 围绕这个分区执行：

- 读取该分区在 Paimon 中的基线数据
- 构建该分区对应的 Imported Base
- 在分区准备完成后，通过 coordinator 接入 Fluss 的现有分区服务流程

## 5. 总体设计

### 5.1 设计目标

最小实现目标如下：

- 对用户暴露简单稳定的 `table.datalake.bootstrap.*` 入口
- 对内部复用现有 heartbeat 拉任务链路
- 围绕单个目标分区逐步推进
- 普通 tiering 与 bootstrap 同优先级，由 coordinator 做公平调度

### 5.2 核心思路

将一次 bootstrap upgrade 拆成两个层次：

- 表级 bootstrap 状态
- `tiering service` 的表内执行逻辑

即：

- 表级上，系统只记录这张表是否已经进入 bootstrap、是否已经完成
- 执行上，coordinator 只负责把“这张表进入 bootstrap”下发给 `tiering service`
- `tiering service` 自己决定本次需要同步哪些 bucket，并在一次执行中完成当前表的转换与接入

这样 coordinator 侧状态保持最小，而 bucket 级执行细节完全留在 `tiering service` 内部处理。

### 5.3 总体流程

1. 用户 `CREATE TABLE`，声明 `table.datalake.bootstrap.enabled = true`
2. 系统创建 Fluss 表并初始化 bootstrap 元数据
3. 系统自动触发 upgrade
4. 系统按默认规则自动推导 hold 分区
5. 系统创建 `bootstrap-upgrade znode`，写入 `status = IN_PROGRESS` 与 `holdPartition`，表示该表已进入 bootstrap 生命周期
6. coordinator 将该表放入 `LakeTableTieringManager`，将其视为一种特殊的 tiering：从 Paimon tiering 到 Fluss
7. `tiering service` 通过 heartbeat 请求 `TieringTask`
8. coordinator 在统一调度入口中公平分配任务，并按需返回一个 `taskType = BOOTSTRAP_UPGRADE` 的 `TieringTask`
9. `tiering service` 收到该表的 bootstrap task 后，开始同步 Paimon 数据，并将其转换为可被 Fluss 加载的 SST 文件
10. `tiering service` 向 coordinator 提交转换写入成功的结果
11. coordinator 创建对应的 partition 和 bucket，并通知对应 Tablet Server 创建 bucket、下载 SST 文件
12. 当 replica 完成加载后，该 partition / bucket 即可开始支持写入
13. bootstrap 完成后，表级状态更新为 `COMPLETE`
14. 该表后续进入正常 tiering 流程：从 Fluss 读取新数据，同步回 Paimon

## 6. 内部元数据设计

### 6.1 元数据分层

建议将元数据分成两层：

- 用户声明配置：保留在表属性中
- 系统运行时状态：保留在独立 bootstrap znode 中

其中：

- 用户声明配置包括 `table.datalake.bootstrap.*`
- 系统运行时状态只保留最小表级状态

### 6.2 用户声明配置

继续复用表属性承载用户声明，例如：

- `table.datalake.format`
- `table.datalake.bootstrap.enabled`
- `table.auto-partition.enabled`
- `table.auto-partition.time-unit`
- `table.auto-partition.num-retention`

这些字段属于表定义的一部分，应随表元数据一起持久化。

### 6.3 内部 bootstrap 状态

建议新增独立 znode，用于记录 bootstrap upgrade 的内部状态，例如：

- `/tabletservers/tables/[tableId]/bootstrap-upgrade`

建议的最小结构如下：

```text
BootstrapUpgradeState {
  status: IN_PROGRESS | COMPLETE
  holdPartition: String
}
```

字段说明：

- `status`
  - 表级 bootstrap 当前状态；节点不存在表示尚未开始，`IN_PROGRESS` 表示该表已经进入 bootstrap 生命周期，可能处于待分配、执行中或失败后等待重试等中间状态，`COMPLETE` 表示已经完成
- `holdPartition`
  - 系统按默认规则自动推导出的当天目标分区

### 6.4 为什么不记录更多分区信息

当前最小方案只处理一个目标分区，因此不需要：

- 预先展开分区列表
- 记录剩余分区集合
- 记录已完成分区集合
- 记录 bucket 级进度

第一版只需要知道：

- 当前目标分区是什么
- 当前是否已经开始
- 当前是否已经完成

bucket 级需要同步哪些数据、如何分配 bucket、一次执行内做到哪里，全部由 `tiering service` 在运行时决定，不持久化到 cluster 元数据中。

## 7. 状态机设计

### 7.1 表级状态

最小状态定义如下：

- 节点不存在
- `IN_PROGRESS`
- `COMPLETE`

状态语义：

- 节点不存在
  - 该表尚未进入 bootstrap 生命周期，coordinator 不分配 `BOOTSTRAP_UPGRADE` 类型 `TieringTask`
- `IN_PROGRESS`
  - 当前表已经进入 bootstrap 流程，可能尚未被分配，也可能正在执行或等待重试
- `COMPLETE`
  - 当前表的 bootstrap 已完成，后续不再分配

### 7.2 状态流转

```text
建表成功并完成初始化 -> IN_PROGRESS   // 系统初始化 bootstrap 状态
IN_PROGRESS -> COMPLETE               // 当前表 bootstrap 已完成
```

### 7.3 状态机约束

- 同一张表在第一期实现中，同一时刻只允许有一个正在执行的 `BOOTSTRAP_UPGRADE` 类型 `TieringTask`
- 只有 `COMPLETE` 才表示该表的 bootstrap upgrade 已完成
- 若 task 超时或失败，znode 仍保持 `IN_PROGRESS`，由 coordinator 重新调度

## 8. 任务模型设计

### 8.1 执行粒度

为保持 coordinator 侧实现最小化，heartbeat 下发的任务粒度定义为：

- 一张待 bootstrap 的表

也就是说：

- coordinator 负责分配“哪张表进入 bootstrap”
- `tiering service` 负责围绕该表的 hold 分区完成本次需要的 bucket 同步和接入

### 8.2 任务结构

建议 heartbeat 返回的 `TieringTask` 至少包含：

```text
TieringTask {
  tableId
  tablePath
  holdPartition
  taskType = BOOTSTRAP_UPGRADE
  taskEpoch
}
```

字段说明：

- `holdPartition`
  - 本次 bootstrap 固定的目标分区，由 coordinator 从 `bootstrap-upgrade znode` 读取后直接放入 task payload
- `taskType`
  - 区分不同类型的 `TieringTask`，例如 `NORMAL_TIERING` 和 `BOOTSTRAP_UPGRADE`
- `taskEpoch`
  - 用于 fencing，防止旧实例误上报完成或心跳

`tiering service` 在拿到 `TieringTask` 后，若 `taskType = BOOTSTRAP_UPGRADE`，则负责：

- 使用 task 中的 `holdPartition`
- 围绕 hold 分区完成 Imported Base 的构建和接入

## 9. 调度与 heartbeat 设计

### 9.1 设计原则

继续复用现有 `tiering service` 的 heartbeat 拉任务模型：

- `tiering service` 不主动扫表
- coordinator 负责在统一调度入口中公平分配不同类型的 `TieringTask`
- service 只负责拉任务、执行任务、续租和上报结果

### 9.2 为什么不让 service 自己做检测

如果让 `tiering service` 自己扫描表或 znode：

- 会引入第二套任务发现机制
- 会绕开 coordinator 的统一调度和 fencing
- 不利于控制 bootstrap 对普通 tiering 的影响

因此最小实现中，任务发现和分配仍然应由 coordinator 负责。

### 9.3 调度策略

为保持实现简单并体现统一调度语义，采用如下最小调度策略：

1. 不同类型的 `TieringTask` 统一进入 coordinator 的调度入口
2. coordinator 基于统一候选集合做公平选择，而不是先分类型再决定优先级
3. `BOOTSTRAP_UPGRADE` 类型 `TieringTask` 仍保留独立并发上限，默认设为 1

coordinator 内部可维护：

- `pendingTieringTasks`
- `pendingBootstrapTieringTasks`  // 由 `LakeTableTieringManager` 维护的 bootstrap 类 tiering task 队列
- `runningBootstrapTasks`
- `maxConcurrentBootstrapTasks = 1`
- `lastScheduledTaskKind`

### 9.4 分配逻辑

当 `tiering service` 在 heartbeat 中请求任务时，coordinator 在统一调度入口中做公平分配：

```java
Task requestTask() {
    List<TaskKind> candidateKinds = fairOrderFrom(lastScheduledTaskKind);
    for (TaskKind kind : candidateKinds) {
        Task task = trySchedule(kind);
        if (task != null) {
            lastScheduledTaskKind = kind;
            return task;
        }
    }
    return null;
}

Task trySchedule(TaskKind kind) {
    switch (kind) {
        case NORMAL_TIERING:
            return requestNormalTieringTask();
        case BOOTSTRAP_UPGRADE:
            return requestBootstrapTieringTaskIfAllowed();
        default:
            return null;
    }
}
```

该策略保证：

- 普通 tiering 与 bootstrap 在同一个调度入口中统一分配
- 公平性由统一候选顺序控制，而不是靠硬编码优先级
- bootstrap 仍通过独立并发上限控制资源占用

### 9.5 为什么这种影响是可控的

因为 bootstrap 任务同时满足以下三个条件：

- 始终只围绕一个目标分区推进
- 在统一调度入口中与普通 tiering 一起参与公平分配
- 总并发被限制为很小的常数，第一版默认 1

因此它不会因为人为降级而长期饥饿，也不会因为并发失控而压制普通 tiering 的处理机会。

## 10. 自动启动升级后的初始化流程

当用户建表成功且 bootstrap 元数据初始化完成后，系统自动执行如下初始化步骤：

1. 按默认规则推导 hold 分区
2. 校验该表的 bootstrap 可执行性
3. 创建 `bootstrap-upgrade znode`，写入：
   - `status = IN_PROGRESS`
   - `holdPartition = ...`
4. 将该表加入 `pendingBootstrapTieringTasks`，由 `LakeTableTieringManager` 统一管理

注意：

- hold 分区是唯一需要持久化的目标分区信息
- 当前最小方案不持久化额外分区列表或 bucket 进度

## 11. coordinator 侧执行逻辑

### 11.1 分配 `BOOTSTRAP_UPGRADE` 类型 `TieringTask`

当 coordinator 需要为某张表分配 `BOOTSTRAP_UPGRADE` 类型 `TieringTask` 时：

1. 读取该表的 `bootstrap-upgrade znode`
2. 若 znode 不存在，则说明尚未进入 bootstrap，不分配
3. 若该表状态为 `COMPLETE`，则不再分配
4. 若该表当前没有正在运行的 `BOOTSTRAP_UPGRADE` 类型 `TieringTask`，则返回该表对应的 `TieringTask`

### 11.2 完成上报处理

当 service 上报某个 `BOOTSTRAP_UPGRADE` 类型 `TieringTask` 完成时，coordinator 执行：

1. 校验 `taskEpoch`
2. 读取 `bootstrap-upgrade znode`
3. 更新 `status = COMPLETE`
4. 将该表切换为正常 tiering 生命周期
5. `runningBootstrapTasks--`

### 11.3 失败上报处理

当 service 上报某个 `BOOTSTRAP_UPGRADE` 类型 `TieringTask` 失败时，coordinator 执行：

1. 校验 `taskEpoch`
2. 读取 `bootstrap-upgrade znode`
3. 保持 `status = IN_PROGRESS` 不变
4. 重新放回 `pendingBootstrapTieringTasks`
5. `runningBootstrapTasks--`

### 11.4 超时处理

复用现有 heartbeat timeout 机制。

若某个 `BOOTSTRAP_UPGRADE` 类型 `TieringTask` 超时未续租：

- coordinator 视为该任务执行失败
- 保持 znode 为 `IN_PROGRESS`
- 允许后续实例重新领取

## 12. `tiering service` 侧执行逻辑

### 12.1 service 侧职责

`tiering service` 继续承担以下通用职责：

- heartbeat 拉任务
- 任务执行中续租
- 上报 finished / failed

新增职责仅为：

- 在统一 `TieringTask` dispatch 中支持 `BOOTSTRAP_UPGRADE` 分支

### 12.2 `BOOTSTRAP_UPGRADE` 类型 `TieringTask` 执行流程

当 service 拿到 `TieringTask` 且 `taskType = BOOTSTRAP_UPGRADE` 时，执行：

1. 读取 task 中的 `holdPartition`
2. 基于 hold 分区识别本次需要同步的 bucket
3. 读取 hold 分区对应的基线数据
4. 完成所有需要同步 bucket 的 Imported Base 构建，并将其输出为 SST 文件
5. 将 SST / Imported Base 落到内部存储
6. 向 coordinator 上报转换写入成功
7. 由 coordinator 触发 Fluss 分区接入流程，包括创建 partition、bucket，并通知对应 Tablet Server 下载 SST
8. 持续 heartbeat 直到该 task 完成
9. 成功则上报 finished，失败则上报 failed

### 12.3 与 Fluss 分区接入流程的关系

`BOOTSTRAP_UPGRADE` 类型 `TieringTask` 的职责不是直接绕过 Fluss 内部流程完成在线接管，而是：

- 先把 hold 分区的数据转换为 Imported Base / SST
- 再由 coordinator 触发：
  - 创建对应 Fluss partition
  - 创建并分配对应 bucket / replica
  - 通知 Tablet Server 下载并加载 SST

也就是说：

- Imported Base 准备是 bootstrap 的前置步骤
- 分区正式接入必须经过 coordinator

## 13. Imported Base 与可见性约束

### 13.1 关键约束

必须满足以下约束：

- hold 分区在 Imported Base 准备完成前，不能写入
- hold 分区必须先完成基础数据准备，才能进入 Fluss 分区创建流程
- 分区接入必须经过 coordinator

### 13.2 可见性原则

第一期建议采用如下原则：

- `tiering service` 在一次任务执行中完成当前需要同步 bucket 的 Imported Base 准备
- 只有 hold 分区的 Imported Base 全部准备好后，才能接入该分区的 Fluss 服务流程
- 表级 `COMPLETE` 只表示本次自动启动的 bootstrap upgrade 目标分区已接入完成

因此：

- 第一版不在 cluster 侧持久化 bucket 级进度
- hold 分区必须先完成 Imported Base，再被接入 Fluss

## 14. 幂等性与恢复

### 14.1 幂等性要求

由于 `BOOTSTRAP_UPGRADE` 类型 `TieringTask` 可能因失败或超时被重试，需要保证：

- 同一个 hold 分区的 Imported Base 生成可以安全重试
- 已成功完成的接入不会因旧 task 误上报被重复提交
- 上报完成与失败时必须基于 `taskEpoch` 做 fencing

### 14.2 恢复机制

第一期恢复策略保持简单：

- 若任务失败或超时，则保持 `bootstrap-upgrade znode` 为 `IN_PROGRESS`
- 等待后续 heartbeat 再次分配
- 重新分配后，由新的 `tiering service` 重新执行该表的 bootstrap

这种方式虽然保守，但实现简单、行为清晰，并且适合当前的最小方案。

## 15. 最小改造点

按模块划分，最小改造点如下。

### 15.1 表属性与 DDL

- 支持并解析 `table.datalake.bootstrap.*`
- 在建表时记录 bootstrap 声明配置
- 在建表成功并完成初始化后自动触发 bootstrap 初始化

### 15.2 coordinator

- 新增 bootstrap-upgrade 元数据管理
- 新增 `pendingBootstrapTieringTasks`
- 在 heartbeat 分配逻辑中支持 `BOOTSTRAP_UPGRADE` 类型的 `TieringTask`
- 支持 `BOOTSTRAP_UPGRADE` 类型 `TieringTask` 的 finished / failed / timeout 处理

### 15.3 RPC

- 扩展 heartbeat task payload
- 增加 `taskType = BOOTSTRAP_UPGRADE`
- 将现有 `tieringEpoch` 泛化为 `taskEpoch` 或为 bootstrap 单独增加 epoch 字段

### 15.4 tiering service

- 继续复用 heartbeat 拉任务
- 在任务执行分发中增加 `BOOTSTRAP_UPGRADE` 分支
- 围绕 hold 分区执行 Imported Base 构建和接入上报

## 16. 取舍与后续演进

### 16.1 本方案的取舍

为了让第一期设计保持最小和稳定，本方案做了如下取舍：

- 在建表后自动开始升级，不要求额外启动动作
- 只处理当天的目标分区
- 不记录额外分区列表
- 不记录 bucket 级进度
- 让 bootstrap 和普通 tiering 在统一调度入口中公平分配任务
- 不引入独立 bootstrap service，而是复用现有 `tiering service`

### 16.2 后续可演进方向

后续如有需要，可逐步演进为：

- 一个表支持多个目标分区
- 目标分区内部按 bucket 更细粒度推进
- 在 znode 或独立元数据中记录 bucket 级进度
- 更丰富的可观测字段，如分轮进度和阶段状态
- 更复杂的 bootstrap 优先级和资源隔离策略
- 从固定目标分区导入扩展到增量追平模式

## 17. 与用户设计文档的口径差异

当前用户文档 `datalake-bootstrap-upgrade-simple-design.md:121` 仍描述为“建表成功后不自动开始升级，用户后续需要额外启动升级”。

本文档按当前实现偏好改为：

- 建表成功后自动开始升级
- 第一版只处理当天的目标分区

因此，如果该内部设计被采纳，建议后续同步更新用户设计文档，避免对外口径与内部实现不一致。

## 18. 设计结论

本设计建议采用如下最小实现方案：

- 用户通过 `table.datalake.bootstrap.*` 声明 Data Lake Bootstrap
- 建表成功后记录 bootstrap 元数据并自动开始升级
- 系统按默认规则自动确定 hold 分区
- bootstrap 内部状态记录在独立 `bootstrap-upgrade znode`
- `tiering service` 继续通过 heartbeat 拉任务
- coordinator 以表级 `TieringTask` 触发 bootstrap，`tiering service` 自己决定需要同步的 bucket，并在一次执行中完成该表的 bootstrap
- 不同类型的 `TieringTask` 在统一调度入口中公平分配，其中 bootstrap 全局低并发执行
- hold 分区先准备 Imported Base / SST，再通过 coordinator 接入 Fluss 分区服务流程
- bootstrap 完成后，该表进入正常 tiering 流程，从 Fluss 增量同步回 Paimon

一句话总结：

> 用户通过 `CREATE TABLE` 声明目标表需要从 data lake bootstrap；系统在建表成功并完成初始化后自动进入一种特殊的 tiering 流程：由 `tiering service` 将 Paimon 数据转换为 Fluss 可加载的 SST，coordinator 再驱动 partition / bucket / replica 接入；bootstrap 完成后，该表继续进入正常 tiering 流程，从 Fluss 增量同步回 Paimon。
