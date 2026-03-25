# Data Lake Bootstrap Upgrade 开发拆分任务

本文档基于 `design_doc/datalake-bootstrap-upgrade-internal-final-design.md`，将实现工作拆分为可由多个 code agent 并行执行的子任务。

目标：

- 先收敛公共协议与状态模型
- 再并行开发 coordinator / 调度 / tiering service / Fluss 接入流程
- 最后统一做失败恢复与联调测试

## 1. 并行执行总览

建议执行顺序：

1. 先完成 `Subtask 0`，统一公共模型与 RPC 协议
2. 再由 `Subtask 1` 落地 bootstrap 状态管理接口，并冻结 coordinator 侧状态访问边界
3. 在 `Subtask 0` 和 `Subtask 1` 接口稳定后，`Subtask 2`、`Subtask 3`、`Subtask 4`、`Subtask 4A` 并行开发
4. 开始并行开发前，需明确接口冻结要求：
   - 建表成功后的 bootstrap 初始化时机由 `Subtask 1` 定义接口
   - `Subtask 2` 只负责统一调度与首次入队接线，不重新定义初始化语义
   - `Subtask 4` 与 `Subtask 4A` 需先对齐 SST 下载指令与加载完成回调结构
5. 最后由 `Subtask 5` 统一完成恢复、超时、联调和测试

建议的 agent 分配：

- `Agent 0` -> `Subtask 0`
- `Agent 1` -> `Subtask 1`
- `Agent 2` -> `Subtask 2`
- `Agent 3` -> `Subtask 3`
- `Agent 4` -> `Subtask 4`
- `Agent 4A` -> `Subtask 4A`
- `Agent 5` -> `Subtask 5`

依赖关系：

```text
Subtask 0
  └── Subtask 1
        ├── Subtask 2
        ├── Subtask 3
        ├── Subtask 4
        └── Subtask 4A

Subtask 2/3/4/4A
  └── Subtask 5
```

并行说明：

- `Subtask 2` 依赖 `Subtask 1` 提供稳定的 bootstrap 状态访问接口
- `Subtask 4` 依赖 `Subtask 1` 的状态更新接口以及 `Subtask 3` 的成功上报结构
- `Subtask 4A` 负责 Tablet Server / Replica 端配套改造，避免该部分隐含在 coordinator 任务中

---

## 2. Subtask 0：统一任务模型与公共协议

### 目标

将 bootstrap 正式纳入统一 `TieringTask` 模型，并定义所有后续实现依赖的公共结构。

### 任务内容

- 为统一任务模型增加 `taskType = BOOTSTRAP_UPGRADE`
- 为 `TieringTask` 增加 `holdPartition`
- 明确 `taskEpoch` 的传递与 fencing 语义
- 定义 bootstrap 转换成功后的上报结构
- 更新相关 RPC / serializer / model 定义

### 产出

- 更新后的 `TieringTask` 结构
- 更新后的 RPC 消息结构
- 序列化与反序列化逻辑
- bootstrap 结果上报结构定义

### 不包含

- 不改 coordinator 调度逻辑
- 不改 tiering service 执行逻辑
- 不改 partition / bucket 激活逻辑

### 完成标准

- coordinator 与 tiering service 都能识别 `taskType = BOOTSTRAP_UPGRADE`
- `holdPartition` 可以通过 task payload 传递
- 相关模块编译通过

### 依赖

- 无

---

## 3. Subtask 1：Coordinator 的 Bootstrap ZK 状态管理

### 目标

实现 `BootstrapUpgradeState` 的 ZooKeeper 管理逻辑。

### 任务内容

- 定义 znode 路径：`/tabletservers/tables/[tableId]/bootstrap-upgrade`
- 定义状态结构：
  - `status = IN_PROGRESS | COMPLETE`
  - `holdPartition`
- 在 bootstrap 初始化时创建 znode，并写入 `IN_PROGRESS`
- 在 bootstrap 完成时将状态更新为 `COMPLETE`
- 提供 coordinator 侧的状态读写 helper / manager

### 产出

- `BootstrapUpgradeState` model
- znode create/read/update 管理逻辑
- 基本状态单测

### 不包含

- 不改统一调度逻辑
- 不改 tiering service
- 不负责超时重试联调

### 补充要求

- 本任务需要同时明确 bootstrap 初始化的调用入口
- 需要给出 coordinator 侧统一的状态访问接口，供 `Subtask 2`、`Subtask 4` 复用
- 需要在文档或接口注释中明确：建表成功后由谁负责创建 znode 并首次将表加入 `pendingTieringTables`

### 完成标准

- 能创建 bootstrap znode
- 能读取 `holdPartition`
- 能将状态从 `IN_PROGRESS` 更新为 `COMPLETE`
- 需输出 coordinator bootstrap 状态接口清单，供 `Subtask 2`、`Subtask 4` 直接对接

### 依赖

- 依赖 `Subtask 0`

---

## 4. Subtask 2：统一调度入口改造

### 目标

把 bootstrap 收敛到统一的 `pendingTieringTables` 模型中，由表状态决定 task type。

### 任务内容

- coordinator 只维护统一的 `pendingTieringTables`
- 接入建表 bootstrap 初始化逻辑：建表成功后将表首次放入 `pendingTieringTables`
- 从 `pendingTieringTables` 中选表
- 通过 `Subtask 1` 暴露的接口读取表对应 bootstrap znode（如果存在）
- 根据表状态决定返回：
  - `taskType = BOOTSTRAP_UPGRADE`
  - 或正常 tiering task
- 将 bootstrap 失败/超时后的表重新放回 `pendingTieringTables`

### 产出

- 统一调度入口实现
- `LakeTableTieringManager` 改造
- heartbeat 请求任务路径改造

### 不包含

- 不改 bootstrap ZK 状态结构
- 不改具体 bootstrap 执行逻辑
- 不改 partition / bucket 激活逻辑

### 完成标准

- `pendingTieringTables` 成为唯一待调度入口
- 建表成功后可以自动完成首次入队
- coordinator 能根据表状态返回不同 `TieringTask`
- 不依赖单独的 bootstrap 调度队列

### 依赖

- 依赖 `Subtask 0`
- 与 `Subtask 1` 在状态读取接口上对接

---

## 5. Subtask 3：Tiering Service 的 BOOTSTRAP_UPGRADE 执行分支

### 目标

在现有 `tiering service` 中增加 `BOOTSTRAP_UPGRADE` 执行路径，实现 Paimon -> SST 转换。

### 任务内容

- 在统一 task dispatch 中支持 `taskType = BOOTSTRAP_UPGRADE`
- 从 task payload 中读取 `holdPartition`
- 识别该 hold 分区需要 bootstrap 的 bucket
- 从 Paimon 读取源数据
- 将源数据转换为 Fluss 可加载的 SST 文件
- 持久化 SST / Imported Base 产物
- 向 coordinator 上报转换写入成功

### 产出

- bootstrap executor / handler
- Paimon -> SST 转换逻辑接线
- 转换成功上报逻辑

### 不包含

- 不负责创建 partition / bucket
- 不负责 Tablet Server 下载 SST
- 不负责修改 bootstrap znode 为 `COMPLETE`

### 完成标准

- 能独立完成一次 `holdPartition` 的 Paimon -> SST 转换流程
- 能向 coordinator 正确上报转换写入成功

### 依赖

- 依赖 `Subtask 0`

---

## 6. Subtask 4：Coordinator 驱动 Fluss 接入流程

### 目标

在 bootstrap 转换成功后，驱动 Fluss 侧 partition / bucket / replica 激活流程。

### 任务内容

- 接收 bootstrap 转换成功上报
- 创建目标 partition
- 创建并分配 bucket / replica
- 下发对 Tablet Server 的 SST 下载 / 加载指令接口
- 在 replica 加载完成后将目标 partition / bucket 置为可写
- 将表切换到正常 tiering 生命周期
- 将 bootstrap znode 更新为 `COMPLETE`

### 边界说明

- 本任务只负责 coordinator 侧编排与状态推进
- Tablet Server / Replica 端如何响应下载、加载、落盘 SST，由 `Subtask 4A` 负责实现

### 产出

- bootstrap completion handler
- partition / bucket / replica 激活逻辑接线
- Tablet Server SST 下载触发逻辑

### 不包含

- 不负责 Paimon -> SST 转换
- 不负责统一调度入口改造

### 完成标准

- 从“转换成功”到“partition / bucket 可写”链路打通
- bootstrap 成功后状态更新为 `COMPLETE`

### 依赖

- 依赖 `Subtask 0`
- 依赖 `Subtask 1` 提供状态更新接口
- 与 `Subtask 3` 在上报结果结构上对接
- 与 `Subtask 4A` 在 TS/Replica 指令与完成回调上对接

---

## 7. Subtask 4A：Tablet Server / Replica 侧 SST 导入与加载

### 目标

实现 Tablet Server / Replica 侧对 bootstrap SST 的接收、下载、加载与完成回报逻辑。

### 任务内容

- 接收 coordinator 下发的 SST 下载 / 加载指令
- 执行 SST 下载与本地落盘
- 驱动 replica 加载 Imported Base / SST
- 在加载完成后向 coordinator 返回完成信号
- 明确失败路径与可重试行为

### 产出

- Tablet Server / Replica 侧 SST 导入处理逻辑
- 加载完成回调或状态上报逻辑
- 相关单测或局部集成测试

### 不包含

- 不负责 coordinator 侧状态推进
- 不负责 Paimon -> SST 转换

### 完成标准

- coordinator 下发 SST 导入指令后，Tablet Server / Replica 能完成下载与加载
- 加载完成后 coordinator 能收到可用于后续状态推进的完成信号
- 需输出 SST 下载指令与加载完成回调结构，供 `Subtask 4` 直接对接

### 依赖

- 依赖 `Subtask 0`
- 与 `Subtask 4` 在指令与回调协议上对接

---

## 8. Subtask 5：失败恢复、超时重试与联调测试

### 目标

打通第一版完整生命周期，并验证恢复与幂等语义。

### 任务内容

- bootstrap task 超时处理
- 失败后保持 znode `IN_PROGRESS`
- 失败/超时后重新放回 `pendingTieringTables`
- `taskEpoch` fencing 校验
- 增加联调与集成测试，覆盖：
  - 建表触发 bootstrap
  - heartbeat 分配 `BOOTSTRAP_UPGRADE`
  - Paimon -> SST
  - Tablet Server / Replica 下载并加载 SST
  - partition / bucket / replica 激活
  - bootstrap 标记为 `COMPLETE`
  - 后续进入正常 tiering 生命周期

### 范围收敛

- 本任务聚焦“恢复语义 + 端到端联调”
- 与恢复无关的普通单测优先下沉到各子任务中完成
- 如联调工作量过大，可拆分为“恢复/fencing”与“端到端测试”两个提交

### 产出

- timeout / retry / fencing 实现
- 集成测试 / e2e 测试

### 不包含

- 不新增 bucket 级 checkpoint 机制
- 不扩展多分区 bootstrap 能力

### 完成标准

- bootstrap 失败后可以通过重试恢复
- 旧任务无法通过过期 `taskEpoch` 错误覆盖新状态
- 端到端链路测试通过

### 依赖

- 依赖 `Subtask 1`
- 依赖 `Subtask 2`
- 依赖 `Subtask 3`
- 依赖 `Subtask 4`
- 依赖 `Subtask 4A`

---

## 9. 每个 Agent 的统一要求

所有 agent 在实现时需遵循以下约束：

- 以 `design_doc/datalake-bootstrap-upgrade-internal-final-design.md` 为准
- 不引入 bucket 级持久化进度
- 不引入独立 bootstrap 调度体系
- bootstrap 必须保持为一种特殊的 `TieringTask`
- `pendingTieringTables` 必须保持为唯一的统一待调度表集合
- 建表 bootstrap 初始化时，znode 立即创建并写入 `IN_PROGRESS`
- `znode 不存在` 不作为正常业务状态使用

## 10. 推荐执行方式

推荐并发方式如下：

1. 先由 `Agent 0` 完成 `Subtask 0`
2. `Subtask 0` 合入后，先由 `Agent 1` 完成 `Subtask 1`，冻结状态访问接口
3. 在 `Subtask 1` 接口冻结后，同时启动：
   - `Agent 2` -> `Subtask 2`
   - `Agent 3` -> `Subtask 3`
   - `Agent 4` -> `Subtask 4`
   - `Agent 4A` -> `Subtask 4A`
4. 待上述任务完成后，由 `Agent 5` 完成 `Subtask 5`

一句话总结：

> 先定协议，再并行做状态管理、统一调度、bootstrap 执行器和 Fluss 接入流程，最后统一完成恢复逻辑与联调测试。
