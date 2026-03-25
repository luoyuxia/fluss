# Paimon 历史分区接管到 Fluss 的整体流程概览

## 1. 目标

把一个或多个 Paimon 历史分区平滑接管到 Fluss。

接管完成后：

- 这些目标历史分区由 Fluss 对外提供服务。
- 这些目标历史分区后续的读写都走 Fluss。
- Paimon 只作为历史来源，不再参与这些目标历史分区的在线服务。

## 2. 三个角色

- `Batch Job`
  - 负责把 Paimon 历史分区转换成 Fluss SST。
- `Coordinator`
  - 负责控制分区接管流程。
- `Tablet Server`
  - 负责持有副本，并对外提供服务。

## 3. 核心原则

- 历史分区在 SST 构建完成前，**不能写入**。
- 历史分区在 imported SST 准备完成前，**不能进入 Fluss 分区创建流程**。
- 分区接管必须经过 Coordinator，不能由 Batch Job 或 Tablet Server 自行完成。
- 一旦分区按现有流程完成就绪，后续所有新增写入都只进入 Fluss。

## 4. 整体流程

### 第 1 步：Batch Job 选择一个或多个历史分区

例如：

- `dt=2026-03-20`

然后为每个目标历史分区绑定一个确定的 Paimon snapshot。

### 第 2 步：Batch Job 把分区转换成 SST

Batch Job 执行以下动作：

- 读取每个目标历史分区在指定 snapshot 下的数据。
- 按主键整理成最终状态。
- 写出 Fluss SST 文件。
- 写出 `manifest.json`。

这些产物直接写到最终的 Imported Base 路径下。

### 第 3 步：Batch Job 通知 Coordinator

Batch Job 转换完成后，通知 Coordinator：

- 哪个分区已经转换完成。
- 对应的 manifest 在哪里。
- 这份数据来自哪个 Paimon snapshot。
- 当前导入版本是多少。

### 第 4 步：Coordinator 接管这些历史分区

Coordinator 收到通知后，执行以下动作：

- 为每个目标历史分区创建 bucket 元数据。
- 为 bucket 分配副本s。
- 选择对应的 Tablet Server。
- 通知这些 Tablet Server 去 持有这些副本。

### 第 5 步：Tablet Server 加载副本

Tablet Server 收到命令后，执行以下动作：

- 下载 `manifest.json`。
- 根据 manifest 下载 SST 文件。
- 打开 Imported Base SST。
- 初始化本地副本。
- 初始化后续写入所需的 `WAL / MemTable`。
- 上报 Coordinator：`副本就绪`。

### 第 6 步：Coordinator 按现有流程完成分区就绪

当 Coordinator 确认：

- bucket 已创建。
- 副本已分配。
- 足够多的副本已就绪。

则该历史分区直接按 Fluss 现有流程进入可服务状态。

### 第 7 步：分区正式由 Fluss 提供读写服务

分区按现有机制就绪后：

- 读走 Fluss。
- 写走 Fluss。
- Paimon 不再参与这些历史分区的在线服务。

## 5. 简化阶段

每个目标历史分区的阶段可以简单理解为：

```text
REMOTE_ONLY
  -> BASE_BUILDING
  -> BASE_BUILT
  -> 进入 Fluss 现有分区创建与副本就绪流程
```

含义如下：

- `REMOTE_ONLY`
  - 还在 Paimon，只能远端读。
- `BASE_BUILDING`
  - Batch Job 正在构建 SST。
- `BASE_BUILT`
  - SST 和 manifest 已经生成，后续直接进入 Fluss 现有分区创建与副本就绪流程。

## 6. 读写规则

### 接管前

- 读：查 Paimon。
- 写：不允许。

### 接管后

- 读：查 Fluss。
- 写：写 Fluss。

## 7. 一句话总结

> Batch Job 先把 Paimon 历史分区转成 Fluss SST，
> 再通知 Coordinator，
> Coordinator 创建 bucket 并分配副本，
> 然后通知 Tablet Server 加载这些 SST，
> 等副本就绪后直接进入 Fluss 现有流程，
> 从这一时刻开始，这些历史分区由 Fluss 对外提供统一的读写服务。
