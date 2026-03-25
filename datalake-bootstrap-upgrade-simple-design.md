# 基于 Data Lake Bootstrap 的 Paimon 表升级为 Fluss 表设计

## 1. 背景

用户已经有一张 Paimon 表，希望平滑升级为 Fluss 表。

目标是：

- 后续新写入由 Fluss 承接
- 一个或多个历史分区逐步接入 Fluss
- 用户不需要手动处理 SST、manifest、bucket、副本等内部细节

## 2. 设计目标

提供一种简单的用户入口，让用户在创建 Fluss 表时声明：

- 该表需要从 data lake bootstrap
- data lake 格式是什么
- 源表是谁
- 历史分区如何接入 Fluss

## 3. 用户入口

采用 `CREATE TABLE ... WITH (...)` 的方式。

核心配置如下：

- `table.datalake.format`
- `table.datalake.bootstrap.enabled`
- `table.datalake.bootstrap.source-catalog` 可选
- `table.datalake.bootstrap.source-table` 可选
- `table.datalake.bootstrap.cutover-partition` 可选
- `table.datalake.bootstrap.historical-partition-policy` 可选

## 4. 推荐配置

### 4.1 极简配置

```sql
CREATE TABLE fluss_catalog.sales.orders (
  dt STRING,
  order_id BIGINT,
  user_id BIGINT,
  amount DECIMAL(18, 2),
  PRIMARY KEY (dt, order_id) NOT ENFORCED
)
WITH (
  'table.datalake.format' = 'paimon',
  'table.datalake.bootstrap.enabled' = 'true',
  'table.auto-partition.enabled' = 'true',
  'table.auto-partition.time-unit' = 'day',
  'table.auto-partition.num-retention' = '2'
);
```

### 4.2 完整配置

```sql
CREATE TABLE fluss_catalog.sales.orders (
  dt STRING,
  order_id BIGINT,
  user_id BIGINT,
  amount DECIMAL(18, 2),
  PRIMARY KEY (dt, order_id) NOT ENFORCED
)
WITH (
  'table.datalake.format' = 'paimon',
  'table.datalake.bootstrap.enabled' = 'true',
  'table.datalake.bootstrap.source-catalog' = 'paimon_catalog',
  'table.datalake.bootstrap.source-table' = 'paimon_catalog.sales.orders',
  'table.datalake.bootstrap.cutover-partition' = 'dt=2026-03-21',
  'table.datalake.bootstrap.historical-partition-policy' = 'latest-2',
  'table.auto-partition.enabled' = 'true',
  'table.auto-partition.time-unit' = 'day',
  'table.auto-partition.num-retention' = '2'
);
```

## 5. 默认规则

### 5.1 源表默认值

如果没有配置 `table.datalake.bootstrap.source-table`，则默认源表为：

- 同 database
- 同 table name
- catalog 取默认 data lake catalog，或 `table.datalake.bootstrap.source-catalog`

例如目标表是 `fluss_catalog.sales.orders`，则默认源表可解析为：

- `paimon_catalog.sales.orders`

### 5.2 cutover 分区默认值

如果没有配置 `table.datalake.bootstrap.cutover-partition`，则：

- 在 `START UPGRADE` 执行时
- 根据当前时间和自动分区规则
- 解析当前分区作为 cutover partition

例如按天分区，升级启动时间是 `2026-03-23`，则默认：

- `dt=2026-03-23`

### 5.3 历史分区接管默认值

如果没有配置 `table.datalake.bootstrap.historical-partition-policy`，则：

- 默认复用 `table.auto-partition.num-retention = N`
- 表示默认接管最近 `N` 个历史分区

## 6. 系统行为

当用户创建带 `table.datalake.bootstrap.*` 配置的 Fluss 表时，系统执行：

- 创建目标 Fluss 表
- 记录 bootstrap 元数据
- 校验源表、schema、主键、分区定义是否兼容
- 初始化升级状态

建表成功后：

- 不自动开始升级
- 用户后续显式执行 `START UPGRADE`

## 7. 升级执行流程

### 第 1 步：用户创建 Fluss 表

通过 `CREATE TABLE ... WITH (...)` 声明该表需要从 data lake bootstrap。

### 第 2 步：用户启动升级

系统根据配置和默认规则确定：

- 源表
- cutover 分区
- 历史分区接管范围

### 第 3 步：系统构建历史分区基线

系统将一个或多个目标历史分区转换成 Fluss 可加载的 SST。

### 第 4 步：系统接入 Fluss 现有流程

系统通知 Coordinator：

- 创建对应分区
- 分配副本
- 通知 Tablet Server 持有副本

### 第 5 步：分区进入 Fluss 服务流程

相关副本完成就绪后：

- 读走 Fluss
- 写走 Fluss
- Paimon 不再参与这些已接入分区的在线服务

## 8. 关键约束

- 历史分区在 Imported Base 准备完成前，不能写入
- 历史分区必须先完成基础数据准备，才能进入 Fluss 分区创建流程
- 分区接入必须经过 Coordinator
- 用户不直接操作 SST、manifest、bucket、副本

## 9. 设计结论

推荐采用 `table.datalake.bootstrap.*` 作为用户入口：

- 用户体验简单
- 命名风格统一
- 默认规则明确
- 内部实现可封装

一句话总结：

> 用户通过 `CREATE TABLE` 声明该 Fluss 表需要从 data lake bootstrap，系统基于默认规则或显式配置，逐步将一个或多个历史分区接入 Fluss，并让后续读写统一由 Fluss 承接。
