# Paimon 表升级为 Fluss 表的用户使用设计文档

## 1. 背景

用户已经有一张正在使用的 Paimon 表，希望逐步升级为 Fluss 表。

从用户视角看，这个需求不应该暴露成一堆底层动作，例如：

- 手动构建 SST
- 手动注册 manifest
- 手动创建分区
- 手动让 Tablet Server 持有副本

这些都属于系统内部实现细节。

对用户而言，更合理的产品能力应该是：

> 将一个已有的 Paimon 表注册为“待升级表”，然后由系统逐步把它接管到 Fluss。

本文档只讨论**用户如何使用这个功能**，不展开底层 SST 构建和分区接管实现细节。

## 2. 目标

设计一套面向用户的升级使用方式，使用户可以：

- 将一个已有 Paimon 表声明为升级来源。
- 创建对应的 Fluss 目标表或绑定已有 Fluss 表。
- 指定从哪个边界开始由 Fluss 承接后续写入。
- 让系统逐步接管一个或多个历史分区。
- 查询升级进度、分区状态和失败原因。
- 在必要时暂停、恢复或取消升级流程。

## 3. 非目标

本文档不覆盖以下内容：

- SST 文件格式细节。
- Batch Job 内部执行细节。
- Coordinator 和 Tablet Server 的内部 RPC 设计。
- Fluss 内部副本恢复和存储实现细节。

## 4. 设计原则

### 4.1 面向“升级计划”，而不是面向底层实现

用户应该操作的是：

- 升级计划
- 历史分区接管
- 升级状态

而不是：

- SST 文件
- manifest 路径
- bucket id
- replica 分配

### 4.2 必须支持异步执行

升级流程通常比较长，可能包含：

- 预检查
- 写入切换
- 多个历史分区的逐步接管
- 失败重试

因此用户接口应天然支持异步任务和状态查询。

### 4.3 必须支持灰度升级

不要要求用户一次性全表切换。

更合适的方式是：

- 先切后续写入到 Fluss
- 再逐步接管一个或多个历史分区
- 用户可以观察进度并按批次推进

### 4.4 必须有清晰的阻塞反馈

当升级无法继续时，系统应明确告诉用户原因，例如：

- schema 不兼容
- 主键定义不兼容
- 目标表不存在
- 历史分区基础数据尚未准备完成
- 分区尚未进入 Fluss 现有分区创建与副本就绪流程

## 5. 面向用户的核心抽象

建议对外暴露三个抽象。

## 5.1 Upgrade Plan

表示“把一个 Paimon 表升级为一个 Fluss 表”的整体计划。

建议包含以下信息：

- `source_table`
- `target_table`
- `cutover_boundary`
- `historical_partition_policy`
- `write_cutover_status`
- `overall_status`
- `created_at`
- `last_updated_at`

## 5.2 Partition Handover

表示某个或某批历史分区的接管状态。

建议包含：

- `partition`
- `source_snapshot`
- `base_build_status`
- `partition_create_status`
- `replica_ready_status`
- `writable`
- `error_message`

## 5.3 Upgrade Status

表示用户用来观察升级过程的总体视图。

建议至少回答：

- 新写入是否已经切到 Fluss
- 哪些历史分区仍在 Paimon
- 哪些历史分区正在接管
- 哪些历史分区已经由 Fluss 提供服务
- 哪些历史分区失败了，以及失败原因是什么

## 6. 推荐的用户使用流程

建议将用户使用流程设计成以下 5 步。

## 6.1 第一步：创建升级计划

用户显式声明：

- 源 Paimon 表是谁
- 目标 Fluss 表是谁
- 从哪里开始让 Fluss 承接后续写入
- 历史分区如何逐步接管

### 建议接口

```sql
CALL create_table_upgrade_plan(
  source_table => 'paimon_catalog.db.orders',
  target_table => 'fluss_catalog.db.orders',
  cutover_partition => 'dt=2026-03-21',
  historical_partition_policy => 'latest-2'
);
```

### 用户理解

用户看到的是：

- “我为这张表创建了一个升级计划”

而不是：

- “我创建了一堆内部任务”

## 6.2 第二步：执行升级预检查

系统自动校验该升级计划是否可执行。

### 建议检查项

- 源 Paimon 表是否存在
- 目标 Fluss 表是否存在，或者是否允许自动创建
- schema 是否兼容
- 主键定义是否兼容
- 分区定义是否兼容
- 是否存在当前不支持的数据类型
- 是否存在无法接管的历史分区

### 建议接口

```sql
CALL validate_table_upgrade_plan(
  target_table => 'fluss_catalog.db.orders'
);
```

### 建议输出级别

- `PASS`
- `WARNING`
- `BLOCKER`

### 用户体验要求

如果失败，不应该只返回 “validation failed”，而应该明确给出：

- 哪个检查项失败
- 为什么失败
- 用户下一步应该怎么做

## 6.3 第三步：切换后续写入到 Fluss

升级的第一条主线是：

- 从某个 cutover 边界开始，后续新写入进入 Fluss

这一步建议做成显式动作，而不是隐式自动切换。

### 建议接口

```sql
CALL start_table_upgrade(
  target_table => 'fluss_catalog.db.orders'
);
```

### 用户看到的结果

- 升级流程开始
- 后续写入开始由 Fluss 承接
- 历史分区仍然会逐步接管，不要求一开始全部完成

## 6.4 第四步：逐步接管历史分区

系统按升级计划逐步接管一个或多个历史分区。

系统内部会自动完成：

- 构建 Imported Base SST
- 通知 Coordinator
- 创建分区
- 分配副本
- 让 Tablet Server 持有副本
- 进入 Fluss 现有分区创建与副本就绪流程

但这些细节不需要用户显式控制。

### 建议接口一：自动按计划推进

```sql
CALL resume_table_upgrade(
  target_table => 'fluss_catalog.db.orders'
);
```

### 建议接口二：用户手动指定分区批次

```sql
CALL handover_historical_partitions(
  target_table => 'fluss_catalog.db.orders',
  partitions => ARRAY['dt=2026-03-20', 'dt=2026-03-19']
);
```

### 为什么需要手动模式

这样可以支持：

- 大表灰度升级
- 重点分区优先接管
- 运维在低峰期控制接管节奏

## 6.5 第五步：查看升级状态

用户必须能方便地观察升级状态。

### 建议接口一：查看整体升级状态

```sql
SHOW TABLE UPGRADE STATUS FOR 'fluss_catalog.db.orders';
```

### 建议接口二：查看分区级接管状态

```sql
SHOW TABLE UPGRADE PARTITIONS FOR 'fluss_catalog.db.orders';
```

### 建议返回信息

| partition | source_snapshot | base_build_status | partition_status | writable | error |
|---|---:|---|---|---|---|
| dt=2026-03-20 | 9281 | READY | READY | true | |
| dt=2026-03-19 | 9273 | BUILDING | REMOTE_ONLY | false | |

## 7. 推荐的用户接口集合

建议第一版优先提供以下接口。

## 7.1 创建升级计划

```sql
CALL create_table_upgrade_plan(...);
```

## 7.2 校验升级计划

```sql
CALL validate_table_upgrade_plan(...);
```

## 7.3 启动升级

```sql
CALL start_table_upgrade(...);
```

## 7.4 接管指定历史分区

```sql
CALL handover_historical_partitions(...);
```

## 7.5 查看升级状态

```sql
SHOW TABLE UPGRADE STATUS FOR ...;
SHOW TABLE UPGRADE PARTITIONS FOR ...;
```

## 7.6 暂停、恢复、取消

```sql
CALL pause_table_upgrade(...);
CALL resume_table_upgrade(...);
CALL cancel_table_upgrade(...);
```

## 8. 推荐的状态模型

对用户来说，不需要看到太多内部状态。

建议只暴露以下几类用户可理解状态。

## 8.1 升级计划状态

- `CREATED`
  - 升级计划已创建，但尚未启动。
- `VALIDATING`
  - 正在执行预检查。
- `READY`
  - 预检查通过，可以启动升级。
- `RUNNING`
  - 升级中。
- `PAUSED`
  - 已暂停。
- `FAILED`
  - 升级失败，需要用户处理。
- `COMPLETED`
  - 升级完成。
- `CANCELLED`
  - 升级已取消。

## 8.2 分区接管状态

- `REMOTE_ONLY`
  - 该历史分区仍只在 Paimon 中。
- `BASE_BUILDING`
  - 正在构建 Imported Base。
- `BASE_READY`
  - 基础 SST 已准备好。
- `HANDING_OVER`
  - 正在接入 Fluss 现有分区创建与副本就绪流程。
- `SERVING`
  - 该历史分区已由 Fluss 提供服务。
- `FAILED`
  - 分区接管失败。

## 9. 错误提示设计

用户最需要的是“发生了什么”以及“下一步怎么办”。

建议错误提示包含：

- `error_code`
- `error_message`
- `suggested_action`

### 示例 1：Schema 不兼容

```text
error_code: INCOMPATIBLE_SCHEMA
error_message: Source Paimon table schema is incompatible with target Fluss table schema.
suggested_action: Align primary key and partition definitions, then rerun validation.
```

### 示例 2：历史分区尚未准备好

```text
error_code: PARTITION_BASE_NOT_READY
error_message: Historical partition dt=2026-03-20 has not finished imported base preparation.
suggested_action: Wait for base build completion or retry handover later.
```

### 示例 3：目标表不存在

```text
error_code: TARGET_TABLE_NOT_FOUND
error_message: Target Fluss table fluss_catalog.db.orders does not exist.
suggested_action: Create the target table first or enable auto-create in the upgrade plan.
```

## 10. 第一版最小可用能力

建议第一版先提供一个尽量简单但可用的用户体验。

### 10.1 第一版对外能力

- 创建升级计划
- 校验升级计划
- 启动升级
- 查看升级状态

### 10.2 第一版用户可配置项

- 源 Paimon 表
- 目标 Fluss 表
- cutover 分区
- 历史分区策略，例如最近 2 个历史分区

### 10.3 第一版系统自动完成的事情

- 历史分区 SST 构建
- Coordinator 注册和分区创建
- 副本分配和持有
- 状态推进
- 基本失败重试

## 11. 推荐的用户心智模型

建议在文档和产品描述中，把这个功能定义为：

> 将一个已有的 Paimon 表注册为待升级表，然后由系统逐步接管到 Fluss。

而不建议定义为：

> 手动把 Paimon 文件转换成 SST，再导入到 Fluss。

前者是用户能理解的产品能力，后者是系统内部实现细节。

## 12. 推荐示例

### 12.1 创建升级计划

```sql
CALL create_table_upgrade_plan(
  source_table => 'paimon_catalog.sales.orders',
  target_table => 'fluss_catalog.sales.orders',
  cutover_partition => 'dt=2026-03-21',
  historical_partition_policy => 'latest-2'
);
```

### 12.2 校验

```sql
CALL validate_table_upgrade_plan(
  target_table => 'fluss_catalog.sales.orders'
);
```

### 12.3 启动

```sql
CALL start_table_upgrade(
  target_table => 'fluss_catalog.sales.orders'
);
```

### 12.4 查看状态

```sql
SHOW TABLE UPGRADE STATUS FOR 'fluss_catalog.sales.orders';
SHOW TABLE UPGRADE PARTITIONS FOR 'fluss_catalog.sales.orders';
```

## 13. 建议结论

建议将“把已有 Paimon 表升级成 Fluss 表”的能力设计成一个**面向用户的表升级功能**，而不是一个底层的文件转换工具。

推荐的对外形态是：

- `Upgrade Plan`
- `Partition Handover`
- `Upgrade Status`
- `Procedure + 状态查询`

推荐的对内实现是：

- Batch Job 负责构建 Imported Base SST
- Coordinator 复用现有分区创建与副本就绪流程
- Tablet Server 持有副本并对外提供读写服务

这样既能让用户容易理解和使用，又能把复杂的内部实现封装在系统内。
