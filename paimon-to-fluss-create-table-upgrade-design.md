# 通过 CREATE TABLE 配置 Paimon 升级来源的设计文档

## 1. 背景

用户已经有一张 Paimon 表，希望将它升级为 Fluss 表。

如果要求用户先创建 Fluss 表、再单独创建 Upgrade Plan、再启动升级，会显得流程偏重。

一种更自然的用户体验是：

> 用户在创建 Fluss 表时，直接通过 `WITH` options 声明：这张 Fluss 表是从某个已有 Paimon 表升级而来。

这样用户的心智更简单：

- 我在创建一张 Fluss 表
- 同时告诉系统，这张表不是从零开始，而是承接一张已有 Paimon 表

本文档聚焦这一种用户入口设计。

## 2. 目标

设计一种基于 `CREATE TABLE ... WITH (...)` 的升级声明方式，使用户可以：

- 在创建 Fluss 表时直接声明升级来源是 Paimon。
- 指定源 Paimon 表。
- 指定后续写入切换边界，或在满足条件时使用系统默认边界。
- 指定历史分区接管策略。
- 让系统自动登记内部升级元数据。
- 后续再显式启动升级流程。

## 3. 非目标

本文档不覆盖：

- Batch Job 如何构建 SST。
- Coordinator 如何创建分区和分配副本。
- Tablet Server 如何持有副本。
- 详细的 Upgrade Status 返回格式。

## 4. 核心设计思路

将“从已有 Paimon 表升级而来”设计成一组 `table.datalake.bootstrap.*` 建表选项。

用户执行的不是一个新的专用语法，而是：

- 普通 `CREATE TABLE`
- 加上一组 `table.datalake.bootstrap.*` 配置

例如：

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
  'table.datalake.bootstrap.source-table' = 'paimon_catalog.sales.orders',
  'table.datalake.bootstrap.cutover-partition' = 'dt=2026-03-21',
  'table.datalake.bootstrap.historical-partition-policy' = 'latest-2'
);
```

该语句的语义是：

- 创建一张 Fluss 表
- 同时声明它承接一张已有的 Paimon 表
- 从指定 cutover 边界开始，后续新写入由 Fluss 承接
- 历史分区按指定策略逐步接管

## 5. 为什么选择这种方案

## 5.1 用户入口自然

用户本来就会先想“我要有一张 Fluss 表”。

那么在建表时顺手声明升级来源，比额外再执行一套独立 plan 创建命令更自然。

## 5.2 目标表定义和升级配置天然放在一起

以下内容本来就强相关：

- Fluss 表 schema
- 主键定义
- 分区定义
- 源 Paimon 表
- cutover 边界
- 历史接管策略

统一放在一个 `CREATE TABLE` 语句中，可读性更好。

## 5.3 有利于提前做兼容性校验

系统可以在建表阶段就直接校验：

- 源表是否存在
- schema 是否兼容
- 主键是否兼容
- 分区定义是否兼容
- cutover 配置是否合法

如果校验失败，直接阻止建表，用户反馈也更直接。

## 6. 推荐的 option 设计

## 6.1 必选项

### `table.datalake.format`

表示关联的数据湖格式。

第一版固定支持的升级来源可以是：

- `paimon`

示例：

```sql
'table.datalake.format' = 'paimon'
```

### `table.datalake.bootstrap.enabled`

表示该表在创建后需要从 data lake 执行 bootstrap。

示例：

```sql
'table.datalake.bootstrap.enabled' = 'true'
```

### `table.datalake.bootstrap.source-table`

表示源 data lake 表标识。

该选项建议为**可选项**。如果用户未指定，则默认采用：

- 同 database
- 同 table name
- 以及默认 data lake catalog（或 `table.datalake.bootstrap.source-catalog`）

也就是说，如果用户在 `fluss_catalog.sales` 下创建 `orders` 表，并配置了：

```sql
'table.datalake.format' = 'paimon',
'table.datalake.bootstrap.enabled' = 'true'
```

但没有显式指定 `table.datalake.bootstrap.source-table`，则系统默认会将源表解析为：

```text
<default_paimon_catalog>.sales.orders
```

如果用户配置了：

```sql
'table.datalake.bootstrap.source-catalog' = 'paimon_catalog'
```

则默认源表解析为：

```text
paimon_catalog.sales.orders
```

示例：

```sql
'table.datalake.bootstrap.source-table' = 'paimon_catalog.sales.orders'
```

### `table.datalake.bootstrap.cutover-partition`

表示后续写入切换到 Fluss 的边界。

该选项建议为**可选项**。

如果用户显式指定，则以用户配置为准。
如果用户未指定，并且目标 Fluss 表启用了时间型自动分区，且系统能够根据当前时间唯一确定当前分区，则默认使用**启动升级时刻**对应的当前分区作为 cutover partition。

例如：

- 如果升级在 `2026-03-23` 启动
- 表按天自动分区
- 当前时区下当前分区为 `dt=2026-03-23`

则默认：

```sql
'table.datalake.bootstrap.cutover-partition' = 'dt=2026-03-23'
```

注意，这里的默认值是在 **`START UPGRADE` 执行时** 解析并固化，而不是在 `CREATE TABLE` 执行时解析。

## 6.2 建议项

### `table.datalake.bootstrap.historical-partition-policy`

表示历史分区接管策略。

建议第一版支持：

- `latest-2`
- `manual`
- `all`

示例：

```sql
'table.datalake.bootstrap.historical-partition-policy' = 'latest-2'
```

该选项建议为**可选项**。

如果用户未显式指定，并且表配置了：

```sql
'table.auto-partition.num-retention' = 'N'
```

则系统默认将历史分区接管策略推导为：

- 最近 `N` 个历史分区

也就是说，在常见自动分区场景下，`table.auto-partition.num-retention` 可以作为默认历史接管窗口，而不需要用户再额外配置 `table.datalake.bootstrap.historical-partition-policy`。

如果用户需要更特殊的行为，例如手动指定接管分区或接管全部历史分区，再显式配置该选项。

### `table.datalake.bootstrap.validation-mode`

表示建表时的校验强度。

建议值：

- `strict`
- `permissive`

### `table.datalake.bootstrap.auto-create-target`

表示当目标 Fluss 表不存在时，是否允许自动创建。

建议值：

- `true`
- `false`

第一版如果是显式 `CREATE TABLE`，这个字段可以不是必须，但可保留扩展空间。

## 6.3 后续可扩展项

- `table.datalake.bootstrap.max-concurrent-handover-partitions`
- `table.datalake.bootstrap.allow-schema-evolution`
- `table.datalake.bootstrap.require-base-ready-before-write`

其中 `table.datalake.bootstrap.require-base-ready-before-write` 建议第一版默认就是 `true`，不鼓励用户修改。

## 6.4 与自动分区参数的关系

对于自动分区表，建议采用以下默认推导规则：

- `table.datalake.bootstrap.cutover-partition`
  - 如果用户显式指定，则直接使用用户配置。
  - 如果用户未指定，且表启用了时间型自动分区，并且系统能唯一确定当前分区，则默认使用**启动升级时刻**对应的当前分区。
- `table.datalake.bootstrap.historical-partition-policy`
  - 如果用户显式指定，则直接使用用户配置。
  - 如果用户未指定，且存在 `table.auto-partition.num-retention = N`，则默认使用“最近 `N` 个历史分区”。

因此，第一版更推荐的简化用户体验是：

- 保留 `table.datalake.format`
- 保留 `table.datalake.bootstrap.enabled`
- `table.datalake.bootstrap.source-table` 可选
- `table.datalake.bootstrap.cutover-partition` 可选
- `table.datalake.bootstrap.historical-partition-policy` 可选
- 复用 `table.auto-partition.num-retention` 作为默认历史分区接管窗口

### 什么时候必须要求用户显式指定 `table.datalake.bootstrap.cutover-partition`

以下场景不建议依赖默认推导，而应要求用户显式指定：

- 目标表不是自动分区表
- 分区不是时间型自动分区
- 系统无法唯一确定当前分区
- 用户希望从非当前分区开始切换，例如从明天或某个更早分区开始

## 7. 推荐示例

## 7.1 极简配置版

适用于：

- 源表与目标表同 database、同名
- 使用默认 source catalog
- cutover partition 使用默认推导
- 历史分区接管窗口由 `table.auto-partition.num-retention` 推导

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

默认推导结果：

- 源表 = 默认 data lake catalog 下的 `sales.orders`
- cutover partition = `START UPGRADE` 时的当前分区
- 历史分区接管窗口 = 最近 `2` 个历史分区

## 7.2 完整配置版

适用于：

- 用户希望显式指定源表
- 用户希望显式指定 cutover partition
- 用户希望覆盖默认历史分区接管策略

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

## 7.3 自动按策略接管历史分区

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
  'table.datalake.bootstrap.source-table' = 'paimon_catalog.sales.orders',
  'table.datalake.bootstrap.cutover-partition' = 'dt=2026-03-21',
  'table.datalake.bootstrap.historical-partition-policy' = 'latest-2'
);
```

语义：

- 创建 Fluss 目标表
- 声明它来自一张已有的 Paimon 表
- 后续写入从 `dt=2026-03-21` 开始切到 Fluss
- 历史分区按最近两个分区的策略逐步接管

## 7.4 手动接管历史分区

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
  'table.datalake.bootstrap.source-table' = 'paimon_catalog.sales.orders',
  'table.datalake.bootstrap.cutover-partition' = 'dt=2026-03-21',
  'table.datalake.bootstrap.historical-partition-policy' = 'manual'
);
```

语义：

- 建立升级关系
- 但历史分区接管由用户后续显式触发

## 8. `CREATE TABLE` 后系统内部应做什么

当用户执行带 `table.datalake.bootstrap.*` 选项的 `CREATE TABLE` 时，系统内部建议做以下事情。

## 8.1 创建目标 Fluss 表定义

按普通建表流程创建：

- schema
- 主键定义
- 分区定义
- table metadata

## 8.2 登记升级元数据

额外写入内部升级元数据，例如：

- 升级来源是 `paimon`
- 源表是谁
- cutover 边界是什么
- 历史接管策略是什么
- 当前升级状态是什么

## 8.3 执行兼容性校验

建表阶段建议做以下校验：

- 源 Paimon 表存在
- 目标 Fluss 表定义与源表兼容
- 主键定义兼容
- 分区定义兼容
- cutover 边界格式合法
- 历史接管策略合法

## 8.4 初始化升级状态

如果建表成功，系统可以初始化内部状态，例如：

- `upgrade_enabled = true`
- `write_cutover_status = PENDING`
- `overall_status = READY`

也就是说：

- 建表成功代表“升级关系已登记完成”
- 不代表“升级已经开始执行”

## 9. 是否在建表后自动开始升级

这里需要明确产品行为。

## 9.1 方案 A：建表只做登记，不自动开始升级

即：

- `CREATE TABLE` 成功后
- 系统只创建目标表并登记升级关系
- 后续由用户显式执行 `START UPGRADE`

### 优点

- 更安全
- 用户有时间先查看状态
- 不会因为建表成功就立即切写
- 更适合生产环境使用

### 建议

第一版推荐采用这个方案。

## 9.2 方案 B：建表成功后自动开始升级

即：

- `CREATE TABLE` 成功后
- 系统自动开始升级流程

### 缺点

- 风险更高
- 用户可能只是想先建表，还没准备好切换
- 可控性较差

### 建议

第一版不推荐。

## 10. 推荐的配套用户流程

如果采用 `CREATE TABLE ... WITH (table.datalake.bootstrap.*)`，推荐的完整用户流程如下。

## 10.1 创建目标 Fluss 表并声明升级来源

```sql
CREATE TABLE ... WITH (
  'table.datalake.format' = 'paimon',
  'table.datalake.bootstrap.enabled' = 'true',
  ...
);
```

## 10.2 查看升级状态

```sql
SHOW TABLE UPGRADE STATUS FOR 'fluss_catalog.sales.orders';
```

## 10.3 显式启动升级

```sql
CALL start_table_upgrade(
  target_table => 'fluss_catalog.sales.orders'
);
```

## 10.4 需要时手动接管指定历史分区

```sql
CALL handover_historical_partitions(
  target_table => 'fluss_catalog.sales.orders',
  partitions => ARRAY['dt=2026-03-20', 'dt=2026-03-19']
);
```

## 11. 用户体验上的好处

这种设计的优点是：

- 用户入口清晰
- 目标表与升级来源天然绑定
- 建表时即可完成兼容性检查
- 复杂内部实现对用户透明
- 后续仍可通过独立命令控制升级开始、暂停和状态查看

## 12. 与独立 Upgrade Plan 方案的关系

这两种方案并不冲突。

可以这样理解：

- `CREATE TABLE ... WITH (table.datalake.bootstrap.*)`
  - 是第一版更友好的用户入口
- 独立 `Upgrade Plan`
  - 是系统内部抽象，或者后续面向高级运维场景的增强能力

也就是说，第一版完全可以：

- 对用户暴露建表配置入口
- 对系统内部仍保留 Upgrade Plan 元数据模型

## 13. 建议结论

建议第一版优先支持：

> 用户在创建 Fluss 表时，通过 `WITH` options 声明该表是从某个已有 Paimon 表升级而来。

推荐行为是：

- `CREATE TABLE` 负责创建目标表并登记升级关系
- 建表时完成兼容性校验
- `table.datalake.bootstrap.cutover-partition` 在满足条件时允许省略，并在 `START UPGRADE` 时解析默认值
- 建表成功后不自动开始升级
- 用户后续再显式执行 `START UPGRADE`

这样既能保证用户入口简单，也能保证生产环境中的操作可控性。
