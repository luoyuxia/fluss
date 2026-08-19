<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# FIP-XX：在现有湖表上创建 Fluss 湖流一体表

| 项目 | 内容 |
| --- | --- |
| 作者 | Yuxia Luo |
| 最后更新 | 2026 年 8 月 19 日 |
| 状态 | Discussion |
| 讨论帖 | [dev@fluss.apache.org](https://lists.apache.org/list.html?dev@fluss.apache.org) |
| 投票帖 | TBD |
| Issue | [Apache Fluss Issues](https://github.com/apache/incubator-fluss/issues) |
| 目标版本 | TBD |

> 相关讨论应在 Fluss 开发者邮件列表中进行，避免在 Wiki 评论区展开长篇讨论。

## 背景与动机

Fluss 提供湖流一体存储能力。用户可以通过 Fluss 处理实时数据，并将数据持久化到 Paimon 等
Lake Storage 中。

部分用户在使用 Fluss 之前已经积累了 Paimon 表及其历史数据。为了让这些表具备 Fluss 的实时
读写能力，用户目前需要手工完成以下工作：

1. 读取 Paimon 表的列、主键、分区键和表属性。
2. 在 Fluss 中创建 Schema 一致的表。
3. 配置 Fluss 表复用原有 Paimon 表作为 Lake Storage。
4. 对于主键表，将 Paimon 当前分区的数据加载到 Fluss 实时服务层。
5. 等待初始化完成后再接入新的实时读写流量。

这些步骤容易出现 Schema 不一致、属性遗漏和数据衔接错误。本 FIP 引入
`CREATE TABLE ON LAKE` 能力，让用户通过一个命令在现有湖表上创建 Fluss 湖流一体表。

创建完成后：

- 原有湖表继续作为 Fluss 表的 Lake Storage，不会创建另一份湖表或复制全部历史数据。
- Fluss 为这张表创建元数据和实时服务能力。
- 对于主键表，Fluss 调用独立的 Bulk Load 能力初始化当前分区。
- 其他历史分区继续保留在湖表中，并通过 Fluss 的 Lake 读取路径访问。

公共接口保持 lake format 无关。本 FIP 的首个实现支持 Paimon。Bulk Load 的数据读取、转换、
写入、调度和恢复机制由独立设计负责。

## Public Interfaces

### Flink Action

在现有 Action SPI 中新增 `create_table_on_lake`。用户通过命令行传入 Fluss 和 Lake Catalog 配置，参数前缀与 Tiering Service 一致：

```bash
flink run fluss-flink-1.20-1.0-SNAPSHOT.jar \
  create_table_on_lake \
  --table my_db.my_table \
  --fluss.bootstrap.servers localhost:9123 \
  --datalake.format paimon \
  --datalake.paimon.metastore filesystem \
  --datalake.paimon.warehouse /tmp/paimon \
  --table-conf bucket.num=16
```

#### 输入参数

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `--table` | 是 | 现有湖表的全限定名称，格式为 `database.table`。Fluss 表使用相同的 database 和 table 名称。 |
| `--fluss.*` | 是 | 去掉 `fluss.` 前缀后传给 Fluss Client；其中 `--fluss.bootstrap.servers` 必填。 |
| `--datalake.format` | 是 | Lake Storage 格式；首个实现仅支持 `paimon`。 |
| `--datalake.paimon.*` | 是 | 去掉 `datalake.paimon.` 前缀后原样传给 Paimon Catalog，包括 DLF Catalog、认证和 warehouse 配置。 |
| `--table-conf` | 否 | 可重复的 `key=value` Fluss 表属性。这些属性覆盖或补充从湖表推导出的属性。`bucket.num` 的覆盖规则见 [Paimon Bucket Mode 兼容性](#paimon-bucket-mode-兼容性)。 |

#### 完成语义

- 对于日志表，Fluss 表创建完成且初始 lake snapshot 注册完成后，Action 输出 table ID 并成功退出。空 Paimon 表没有 snapshot 时跳过注册。
- 对于主键表，Action 提交 Flink Batch 作业并等待作业成功。全部 Bucket 初始化完成、表切换为可写后，Action 才成功退出。
- 任一步失败时 Action 以失败结束，不输出成功结果。

### `Admin` API

在 `Admin` 接口中增加以下方法：

```java
/**
 * Create a Fluss table on an existing lake table.
 *
 * <p>The returned future completes with the actual table metadata after the Fluss table is
 * created. The caller is responsible for synchronizing the initial lake snapshot and running any
 * required initialization job.
 *
 * @param tablePath path of the existing lake table
 * @param properties properties that override or supplement values derived from the lake table
 */
CompletableFuture<TableInfo> createTableOnLake(
        TablePath tablePath, Map<String, String> properties);
```

`CompletableFuture` 只表示 Fluss 表元数据创建完成，并返回 Coordinator 应用系统默认值和用户属性后的最终 `TableInfo`。初始 snapshot 注册和主键表初始化由 Action 在 Admin future 完成后继续执行。

### `LakeCatalog` API

在 `LakeCatalog` 接口中增加以下方法：

```java
/**
 * Read an existing lake table and map its metadata to a Fluss {@link TableDescriptor}.
 *
 * @param tablePath path of the lake table
 * @return the Fluss table descriptor mapped from the lake table
 * @throws TableNotExistException if the table does not exist in the lake
 */
TableDescriptor getTableDescriptor(TablePath tablePath) throws TableNotExistException;
```

该接口保持 lake format 无关。Paimon 实现负责读取并映射列、主键、分区键和可映射的表属性。
其他 Lake Storage 可以在后续实现相同接口。

### Coordinator RPC

新增 `CREATE_TABLE_ON_LAKE` RPC：

```protobuf
message CreateTableOnLakeRequest {
  required PbTablePath table_path = 1;
  // User-specified properties override values derived from the lake table.
  repeated PbKeyValue properties = 2;
}

message CreateTableOnLakeResponse {
  required int64 table_id = 1;
  required int32 schema_id = 2;
  required bytes table_json = 3;
  required int64 created_time = 4;
  required int64 modified_time = 5;
  optional string remote_data_dir = 6;
}
```

response 字段与现有 `GetTableInfoResponse` 对齐。Client 将其还原为 `TableInfo`。本 FIP 不定义 Bulk Load 专用 RPC；具体消息格式由 Bulk Load 设计定义。

## Proposed Changes

### 用户如何使用

用户首先在 Fluss 配置的 Lake Catalog 中准备一张已经存在的 Paimon 表。例如，该表在 Lake Catalog 中的路径为 `my_db.my_table`。然后运行 `create_table_on_lake` Action：

```bash
flink run fluss-flink-1.20-1.0-SNAPSHOT.jar \
  create_table_on_lake \
  --table my_db.my_table \
  --fluss.bootstrap.servers localhost:9123 \
  --datalake.format paimon \
  --datalake.paimon.metastore filesystem \
  --datalake.paimon.warehouse /tmp/paimon
```

对于 `BUCKET_UNAWARE` append-only 表，用户可以重复使用 `--table-conf` 传入 Fluss 表属性，例如 `--table-conf bucket.num=16`。如果用户没有指定 `bucket.num`，Fluss 使用集群配置 `default.bucket.number`。该配置当前的默认值为 `1`，但集群管理员可以统一调整。

`--datalake.paimon.*` 必须指向 Fluss 集群配置的同一个 Paimon Catalog。Action 和 Coordinator 会分别读取目标表：Action 用于选择日志表或主键表路径并获取 snapshot；Coordinator 用于独立校验并创建 Fluss 元数据。DLF 插件和认证依赖由 Action 运行时 classpath 提供，敏感值可以使用 Config Provider 在进程启动时解析。

用户只需要运行一次 Action。Action 按以下顺序执行：

1. 根据 `--datalake.paimon.*` 创建 Paimon Catalog，读取目标表并记录当前 Paimon Snapshot。
2. 日志表直接调用 `Admin.createTableOnLake(tablePath, properties)`。Coordinator 从集群配置的 Paimon Catalog 再次读取 Schema、分区键、Bucket 和表属性，完成兼容性校验并创建同名 Fluss 表。Admin 返回最终 `TableInfo` 后，Action 使用建表前记录的 snapshot ID 注册初始 lake snapshot，Bucket offsets 为空，然后成功退出。空表没有 snapshot 时跳过注册。
3. 主键表由 Action 构造并提交 Flink Batch 作业。Coordinator 创建 Fluss 表并将其标记为初始化中，此时 Fluss Server 拒绝写入。Batch 作业固定读取选定的 Paimon Snapshot，转换并排序数据，为每个目标 Bucket 生成 RocksDB SST。
4. Batch 作业将 SST 上传到 Fluss 可访问的远程存储，并向对应的 Fluss Bucket 提交 Bulk Load。
5. 全部 Bucket 的 Bulk Load 完成后，Batch 作业通知 Coordinator 将表切换为可写。Action 等待作业成功后再成功退出。

本 FIP 只规定 Bulk Load 的表状态语义：主键表创建后先处于初始化中且不可写；只有全部 Bucket
的 Bulk Load 都成功后，Coordinator Server 才能将表切换为可写。任一 Bucket 尚未完成或执行
失败时，表都继续保持不可写，避免用户写入与初始化数据并发。Flink 作业的运行状态用于展示
初始化进度和失败原因。SST 提交协议、失败恢复、重试和幂等机制由独立的 Bulk Load 设计定义。

Fluss 表进入正常状态后，用户可以像使用普通 Fluss 表一样进行读写。例如：

```sql
SELECT * FROM fluss_catalog.my_db.my_table;

INSERT INTO fluss_catalog.my_db.my_table
SELECT * FROM source_table;
```

上述 `fluss_catalog` 仅为示例，实际名称由用户的 Flink Catalog 配置决定。

### 用户可见的约束

所有表都需要满足以下条件：

- Lake Catalog 中存在指定的湖表。
- Fluss Catalog 中不存在同名 Fluss 表。
- 湖表的列类型可以完整映射为 Fluss 类型。
- 用户传入的 Fluss 表属性合法。

主键表还需要满足以下条件：

- Paimon Bucket Mode 为 `HASH_FIXED`。
- 包含零个或一个分区键；零个分区键表示非分区表。
- 存在分区键时，该分区键为时间分区，并且 Paimon 的
  `partition.timestamp-formatter` 能够被 Fluss 识别。

Coordinator 在创建 Fluss 表之前校验这些条件，并通过 `InvalidTableException` 等已有异常返回
具体原因。

### Paimon Bucket Mode 兼容性

Fluss 当前依赖 Paimon 1.3.1。Paimon 的 `BucketMode` 枚举包含 `HASH_FIXED`、
`HASH_DYNAMIC`、`KEY_DYNAMIC`、`BUCKET_UNAWARE` 和 `POSTPONE_MODE`。实现必须通过
`FileStoreTable.bucketMode()` 判断模式，不能只读取 `bucket` 属性，因为 `bucket=-1` 在
append-only 表和主键表中表示不同模式。

首期支持范围如下：

| Paimon Bucket Mode | Paimon 表类型与配置 | 支持状态 | Fluss Bucket 映射 |
| --- | --- | --- | --- |
| `HASH_FIXED` | 主键表或 append-only 表，`bucket=N` 且 `N>0` | 支持 | Fluss 使用相同的 Bucket 数、Bucket Key 和 Paimon 默认哈希策略。用户未指定 `bucket.num` 时取 `N`；指定值必须等于 `N`。 |
| `BUCKET_UNAWARE` | append-only 表，`bucket=-1` | 支持 | Paimon 忽略 Bucket 概念并将数据写入物理 `bucket-0`。用户未指定 `bucket.num` 时，Fluss 使用集群配置 `default.bucket.number`；用户也可以指定任意大于 0 的固定 Bucket 数。 |
| `HASH_DYNAMIC` | 不跨分区更新的主键表，`bucket=-1` | 不支持 | Paimon 通过 HASH Index 维护主键哈希到 Bucket 的动态映射，并可自动扩展 Bucket，无法映射为 Fluss 的静态 Bucket 函数。 |
| `KEY_DYNAMIC` | 跨分区更新的主键表，`bucket=-1` | 不支持 | Paimon 维护主键到 Partition 和 Bucket 的动态映射，Fluss 无法根据主键和固定 Bucket 数重建该映射。 |
| `POSTPONE_MODE` | 主键表，`bucket=-2` | 不支持 | Bucket 在后台 Compaction 时确定，不同 Partition 可以形成不同 Bucket 数，无法直接映射为一个固定 Bucket 数的 Fluss 表。 |

Paimon 官方文档将 `HASH_DYNAMIC` 和 `KEY_DYNAMIC` 统称为 Dynamic Bucket。两者在代码中的
枚举名都包含 `DYNAMIC`。主键不跨分区更新时使用 `HASH_DYNAMIC`；主键可能跨分区更新时使用
`KEY_DYNAMIC`。[Paimon Data Distribution](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/)
说明了两种动态映射的行为。

`HASH_FIXED` 首期只支持 Paimon 默认的 `bucket-function.type=default`。Paimon 的 `mod` Bucket
Function 与 Fluss 当前的 Paimon Bucket Function 不同，即使 Bucket 数相同也不能保证记录进入
相同 Bucket，因此应返回不支持错误。

`bucket.num` 按以下规则处理：

1. `HASH_FIXED`：从 Paimon 读取 Bucket 数。用户省略 `bucket.num` 时直接复用；用户指定不同
   的值时返回 `InvalidTableException`，不自动重写 Paimon 数据。
2. `BUCKET_UNAWARE`：用户省略 `bucket.num` 时使用 Fluss 集群配置 `default.bucket.number`；用户
   可以指定任意大于 0 的值覆盖集群默认值。
3. `HASH_DYNAMIC`、`KEY_DYNAMIC` 和 `POSTPONE_MODE`：即使用户提供 `bucket.num` 也返回
   `InvalidTableException`。设置一个固定数值不能改变现有 Paimon 数据的 Bucket 映射。

Paimon 对 append-only `BUCKET_UNAWARE` 表的说明见
[Paimon DataFile](https://paimon.apache.org/docs/1.3/concepts/spec/datafile/)：其表配置为
`bucket=-1`，物理数据写入 `bucket-0`，读写并行度不受该物理 Bucket 限制。

#### 不支持模式的后续演进

`HASH_DYNAMIC` 和 `KEY_DYNAMIC` 的直接支持需要 Fluss 实现与 Paimon 兼容的动态
Key-to-Bucket 映射，或者先把 Paimon 表安全地重写为 `HASH_FIXED`。两种方案都涉及 Paimon
数据布局或 Fluss Bucket 模型的变化，不在本 FIP 中实现。

`POSTPONE_MODE` 用于解决建表时难以确定固定 Bucket 数的问题，并允许不同 Partition 使用不同
Bucket 数。Fluss 当前的 `bucket.num` 是表级固定配置，不能保留这种按 Partition 自适应的布局。
仅把 Fluss 的 `bucket.num` 设置为某个 Partition 当前的 Bucket 数，也不会改变 Paimon 表仍处于
`POSTPONE_MODE` 的事实，后续 Partition 仍可能得到不同的 Bucket 数。

已与 Paimon 团队沟通，Paimon 计划引入将 Postpone Bucket 转换并固定为 Fixed Bucket 的能力。
商业化方案将基于该能力支持 `POSTPONE_MODE`。长期仍需要考虑在 Fluss 中提供原生的 Bucket
Rescale 能力，以适应表接入 Fluss 后的数据增长、并行度调整和负载变化。

### 一致性与写入切换

Create Table on Lake 将 Bulk Load 开始时选定的 Paimon Snapshot 记为 `S_load`。Flink 作业的所有读取、转换和 SST 生成都固定基于 `S_load`，不能在执行过程中切换到新的 Snapshot。

在正式向 Fluss 集群提交 Bulk Load 结果并将表切换为可写之前，系统执行以下检查：

1. 再次读取 Paimon 表的最新 Snapshot，记为 `S_latest`。
2. 比较 `S_latest` 和 `S_load`：
   - 如果 `S_latest == S_load`，说明 Bulk Load 期间没有新的 Paimon Snapshot 提交，允许正式提交 Bulk Load 结果。所有 Bucket 提交成功后，Coordinator Server 将表标记为可写。
   - 如果 `S_latest != S_load`，说明源表在 Bulk Load 期间发生了新的提交。系统中止本次 Bulk Load，不发布生成的 SST，Fluss 表继续保持初始化中且不可写。已上传的临时 SST 和未完成的操作状态由 Bulk Load abort 流程清理。重试时必须选择新的最新 Snapshot，并重新生成全部 SST。
3. 首期不尝试自动读取 `(S_load, S_latest]` 范围内的增量变更。任何 Snapshot ID 变化都按冲突处理。

Bulk Load 成功后，用户不能继续通过 Paimon 原生写入路径直接提交数据或 Schema 变更。后续业务写入统一进入 Fluss；Fluss 自身的 Lake Tiering 仍可按既有协议向 Paimon 提交数据。

- **社区部署**：Fluss 无法强制约束所有外部 Paimon Writer。用户负责在切换前停止这些 Writer，并保证切换后不再恢复直接写入。
- **DLF 托管部署**：DLF 与 Fluss 协调表的写入所有权。表切换到 Fluss 后，DLF 拒绝用户发起的直接 Paimon 提交，只允许 Fluss 授权的写入继续执行。

### 非目标

本 FIP 不定义以下内容：

- 如何扫描 Paimon Snapshot、Manifest、Partition 或 Bucket。
- 如何把 Paimon Row 转换为 Fluss KV 数据。
- 如何生成、上传和管理 RocksDB SST 文件。
- Bulk Load 的 Split、并发、Checkpoint、重试和资源管理机制。
- Bulk Load 的内部 RPC、Protobuf 和提交协议。
- Bulk Load 的性能调优和资源配置。

## 兼容性、弃用与迁移计划

本 FIP 增加新的 Flink Action、`Admin` 方法、`LakeCatalog` 方法和 Coordinator RPC，不改变现有
建表接口的行为，也不弃用现有功能。

现有 Fluss 表和湖表无需迁移。只有主动运行 `create_table_on_lake` Action 的用户会进入新流程。

参与该流程的 Flink connector、Fluss client 和 Coordinator 必须同时支持
`CREATE_TABLE_ON_LAKE`。主键表场景还需要部署兼容的 Bulk Load 实现。版本不匹配时应返回明确
的不支持错误。

本 FIP 的首个实现只支持 Paimon。其他 Lake Storage 在实现元数据映射和相应 Bulk Load 能力后，
可以复用同一个公共接口。

## 被拒绝的替代方案

### 在 Tiering Service 中执行 Bulk Load

一种替代方案是复用 Tiering Service 执行 Bulk Load。Bulk Load 通常需要读取和导入大量历史数据，
执行时间和资源消耗与正常 Tiering 任务不同。将两类任务放在同一个服务中，会使正常 Tiering 的
调度和处理进度变得不可控，影响 Tiering 的稳定性。同时，两类任务共用状态管理和恢复机制，不利于
独立跟踪 Bulk Load 的执行进度，也会增加任务调度、失败恢复和状态管理的代码复杂度。因此，本 FIP
不采用该方案。
