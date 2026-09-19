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

# FIP-25 日志表阶段 PR 实施计划

本文基于 [FIP-25](./fip-create-table-on-lake-en.md)，规划第一阶段将已有 Paimon
append-only 表原地提升为启用 Datalake 的 Fluss 日志表。第一阶段提供
`sys.enable_fluss_on_lake_table` Procedure，但只接受日志表；主键表及其 Bulk Load
初始化流程留到后续 PR。

## 1. 目标和边界

本 PR 交付以下完整闭环：

1. 从已有 Paimon append-only 表读取并校验表定义。
2. 创建同名、初始状态为 lake-disabled 的 Fluss 日志表。
3. 如果 Paimon 表已有 snapshot，将该 snapshot 作为初始 lake snapshot 注册到 Fluss，
   Bucket offsets 为空。
4. 服务端校验 Fluss 与 Paimon 的 Schema 和 snapshot 一致后启用 datalake。
5. Procedure 同步返回成功；返回时表已完成提升，可以通过 Fluss 读写。

第一阶段支持：

- Paimon append-only 表，包括非分区表和分区表。
- Paimon `HASH_FIXED` 和 `BUCKET_UNAWARE` Bucket Mode。
- 不包含 Fluss legacy 系统列的 clean schema。
- 空 Paimon 表和已经包含历史数据的 Paimon 表。
- 用户通过 `properties` 补充或覆盖允许修改的 Fluss 表属性。

第一阶段不支持：

- 任何带主键的 Paimon 表。
- Bulk Load、SST 生成、KV snapshot 初始化和初始化状态机。
- `partitions` 参数；它只用于主键表的 Bulk Load 范围选择。
- Paimon `HASH_DYNAMIC`、`KEY_DYNAMIC` 和 `POSTPONE_MODE`。
- 使用 `upsert-key`、`rowkind.field`、row tracking、data evolution、deletion vectors 或列默认值的
  Paimon append-only 表。这些表虽然没有主键，但其更新、删除或写入语义不能直接映射为 Fluss
  日志表。
- Paimon 以外的 Lake Storage。
- 改写已有 Paimon 表的数据、Schema、分区或 Bucket 配置。
- 自动迁移仍在运行的原生 Paimon Writer。

所有不支持的表定义必须在创建 Fluss 元数据前失败。用户执行 Procedure 前必须停止原生
Paimon Writer 和直接 Schema 变更；Procedure 成功后，应用写入统一切换到 Fluss。

## 2. 用户接口

注册 FIP 定义的 Procedure：

```sql
CALL sys.enable_fluss_on_lake_table('my_db.my_log_table');

CALL sys.enable_fluss_on_lake_table(
    'my_db.my_log_table',
    'bucket.num=16,table.log.format=compacted'
);
```

第一阶段只注册以下两个签名：

```text
enable_fluss_on_lake_table(table STRING)
enable_fluss_on_lake_table(table STRING, properties STRING)
```

后续主键表 PR 再增加包含 `partitions` 的签名。已有两个签名保持不变。

Procedure 返回一行 `message STRING`：

```text
Successfully enabled Fluss on Paimon table 'my_db.my_log_table'.
```

日志表流程不提交 Flink 作业，因此：

- Procedure 始终同步执行。
- 不返回 Flink Job ID。
- `table.dml-sync` 不改变本阶段行为。

## 3. 端到端流程

```text
CALL sys.enable_fluss_on_lake_table(table, properties)
  │
  └─ EnableFlussOnLakeTableProcedure
       │
       ├─ 解析 table 和 properties
       ├─ Admin.createTableOnLake(tablePath, properties)
       │    │
       │    └─ Coordinator + PaimonLakeCatalog
       │         ├─ 读取并校验 Paimon 表定义
       │         ├─ 主键表或不支持的 Bucket Mode → 创建前失败
       │         └─ 创建 lake-disabled Fluss 日志表并返回 TableInfo
       │
       ├─ 新创建的表
       │    ├─ 读取当前 Paimon snapshot ID
       │    ├─ snapshot 存在 → 以空 Bucket offsets 注册初始 snapshot
       │    └─ snapshot 不存在 → 跳过注册
       │
       ├─ 已存在的 Fluss 表
       │    └─ 跳过创建和初始 snapshot 注册
       │
       └─ Admin.alterTable(table.datalake.enabled=true)
            │
            ├─ 校验 Fluss/Paimon Schema
            ├─ 校验 Paimon snapshot == Fluss 已注册 snapshot
            └─ 校验通过 → 启用 datalake → 返回成功
```

这个顺序保证 datalake 只在初始 snapshot 已注册后启用。已有 Paimon 表不会被重新创建，
历史数据也不会被重新写入 Paimon。

## 4. 代码改动

### 4.1 LakeCatalog 元数据读取接口

在 `fluss-common` 的 `LakeCatalog` 增加一个 lake-format-neutral 方法：

```java
default TableDescriptor getTableDescriptor(TablePath tablePath)
        throws TableNotExistException {
    throw new UnsupportedOperationException(
            "Reading table metadata is not supported by this lake catalog.");
}
```

`PluginLakeStorageWrapper.ClassLoaderFixingLakeCatalog` 必须在插件 ClassLoader 上下文中转发
该方法。其他 LakeCatalog 实现依靠 default method 保持兼容；第一阶段只有 Paimon 实现该方法。

这个接口及 Paimon 映射单独放在 PR 1。PR 1 不增加 Admin/RPC，不创建 Fluss 表，也不读取
snapshot。这样 review 可以先确定映射契约，再让后续控制面代码依赖一个已经稳定的
`TableDescriptor`。

### 4.2 Paimon 表定义映射

在 `PaimonLakeCatalog` 中实现：

1. 根据 `TablePath` 获取 Paimon 表并转换为 `FileStoreTable`。
2. 检查 `schema().primaryKeys()` 为空。非空时抛出包含表名的明确不支持错误。
3. 通过 `CoreOptions` 拒绝 `upsert-key`、`rowkind.field`、row tracking、data evolution 和
   deletion vectors 等不兼容的 append-table 语义，并拒绝带列默认值的 Schema。
4. 映射字段顺序、字段名、类型、nullable 和分区键。
5. 字段原样映射。Paimon mapper 不单独处理 Fluss legacy 系统列；如果 Schema 包含
   `__bucket`、`__offset` 或 `__timestamp`，创建 Fluss 元数据时由现有
   `TableDescriptorValidation` 的保留列名检查拒绝。
6. 使用 `FileStoreTable.bucketMode()` 判断 Bucket Mode：
   - `HASH_FIXED`：复用 Paimon Bucket 数和 Bucket Key；只接受兼容的默认 Bucket
     Function。
   - `BUCKET_UNAWARE`：不从 Paimon 推导 Bucket 数，由 Coordinator 应用用户值或
     `default.bucket.number`。
   - 其他 Bucket Mode：创建 Fluss 元数据前拒绝。
7. 从 typed metadata 派生兼容的时间分区属性，不把 Paimon raw option 或其中的 `fluss.*`
   option 复制到 `TableDescriptor`。

表定义读取每次都从当前 Paimon 表执行，不依赖缓存的 `TableInfo`。

#### Option 策略

Paimon source option 不采用“未分类即拒绝”的严格白名单。Paimon 后续可能增加只影响 compaction、
snapshot 保留或其他内部行为的 option；仅因为当前 Fluss 版本不认识该 key 就拒绝，会造成不必要的
前向兼容问题。

采用“typed metadata 派生 + 有效语义校验”的策略：

1. **不复制 raw option**：包括 `fluss.*` 在内的 Paimon raw option 都不写入
   `TableDescriptor`。普通 Fluss properties 来自 promotion 请求或 server 默认值。
2. **校验实际能力**：通过 Paimon typed API 检查最终生效的表定义，例如 `primaryKeys()`、
   `bucketMode()`、Bucket Function、Schema 和分区定义。只要实际能力不在
   支持矩阵内，就拒绝 promotion。
3. **派生受支持语义**：只从 Paimon typed API 派生兼容的 Fluss auto-partition 定义。
   Paimon partition expiration 不映射成 Fluss retention，两侧生命周期独立配置。
4. **忽略其他 source option**：不因为 Fluss 不认识 raw option 而拒绝；这些 option 继续保留在
   原 Paimon 表中。

例如，未来 Paimon 新增一个只控制 compaction 调度的 option，只要当前 Paimon runtime 能正常加载
该表，并且表的 Schema、Bucket Mode、Bucket Function 和分区语义仍通过校验，promotion 不应失败。
如果新 option 改变了 Bucket Mode 等可观察语义，则由 typed API 的结果触发拒绝，而不是依赖
Fluss 预先知道该 option 名称。

Procedure 的 `properties` 按属性所有权校验，不再维护一份与 `ConfigOptions` 重复的固定白名单：

1. `bucket.num` 作为建表特殊参数，按 `HASH_FIXED`/`BUCKET_UNAWARE` 规则处理，不写入普通
   properties map。
2. 当前 server 版本能够识别的 `table.*` 属性进入现有 `TableDescriptorValidation`，复用其类型、
   取值和表类型校验。未知 `table.*` 沿用现有错误，提示当前集群版本不支持。
3. `table.datalake.enabled` 和自定义 lake database/table 映射由 promotion 流程管理，无条件拒绝。
   `table.datalake.format` 和 `table.datalake.historical-partition.enabled` 使用现有 table
   validation。
4. 从 Paimon 推导出的 auto-partition 属性只允许省略或显式提供相同值；冲突值拒绝。Paimon
   无法推导 auto-partition 属性时，调用方可以提供完整配置，并由 `TableDescriptorValidation`
   校验。Schema、分区键和 Bucket Key 不属于该参数，不能覆盖。lake format 由当前集群配置自动设置。
5. 仅适用于特定表类型的 Fluss 属性交给 `TableDescriptorValidation` 统一校验。
6. `paimon.*` 和非 `table.*` 的 custom property 由现有 table property 白名单拒绝。这个参数只表示 Fluss table
   properties；如果以后需要 custom properties，应通过明确的 API 契约单独支持。

Procedure 参数中的 Fluss properties 仍由当前 server 的 `FlussConfigUtils.TABLE_OPTIONS` 和
`TableDescriptorValidation` 校验。这一白名单只处理调用方提交的属性，不读取 Paimon source
options。

PR 1 同时提交一张可评审的 mapping matrix，记录需要映射的 option、需要校验的有效语义、已知
不兼容项、映射出的 Fluss 属性和拒绝原因。测试逐项覆盖该矩阵，并增加一个未知但不影响表定义的
source option，验证它不会进入 `TableDescriptor`，也不会阻止 promotion。

### 4.3 Admin API 和 RPC

在 `Admin` 增加：

```java
CompletableFuture<TableInfo> createTableOnLake(
        TablePath tablePath, Map<String, String> properties);
```

增加 FIP 定义的 `CREATE_TABLE_ON_LAKE` RPC，API key 为 `1068`，协议版本为 `0`：

```protobuf
message CreateTableOnLakeRequest {
  required PbTablePath table_path = 1;
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

需要同步修改：

- `FlussApi.proto` 和生成流程。
- `ApiKeys`、RPC message wrapper 和 `AdminGateway`。
- `FlussAdmin` 请求构造和 response 到 `TableInfo` 的转换。
- server RPC 注册与 `CoordinatorService.createTableOnLake()`。

生成文件通过项目现有 protogen 流程更新，不手工编辑。

### 4.4 Coordinator 创建 Fluss 日志表

`CoordinatorService.createTableOnLake()` 在现有 Coordinator write lock/event 语义内按以下
顺序处理：

1. 校验 `TablePath`。目标 Fluss database 已存在时鉴权 database 的 `CREATE` 操作；不存在时
   改为鉴权集群的 `CREATE` 操作。
2. 确认集群已配置 Paimon Lake Storage，并且同名 Fluss 表不存在。
3. 调用 `LakeCatalog.getTableDescriptor()` 读取 Paimon 表定义；Paimon lake catalog 在转换
   阶段拒绝当前不支持的主键表。
4. 解析并合并用户属性：
   - 拒绝 `table.datalake.enabled` 和自定义 lake database/table 映射。
   - `table.datalake.format` 和 `table.datalake.historical-partition.enabled` 使用现有 table
     validation。
   - Schema、主键、分区键和 Bucket Key 只能由 Paimon 表推导；lake format 使用当前集群配置。
   - 显式设置的推导属性必须与 Paimon 定义一致。
5. 处理 Bucket 数：
   - `HASH_FIXED`：未指定时复用 Paimon Bucket 数；指定值不相等时拒绝。
   - `BUCKET_UNAWARE`：未指定时使用 `default.bucket.number`；指定值必须大于 0。
6. 不写入 `table.datalake.enabled`，使用其默认值 `false`，并通过现有系统默认逻辑应用 lake
   format、replication factor 等配置。
7. 如果目标 Fluss database 不存在，使用默认 `DatabaseDescriptor` 自动创建；通过
   `ignoreIfExists=true` 处理并发建库。
8. 复用普通建表路径的 descriptor 校验、replica capacity 检查、assignment 和
   `MetadataManager.createTable()`。
9. 跳过 `LakeCatalog.createTable()`，避免创建或覆盖原 Paimon 表。
10. 从 `MetadataManager` 读取最终 `TableInfo` 并填充 RPC response。

应把普通 `createTable()` 中可复用的 defaults、validation、assignment 和 metadata
creation 逻辑抽取为小范围私有方法，避免长期维护两份建表实现。重构只覆盖两条创建路径
共同需要的代码。

### 4.5 注册日志表初始 snapshot

在负责启用校验的 PR 3 中，给 `LakeCatalog` 增加 FIP 定义的：

```java
default Optional<Long> getLatestDataChangeSnapshotId(TablePath tablePath)
        throws TableNotExistException {
    throw new UnsupportedOperationException(
            "Reading data-change snapshots is not supported by this lake catalog.");
}
```

`PaimonLakeCatalog` 返回最新 `APPEND` 或 `OVERWRITE` snapshot 的 ID，并跳过不改变逻辑数据的
`COMPACT` 和 `ANALYZE` snapshot；没有 data-change snapshot 时返回 `Optional.empty()`。
`ClassLoaderFixingLakeCatalog` 在插件 ClassLoader 上下文中转发该方法。这个 API 与 snapshot
一致性校验放在同一个 PR，避免把它混入表定义映射 PR。

Procedure 在新 Fluss 表创建成功后读取 Paimon 当前 data-change snapshot ID。存在 snapshot 时复用
`FlussTableLakeSnapshotCommitter` 的 prepare/commit 协议：

```java
Map<TableBucket, Long> emptyOffsets = Collections.emptyMap();

String offsetsPath =
        committer.prepareLakeSnapshot(
                tableInfo.getTableId(), tableInfo.getTablePath(), emptyOffsets);

committer.commit(
        tableInfo.getTableId(),
        tableInfo.getTablePath(),
        LakeCommitResult.committedIsReadable(snapshotId),
        offsetsPath,
        emptyOffsets,
        Collections.emptyMap());
```

初始 snapshot 的约束：

- snapshot ID 是 Procedure 在 Fluss 表创建完成后观察到的 Paimon data-change snapshot。
- tiered offsets 和 max tiered timestamps 都为空。
- 不为每个 Fluss Bucket 填写 `0`、`-1` 或 Paimon Bucket offset。
- 空 Paimon 表没有 snapshot，直接跳过该步骤。
- 不新增另一套 snapshot RPC，不在 Procedure 中复制 prepare/commit 消息拼装逻辑。

为了创建 Paimon Catalog 和 snapshot committer，`FlinkCatalog.getProcedure()` 需要向
Procedure 注入以下上下文，而不再只注入 `Admin`：

- 由 bootstrap servers 和 security options 构造的 Fluss `Configuration`。
- 当前 Fluss Catalog 的 Paimon Catalog properties 的不可变副本。
- Catalog ClassLoader。

可以引入一个不可变的 `FlussProcedureContext`，由 `ProcedureManager` 统一注入。
普通 Procedure 继续只使用其中的 `Admin`；新 Procedure 根据 `TableInfo` 中的 `PAIMON` format，
通过 `LakeStoragePluginSetUp` 创建 `LakeStorage` 和 `LakeCatalog`，再调用
`getLatestDataChangeSnapshotId()`。Procedure 负责关闭创建的 `LakeCatalog` 和 snapshot committer。上下文
不得在日志或异常中输出认证信息。

### 4.6 服务端启用前校验

Procedure 注册初始 snapshot 后，通过 `Admin.alterTable()` 设置：

```text
table.datalake.enabled=true
```

当 `false -> true` 且目标 Paimon 表已经存在时，Coordinator 在持久化属性前逐项检查：

1. 读取最新 Fluss `TableInfo` 和目标 Paimon `TableDescriptor`。
2. 按建表映射规则验证 Schema、分区键和 Bucket 定义兼容。
3. 通过 `LakeCatalog.getLatestDataChangeSnapshotId()` 读取最新 Paimon data-change snapshot。
4. 从 Fluss lake table metadata 读取已注册的最新 snapshot。
5. 两边都有 data-change snapshot 且 ID 相等，或者两边都没有 data-change snapshot 时允许启用。
   已注册 snapshot 之后只有 `COMPACT` 或 `ANALYZE` snapshot 时也允许启用。
6. 只有一边存在 data-change snapshot，或者 ID 不相等时抛出
   `InvalidAlterTableException`。

校验必须发生在 Paimon Catalog 变更和 Fluss 属性持久化之前。校验失败时，Fluss 表保持
lake-disabled。对已经启用 datalake 的表再次设置 `true` 保持现有幂等行为。

### 4.7 Procedure 实现

新增 `EnableFlussOnLakeTableProcedure` 并在 `ProcedureManager` 注册
`sys.enable_fluss_on_lake_table`。

Procedure 负责：

1. 解析 `database.table`，拒绝缺少 database、空 identifier 和多余层级。
2. 解析逗号分隔的 `key=value` properties：trim key/value，拒绝空 key、缺少 `=`、
   重复 key 和空 value。第一阶段不支持在 key/value 中转义逗号。
3. 调用 `Admin.createTableOnLake()`。
4. 新建成功时读取 Paimon snapshot，并按 4.5 注册初始 snapshot。
5. 收到 `TableAlreadyExistException` 时获取现有 `TableInfo`，跳过创建和 snapshot 注册，
   直接进入服务端校验启用流程。
6. 调用 `Admin.alterTable()` 启用 datalake。
7. 返回成功消息。

Procedure 不自行判断主键表是否可支持。服务端必须在创建元数据前拒绝主键表，Procedure
只保留清晰的异常传播和用户提示。

## 5. 失败、并发和重试语义

| 失败位置 | 最终状态 | 处理方式 |
| --- | --- | --- |
| 表定义、属性或 Bucket 校验 | 没有 Fluss 表 | 修正输入后重试 |
| Fluss 元数据创建 | 没有 Fluss 表，或由现有事务语义决定 | 传播原始错误 |
| 初始 snapshot prepare/commit | Fluss 表存在但 lake-disabled | 返回包含 table ID 的错误，不自动删除表 |
| 启用前 Schema/snapshot 校验 | Fluss 表存在但 lake-disabled | 停止外部 Paimon 写入并排查不一致 |
| datalake 属性持久化 | 保持原状态 | 传播 `alterTable` 错误 |

补充约束：

- 两个并发的首次调用最多一个成功创建 Fluss 表。
- 完整成功后的重复调用通过已有表分支和幂等 `alterTable(true)` 返回成功。
- 在 snapshot 注册前失败后，重复调用不会覆盖或猜测初始 snapshot；用户需确认并删除残留
  lake-disabled Fluss 表后重试。第一阶段不自动删除，因为并发调用可能已经开始使用该表。
- Procedure 观察 snapshot 后如果有原生 Paimon Writer 提交新 snapshot，服务端启用前校验
  会拒绝操作。用户必须完成写入所有权切换后重试。
- 如果启用前 Paimon Schema 发生变化，Schema 校验拒绝操作。

## 6. 测试计划

### 6.1 LakeCatalog 和 Paimon 映射

在 `PaimonLakeCatalogTest` 覆盖：

- 分区的 `HASH_FIXED` append-only 表。
- 非分区的 `BUCKET_UNAWARE` append-only 表。
- 字段顺序、类型、nullable、分区键、Bucket Key、Bucket 数和允许映射的属性。
- clean schema。
- 空表返回空 snapshot；有数据表返回准确的 latest snapshot ID。
- 主键表、不支持的数据类型和 Bucket Function。
- 已知不兼容 option 被拒绝；未知且不改变有效表定义的 source option 被忽略。
- 表不存在时转换为 Fluss `TableNotExistException`。

在 `LakeStorageTest` 覆盖 wrapper 对两个新增方法的委托和 ClassLoader 上下文。

### 6.2 Admin、RPC 和 Coordinator

- request 支持空 properties 和多个 properties。
- response 的全部字段能够还原完整 `TableInfo`。
- RPC 序列化、API key 注册、权限和旧 server 不支持新 API 的行为。
- database 不存在时自动创建、同名 Fluss 表已存在、Paimon 表不存在和未配置 Paimon Lake
  Storage。
- 主键表、不支持的 Bucket Mode 和 legacy system columns 在 Fluss 元数据创建前失败。
- `HASH_FIXED` Bucket 一致性；`BUCKET_UNAWARE` 默认值和用户覆盖。
- 验证 `createTableOnLake()` 没有调用 `LakeCatalog.createTable()`。
- 并发创建最多一个成功。
- 返回的 `TableInfo` 与 `MetadataManager` 中保存的内容一致。
- 启用时 data-change snapshot 的相等、双方为空、仅一方存在、ID 不同，以及只有后续
  compaction snapshot 的场景。
- 校验失败时 `table.datalake.enabled` 仍为 `false`。

### 6.3 Procedure

- Procedure 列表、查找和两个签名。
- `table` 和 `properties` 的合法与非法输入。
- properties trim、重复 key、空 key/value、缺少 `=` 和不支持的逗号转义。
- 新表有 snapshot：注册相同 snapshot ID，两个 offsets map 为空，然后启用。
- 新表无 snapshot：跳过注册，直接通过双方为空的校验启用。
- 已有且已成功提升的表：重复调用成功，不重复提交 snapshot。
- 主键表和 `partitions` 使用给出明确的不支持信息。
- snapshot prepare/commit、Admin API 和 alterTable 异常正确传播。
- Fluss/Paimon Catalog 凭证不会出现在日志和错误消息中。
- 各受支持 Flink 版本能够发现并执行 Procedure。

### 6.4 Paimon 端到端测试

至少覆盖以下矩阵：

| 表类型 | Bucket Mode | 分区 | 初始数据 | 预期 |
| --- | --- | --- | --- | --- |
| append-only | `HASH_FIXED` | 否 | 有 | 成功并复用 Bucket 数 |
| append-only | `HASH_FIXED` | 是 | 有 | 成功并保留分区定义 |
| append-only | `BUCKET_UNAWARE` | 否 | 有 | 使用默认或显式 Fluss Bucket 数 |
| append-only | 任一支持模式 | 任意 | 空 | 无初始 snapshot，成功启用 |
| primary-key | 任意 | 任意 | 任意 | 创建 Fluss 元数据前拒绝 |

每个成功用例验证：

1. Paimon table UUID/path、Schema、属性和历史数据没有改变。
2. 没有创建第二张 Paimon 表。
3. Fluss `TableInfo` 的 Schema、分区键、Bucket 和属性符合映射规则。
4. Fluss latest lake snapshot 等于 Procedure 观察到的 Paimon snapshot，offsets 为空。
5. Procedure 返回时 `table.datalake.enabled=true`。
6. 通过 Fluss 写入新数据并执行正常 Lake Tiering 后，新 snapshot 仍进入同一张 Paimon 表，
   并开始携带真实 Fluss log offsets。
7. 完整成功后再次调用 Procedure，结果保持幂等。

## 7. PR 拆分

建议拆成五个可以独立评审和验证的 PR。前三个 PR 合入后再注册用户入口，避免 Procedure 暴露
尚未闭环的流程。

### PR 1：`[paimon] Map existing Paimon log table metadata to Fluss`

范围：

- 增加 `LakeCatalog.getTableDescriptor(TablePath)` default method。
- 在 `ClassLoaderFixingLakeCatalog` 中转发该方法。
- 在 `PaimonLakeCatalog` 完成 append-only 表的 Schema、分区、Bucket 和时间分区语义转换。
- 引入有效语义校验和 mapping matrix；不复制 Paimon raw options。
- 拒绝主键表、不支持的类型、Bucket Mode、Bucket Function 和已知不兼容语义；不因未知
  source option 本身而失败。

测试只验证读取与映射，不创建 Fluss 表。该 PR 合入后没有新的用户入口。

### PR 2：`[server] Add create-table-on-lake control-plane API`

范围：

- 增加 API key `1068` 的 RPC、`Admin.createTableOnLake()` 和 `FlussAdmin` 实现。
- Coordinator 按属性所有权合并用户 `properties`，并复用当前 server 的
  `FlussConfigUtils.TABLE_OPTIONS` 和 `TableDescriptorValidation`。
- 复用普通建表的 defaults、validation、assignment 和 metadata creation。
- 创建 lake-disabled Fluss 日志表并返回最终 `TableInfo`，不调用
  `LakeCatalog.createTable()`。
- 服务端再次拒绝主键表，保证即使调用方绕过 Procedure 也不会留下元数据。

该 PR 的成功语义只到 Fluss 元数据创建完成，符合 Admin API 的契约；仍不注册 Procedure。

### PR 3：`[server] Validate an existing Paimon table before enabling datalake`

范围：

- 增加 `LakeCatalog.getLatestDataChangeSnapshotId()`、Paimon 实现和 ClassLoader wrapper。
- 在 `false -> true` 的 `alterTable` 路径中复用 PR 1 的 descriptor 映射，逐项校验 Schema、
  分区和 Bucket 定义。
- 比较 Paimon latest data-change snapshot 与 Fluss 已注册 snapshot，忽略后续 compaction
  和 statistics snapshot。
- 校验通过前不修改 Paimon Catalog，也不持久化 Fluss 属性。
- 保留普通新表启用 datalake 时创建 Paimon 表的原有分支。

这个 PR 测试 data-change snapshot 不匹配和只新增 compaction snapshot 的场景，以及校验失败
时属性未被修改。

### PR 4：`[flink] Add log-only enable-fluss-on-lake-table procedure`

范围：

- 扩展 Procedure context，提供 Fluss configuration 和当前 Paimon Catalog 配置。
- 注册只有 `table` 和 `table, properties` 两个签名的
  `sys.enable_fluss_on_lake_table`。
- 串联建表、日志表初始 snapshot 注册和 `alterTable(true)`。
- 主键表、`partitions` 和 Bulk Load 均返回明确的不支持错误。
- 覆盖失败残留、重试和完整成功后的幂等行为。

该 PR 合入时，日志表路径形成可用的同步闭环，不提交 Flink 作业。

### PR 5：`[test] Verify Paimon log table promotion end to end`

范围：

- 覆盖 `HASH_FIXED`、`BUCKET_UNAWARE`、分区/非分区、空表/有历史数据。
- 验证原 Paimon 表没有被创建、覆盖或改写。
- 验证初始 snapshot、空 offsets、启用状态和重复调用。
- 验证首次正常 Lake Tiering 继续写入同一张 Paimon 表。
- 补齐 Procedure 用户文档、支持矩阵和停写原生 Paimon Writer 的操作说明。

如果仓库要求功能 PR 自带端到端测试，可以把 PR 5 的对应场景并入 PR 4；PR 1 到 PR 3 的
单元测试和模块级集成测试仍分别留在各自 PR 中。

## 8. 验证命令

```bash
./mvnw spotless:check
./mvnw validate

./mvnw verify \
  -pl fluss-common,fluss-rpc,fluss-client,fluss-server \
  -am

./mvnw verify \
  -pl fluss-lake/fluss-lake-paimon \
  -am

./mvnw verify \
  -pl fluss-flink/fluss-flink-common \
  -am
```

根据实际受影响的 Flink 版本模块补充 procedure 集成测试。修改 protobuf 后先执行项目规定的
protogen 流程，再运行上述 reactor。

## 9. 完成定义

- `sys.enable_fluss_on_lake_table` 能提升支持范围内的 Paimon append-only 表。
- 主键表和其他不支持表型在任何 Fluss 元数据产生前失败。
- 已有 Paimon 表及历史数据保持原位，没有被创建、覆盖或改写。
- Fluss 创建结果与 Paimon Schema、分区和 Bucket 定义兼容。
- 有历史 snapshot 时注册相同 snapshot ID，初始 Bucket offsets 为空。
- 空表不创建虚假的 snapshot 或 offsets。
- 服务端只在 Schema 和 data-change snapshot 一致时启用 datalake。
- Procedure 成功返回时表已经可用，不存在后台初始化任务。
- 完整成功后的重复调用保持幂等。
- 首次正常 tiering 能从已注册的 snapshot 边界继续推进，并写入真实 Fluss offsets。
- 所有相关单元测试、RPC 测试、Procedure 测试和 Paimon 端到端测试通过。
