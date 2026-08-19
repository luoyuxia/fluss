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

# Create Log Table on Lake PR 实施计划

本文基于 [FIP：在现有湖表上创建 Fluss 湖流一体表](./fip-create-table-on-lake.md)，只规划第一版 Paimon 日志表支持。

统一入口为独立的 `create_table_on_lake` Flink Action。用户通过命令行传入 Fluss 和 Paimon Catalog 配置，格式与 Tiering Service 一致。Action 直接读取目标 Paimon 表并判断是否包含主键：日志表直接调用 Admin 创建；主键表后续由 Action 启动 Flink Batch 作业并等待完成。第一版尚未实现主键表 Batch 作业，因此遇到主键表时立即返回明确的不支持错误。

## 1. 第一版执行流程

```text
flink run <fluss-flink-jar> create_table_on_lake (...)
    -> Action 根据 --datalake.paimon.* 创建 Paimon Catalog 并读取目标表
    -> 判断 primaryKeys 是否为空
       -> 非空：返回主键表暂不支持
       -> 为空：记录 Paimon 当前 snapshot ID
           -> 调用 Admin.createTableOnLake(...)
           -> Coordinator 再次读取并校验 Paimon 表
           -> 创建同名 Fluss 日志表
           -> 返回创建后的 TableInfo
           -> Action 向 Fluss 注册初始 lake snapshot，Bucket offsets 为空
    -> Action 同步返回成功结果
```

日志表不需要启动 Flink 作业、Bulk Load、SST、初始化状态或写入门禁。Action 返回时，Fluss 表已经创建；如果 Paimon 表存在 snapshot，初始 lake snapshot 也已注册完成。表此时立即可写。

## 2. 支持范围

支持 Paimon append-only 表的以下 Bucket Mode：

| Bucket Mode | Paimon 配置 | Fluss Bucket 数 |
| --- | --- | --- |
| `HASH_FIXED` | `bucket=N`，且 `N > 0` | 默认复用 `N`；用户指定的 `bucket.num` 必须等于 `N` |
| `BUCKET_UNAWARE` | `bucket=-1` | 默认使用 `default.bucket.number`；用户可以指定任意大于 0 的 `bucket.num` |

现有 Paimon 表可以使用 clean schema，也可以是包含 `__bucket`、`__offset` 和 `__timestamp` 的 legacy schema。第一版不会改写湖表 Schema；读取时只过滤 legacy 系统列，不再强制要求系统列存在或位于末尾。

第一版不支持：

- Paimon 主键表。
- `HASH_DYNAMIC`、`KEY_DYNAMIC` 和 `POSTPONE_MODE`。
- Bulk Load 和任何历史数据复制。
- 改写现有 Paimon 表的 Schema、属性或数据。
- Paimon 之外的 Lake Storage。

所有不支持场景必须在创建 Fluss 元数据前失败。

## 3. Public API 和 RPC

### 3.1 `LakeCatalog`

增加读取现有湖表描述的 lake-format-neutral 接口：

```java
TableDescriptor getTableDescriptor(TablePath tablePath) throws TableNotExistException;
```

第一版保持接口简单，不引入新的 metadata result 类型。Coordinator 只通过该接口读取建表所需描述，不负责读取或注册 snapshot。

其他 LakeCatalog 实现可以通过 default method 返回不支持，避免本 PR 为其他 lake format 增加无意义实现。

### 3.2 `Admin`

增加：

```java
CompletableFuture<TableInfo> createTableOnLake(
        TablePath tablePath, Map<String, String> properties);
```

返回值是 Coordinator 完成系统默认值和用户属性合并后实际创建的 `TableInfo`，包含 table ID、最终 Schema、分区键、Bucket Key、Bucket 数和表属性。

future 的完成语义：

- Fluss 表元数据已创建。
- 返回的 `TableInfo` 是 Coordinator 最终创建的表信息。
- 初始 lake snapshot 由 Action 在 future 完成后另行注册。

### 3.3 Coordinator RPC

在 `FlussApi.proto` 增加：

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

response 字段与现有 `GetTableInfoResponse` 对齐，client 复用相同的 `TableInfo.of(...)` 转换逻辑。修改 proto 后通过现有生成流程更新代码，不手工修改生成文件。

### 3.4 Flink Action

在现有 Action SPI 中新增并注册 `create_table_on_lake`：

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

参数规则与 Tiering Service 对齐：

- `--fluss.*` 去掉 `fluss.` 前缀后构造 Fluss `Configuration`。
- `--datalake.format` 第一版必须为 `paimon`。
- `--datalake.paimon.*` 去掉 `datalake.paimon.` 前缀后原样传给 Paimon Catalog，包括 DLF Catalog、认证和 warehouse 配置。
- Action 的 Paimon 配置必须指向 Fluss 集群配置的同一个 Catalog；Coordinator 会使用集群侧配置再次读取并校验目标表。
- `--table` 使用 `database.table` 格式。
- `--table-conf key=value` 可以重复，用于传入 `bucket.num` 等 Fluss 表属性。

Action 先从 Paimon Catalog 获取表：

```java
FileStoreTable table = getPaimonTable(tablePath);
if (!table.schema().primaryKeys().isEmpty()) {
    throw new UnsupportedOperationException(
            "Creating a Fluss table on a Paimon primary-key table "
                    + "is not supported yet.");
}

Long snapshotId = table.latestSnapshot().map(Snapshot::id).orElse(null);
TableInfo tableInfo = admin.createTableOnLake(tablePath, properties).get();
if (snapshotId != null) {
    commitInitialLakeSnapshot(tableInfo, snapshotId);
}
```

Action 通过 `ConnectionFactory` 使用 `--fluss.*` 建立连接，通过与 Tiering Service 相同的 `--datalake.paimon.*` 配置创建 Paimon Catalog。DLF 插件和认证依赖由 Action 运行时 classpath 提供；凭证可以继续使用 Fluss 已有的 Config Provider 语法在进程启动时解析。Action 不从 Coordinator 反向读取敏感 Catalog 配置。

成功结果建议包含 table ID：

```text
Create table on lake succeeded for my_db.my_table, table_id=123
```

后续支持主键表时，只需要把 Action 中的不支持分支替换为“构造、提交并等待 Flink Batch 作业”。日志表 Admin API 和完成语义保持不变。

## 4. 实现步骤

### 4.1 Paimon 元数据读取

在 `PaimonLakeCatalog` 中：

1. 从 Paimon Catalog 获取目标表并转换为 `FileStoreTable`。
2. 映射字段顺序、字段名、类型、nullable、分区键和表属性。
3. 检查 primary key 为空。
4. 通过 `FileStoreTable.bucketMode()` 判断 Bucket Mode，不能只读取 `bucket` 属性。
5. `HASH_FIXED` 读取 Paimon Bucket 数和 Bucket Key，并校验默认 Bucket Function。
6. `BUCKET_UNAWARE` 将最终 Fluss Bucket 数留给 Coordinator 补齐。
7. Action 从同一个 `FileStoreTable` 直接获取 latest snapshot：

```java
return fileStoreTable.latestSnapshot().map(Snapshot::id).orElse(null);
```

空 Paimon 表没有 snapshot 时返回 `null`。

### 4.2 Coordinator 创建日志表

`CoordinatorService.createTableOnLake` 按以下顺序执行：

1. 校验 `TablePath` 和 Fluss database 的 CREATE 权限。
2. 确认 Fluss database 存在且没有同名 Fluss 表。
3. 获取当前 LakeCatalog；第一版要求其格式为 Paimon。
4. 获取 `TableDescriptor`，并再次检查 primary key 为空。Action 的检查用于选择路径，Coordinator 的检查用于保证正确性。
5. 解析、校验并合并用户属性。
6. 处理 Bucket 数：
   - `HASH_FIXED`：省略时复用 Paimon Bucket 数；指定值不相等时拒绝。
   - `BUCKET_UNAWARE`：省略时使用 `default.bucket.number`；指定值必须大于 0。
7. 应用 replication factor 等现有系统默认值。
8. 复用现有 create-table 的 descriptor 校验、assignment、replica capacity 和 `MetadataManager.createTable` 逻辑。
9. 跳过 `LakeCatalog.createTable`，避免创建或覆盖 Paimon 表。
10. 从 `MetadataManager` 读取最终 `TableInfo`，填充 RPC response 并返回。

现有 `CoordinatorService.createTable` 中的默认值、校验、assignment 和元数据创建逻辑应抽取为小范围内部公共方法，避免复制整套建表流程。

### 4.3 Action 注册初始 lake snapshot

初始 lake snapshot 表示接入 Fluss 前已经存在的 Paimon 数据边界：

- snapshot ID 使用 `FileStoreTable.latestSnapshot()` 返回的 ID。
- Bucket offsets 使用空 `Map<TableBucket, Long>`。
- 不为每个 Bucket 写入 `0` 或 `-1`。
- prepare 阶段生成内容为空的 tiered-offsets 文件。
- commit 阶段注册 `{tableId, snapshotId, tieredOffsetsFilePath}`。
- 不生成用于 TabletServer metrics 通知的 `PbLakeTableSnapshotInfo`，因为没有 Fluss log end offset。

Action 直接复用 `FlussTableLakeSnapshotCommitter` 的现有公开方法，执行与正常 Tiering 相同的 prepare/commit 流程：

1. Action 在调用 Admin 前从 Paimon 表读取并保存 latest snapshot ID。
2. Admin 返回 `TableInfo` 后，Action 使用其中的 table ID 和 table path 构造空 offsets prepare 请求。
3. prepare 成功后，使用返回的 offsets file path 和之前保存的 snapshot ID 构造 V2 commit 请求。
4. commit 成功后 Action 返回成功。

调用方式如下：

```java
Map<TableBucket, Long> emptyOffsets = Collections.emptyMap();

try (FlussTableLakeSnapshotCommitter committer =
        new FlussTableLakeSnapshotCommitter(flussConf)) {
    committer.open();

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
}
```

这里不新增 snapshot 提交接口，也不在 Action 中拼装 `PrepareLakeTableSnapshotRequest` 或 `CommitLakeTableSnapshotRequest`。`LakeCommitResult.committedIsReadable(snapshotId)` 表示该 Paimon snapshot 可以直接用于读取；两个 offsets map 都为空，因此不会产生 Fluss log end offset 或 max tiered timestamp。

空 Paimon 表没有 snapshot，跳过初始 snapshot 注册。首次正常 Lake Tiering 创建第一个 lake snapshot。

### 4.4 失败和并发语义

- descriptor 校验、属性校验和 Bucket 校验失败时，不创建 Fluss 元数据。
- snapshot prepare 或 commit 失败时，Action 返回失败。第一版复用现有 snapshot 提交流程，不增加单独的 offsets 文件清理接口。
- 是否自动删除已经创建的 Fluss 表需要单独确认。直接调用 `Admin.dropTable` 可以让用户重试，但从 Admin 返回到清理之间表已经存在，盲目删除可能影响并发访问。第一版建议保留表并返回包含 table ID 的明确错误，由用户确认后删除或补提 snapshot。
- 两个并发请求最多一个成功；本 API 不提供 `ignoreIfExists`。
- 第一次请求完整成功后再次调用，返回 `TableAlreadyExistException`，不重复注册 snapshot。
- Action 读取后的 Paimon 表仍可能发生变化，因此 Coordinator 必须重复检查表类型和兼容性。初始 snapshot 使用 Action 在调用 Admin 前保存的 ID。

第一版不要求阻止外部 Paimon Writer 在建表期间继续提交。Action 记录调用 Admin 前观察到的 latest snapshot，后续外部写入约束由用户文档说明。

### 4.5 Action 参数解析

- `--table` 必须是 `database.table`。
- `--fluss.bootstrap.servers` 必填，其他 `--fluss.*` 配置透传给 Fluss Client。
- `--datalake.format` 必填且第一版仅允许 `paimon`。
- `--datalake.paimon.*` 至少包含目标 Paimon Catalog 所需配置；Action 不枚举 DLF 的供应商配置项。
- `--table-conf` 使用可重复的 `key=value`，trim key 和 value，拒绝空 key、缺少 `=`、重复 key 和空 value。
- table property 的合法性和覆盖规则由 Coordinator 最终校验，Action 不复制配置校验逻辑。
- Admin 异常直接导致 Action 失败，不输出成功结果。

## 5. 测试计划

### 5.1 `PaimonLakeCatalogTest`

- `HASH_FIXED` 的非分区和分区 append-only 表。
- `BUCKET_UNAWARE` 的非分区和分区 append-only 表。
- 字段、nullable、分区键、Bucket Key、Bucket 数和属性逐项映射。
- 主键表、不支持类型、不支持 Bucket Mode 和非 `FileStoreTable`。

### 5.2 Client 和 RPC 测试

- 空 properties 和包含 `bucket.num` 的 request 构造。
- response 的所有字段能够还原出完整 `TableInfo`。
- RPC 序列化、API 注册和旧 server 版本不兼容错误。

### 5.3 `CoordinatorService` 测试

- 两种 Bucket Mode 的默认值和用户覆盖规则。
- 主键表在创建 Fluss 元数据前失败。
- database 不存在、同名表已存在、湖表不存在、无 LakeCatalog 和鉴权失败。
- 验证没有调用 `LakeCatalog.createTable`。
- 返回的 `TableInfo` 与 MetadataManager 中实际保存的信息一致。
- 并发创建最多一个成功。

### 5.4 Flink Action 测试

- Action SPI 注册、名称解析和帮助信息。
- `--fluss.*`、`--datalake.paimon.*` 和重复 `--table-conf` 的解析与前缀移除。
- Action 能识别 append-only 表并调用 Admin。
- Action 识别主键表后直接失败，且没有调用 Admin 创建表。
- Action 从有数据的 Paimon 表保存 latest snapshot ID，并在 Admin 返回后提交相同 ID。
- 空 Paimon 表的 snapshot ID 为 `null`，Action 跳过 snapshot 提交。
- 初始 snapshot 的 tiered Bucket offsets 为空。
- snapshot prepare/commit 失败时 Action 返回包含 table ID 的错误；已创建的 Fluss 表按 4.4 的约定保留。
- 缺少 Fluss/Paimon 配置、非法 table path、非法或重复 table property 和 Admin 异常传播。
- 各支持 Flink 版本的 Action SPI 文件都包含该 Factory。

### 5.5 Paimon 端到端测试

1. 创建包含历史数据的 Paimon append-only 表并记录 snapshot ID。
2. 使用与 Tiering Service 相同的 Fluss/Paimon 参数运行 `create_table_on_lake` Action。
3. 验证返回的 table ID 与 Fluss `TableInfo` 一致。
4. 验证没有创建第二张 Paimon 表，原表和历史数据不变。
5. 验证 Fluss latest lake snapshot ID 等于建表前记录的 Paimon snapshot ID，Bucket offsets 为空。
6. Action 返回后立即向 Fluss 表写入新数据。
7. 验证实时读取可见新数据，lake 路径可见原有历史数据。
8. 执行正常 Lake Tiering，验证新数据进入同一张 Paimon 表，新 snapshot 携带实际 Fluss log offsets。

分别覆盖 `HASH_FIXED` 和 `BUCKET_UNAWARE`。

## 6. PR 和 commit 组织

建议用一个 PR 完成，按以下 commit 组织：

1. `[paimon] Read existing append-only table metadata`
2. `[rpc] Add create-table-on-lake Admin API returning TableInfo`
3. `[server] Create Fluss log table on existing lake table`
4. `[flink] Add action and commit initial lake snapshot`
5. `[test] Add create-table-on-lake integration coverage`

如果单个 PR 过大，可以拆成：

- PR 1：LakeCatalog 和 Paimon 元数据读取。
- PR 2：Admin、RPC、Coordinator、Action 和端到端测试。

PR 1 不注册用户入口；PR 2 合入后一次性开放完整日志表能力。

## 7. 验证命令

```bash
./mvnw spotless:check
./mvnw validate
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server -am
./mvnw verify -pl fluss-lake/fluss-lake-paimon -am
./mvnw verify -pl fluss-flink/fluss-flink-common -am
```

提交前检查完整 diff，确认没有引入主键表 Batch 作业、Bulk Load、初始化状态或无关重构。

## 8. 完成定义

- Action 能直接识别日志表和主键表，第一版只允许日志表继续创建。
- `Admin.createTableOnLake` 返回最终创建的 `TableInfo`。
- `HASH_FIXED` 和 `BUCKET_UNAWARE` 的 Bucket 规则符合 FIP。
- 不支持的表在创建 Fluss 元数据前失败。
- 不创建、覆盖或改写原 Paimon 表。
- 有历史 snapshot 时，Fluss 记录相同 snapshot ID，初始 Bucket offsets 为空。
- snapshot 注册失败时 Action 返回包含 table ID 的明确错误，并保留已经创建的 Fluss 表；用户排查后可以删除该表再重试。
- Action 返回时表已经可写，不存在后台初始化任务。
- 后续首次正常 tiering 能从该边界继续推进 snapshot 和实际 log offsets。
- API、RPC、Coordinator、Action 和端到端测试全部通过。
