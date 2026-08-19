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

# Create Table on Lake PR 实施计划

> 本文保留早期的全量拆分草案。第一版日志表实现及当前 Action 入口以 [Create Log Table on Lake PR 实施计划](./pr-plan-create-log-table-on-lake.md) 为准；本文中的 Procedure 和旧完成语义不再作为第一版实现依据。

本文基于 [FIP：在现有湖表上创建 Fluss 湖流一体表](./fip-create-table-on-lake.md)，将实现拆分为可独立审查、验证和回滚的 Pull Request。首期范围只包含 Paimon；主键表的数据读取、SST 生成、上传、提交和恢复依赖独立的 Bulk Load 设计与实现。

## 1. 实施原则

- 先确定跨模块协议和表状态，再实现各层逻辑，避免 client、server 和 Flink 对完成语义产生不同理解。
- 公共 API 保持 lake format 无关，Paimon 特有的 Bucket Mode、类型和属性映射放在 `fluss-lake-paimon`。
- 每个 PR 都应能独立编译并包含对应单元测试；未接通的能力不注册为用户可调用的 procedure。
- 日志表可以先形成端到端闭环。主键表必须在 Bulk Load 和初始化状态切换能力可用后才对用户开放。
- 不手工修改 Protobuf 生成文件；修改 `FlussApi.proto` 后通过项目现有代码生成流程更新生成结果。

## 2. 合入前必须确认的问题

### P0-1：Flink 作业与 Coordinator 的职责边界

FIP 当前同时包含以下约定：

1. `Admin.createTableOnLake()` 返回 `CompletableFuture<Void>`，主键表在 Fluss 表创建且 Bulk Load 请求被受理后完成。
2. procedure 启动的 Flink 作业在 Admin 调用返回后读取 Paimon Snapshot、生成 SST，并提交 Bulk Load。

按这两个定义逐步检查：

1. Flink 作业需要固定读取 Coordinator 建表时选定的 `S_load`。
2. `Void` 返回值不包含 `S_load`、解析后的表描述、初始化 ID 或待处理 Bucket 信息。
3. Flink 作业因此无法仅凭该 API 确认读取边界，也无法把后续提交关联到本次初始化。
4. 所以，在实现 RPC 前必须补齐 Flink 作业获取初始化上下文的协议。

推荐由独立 Bulk Load 设计定义 `initializationId`、`snapshotId`、目标表信息和提交入口。`createTableOnLake` 返回值可以扩展为结果对象，或者创建表后由初始化 ID 查询上下文。最终选择需要同步更新 FIP 中的 Admin API、RPC 和 procedure 完成语义。

### P0-2：初始化状态的持久化模型

当前 `TableInfo`/Catalog 元数据没有 FIP 所述的 `INITIALIZING` 表生命周期。需要确认：

- 状态存放在表元数据、独立初始化记录，还是 Bulk Load 任务元数据中。
- Coordinator failover 后如何恢复状态。
- TabletServer 如何获知并拒绝写入，读请求是否允许。
- 日志表是否直接创建为可写状态。
- Bulk Load 成功、失败、取消和重试分别如何迁移状态。
- 用户能否删除初始化中的表，以及删除时如何清理任务和临时文件。

状态模型和写入门禁应由 Bulk Load PR 先提供，或与控制面协议 PR 同时提供。不能用普通表属性临时代替持久化状态。

### P0-3：数据库和湖表命名语义

需要明确 Lake Catalog 中存在 `database.table`、但 Fluss 中 database 不存在时的行为。建议沿用 `createTable`：返回 `DatabaseNotExistException`，不隐式创建 database。还需要确认湖表大小写、quoted identifier 和 Fluss `TablePath.validate()` 的一致性规则。

### P0-4：属性覆盖白名单

FIP 允许 `properties` 覆盖或补充湖表推导值，但 Schema、主键、分区键、lake format、Bucket Key 和部分 Bucket 配置不能被任意覆盖。需要在编码前列出：

- 可覆盖的 Fluss 属性。
- 只允许由湖表推导的属性。
- 冲突时拒绝的属性及异常消息。
- 未识别、重复和空白 `key=value` 的处理规则。

## 3. PR 拆分总览

| PR | 目标 | 主要模块 | 依赖 | 用户可见 |
| --- | --- | --- | --- | --- |
| PR 1 | 定义元数据读取接口与 Paimon 映射 | `fluss-common`, `fluss-lake-paimon` | 无 | 否 |
| PR 2 | 增加 Coordinator RPC 与 Admin client 链路 | `fluss-rpc`, `fluss-client`, `fluss-server` | PR 1、P0 协议结论 | API 可用，日志表可闭环 |
| PR 3 | 增加持久化初始化状态和写入门禁 | `fluss-common`, `fluss-server`, `fluss-client` | Bulk Load 状态设计 | 否 |
| PR 4 | 接入主键表 Bulk Load 编排 | Flink、Bulk Load 相关模块、server | PR 2、PR 3、Bulk Load 基础能力 | 主键表后端能力可用 |
| PR 5 | 增加并注册 Flink procedure | `fluss-flink-common` 及版本测试模块 | PR 2；主键开放还依赖 PR 4 | 是 |
| PR 6 | 端到端测试、文档和兼容性收尾 | lake、Flink、dist/docs | PR 1—5 | 是 |

如果希望先交付日志表，可以在 PR 2 后合入只支持日志表的 PR 5，并对主键表返回项目统一且信息明确的不支持错误。不能创建一个永久停留在初始化中的主键表。

## 4. 详细 PR 计划

### PR 1：[paimon] Map existing Paimon table metadata to Fluss

#### 目标

为 `LakeCatalog` 增加读取现有湖表元数据的 lake-format-neutral 能力，并在 Paimon 实现中完成支持范围校验和 `TableDescriptor` 映射。本 PR 不创建 Fluss 表。

#### 代码改动

- 在 `fluss-common` 的 `LakeCatalog` 增加读取接口。
  - 根据最终协议决定返回 `TableDescriptor`，还是包含 snapshot/bucket metadata 的结果对象。
  - 为所有 LakeCatalog 实现补齐方法。非 Paimon 实现应明确返回不支持；如果使用 default method，异常类型和消息必须固定。
- 在 `PaimonLakeCatalog` 加载现有 `FileStoreTable`。
- 将 Paimon 字段类型、nullable、主键和分区键映射为 Fluss Schema。
- 通过 `FileStoreTable.bucketMode()` 分支处理 Bucket Mode：
  - `HASH_FIXED`：读取正数 Bucket 数；仅接受默认 Bucket Function；Bucket Key 按已确认的兼容规则映射。
  - `BUCKET_UNAWARE`：只接受 append-only 表；Bucket 数留给 Coordinator 根据用户参数或集群默认值补齐。
  - `HASH_DYNAMIC`、`KEY_DYNAMIC`、`POSTPONE_MODE`：返回包含实际模式的明确错误。
- 校验主键表最多一个分区键；有分区键时校验时间分区及 `partition.timestamp-formatter`。
- 仅映射已确认安全的表属性，避免把 Paimon 内部属性直接泄漏为 Fluss 属性。

#### 测试

- `PaimonLakeCatalogTest` 覆盖日志表、主键表、分区表和非分区表。
- 覆盖所有五种 Bucket Mode；如果测试 API 难以构造某种模式，至少为判定函数提供单元测试。
- 覆盖默认和非默认 Bucket Function。
- 覆盖不支持的数据类型、复合主键、多个分区键、非法时间 formatter、湖表不存在。
- 对映射结果逐项断言列顺序、类型、nullable、主键、分区键、Bucket Key、Bucket 数和属性。

#### 合入条件

- 映射行为不依赖 Flink 类，`fluss-common` 不增加 Paimon 依赖。
- 其他 LakeCatalog 实现保持编译通过，且“不支持读取”行为可预测。

### PR 2：[server] Add create-table-on-lake control-plane API

#### 目标

打通 Admin API、RPC 和 Coordinator。Coordinator 从 LakeCatalog 读取描述，合并属性，完成校验，并复用现有表创建基础设施写入 Fluss 元数据。

#### 代码改动

- 在 `FlussApi.proto` 增加 request/response，并在 `ApiKeys` 分配未占用的 API key 和首个版本。
- 增加 RPC message wrapper、`AdminGateway` 方法以及 server/client 注册。
- 在 `Admin` 和 `FlussAdmin` 增加 `createTableOnLake`。
- Coordinator 按固定顺序执行：
  1. 校验 `TablePath`。
  2. 鉴权 Fluss database 的 CREATE 权限。
  3. 确认 Fluss database 存在且同名 Fluss 表不存在。
  4. 获取 LakeCatalog，并拒绝未配置 lake storage 或非 Paimon 的首期请求。
  5. 读取湖表元数据。
  6. 解析并校验用户属性。
  7. 按 P0-4 规则合并属性；`HASH_FIXED` 和 `BUCKET_UNAWARE` 分别应用 FIP 的 `bucket.num` 规则。
  8. 应用 replication factor、`default.bucket.number` 等系统默认值。
  9. 调用 `MetadataManager.validateTableDescriptor` 并检查 replica capacity。
  10. 创建 assignment 和 Fluss 元数据，但不调用 `LakeCatalog.createTable`，因为湖表已经存在。
- 抽取当前 `CoordinatorService.createTable` 中可复用的 default、validation、assignment 和 metadata creation 逻辑，控制重构范围，避免复制后产生两套校验。
- 明确并发语义：两个相同请求并发时至多一个成功；本 API 不提供 `ignoreIfExists`。
- 旧 server 不识别新 API 时，client 保留现有 unsupported-version 行为。

#### 测试

- RPC request/response 序列化测试和 `ApiManagerTest`。
- `FlussAdmin` 请求构造测试，包括空 properties、多个属性和非法 `TablePath`。
- `CoordinatorService` 测试成功路径、鉴权、database/table 不存在或冲突、无 LakeCatalog、非 Paimon、属性冲突和 replica capacity。
- 验证成功后 Fluss 和 Paimon 指向同一湖表，并验证没有调用 `LakeCatalog.createTable`。
- 验证 `HASH_FIXED` Bucket 一致性和 `BUCKET_UNAWARE` 默认/覆盖规则。

#### 合入条件

- 日志表创建完成后立即可用。
- 如果 PR 3/PR 4 尚未合入，主键表在产生任何 Fluss 元数据前被拒绝。

### PR 3：[server] Persist table initialization state and reject writes

#### 目标

提供主键表初始化所需的状态机、failover 恢复和写入门禁。具体字段和 RPC 以独立 Bulk Load 设计为准。

#### 状态转换

建议最小状态集合如下：

```text
CREATING/INITIALIZING -> READY
                      -> INITIALIZING_FAILED
                      -> DROPPED
```

逐项约束：

1. 主键表元数据与 `INITIALIZING` 状态必须原子地创建，避免表短暂可写。
2. 只有匹配当前 `initializationId` 的成功完成请求才能转为 `READY`。
3. 任一 Bucket 未完成时不能转为 `READY`。
4. 失败或 snapshot 冲突后保持不可写；重试创建新的 attempt，但不能复用旧 attempt 的完成事件。
5. Coordinator failover 后从持久化元数据恢复当前 attempt 和状态。

#### 代码改动与测试

- 增加可持久化的初始化记录和状态查询/更新入口。
- 在所有写入入口统一检查状态，包括 append/upsert、相关 RPC 和可能绕过 client 的 server 路径。
- 定义初始化中的读、alter、drop、tiering 和 partition 操作行为。
- 测试非法状态转换、重复完成、旧 attempt 回调、部分 Bucket 完成、失败、删除和 Coordinator failover。
- 集成测试验证初始化中写入被拒绝，`READY` 后恢复写入。

### PR 4：[paimon] Bootstrap primary-key table through Bulk Load

#### 目标

让 Flink 作业基于固定 Paimon Snapshot 初始化主键表。本 PR 只负责编排，SST 格式、上传协议、幂等和恢复复用独立 Bulk Load 能力。

#### 作业阶段

1. 调用控制面 API，获得 `initializationId`、`S_load` 和目标表上下文。
2. 非分区表读取全表；单时间分区表只解析并读取当前分区。当前分区的时区和 formatter 规则必须在测试中固定。
3. 所有 source split 固定到 `S_load`，禁止跟随最新 Snapshot。
4. 按 Fluss RowType、主键编码和 Bucket 函数转换并排序。
5. 为各目标 Bucket 生成 SST，上传到 Fluss 可访问的远程目录。
6. 提交前重新读取 `S_latest`：
   - `S_latest == S_load`：提交全部 Bucket。
   - `S_latest != S_load`：abort，不发布 SST，状态保持不可写。
7. 等待全部 Bucket 完成后，用 `initializationId` 请求 Coordinator 切换为 `READY`。

#### 测试

- 非分区和单时间分区主键表。
- 多 Bucket 数据路由，逐条验证 Fluss 与 Paimon 默认哈希结果一致。
- null、各支持类型、复合主键和大数据量 split。
- 执行中出现新 Snapshot 时 abort；断言没有表进入 `READY`。
- 作业失败、重启、重复提交和 Coordinator failover。
- 临时 SST 在成功、失败和 abort 后的清理。

#### 合入条件

- Bulk Load 数据校验不只比较行数，还比较按主键读取的完整行值。
- 作业恢复不会把不同 snapshot 或不同 attempt 的文件混合提交。

### PR 5：[flink] Add `sys.create_table_on_lake` procedure

#### 目标

提供 FIP 定义的 Flink SQL 入口，并只在后端支持的能力范围内注册。

#### 代码改动

- 新增 `CreateTableOnLakeProcedure`，提供一个参数和两个参数的 procedure signature。
- 在 `ProcedureManager` 注册 `sys.create_table_on_lake`。
- 解析 `database.table`，拒绝缺少 database、空 identifier 或多余层级。
- 解析逗号分隔的 `key=value`：trim key/value，拒绝空 key、缺少 `=` 和重复 key。是否支持转义逗号必须在 FIP 中明确；首期不支持时错误消息应给出限制。
- 调用 Admin API 或按 P0-1 的最终方案提交 Flink 作业。
- 返回信息包含 table path；若接口为异步初始化，返回内容还应包含可查询的 job/initialization ID。

#### 测试

- procedure 列表与查找测试。
- 一个参数、两个参数、空属性和多个属性。
- 非法 table path、非法 property、重复 key 和 Admin 异常传播。
- 各受支持 Flink 版本的 `FlinkProcedureITCase` 执行真实 `CALL`。
- 日志表 procedure 返回时表已可读写；主键表完成语义按 P0-1 的最终结论断言。

### PR 6：[test] Verify create-table-on-lake end to end

#### 目标

补齐跨模块行为、升级兼容性和用户文档，作为功能开放前的发布门槛。

#### 端到端矩阵

| 表类型 | Paimon 模式 | 分区 | 预期 |
| --- | --- | --- | --- |
| append-only | `HASH_FIXED` | 无/有 | 复用 Bucket 数并成功建表 |
| append-only | `BUCKET_UNAWARE` | 无/有 | 使用默认或用户指定的 Fluss Bucket 数 |
| primary key | `HASH_FIXED` | 无 | 全表初始化后可写 |
| primary key | `HASH_FIXED` | 单时间分区 | 仅初始化当前分区后可写 |
| primary key | `HASH_DYNAMIC` | 任意 | 建表前拒绝 |
| primary key | `KEY_DYNAMIC` | 任意 | 建表前拒绝 |
| primary key | `POSTPONE_MODE` | 任意 | 建表前拒绝 |

每个成功用例验证：

- 原 Paimon table UUID/path 未变化，没有创建第二张湖表。
- Fluss Schema、主键、分区键、Bucket 数和属性正确。
- 历史数据可通过预期的 lake/lookup 路径读取。
- 切换后 Fluss 写入可见，并能按既有 tiering 协议进入同一 Paimon 表。

每个失败用例验证 Fluss Catalog 中没有残留的半成品表；若失败发生在主键表元数据创建后，则验证其状态、清理和重试行为符合状态机定义。

#### 文档与发布检查

- 增加 procedure、Admin API、支持矩阵和外部 Paimon Writer 停写要求的用户文档。
- 记录 client/server/Flink connector 的最低兼容版本。
- 增加错误排查说明：unsupported Bucket Mode、snapshot conflict、初始化失败和表不可写。
- 确认 DLF 写入所有权协调属于社区代码、扩展点还是托管服务逻辑，避免在社区实现中引入供应商依赖。

## 5. 每个 PR 的验证命令

按实际受影响模块选择最小 Maven reactor，并包含依赖模块。例如：

```bash
# 格式和许可证
./mvnw spotless:check
./mvnw validate

# 公共 API、RPC、client 和 server
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server -am

# Paimon 映射和集成测试
./mvnw verify -pl fluss-lake/fluss-lake-paimon -am

# Flink procedure；根据当前 profile/模块约定运行所有受支持版本
./mvnw verify -pl fluss-flink/fluss-flink-common -am
```

提交前还需运行：

1. `git diff main...HEAD`，确认只包含当前 PR 的职责。
2. 检查 Java 8 源码兼容性和禁止依赖。
3. 检查新增 public/protected API 的 Javadoc 和稳定性注解。
4. 检查测试只使用 AssertJ，且没有 `@Timeout`。
5. 对 Protobuf 变更执行项目规定的生成流程，并确认生成结果完整。

## 6. 完成定义

功能只有在以下条件全部满足后才算完成：

- Paimon 支持矩阵中的合法表能稳定映射，非法表在创建 Fluss 元数据前失败。
- 日志表建表返回后立即可读写。
- 主键表从元数据创建开始保持不可写，只有固定 snapshot 的全部 Bucket 初始化成功后才可写。
- snapshot 冲突、作业失败和 Coordinator failover 不会发布部分初始化结果。
- 重试具有 attempt 隔离和幂等语义，不会混用旧 SST。
- 外部 Paimon Writer 的停写责任和托管环境写入所有权约束有明确文档。
- client、server、Flink connector 版本不匹配时返回可诊断的错误。
- 全量自审确认实现与 FIP 的接口、支持范围和完成语义一致。
