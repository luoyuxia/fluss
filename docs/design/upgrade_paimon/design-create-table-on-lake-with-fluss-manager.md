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

# 通过 Fluss Manager 执行 Create Table on Lake

本文基于 [FIP：在现有湖表上创建 Fluss 湖流一体表](./fip-create-table-on-lake.md)，描述由
Fluss Manager 异步拉起 Action JAR 的整体执行方式。本文只定义组件职责、调用流程和错误边界，
DLF 侧的状态字段及状态值由 DLF 与 Fluss Manager 的既有协议约定。

## 1. 设计结论

Create Table on Lake 由一个 Fluss Action JAR 统一执行。Fluss Manager 只负责解析配置、构造
JAR 参数和异步启动作业，不参与 Paimon 表校验、Fluss 建表、表类型判断、Bulk Load 或 lake
snapshot 注册。

```text
DLF
  -> Fluss Manager
       -> 解析 Fluss、DLF/Paimon 和 Flink 参数
       -> 异步拉起 CreateTableOnLake Action JAR
       -> 不等待 deployment ID、Job ID 或作业完成
       -> 向 DLF 返回“升级中”
                    |
                    v
          CreateTableOnLake Action JAR
            -> 读取 Paimon 表并固定当前 snapshot
            -> 调用 Admin.createTableOnLake(...)
                  -> Coordinator 权威校验湖表和建表参数
                  -> 创建 Fluss 表
                  -> 返回 TableInfo
            -> 日志表：直接注册初始 lake snapshot
            -> 主键表：执行 Flink Batch Load
                         -> Batch Load 成功后注册初始 lake snapshot
            -> 正常退出或抛出异常
            -> TODO: 通过 statusReporter 上报最终结果
```

该设计不增加独立的 `validateCreateTableOnLake` RPC。Action JAR 通过一次
`Admin.createTableOnLake` 调用触发 Server 端的校验和创建，避免校验与创建使用两套规则。

## 2. 组件职责

### 2.1 Fluss Manager

Fluss Manager 负责：

1. 接收 DLF 的 Create Table on Lake 请求。
2. 解析目标 Fluss 集群、database、table 和用户指定的表属性。
3. 加载 Fluss bootstrap servers 和认证配置。
4. 加载 DLF/Paimon Catalog 配置。
5. 加载 Flink workspace、namespace、JAR URI、引擎版本和资源配置。
6. 生成或透传本次请求的 `operationId`。
7. 构造 Action JAR main arguments。
8. 在后台创建并启动 deployment。
9. 按 DLF 协议返回“升级中”。

Fluss Manager 不负责：

- 连接 Paimon Catalog 或读取 Paimon 表。
- 校验 Paimon Schema、Bucket Mode 或主键。
- 调用 `Admin.createTableOnLake`。
- 判断日志表或主键表。
- 构造或等待 Batch Load Pipeline。
- 注册 lake snapshot。
- 等待 deployment ID、Flink Job ID 或作业终态后再响应 DLF。

Manager 可以复用现有 Tiering Service 的以下基础设施：

- Fluss 版本与 Flink/VVR 引擎版本匹配。
- JAR URI 选择。
- workspace、namespace 和 deployment target 选择。
- Fluss、DLF/Paimon 和 Config Provider 参数生成。
- deployment 创建和启动 API。

Create Table on Lake 使用 table/operation 级的一次性 deployment，不能复用或覆盖 cluster 级
Tiering Service deployment。deployment 名称和 label 应包含 `operationId`，避免多个表的升级任务
互相覆盖。

### 2.2 CreateTableOnLake Action JAR

Action JAR 负责完整的业务流程：

1. 解析 Manager 传入的参数并解析 Config Provider。
2. 创建 Paimon Catalog，获取目标 `FileStoreTable`。
3. 记录调用 `Admin.createTableOnLake` 前观察到的最新 Paimon snapshot，记为 `S_load`。
4. 调用 `Admin.createTableOnLake(tablePath, properties)`。
5. 根据返回的 `TableInfo.hasPrimaryKey()` 选择日志表或主键表流程。
6. 在全部操作完成后正常退出；任一步失败时重新抛出异常。
7. TODO: 接入 `statusReporter`，向 DLF 上报最终成功或失败。

统一执行骨架如下：

```java
public void run() throws Exception {
    try {
        FileStoreTable paimonTable = getPaimonTable();
        Optional<Snapshot> snapshot = paimonTable.latestSnapshot();

        TableInfo tableInfo =
                admin.createTableOnLake(tablePath, tableProperties).get();

        if (tableInfo.hasPrimaryKey()) {
            executeBatchLoad(tableInfo, paimonTable, snapshot);
        }

        if (snapshot.isPresent()) {
            registerLakeSnapshot(
                    tableInfo,
                    snapshot.get().id(),
                    Collections.emptyMap());
        }

        // TODO: statusReporter.reportSucceeded(operationId);
    } catch (Exception e) {
        // TODO: unwrap the exception and report failure through statusReporter.
        throw e;
    }
}
```

示例代码只表达职责边界。`statusReporter` 留作 TODO；DLF 状态协议、错误码映射和 Reporter 的
具体实现由 Fluss Manager 与 DLF 的集成约定决定。

### 2.3 Fluss Client

`Admin` 提供：

```java
CompletableFuture<TableInfo> createTableOnLake(
        TablePath tablePath, Map<String, String> properties);
```

返回值继续使用 `TableInfo`，不增加 `CreateTableOnLakeResult`。Action 可以通过
`TableInfo.hasPrimaryKey()` 判断是否需要 Batch Load，并使用 Action 在建表前记录的 snapshot
作为初始 lake snapshot。

该 future 完成只表示 Fluss 表元数据已经创建。它不表示：

- 初始 lake snapshot 已注册。
- 主键表历史数据已经 Load 到 Fluss。
- 整个 Action 已经成功。

### 2.4 Coordinator Server

Coordinator 在 `createTableOnLake` RPC 内完成权威校验和元数据创建：

1. 校验 `TablePath` 和 CREATE 权限。
2. 校验 Fluss database 存在。
3. 校验不存在同名 Fluss 表。
4. 获取 Fluss 集群配置的 `LakeCatalog`，首期要求其格式为 Paimon。
5. 读取并映射 Paimon 表的 Schema、主键、分区键、Bucket 和表属性。
6. 合并并校验用户传入的 Fluss 表属性。
7. 应用 replication factor、默认 Bucket 数等系统默认值。
8. 执行现有 descriptor、assignment 和 replica capacity 校验。
9. 只创建 Fluss 元数据，不调用 `LakeCatalog.createTable`，避免创建或覆盖 Paimon 表。
10. 返回 Coordinator 实际创建的 `TableInfo`。

Action 读取 Paimon 表用于选择执行路径和固定 snapshot；Coordinator 再次读取同一张 Paimon 表
用于保证创建正确性。两侧配置必须指向同一个 Paimon Catalog。

## 3. 日志表流程

日志表不需要启动 Flink Batch Pipeline：

```text
Action JAR main()
  -> 获取 Paimon latest snapshot
  -> Admin.createTableOnLake(...)
  -> Coordinator 校验并创建 Fluss 日志表
  -> 使用 empty Bucket offsets 注册初始 lake snapshot
  -> 正常退出
  -> TODO: 上报成功
  -> main() 正常退出
```

初始 snapshot 注册复用正常 Tiering 的 prepare/commit 流程：

- snapshot ID 使用建表前记录的 `S_load`。
- Bucket offsets 使用空 `Map<TableBucket, Long>`。
- 不为 Bucket 人工写入 `0` 或 `-1`。
- 空 Paimon 表没有 snapshot 时跳过注册。

日志表 Action 成功退出时，Fluss 表已经创建，存在的 Paimon snapshot 已经注册。

## 4. 主键表流程

主键表由同一个 Action JAR 构造并执行 Batch Load：

```text
Action JAR main()
  -> 获取并固定 S_load
  -> Admin.createTableOnLake(...)
  -> 构造 Paimon -> Fluss Batch Pipeline
  -> 固定读取 S_load
  -> Load 数据到 Fluss
  -> 等待 Batch Job 成功
  -> 注册 S_load，Bucket offsets 为空
  -> 正常退出
  -> TODO: 上报成功
  -> main() 正常退出
```

Manager 已经异步启动整个 Action JAR，因此主键表流程可以在 JAR 内使用阻塞的
`StreamExecutionEnvironment.execute()`。该调用只阻塞运行 Action 的 Flink Application，不阻塞
DLF 请求。Batch Job 完成后，JAR 才能可靠地继续注册 snapshot。

如果使用 `executeAsync()`，必须通过 `JobClient.getJobExecutionResult()` 等待作业完成后再注册
snapshot；否则 `main()` 可能在 Batch Job 完成前退出。首版没有必要引入这一额外异步层。

## 5. Snapshot 一致性

Action 在建表前记录 Paimon snapshot `S_load`。后续读取和注册必须始终使用同一个 snapshot：

1. 日志表直接将 `S_load` 注册到 Fluss。
2. 主键表的 Batch Load 固定读取 `S_load`。
3. 主键表只有在 `S_load` 的全部数据成功 Load 后，才能将 `S_load` 注册到 Fluss。

JAR 启动后不能重新获取 latest snapshot 并替换 `S_load`，否则实际加载的数据边界可能与 Fluss
记录的 snapshot 不一致。

第一版不阻止外部 Paimon Writer 在 Action 执行期间继续提交。主键表在最终发布前是否检查
latest snapshot 仍等于 `S_load`，由主键表 Bulk Load 设计规定。

## 6. 参数协议

Manager 至少向 Action JAR 传入：

```text
operationId
table=database.table
fluss.bootstrap.servers
fluss authentication/config-provider 配置
datalake.format=paimon
datalake.paimon.* Catalog 配置
table-conf.*
DLF 状态上报所需信息（TODO）
```

参数前缀与 Tiering Service 保持一致：

- `fluss.*` 去掉前缀后构造 Fluss `Configuration`。
- `datalake.paimon.*` 去掉前缀后构造 Paimon Catalog。
- `table-conf` 用于传递用户覆盖的 Fluss 表属性。

敏感凭证不应直接出现在明文 main arguments 中。优先使用 Config Provider、临时凭证或密钥
引用，避免凭证出现在 deployment 配置和日志中。

## 7. 返回和状态语义

Manager 向 DLF 返回“升级中”只表示异步启动流程已经被接收，不表示：

- Action JAR 已经开始执行。
- Paimon 表已经通过校验。
- Fluss 表已经创建。
- Flink deployment 或 Job 已经创建。

`statusReporter` 接入后，业务结果由 Action JAR 在后台上报。Action 只有在 snapshot 注册成功后
才能上报成功。接入前，Action 通过正常退出或抛出异常表达结果，DLF 最终状态同步暂不在第一版
范围内。

不在本设计中定义新的 Fluss 表升级状态字段。DLF 展示的状态和状态转换使用 DLF 与 Fluss
Manager 的既有协议。

## 8. 错误处理

错误按照发生位置划分：

| 失败位置 | 责任方 | 处理方式 |
| --- | --- | --- |
| 请求参数无法解析 | Manager | 不启动 JAR，直接返回请求错误 |
| deployment 创建或启动 API 失败 | Manager 后台 launcher | 更新 DLF 约定的失败状态 |
| JAR 下载、JVM 启动或类加载失败 | Manager/VVR 监控 | 根据 application/deployment 状态更新失败状态 |
| Paimon 参数或访问失败 | Action JAR | 抛出异常；状态上报为 TODO |
| Paimon 表不存在或不兼容 | Action JAR | 抛出异常；状态上报为 TODO |
| Coordinator 建表校验失败 | Action JAR | 解包 Admin future 异常并重新抛出；状态上报为 TODO |
| Batch Load 失败 | Action JAR | 抛出异常；状态上报为 TODO |
| snapshot prepare/commit 失败 | Action JAR | 抛出异常；状态上报为 TODO |

Action 重新抛出原始异常，让 Flink/VVR 记录 application 失败，便于排查日志和平台问题。
后续实现 `statusReporter` 时，上报失败不能覆盖原始业务异常。

只查询 Flink Job 状态不足以判断整个 Action 是否成功：

- 日志表不会创建 Flink Job。
- 主键表的 Flink Job 可能已经 FINISHED，但随后 snapshot 注册仍可能失败。

`statusReporter` 接入后，业务成功以 Action 上报为准，Flink/VVR 状态作为运行监控和故障排查
信息。接入前只能依赖 application/deployment 状态和日志排查结果。

## 9. Flink/VVR 运行要求

Manager 使用与 Tiering Service 相同的方式启动 Action JAR，但 Create Table on Lake 是一次性
Application，而不是长期运行的 Streaming Job。

需要在实现前验证 VVR 对以下场景的行为：

1. 日志表路径的 `main()` 不调用 `env.execute()`，完成建表和 snapshot 注册后直接返回。
2. VVR 是否允许没有 JobGraph 的 Application 正常结束。
3. 没有 Flink Job ID 时，是否仍能查询 application/deployment 的失败日志。

如果 VVR 强制要求 JAR 提交 JobGraph，应优先让日志表 Action 作为普通 Java Application/Driver
执行。只有平台不支持该方式时，才考虑为日志表构造一个最小 metadata Job；该兼容处理不应将
表类型判断和 snapshot 逻辑移入 Manager。

## 10. PR 拆分

### PR 1：Fluss Core Create Table on Lake

- 增加 `LakeCatalog.getTableDescriptor(TablePath)`。
- 实现 Paimon 表到 Fluss `TableDescriptor` 的映射与校验。
- 增加 `Admin.createTableOnLake`。
- 增加 Client、RPC 和 Coordinator 实现。
- 返回最终创建的 `TableInfo`。
- 不增加独立 validate RPC。

### PR 2：日志表 Action

- 增加 Action 参数解析和 Config Provider 支持。
- 根据 DLF/Paimon 参数创建 Paimon Catalog。
- 读取并固定 latest snapshot。
- 调用 `Admin.createTableOnLake`。
- 对日志表使用 empty offsets 注册初始 snapshot。
- 预留 `statusReporter` TODO，第一版不实现 DLF 最终状态上报。
- 主键表返回明确的暂不支持错误。

### PR 3：Fluss Manager 集成

- 增加 DLF 请求到 Action 参数的转换。
- 复用 Tiering Service 的 Fluss、DLF/Paimon 和 VVR 参数生成能力。
- 为每个 operation 创建独立 deployment。
- 异步创建和启动 Action JAR。
- 处理 JAR 启动前的失败。
- Manager 不实现表类型分支和 snapshot 操作。

### PR 4：主键表 Batch Load

- 在同一个 Action 中增加主键表分支。
- 固定读取 `S_load`。
- 构造并执行 Paimon 到 Fluss 的 Batch Pipeline。
- Batch Job 成功后注册初始 snapshot。
- 完成主键表的失败恢复、重试和一致性检查。

## 11. 第一版验收标准

第一版日志表能力满足以下条件：

1. DLF 调用 Fluss Manager 后，Manager 能异步拉起 Create Table on Lake Action JAR。
2. Manager 不连接 Paimon、不调用 Fluss Admin，也不判断表类型。
3. Action 能使用 Manager 传入的 Fluss 和 DLF/Paimon 参数连接两侧系统。
4. 不兼容表在 Fluss 元数据创建前失败，Action 抛出的异常包含具体错误。
5. 兼容日志表能创建对应的 Fluss 表。
6. 有 snapshot 的日志表能注册相同 snapshot ID，Bucket offsets 为空。
7. 空 Paimon 表跳过 snapshot 注册。
8. Action 只有在建表和 snapshot 注册都成功后才正常退出。
9. 原 Paimon 表及历史数据不被修改或复制。
10. `statusReporter` 作为明确的后续 TODO，不阻塞第一版日志表能力。
