1. 背景与目标
   1.1 现状
- 自动分区表：通过 `table.auto-partition.*` 配置，Fluss 会按时间粒度自动创建分区，并根据 `table.auto-partition.num-retention` 定期**删除过期分区**（见 `AutoPartitionManager#dropPartitions`）。过期分区在 Fluss 侧不再有元数据与存储。
- 点查（Point Lookup）：主键表通过 `Table#newLookup()` 得到 `Lookuper`，按主键（或前缀键）从 Tablet 查询。分区表会先用 `PartitionGetter` 从 lookup key 解析出分区名，再通过 `ClientUtils.getPartitionId()` 拉取并校验分区元数据；若分区不存在（例如已过期被 drop），会抛出 `PartitionNotExistException`。
- 当前点查对过期分区的行为：在 `PrimaryKeyLookuper` 和 `PrefixKeyLookuper` 中，对 `PartitionNotExistException` 的处理是**直接返回空结果**（`LookupResult(Collections.emptyList())`），即**无法查到已过期分区内的数据**。
- 数据湖与批量/流式读：开启 Lakehouse 后，数据会通过 Tiering 同步到 Paimon/Iceberg/Lance。Flink 等引擎的 **Union Read** 已支持：在 Fluss 中读未过期分区，在数据湖中读已过期分区，从而能查到全量数据。但 **Java 客户端的点查** 只走 Fluss，不会回源数据湖。

1.2 目标
- 对**开启了数据湖的自动分区表**（或更一般地，分区表 + 数据湖），当点查命中的分区在 Fluss 侧已过期（不存在）时，**能够从数据湖完成点查并返回数据**，而不是直接返回空。
- 行为上：先按现有逻辑在 Fluss 点查；若因分区不存在而无法查（或明确识别为“分区已过期”），则**回源数据湖**做一次点查，合并或替代结果，使用户能查到过期分区内的记录。

2. 问题与挑战

2.1 问题简述

对于自动分区表，当某个分区在 Fluss 中被 TTL（过期删除）后，客户端点查再也查不到该分区内的数据；但这些数据已经通过 Tiering 同步到 Paimon，理应可以从 Paimon 查询得到。

2.2 挑战一：客户端不知道向哪台 Tablet Server 发点查

- 当前点查路径是：客户端根据 lookup key 解析出**分区名** → 通过元数据得到 **partitionId** 以及该分区下各 bucket 的 **leader（Tablet Server）** → 按 `TableBucket(tableId, partitionId, bucketId)` 聚合请求并发送到对应 Replica。
- 分区从 Fluss 中 TTL 后，该分区的 **partitionId、bucket 与 leader 信息在集群元数据中已不存在**，客户端无法构造有效的 `TableBucket`，也就无法通过 `getReplicaOrException(tb)` 路由到任何一台 Tablet Server。
- 因此，**过期分区的点查不能走现有「按 TableBucket 路由到 Replica」的路径**。若采用 **Tablet Server 执行**数据湖点查，需要**新的路由与 RPC**：客户端在分区不存在时，不按 `TableBucket` 路由，而是需要选一台 tablet server 去发送请求。

2.3 挑战二：Paimon 点查慢，可能阻塞 Fluss 点查

- 若在 **Tablet Server** 侧做「分区不存在时回源 Paimon」：
    - 当前实现中，lookup 请求是按 **Tablet Server 聚合**的：同一台机器会收到多个 `TableBucket` 的点查请求，在一个循环里对每个 `TableBucket` 调用 `getReplicaOrException(tb)` 和 `replica.lookups(entry.getValue())`。
    - 若其中部分请求因分区已过期需要回源 Paimon，而 Paimon 点查延迟远高于本地 KV，则**同一次批量中的 Paimon 请求会拖慢整批**，导致「查 Fluss 的请求」被「查 Paimon 的请求」阻塞，影响整体吞吐。
- **解决方案**：在客户端和服务器端实现查找请求的分离批处理机制，将内存查找和湖存储查找分为不同的批次，使用不同的处理管道。
    - **客户端侧**：根据数据源类型（Fluss或湖存储）对查找请求进行分组，分别构建Fluss查找批次和湖查找批次。
    - **服务端侧**：接收不同类型的查找请求，使用独立的处理线程池，避免慢速湖存储查找阻塞快速内存查找。

3. 范围与约束

- **表类型**：主键表、分区表，且开启数据湖（`table.datalake.enabled = true`，并配置了 `table.datalake.format`，如 Paimon/Iceberg/Lance）。
- **触发条件**：点查时根据 lookup key 解析出的分区在 Fluss 元数据中不存在（例如被自动分区策略 drop）。
- **点查类型**：只支持主键点查（`PrimaryKeyLookuper`）；不支持前缀点查（`PrefixKeyLookuper`）
- **数据湖格式**：目前只考虑 Paimon，Iceberg & Lance 没办法很好地按主键进行点查

4. 关键设计点

4.1 何时触发“从数据湖点查”

- 在客户端点查流程中，当因分区不存在而得到 `PartitionNotExistException`（或等价：`getPartitionId` 失败）时，若表配置了数据湖 & 自动分区，则**不直接返回空**，而是进入“数据湖点查”分支。

4.2 数据湖点查的执行位置：Tablet Server

- **采用 Tablet Server 执行**数据湖点查：
    - 客户端在分区不存在（`PartitionNotExistException`）且表开启数据湖时，**不**在本地查 Paimon，而是向集群发送 **Lookup** 请求；由 **Tablet Server** 访问 Paimon，执行点查并返回结果。
    - **优点**：
        - 客户端无需携带 Paimon/Catalog 配置与依赖，行为统一、易运维。
        - **Tablet Server 侧可做缓存**：未来可在 Tablet Server 对 Paimon 点查结果做本地缓存（或近端缓存），重复查询同一 key 时直接命中缓存，降低 Paimon 延迟、提升整体吞吐。
        - 与现有「点查走 Tablet」的模型一致，扩展点集中在一侧。

4.3 路由与 RPC：分区过期时如何发到 Tablet Server

- 分区 TTL 后没有 partitionId，并且对应的 replica 已经在 Fluss 集群中删掉了，没办法通过从 metadata 看应该去哪个 tablet server 进行点查
    - **通过哈希路由**：为了简化实现，可以直接通过哈希方式选择tablet server，哈希「表名 + 分区名 + bucketId」来确定发送lookup请求的目标 tablet server

4.4 Tablet Server 侧实现要点

- **执行隔离**：处理 Lake Lookup 的线程与处理普通 Fluss lookup 的线程**分离**，例如：
    - 使用**独立线程池**专门执行 Paimon 点查，将 Paimon 点查放入**异步队列**，完成后回调/写回 response，不阻塞当前 `ReplicaManager.lookups` 循环。

4.5 查找请求分离的具体实现

- **客户端实现**：
    - 实现点查类型区分机制，将正常的从 Fluss 集群查找和湖存储查找分开处理
    - 在将 lookup 攒批的处理中，根据查找类型，对 lookup 请求进行分组
    - 正常的从 Fluss集群和从湖存储查找的请求区别开来，保证从湖存储查找的请求不 block 从 Fluss 集群的请求，保证 look up 的吞吐

- **服务端实现**：
    - 添加独立线程池处理湖存储查找请求，实现与从 Fluss集群的资源隔离，避免慢速的湖存储查找阻塞快速的内存查找
    - 扩展 `LookupRequest` 协议，支持湖存储查找所需的额外信息字段，如分区名称和表路径信息
    - 在服务端接收到湖存储查找请求后，使用专门的处理逻辑和独立线程池执行对湖存储系统的查询操作

