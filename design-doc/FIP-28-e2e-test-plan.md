# FIP-28 端到端测试计划

本文档描述如何在真实环境中测试 "Support Write and Lookup for Expired Partitions" 功能，重点覆盖历史分区与当前分区共存场景下的正确性和隔离性。

---

## 1. 环境准备

### 1.1 表定义

测试需要两种表：PK 表和 Log 表，均启用 auto-partition + lake tiering。

全限定表名：`fluss_catalog.fluss.pk_table`、`fluss_catalog.fluss.log_table`。

**PK 表**:

```sql
CREATE CATALOG fluss_catalog WITH ('type' = 'fluss', 'bootstrap.servers' = 'localhost:9123');
USE CATALOG fluss_catalog;

CREATE TABLE fluss.pk_table (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '30',  -- 初始设大，确保 dt='20260501' 分区存活
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

**Log 表**:

```sql
CREATE TABLE fluss.log_table (
    dt STRING,
    event_id BIGINT,
    payload STRING
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '30',  -- 初始设大
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

### 1.2 如何构造"历史分区与当前分区共存"

**核心问题**：需要确保数据先 tiering 到 lake，然后再让分区过期。如果分区在 tiering 完成前就被删除，lake 中不会有旧值，后续 old-value lookup 无法验证。

**构造方法（ALTER TABLE 调小 retention）**：

```
今天是 20260518

第一阶段 — 写入 + tiering（num-retention = 30，dt='20260501' 在 retention window 内）:
1. 建表时 num-retention = 30，覆盖 dt='20260501'
2. 向 dt='20260501' 写入初始数据（正常写入，分区存活）
3. 等待 tiering 完成，数据落入 Paimon

第二阶段 — 缩小 retention 触发过期:
4. ALTER TABLE ... SET ('table.auto-partition.num-retention' = '3')
5. AutoPartitionManager 检测到 dt='20260501' 超出新的 3 天 retention → 删除该分区
6. 此时 dt='20260501' 已过期，lake 中有数据

第三阶段 — 验证历史路径:
7. 再次写入 dt='20260501' → 客户端检测到分区过期 → redirect 到 __historical__
```

```sql
-- 第一阶段: 写入数据（分区在 retention window 内，正常路径）
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260501', 1, 'Alice', 100.0);
-- 等待 tiering 完成...

-- 第二阶段: 调小 retention，触发分区过期
ALTER TABLE fluss_catalog.fluss.pk_table SET ('table.auto-partition.num-retention' = '3');
-- 等待 AutoPartitionManager 检查周期，dt='20260501' 被删除

-- 第三阶段: 现在写入会 redirect 到 __historical__
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260501', 1, 'Alice', 200.0);
```

- **当前分区**：`dt = '20260518'`、`'20260517'`、`'20260516'`（在 3 天 retention window 内）
- **过期分区**：`dt = '20260501'` 等（调小 retention 后落在 window 外，且 lake 中已有数据）

两者可以**在同一个 producer/writer 中交替写入**，从而构造共存场景。

### 1.3 前置确认

- Paimon catalog 已配置且可用
- Tiering 服务正常运行
- `__historical__` 分区尚未存在（首次写入过期分区时应触发自动创建）

---

## 2. 测试场景

### 场景 1: 基本功能 — PK 表写入过期分区

**目的**: 验证 PK 表对过期分区的 upsert 能正确 redirect 到 `__historical__`，并生成正确 changelog。

**步骤**:

1. 先向 lake 写入初始数据（确保 lake 中有旧值可供 old-value lookup）：
   - 建表时 `num-retention = 30`，`dt = '20260501'` 在 retention window 内
   - 写入 `(dt='20260501', id=1, name='Alice', amount=100.0)`（正常写入路径）
   - 等待 tiering 完成，数据落入 Paimon
   - ALTER TABLE 调小 `num-retention = 3`，触发 AutoPartitionManager 删除 `dt = '20260501'`

```sql
-- 步骤 1a: 写入初始数据（num-retention=30，分区存活，走正常路径）
INSERT INTO fluss_catalog.fluss.pk_table VALUES
    ('20260501', 1, 'Alice', 100.0),
    ('20260501', 10, 'Lake-Only', 999.0);

-- 步骤 1b: 等待 tiering 完成（确认数据已落入 Paimon）

-- 步骤 1c: 调小 retention，触发分区过期
ALTER TABLE fluss_catalog.fluss.pk_table SET ('table.auto-partition.num-retention' = '3');
-- 等待 AutoPartitionManager 周期性检查，dt='20260501' 超出 3 天 retention → 被删除
-- 此时 lake 中已有 (id=1, 'Alice', 100.0) 和 (id=10, 'Lake-Only', 999.0)
```

2. 写入过期分区的 upsert：

```sql
-- 步骤 2: 分区已过期，写入会 redirect 到 __historical__
INSERT INTO fluss_catalog.fluss.pk_table VALUES
    ('20260501', 1, 'Alice', 200.0),   -- 更新已有 key
    ('20260501', 2, 'Bob', 50.0);      -- 插入新 key
```

3. 写入过期分区的 delete：

```sql
-- 步骤 3: 删除过期分区中的 key
DELETE FROM fluss_catalog.fluss.pk_table WHERE dt = '20260501' AND id = 1;
```

4. 连续写入两次同 key 的 upsert（不等待 flush），验证 old-value 来自 prewrite buffer：

```sql
-- 步骤 4: 连续 upsert 同一 key，第二次的 old-value 应来自 prewrite buffer
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260501', 3, 'V1', 10.0);
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260501', 3, 'V2', 20.0);
```

**验证**:

```sql
-- 验证 __historical__ 消费 changelog（Flink streaming 模式）
SELECT * FROM fluss_catalog.fluss.pk_table$historical /*+ OPTIONS('scan.startup.mode' = 'earliest') */;
```

- `__historical__` 分区被自动创建
- 从 `__historical__` 消费 changelog：
  - id=1 的 upsert 产生 `UPDATE_BEFORE(amount=100.0)` + `UPDATE_AFTER(amount=200.0)`（old-value 从 lake 获取）
  - id=2 的 insert 产生 `INSERT(name='Bob', amount=50.0)`
  - id=1 的 delete 产生 `DELETE(name='Alice', amount=200.0)`
  - id=3 的第二次 upsert 产生 `UPDATE_BEFORE(name='V1', amount=10.0)` + `UPDATE_AFTER(name='V2', amount=20.0)`（old-value 从 prewrite buffer 获取）
- composite key 隔离正确：不同 original partition 的相同 id 不会碰撞

### 场景 2: 基本功能 — Log 表写入过期分区

**步骤**:

1. 向当前分区 `dt = '20260518'` 写入若干条记录
2. 同时向过期分区 `dt = '20260401'` 写入若干条记录
3. 从 `__historical__` 消费

```sql
-- 步骤 1: 写入当前分区（正常路径）
INSERT INTO fluss_catalog.fluss.log_table VALUES
    ('20260518', 1001, 'event-a'),
    ('20260518', 1002, 'event-b'),
    ('20260518', 1003, 'event-c');

-- 步骤 2: 写入过期分区（redirect 到 __historical__）
INSERT INTO fluss_catalog.fluss.log_table VALUES
    ('20260401', 2001, 'late-event-x'),
    ('20260401', 2002, 'late-event-y');

-- 步骤 3: 消费当前分区
SELECT * FROM fluss_catalog.fluss.log_table /*+ OPTIONS('scan.startup.mode' = 'earliest') */
WHERE dt = '20260518';

-- 步骤 3: 消费 __historical__
SELECT * FROM fluss_catalog.fluss.log_table$historical /*+ OPTIONS('scan.startup.mode' = 'earliest') */;
```

**验证**:

- 过期分区的记录出现在 `__historical__` 中
- 当前分区的记录正常出现在 `dt = '20260518'` 分区中
- consumer 能从 `__historical__` 的 row payload 中还原 `dt = '20260401'`

### 场景 3: 基本功能 — 过期分区 Lookup

**步骤**:

1. 准备 lake 中的数据（同场景 1 步骤 1：写入 → 等 tiering → ALTER TABLE 调小 retention → 分区过期）
2. 对已过期的 partition 执行 point lookup（使用仅在 lake 中、未被场景 1 写入 `__historical__` KV store 的 key）：
   - `lookup(dt='20260501', id=10)` — 数据仅在 lake，`__historical__` KV store 无此 key → 触发 lake fallback
3. 向过期分区写入新数据：
   - `(dt='20260501', id=3, name='Charlie', amount=300.0)`
4. 立即 lookup：
   - `lookup(dt='20260501', id=3)` — 数据在 local（尚未 tier）

```sql
-- 步骤 1: 准备 lake 数据（同场景 1 步骤 1a-1c）
-- INSERT → 等 tiering 完成 → ALTER TABLE SET num-retention=3 → 分区过期

-- 步骤 2: Lookup join 查询 lake 中的过期分区数据
-- 使用 id=10（仅在 Paimon lake 中，未被场景 1 步骤 2-4 写入 __historical__）
-- 注意: 不能用 id=1，因为场景 1 步骤 3 的 DELETE 在 __historical__ KV store 中留下了 tombstone，
-- tombstone 会阻断 lake fallback，导致 lookup 返回 null
CREATE TEMPORARY TABLE lookup_keys (
    dt STRING,
    id BIGINT,
    proc_time AS PROCTIME()
) WITH ('connector' = 'datagen', 'rows-per-second' = '1',
        'fields.dt.kind' = 'sequence', 'fields.dt.start' = '20260501', 'fields.dt.end' = '20260501',
        'fields.id.kind' = 'sequence', 'fields.id.start' = '10', 'fields.id.end' = '10');

SELECT k.dt, k.id, t.name, t.amount
FROM lookup_keys k
JOIN fluss_catalog.fluss.pk_table FOR SYSTEM_TIME AS OF k.proc_time AS t
ON k.dt = t.dt AND k.id = t.id;
-- 预期返回: ('20260501', 10, 'Lake-Only', 999.0) — 来自 lake fallback

-- 步骤 3: 向过期分区写入新数据
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260501', 3, 'Charlie', 300.0);

-- 步骤 4: 立即 lookup 新写入的数据（local-first）
-- 修改 lookup_keys 的 id 范围为 3
-- 预期返回: ('20260501', 3, 'Charlie', 300.0) — 来自 local RocksDB

-- 验证不存在的 key
-- lookup(dt='20260501', id=999) → 预期返回 null
```

**验证**:

- 步骤 2: id=10 不在 `__historical__` KV store → lake fallback 返回 `('20260501', 10, 'Lake-Only', 999.0)`
- 步骤 4: 返回最新值（local-first，不依赖 tiering）
- `lookup(dt='20260501', id=999)` → 返回 null

#### 场景 3a: Lookup 触发 `__historical__` 创建

**目的**: 验证 lookup 路径也能触发 `__historical__` 的自动创建（设计文档 A.1: "regardless of whether the operation is a write or a lookup"）。

**前提**: `__historical__` 尚未创建（使用独立的表，或确保在场景 1 之前执行）。

**步骤**:

1. 准备 lake 中的数据（同场景 1 步骤 1）
2. 不先写入，直接执行 lookup：`lookup(dt='20260501', id=1)`

```sql
-- 前提: 使用独立的表确保 __historical__ 尚未创建
CREATE TABLE fluss.pk_table_3a (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '30',  -- 初始设大，确保分区存活
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);

-- 步骤 1a: 写入数据（分区存活，正常路径）
INSERT INTO fluss_catalog.fluss.pk_table_3a VALUES ('20260501', 1, 'Alice', 100.0);

-- 步骤 1b: 等待 tiering 完成

-- 步骤 1c: 调小 retention，触发分区过期
ALTER TABLE fluss_catalog.fluss.pk_table_3a SET ('table.auto-partition.num-retention' = '3');
-- 等待 AutoPartitionManager 删除 dt='20260501'

-- 步骤 2: 不先写入，直接 lookup（会触发 __historical__ 创建）
-- 使用 lookup join 驱动
SELECT k.dt, k.id, t.name, t.amount
FROM lookup_keys k
JOIN fluss_catalog.fluss.pk_table_3a FOR SYSTEM_TIME AS OF k.proc_time AS t
ON k.dt = t.dt AND k.id = t.id;
-- 预期: __historical__ 被自动创建，返回 ('20260501', 1, 'Alice', 100.0)
```

**验证**:

- `__historical__` 被 lookup 路径自动创建
- lookup 返回正确值（lake fallback）

### 场景 4: 多过期分区写入同一 `__historical__` — Composite Key 隔离

**目的**: 验证不同 original partition 的 key space 在 `__historical__` 中完全隔离。

**步骤**:

1. 向过期分区 `dt = '20260401'` 写入 `(id=1, name='April-Alice', amount=100.0)`
2. 向过期分区 `dt = '20260301'` 写入 `(id=1, name='March-Alice', amount=200.0)`
3. 分别 lookup：
   - `lookup(dt='20260401', id=1)`
   - `lookup(dt='20260301', id=1)`

```sql
-- 步骤 1: 写入过期分区 dt='20260401'
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260401', 1, 'April-Alice', 100.0);

-- 步骤 2: 写入过期分区 dt='20260301'（同一 id=1，不同分区）
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260301', 1, 'March-Alice', 200.0);

-- 步骤 3: 分别 lookup 验证隔离性
-- lookup(dt='20260401', id=1) → 预期: name='April-Alice', amount=100.0
-- lookup(dt='20260301', id=1) → 预期: name='March-Alice', amount=200.0
-- 两个不同 original partition 的 id=1 互不干扰

-- 验证 changelog: 两条都是 INSERT（不是 UPDATE，因为 composite key 不同）
SELECT * FROM fluss_catalog.fluss.pk_table$historical /*+ OPTIONS('scan.startup.mode' = 'earliest') */;
```

**验证**:

- lookup 返回各自正确的值，不互相干扰
- `dt='20260401'` 的 id=1 返回 `name='April-Alice'`
- `dt='20260301'` 的 id=1 返回 `name='March-Alice'`

---

### 场景 5: 隔离性 — 同一 Pipeline 中过期分区写入不影响实时分区写入延迟

**目的**: 最核心的隔离性验证。过期分区的写入可以慢（lake I/O 耗时），但不能拖慢同一 pipeline 中实时分区的写入延迟。**实时分区的写入延迟是唯一关注指标**，过期分区写入延迟不关心。

**场景描述**:

真实业务中，一条 Flink pipeline 的上游数据流天然混合了实时数据和迟到数据。同一个 Sink 算子内的同一个 writer 会同时处理两类记录：
- 实时分区记录 → 走同步本地 RocksDB，毫秒级完成
- 过期分区记录 → redirect 到 `__historical__`，服务端需要 lake old-value lookup，可能耗时数百毫秒甚至秒级

如果隔离做得不好，过期分区的慢写入会阻塞 Sender 线程或 batch drain，导致实时分区的 batch 也被堵住，实时写入延迟从毫秒级劣化到秒级。

**步骤**:

1. 创建 Flink 作业，数据源产生混合流，写入同一张 Fluss PK 表：
   - 实时记录：目标分区 `dt = '20260518'`（当前分区，存在于 metadata）
   - 迟到记录：目标分区 `dt = '20260401'`（已过期，不存在于 metadata）
   - 两类记录交替产生，模拟正常业务流中夹杂少量迟到数据
2. 在作业运行期间，持续观察**实时分区的写入延迟**

```sql
-- 步骤 1: 创建 datagen 模拟混合数据源
CREATE TEMPORARY TABLE mixed_source (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1000',
    'fields.id.kind' = 'random',
    'fields.id.min' = '1',
    'fields.id.max' = '100000',
    'fields.name.length' = '10',
    'fields.amount.min' = '1.0',
    'fields.amount.max' = '10000.0'
);
-- 注意：datagen 不支持按比例生成不同 dt 值，
-- 实际测试中需要自定义 Source 或用 UDF 按概率分配：
-- 90% 的记录 dt='20260518'（实时），10% dt='20260401'（过期）

-- 步骤 1: 写入目标表
INSERT INTO fluss_catalog.fluss.pk_table
SELECT
    CASE WHEN RAND() < 0.9 THEN '20260518' ELSE '20260401' END AS dt,
    id, name, amount
FROM mixed_source;

-- 步骤 2: 基准对比 — 纯实时写入（无过期分区）
INSERT INTO fluss_catalog.fluss.pk_table
SELECT '20260518' AS dt, id, name, amount
FROM mixed_source;

-- 对比两个作业运行时实时分区的 p99 写入延迟
```

**验证**:

- 混入过期分区数据后，实时分区 p99 写入延迟相对于纯实时基准的劣化 < 10%
- 过期分区写入可以慢，但最终全部写入 `__historical__` 成功
- Sink 算子不出现由过期分区写入引起的反压
- Flink checkpoint 在混合写入期间正常完成，不因历史 batch 的 slow ACK 导致 checkpoint timeout

**隔离机制 — "历史路径最多占 1/10 总资源"原则（设计文档 C.2, C.4）**:

```
客户端侧（C.4.1 historical in-flight cap）:
├── Sender: historicalWriteInFlightCap = maxInFlight / 10
│   → 历史 in-flight 达到 cap 时跳过，继续发送实时 batch
├── 内存: 历史 in-flight 内存 ≤ cap × batchSize，自然有界
└── flush: 等待所有 batch ACK（包括历史），但 cap 限制了等待量

服务端侧:
├── 历史 request queue（C.3）: 默认 ~1/10 总 request queue，满时返回 THROTTLED
├── ioExecutor（C.2）: 历史专用线程池，实时路径不使用，无资源竞争
└── 历史写入提交到 ioExecutor，RPC 线程立即释放
```

### 场景 6: 隔离性 — 同一 Pipeline 中过期分区 Lookup 不影响实时分区 Lookup 延迟

**目的**: 在 lookup join 场景中，过期分区 key 的 lake fallback 查询（慢）不影响实时分区 key 的本地 lookup（快）。**实时分区的 lookup 延迟是唯一关注指标**。

**步骤**:

1. 创建 Flink lookup join 作业：
   - 主流包含混合 join key：部分对应当前分区，部分对应过期分区
   - Fluss PK 表作为 lookup 维表
2. 观察实时分区 key 的 lookup 延迟

```sql
-- 前提: pk_table 中已有数据
-- 当前分区 dt='20260518' 有数据（在 local RocksDB）
-- 过期分区 dt='20260501' 有数据（仅在 lake）

-- 步骤 1: 创建混合 join key 的主流
CREATE TEMPORARY TABLE lookup_driver (
    dt STRING,
    id BIGINT,
    extra_info STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1000',
    'fields.id.kind' = 'random',
    'fields.id.min' = '1',
    'fields.id.max' = '100',
    'fields.extra_info.length' = '5'
);
-- 同样需要自定义逻辑按比例分配 dt

-- 步骤 1: 执行 lookup join（混合实时 + 过期分区 key）
SELECT d.dt, d.id, d.extra_info, t.name, t.amount
FROM (
    SELECT
        CASE WHEN RAND() < 0.9 THEN '20260401' ELSE '20260401' END AS dt,
        id, extra_info, proc_time
    FROM lookup_driver
) d
LEFT JOIN fluss_catalog.fluss.pk_table FOR SYSTEM_TIME AS OF d.proc_time AS t
ON d.dt = t.dt AND d.id = t.id;

-- 步骤 2: 基准对比 — 纯实时 key lookup（无过期分区 key）
SELECT d.dt, d.id, d.extra_info, t.name, t.amount
FROM (
    SELECT '20260518' AS dt, id, extra_info, proc_time
    FROM lookup_driver
) d
JOIN fluss_catalog.fluss.pk_table FOR SYSTEM_TIME AS OF d.proc_time AS t
ON d.dt = t.dt AND d.id = t.id;

-- 对比两个作业中实时分区 key 的 p99 lookup 延迟
```

**验证**:

- 混入过期分区 key 后，实时分区 key 的 p99 lookup 延迟相对于纯实时基准的劣化 < 10%
- 过期分区 key 的 lookup 可以慢（lake fallback），但不阻塞同一 LookupSender 中实时 key 的返回
- 隔离机制（C.4.2）：LookupSender 拆分 inflight 信号量，历史 permit = 总量的 1/10，历史 permit 占满时实时 lookup 不受影响

---

### 场景 7: Flow Control — 过期分区 throttle 不影响实时分区

**目的**: 极端场景 — 历史 request queue 被过期分区操作打满时，throttle 只影响过期分区的写入，实时分区写入完全不受影响。

**步骤**:

1. 配置较小的历史 request queue（如容量 10）和较小的 ioExecutor（如 2 线程）
2. 在同一个 Flink 作业中，混合大量过期分区数据和少量实时数据，制造历史 request queue 过载
3. 观察实时分区写入

```sql
-- 步骤 1: 服务端配置（server.yaml）
-- server.historical-request-queue-ratio: 0.01  # 极小比例，容易打满
-- server.historical-io-thread-num: 2           # 少量线程，增加排队

-- 步骤 2: 制造过载 — 大量过期分区 + 少量实时
INSERT INTO fluss_catalog.fluss.pk_table
SELECT
    CASE WHEN RAND() < 0.2 THEN '20260518' ELSE '20260401' END AS dt,
    id, name, amount
FROM mixed_source;
-- 80% 过期分区数据，制造历史 request queue 过载

-- 步骤 3: 同时运行纯实时写入基准作业
INSERT INTO fluss_catalog.fluss.pk_table
SELECT '20260518' AS dt, id, name, amount
FROM mixed_source;

-- 对比: 步骤 2 中实时分区 p99 延迟 vs 步骤 3 纯实时基准
-- 观察 HISTORICAL_PARTITION_THROTTLED 错误计数（metrics / 日志）
-- 验证过期分区数据最终全部写入成功
```

**验证**:

- 过期分区写入收到 `HISTORICAL_PARTITION_THROTTLED`，客户端自动 exponential backoff retry（100ms → 200ms → 400ms → ... → 5s cap，±20% jitter）
- **实时分区写入完全不受影响**：不返回 throttle 错误，延迟不劣化
- 过期分区的 retry 不导致 Sink 反压传导到实时路径
- backoff 期间 batch 在 RecordAccumulator 中被跳过（drain 阶段检查 `retryAfterMs`），不占用 in-flight 资源
- 过期分区数据经过 retry 最终全部写入成功（可以慢，但不丢）

---

### 场景 8: Recovery — 重启后历史状态恢复

**步骤**:

1. 向过期分区写入 PK 数据（如 100 条）
2. 确认 lookup 能查到写入的数据
3. 重启 TabletServer（或 kill + restart `__historical__` bucket 的 leader）
4. 等待 recovery 完成
5. 再次 lookup 之前写入的数据

```sql
-- 步骤 1: 批量写入过期分区数据
INSERT INTO fluss_catalog.fluss.pk_table VALUES
    ('20260401', 1, 'user-1', 10.0),
    ('20260401', 2, 'user-2', 20.0),
    ('20260401', 3, 'user-3', 30.0),
    -- ... 省略，实际写入 100 条
    ('20260401', 100, 'user-100', 1000.0);

-- 步骤 2: 确认 lookup 能查到（通过 lookup join 或直接 API 调用）
-- lookup(dt='20260401', id=1)  → 预期: name='user-1', amount=10.0
-- lookup(dt='20260401', id=50) → 预期: name='user-50', amount=500.0

-- 步骤 3: 重启 TabletServer
-- $ ./bin/tablet-server.sh stop
-- $ ./bin/tablet-server.sh start

-- 步骤 4: 等待 recovery 完成（观察日志）
-- 日志应显示: "Replaying WAL for __historical__ from tieredOffset=..."

-- 步骤 5: 再次 lookup 验证数据恢复
-- lookup(dt='20260401', id=1)  → 预期: name='user-1', amount=10.0
-- lookup(dt='20260401', id=50) → 预期: name='user-50', amount=500.0

-- 同时验证当前分区不受影响
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260518', 999, 'realtime', 1.0);
-- lookup(dt='20260518', id=999) → 预期: name='realtime', amount=1.0
```

**验证**:

- recovery 后所有之前写入的数据都能通过 lookup 查到
- recovery 期间当前分区的读写不受影响（`__historical__` recovery 只影响历史路径）
- recovery 日志显示从 tiered offset replay WAL

### 场景 9: Cleanup — Tiering 完成后 RocksDB 清理

**步骤**:

1. 向过期分区写入一批 PK 数据
2. 确认 lookup 能查到（数据在 local RocksDB）
3. 等待 tiering 完成（`tieredOffset >= logEndOffset`）
4. 确认 cleanup 触发（观察日志或 metrics）
5. 再次 lookup 之前写入的数据

```sql
-- 步骤 1: 写入过期分区数据
INSERT INTO fluss_catalog.fluss.pk_table VALUES
    ('20260415', 1, 'cleanup-test-1', 100.0),
    ('20260415', 2, 'cleanup-test-2', 200.0),
    ('20260415', 3, 'cleanup-test-3', 300.0);

-- 步骤 2: 确认 lookup 能查到（数据在 local RocksDB）
-- lookup(dt='20260415', id=1) → 预期: name='cleanup-test-1', amount=100.0

-- 步骤 3-4: 等待 tiering 完成 + cleanup 触发
-- 观察日志: "Historical partition RocksDB cleanup completed for ..."
-- 观察日志: "tieredOffset >= logEndOffset, cleaning up historical RocksDB"

-- 步骤 5: cleanup 后再次 lookup（fall through 到 lake）
-- lookup(dt='20260415', id=1) → 预期: name='cleanup-test-1', amount=100.0（来自 lake）

-- 额外验证: cleanup 后再次写入 → RocksDB lazy 重建
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260415', 4, 'after-cleanup', 400.0);
-- lookup(dt='20260415', id=4) → 预期: name='after-cleanup', amount=400.0（来自重建的 RocksDB）
```

**验证**:

- cleanup 后 historical RocksDB 被清理
- lookup 仍返回正确值（fall through 到 lake）
- 再向过期分区写入新数据 → RocksDB 被 lazy 重建 → 写入成功

### 场景 10: Cleanup 与并发操作的协调

**步骤**:

1. 向过期分区写入一批数据
2. 触发 tiering，等待接近完成
3. 在 tiering 完成（即将触发 cleanup）的同时：
   - 持续 lookup 过期分区数据
   - 向过期分区写入新数据
4. 观察行为

```sql
-- 步骤 1: 写入初始数据
INSERT INTO fluss_catalog.fluss.pk_table VALUES
    ('20260410', 1, 'concurrent-1', 100.0),
    ('20260410', 2, 'concurrent-2', 200.0);

-- 步骤 2-3: 等待 tiering 接近完成时，并发执行以下操作：

-- 并发操作 A: 持续 lookup（在 Flink 作业或脚本中循环执行）
-- lookup(dt='20260410', id=1) → 每次都应返回正确值，无异常

-- 并发操作 B: 写入新数据（在 cleanup 时间窗口内执行）
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20260410', 3, 'during-cleanup', 300.0);

-- 步骤 4: cleanup 完成后验证
-- lookup(dt='20260410', id=1) → 预期: name='concurrent-1', amount=100.0（来自 lake）
-- lookup(dt='20260410', id=3) → 预期: name='during-cleanup', amount=300.0
--   如果写入在 cleanup 之前到达: 数据在重建的 RocksDB 或 lake 中
--   如果写入导致 cleanup 被跳过: 数据在 local RocksDB 中
```

**验证**:

- 如果 cleanup 时有新写入到达 → cleanup 被跳过（re-check `tieredOffset >= logEndOffset` 失败）
- 如果 cleanup 时有并发 lookup → reference counting 协调，lookup 不会读到已关闭的 RocksDB
- cleanup 完成后 → 后续 lookup fall through 到 lake → 返回正确值

**实现提示**:

- 可通过注入测试钩子（如在 cleanup re-check 前添加可控延迟/latch）来拉大时间窗口，确保并发操作能在 cleanup 过程中执行
- 或使用小数据量 + 短 tiering 间隔，反复触发 cleanup，同时持续进行 lookup/write，通过统计验证无异常

---

### 场景 11: 边界场景 — `dynamicPartitionEnabled = false`

**目的**: 验证 `__historical__` 作为系统分区不受 `dynamicPartitionEnabled` 配置限制。

**步骤**:

1. 创建 auto-partitioned + lake-enabled 的 PK 表，设置 `'table.auto-partition.dynamic-partition.enabled' = 'false'`
2. 向过期分区写入数据

```sql
-- 步骤 1: 创建禁用动态分区的表（初始 retention 设大）
CREATE TABLE fluss.pk_table_no_dynamic (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '30',
    'table.auto-partition.dynamic-partition.enabled' = 'false',
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);

-- 步骤 1a: 向 dt='20260401' 写入数据（retention=30，分区存活）
INSERT INTO fluss_catalog.fluss.pk_table_no_dynamic VALUES ('20260401', 1, 'test', 100.0);
-- 等待 tiering 完成（如需要 lake 数据）

-- 步骤 1b: 调小 retention，触发分区过期
ALTER TABLE fluss_catalog.fluss.pk_table_no_dynamic SET ('table.auto-partition.num-retention' = '3');
-- 等待 AutoPartitionManager 删除 dt='20260401'

-- 步骤 2: 向已过期分区写入数据（应该触发 __historical__ 创建，不受 dynamic-partition 限制）
INSERT INTO fluss_catalog.fluss.pk_table_no_dynamic VALUES ('20260401', 1, 'test-updated', 200.0);

-- 验证: 数据出现在 __historical__ 中
SELECT * FROM fluss_catalog.fluss.pk_table_no_dynamic$historical /*+ OPTIONS('scan.startup.mode' = 'earliest') */;
-- 预期返回: 包含 dt='20260401' 的记录
```

**验证**:

- `__historical__` 分区被成功创建（系统分区创建绕过 `dynamicPartitionEnabled`）
- 写入成功，数据出现在 `__historical__` 中

### 场景 12: 边界场景 — 非过期分区的错误处理

**步骤**:

1. 向不存在的但命名合法且在 retention window 内的分区写入（如未来日期 `dt = '20990101'`）
2. 向不存在的但命名非法的分区写入（如 `dt = 'invalid-date'`）
3. 向非 lake-enabled 表的不存在分区写入

```sql
-- 步骤 1: 写入未来日期分区（在 retention window 内，但分区不存在）
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('20990101', 1, 'future', 100.0);
-- 预期: 抛出 PartitionNotExistException，不 redirect 到 __historical__

-- 步骤 2: 写入非法日期分区
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('invalid-date', 1, 'bad', 100.0);
-- 预期: 抛出 PartitionNotExistException，不 redirect 到 __historical__

-- 步骤 3: 创建非 lake-enabled 表并写入不存在的分区
CREATE TABLE fluss.pk_table_no_lake (
    dt STRING,
    id BIGINT,
    name STRING,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '3',
    'bucket.num' = '4'
    -- 注意: 没有 'table.datalake.enabled' = 'true'
);

INSERT INTO fluss_catalog.fluss.pk_table_no_lake VALUES ('20260401', 1, 'no-lake', 100.0);
-- 预期: 抛出 PartitionNotExistException，非 lake-enabled 表不支持历史分区
```

**验证**:

- 三种情况都抛 `PartitionNotExistException`，不 redirect 到 `__historical__`
- 过期谓词准确区分真正过期 vs 其他不存在情况

### 场景 13: 边界场景 — `__historical__` 名称保留

**步骤**:

1. 尝试通过 DDL 创建名为 `__historical__` 的分区
2. 如果动态分区命名模式能产生 `__historical__`（极不可能），验证被拒绝

```sql
-- 步骤 1: 尝试直接写入名为 __historical__ 的分区（通过 INSERT）
INSERT INTO fluss_catalog.fluss.pk_table VALUES ('__historical__', 1, 'hack', 100.0);
-- 预期: 被拒绝，__historical__ 是系统保留名称，用户不能直接创建

-- 步骤 2: 尝试通过 ALTER TABLE 添加 __historical__ 分区（如果支持）
-- ALTER TABLE fluss_catalog.fluss.pk_table ADD PARTITION (dt = '__historical__');
-- 预期: 被拒绝，返回错误信息表明 __historical__ 是保留名称
```

**验证**:

- 用户创建 `__historical__` 被拒绝，返回错误
- 只有系统通过内部路径才能创建

---

### 数据正确性 — 三路径一致性验证

> **核心思路**: 对同一批数据，通过三条独立路径读取并比对最终状态，任何两条路径不一致即为 bug。
>
> - **路径 A — Fluss Lookup**: 通过 lookup join 逐 key 查询 Fluss（PK 表专属）
> - **路径 B — Changelog Replay**: 从 `__historical__` 消费完整 changelog，replay 得到最终状态
> - **路径 C — Paimon 直查**: tiering 完成后，绕过 Fluss 直接查询 Paimon 表获取物化结果
>
> 本节所有场景**从零开始**，包含独立的表创建、环境构造、数据写入和验证流程，不依赖前面场景的数据或状态。

#### 环境准备

**Paimon 直查 catalog 注册**（所有正确性场景共用）:

```sql
-- 注册 Paimon catalog，直接访问底层 lake 存储（绕过 Fluss）
CREATE CATALOG paimon_direct WITH (
    'type' = 'paimon',
    'warehouse' = '<paimon-warehouse-path>'
);

-- 注册用于存放比对结果的 database
CREATE DATABASE IF NOT EXISTS paimon_direct.verify;
```

### 场景 14: PK 表历史分区 — INSERT / UPDATE / DELETE 后三路径一致性

**目的**: 对历史分区执行一系列 INSERT / UPDATE / DELETE 操作后，验证三条读取路径返回完全一致的最终状态。每条操作的预期结果可手动推导。

#### 第一步：建表

```sql
CREATE TABLE fluss.pk_verify_14 (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '30',
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

#### 第二步：写入初始数据并制造分区过期

```sql
-- 2a: 写入初始数据（num-retention=30，dt='20260501' 在 retention window 内，走正常路径）
INSERT INTO fluss_catalog.fluss.pk_verify_14 VALUES
    ('20260501', 1, 'Alice', 100.0),
    ('20260501', 2, 'Bob', 200.0),
    ('20260501', 10, 'Lake-Only', 999.0);

-- 2b: 等待 tiering 完成（确认数据已落入 Paimon）
-- 可通过 Paimon 直查确认:
-- SELECT COUNT(*) FROM paimon_direct.fluss.pk_verify_14 WHERE dt = '20260501';
-- 预期: 3

-- 2c: 调小 retention，触发分区过期
ALTER TABLE fluss_catalog.fluss.pk_verify_14 SET ('table.auto-partition.num-retention' = '3');
-- 等待 AutoPartitionManager 检查周期，dt='20260501' 超出 3 天 retention → 被删除
-- 此时 lake 中有: (id=1, 'Alice', 100.0), (id=2, 'Bob', 200.0), (id=10, 'Lake-Only', 999.0)
```

#### 第三步：对过期分区执行混合操作

```sql
-- 3a: INSERT 新 key
INSERT INTO fluss_catalog.fluss.pk_verify_14 VALUES
    ('20260501', 20, 'New-1', 200.0),
    ('20260501', 21, 'New-2', 210.0),
    ('20260501', 22, 'New-3', 220.0);

-- 3b: UPDATE lake 中已有的 key（id=1 的旧值在 lake 中）
INSERT INTO fluss_catalog.fluss.pk_verify_14 VALUES
    ('20260501', 1, 'Alice-Updated', 150.0);

-- 3c: DELETE
DELETE FROM fluss_catalog.fluss.pk_verify_14 WHERE dt = '20260501' AND id = 21;
DELETE FROM fluss_catalog.fluss.pk_verify_14 WHERE dt = '20260501' AND id = 2;

-- 3d: 二次 UPDATE（INSERT 后再 UPDATE，old-value 来自 prewrite buffer / RocksDB）
INSERT INTO fluss_catalog.fluss.pk_verify_14 VALUES
    ('20260501', 20, 'New-1-V2', 250.0);
```

#### 手动推导的预期最终状态

| dt | id | name | amount | 说明 |
|---|---|---|---|---|
| 20260501 | 1 | Alice-Updated | 150.0 | lake 旧值 `Alice/100.0` 被 UPDATE |
| 20260501 | 10 | Lake-Only | 999.0 | lake 旧值，未被修改 |
| 20260501 | 20 | New-1-V2 | 250.0 | INSERT `New-1/200.0` 后再 UPDATE |
| 20260501 | 22 | New-3 | 220.0 | INSERT，未被修改 |

（id=2 和 id=21 已被 DELETE，不应出现；共 **4 行**）

#### 第四步：等待 tiering 完成

```sql
-- 等待 __historical__ 的数据落入 Paimon
-- 可观察日志或轮询 Paimon 直查:
-- SELECT COUNT(*) FROM paimon_direct.fluss.pk_verify_14 WHERE dt = '20260501';
-- 当行数稳定为 4 时表示 tiering 完成
```

#### 第五步：三路径验证

**路径 A — Fluss Lookup**:

```sql
CREATE TEMPORARY TABLE verify_keys_14 (
    id BIGINT,
    dt AS '20260501',
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'number-of-rows' = '22',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '22'
);

SELECT k.dt, k.id, t.name, t.amount
FROM verify_keys_14 k
LEFT JOIN fluss_catalog.fluss.pk_verify_14 FOR SYSTEM_TIME AS OF k.proc_time AS t
ON k.dt = t.dt AND k.id = t.id;

-- 预期:
-- id=1  → name='Alice-Updated', amount=150.0
-- id=2  → null（已 DELETE）
-- id=10 → name='Lake-Only', amount=999.0
-- id=20 → name='New-1-V2', amount=250.0
-- id=21 → null（已 DELETE）
-- id=22 → name='New-3', amount=220.0
```

**路径 B — Changelog Replay**:

```sql
-- 将 __historical__ 的 changelog 写入 Paimon 结果表，利用 upsert 语义自动 replay
CREATE TABLE paimon_direct.verify.changelog_result_14 (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
);

-- 设置 Flink 为 streaming 模式
SET 'execution.runtime-mode' = 'streaming';

INSERT INTO paimon_direct.verify.changelog_result_14
SELECT dt, id, name, amount
FROM fluss_catalog.fluss.pk_verify_14$historical
    /*+ OPTIONS('scan.startup.mode' = 'earliest') */;

-- 等待作业消费完毕后取消，查询结果:
SELECT * FROM paimon_direct.verify.changelog_result_14 ORDER BY id;
-- 预期: 与上述最终状态表完全一致（4 行）
```

**路径 C — Paimon 直查**:

```sql
SELECT * FROM paimon_direct.fluss.pk_verify_14
WHERE dt = '20260501'
ORDER BY id;
-- 预期: 4 行，与上述最终状态表完全一致
```

**比对**:

```sql
-- 行数一致
SELECT 'changelog' AS path, COUNT(*) AS cnt FROM paimon_direct.verify.changelog_result_14
UNION ALL
SELECT 'paimon', COUNT(*) FROM paimon_direct.fluss.pk_verify_14 WHERE dt = '20260501';
-- 两者均为 4

-- EXCEPT 差集为空
(SELECT dt, id, name, amount FROM paimon_direct.verify.changelog_result_14
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.fluss.pk_verify_14 WHERE dt = '20260501')
UNION ALL
(SELECT dt, id, name, amount FROM paimon_direct.fluss.pk_verify_14 WHERE dt = '20260501'
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.verify.changelog_result_14);
-- 预期: 0 行
```

**验证**:

- 三条路径返回的最终状态完全一致（4 行，每行每列的值相同）
- 已 DELETE 的 key（id=2, id=21）在三条路径中均不返回
- 未被修改的 lake 旧值（id=10）在三条路径中均正确返回
- UPDATE 后的值（id=1, id=20）在三条路径中均为最后一次写入值

### 场景 15: PK 表历史分区 — 多过期分区 + Composite Key 隔离的三路径一致性

**目的**: 多个不同过期分区的数据写入同一个 `__historical__`，验证 composite key 隔离在三路径比对下完全正确——相同 id 在不同 original partition 下互不干扰。

#### 第一步：建表

```sql
CREATE TABLE fluss.pk_verify_15 (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '30',
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

#### 第二步：写入初始数据并制造分区过期

```sql
-- 2a: 向两个分区各写入初始数据
INSERT INTO fluss_catalog.fluss.pk_verify_15 VALUES
    ('20260401', 1, 'April-Alice', 100.0),
    ('20260401', 2, 'April-Bob', 200.0),
    ('20260401', 3, 'April-Charlie', 300.0),
    ('20260402', 1, 'May-Alice', 1000.0),
    ('20260402', 2, 'May-Bob', 2000.0),
    ('20260402', 3, 'May-Charlie', 3000.0);

-- 2b: 等待 tiering 完成

-- 2c: 调小 retention，触发两个分区过期
ALTER TABLE fluss_catalog.fluss.pk_verify_15 SET ('table.auto-partition.num-retention' = '3');
-- 等待 dt='20260401' 和 dt='20260402' 都被删除
```

#### 第三步：对两个过期分区分别执行操作

```sql
-- dt='20260401': UPDATE id=1, DELETE id=2, INSERT id=4
INSERT INTO fluss_catalog.fluss.pk_verify_15 VALUES
    ('20260401', 1, 'April-Alice-V2', 150.0),
    ('20260401', 4, 'April-Dave', 400.0);
DELETE FROM fluss_catalog.fluss.pk_verify_15 WHERE dt = '20260401' AND id = 2;

-- dt='20260402': UPDATE id=1（注意 id 与 dt='20260401' 的 id=1 相同）, DELETE id=3, INSERT id=5
INSERT INTO fluss_catalog.fluss.pk_verify_15 VALUES
    ('20260402', 1, 'May-Alice-V2', 1500.0),
    ('20260402', 5, 'May-Eve', 5000.0);
DELETE FROM fluss_catalog.fluss.pk_verify_15 WHERE dt = '20260402' AND id = 3;
```

#### 预期最终状态

**dt='20260401'**:

| dt | id | name | amount | 说明 |
|---|---|---|---|---|
| 20260401 | 1 | April-Alice-V2 | 150.0 | UPDATE |
| 20260401 | 3 | April-Charlie | 300.0 | 未修改 |
| 20260401 | 4 | April-Dave | 400.0 | 新 INSERT |

（id=2 已 DELETE；共 **3 行**）

**dt='20260402'**:

| dt | id | name | amount | 说明 |
|---|---|---|---|---|
| 20260402 | 1 | May-Alice-V2 | 1500.0 | UPDATE |
| 20260402 | 2 | May-Bob | 2000.0 | 未修改 |
| 20260402 | 5 | May-Eve | 5000.0 | 新 INSERT |

（id=3 已 DELETE；共 **3 行**；总计 **6 行**）

**关键**: id=1 同时存在于两个分区，值完全不同（150.0 vs 1500.0），composite key 隔离正确则互不干扰。

#### 第四步：等待 tiering 完成

#### 第五步：三路径验证

**路径 A — Fluss Lookup**:

```sql
-- 构造两个分区的所有待验证 key
CREATE TEMPORARY TABLE verify_keys_15 (
    dt STRING,
    id BIGINT,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'number-of-rows' = '10',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '5'
);
-- 注意: datagen 无法精确生成两组 dt，实际需用自定义 Source 或两轮 lookup

-- 第一组: dt='20260401'
-- lookup(dt='20260401', id=1) → April-Alice-V2 / 150.0
-- lookup(dt='20260401', id=2) → null（已 DELETE）
-- lookup(dt='20260401', id=3) → April-Charlie / 300.0
-- lookup(dt='20260401', id=4) → April-Dave / 400.0

-- 第二组: dt='20260402'
-- lookup(dt='20260402', id=1) → May-Alice-V2 / 1500.0（不是 April-Alice-V2！）
-- lookup(dt='20260402', id=2) → May-Bob / 2000.0
-- lookup(dt='20260402', id=3) → null（已 DELETE）
-- lookup(dt='20260402', id=5) → May-Eve / 5000.0
```

**路径 B — Changelog Replay**:

```sql
CREATE TABLE paimon_direct.verify.changelog_result_15 (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
);

INSERT INTO paimon_direct.verify.changelog_result_15
SELECT dt, id, name, amount
FROM fluss_catalog.fluss.pk_verify_15$historical
    /*+ OPTIONS('scan.startup.mode' = 'earliest') */;

-- 等待消费完毕后查询:
SELECT * FROM paimon_direct.verify.changelog_result_15 ORDER BY dt, id;
-- 预期: 6 行，与上述最终状态完全一致
```

**路径 C — Paimon 直查**:

```sql
SELECT * FROM paimon_direct.fluss.pk_verify_15
WHERE dt IN ('20260401', '20260402')
ORDER BY dt, id;
-- 预期: 6 行
```

**比对**:

```sql
-- 行数
SELECT 'changelog' AS path, COUNT(*) FROM paimon_direct.verify.changelog_result_15
UNION ALL
SELECT 'paimon', COUNT(*) FROM paimon_direct.fluss.pk_verify_15 WHERE dt IN ('20260401', '20260402');
-- 均为 6

-- EXCEPT 差集
(SELECT dt, id, name, amount FROM paimon_direct.verify.changelog_result_15
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.fluss.pk_verify_15 WHERE dt IN ('20260401', '20260402'))
UNION ALL
(SELECT dt, id, name, amount FROM paimon_direct.fluss.pk_verify_15 WHERE dt IN ('20260401', '20260402')
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.verify.changelog_result_15);
-- 预期: 0 行
```

**验证**:

- 三条路径返回 6 行，完全一致
- id=1 在 `dt='20260401'` 和 `dt='20260402'` 下返回各自独立的值，无串扰
- 分区级 DELETE 不影响另一个分区的同 id 数据

### 场景 16: Tiering 前后 Lookup 一致性

**目的**: 同一组 key 在 tiering 前（数据在 `__historical__` 的 local RocksDB）和 tiering 后 cleanup 完成（数据在 lake）两个阶段的 lookup 结果完全一致。确保 RocksDB → lake 的数据迁移不丢失、不篡改。

#### 第一步：建表

```sql
CREATE TABLE fluss.pk_verify_16 (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '3',
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

注意：此表直接用 `num-retention = 3`，`dt='20260420'` 已在 retention window 外，写入即 redirect 到 `__historical__`。

#### 第二步：写入过期分区数据

```sql
INSERT INTO fluss_catalog.fluss.pk_verify_16 VALUES
    ('20260420', 1, 'pre-tier-1', 100.0),
    ('20260420', 2, 'pre-tier-2', 200.0),
    ('20260420', 3, 'pre-tier-3', 300.0),
    ('20260420', 4, 'pre-tier-4', 400.0),
    ('20260420', 5, 'pre-tier-5', 500.0),
    ('20260420', 6, 'pre-tier-6', 600.0),
    ('20260420', 7, 'pre-tier-7', 700.0),
    ('20260420', 8, 'pre-tier-8', 800.0),
    ('20260420', 9, 'pre-tier-9', 900.0),
    ('20260420', 10, 'pre-tier-10', 1000.0);
```

#### 第三步：tiering 前 Lookup（数据在 local RocksDB）

```sql
-- 立即 lookup，记录结果集 R1
-- lookup(dt='20260420', id=1)  → 预期: pre-tier-1 / 100.0
-- lookup(dt='20260420', id=2)  → 预期: pre-tier-2 / 200.0
-- ...
-- lookup(dt='20260420', id=10) → 预期: pre-tier-10 / 1000.0
-- lookup(dt='20260420', id=99) → 预期: null（不存在的 key）

-- 使用 lookup join 收集全量结果写入 Paimon:
CREATE TABLE paimon_direct.verify.lookup_before_tier_16 (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
);

-- 生成 id = 1 ~ 10 的 lookup key
CREATE TEMPORARY TABLE keys_16 (
    id BIGINT,
    dt AS '20260420',
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'number-of-rows' = '10',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10'
);

INSERT INTO paimon_direct.verify.lookup_before_tier_16
SELECT k.dt, k.id, t.name, t.amount
FROM keys_16 k
JOIN fluss_catalog.fluss.pk_verify_16 FOR SYSTEM_TIME AS OF k.proc_time AS t
ON k.dt = t.dt AND k.id = t.id;
```

#### 第四步：等待 tiering + RocksDB cleanup

```
观察日志:
  "Historical partition RocksDB cleanup completed for ..."
  "tieredOffset >= logEndOffset, cleaning up historical RocksDB"
```

#### 第五步：tiering 后 Lookup（数据在 lake，RocksDB 已清理）

```sql
CREATE TABLE paimon_direct.verify.lookup_after_tier_16 (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
);

-- 重新创建 key source（或复用）
CREATE TEMPORARY TABLE keys_16_after (
    id BIGINT,
    dt AS '20260420',
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'number-of-rows' = '10',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '10'
);

INSERT INTO paimon_direct.verify.lookup_after_tier_16
SELECT k.dt, k.id, t.name, t.amount
FROM keys_16_after k
JOIN fluss_catalog.fluss.pk_verify_16 FOR SYSTEM_TIME AS OF k.proc_time AS t
ON k.dt = t.dt AND k.id = t.id;
```

#### 第六步：比对 R1 vs R2

```sql
-- 行数一致
SELECT 'before' AS phase, COUNT(*) FROM paimon_direct.verify.lookup_before_tier_16
UNION ALL
SELECT 'after', COUNT(*) FROM paimon_direct.verify.lookup_after_tier_16;
-- 均为 10

-- EXCEPT 差集为空
(SELECT dt, id, name, amount FROM paimon_direct.verify.lookup_before_tier_16
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.verify.lookup_after_tier_16)
UNION ALL
(SELECT dt, id, name, amount FROM paimon_direct.verify.lookup_after_tier_16
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.verify.lookup_before_tier_16);
-- 预期: 0 行

-- 也与 Paimon 直查比对
(SELECT dt, id, name, amount FROM paimon_direct.verify.lookup_after_tier_16
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.fluss.pk_verify_16 WHERE dt = '20260420')
UNION ALL
(SELECT dt, id, name, amount FROM paimon_direct.fluss.pk_verify_16 WHERE dt = '20260420'
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.verify.lookup_after_tier_16);
-- 预期: 0 行
```

**验证**:

- R1（tiering 前，来自 RocksDB）与 R2（tiering 后，来自 lake fallback）完全一致
- R2 与 Paimon 直查结果也完全一致
- 数据类型精度无损（特别是 DOUBLE 类型的 amount 字段，如 100.0 不变为 100.00000001）
- 不存在的 key 在两个阶段都返回 null

### 场景 17: Log 表历史分区 — 写入后双路径一致性

**目的**: Log 表没有 PK、没有 lookup 路径，验证 changelog 消费和 Paimon 直查两条路径一致。

#### 第一步：建表

```sql
CREATE TABLE fluss.log_verify_17 (
    dt STRING,
    event_id BIGINT,
    payload STRING
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '3',
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

注意：`num-retention = 3`，`dt='20260401'` 已在 retention window 外，写入即 redirect 到 `__historical__`。

#### 第二步：写入过期分区数据

```sql
INSERT INTO fluss_catalog.fluss.log_verify_17 VALUES
    ('20260401', 1001, 'event-alpha'),
    ('20260401', 1002, 'event-beta'),
    ('20260401', 1003, 'event-gamma'),
    ('20260401', 1004, 'event-delta'),
    ('20260401', 1005, 'event-epsilon'),
    ('20260401', 1006, 'event-zeta'),
    ('20260401', 1007, 'event-eta'),
    ('20260401', 1008, 'event-theta'),
    ('20260401', 1009, 'event-iota'),
    ('20260401', 1010, 'event-kappa');
```

#### 第三步：等待 tiering 完成

#### 第四步：双路径验证

**路径 B — Changelog 消费**:

```sql
CREATE TABLE paimon_direct.verify.changelog_result_17 (
    dt STRING,
    event_id BIGINT,
    payload STRING
);

INSERT INTO paimon_direct.verify.changelog_result_17
SELECT dt, event_id, payload
FROM fluss_catalog.fluss.log_verify_17$historical
    /*+ OPTIONS('scan.startup.mode' = 'earliest') */;

-- 等待消费完毕后查询:
SELECT * FROM paimon_direct.verify.changelog_result_17 ORDER BY event_id;
-- 预期: 10 行，event_id = 1001 ~ 1010
```

**路径 C — Paimon 直查**:

```sql
SELECT * FROM paimon_direct.fluss.log_verify_17
WHERE dt = '20260401'
ORDER BY event_id;
-- 预期: 10 行
```

**比对**:

```sql
-- 行数
SELECT 'changelog' AS path, COUNT(*) FROM paimon_direct.verify.changelog_result_17
UNION ALL
SELECT 'paimon', COUNT(*) FROM paimon_direct.fluss.log_verify_17 WHERE dt = '20260401';
-- 均为 10

-- EXCEPT 差集
(SELECT dt, event_id, payload FROM paimon_direct.verify.changelog_result_17
 EXCEPT
 SELECT dt, event_id, payload FROM paimon_direct.fluss.log_verify_17 WHERE dt = '20260401')
UNION ALL
(SELECT dt, event_id, payload FROM paimon_direct.fluss.log_verify_17 WHERE dt = '20260401'
 EXCEPT
 SELECT dt, event_id, payload FROM paimon_direct.verify.changelog_result_17);
-- 预期: 0 行
```

**验证**:

- 两条路径行数均为 10，EXCEPT 差集为空
- 每条记录的 `event_id` 和 `payload` 完全一致
- 记录不重复（`COUNT(DISTINCT event_id) = 10`）、不丢失
- consumer 能从 `__historical__` 的 row payload 中还原原始 `dt = '20260401'`

---

### 大规模数据正确性验证

> 上述场景 14-17 使用少量已知数据做精确比对，适合定位 bug。本节用 **10 万~100 万级** 数据量做压力下的正确性验证，覆盖多 bucket 分布、多轮 tiering、batch 边界、并发竞争等小数据量无法暴露的问题。
>
> **核心方法**: 写入端维护一份 **"真值"（ground truth）**，读取端通过多条路径独立计算结果，与真值比对。比对指标包括：行数、checksum（所有 amount 求和）、逐 key 抽样校验。

### 场景 18: PK 表 — 10 万级 INSERT + UPDATE + DELETE 全量比对

**目的**: 大数据量下验证 `__historical__` 路径的写入和读取正确性。数据量足以覆盖所有 bucket（4 个 bucket，每 bucket 约 2.5 万 key）、多次 batch flush、多轮 tiering。

**步骤**:

1. 创建独立的测试表（避免与其他场景数据干扰）：

```sql
CREATE TABLE fluss.pk_correctness_large (
    dt STRING,
    id BIGINT,
    name STRING,
    amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '1',
    'table.auto-partition.num-retention' = '3',
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4'
);
```

2. 分阶段写入（总计 10 万条 key，通过自定义 Flink Source 或脚本批量生成）：

```
阶段 1 — 全量 INSERT:
  向过期分区 dt='20260401' 写入 id = 1 ~ 100,000
  每条记录: (dt='20260401', id, name='orig-{id}', amount=id * 1.0)
  → 写入完成后 ground truth: 100,000 条记录

阶段 2 — 批量 UPDATE（30%）:
  对 id = 1 ~ 30,000 执行 upsert
  每条记录: (dt='20260401', id, name='updated-{id}', amount=id * 2.0)
  → ground truth: 100,000 条，其中 30,000 条的 name/amount 被更新

阶段 3 — 批量 DELETE（10%）:
  对 id = 90,001 ~ 100,000 执行 delete
  → ground truth: 90,000 条存活记录

阶段 4 — 二次 UPDATE（覆盖部分已更新的 key）:
  对 id = 1 ~ 10,000 执行 upsert
  每条记录: (dt='20260401', id, name='final-{id}', amount=id * 3.0)
  → ground truth: 90,000 条，其中 id=1~10,000 的值为最后一次 UPDATE 值
```

```sql
-- 阶段 1: 全量 INSERT（使用 datagen 或自定义 Source）
INSERT INTO fluss_catalog.fluss.pk_correctness_large
SELECT '20260401' AS dt,
       id,
       CONCAT('orig-', CAST(id AS STRING)) AS name,
       CAST(id AS DOUBLE) AS amount
FROM source_100k;  -- 生成 id = 1 ~ 100,000

-- 阶段 2: UPDATE 前 30%
INSERT INTO fluss_catalog.fluss.pk_correctness_large
SELECT '20260401' AS dt,
       id,
       CONCAT('updated-', CAST(id AS STRING)) AS name,
       CAST(id AS DOUBLE) * 2.0 AS amount
FROM source_30k;  -- 生成 id = 1 ~ 30,000

-- 阶段 3: DELETE 后 10%
-- 通过程序调用 DELETE 或生成 -D changelog 写入
-- DELETE FROM ... WHERE dt = '20260401' AND id BETWEEN 90001 AND 100000

-- 阶段 4: 二次 UPDATE 前 10%
INSERT INTO fluss_catalog.fluss.pk_correctness_large
SELECT '20260401' AS dt,
       id,
       CONCAT('final-', CAST(id AS STRING)) AS name,
       CAST(id AS DOUBLE) * 3.0 AS amount
FROM source_10k;  -- 生成 id = 1 ~ 10,000
```

3. 等待所有数据 tiering 完成。

4. 通过三条路径验证，与 ground truth 比对：

```sql
-- === 路径 A: Fluss Lookup — 逐 key 遍历 ===
-- 使用 Flink 作业遍历 id = 1 ~ 100,000 做 lookup join
-- 将结果写入 Paimon 结果表 lookup_result

CREATE TABLE paimon_direct.verify.lookup_result_18 (
    dt STRING, id BIGINT, name STRING, amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
);

INSERT INTO paimon_direct.verify.lookup_result_18
SELECT k.dt, k.id, t.name, t.amount
FROM all_keys_source k  -- 生成 dt='20260401', id = 1 ~ 100,000
LEFT JOIN fluss_catalog.fluss.pk_correctness_large
    FOR SYSTEM_TIME AS OF k.proc_time AS t
ON k.dt = t.dt AND k.id = t.id
WHERE t.id IS NOT NULL;  -- 过滤已删除的 key

-- === 路径 B: Changelog Replay — 写入 upsert 结果表 ===
CREATE TABLE paimon_direct.verify.changelog_result_18 (
    dt STRING, id BIGINT, name STRING, amount DOUBLE,
    PRIMARY KEY (dt, id) NOT ENFORCED
);

INSERT INTO paimon_direct.verify.changelog_result_18
SELECT dt, id, name, amount
FROM fluss_catalog.fluss.pk_correctness_large$historical
    /*+ OPTIONS('scan.startup.mode' = 'earliest') */;
-- Paimon 的 upsert 语义会自动 replay changelog

-- === 路径 C: Paimon 直查（tiering 后的物化数据）===
-- SELECT * FROM paimon_direct.fluss.pk_correctness_large WHERE dt = '20260401';

-- === 比对: 行数 + checksum + EXCEPT ===

-- 1) 行数比对（三条路径均应为 90,000）
SELECT 'lookup' AS path, COUNT(*) FROM paimon_direct.verify.lookup_result_18
UNION ALL
SELECT 'changelog', COUNT(*) FROM paimon_direct.verify.changelog_result_18
UNION ALL
SELECT 'paimon', COUNT(*) FROM paimon_direct.fluss.pk_correctness_large WHERE dt = '20260401';

-- 2) checksum 比对（amount 总和应一致）
SELECT 'lookup' AS path, SUM(amount) FROM paimon_direct.verify.lookup_result_18
UNION ALL
SELECT 'changelog', SUM(amount) FROM paimon_direct.verify.changelog_result_18
UNION ALL
SELECT 'paimon', SUM(amount) FROM paimon_direct.fluss.pk_correctness_large WHERE dt = '20260401';

-- 3) EXCEPT 差集比对（应为空）
-- Lookup vs Paimon
(SELECT dt, id, name, amount FROM paimon_direct.verify.lookup_result_18
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.fluss.pk_correctness_large WHERE dt = '20260401')
UNION ALL
-- Paimon vs Lookup（双向 EXCEPT 确保完全一致）
(SELECT dt, id, name, amount FROM paimon_direct.fluss.pk_correctness_large WHERE dt = '20260401'
 EXCEPT
 SELECT dt, id, name, amount FROM paimon_direct.verify.lookup_result_18);
-- 结果应为空（0 行）

-- Changelog vs Paimon（同理）
-- ...
```

**Ground Truth 预期最终状态**:

| id 范围 | name | amount | 说明 |
|---|---|---|---|
| 1 ~ 10,000 | `final-{id}` | `id * 3.0` | 阶段 4 二次 UPDATE |
| 10,001 ~ 30,000 | `updated-{id}` | `id * 2.0` | 阶段 2 UPDATE，未被阶段 4 覆盖 |
| 30,001 ~ 90,000 | `orig-{id}` | `id * 1.0` | 阶段 1 INSERT，未被修改 |
| 90,001 ~ 100,000 | — | — | 阶段 3 已 DELETE，不应存在 |

**验证**:

- 三条路径行数均为 **90,000**
- 三条路径 `SUM(amount)` 值一致（可预先计算 ground truth checksum）
- 双向 `EXCEPT` 差集为空
- 抽样检查边界 key：id=1, id=10000, id=10001, id=30000, id=30001, id=90000, id=90001（应为 null）

### 场景 19: 多过期分区大规模并发写入 — 跨分区正确性

**目的**: 验证多个不同过期分区同时大量写入 `__historical__` 时，composite key 隔离在大数据量下依然正确，不发生跨分区数据串扰。

**步骤**:

1. 同时向 5 个过期分区各写入 5 万条数据（总计 25 万条），每个分区使用**相同的 id 范围**（id = 1 ~ 50,000），但不同的 name 前缀和 amount 系数：

```
dt='20260301': name='mar-{id}', amount=id * 1.0   (5 万条)
dt='20260302': name='mar2-{id}', amount=id * 2.0  (5 万条)
dt='20260303': name='mar3-{id}', amount=id * 3.0  (5 万条)
dt='20260304': name='mar4-{id}', amount=id * 4.0  (5 万条)
dt='20260305': name='mar5-{id}', amount=id * 5.0  (5 万条)
```

```sql
-- 使用多个并发 Flink 作业或单个作业内 UNION ALL 同时写入
INSERT INTO fluss_catalog.fluss.pk_correctness_large
SELECT dt, id, name, amount FROM (
    SELECT '20260301' AS dt, id, CONCAT('mar-', CAST(id AS STRING)) AS name,
           CAST(id AS DOUBLE) * 1.0 AS amount FROM source_50k
    UNION ALL
    SELECT '20260302', id, CONCAT('mar2-', CAST(id AS STRING)),
           CAST(id AS DOUBLE) * 2.0 FROM source_50k
    UNION ALL
    SELECT '20260303', id, CONCAT('mar3-', CAST(id AS STRING)),
           CAST(id AS DOUBLE) * 3.0 FROM source_50k
    UNION ALL
    SELECT '20260304', id, CONCAT('mar4-', CAST(id AS STRING)),
           CAST(id AS DOUBLE) * 4.0 FROM source_50k
    UNION ALL
    SELECT '20260305', id, CONCAT('mar5-', CAST(id AS STRING)),
           CAST(id AS DOUBLE) * 5.0 FROM source_50k
);
```

2. 等待 tiering 完成。

3. 分别对每个过期分区做全量 lookup 验证：

```sql
-- 对每个分区分别验证: 行数 + checksum + 抽样
-- 以 dt='20260301' 为例:

-- 行数: 应为 50,000
SELECT COUNT(*) FROM paimon_direct.verify.lookup_result_19
WHERE dt = '20260301';

-- checksum: SUM(amount) = SUM(id * 1.0 for id=1..50000) = 1,250,025,000
SELECT SUM(amount) FROM paimon_direct.verify.lookup_result_19
WHERE dt = '20260301';

-- 抽样: 特定 key 值正确
-- lookup(dt='20260301', id=1)     → name='mar-1', amount=1.0
-- lookup(dt='20260301', id=50000) → name='mar-50000', amount=50000.0
-- lookup(dt='20260302', id=1)     → name='mar2-1', amount=2.0（不是 'mar-1'）
-- lookup(dt='20260302', id=50000) → name='mar2-50000', amount=100000.0

-- 关键: 同一 id 在不同分区的值不同，验证 composite key 隔离
-- id=1 在 5 个分区应分别返回:
--   dt='20260301' → amount=1.0
--   dt='20260302' → amount=2.0
--   dt='20260303' → amount=3.0
--   dt='20260304' → amount=4.0
--   dt='20260305' → amount=5.0
```

**验证**:

- 每个分区恰好 50,000 条，总计 250,000 条
- 每个分区的 `SUM(amount)` 与预计值一致（checksum 不同 = 分区间数据隔离）
- 同一 id 在不同 dt 下返回各自独立的值，无串扰
- `EXCEPT` 比对 Paimon 直查与 lookup 结果一致

### 场景 20: 长时间持续写入 — 多轮 tiering 后累积正确性

**目的**: 模拟真实业务场景，持续向历史分区写入数据，经历多轮 tiering cycle，验证数据在多次 "写入 → tiering → cleanup → 再写入 → 再 tiering" 循环后仍然正确。小数据量通常只经历一轮 tiering，无法暴露跨轮次的状态残留问题。

**步骤**:

1. 启动一个持续写入作业，以稳定速率向过期分区写入数据：

```
写入速率: ~1000 条/秒
目标分区: dt='20260401'（过期）
持续时间: 至少覆盖 3 轮 tiering cycle
  （假设 tiering 间隔 60s，则持续写入 5 分钟以上）
数据生成:
  - 每条记录 id 从 1 开始自增（全局唯一）
  - name = 'batch-{batch_num}-{id}'
  - amount = id * 1.0
```

```sql
-- 使用 datagen 持续写入
CREATE TEMPORARY TABLE continuous_source (
    id BIGINT,
    name STRING,
    amount DOUBLE
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1000',
    'fields.id.kind' = 'sequence',
    'fields.id.start' = '1',
    'fields.id.end' = '300000',  -- 5 分钟 × 1000 TPS = 300,000 条
    'fields.name.length' = '20',
    'fields.amount.min' = '1.0',
    'fields.amount.max' = '100000.0'
);

INSERT INTO fluss_catalog.fluss.pk_correctness_large
SELECT '20260401' AS dt, id, name, amount
FROM continuous_source;
```

2. 写入过程中，观察 tiering 和 cleanup 日志，确认发生多轮：

```
预期日志时间线:
  T+0s:    开始写入
  T+60s:   第 1 轮 tiering 触发，~60,000 条数据落入 Paimon
  T+60s+:  RocksDB cleanup（如果 tieredOffset >= logEndOffset）
  T+70s:   新写入导致 RocksDB 被 lazy 重建
  T+120s:  第 2 轮 tiering 触发
  ...
  T+300s:  写入完成，最终一轮 tiering
```

3. 写入完成后，等待最后一轮 tiering 完成。

4. 验证：

```sql
-- 1) 行数: 应为 300,000（所有 id 唯一，无 UPDATE/DELETE）
SELECT COUNT(*) FROM paimon_direct.fluss.pk_correctness_large
WHERE dt = '20260401';

-- 2) id 连续性: 最小 id = 1, 最大 id = 300,000, COUNT(DISTINCT id) = 300,000
SELECT MIN(id), MAX(id), COUNT(DISTINCT id)
FROM paimon_direct.fluss.pk_correctness_large
WHERE dt = '20260401';

-- 3) Fluss lookup 抽样（多轮 tiering 边界附近的 key）
-- 第 1 轮 tiering 边界附近:
-- lookup(dt='20260401', id=59999)
-- lookup(dt='20260401', id=60000)
-- lookup(dt='20260401', id=60001)
-- 第 2 轮边界附近: 同理
-- 每个 key 都应返回正确值

-- 4) 无重复: 如果有重复 id，以下查询返回非空
SELECT id, COUNT(*) AS cnt
FROM paimon_direct.fluss.pk_correctness_large
WHERE dt = '20260401'
GROUP BY id
HAVING COUNT(*) > 1;
-- 应返回空（0 行）
```

**验证**:

- 300,000 条记录全部写入成功，无丢失（行数 = 300,000）
- 无重复记录（每个 id 只出现一次）
- id 连续无间断（MIN=1, MAX=300,000, DISTINCT COUNT=300,000）
- 多轮 tiering 边界附近的 key lookup 正确（不存在跨轮次的数据残留或覆盖）
- Paimon 直查结果与 Fluss lookup 一致

### 场景 21: PK 表 — 大规模 UPDATE 覆盖后 old-value changelog 正确性

**目的**: 验证大量 UPDATE 操作产生的 changelog（-U / +U）在大数据量下语义正确。重点检查 old-value lookup 在高并发下是否准确——old-value 应精确反映该 key 被更新前的值，不能出现串值或丢失。

**步骤**:

1. 先全量写入 5 万条初始数据（确保 lake 中有旧值可供 old-value lookup）：

```sql
-- 阶段 1: 写入初始数据（分区存活期间写入，正常路径）
-- 建表时 num-retention = 30
INSERT INTO fluss_catalog.fluss.pk_correctness_large
SELECT '20260501' AS dt, id,
       CONCAT('v1-', CAST(id AS STRING)) AS name,
       CAST(id AS DOUBLE) AS amount
FROM source_50k;  -- id = 1 ~ 50,000

-- 等待 tiering 完成（数据落入 Paimon）
-- ALTER TABLE 调小 retention → 分区过期
```

2. 分区过期后，对所有 5 万条 key 做全量 UPDATE：

```sql
-- 阶段 2: 全量 UPDATE（分区已过期，走 __historical__ 路径）
INSERT INTO fluss_catalog.fluss.pk_correctness_large
SELECT '20260501' AS dt, id,
       CONCAT('v2-', CAST(id AS STRING)) AS name,
       CAST(id AS DOUBLE) * 10.0 AS amount
FROM source_50k;  -- id = 1 ~ 50,000，每条都是 UPDATE
```

3. 消费 `__historical__` 的 changelog，验证每条 UPDATE 的 old-value：

```sql
-- 消费 changelog
SELECT * FROM fluss_catalog.fluss.pk_correctness_large$historical
    /*+ OPTIONS('scan.startup.mode' = 'earliest') */;

-- 将 changelog 写入审计表（保留 RowKind 信息）
-- 对于 id = N 的 UPDATE:
--   -U: (dt='20260501', N, 'v1-N', N * 1.0)     ← old-value 来自 lake
--   +U: (dt='20260501', N, 'v2-N', N * 10.0)    ← new-value
```

**验证**:

- 每个 id 恰好产生一对 `-U` / `+U`（共 50,000 对 = 100,000 条 changelog 记录）
- 每条 `-U` 的 old-value 精确等于 `(name='v1-{id}', amount=id * 1.0)`
- 每条 `+U` 的 new-value 精确等于 `(name='v2-{id}', amount=id * 10.0)`
- old-value 无串值：id=X 的 `-U` 的 amount 不等于其他 id 的值
- 不遗漏：50,000 个 id 都有对应的 `-U` / `+U` 对

---

## 3. 性能基准测试

### 3.1 实时分区写入延迟（核心指标）

| 场景 | 关注指标 | 预期 |
|---|---|---|
| 仅实时分区写入（基准） | p99 延迟 | 作为基准值 |
| 混入 10% 过期分区数据 | 实时分区 p99 延迟 | 相对基准劣化 < 10% |
| 混入 50% 过期分区数据 | 实时分区 p99 延迟 | 相对基准劣化 < 10% |
| ioExecutor 队列接近满 | 实时分区 p99 延迟 | 不受影响 |

过期分区的写入延迟不需要关注，可以慢。

### 3.2 实时分区 Lookup 延迟（核心指标）

| 场景 | 关注指标 | 预期 |
|---|---|---|
| 仅实时分区 lookup（基准） | p99 延迟 | 作为基准值 (< 1ms 级别) |
| 混入过期分区 key 的 lookup | 实时分区 key 的 p99 延迟 | 相对基准劣化 < 10% |

过期分区 lookup 延迟（lake fallback）不需要关注。

### 3.3 Recovery 时间

| 场景 | 关注指标 |
|---|---|
| 不同数据量（1K / 10K / 100K 条） | recovery 时间与 WAL replay 数据量成线性关系 |
| recovery 期间当前分区可用性 | 当前分区不受影响 |

验证方式：测量不同数据量下的 recovery 时间，确认线性关系，无异常瓶颈。

---

## 4. 观测与验证手段

### 4.1 关键日志

- `__historical__` 创建日志
- ioExecutor 队列满 / throttle 日志
- Recovery WAL replay 进度日志
- Cleanup 触发与完成日志
- Lake fallback lookup 日志

### 4.2 关键 Metrics（如已暴露）

- `ioExecutor` 队列长度、活跃线程数
- 历史写入 / lookup 的延迟分布
- 当前分区写入 / lookup 的延迟分布
- `HISTORICAL_PARTITION_THROTTLED` 错误计数
- Lake fallback lookup 次数和延迟

### 4.3 数据一致性校验

对每个测试场景，可通过以下方式验证数据一致性：

1. **Fluss lookup vs Paimon 直接查询**: 对同一个 key，Fluss lookup 的返回值应与 Paimon 表直接查询的结果一致（tiering 完成后）
2. **Changelog 完整性**: 从 `__historical__` 消费的 changelog 应用到初始状态后，最终状态应与 lookup 结果一致
3. **跨分区隔离**: 对同一个 id，不同 original partition 的 lookup 应返回各自独立的值

---

## 5. 测试执行 Checklist

```
基本功能:
[ ] 场景 1: PK 表写入过期分区 — upsert + delete + changelog 验证（含 prewrite buffer old-value 路径）
[ ] 场景 2: Log 表写入过期分区 — redirect + 消费验证
[ ] 场景 3: 过期分区 Lookup — lake fallback + local-first
[ ] 场景 3a: Lookup 触发 __historical__ 创建
[ ] 场景 4: 多过期分区 composite key 隔离

隔离性（同一 Pipeline 验证，实时分区 p99 劣化 < 10%）:
[ ] 场景 5: 过期分区写入不影响实时分区写入延迟 + checkpoint 正常完成
[ ] 场景 6: 过期分区 Lookup 不影响实时分区 Lookup 延迟
[ ] 场景 7: Flow Control — throttle 只影响过期分区

Recovery & Cleanup:
[ ] 场景 8:  重启后历史状态恢复
[ ] 场景 9:  Tiering 完成后 RocksDB 清理
[ ] 场景 10: Cleanup 与并发操作协调

边界场景:
[ ] 场景 11: dynamicPartitionEnabled = false
[ ] 场景 12: 非过期分区错误处理
[ ] 场景 13: __historical__ 名称保留

数据正确性 — 三路径一致性（独立建表，Lookup / Changelog Replay / Paimon 直查）:
[ ] 场景 14: PK 表 INSERT/UPDATE/DELETE 混合 — 三路径比对（手动推导预期 4 行）
[ ] 场景 15: PK 表多过期分区 + Composite Key 隔离 — 三路径比对（同 id 不同分区互不干扰）
[ ] 场景 16: Tiering 前后 Lookup 一致性（RocksDB vs lake fallback，EXCEPT 差集为空）
[ ] 场景 17: Log 表双路径一致性（Changelog 消费 vs Paimon 直查，10 条记录）

大规模数据正确性（10 万~30 万级，覆盖多 bucket / 多轮 tiering / 并发竞争）:
[ ] 场景 18: PK 表 10 万级 INSERT+UPDATE+DELETE 全量三路径比对（行数 + checksum + EXCEPT 差集）
[ ] 场景 19: 5 个过期分区各 5 万条并发写入 — 跨分区 composite key 隔离验证（25 万条）
[ ] 场景 20: 持续写入 30 万条，经历 3+ 轮 tiering cycle — 无丢失无重复
[ ] 场景 21: 5 万条全量 UPDATE — old-value changelog 语义正确性（-U/+U 配对，无串值）

性能基准:
[ ] 混合写入时实时分区延迟对比（0% / 10% / 50% 过期，p99 劣化 < 10%）
[ ] 混合 Lookup 时实时分区延迟对比（p99 劣化 < 10%）
[ ] Recovery 时间线性关系验证
```
