# Union Read 基准测试：Deletion Vector (DV) 模式 vs 非 DV 模式

本目录提供一套**可手动执行**的基准测试流程，用于对比 Fluss 主键表在
**开启 Deletion Vector（DV）** 与 **未开启 DV** 两种模式下，
**Union Read（Fluss + Paimon 合并读）** 的查询性能差异。

测试指标：**聚合查询延迟**（`SELECT COUNT(*) / SUM(...)` 的端到端 wall-clock 时间）。

---

## 1. 概述

对比**同一份数据、同一套配置**下，只切换 `table.deletion-vectors.enabled` 时，
Union Read 聚合查询（`COUNT(*)/SUM(...)`）的延迟差异。

- 主流程：§7 多轮「写 → 等同步 → compaction」搭好 lake → §8 停 tiering 再写一批做残留 log → §9 测延迟。
- 唯一变量：DV 开关（两张表 schema / 数据 / 负载 / freshness / compaction 完全一致）。
- **前提**：对比延迟前必须先过 [§9.4 结果校验门](#94-结果校验门必做先于比较延迟)（两表聚合值逐列相等），否则不可比。

---

## 2. 测试目标与指标

- **主指标**：批模式 Union Read 聚合查询延迟
  - `Q1 = SELECT COUNT(*) FROM <table>`
  - `Q2 = SELECT COUNT(*), SUM(amount), SUM(version) FROM <table>`（强制读取列值，避免 count 下推走捷径）
- **对照组**：
  - `T_baseline`：`table.datalake.enabled=true`，**不开 DV**（MOR）
  - `T_dv`：`table.datalake.enabled=true`，**开 DV**（merge-free）
- **辅助指标（可选，验证机制）**：`$lake` 后缀的 Paimon-only 读延迟，隔离出纯 Paimon 层 MOR vs merge-free 的差异。
- **控制变量**：两张表 schema、数据、更新负载、freshness、compaction 配置完全一致，**仅 DV 开关不同**。

---

## 3. 环境准备

### 3.1 组件版本

| 组件 | 版本 |
|---|---|
| Fluss | 当前分支（`paimon-dv-support`）构建产物 |
| Flink | 1.20（与 `fluss-flink-1.20` 对齐；其他受支持版本亦可） |
| Paimon | 1.3（Tiering Service 要求）|
| JDK | 运行时 Java 8 兼容，构建用 Java 11 |

### 3.2 构建产物

```bash
cd /Users/yuxia/Projects/fluss/fluss
./mvnw clean install -DskipTests -T 1C
# Fluss 发行包： fluss-dist/target/fluss-*-bin.tgz
# Tiering 作业 jar： fluss-flink/fluss-flink-tiering/target/fluss-flink-tiering-*.jar
# Union Read 所需 jar： fluss-lake/fluss-lake-paimon/target/fluss-lake-paimon-*.jar
```

### 3.3 把 Union Read 所需 jar 放进 Flink

把以下 jar 放入 `${FLINK_HOME}/lib`（Union Read 在 Flink 侧合并 Fluss + Paimon 需要）：

- `fluss-flink-1.20-*.jar`
- `fluss-lake-paimon-*.jar`
- `paimon-bundle-1.3.*.jar`

---

## 4. 集群配置（server.yaml）

在 `conf/server.yaml` 里启用 Paimon 作为 lakehouse 存储（本地 filesystem 仓库即可）：

```yaml
# 集群级开启 lakehouse
datalake.enabled: true
datalake.format: paimon

# Paimon 文件系统 catalog（本地基准用 filesystem 即可）
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon-warehouse

# Fluss 远端数据目录（tiering 提交时用同一路径）
remote.data.dir: /tmp/fluss-remote-data
```

> Tiering 作业提交时要带上同样的 `--datalake.*` 与 `--fluss.remote.data.dir`（见 §6），
> 二者必须指向**同一个 warehouse 和 remote.data.dir**。

启动集群：

```bash
# 1) 启动 ZooKeeper（独立进程）
# 2) 启动 CoordinatorServer
./bin/coordinator-server.sh start
# 3) 启动 TabletServer（基准建议 1~3 个）
./bin/tablet-server.sh start
```

---

## 5. 建表

### 5.0 先创建并切换 Fluss Catalog

建表前要在 Flink SQL 里创建 Fluss catalog 并切换进去（`bootstrap.servers` 填你的 CoordinatorServer 地址）：

```sql
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG fluss_catalog;
-- 默认 database 为 `fluss`；如需隔离可另建一个再切入：
-- CREATE DATABASE IF NOT EXISTS fluss_benchmark;
-- USE fluss_benchmark;
```

之后所有 DDL / 读写 / `$lake` 查询都在这个 catalog 下进行。

### 5.1 建两张对照表

两张表 schema 完全相同，**唯一区别是 `T_dv` 多了
`'table.deletion-vectors.enabled' = 'true'`**。

完整 DDL 见 [`benchmark.sql`](benchmark.sql) 的 SECTION 0。核心片段：

```sql
-- 非 DV（MOR）基线表
CREATE TABLE benchmark_orders_baseline (
    order_id BIGINT,
    user_id  BIGINT,
    amount   DECIMAL(10, 2),
    status   INT,
    version  BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s',
    'table.datalake.auto-compaction' = 'true',
    'bucket.num' = '1'
);

-- DV（merge-free）表：仅多一行 DV 开关
CREATE TABLE benchmark_orders_dv (
    order_id BIGINT,
    user_id  BIGINT,
    amount   DECIMAL(10, 2),
    status   INT,
    version  BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s',
    'table.datalake.auto-compaction' = 'true',
    'table.deletion-vectors.enabled' = 'true',
    'bucket.num' = '1'
);
```

约束（来自 `ConfigOptions.TABLE_DELETION_VECTORS_ENABLED`）：DV 必须在建表时指定、不可后改，
要求主键表 + `table.datalake.enabled=true` + FULL changelog image（默认）。

---

## 6. 启动 Tiering Service

Tiering 作业把 Fluss 数据持续同步到 Paimon，入口类
`org.apache.fluss.flink.tiering.FlussLakeTieringEntrypoint`。把
`fluss-flink-tiering-*.jar` 放到 `${FLINK_HOME}/opt/` 下，然后提交：

```bash
./bin/flink run \
  -Dpipeline.name="Fluss Tiering Service" \
  opt/fluss-flink-tiering-1.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers localhost:9123 \
  --datalake.format paimon \
  --datalake.paimon.metastore filesystem \
  --datalake.paimon.warehouse /tmp/paimon-warehouse \
  --fluss.remote.data.dir /tmp/fluss-remote-data
```

参数必须与集群 `server.yaml` 一致：`--datalake.paimon.warehouse` 指向同一个 Paimon 仓库，
`--fluss.remote.data.dir` 指向集群的 `remote.data.dir`（§4）。保持该作业一直运行，两张表共用同一个 tiering 作业。

---

## 7. 数据准备流程（迭代建表）

核心流程：**多轮** `写一批 → 等同步结束 → append & compaction`，一轮轮把 lake 搭起来、
并把 DV readable snapshot 逐轮向前推进（每轮 compaction 触发一次 readable Switch、构建/扩展 DV 索引）。
最后再[停掉 tiering 写一批做残留 log](#8-停止-tiering-制造残留-log)。两张表每一步都写**完全相同**的数据。

```
┌─ 重复 K 轮（medium K=3）─────────────────────────────┐
│  ① 写一批数据（向两表各 INSERT 第 i 轮，全量过一遍 N 个 key）   │
│  ② 等待同步结束（轮询 $lake，直到这批已 tiering 到 Paimon）     │
│  ③ append & compaction（tiering commit 触发 compaction，       │
│     出现新的 compact 快照 → DV readable snapshot 前移）         │
└──────────────────────────────────────────────────────┘
        ↓ K 轮后
   §8 停止 tiering service → 再写一批（停留在残留 log）
        ↓
   §9 union read 聚合测量（先过 §9.4 校验门）
```

工作负载参数：

| 参数 | 含义 | smoke | **medium（文档默认）** | large |
|---|---|---|---|---|
| `N` | 不同主键数（每轮全量过一遍）| 200,000 | 5,000,000 | 10,000,000 |
| `K` | 轮数（= 每 key 更新次数 `U`）| 3 | 3 | 10 |
| 每轮行数 | `= N` | 200,000 | 5,000,000 | 10,000,000 |
| §8 残留批 | 停 tiering 后追加的新 key 数 | 1,000,000 | 5,000,000 | 50,000,000+ |

> 文档默认 **medium**（lake ~200MB、残留 500 万新 key），在 24G 单机上能稳定跑完且有可见差距。
> 只想快速验证流程跑通用 **smoke**（lake ~8MB、扫描 <1s，测不出差距）；要更大差距用 **large** 或[加宽行](#12-放大--复现差距的旋钮)。
> 改规模时同步替换 `N`、轮数 `K` 和 §8 残留批大小。

**① 每轮写一批** — [`benchmark.sql`](benchmark.sql) 的 SECTION 1 已把 **3 轮全部展开**（round 1/2/3 对应 `version = 1/2/3`，每轮向两表各写一遍 N 个 key）。
**按块执行**：跑完一轮的两条 INSERT 后，先做下面 ②③ 的轮询确认本轮已同步+compaction，再跑下一轮，不要一次性全发。每轮单条形如：

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';   -- 保证每个 key 写入顺序确定（仅影响落点，不影响延迟对比）

CREATE TEMPORARY TABLE gen_r (seq BIGINT) WITH (   -- 整个 session 建一次即可
    'connector'='datagen','fields.seq.kind'='sequence',
    'fields.seq.start'='0','fields.seq.end'='4999999',   -- = N-1（medium N=500万）
    'number-of-rows'='5000000',                          -- = N，让 datagen 成为有界源（batch 必需）
    'rows-per-second'='10000000');                       -- 抬掉默认 1万/s 限速，否则巨慢

-- ROUND i：对 N 个 key 各更新一次，version = 轮号（两表写相同数据）
INSERT INTO benchmark_orders_baseline
SELECT seq AS order_id, MOD(seq,100000) AS user_id,
       CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)) AS amount,
       CAST(MOD(seq,5) AS INT) AS status,
       CAST(1 AS BIGINT) AS version      -- round 1=1 / round 2=2 / round 3=3
FROM gen_r;
-- 同一轮对 benchmark_orders_dv 执行完全相同的 INSERT（version 一致）
```

**② 等待同步结束 + ③ append & compaction**（每轮 INSERT 后轮询，直到这一轮已落 Paimon 且产生了新的 compact 快照）：

```sql
SET 'execution.runtime-mode' = 'batch';
-- $lake 行数应稳定到 N；快照表里应出现新的一行（commit_kind 含 COMPACT）
SELECT COUNT(*) FROM benchmark_orders_dv$lake;
SELECT snapshot_id, commit_kind, total_record_count
FROM benchmark_orders_dv$lake$snapshots ORDER BY snapshot_id DESC LIMIT 5;
```

- compaction 依赖建表时的 `'table.datalake.auto-compaction' = 'true'`：tiering 每次 commit 会触发 compaction，
  从而产生 compact 快照、推进 DV readable snapshot。等到 `$lake` 行数稳定（≈ N）且出现新的 compact 快照，本轮即完成。
- 然后回到 ①，version 改成下一轮号，重复至 K 轮。

K 轮后，lake 里是 N 个 key、经过 K 次 compaction 的稳定状态；DV readable snapshot 已推进到接近 K 轮的末尾。

---

## 8. 停止 tiering，制造残留 log

这是放大差距的关键一步：停掉 tiering 后再写一批，这批全部停留在 **Fluss 残留 log**（lake 冻结）。
union read 时——非 DV 要把整段残留 log 物化进 `TreeMap` 并按主键 sort-merge，DV 只是流式 + 位图过滤。

1. **停止 tiering 作业**：`flink cancel <jobId>`（或在 Web UI 取消）。lake 侧从此冻结。
2. **向两表各写最后一批**（相同数据），见 [`benchmark.sql`](benchmark.sql) 的 SECTION 2：

   ```sql
   -- medium：追加 500 万个新 key（order_id = 100000000 + seq，与 lake 的 [0,N) 不重叠 → 纯插入）。
   SET 'execution.runtime-mode' = 'batch';
   SET 'parallelism.default' = '1';
   CREATE TEMPORARY TABLE gen2 (seq BIGINT) WITH (
       'connector'='datagen','fields.seq.kind'='sequence',
       'fields.seq.start'='0','fields.seq.end'='4999999',
       'number-of-rows'='5000000',     -- 让 datagen 成为有界源（batch 必需）
       'rows-per-second'='10000000');  -- 抬掉默认 1万/s 限速
   INSERT INTO benchmark_orders_baseline
   SELECT 100000000 + seq, MOD(seq,100000),
          CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)), CAST(MOD(seq,5) AS INT),
          CAST(1 AS BIGINT)
   FROM gen2;
   -- 对 benchmark_orders_dv 执行完全相同的 INSERT
   ```

3. 进入 §9：**先过 §9.4 结果校验门**（两表聚合值必须一致），再测延迟。

> ⚠️ 这里用**大范围新 key**（纯插入）就是为了让非 DV 的内存 `TreeMap` 真的装下 1000 万条；
> 若改成 upsert 已存在 key（`MOD(seq,N)`），`TreeMap` 会塌缩到 N、撑不大（见 §12）。

---

## 9. 执行基准查询并测量延迟

### 9.1 切到批模式做 Union Read

Union Read 在**批模式**下会自动合并 Fluss + Paimon（无需任何 hint，直接查表名即可）；
聚合查询会终止，便于测 wall-clock。查询见 [`benchmark.sql`](benchmark.sql) 的 SECTION 4。

> ⚠️ **读取要调大并发**：§7/§8 的写入把 `parallelism.default` 设成了 1，SQL 客户端会沿用到读查询。
> 测量前**务必重设**为 TaskManager 可用 slot 数（如 8）。单 bucket 下，DV 的多个数据文件 split 会被打散到多个 reader 并发读，
> 调大并发能加速 DV；非 DV 每 bucket 只有一个混合 split、始终单 reader，不受并发影响。两表用相同并发才可比：

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '8';     -- 读并发：取 TaskManager slot 数

-- Q1
SELECT COUNT(*) FROM benchmark_orders_baseline;
-- Q2（强制读取列值）
SELECT COUNT(*) AS cnt, SUM(amount) AS sum_amt, SUM(version) AS sum_ver
FROM benchmark_orders_baseline;
```

对 `benchmark_orders_dv` 执行相同查询。两表用**相同并发**才可比。

### 9.2 测量方法

每条查询：**先 1~2 轮 warmup（不计入），再测 5 轮取中位数**。记录耗时的方式：

- **Flink Web UI**：读每个 batch job 的 *Duration*（Job Runtime）——最准；
- 或 **SQL Client `-f`**：把单条查询写进 `q.sql`，用 `time ${FLINK_HOME}/bin/sql-client.sh -f q.sql` 计时（含客户端启动开销，注意扣除）。

### 9.3 （可选）隔离机制：Paimon-only 对照

为验证差异主要来自 Paimon 层 MOR vs merge-free，再测 `$lake` 后缀的纯 Paimon 读
（见 [`benchmark.sql`](benchmark.sql) 的 SECTION 5a）：

```sql
SELECT COUNT(*), SUM(amount) FROM benchmark_orders_baseline$lake;
SELECT COUNT(*), SUM(amount) FROM benchmark_orders_dv$lake;
```

`$lake` 对照会给出最干净、最大的差距（不含 Fluss log 合并部分）。

### 9.4 结果校验门（必做，先于比较延迟）

DV 与非 DV 走两条不同读路径，**只有当二者返回结果完全一致时，延迟对比才成立**。
在记录任何延迟前，先跑 Q2 校验两表聚合值逐列相等：

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS cnt, SUM(amount) AS sum_amt, SUM(version) AS sum_ver
FROM benchmark_orders_baseline;   -- 记下 (cnt, sum_amt, sum_ver)

SELECT COUNT(*) AS cnt, SUM(amount) AS sum_amt, SUM(version) AS sum_ver
FROM benchmark_orders_dv;         -- 必须与上面逐列相等
```

若两行不相等（行数或 SUM 不同），说明出现了重复/丢行，**此时延迟不可比**，应排查后再测。

---

## 放大说明：怎么把 §8 的残留 log 差距放到最大

§8「停 tiering + 写一批」是主流程，也是让差距最明显的一步：停掉 tiering 后再写，残留 log 随写入量变大，
非 DV 要把整段残留 log 物化排序归并（O(N log N)、O(N) 内存），DV 只是流式过滤，两者结果一致。要放到最大：
- 追加批要够大，且覆盖**大范围不同 key**——`MOD(seq, N)` 会让 `TreeMap` 塌缩到 N，改用 `order_id = 100000000 + seq`（纯插入）让 `TreeMap` 真的装下几千万条；
- 纯插入还天然无跨层去重歧义；upsert 已存在 key 更贴近真实但务必先过 §9.4 校验门；
- 追加批大到一定程度，非 DV 可能因为 `TreeMap` 撑爆堆而变慢甚至 OOM（把 TaskManager 堆调小可放大这一点），DV 照常流式跑完。

---

## 10. 结果记录模板

| 查询 | 模式 | r1 | r2 | r3 | r4 | r5 | 中位数(ms) | DV 加速比 |
|---|---|---|---|---|---|---|---|---|
| Q1 COUNT(*) Union | baseline | | | | | | | — |
| Q1 COUNT(*) Union | dv | | | | | | | `baseline/dv` |
| Q2 SUM Union | baseline | | | | | | | — |
| Q2 SUM Union | dv | | | | | | | `baseline/dv` |
| Paimon-only COUNT | baseline | | | | | | | — |
| Paimon-only COUNT | dv | | | | | | | `baseline/dv` |
| **§8 残留 log** Q2 SUM Union | baseline | | | | | | | — |
| **§8 残留 log** Q2 SUM Union | dv | | | | | | | `baseline/dv` |

先在表头记录「结果校验门是否通过」（两表 Q2 聚合值逐列相等：是/否）。
同时记录环境：TabletServer 数、`N`、`U`、追加批大小、Paimon 快照数、残留 log 记录数。

---

## 11. 预期结论

在相同配置与负载下（且已通过 §9.4 结果校验门）：

- **DV 模式 Union Read 的聚合延迟显著低于非 DV 模式**：残留 log 从「物化 + sort-merge」变成「流式 + 位图过滤」，Paimon 层从 MOR 归并变成 merge-free 顺序扫描；
- **§8（停 tiering + 大批写入）下差距最大**：非 DV 要对整段残留 log 做 O(N log N) 的 sort-merge，DV 只做 O(N) 流式过滤，差距随追加批大小线性拉开；
- **差距也随 `U`（每 key 更新次数）增大而扩大**；
- **`$lake` Paimon-only 对照**给出 Paimon 层机制差距，用于佐证提升来源。

---

## 12. 放大 / 复现差距的旋钮

- **停 tiering 后的追加批大小（§8，最有效）**：直接决定残留 log 记录数，即非 DV sort-merge 的规模；
- ↑ `U`（每 key 更新次数）：放大非 DV 的 MOR / 归并成本；
- ↑ `N` 与总行数：放大绝对延迟，使差距更易测量；
- ↓ `table.datalake.freshness`：更频繁地产生小 commit / sorted run，更贴近真实流式状态；
- 关掉 `table.datalake.auto-compaction`（两表都关）：保留更多未合并 run，进一步放大非 DV 归并成本（注意：DV 索引依赖 compaction，若关闭需手动触发一次 compaction 让 DV 表建好索引）。

> 并发：本基准用**单 bucket**。`FlinkSourceEnumerator.getSplitOwner` 已改为把 `LakeSnapshotSplit` 按 splitId 打散到不同 reader，
> 所以即使只有一个 bucket，DV 的多个数据文件 split 也能分到多个 reader 并发读（非 DV 是每 bucket 一个混合 split，仍只能一个 reader）。
> 因此**读取时调大 `parallelism.default`**（§9.1）能加速 DV，非 DV 不受益——这本身就是一条 DV 优势。

---

## 13. 文件清单

| 文件 | 作用 |
|---|---|
| `README.md` | 本测试流程说明 |
| `benchmark.sql` | 单文件 runbook：建 catalog + 建表（prelude/SECTION 0）→ 迭代写入/同步/compaction（SECTION 1）→ 停 tiering + 残留批（SECTION 2）→ 校验门（SECTION 3）→ union read 测量（SECTION 4）→ 机制对照 5a/并发扫描 5b |
