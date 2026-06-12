# Paimon DV 手动端到端测试计划

目标：验证 DV 表数据正确性——不丢、不重复。在本地启动 Fluss + Flink 集群，通过 Flink SQL Client 写入/查询。

---

## 一、环境准备

### 1.1 前置条件

- Java 11
- Maven 3.8.6+
- Flink 1.20（从 https://flink.apache.org/downloads/ 下载）
- Paimon 版本 1.3.1（与项目 pom.xml 一致）

### 1.2 编译 Fluss

```bash
cd /Users/yuxia/Projects/fluss/fluss
./mvnw clean install -DskipTests -T 1C
```

关键产物：

| 产物 | 路径 |
|------|------|
| Fluss 发行包 | `fluss-dist/target/fluss-1.0-SNAPSHOT-bin/fluss-1.0-SNAPSHOT/` |
| Flink 连接器 | `fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-1.0-SNAPSHOT.jar` |
| Tiering JAR | `fluss-flink/fluss-flink-tiering/target/fluss-flink-tiering-1.0-SNAPSHOT.jar` |
| Lake-Paimon JAR | `fluss-lake/fluss-lake-paimon/target/fluss-lake-paimon-1.0-SNAPSHOT.jar` |

### 1.3 配置 Fluss

编辑 `fluss-dist/target/fluss-1.0-SNAPSHOT-bin/fluss-1.0-SNAPSHOT/conf/server.yaml`，末尾追加：

```yaml
datalake.enabled: true
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon-warehouse
```

### 1.4 启动 Fluss

```bash
cd fluss-dist/target/fluss-1.0-SNAPSHOT-bin/fluss-1.0-SNAPSHOT
./bin/local-cluster.sh start
```

验证：`jps` 能看到 ZooKeeper、CoordinatorServer、TabletServer 三个进程。

### 1.5 配置并启动 Flink

```bash
# 复制 JAR 到 Flink lib
cp fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-1.0-SNAPSHOT.jar $FLINK_HOME/lib/
cp fluss-lake/fluss-lake-paimon/target/fluss-lake-paimon-1.0-SNAPSHOT.jar $FLINK_HOME/lib/
curl -fL -o $FLINK_HOME/lib/paimon-bundle-1.3.1.jar \
  https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/1.3.1/paimon-bundle-1.3.1.jar

# DV 必需：RoaringBitmap（fluss-flink-common 依赖但未被 shade 进 connector JAR）
curl -fL -o $FLINK_HOME/lib/RoaringBitmap-1.3.0.jar \
  https://repo1.maven.org/maven2/org/roaringbitmap/RoaringBitmap/1.3.0/RoaringBitmap-1.3.0.jar

# 启动 Flink
$FLINK_HOME/bin/start-cluster.sh
```

### 1.6 启动 Tiering Service

```bash
$FLINK_HOME/bin/flink run \
  -Dpipeline.name="Fluss Tiering Service" \
  opt/fluss-flink-tiering-1.0-SNAPSHOT.jar \
  --fluss.bootstrap.servers localhost:9123 \
  --datalake.format paimon \
  --datalake.paimon.metastore filesystem \
  --datalake.paimon.warehouse /tmp/paimon-warehouse \
  --fluss.remote.data.dir /tmp/fluss-remote-data
```

> **DV 必需**：`--remote.data.dir` 必须配置，且与 `server.yaml` 中的 `remote.data.dir` 一致。
> DV 的 row position scan 需要用它来上传索引文件（`TieringCommitOperator` → `FlussConfigUtils.getDefaultRemoteDataDir()`）。
> 缺少此配置会导致 tiering service 报错 `IllegalConfigurationException: Either remote.data.dir or remote.data.dirs must be configured`，
> DV 数据无法生成，union read 时 TabletServer 的 `readableSnapshotId` 停留在 -1，最终抛出 `StaleSnapshotException`。

验证：Flink Dashboard (http://localhost:8081) 看到 "Fluss Tiering Service" 状态为 RUNNING。

### 1.7 进入 SQL Client

```bash
$FLINK_HOME/bin/sql-client.sh
```

```sql
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG fluss_catalog;
USE fluss;
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
```

环境就绪。

---

## 二、测试用例

> 所有用例的核心判定标准：**行数正确（不丢不重复）、每个 key 的值为最新写入版本**。
>
> 每步 INSERT 完成后，需等待 tiering + compaction。建议等 15-20 秒后再查询。如果结果不对，先多等一会再查，readable snapshot 需要 compaction 完成后才能生成。
>
> 可通过 `SELECT snapshot_id FROM <table>$lake$snapshots;` 确认 tiering 进度。

### 用例 1：基本 Insert + Update

```sql
CREATE TABLE dv_t1 (
  c1 INT, c2 STRING, c3 STRING,
  PRIMARY KEY (c1) NOT ENFORCED
) WITH (
  'bucket.num' = '1',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '500ms',
  'table.datalake.auto-compaction' = 'true',
  'table.deletion-vectors.enabled' = 'true',
  'paimon.deletion-vectors.enabled' = 'true',
  'paimon.num-sorted-run.compaction-trigger' = '2'
);
```

**写入初始数据：**

```sql
INSERT INTO dv_t1 VALUES (0,'a0','b0'),(1,'a1','b1'),(2,'a2','b2'),(3,'a3','b3'),(4,'a4','b4');
```

等待 15 秒。

**更新 key 0, 1, 3 + 新增 key 5：**

```sql
INSERT INTO dv_t1 VALUES (0,'a0_v2','b0_v2'),(1,'a1_v2','b1_v2'),(3,'a3_v2','b3_v2'),(5,'a5','b5');
```

等待 15 秒。

**验证：**

```sql
SELECT * FROM dv_t1 ORDER BY c1;
```

预期 6 行：

| c1 | c2 | c3 |
|----|----|----|
| 0 | a0_v2 | b0_v2 |
| 1 | a1_v2 | b1_v2 |
| 2 | a2 | b2 |
| 3 | a3_v2 | b3_v2 |
| 4 | a4 | b4 |
| 5 | a5 | b5 |

```sql
SELECT count(*) AS cnt FROM dv_t1;
-- 预期: 6

SELECT c1, count(*) AS n FROM dv_t1 GROUP BY c1 HAVING count(*) > 1;
-- 预期: 空（无重复）
```

---

### 用例 2：同一 Key 多轮更新

验证同一个 key 经过多轮覆盖后，最终只保留最后一次写入的值。

```sql
CREATE TABLE dv_t2 (
  id INT, val STRING, round_tag STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket.num' = '1',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '500ms',
  'table.datalake.auto-compaction' = 'true',
  'table.deletion-vectors.enabled' = 'true',
  'paimon.deletion-vectors.enabled' = 'true',
  'paimon.num-sorted-run.compaction-trigger' = '2'
);
```

**Round 1：**

```sql
INSERT INTO dv_t2 VALUES (1,'v1','r1'),(2,'v2','r1'),(3,'v3','r1');
```

等 15 秒。验证：

```sql
SELECT * FROM dv_t2 ORDER BY id;
-- 预期 3 行，val 都是 r1 版本
```

**Round 2：更新 key 1, 2**

```sql
INSERT INTO dv_t2 VALUES (1,'v1_r2','r2'),(2,'v2_r2','r2');
```

等 15 秒。验证：

```sql
SELECT * FROM dv_t2 ORDER BY id;
-- 预期: (1,v1_r2,r2), (2,v2_r2,r2), (3,v3,r1)
```

**Round 3：再更新 key 1 + 新增 key 4, 5**

```sql
INSERT INTO dv_t2 VALUES (1,'v1_r3','r3'),(4,'v4','r3'),(5,'v5','r3');
```

等 15 秒。验证：

```sql
SELECT * FROM dv_t2 ORDER BY id;
-- 预期: (1,v1_r3,r3), (2,v2_r2,r2), (3,v3,r1), (4,v4,r3), (5,v5,r3)
```

**Round 4：第四次更新 key 1**

```sql
INSERT INTO dv_t2 VALUES (1,'v1_r4','r4');
```

等 15 秒。

**最终验证：**

```sql
SELECT * FROM dv_t2 ORDER BY id;
```

预期 5 行：

| id | val | round_tag |
|----|-----|-----------|
| 1 | v1_r4 | r4 |
| 2 | v2_r2 | r2 |
| 3 | v3 | r1 |
| 4 | v4 | r3 |
| 5 | v5 | r3 |

```sql
SELECT count(*) AS cnt FROM dv_t2;
-- 预期: 5

SELECT id, count(*) AS n FROM dv_t2 GROUP BY id HAVING count(*) > 1;
-- 预期: 空
```

---

### 用例 3：分区表

验证分区表场景下跨分区的数据正确性，不同分区之间的 DV 互不干扰。

```sql
CREATE TABLE dv_t3 (
  id INT, name STRING, region STRING,
  PRIMARY KEY (id, region) NOT ENFORCED
) PARTITIONED BY (region) WITH (
  'bucket.num' = '1',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '500ms',
  'table.datalake.auto-compaction' = 'true',
  'table.deletion-vectors.enabled' = 'true',
  'paimon.deletion-vectors.enabled' = 'true',
  'paimon.num-sorted-run.compaction-trigger' = '2'
);
```

**写入两个分区：**

```sql
INSERT INTO dv_t3 VALUES
  (1,'alice','east'),(2,'bob','east'),(3,'carol','east'),
  (1,'dave','west'),(2,'eve','west');
```

等 20 秒（分区表需要更多时间）。

**只更新 east 分区：**

```sql
INSERT INTO dv_t3 VALUES (1,'alice_v2','east'),(3,'carol_v2','east');
```

等 20 秒。

**验证：**

```sql
SELECT * FROM dv_t3 ORDER BY region, id;
```

预期 5 行：

| id | name | region |
|----|------|--------|
| 1 | alice_v2 | east |
| 2 | bob | east |
| 3 | carol_v2 | east |
| 1 | dave | west |
| 2 | eve | west |

```sql
SELECT count(*) AS cnt FROM dv_t3;
-- 预期: 5

SELECT id, region, count(*) AS n FROM dv_t3 GROUP BY id, region HAVING count(*) > 1;
-- 预期: 空
```

关键点：west 分区的数据不受 east 分区更新的影响，两个分区的 id=1 是不同的 key（因为 PK 包含 region）。

---

### 用例 4：多 Bucket

用例 1-3 都是单 bucket，这里验证多 bucket 下数据按 key hash 分散后的正确性。

```sql
CREATE TABLE dv_t4 (
  id INT, val STRING, extra STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket.num' = '3',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '500ms',
  'table.datalake.auto-compaction' = 'true',
  'table.deletion-vectors.enabled' = 'true',
  'paimon.deletion-vectors.enabled' = 'true',
  'paimon.num-sorted-run.compaction-trigger' = '2'
);
```

**写入 20 行：**

```sql
INSERT INTO dv_t4 VALUES
  (0,'v0','e0'),(1,'v1','e1'),(2,'v2','e2'),(3,'v3','e3'),(4,'v4','e4'),
  (5,'v5','e5'),(6,'v6','e6'),(7,'v7','e7'),(8,'v8','e8'),(9,'v9','e9'),
  (10,'v10','e10'),(11,'v11','e11'),(12,'v12','e12'),(13,'v13','e13'),(14,'v14','e14'),
  (15,'v15','e15'),(16,'v16','e16'),(17,'v17','e17'),(18,'v18','e18'),(19,'v19','e19');
```

等 20 秒。

**更新偶数 key：**

```sql
INSERT INTO dv_t4 VALUES
  (0,'u0','x0'),(2,'u2','x2'),(4,'u4','x4'),(6,'u6','x6'),(8,'u8','x8'),
  (10,'u10','x10'),(12,'u12','x12'),(14,'u14','x14'),(16,'u16','x16'),(18,'u18','x18');
```

等 20 秒。

**验证：**

```sql
SELECT count(*) AS cnt FROM dv_t4;
-- 预期: 20

SELECT id, count(*) AS n FROM dv_t4 GROUP BY id HAVING count(*) > 1;
-- 预期: 空

-- 抽查偶数 key 已更新
SELECT * FROM dv_t4 WHERE id = 0;
-- 预期: (0, u0, x0)

SELECT * FROM dv_t4 WHERE id = 4;
-- 预期: (4, u4, x4)

-- 抽查奇数 key 未变
SELECT * FROM dv_t4 WHERE id = 1;
-- 预期: (1, v1, e1)

SELECT * FROM dv_t4 WHERE id = 7;
-- 预期: (7, v7, e7)
```

---

### 用例 5：万级数据量 + 多轮写入更新

验证大数据量下的不丢不重复。分多批写入，每批之间有 tiering + compaction，模拟持续写入场景。

```sql
CREATE TABLE dv_t5 (
  id INT, val STRING, batch_tag STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket.num' = '3',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '500ms',
  'table.datalake.auto-compaction' = 'true',
  'table.deletion-vectors.enabled' = 'true',
  'paimon.deletion-vectors.enabled' = 'true',
  'paimon.num-sorted-run.compaction-trigger' = '2'
);
```

先在 default catalog 中准备好 datagen 源表（后续复用）：

```sql
USE CATALOG default_catalog;

CREATE TEMPORARY TABLE seq_10k (id INT) WITH (
  'connector'='datagen','number-of-rows'='10000',
  'fields.id.kind'='sequence','fields.id.start'='1','fields.id.end'='10000'
);

CREATE TEMPORARY TABLE seq_5k (id INT) WITH (
  'connector'='datagen','number-of-rows'='5000',
  'fields.id.kind'='sequence','fields.id.start'='1','fields.id.end'='5000'
);

CREATE TEMPORARY TABLE seq_3k (id INT) WITH (
  'connector'='datagen','number-of-rows'='3000',
  'fields.id.kind'='sequence','fields.id.start'='1','fields.id.end'='3000'
);

USE CATALOG fluss_catalog;
USE fluss;
```

**Batch 1：写入 10000 行（id 1~10000）**

```sql
INSERT INTO dv_t5
  SELECT id, CONCAT('val_',CAST(id AS STRING)), 'b1'
  FROM default_catalog.default_database.seq_10k;
```

等 30 秒。快速检查：

```sql
SELECT count(*) AS cnt FROM dv_t5;
-- 预期: 10000
```

**Batch 2：更新 5000 行（偶数 id：2,4,...,10000）**

```sql
INSERT INTO dv_t5
  SELECT id*2, CONCAT('upd_',CAST(id*2 AS STRING)), 'b2'
  FROM default_catalog.default_database.seq_5k;
```

等 30 秒。

**Batch 3：再更新 3000 行（id 1~3000 全覆盖，含奇数和偶数）**

```sql
INSERT INTO dv_t5
  SELECT id, CONCAT('latest_',CAST(id AS STRING)), 'b3'
  FROM default_catalog.default_database.seq_3k;
```

等 30 秒。

**最终验证：**

```sql
-- 总行数：始终 10000，不多不少
SELECT count(*) AS cnt FROM dv_t5;
-- 预期: 10000

-- 无重复 key
SELECT id, count(*) AS n FROM dv_t5 GROUP BY id HAVING count(*) > 1;
-- 预期: 空

-- 验证各 batch 的行数分布：
-- id 1~3000：全部被 batch3 覆盖 → batch_tag = 'b3'
-- id 3001~10000 中的偶数（3002,3004,...,10000）：被 batch2 覆盖 → batch_tag = 'b2'
-- id 3001~10000 中的奇数（3001,3003,...,9999）：保持 batch1 → batch_tag = 'b1'

SELECT batch_tag, count(*) AS cnt FROM dv_t5 GROUP BY batch_tag ORDER BY batch_tag;
-- 预期:
-- b1: 3500  (id 3001,3003,...,9999 → 3500 个奇数)
-- b2: 3500  (id 3002,3004,...,10000 → 3500 个偶数)
-- b3: 3000  (id 1~3000)

-- 抽查 batch3 覆盖区域
SELECT * FROM dv_t5 WHERE id = 1;
-- 预期: (1, latest_1, b3)

SELECT * FROM dv_t5 WHERE id = 2;
-- 预期: (2, latest_2, b3)   ← 虽然 batch2 也更新过 id=2，但 batch3 更晚

SELECT * FROM dv_t5 WHERE id = 3000;
-- 预期: (3000, latest_3000, b3)

-- 抽查 batch2 覆盖区域（偶数，id > 3000）
SELECT * FROM dv_t5 WHERE id = 3002;
-- 预期: (3002, upd_3002, b2)

SELECT * FROM dv_t5 WHERE id = 10000;
-- 预期: (10000, upd_10000, b2)

-- 抽查 batch1 保留区域（奇数，id > 3000）
SELECT * FROM dv_t5 WHERE id = 3001;
-- 预期: (3001, val_3001, b1)

SELECT * FROM dv_t5 WHERE id = 9999;
-- 预期: (9999, val_9999, b1)
```

---

### 用例 6：DELETE 删除已有 Key

验证对已 tier 到 Paimon 的 key 执行 DELETE 后，union read 不再返回被删除的行。

```sql
CREATE TABLE dv_t6 (
  id INT, val STRING, memo STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket.num' = '1',
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '500ms',
  'table.datalake.auto-compaction' = 'true',
  'table.deletion-vectors.enabled' = 'true',
  'paimon.deletion-vectors.enabled' = 'true',
  'paimon.num-sorted-run.compaction-trigger' = '2'
);
```

**写入初始数据：**

```sql
INSERT INTO dv_t6 VALUES (1,'a','m1'),(2,'b','m2'),(3,'c','m3'),(4,'d','m4'),(5,'e','m5');
```

等 15 秒。确认 5 行：

```sql
SELECT count(*) AS cnt FROM dv_t6;
-- 预期: 5
```

**删除 key 2 和 key 4：**

```sql
DELETE FROM dv_t6 WHERE id = 2;
DELETE FROM dv_t6 WHERE id = 4;
```

等 15 秒。

**验证删除后的数据：**

```sql
SELECT * FROM dv_t6 ORDER BY id;
```

预期 3 行：

| id | val | memo |
|----|-----|------|
| 1 | a | m1 |
| 3 | c | m3 |
| 5 | e | m5 |

```sql
SELECT count(*) AS cnt FROM dv_t6;
-- 预期: 3

SELECT id, count(*) AS n FROM dv_t6 GROUP BY id HAVING count(*) > 1;
-- 预期: 空
```

**再写入新数据 + 复用已删除的 key：**

```sql
INSERT INTO dv_t6 VALUES (2,'b_new','m2_new'),(6,'f','m6');
```

等 15 秒。

**验证复用 key 后的数据：**

```sql
SELECT * FROM dv_t6 ORDER BY id;
```

预期 5 行：

| id | val | memo |
|----|-----|------|
| 1 | a | m1 |
| 2 | b_new | m2_new |
| 3 | c | m3 |
| 5 | e | m5 |
| 6 | f | m6 |

```sql
SELECT count(*) AS cnt FROM dv_t6;
-- 预期: 5

SELECT id, count(*) AS n FROM dv_t6 GROUP BY id HAVING count(*) > 1;
-- 预期: 空
```

关键点：
- key 4 被删除后没有重新写入，最终不存在
- key 2 被删除后又重新写入了新值，最终存在且为新值
- 被删除的 key 不会作为"幽灵行"残留在 union read 结果中

---

## 三、清理

```bash
# SQL Client 中
quit;

# 取消 tiering 作业
$FLINK_HOME/bin/flink cancel <job-id>

# 停 Flink
$FLINK_HOME/bin/stop-cluster.sh

# 停 Fluss
cd fluss-dist/target/fluss-1.0-SNAPSHOT-bin/fluss-1.0-SNAPSHOT
./bin/local-cluster.sh stop

# 清数据（可选）
rm -rf /tmp/fluss-data /tmp/fluss-remote-data /tmp/paimon-warehouse
```

---

## 四、故障排查

| 现象 | 排查 |
|------|------|
| `StaleSnapshotException: requested N, current -1` | Tiering service 启动时缺少 `--remote.data.dir`，导致 DV row position scan 失败。检查 Flink TaskManager 日志中是否有 `IllegalConfigurationException: Either remote.data.dir or remote.data.dirs must be configured`。修复：重启 tiering service 并加上 `--remote.data.dir /tmp/fluss-remote-data` |
| 查到重复行 | readable snapshot 可能未生成，等更久再查；用 `SELECT * FROM table$lake$snapshots` 确认有 COMPACT 类型的 snapshot |
| 行数少了（丢数据） | INSERT 作业是否成功完成；tiering service 是否 RUNNING；等更久让 tiering 追上 |
| 查到旧值 | readable snapshot 还未推进到最新，多等一会 |
| `ClassNotFoundException: org.roaringbitmap.longlong.Roaring64Bitmap` | `$FLINK_HOME/lib/` 缺少 `RoaringBitmap-1.3.0.jar`。DV 功能依赖此库但 Flink connector JAR 未 shade 进去，需单独添加 |
| 查询直接报错 | 检查 `$FLINK_HOME/lib/` 下 JAR 是否齐全（fluss connector + lake-paimon + paimon-bundle + RoaringBitmap） |
| 分区表结果为空 | 分区创建 + tiering 需要更多时间，等 30 秒再查 |

---

## 五、Self-Review

1. **覆盖维度**：6 个用例覆盖了数据正确性容易出问题的维度——基本 insert+update（用例 1）、同 key 多轮覆盖（用例 2）、分区表（用例 3）、多 bucket（用例 4）、万级数据量多轮写入（用例 5）、DELETE 删除（用例 6）。

2. **验证手段统一**：每个用例都用三条 SQL 做验证——`SELECT *` 检查值、`count(*)` 检查总行数、`GROUP BY HAVING count(*) > 1` 检查无重复。这三个加在一起就是"不丢不重复"的完整验证。

3. **等待时间**：DV 表的 readable snapshot 生成需要 tiering + compaction 两步都完成。freshness=500ms 只控制 tiering 频率，compaction 由 `num-sorted-run.compaction-trigger=2` 触发，需要两轮 tiering 才会触发第一次 compaction。所以建议每步等 15-20 秒。如果结果不对，第一反应是多等一会而不是怀疑数据有问题。
