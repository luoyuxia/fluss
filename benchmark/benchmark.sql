-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

-- ############################################################################
-- Union Read benchmark: DV vs non-DV, single runbook.  See README.md.
--
-- HOW TO RUN: execute this file BLOCK BY BLOCK in the Flink SQL client
-- (./bin/sql-client.sh). Two steps need a human pause and CANNOT be done in
-- SQL -- they are marked with:
--   >>> PAUSE  : wait for tiering to sync + compact (poll the queries shown)
--   >>> ACTION : do something outside SQL (cancel the tiering Flink job)
--
-- MEDIUM preset (this file's defaults): N = 5,000,000 keys, K = 3 rounds,
-- residual batch = 5,000,000 NEW-key inserts (large distinct-key range, so the
-- non-DV sort-merge / in-memory TreeMap actually grows with it).
-- For a quick flow check use smoke (N=200,000, residual 1,000,000); for a
-- bigger gap go large (N=10,000,000) and/or widen rows (README §7/§12).
-- ############################################################################

-- ==== prelude: create + use the Fluss catalog (edit bootstrap.servers) =======
-- Run this first; all tables / reads / $lake queries below live in this catalog.

CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG fluss_catalog;

-- default database is `fluss`; uncomment to isolate this benchmark in its own db:
-- CREATE DATABASE IF NOT EXISTS fluss_benchmark;
-- USE fluss_benchmark;


-- ############################################################################
-- SECTION 0 — create the two comparison tables
--   only difference: 'table.deletion-vectors.enabled' on the DV table
-- ############################################################################

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

CREATE TEMPORARY TABLE gen_r (seq BIGINT) WITH (
    'connector' = 'datagen',
    'fields.seq.kind'  = 'sequence',
    'fields.seq.start' = '0',
    'fields.seq.end'   = '4999999',     -- = N - 1 (medium N = 5,000,000)
    'number-of-rows'   = '5000000',     -- = N; makes the source BOUNDED for batch INSERT
    'rows-per-second'  = '10000000'     -- lift the default 10k/s throttle (else 5M rows ~8min)
);

-- >>> ACTION: start the tiering service now (README §6), from ${FLINK_HOME}:
--     ./bin/flink run \
--       -Dpipeline.name="Fluss Tiering Service" \
--       opt/fluss-flink-tiering-1.0-SNAPSHOT.jar \
--       --fluss.bootstrap.servers localhost:9123 \
--       --datalake.format paimon \
--       --datalake.paimon.metastore filesystem \
--       --datalake.paimon.warehouse /tmp/fluss-benchmark/paimon \
--       --fluss.remote.data.dir /tmp/fluss-remote-data
--     (warehouse + remote.data.dir must match server.yaml) and keep it running
--     through SECTION 1.


-- ############################################################################
-- SECTION 1 — iterative load: K=3 rounds of  write -> wait sync -> compaction
--   each round writes one full pass over N keys (version = round number).
-- ############################################################################

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

-- parallelism=1 keeps per-key write order deterministic (affects stored values
-- only, not latency). created once, reused by all rounds.
SET 'parallelism.default' = '1';


-- -------------------------- ROUND 1 (version = 1) --------------------------
INSERT INTO benchmark_orders_baseline
SELECT seq, MOD(seq,100000), CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)),
       CAST(MOD(seq,5) AS INT), CAST(1 AS BIGINT) FROM gen_r;
INSERT INTO benchmark_orders_dv
SELECT seq, MOD(seq,100000), CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)),
       CAST(MOD(seq,5) AS INT), CAST(1 AS BIGINT) FROM gen_r;

-- >>> PAUSE: re-run these until $lake count is stable (~N) AND a new snapshot
--           with commit_kind = COMPACT has appeared, then go to ROUND 2.
SELECT COUNT(*) FROM benchmark_orders_dv$lake;
SELECT snapshot_id, commit_kind, total_record_count
FROM benchmark_orders_dv$lake$snapshots ORDER BY snapshot_id DESC LIMIT 5;

-- -------------------------- ROUND 2 (version = 2) --------------------------
INSERT INTO benchmark_orders_baseline
SELECT seq, MOD(seq,100000), CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)),
       CAST(MOD(seq,5) AS INT), CAST(2 AS BIGINT) FROM gen_r;
INSERT INTO benchmark_orders_dv
SELECT seq, MOD(seq,100000), CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)),
       CAST(MOD(seq,5) AS INT), CAST(2 AS BIGINT) FROM gen_r;

-- >>> PAUSE: wait for sync + COMPACT snapshot (same poll), then go to ROUND 3.
SELECT COUNT(*) FROM benchmark_orders_dv$lake;
SELECT snapshot_id, commit_kind, total_record_count
FROM benchmark_orders_dv$lake$snapshots ORDER BY snapshot_id DESC LIMIT 5;

-- -------------------------- ROUND 3 (version = 3) --------------------------
INSERT INTO benchmark_orders_baseline
SELECT seq, MOD(seq,100000), CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)),
       CAST(MOD(seq,5) AS INT), CAST(3 AS BIGINT) FROM gen_r;
INSERT INTO benchmark_orders_dv
SELECT seq, MOD(seq,100000), CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)),
       CAST(MOD(seq,5) AS INT), CAST(3 AS BIGINT) FROM gen_r;

-- >>> PAUSE: wait for sync + COMPACT snapshot (same poll).
SELECT COUNT(*) FROM benchmark_orders_dv$lake;
SELECT snapshot_id, commit_kind, total_record_count
FROM benchmark_orders_dv$lake$snapshots ORDER BY snapshot_id DESC LIMIT 5;

-- For more rounds (large K=10): copy a ROUND block and bump the version literal.


-- ############################################################################
-- SECTION 2 — stop tiering, then write the residual batch (the amplifier)
--   everything written here stays in the Fluss residual log (lake is frozen).
--   non-DV must sort-merge the whole residual log; DV just streams + bitmap.
-- ############################################################################

-- >>> ACTION: STOP the tiering job now:  flink cancel <jobId>   (or via Web UI)
--            lake is frozen from here on.

SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';

CREATE TEMPORARY TABLE gen2 (seq BIGINT) WITH (
    'connector' = 'datagen',
    'fields.seq.kind'  = 'sequence',
    'fields.seq.start' = '0',
    'fields.seq.end'   = '4999999',     -- residual batch: 5,000,000 rows (medium)
    'number-of-rows'   = '5000000',     -- makes the source BOUNDED for batch INSERT
    'rows-per-second'  = '10000000'     -- lift the default 10k/s throttle (else 5M rows ~8min)
);

-- 5,000,000 NEW keys (order_id = 100000000 + seq, disjoint from the lake's [0, N)
-- keys) -> pure inserts. The non-DV union read must materialize ALL 5M into its
-- in-memory TreeMap and sort-merge (O(M) heap -> heavy GC as M grows); DV just
-- streams + bitmap-filters (O(1) heap). The latency gap widens as non-DV nears
-- its heap/GC wall. (Upsert via MOD(seq,N) would cap the TreeMap at N -- don't.)
INSERT INTO benchmark_orders_baseline
SELECT 100000000 + seq, MOD(seq,100000), CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)),
       CAST(MOD(seq,5) AS INT), CAST(1 AS BIGINT) FROM gen2;
INSERT INTO benchmark_orders_dv
SELECT 100000000 + seq, MOD(seq,100000), CAST(MOD(seq,1000)+1 AS DECIMAL(10,2)),
       CAST(MOD(seq,5) AS INT), CAST(1 AS BIGINT) FROM gen2;


-- ############################################################################
-- SECTION 3 — result equivalence gate (MUST pass before comparing latency)
--   the two rows MUST be identical column-by-column; otherwise the union reads
--   returned different data and the latency comparison is invalid.
-- ############################################################################

SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS cnt, SUM(amount) AS sum_amt, SUM(version) AS sum_ver
FROM benchmark_orders_baseline;
SELECT COUNT(*) AS cnt, SUM(amount) AS sum_amt, SUM(version) AS sum_ver
FROM benchmark_orders_dv;


-- ############################################################################
-- SECTION 4 — MAIN METRIC: union read aggregation latency (batch = union read)
--   run after 1-2 warmup rounds; take the median wall-clock over >=5 rounds.
--   (read the Job Runtime in the Flink Web UI)
-- ############################################################################

SET 'execution.runtime-mode' = 'batch';
-- IMPORTANT: SECTION 1/2 set parallelism=1 for the writes; raise it for reads,
-- otherwise reads run single-threaded and hide DV's parallelism advantage.
-- With a single bucket, the enumerator (getSplitOwner) spreads the DV table's
-- per-file LakeSnapshotSplits across readers, so a higher parallelism speeds up
-- the DV read; non-DV stays single-reader (one hybrid split per bucket).
-- Use your TaskManager slot count. Same value for both tables.
SET 'parallelism.default' = '8';

-- Q1 COUNT(*)
SELECT COUNT(*) FROM benchmark_orders_baseline;
SELECT COUNT(*) FROM benchmark_orders_dv;

-- Q2 forces reading column values
SELECT COUNT(*) AS cnt, SUM(amount) AS sum_amt, SUM(version) AS sum_ver
FROM benchmark_orders_baseline;
SELECT COUNT(*) AS cnt, SUM(amount) AS sum_amt, SUM(version) AS sum_ver
FROM benchmark_orders_dv;


-- ############################################################################
-- SECTION 5 (optional) — mechanism checks
-- ############################################################################

-- 5a. Paimon-only read ($lake): isolates MOR vs merge-free on the lake layer.
SELECT COUNT(*), SUM(amount) FROM benchmark_orders_baseline$lake;
SELECT COUNT(*), SUM(amount) FROM benchmark_orders_dv$lake;

-- 5b. Parallelism scalability (run in the SETTLED state, i.e. BEFORE SECTION 2;
--     change the value to 1/2/4/8/16 and re-measure each time).
--     Single bucket: non-DV stays single-reader (one hybrid split); DV's per-file
--     splits spread across readers, so its latency keeps dropping with parallelism.
-- SET 'parallelism.default' = '8';
-- SELECT COUNT(*), SUM(amount), SUM(version) FROM benchmark_orders_baseline;
-- SELECT COUNT(*), SUM(amount), SUM(version) FROM benchmark_orders_dv;
