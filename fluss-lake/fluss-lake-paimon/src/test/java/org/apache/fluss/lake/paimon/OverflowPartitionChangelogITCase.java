/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.flink.tiering.source.TieringSourceOptions.POLL_TIERING_TABLE_INTERVAL;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for overflow partition changelog generation — testing that upserts to the overflow
 * partition correctly look up old values from Paimon (historical partitions) and produce proper
 * changelog entries (UPDATE_BEFORE / UPDATE_AFTER).
 *
 * <p>Test flow:
 *
 * <ol>
 *   <li>Create an auto-partitioned PK table with datalake enabled
 *   <li>Start tiering job
 *   <li>Write initial data to a partition, wait for tiering to Paimon
 *   <li>Drop the partition (simulating expiration)
 *   <li>Write upserts targeting the dropped partition → redirected to overflow
 *   <li>Read changelog from overflow partition
 *   <li>Verify UPDATE_BEFORE (old value from Paimon) + UPDATE_AFTER (new value) are generated
 * </ol>
 */
class OverflowPartitionChangelogITCase {

    private static final String DEFAULT_DB = "fluss";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initConfig())
                    .build();

    private static String warehousePath;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);
        conf.setString("datalake.format", "paimon");
        conf.setString("datalake.paimon.metastore", "filesystem");
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-overflow-changelog-test")
                            .resolve("warehouse")
                            .toString();
            conf.setString("datalake.paimon.warehouse", warehousePath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create warehouse path", e);
        }
        return conf;
    }

    private static Connection conn;
    private static Admin admin;
    private static Configuration clientConf;
    private StreamExecutionEnvironment execEnv;

    @BeforeAll
    static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @BeforeEach
    void beforeEach() {
        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        execEnv.setParallelism(2);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    private JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.set(POLL_TIERING_TABLE_INTERVAL, Duration.ofMillis(500L));
        return LakeTieringJobBuilder.newBuilder(
                        execEnv,
                        flussConfig,
                        Configuration.fromMap(getPaimonCatalogConf()),
                        new Configuration(),
                        DataLakeFormat.PAIMON.toString())
                .build();
    }

    private static Map<String, String> getPaimonCatalogConf() {
        Map<String, String> paimonConf = new HashMap<>();
        paimonConf.put("metastore", "filesystem");
        paimonConf.put("warehouse", warehousePath);
        return paimonConf;
    }

    /**
     * Test that upserts to an overflow partition generate correct changelog (UPDATE_BEFORE /
     * UPDATE_AFTER) by looking up old values from Paimon.
     *
     * <p>Scenario: A record (id=1, name="Alice", dt="2026") is written to partition "2026" and
     * tiered to Paimon. After the partition is dropped, an upsert with (id=1, name="Alice_v2",
     * dt="2026") lands in the overflow partition. The overflow tablet should look up the old value
     * from Paimon and produce a -U (old: "Alice") / +U (new: "Alice_v2") changelog pair.
     */
    @Test
    void testOverflowUpsertGeneratesChangelogFromPaimon() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // 1. Create auto-partitioned PK table with FULL changelog image
            TablePath tablePath = TablePath.of(DEFAULT_DB, "overflow_changelog_test");
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("name", DataTypes.STRING())
                            .column("dt", DataTypes.STRING())
                            .primaryKey("id", "dt")
                            .build();

            TableDescriptor tableDescriptor =
                    TableDescriptor.builder()
                            .schema(schema)
                            .distributedBy(1)
                            .partitionedBy("dt")
                            .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                            .property(
                                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                    AutoPartitionTimeUnit.YEAR)
                            .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                            .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(10))
                            .build();

            admin.createTable(tablePath, tableDescriptor, true).get();
            long tableId = admin.getTableInfo(tablePath).get().getTableId();

            // 2. Wait for auto-partitions
            waitUntilPartitions(tablePath, 2);

            // 3. Write initial data to "2026" partition
            String targetPartition = "2026";
            try (Connection connection = ConnectionFactory.createConnection(clientConf);
                    Table table = connection.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                writer.upsert(row(1, "Alice", targetPartition));
                writer.upsert(row(2, "Bob", targetPartition));
                writer.upsert(row(3, "Charlie", targetPartition));
                writer.flush();
            }

            // 4. Wait for data to be tiered to Paimon
            Map<Long, String> partitions =
                    FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getPartitionIdAndNames(tablePath);
            Long targetPartitionId = null;
            for (Map.Entry<Long, String> entry : partitions.entrySet()) {
                if (entry.getValue().equals(targetPartition)) {
                    targetPartitionId = entry.getKey();
                    break;
                }
            }
            assertThat(targetPartitionId)
                    .as("Partition '%s' should exist", targetPartition)
                    .isNotNull();

            TableBucket targetBucket = new TableBucket(tableId, targetPartitionId, 0);
            waitUntilBucketSynced(targetBucket);

            // 5. Drop the partition (simulating expiration)
            admin.dropPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("dt", targetPartition)),
                            false)
                    .get();

            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        List<PartitionInfo> remainingPartitions =
                                admin.listPartitionInfos(tablePath).get();
                        assertThat(
                                        remainingPartitions.stream()
                                                .anyMatch(
                                                        p ->
                                                                p.getPartitionName()
                                                                        .equals(targetPartition)))
                                .as("Partition %s should be dropped", targetPartition)
                                .isFalse();
                    });

            Thread.sleep(3_000);

            // 6. Write upserts targeting the dropped partition → redirected to overflow
            // These upserts update existing keys (id=1,2) and insert a new key (id=10)
            try (Connection connection = ConnectionFactory.createConnection(clientConf);
                    Table table = connection.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                // Update existing records (should generate -U/+U from Paimon lookup)
                writer.upsert(row(1, "Alice_v2", targetPartition));
                writer.upsert(row(2, "Bob_v2", targetPartition));
                // Insert a new record (should generate +I)
                writer.upsert(row(10, "NewRecord", targetPartition));
                writer.flush();
            }

            // 7. Wait for the overflow partition to be created
            long overflowPartitionId =
                    waitValue(
                            () -> {
                                List<PartitionInfo> parts =
                                        admin.listPartitionInfos(tablePath).get();
                                Optional<PartitionInfo> overflowOpt =
                                        parts.stream()
                                                .filter(
                                                        p ->
                                                                p.getPartitionName()
                                                                        .equals(
                                                                                PhysicalTablePath
                                                                                        .OVERFLOW_PARTITION_NAME))
                                                .findFirst();
                                return overflowOpt.map(PartitionInfo::getPartitionId);
                            },
                            Duration.ofMinutes(1),
                            "Overflow partition should be created");

            TableBucket overflowBucket = new TableBucket(tableId, overflowPartitionId, 0);
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(overflowBucket);

            // 8. Read changelog from the overflow partition and verify
            try (Connection connection = ConnectionFactory.createConnection(clientConf);
                    Table table = connection.getTable(tablePath)) {
                LogScanner logScanner = table.newScan().createLogScanner();
                logScanner.subscribeFromBeginning(overflowPartitionId, 0);

                // We expect:
                // - id=1 update: UPDATE_BEFORE(1,"Alice","2026") +
                // UPDATE_AFTER(1,"Alice_v2","2026")
                // - id=2 update: UPDATE_BEFORE(2,"Bob","2026") + UPDATE_AFTER(2,"Bob_v2","2026")
                // - id=10 insert: INSERT(10,"NewRecord","2026")
                // Total: 5 records
                List<ScanRecord> allRecords = new ArrayList<>();
                long deadline = System.currentTimeMillis() + 60_000;
                while (allRecords.size() < 5 && System.currentTimeMillis() < deadline) {
                    ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                    for (TableBucket bucket : scanRecords.buckets()) {
                        for (ScanRecord record : scanRecords.records(bucket)) {
                            allRecords.add(record);
                        }
                    }
                }

                assertThat(allRecords)
                        .as(
                                "Expected 5 changelog records (2x -U/+U pairs + 1 insert), got %d",
                                allRecords.size())
                        .hasSize(5);

                // Records should be ordered: for each key, -U comes before +U
                // Verify id=1 update pair
                ScanRecord rec0 = allRecords.get(0);
                assertThat(rec0.getChangeType()).isEqualTo(ChangeType.UPDATE_BEFORE);
                assertThat(rec0.getRow().getInt(0)).isEqualTo(1);
                assertThat(rec0.getRow().getString(1).toString()).isEqualTo("Alice");

                ScanRecord rec1 = allRecords.get(1);
                assertThat(rec1.getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
                assertThat(rec1.getRow().getInt(0)).isEqualTo(1);
                assertThat(rec1.getRow().getString(1).toString()).isEqualTo("Alice_v2");

                // Verify id=2 update pair
                ScanRecord rec2 = allRecords.get(2);
                assertThat(rec2.getChangeType()).isEqualTo(ChangeType.UPDATE_BEFORE);
                assertThat(rec2.getRow().getInt(0)).isEqualTo(2);
                assertThat(rec2.getRow().getString(1).toString()).isEqualTo("Bob");

                ScanRecord rec3 = allRecords.get(3);
                assertThat(rec3.getChangeType()).isEqualTo(ChangeType.UPDATE_AFTER);
                assertThat(rec3.getRow().getInt(0)).isEqualTo(2);
                assertThat(rec3.getRow().getString(1).toString()).isEqualTo("Bob_v2");

                // Verify id=10 insert
                ScanRecord rec4 = allRecords.get(4);
                assertThat(rec4.getChangeType()).isEqualTo(ChangeType.INSERT);
                assertThat(rec4.getRow().getInt(0)).isEqualTo(10);
                assertThat(rec4.getRow().getString(1).toString()).isEqualTo("NewRecord");

                logScanner.close();
            }

        } finally {
            jobClient.cancel().get();
        }
    }

    private Map<Long, String> waitUntilPartitions(TablePath tablePath, int expectPartitions) {
        return waitValue(
                () -> {
                    Map<Long, String> gotPartitions =
                            FLUSS_CLUSTER_EXTENSION
                                    .getZooKeeperClient()
                                    .getPartitionIdAndNames(tablePath);
                    return expectPartitions == gotPartitions.size()
                            ? Optional.of(gotPartitions)
                            : Optional.empty();
                },
                Duration.ofMinutes(1),
                String.format("expect %d partitions to be created", expectPartitions));
    }

    private void waitUntilBucketSynced(TableBucket tableBucket) {
        waitUntil(
                () -> {
                    Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tableBucket);
                    return replica.getLogTablet().getLakeTableSnapshotId() >= 0;
                },
                Duration.ofMinutes(2),
                "bucket " + tableBucket + " not synced to Paimon");
    }
}
