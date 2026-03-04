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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.LakeTieringJobBuilder;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
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
 * ITCase for lake lookup - testing point lookups against Paimon for expired partitions.
 *
 * <p>This test verifies that when a partition is dropped from Fluss (expired), clients can still
 * lookup data from the dropped partition via Paimon lake storage.
 */
class PaimonLakeLookupITCase {

    private static final String DEFAULT_DB = "fluss";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);
        conf.setString("datalake.format", "paimon");
        conf.setString("datalake.paimon.metastore", "filesystem");
        try {
            warehousePath =
                    Files.createTempDirectory("fluss-lake-lookup-test")
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
    protected static Configuration clientConf;
    protected static String warehousePath;
    protected static Catalog paimonCatalog;
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

    protected JobClient buildTieringJob(StreamExecutionEnvironment execEnv) throws Exception {
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

    protected static Map<String, String> getPaimonCatalogConf() {
        Map<String, String> paimonConf = new HashMap<>();
        paimonConf.put("metastore", "filesystem");
        paimonConf.put("warehouse", warehousePath);
        return paimonConf;
    }

    /**
     * Test lake lookup for expired partition.
     *
     * <p>The test flow: 1. Create an auto-partitioned primary key table with datalake enabled 2.
     * Start tiering job 3. Write data to partitions 4. Wait until data is tiered to Paimon 5. Drop
     * one partition 6. Verify that point lookup still works for the dropped partition via Paimon
     */
    @Test
    void testLakeLookupForExpiredPartition() throws Exception {
        // Start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // Create auto-partitioned primary key table
            TablePath tablePath = TablePath.of(DEFAULT_DB, "lake_lookup_pk_table");
            Schema schema =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("name", DataTypes.STRING())
                            .column("date_str", DataTypes.STRING())
                            .primaryKey("id", "date_str")
                            .build();

            TableDescriptor tableDescriptor =
                    TableDescriptor.builder()
                            .schema(schema)
                            .distributedBy(1)
                            .partitionedBy("date_str")
                            .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                            .property(
                                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                    AutoPartitionTimeUnit.YEAR)
                            .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                            .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(10))
                            .build();

            admin.createTable(tablePath, tableDescriptor, true).get();
            long tableId = admin.getTableInfo(tablePath).get().getTableId();

            // Wait for partitions to be created (default pre-create is 3)
            Map<Long, String> partitionNameByIds = waitUntilPartitions(tablePath, 2);
            assertThat(partitionNameByIds).hasSize(2);

            // Write data to all partitions
            Map<String, List<InternalRow>> writtenRowsByPartition = new HashMap<>();
            try (Connection connection = ConnectionFactory.createConnection(clientConf);
                    Table table = connection.getTable(tablePath)) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (String partitionName : partitionNameByIds.values()) {
                    List<InternalRow> partitionRows =
                            Arrays.asList(
                                    row(1, "name1_" + partitionName, partitionName),
                                    row(2, "name2_" + partitionName, partitionName),
                                    row(3, "name3_" + partitionName, partitionName));
                    writtenRowsByPartition.put(partitionName, partitionRows);
                    for (InternalRow row : partitionRows) {
                        writer.upsert(row);
                    }
                }
                writer.flush();
            }

            // Wait until all buckets are synced to Paimon
            for (Long partitionId : partitionNameByIds.keySet()) {
                TableBucket tableBucket = new TableBucket(tableId, partitionId, 0);
                waitUntilBucketSynced(tableBucket);
            }

            // Select one partition to drop
            Long partitionToDropId = partitionNameByIds.keySet().iterator().next();
            String partitionToDropName = partitionNameByIds.get(partitionToDropId);
            List<InternalRow> rowsInDroppedPartition =
                    writtenRowsByPartition.get(partitionToDropName);

            // Drop the partition
            admin.dropPartition(
                            tablePath,
                            new PartitionSpec(
                                    Collections.singletonMap("date_str", partitionToDropName)),
                            false)
                    .get();

            // Wait until partition is dropped
            retry(
                    Duration.ofSeconds(30),
                    () -> {
                        List<org.apache.fluss.metadata.PartitionInfo> remainingPartitions =
                                admin.listPartitionInfos(tablePath).get();
                        assertThat(remainingPartitions).hasSize(1);
                    });

            // Now verify that lake lookup works for the dropped partition
            try (Connection connection = ConnectionFactory.createConnection(clientConf);
                    Table table = connection.getTable(tablePath)) {
                Lookuper lookuper = table.newLookup().createLookuper();
                Thread.sleep(500);

                // Verify lookup for dropped partition returns data from Paimon
                for (InternalRow expectedRow : rowsInDroppedPartition) {
                    int id = expectedRow.getInt(0);
                    InternalRow lookupResult =
                            lookuper.lookup(row(id, partitionToDropName)).get().getSingletonRow();

                    // Should be able to find the row from Paimon
                    assertThat(lookupResult)
                            .as(
                                    "Should be able to lookup row from dropped partition %s via Paimon",
                                    partitionToDropName)
                            .isNotNull();
                    assertThat(lookupResult.getInt(0)).isEqualTo(id);
                    assertThat(lookupResult.getString(1).toString())
                            .isEqualTo(expectedRow.getString(1).toString());
                    assertThat(lookupResult.getString(2).toString()).isEqualTo(partitionToDropName);
                }

                // Verify lookup for non-dropped partitions still works
                for (Map.Entry<String, List<InternalRow>> entry :
                        writtenRowsByPartition.entrySet()) {
                    if (entry.getKey().equals(partitionToDropName)) {
                        continue; // Already tested
                    }
                    String partitionName = entry.getKey();
                    for (InternalRow expectedRow : entry.getValue()) {
                        InternalRow lookupResult =
                                lookuper.lookup(row(expectedRow.getInt(0), partitionName))
                                        .get()
                                        .getSingletonRow();
                        assertThat(lookupResult).isNotNull();
                        assertThat(lookupResult.getInt(0)).isEqualTo(expectedRow.getInt(0));
                    }
                }

                // Verify lookup for non-existent key returns null
                InternalRow nonExistentResult =
                        lookuper.lookup(row(99999, partitionToDropName)).get().getSingletonRow();
                assertThat(nonExistentResult).isNull();
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
