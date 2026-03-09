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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for overflow partition write — testing that writes targeting expired (dropped) partitions
 * are automatically redirected to the {@code __overflow__} partition.
 *
 * <p>This test verifies that when a partition is dropped from Fluss (simulating expiration), the
 * client transparently redirects writes for that partition to a single overflow partition, which is
 * lazily created on first redirect.
 */
class OverflowPartitionWriteITCase {

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
                    Files.createTempDirectory("fluss-overflow-write-test")
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

    @BeforeAll
    static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
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

    /**
     * Test that writes to a dropped partition are redirected to the overflow partition, regardless
     * of whether dynamic partition creation is enabled.
     *
     * <p>Test flow:
     *
     * <ol>
     *   <li>Create an auto-partitioned PK table with datalake enabled
     *   <li>Wait for auto-partitions to be created
     *   <li>Create a past-year partition manually (auto-partition won't recreate it after drop)
     *   <li>Write initial data to that partition
     *   <li>Drop the partition (simulating expiration)
     *   <li>Write late data targeting the dropped partition — verifies redirect works with both
     *       {@code dynamicPartitionEnabled=true} and {@code dynamicPartitionEnabled=false}
     *   <li>Verify the {@code __overflow__} partition is created
     *   <li>Verify data can be read from the overflow partition
     * </ol>
     */
    @Test
    void testWriteToDroppedPartitionRedirectsToOverflow() throws Exception {
        // 1. Create auto-partitioned PK table with datalake enabled
        TablePath tablePath = TablePath.of(DEFAULT_DB, "overflow_write_test");
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
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();

        // 2. Wait for auto-partitions to be created (current year + next year)
        waitUntilPartitions(tablePath, 2);

        // 3. Create a past-year partition manually — auto-partition won't recreate it after drop,
        // which eliminates the race condition between drop and the auto-partition service.
        String targetPartitionName = "2026";

        // 4. Write initial data to the past-year partition
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "initial_1", targetPartitionName));
            writer.upsert(row(2, "initial_2", targetPartitionName));
            writer.flush();
        }

        // 5. Drop the partition (simulating expiration)
        admin.dropPartition(
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("dt", targetPartitionName)),
                        false)
                .get();

        // Wait until partition is fully dropped
        retry(
                Duration.ofSeconds(30),
                () -> {
                    List<PartitionInfo> partitions = admin.listPartitionInfos(tablePath).get();
                    assertThat(
                                    partitions.stream()
                                            .anyMatch(
                                                    p ->
                                                            p.getPartitionName()
                                                                    .equals(targetPartitionName)))
                            .as("Partition %s should be dropped", targetPartitionName)
                            .isFalse();
                });

        // 6. Write late data targeting the dropped partition using the DEFAULT conf
        // (dynamicPartitionEnabled=true). The overflow redirect must work regardless of this flag.
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(10, "late_data_1", targetPartitionName));
            writer.upsert(row(11, "late_data_2", targetPartitionName));
            writer.flush();
        }

        // 7. Verify the overflow partition was created
        long overflowPartitionId =
                waitValue(
                        () -> {
                            List<PartitionInfo> partitions =
                                    admin.listPartitionInfos(tablePath).get();
                            Optional<PartitionInfo> overflowOpt =
                                    partitions.stream()
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

        // Wait for the overflow partition buckets to be ready
        TableBucket overflowBucket = new TableBucket(tableId, overflowPartitionId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(overflowBucket);

        // 8. Verify data can be read from the overflow partition
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(tablePath)) {
            LogScanner logScanner = table.newScan().createLogScanner();
            logScanner.subscribeFromBeginning(overflowPartitionId, 0);

            List<InternalRow> rows = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (rows.size() < 2 && System.currentTimeMillis() < deadline) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : scanRecords.buckets()) {
                    for (ScanRecord record : scanRecords.records(bucket)) {
                        rows.add(record.getRow());
                    }
                }
            }

            assertThat(rows).as("Should read 2 records from the overflow partition").hasSize(2);

            // Verify the rows contain the expected ids
            List<Integer> ids = rows.stream().map(r -> r.getInt(0)).collect(Collectors.toList());
            assertThat(ids).containsExactlyInAnyOrder(10, 11);

            // Verify the partition column still carries the original partition value
            for (InternalRow row : rows) {
                assertThat(row.getString(2).toString())
                        .as("Row in overflow partition should carry original partition value")
                        .isEqualTo(targetPartitionName);
            }

            logScanner.close();
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
}
