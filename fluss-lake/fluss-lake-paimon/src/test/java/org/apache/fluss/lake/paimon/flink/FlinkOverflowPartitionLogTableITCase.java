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

package org.apache.fluss.lake.paimon.flink;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case for overflow partition write + tiering + union read for log tables.
 *
 * <p>Tests the complete end-to-end flow:
 *
 * <ol>
 *   <li>Write to an auto-partitioned log table with datalake enabled
 *   <li>Tiering service syncs the data to Paimon
 *   <li>Write late-arriving data to an expired (dropped) partition
 *   <li>The client transparently redirects the write to the {@code __overflow__} partition
 *   <li>The tiering service syncs the overflow partition data to Paimon, routing each record to its
 *       original Paimon partition based on the partition column in the row
 *   <li>A union read (Paimon snapshot + Fluss log) returns all data, including the late data
 * </ol>
 *
 * <p>Key design aspect: the {@code AppendOnlyWriter} in the tiering service extracts the target
 * Paimon partition directly from the row data (the {@code c} column value), so overflow records
 * with {@code c = "2020"} are correctly tiered to Paimon partition {@code c = "2020"} rather than
 * to a synthetic {@code __overflow__} partition.
 */
class FlinkOverflowPartitionLogTableITCase extends FlinkUnionReadTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    /**
     * End-to-end test for overflow partition write, tiering, and union read.
     *
     * <p>Flow:
     *
     * <ol>
     *   <li>Create an auto-partitioned log table (columns: a INT, b STRING, c STRING (partition))
     *   <li>Start the tiering job
     *   <li>Write data to the auto-created active partitions (current year + next year)
     *   <li>Wait for the tiering service to sync the active-partition data to Paimon
     *   <li>Manually create a past-year partition "2020" and write data to it
     *   <li>Wait for the "2020" data to be tiered to Paimon
     *   <li>Drop partition "2020" from Fluss to simulate expiration
     *   <li>Write late data targeting "2020" — the client redirects to {@code __overflow__}
     *   <li>Wait for the overflow partition to be tiered (data lands in Paimon "2020")
     *   <li>Assert that a batch union read returns all expected rows
     * </ol>
     */
    @Test
    void testOverflowPartitionWriteAndTiering() throws Exception {
        // 1. Create auto-partitioned log table: schema is (a INT, b STRING, c STRING)
        //    where c is the partition key, auto-partition enabled (YEAR granularity)
        String tableName = "overflow_log_table";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        long tableId =
                createLogTable(
                        tablePath,
                        DEFAULT_BUCKET_NUM,
                        true,
                        Collections.emptyMap(),
                        Collections.emptyMap());

        // 2. Start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        try {
            // 3. Wait for auto-partitions to be created (current year + next year)
            Map<Long, String> autoPartitions = waitUntilPartitions(tablePath);
            assertThat(autoPartitions).hasSize(2);

            // 4. Write data to all active partitions and collect expected rows
            List<Row> allExpectedRows = new ArrayList<>();
            int id = 1;
            for (Map.Entry<Long, String> entry : autoPartitions.entrySet()) {
                String partition = entry.getValue();
                List<InternalRow> partitionRows =
                        Arrays.asList(
                                row(id++, "active_row_1_" + partition, partition),
                                row(id++, "active_row_2_" + partition, partition));
                writeRows(tablePath, partitionRows, true);
                for (InternalRow r : partitionRows) {
                    allExpectedRows.add(
                            Row.of(
                                    r.getInt(0),
                                    r.getString(1).toString(),
                                    r.getString(2).toString()));
                }
            }

            // 5. Wait until the tiering service syncs all active partitions to Paimon
            waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, true);

            String expiredPartition = "2026";
            admin.dropPartition(
                            tablePath,
                            new PartitionSpec(Collections.singletonMap("c", expiredPartition)),
                            false)
                    .get();

            // wait util the partition expired
            Map<Long, String> partitionsWithExpired =
                    waitUntilPartitions(
                            getFlussClusterExtension().getZooKeeperClient(), tablePath, 1);

            Thread.sleep(3_000);

            // 7. Write initial data to the "2026" partition
            List<InternalRow> initialExpiredRows =
                    Arrays.asList(
                            row(100, "initial_2020_row_1", expiredPartition),
                            row(101, "initial_2020_row_2", expiredPartition));
            writeRows(tablePath, initialExpiredRows, true);
            for (InternalRow r : initialExpiredRows) {
                allExpectedRows.add(
                        Row.of(r.getInt(0), r.getString(1).toString(), r.getString(2).toString()));
            }

            // 10. Write late data targeting the now-dropped "2020" partition.
            //     The client detects the partition is missing on the server (auto-partitioned +
            //     datalake table) and transparently redirects to the __overflow__ partition.
            List<InternalRow> lateRows =
                    Arrays.asList(
                            row(200, "late_overflow_row_1", expiredPartition),
                            row(201, "late_overflow_row_2", expiredPartition));
            writeRows(tablePath, lateRows, true);
            for (InternalRow r : lateRows) {
                // Expected: the late data appears in union read as original partition value "2020"
                allExpectedRows.add(
                        Row.of(r.getInt(0), r.getString(1).toString(), r.getString(2).toString()));
            }

            // 11. Wait until the __overflow__ partition is created and tiered.
            //     After tiering, the overflow records (c = "2026") land in Paimon partition "2026"
            //     because AppendOnlyWriter extracts the target partition from the row data.
            Map<Long, String> partitionsAfterOverflow =
                    waitUntilPartitions(
                            getFlussClusterExtension().getZooKeeperClient(), tablePath, 2);
            Long overflowPartitionId =
                    partitionsAfterOverflow.entrySet().stream()
                            .filter(
                                    e ->
                                            e.getValue()
                                                    .equals(
                                                            PhysicalTablePath
                                                                    .OVERFLOW_PARTITION_NAME))
                            .map(Map.Entry::getKey)
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "Overflow partition "
                                                            + PhysicalTablePath
                                                                    .OVERFLOW_PARTITION_NAME
                                                            + " not found after writing to expired partition"));

            TableBucket overflowBucket = new TableBucket(tableId, overflowPartitionId, 0);
            waitUntilBucketSynced(overflowBucket);

            // 12. Union read: batch SELECT * should return ALL rows:
            //     - active partition rows (from Paimon snapshot + Fluss log delta)
            //     - initial "2020" rows (from Paimon — "2020" is expired in Fluss)
            //     - late "2020" rows (from Paimon "2020" — tiered via __overflow__)
            List<Row> actual =
                    CollectionUtil.iteratorToList(
                            batchTEnv.executeSql("select * from " + tableName).collect());
            System.out.println(actual);

            assertThat(actual)
                    .as(
                            "Union read should return all rows including late data "
                                    + "written to the overflow partition")
                    .containsExactlyInAnyOrderElementsOf(allExpectedRows);

        } finally {
            jobClient.cancel().get();
        }
    }
}
