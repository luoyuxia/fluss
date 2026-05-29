/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.flink;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test case for Flink union read on Paimon tables with deletion vectors enabled and
 * auto-compaction (instead of manual compaction via {@code CompactHelper}).
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Readable snapshot is produced by Paimon auto-compaction
 *   <li>Union read returns correct data after auto-compaction
 *   <li>Multiple rounds of writes and tiering work correctly with DV + auto-compaction
 * </ul>
 */
class FlinkUnionReadDvAutoCompactionITCase extends FlinkUnionReadTestBase {

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    /**
     * Test union read on a DV-enabled table with Paimon auto-compaction.
     *
     * <p>Unlike {@link FlinkUnionReadDvTableITCase} which uses manual compaction via {@code
     * CompactHelper}, this test relies on Paimon's auto-compaction (triggered by {@code
     * paimon.num-sorted-run.compaction-trigger=2}) to produce readable snapshots.
     *
     * <p>Test flow:
     *
     * <ol>
     *   <li>Create a DV-enabled table with auto-compaction (1 bucket)
     *   <li>Write initial data (keys 0-4), wait for tiering sync
     *   <li>Write overlapping updates (keys 0,1,3 updated, key 5 added), wait for tiering sync
     *   <li>Write more data (keys 6-10) to trigger auto-compaction
     *   <li>Wait for readable snapshot produced by auto-compaction
     *   <li>Write post-snapshot data (keys 11-15) and verify union read
     * </ol>
     */
    @Test
    void testUnionReadDvTableWithAutoCompaction() throws Exception {
        JobClient jobClient = buildTieringJob(execEnv);
        Throwable testError = null;
        try {
            // Step 1: Create auto-compaction DV table (1 bucket)
            String tableName = "testUnionReadDvTableWithAutoCompaction";
            TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
            int bucketNum = 1;
            long tableId = createDvAutoCompactionTable(tablePath, bucketNum);

            // Step 2: Round 1 - Write initial data (keys 0-4)
            List<Row> writtenRows = new ArrayList<>();
            writtenRows.addAll(writeRows(tablePath, 0, 5, "v"));

            Map<TableBucket, Long> bucketLogEndOffset =
                    getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, bucketNum, false);
            assertReplicaStatus(bucketLogEndOffset);

            // Step 3: Round 2 - Overlapping key updates + new key
            List<InternalRow> round2Rows = new ArrayList<>();
            round2Rows.add(row(0, "v0_updated", "v0_updated"));
            round2Rows.add(row(1, "v1_updated", "v1_updated"));
            round2Rows.add(row(5, "v5", "v5"));
            round2Rows.add(row(3, "v3_updated", "v3_updated"));
            writeRows(tablePath, round2Rows, false);

            // Update expected rows: latest values for keys 0, 1, 3
            writtenRows.removeIf(
                    r -> {
                        int key = (int) r.getField(0);
                        return key == 0 || key == 1 || key == 3;
                    });
            writtenRows.add(Row.of(0, "v0_updated", "v0_updated"));
            writtenRows.add(Row.of(1, "v1_updated", "v1_updated"));
            writtenRows.add(Row.of(5, "v5", "v5"));
            writtenRows.add(Row.of(3, "v3_updated", "v3_updated"));

            bucketLogEndOffset = getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, bucketNum, false);
            assertReplicaStatus(bucketLogEndOffset);

            // Step 4: Round 3 - Write more data to drive auto-compaction
            writtenRows.addAll(writeRows(tablePath, 6, 11, "v"));

            bucketLogEndOffset = getBucketLogEndOffset(tableId, bucketNum, null);
            waitUntilBucketSynced(tablePath, tableId, bucketNum, false);
            assertReplicaStatus(bucketLogEndOffset);

            // Step 5: Wait for readable snapshot produced by auto-compaction
            CommonTestUtils.retry(
                    Duration.ofMinutes(2),
                    () -> {
                        LakeSnapshot snapshot = admin.getReadableLakeSnapshot(tablePath).get();
                        assertThat(snapshot.getSnapshotId()).isGreaterThan(0);
                    });

            // Step 6: Write post-readable-snapshot data
            writtenRows.addAll(writeRows(tablePath, 11, 16, "v"));

            // Step 7: Verify union read returns all distinct keys with latest values
            CloseableIterator<Row> rowIter =
                    streamTEnv.executeSql("select * from " + tableName).collect();
            assertResultsIgnoreOrder(rowIter, toString(writtenRows), true);
        } catch (Throwable t) {
            testError = t;
            throw t;
        } finally {
            try {
                jobClient.cancel().get();
            } catch (Exception e) {
                if (testError != null) {
                    testError.addSuppressed(e);
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Create a PK table with deletion vectors and auto-compaction enabled.
     *
     * @param tablePath the table path
     * @param bucketNum number of buckets
     * @return the created table ID
     */
    private long createDvAutoCompactionTable(TablePath tablePath, int bucketNum) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("c3", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION.key(), "true")
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .customProperty("paimon.deletion-vectors.enabled", "true")
                        .customProperty("paimon.num-sorted-run.compaction-trigger", "2")
                        .build();
        return createTable(tablePath, tableDescriptor);
    }

    private List<Row> writeRows(TablePath tablePath, int from, int to, String valuePrefix)
            throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> flinkRows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(row(i, valuePrefix + i, valuePrefix + i));
            flinkRows.add(Row.of(i, valuePrefix + i, valuePrefix + i));
        }
        writeRows(tablePath, rows, false);
        return flinkRows;
    }

    private Map<TableBucket, Long> getBucketLogEndOffset(
            long tableId, int bucketNum, Long partitionId) {
        Map<TableBucket, Long> bucketLogEndOffsets = new HashMap<>();
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, i);
            Replica replica = getLeaderReplica(tableBucket);
            bucketLogEndOffsets.put(tableBucket, replica.getLocalLogEndOffset());
        }
        return bucketLogEndOffsets;
    }

    private List<String> toString(List<Row> rows) {
        return rows.stream().map(Row::toString).collect(Collectors.toList());
    }
}
