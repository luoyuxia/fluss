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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.TableWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.lake.iceberg.testutils.FlinkIcebergTieringTestBase;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case covering DV generation during tiering only. */
class IcebergDvTieringITCase extends FlinkIcebergTieringTestBase {

    private static final String DEFAULT_DB = "fluss";
    private static final String CATALOG_NAME = "test_iceberg_lake";


    @Test
    void t1() throws Exception {

        System.out.println(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        System.out.println(warehousePath);

        Thread.sleep(900_000_000);
    }

    @Test
    void testDvFilesGeneratedDuringTiering() throws Exception {
        StreamTableEnvironment batchTEnv = createBatchTableEnv();
        JobClient jobClient = buildTieringJob(execEnv);
        try {
            TablePath tablePath = TablePath.of(DEFAULT_DB, "dv_tiering_table");
            String tableName = tablePath.getTableName();
            long tableId = createDvEnabledTable(tablePath, 1);

            writeUpserts(tablePath, Arrays.asList(row(1, "a"), row(2, "b"), row(1, "c")));
            Map<TableBucket, Long> bucketLogEndOffset = getBucketLogEndOffset(tableId, 1);
            waitUntilBucketSynced(tablePath, tableId, 1, false);
            assertReplicaStatus(tablePath, tableId, 1, false, bucketLogEndOffset);

            writeUpserts(tablePath, Arrays.asList(row(1, "a_2"), row(4, "d")));
            writeDeletes(tablePath, List.of(row(2, "b")));
            bucketLogEndOffset = getBucketLogEndOffset(tableId, 1);
            waitUntilBucketSynced(tablePath, tableId, 1, false);
            assertReplicaStatus(tablePath, tableId, 1, false, bucketLogEndOffset);

            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        assertThat(hasDeleteFiles(tablePath)).isTrue();
                        assertThat(currentRecordsByKey(tablePath))
                                .containsExactlyInAnyOrderEntriesOf(Map.of(1, "a_2", 4, "d"));
                    });

            jobClient.cancel().get();

            writeUpserts(tablePath, Arrays.asList(row(4, "d_2"), row(5, "e")));
            writeDeletes(tablePath, List.of(row(1, "a_2")));

            List<Row> unionReadResults =
                    org.apache.flink.util.CollectionUtil.iteratorToList(
                            batchTEnv.executeSql("select * from " + tableName).collect());
            assertThat(unionReadResults)
                    .containsExactlyInAnyOrder(Row.of(4, "d_2"), Row.of(5, "e"));

            System.out.println(FLUSS_CLUSTER_EXTENSION.getClientConfig());
            System.out.println(warehousePath);

            Thread.sleep(500_000_000);

        } finally {
            //            jobClient.cancel().get();
        }
    }

    private long createDvEnabledTable(TablePath tablePath, int bucketNum) throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(bucketNum)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                        .property(ConfigOptions.TABLE_DV_ENABLED, true)
                        .property(ConfigOptions.TABLE_CHANGELOG_IMAGE, ChangelogImage.FULL)
                        .build();
        return createTable(tablePath, tableDescriptor);
    }

    private void writeUpserts(TablePath tablePath, List<InternalRow> rows) throws Exception {
        writeRows(tablePath, rows, false);
    }

    private void writeDeletes(TablePath tablePath, List<InternalRow> rows) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            TableWriter tableWriter = table.newUpsert().createWriter();
            UpsertWriter upsertWriter = (UpsertWriter) tableWriter;
            for (InternalRow row : rows) {
                upsertWriter.delete(row);
            }
            tableWriter.flush();
        }
    }

    private Map<TableBucket, Long> getBucketLogEndOffset(long tableId, int bucketNum) {
        Map<TableBucket, Long> bucketLogEndOffsets = new HashMap<>();
        for (int i = 0; i < bucketNum; i++) {
            TableBucket tableBucket = new TableBucket(tableId, i);
            Replica replica = getLeaderReplica(tableBucket);
            bucketLogEndOffsets.put(tableBucket, replica.getLocalLogEndOffset());
        }
        return bucketLogEndOffsets;
    }

    private StreamTableEnvironment createBatchTableEnv() {
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        StreamTableEnvironment batchTEnv =
                StreamTableEnvironment.create(execEnv, EnvironmentSettings.inBatchMode());
        batchTEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        batchTEnv.executeSql("use catalog " + CATALOG_NAME);
        batchTEnv.executeSql("use " + DEFAULT_DB);
        return batchTEnv;
    }

    private boolean hasDeleteFiles(TablePath tablePath) throws Exception {
        org.apache.iceberg.Table table = icebergCatalog.loadTable(toIceberg(tablePath));
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.deletes().isEmpty()) {
                    return true;
                }
            }
            return false;
        }
    }

    private Map<Integer, String> currentRecordsByKey(TablePath tablePath) throws Exception {
        return getIcebergRecords(tablePath).stream()
                .collect(
                        Collectors.toMap(
                                record -> (Integer) record.getField("c1"),
                                record -> record.getField("c2").toString()));
    }
}
