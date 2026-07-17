/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.lake.paimon.write;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end IT case for writing expired primary-key partitions through historical storage. */
class HistoricalPartitionWriteITCase extends FlinkPaimonTieringTestBase {

    private static final String EXPIRED_PARTITION_NAME = "20240101";
    private static final int INITIAL_PARTITION_RETENTION = 100000;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(historicalWriteConfig())
                    .setNumOfTabletServers(3)
                    .build();

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll(FLUSS_CLUSTER_EXTENSION.getClientConfig());
    }

    @Test
    void testHistoricalPrimaryKeyUpdateAndDelete() throws Exception {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "historical_write_pk");
        Schema schema = partitionedPkSchema();
        long tableId = createTable(tablePath, partitionedPkDescriptor(schema));
        PartitionSpec expiredPartitionSpec = partitionSpec(EXPIRED_PARTITION_NAME);
        admin.createPartition(tablePath, expiredPartitionSpec, false).get();
        long originalPartitionId = getPartitionId(tablePath, EXPIRED_PARTITION_NAME);
        FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, originalPartitionId);

        InternalRow oldRow = row(1, EXPIRED_PARTITION_NAME, "old-value");
        writeRows(tablePath, Collections.singletonList(oldRow), false);
        TableBucket originalBucket = new TableBucket(tableId, originalPartitionId, 0);
        FLUSS_CLUSTER_EXTENSION.triggerAndWaitSnapshots(Collections.singleton(originalBucket));

        JobClient tieringJob = buildTieringJob(execEnv);
        try {
            assertReplicaStatus(originalBucket, 1);
            assertPaimonRow(tablePath, oldRow);
        } finally {
            tieringJob.cancel().get();
        }

        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.set(
                                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION.key(),
                                        "1")),
                        false)
                .get();
        admin.dropPartition(tablePath, expiredPartitionSpec, true).get();
        waitUntilPartitionDropped(tablePath, EXPIRED_PARTITION_NAME);

        Configuration lateWriteConf = new Configuration(clientConf);
        lateWriteConf.set(ConfigOptions.CLIENT_WRITER_DYNAMIC_CREATE_PARTITION_ENABLED, false);
        try (Connection lateWriteConnection = ConnectionFactory.createConnection(lateWriteConf);
                Table table = lateWriteConnection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            InternalRow updatedRow = row(1, EXPIRED_PARTITION_NAME, "new-value");
            CompletableFuture<UpsertResult> upsertFuture = writer.upsert(updatedRow);
            writer.flush();
            UpsertResult upsertResult = upsertFuture.get();

            long historicalPartitionId = getPartitionId(tablePath, HISTORICAL_PARTITION_VALUE);
            assertThat(admin.listPartitionInfos(tablePath).get())
                    .extracting(PartitionInfo::getPartitionName)
                    .contains(HISTORICAL_PARTITION_VALUE)
                    .doesNotContain(EXPIRED_PARTITION_NAME);
            TableBucket historicalBucket = new TableBucket(tableId, historicalPartitionId, 0);
            assertThat(upsertResult.getBucket()).isEqualTo(historicalBucket);
            assertThat(upsertResult.getLogEndOffset()).isEqualTo(2L);

            Lookuper lookuper = table.newLookup().createLookuper();
            assertThatRow(lookuper.lookup(row(1, EXPIRED_PARTITION_NAME)).get().getSingletonRow())
                    .withSchema(schema.getRowType())
                    .isEqualTo(updatedRow);

            // Verify the replicated historical WAL can be lazily recovered after leader failover.
            int previousLeader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(historicalBucket);
            FLUSS_CLUSTER_EXTENSION.stopTabletServer(previousLeader);
            try {
                assertThat(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(historicalBucket))
                        .isNotEqualTo(previousLeader);
                assertThatRow(
                                lookuper.lookup(row(1, EXPIRED_PARTITION_NAME))
                                        .get()
                                        .getSingletonRow())
                        .withSchema(schema.getRowType())
                        .isEqualTo(updatedRow);
            } finally {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(previousLeader);
            }

            tieringJob = buildTieringJob(execEnv);
            try {
                // FULL changelog mode writes UPDATE_BEFORE and UPDATE_AFTER for the lake-backed
                // historical update.
                assertReplicaStatus(historicalBucket, 2);
                assertPaimonRow(tablePath, updatedRow);
                assertPaimonPartitions(tablePath);

                writer.delete(row(1, EXPIRED_PARTITION_NAME, null)).get();
                writer.flush();
                assertThat(lookuper.lookup(row(1, EXPIRED_PARTITION_NAME)).get().getRowList())
                        .isEmpty();

                assertReplicaStatus(historicalBucket, 3);
                assertPaimonTableEmpty(tablePath);
                assertThat(lookuper.lookup(row(1, EXPIRED_PARTITION_NAME)).get().getRowList())
                        .isEmpty();
            } finally {
                tieringJob.cancel().get();
            }
        }
    }

    @Override
    protected FlussClusterExtension getFlussClusterExtension() {
        return FLUSS_CLUSTER_EXTENSION;
    }

    private static Configuration historicalWriteConfig() {
        Configuration conf = initConfig();
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }

    private static Schema partitionedPkSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .column("value", DataTypes.STRING())
                .primaryKey("id", "dt")
                .build();
    }

    private static TableDescriptor partitionedPkDescriptor(Schema schema) {
        return TableDescriptor.builder()
                .schema(schema)
                .distributedBy(1, "id")
                .partitionedBy("dt")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.DAY)
                .property(
                        ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION,
                        INITIAL_PARTITION_RETENTION)
                .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500))
                .build();
    }

    private static PartitionSpec partitionSpec(String partitionName) {
        return new PartitionSpec(Collections.singletonMap("dt", partitionName));
    }

    private static long getPartitionId(TablePath tablePath, String partitionName) throws Exception {
        for (PartitionInfo partitionInfo : admin.listPartitionInfos(tablePath).get()) {
            if (partitionName.equals(partitionInfo.getPartitionName())) {
                return partitionInfo.getPartitionId();
            }
        }
        throw new IllegalStateException("Partition " + partitionName + " does not exist.");
    }

    private static void waitUntilPartitionDropped(TablePath tablePath, String partitionName) {
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(admin.listPartitionInfos(tablePath).get())
                                .noneMatch(p -> partitionName.equals(p.getPartitionName())));
    }

    private static void assertPaimonRow(TablePath tablePath, InternalRow expectedRow)
            throws Exception {
        try (CloseableIterator<org.apache.paimon.data.InternalRow> rows = paimonRows(tablePath)) {
            assertThat(rows.hasNext()).isTrue();
            org.apache.paimon.data.InternalRow actualRow = rows.next();
            assertThat(actualRow.getInt(0)).isEqualTo(expectedRow.getInt(0));
            assertThat(actualRow.getString(1).toString())
                    .isEqualTo(expectedRow.getString(1).toString());
            assertThat(actualRow.getString(2).toString())
                    .isEqualTo(expectedRow.getString(2).toString());
            assertThat(rows.hasNext()).isFalse();
        }
    }

    private static void assertPaimonTableEmpty(TablePath tablePath) throws Exception {
        try (CloseableIterator<org.apache.paimon.data.InternalRow> rows = paimonRows(tablePath)) {
            assertThat(rows.hasNext()).isFalse();
        }
    }

    private static CloseableIterator<org.apache.paimon.data.InternalRow> paimonRows(
            TablePath tablePath) throws Exception {
        FileStoreTable table =
                (FileStoreTable)
                        getPaimonCatalog()
                                .getTable(
                                        Identifier.create(
                                                tablePath.getDatabaseName(),
                                                tablePath.getTableName()));
        RecordReader<org.apache.paimon.data.InternalRow> reader =
                table.newRead().createReader(table.newReadBuilder().newScan().plan());
        return reader.toCloseableIterator();
    }

    private static void assertPaimonPartitions(TablePath tablePath) throws Exception {
        assertThat(
                        getPaimonCatalog()
                                .listPartitions(
                                        Identifier.create(
                                                tablePath.getDatabaseName(),
                                                tablePath.getTableName())))
                .extracting(partition -> partition.spec().get("dt"))
                .containsExactly(EXPIRED_PARTITION_NAME);
    }
}
