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

package org.apache.fluss.server.replica;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.historical.HistoricalKvHandle;
import org.apache.fluss.server.kv.historical.HistoricalKvManager;
import org.apache.fluss.server.kv.historical.HistoricalKvStateAccessor;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_BUCKET_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HistoricalPkWriteProcessor}. */
class HistoricalPkWriteProcessorTest extends ReplicaTestBase {

    private static final long TABLE_ID = 987654L;
    private static final long PARTITION_ID = 123L;
    private static final TablePath TABLE_PATH =
            TablePath.of("historical_write_db", "historical_write_table");
    private static final String ORIGINAL_PARTITION = "us$20240107";
    private static final String HISTORICAL_PARTITION = "us$" + HISTORICAL_PARTITION_VALUE;
    private static final TableBucket TABLE_BUCKET = new TableBucket(TABLE_ID, PARTITION_ID, 0);

    @Test
    void testHistoricalInsertUpdateAndDelete() throws Exception {
        TableInfo tableInfo = registerHistoricalTableAndBecomeLeader();
        Replica replica = replicaManager.getReplicaOrException(TABLE_BUCKET);
        assertThat(replica.getKvTablet()).isNull();
        assertThat(kvManager.getKv(TABLE_BUCKET)).isEmpty();
        ExecutorService lookupExecutor = Executors.newSingleThreadExecutor();
        TestingHistoricalLakeLookupManager lakeLookupManager =
                new TestingHistoricalLakeLookupManager(
                        lookupConfiguration(), lookupExecutor, kvManager.getHistoricalKvManager());
        HistoricalPkWriteProcessor processor =
                new HistoricalPkWriteProcessor(
                        kvManager.getHistoricalKvManager(), lakeLookupManager);

        RowType rowType = tableInfo.getRowType();
        byte[] primaryKey =
                KeyEncoder.ofPrimaryKeyEncoder(
                                rowType,
                                tableInfo.getPhysicalPrimaryKeys(),
                                tableInfo.getTableConfig(),
                                tableInfo.isDefaultBucketKey())
                        .encodeKey(row(1, "us", "20240107", "v1"));

        try {
            KvRecordBatch insertBatch =
                    batch(primaryKey, rowType, new Object[] {1, "us", "20240107", "v1"});
            assertThat(
                            processor
                                    .process(
                                            replica,
                                            new PutKvDataForBucket(
                                                    TABLE_BUCKET, insertBatch, ORIGINAL_PARTITION),
                                            null,
                                            MergeMode.DEFAULT,
                                            1)
                                    .lastOffset())
                    .isZero();

            HistoricalKvHandle handle =
                    kvManager
                            .getHistoricalKvManager()
                            .getIfPresent(TABLE_BUCKET)
                            .orElseThrow(() -> new AssertionError("historical handle is missing"));
            assertThat(handle.getDirectory()).isEqualTo(replica.getKvTabletDir());
            HistoricalKvStateAccessor stateAccessor =
                    new HistoricalKvStateAccessor(handle, ORIGINAL_PARTITION);
            assertHistoricalValue(
                    stateAccessor,
                    primaryKey,
                    replica.schemaGetter(),
                    tableInfo,
                    "us",
                    "20240107",
                    "v1");
            assertThat(lakeLookupManager.lookupCount).hasValue(1);

            KvRecordBatch updateBatch =
                    batch(primaryKey, rowType, new Object[] {1, "us", "20240107", "v2"});
            assertThat(
                            processor
                                    .process(
                                            replica,
                                            new PutKvDataForBucket(
                                                    TABLE_BUCKET, updateBatch, ORIGINAL_PARTITION),
                                            null,
                                            MergeMode.DEFAULT,
                                            1)
                                    .lastOffset())
                    .isEqualTo(2L);
            assertHistoricalValue(
                    stateAccessor,
                    primaryKey,
                    replica.schemaGetter(),
                    tableInfo,
                    "us",
                    "20240107",
                    "v2");
            assertThat(lakeLookupManager.lookupCount).hasValue(1);

            KvRecordBatch deleteBatch = batch(primaryKey, rowType, null);
            processor.process(
                    replica,
                    new PutKvDataForBucket(TABLE_BUCKET, deleteBatch, ORIGINAL_PARTITION),
                    null,
                    MergeMode.DEFAULT,
                    1);
            assertThat(stateAccessor.lookup(primaryKey, stateAccessor.encodeKey(primaryKey)))
                    .isEqualTo(KvStateLookupResult.deleted());
            assertThat(lakeLookupManager.lookupCount).hasValue(1);

            kvManager.getHistoricalKvManager().invalidateBucket(TABLE_BUCKET);
            new HistoricalKvRecoverer(
                            kvManager.getHistoricalKvManager(),
                            new TestSnapshotContext(conf.get(ConfigOptions.REMOTE_DATA_DIR)),
                            localDiskManager)
                    .recover(replica);
            HistoricalKvHandle recoveredHandle =
                    kvManager
                            .getHistoricalKvManager()
                            .getIfPresent(TABLE_BUCKET)
                            .orElseThrow(() -> new AssertionError("recovered handle is missing"));
            HistoricalKvStateAccessor recoveredAccessor =
                    new HistoricalKvStateAccessor(recoveredHandle, ORIGINAL_PARTITION);
            assertThat(
                            recoveredAccessor.lookup(
                                    primaryKey, recoveredAccessor.encodeKey(primaryKey)))
                    .as(
                            "lakeEnd=%s, logStart=%s, localStart=%s, highWatermark=%s, localEnd=%s",
                            replica.getLakeLogEndOffset(),
                            replica.getLogTablet().logStartOffset(),
                            replica.getLogTablet().localLogStartOffset(),
                            replica.getLogHighWatermark(),
                            replica.getLocalLogEndOffset())
                    .isEqualTo(KvStateLookupResult.deleted());
        } finally {
            lakeLookupManager.close();
            lookupExecutor.shutdownNow();
        }
    }

    private TableInfo registerHistoricalTableAndBecomeLeader() throws Exception {
        replicaManager.getDiskUsageMonitor().update(0.10);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id", "region", "dt")
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1, "id")
                        .partitionedBy("region", "dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 2)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                        .build();
        TableInfo tableInfo =
                TableInfo.of(TABLE_PATH, TABLE_ID, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
        zkClient.registerTable(
                TABLE_PATH,
                TableRegistration.newTable(TABLE_ID, DEFAULT_REMOTE_DATA_DIR, descriptor));
        zkClient.registerFirstSchema(TABLE_PATH, schema);

        BucketMetadata bucketMetadata =
                new BucketMetadata(
                        TABLE_BUCKET.getBucket(),
                        TABLET_SERVER_ID,
                        INITIAL_LEADER_EPOCH,
                        Collections.singletonList(TABLET_SERVER_ID));
        ServerInfo tabletServer =
                new ServerInfo(
                        TABLET_SERVER_ID,
                        "rack1",
                        Endpoint.fromListenersString("CLIENT://localhost:90"),
                        ServerType.TABLET_SERVER);
        serverMetadataCache.updateClusterMetadata(
                new ClusterMetadata(
                        null,
                        Collections.singleton(tabletServer),
                        Collections.singletonList(
                                new TableMetadata(tableInfo, Collections.emptyList())),
                        Collections.singletonList(
                                new PartitionMetadata(
                                        TABLE_ID,
                                        HISTORICAL_PARTITION,
                                        PARTITION_ID,
                                        Collections.singletonList(bucketMetadata)))));

        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> leaderFuture =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(TABLE_PATH, HISTORICAL_PARTITION),
                                TABLE_BUCKET,
                                Collections.singletonList(TABLET_SERVER_ID),
                                new LeaderAndIsr(
                                        TABLET_SERVER_ID,
                                        INITIAL_LEADER_EPOCH,
                                        Collections.singletonList(TABLET_SERVER_ID),
                                        Collections.emptyList(),
                                        INITIAL_COORDINATOR_EPOCH,
                                        INITIAL_BUCKET_EPOCH))),
                leaderFuture::complete);
        assertThat(leaderFuture.get(10, TimeUnit.SECONDS))
                .containsOnly(new NotifyLeaderAndIsrResultForBucket(TABLE_BUCKET));
        return tableInfo;
    }

    private static KvRecordBatch batch(byte[] primaryKey, RowType rowType, Object[] value)
            throws Exception {
        return KvRecordTestUtils.KvRecordBatchFactory.of(DEFAULT_SCHEMA_ID)
                .ofRecords(
                        KvRecordTestUtils.KvRecordFactory.of(rowType).ofRecord(primaryKey, value));
    }

    private static void assertHistoricalValue(
            HistoricalKvStateAccessor stateAccessor,
            byte[] primaryKey,
            SchemaGetter schemaGetter,
            TableInfo tableInfo,
            String expectedRegion,
            String expectedDt,
            String expectedValue)
            throws Exception {
        KvStateLookupResult result =
                stateAccessor.lookup(primaryKey, stateAccessor.encodeKey(primaryKey));
        assertThat(result.isPresent()).isTrue();
        BinaryValue value =
                new ValueDecoder(schemaGetter, tableInfo.getTableConfig().getKvFormat())
                        .decodeValue(result.value());
        assertThat(value.row.getString(1)).isEqualTo(BinaryString.fromString(expectedRegion));
        assertThat(value.row.getString(2)).isEqualTo(BinaryString.fromString(expectedDt));
        assertThat(value.row.getString(3)).isEqualTo(BinaryString.fromString(expectedValue));
    }

    private final class TestingHistoricalLakeLookupManager extends HistoricalLakeLookupManager {
        private final AtomicInteger lookupCount = new AtomicInteger();

        private TestingHistoricalLakeLookupManager(
                Configuration configuration,
                ExecutorService executor,
                HistoricalKvManager historicalKvManager) {
            super(
                    configuration,
                    null,
                    executor,
                    TABLET_SERVER_ID,
                    Ticker.systemTicker(),
                    Scheduler.disabledScheduler(),
                    historicalKvManager);
        }

        @Override
        @Nullable
        byte[] lookupValue(
                TableInfo tableInfo,
                ResolvedPartitionSpec originalPartitionSpec,
                int bucketId,
                byte[] key) {
            lookupCount.incrementAndGet();
            return null;
        }

        @Override
        LakeTableLookuper createLakeTableLookuper(TablePath tablePath, String ioTmpDir) {
            throw new AssertionError("The processor must use the synchronous lookup primitive");
        }
    }

    private Configuration lookupConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS, 1);
        configuration.set(ConfigOptions.IO_TMP_DIR, tempDir.getAbsolutePath());
        return configuration;
    }
}
