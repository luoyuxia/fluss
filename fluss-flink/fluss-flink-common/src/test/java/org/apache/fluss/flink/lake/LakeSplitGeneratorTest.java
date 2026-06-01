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

package org.apache.fluss.flink.lake;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.rpc.messages.PbLakeDvEntry;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LakeSplitGenerator} with DV (Deletion Vector) support. */
class LakeSplitGeneratorTest {

    private static final long TABLE_ID = 100L;
    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final int BUCKET_COUNT = 3;

    // ---- Non-partitioned PK table with DV enabled ----

    @Test
    void testDvEnabledWithDataGap() throws Exception {
        // Lake snapshot offsets: bucket 0=100, bucket 1=200, bucket 2=300
        // Stopping offsets: bucket 0=150, bucket 1=250, bucket 2=350
        // All buckets have data gaps, so DV should be fetched for all

        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 1), 200L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 2), 300L);

        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 150L);
        stoppingOffsets.put(1, 250L);
        stoppingOffsets.put(2, 350L);

        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);
        // Configure DV responses for all 3 buckets
        for (int b = 0; b < BUCKET_COUNT; b++) {
            admin.addDvResponse(
                    new TableBucket(TABLE_ID, b),
                    createDvResponse("file" + b + ".parquet", new byte[] {(byte) b}));
        }

        LakeSplitGenerator generator = createGenerator(true, false, admin, stoppingOffsets);
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        // DV enabled: each bucket produces LakeSnapshotSplit + LogSplit = 6 splits total
        assertThat(splits).hasSize(BUCKET_COUNT * 2);
        for (int b = 0; b < BUCKET_COUNT; b++) {
            LakeSnapshotSplit lakeSplit = findLakeSnapshotSplitForBucket(splits, b);
            assertThat(lakeSplit.getLakeDvMap()).isNotNull();
            assertThat(lakeSplit.getLakeDvMap()).hasSize(1);
            assertThat(lakeSplit.getLakeDvMap()).containsKey("file" + b + ".parquet");

            LogSplit logSplit = findLogSplitForBucket(splits, b);
            assertThat(logSplit).isNotNull();
        }

        // Verify all 3 buckets were fetched
        assertThat(admin.dvFetchedBuckets).hasSize(BUCKET_COUNT);
    }

    @Test
    void testDvEnabledNoDataGap() throws Exception {
        // Lake snapshot offsets equal stopping offsets → no DV needed
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 1), 200L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 2), 300L);

        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 100L);
        stoppingOffsets.put(1, 200L);
        stoppingOffsets.put(2, 300L);

        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);

        LakeSplitGenerator generator = createGenerator(true, false, admin, stoppingOffsets);
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits).hasSize(BUCKET_COUNT);
        for (SourceSplitBase split : splits) {
            LakeSnapshotAndFlussLogSplit hybridSplit = (LakeSnapshotAndFlussLogSplit) split;
            // No DV should be attached since there's no data gap
            assertThat(hybridSplit.getDvSnapshot()).isNull();
        }

        // Verify no DV fetch was made
        assertThat(admin.dvFetchedBuckets).isEmpty();
    }

    @Test
    void testDvEnabledMixedGap() throws Exception {
        // Bucket 0: gap (100 vs 150) → DV needed
        // Bucket 1: no gap (200 vs 200) → DV not needed
        // Bucket 2: gap (300 vs 400) → DV needed
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 1), 200L);
        lakeOffsets.put(new TableBucket(TABLE_ID, 2), 300L);

        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 150L);
        stoppingOffsets.put(1, 200L);
        stoppingOffsets.put(2, 400L);

        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);
        admin.addDvResponse(
                new TableBucket(TABLE_ID, 0), createDvResponse("file0.parquet", new byte[] {1}));
        admin.addDvResponse(
                new TableBucket(TABLE_ID, 2), createDvResponse("file2.parquet", new byte[] {2}));

        LakeSplitGenerator generator = createGenerator(true, false, admin, stoppingOffsets);
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        // Bucket 0: DV → LakeSnapshotSplit + LogSplit (2 splits)
        // Bucket 1: no DV (no gap) → LakeSnapshotAndFlussLogSplit (1 split)
        // Bucket 2: DV → LakeSnapshotSplit + LogSplit (2 splits)
        assertThat(splits).hasSize(5);

        // Bucket 0: has DV - split into separate lake + log
        LakeSnapshotSplit lakeSplit0 = findLakeSnapshotSplitForBucket(splits, 0);
        assertThat(lakeSplit0.getLakeDvMap()).isNotNull();
        assertThat(lakeSplit0.getLakeDvMap()).containsKey("file0.parquet");
        LogSplit logSplit0 = findLogSplitForBucket(splits, 0);
        assertThat(logSplit0).isNotNull();

        // Bucket 1: no DV (no gap) - still LakeSnapshotAndFlussLogSplit
        LakeSnapshotAndFlussLogSplit split1 = findHybridSplitForBucket(splits, 1);
        assertThat(split1.getDvSnapshot()).isNull();

        // Bucket 2: has DV - split into separate lake + log
        LakeSnapshotSplit lakeSplit2 = findLakeSnapshotSplitForBucket(splits, 2);
        assertThat(lakeSplit2.getLakeDvMap()).isNotNull();
        assertThat(lakeSplit2.getLakeDvMap()).containsKey("file2.parquet");
        LogSplit logSplit2 = findLogSplitForBucket(splits, 2);
        assertThat(logSplit2).isNotNull();

        // Verify only buckets 0 and 2 were fetched
        assertThat(admin.dvFetchedBuckets)
                .containsExactlyInAnyOrder(
                        new TableBucket(TABLE_ID, 0), new TableBucket(TABLE_ID, 2));
    }

    @Test
    void testDvDisabled() throws Exception {
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, 0), 100L);

        Map<Integer, Long> stoppingOffsets = new HashMap<>();
        stoppingOffsets.put(0, 200L);
        stoppingOffsets.put(1, 200L);
        stoppingOffsets.put(2, 200L);

        TestAdmin admin = new TestAdmin(lakeOffsets, stoppingOffsets);

        // DV disabled
        LakeSplitGenerator generator = createGenerator(false, false, admin, stoppingOffsets);
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits).isNotNull();
        // Verify no DV fetch was made
        assertThat(admin.dvFetchedBuckets).isEmpty();
    }

    // ---- Partitioned PK table with DV enabled ----

    @Test
    void testDvEnabledPartitionedTable() throws Exception {
        long partition1Id = 1001L;
        long partition2Id = 1002L;

        // Lake snapshot offsets with partition IDs
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(TABLE_ID, partition1Id, 0), 100L);
        lakeOffsets.put(new TableBucket(TABLE_ID, partition1Id, 1), 200L);
        lakeOffsets.put(new TableBucket(TABLE_ID, partition2Id, 0), 50L);
        lakeOffsets.put(new TableBucket(TABLE_ID, partition2Id, 1), 60L);

        // Stopping offsets per partition
        Map<Integer, Long> stoppingOffsetsP1 = new HashMap<>();
        stoppingOffsetsP1.put(0, 150L); // gap
        stoppingOffsetsP1.put(1, 200L); // no gap
        stoppingOffsetsP1.put(2, 0L); // no lake data

        Map<Integer, Long> stoppingOffsetsP2 = new HashMap<>();
        stoppingOffsetsP2.put(0, 100L); // gap
        stoppingOffsetsP2.put(1, 60L); // no gap
        stoppingOffsetsP2.put(2, 0L); // no lake data

        // Partition infos
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfos.add(
                new PartitionInfo(
                        partition1Id,
                        new ResolvedPartitionSpec(
                                Collections.singletonList("dt"),
                                Collections.singletonList("2025-01-01")),
                        null));
        partitionInfos.add(
                new PartitionInfo(
                        partition2Id,
                        new ResolvedPartitionSpec(
                                Collections.singletonList("dt"),
                                Collections.singletonList("2025-01-02")),
                        null));

        TestAdmin admin =
                new TestAdmin(lakeOffsets, null) {
                    @Override
                    protected Map<Integer, Long> getStoppingOffsetsForPartition(
                            String partitionName) {
                        if ("2025-01-01".equals(partitionName)) {
                            return stoppingOffsetsP1;
                        } else if ("2025-01-02".equals(partitionName)) {
                            return stoppingOffsetsP2;
                        }
                        return Collections.emptyMap();
                    }
                };

        // Add DV responses for buckets with data gap
        admin.addDvResponse(
                new TableBucket(TABLE_ID, partition1Id, 0),
                createDvResponse("p1-file0.parquet", new byte[] {1}));
        admin.addDvResponse(
                new TableBucket(TABLE_ID, partition2Id, 0),
                createDvResponse("p2-file0.parquet", new byte[] {2}));

        LakeSplitGenerator generator =
                createPartitionedGenerator(true, admin, partitionInfos, BUCKET_COUNT);
        List<SourceSplitBase> splits = generator.generateHybridLakeFlussSplits();

        assertThat(splits).isNotNull();

        // Verify DV was only fetched for buckets with gaps
        // partition1/bucket0: gap (100 vs 150) → fetched
        // partition1/bucket1: no gap (200 vs 200) → not fetched
        // partition2/bucket0: gap (50 vs 100) → fetched
        // partition2/bucket1: no gap (60 vs 60) → not fetched
        assertThat(admin.dvFetchedBuckets)
                .containsExactlyInAnyOrder(
                        new TableBucket(TABLE_ID, partition1Id, 0),
                        new TableBucket(TABLE_ID, partition2Id, 0));

        // Verify DV data passed correct partition IDs
        for (TableBucket fetchedBucket : admin.dvFetchedBuckets) {
            assertThat(fetchedBucket.getPartitionId()).isNotNull();
        }
    }

    // ---- Helper methods ----

    private LakeSnapshotAndFlussLogSplit findHybridSplitForBucket(
            List<SourceSplitBase> splits, int bucketId) {
        for (SourceSplitBase split : splits) {
            if (split.getTableBucket().getBucket() == bucketId
                    && split instanceof LakeSnapshotAndFlussLogSplit) {
                return (LakeSnapshotAndFlussLogSplit) split;
            }
        }
        throw new IllegalStateException("No hybrid split found for bucket " + bucketId);
    }

    private LakeSnapshotSplit findLakeSnapshotSplitForBucket(
            List<SourceSplitBase> splits, int bucketId) {
        for (SourceSplitBase split : splits) {
            if (split.getTableBucket().getBucket() == bucketId
                    && split instanceof LakeSnapshotSplit) {
                return (LakeSnapshotSplit) split;
            }
        }
        throw new IllegalStateException("No LakeSnapshotSplit found for bucket " + bucketId);
    }

    private LogSplit findLogSplitForBucket(List<SourceSplitBase> splits, int bucketId) {
        for (SourceSplitBase split : splits) {
            if (split.getTableBucket().getBucket() == bucketId && split instanceof LogSplit) {
                return (LogSplit) split;
            }
        }
        throw new IllegalStateException("No LogSplit found for bucket " + bucketId);
    }

    private LakeSplitGenerator createGenerator(
            boolean dvEnabled,
            boolean isPartitioned,
            TestAdmin admin,
            Map<Integer, Long> stoppingOffsets) {
        TableInfo tableInfo = createTableInfo(dvEnabled, isPartitioned);
        TestingLakeSource lakeSource =
                new TestingLakeSource(
                        BUCKET_COUNT,
                        Collections.singletonList(
                                new PartitionInfo(
                                        0L,
                                        new ResolvedPartitionSpec(
                                                Collections.emptyList(), Collections.emptyList()),
                                        null)));

        OffsetsInitializer stoppingInitializer =
                (partitionName, buckets, retriever) -> stoppingOffsets;

        OffsetsInitializer.BucketOffsetsRetriever retriever =
                new OffsetsInitializer.BucketOffsetsRetriever() {
                    @Override
                    public Map<Integer, Long> latestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        return stoppingOffsets;
                    }

                    @Override
                    public Map<Integer, Long> earliestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        return Collections.emptyMap();
                    }

                    @Override
                    public Map<Integer, Long> offsetsFromTimestamp(
                            String partitionName, Collection<Integer> buckets, long timestamp) {
                        return Collections.emptyMap();
                    }
                };

        return new LakeSplitGenerator(
                tableInfo,
                admin,
                lakeSource,
                retriever,
                stoppingInitializer,
                BUCKET_COUNT,
                () -> Collections.emptySet());
    }

    private LakeSplitGenerator createPartitionedGenerator(
            boolean dvEnabled,
            TestAdmin admin,
            List<PartitionInfo> partitionInfos,
            int bucketCount) {
        TableInfo tableInfo = createTableInfo(dvEnabled, true);
        TestingLakeSource lakeSource = new TestingLakeSource(bucketCount, partitionInfos);

        OffsetsInitializer stoppingInitializer =
                (partitionName, buckets, retriever) ->
                        admin.getStoppingOffsetsForPartition(partitionName);

        OffsetsInitializer.BucketOffsetsRetriever retriever =
                new OffsetsInitializer.BucketOffsetsRetriever() {
                    @Override
                    public Map<Integer, Long> latestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        return admin.getStoppingOffsetsForPartition(partitionName);
                    }

                    @Override
                    public Map<Integer, Long> earliestOffsets(
                            String partitionName, Collection<Integer> buckets) {
                        return Collections.emptyMap();
                    }

                    @Override
                    public Map<Integer, Long> offsetsFromTimestamp(
                            String partitionName, Collection<Integer> buckets, long timestamp) {
                        return Collections.emptyMap();
                    }
                };

        return new LakeSplitGenerator(
                tableInfo,
                admin,
                lakeSource,
                retriever,
                stoppingInitializer,
                bucketCount,
                () -> new HashSet<>(partitionInfos));
    }

    private static TableInfo createTableInfo(boolean dvEnabled, boolean isPartitioned) {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING());
        if (isPartitioned) {
            schemaBuilder.column("dt", DataTypes.STRING()).primaryKey("id", "dt");
        } else {
            schemaBuilder.primaryKey("id");
        }

        TableDescriptor.Builder descriptorBuilder =
                TableDescriptor.builder()
                        .schema(schemaBuilder.build())
                        .distributedBy(BUCKET_COUNT, "id")
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, dvEnabled);
        if (isPartitioned) {
            descriptorBuilder.partitionedBy("dt");
        }

        return TableInfo.of(
                TABLE_PATH,
                TABLE_ID,
                0,
                descriptorBuilder.build(),
                null,
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    private static GetDvSnapshotResponse createDvResponse(String filePath, byte[] bitmap) {
        GetDvSnapshotResponse response = new GetDvSnapshotResponse();
        PbLakeDvEntry entry =
                response.addLakeDvEntry().setFilePath(filePath).setDeletedPositionsBitmap(bitmap);
        response.setLogEndOffset(500L);
        response.setSnapshotStartOffset(100L);
        return response;
    }

    /** A mock Admin that tracks DV fetch calls and returns configured responses. */
    private static class TestAdmin extends TestAdminAdapter {
        private final Map<TableBucket, Long> lakeOffsets;
        @Nullable private final Map<Integer, Long> defaultStoppingOffsets;
        private final Map<TableBucket, GetDvSnapshotResponse> dvResponses = new HashMap<>();
        final Set<TableBucket> dvFetchedBuckets = new HashSet<>();

        TestAdmin(
                Map<TableBucket, Long> lakeOffsets,
                @Nullable Map<Integer, Long> defaultStoppingOffsets) {
            this.lakeOffsets = lakeOffsets;
            this.defaultStoppingOffsets = defaultStoppingOffsets;
        }

        void addDvResponse(TableBucket tb, GetDvSnapshotResponse response) {
            dvResponses.put(tb, response);
        }

        protected Map<Integer, Long> getStoppingOffsetsForPartition(String partitionName) {
            return defaultStoppingOffsets != null ? defaultStoppingOffsets : Collections.emptyMap();
        }

        @Override
        public CompletableFuture<LakeSnapshot> getReadableLakeSnapshot(TablePath tablePath) {
            return CompletableFuture.completedFuture(new LakeSnapshot(1L, lakeOffsets));
        }

        @Override
        public CompletableFuture<GetDvSnapshotResponse> getDvSnapshot(
                TablePath tablePath,
                long tableId,
                @Nullable Long partitionId,
                int bucketId,
                long readableSnapshotId) {
            TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
            dvFetchedBuckets.add(tb);
            GetDvSnapshotResponse response = dvResponses.get(tb);
            if (response != null) {
                return CompletableFuture.completedFuture(response);
            }
            // Return empty response
            GetDvSnapshotResponse emptyResponse =
                    new GetDvSnapshotResponse().setLogEndOffset(0L).setSnapshotStartOffset(0L);
            return CompletableFuture.completedFuture(emptyResponse);
        }
    }
}
