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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.PartitionUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.flink.source.split.LogSplit.NO_STOPPING_OFFSET;
import static org.apache.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;

/** A generator for lake splits. */
public class LakeSplitGenerator {

    private final TableInfo tableInfo;
    private final Admin flussAdmin;
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final int bucketCount;
    private final Supplier<Set<PartitionInfo>> listPartitionSupplier;

    private final LakeSource<LakeSplit> lakeSource;

    public LakeSplitGenerator(
            TableInfo tableInfo,
            Admin flussAdmin,
            LakeSource<LakeSplit> lakeSource,
            OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever,
            OffsetsInitializer stoppingOffsetInitializer,
            int bucketCount,
            Supplier<Set<PartitionInfo>> listPartitionSupplier) {
        this.tableInfo = tableInfo;
        this.flussAdmin = flussAdmin;
        this.lakeSource = lakeSource;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.bucketCount = bucketCount;
        this.listPartitionSupplier = listPartitionSupplier;
    }

    /**
     * Return A list of hybrid lake snapshot {@link LakeSnapshotSplit}, {@link
     * LakeSnapshotAndFlussLogSplit} and the corresponding Fluss {@link LogSplit} based on the lake
     * snapshot. Return null if no lake snapshot exists.
     */
    @Nullable
    public List<SourceSplitBase> generateHybridLakeFlussSplits() throws Exception {
        LakeSnapshot lakeSnapshotInfo;
        try {
            lakeSnapshotInfo = flussAdmin.getReadableLakeSnapshot(tableInfo.getTablePath()).get();
        } catch (Exception exception) {
            if (ExceptionUtils.stripExecutionException(exception)
                    instanceof LakeTableSnapshotNotExistException) {
                return null;
            }
            throw exception;
        }

        boolean isLogTable = !tableInfo.hasPrimaryKey();
        boolean isPartitioned = tableInfo.isPartitioned();

        Map<String, Map<Integer, List<LakeSplit>>> lakeSplits =
                groupLakeSplits(
                        lakeSource
                                .createPlanner(
                                        (LakeSource.PlannerContext) lakeSnapshotInfo::getSnapshotId)
                                .plan());

        Map<TableBucket, Long> tableBucketsOffset = lakeSnapshotInfo.getTableBucketsOffset();
        if (isPartitioned) {
            Set<PartitionInfo> partitionInfos = listPartitionSupplier.get();
            Map<Long, String> partitionNameById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getPartitionName));
            return generatePartitionTableSplit(
                    lakeSplits, isLogTable, tableBucketsOffset, partitionNameById);
        } else {
            Map<Integer, List<LakeSplit>> nonPartitionLakeSplits =
                    lakeSplits.isEmpty() ? null : lakeSplits.values().iterator().next();
            // non-partitioned table
            return generateNoPartitionedTableSplit(
                    nonPartitionLakeSplits, isLogTable, tableBucketsOffset);
        }
    }

    private Map<String, Map<Integer, List<LakeSplit>>> groupLakeSplits(List<LakeSplit> lakeSplits) {
        Map<String, Map<Integer, List<LakeSplit>>> result = new HashMap<>();
        for (LakeSplit split : lakeSplits) {
            String partition = String.join(PARTITION_SPEC_SEPARATOR, split.partition());
            int bucket = split.bucket();
            // Get or create the partition group
            Map<Integer, List<LakeSplit>> bucketMap =
                    result.computeIfAbsent(partition, k -> new HashMap<>());
            List<LakeSplit> splitList = bucketMap.computeIfAbsent(bucket, k -> new ArrayList<>());
            splitList.add(split);
        }
        return result;
    }

    private List<SourceSplitBase> generatePartitionTableSplit(
            Map<String, Map<Integer, List<LakeSplit>>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Long, String> partitionNameById) {
        List<SourceSplitBase> splits = new ArrayList<>();
        Map<String, Long> flussPartitionIdByName =
                partitionNameById.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getValue,
                                        Map.Entry::getKey,
                                        (existing, replacement) -> existing,
                                        LinkedHashMap::new));
        long lakeSplitPartitionId = -1L;

        // For PK tables, find the __historical__ partition and pre-fetch its stopping offsets.
        // Expired partitions (lake-only) for PK tables will subscribe to __historical__'s log
        // to get un-tiered updates/deletes.
        Long historicalPartitionId = null;
        for (Map.Entry<String, Long> entry : flussPartitionIdByName.entrySet()) {
            if (PartitionUtils.isHistoricalPartitionName(entry.getKey())) {
                historicalPartitionId = entry.getValue();
                break;
            }
        }
        Map<Integer, Long> historicalBucketEndOffset = null;
        if (historicalPartitionId != null) {
            historicalBucketEndOffset =
                    stoppingOffsetInitializer.getBucketOffsets(
                            partitionNameById.get(historicalPartitionId),
                            IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                            bucketOffsetsRetriever);
        }
        List<String> expiredPartitionsWithLakeData = new ArrayList<>();

        // iterate lake splits
        for (Map.Entry<String, Map<Integer, List<LakeSplit>>> lakeSplitEntry :
                lakeSplits.entrySet()) {
            String partitionName = lakeSplitEntry.getKey();
            Map<Integer, List<LakeSplit>> lakeSplitsOfPartition = lakeSplitEntry.getValue();
            Long partitionId = flussPartitionIdByName.remove(partitionName);
            if (partitionId != null) {
                // mean the partition also exist in fluss partition
                Map<Integer, Long> bucketEndOffset =
                        stoppingOffsetInitializer.getBucketOffsets(
                                partitionName,
                                IntStream.range(0, bucketCount)
                                        .boxed()
                                        .collect(Collectors.toList()),
                                bucketOffsetsRetriever);
                splits.addAll(
                        generateSplit(
                                lakeSplitsOfPartition,
                                partitionId,
                                partitionName,
                                isLogTable,
                                tableBucketSnapshotLogOffset,
                                bucketEndOffset,
                                null));

            } else {
                // only lake data (expired partition)
                if (!isLogTable && historicalPartitionId != null) {
                    // PK table: generate LakeSnapshotAndFlussLogSplit that subscribes to
                    // __historical__ log filtered by this expired partition's key value
                    expiredPartitionsWithLakeData.add(partitionName);
                    splits.addAll(
                            generateExpiredPartitionSplitsForPkTable(
                                    lakeSplitsOfPartition,
                                    partitionName,
                                    lakeSplitPartitionId--,
                                    historicalPartitionId,
                                    tableBucketSnapshotLogOffset,
                                    historicalBucketEndOffset));
                } else {
                    splits.addAll(
                            toLakeSnapshotSplits(
                                    lakeSplitsOfPartition,
                                    partitionName,
                                    // now, we can't get partition id for the partition only
                                    // in lake, set them to a arbitrary partition id, but
                                    // make sure different partition have different partition id
                                    // to enable different partition can be distributed to different
                                    // tasks
                                    lakeSplitPartitionId--));
                }
            }
        }

        // iterate remain fluss splits
        for (Map.Entry<String, Long> partitionIdByNameEntry : flussPartitionIdByName.entrySet()) {
            String partitionName = partitionIdByNameEntry.getKey();
            Long partitionId = partitionIdByNameEntry.getValue();
            Map<Integer, Long> bucketEndOffset =
                    stoppingOffsetInitializer.getBucketOffsets(
                            partitionName,
                            IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                            bucketOffsetsRetriever);
            // For PK tables, if this is the __historical__ partition and there are expired
            // partitions with lake data, set the exclude filter to avoid processing records
            // that are already handled by the per-expired-partition splits.
            List<String> logExcludePartitions = null;
            if (!isLogTable
                    && PartitionUtils.isHistoricalPartitionName(partitionName)
                    && !expiredPartitionsWithLakeData.isEmpty()) {
                logExcludePartitions = expiredPartitionsWithLakeData;
            }
            splits.addAll(
                    generateSplit(
                            null,
                            partitionId,
                            partitionName,
                            isLogTable,
                            tableBucketSnapshotLogOffset,
                            bucketEndOffset,
                            logExcludePartitions));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Integer, Long> bucketEndOffset,
            @Nullable List<String> logExcludePartitions) {
        List<SourceSplitBase> splits = new ArrayList<>();
        if (isLogTable) {
            if (lakeSplits != null) {
                splits.addAll(toLakeSnapshotSplits(lakeSplits, partitionName, partitionId));
            }
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = bucketEndOffset.get(bucket);
                if (snapshotLogOffset == null) {
                    // no data committed to lake for this bucket, scan from fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET || stoppingOffset > 0) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        EARLIEST_OFFSET,
                                        stoppingOffset));
                    }
                } else {
                    // need to read remain fluss log
                    if (stoppingOffset == NO_STOPPING_OFFSET
                            || snapshotLogOffset < stoppingOffset) {
                        splits.add(
                                new LogSplit(
                                        tableBucket,
                                        partitionName,
                                        snapshotLogOffset,
                                        stoppingOffset));
                    }
                }
            }
        } else {
            // it's primary key table
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                Long stoppingOffset = bucketEndOffset.get(bucket);
                splits.add(
                        generateSplitForPrimaryKeyTableBucket(
                                lakeSplits != null ? lakeSplits.get(bucket) : null,
                                tableBucket,
                                partitionName,
                                snapshotLogOffset,
                                stoppingOffset,
                                logExcludePartitions));
            }
        }

        return splits;
    }

    private List<SourceSplitBase> toLakeSnapshotSplits(
            Map<Integer, List<LakeSplit>> lakeSplits,
            @Nullable String partitionName,
            @Nullable Long partitionId) {
        List<SourceSplitBase> splits = new ArrayList<>();
        // we may have multiple table buckets; so we need to
        // introduce an index to make split unique
        int index = 0;
        for (LakeSplit lakeSplit :
                lakeSplits.values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), partitionId, lakeSplit.bucket());
            splits.add(new LakeSnapshotSplit(tableBucket, partitionName, lakeSplit, index++));
        }
        return splits;
    }

    private SourceSplitBase generateSplitForPrimaryKeyTableBucket(
            @Nullable List<LakeSplit> lakeSplits,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset,
            @Nullable List<String> logExcludePartitions) {
        // no snapshot data for this bucket or no a corresponding log offset in this bucket,
        // can only scan from change log
        if (snapshotLogOffset == null || snapshotLogOffset < 0) {
            return new LakeSnapshotAndFlussLogSplit(
                    tableBucket,
                    partitionName,
                    null,
                    EARLIEST_OFFSET,
                    stoppingOffset,
                    0,
                    0,
                    true,
                    null,
                    null,
                    logExcludePartitions);
        }

        return new LakeSnapshotAndFlussLogSplit(
                tableBucket,
                partitionName,
                lakeSplits,
                snapshotLogOffset,
                stoppingOffset,
                0,
                0,
                lakeSplits == null,
                null,
                null,
                logExcludePartitions);
    }

    /**
     * Generate splits for an expired partition (lake-only) in a PK table. Each bucket gets a {@link
     * LakeSnapshotAndFlussLogSplit} that reads Paimon snapshot data and subscribes to the {@code
     * __historical__} partition's log, filtered to only include records matching this expired
     * partition's key value.
     */
    private List<SourceSplitBase> generateExpiredPartitionSplitsForPkTable(
            Map<Integer, List<LakeSplit>> lakeSplitsOfPartition,
            String partitionName,
            long fakePartitionId,
            long historicalPartitionId,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Integer, Long> historicalBucketEndOffset) {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            List<LakeSplit> bucketLakeSplits = lakeSplitsOfPartition.get(bucket);
            TableBucket tableBucket =
                    new TableBucket(tableInfo.getTableId(), fakePartitionId, bucket);

            // Get the tiered offset for this bucket in __historical__
            TableBucket historicalTableBucket =
                    new TableBucket(tableInfo.getTableId(), historicalPartitionId, bucket);
            Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(historicalTableBucket);
            long startingOffset =
                    (snapshotLogOffset != null && snapshotLogOffset >= 0)
                            ? snapshotLogOffset
                            : EARLIEST_OFFSET;

            Long stoppingOffset =
                    historicalBucketEndOffset != null
                            ? historicalBucketEndOffset.get(bucket)
                            : NO_STOPPING_OFFSET;
            if (stoppingOffset == null) {
                stoppingOffset = NO_STOPPING_OFFSET;
            }

            splits.add(
                    new LakeSnapshotAndFlussLogSplit(
                            tableBucket,
                            partitionName,
                            bucketLakeSplits,
                            startingOffset,
                            stoppingOffset,
                            0,
                            0,
                            bucketLakeSplits == null,
                            historicalPartitionId,
                            partitionName,
                            null));
        }
        return splits;
    }

    private List<SourceSplitBase> generateNoPartitionedTableSplit(
            @Nullable Map<Integer, List<LakeSplit>> lakeSplits,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset) {
        // iterate all bucket
        // assume bucket is from 0 to bucket count
        Map<Integer, Long> bucketEndOffset =
                stoppingOffsetInitializer.getBucketOffsets(
                        null,
                        IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                        bucketOffsetsRetriever);
        return generateSplit(
                lakeSplits,
                null,
                null,
                isLogTable,
                tableBucketSnapshotLogOffset,
                bucketEndOffset,
                null);
    }
}
