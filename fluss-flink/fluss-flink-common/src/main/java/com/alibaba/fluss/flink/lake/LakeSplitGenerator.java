/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.lake;

import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lake.source1.LakeSource;
import com.alibaba.fluss.lake.source1.LakeSplit;
import com.alibaba.fluss.lake.source1.LakeSplitPlanContext;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** A generator for lake splits. */
public class LakeSplitGenerator {

    private final TableInfo tableInfo;
    private final Admin flussAdmin;
    private final OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final int bucketCount;

    private final LakeSource<LakeSplit> lakeSource;

    public LakeSplitGenerator(
            TableInfo tableInfo,
            Admin flussAdmin,
            LakeSource<LakeSplit> lakeSource,
            OffsetsInitializer.BucketOffsetsRetriever bucketOffsetsRetriever,
            OffsetsInitializer stoppingOffsetInitializer,
            int bucketCount) {
        this.tableInfo = tableInfo;
        this.flussAdmin = flussAdmin;
        this.lakeSource = lakeSource;
        this.bucketOffsetsRetriever = bucketOffsetsRetriever;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.bucketCount = bucketCount;
    }

    public List<SourceSplitBase> generateLakeSplits() throws Exception {
        // get the file store
        LakeSnapshot lakeSnapshotInfo =
                flussAdmin.getLatestLakeSnapshot(tableInfo.getTablePath()).get();

        boolean isLogTable = !tableInfo.hasPrimaryKey();
        boolean isPartitioned = tableInfo.isPartitioned();

        if (isPartitioned) {
            List<PartitionInfo> partitionInfos =
                    flussAdmin.listPartitionInfos(tableInfo.getTablePath()).get();
            Map<Long, String> partitionNameById =
                    partitionInfos.stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionInfo::getPartitionId,
                                            PartitionInfo::getPartitionName));
            return generatePartitionTableSplit(
                    lakeSnapshotInfo.getSnapshotId(),
                    isLogTable,
                    lakeSnapshotInfo.getTableBucketsOffset(),
                    partitionNameById);
        } else {
            // non-partitioned table
            return generateNoPartitionedTableSplit(
                    lakeSnapshotInfo.getSnapshotId(),
                    isLogTable,
                    lakeSnapshotInfo.getTableBucketsOffset());
        }
    }

    private List<SourceSplitBase> generatePartitionTableSplit(
            long snapshotId,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Long, String> partitionNameById)
            throws Exception {
        List<SourceSplitBase> splits = new ArrayList<>();
        for (Map.Entry<Long, String> partitionNameByIdEntry : partitionNameById.entrySet()) {
            long partitionId = partitionNameByIdEntry.getKey();
            String partitionName = partitionNameByIdEntry.getValue();
            Map<Integer, Long> bucketEndOffset =
                    stoppingOffsetInitializer.getBucketOffsets(
                            partitionName,
                            IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                            bucketOffsetsRetriever);
            splits.addAll(
                    generateSplit(
                            snapshotId,
                            partitionId,
                            partitionName,
                            isLogTable,
                            tableBucketSnapshotLogOffset,
                            bucketEndOffset));
        }
        return splits;
    }

    private List<SourceSplitBase> generateSplit(
            long snapshotId,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset,
            Map<Integer, Long> bucketEndOffset)
            throws Exception {
        List<SourceSplitBase> splits = new ArrayList<>();
        if (isLogTable) {
            // it's log table, we don't care about bucket, and we can't get bucket in paimon's
            // dynamic bucket; so first generate split for the whole paimon snapshot,
            // then generate log split for each bucket paimon snapshot + fluss log
            splits.addAll(generateSplitForLogSnapshot(snapshotId, partitionId, partitionName));
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket =
                        new TableBucket(tableInfo.getTableId(), partitionId, bucket);
                Long snapshotLogOffset = tableBucketSnapshotLogOffset.get(tableBucket);
                long stoppingOffset = bucketEndOffset.get(bucket);
                if (snapshotLogOffset == null) {
                    // no any data commit to this bucket, scan from fluss log
                    splits.add(
                            new LogSplit(
                                    tableBucket, partitionName, EARLIEST_OFFSET, stoppingOffset));
                } else {
                    // need to read remain fluss log
                    if (snapshotLogOffset < stoppingOffset) {
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
                long stoppingOffset = bucketEndOffset.get(bucket);
                splits.add(
                        generateSplitForPrimaryKeyTableBucket(
                                snapshotId,
                                tableBucket,
                                partitionName,
                                snapshotLogOffset,
                                stoppingOffset));
            }
        }

        return splits;
    }

    private List<SourceSplitBase> generateSplitForLogSnapshot(
            long snapshotId, @Nullable Long partitionId, @Nullable String partitionName)
            throws Exception {
        List<SourceSplitBase> splits = new ArrayList<>();

        ResolvedPartitionSpec resolvedPartitionSpec = null;

        if (partitionName != null) {
            resolvedPartitionSpec =
                    ResolvedPartitionSpec.fromPartitionName(
                            tableInfo.getPartitionKeys(), partitionName);
        }

        LakeSplitPlanContext lakeSplitPlanContext =
                new LakeSplitPlanContext(
                        snapshotId,
                        null,
                        null,
                        resolvedPartitionSpec != null
                                ? Collections.singletonList(resolvedPartitionSpec)
                                : null);

        // for snapshot splits, we always use bucket = -1 ad the bucket since we can't get bucket in
        // paimon's log table since we won't create about bucket
        // snapshot splits + one log split
        List<LakeSplit> lakeSplits = lakeSource.plan(lakeSplitPlanContext);
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), partitionId, -1);
        for (LakeSplit lakeSplit : lakeSplits) {
            splits.add(new LakeSnapshotSplit(tableBucket, partitionName, lakeSplit));
        }
        return splits;
    }

    private SourceSplitBase generateSplitForPrimaryKeyTableBucket(
            long snapshotId,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable Long snapshotLogOffset,
            long stoppingOffset)
            throws IOException {
        // no snapshot data for this bucket or no a corresponding log offset in this bucket,
        // can only scan from change log
        if (snapshotLogOffset == null || snapshotLogOffset < 0) {
            return new PaimonSnapshotAndFlussLogSplit(
                    tableBucket, partitionName, null, EARLIEST_OFFSET, stoppingOffset);
        }

        ResolvedPartitionSpec resolvedPartitionSpec = null;

        if (partitionName != null) {
            resolvedPartitionSpec =
                    ResolvedPartitionSpec.fromPartitionName(
                            tableInfo.getPartitionKeys(), partitionName);
        }

        LakeSplitPlanContext lakeSplitPlanContext =
                new LakeSplitPlanContext(
                        snapshotId,
                        null,
                        tableBucket.getBucket(),
                        resolvedPartitionSpec != null
                                ? Collections.singletonList(resolvedPartitionSpec)
                                : null);

        List<LakeSplit> lakeSplits = lakeSource.plan(lakeSplitPlanContext);

        checkState(lakeSplits.size() == 1, "Splits for primary key table must be 1.");
        return new LakeSnapshotAndFlussLogSplit(
                tableBucket, partitionName, lakeSplits, snapshotLogOffset, stoppingOffset);
    }

    private List<SourceSplitBase> generateNoPartitionedTableSplit(
            long snapshotId,
            boolean isLogTable,
            Map<TableBucket, Long> tableBucketSnapshotLogOffset)
            throws Exception {
        // iterate all bucket
        // assume bucket is from 0 to bucket count
        Map<Integer, Long> bucketEndOffset =
                stoppingOffsetInitializer.getBucketOffsets(
                        null,
                        IntStream.range(0, bucketCount).boxed().collect(Collectors.toList()),
                        bucketOffsetsRetriever);
        return generateSplit(
                snapshotId, null, null, isLogTable, tableBucketSnapshotLogOffset, bucketEndOffset);
    }
}
