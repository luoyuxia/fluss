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

package com.alibaba.fluss.flink.laketiering;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.LakeTableSnapshotNotExistException;
import com.alibaba.fluss.flink.source.split.TablePathLogSplit;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** . */
public class LakeTieringSourceEnumerator
        implements SplitEnumerator<TablePathLogSplit, LakeTieringEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTieringSourceEnumerator.class);

    private final SplitEnumeratorContext<TablePathLogSplit> context;

    private final Configuration flussConf;

    private final Queue<TablePathLogSplit> pendingSplits;
    private Connection connection;
    private Admin flussAdmin;

    private volatile boolean closed = false;

    public LakeTieringSourceEnumerator(
            Configuration flussConfig, SplitEnumeratorContext<TablePathLogSplit> context) {
        this.flussConf = checkNotNull(flussConfig);
        this.context = context;
        this.pendingSplits = new ArrayDeque<>();
    }

    @Override
    public void start() {
        // no resources to start
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();

        // todo: add a thread to periodly send heart beart
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        // todo: uncomment it
        //        SourceSplitBase split = pendingSplits.poll();
        //        if (split != null) {
        //            context.assignSplit(split, subtaskId);
        //        }

        TableBucket tableBucket = new TableBucket(0, 1);

        TablePathLogSplit logSplit =
                new TablePathLogSplit(TablePath.of("test", "test"), tableBucket, null, 0, 1);

        context.assignSplit(logSplit, subtaskId);
    }

    @Override
    public void addSplitsBack(List<TablePathLogSplit> splits, int subtaskId) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public LakeTieringEnumeratorState snapshotState(long checkpointId) throws Exception {
        // currently, we don‘t snapshot any state
        return new LakeTieringEnumeratorState();
    }

    @Override
    public void close() throws IOException {
        // no resources to close
        try {
            closed = true;
            if (flussAdmin != null) {
                flussAdmin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close Flink Source enumerator.", e);
        }
    }

    private void onRequestNewTieringTable(TablePath tablePath, long tableId) throws Exception {
        TableInfo tableInfo = flussAdmin.getTableInfo(tablePath).get();
        int bucketCount = tableInfo.getNumBuckets();
        if (tableInfo.getTableId() != tableId) {
            // the table has been dropped and recreated, not to tier this table
            return;
        }
        Map<TableBucket, Long> tableBucketsOffset = new HashMap<>();
        try {
            tableBucketsOffset =
                    flussAdmin.getLatestLakeSnapshot(tablePath).get().getTableBucketsOffset();
        } catch (LakeTableSnapshotNotExistException e) {
            // do nothing, normal case
        }
        if (tableInfo.isPartitioned()) {
            List<PartitionInfo> partitions = flussAdmin.listPartitionInfos(tablePath).get();
            for (PartitionInfo partitionInfo : partitions) {
                Map<Integer, Long> latestOffsets =
                        latestOffsets(tablePath, partitionInfo.getPartitionName(), bucketCount);
                for (int bucket = 0; bucket < bucketCount; bucket++) {
                    TableBucket tableBucket =
                            new TableBucket(tableId, partitionInfo.getPartitionId(), bucket);
                    long startOffset =
                            tableBucketsOffset.getOrDefault(tableBucket, EARLIEST_OFFSET);
                    long endOffset = latestOffsets.get(bucket);
                    pendingSplits.add(
                            new TablePathLogSplit(
                                    tablePath,
                                    tableBucket,
                                    partitionInfo.getPartitionName(),
                                    startOffset,
                                    endOffset));
                }
            }
        } else {
            Map<Integer, Long> latestOffsets = latestOffsets(tablePath, null, bucketCount);
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                TableBucket tableBucket = new TableBucket(tableId, bucket);
                long startOffset = tableBucketsOffset.getOrDefault(tableBucket, EARLIEST_OFFSET);
                long endOffset = latestOffsets.get(bucket);
                pendingSplits.add(
                        new TablePathLogSplit(
                                tablePath, tableBucket, null, startOffset, endOffset));
            }
        }
    }

    private Map<Integer, Long> latestOffsets(
            TablePath tablePath, @Nullable String partition, int buckets) throws Exception {
        if (partition == null) {
            return flussAdmin
                    .listOffsets(
                            tablePath,
                            IntStream.range(0, buckets).boxed().collect(Collectors.toList()),
                            new OffsetSpec.LatestSpec())
                    .all()
                    .get();
        } else {
            return flussAdmin
                    .listOffsets(
                            tablePath,
                            partition,
                            IntStream.range(0, buckets).boxed().collect(Collectors.toList()),
                            new OffsetSpec.LatestSpec())
                    .all()
                    .get();
        }
    }
}
