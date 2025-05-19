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

package com.alibaba.fluss.flink.laketiering.committer;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.laketiering.TableBucketWriteResult;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Flink operator to combine {@link WriteResult} to {@link Committable} which will then be
 * committed to lake & Fluss cluster.
 */
public class LakeTieringCommitterOperator<WriteResult, Committable>
        extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<TableBucketWriteResult<WriteResult>, Committable> {

    private static final long serialVersionUID = 1L;

    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final TableLakeSnapshotCommitter tableLakeSnapshotCommitter;

    private final Map<Long, Map<Long, List<TableBucketWriteResult<WriteResult>>>>
            partitionedTableCollectedResult;
    private final Map<Long, List<TableBucketWriteResult<WriteResult>>>
            nonPartitionedTableCollectedResult;
    private final Map<Long, Integer> bucketNumByTableId;

    public LakeTieringCommitterOperator(
            Configuration flussConf,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.tableLakeSnapshotCommitter = new TableLakeSnapshotCommitter(flussConf);
        this.partitionedTableCollectedResult = new HashMap<>();
        this.nonPartitionedTableCollectedResult = new HashMap<>();
        this.bucketNumByTableId = new HashMap<>();
    }

    @Override
    public void open() {
        tableLakeSnapshotCommitter.open();
    }

    @Override
    public void processElement(StreamRecord<TableBucketWriteResult<WriteResult>> streamRecord)
            throws Exception {
        TableBucketWriteResult<WriteResult> tableBucketWriteResult = streamRecord.getValue();
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        long tableId = tableBucket.getTableId();
        registerTableBucketWriteResult(tableBucketWriteResult);

        // may collect all write results for the table
        boolean isPartitioned = tableBucket.getPartitionId() != null;
        List<TableBucketWriteResult<WriteResult>> committableWriteResults =
                collectTableAllBucketWriteResult(tableId, isPartitioned);

        if (committableWriteResults != null) {
            commitWriteResults(
                    tableId, tableBucketWriteResult.tablePath(), committableWriteResults);
            // clear the table id
            bucketNumByTableId.remove(tableId);
            if (isPartitioned) {
                partitionedTableCollectedResult.remove(tableId);
            } else {
                nonPartitionedTableCollectedResult.remove(tableId);
            }
            // todo: mark the table as tiered finished
        }
    }

    private void commitWriteResults(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter()) {
            List<WriteResult> writeResults =
                    committableWriteResults.stream()
                            .map(TableBucketWriteResult::writeResult)
                            .collect(Collectors.toList());
            // to committable
            Committable committable = lakeCommitter.toCommitable(writeResults);
            // get latest snapshot
            Long latestSnapshot = tableLakeSnapshotCommitter.getLatestLakeSnapshot(tablePath);
            // commit to lake
            // todo: underlying lake should check the latest snapshot of Fluss
            // is less than lake's current snapshot
            long commitedSnapshotId = lakeCommitter.commit(latestSnapshot, committable);
            // commit to fluss
            Map<TableBucket, Long> logEndOffsets = new HashMap<>();
            for (TableBucketWriteResult<WriteResult> writeResult : committableWriteResults) {
                logEndOffsets.put(writeResult.tableBucket(), writeResult.logEndOffset());
            }
            tableLakeSnapshotCommitter.commit(
                    new TableLakeSnapshot(tableId, commitedSnapshotId, logEndOffsets));
        }
    }

    private void registerTableBucketWriteResult(
            TableBucketWriteResult<WriteResult> tableBucketWriteResult) throws Exception {
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        TablePath tablePath = tableBucketWriteResult.tablePath();
        long tableId = tableBucket.getTableId();
        if (!bucketNumByTableId.containsKey(tableId)) {
            TableInfo tableInfo = tableLakeSnapshotCommitter.getTableInfo(tablePath);
            // todo: check whether the table id of the table info is equal to the tableif in the
            // write result
            bucketNumByTableId.put(tableId, tableInfo.getNumBuckets());
        }

        if (tableBucket.getPartitionId() == null) {
            List<TableBucketWriteResult<WriteResult>> tableBucketWriteResults =
                    nonPartitionedTableCollectedResult.computeIfAbsent(
                            tableId, (id) -> new ArrayList<>());
            tableBucketWriteResults.add(tableBucketWriteResult);
        } else {
            Map<Long, List<TableBucketWriteResult<WriteResult>>> writeResultByPartition =
                    partitionedTableCollectedResult.computeIfAbsent(
                            tableId, (id) -> new HashMap<>());
            List<TableBucketWriteResult<WriteResult>> tableBucketWriteResults =
                    writeResultByPartition.computeIfAbsent(
                            tableBucket.getPartitionId(), (partitionId) -> new ArrayList<>());
            tableBucketWriteResults.add(tableBucketWriteResult);
        }
    }

    @Nullable
    private List<TableBucketWriteResult<WriteResult>> collectTableAllBucketWriteResult(
            long tableId, boolean isPartitioned) {
        if (isPartitioned) {
            Map<Long, List<TableBucketWriteResult<WriteResult>>> writeResultByPartition =
                    partitionedTableCollectedResult.get(tableId);
            List<TableBucketWriteResult<WriteResult>> writeResults = new ArrayList<>();
            for (Map.Entry<Long, List<TableBucketWriteResult<WriteResult>>> tableWriteResultEntry :
                    writeResultByPartition.entrySet()) {
                List<TableBucketWriteResult<WriteResult>> partitionWriteResults =
                        collectWriteResult(tableId, tableWriteResultEntry.getValue());
                if (partitionWriteResults != null) {
                    writeResults.addAll(partitionWriteResults);
                } else {
                    return null;
                }
            }
            return writeResults;
        } else {
            return collectWriteResult(tableId, nonPartitionedTableCollectedResult.get(tableId));
        }
    }

    @Nullable
    private List<TableBucketWriteResult<WriteResult>> collectWriteResult(
            long tableId, List<TableBucketWriteResult<WriteResult>> tableBucketWriteResults) {
        Set<TableBucket> collectedBuckets = new HashSet<>();
        List<TableBucketWriteResult<WriteResult>> writeResults = new ArrayList<>();
        for (TableBucketWriteResult<WriteResult> tableBucketWriteResult : tableBucketWriteResults) {
            if (!collectedBuckets.add(tableBucketWriteResult.tableBucket())) {
                // it means the write results contain more than two write result
                // for same table, it shouldn't happen, let's throw exception to
                // avoid unexpected behavior
                throw new IllegalStateException(
                        String.format(
                                "Found more than two write results for same "
                                        + "bucket %s of table %d",
                                tableBucketWriteResult.tableBucket(), tableId));
            }
            writeResults.add(tableBucketWriteResult);
        }
        int bucketCount = bucketNumByTableId.get(tableId);
        if (bucketCount == writeResults.size()) {
            return writeResults;
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        tableLakeSnapshotCommitter.close();
    }
}
