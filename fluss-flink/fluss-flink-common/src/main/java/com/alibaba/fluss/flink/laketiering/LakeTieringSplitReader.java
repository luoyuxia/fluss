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
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.source.reader.BoundedSplitReader;
import com.alibaba.fluss.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.flink.source.split.LogSplit;
import com.alibaba.fluss.flink.source.split.SnapshotSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** The {@link SplitReader} implementation which will read Fluss and write to paimon. */
public class LakeTieringSplitReader<WriteResult>
        implements SplitReader<TableBucketWriteResult<WriteResult>, LogSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTieringSplitReader.class);

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(10000L);

    private final LakeTieringFactory<WriteResult, ?> lakeTieringFactory;

    // the id for the pending tables to be tiered
    private final Queue<Long> pendingTieringTables;
    // the table_id to the pending splits
    private final Map<Long, Set<SourceSplitBase>> pendingTieringSplits;

    private final Map<TableBucket, LakeWriter<WriteResult>> lakeWriters;
    private final Connection connection;

    @Nullable private Long currentTableId;
    @Nullable private TablePath currentTablePath;
    @Nullable private LogScanner currentLogScanner;
    @Nullable private Table currentTable;

    private final Queue<SnapshotSplit> currentPendingSnapshotSplits;
    @Nullable private BoundedSplitReader currentSnapshotSplitReader;
    @Nullable private SnapshotSplit currentSnapshotSplit;

    // map from table bucket to split id
    private final Map<TableBucket, String> currentTableSplitsByBucket;
    private final Map<TableBucket, Long> currentTableStoppingOffsets;
    private final Set<LogSplit> currentTableEmptyLogSplits;

    public LakeTieringSplitReader(
            Configuration flussConf, LakeTieringFactory<WriteResult, ?> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.connection = ConnectionFactory.createConnection(flussConf);
        this.pendingTieringTables = new ArrayDeque<>();
        this.pendingTieringSplits = new HashMap<>();
        this.currentTableStoppingOffsets = new HashMap<>();
        this.currentTableEmptyLogSplits = new HashSet<>();
        this.currentTableSplitsByBucket = new HashMap<>();
        this.lakeWriters = new HashMap<>();
        this.currentPendingSnapshotSplits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> fetch() throws IOException {
        // check empty splits
        if (!currentTableEmptyLogSplits.isEmpty()) {
            LOG.info("Empty split(s) {} finished.", currentTableEmptyLogSplits);
            TableBucketWriteResultWithSplitIds records = forEmptySplits(currentTableEmptyLogSplits);
            currentTableEmptyLogSplits.clear();
            return records;
        }
        checkSplitOrStartNext();

        // may read snapshot firstly
        if (currentSnapshotSplitReader != null) {
            CloseableIterator<RecordAndPos> recordIterator = currentSnapshotSplitReader.readBatch();
            if (recordIterator == null) {
                LOG.info("Split {} is finished", currentSnapshotSplit.splitId());
                return finishCurrentSnapshotSplit();
            } else {
                return forSnapshotSplitRecords(
                        currentSnapshotSplit.getTableBucket(), recordIterator);
            }
        } else {
            if (currentLogScanner != null) {
                ScanRecords scanRecords = currentLogScanner.poll(POLL_TIMEOUT);
                return forLogRecords(scanRecords);
            } else {
                return emptyTableBucketWriteResultWithSplitIds();
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<LogSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }
        for (LogSplit split : splitsChange.splits()) {
            LOG.info("add split {}", split.splitId());
            long tableId = split.getTableBucket().getTableId();
            // the split belongs to the current table
            if (currentTableId != null && currentTableId == tableId) {
                addSplitToCurrentTable(split);
            } else {
                Set<SourceSplitBase> alreadyPendingSplits = pendingTieringSplits.get(tableId);
                if (alreadyPendingSplits != null) {
                    // add to the already pending splits
                    alreadyPendingSplits.add(split);
                } else {
                    Set<SourceSplitBase> pendingSplits = new HashSet<>();
                    pendingSplits.add(split);
                    pendingTieringSplits.put(tableId, pendingSplits);
                    pendingTieringTables.add(tableId);
                }
            }
        }
    }

    private void addSplitToCurrentTable(SourceSplitBase split) {
        this.currentTableSplitsByBucket.put(split.getTableBucket(), split.splitId());
        if (split instanceof SnapshotSplit) {
            this.currentPendingSnapshotSplits.add((SnapshotSplit) split);
        } else if (split instanceof LogSplit) {
            subscribeLog((LogSplit) split);
        }
    }

    private void checkSplitOrStartNext() {
        if (currentSnapshotSplitReader != null) {
            return;
        }

        // may poll next snapshot split to read
        SnapshotSplit nextSnapshotSplit = currentPendingSnapshotSplits.poll();
        if (nextSnapshotSplit != null) {
            Table table = getOrMoveToTable(nextSnapshotSplit);
            currentSnapshotSplit = nextSnapshotSplit;
            currentSnapshotSplitReader =
                    new BoundedSplitReader(
                            table.newScan()
                                    .createBatchScanner(
                                            currentSnapshotSplit.getTableBucket(),
                                            currentSnapshotSplit.getSnapshotId()),
                            0);
            return;
        }

        // use current log scanner to read
        if (currentLogScanner != null) {
            return;
        }

        // may poll next table to read
        Long pendingTableId = pendingTieringTables.poll();
        if (pendingTableId == null) {
            return;
        }

        Set<SourceSplitBase> pendingSplits = pendingTieringSplits.remove(pendingTableId);
        for (SourceSplitBase split : pendingSplits) {
            if (split instanceof SnapshotSplit) {
                this.currentPendingSnapshotSplits.add((SnapshotSplit) split);
            } else if (split instanceof LogSplit) {
                Table table = getOrMoveToTable(split);
                currentLogScanner = table.newScan().createLogScanner();
                subscribeLog((LogSplit) split);
            }
        }
    }

    private Table getOrMoveToTable(SourceSplitBase split) {
        if (currentTable == null) {
            TablePath tablePath = getTablePath(split);
            currentTable = connection.getTable(tablePath);
            currentTablePath = tablePath;
            currentTableId = split.getTableBucket().getTableId();
            LOG.info("Start to tier table {} with table id {}.", currentTablePath, currentTableId);
        }
        return currentTable;
    }

    private RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> forLogRecords(
            ScanRecords scanRecords) throws IOException {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();
        for (TableBucket bucket : scanRecords.buckets()) {
            List<ScanRecord> bucketScanRecords = scanRecords.records(bucket);
            if (bucketScanRecords.isEmpty()) {
                continue;
            }
            // no any stopping offset, just skip handle the records for the bucket
            Long stoppingOffset = currentTableStoppingOffsets.get(bucket);
            if (stoppingOffset == null) {
                continue;
            }
            LakeWriter<WriteResult> lakeWriter = getOrCreateLakeWriter(bucket);
            for (ScanRecord record : bucketScanRecords) {
                lakeWriter.write(record);
            }
            ScanRecord lastRecord = bucketScanRecords.get(bucketScanRecords.size() - 1);
            // has arrived into the end of the split,
            if (lastRecord.logOffset() >= stoppingOffset - 1) {
                currentTableStoppingOffsets.remove(bucket);
                if (bucket.getPartitionId() != null) {
                    currentLogScanner.unsubscribe(bucket.getPartitionId(), bucket.getBucket());
                } else {
                    // todo: should unsubscribe the log split if unsubscribe bucket for
                    // un-partitioned table is supported
                }
                // put write result of the bucket
                writeResults.put(bucket, completeLakeWriter(bucket));
                String currentSplitId = currentTableSplitsByBucket.remove(bucket);
                // put split of the bucket
                finishedSplitIds.put(bucket, currentSplitId);
                LOG.info("Split {} has been finished.", currentSplitId);
            }
        }

        if (!finishedSplitIds.isEmpty()) {
            mayFinishCurrentTable();
        }

        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    private LakeWriter<WriteResult> getOrCreateLakeWriter(TableBucket bucket) throws IOException {
        LakeWriter<WriteResult> lakeWriter = lakeWriters.get(bucket);
        if (lakeWriter == null) {
            lakeWriter =
                    lakeTieringFactory.createLakeWriter(
                            new LakeTieringWriterInitContext(currentTablePath, bucket));
            lakeWriters.put(bucket, lakeWriter);
        }
        return lakeWriter;
    }

    private TableBucketWriteResult<WriteResult> completeLakeWriter(TableBucket bucket)
            throws IOException {
        LakeWriter<WriteResult> lakeWriter = lakeWriters.remove(bucket);
        WriteResult writeResult = lakeWriter.complete();
        lakeWriter.close();
        return toTableBucketWriteResult(currentTablePath, bucket, writeResult);
    }

    private TableBucketWriteResultWithSplitIds forEmptySplits(Set<LogSplit> emptySplits) {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();
        for (LogSplit logSplit : emptySplits) {
            TableBucket tableBucket = logSplit.getTableBucket();
            finishedSplitIds.put(tableBucket, logSplit.splitId());
            writeResults.put(
                    tableBucket,
                    toTableBucketWriteResult(getTablePath(logSplit), tableBucket, null));
        }
        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    private void mayFinishCurrentTable() throws IOException {
        // no any pending splits for the table, just finish the table
        if (currentTableSplitsByBucket.isEmpty()) {
            LOG.info(
                    "Finish tier current table {} of table id {}.",
                    currentTablePath,
                    currentTableId);
            finishCurrentTable();
        }
    }

    private TableBucketWriteResultWithSplitIds finishCurrentSnapshotSplit() throws IOException {
        TableBucket tableBucket = currentSnapshotSplit.getTableBucket();
        String splitId = currentTableSplitsByBucket.remove(tableBucket);
        closeCurrentSnapshotSplit();
        mayFinishCurrentTable();
        return new TableBucketWriteResultWithSplitIds(
                Collections.singletonMap(tableBucket, completeLakeWriter(tableBucket)),
                Collections.singletonMap(tableBucket, splitId));
    }

    private TableBucketWriteResultWithSplitIds forSnapshotSplitRecords(
            TableBucket bucket, CloseableIterator<RecordAndPos> recordIterator) throws IOException {
        LakeWriter<WriteResult> lakeWriter = getOrCreateLakeWriter(bucket);
        while (recordIterator.hasNext()) {
            ScanRecord scanRecord = recordIterator.next().record();
            lakeWriter.write(scanRecord);
        }
        recordIterator.close();
        return emptyTableBucketWriteResultWithSplitIds();
    }

    private TableBucketWriteResultWithSplitIds emptyTableBucketWriteResultWithSplitIds() {
        return new TableBucketWriteResultWithSplitIds();
    }

    private void closeCurrentSnapshotSplit() throws IOException {
        try {
            currentSnapshotSplitReader.close();
        } catch (Exception e) {
            throw new IOException("Fail to close current snapshot split.", e);
        }
        currentSnapshotSplitReader = null;
        currentSnapshotSplit = null;
    }

    private void finishCurrentTable() throws IOException {
        try {
            if (currentLogScanner != null) {
                currentLogScanner.close();
                currentLogScanner = null;
            }

            if (currentSnapshotSplitReader != null) {
                currentSnapshotSplitReader.close();
                currentSnapshotSplitReader = null;
            }

            if (currentTable != null) {
                currentTable.close();
                currentTable = null;
            }
        } catch (Exception e) {
            throw new IOException("Fail to finish current table.", e);
        }

        // before switch to a new table, mark all as empty or null
        currentTableId = null;
        currentTablePath = null;
        currentPendingSnapshotSplits.clear();
        currentTableStoppingOffsets.clear();
        currentTableEmptyLogSplits.clear();
        currentTableSplitsByBucket.clear();
    }

    @Override
    public void wakeUp() {
        if (currentLogScanner != null) {
            currentLogScanner.wakeup();
        }
    }

    @Override
    public void close() throws Exception {
        if (currentLogScanner != null) {
            currentLogScanner.close();
        }
        if (currentTable != null) {
            currentTable.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    private void subscribeLog(LogSplit logSplit) {
        // assign bucket offset dynamically
        TableBucket tableBucket = logSplit.getTableBucket();
        long stoppingOffset =
                logSplit.getStoppingOffset()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "stopping offset should not be null"));
        long startingOffset = logSplit.getStartingOffset();
        if (startingOffset >= stoppingOffset) {
            currentTableEmptyLogSplits.add(logSplit);
        } else if (stoppingOffset >= 0) {
            currentTableStoppingOffsets.put(tableBucket, stoppingOffset);
        } else {
            throw new FlinkRuntimeException(
                    String.format(
                            "Invalid stopping offset %d for bucket %s",
                            stoppingOffset, tableBucket));
        }

        Long partitionId = tableBucket.getPartitionId();
        int bucket = tableBucket.getBucket();
        checkNotNull(currentLogScanner, "current log scanner shouldn't be null.");
        if (partitionId != null) {
            currentLogScanner.subscribe(partitionId, bucket, startingOffset);
        } else {
            // If no partition id, subscribe by bucket only.
            currentLogScanner.subscribe(bucket, startingOffset);
        }
        LOG.info(
                "Subscribe to read log for split {} from starting offset {} to end offset {}.",
                logSplit.splitId(),
                startingOffset,
                stoppingOffset);
    }

    private TablePath getTablePath(SourceSplitBase split) {
        //
        return TablePath.of("test", "test");
    }

    private TableBucketWriteResult<WriteResult> toTableBucketWriteResult(
            TablePath tablePath, TableBucket tableBucket, @Nullable WriteResult writeResult) {
        return new TableBucketWriteResult<>(tablePath, tableBucket, writeResult);
    }

    private class TableBucketWriteResultWithSplitIds
            implements RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> {

        private final Iterator<TableBucket> bucketIterator;

        private final Map<TableBucket, TableBucketWriteResult<WriteResult>> bucketWriteResults;
        private final Map<TableBucket, String> bucketSplits;

        @Nullable private TableBucketWriteResult<WriteResult> writeResultForCurrentSplit;

        public TableBucketWriteResultWithSplitIds() {
            this(Collections.emptyMap(), Collections.emptyMap());
        }

        public TableBucketWriteResultWithSplitIds(
                Map<TableBucket, TableBucketWriteResult<WriteResult>> bucketWriteResults,
                Map<TableBucket, String> bucketSplits) {
            this.bucketIterator = bucketWriteResults.keySet().iterator();
            this.bucketWriteResults = bucketWriteResults;
            this.bucketSplits = bucketSplits;
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (bucketIterator.hasNext()) {
                TableBucket currentBucket = bucketIterator.next();
                writeResultForCurrentSplit = bucketWriteResults.get(currentBucket);
                return bucketSplits.get(currentBucket);
            } else {
                writeResultForCurrentSplit = null;
                return null;
            }
        }

        @Nullable
        @Override
        public TableBucketWriteResult<WriteResult> nextRecordFromSplit() {
            if (writeResultForCurrentSplit != null) {
                TableBucketWriteResult<WriteResult> bucketWriteResult = writeResultForCurrentSplit;
                writeResultForCurrentSplit = null;
                return bucketWriteResult;
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return new HashSet<>(bucketSplits.values());
        }
    }
}
