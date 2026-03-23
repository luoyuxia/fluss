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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.flink.source.reader.BoundedSplitReader;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.tiering.source.split.TieringLogSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSnapshotSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSplit;
import org.apache.fluss.flink.utils.LakeSourceUtils;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.LakeTieringTaskType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** The {@link SplitReader} implementation which will read Fluss and write to lake. */
public class TieringSplitReader<WriteResult>
        implements SplitReader<TableBucketWriteResult<WriteResult>, TieringSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSplitReader.class);

    public static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(10_000L);

    // unknown bucket timestamp for empty split or snapshot split
    private static final long UNKNOWN_BUCKET_TIMESTAMP = -1;

    // unknown bucket offset for empty split or snapshot split
    private static final long UNKNOWN_BUCKET_OFFSET = -1;

    private final LakeTieringFactory<WriteResult, ?> lakeTieringFactory;

    private final Duration pollTimeout;

    // the id for the pending tables to be tiered
    private final Queue<Long> pendingTieringTables;
    // the table_id to the pending splits
    private final Map<Long, Set<TieringSplit>> pendingTieringSplits;

    private final Set<Long> reachTieringMaxDurationTables;

    private final Map<TableBucket, LakeWriter<WriteResult>> lakeWriters;
    private final Connection connection;

    @Nullable private Long currentTableId;
    @Nullable private TablePath currentTablePath;
    @Nullable private LakeTieringTaskType currentTableTaskType;
    @Nullable private LogScanner currentLogScanner;
    @Nullable private Table currentTable;

    private final Queue<TieringSnapshotSplit> currentPendingSnapshotSplits;
    @Nullable private BoundedSplitReader currentSnapshotSplitReader;
    @Nullable private TieringSnapshotSplit currentSnapshotSplit;
    @Nullable private Integer currentTableNumberOfSplits;

    // map from table bucket to split id
    private final Map<TableBucket, TieringSplit> currentTableSplitsByBucket;
    private final Map<TableBucket, Long> currentTableStoppingOffsets;

    private final Map<TableBucket, LogOffsetAndTimestamp> currentTableTieredOffsetAndTimestamp;

    private final Set<TieringSplit> currentEmptySplits;

    public TieringSplitReader(
            Connection connection, LakeTieringFactory<WriteResult, ?> lakeTieringFactory) {
        this(connection, lakeTieringFactory, DEFAULT_POLL_TIMEOUT);
    }

    @VisibleForTesting
    protected TieringSplitReader(
            Connection connection,
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory,
            Duration pollTimeout) {
        this.lakeTieringFactory = lakeTieringFactory;
        // owned by TieringSourceReader
        this.connection = connection;
        this.pendingTieringTables = new ArrayDeque<>();
        this.pendingTieringSplits = new HashMap<>();
        this.currentTableStoppingOffsets = new HashMap<>();
        this.currentTableTieredOffsetAndTimestamp = new HashMap<>();
        this.currentEmptySplits = new HashSet<>();
        this.currentTableSplitsByBucket = new HashMap<>();
        this.lakeWriters = new HashMap<>();
        this.currentPendingSnapshotSplits = new ArrayDeque<>();
        this.reachTieringMaxDurationTables = new HashSet<>();
        this.pollTimeout = pollTimeout;
    }

    @Override
    public RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> fetch() throws IOException {
        // check empty splits
        if (!currentEmptySplits.isEmpty()) {
            LOG.info("Empty split(s) {} finished.", currentEmptySplits);
            TableBucketWriteResultWithSplitIds records = forEmptySplits(currentEmptySplits);
            currentEmptySplits.forEach(
                    split -> currentTableSplitsByBucket.remove(split.getTableBucket()));
            mayFinishCurrentTable();
            currentEmptySplits.clear();
            return records;
        }
        checkSplitOrStartNext();

        // may read snapshot firstly
        if (currentSnapshotSplitReader != null) {
            // for snapshot split, we don't force to complete it
            // since we rely on the log offset for the snapshot to
            // do next tiering, if force to complete, we can't get the log offset
            CloseableIterator<RecordAndPos> recordIterator = currentSnapshotSplitReader.readBatch();
            if (recordIterator == null) {
                LOG.info("Split {} is finished", currentSnapshotSplit.splitId());
                return finishCurrentSnapshotSplit();
            } else {
                return forSnapshotSplitRecords(
                        currentSnapshotSplit.getTableBucket(), recordIterator);
            }
        } else if (currentSnapshotSplit != null
                && currentSnapshotSplit.getTaskType() == LakeTieringTaskType.BOOTSTRAP_UPGRADE) {
            return forBootstrapSnapshotSplit(currentSnapshotSplit);
        } else {
            if (currentLogScanner != null) {
                // force to complete records
                if (reachTieringMaxDurationTables.contains(currentTableId)) {
                    return forceCompleteTieringLogRecords();
                }
                ScanRecords scanRecords = currentLogScanner.poll(pollTimeout);
                return forLogRecords(scanRecords);
            } else {
                return emptyTableBucketWriteResultWithSplitIds();
            }
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TieringSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }
        for (TieringSplit split : splitsChange.splits()) {
            LOG.info("add split {}", split.splitId());
            if (split.shouldSkipCurrentRound()) {
                // if the split is forced to ignore,
                // mark it as empty
                LOG.info(
                        "ignore split {} since the split is set to skip the current round of tiering.",
                        split.splitId());
                currentEmptySplits.add(split);
                continue;
            }
            long tableId = split.getTableBucket().getTableId();
            // the split belongs to the current table
            if (currentTableId != null && currentTableId == tableId) {
                addSplitToCurrentTable(split);
            } else {
                Set<TieringSplit> alreadyPendingSplits = pendingTieringSplits.get(tableId);
                if (alreadyPendingSplits != null) {
                    // add to the already pending splits
                    alreadyPendingSplits.add(split);
                } else {
                    Set<TieringSplit> pendingSplits = new HashSet<>();
                    pendingSplits.add(split);
                    pendingTieringSplits.put(tableId, pendingSplits);
                    pendingTieringTables.add(tableId);
                }
            }
        }
    }

    private void addSplitToCurrentTable(TieringSplit split) {
        if (currentTableTaskType != null && currentTableTaskType != split.getTaskType()) {
            throw new IllegalStateException(
                    String.format(
                            "Mixed task types are not allowed in one table round: current=%s, incoming=%s, table=%s.",
                            currentTableTaskType, split.getTaskType(), split.getTablePath()));
        }
        this.currentTableSplitsByBucket.put(split.getTableBucket(), split);
        if (split.isTieringSnapshotSplit()) {
            this.currentPendingSnapshotSplits.add((TieringSnapshotSplit) split);
        } else if (split.isTieringLogSplit()) {
            subscribeLog((TieringLogSplit) split);
        }
    }

    private void checkSplitOrStartNext() {
        if (currentSnapshotSplitReader != null) {
            return;
        }

        // may poll next snapshot split to read
        TieringSnapshotSplit nextSnapshotSplit = currentPendingSnapshotSplits.poll();
        if (nextSnapshotSplit != null) {
            Table table = getOrMoveToTable(nextSnapshotSplit);
            currentSnapshotSplit = nextSnapshotSplit;
            if (nextSnapshotSplit.getTaskType() == LakeTieringTaskType.BOOTSTRAP_UPGRADE) {
                // Bootstrap-upgrade snapshot split reads from lake snapshot source directly.
                return;
            }
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

        Set<TieringSplit> pendingSplits = pendingTieringSplits.remove(pendingTableId);
        for (TieringSplit split : pendingSplits) {
            getOrMoveToTable(split);
            addSplitToCurrentTable(split);
        }
    }

    private Table getOrMoveToTable(TieringSplit split) {
        if (currentTable == null) {
            TablePath tablePath = split.getTablePath();
            currentTable = connection.getTable(tablePath);
            currentTablePath = tablePath;
            currentTableId = split.getTableBucket().getTableId();
            currentTableTaskType = split.getTaskType();
            currentTableNumberOfSplits = split.getNumberOfSplits();
            TableInfo currentTableInfo = checkNotNull(currentTable).getTableInfo();
            // check currentTable's id for the table path is same with table id of the tiering
            // split, if not, it means the tiering split is for a previous dropped table. let's fail
            // directly
            // todo: we should skip and notify enumerator that the table id is not tiering now
            // instead of fail directly
            checkArgument(
                    currentTableInfo.getTableId() == split.getTableBucket().getTableId(),
                    "The current table id %s for table path %s is different from the table id %s in TieringSplit split.",
                    currentTableInfo.getTableId(),
                    tablePath,
                    split.getTableBucket().getTableId());
            LOG.info("Start to tier table {} with table id {}.", currentTablePath, currentTableId);
        }
        return currentTable;
    }

    private void mayCreateLogScanner() {
        if (currentLogScanner == null) {
            currentLogScanner = checkNotNull(currentTable).newScan().createLogScanner();
        }
    }

    private RecordsWithSplitIds<TableBucketWriteResult<WriteResult>>
            forceCompleteTieringLogRecords() throws IOException {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();

        // force finish all splits
        Iterator<Map.Entry<TableBucket, TieringSplit>> currentTieringSplitsIterator =
                currentTableSplitsByBucket.entrySet().iterator();
        while (currentTieringSplitsIterator.hasNext()) {
            Map.Entry<TableBucket, TieringSplit> entry = currentTieringSplitsIterator.next();
            TableBucket bucket = entry.getKey();
            TieringSplit split = entry.getValue();
            if (split != null && split.isTieringLogSplit()) {
                // get the current offset, timestamp that tiered so far
                LogOffsetAndTimestamp logOffsetAndTimestamp =
                        currentTableTieredOffsetAndTimestamp.get(bucket);
                long logEndOffset =
                        logOffsetAndTimestamp == null
                                ? UNKNOWN_BUCKET_OFFSET
                                // logEndOffset is equal to offset tiered + 1
                                : logOffsetAndTimestamp.logOffset + 1;
                long timestamp =
                        logOffsetAndTimestamp == null
                                ? UNKNOWN_BUCKET_TIMESTAMP
                                : logOffsetAndTimestamp.timestamp;
                TableBucketWriteResult<WriteResult> bucketWriteResult =
                        completeLakeWriter(
                                bucket, split.getPartitionName(), logEndOffset, timestamp);

                if (logEndOffset == UNKNOWN_BUCKET_OFFSET) {
                    // when the log end offset is unknown, the write result must be
                    // null, otherwise, we should throw exception directly to avoid data
                    // inconsistent
                    checkState(
                            bucketWriteResult.writeResult() == null,
                            "bucketWriteResult must be null when log end offset is unknown when tiering "
                                    + split);
                }

                writeResults.put(bucket, bucketWriteResult);
                finishedSplitIds.put(bucket, split.splitId());
                LOG.info(
                        "Split {} is forced to be finished due to tiering reach max duration, "
                                + "write result {}, logEndOffset {}, timestamp {}",
                        split.splitId(),
                        bucketWriteResult,
                        logEndOffset,
                        timestamp);
                currentTieringSplitsIterator.remove();
            }
        }
        reachTieringMaxDurationTables.remove(this.currentTableId);
        mayFinishCurrentTable();
        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    private RecordsWithSplitIds<TableBucketWriteResult<WriteResult>> forLogRecords(
            ScanRecords scanRecords) throws IOException {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();
        LOG.info("for log records to tier table {}.", currentTableId);
        for (TableBucket bucket : scanRecords.buckets()) {
            LOG.info("tiering table bucket {}.", bucket);
            List<ScanRecord> bucketScanRecords = scanRecords.records(bucket);
            if (bucketScanRecords.isEmpty()) {
                continue;
            }
            LOG.info("tiering table bucket is not empty {}.", bucket);
            // no any stopping offset, just skip handle the records for the bucket
            Long stoppingOffset = currentTableStoppingOffsets.get(bucket);
            if (stoppingOffset == null) {
                continue;
            }
            LOG.info("tiering table bucket stoppingOffset is not empty {}.", bucket);
            LakeWriter<WriteResult> lakeWriter =
                    getOrCreateLakeWriter(
                            bucket, currentTableSplitsByBucket.get(bucket).getPartitionName());
            for (ScanRecord record : bucketScanRecords) {
                // if record is less than stopping offset
                if (record.logOffset() < stoppingOffset) {
                    lakeWriter.write(record);
                }
            }
            ScanRecord lastRecord = bucketScanRecords.get(bucketScanRecords.size() - 1);
            currentTableTieredOffsetAndTimestamp.put(
                    bucket,
                    new LogOffsetAndTimestamp(lastRecord.logOffset(), lastRecord.timestamp()));
            // has arrived into the end of the split,
            if (lastRecord.logOffset() >= stoppingOffset - 1) {
                currentTableStoppingOffsets.remove(bucket);
                if (bucket.getPartitionId() != null) {
                    currentLogScanner.unsubscribe(bucket.getPartitionId(), bucket.getBucket());
                } else {
                    // todo: should unsubscribe the log split if unsubscribe bucket for
                    // un-partitioned table is supported
                }
                TieringSplit currentTieringSplit = currentTableSplitsByBucket.remove(bucket);
                String currentSplitId = currentTieringSplit.splitId();
                // put write result of the bucket
                writeResults.put(
                        bucket,
                        completeLakeWriter(
                                bucket,
                                currentTieringSplit.getPartitionName(),
                                stoppingOffset,
                                lastRecord.timestamp()));
                // put split of the bucket
                finishedSplitIds.put(bucket, currentSplitId);
                LOG.info(
                        "Finish tier bucket {} for table {}, split: {}.",
                        bucket,
                        currentTablePath,
                        currentSplitId);
            }
        }

        if (!finishedSplitIds.isEmpty()) {
            mayFinishCurrentTable();
        }

        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    private LakeWriter<WriteResult> getOrCreateLakeWriter(
            TableBucket bucket, @Nullable String partitionName) throws IOException {
        LakeWriter<WriteResult> lakeWriter = lakeWriters.get(bucket);
        if (lakeWriter == null) {
            lakeWriter =
                    lakeTieringFactory.createLakeWriter(
                            new TieringWriterInitContext(
                                    currentTablePath,
                                    bucket,
                                    partitionName,
                                    currentTable.getTableInfo(),
                                    currentTableSplitsByBucket.get(bucket).getTaskType()));
            lakeWriters.put(bucket, lakeWriter);
        }
        return lakeWriter;
    }

    private TableBucketWriteResult<WriteResult> completeLakeWriter(
            TableBucket bucket,
            @Nullable String partitionName,
            long logEndOffset,
            long maxTimestamp)
            throws IOException {
        LakeWriter<WriteResult> lakeWriter = lakeWriters.remove(bucket);
        WriteResult writeResult = null;
        if (lakeWriter != null) {
            writeResult = lakeWriter.complete();
            lakeWriter.close();
        }
        return toTableBucketWriteResult(
                currentTablePath,
                bucket,
                partitionName,
                writeResult,
                logEndOffset,
                maxTimestamp,
                checkNotNull(currentTableNumberOfSplits));
    }

    private TableBucketWriteResultWithSplitIds forEmptySplits(Set<TieringSplit> emptySplits) {
        Map<TableBucket, TableBucketWriteResult<WriteResult>> writeResults = new HashMap<>();
        Map<TableBucket, String> finishedSplitIds = new HashMap<>();
        for (TieringSplit tieringSplit : emptySplits) {
            TableBucket tableBucket = tieringSplit.getTableBucket();
            finishedSplitIds.put(tableBucket, tieringSplit.splitId());
            writeResults.put(
                    tableBucket,
                    toTableBucketWriteResult(
                            tieringSplit.getTablePath(),
                            tableBucket,
                            tieringSplit.getPartitionName(),
                            null,
                            UNKNOWN_BUCKET_OFFSET,
                            UNKNOWN_BUCKET_TIMESTAMP,
                            tieringSplit.getNumberOfSplits()));
        }
        return new TableBucketWriteResultWithSplitIds(writeResults, finishedSplitIds);
    }

    private void mayFinishCurrentTable() throws IOException {
        // no any pending splits for the table, just finish the table
        if (currentTableSplitsByBucket.isEmpty()) {
            finishCurrentTable();
        }
    }

    private TableBucketWriteResultWithSplitIds finishCurrentSnapshotSplit() throws IOException {
        TableBucket tableBucket = currentSnapshotSplit.getTableBucket();
        long logEndOffset = currentSnapshotSplit.getLogOffsetOfSnapshot();
        String splitId = currentTableSplitsByBucket.remove(tableBucket).splitId();
        TableBucketWriteResult<WriteResult> writeResult =
                completeLakeWriter(
                        tableBucket,
                        currentSnapshotSplit.getPartitionName(),
                        logEndOffset,
                        UNKNOWN_BUCKET_TIMESTAMP);
        LOG.info(
                "Finish tier bucket {} for table {}, split: {}.",
                tableBucket,
                currentTablePath,
                splitId);
        closeCurrentSnapshotSplit();
        mayFinishCurrentTable();
        return new TableBucketWriteResultWithSplitIds(
                Collections.singletonMap(tableBucket, writeResult),
                Collections.singletonMap(tableBucket, splitId));
    }

    private TableBucketWriteResultWithSplitIds forSnapshotSplitRecords(
            TableBucket bucket, CloseableIterator<RecordAndPos> recordIterator) throws IOException {
        LakeWriter<WriteResult> lakeWriter =
                getOrCreateLakeWriter(
                        bucket, checkNotNull(currentSnapshotSplit).getPartitionName());
        while (recordIterator.hasNext()) {
            ScanRecord scanRecord = recordIterator.next().record();
            lakeWriter.write(scanRecord);
        }
        recordIterator.close();
        return emptyTableBucketWriteResultWithSplitIds();
    }

    private TableBucketWriteResultWithSplitIds forBootstrapSnapshotSplit(
            TieringSnapshotSplit snapshotSplit) throws IOException {
        LakeSource<LakeSplit> lakeSource = createBootstrapLakeSource();
        BootstrapSstWriter bootstrapSstWriter =
                new BootstrapSstWriter(checkNotNull(currentTable).getTableInfo(), snapshotSplit);
        try {
            Planner<LakeSplit> planner = lakeSource.createPlanner(snapshotSplit::getSnapshotId);
            List<LakeSplit> lakeSplits = planner.plan();
            for (LakeSplit lakeSplit : lakeSplits) {
                if (!matchesBootstrapBucket(snapshotSplit, lakeSplit)) {
                    continue;
                }
                RecordReader recordReader =
                        lakeSource.createRecordReader(
                                (LakeSource.ReaderContext<LakeSplit>) () -> lakeSplit);
                try (CloseableIterator<LogRecord> recordIterator = recordReader.read()) {
                    while (recordIterator.hasNext()) {
                        bootstrapSstWriter.write(recordIterator.next());
                    }
                }
            }
            String bootstrapArtifactPath = bootstrapSstWriter.flush();
            return finishCurrentBootstrapSnapshotSplit(snapshotSplit, bootstrapArtifactPath);
        } catch (Exception e) {
            throw new IOException(
                    String.format(
                            "Failed to process bootstrap snapshot split %s for table %s.",
                            snapshotSplit.splitId(), snapshotSplit.getTablePath()),
                    e);
        }
    }

    private LakeSource<LakeSplit> createBootstrapLakeSource() {
        TableInfo tableInfo = checkNotNull(currentTable).getTableInfo();
        LakeSource<LakeSplit> lakeSource =
                LakeSourceUtils.createLakeSource(
                        checkNotNull(currentTablePath), tableInfo.getCustomProperties().toMap());
        if (lakeSource == null) {
            throw new IllegalStateException(
                    String.format(
                            "Lake source is unavailable for bootstrap-upgrade table %s.",
                            currentTablePath));
        }
        return lakeSource;
    }

    private boolean matchesBootstrapBucket(
            TieringSnapshotSplit snapshotSplit, LakeSplit lakeSplit) {
        if (lakeSplit.bucket() != snapshotSplit.getTableBucket().getBucket()) {
            return false;
        }
        String targetPartitionName = snapshotSplit.getPartitionName();
        if (targetPartitionName == null) {
            return lakeSplit.partition().isEmpty();
        }
        return targetPartitionName.equals(String.join("$", lakeSplit.partition()));
    }

    private TableBucketWriteResultWithSplitIds emptyTableBucketWriteResultWithSplitIds() {
        return new TableBucketWriteResultWithSplitIds();
    }

    private void closeCurrentSnapshotSplit() throws IOException {
        if (currentSnapshotSplitReader == null) {
            currentSnapshotSplit = null;
            return;
        }
        try {
            currentSnapshotSplitReader.close();
        } catch (Exception e) {
            throw new IOException("Fail to close current snapshot split reader.", e);
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
        reachTieringMaxDurationTables.remove(currentTableId);
        // before switch to a new table, mark all as empty or null
        currentTableId = null;
        currentTablePath = null;
        currentTableTaskType = null;
        currentTableNumberOfSplits = null;
        currentPendingSnapshotSplits.clear();
        currentTableStoppingOffsets.clear();
        currentTableTieredOffsetAndTimestamp.clear();
        currentTableSplitsByBucket.clear();
    }

    /**
     * Handle a table reach max tiering duration. This will mark the current table as reaching max
     * duration, and it will be force completed in the next fetch cycle.
     */
    public void handleTableReachTieringMaxDuration(long tableId) {
        LOG.info(
                "handleTableReachTieringMaxDuration, currentTableId: {}, pendingTieringSplits: {}",
                currentTableId,
                pendingTieringSplits);
        if ((currentTableId != null && currentTableId.equals(tableId))
                || pendingTieringSplits.containsKey(tableId)) {
            LOG.info("Table {} reach tiering max duration, will force to complete.", tableId);
            reachTieringMaxDurationTables.add(tableId);
        }
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

        // don't need to close connection, will be closed by TieringSourceReader
    }

    private void subscribeLog(TieringLogSplit logSplit) {
        // assign bucket offset dynamically
        TableBucket tableBucket = logSplit.getTableBucket();
        long stoppingOffset = logSplit.getStoppingOffset();
        long startingOffset = logSplit.getStartingOffset();
        if (startingOffset >= stoppingOffset || stoppingOffset <= 0) {
            currentEmptySplits.add(logSplit);
            return;
        } else {
            currentTableStoppingOffsets.put(tableBucket, stoppingOffset);
        }

        mayCreateLogScanner();
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

    private TableBucketWriteResult<WriteResult> toTableBucketWriteResult(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable WriteResult writeResult,
            long endLogOffset,
            long maxTimestamp,
            int numberOfSplits) {
        return new TableBucketWriteResult<>(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                endLogOffset,
                maxTimestamp,
                numberOfSplits);
    }

    private TableBucketWriteResult<WriteResult> toTableBucketWriteResult(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable WriteResult writeResult,
            long endLogOffset,
            long maxTimestamp,
            int numberOfSplits,
            @Nullable String bootstrapArtifactPath) {
        return new TableBucketWriteResult<>(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                endLogOffset,
                maxTimestamp,
                numberOfSplits,
                bootstrapArtifactPath);
    }

    private TableBucketWriteResultWithSplitIds finishCurrentBootstrapSnapshotSplit(
            TieringSnapshotSplit snapshotSplit, @Nullable String bootstrapArtifactPath)
            throws IOException {
        TableBucket tableBucket = snapshotSplit.getTableBucket();
        String splitId = currentTableSplitsByBucket.remove(tableBucket).splitId();
        TableBucketWriteResult<WriteResult> writeResult =
                toTableBucketWriteResult(
                        snapshotSplit.getTablePath(),
                        tableBucket,
                        snapshotSplit.getPartitionName(),
                        null,
                        snapshotSplit.getLogOffsetOfSnapshot(),
                        UNKNOWN_BUCKET_TIMESTAMP,
                        checkNotNull(currentTableNumberOfSplits),
                        bootstrapArtifactPath);
        LOG.info(
                "Finish bootstrap tier bucket {} for table {}, split: {}, artifact={}.",
                tableBucket,
                currentTablePath,
                splitId,
                bootstrapArtifactPath);
        closeCurrentSnapshotSplit();
        mayFinishCurrentTable();
        return new TableBucketWriteResultWithSplitIds(
                Collections.singletonMap(tableBucket, writeResult),
                Collections.singletonMap(tableBucket, splitId));
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

    private static final class LogOffsetAndTimestamp {

        private final long logOffset;
        private final long timestamp;

        public LogOffsetAndTimestamp(long logOffset, long timestamp) {
            this.logOffset = logOffset;
            this.timestamp = timestamp;
        }
    }

    /** Writes bootstrap-upgrade records into one SST artifact for a split. */
    private static final class BootstrapSstWriter {
        private static final String BOOTSTRAP_SST_OUTPUT_DIR_KEY =
                "table.datalake.bootstrap.sst.output-dir";

        private final TableInfo tableInfo;
        private final TieringSnapshotSplit split;
        private final RowSerializer rowSerializer;
        private final KeyEncoder primaryKeyEncoder;
        private final short schemaId;
        private final Map<ByteArrayWrapper, byte[]> latestValuesByKey;

        private BootstrapSstWriter(TableInfo tableInfo, TieringSnapshotSplit split) {
            if (!tableInfo.hasPrimaryKey()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Bootstrap-upgrade SST generation requires primary-key table, but got %s.",
                                tableInfo.getTablePath()));
            }
            this.tableInfo = tableInfo;
            this.split = split;
            DataType[] rowFieldTypes =
                    tableInfo.getSchema().getColumns().stream()
                            .map(Schema.Column::getDataType)
                            .toArray(DataType[]::new);
            this.rowSerializer =
                    new RowSerializer(rowFieldTypes, BinaryRow.BinaryRowFormat.COMPACTED);
            this.primaryKeyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            tableInfo.getRowType(),
                            tableInfo.getPhysicalPrimaryKeys(),
                            tableInfo.getTableConfig(),
                            tableInfo.isDefaultBucketKey());
            this.schemaId = (short) tableInfo.getSchemaId();
            this.latestValuesByKey = new HashMap<>();
        }

        private void write(LogRecord record) {
            byte[] keyBytes = primaryKeyEncoder.encodeKey(record.getRow());
            ByteArrayWrapper key = new ByteArrayWrapper(keyBytes);
            if (record.getChangeType() == ChangeType.DELETE) {
                latestValuesByKey.remove(key);
                return;
            }
            BinaryRow binaryRow = rowSerializer.toBinaryRow(record.getRow());
            latestValuesByKey.put(key, ValueEncoder.encodeValue(schemaId, binaryRow));
        }

        private @Nullable String flush() throws IOException {
            if (latestValuesByKey.isEmpty()) {
                return null;
            }
            Path sstPath = prepareSstPath();
            RocksDB.loadLibrary();
            try (EnvOptions envOptions = new EnvOptions();
                    Options options = new Options();
                    SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)) {
                sstFileWriter.open(sstPath.toString());
                latestValuesByKey.entrySet().stream()
                        .sorted(
                                (left, right) ->
                                        Arrays.compareUnsigned(
                                                left.getKey().bytes, right.getKey().bytes))
                        .forEach(
                                entry -> {
                                    try {
                                        sstFileWriter.put(entry.getKey().bytes, entry.getValue());
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                sstFileWriter.finish();
            } catch (RuntimeException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception) {
                    throw new IOException("Failed to write bootstrap SST file.", cause);
                }
                throw e;
            } catch (Exception e) {
                throw new IOException("Failed to write bootstrap SST file.", e);
            }
            Path manifestPath = writeManifest(sstPath);
            LOG.info(
                    "Bootstrap SST generated from reader: table={}, bucket={}, partition={}, sstPath={}, manifestPath={}, kvCount={}.",
                    tableInfo.getTablePath(),
                    split.getTableBucket(),
                    split.getPartitionName(),
                    sstPath,
                    manifestPath,
                    latestValuesByKey.size());
            return manifestPath.toString();
        }

        private Path prepareSstPath() throws IOException {
            String outputRoot =
                    tableInfo.getCustomProperties().toMap().get(BOOTSTRAP_SST_OUTPUT_DIR_KEY);
            if (outputRoot == null || outputRoot.trim().isEmpty()) {
                outputRoot =
                        System.getProperty("java.io.tmpdir")
                                + File.separator
                                + "fluss-bootstrap-sst";
            }
            String partitionSegment =
                    sanitizePathSegment(
                            split.getPartitionName() == null
                                    ? "nopartition"
                                    : split.getPartitionName());
            Path outputDir =
                    Path.of(
                            outputRoot,
                            "table-" + split.getTableBucket().getTableId(),
                            "partition-" + partitionSegment,
                            "bucket-" + split.getTableBucket().getBucket());
            Files.createDirectories(outputDir);
            return outputDir.resolve("bootstrap-" + UUID.randomUUID() + ".sst");
        }

        private Path writeManifest(Path sstPath) throws IOException {
            long sstSizeBytes = Files.size(sstPath);
            String sha256 = sha256Hex(sstPath);
            long nowMs = System.currentTimeMillis();
            Path manifestPath =
                    sstPath.resolveSibling(sstPath.getFileName().toString() + ".manifest.json");

            String partition = split.getPartitionName() == null ? "" : split.getPartitionName();
            String json =
                    "{\n"
                            + "  \"version\": 1,\n"
                            + "  \"tableId\": "
                            + split.getTableBucket().getTableId()
                            + ",\n"
                            + "  \"tablePath\": \""
                            + escapeJson(tableInfo.getTablePath().toString())
                            + "\",\n"
                            + "  \"partition\": \""
                            + escapeJson(partition)
                            + "\",\n"
                            + "  \"bucket\": "
                            + split.getTableBucket().getBucket()
                            + ",\n"
                            + "  \"sourceSnapshotId\": "
                            + split.getSnapshotId()
                            + ",\n"
                            + "  \"sstPath\": \""
                            + escapeJson(sstPath.toString())
                            + "\",\n"
                            + "  \"sstSizeBytes\": "
                            + sstSizeBytes
                            + ",\n"
                            + "  \"rowCount\": "
                            + latestValuesByKey.size()
                            + ",\n"
                            + "  \"sha256\": \""
                            + sha256
                            + "\",\n"
                            + "  \"createdAtMs\": "
                            + nowMs
                            + "\n"
                            + "}\n";
            Files.writeString(manifestPath, json);
            return manifestPath;
        }

        private static String sha256Hex(Path file) throws IOException {
            MessageDigest digest;
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IOException("SHA-256 algorithm is unavailable.", e);
            }
            byte[] buffer = new byte[8192];
            try (InputStream in = Files.newInputStream(file);
                    DigestInputStream digestStream = new DigestInputStream(in, digest)) {
                while (digestStream.read(buffer) >= 0) {
                    // read fully for digest
                }
            }
            return toHex(digest.digest());
        }

        private static String toHex(byte[] bytes) {
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                builder.append(Character.forDigit((b >> 4) & 0xF, 16));
                builder.append(Character.forDigit(b & 0xF, 16));
            }
            return builder.toString();
        }

        private static String escapeJson(String value) {
            return value.replace("\\", "\\\\").replace("\"", "\\\"");
        }

        private static String sanitizePathSegment(String value) {
            return value.replace(File.separatorChar, '_').replace('$', '_').replace('=', '_');
        }

        private static final class ByteArrayWrapper {
            private final byte[] bytes;

            private ByteArrayWrapper(byte[] bytes) {
                this.bytes = bytes;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof ByteArrayWrapper)) {
                    return false;
                }
                ByteArrayWrapper that = (ByteArrayWrapper) o;
                return Arrays.equals(bytes, that.bytes);
            }

            @Override
            public int hashCode() {
                return Arrays.hashCode(bytes);
            }
        }
    }
}
