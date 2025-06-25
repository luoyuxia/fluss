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

package com.alibaba.fluss.lake.paimon.source;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.InvalidTimestampException;
import com.alibaba.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import com.alibaba.fluss.lake.source.FetchContext;
import com.alibaba.fluss.lake.source.FlussLogSource;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.utils.CloseableIterator;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.IncrementalDeltaStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toChangeType;
import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimonPartitionBinaryRow;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/**
 * An implementation of {@link FlussLogSource} for Paimon to enable paimon data as the Fluss log
 * source.
 */
public class PaimonFlussLogSource implements FlussLogSource {

    private final FileStoreTable fileStoreTable;

    // todo: when support schema evolution, should
    // recalculate offset/timestamp column index
    private final int offsetColumnIndex;
    private final int timestampColumnIndex;
    private final boolean isLogTable;

    public PaimonFlussLogSource(TablePath tablePath, Configuration lakeProperties) {
        CatalogContext catalogContext =
                CatalogContext.create(Options.fromMap(lakeProperties.toMap()));
        try (Catalog catalog = CatalogFactory.createCatalog(catalogContext)) {
            fileStoreTable =
                    (FileStoreTable)
                            catalog.getTable(
                                    Identifier.create(
                                            tablePath.getDatabaseName(), tablePath.getTableName()));
            offsetColumnIndex = fileStoreTable.rowType().getFieldIndex(OFFSET_COLUMN_NAME);
            timestampColumnIndex = fileStoreTable.rowType().getFieldIndex(TIMESTAMP_COLUMN_NAME);
            isLogTable = fileStoreTable.primaryKeys().isEmpty();
        } catch (Exception e) {
            throw new RuntimeException("Fail to get file store table for " + tablePath, e);
        }
    }

    @Override
    public long lookupLogOffsetByTimeStamp(
            @Nullable String partitionName, int bucket, long timestamp, long lakeSnapshotId)
            throws InvalidTimestampException {
        return 0;
        // todo
    }

    @Override
    public CloseableIterator<LogRecord> fetchLogRecords(FetchContext context) throws IOException {
        if (fileStoreTable.bucketMode() == BucketMode.HASH_FIXED) {
            return fetchForBucketAware(
                    context.partitionName(),
                    context.bucket(),
                    fileStoreTable,
                    context.lakeSnapshotId(),
                    context.fetchStartOffset());
        } else if (fileStoreTable.bucketMode() == BucketMode.BUCKET_UNAWARE) {
            return fetchForBucketUnAware(context);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported bucket mode." + fileStoreTable.bucketMode());
        }
    }

    private static int getColumnIndex(FileStoreTable table, String columnName) {
        return table.schema().fieldNames().indexOf(columnName);
    }

    private CloseableIterator<LogRecord> fetchForBucketUnAware(FetchContext context)
            throws IOException {
        RecordReader<InternalRow> reader =
                createReader(
                        context.partitionName(),
                        context.bucket(),
                        fileStoreTable,
                        context.lakeSnapshotId(),
                        context.fetchStartOffset());
        PaimonRecords paimonRecords = new PaimonRecords(reader);
        return new PaimonRecordsWithTargetEndOffset(
                paimonRecords,
                context.logEndOffsetOfSnapshot(),
                context.fetchStartOffset(),
                context);
    }

    private CloseableIterator<LogRecord> fetchForBucketAware(
            @Nullable String partition,
            int bucket,
            FileStoreTable fileStoreTable,
            long endSnapshotId,
            long fetchStartOffset)
            throws IOException {
        RecordReader<InternalRow> reader =
                createReader(partition, bucket, fileStoreTable, endSnapshotId, fetchStartOffset);
        return new PaimonRecords(reader);
    }

    private static RecordReader<InternalRow> createReader(
            @Nullable String partition,
            int bucket,
            FileStoreTable fileStoreTable,
            long endSnapshotId,
            long fetchStartOffset)
            throws IOException {
        // only scan the data files that log offsets is greater or equal to fetchStartOffset
        Predicate predicate =
                new PredicateBuilder(fileStoreTable.rowType())
                        .greaterOrEqual(
                                getColumnIndex(fileStoreTable, OFFSET_COLUMN_NAME),
                                fetchStartOffset);
        // only scan given bucket
        SnapshotReader snapshotReader =
                fileStoreTable.newSnapshotReader().withBucket(bucket).withFilter(predicate);
        if (partition != null) {
            snapshotReader.withPartitionFilter(
                    Collections.singletonList(
                            toPaimonPartitionBinaryRow(fileStoreTable.partitionKeys(), partition)));
        }

        // plan the files that start from fetchStartOffset to end snapshot id
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        snapshotManager.earliestSnapshot();
        StartingScanner startingScanner =
                IncrementalDeltaStartingScanner.betweenSnapshotIds(
                        snapshotManager.earliestSnapshotId(),
                        endSnapshotId,
                        snapshotManager,
                        ScanMode.DELTA);
        StartingScanner.ScannedResult scannedResult =
                (StartingScanner.ScannedResult) startingScanner.scan(snapshotReader);
        List<Split> splits = scannedResult.plan().splits();

        // read splits
        ReadBuilder tableReadBuilder = fileStoreTable.newReadBuilder().withFilter(predicate);

        return tableReadBuilder.newRead().executeFilter().createReader(splits);
    }

    private class PaimonRecords implements CloseableIterator<LogRecord> {

        private final org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRecordsIterator;

        private PaimonRecords(RecordReader<InternalRow> paimonReader) {
            this.paimonRecordsIterator = paimonReader.toCloseableIterator();
        }

        @Override
        public boolean hasNext() {
            return paimonRecordsIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            InternalRow paimonRow = paimonRecordsIterator.next();
            long timestamp = paimonRow.getLong(timestampColumnIndex);
            long offset = paimonRow.getLong(offsetColumnIndex);
            return new GenericRecord(
                    offset,
                    timestamp,
                    isLogTable ? ChangeType.APPEND_ONLY : toChangeType(paimonRow.getRowKind()),
                    new PaimonRowAsFlussRow(paimonRow));
        }

        @Override
        public void close() {
            try {
                paimonRecordsIterator.close();
            } catch (Exception e) {
                throw new FlussRuntimeException("Fail to close paimonRecordsIterator.", e);
            }
        }
    }

    private class PaimonRecordsWithTargetEndOffset implements CloseableIterator<LogRecord> {

        private PaimonRecords currentPaimonRecords;

        private final long targetLogEndOffset;
        private final FetchContext fetchContext;

        private long expectNextOffset;

        private PaimonRecordsWithTargetEndOffset(
                PaimonRecords currentPaimonRecords,
                long targetLogEndOffset,
                long expectNextOffset,
                FetchContext fetchContext) {
            this.currentPaimonRecords = currentPaimonRecords;
            this.targetLogEndOffset = targetLogEndOffset;
            this.expectNextOffset = expectNextOffset;
            this.fetchContext = fetchContext;
        }

        @Override
        public boolean hasNext() {
            return expectNextOffset < targetLogEndOffset;
        }

        @Override
        public LogRecord next() {
            // if has no next record
            if (!currentPaimonRecords.hasNext()) {
                fetchRecords(expectNextOffset);
            }
            if (!currentPaimonRecords.hasNext()) {
                throw new UnsupportedOperationException("xxxxxx");
            }

            LogRecord currentLog = currentPaimonRecords.next();
            if (currentLog.logOffset() == expectNextOffset) {
                expectNextOffset++;
                return currentLog;
            } else {
                // fetch record again
                fetchRecords(expectNextOffset);
                return next();
            }
        }

        private void fetchRecords(long nextFetchOffset) {
            if (currentPaimonRecords != null) {
                currentPaimonRecords.close();
            }
            // create a new reader to fetch records
            try {
                RecordReader<InternalRow> reader =
                        createReader(
                                fetchContext.partitionName(),
                                fetchContext.bucket(),
                                fileStoreTable,
                                fetchContext.lakeSnapshotId(),
                                nextFetchOffset);
                currentPaimonRecords = new PaimonRecords(reader);
            } catch (IOException e) {
                throw new FlussRuntimeException("Fail to create paimon reader.", e);
            }
        }

        @Override
        public void close() {
            if (currentPaimonRecords != null) {
                currentPaimonRecords.close();
            }
        }
    }
}
