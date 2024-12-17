/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.metrics.ScannerMetricGroup;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.exception.CorruptMessageException;
import com.alibaba.fluss.exception.CorruptRecordException;
import com.alibaba.fluss.exception.FetchException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.InternalRow.FieldGetter;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;
import com.alibaba.fluss.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * {@link CompletedFetch} represents the result that was returned from the tablet server via a
 * {@link FetchLogRequest}, which can be a {@link LogRecordBatch} or remote log segments path. It
 * contains logic to maintain state between calls to {@link #fetchRecords(int)}.
 */
@Internal
abstract class CompletedFetch {
    static final Logger LOG = LoggerFactory.getLogger(CompletedFetch.class);

    final TableBucket tableBucket;
    final ApiError error;
    final int sizeInBytes;
    final long highWatermark;

    private final boolean isCheckCrcs;
    private final Iterator<LogRecordBatch> batches;
    private final LogScannerStatus logScannerStatus;
    private final LogRecordReadContext readContext;
    @Nullable protected final Projection projection;
    protected final InternalRow.FieldGetter[] fieldGetters;

    private final ScannerMetricGroup scannerMetricGroup;

    private LogRecordBatch currentBatch;
    private LogRecord lastRecord;
    private CloseableIterator<LogRecord> records;
    private int recordsRead = 0;
    private Exception cachedRecordException = null;
    private boolean corruptLastRecord = false;
    private long nextFetchOffset;
    private boolean isConsumed = false;
    private boolean initialized = false;
    private boolean ignoreCrcs;

    public CompletedFetch(
            TableBucket tableBucket,
            ApiError error,
            int sizeInBytes,
            long highWatermark,
            Iterator<LogRecordBatch> batches,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrcs,
            boolean ignoreCrcs,
            long fetchOffset,
            @Nullable Projection projection,
            ScannerMetricGroup scannerMetricGroup) {
        this.tableBucket = tableBucket;
        this.error = error;
        this.sizeInBytes = sizeInBytes;
        this.highWatermark = highWatermark;
        this.batches = batches;
        this.readContext = readContext;
        this.isCheckCrcs = isCheckCrcs;
        this.ignoreCrcs = ignoreCrcs;
        this.logScannerStatus = logScannerStatus;
        this.projection = projection;
        this.nextFetchOffset = fetchOffset;
        RowType rowType = readContext.getRowType();
        this.fieldGetters = new FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(rowType.getChildren().get(i), i);
        }
        this.scannerMetricGroup = scannerMetricGroup;
    }

    protected abstract ScanRecord toScanRecord(LogRecord record);

    boolean isConsumed() {
        return isConsumed;
    }

    boolean isInitialized() {
        return initialized;
    }

    long nextFetchOffset() {
        return nextFetchOffset;
    }

    void setInitialized() {
        this.initialized = true;
    }

    /**
     * Draining a {@link CompletedFetch} will signal that the data has been consumed and the
     * underlying resources are closed. This is somewhat analogous to {@link Closeable#close()
     * closing}, though no error will result if a caller invokes {@link #fetchRecords(int)}; an
     * empty {@link List list} will be returned instead.
     */
    void drain() {
        if (!isConsumed) {
            maybeCloseRecordStream();
            cachedRecordException = null;
            isConsumed = true;

            // we move the bucket to the end if we received some bytes.
            if (recordsRead > 0) {
                logScannerStatus.moveBucketToEnd(tableBucket);
            }
        }
    }

    /**
     * The {@link LogRecordBatch batch} of {@link LogRecord records} is converted to a {@link List
     * list} of {@link ScanRecord scan records} and returned.
     *
     * @param maxRecords The number of records to return; the number returned may be {@code 0 <=
     *     maxRecords}
     * @return {@link ScanRecord scan records}
     */
    public List<ScanRecord> fetchRecords(int maxRecords) {
        if (corruptLastRecord) {
            if (ignoreCrcs) {
                LOG.warn("Received corrupt record from {}.", tableBucket);
                scannerMetricGroup.corruptRecords().inc();
                return Collections.emptyList();
            } else {
                throw new FetchException(
                        "Received exception when fetching the next record from "
                                + tableBucket
                                + ". If needed, please back to past the record to continue scanning.",
                        cachedRecordException);
            }
        }

        if (isConsumed) {
            return Collections.emptyList();
        }

        List<ScanRecord> scanRecords = new ArrayList<>();
        try {
            for (int i = 0; i < maxRecords; i++) {
                // Only move to next record if there was no exception in the last fetch.
                if (cachedRecordException == null) {
                    corruptLastRecord = true;
                    lastRecord = nextFetchedRecord();
                    corruptLastRecord = false;
                }

                if (lastRecord == null) {
                    break;
                }

                ScanRecord record = toScanRecord(lastRecord);
                scanRecords.add(record);
                recordsRead++;
                nextFetchOffset = lastRecord.logOffset() + 1;
                cachedRecordException = null;
            }
        } catch (Exception e) {
            if (e instanceof CorruptMessageException && ignoreCrcs) {
                LOG.warn("Received corrupt record from {}.", tableBucket, e);
                scannerMetricGroup.corruptRecords().inc();
            } else {
                cachedRecordException = e;
                if (scanRecords.isEmpty()) {
                    throw new FetchException(
                            "Received exception when fetching the next record from "
                                    + tableBucket
                                    + ". If needed, please back to past the record to continue scanning.",
                            e);
                }
            }
        }

        return scanRecords;
    }

    private LogRecord nextFetchedRecord() throws Exception {
        while (true) {
            if (records == null || !records.hasNext()) {
                maybeCloseRecordStream();

                if (!batches.hasNext()) {
                    // In batch, we preserve the last offset in a batch. By using the next offset
                    // computed from the last offset in the batch, we ensure that the offset of the
                    // next fetch will point to the next batch, which avoids unnecessary re-fetching
                    // of the same batch (in the worst case, the scanner could get stuck fetching
                    // the same batch repeatedly).
                    if (currentBatch != null) {
                        nextFetchOffset = currentBatch.nextLogOffset();
                    }
                    drain();
                    return null;
                }

                currentBatch = batches.next();
                // TODO get last epoch.
                maybeEnsureValid(currentBatch);

                records = currentBatch.records(readContext);
            } else {
                LogRecord record = records.next();
                // skip any records out of range.
                if (record.logOffset() >= nextFetchOffset) {
                    return record;
                }
            }
        }
    }

    private void maybeEnsureValid(LogRecordBatch batch) {
        if (isCheckCrcs) {
            if (projection != null) {
                LOG.debug("Skipping CRC check for column projected log record batch.");
                return;
            }
            try {
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                throw new FetchException(
                        "Record batch for bucket "
                                + tableBucket
                                + " at offset "
                                + batch.baseLogOffset()
                                + " is invalid, cause: "
                                + e.getMessage());
            }
        }
    }

    private void maybeCloseRecordStream() {
        if (records != null) {
            // release underlying resources
            records.close();
            records = null;
        }
    }
}
