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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.flink.lake.split.DvAwareFlussLogSplit;
import org.apache.fluss.flink.source.reader.ScanRecordIterator;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** A DV-aware bounded log scanner used by batch union read. */
public class DvAwareFlussLogSplitScanner implements BatchScanner {

    private final LogScanner logScanner;
    private final long stoppingOffset;
    private final LogDvFilter logDvFilter;
    private boolean logFinished;

    public DvAwareFlussLogSplitScanner(
            Table table, DvAwareFlussLogSplit split, @Nullable int[] projectedFields)
            throws IOException {
        this.logScanner =
                projectedFields == null
                        ? table.newScan().createLogScanner()
                        : table.newScan().project(projectedFields).createLogScanner();
        this.stoppingOffset =
                split.getStoppingOffset()
                        .orElseThrow(() -> new RuntimeException("Stopping offset is absent."));
        this.logDvFilter =
                new LogDvFilter(DvBitmapUtils.deserializeLogDvSnapshot(split.getLogDvSnapshot()));
        this.logFinished = split.getStartingOffset() >= stoppingOffset || stoppingOffset <= 0;

        TableBucket tableBucket = split.getTableBucket();
        if (tableBucket.getPartitionId() != null) {
            this.logScanner.subscribe(
                    tableBucket.getPartitionId(),
                    tableBucket.getBucket(),
                    split.getStartingOffset());
        } else {
            this.logScanner.subscribe(tableBucket.getBucket(), split.getStartingOffset());
        }
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
        while (!logFinished) {
            List<ScanRecord> rows = new ArrayList<>();
            ScanRecords scanRecords = logScanner.poll(timeout);
            for (ScanRecord scanRecord : scanRecords) {
                if (logDvFilter.shouldEmit(scanRecord)) {
                    rows.add(scanRecord);
                }
                if (scanRecord.logOffset() >= stoppingOffset - 1) {
                    logFinished = true;
                    break;
                }
            }
            if (!rows.isEmpty()) {
                return new ScanRecordRowIterator(rows.iterator());
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        try {
            logScanner.close();
        } catch (Exception e) {
            throw new IOException("Failed to close DV-aware log scanner.", e);
        }
    }

    private static class ScanRecordRowIterator implements ScanRecordIterator {
        private final Iterator<ScanRecord> iterator;

        private ScanRecordRowIterator(Iterator<ScanRecord> iterator) {
            this.iterator = iterator;
        }

        @Override
        public ScanRecord nextScanRecord() {
            ScanRecord record = iterator.next();
            return new ScanRecord(
                    record.logOffset(), record.timestamp(), ChangeType.INSERT, record.getRow());
        }

        @Override
        public void close() {}

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public InternalRow next() {
            return nextScanRecord().getRow();
        }
    }
}
