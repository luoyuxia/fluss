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

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.flink.lake.split.DvAwareLakeSnapshotSplit;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.PositionedRecordReader;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;

/** A scanner for reading one DV-aware lake snapshot split. */
public class DvAwareLakeSnapshotSplitScanner implements BatchScanner {

    private final LakeSource<LakeSplit> lakeSource;
    private final DvAwareLakeSnapshotSplit split;

    @Nullable private CloseableIterator<InternalRow> rowsIterator;

    public DvAwareLakeSnapshotSplitScanner(
            LakeSource<LakeSplit> lakeSource, DvAwareLakeSnapshotSplit split) {
        this.lakeSource = lakeSource;
        this.split = split;
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (rowsIterator == null) {
            RecordReader reader =
                    lakeSource.createRecordReader(
                            (LakeSource.ReaderContext<LakeSplit>) split::getLakeSplit);
            RoaringBitmap deletionVector =
                    DvBitmapUtils.deserializeBitmap(split.getDeletionVector());
            CloseableIterator<LogRecord> records;
            if (deletionVector != null) {
                if (!(reader instanceof PositionedRecordReader)) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Lake reader %s does not support row-position filtering for split %s.",
                                    reader.getClass().getName(), split));
                }
                records =
                        new LakeDvFilterIterator(
                                ((PositionedRecordReader) reader).readWithPosition(),
                                deletionVector);
            } else {
                records = reader.read();
            }
            rowsIterator = InternalRowIterator.wrap(records);
        }
        return rowsIterator.hasNext() ? rowsIterator : null;
    }

    @Override
    public void close() throws IOException {
        if (rowsIterator != null) {
            rowsIterator.close();
        }
    }

    private static class InternalRowIterator implements CloseableIterator<InternalRow> {
        private final CloseableIterator<LogRecord> recordIterator;

        private static InternalRowIterator wrap(CloseableIterator<LogRecord> recordIterator) {
            return new InternalRowIterator(recordIterator);
        }

        private InternalRowIterator(CloseableIterator<LogRecord> recordIterator) {
            this.recordIterator = recordIterator;
        }

        @Override
        public void close() {
            recordIterator.close();
        }

        @Override
        public boolean hasNext() {
            return recordIterator.hasNext();
        }

        @Override
        public InternalRow next() {
            return recordIterator.next().getRow();
        }
    }
}
