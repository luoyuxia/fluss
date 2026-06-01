/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toChangeType;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/** Record reader for paimon table. */
public class PaimonRecordReader implements RecordReader {

    protected final FileStoreTable fileStoreTable;
    protected final @Nullable PaimonSplit split;
    protected PaimonRowAsFlussRecordIterator iterator;
    protected @Nullable int[][] project;
    protected RowType paimonRowType;

    public PaimonRecordReader(
            FileStoreTable fileStoreTable,
            @Nullable PaimonSplit split,
            @Nullable int[][] project,
            @Nullable Predicate predicate)
            throws IOException {
        this.fileStoreTable = fileStoreTable;
        this.split = split;
        this.project = project;
        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
        RowType paimonFullRowType = fileStoreTable.rowType();
        if (project != null) {
            readBuilder = applyProject(readBuilder, project, paimonFullRowType);
        }

        if (predicate != null) {
            readBuilder.withFilter(predicate);
        }

        TableRead tableRead = readBuilder.newRead().executeFilter();
        paimonRowType = readBuilder.readType();
        if (split == null) {
            iterator =
                    new PaimonRecordReader.PaimonRowAsFlussRecordIterator(
                            org.apache.paimon.utils.CloseableIterator.empty(), paimonRowType);
        } else {
            org.apache.paimon.reader.RecordReader<InternalRow> recordReader =
                    tableRead.createReader(split.dataSplit());
            iterator =
                    new PaimonRecordReader.PaimonRowAsFlussRecordIterator(
                            recordReader.toCloseableIterator(), paimonRowType);
        }
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        return iterator;
    }

    @Override
    public CloseableIterator<RowWithPosResult> readWithPos() throws IOException {
        // Use the caller's projection directly (without extra __offset/__timestamp
        // columns that read() adds via applyProject).
        ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
        if (project != null) {
            int[] projection = Arrays.stream(project).mapToInt(p -> p[0]).toArray();
            readBuilder.withProjection(projection);
        }

        org.apache.paimon.reader.RecordReader<InternalRow> batchReader =
                readBuilder.newRead().createReader(split.dataSplit());
        return new PaimonRowWithPosIterator(batchReader);
    }

    private ReadBuilder applyProject(
            ReadBuilder readBuilder, int[][] projects, RowType paimonFullRowType) {
        int[] projectIds = Arrays.stream(projects).mapToInt(project -> project[0]).toArray();

        int offsetFieldPos = paimonFullRowType.getFieldIndex(OFFSET_COLUMN_NAME);
        int timestampFieldPos = paimonFullRowType.getFieldIndex(TIMESTAMP_COLUMN_NAME);

        int[] paimonProject =
                IntStream.concat(
                                IntStream.of(projectIds),
                                IntStream.of(offsetFieldPos, timestampFieldPos))
                        .toArray();

        return readBuilder.withProjection(paimonProject);
    }

    /**
     * Lazy iterator wrapping Paimon's batch reader, yielding each row with its physical file
     * position.
     *
     * <p>Position comes from {@link FileRecordIterator#returnedPosition()} when the underlying
     * batch iterator implements {@link FileRecordIterator}. Otherwise, a sequential row counter is
     * used as fallback.
     */
    private static class PaimonRowWithPosIterator implements CloseableIterator<RowWithPosResult> {

        private final org.apache.paimon.reader.RecordReader<InternalRow> batchReader;
        private final PaimonRowAsFlussRow rowWrapper = new PaimonRowAsFlussRow();
        private final RowWithPosResult reusable = new RowWithPosResult();

        private org.apache.paimon.reader.RecordReader.RecordIterator<InternalRow> currentBatch;
        private long rowCounter;
        private boolean advanced;
        private boolean hasNext;

        PaimonRowWithPosIterator(org.apache.paimon.reader.RecordReader<InternalRow> batchReader) {
            this.batchReader = batchReader;
        }

        @Override
        public boolean hasNext() {
            if (!advanced) {
                advanceNext();
            }
            return hasNext;
        }

        @Override
        public RowWithPosResult next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            advanced = false;
            return reusable;
        }

        private void advanceNext() {
            advanced = true;
            hasNext = false;
            try {
                while (true) {
                    if (currentBatch != null) {
                        InternalRow row = currentBatch.next();
                        if (row != null) {
                            long position;
                            String fileName = null;
                            if (currentBatch instanceof FileRecordIterator) {
                                FileRecordIterator<?> fileIter =
                                        (FileRecordIterator<?>) currentBatch;
                                position = fileIter.returnedPosition();
                                fileName = fileIter.filePath().getName();
                            } else {
                                position = rowCounter;
                            }
                            rowCounter++;
                            reusable.set(rowWrapper.replaceRow(row), position, fileName);
                            hasNext = true;
                            return;
                        }
                        currentBatch.releaseBatch();
                    }
                    currentBatch = batchReader.readBatch();
                    if (currentBatch == null) {
                        return;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read batch.", e);
            }
        }

        @Override
        public void close() {
            try {
                batchReader.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close batch reader.", e);
            }
        }
    }

    /** Iterator for paimon row as fluss record. */
    public static class PaimonRowAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator;

        private final ProjectedRow projectedRow;
        private final PaimonRowAsFlussRow paimonRowAsFlussRow;

        private final int logOffsetColIndex;
        private final int timestampColIndex;

        public PaimonRowAsFlussRecordIterator(
                org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator,
                RowType paimonRowType) {
            this.paimonRowIterator = paimonRowIterator;
            this.logOffsetColIndex = paimonRowType.getFieldIndex(OFFSET_COLUMN_NAME);
            this.timestampColIndex = paimonRowType.getFieldIndex(TIMESTAMP_COLUMN_NAME);

            int[] project = IntStream.range(0, paimonRowType.getFieldCount() - 2).toArray();
            projectedRow = ProjectedRow.from(project);
            paimonRowAsFlussRow = new PaimonRowAsFlussRow();
        }

        @Override
        public void close() {
            try {
                paimonRowIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return paimonRowIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            InternalRow paimonRow = paimonRowIterator.next();
            ChangeType changeType = toChangeType(paimonRow.getRowKind());
            long offset = paimonRow.getLong(logOffsetColIndex);
            long timestamp = paimonRow.getTimestamp(timestampColIndex, 6).getMillisecond();

            return new GenericRecord(
                    offset,
                    timestamp,
                    changeType,
                    projectedRow.replaceRow(paimonRowAsFlussRow.replaceRow(paimonRow)));
        }
    }
}
