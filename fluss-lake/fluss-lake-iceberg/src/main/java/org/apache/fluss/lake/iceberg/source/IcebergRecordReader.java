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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.lake.source.PositionedRecord;
import org.apache.fluss.lake.source.PositionedRecordReader;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.ProjectedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;

/**
 * Iceberg record reader. The filter is applied during the plan phase of IcebergSplitPlanner, so the
 * RecordReader does not need to apply the filter again.
 *
 * <p>Refer to {@link org.apache.iceberg.data.GenericReader#open(FileScanTask)} and {@link
 * org.apache.iceberg.Scan#ignoreResiduals()} for details.
 */
public class IcebergRecordReader implements PositionedRecordReader {

    private final FileScanTask fileScanTask;
    private final Table table;
    protected final @Nullable int[][] project;

    public IcebergRecordReader(FileScanTask fileScanTask, Table table, @Nullable int[][] project) {
        this.fileScanTask = fileScanTask;
        this.table = table;
        this.project = project;
    }

    @Override
    public CloseableIterator<LogRecord> read() throws IOException {
        OpenedReader openedReader = openReader();
        return new IcebergRecordAsFlussRecordIterator(openedReader.records, openedReader.struct);
    }

    @Override
    public CloseableIterator<PositionedRecord> readWithPosition() throws IOException {
        OpenedReader openedReader = openReader();
        return new IcebergPositionedRecordIterator(openedReader.records, openedReader.struct);
    }

    private OpenedReader openReader() {
        TableScan tableScan = createTableScan();
        IcebergGenericReader reader = new IcebergGenericReader(tableScan, true);
        return new OpenedReader(reader.open(fileScanTask), tableScan.schema().asStruct());
    }

    private TableScan createTableScan() {
        int[][] effectiveProject = project != null ? project : allDataFieldProject(table.schema());
        return applyProject(table.newScan(), effectiveProject);
    }

    private int[][] allDataFieldProject(Schema schema) {
        Types.StructType structType = schema.asStruct();
        List<int[]> projects = new ArrayList<>();
        for (int i = 0; i < structType.fields().size(); i++) {
            String fieldName = structType.fields().get(i).name();
            if (!OFFSET_COLUMN_NAME.equals(fieldName) && !TIMESTAMP_COLUMN_NAME.equals(fieldName)) {
                projects.add(new int[] {i});
            }
        }
        return projects.toArray(new int[0][]);
    }

    private TableScan applyProject(TableScan tableScan, int[][] projects) {
        Types.StructType structType = tableScan.schema().asStruct();
        List<Types.NestedField> cols = new ArrayList<>(projects.length + 3);

        for (int[] projectedField : projects) {
            cols.add(structType.fields().get(projectedField[0]));
        }

        cols.add(structType.field(OFFSET_COLUMN_NAME));
        cols.add(structType.field(TIMESTAMP_COLUMN_NAME));
        cols.add(MetadataColumns.ROW_POSITION);
        return tableScan.project(new Schema(cols));
    }

    private static GenericRecord toGenericRecord(
            Record icebergRecord,
            ProjectedRow projectedRow,
            IcebergRecordAsFlussRow icebergRecordAsFlussRow,
            int logOffsetColIndex,
            int timestampColIndex) {
        long offset = icebergRecord.get(logOffsetColIndex, Long.class);
        long timestamp =
                icebergRecord
                        .get(timestampColIndex, OffsetDateTime.class)
                        .toInstant()
                        .toEpochMilli();
        return new GenericRecord(
                offset,
                timestamp,
                ChangeType.INSERT,
                projectedRow.replaceRow(
                        icebergRecordAsFlussRow.replaceIcebergRecord(icebergRecord)));
    }

    /** Iterator for iceberg record as fluss record. */
    public static class IcebergRecordAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.iceberg.io.CloseableIterator<Record> icebergRecordIterator;
        private final ProjectedRow projectedRow;
        private final IcebergRecordAsFlussRow icebergRecordAsFlussRow;
        private final int logOffsetColIndex;
        private final int timestampColIndex;

        public IcebergRecordAsFlussRecordIterator(
                CloseableIterable<Record> icebergRecordIterable, Types.StructType struct) {
            this.icebergRecordIterator = icebergRecordIterable.iterator();
            this.logOffsetColIndex = struct.fields().size() - 3;
            this.timestampColIndex = struct.fields().size() - 2;
            int[] project = IntStream.range(0, struct.fields().size() - 3).toArray();
            this.projectedRow = ProjectedRow.from(project);
            this.icebergRecordAsFlussRow = new IcebergRecordAsFlussRow();
        }

        @Override
        public void close() {
            try {
                icebergRecordIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return icebergRecordIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            return toGenericRecord(
                    icebergRecordIterator.next(),
                    projectedRow,
                    icebergRecordAsFlussRow,
                    logOffsetColIndex,
                    timestampColIndex);
        }
    }

    /** Iterator for iceberg record as fluss positioned record. */
    public static class IcebergPositionedRecordIterator
            implements CloseableIterator<PositionedRecord> {

        private final org.apache.iceberg.io.CloseableIterator<Record> icebergRecordIterator;
        private final ProjectedRow projectedRow;
        private final IcebergRecordAsFlussRow icebergRecordAsFlussRow;
        private final int logOffsetColIndex;
        private final int timestampColIndex;
        private final int rowPositionColIndex;

        public IcebergPositionedRecordIterator(
                CloseableIterable<Record> icebergRecordIterable, Types.StructType struct) {
            this.icebergRecordIterator = icebergRecordIterable.iterator();
            this.logOffsetColIndex = struct.fields().size() - 3;
            this.timestampColIndex = struct.fields().size() - 2;
            this.rowPositionColIndex = struct.fields().size() - 1;
            int[] project = IntStream.range(0, struct.fields().size() - 3).toArray();
            this.projectedRow = ProjectedRow.from(project);
            this.icebergRecordAsFlussRow = new IcebergRecordAsFlussRow();
        }

        @Override
        public void close() {
            try {
                icebergRecordIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return icebergRecordIterator.hasNext();
        }

        @Override
        public PositionedRecord next() {
            Record icebergRecord = icebergRecordIterator.next();
            return new PositionedRecord(
                    toGenericRecord(
                            icebergRecord,
                            projectedRow,
                            icebergRecordAsFlussRow,
                            logOffsetColIndex,
                            timestampColIndex),
                    icebergRecord.get(rowPositionColIndex, Long.class));
        }
    }

    private static class OpenedReader {
        private final CloseableIterable<Record> records;
        private final Types.StructType struct;

        private OpenedReader(CloseableIterable<Record> records, Types.StructType struct) {
            this.records = records;
            this.struct = struct;
        }
    }
}
