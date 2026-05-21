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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IndexedLogRecord}. */
class IndexedLogRecordTest extends LogTestBase {

    @Test
    void testWriteToAndReadFromWithRowId() throws IOException {
        DataType[] fieldTypes = baseRowType.getChildren().toArray(new DataType[0]);
        IndexedRow row = new IndexedRow(fieldTypes);
        IndexedRowWriter writer =
                new IndexedRowWriter(baseRowType.getChildren().toArray(new DataType[0]));
        writer.writeInt(10);
        writer.writeString(BinaryString.fromString("abc"));
        row.pointTo(writer.segment(), 0, writer.position());

        long rowId = 12345L;
        IndexedLogRecord.writeTo(outputView, ChangeType.INSERT, row, rowId);

        IndexedLogRecord logRecord =
                IndexedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        fieldTypes,
                        true);

        assertThat(logRecord.logOffset()).isEqualTo(1000);
        assertThat(logRecord.timestamp()).isEqualTo(10001);
        assertThat(logRecord.getChangeType()).isEqualTo(ChangeType.INSERT);
        assertThat(logRecord.getRowId()).isEqualTo(rowId);
        // getRow() must not include RowId bytes
        assertThat(logRecord.getRow()).isEqualTo(row);
    }

    @Test
    void testWriteToAndReadFromWithRowIdBoundaryValues() throws IOException {
        DataType[] fieldTypes = baseRowType.getChildren().toArray(new DataType[0]);
        long[] rowIds = {0L, 127L, 128L, 16383L, 16384L, 2097151L, Long.MAX_VALUE};

        for (long rowId : rowIds) {
            MemorySegmentOutputView view = new MemorySegmentOutputView(200);
            IndexedRow row = new IndexedRow(fieldTypes);
            IndexedRowWriter writer =
                    new IndexedRowWriter(baseRowType.getChildren().toArray(new DataType[0]));
            writer.writeInt(7);
            writer.writeString(BinaryString.fromString("x"));
            row.pointTo(writer.segment(), 0, writer.position());

            IndexedLogRecord.writeTo(view, ChangeType.UPDATE_AFTER, row, rowId);

            IndexedLogRecord logRecord =
                    IndexedLogRecord.readFrom(
                            MemorySegment.wrap(view.getCopyOfBuffer()),
                            0,
                            500,
                            9999,
                            fieldTypes,
                            true);

            assertThat(logRecord.getRowId()).isEqualTo(rowId);
            assertThat(logRecord.getRow()).isEqualTo(row);
        }
    }

    @Test
    void testReadFromWithoutDvReturnsNoRowId() throws IOException {
        DataType[] fieldTypes = baseRowType.getChildren().toArray(new DataType[0]);
        IndexedRow row = new IndexedRow(fieldTypes);
        IndexedRowWriter writer =
                new IndexedRowWriter(baseRowType.getChildren().toArray(new DataType[0]));
        writer.writeInt(10);
        writer.writeString(BinaryString.fromString("abc"));
        row.pointTo(writer.segment(), 0, writer.position());

        // write without RowId (non-DV format)
        IndexedLogRecord.writeTo(outputView, ChangeType.APPEND_ONLY, row);

        IndexedLogRecord logRecord =
                IndexedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        fieldTypes,
                        false);

        assertThat(logRecord.getRowId()).isEqualTo(LogRecord.NO_ROW_ID);
        assertThat(logRecord.getRow()).isEqualTo(row);
    }

    @Test
    void testBase() throws IOException {
        DataType[] fieldTypes = baseRowType.getChildren().toArray(new DataType[0]);
        // create row.
        IndexedRow row = new IndexedRow(fieldTypes);
        IndexedRowWriter writer =
                new IndexedRowWriter(baseRowType.getChildren().toArray(new DataType[0]));
        writer.writeInt(10);
        writer.writeString(BinaryString.fromString("abc"));
        row.pointTo(writer.segment(), 0, writer.position());

        IndexedLogRecord.writeTo(outputView, ChangeType.APPEND_ONLY, row);
        // Test read from.
        IndexedLogRecord defaultLogRecord =
                IndexedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        fieldTypes);

        assertThat(defaultLogRecord.getSizeInBytes()).isEqualTo(17);
        assertThat(defaultLogRecord.logOffset()).isEqualTo(1000);
        assertThat(defaultLogRecord.timestamp()).isEqualTo(10001);
        assertThat(defaultLogRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
        assertThat(defaultLogRecord.getRow()).isEqualTo(row);
    }

    @Test
    void testWriteToAndReadFromWithRandomData() throws IOException {
        // Test write to.
        IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
        IndexedLogRecord.writeTo(outputView, ChangeType.APPEND_ONLY, row);
        DataType[] allColTypes =
                TestInternalRowGenerator.createAllRowType().getChildren().toArray(new DataType[0]);

        // Test read from.
        LogRecord defaultLogRecord =
                IndexedLogRecord.readFrom(
                        MemorySegment.wrap(outputView.getCopyOfBuffer()),
                        0,
                        1000,
                        10001,
                        allColTypes);

        assertThat(defaultLogRecord.logOffset()).isEqualTo(1000);
        assertThat(defaultLogRecord.timestamp()).isEqualTo(10001);
        assertThat(defaultLogRecord.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
        assertThat(defaultLogRecord.getRow()).isEqualTo(row);
    }
}
