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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.compacted.CompactedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for LogRecordBatchStatisticsParser. */
public class LogRecordBatchStatisticsParserTest {

    @Test
    public void testParseStatisticsFromByteArray() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {10L, 0L};
        CompactedRow minValues = createTestCompactedRow(rowType, -100, "aaa");
        CompactedRow maxValues = createTestCompactedRow(rowType, 999, "zzz");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse using byte array
        ByteViewLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(writtenData, rowType);

        // Verify parsing results
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(parsedStats.getNullCount(0)).isEqualTo(10L);
        assertThat(parsedStats.getNullCount(1)).isEqualTo(0L);

        // Verify min/max values
        InternalRow parsedMinValues = parsedStats.getMinValues();
        InternalRow parsedMaxValues = parsedStats.getMaxValues();

        assertThat(parsedMinValues.getInt(0)).isEqualTo(-100);
        assertThat(parsedMinValues.getString(1).toString()).isEqualTo("aaa");
        assertThat(parsedMaxValues.getInt(0)).isEqualTo(999);
        assertThat(parsedMaxValues.getString(1).toString()).isEqualTo("zzz");
    }

    @Test
    public void testParseStatisticsFromByteBuffer() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {3L, 7L};
        CompactedRow minValues = createTestCompactedRow(rowType, 0, "first");
        CompactedRow maxValues = createTestCompactedRow(rowType, 500, "last");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse using direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        ByteViewLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(buffer, rowType);

        // Verify parsing results
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(parsedStats.getNullCount(0)).isEqualTo(3L);
        assertThat(parsedStats.getNullCount(1)).isEqualTo(7L);

        // Verify min/max values
        InternalRow parsedMinValues = parsedStats.getMinValues();
        InternalRow parsedMaxValues = parsedStats.getMaxValues();

        assertThat(parsedMinValues.getInt(0)).isEqualTo(0);
        assertThat(parsedMinValues.getString(1).toString()).isEqualTo("first");
        assertThat(parsedMaxValues.getInt(0)).isEqualTo(500);
        assertThat(parsedMaxValues.getString(1).toString()).isEqualTo("last");
    }

    @Test
    public void testParseStatisticsWithNullMinMaxValues() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data with null min/max values
        Long[] nullCounts = new Long[] {0L, 0L};

        // Write statistics using writer with null min/max values
        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] writtenData = writeStatisticsForTest(writer, null, null, nullCounts);

        // Parse statistics
        ByteViewLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(writtenData, rowType);

        // Verify parsing results
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(parsedStats.hasMinValues()).isFalse();
        assertThat(parsedStats.hasMaxValues()).isFalse();
    }

    @Test
    public void testParseStatisticsWithEmptyData() {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Test with null data
        ByteViewLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics((byte[]) null, rowType);
        assertThat(parsedStats).isNull();

        // Test with empty data
        parsedStats = LogRecordBatchStatisticsParser.parseStatistics(new byte[0], rowType);
        assertThat(parsedStats).isNull();

        // Test with null ByteBuffer
        parsedStats = LogRecordBatchStatisticsParser.parseStatistics((ByteBuffer) null, rowType);
        assertThat(parsedStats).isNull();

        // Test with empty direct ByteBuffer
        ByteBuffer emptyBuffer = ByteBuffer.allocateDirect(0);
        parsedStats = LogRecordBatchStatisticsParser.parseStatistics(emptyBuffer, rowType);
        assertThat(parsedStats).isNull();
    }

    @Test
    public void testParseStatisticsWithInvalidSize() {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Test with invalid size
        MemorySegment segment = MemorySegment.wrap(new byte[100]);
        ByteViewLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, 0, rowType);
        assertThat(parsedStats).isNull();

        parsedStats = LogRecordBatchStatisticsParser.parseStatistics(segment, 0, -1, rowType);
        assertThat(parsedStats).isNull();
    }

    @Test
    public void testIsValidStatistics() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create valid statistics
        Long[] nullCounts = new Long[] {5L, 2L};
        CompactedRow minValues = createTestCompactedRow(rowType, 1, "min");
        CompactedRow maxValues = createTestCompactedRow(rowType, 100, "max");

        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Test with byte array
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(writtenData, rowType)).isTrue();

        // Test with direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(buffer, rowType)).isTrue();
    }

    @Test
    public void testIsValidStatisticsWithInvalidData() {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Test with invalid data
        assertThat(
                        LogRecordBatchStatisticsParser.isValidStatistics(
                                ByteBuffer.allocateDirect(0), rowType))
                .isFalse();

        // Test with null data
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics((byte[]) null, rowType))
                .isFalse();
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics((ByteBuffer) null, rowType))
                .isFalse();
    }

    @Test
    public void testGetStatisticsSize() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test statistics
        Long[] nullCounts = new Long[] {1L, 2L};
        CompactedRow minValues = createTestCompactedRow(rowType, 1, "a");
        CompactedRow maxValues = createTestCompactedRow(rowType, 100, "z");

        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Test with byte array
        int size = LogRecordBatchStatisticsParser.getStatisticsSize(writtenData);
        assertThat(size).isEqualTo(writtenData.length);

        // Test with direct ByteBuffer
        ByteBuffer buffer = ByteBuffer.allocateDirect(writtenData.length);
        buffer.put(writtenData);
        buffer.flip();
        size = LogRecordBatchStatisticsParser.getStatisticsSize(buffer);
        assertThat(size).isEqualTo(writtenData.length);
    }

    @Test
    public void testGetStatisticsSizeWithInvalidData() {
        // Test with invalid data
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize(ByteBuffer.allocateDirect(0)))
                .isEqualTo(-1);

        // Test with null data
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize((byte[]) null)).isEqualTo(-1);
        assertThat(LogRecordBatchStatisticsParser.getStatisticsSize((ByteBuffer) null))
                .isEqualTo(-1);
    }

    @Test
    public void testParseStatisticsWithMultipleFields() throws Exception {
        RowType rowType =
                DataTypes.ROW(
                        new IntType(false),
                        new StringType(false),
                        new IntType(false),
                        new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {0L, 5L, 10L, 0L};
        CompactedRow minValues = createTestCompactedRow(rowType, 1, "a", 10, "x");
        CompactedRow maxValues = createTestCompactedRow(rowType, 100, "z", 999, "y");

        // Write statistics using writer
        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] writtenData = writeStatisticsForTest(writer, minValues, maxValues, nullCounts);

        // Parse statistics
        ByteViewLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(writtenData, rowType);

        // Verify parsing results
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getFieldCount()).isEqualTo(4);
        assertThat(parsedStats.getNullCounts()).isEqualTo(nullCounts);

        // Verify min/max values
        InternalRow parsedMinValues = parsedStats.getMinValues();
        InternalRow parsedMaxValues = parsedStats.getMaxValues();

        assertThat(parsedMinValues.getInt(0)).isEqualTo(1);
        assertThat(parsedMinValues.getString(1).toString()).isEqualTo("a");
        assertThat(parsedMinValues.getInt(2)).isEqualTo(10);
        assertThat(parsedMinValues.getString(3).toString()).isEqualTo("x");

        assertThat(parsedMaxValues.getInt(0)).isEqualTo(100);
        assertThat(parsedMaxValues.getString(1).toString()).isEqualTo("z");
        assertThat(parsedMaxValues.getInt(2)).isEqualTo(999);
        assertThat(parsedMaxValues.getString(3).toString()).isEqualTo("y");
    }

    @Test
    public void testParseStatisticsWithPartialMinMaxValues() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data with only min values (no max values)
        Long[] nullCounts = new Long[] {0L, 0L};
        CompactedRow minValues = createTestCompactedRow(rowType, 1, "min");

        // Write statistics using writer with only min values
        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] writtenData = writeStatisticsForTest(writer, minValues, null, nullCounts);

        // Parse statistics
        ByteViewLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(writtenData, rowType);

        // Verify parsing results
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(parsedStats.hasMinValues()).isTrue();
        assertThat(parsedStats.hasMaxValues()).isFalse();

        // Verify min values
        InternalRow parsedMinValues = parsedStats.getMinValues();
        assertThat(parsedMinValues.getInt(0)).isEqualTo(1);
        assertThat(parsedMinValues.getString(1).toString()).isEqualTo("min");
    }

    // Helper methods for creating test data
    private CompactedRow createTestCompactedRow(RowType rowType, Object... values)
            throws IOException {
        CompactedRowWriter writer = new CompactedRowWriter(rowType.getFieldCount());
        writer.reset();

        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                writer.setNullAt(i);
            } else {
                DataType fieldType = rowType.getTypeAt(i);
                if (fieldType instanceof StringType) {
                    // Convert String to BinaryString for StringType fields
                    writer.writeString(BinaryString.fromString((String) values[i]));
                } else if (fieldType instanceof IntType) {
                    // Write int values directly
                    writer.writeInt((Integer) values[i]);
                } else {
                    // For other types, use the field writer
                    CompactedRowWriter.FieldWriter fieldWriter =
                            CompactedRowWriter.createFieldWriter(fieldType);
                    fieldWriter.writeField(writer, i, values[i]);
                }
            }
        }

        CompactedRow row = new CompactedRow(rowType.getFieldCount(), null);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }

    private byte[] writeStatisticsForTest(
            LogRecordBatchStatisticsWriter writer,
            InternalRow minValues,
            InternalRow maxValues,
            Long[] nullCounts)
            throws IOException {

        // Allocate enough memory for statistics
        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        int bytesWritten =
                writer.writeStatistics(
                        minValues, maxValues, nullCounts, new MemorySegmentOutputView(segment));

        // Copy the written data to a byte array
        byte[] result = new byte[bytesWritten];
        segment.get(0, result, 0, bytesWritten);
        return result;
    }
}
