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
import com.alibaba.fluss.row.compacted.CompactedRowDeserializer;
import com.alibaba.fluss.row.compacted.CompactedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for ByteViewLogRecordBatchStatistics. */
public class ByteViewLogRecordBatchStatisticsTest {

    @Test
    public void testByteViewStatisticsCreation() {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data
        Long[] nullCounts = new Long[] {0L, 5L};

        // Create min/max values as CompactedRow
        CompactedRow minRow = createTestCompactedRow(rowType, 1, "min");
        CompactedRow maxRow = createTestCompactedRow(rowType, 100, "max");

        // Create statistics
        ByteViewLogRecordBatchStatistics statistics =
                new ByteViewLogRecordBatchStatistics(
                        minRow.getSegment(),
                        minRow.getOffset(),
                        minRow.getSizeInBytes(),
                        rowType,
                        nullCounts,
                        0,
                        0,
                        minRow.getSizeInBytes(),
                        maxRow.getSizeInBytes());

        // Verify basic properties
        assertThat(statistics.getFieldCount()).isEqualTo(2);
        assertThat(statistics.hasMinValues()).isTrue();
        assertThat(statistics.hasMaxValues()).isTrue();

        // Verify null counts
        assertThat(statistics.getNullCounts()).isEqualTo(nullCounts);
        assertThat(statistics.getNullCount(0)).isEqualTo(0L);
        assertThat(statistics.getNullCount(1)).isEqualTo(5L);

        // Verify row type
        assertThat(statistics.getRowType()).isEqualTo(rowType);
        assertThat(statistics.getFieldType(0)).isInstanceOf(IntType.class);
        assertThat(statistics.getFieldType(1)).isInstanceOf(StringType.class);
    }

    @Test
    public void testByteViewStatisticsWithNullValues() {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test data with null values
        Long[] nullCounts = new Long[] {10L, 0L};

        // Create statistics with no min/max values
        ByteViewLogRecordBatchStatistics statistics =
                new ByteViewLogRecordBatchStatistics(null, 0, 0, rowType, nullCounts, 0, 0, 0, 0);

        // Verify properties
        assertThat(statistics.hasMinValues()).isFalse();
        assertThat(statistics.hasMaxValues()).isFalse();
        assertThat(statistics.getMinValues()).isNull();
        assertThat(statistics.getMaxValues()).isNull();
        assertThat(statistics.getNullCounts()).isEqualTo(nullCounts);
    }

    @Test
    public void testByteViewStatisticsParser() throws IOException {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test statistics using CompactedRow
        CompactedRow minValues = createTestCompactedRow(rowType, 1, "min");
        CompactedRow maxValues = createTestCompactedRow(rowType, 100, "max");
        Long[] nullCounts = new Long[] {0L, 5L};

        // Create a combined memory segment that contains both min and max values
        // This simulates how statistics are actually stored in a single memory segment
        int minSize = minValues.getSizeInBytes();
        int maxSize = maxValues.getSizeInBytes();
        int totalSize = minSize + maxSize;

        MemorySegment combinedSegment = MemorySegment.allocateHeapMemory(totalSize);

        // Copy min values to the beginning
        combinedSegment.put(
                0, minValues.getSegment().getHeapMemory(), minValues.getOffset(), minSize);

        // Copy max values after min values
        combinedSegment.put(
                minSize, maxValues.getSegment().getHeapMemory(), maxValues.getOffset(), maxSize);

        // Create ByteViewLogRecordBatchStatistics with correct offsets
        ByteViewLogRecordBatchStatistics originalStats =
                new ByteViewLogRecordBatchStatistics(
                        combinedSegment,
                        0, // position
                        totalSize, // size
                        rowType,
                        nullCounts,
                        0, // minValuesOffset = 0 (min values start at position 0)
                        minSize, // maxValuesOffset = minSize (max values start after min values)
                        minSize, // minValuesSize
                        maxSize); // maxValuesSize

        // Test basic functionality
        assertThat(originalStats).isNotNull();
        assertThat(originalStats.getFieldCount()).isEqualTo(2);
        assertThat(originalStats.hasMinValues()).isTrue();
        assertThat(originalStats.hasMaxValues()).isTrue();
        assertThat(originalStats.getNullCounts()).isEqualTo(nullCounts);

        // Verify the values can be accessed as InternalRow
        InternalRow minRow = originalStats.getMinValues();
        InternalRow maxRow = originalStats.getMaxValues();

        assertThat(minRow.getInt(0)).isEqualTo(1);
        assertThat(minRow.getString(1).toString()).isEqualTo("min");
        assertThat(maxRow.getInt(0)).isEqualTo(100);
        assertThat(maxRow.getString(1).toString()).isEqualTo("max");
    }

    @Test
    public void testByteViewStatisticsWriter() throws Exception {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Create test statistics using CompactedRow
        CompactedRow minValues = createTestCompactedRow(rowType, 1, "min");
        CompactedRow maxValues = createTestCompactedRow(rowType, 100, "max");
        Long[] nullCounts = new Long[] {0L, 5L};

        // Create a simple test statistics object
        LogRecordBatchStatistics stats =
                new LogRecordBatchStatistics() {
                    @Override
                    public InternalRow getMinValues() {
                        return minValues;
                    }

                    @Override
                    public InternalRow getMaxValues() {
                        return maxValues;
                    }

                    @Override
                    public Long[] getNullCounts() {
                        return nullCounts;
                    }
                };

        // Create writer and write statistics using zero-copy approach
        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] writtenData =
                writeStatisticsForTest(
                        writer, stats.getMinValues(), stats.getMaxValues(), stats.getNullCounts());

        // Parse the written data
        ByteViewLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(writtenData, rowType);

        // Verify the parsed statistics match the original
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getNullCounts()).isEqualTo(nullCounts);

        InternalRow parsedMinValues = parsedStats.getMinValues();
        InternalRow parsedMaxValues = parsedStats.getMaxValues();

        assertThat(parsedMinValues.getInt(0)).isEqualTo(1);
        assertThat(parsedMinValues.getString(1).toString()).isEqualTo("min");
        assertThat(parsedMaxValues.getInt(0)).isEqualTo(100);
        assertThat(parsedMaxValues.getString(1).toString()).isEqualTo("max");
    }

    @Test
    public void testByteViewStatisticsValidation() throws IOException {
        RowType rowType = DataTypes.ROW(new IntType(false), new StringType(false));

        // Test valid statistics using CompactedRow
        CompactedRow minValues = createTestCompactedRow(rowType, 1, "min");
        CompactedRow maxValues = createTestCompactedRow(rowType, 100, "max");
        Long[] nullCounts = new Long[] {0L, 5L};

        // Create a simple test statistics object
        LogRecordBatchStatistics stats =
                new LogRecordBatchStatistics() {
                    @Override
                    public InternalRow getMinValues() {
                        return minValues;
                    }

                    @Override
                    public InternalRow getMaxValues() {
                        return maxValues;
                    }

                    @Override
                    public Long[] getNullCounts() {
                        return nullCounts;
                    }
                };

        LogRecordBatchStatisticsWriter writer = new LogRecordBatchStatisticsWriter(rowType);
        byte[] serializedData =
                writeStatisticsForTest(
                        writer, stats.getMinValues(), stats.getMaxValues(), stats.getNullCounts());

        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(serializedData, rowType))
                .isTrue();

        // Test invalid data
        byte[] invalidData = {1, 0, 3}; // Invalid format
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics(invalidData, rowType))
                .isFalse();

        // Test null data
        assertThat(LogRecordBatchStatisticsParser.isValidStatistics((byte[]) null, rowType))
                .isFalse();
    }

    /**
     * Helper method to write statistics for testing purposes using zero-copy approach. This method
     * creates a temporary memory segment and copies the result to a byte array for testing,
     * avoiding the use of the deprecated byte[] returning method.
     *
     * @param writer The statistics writer
     * @param minValues The minimum values
     * @param maxValues The maximum values
     * @param nullCounts The null counts
     * @return The serialized statistics as a byte array
     * @throws IOException If writing fails
     */
    private byte[] writeStatisticsForTest(
            LogRecordBatchStatisticsWriter writer,
            InternalRow minValues,
            InternalRow maxValues,
            Long[] nullCounts)
            throws IOException {
        // Use a temporary memory segment for testing
        MemorySegment tempSegment =
                MemorySegment.wrap(new byte[2048]); // Conservative size estimate
        int size =
                writer.writeStatistics(
                        minValues, maxValues, nullCounts, new MemorySegmentOutputView(tempSegment));
        byte[] result = new byte[size];
        tempSegment.get(0, result, 0, size);
        return result;
    }

    /** Create a test CompactedRow with the given values. */
    private CompactedRow createTestCompactedRow(RowType rowType, int intValue, String stringValue) {
        DataType[] fieldTypes = new DataType[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldTypes[i] = rowType.getTypeAt(i);
        }

        CompactedRowWriter writer = new CompactedRowWriter(rowType.getFieldCount());
        writer.reset();

        // Write int value
        writer.writeInt(intValue);

        // Write string value
        writer.writeString(BinaryString.fromString(stringValue));

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(fieldTypes);
        CompactedRow row = new CompactedRow(rowType.getFieldCount(), deserializer);
        row.pointTo(writer.segment(), 0, writer.position());
        return row;
    }
}
