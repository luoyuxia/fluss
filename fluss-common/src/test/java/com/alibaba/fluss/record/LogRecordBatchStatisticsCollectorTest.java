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
import com.alibaba.fluss.memory.TestingMemorySegmentPool;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.testutils.DataTestUtils;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Comprehensive test for {@link LogRecordBatchStatisticsCollector}. */
public class LogRecordBatchStatisticsCollectorTest {

    private LogRecordBatchStatisticsCollector collector;
    private RowType testRowType;

    // Test data for different data types
    private static final List<Object[]> MIXED_TYPE_DATA =
            Arrays.asList(
                    new Object[] {1, "a", 10.5, true, 100L, 1.23f},
                    new Object[] {2, "b", 20.3, false, 200L, 2.34f},
                    new Object[] {3, "c", 15.7, true, 150L, 3.45f},
                    new Object[] {4, "d", 8.9, false, 300L, 4.56f},
                    new Object[] {5, "e", 30.1, true, 250L, 5.67f});

    private static final RowType MIXED_TYPE_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("value", DataTypes.DOUBLE()),
                    new DataField("flag", DataTypes.BOOLEAN()),
                    new DataField("bigint_val", DataTypes.BIGINT()),
                    new DataField("float_val", DataTypes.FLOAT()));

    // Test data with null values
    private static final List<Object[]> DATA_WITH_NULLS =
            Arrays.asList(
                    new Object[] {1, "a", 10.5},
                    new Object[] {null, "b", 20.3},
                    new Object[] {3, null, 15.7},
                    new Object[] {4, "d", null},
                    new Object[] {5, "e", 30.1});

    private static final RowType DATA_WITH_NULLS_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("value", DataTypes.DOUBLE()));

    // Test data for decimal type
    private static final List<Object[]> DECIMAL_DATA =
            Arrays.asList(
                    new Object[] {Decimal.fromBigDecimal(new BigDecimal("10.50"), 10, 2)},
                    new Object[] {Decimal.fromBigDecimal(new BigDecimal("20.30"), 10, 2)},
                    new Object[] {Decimal.fromBigDecimal(new BigDecimal("15.70"), 10, 2)},
                    new Object[] {Decimal.fromBigDecimal(new BigDecimal("8.90"), 10, 2)},
                    new Object[] {Decimal.fromBigDecimal(new BigDecimal("30.10"), 10, 2)});

    private static final RowType DECIMAL_ROW_TYPE =
            DataTypes.ROW(new DataField("decimal_val", DataTypes.DECIMAL(10, 2)));

    @BeforeEach
    void setUp() {
        testRowType = MIXED_TYPE_ROW_TYPE;
        collector = new LogRecordBatchStatisticsCollector(testRowType);
    }

    @Test
    void testConstructor() {
        assertThat(collector.getRowType()).isEqualTo(testRowType);
        assertThat(collector.getRowType().getFieldCount()).isEqualTo(6);
    }

    @Test
    void testProcessRowWithMixedTypes() throws IOException {
        // Process test data
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write statistics to memory segment
        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));

        // Verify that statistics were written
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, testRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls in this test
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(6);
        for (int i = 0; i < 6; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }

        // Verify min/max values for each field
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // Field 0: INT - should be min=1, max=5
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(maxValues.getInt(0)).isEqualTo(5);

        // Field 1: STRING - should be min="a", max="e" (lexicographic order)
        assertThat(minValues.getString(1).toString()).isEqualTo("a");
        assertThat(maxValues.getString(1).toString()).isEqualTo("e");

        // Field 2: DOUBLE - should be min=8.9, max=30.1
        assertThat(minValues.getDouble(2)).isEqualTo(8.9);
        assertThat(maxValues.getDouble(2)).isEqualTo(30.1);

        // Field 3: BOOLEAN - should be min=false, max=true
        assertThat(minValues.getBoolean(3)).isFalse();
        assertThat(maxValues.getBoolean(3)).isTrue();

        // Field 4: BIGINT - should be min=100L, max=300L
        assertThat(minValues.getLong(4)).isEqualTo(100L);
        assertThat(maxValues.getLong(4)).isEqualTo(300L);

        // Field 5: FLOAT - should be min=1.23f, max=5.67f
        assertThat(minValues.getFloat(5)).isEqualTo(1.23f);
        assertThat(maxValues.getFloat(5)).isEqualTo(5.67f);
    }

    @Test
    void testProcessRowWithNullValues() throws IOException {
        LogRecordBatchStatisticsCollector nullCollector =
                new LogRecordBatchStatisticsCollector(DATA_WITH_NULLS_ROW_TYPE);

        // Process test data with nulls
        for (Object[] data : DATA_WITH_NULLS) {
            InternalRow row = DataTestUtils.row(data);
            nullCollector.processRow(row);
        }

        // Write statistics to memory segment
        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = nullCollector.writeStatistics(new MemorySegmentOutputView(segment));

        // Verify that statistics were written
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, DATA_WITH_NULLS_ROW_TYPE);
        assertThat(statistics).isNotNull();

        // Verify null counts
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(3);

        // Field 0 (id): 1 null out of 5 rows
        assertThat(nullCounts[0]).isEqualTo(1L);

        // Field 1 (name): 1 null out of 5 rows
        assertThat(nullCounts[1]).isEqualTo(1L);

        // Field 2 (value): 1 null out of 5 rows
        assertThat(nullCounts[2]).isEqualTo(1L);

        // Verify min/max values (should ignore nulls)
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // Field 0 (id): min=1, max=5 (excluding null)
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(maxValues.getInt(0)).isEqualTo(5);

        // Field 1 (name): min="a", max="e" (excluding null)
        assertThat(minValues.getString(1).toString()).isEqualTo("a");
        assertThat(maxValues.getString(1).toString()).isEqualTo("e");

        // Field 2 (value): min=10.5, max=30.1 (excluding null)
        assertThat(minValues.getDouble(2)).isEqualTo(10.5);
        assertThat(maxValues.getDouble(2)).isEqualTo(30.1);
    }

    @Test
    void testProcessRowWithDecimalType() throws IOException {
        LogRecordBatchStatisticsCollector decimalCollector =
                new LogRecordBatchStatisticsCollector(DECIMAL_ROW_TYPE);

        // Process decimal test data
        for (Object[] data : DECIMAL_DATA) {
            InternalRow row = DataTestUtils.row(data);
            decimalCollector.processRow(row);
        }

        // Write statistics to memory segment
        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = decimalCollector.writeStatistics(new MemorySegmentOutputView(segment));

        // Verify that statistics were written
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, DECIMAL_ROW_TYPE);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(1);
        assertThat(nullCounts[0]).isEqualTo(0L);

        // Verify min/max values for decimal field
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // Decimal field: min=8.90, max=30.10
        Decimal minDecimal = minValues.getDecimal(0, 10, 2);
        Decimal maxDecimal = maxValues.getDecimal(0, 10, 2);

        assertThat(minDecimal.toBigDecimal()).isEqualByComparingTo(new BigDecimal("8.90"));
        assertThat(maxDecimal.toBigDecimal()).isEqualByComparingTo(new BigDecimal("30.10"));
    }

    @Test
    void testWriteStatisticsWithNoRows() throws IOException {
        // Don't process any rows
        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));

        // Should return 0 when no rows were processed
        assertThat(bytesWritten).isEqualTo(0);

        // Verify that no statistics can be parsed when no data was written
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, testRowType);
        assertThat(statistics).isNull();
    }

    @Test
    void testWriteStatisticsWithSingleRow() throws IOException {
        // Process single row
        Object[] singleData = MIXED_TYPE_DATA.get(0);
        InternalRow row = DataTestUtils.row(singleData);
        collector.processRow(row);

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = collector.writeStatistics(new MemorySegmentOutputView(segment));

        // Should write statistics for single row
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, testRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(6);
        for (int i = 0; i < 6; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }

        // Verify min/max values - for single row, min and max should be the same
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // All fields should have same min and max values
        assertThat(minValues.getInt(0)).isEqualTo(maxValues.getInt(0)).isEqualTo(1);
        assertThat(minValues.getString(1).toString())
                .isEqualTo(maxValues.getString(1).toString())
                .isEqualTo("a");
        assertThat(minValues.getDouble(2)).isEqualTo(maxValues.getDouble(2)).isEqualTo(10.5);
        assertThat(minValues.getBoolean(3)).isEqualTo(maxValues.getBoolean(3)).isTrue();
        assertThat(minValues.getLong(4)).isEqualTo(maxValues.getLong(4)).isEqualTo(100L);
        assertThat(minValues.getFloat(5)).isEqualTo(maxValues.getFloat(5)).isEqualTo(1.23f);
    }

    @Test
    void testWriteStatisticsWithLargeDataset() throws IOException {
        // Create a new collector for this test to avoid interference
        LogRecordBatchStatisticsCollector largeDatasetCollector =
                new LogRecordBatchStatisticsCollector(testRowType);

        // Process large dataset
        for (int i = 0; i < 1000; i++) {
            Object[] data = {i, "row" + i, (double) i, i % 2 == 0, (long) i, (float) i};
            InternalRow row = DataTestUtils.row(data);
            largeDatasetCollector.processRow(row);
        }

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten =
                largeDatasetCollector.writeStatistics(new MemorySegmentOutputView(segment));

        // Should write statistics for large dataset
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, testRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(6);
        for (int i = 0; i < 6; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }

        // Verify min/max values for large dataset
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // Print actual values for debugging
        System.out.println(
                "Actual min values: "
                        + minValues.getInt(0)
                        + ", "
                        + minValues.getString(1)
                        + ", "
                        + minValues.getDouble(2)
                        + ", "
                        + minValues.getBoolean(3)
                        + ", "
                        + minValues.getLong(4)
                        + ", "
                        + minValues.getFloat(5));

        System.out.println(
                "Actual max values: "
                        + maxValues.getInt(0)
                        + ", "
                        + maxValues.getString(1)
                        + ", "
                        + maxValues.getDouble(2)
                        + ", "
                        + maxValues.getBoolean(3)
                        + ", "
                        + maxValues.getLong(4)
                        + ", "
                        + maxValues.getFloat(5));

        // Field 0: INT - should be min=0, max=999
        assertThat(minValues.getInt(0)).isEqualTo(0);
        assertThat(maxValues.getInt(0)).isEqualTo(999);

        // Field 1: STRING - should be min="row0", max="row999" (lexicographic order)
        assertThat(minValues.getString(1).toString()).isEqualTo("row0");
        assertThat(maxValues.getString(1).toString()).isEqualTo("row999");

        // Field 2: DOUBLE - should be min=0.0, max=999.0
        assertThat(minValues.getDouble(2)).isEqualTo(0.0);
        assertThat(maxValues.getDouble(2)).isEqualTo(999.0);

        // Field 3: BOOLEAN - should be min=false, max=true (since we have both true and false)
        assertThat(minValues.getBoolean(3)).isFalse();
        assertThat(maxValues.getBoolean(3)).isTrue();

        // Field 4: BIGINT - should be min=0L, max=999L
        assertThat(minValues.getLong(4)).isEqualTo(0L);
        assertThat(maxValues.getLong(4)).isEqualTo(999L);

        // Field 5: FLOAT - should be min=0.0f, max=999.0f
        assertThat(minValues.getFloat(5)).isEqualTo(0.0f);
        assertThat(maxValues.getFloat(5)).isEqualTo(999.0f);
    }

    @Test
    void testReset() throws IOException {
        // Process some rows
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write statistics before reset
        MemorySegment segment1 = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten1 = collector.writeStatistics(new MemorySegmentOutputView(segment1));
        assertThat(bytesWritten1).isGreaterThan(0);

        // Verify statistics before reset
        LogRecordBatchStatistics statistics1 =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment1, 0, bytesWritten1, testRowType);
        assertThat(statistics1).isNotNull();
        assertThat(statistics1.getNullCounts()[0]).isEqualTo(0L);

        // Reset collector
        collector.reset();

        // Write statistics after reset
        MemorySegment segment2 = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten2 = collector.writeStatistics(new MemorySegmentOutputView(segment2));

        // Should return 0 after reset
        assertThat(bytesWritten2).isEqualTo(0);

        // Verify that no statistics can be parsed after reset
        LogRecordBatchStatistics statistics2 =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment2, 0, bytesWritten2, testRowType);
        assertThat(statistics2).isNull();
    }

    @Test
    void testResetAndReprocess() throws IOException {
        // Process initial data
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write first statistics
        MemorySegment segment1 = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten1 = collector.writeStatistics(new MemorySegmentOutputView(segment1));
        assertThat(bytesWritten1).isGreaterThan(0);

        // Verify first statistics
        LogRecordBatchStatistics statistics1 =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment1, 0, bytesWritten1, testRowType);
        assertThat(statistics1).isNotNull();
        assertThat(statistics1.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(statistics1.getMaxValues().getInt(0)).isEqualTo(5);

        // Reset and process new data with same row type
        collector.reset();
        for (Object[] data : MIXED_TYPE_DATA) {
            InternalRow row = DataTestUtils.row(data);
            collector.processRow(row);
        }

        // Write second statistics
        MemorySegment segment2 = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten2 = collector.writeStatistics(new MemorySegmentOutputView(segment2));
        assertThat(bytesWritten2).isGreaterThan(0);

        // Verify second statistics should be identical to first
        LogRecordBatchStatistics statistics2 =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment2, 0, bytesWritten2, testRowType);
        assertThat(statistics2).isNotNull();
        assertThat(statistics2.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(statistics2.getMaxValues().getInt(0)).isEqualTo(5);

        // Verify that both statistics are identical
        assertThat(statistics2.getMinValues().getInt(0))
                .isEqualTo(statistics1.getMinValues().getInt(0));
        assertThat(statistics2.getMaxValues().getInt(0))
                .isEqualTo(statistics1.getMaxValues().getInt(0));
        assertThat(statistics2.getNullCounts()).isEqualTo(statistics1.getNullCounts());
    }

    @Test
    void testAllSupportedDataTypes() throws IOException {
        // Test all supported data types individually
        RowType simpleRowType =
                DataTypes.ROW(
                        new DataField("boolean_val", DataTypes.BOOLEAN()),
                        new DataField("byte_val", DataTypes.TINYINT()),
                        new DataField("short_val", DataTypes.SMALLINT()),
                        new DataField("int_val", DataTypes.INT()),
                        new DataField("long_val", DataTypes.BIGINT()),
                        new DataField("float_val", DataTypes.FLOAT()),
                        new DataField("double_val", DataTypes.DOUBLE()),
                        new DataField("string_val", DataTypes.STRING()),
                        new DataField("decimal_val", DataTypes.DECIMAL(10, 2)),
                        new DataField("date_val", DataTypes.DATE()));

        LogRecordBatchStatisticsCollector typeCollector =
                new LogRecordBatchStatisticsCollector(simpleRowType);

        // Test data for all types
        Object[] testData = {
            true,
            (byte) 1,
            (short) 2,
            3,
            4L,
            5.0f,
            6.0,
            "test",
            Decimal.fromBigDecimal(new BigDecimal("10.50"), 10, 2),
            1000
        };

        InternalRow row = DataTestUtils.row(testData);
        typeCollector.processRow(row);

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = typeCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, simpleRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }

        // Verify min/max values for each field type
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // For single row, min and max should be the same
        assertThat(minValues.getBoolean(0)).isEqualTo(maxValues.getBoolean(0)).isTrue();
        assertThat(minValues.getByte(1)).isEqualTo(maxValues.getByte(1)).isEqualTo((byte) 1);
        assertThat(minValues.getShort(2)).isEqualTo(maxValues.getShort(2)).isEqualTo((short) 2);
        assertThat(minValues.getInt(3)).isEqualTo(maxValues.getInt(3)).isEqualTo(3);
        assertThat(minValues.getLong(4)).isEqualTo(maxValues.getLong(4)).isEqualTo(4L);
        assertThat(minValues.getFloat(5)).isEqualTo(maxValues.getFloat(5)).isEqualTo(5.0f);
        assertThat(minValues.getDouble(6)).isEqualTo(maxValues.getDouble(6)).isEqualTo(6.0);
        assertThat(minValues.getString(7).toString())
                .isEqualTo(maxValues.getString(7).toString())
                .isEqualTo("test");

        Decimal minDecimal = minValues.getDecimal(8, 10, 2);
        Decimal maxDecimal = maxValues.getDecimal(8, 10, 2);
        assertThat(minDecimal.toBigDecimal())
                .isEqualByComparingTo(maxDecimal.toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("10.50"));

        assertThat(minValues.getInt(9)).isEqualTo(maxValues.getInt(9)).isEqualTo(1000);
    }

    @Test
    void testMinMaxCalculation() throws IOException {
        // Test with data that has clear min/max values
        List<Object[]> minMaxData =
                Arrays.asList(
                        new Object[] {1, "a", 10.5},
                        new Object[] {5, "z", 30.1},
                        new Object[] {3, "m", 20.3});

        RowType minMaxRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        LogRecordBatchStatisticsCollector minMaxCollector =
                new LogRecordBatchStatisticsCollector(minMaxRowType);

        for (Object[] data : minMaxData) {
            InternalRow row = DataTestUtils.row(data);
            minMaxCollector.processRow(row);
        }

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = minMaxCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, minMaxRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(3);
        for (int i = 0; i < 3; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }

        // Verify min/max values
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // Field 0 (id): min=1, max=5
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(maxValues.getInt(0)).isEqualTo(5);

        // Field 1 (name): min="a", max="z" (lexicographic order)
        assertThat(minValues.getString(1).toString()).isEqualTo("a");
        assertThat(maxValues.getString(1).toString()).isEqualTo("z");

        // Field 2 (value): min=10.5, max=30.1
        assertThat(minValues.getDouble(2)).isEqualTo(10.5);
        assertThat(maxValues.getDouble(2)).isEqualTo(30.1);
    }

    @Test
    void testNullHandling() throws IOException {
        // Test with data containing nulls
        List<Object[]> nullData =
                Arrays.asList(
                        new Object[] {1, "a", 10.5},
                        new Object[] {null, "b", null},
                        new Object[] {3, null, 20.3});

        RowType nullRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        LogRecordBatchStatisticsCollector nullCollector =
                new LogRecordBatchStatisticsCollector(nullRowType);

        for (Object[] data : nullData) {
            InternalRow row = DataTestUtils.row(data);
            nullCollector.processRow(row);
        }

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = nullCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, nullRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(3);

        // Field 0 (id): 1 null out of 3 rows
        assertThat(nullCounts[0]).isEqualTo(1L);

        // Field 1 (name): 1 null out of 3 rows
        assertThat(nullCounts[1]).isEqualTo(1L);

        // Field 2 (value): 1 null out of 3 rows
        assertThat(nullCounts[2]).isEqualTo(1L);

        // Verify min/max values (should ignore nulls)
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // Field 0 (id): min=1, max=3 (excluding null)
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(maxValues.getInt(0)).isEqualTo(3);

        // Field 1 (name): min="a", max="b" (excluding null)
        assertThat(minValues.getString(1).toString()).isEqualTo("a");
        assertThat(maxValues.getString(1).toString()).isEqualTo("b");

        // Field 2 (value): min=10.5, max=20.3 (excluding null)
        assertThat(minValues.getDouble(2)).isEqualTo(10.5);
        assertThat(maxValues.getDouble(2)).isEqualTo(20.3);
    }

    @Test
    void testStringComparison() throws IOException {
        // Test string min/max calculation
        List<Object[]> stringData =
                Arrays.asList(
                        new Object[] {"zebra", "apple"},
                        new Object[] {"apple", "zebra"},
                        new Object[] {"banana", "cherry"});

        RowType stringRowType =
                DataTypes.ROW(
                        new DataField("str1", DataTypes.STRING()),
                        new DataField("str2", DataTypes.STRING()));

        LogRecordBatchStatisticsCollector stringCollector =
                new LogRecordBatchStatisticsCollector(stringRowType);

        for (Object[] data : stringData) {
            InternalRow row = DataTestUtils.row(data);
            stringCollector.processRow(row);
        }

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = stringCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, stringRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(2);
        for (int i = 0; i < 2; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }

        // Verify min/max values for string fields
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // Field 0 (str1): min="apple", max="zebra" (lexicographic order)
        assertThat(minValues.getString(0).toString()).isEqualTo("apple");
        assertThat(maxValues.getString(0).toString()).isEqualTo("zebra");

        // Field 1 (str2): min="apple", max="zebra" (lexicographic order)
        assertThat(minValues.getString(1).toString()).isEqualTo("apple");
        assertThat(maxValues.getString(1).toString()).isEqualTo("zebra");
    }

    @Test
    void testBooleanMinMax() throws IOException {
        // Test boolean min/max calculation (false < true)
        List<Object[]> booleanData =
                Arrays.asList(
                        new Object[] {true, false},
                        new Object[] {false, true},
                        new Object[] {true, true});

        RowType booleanRowType =
                DataTypes.ROW(
                        new DataField("bool1", DataTypes.BOOLEAN()),
                        new DataField("bool2", DataTypes.BOOLEAN()));

        LogRecordBatchStatisticsCollector booleanCollector =
                new LogRecordBatchStatisticsCollector(booleanRowType);

        for (Object[] data : booleanData) {
            InternalRow row = DataTestUtils.row(data);
            booleanCollector.processRow(row);
        }

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = booleanCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, booleanRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(2);
        for (int i = 0; i < 2; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }

        // Verify min/max values for boolean fields
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        // Field 0 (bool1): min=false, max=true (since we have both values)
        assertThat(minValues.getBoolean(0)).isFalse();
        assertThat(maxValues.getBoolean(0)).isTrue();

        // Field 1 (bool2): min=false, max=true (since we have both values)
        assertThat(minValues.getBoolean(1)).isFalse();
        assertThat(maxValues.getBoolean(1)).isTrue();
    }

    @Test
    void testLargeFieldCount() throws IOException {
        // Test with large number of fields
        DataField[] fields = new DataField[100];
        for (int i = 0; i < 100; i++) {
            fields[i] = new DataField("field" + i, DataTypes.INT());
        }

        RowType largeRowType = new RowType(Arrays.asList(fields));
        LogRecordBatchStatisticsCollector largeCollector =
                new LogRecordBatchStatisticsCollector(largeRowType);

        Object[] data = new Object[100];
        Arrays.fill(data, 42);
        InternalRow row = DataTestUtils.row(data);
        largeCollector.processRow(row);

        MemorySegment segment = new TestingMemorySegmentPool(8192).nextSegment();
        int bytesWritten = largeCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, largeRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - all fields should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(100);
        for (int i = 0; i < 100; i++) {
            assertThat(nullCounts[i]).isEqualTo(0L);
        }

        // Verify min/max values - all fields should have same min and max (42)
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNotNull();
        assertThat(maxValues).isNotNull();

        for (int i = 0; i < 100; i++) {
            assertThat(minValues.getInt(i)).isEqualTo(42);
            assertThat(maxValues.getInt(i)).isEqualTo(42);
        }
    }

    @Test
    void testUnsupportedDataType() throws IOException {
        // Test with unsupported data type (should not throw exception, just skip min/max
        // collection)
        RowType unsupportedRowType = DataTypes.ROW(new DataField("binary_val", DataTypes.BYTES()));

        LogRecordBatchStatisticsCollector unsupportedCollector =
                new LogRecordBatchStatisticsCollector(unsupportedRowType);

        Object[] data = {new byte[] {1, 2, 3}};
        InternalRow row = DataTestUtils.row(data);

        // Should not throw exception
        unsupportedCollector.processRow(row);

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten =
                unsupportedCollector.writeStatistics(new MemorySegmentOutputView(segment));

        // Should still write statistics (null counts)
        assertThat(bytesWritten).isGreaterThan(0);

        // Parse and verify statistics correctness
        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, unsupportedRowType);
        assertThat(statistics).isNotNull();

        // Verify null counts - should have 0 nulls
        Long[] nullCounts = statistics.getNullCounts();
        assertThat(nullCounts).hasSize(1);
        assertThat(nullCounts[0]).isEqualTo(0L);

        // For unsupported types, min/max values should be null
        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();
        assertThat(minValues).isNull();
        assertThat(maxValues).isNull();
    }

    @Test
    void testSimpleMinMax() throws IOException {
        // Create a simple collector with just one INT field
        RowType simpleRowType = DataTypes.ROW(new DataField("value", DataTypes.INT()));
        LogRecordBatchStatisticsCollector simpleCollector =
                new LogRecordBatchStatisticsCollector(simpleRowType);

        // Process just 3 rows with clear min/max
        simpleCollector.processRow(DataTestUtils.row(new Object[] {1}));
        simpleCollector.processRow(DataTestUtils.row(new Object[] {3}));
        simpleCollector.processRow(DataTestUtils.row(new Object[] {2}));

        MemorySegment segment = new TestingMemorySegmentPool(1024).nextSegment();
        int bytesWritten = simpleCollector.writeStatistics(new MemorySegmentOutputView(segment));

        assertThat(bytesWritten).isGreaterThan(0);

        LogRecordBatchStatistics statistics =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, bytesWritten, simpleRowType);
        assertThat(statistics).isNotNull();

        InternalRow minValues = statistics.getMinValues();
        InternalRow maxValues = statistics.getMaxValues();

        System.out.println(
                "Simple test - min: " + minValues.getInt(0) + ", max: " + maxValues.getInt(0));

        // Verify statistics correctness - should process all rows correctly
        assertThat(minValues.getInt(0)).isEqualTo(1); // min should be 1
        assertThat(maxValues.getInt(0)).isEqualTo(3); // max should be 3
        assertThat(statistics.getNullCounts()[0]).isEqualTo(0L); // no nulls
    }
}
