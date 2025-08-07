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

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.BooleanType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.StringType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LogRecordBatchStatistics} and {@link LogRecordBatchStatisticsSerializer}. */
public class LogRecordBatchStatisticsTest {

    @Test
    public void testStatisticsCollectorBasic() {
        RowType rowType =
                DataTypes.ROW(new IntType(false), new StringType(false), new BooleanType(false));

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process some rows
        collector.processRow(GenericRow.of(1, BinaryString.fromString("hello"), true));
        collector.processRow(GenericRow.of(2, BinaryString.fromString("world"), false));
        collector.processRow(GenericRow.of(3, BinaryString.fromString("test"), true));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check min values
        assertThat(stats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(stats.getMinValues().getString(1).toString()).isEqualTo("hello");
        assertThat(stats.getMinValues().getBoolean(2)).isEqualTo(false);

        // Check max values
        assertThat(stats.getMaxValues().getInt(0)).isEqualTo(3);
        assertThat(stats.getMaxValues().getString(1).toString()).isEqualTo("world");
        assertThat(stats.getMaxValues().getBoolean(2)).isEqualTo(true);

        // Check null counts (should be 0 for all fields)
        assertThat(stats.getNullCounts()[0]).isEqualTo(0);
        assertThat(stats.getNullCounts()[1]).isEqualTo(0);
        assertThat(stats.getNullCounts()[2]).isEqualTo(0);
    }

    @Test
    public void testStatisticsCollectorWithNulls() {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with some null values
        Object[] row1 = new Object[2];
        row1[0] = 1;
        row1[1] = BinaryString.fromString("hello");
        collector.processRow(GenericRow.of(row1));

        Object[] row2 = new Object[2];
        row2[0] = null;
        row2[1] = BinaryString.fromString("world");
        collector.processRow(GenericRow.of(row2));

        Object[] row3 = new Object[2];
        row3[0] = 3;
        row3[1] = null;
        collector.processRow(GenericRow.of(row3));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check null counts
        assertThat(stats.getNullCounts()[0]).isEqualTo(1); // one null in first column
        assertThat(stats.getNullCounts()[1]).isEqualTo(1); // one null in second column

        // Check min/max values (should only consider non-null values)
        assertThat(stats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(stats.getMaxValues().getInt(0)).isEqualTo(3);
        assertThat(stats.getMinValues().getString(1).toString()).isEqualTo("hello");
        assertThat(stats.getMaxValues().getString(1).toString()).isEqualTo("world");
    }

    @Test
    public void testStatisticsSerializationDeserialization() throws IOException {
        RowType rowType =
                DataTypes.ROW(new IntType(false), new StringType(false), new BooleanType(false));

        // Create statistics
        InternalRow minValues =
                GenericRow.of(new Object[] {1, BinaryString.fromString("hello"), false});
        InternalRow maxValues =
                GenericRow.of(new Object[] {3, BinaryString.fromString("world"), true});
        Long[] nullCounts = new Long[] {0L, 0L, 0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        assertThat(serialized).isNotEmpty();

        // Deserialize
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);
        assertThat(deserializedStats).isNotNull();

        // Check min values
        assertThat(deserializedStats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(deserializedStats.getMinValues().getString(1).toString()).isEqualTo("hello");
        assertThat(deserializedStats.getMinValues().getBoolean(2)).isEqualTo(false);

        // Check max values
        assertThat(deserializedStats.getMaxValues().getInt(0)).isEqualTo(3);
        assertThat(deserializedStats.getMaxValues().getString(1).toString()).isEqualTo("world");
        assertThat(deserializedStats.getMaxValues().getBoolean(2)).isEqualTo(true);

        // Check null counts
        assertThat(deserializedStats.getNullCounts()[0]).isEqualTo(0);
        assertThat(deserializedStats.getNullCounts()[1]).isEqualTo(0);
        assertThat(deserializedStats.getNullCounts()[2]).isEqualTo(0);
    }

    @Test
    public void testEmptyStatisticsSerialization() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT());

        // Test null statistics
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(null, rowType);
        assertThat(serialized).isEmpty();

        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);
        assertThat(deserializedStats).isNull();

        // Test empty data
        LogRecordBatchStatistics emptyStats =
                LogRecordBatchStatisticsSerializer.deserialize(new byte[0], rowType);
        assertThat(emptyStats).isNull();
    }

    // ==================== 新增的序列化反序列化测试 ====================

    @Test
    public void testAllNumericTypesSerialization() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE());

        // Create statistics with all numeric types
        InternalRow minValues =
                GenericRow.of(
                        (byte) -128,
                        (short) -32768,
                        -2147483648,
                        -9223372036854775808L,
                        -3.4028235E38f,
                        -1.7976931348623157E308);
        InternalRow maxValues =
                GenericRow.of(
                        (byte) 127,
                        (short) 32767,
                        2147483647,
                        9223372036854775807L,
                        3.4028235E38f,
                        1.7976931348623157E308);
        Long[] nullCounts = new Long[] {0L, 0L, 0L, 0L, 0L, 0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify all numeric types
        assertThat(deserializedStats.getMinValues().getByte(0)).isEqualTo((byte) -128);
        assertThat(deserializedStats.getMinValues().getShort(1)).isEqualTo((short) -32768);
        assertThat(deserializedStats.getMinValues().getInt(2)).isEqualTo(-2147483648);
        assertThat(deserializedStats.getMinValues().getLong(3)).isEqualTo(-9223372036854775808L);
        assertThat(deserializedStats.getMinValues().getFloat(4)).isEqualTo(-3.4028235E38f);
        assertThat(deserializedStats.getMinValues().getDouble(5))
                .isEqualTo(-1.7976931348623157E308);

        assertThat(deserializedStats.getMaxValues().getByte(0)).isEqualTo((byte) 127);
        assertThat(deserializedStats.getMaxValues().getShort(1)).isEqualTo((short) 32767);
        assertThat(deserializedStats.getMaxValues().getInt(2)).isEqualTo(2147483647);
        assertThat(deserializedStats.getMaxValues().getLong(3)).isEqualTo(9223372036854775807L);
        assertThat(deserializedStats.getMaxValues().getFloat(4)).isEqualTo(3.4028235E38f);
        assertThat(deserializedStats.getMaxValues().getDouble(5)).isEqualTo(1.7976931348623157E308);
    }

    @Test
    public void testStringSerializationWithSpecialCharacters() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.STRING());

        // Test strings with special characters
        String testString = "Hello\nWorld\tTest\r\n\"Quote\"'Single'\\Backslash";
        InternalRow minValues = GenericRow.of(BinaryString.fromString(testString));
        InternalRow maxValues = GenericRow.of(BinaryString.fromString(testString));
        Long[] nullCounts = new Long[] {0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify string content
        assertThat(deserializedStats.getMinValues().getString(0).toString()).isEqualTo(testString);
        assertThat(deserializedStats.getMaxValues().getString(0).toString()).isEqualTo(testString);
    }

    @Test
    public void testBooleanSerialization() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.BOOLEAN());

        // Test all boolean combinations
        InternalRow minValues = GenericRow.of(false);
        InternalRow maxValues = GenericRow.of(true);
        Long[] nullCounts = new Long[] {0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify boolean values
        assertThat(deserializedStats.getMinValues().getBoolean(0)).isFalse();
        assertThat(deserializedStats.getMaxValues().getBoolean(0)).isTrue();
    }

    @Test
    public void testNullValuesSerialization() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN());

        // Create statistics with null values
        InternalRow minValues = GenericRow.of(1, null, false);
        InternalRow maxValues = GenericRow.of(3, null, true);
        Long[] nullCounts = new Long[] {0L, 2L, 0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify null handling
        assertThat(deserializedStats.getMinValues().isNullAt(1)).isTrue();
        assertThat(deserializedStats.getMaxValues().isNullAt(1)).isTrue();
        assertThat(deserializedStats.getNullCounts()[1]).isEqualTo(2);
    }

    @Test
    public void testLargeNullCounts() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT());

        // Test with large null counts
        InternalRow minValues = GenericRow.of(1);
        InternalRow maxValues = GenericRow.of(100);
        Long[] nullCounts = new Long[] {Long.MAX_VALUE};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify large null count
        assertThat(deserializedStats.getNullCounts()[0]).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    public void testMixedNullAndNonNullValues() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING(), DataTypes.BOOLEAN());

        // Create statistics with mixed null and non-null values
        InternalRow minValues = GenericRow.of(1, BinaryString.fromString("min"), false);
        InternalRow maxValues = GenericRow.of(100, BinaryString.fromString("max"), true);
        Long[] nullCounts = new Long[] {0L, 5L, 10L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify mixed values
        assertThat(deserializedStats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(deserializedStats.getMinValues().getString(1).toString()).isEqualTo("min");
        assertThat(deserializedStats.getMinValues().getBoolean(2)).isFalse();

        assertThat(deserializedStats.getMaxValues().getInt(0)).isEqualTo(100);
        assertThat(deserializedStats.getMaxValues().getString(1).toString()).isEqualTo("max");
        assertThat(deserializedStats.getMaxValues().getBoolean(2)).isTrue();

        assertThat(deserializedStats.getNullCounts()[0]).isEqualTo(0);
        assertThat(deserializedStats.getNullCounts()[1]).isEqualTo(5);
        assertThat(deserializedStats.getNullCounts()[2]).isEqualTo(10);
    }

    @Test
    public void testEmptyStringSerialization() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.STRING());

        // Test empty string
        InternalRow minValues = GenericRow.of(BinaryString.fromString(""));
        InternalRow maxValues = GenericRow.of(BinaryString.fromString(""));
        Long[] nullCounts = new Long[] {0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify empty string
        assertThat(deserializedStats.getMinValues().getString(0).toString()).isEmpty();
        assertThat(deserializedStats.getMaxValues().getString(0).toString()).isEmpty();
    }

    @Test
    public void testUnicodeStringSerialization() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.STRING());

        // Test Unicode strings
        String unicodeString = "Hello world 测试 🚀";
        InternalRow minValues = GenericRow.of(BinaryString.fromString(unicodeString));
        InternalRow maxValues = GenericRow.of(BinaryString.fromString(unicodeString));
        Long[] nullCounts = new Long[] {0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify Unicode string
        assertThat(deserializedStats.getMinValues().getString(0).toString())
                .isEqualTo(unicodeString);
        assertThat(deserializedStats.getMaxValues().getString(0).toString())
                .isEqualTo(unicodeString);
    }

    @Test
    public void testZeroValuesSerialization() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.FLOAT(), DataTypes.DOUBLE());

        // Test zero values
        InternalRow minValues = GenericRow.of(0, 0.0f, 0.0);
        InternalRow maxValues = GenericRow.of(0, 0.0f, 0.0);
        Long[] nullCounts = new Long[] {0L, 0L, 0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify zero values
        assertThat(deserializedStats.getMinValues().getInt(0)).isEqualTo(0);
        assertThat(deserializedStats.getMinValues().getFloat(1)).isEqualTo(0.0f);
        assertThat(deserializedStats.getMinValues().getDouble(2)).isEqualTo(0.0);
    }

    @Test
    public void testNegativeValuesSerialization() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.FLOAT(), DataTypes.DOUBLE());

        // Test negative values
        InternalRow minValues = GenericRow.of(-100, -3.14f, -2.718);
        InternalRow maxValues = GenericRow.of(-1, -0.001f, -0.0001);
        Long[] nullCounts = new Long[] {0L, 0L, 0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify negative values
        assertThat(deserializedStats.getMinValues().getInt(0)).isEqualTo(-100);
        assertThat(deserializedStats.getMinValues().getFloat(1)).isEqualTo(-3.14f);
        assertThat(deserializedStats.getMinValues().getDouble(2)).isEqualTo(-2.718);

        assertThat(deserializedStats.getMaxValues().getInt(0)).isEqualTo(-1);
        assertThat(deserializedStats.getMaxValues().getFloat(1)).isEqualTo(-0.001f);
        assertThat(deserializedStats.getMaxValues().getDouble(2)).isEqualTo(-0.0001);
    }

    @Test
    public void testFieldCountMismatch() {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());

        // Create invalid data with wrong field count
        byte[] invalidData = {1, 0, 3}; // Version 1, field count 3 (but rowType has 2 fields)

        assertThatThrownBy(
                        () -> LogRecordBatchStatisticsSerializer.deserialize(invalidData, rowType))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Field count mismatch");
    }

    @Test
    public void testUnsupportedVersion() {
        RowType rowType = DataTypes.ROW(DataTypes.INT());

        // Create invalid data with unsupported version
        byte[] invalidData = {99, 0, 1}; // Version 99, field count 1

        assertThatThrownBy(
                        () -> LogRecordBatchStatisticsSerializer.deserialize(invalidData, rowType))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported statistics version: 99");
    }

    @Test
    public void testStatisticsCollectorReset() {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process some rows
        collector.processRow(GenericRow.of(1, BinaryString.fromString("first")));
        collector.processRow(GenericRow.of(2, BinaryString.fromString("second")));

        LogRecordBatchStatistics stats1 = collector.getStatistics();
        assertThat(stats1).isNotNull();
        assertThat(stats1.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(stats1.getMaxValues().getInt(0)).isEqualTo(2);

        // Reset collector
        collector.reset();

        // Process new rows
        collector.processRow(GenericRow.of(10, BinaryString.fromString("reset")));
        collector.processRow(GenericRow.of(20, BinaryString.fromString("test")));

        LogRecordBatchStatistics stats2 = collector.getStatistics();
        assertThat(stats2).isNotNull();
        assertThat(stats2.getMinValues().getInt(0)).isEqualTo(10);
        assertThat(stats2.getMaxValues().getInt(0)).isEqualTo(20);
    }

    @Test
    public void testStatisticsCollectorEmpty() {
        RowType rowType = DataTypes.ROW(DataTypes.INT());

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // No rows processed
        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNull();
    }

    @Test
    public void testDefaultLogRecordBatchStatisticsToString() {
        InternalRow minValues = GenericRow.of(1, BinaryString.fromString("min"));
        InternalRow maxValues = GenericRow.of(100, BinaryString.fromString("max"));
        Long[] nullCounts = new Long[] {0L, 5L};

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        String toString = stats.toString();
        assertThat(toString).contains("DefaultLogRecordBatchStatistics");
        assertThat(toString).contains("minValues=");
        assertThat(toString).contains("maxValues=");
        assertThat(toString).contains("nullCounts=");
    }

    // ==================== 新增的数据类型测试 ====================

    @Test
    public void testDecimalTypeStatistics() {
        RowType rowType = DataTypes.ROW(DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(20, 5));

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with decimal values
        collector.processRow(
                GenericRow.of(
                        Decimal.fromBigDecimal(new java.math.BigDecimal("123.45"), 10, 2),
                        Decimal.fromBigDecimal(
                                new java.math.BigDecimal("1234567890.12345"), 20, 5)));
        collector.processRow(
                GenericRow.of(
                        Decimal.fromBigDecimal(new java.math.BigDecimal("67.89"), 10, 2),
                        Decimal.fromBigDecimal(
                                new java.math.BigDecimal("9876543210.98765"), 20, 5)));
        collector.processRow(
                GenericRow.of(
                        Decimal.fromBigDecimal(new java.math.BigDecimal("999.99"), 10, 2),
                        Decimal.fromBigDecimal(
                                new java.math.BigDecimal("1111111111.11111"), 20, 5)));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check min values
        assertThat(stats.getMinValues().getDecimal(0, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("67.89"));
        assertThat(stats.getMinValues().getDecimal(1, 20, 5).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("1111111111.11111"));

        // Check max values
        assertThat(stats.getMaxValues().getDecimal(0, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("999.99"));
        assertThat(stats.getMaxValues().getDecimal(1, 20, 5).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("9876543210.98765"));

        // Check null counts
        assertThat(stats.getNullCounts()[0]).isEqualTo(0);
        assertThat(stats.getNullCounts()[1]).isEqualTo(0);
    }

    @Test
    public void testDateTypeStatistics() {
        RowType rowType = DataTypes.ROW(DataTypes.DATE());

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with date values (days since epoch)
        collector.processRow(GenericRow.of((int) LocalDate.of(2023, 1, 1).toEpochDay()));
        collector.processRow(GenericRow.of((int) LocalDate.of(2023, 6, 15).toEpochDay()));
        collector.processRow(GenericRow.of((int) LocalDate.of(2023, 12, 31).toEpochDay()));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check min/max values
        assertThat(stats.getMinValues().getInt(0))
                .isEqualTo((int) LocalDate.of(2023, 1, 1).toEpochDay());
        assertThat(stats.getMaxValues().getInt(0))
                .isEqualTo((int) LocalDate.of(2023, 12, 31).toEpochDay());

        // Check null counts
        assertThat(stats.getNullCounts()[0]).isEqualTo(0);
    }

    @Test
    public void testTimeTypeStatistics() {
        RowType rowType = DataTypes.ROW(DataTypes.TIME(3));

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with time values (milliseconds of day)
        collector.processRow(
                GenericRow.of((int) (LocalTime.of(9, 30, 0).toNanoOfDay() / 1_000_000)));
        collector.processRow(
                GenericRow.of((int) (LocalTime.of(12, 0, 0).toNanoOfDay() / 1_000_000)));
        collector.processRow(
                GenericRow.of((int) (LocalTime.of(18, 45, 30).toNanoOfDay() / 1_000_000)));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check min/max values
        assertThat(stats.getMinValues().getInt(0))
                .isEqualTo((int) (LocalTime.of(9, 30, 0).toNanoOfDay() / 1_000_000));
        assertThat(stats.getMaxValues().getInt(0))
                .isEqualTo((int) (LocalTime.of(18, 45, 30).toNanoOfDay() / 1_000_000));

        // Check null counts
        assertThat(stats.getNullCounts()[0]).isEqualTo(0);
    }

    @Test
    public void testTimestampNtzTypeStatistics() {
        RowType rowType = DataTypes.ROW(DataTypes.TIMESTAMP(6));

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with timestamp values
        collector.processRow(
                GenericRow.of(
                        TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 9, 30, 0))));
        collector.processRow(
                GenericRow.of(
                        TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 6, 15, 12, 0, 0))));
        collector.processRow(
                GenericRow.of(
                        TimestampNtz.fromLocalDateTime(
                                LocalDateTime.of(2023, 12, 31, 18, 45, 30))));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check min/max values
        assertThat(stats.getMinValues().getTimestampNtz(0, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 9, 30, 0));
        assertThat(stats.getMaxValues().getTimestampNtz(0, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 31, 18, 45, 30));

        // Check null counts
        assertThat(stats.getNullCounts()[0]).isEqualTo(0);
    }

    @Test
    public void testTimestampLtzTypeStatistics() {
        RowType rowType = DataTypes.ROW(DataTypes.TIMESTAMP_LTZ(6));

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with timestamp with local time zone values
        collector.processRow(
                GenericRow.of(
                        TimestampLtz.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 9, 30, 0))));
        collector.processRow(
                GenericRow.of(
                        TimestampLtz.fromLocalDateTime(LocalDateTime.of(2023, 6, 15, 12, 0, 0))));
        collector.processRow(
                GenericRow.of(
                        TimestampLtz.fromLocalDateTime(
                                LocalDateTime.of(2023, 12, 31, 18, 45, 30))));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check min/max values
        assertThat(stats.getMinValues().getTimestampLtz(0, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 9, 30, 0));
        assertThat(stats.getMaxValues().getTimestampLtz(0, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 31, 18, 45, 30));

        // Check null counts
        assertThat(stats.getNullCounts()[0]).isEqualTo(0);
    }

    @Test
    public void testAllNewTypesSerialization() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.DATE(),
                        DataTypes.TIME(3),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP_LTZ(6));

        // Create statistics with all new types
        InternalRow minValues =
                GenericRow.of(
                        Decimal.fromBigDecimal(new java.math.BigDecimal("123.45"), 10, 2),
                        (int) LocalDate.of(2023, 1, 1).toEpochDay(),
                        (int) (LocalTime.of(9, 30, 0).toNanoOfDay() / 1_000_000),
                        TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 9, 30, 0)),
                        TimestampLtz.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 9, 30, 0)));

        InternalRow maxValues =
                GenericRow.of(
                        Decimal.fromBigDecimal(new java.math.BigDecimal("999.99"), 10, 2),
                        (int) LocalDate.of(2023, 12, 31).toEpochDay(),
                        (int) (LocalTime.of(18, 45, 30).toNanoOfDay() / 1_000_000),
                        TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 12, 31, 18, 45, 30)),
                        TimestampLtz.fromLocalDateTime(LocalDateTime.of(2023, 12, 31, 18, 45, 30)));

        Long[] nullCounts = new Long[] {0L, 0L, 0L, 0L, 0L};

        LogRecordBatchStatistics originalStats =
                new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);

        // Serialize and deserialize
        byte[] serialized = LogRecordBatchStatisticsSerializer.serialize(originalStats, rowType);
        LogRecordBatchStatistics deserializedStats =
                LogRecordBatchStatisticsSerializer.deserialize(serialized, rowType);

        // Verify all new types
        assertThat(deserializedStats.getMinValues().getDecimal(0, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("123.45"));
        assertThat(deserializedStats.getMinValues().getInt(1))
                .isEqualTo((int) LocalDate.of(2023, 1, 1).toEpochDay());
        assertThat(deserializedStats.getMinValues().getInt(2))
                .isEqualTo((int) (LocalTime.of(9, 30, 0).toNanoOfDay() / 1_000_000));
        assertThat(deserializedStats.getMinValues().getTimestampNtz(3, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 9, 30, 0));
        assertThat(deserializedStats.getMinValues().getTimestampLtz(4, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 9, 30, 0));

        assertThat(deserializedStats.getMaxValues().getDecimal(0, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("999.99"));
        assertThat(deserializedStats.getMaxValues().getInt(1))
                .isEqualTo((int) LocalDate.of(2023, 12, 31).toEpochDay());
        assertThat(deserializedStats.getMaxValues().getInt(2))
                .isEqualTo((int) (LocalTime.of(18, 45, 30).toNanoOfDay() / 1_000_000));
        assertThat(deserializedStats.getMaxValues().getTimestampNtz(3, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 31, 18, 45, 30));
        assertThat(deserializedStats.getMaxValues().getTimestampLtz(4, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 31, 18, 45, 30));
    }

    @Test
    public void testDecimalWithNulls() {
        RowType rowType = DataTypes.ROW(DataTypes.DECIMAL(10, 2));

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with some null decimal values
        collector.processRow(
                GenericRow.of(Decimal.fromBigDecimal(new java.math.BigDecimal("123.45"), 10, 2)));
        collector.processRow(GenericRow.of((Object) null));
        collector.processRow(
                GenericRow.of(Decimal.fromBigDecimal(new java.math.BigDecimal("999.99"), 10, 2)));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check null count
        assertThat(stats.getNullCounts()[0]).isEqualTo(1);

        // Check min/max values (should only consider non-null values)
        assertThat(stats.getMinValues().getDecimal(0, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("123.45"));
        assertThat(stats.getMaxValues().getDecimal(0, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("999.99"));
    }

    @Test
    public void testTimestampWithDifferentPrecisions() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP_LTZ(9));

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with timestamps of different precisions
        collector.processRow(
                GenericRow.of(
                        TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 9, 30, 0)),
                        TimestampNtz.fromLocalDateTime(
                                LocalDateTime.of(2023, 1, 1, 9, 30, 0, 123456000)),
                        TimestampLtz.fromLocalDateTime(
                                LocalDateTime.of(2023, 1, 1, 9, 30, 0, 123456789))));

        collector.processRow(
                GenericRow.of(
                        TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 12, 31, 18, 45, 30)),
                        TimestampNtz.fromLocalDateTime(
                                LocalDateTime.of(2023, 12, 31, 18, 45, 30, 654321000)),
                        TimestampLtz.fromLocalDateTime(
                                LocalDateTime.of(2023, 12, 31, 18, 45, 30, 987654321))));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check min/max values for different precisions
        assertThat(stats.getMinValues().getTimestampNtz(0, 3).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 9, 30, 0));
        assertThat(stats.getMaxValues().getTimestampNtz(0, 3).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 31, 18, 45, 30));

        assertThat(stats.getMinValues().getTimestampNtz(1, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 9, 30, 0, 123456000));
        assertThat(stats.getMaxValues().getTimestampNtz(1, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 31, 18, 45, 30, 654321000));

        assertThat(stats.getMinValues().getTimestampLtz(2, 9).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 9, 30, 0, 123456789));
        assertThat(stats.getMaxValues().getTimestampLtz(2, 9).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 31, 18, 45, 30, 987654321));
    }

    @Test
    public void testMixedTypesWithNewTypes() {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.INT(),
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.STRING());

        LogRecordBatchStatisticsCollector collector =
                new LogRecordBatchStatisticsCollector(rowType);

        // Process rows with mixed types
        collector.processRow(
                GenericRow.of(
                        1,
                        Decimal.fromBigDecimal(new java.math.BigDecimal("123.45"), 10, 2),
                        (int) LocalDate.of(2023, 1, 1).toEpochDay(),
                        TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 1, 1, 9, 30, 0)),
                        BinaryString.fromString("first")));

        collector.processRow(
                GenericRow.of(
                        100,
                        Decimal.fromBigDecimal(new java.math.BigDecimal("999.99"), 10, 2),
                        (int) LocalDate.of(2023, 12, 31).toEpochDay(),
                        TimestampNtz.fromLocalDateTime(LocalDateTime.of(2023, 12, 31, 18, 45, 30)),
                        BinaryString.fromString("last")));

        LogRecordBatchStatistics stats = collector.getStatistics();
        assertThat(stats).isNotNull();

        // Check all types work together
        assertThat(stats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(stats.getMinValues().getDecimal(1, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("123.45"));
        assertThat(stats.getMinValues().getInt(2))
                .isEqualTo((int) LocalDate.of(2023, 1, 1).toEpochDay());
        assertThat(stats.getMinValues().getTimestampNtz(3, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 1, 1, 9, 30, 0));
        assertThat(stats.getMinValues().getString(4).toString()).isEqualTo("first");

        assertThat(stats.getMaxValues().getInt(0)).isEqualTo(100);
        assertThat(stats.getMaxValues().getDecimal(1, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new java.math.BigDecimal("999.99"));
        assertThat(stats.getMaxValues().getInt(2))
                .isEqualTo((int) LocalDate.of(2023, 12, 31).toEpochDay());
        assertThat(stats.getMaxValues().getTimestampNtz(3, 6).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 31, 18, 45, 30));
        assertThat(stats.getMaxValues().getString(4).toString()).isEqualTo("last");
    }
}
