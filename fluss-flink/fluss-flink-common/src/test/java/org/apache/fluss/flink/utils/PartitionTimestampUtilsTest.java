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

package org.apache.fluss.flink.utils;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.ZoneId;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionTimestampUtils}. */
class PartitionTimestampUtilsTest {

    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");

    @ParameterizedTest
    @MethodSource("provideTimestampMappingTestCases")
    void testMapTimestampToPartition(
            long timestampMs,
            AutoPartitionTimeUnit timeUnit,
            ZoneId timeZone,
            String expectedPartition) {
        String actualPartition =
                PartitionTimestampUtils.mapTimestampToPartition(timestampMs, timeUnit, timeZone);
        assertThat(actualPartition).isEqualTo(expectedPartition);
    }

    private static Stream<Arguments> provideTimestampMappingTestCases() {
        // 2026-01-19 12:30:00 UTC = 1768926600000 ms
        // 2026-01-19 20:30:00 Asia/Shanghai (UTC+8) = 1768926600000 ms
        long timestamp = 1768926600000L;

        return Stream.of(
                // HOUR time unit - different time zones
                Arguments.of(timestamp, AutoPartitionTimeUnit.HOUR, UTC, "2026011912"),
                Arguments.of(timestamp, AutoPartitionTimeUnit.HOUR, SHANGHAI, "2026011920"),

                // DAY time unit - different time zones
                Arguments.of(timestamp, AutoPartitionTimeUnit.DAY, UTC, "20260119"),
                Arguments.of(timestamp, AutoPartitionTimeUnit.DAY, SHANGHAI, "20260119"),

                // MONTH time unit
                Arguments.of(timestamp, AutoPartitionTimeUnit.MONTH, UTC, "202601"),
                Arguments.of(timestamp, AutoPartitionTimeUnit.MONTH, SHANGHAI, "202601"),

                // QUARTER time unit
                Arguments.of(timestamp, AutoPartitionTimeUnit.QUARTER, UTC, "20261"),
                Arguments.of(timestamp, AutoPartitionTimeUnit.QUARTER, SHANGHAI, "20261"),

                // YEAR time unit
                Arguments.of(timestamp, AutoPartitionTimeUnit.YEAR, UTC, "2026"),
                Arguments.of(timestamp, AutoPartitionTimeUnit.YEAR, SHANGHAI, "2026"));
    }

    @Test
    void testMapTimestampToPartitionWithDayBoundary() {
        // Test timestamp at exactly midnight UTC
        // 2026-01-19 00:00:00 UTC = 1768881600000 ms
        long midnightUtc = 1768881600000L;

        assertThat(
                        PartitionTimestampUtils.mapTimestampToPartition(
                                midnightUtc, AutoPartitionTimeUnit.DAY, UTC))
                .isEqualTo("20260119");

        // Same timestamp in Asia/Shanghai is 2026-01-19 08:00:00
        assertThat(
                        PartitionTimestampUtils.mapTimestampToPartition(
                                midnightUtc, AutoPartitionTimeUnit.DAY, SHANGHAI))
                .isEqualTo("20260119");

        // Same timestamp in Asia/Shanghai is 2026-01-19 08:00:00 - hour partition
        assertThat(
                        PartitionTimestampUtils.mapTimestampToPartition(
                                midnightUtc, AutoPartitionTimeUnit.HOUR, SHANGHAI))
                .isEqualTo("2026011908");
    }

    @Test
    void testCreatePartitionFilter() {
        RowType partitionRowType = RowType.builder().field("dt", DataTypes.STRING()).build();
        String autoPartitionKey = "dt";
        String targetPartition = "20260119";

        Predicate filter =
                PartitionTimestampUtils.createPartitionFilter(
                        partitionRowType, autoPartitionKey, targetPartition);

        assertThat(filter).isInstanceOf(LeafPredicate.class);
        LeafPredicate leafPredicate = (LeafPredicate) filter;

        // Verify the predicate is a GreaterOrEqual
        assertThat(leafPredicate.function().name()).isEqualTo(">=");
        assertThat(leafPredicate.fieldIndex()).isEqualTo(0);
        assertThat(leafPredicate.literals()).hasSize(1);
        assertThat(leafPredicate.literals().get(0))
                .isEqualTo(BinaryString.fromString(targetPartition));
    }

    @Test
    void testCreatePartitionFilterWithMultiplePartitionColumns() {
        // Table with multiple partition columns: region, dt
        RowType partitionRowType =
                RowType.builder()
                        .field("region", DataTypes.STRING())
                        .field("dt", DataTypes.STRING())
                        .build();
        String autoPartitionKey = "dt"; // Only filter on auto-partition key
        String targetPartition = "20260119";

        Predicate filter =
                PartitionTimestampUtils.createPartitionFilter(
                        partitionRowType, autoPartitionKey, targetPartition);

        assertThat(filter).isInstanceOf(LeafPredicate.class);
        LeafPredicate leafPredicate = (LeafPredicate) filter;

        // Verify the predicate filters on 'dt' (index 1), not 'region' (index 0)
        assertThat(leafPredicate.function().name()).isEqualTo(">=");
        assertThat(leafPredicate.fieldIndex()).isEqualTo(1); // 'dt' is at index 1
        assertThat(leafPredicate.fieldName()).isEqualTo("dt");
    }

    @Test
    void testPartitionFilterEvaluation() {
        RowType partitionRowType = RowType.builder().field("dt", DataTypes.STRING()).build();
        String autoPartitionKey = "dt";
        String targetPartition = "20260119";

        Predicate filter =
                PartitionTimestampUtils.createPartitionFilter(
                        partitionRowType, autoPartitionKey, targetPartition);

        // Test filter evaluation with different partition values
        // Partition "20260119" should match (equal)
        assertThat(filter.test(createPartitionRow("20260119"))).isTrue();

        // Partition "20260120" should match (greater)
        assertThat(filter.test(createPartitionRow("20260120"))).isTrue();

        // Partition "20260118" should NOT match (less)
        assertThat(filter.test(createPartitionRow("20260118"))).isFalse();

        // Partition "20260201" should match (greater)
        assertThat(filter.test(createPartitionRow("20260201"))).isTrue();
    }

    @Test
    void testPartitionFilterEvaluationWithMultipleColumns() {
        // Partition row type with two columns
        RowType partitionRowType =
                RowType.builder()
                        .field("region", DataTypes.STRING())
                        .field("dt", DataTypes.STRING())
                        .build();
        String autoPartitionKey = "dt";
        String targetPartition = "20260119";

        Predicate filter =
                PartitionTimestampUtils.createPartitionFilter(
                        partitionRowType, autoPartitionKey, targetPartition);

        // Test filter evaluation - should only check 'dt' column
        assertThat(filter.test(createMultiColumnPartitionRow("us-east", "20260119"))).isTrue();
        assertThat(filter.test(createMultiColumnPartitionRow("us-west", "20260120"))).isTrue();
        assertThat(filter.test(createMultiColumnPartitionRow("eu-west", "20260118"))).isFalse();
    }

    private org.apache.fluss.row.InternalRow createPartitionRow(String partitionValue) {
        org.apache.fluss.row.GenericRow row = new org.apache.fluss.row.GenericRow(1);
        row.setField(0, BinaryString.fromString(partitionValue));
        return row;
    }

    private org.apache.fluss.row.InternalRow createMultiColumnPartitionRow(
            String region, String dt) {
        org.apache.fluss.row.GenericRow row = new org.apache.fluss.row.GenericRow(2);
        row.setField(0, BinaryString.fromString(region));
        row.setField(1, BinaryString.fromString(dt));
        return row;
    }
}
