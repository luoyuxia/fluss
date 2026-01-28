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
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.PartitionUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Utility class for partition-timestamp scan startup mode.
 *
 * <p>Provides methods to map a timestamp to a partition value and create partition filters.
 */
public class PartitionTimestampUtils {

    /**
     * Maps a timestamp to a partition value string based on auto-partition time unit.
     *
     * <p>Uses the same format as {@link PartitionUtils#generateAutoPartitionTime}.
     *
     * @param timestampMs the timestamp in milliseconds
     * @param timeUnit the auto-partition time unit (HOUR, DAY, MONTH, QUARTER, YEAR)
     * @param timeZone the time zone for the conversion
     * @return the partition value string (e.g., "20260119" for DAY)
     */
    public static String mapTimestampToPartition(
            long timestampMs, AutoPartitionTimeUnit timeUnit, ZoneId timeZone) {
        ZonedDateTime dateTime = Instant.ofEpochMilli(timestampMs).atZone(timeZone);
        return PartitionUtils.generateAutoPartitionTime(dateTime, 0, timeUnit);
    }

    /**
     * Creates a partition filter predicate: autoPartitionKey >= targetPartition.
     *
     * <p>This filter is used to prune partitions in both Fluss and Data Lake sources. For tables
     * with multiple partition columns, this only filters on the auto-partition key column.
     *
     * @param partitionRowType the row type of all partition columns
     * @param autoPartitionKey the name of the auto-partition key column
     * @param targetPartitionValue the target partition value to filter from
     * @return the predicate representing autoPartitionKey >= targetPartitionValue
     */
    public static Predicate createPartitionFilter(
            RowType partitionRowType, String autoPartitionKey, String targetPartitionValue) {
        int fieldIndex = partitionRowType.getFieldIndex(autoPartitionKey);
        if (fieldIndex < 0) {
            throw new IllegalArgumentException(
                    "Auto-partition key '" + autoPartitionKey + "' not found in partition columns: "
                            + partitionRowType.getFieldNames());
        }
        PredicateBuilder builder = new PredicateBuilder(partitionRowType);
        return builder.greaterOrEqual(fieldIndex, BinaryString.fromString(targetPartitionValue));
    }
}
