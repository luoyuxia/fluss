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

package org.apache.fluss.utils;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.metadata.TablePath.detectInvalidName;
import static org.apache.fluss.metadata.TablePath.validatePrefix;

/** Utils for partition. */
public class PartitionUtils {

    public static final String HISTORICAL_PARTITION_VALUE = "__historical__";

    public static final List<DataTypeRoot> PARTITION_KEY_SUPPORTED_TYPES =
            Arrays.asList(
                    DataTypeRoot.CHAR,
                    DataTypeRoot.STRING,
                    DataTypeRoot.BOOLEAN,
                    DataTypeRoot.BINARY,
                    DataTypeRoot.BYTES,
                    DataTypeRoot.TINYINT,
                    DataTypeRoot.SMALLINT,
                    DataTypeRoot.INTEGER,
                    DataTypeRoot.DATE,
                    DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    DataTypeRoot.BIGINT,
                    DataTypeRoot.FLOAT,
                    DataTypeRoot.DOUBLE,
                    DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                    DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

    private static final String YEAR_FORMAT = "yyyy";
    private static final String QUARTER_FORMAT = "yyyyQ";
    private static final String MONTH_FORMAT = "yyyyMM";
    private static final String DAY_FORMAT = "yyyyMMdd";
    private static final String HOUR_FORMAT = "yyyyMMddHH";

    public static void validatePartitionSpec(
            TablePath tablePath,
            List<String> partitionKeys,
            PartitionSpec partitionSpec,
            boolean isCreate) {
        Map<String, String> partitionSpecMap = partitionSpec.getSpecMap();
        if (partitionKeys.size() != partitionSpecMap.size()) {
            throw new InvalidPartitionException(
                    String.format(
                            "PartitionSpec size is not equal to partition keys size for partitioned table %s.",
                            tablePath));
        }

        List<String> reOrderedPartitionValues = new ArrayList<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            if (!partitionSpecMap.containsKey(partitionKey)) {
                throw new InvalidPartitionException(
                        String.format(
                                "PartitionSpec %s does not contain partition key '%s' for partitioned table %s.",
                                partitionSpec, partitionKey, tablePath));
            } else {
                reOrderedPartitionValues.add(partitionSpecMap.get(partitionKey));
            }
        }

        validatePartitionValues(reOrderedPartitionValues, isCreate);
    }

    @VisibleForTesting
    static void validatePartitionValues(List<String> partitionValues, boolean isCreate) {
        for (String value : partitionValues) {
            String invalidNameError = detectInvalidName(value);
            if (invalidNameError != null || (isCreate && validatePrefix(value) != null)) {
                throw new InvalidPartitionException(
                        "The partition value "
                                + value
                                + " is invalid: "
                                + (invalidNameError != null
                                        ? invalidNameError
                                        : validatePrefix(value)));
            }
        }
    }

    /**
     * Validates that the partition time value in the given {@link PartitionSpec} is valid and not
     * out-of-date when auto-partition is enabled. Throws {@link InvalidPartitionException} if the
     * format doesn't match or the partition is older than the earliest retained one.
     */
    public static void validateAutoPartitionTime(
            PartitionSpec partitionSpec,
            List<String> partitionKeys,
            AutoPartitionStrategy autoPartitionStrategy) {
        if (!autoPartitionStrategy.isAutoPartitionEnabled()) {
            return;
        }
        String autoPartitionKey =
                autoPartitionStrategy.key() != null
                        ? autoPartitionStrategy.key()
                        : partitionKeys.get(0);
        String partitionTime = partitionSpec.getSpecMap().get(autoPartitionKey);
        AutoPartitionTimeUnit timeUnit = autoPartitionStrategy.timeUnit();
        if (partitionTime == null || !isValidPartitionTime(partitionTime, timeUnit)) {
            throw new InvalidPartitionException(
                    String.format(
                            "Partition value '%s' does not match the expected format '%s' "
                                    + "for auto-partition time unit '%s'.",
                            partitionTime, getPartitionTimeFormat(timeUnit), timeUnit));
        }
        ZonedDateTime currentZonedDateTime =
                ZonedDateTime.ofInstant(Instant.now(), autoPartitionStrategy.timeZone().toZoneId());
        // Get the earliest partition time that needs to be retained.
        String lastRetainPartitionTime =
                generateAutoPartitionTime(
                        currentZonedDateTime, -autoPartitionStrategy.numToRetain(), timeUnit);
        if (lastRetainPartitionTime.compareTo(partitionTime) > 0) {
            throw new InvalidPartitionException(
                    String.format(
                            "Partition value '%s' is out-of-date. The earliest retained "
                                    + "partition is '%s'.",
                            partitionTime, lastRetainPartitionTime));
        }
    }

    /** Returns the auto partition key index for the partition keys, if it can be resolved. */
    public static Optional<Integer> getAutoPartitionKeyIndex(
            List<String> partitionKeys, AutoPartitionStrategy autoPartitionStrategy) {
        if (!autoPartitionStrategy.isAutoPartitionEnabled() || partitionKeys.isEmpty()) {
            return Optional.empty();
        }

        String autoPartitionKey =
                autoPartitionStrategy.key() != null
                        ? autoPartitionStrategy.key()
                        : partitionKeys.get(0);
        int autoPartitionKeyIndex = partitionKeys.indexOf(autoPartitionKey);
        if (autoPartitionKeyIndex < 0) {
            return Optional.empty();
        }
        return Optional.of(autoPartitionKeyIndex);
    }

    /** Returns true if the partition name is a historical system partition for the table. */
    public static boolean isHistoricalPartitionName(TableInfo tableInfo, String partitionName) {
        if (!tableInfo.isAutoPartitioned()) {
            return false;
        }
        return isHistoricalPartitionName(
                tableInfo.getPartitionKeys(),
                tableInfo.getTableConfig().getAutoPartitionStrategy(),
                partitionName);
    }

    /** Returns true if the partition name is a historical system partition. */
    public static boolean isHistoricalPartitionName(
            List<String> partitionKeys,
            AutoPartitionStrategy autoPartitionStrategy,
            String partitionName) {
        ResolvedPartitionSpec partitionSpec;
        try {
            partitionSpec = ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
        } catch (IllegalArgumentException e) {
            return false;
        }

        return isHistoricalPartitionSpec(
                partitionKeys, autoPartitionStrategy, partitionSpec.toPartitionSpec());
    }

    /** Returns true if the partition spec targets the auto key with the historical value. */
    public static boolean isHistoricalPartitionSpec(
            List<String> partitionKeys,
            AutoPartitionStrategy autoPartitionStrategy,
            PartitionSpec partitionSpec) {
        Map<String, String> partitionSpecMap = partitionSpec.getSpecMap();
        if (partitionKeys.size() != partitionSpecMap.size()) {
            return false;
        }

        Optional<Integer> autoPartitionKeyIndex =
                getAutoPartitionKeyIndex(partitionKeys, autoPartitionStrategy);
        if (!autoPartitionKeyIndex.isPresent()) {
            return false;
        }

        if (!partitionSpecMap.keySet().containsAll(partitionKeys)) {
            return false;
        }

        String autoPartitionKey = partitionKeys.get(autoPartitionKeyIndex.get());
        return HISTORICAL_PARTITION_VALUE.equals(partitionSpecMap.get(autoPartitionKey));
    }

    /**
     * Converts an original partition name to the corresponding historical system partition spec.
     */
    public static ResolvedPartitionSpec toHistoricalPartitionSpec(
            TableInfo tableInfo, String originalPartitionName) {
        AutoPartitionStrategy autoPartitionStrategy =
                tableInfo.getTableConfig().getAutoPartitionStrategy();
        Optional<Integer> autoPartitionKeyIndex =
                getAutoPartitionKeyIndex(tableInfo.getPartitionKeys(), autoPartitionStrategy);
        if (!autoPartitionKeyIndex.isPresent()) {
            throw new InvalidPartitionException(
                    String.format(
                            "Cannot resolve historical partition for table %s because auto partition key is not available.",
                            tableInfo.getTablePath()));
        }

        ResolvedPartitionSpec originalPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(
                        tableInfo.getPartitionKeys(), originalPartitionName);
        List<String> historicalPartitionValues =
                new ArrayList<>(originalPartitionSpec.getPartitionValues());
        historicalPartitionValues.set(autoPartitionKeyIndex.get(), HISTORICAL_PARTITION_VALUE);
        return new ResolvedPartitionSpec(tableInfo.getPartitionKeys(), historicalPartitionValues);
    }

    /**
     * Returns whether a missing physical partition is eligible for historical lookup or write.
     *
     * <p>The table must be an auto-partitioned Paimon lake table, and the partition must contain a
     * valid auto-partition time older than the retention window. This method only determines
     * routing eligibility; it does not verify that the historical partition or requested data
     * exists.
     */
    public static boolean isHistoricalLookupCandidatePartition(
            TableInfo tableInfo, String partitionName, Instant now) {
        // Historical lookup only applies to auto-partitioned Paimon lake tables.
        if (!tableInfo.isAutoPartitioned()) {
            return false;
        }
        if (!tableInfo.getTableConfig().isDataLakeEnabled()) {
            return false;
        }
        if (tableInfo.getTableConfig().getDataLakeFormat().orElse(null) != DataLakeFormat.PAIMON) {
            return false;
        }

        ResolvedPartitionSpec partitionSpec;
        try {
            partitionSpec =
                    ResolvedPartitionSpec.fromPartitionName(
                            tableInfo.getPartitionKeys(), partitionName);
        } catch (IllegalArgumentException e) {
            return false;
        }

        AutoPartitionStrategy autoPartitionStrategy =
                tableInfo.getTableConfig().getAutoPartitionStrategy();
        Optional<Integer> autoPartitionKeyIndex =
                getAutoPartitionKeyIndex(tableInfo.getPartitionKeys(), autoPartitionStrategy);
        if (!autoPartitionKeyIndex.isPresent()) {
            return false;
        }

        // Extract the auto partition value from single-level or multi-level partition names.
        String autoPartitionValue =
                partitionSpec.getPartitionValues().get(autoPartitionKeyIndex.get());
        AutoPartitionTimeUnit timeUnit = autoPartitionStrategy.timeUnit();
        if (!isValidPartitionTime(autoPartitionValue, timeUnit)) {
            return false;
        }

        if (autoPartitionStrategy.numToRetain() < 0) {
            return false;
        }

        // Only partitions older than the retention window should fall back to historical lookup.
        ZonedDateTime current =
                ZonedDateTime.ofInstant(now, autoPartitionStrategy.timeZone().toZoneId());
        String earliestRetained =
                generateAutoPartitionTime(current, -autoPartitionStrategy.numToRetain(), timeUnit);
        return earliestRetained.compareTo(autoPartitionValue) > 0;
    }

    /**
     * Validates a historical system partition create request and returns the resolved partition
     * spec.
     */
    public static ResolvedPartitionSpec validateHistoricalPartitionSpec(
            TablePath tablePath,
            List<String> partitionKeys,
            AutoPartitionStrategy autoPartitionStrategy,
            PartitionSpec partitionSpec) {
        validatePartitionSpec(tablePath, partitionKeys, partitionSpec, false);

        if (!isHistoricalPartitionSpec(partitionKeys, autoPartitionStrategy, partitionSpec)) {
            throw new InvalidPartitionException(
                    String.format(
                            "Invalid historical partition spec %s for partitioned table %s.",
                            partitionSpec, tablePath));
        }

        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionSpec(partitionKeys, partitionSpec);
        int autoKeyIndex = getAutoPartitionKeyIndex(partitionKeys, autoPartitionStrategy).get();
        List<String> nonAutoPartitionValues =
                new ArrayList<>(resolvedPartitionSpec.getPartitionValues());
        nonAutoPartitionValues.remove(autoKeyIndex);
        validatePartitionValues(nonAutoPartitionValues, true);
        return resolvedPartitionSpec;
    }

    /**
     * Generate {@link ResolvedPartitionSpec} for auto partition in server. When we auto creating a
     * partition, we need to first generate a {@link ResolvedPartitionSpec}.
     *
     * <p>The value is the formatted time with the specified time unit.
     *
     * @param partitionKeys the partition keys
     * @param current the current time
     * @param offset the offset
     * @param timeUnit the time unit
     * @return the resolved partition spec
     */
    public static ResolvedPartitionSpec generateAutoPartition(
            List<String> partitionKeys,
            ZonedDateTime current,
            int offset,
            AutoPartitionTimeUnit timeUnit) {
        String autoPartitionFieldSpec = generateAutoPartitionTime(current, offset, timeUnit);

        return ResolvedPartitionSpec.fromPartitionName(partitionKeys, autoPartitionFieldSpec);
    }

    public static String generateAutoPartitionTime(
            ZonedDateTime current, int offset, AutoPartitionTimeUnit timeUnit) {
        String autoPartitionFieldSpec;
        switch (timeUnit) {
            case YEAR:
                autoPartitionFieldSpec = getFormattedTime(current.plusYears(offset), YEAR_FORMAT);
                break;
            case QUARTER:
                autoPartitionFieldSpec =
                        getFormattedTime(current.plusMonths(offset * 3L), QUARTER_FORMAT);
                break;
            case MONTH:
                autoPartitionFieldSpec = getFormattedTime(current.plusMonths(offset), MONTH_FORMAT);
                break;
            case DAY:
                autoPartitionFieldSpec = getFormattedTime(current.plusDays(offset), DAY_FORMAT);
                break;
            case HOUR:
                autoPartitionFieldSpec = getFormattedTime(current.plusHours(offset), HOUR_FORMAT);
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
        return autoPartitionFieldSpec;
    }

    /** Returns the time string format pattern for the given time unit. */
    private static String getPartitionTimeFormat(AutoPartitionTimeUnit timeUnit) {
        switch (timeUnit) {
            case YEAR:
                return YEAR_FORMAT;
            case QUARTER:
                return QUARTER_FORMAT;
            case MONTH:
                return MONTH_FORMAT;
            case DAY:
                return DAY_FORMAT;
            case HOUR:
                return HOUR_FORMAT;
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + timeUnit);
        }
    }

    /**
     * Returns true if the given time string matches the format expected for the given time unit.
     */
    private static boolean isValidPartitionTime(String time, AutoPartitionTimeUnit timeUnit) {
        try {
            DateTimeFormatter.ofPattern(getPartitionTimeFormat(timeUnit)).parse(time);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private static String getFormattedTime(ZonedDateTime zonedDateTime, String format) {
        return DateTimeFormatter.ofPattern(format).format(zonedDateTime);
    }

    /**
     * Parses a partition value string back to its typed Fluss internal representation. This is the
     * reverse operation of {@link #convertValueOfType(Object, DataTypeRoot)}.
     *
     * @param value the string representation of the partition value
     * @param type the data type root of the partition column
     * @return the typed value as a Fluss internal data structure
     */
    public static Object parseValueOfType(String value, DataTypeRoot type) {
        switch (type) {
            case CHAR:
            case STRING:
                return BinaryString.fromString(value);
            case BOOLEAN:
                if ("true".equalsIgnoreCase(value)) {
                    return true;
                } else if ("false".equalsIgnoreCase(value)) {
                    return false;
                }
                throw new IllegalArgumentException(
                        "Invalid boolean partition value: '"
                                + value
                                + "'. Expected 'true' or 'false'.");
            case BINARY:
            case BYTES:
                return PartitionNameConverters.parseHexString(value);
            case TINYINT:
                return Byte.parseByte(value);
            case SMALLINT:
                return Short.parseShort(value);
            case INTEGER:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case DATE:
                return PartitionNameConverters.parseDayString(value);
            case TIME_WITHOUT_TIME_ZONE:
                return PartitionNameConverters.parseMilliString(value);
            case FLOAT:
                return PartitionNameConverters.parseFloat(value);
            case DOUBLE:
                return PartitionNameConverters.parseDouble(value);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return PartitionNameConverters.parseTimestampNtz(value);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return PartitionNameConverters.parseTimestampLtz(value);
            default:
                throw new IllegalArgumentException("Unsupported DataTypeRoot: " + type);
        }
    }

    public static String convertValueOfType(Object value, DataTypeRoot type) {
        String stringPartitionKey = "";
        switch (type) {
            case CHAR:
            case STRING:
                stringPartitionKey = ((BinaryString) value).toString();
                break;
            case BOOLEAN:
                Boolean booleanValue = (Boolean) value;
                stringPartitionKey = booleanValue.toString();
                break;
            case BINARY:
            case BYTES:
                byte[] bytesValue = (byte[]) value;
                stringPartitionKey = PartitionNameConverters.hexString(bytesValue);
                break;
            case TINYINT:
                Byte tinyIntValue = (Byte) value;
                stringPartitionKey = tinyIntValue.toString();
                break;
            case SMALLINT:
                Short smallIntValue = (Short) value;
                stringPartitionKey = smallIntValue.toString();
                break;
            case INTEGER:
                Integer intValue = (Integer) value;
                stringPartitionKey = intValue.toString();
                break;
            case BIGINT:
                Long bigIntValue = (Long) value;
                stringPartitionKey = bigIntValue.toString();
                break;
            case DATE:
                Integer dateValue = (Integer) value;
                stringPartitionKey = PartitionNameConverters.dayToString(dateValue);
                break;
            case TIME_WITHOUT_TIME_ZONE:
                Integer timeValue = (Integer) value;
                stringPartitionKey = PartitionNameConverters.milliToString(timeValue);
                break;
            case FLOAT:
                Float floatValue = (Float) value;
                stringPartitionKey = PartitionNameConverters.reformatFloat(floatValue);
                break;
            case DOUBLE:
                Double doubleValue = (Double) value;
                stringPartitionKey = PartitionNameConverters.reformatDouble(doubleValue);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampLtz timeStampLTZValue = (TimestampLtz) value;
                stringPartitionKey = PartitionNameConverters.timestampToString(timeStampLTZValue);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampNtz timeStampNTZValue = (TimestampNtz) value;
                stringPartitionKey = PartitionNameConverters.timestampToString(timeStampNTZValue);
                break;
            default:
                throw new IllegalArgumentException("Unsupported DataTypeRoot: " + type);
        }
        return stringPartitionKey;
    }

    /** Projects {@code tableInfo}'s row type down to its partition key columns, in key order. */
    public static RowType partitionRowType(TableInfo tableInfo) {
        RowType schema = tableInfo.getRowType();
        List<String> fieldNames = schema.getFieldNames();
        int[] indexes =
                tableInfo.getPartitionKeys().stream().mapToInt(fieldNames::indexOf).toArray();
        return schema.project(indexes);
    }

    /**
     * Builds a row of typed partition values by parsing each string with {@link
     * #parseValueOfType(String, DataTypeRoot)} for the column at that ordinal in {@code
     * partitionRowType}.
     */
    public static GenericRow toPartitionRow(
            List<String> partitionValues, RowType partitionRowType) {
        GenericRow row = new GenericRow(partitionValues.size());
        for (int i = 0; i < partitionValues.size(); i++) {
            DataTypeRoot type = partitionRowType.getTypeAt(i).getTypeRoot();
            row.setField(i, parseValueOfType(partitionValues.get(i), type));
        }
        return row;
    }
}
