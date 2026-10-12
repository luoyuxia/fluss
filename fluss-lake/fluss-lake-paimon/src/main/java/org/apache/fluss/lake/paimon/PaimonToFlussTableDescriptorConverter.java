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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.paimon.utils.PaimonDataTypeToFlussDataType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.PartitionUtils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.BucketSpec;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converts an existing Paimon append-only table to a Fluss table descriptor. */
final class PaimonToFlussTableDescriptorConverter {

    private PaimonToFlussTableDescriptorConverter() {}

    static TableDescriptor convert(TablePath tablePath, FileStoreTable table) {
        validateAppendOnlyTable(tablePath, table);

        List<DataField> dataFields = table.schema().fields();
        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (DataField dataField : dataFields) {
            try {
                schemaBuilder
                        .column(
                                dataField.name(),
                                dataField.type().accept(PaimonDataTypeToFlussDataType.INSTANCE))
                        .withComment(dataField.description());
            } catch (UnsupportedOperationException e) {
                throw new InvalidTableException(
                        String.format(
                                "Paimon table %s column '%s' uses unsupported type %s.",
                                tablePath, dataField.name(), dataField.type()),
                        e);
            }
        }

        TableDescriptor.Builder descriptorBuilder =
                TableDescriptor.builder()
                        .schema(schemaBuilder.build())
                        .partitionedBy(table.partitionKeys())
                        .properties(deriveAutoPartitionProperties(table))
                        .comment(table.comment().orElse(null));
        applyDistribution(tablePath, table, descriptorBuilder);
        return descriptorBuilder.build();
    }

    private static void validateAppendOnlyTable(TablePath tablePath, FileStoreTable table) {
        if (!table.primaryKeys().isEmpty()) {
            throw new InvalidTableException(
                    String.format(
                            "Paimon primary-key table %s cannot be promoted as a Fluss log table currently.",
                            tablePath));
        }

        CoreOptions coreOptions = new CoreOptions(table.options());
        // TODO: Support rowkind.field for Paimon append tables. Preserve Paimon's
        // +I/+U append and -D/-U reject/filter rules (including ignore-delete and
        // ignore-update-before) before records enter the Fluss log. Adapt the
        // record and Arrow tiering paths, which can bypass TableWriteImpl's
        // row-kind checks, and test both paths before removing this rejection.
        if (coreOptions.rowkindField().isPresent()) {
            throw unsupportedAppendTableFeature(tablePath, CoreOptions.ROWKIND_FIELD.key());
        }
        if (coreOptions.dataEvolutionEnabled()) {
            throw unsupportedAppendTableFeature(
                    tablePath, CoreOptions.DATA_EVOLUTION_ENABLED.key());
        }
        if (coreOptions.rowTrackingEnabled()) {
            throw unsupportedAppendTableFeature(tablePath, CoreOptions.ROW_TRACKING_ENABLED.key());
        }
        if (coreOptions.deletionVectorsEnabled()) {
            throw unsupportedAppendTableFeature(
                    tablePath, CoreOptions.DELETION_VECTORS_ENABLED.key());
        }

        for (DataField field : table.schema().fields()) {
            if (field.defaultValue() != null) {
                throw new InvalidTableException(
                        String.format(
                                "Paimon table %s column '%s' has a default value, which cannot be "
                                        + "represented in a Fluss table.",
                                tablePath, field.name()));
            }
        }
    }

    private static InvalidTableException unsupportedAppendTableFeature(
            TablePath tablePath, String option) {
        return new InvalidTableException(
                String.format(
                        "Paimon table %s enables unsupported append-table feature '%s'.",
                        tablePath, option));
    }

    private static Map<String, String> deriveAutoPartitionProperties(FileStoreTable table) {
        if (table.partitionKeys().isEmpty()) {
            return Collections.emptyMap();
        }

        CoreOptions coreOptions = new CoreOptions(table.options());
        String paimonFormatter = coreOptions.partitionTimestampFormatter();
        String timeKey = inferTimePartitionKey(table.partitionKeys(), coreOptions);
        if (paimonFormatter == null || timeKey == null) {
            return Collections.emptyMap();
        }

        AutoPartitionTimeUnit timeUnit = inferTimeUnit(paimonFormatter, coreOptions);
        if (timeUnit == null) {
            return Collections.emptyMap();
        }

        DataType timeKeyType = table.rowType().getTypeAt(table.rowType().getFieldIndex(timeKey));
        // Fluss reads DATE partition values as yyyy-MM-dd (for example, 2025-10-13).
        // Auto partitioning with yyyyMMdd would generate 20251013, which cannot be read as DATE.
        // Keep other DATE formats as regular partitions; inferTimeUnit maps yyyy-MM-dd to DAY.
        // Other key types may use formats such as yyyyMMdd.
        if (timeKeyType.getTypeRoot() == DataTypeRoot.DATE
                && !"yyyy-MM-dd".equals(paimonFormatter)) {
            return Collections.emptyMap();
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), Boolean.TRUE.toString());
        properties.put(ConfigOptions.TABLE_AUTO_PARTITION_KEY.key(), timeKey);
        properties.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), paimonFormatter);
        properties.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), timeUnit.toString());
        properties.put(
                ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(),
                coreOptions.sinkProcessTimeZone().getId());
        if (table.partitionKeys().size() > 1) {
            properties.put(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE.key(), "0");
        }
        return properties;
    }

    /**
     * Returns the partition key referenced by the Paimon timestamp pattern, or the first partition
     * key when no pattern is configured. Returns {@code null} when the configured pattern does not
     * reference exactly one partition key.
     */
    @Nullable
    private static String inferTimePartitionKey(
            List<String> partitionKeys, CoreOptions coreOptions) {
        String pattern = coreOptions.partitionTimestampPattern();
        if (pattern == null) {
            return partitionKeys.get(0);
        }
        for (String partitionKey : partitionKeys) {
            if (("$" + partitionKey).equals(pattern)) {
                return partitionKey;
            }
        }
        return null;
    }

    /**
     * Returns the single Fluss time unit compatible with the Paimon timestamp formatter, or {@code
     * null} when the formatter cannot be mapped unambiguously.
     */
    @Nullable
    private static AutoPartitionTimeUnit inferTimeUnit(
            String paimonFormatter, CoreOptions coreOptions) {
        List<AutoPartitionTimeUnit> compatibleUnits = new ArrayList<>();
        // Find every Fluss time unit that accepts the Paimon formatter. A unique match is required.
        for (AutoPartitionTimeUnit candidate : AutoPartitionTimeUnit.values()) {
            Map<String, String> options = new HashMap<>();
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key(), Boolean.TRUE.toString());
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_FORMAT.key(), paimonFormatter);
            options.put(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key(), candidate.toString());
            options.put(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE.key(),
                    coreOptions.sinkProcessTimeZone().getId());
            try {
                PartitionUtils.validateTimeFormat(candidate, AutoPartitionStrategy.from(options));
                compatibleUnits.add(candidate);
            } catch (IllegalArgumentException ignored) {
                // The formatter is not compatible with this Fluss time unit.
            }
        }
        return compatibleUnits.size() == 1 ? compatibleUnits.get(0) : null;
    }

    private static void applyDistribution(
            TablePath tablePath, FileStoreTable table, TableDescriptor.Builder descriptorBuilder) {
        BucketMode bucketMode = table.bucketMode();
        BucketSpec bucketSpec = table.bucketSpec();
        if (bucketMode == BucketMode.HASH_FIXED) {
            CoreOptions coreOptions = new CoreOptions(table.options());
            if (coreOptions.bucketFunctionType() != CoreOptions.BucketFunctionType.DEFAULT) {
                throw new InvalidTableException(
                        String.format(
                                "Paimon table %s uses unsupported bucket function '%s'.",
                                tablePath, coreOptions.bucketFunctionType()));
            }
            descriptorBuilder.distributedBy(bucketSpec.getNumBuckets(), bucketSpec.getBucketKeys());
        } else if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            descriptorBuilder.distributedBy(null, Collections.<String>emptyList());
        } else {
            throw new InvalidTableException(
                    String.format(
                            "Paimon table %s uses unsupported bucket mode '%s'.",
                            tablePath, bucketMode));
        }
    }
}
