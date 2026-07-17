/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.client.write;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.HistoricalPartitionResolver;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.utils.PartitionUtils.HISTORICAL_PARTITION_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Test for {@link DynamicPartitionCreator}. */
class DynamicPartitionCreatorTest {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "write_table");
    private static final String EXPIRED_PARTITION = "20000101";

    private MetadataUpdater metadataUpdater;
    private Admin admin;
    private HistoricalPartitionResolver historicalPartitionResolver;

    @BeforeEach
    void beforeEach() {
        metadataUpdater = mock(MetadataUpdater.class);
        admin = mock(Admin.class);
        historicalPartitionResolver = mock(HistoricalPartitionResolver.class);
    }

    @Test
    void testOriginalPartitionCacheHit() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "dt");
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, "20260715");
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.of(1L));

        ResolvedWriteTarget target = creator(false).resolveWriteTarget(originalPath, tableInfo);

        assertNormalTarget(target, originalPath);
        verify(metadataUpdater, never()).checkAndUpdatePartitionMetadata(any());
        verify(historicalPartitionResolver, never()).resolveHistoricalPartitionId(any(), any());
    }

    @Test
    void testOriginalPartitionRefreshHit() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "dt");
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, "20260715");
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.empty());
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(true);

        ResolvedWriteTarget target = creator(false).resolveWriteTarget(originalPath, tableInfo);

        assertNormalTarget(target, originalPath);
        verify(historicalPartitionResolver, never()).resolveHistoricalPartitionId(any(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExpiredPartitionUsesHistoricalTarget(boolean dynamicPartitionEnabled) {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "dt");
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, EXPIRED_PARTITION);
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.empty());
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(false);
        when(historicalPartitionResolver.resolveHistoricalPartitionId(tableInfo, EXPIRED_PARTITION))
                .thenReturn(CompletableFuture.completedFuture(2L));

        ResolvedWriteTarget target =
                creator(dynamicPartitionEnabled).resolveWriteTarget(originalPath, tableInfo);

        assertThat(target.isHistorical()).isTrue();
        assertThat(target.physicalTablePath())
                .isEqualTo(PhysicalTablePath.of(TABLE_PATH, HISTORICAL_PARTITION_VALUE));
        assertThat(target.originalPartitionName()).isEqualTo(EXPIRED_PARTITION);
        assertThat(target.partitionId()).isEqualTo(2L);
        verify(admin, never()).createPartition(any(), any(), eq(true));
    }

    @Test
    void testConfirmedHistoricalPartitionSkipsOriginalMetadataRefresh() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "dt");
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, EXPIRED_PARTITION);
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.empty());
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(false);
        when(historicalPartitionResolver.resolveHistoricalPartitionId(tableInfo, EXPIRED_PARTITION))
                .thenReturn(CompletableFuture.completedFuture(2L));
        DynamicPartitionCreator creator = creator(false);

        assertThat(creator.resolveWriteTarget(originalPath, tableInfo).isHistorical()).isTrue();
        assertThat(creator.resolveWriteTarget(originalPath, tableInfo).isHistorical()).isTrue();

        verify(metadataUpdater, times(1)).checkAndUpdatePartitionMetadata(originalPath);
        verify(historicalPartitionResolver, times(2))
                .resolveHistoricalPartitionId(tableInfo, EXPIRED_PARTITION);
    }

    @Test
    void testOriginalPartitionCacheHitClearsHistoricalRouting() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "dt");
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, EXPIRED_PARTITION);
        when(metadataUpdater.getPartitionId(originalPath))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(1L));
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(false);
        when(historicalPartitionResolver.resolveHistoricalPartitionId(tableInfo, EXPIRED_PARTITION))
                .thenReturn(CompletableFuture.completedFuture(2L));
        DynamicPartitionCreator creator = creator(false);

        assertThat(creator.resolveWriteTarget(originalPath, tableInfo).isHistorical()).isTrue();
        assertNormalTarget(creator.resolveWriteTarget(originalPath, tableInfo), originalPath);

        verify(metadataUpdater, times(1)).checkAndUpdatePartitionMetadata(originalPath);
        verify(historicalPartitionResolver, times(1))
                .resolveHistoricalPartitionId(tableInfo, EXPIRED_PARTITION);
    }

    @Test
    void testCurrentPartitionUsesNormalDynamicCreate() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "dt");
        String currentPartition =
                LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE);
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, currentPartition);
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.empty());
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(false);
        when(admin.createPartition(eq(TABLE_PATH), any(), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        ResolvedWriteTarget target = creator(true).resolveWriteTarget(originalPath, tableInfo);

        assertNormalTarget(target, originalPath);
        verify(admin).createPartition(eq(TABLE_PATH), any(), eq(true));
        verify(historicalPartitionResolver, never()).resolveHistoricalPartitionId(any(), any());
    }

    @Test
    void testCurrentPartitionFailsWhenDynamicCreateDisabled() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "dt");
        String currentPartition =
                LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE);
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, currentPartition);
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.empty());
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(false);

        assertThatThrownBy(() -> creator(false).resolveWriteTarget(originalPath, tableInfo))
                .isInstanceOf(PartitionNotExistException.class)
                .hasMessage("Table partition '%s' does not exist.", originalPath);

        verify(admin, never()).createPartition(any(), any(), eq(true));
    }

    @Test
    void testNonPaimonExpiredPartitionKeepsNormalValidation() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.ICEBERG, "dt");
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, EXPIRED_PARTITION);
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.empty());
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(false);

        assertThatThrownBy(() -> creator(true).resolveWriteTarget(originalPath, tableInfo))
                .isInstanceOf(InvalidPartitionException.class)
                .hasMessageContaining("Partition value '20000101' is out-of-date");

        verify(historicalPartitionResolver, never()).resolveHistoricalPartitionId(any(), any());
        verify(admin, never()).createPartition(any(), any(), eq(true));
    }

    @Test
    void testHistoricalResolveFailureDoesNotCreateNormalPartition() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "dt");
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, EXPIRED_PARTITION);
        RuntimeException expected = new RuntimeException("historical create failed");
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.empty());
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(false);
        when(historicalPartitionResolver.resolveHistoricalPartitionId(tableInfo, EXPIRED_PARTITION))
                .thenReturn(failedFuture(expected));

        assertThatThrownBy(() -> creator(true).resolveWriteTarget(originalPath, tableInfo))
                .isSameAs(expected);

        verify(admin, never()).createPartition(any(), any(), eq(true));
    }

    @Test
    void testMultiLevelPartitionKeepsStaticPrefix() {
        TableInfo tableInfo = createTableInfo(true, DataLakeFormat.PAIMON, "region", "dt");
        String originalPartition = "us$" + EXPIRED_PARTITION;
        PhysicalTablePath originalPath = PhysicalTablePath.of(TABLE_PATH, originalPartition);
        when(metadataUpdater.getPartitionId(originalPath)).thenReturn(Optional.empty());
        when(metadataUpdater.checkAndUpdatePartitionMetadata(originalPath)).thenReturn(false);
        when(historicalPartitionResolver.resolveHistoricalPartitionId(tableInfo, originalPartition))
                .thenReturn(CompletableFuture.completedFuture(3L));

        ResolvedWriteTarget target = creator(false).resolveWriteTarget(originalPath, tableInfo);

        assertThat(target.physicalTablePath())
                .isEqualTo(PhysicalTablePath.of(TABLE_PATH, "us$" + HISTORICAL_PARTITION_VALUE));
        assertThat(target.originalPartitionName()).isEqualTo(originalPartition);
        assertThat(target.partitionId()).isEqualTo(3L);
    }

    private DynamicPartitionCreator creator(boolean dynamicPartitionEnabled) {
        return new DynamicPartitionCreator(
                metadataUpdater,
                admin,
                dynamicPartitionEnabled,
                ignored -> {},
                historicalPartitionResolver);
    }

    private static void assertNormalTarget(
            ResolvedWriteTarget target, PhysicalTablePath expectedPath) {
        assertThat(target.isHistorical()).isFalse();
        assertThat(target.physicalTablePath()).isEqualTo(expectedPath);
        assertThat(target.originalPartitionName()).isNull();
    }

    private static TableInfo createTableInfo(
            boolean dataLakeEnabled, DataLakeFormat dataLakeFormat, String... partitionKeys) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("region", DataTypes.STRING())
                        .column("dt", DataTypes.STRING())
                        .build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .partitionedBy(partitionKeys)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT,
                                AutoPartitionTimeUnit.DAY)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 2)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIMEZONE, "UTC")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, dataLakeEnabled)
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, dataLakeFormat)
                        .build();
        return TableInfo.of(TABLE_PATH, 1L, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }
}
