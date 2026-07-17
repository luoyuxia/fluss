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

package org.apache.fluss.client.metadata;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

/** Test for {@link HistoricalPartitionResolver}. */
class HistoricalPartitionResolverTest {

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "historical_table");
    private static final String ORIGINAL_PARTITION = "20000101";
    private static final PhysicalTablePath HISTORICAL_PATH =
            PhysicalTablePath.of(TABLE_PATH, HISTORICAL_PARTITION_VALUE);

    private MetadataUpdater metadataUpdater;
    private Admin admin;
    private HistoricalPartitionResolver resolver;
    private TableInfo tableInfo;

    @BeforeEach
    void beforeEach() {
        metadataUpdater = mock(MetadataUpdater.class);
        admin = mock(Admin.class);
        resolver = new HistoricalPartitionResolver(metadataUpdater, admin);
        tableInfo = createTableInfo(TABLE_PATH, "dt");
    }

    @Test
    void testResolveFromCache() throws Exception {
        when(metadataUpdater.getPartitionId(HISTORICAL_PATH)).thenReturn(Optional.of(11L));

        assertThat(resolver.resolveHistoricalPartitionId(tableInfo, ORIGINAL_PARTITION).get())
                .isEqualTo(11L);

        verify(metadataUpdater, never()).checkAndUpdatePartitionMetadata(any());
        verify(admin, never()).createPartition(any(), any(), eq(true));
    }

    @Test
    void testResolveAfterMetadataRefresh() throws Exception {
        when(metadataUpdater.getPartitionId(HISTORICAL_PATH))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(12L));

        assertThat(resolver.resolveHistoricalPartitionId(tableInfo, ORIGINAL_PARTITION).get())
                .isEqualTo(12L);

        verify(metadataUpdater).checkAndUpdatePartitionMetadata(HISTORICAL_PATH);
        verify(admin, never()).createPartition(any(), any(), eq(true));
    }

    @Test
    void testCreateAndResolveHistoricalPartition() throws Exception {
        when(metadataUpdater.getPartitionId(HISTORICAL_PATH))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(13L));
        when(admin.createPartition(eq(TABLE_PATH), any(), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        assertThat(resolver.resolveHistoricalPartitionId(tableInfo, ORIGINAL_PARTITION).get())
                .isEqualTo(13L);

        ArgumentCaptor<PartitionSpec> partitionSpecCaptor =
                ArgumentCaptor.forClass(PartitionSpec.class);
        verify(admin).createPartition(eq(TABLE_PATH), partitionSpecCaptor.capture(), eq(true));
        assertThat(partitionSpecCaptor.getValue().getSpecMap())
                .containsEntry("dt", HISTORICAL_PARTITION_VALUE);
        verify(metadataUpdater, times(2)).checkAndUpdatePartitionMetadata(HISTORICAL_PATH);
    }

    @Test
    void testConcurrentResolveSharesInFlightFuture() throws Exception {
        CompletableFuture<Void> createFuture = new CompletableFuture<>();
        when(metadataUpdater.getPartitionId(HISTORICAL_PATH))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(14L));
        when(admin.createPartition(eq(TABLE_PATH), any(), eq(true))).thenReturn(createFuture);

        CompletableFuture<Long> first =
                resolver.resolveHistoricalPartitionId(tableInfo, ORIGINAL_PARTITION);
        CompletableFuture<Long> second =
                resolver.resolveHistoricalPartitionId(tableInfo, ORIGINAL_PARTITION);

        assertThat(second).isSameAs(first);
        verify(admin).createPartition(eq(TABLE_PATH), any(), eq(true));

        createFuture.complete(null);
        assertThat(first.get()).isEqualTo(14L);
        assertThat(second.get()).isEqualTo(14L);
    }

    @Test
    void testFailedResolveCanBeRetried() throws Exception {
        RuntimeException createFailure = new RuntimeException("create failed");
        when(metadataUpdater.getPartitionId(HISTORICAL_PATH))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(15L));
        when(admin.createPartition(eq(TABLE_PATH), any(), eq(true)))
                .thenReturn(failedFuture(createFailure))
                .thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Long> failed =
                resolver.resolveHistoricalPartitionId(tableInfo, ORIGINAL_PARTITION);
        assertThatThrownBy(failed::get).hasRootCause(createFailure);

        assertThat(resolver.resolveHistoricalPartitionId(tableInfo, ORIGINAL_PARTITION).get())
                .isEqualTo(15L);
        verify(admin, times(2)).createPartition(eq(TABLE_PATH), any(), eq(true));
    }

    @Test
    void testCreateWithoutRefreshedMetadataFails() {
        when(metadataUpdater.getPartitionId(HISTORICAL_PATH)).thenReturn(Optional.empty());
        when(admin.createPartition(eq(TABLE_PATH), any(), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        assertThatThrownBy(
                        () ->
                                resolver.resolveHistoricalPartitionId(tableInfo, ORIGINAL_PARTITION)
                                        .get())
                .hasRootCauseInstanceOf(UnknownTableOrBucketException.class)
                .hasRootCauseMessage(
                        "Historical partition "
                                + HISTORICAL_PATH
                                + " does not exist after creation.");
    }

    @Test
    void testMultiLevelPartitionKeepsStaticPrefix() throws Exception {
        TableInfo multiLevelTable = createTableInfo(TABLE_PATH, "region", "dt");
        PhysicalTablePath historicalPath =
                PhysicalTablePath.of(TABLE_PATH, "us$" + HISTORICAL_PARTITION_VALUE);
        when(metadataUpdater.getPartitionId(historicalPath))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(16L));
        when(admin.createPartition(eq(TABLE_PATH), any(), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));

        assertThat(
                        resolver.resolveHistoricalPartitionId(
                                        multiLevelTable, "us$" + ORIGINAL_PARTITION)
                                .get())
                .isEqualTo(16L);

        ArgumentCaptor<PartitionSpec> partitionSpecCaptor =
                ArgumentCaptor.forClass(PartitionSpec.class);
        verify(admin).createPartition(eq(TABLE_PATH), partitionSpecCaptor.capture(), eq(true));
        assertThat(partitionSpecCaptor.getValue().getSpecMap())
                .containsEntry("region", "us")
                .containsEntry("dt", HISTORICAL_PARTITION_VALUE);
    }

    private static TableInfo createTableInfo(TablePath tablePath, String... partitionKeys) {
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
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DATALAKE_FORMAT, DataLakeFormat.PAIMON)
                        .build();
        return TableInfo.of(tablePath, 1L, 1, descriptor, DEFAULT_REMOTE_DATA_DIR, 1L, 1L);
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }
}
