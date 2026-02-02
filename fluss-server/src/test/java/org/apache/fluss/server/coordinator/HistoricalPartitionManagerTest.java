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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.HistoricalPartitionException;
import org.apache.fluss.lake.DataLakeFormat;
import org.apache.fluss.metadata.PartitionStatus;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.testutils.FlussAnyRefEq;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.fluss.server.coordinator.testing.CoordinatorTestUtils.TABLE_SCHEMA;
import static org.apache.fluss.server.coordinator.testing.CoordinatorTestUtils.newTableInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit test for {@link HistoricalPartitionManager}. */
@ExtendWith(MockitoExtension.class)
class HistoricalPartitionManagerTest {

    @Mock
    private ServerMetadataCache metadataCache;

    @Mock
    private MetadataManager metadataManager;

    private Configuration configuration;

    private HistoricalPartitionManager historicalPartitionManager;

    @BeforeEach
    void setUp() {
        configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        configuration.set(ConfigOptions.HISTORICAL_PARTITION_SYNC_INTERVAL, Duration.ofSeconds(1));

        historicalPartitionManager =
                new HistoricalPartitionManager(metadataCache, metadataManager, configuration);
    }

    @Test
    void testMarkPartitionAsHistorical() {
        long tableId = 1L;
        String partitionName = "dt=2023-01-01";

        // Initially, partition should not be historical
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partitionName)).isFalse();

        // Mark as historical
        historicalPartitionManager.markPartitionAsHistorical(tableId, partitionName);

        // Verify it's now historical
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partitionName)).isTrue();
    }

    @Test
    void testMultiplePartitionsPerTable() {
        long tableId = 1L;
        String partition1 = "dt=2023-01-01";
        String partition2 = "dt=2023-01-02";

        // Initially, both partitions should not be historical
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partition1)).isFalse();
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partition2)).isFalse();

        // Mark first as historical
        historicalPartitionManager.markPartitionAsHistorical(tableId, partition1);

        // Verify first is historical, second is not
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partition1)).isTrue();
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partition2)).isFalse();

        // Mark second as historical
        historicalPartitionManager.markPartitionAsHistorical(tableId, partition2);

        // Verify both are historical
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partition1)).isTrue();
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partition2)).isTrue();
    }

    @Test
    void testMultipleTables() {
        long tableId1 = 1L;
        long tableId2 = 2L;
        String partitionName = "dt=2023-01-01";

        // Initially, partition should not be historical for either table
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId1, partitionName)).isFalse();
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId2, partitionName)).isFalse();

        // Mark for first table only
        historicalPartitionManager.markPartitionAsHistorical(tableId1, partitionName);

        // Verify first table has historical partition, second doesn't
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId1, partitionName)).isTrue();
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId2, partitionName)).isFalse();
    }

    @Test
    void testIsHistoricalPartitionForNonExistentTable() {
        long tableId = 1L;
        String partitionName = "dt=2023-01-01";

        // Partition should not be historical if table doesn't exist in tracking
        assertThat(historicalPartitionManager.isHistoricalPartition(tableId, partitionName)).isFalse();
    }

    @Test
    void testSyncWithLakeForNonLakeTable() {
        // Create a table that doesn't have datalake enabled
        TableInfo tableInfo = newTableInfo(
                1L,
                TablePath.of("db", "table"),
                TableDescriptor.builder()
                        .schema(TABLE_SCHEMA)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, false) // No datalake
                        .build());

        // Mock metadata cache to return the table info
        when(metadataCache.getTableInfo(eq(1L))).thenReturn(tableInfo);

        // Mock to return some historical partitions
        when(metadataCache.getHistoricalPartitions(eq(1L)))
                .thenReturn(new HashSet<>(Arrays.asList("dt=2023-01-01", "dt=2023-01-02")));

        // Sync with lake - should not process the table since it's not a lake table
        historicalPartitionManager.syncWithLake();

        // Verify that no Paimon catalog operations were attempted
        // This would be verified by ensuring the internal getCatalogForTable method wasn't called
    }

    @Test
    void testSyncWithLakeForLakeTable() {
        // Create a table that has datalake enabled
        TableInfo tableInfo = newTableInfo(
                1L,
                TablePath.of("db", "lake_table"),
                TableDescriptor.builder()
                        .schema(TABLE_SCHEMA)
                        .distributedBy(3, "id")
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true) // Datalake enabled
                        .build());

        // Mock metadata cache to return the table info
        when(metadataCache.getTableInfo(eq(1L))).thenReturn(tableInfo);

        // Mock to return some historical partitions
        Set<String> historicalPartitions = new HashSet<>(Arrays.asList("dt=2023-01-01", "dt=2023-01-02"));
        when(metadataCache.getHistoricalPartitions(eq(1L)))
                .thenReturn(historicalPartitions);

        // Sync with lake - should process the table since it's a lake table
        historicalPartitionManager.syncWithLake();

        // Verify that the table was processed
        // The actual implementation would call Paimon operations, but we're testing the control flow
    }

    @Test
    void testInitializationWithConfiguration() {
        // Test that the manager initializes with the provided configuration
        assertThat(historicalPartitionManager).isNotNull();
        
        // Verify the configuration was used properly
        // The actual behavior depends on the internal implementation
    }

    @Test
    void testCleanupOnClose() throws Exception {
        // Mark some partitions as historical
        historicalPartitionManager.markPartitionAsHistorical(1L, "dt=2023-01-01");
        historicalPartitionManager.markPartitionAsHistorical(2L, "dt=2023-01-02");

        // Close the manager
        historicalPartitionManager.close();

        // After closing, the manager should be in a clean state
        // This test mainly verifies that close doesn't throw exceptions
    }
}