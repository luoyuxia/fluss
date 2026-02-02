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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.groups.CoordinatorMetricGroup;
import org.apache.fluss.metadata.PartitionStatus;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.lake.paimon.catalog.PaimonCatalogFactory;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * Manager for historical partitions in datalake-enabled tables.
 *
 * <p>This manager is responsible for:
 *
 * <ul>
 *   <li>Tracking partitions that have transitioned from ACTIVE to HISTORICAL status
 *   <li>Periodically syncing with Paimon to detect expired partitions in the lake
 *   <li>Cleaning up historical partition metadata when lake data expires
 * </ul>
 *
 * <p>Historical partition support is automatically enabled for tables where:
 *
 * <ul>
 *   <li>datalake.enabled = true
 *   <li>auto-partitioning is enabled
 * </ul>
 */
public class HistoricalPartitionManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalPartitionManager.class);

    /** Default interval to check lake partition expiration. */
    private final ScheduledExecutorService syncExecutor;
    private final ServerMetadataCache metadataCache;
    private final MetadataManager metadataManager;
    private final long syncIntervalMs;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    // Metrics
    private Counter historicalPartitionCount;
    private Counter expiredPartitionCleanupCount;
    private Counter syncFailureCount;

    private final Lock lock = new ReentrantLock();

    /**
     * Map of table ID -> set of historical partition names. Only tracks partitions that are in
     * HISTORICAL status.
     */
    @GuardedBy("lock")
    private final Map<Long, Set<String>> historicalPartitionsByTable = new HashMap<>();

    /** Map of table ID -> TableInfo for datalake-enabled auto-partitioned tables. */
    @GuardedBy("lock")
    private final Map<Long, TableInfo> trackedTables = new HashMap<>();

    public HistoricalPartitionManager(
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            Configuration conf) {
        this(
                metadataCache,
                metadataManager,
                conf,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("historical-partition-manager")));
    }

    @VisibleForTesting
    HistoricalPartitionManager(
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            Configuration conf,
            ScheduledExecutorService syncExecutor) {
        this.metadataCache = metadataCache;
        this.metadataManager = metadataManager;
        this.syncExecutor = syncExecutor;
        this.syncIntervalMs = conf.get(ConfigOptions.HISTORICAL_PARTITION_SYNC_INTERVAL).toMillis();
        
        // Initialize metrics
        // Note: We'll need to initialize these properly in the future
        this.historicalPartitionCount = new SimpleCounter();
        this.expiredPartitionCleanupCount = new SimpleCounter();
        this.syncFailureCount = new SimpleCounter();
    }

    /** Start the historical partition manager. Schedules periodic sync with the data lake. */
    public void start() {
        checkNotClosed();
        syncExecutor.scheduleWithFixedDelay(
                this::syncWithLake,
                syncIntervalMs,
                syncIntervalMs,
                TimeUnit.MILLISECONDS);
        LOG.info(
                "Historical partition manager started with sync interval {}ms.",
                syncIntervalMs);
    }

    /**
     * Register a datalake-enabled auto-partitioned table for historical partition tracking.
     *
     * @param tableInfo the table info
     */
    public void registerTable(TableInfo tableInfo) {
        checkNotClosed();
        if (!shouldTrackTable(tableInfo)) {
            return;
        }

        inLock(
                lock,
                () -> {
                    trackedTables.put(tableInfo.getTableId(), tableInfo);
                    historicalPartitionsByTable.computeIfAbsent(
                            tableInfo.getTableId(), k -> new HashSet<>());
                });
        LOG.info(
                "Registered table {} (id={}) for historical partition tracking.",
                tableInfo.getTablePath(),
                tableInfo.getTableId());
    }

    /**
     * Unregister a table from historical partition tracking.
     *
     * @param tableId the table ID
     */
    public void unregisterTable(long tableId) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    trackedTables.remove(tableId);
                    historicalPartitionsByTable.remove(tableId);
                });
    }

    /**
     * Mark a partition as historical.
     *
     * <p>This is called when a partition expires in Fluss but the table has datalake enabled. The
     * partition metadata is retained, but Fluss data is cleaned up.
     *
     * @param tableId the table ID
     * @param partitionName the partition name
     */
    public void markPartitionAsHistorical(long tableId, String partitionName) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    Set<String> historicalPartitions = historicalPartitionsByTable.get(tableId);
                    if (historicalPartitions != null) {
                        boolean added = historicalPartitions.add(partitionName);
                        if (added) {
                            historicalPartitionCount.inc();
                        }
                        LOG.info(
                                "Marked partition {} of table {} as historical.",
                                partitionName,
                                tableId);
                    }
                });
    }

    /**
     * Remove a partition from historical tracking.
     *
     * <p>This is called when a partition is fully cleaned up (both Fluss and lake data expired).
     *
     * @param tableId the table ID
     * @param partitionName the partition name
     */
    public void removeHistoricalPartition(long tableId, String partitionName) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    Set<String> historicalPartitions = historicalPartitionsByTable.get(tableId);
                    if (historicalPartitions != null) {
                        boolean removed = historicalPartitions.remove(partitionName);
                        if (removed) {
                            historicalPartitionCount.dec();
                        }
                        LOG.info(
                                "Removed historical partition {} of table {}.",
                                partitionName,
                                tableId);
                    }
                });
    }

    /**
     * Check if a partition is a historical partition.
     *
     * @param tableId the table ID
     * @param partitionName the partition name
     * @return true if the partition is historical
     */
    public boolean isHistoricalPartition(long tableId, String partitionName) {
        return inLock(
                lock,
                () -> {
                    Set<String> historicalPartitions = historicalPartitionsByTable.get(tableId);
                    return historicalPartitions != null
                            && historicalPartitions.contains(partitionName);
                });
    }

    /**
     * Get the status of a partition.
     *
     * @param tableId the table ID
     * @param partitionName the partition name
     * @return the partition status
     */
    public PartitionStatus getPartitionStatus(long tableId, String partitionName) {
        if (isHistoricalPartition(tableId, partitionName)) {
            return PartitionStatus.HISTORICAL;
        }
        return PartitionStatus.ACTIVE;
    }

    /**
     * Get all historical partitions for a table.
     *
     * @param tableId the table ID
     * @return set of historical partition names
     */
    public Set<String> getHistoricalPartitions(long tableId) {
        return inLock(
                lock,
                () -> {
                    Set<String> historicalPartitions = historicalPartitionsByTable.get(tableId);
                    return historicalPartitions != null
                            ? new HashSet<>(historicalPartitions)
                            : new HashSet<>();
                });
    }

    /**
     * Check if a table should be tracked for historical partitions.
     *
     * @param tableInfo the table info
     * @return true if the table should be tracked
     */
    private boolean shouldTrackTable(TableInfo tableInfo) {
        // Track tables that have both datalake enabled and auto-partitioning enabled
        return tableInfo.getTableConfig().isDataLakeEnabled()
                && tableInfo.isPartitioned()
                && tableInfo.getTableConfig().getAutoPartitionStrategy().isAutoPartitionEnabled();
    }

    /**
     * Sync with the data lake to detect expired partitions.
     *
     * <p>For each tracked table:
     *
     * <ul>
     *   <li>Query Paimon to check if historical partitions have expired
     *   <li>Clean up metadata for partitions that have expired in both Fluss and lake
     * </ul>
     */
    private void syncWithLake() {
        LOG.debug("Starting sync with data lake for historical partitions.");
        
        // Get a snapshot of tracked tables and historical partitions
        Map<Long, TableInfo> tables;
        Map<Long, Set<String>> partitionsToCheck;

        // Copy under lock to avoid long lock holding
        inLock(
                lock,
                () -> {
                    tables = new HashMap<>(trackedTables);
                    partitionsToCheck = new HashMap<>();
                    for (Map.Entry<Long, Set<String>> entry : historicalPartitionsByTable.entrySet()) {
                        if (tables.containsKey(entry.getKey())) {
                            partitionsToCheck.put(entry.getKey(), new HashSet<>(entry.getValue()));
                        }
                    }
                });

        // Process each table
        for (Map.Entry<Long, TableInfo> tableEntry : tables.entrySet()) {
            long tableId = tableEntry.getKey();
            TableInfo tableInfo = tableEntry.getValue();
            Set<String> historicalPartitions = partitionsToCheck.get(tableId);
            
            if (historicalPartitions == null || historicalPartitions.isEmpty()) {
                continue;
            }
            
            try {
                // Create a Paimon handler for this table to check expiration
                PaimonHistoricalPartitionHandler paimonHandler = new PaimonHistoricalPartitionHandler(
                        getCatalogForTable(tableInfo), tableInfo.getTablePath());
                
                for (String partitionName : historicalPartitions) {
                    try {
                        boolean isExpiredInPaimon = paimonHandler.isPartitionExpiredInPaimon(partitionName);
                        
                        if (isExpiredInPaimon) {
                            // Partition has expired in Paimon, we can fully clean it up
                            LOG.info(
                                    "Partition {} of table {} (id={}) has expired in Paimon, cleaning up metadata.",
                                    partitionName, tableInfo.getTablePath(), tableId);
                            
                            // Remove from historical tracking
                            inLock(lock, () -> {
                                Set<String> histParts = historicalPartitionsByTable.get(tableId);
                                if (histParts != null) {
                                    histParts.remove(partitionName);
                                }
                            });
                            
                            // Clean up the partition metadata from ZK/storage
                            try {
                                metadataManager.dropPartition(
                                        tableInfo.getTablePath(),
                                        org.apache.fluss.metadata.ResolvedPartitionSpec.fromPartitionName(
                                                tableInfo.getPartitionKeys(), partitionName),
                                        true); // force delete even if not in ZK
                                expiredPartitionCleanupCount.inc();
                                LOG.debug(
                                        "Successfully cleaned up partition {} for table {}.",
                                        partitionName, tableInfo.getTablePath());
                            } catch (Exception e) {
                                syncFailureCount.inc();
                                LOG.error(
                                        "Failed to clean up partition {} for table {}, will retry in next sync.",
                                        partitionName, tableInfo.getTablePath(), e);
                                // Continue processing other partitions despite this failure
                                continue;
                            }
                        }
                    } catch (Exception e) {
                        LOG.warn(
                                "Failed to check expiration for partition {} of table {}, continuing...",
                                partitionName, tableInfo.getTablePath(), e);
                    }
                }
                
                paimonHandler.close();
            } catch (Exception e) {
                syncFailureCount.inc();
                LOG.warn(
                        "Failed to sync historical partitions for table {}, continuing with other tables.",
                        tableInfo.getTablePath(), e);
            }
        }

        LOG.debug("Completed sync with data lake for historical partitions.");
    }
    

    private org.apache.paimon.catalog.Catalog getCatalogForTable(TableInfo tableInfo) {
        // Create Paimon catalog based on table configuration
        try {
            return org.apache.fluss.lake.paimon.catalog.PaimonCatalogFactory.createCatalog(tableInfo);
        } catch (Exception e) {
            LOG.error("Failed to create Paimon catalog for table {}", tableInfo.getTablePath(), e);
            throw new RuntimeException(
                    String.format("Could not create Paimon catalog for table %s", tableInfo.getTablePath()), e);
        }
    }

    /**
     * Get the total count of historical partitions across all tables.
     *
     * @return the total number of historical partitions
     */
    public int getTotalHistoricalPartitionCount() {
        return inLock(
                lock,
                () -> {
                    int totalCount = 0;
                    for (Set<String> partitions : historicalPartitionsByTable.values()) {
                        totalCount += partitions.size();
                    }
                    return totalCount;
                });
    }

    private void checkNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("HistoricalPartitionManager is already closed.");
        }
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            syncExecutor.shutdownNow();
            LOG.info("Historical partition manager closed.");
        }
    }
}
