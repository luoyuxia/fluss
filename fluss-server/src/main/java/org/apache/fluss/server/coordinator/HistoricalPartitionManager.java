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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionStatus;
import org.apache.fluss.metadata.TableInfo;
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
    private static final long DEFAULT_LAKE_SYNC_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

    private final ScheduledExecutorService syncExecutor;
    private final MetadataManager metadataManager;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

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

    public HistoricalPartitionManager(MetadataManager metadataManager, Configuration conf) {
        this(
                metadataManager,
                conf,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("historical-partition-manager")));
    }

    @VisibleForTesting
    HistoricalPartitionManager(
            MetadataManager metadataManager,
            Configuration conf,
            ScheduledExecutorService syncExecutor) {
        this.metadataManager = metadataManager;
        this.syncExecutor = syncExecutor;
    }

    /** Start the historical partition manager. Schedules periodic sync with the data lake. */
    public void start() {
        checkNotClosed();
        syncExecutor.scheduleWithFixedDelay(
                this::syncWithLake,
                DEFAULT_LAKE_SYNC_INTERVAL_MS,
                DEFAULT_LAKE_SYNC_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
        LOG.info(
                "Historical partition manager started with sync interval {}ms.",
                DEFAULT_LAKE_SYNC_INTERVAL_MS);
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
                        historicalPartitions.add(partitionName);
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
                        historicalPartitions.remove(partitionName);
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
        Map<Long, TableInfo> tables;
        Map<Long, Set<String>> partitionsToCheck;

        // Copy under lock to avoid long lock holding
        inLock(
                lock,
                () -> {
                    // Nothing to sync
                });

        // TODO: Implement actual sync with Paimon
        // For each table, check if historical partitions have expired in Paimon
        // using PaimonHistoricalPartitionHandler.isPartitionExpiredInPaimon()
        // If expired, clean up the partition metadata

        LOG.debug("Completed sync with data lake for historical partitions.");
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
