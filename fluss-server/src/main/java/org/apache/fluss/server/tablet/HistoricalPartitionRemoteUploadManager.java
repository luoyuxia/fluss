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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.replica.ReplicaManager;
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
 * Manager for handling remote log uploads specifically for historical partitions on tablet server.
 *
 * <p>For historical partitions with low update frequency, this manager periodically uploads
 * inactive log segments to remote storage and relies on remote TTL for cleanup. This manager
 * operates on the tablet server where the log segments actually reside.
 */
public class HistoricalPartitionRemoteUploadManager implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(HistoricalPartitionRemoteUploadManager.class);

    private final ScheduledExecutorService remoteUploadExecutor;
    private final ReplicaManager replicaManager;
    private final RemoteLogManager remoteLogManager;
    private final long remoteUploadIntervalMs;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final Lock lock = new ReentrantLock();

    /**
     * Map of table ID -> set of historical partition names. Only tracks partitions that are in
     * HISTORICAL status.
     */
    @GuardedBy("lock")
    private final Map<Long, Set<String>> historicalPartitionsByTable = new HashMap<>();

    public HistoricalPartitionRemoteUploadManager(
            ReplicaManager replicaManager, RemoteLogManager remoteLogManager, Configuration conf) {
        this(
                replicaManager,
                remoteLogManager,
                conf,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("historical-partition-remote-upload")));
    }

    HistoricalPartitionRemoteUploadManager(
            ReplicaManager replicaManager,
            RemoteLogManager remoteLogManager,
            Configuration conf,
            ScheduledExecutorService remoteUploadExecutor) {
        this.replicaManager = replicaManager;
        this.remoteLogManager = remoteLogManager;
        this.remoteUploadExecutor = remoteUploadExecutor;
        this.remoteUploadIntervalMs =
                conf.get(ConfigOptions.HISTORICAL_PARTITION_REMOTE_UPLOAD_INTERVAL).toMillis();
    }

    /** Start the historical partition remote upload manager. */
    public void start() {
        checkNotClosed();
        remoteUploadExecutor.scheduleWithFixedDelay(
                this::uploadInactiveSegmentsToRemote,
                remoteUploadIntervalMs,
                remoteUploadIntervalMs,
                TimeUnit.MILLISECONDS);
        LOG.info(
                "Historical partition remote upload manager started with interval {}ms.",
                remoteUploadIntervalMs);
    }

    /**
     * Register a historical partition for remote log upload tracking.
     *
     * @param tableId the table ID
     * @param partitionName the partition name
     */
    public void registerHistoricalPartition(long tableId, String partitionName) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    historicalPartitionsByTable
                            .computeIfAbsent(tableId, k -> new HashSet<>())
                            .add(partitionName);
                });
        LOG.info(
                "Registered historical partition {} for table {} (id={}) for remote log upload tracking.",
                partitionName,
                tableId);
    }

    /**
     * Unregister a historical partition from remote log upload tracking.
     *
     * @param tableId the table ID
     * @param partitionName the partition name
     */
    public void unregisterHistoricalPartition(long tableId, String partitionName) {
        checkNotClosed();
        inLock(
                lock,
                () -> {
                    Set<String> historicalPartitions = historicalPartitionsByTable.get(tableId);
                    if (historicalPartitions != null) {
                        historicalPartitions.remove(partitionName);
                        if (historicalPartitions.isEmpty()) {
                            historicalPartitionsByTable.remove(tableId);
                        }
                    }
                });
    }

    /**
     * Upload inactive log segments of historical partitions to remote storage.
     *
     * <p>For historical partitions with low update frequency, we periodically trigger the remote
     * log manager to upload inactive segments to remote storage and rely on remote TTL for cleanup.
     */
    private void uploadInactiveSegmentsToRemote() {
        LOG.debug("Starting remote upload of inactive segments for historical partitions.");

        // Get a snapshot of historical partitions to process
        Map<Long, Set<String>> partitionsToProcess = new HashMap<>();
        inLock(
                lock,
                () -> {
                    for (Map.Entry<Long, Set<String>> entry :
                            historicalPartitionsByTable.entrySet()) {
                        if (!entry.getValue().isEmpty()) {
                            partitionsToProcess.put(
                                    entry.getKey(), new HashSet<>(entry.getValue()));
                        }
                    }
                });

        // Process each historical partition
        for (Map.Entry<Long, Set<String>> entry : partitionsToProcess.entrySet()) {
            long tableId = entry.getKey();
            Set<String> partitions = entry.getValue();

            for (String partitionName : partitions) {
                try {
                    LOG.debug(
                            "Processing remote upload for historical partition {} of table {}.",
                            partitionName,
                            tableId);

                    // For historical partitions with low update frequency, we want to force
                    // the upload of inactive segments to remote storage regardless of size thresholds.
                    // This ensures that even small amounts of data in historical partitions are moved
                    // to remote storage where they can be managed by remote TTL policies.
                                        
                    // Find replicas that correspond to this historical partition and trigger
                    // immediate remote log tiering for them.
                    try {
                        // Iterate through all replicas managed by the replica manager
                        // to find those that belong to this historical partition
                        replicaManager.onlineReplicas().forEach(replica -> {
                            if (replica.getTableBucket().getTableId() == tableId) {
                                // Check if this replica's partition matches the historical partition
                                Long partitionId = replica.getTableBucket().getPartitionId();
                                if (partitionId != null) {
                                    // This is a replica for the table; we need to check if it matches
                                    // the specific historical partition by name
                                    String replicaPartitionName = replica.getPhysicalTablePath().getPartitionName();
                                    if (replicaPartitionName != null && 
                                         replicaPartitionName.equals(partitionName)) {
                                        // This replica belongs to the historical partition
                                        // Force remote log tiering for this replica
                                        try {
                                            // Get the replica's log tablet to check if it has segments that should be tiered
                                            LogTablet logTablet = replica.getLogTablet();
                                            
                                            // The remote log tiering happens automatically when a replica becomes leader
                                            // via the RemoteLogManager. For historical partitions, we want to make sure
                                            // the segments are processed even if they're small.
                                            
                                            LOG.debug(
                                                "Triggering remote log tiering for historical partition {} of table {} (bucket {})",
                                                partitionName, tableId, replica.getTableBucket());
                                            
                                            // The actual upload is handled by the existing LogTieringTask which runs
                                            // periodically. The task checks for candidate segments to copy based on
                                            // various conditions. For historical partitions, we ensure that the
                                            // remote log manager continues to process segments.
                                            
                                            // Now we can force immediate processing for this historical partition
                                            // to ensure segments are uploaded regardless of size thresholds
                                            remoteLogManager.forceImmediateProcessing(replica);
                                            LOG.debug(
                                                "Forced immediate remote log processing for historical partition {} of table {} (bucket {})",
                                                partitionName, tableId, replica.getTableBucket());
                                        } catch (Exception e) {
                                            LOG.warn(
                                                "Failed to trigger remote log tiering for replica {}", 
                                                replica.getTableBucket(), e);
                                        }
                                    }
                                }
                            }
                        });
                    } catch (Exception e) {
                        LOG.warn(
                            "Failed to process historical partition {} of table {} for remote upload", 
                            partitionName, tableId, e);
                    }

                } catch (Exception e) {
                    LOG.warn(
                            "Failed to process remote upload for historical partition {} of table {}, continuing...",
                            partitionName,
                            tableId,
                            e);
                }
            }
        }

        LOG.debug("Completed remote upload of inactive segments for historical partitions.");
    }

    private void checkNotClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException(
                    "HistoricalPartitionRemoteUploadManager is already closed.");
        }
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            remoteUploadExecutor.shutdownNow();
            LOG.info("Historical partition remote upload manager closed.");
        }
    }
}
