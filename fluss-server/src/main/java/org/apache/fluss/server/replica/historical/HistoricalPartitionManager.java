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

package org.apache.fluss.server.replica.historical;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ConfigException;
import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.exception.InvalidPartitionException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.KvStateLookupResult.Status;
import org.apache.fluss.server.kv.historical.HistoricalValueLookup;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.Replica.HistoricalKvCleanupState;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.utils.ByteArraySlice;
import org.apache.fluss.utils.ByteArrayWrapper;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Coordinates lookup, write, and lifecycle operations for historical partitions. */
@Internal
public final class HistoricalPartitionManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalPartitionManager.class);
    private static final long MAX_HISTORICAL_KV_SIZE_BYTES = 5L * 1024 * 1024 * 1024;

    private final HistoricalPartitionTaskExecutor taskExecutor;
    private final HistoricalLakeLookupManager lakeLookupManager;
    private final Clock clock;
    private final @Nullable Scheduler cleanupScheduler;
    private final long maxHistoricalKvSizeBytes;

    private volatile long cleanupIdleTimeMs;
    private volatile boolean closed;

    /** Creates a historical partition manager from the tablet server dependencies. */
    public HistoricalPartitionManager(
            Configuration conf,
            @Nullable PluginManager pluginManager,
            LocalDiskManager localDiskManager,
            File dataDir,
            long dataDirVolumeBytes,
            Scheduler scheduler,
            Clock clock) {
        this(
                conf,
                new HistoricalPartitionTaskExecutor(conf),
                new HistoricalLakeLookupManager(
                        conf,
                        pluginManager,
                        localDiskManager,
                        dataDir,
                        dataDirVolumeBytes,
                        scheduler),
                clock,
                MAX_HISTORICAL_KV_SIZE_BYTES,
                scheduler);
    }

    @VisibleForTesting
    HistoricalPartitionManager(
            Configuration conf,
            HistoricalPartitionTaskExecutor taskExecutor,
            HistoricalLakeLookupManager lakeLookupManager,
            Clock clock,
            long maxHistoricalKvSizeBytes,
            @Nullable Scheduler cleanupScheduler) {
        Duration cleanupIdleTime =
                conf.get(ConfigOptions.SERVER_HISTORICAL_PARTITION_KV_CLEANUP_IDLE_TIME);
        checkArgument(
                !cleanupIdleTime.isNegative(),
                "%s must not be negative.",
                ConfigOptions.SERVER_HISTORICAL_PARTITION_KV_CLEANUP_IDLE_TIME.key());
        checkArgument(
                maxHistoricalKvSizeBytes > 0L, "maxHistoricalKvSizeBytes must be greater than 0.");
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.lakeLookupManager =
                checkNotNull(lakeLookupManager, "lakeLookupManager must not be null");
        this.clock = checkNotNull(clock, "clock must not be null");
        this.cleanupScheduler = cleanupScheduler;
        this.cleanupIdleTimeMs = cleanupIdleTime.toMillis();
        this.maxHistoricalKvSizeBytes = maxHistoricalKvSizeBytes;
    }

    /** Starts the resources used by historical partition operations. */
    public void startup(Scheduler scheduler) {
        lakeLookupManager.startup(scheduler);
    }

    /** Records new lake progress and schedules any cleanup that it makes eligible. */
    public void onLakeProgress(Replica replica, long lakeSnapshotId, long lakeLogEndOffset) {
        if (!replica.isLeader() || !replica.isKvTable()) {
            return;
        }
        int expectedLeaderEpoch = replica.getLeaderEpoch();
        long localLogEndOffset = replica.getLocalLogEndOffset();
        if (lakeLogEndOffset != localLogEndOffset) {
            return;
        }
        HistoricalKvCleanupState cleanupState = replica.getHistoricalKvCleanupState();
        if (cleanupState == null) {
            return;
        }
        cleanupState.updateCleanupCandidate(lakeSnapshotId, expectedLeaderEpoch, lakeLogEndOffset);
        tryScheduleCleanup(replica, cleanupState);
    }

    /** Looks up historical keys from local KV state and then lake storage. */
    public CompletableFuture<LookupResultForBucket> lookup(
            Replica replica,
            LookupDataForBucket lookupData,
            LakeTableLookuper.LookupMetricRecorder lookupMetricRecorder) {
        TableBucket tableBucket = lookupData.tableBucket();
        try {
            LakeTableLookuper.LookupMetricRecorder checkedMetricRecorder =
                    checkNotNull(lookupMetricRecorder, "lookupMetricRecorder must not be null.");
            return taskExecutor.submit(
                    () -> lookupInternal(replica, lookupData, checkedMetricRecorder),
                    () ->
                            new LookupResultForBucket(
                                    tableBucket,
                                    lookupData.originalPartitionName(),
                                    ApiError.fromThrowable(
                                            new HistoricalPartitionThrottledException(
                                                    "Historical lookup is throttled for "
                                                            + tableBucket
                                                            + " (original partition "
                                                            + lookupData.originalPartitionName()
                                                            + ") because the historical request "
                                                            + "queue is full."))));
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(
                    new LookupResultForBucket(
                            tableBucket,
                            lookupData.originalPartitionName(),
                            ApiError.fromThrowable(e)));
        }
    }

    /** Writes records to the local KV state of a historical partition. */
    public CompletableFuture<PutKvResultForBucket> putKv(
            Replica replica,
            PutKvDataForBucket putData,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            int requiredAcks) {
        try {
            String originalPartitionName =
                    checkNotNull(
                            putData.originalPartitionName(),
                            "originalPartitionName must not be null");
            HistoricalKvCleanupState cleanupState = replica.getHistoricalKvCleanupState();
            if (cleanupState == null) {
                throw new KvStorageException(
                        "Local historical KV state is not ready for "
                                + replica.getTableBucket()
                                + " because its KV tablet is being initialized or rebuilt.");
            }
            if (cleanupState.maxSizeReached()) {
                return CompletableFuture.completedFuture(
                        maxSizeThrottledResult(
                                putData, originalPartitionName, maxHistoricalKvSizeBytes));
            }
            long liveSstSize = replica.logicalStorageKvSize();
            if (liveSstSize >= maxHistoricalKvSizeBytes) {
                markMaxSizeReached(replica, cleanupState);
                return CompletableFuture.completedFuture(
                        maxSizeThrottledResult(
                                putData, originalPartitionName, maxHistoricalKvSizeBytes));
            }
            cleanupState.recordWrite(clock.milliseconds());
            return taskExecutor.submitOrdered(
                    putData.tableBucket(),
                    () -> {
                        try {
                            LogAppendInfo appendInfo =
                                    processPut(
                                            replica,
                                            putData,
                                            targetColumns,
                                            mergeMode,
                                            requiredAcks);
                            return PutKvResultForBucket.historicalSuccess(
                                    putData.tableBucket(),
                                    appendInfo.lastOffset() + 1,
                                    originalPartitionName);
                        } catch (Throwable t) {
                            return PutKvResultForBucket.historicalFailure(
                                    putData.tableBucket(),
                                    ApiError.fromThrowable(t),
                                    originalPartitionName);
                        }
                    },
                    () -> requestLimitThrottledResult(putData, originalPartitionName));
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(
                    PutKvResultForBucket.historicalFailure(
                            putData.tableBucket(),
                            ApiError.fromThrowable(e),
                            putData.originalPartitionName()));
        }
    }

    /** Validates dynamic historical partition configuration changes. */
    public void validate(Configuration newConf) throws ConfigException {
        Duration newCleanupIdleTime =
                newConf.get(ConfigOptions.SERVER_HISTORICAL_PARTITION_KV_CLEANUP_IDLE_TIME);
        if (newCleanupIdleTime.isNegative()) {
            throw new ConfigException(
                    String.format(
                            "Invalid configuration for %s, it must not be negative.",
                            ConfigOptions.SERVER_HISTORICAL_PARTITION_KV_CLEANUP_IDLE_TIME.key()));
        }
    }

    /** Applies dynamic historical partition configuration changes. */
    public void reconfigure(Configuration newConf) {
        lakeLookupManager.reconfigure(newConf);
        long newCleanupIdleTimeMs =
                newConf.get(ConfigOptions.SERVER_HISTORICAL_PARTITION_KV_CLEANUP_IDLE_TIME)
                        .toMillis();
        if (newCleanupIdleTimeMs == cleanupIdleTimeMs) {
            return;
        }
        long oldCleanupIdleTimeMs = cleanupIdleTimeMs;
        cleanupIdleTimeMs = newCleanupIdleTimeMs;
        LOG.info(
                "Historical KV cleanup idle time reconfigured: {} ms -> {} ms.",
                oldCleanupIdleTimeMs,
                newCleanupIdleTimeMs);
    }

    /** Invalidates the cached lake lookuper for the given table. */
    public void invalidateTableLookuper(long tableId) {
        lakeLookupManager.invalidateTableLookuper(tableId);
    }

    /** Requires future fallback lookups to refresh after the given lake snapshot notification. */
    public void requireLakeSnapshot(long tableId, long lakeSnapshotId) {
        lakeLookupManager.requireLakeSnapshot(tableId, lakeSnapshotId);
    }

    /** Returns the number of accepted historical operations that have not completed. */
    public int numInflightRequests() {
        return taskExecutor.numInflightRequests();
    }

    /** Returns the current disk usage of the historical lake lookup cache. */
    public long lookupCacheDiskSize() {
        return lakeLookupManager.lookupCacheDiskSize();
    }

    /** Returns the number of cached historical lake table lookupers. */
    public int cachedTableCount() {
        return lakeLookupManager.cachedTableCount();
    }

    /** Returns the counter for lookuper evictions caused by the table cache capacity. */
    public Counter capacityEvictions() {
        return lakeLookupManager.capacityEvictions();
    }

    @VisibleForTesting
    LogAppendInfo processPut(
            Replica replica,
            PutKvDataForBucket putData,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            int requiredAcks)
            throws Exception {
        TableInfo tableInfo = replica.getTableInfo();
        String originalPartitionName =
                checkNotNull(
                        putData.originalPartitionName(), "originalPartitionName must not be null");
        ResolvedPartitionSpec originalPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(
                        tableInfo.getPartitionKeys(), originalPartitionName);
        // The public put path holds the TableBucket ordering slot until processPut returns, so
        // local state cannot be changed by a later historical write between resolve and apply.
        int expectedLeaderEpoch = replica.getLeaderEpoch();
        List<byte[]> keysRequiringLakeLookup =
                replica.findKeysRequiringLakeLookup(
                        putData.records(),
                        targetColumns,
                        mergeMode,
                        originalPartitionName,
                        expectedLeaderEpoch,
                        requiredAcks);

        Map<ByteArrayWrapper, KvStateLookupResult> lakeResults = new HashMap<>();
        if (!keysRequiringLakeLookup.isEmpty()) {
            List<byte[]> lakeValues =
                    lakeLookupManager.lookup(
                            new LookupDataForBucket(
                                    putData.tableBucket(),
                                    keysRequiringLakeLookup,
                                    originalPartitionName),
                            tableInfo,
                            replica.getLatestSchemaInfo(),
                            originalPartitionSpec,
                            replica.tableMetrics()::recordHistoricalLakeLookup);
            for (int i = 0; i < keysRequiringLakeLookup.size(); i++) {
                byte[] lakeValue = lakeValues.get(i);
                lakeResults.put(
                        new ByteArrayWrapper(keysRequiringLakeLookup.get(i)),
                        lakeValue == null
                                ? KvStateLookupResult.notFound()
                                : KvStateLookupResult.present(lakeValue));
            }
        }

        HistoricalValueLookup memoizedLakeLookup =
                primaryKey -> {
                    KvStateLookupResult result =
                            checkNotNull(
                                    lakeResults.get(new ByteArrayWrapper(primaryKey)),
                                    "No resolved lake value for a historical write key");
                    return result.value();
                };

        return replica.putHistoricalRecordsToLeader(
                putData.records(),
                targetColumns,
                mergeMode,
                originalPartitionName,
                memoizedLakeLookup,
                expectedLeaderEpoch,
                requiredAcks);
    }

    private void markMaxSizeReached(Replica replica, HistoricalKvCleanupState cleanupState) {
        if (cleanupState.markMaxSizeReached()) {
            LOG.warn(
                    "Pausing historical writes for {} because its live SST size reached the "
                            + "maximum size {} bytes.",
                    replica.getTableBucket(),
                    maxHistoricalKvSizeBytes);

            tryScheduleCleanup(replica, cleanupState);
        }
    }

    private void tryScheduleCleanup(Replica replica, HistoricalKvCleanupState cleanupState) {
        HistoricalKvCleanupState.CleanupCandidate candidate = cleanupState.cleanupCandidate();
        if (candidate == null) {
            return;
        }
        scheduleCleanup(
                replica,
                cleanupState,
                cleanupState.maxSizeReached(),
                candidate.lakeSnapshotId(),
                candidate.expectedLeaderEpoch(),
                candidate.tieredLogEndOffset());
    }

    private void scheduleCleanup(
            Replica replica,
            HistoricalKvCleanupState cleanupState,
            boolean maxSizeReached,
            long lakeSnapshotId,
            int expectedLeaderEpoch,
            long tieredLogEndOffset) {
        // Reject cleanup if the replica no longer matches the cleanup state, leader epoch, or
        // tiered log end offset used when it was scheduled.
        if (closed
                || replica.getHistoricalKvCleanupState() != cleanupState
                || expectedLeaderEpoch != replica.getLeaderEpoch()
                || replica.getLocalLogEndOffset() != tieredLogEndOffset) {
            return;
        }

        if (!maxSizeReached
                && deferIdleCleanupIfNeeded(
                        replica,
                        cleanupState,
                        lakeSnapshotId,
                        expectedLeaderEpoch,
                        tieredLogEndOffset)) {
            return;
        }

        CompletableFuture<Void> cleanupFuture;
        cleanupFuture =
                taskExecutor.submitOrderedMaintenance(
                        replica.getTableBucket(),
                        () ->
                                runCleanup(
                                        replica,
                                        cleanupState,
                                        maxSizeReached,
                                        lakeSnapshotId,
                                        expectedLeaderEpoch,
                                        tieredLogEndOffset));
        cleanupFuture.whenComplete(
                (ignored, error) -> {
                    if (error != null) {
                        LOG.error(
                                "Historical KV cleanup failed for {}.",
                                replica.getTableBucket(),
                                error);
                    }
                });
    }

    private void runCleanup(
            Replica replica,
            HistoricalKvCleanupState cleanupState,
            boolean maxSizeReached,
            long lakeSnapshotId,
            int expectedLeaderEpoch,
            long tieredLogEndOffset) {
        if (replica.getHistoricalKvCleanupState() != cleanupState
                || expectedLeaderEpoch != replica.getLeaderEpoch()) {
            return;
        }
        if (!maxSizeReached
                && deferIdleCleanupIfNeeded(
                        replica,
                        cleanupState,
                        lakeSnapshotId,
                        expectedLeaderEpoch,
                        tieredLogEndOffset)) {
            return;
        }

        if (replica.cleanupHistoricalKv(
                expectedLeaderEpoch,
                tieredLogEndOffset,
                () -> requireLakeSnapshot(replica.getTableBucket().getTableId(), lakeSnapshotId))) {
            LOG.info(
                    "Cleaned {} local historical KV state for {}.",
                    maxSizeReached ? "max-size-triggered" : "idle-triggered",
                    replica.getTableBucket());
        }
    }

    /**
     * Returns whether idle cleanup must stop now. If the idle window has not elapsed, schedules the
     * next check at its deadline.
     */
    private boolean deferIdleCleanupIfNeeded(
            Replica replica,
            HistoricalKvCleanupState cleanupState,
            long lakeSnapshotId,
            int expectedLeaderEpoch,
            long tieredLogEndOffset) {
        long idleTimeMs = cleanupIdleTimeMs;
        if (idleTimeMs <= 0L) {
            return true;
        }
        long delayMs = remainingIdleCleanupDelayMs(cleanupState, idleTimeMs);
        if (delayMs <= 0L) {
            return false;
        }
        checkNotNull(cleanupScheduler, "cleanupScheduler must not be null")
                .scheduleOnce(
                        "historical-kv-idle-cleanup-" + replica.getTableBucket(),
                        () ->
                                scheduleCleanup(
                                        replica,
                                        cleanupState,
                                        false,
                                        lakeSnapshotId,
                                        expectedLeaderEpoch,
                                        tieredLogEndOffset),
                        delayMs);
        return true;
    }

    /** Returns the remaining delay before idle cleanup is eligible, or {@code 0} if it is due. */
    private long remainingIdleCleanupDelayMs(
            HistoricalKvCleanupState cleanupState, long idleTimeMs) {
        long now = clock.milliseconds();
        long lastWriteMs = cleanupState.lastWriteMs();
        if (now < lastWriteMs) {
            // A backward clock jump restarts the idle window instead of cleaning prematurely.
            return idleTimeMs;
        }
        return Math.max(0L, idleTimeMs - (now - lastWriteMs));
    }

    private static PutKvResultForBucket requestLimitThrottledResult(
            PutKvDataForBucket putData, String originalPartitionName) {
        return PutKvResultForBucket.historicalFailure(
                putData.tableBucket(),
                ApiError.fromThrowable(
                        new HistoricalPartitionThrottledException(
                                "Historical write is throttled for "
                                        + putData.tableBucket()
                                        + " (original partition "
                                        + originalPartitionName
                                        + ") because the historical request queue is full.")),
                originalPartitionName);
    }

    private static PutKvResultForBucket maxSizeThrottledResult(
            PutKvDataForBucket putData, String originalPartitionName, long maxHistoricalKvSize) {
        return PutKvResultForBucket.historicalFailure(
                putData.tableBucket(),
                ApiError.fromThrowable(
                        new HistoricalPartitionThrottledException(
                                "Historical write is throttled for "
                                        + putData.tableBucket()
                                        + " (original partition "
                                        + originalPartitionName
                                        + ") because its local historical KV state reached the live "
                                        + "SST maximum size of "
                                        + maxHistoricalKvSize
                                        + " bytes. New writes are paused until lake tiering "
                                        + "covers all previously accepted writes and cleanup of "
                                        + "the local historical KV state completes.")),
                originalPartitionName);
    }

    @Override
    public void close() {
        closed = true;
        taskExecutor.close();
        lakeLookupManager.close();
    }

    private LookupResultForBucket lookupInternal(
            Replica replica,
            LookupDataForBucket lookupData,
            LakeTableLookuper.LookupMetricRecorder lookupMetricRecorder) {
        TableBucket tableBucket = lookupData.tableBucket();
        String originalPartitionName = lookupData.originalPartitionName();
        try {
            TableInfo tableInfo = replica.getTableInfo();
            if (originalPartitionName == null) {
                throw new InvalidPartitionException(
                        "Historical lookup request must carry the original partition name.");
            }
            ResolvedPartitionSpec originalPartitionSpec =
                    ResolvedPartitionSpec.fromPartitionName(
                            tableInfo.getPartitionKeys(), originalPartitionName);

            List<KvStateLookupResult> localResults =
                    replica.lookupHistoricalLocal(originalPartitionName, lookupData.keys());
            List<byte[]> missingKeys = new ArrayList<>();
            for (int i = 0; i < localResults.size(); i++) {
                KvStateLookupResult localResult = localResults.get(i);
                // Only a true local miss falls back to lake. A local value or tombstone is
                // authoritative and must not be overwritten by an older lake value.
                if (localResult.status() == Status.NOT_FOUND) {
                    missingKeys.add(lookupData.keys().get(i));
                }
            }

            List<byte[]> lakeValues = Collections.emptyList();
            if (!missingKeys.isEmpty()) {
                // Look up all local misses together. Results preserve the order of missingKeys.
                lakeValues =
                        lakeLookupManager.lookup(
                                new LookupDataForBucket(
                                        tableBucket, missingKeys, originalPartitionName),
                                tableInfo,
                                replica.getLatestSchemaInfo(),
                                originalPartitionSpec,
                                lookupMetricRecorder);
            }

            Iterator<byte[]> lakeValueIterator = lakeValues.iterator();
            List<ByteArraySlice> values = new ArrayList<>(localResults.size());
            KvValueLayout localValueLayout =
                    KvValueLayout.fromTableConfig(tableInfo.getTableConfig());
            for (KvStateLookupResult localResult : localResults) {
                // Consume one lake value for each NOT_FOUND result; local values and tombstones
                // keep their original positions without advancing the lake iterator.
                if (localResult.status() == Status.NOT_FOUND) {
                    values.add(KvValueLayout.PLAIN.toValueBodySlice(lakeValueIterator.next()));
                } else {
                    values.add(localValueLayout.toValueBodySlice(localResult.value()));
                }
            }
            return new LookupResultForBucket(tableBucket, values, originalPartitionName);
        } catch (Exception e) {
            return new LookupResultForBucket(
                    tableBucket, originalPartitionName, ApiError.fromThrowable(e));
        }
    }
}
