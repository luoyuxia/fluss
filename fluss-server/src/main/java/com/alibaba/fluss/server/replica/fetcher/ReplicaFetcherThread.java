/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica.fetcher;

import com.alibaba.fluss.exception.CorruptRecordException;
import com.alibaba.fluss.exception.InvalidRecordException;
import com.alibaba.fluss.exception.RemoteStorageException;
import com.alibaba.fluss.exception.StorageException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.remote.RemoteLogFetchInfo;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.server.log.LogAppendInfo;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.log.remote.RemoteLogManager;
import com.alibaba.fluss.server.log.remote.RemoteLogStorage.IndexType;
import com.alibaba.fluss.server.metrics.group.TabletServerMetricGroup;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;
import com.alibaba.fluss.utils.log.FairBucketStatusMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Replica fetcher thread to fetch data from leader. */
final class ReplicaFetcherThread extends ShutdownableThread {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaFetcherThread.class);

    private final ReplicaManager replicaManager;
    private final LeaderEndpoint leader;
    private final int fetchBackOffMs;

    // TODO this range-robin fair map will take effect after we introduce fetch response limit size
    // in FetchLogRequest. trace id: FLUSS-56111098
    /**
     * A fair status map to store bucket fetch status. {@link TableBucket} -> {@link
     * BucketFetchStatus}.
     *
     * <p>Using this map instead of concurrent hash map to make sure each table bucket have the same
     * chance to be selected.
     */
    @GuardedBy("bucketStatusMapLock")
    private final FairBucketStatusMap<BucketFetchStatus> fairBucketStatusMap =
            new FairBucketStatusMap<>();

    private final Lock bucketStatusMapLock = new ReentrantLock();
    private final Condition bucketStatusMapCondition = bucketStatusMapLock.newCondition();

    private final TabletServerMetricGroup serverMetricGroup;

    public ReplicaFetcherThread(
            String name, ReplicaManager replicaManager, LeaderEndpoint leader, int fetchBackOffMs) {
        super(name, false);
        this.replicaManager = replicaManager;
        this.leader = leader;
        this.fetchBackOffMs = fetchBackOffMs;
        this.serverMetricGroup = replicaManager.getServerMetricGroup();
    }

    public LeaderEndpoint getLeader() {
        return leader;
    }

    public int getBucketCount() {
        return fairBucketStatusMap.size();
    }

    @Override
    public void doWork() {
        maybeFetch();
        // TODO, if we support fetch from follower, we need to complete delayed fetch log operation
        // here.
    }

    private void maybeFetch() {
        Optional<FetchLogRequest> fetchRequestOpt =
                inLock(
                        bucketStatusMapLock,
                        () -> {
                            Optional<FetchLogRequest> fetchLogRequest = Optional.empty();
                            try {
                                fetchLogRequest =
                                        leader.buildFetchLogRequest(
                                                fairBucketStatusMap.bucketStatusMap());
                                if (!fetchLogRequest.isPresent()) {
                                    LOG.info(
                                            "There are no active buckets. Back off for {} ms before "
                                                    + "sending a fetch fetchLogRequest",
                                            fetchBackOffMs);
                                    bucketStatusMapCondition.await(
                                            fetchBackOffMs, TimeUnit.MILLISECONDS);
                                }
                            } catch (InterruptedException e) {
                                LOG.error("Interrupted while awaiting fetch back off ms.", e);
                            }
                            return fetchLogRequest;
                        });

        fetchRequestOpt.ifPresent(this::processFetchLogRequest);
    }

    void removeBuckets(Set<TableBucket> tableBuckets) throws InterruptedException {
        bucketStatusMapLock.lockInterruptibly();
        try {
            tableBuckets.forEach(fairBucketStatusMap::remove);
        } finally {
            bucketStatusMapLock.unlock();
        }
    }

    void addBuckets(Map<TableBucket, InitialFetchStatus> initialFetchStatusMap)
            throws InterruptedException {
        bucketStatusMapLock.lockInterruptibly();
        try {
            initialFetchStatusMap.forEach(
                    (tableBucket, initialFetchStatus) -> {
                        BucketFetchStatus currentStatus =
                                fairBucketStatusMap.statusValue(tableBucket);
                        BucketFetchStatus updatedStatus =
                                bucketFetchStatus(tableBucket, initialFetchStatus, currentStatus);

                        fairBucketStatusMap.updateAndMoveToEnd(tableBucket, updatedStatus);
                    });

            bucketStatusMapCondition.signalAll();
        } finally {
            bucketStatusMapLock.unlock();
        }
    }

    private void removeBucket(TableBucket tableBucket) {
        try {
            inLock(bucketStatusMapLock, () -> removeBuckets(Collections.singleton(tableBucket)));
        } catch (InterruptedException e) {
            LOG.error("Interrupted while marking bucket as failed.", e);
        }
    }

    private void delayBuckets(Set<TableBucket> buckets, long delay) throws InterruptedException {
        bucketStatusMapLock.lockInterruptibly();
        try {
            for (TableBucket tableBucket : buckets) {
                BucketFetchStatus currentFetchStatus = fairBucketStatusMap.statusValue(tableBucket);
                if (currentFetchStatus != null && !currentFetchStatus.isDelayed()) {
                    // Updating the bucket fetch status and moving to end with a new DelayedItem.
                    BucketFetchStatus updatedFetchStatus =
                            new BucketFetchStatus(
                                    currentFetchStatus.tableId(),
                                    currentFetchStatus.fetchOffset(),
                                    new DelayedItem(delay));
                    fairBucketStatusMap.updateAndMoveToEnd(tableBucket, updatedFetchStatus);
                }
            }
            bucketStatusMapCondition.signalAll();
        } finally {
            bucketStatusMapLock.unlock();
        }
    }

    // TODO add fetch session to reduce the fetch request byte size.
    private void processFetchLogRequest(FetchLogRequest fetchRequest) {
        Set<TableBucket> bucketsWithError = new HashSet<>();
        Map<TableBucket, FetchLogResultForBucket> responseData = new HashMap<>();
        try {
            LOG.trace(
                    "Sending fetch log request {} to leader {}", fetchRequest, leader.leaderNode());
            // TODO this need not blocking to wait fetch log complete, change to async, see
            // FLUSS-56115172.
            LOG.info("Begin to fetch from leader node {}", leader.leaderNode());
            long startTime = System.currentTimeMillis();
            try {
                responseData = leader.fetchLog(fetchRequest).get(1, TimeUnit.MINUTES);
                LOG.info(
                        "Fetch from leader node {} cost {} ms",
                        leader.leaderNode(),
                        System.currentTimeMillis() - startTime);
            } catch (Exception t) {
                LOG.info(
                        "fetch from leader node {} exception with time {}",
                        leader.leaderNode(),
                        System.currentTimeMillis() - startTime,
                        t);
                throw t;
            }
        } catch (Throwable t) {
            if (isRunning()) {
                LOG.warn("Error in response for fetch log request {}", fetchRequest, t);
                //                inLock(
                //                        bucketStatusMapLock,
                //                        () ->
                // bucketsWithError.addAll(fairBucketStatusMap.bucketSet()));
            }
        }

        if (!responseData.isEmpty()) {
            bucketStatusMapLock.lock();
            try {
                handleFetchLogResponse(responseData, bucketsWithError);
            } finally {
                bucketStatusMapLock.unlock();
            }
        }

        if (!bucketsWithError.isEmpty()) {
            handleBucketWithError(bucketsWithError);
        }
    }

    private void handleFetchLogResponse(
            Map<TableBucket, FetchLogResultForBucket> responseData,
            Set<TableBucket> replicasWithError) {
        responseData.forEach(
                (tableBucket, replicaData) -> {
                    BucketFetchStatus currentFetchStatus =
                            fairBucketStatusMap.statusValue(tableBucket);
                    if (currentFetchStatus == null || !currentFetchStatus.isReadyForFetch()) {
                        return;
                    }

                    // TODO different error using different fix way.
                    switch (replicaData.getError().error()) {
                        case NONE:
                            handleFetchLogResponseOfSuccessBucket(
                                    tableBucket, currentFetchStatus, replicaData);
                            break;
                        case LOG_OFFSET_OUT_OF_RANGE_EXCEPTION:
                            if (!handleOutOfRangeError(tableBucket, currentFetchStatus)) {
                                replicasWithError.add(tableBucket);
                            }
                            break;
                        case NOT_LEADER_OR_FOLLOWER:
                            LOG.debug(
                                    "Remote server is not the leader for replica {}, which indicate "
                                            + "that the replica is being moved.",
                                    tableBucket);
                            break;
                        default:
                            LOG.error(
                                    "Error in response for fetching replica {}, error message is {}",
                                    tableBucket,
                                    replicaData.getErrorMessage());
                            replicasWithError.add(tableBucket);
                    }
                });
    }

    private void handleFetchLogResponseOfSuccessBucket(
            TableBucket tableBucket,
            BucketFetchStatus currentFetchStatus,
            FetchLogResultForBucket replicaData) {
        try {
            long nextFetchOffset = -1L;
            if (replicaData.fetchFromRemote()) {
                nextFetchOffset = processFetchResultFromRemoteStorage(tableBucket, replicaData);
            } else {
                LogAppendInfo logAppendInfo =
                        processFetchResultFromLocalStorage(
                                tableBucket, currentFetchStatus.fetchOffset(), replicaData);
                if (logAppendInfo.validBytes() > 0) {
                    nextFetchOffset = logAppendInfo.lastOffset() + 1;
                }
            }

            if (nextFetchOffset != -1L && fairBucketStatusMap.contains(tableBucket)) {
                BucketFetchStatus newFetchStatus =
                        new BucketFetchStatus(currentFetchStatus.tableId(), nextFetchOffset, null);
                fairBucketStatusMap.updateAndMoveToEnd(tableBucket, newFetchStatus);
            }
        } catch (Exception e) {
            if (e instanceof CorruptRecordException || e instanceof InvalidRecordException) {
                // we log the error and continue to ensure if there is a corrupt record in a table
                // bucket, it does not bring the fetcher thread down and cause other table bucket to
                // also lag.
                LOG.error(
                        "Found invalid record during fetch for bucket {} at offset {}",
                        tableBucket,
                        currentFetchStatus.fetchOffset(),
                        e);
            } else if (e instanceof StorageException) {
                LOG.error(
                        "Error while processing data for bucket {} at offset {}",
                        tableBucket,
                        currentFetchStatus.fetchOffset(),
                        e);
                removeBucket(tableBucket);
            } else {
                LOG.error(
                        "Unexpected error occurred while processing data for bucket {} at offset {}",
                        tableBucket,
                        currentFetchStatus.fetchOffset(),
                        e);
                removeBucket(tableBucket);
            }
        }
    }

    private boolean handleOutOfRangeError(TableBucket tableBucket, BucketFetchStatus fetchStatus) {
        try {
            BucketFetchStatus newFetchStatus = fetchOffsetAndTruncate(tableBucket);
            fairBucketStatusMap.updateAndMoveToEnd(tableBucket, newFetchStatus);
            LOG.info(
                    "Current offset {} for table bucket {} is out of range, which typically implies "
                            + "a leader change, Reset fetch offset to {}",
                    fetchStatus.fetchOffset(),
                    tableBucket,
                    newFetchStatus.fetchOffset());
        } catch (Exception e) {
            LOG.error("Error getting fetch offset for {} due to error", tableBucket, e);
            return false;
        }
        return true;
    }

    /** Handle a replica whose offset is out of range and return a new fetch offset. */
    private BucketFetchStatus fetchOffsetAndTruncate(TableBucket tableBucket) throws Exception {
        long replicaEndOffset =
                replicaManager.getReplicaOrException(tableBucket).getLocalLogEndOffset();

        /*
         * Unclean leader election: A follower goes down, in the meanwhile the leader keeps
         * appending messages. The follower comes back up, and before it has completely caught up
         * with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
         * elected as the new leader, and it starts appending messages from the client. The old
         * leader comes back up, becomes a follower, and it may discover that the current leader's
         * end offset is behind its own end offset.
         *
         * <p>In such a case, truncate the current follower's log to the current leader's end offset
         * and continue fetching.
         *
         * <p>There is a potential for a mismatch between the logs of the two replicas here. We
         * don't fix this mismatch as of now.
         */
        long leaderEndOffset;
        try {
            leaderEndOffset = leader.fetchLocalLogEndOffset(tableBucket).get(1, TimeUnit.MINUTES);
        } catch (Exception t) {
            LOG.error("fetchLocalLogEndOffset exception.", t);
            throw t;
        }
        if (leaderEndOffset < replicaEndOffset) {
            LOG.warn(
                    "Reset fetch offset for bucket {} from {} to current leader's latest offset {}",
                    tableBucket,
                    replicaEndOffset,
                    leaderEndOffset);
            truncate(tableBucket, leaderEndOffset);
            return new BucketFetchStatus(tableBucket.getTableId(), leaderEndOffset, null);
        } else {
            /*
             * If the leader's log end offset is greater than the follower's log end offset,
             * there are two possibilities:
             * 1. The follower could have been down for a long time and when
             *    it starts up, its end offset could be smaller than the leader's start offset because the
             *    leader has deleted old logs (log.logEndOffset < leaderStartOffset).
             * 2. When unclean leader election occurs, it is possible that the old leader's high watermark
             *    is greater than the new leader's log end offset. So when the old leader truncates its offset
             *    to its high watermark and starts to fetch from the new leader, an OffsetOutOfRangeException
             *    will be thrown. After that some more messages are write to the new leader. While the old leader
             *    is trying to handle the OffsetOutOfRangeException and query the log end offset of the new leader,
             *    the new leader's log end offset becomes higher than the follower's log end offset.
             *
             * If the follower's current log end offset is smaller than the leader's log
             * start offset, the follower should truncate all its logs, roll out a new segment and start to fetch
             * from the current leader's log start offset since the data are all stale.
             *
             * In the second case, the follower should just keep the current log segments and retry the fetch.
             * In the second case, there will be some inconsistency of data between old and new leader. We are
             * not solving it here. If users want to have strong consistency guarantees, appropriate configurations
             * needs to be set for both tablet servers and producers.
             *
             * */
            long leaderStartOffset;
            try {
                leaderStartOffset =
                        leader.fetchLocalLogStartOffset(tableBucket).get(1, TimeUnit.MINUTES);
            } catch (Exception e) {
                LOG.error("fetchLocalLogStartOffset exception.", e);
                throw e;
            }
            LOG.warn(
                    "Reset fetch offset for bucket {} from {} to current leader's start offset {}",
                    tableBucket,
                    replicaEndOffset,
                    leaderEndOffset);
            // Only truncate log when current leader's log start offset is greater than follower's
            // log end offset.
            if (leaderStartOffset > replicaEndOffset) {
                truncateFullyAndStartAt(tableBucket, leaderStartOffset);
            }

            long offsetToFetch = Math.max(leaderStartOffset, replicaEndOffset);
            return new BucketFetchStatus(tableBucket.getTableId(), offsetToFetch, null);
        }
    }

    private void handleBucketWithError(Set<TableBucket> buckets) {
        if (!buckets.isEmpty()) {
            LOG.debug("Handling errors in processFetchLogRequest for buckets {}", buckets);
            try {
                delayBuckets(buckets, fetchBackOffMs);
            } catch (InterruptedException e) {
                LOG.error("Interrupted while handle replica with error.", e);
            }
        }
    }

    /**
     * Returns initial bucket fetch status based on current status and the provided {@link
     * InitialFetchStatus}.
     */
    private BucketFetchStatus bucketFetchStatus(
            TableBucket tableBucket,
            InitialFetchStatus initialFetchStatus,
            @Nullable BucketFetchStatus currentFetchStatus) {
        if (currentFetchStatus != null) {
            return currentFetchStatus;
        } else {
            return new BucketFetchStatus(
                    tableBucket.getTableId(), initialFetchStatus.initOffset(), null);
        }
    }

    Optional<BucketFetchStatus> fetchStatus(TableBucket tableBucket) {
        return inLock(
                bucketStatusMapLock,
                () -> Optional.ofNullable(fairBucketStatusMap.statusValue(tableBucket)));
    }

    private LogAppendInfo processFetchResultFromLocalStorage(
            TableBucket tableBucket, long fetchOffset, FetchLogResultForBucket replicaData)
            throws Exception {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        LogTablet logTablet = replica.getLogTablet();

        MemoryLogRecords records = (MemoryLogRecords) replicaData.recordsOrEmpty();
        if (fetchOffset != logTablet.localLogEndOffset()) {
            throw new IllegalStateException(
                    String.format(
                            "Offset mismatch for replica %s: fetched offset %s, log end offset %s",
                            tableBucket, fetchOffset, logTablet.localLogEndOffset()));
        }

        LOG.trace(
                "Follower has replica log end offset {} for replica {}. received {} bytes of message and leader high watermark {}",
                logTablet.localLogEndOffset(),
                tableBucket,
                records.sizeInBytes(),
                replicaData.getHighWatermark());

        // Append the messages to the follower log tablet.
        LogAppendInfo logAppendInfo = replica.appendRecordsToFollower(records);
        LOG.trace(
                "Follower has replica log end offset {} after appending {} bytes of messages for replica {}",
                logTablet.localLogEndOffset(),
                records.sizeInBytes(),
                tableBucket);

        // For the follower replica, we do not need to keep its segment base offset and physical
        // position. These values will be computed upon becoming leader or handling a preferred read
        // replica fetch.
        logTablet.updateHighWatermark(replicaData.getHighWatermark());
        LOG.trace(
                "Follower received high watermark {} from the leader for replica {}",
                replicaData.getHighWatermark(),
                tableBucket);

        serverMetricGroup.replicationBytesIn().inc(records.sizeInBytes());

        return logAppendInfo;
    }

    private long processFetchResultFromRemoteStorage(
            TableBucket tb, FetchLogResultForBucket replicaData) throws Exception {
        RemoteLogFetchInfo rlFetchInfo = replicaData.remoteLogFetchInfo();
        checkNotNull(rlFetchInfo, "RemoteLogFetchInfo is null");
        Replica replica = replicaManager.getReplicaOrException(tb);
        long nextFetchOffset = -1L;
        RemoteLogManager rlm = replicaManager.getRemoteLogManager();

        // TODO after introduce leader epoch cache, we need to rebuild the local leader epoch
        // cache.

        // update next fetch offset and writer id snapshot in local.
        for (RemoteLogSegment remoteLogSegment : rlFetchInfo.remoteLogSegmentList()) {
            // build writer snapshots until remoteLogSegment.endOffset() and start segment from
            // until remoteLogSegment.endOffset().
            nextFetchOffset = remoteLogSegment.remoteLogEndOffset();

            // Truncate the existing local log before restoring the writer id snapshots.
            replica.truncateFullyAndStartAt(nextFetchOffset);

            // TODO maybe need increase log start offset.

            // Restore writer snapshot.
            LogTablet log = replica.getLogTablet();
            File snapshotFile = FlussPaths.writerSnapshotFile(log.getLogDir(), nextFetchOffset);
            buildWriterIdSnapshotFile(snapshotFile, remoteLogSegment, rlm);

            // Reload writer id snapshot.
            log.writerStateManager().truncateFullyAndReloadSnapshots();
            log.loadWriterSnapshot(nextFetchOffset);
            LOG.debug(
                    "Build the writer snapshots from remote storage for {} with active writer size: {} and remoteLogEndOffset: {}",
                    tb,
                    log.writerStateManager().activeWriters().size(),
                    nextFetchOffset);
        }
        return nextFetchOffset;
    }

    private void buildWriterIdSnapshotFile(
            File snapshotFile, RemoteLogSegment remoteLogSegment, RemoteLogManager rlm)
            throws RemoteStorageException, IOException {
        File tmpSnapshotFile = new File(snapshotFile.getAbsolutePath() + ".tmp");
        // Copy it to snapshot file in atomic manner.
        Files.copy(
                rlm.getRemoteLogStorage()
                        .fetchIndex(remoteLogSegment, IndexType.WRITER_ID_SNAPSHOT),
                tmpSnapshotFile.toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        FileUtils.atomicMoveWithFallback(tmpSnapshotFile.toPath(), snapshotFile.toPath(), false);
    }

    private void truncate(TableBucket tableBucket, long offset) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        LogTablet log = replica.getLogTablet();

        if (offset < log.getHighWatermark()) {
            LOG.warn(
                    "Truncating {} to offset {} below high watermark {}",
                    tableBucket,
                    offset,
                    log.getHighWatermark());
        }

        replica.truncateTo(offset);
    }

    private void truncateFullyAndStartAt(TableBucket tableBucket, long offset) {
        Replica replica = replicaManager.getReplicaOrException(tableBucket);
        replica.truncateFullyAndStartAt(offset);
    }

    @Override
    public boolean initiateShutdown() {
        return super.initiateShutdown();
    }

    @Override
    public void awaitShutdown() throws InterruptedException {
        super.awaitShutdown();
        // We don't expect any exceptions here, but catch and log any errors to avoid failing the
        // caller, especially during shutdown. It is safe to catch the exception here without
        // causing correctness issue because we are going to shut down the thread and will not
        // re-use the leaderEndpoint anyway.
        try {
            leader.close();
        } catch (Throwable t) {
            LOG.error("Failed to close after shutting down replica fetcher thread.", t);
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        initiateShutdown();
        inLock(bucketStatusMapLock, bucketStatusMapCondition::signalAll);
        awaitShutdown();
    }
}
