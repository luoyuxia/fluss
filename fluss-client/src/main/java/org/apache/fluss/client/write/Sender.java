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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.WriterMetricGroup;
import org.apache.fluss.client.write.RecordAccumulator.ReadyCheckResult;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.LeaderNotAvailableException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.PbPutKvRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.ExponentialBackoff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makeProduceLogRequest;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.makePutKvRequest;
import static org.apache.fluss.client.utils.ClientRpcMessageUtils.validatePutKvWriteBatches;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This background thread handles the sending of produce requests to the tablet server. This thread
 * makes metadata requests to renew its view of the cluster and then sends produce requests to the
 * appropriate nodes.
 */
public class Sender implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Sender.class);

    /** the record accumulator that batches records. */
    private final RecordAccumulator accumulator;

    /** the maximum request timeout to attempt to send to the server. */
    private final int maxRequestTimeoutMs;

    /** the maximum request size to attempt to send to the server. */
    private final int maxRequestSize;

    /** the number of acknowledgements to request from the server. */
    private final short acks;

    /** the number of times to retry a failed write batch before giving up. */
    private final int retries;

    /** true while the sender thread is still running. */
    private volatile boolean running;

    /** true when the caller wants to ignore all unsent/inflight messages and force close. */
    private volatile boolean forceClose;

    /**
     * A per-bucket queue of batches ordered by creation time for tracking the in-flight batches.
     */
    @GuardedBy("inFlightBatchesLock")
    private final Map<TableBucket, List<ReadyWriteBatch>> inFlightBatches;

    private final Object inFlightBatchesLock = new Object();

    // TODO if we introduce client metadata cache, these parameters need to remove.
    private final MetadataUpdater metadataUpdater;

    /** all the state related to writer, in particular the writer id and batch sequence numbers. */
    private final IdempotenceManager idempotenceManager;

    private final WriterMetricGroup writerMetricGroup;
    private final DynamicPartitionCreator dynamicPartitionCreator;
    private final ExponentialBackoff historicalThrottleBackoff;

    public Sender(
            RecordAccumulator accumulator,
            int maxRequestTimeoutMs,
            int maxRequestSize,
            short acks,
            int retries,
            MetadataUpdater metadataUpdater,
            IdempotenceManager idempotenceManager,
            WriterMetricGroup writerMetricGroup,
            DynamicPartitionCreator dynamicPartitionCreator) {
        this.accumulator = accumulator;
        this.maxRequestSize = maxRequestSize;
        this.maxRequestTimeoutMs = maxRequestTimeoutMs;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.inFlightBatches = new HashMap<>();

        this.metadataUpdater = metadataUpdater;
        checkNotNull(metadataUpdater.getCoordinatorServer());

        this.idempotenceManager = idempotenceManager;
        this.writerMetricGroup = writerMetricGroup;
        this.dynamicPartitionCreator = dynamicPartitionCreator;
        this.historicalThrottleBackoff = new ExponentialBackoff(100L, 2, 5000L, 0.2);

        // TODO add retry logic while send failed. See FLUSS-56364375
    }

    @VisibleForTesting
    int numOfInFlightBatches(TableBucket tb) {
        synchronized (inFlightBatchesLock) {
            return inFlightBatches.containsKey(tb) ? inFlightBatches.get(tb).size() : 0;
        }
    }

    @Override
    public void run() {
        LOG.debug("Starting Fluss write sender thread.");

        // main loop, runs until close is called.
        while (running) {
            try {
                runOnce();
            } catch (Throwable t) {
                LOG.error("Uncaught error in Fluss write sender thread: ", t);
            }
        }

        LOG.debug(
                "Beginning shutdown of Fluss log record write I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be requests in the accumulator or
        // waiting for acknowledgment, wait until these are completed.
        // TODO Check the in flight request count in the accumulator.
        while (!forceClose && ((accumulator.hasUnDrained()))) {
            try {
                runOnce();
            } catch (Exception e) {
                LOG.error("Uncaught error in Fluss write sender I/O thread: ", e);
            }
        }

        destroyResources();

        // TODO if force close failed, add logic to abort incomplete batches.
        LOG.debug("Shutdown of Fluss write sender I/O thread has completed.");
    }

    /** Run a single iteration of sending. */
    public void runOnce() throws Exception {
        if (idempotenceManager.idempotenceEnabled()) {
            // may be wait for writer id.
            Set<PhysicalTablePath> targetTables = accumulator.getPhysicalTablePathsInBatches();
            // TODO: only request to init writer_id when we have valid target tables
            try {
                idempotenceManager.maybeWaitForWriterId(targetTables);
            } catch (Throwable t) {
                // TODO: If 'only request to init writer_id when we have valid target tables' have
                // been down, this if check can be removed.
                if (!targetTables.isEmpty()) {
                    maybeAbortBatches((Exception) t);
                } else {
                    LOG.trace("No target tables, ignore init writer id error", t);
                }
            }
        }

        // do send.
        sendWriteData();
    }

    public boolean isRunning() {
        return running;
    }

    private void addToInflightBatches(Map<Integer, List<ReadyWriteBatch>> batches) {
        synchronized (inFlightBatchesLock) {
            batches.values().forEach(this::addToInflightBatches);
        }
    }

    private void sendWriteData() throws Exception {
        Cluster clusterSnapshot = metadataUpdater.getCluster();

        // get the list of buckets with data ready to send.
        ReadyCheckResult readyCheckResult = accumulator.ready(clusterSnapshot);

        // if there are any buckets whose leaders are not known yet, force metadata update
        if (!readyCheckResult.unknownLeaderTables.isEmpty()) {
            try {
                metadataUpdater.updatePhysicalTableMetadata(readyCheckResult.unknownLeaderTables);
            } catch (Exception e) {
                Throwable t = ExceptionUtils.stripExecutionException(e);
                if (t instanceof PartitionNotExistException) {
                    // A missing original partition may have expired into historical storage.
                    // Clear stale bucket and partition metadata first, then resolve each affected
                    // queue again so eligible batches can switch to the historical target.
                    metadataUpdater.invalidPhysicalTableBucketAndPartitionMeta(
                            readyCheckResult.unknownLeaderTables);
                    readyCheckResult.unknownLeaderTables.forEach(this::rerouteBatches);
                } else {
                    throw e;
                }
            }
            LOG.debug(
                    "Client update metadata due to unknown leader tables from the batched records: {}",
                    readyCheckResult.unknownLeaderTables);
        }

        Set<Integer> readyNodes = readyCheckResult.readyNodes;
        if (readyNodes.isEmpty()) {
            // TODO The method sendWriteData is in a busy loop. If there is no data continuously, it
            // will cause the CPU to be occupied.
            // In the future, we need to introduce delay logic to deal with it.
            // TODO: condition waiter
            Thread.sleep(readyCheckResult.nextReadyCheckDelayMs);
        }

        // get the list of batches prepare to send.
        Map<Integer, List<ReadyWriteBatch>> batches =
                accumulator.drain(clusterSnapshot, readyNodes, maxRequestSize);

        if (!batches.isEmpty()) {
            addToInflightBatches(batches);

            // TODO add logic for batch expire.

            sendWriteRequests(batches);

            // move metrics update to the end to make sure the batches has been built.
            updateWriterMetrics(batches);
        }
    }

    private void rerouteBatches(PhysicalTablePath physicalTablePath) {
        TableInfo tableInfo = accumulator.getTableInfo(physicalTablePath);
        if (tableInfo == null) {
            return;
        }

        ResolvedWriteTarget resolvedWriteTarget =
                dynamicPartitionCreator.resolveWriteTarget(physicalTablePath, tableInfo);
        if (resolvedWriteTarget.isHistorical()) {
            accumulator.rerouteToHistorical(physicalTablePath, resolvedWriteTarget);
        }
    }

    private void completeBatch(ReadyWriteBatch readyWriteBatch) {
        if (idempotenceManager.idempotenceEnabled()) {
            idempotenceManager.handleCompletedBatch(readyWriteBatch);
        }
        if (readyWriteBatch.writeBatch().complete()) {
            maybeRemoveAndDeallocateBatch(readyWriteBatch);
        }
    }

    /** Complete batch with bucket and log end offset info (for KV batches). */
    private void completeBatch(
            ReadyWriteBatch readyWriteBatch, TableBucket bucket, long logEndOffset) {
        if (idempotenceManager.idempotenceEnabled()) {
            idempotenceManager.handleCompletedBatch(readyWriteBatch);
        }
        if (readyWriteBatch.writeBatch().complete(bucket, logEndOffset)) {
            maybeRemoveAndDeallocateBatch(readyWriteBatch);
        }
    }

    private void failBatch(
            ReadyWriteBatch batch, Exception exception, boolean adjustBatchSequences) {
        if (batch.writeBatch().completeExceptionally(exception)) {
            if (idempotenceManager.idempotenceEnabled()) {
                try {
                    // This call can throw an exception in the rare case that there's an invalid
                    // state
                    // transition attempted. Catch these so as not to interfere with the rest of the
                    // logic.
                    idempotenceManager.handleFailedBatch(batch, exception, adjustBatchSequences);
                } catch (Exception e) {
                    LOG.debug(
                            "Encountered error when idempotence manager was handling a failed batch",
                            e);
                }
            }
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    private void maybeAbortBatches(Exception exception) {
        if (accumulator.hasIncomplete()) {
            LOG.error("Aborting write batches due to fatal error", exception);
            accumulator.abortAllBatches(exception);
        }
    }

    private void reEnqueueBatch(ReadyWriteBatch readyWriteBatch) {
        accumulator.reEnqueue(readyWriteBatch);
        maybeRemoveFromInflightBatches(readyWriteBatch);

        // metrics for retry record count.
        writerMetricGroup.recordsRetryTotal().inc(readyWriteBatch.writeBatch().getRecordCount());
    }

    /**
     * We can retry a round of send if the error is transient and the number of attempts taken is
     * fewer than the maximum allowed. We can also retry {@link OutOfOrderSequenceException}
     * exceptions for future batches, since if the first batch has failed, the future batches are
     * certain to fail with an {@link OutOfOrderSequenceException} exception.
     */
    private boolean canRetry(ReadyWriteBatch readyWriteBatch, Errors error) {
        WriteBatch batch = readyWriteBatch.writeBatch();
        return batch.attempts() < retries
                && !batch.isDone()
                && ((error.exception() instanceof RetriableException)
                        || (idempotenceManager.idempotenceEnabled()
                                && idempotenceManager.canRetry(
                                        batch, readyWriteBatch.tableBucket(), error)));
    }

    private void maybeRemoveAndDeallocateBatch(ReadyWriteBatch readyWriteBatch) {
        maybeRemoveFromInflightBatches(readyWriteBatch);
        accumulator.deallocate(readyWriteBatch.writeBatch());
    }

    private void maybeRemoveFromInflightBatches(ReadyWriteBatch batch) {
        synchronized (inFlightBatchesLock) {
            List<ReadyWriteBatch> batches = inFlightBatches.get(batch.tableBucket());
            if (batches != null) {
                batches.remove(batch);
                if (batches.isEmpty()) {
                    inFlightBatches.remove(batch.tableBucket());
                }
            }
        }
    }

    private void addToInflightBatches(List<ReadyWriteBatch> batches) {
        synchronized (inFlightBatchesLock) {
            batches.forEach(
                    batch ->
                            inFlightBatches
                                    .computeIfAbsent(batch.tableBucket(), k -> new ArrayList<>())
                                    .add(batch));
        }
    }

    /** Transfer the record batches into a list of produce log requests on a per-node basis. */
    private void sendWriteRequests(Map<Integer, List<ReadyWriteBatch>> collated) {
        collated.forEach((leaderId, batches) -> sendWriteRequest(leaderId, acks, batches));
    }

    /**
     * Create a write request from the given record batches. The write request maybe {@link
     * ProduceLogRequest} or {@link PutKvRequest}.
     */
    private void sendWriteRequest(int destination, short acks, List<ReadyWriteBatch> batches) {
        if (batches.isEmpty()) {
            return;
        }

        // Group record batches by table id while keeping their drain order.
        Map<Long, List<ReadyWriteBatch>> writeBatchByTable = new LinkedHashMap<>();
        batches.forEach(
                batch ->
                        writeBatchByTable
                                .computeIfAbsent(
                                        batch.tableBucket().getTableId(), k -> new ArrayList<>())
                                .add(batch));

        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(destination);
        LeaderNotAvailableException unavailableException =
                gateway == null
                        ? new LeaderNotAvailableException(
                                "Server " + destination + " is not found in metadata cache.")
                        : null;
        writeBatchByTable.forEach(
                (tableId, writeBatches) -> {
                    if (isLogBatches(writeBatches)) {
                        for (List<ReadyWriteBatch> requestGroup :
                                packProduceLogRequestGroups(writeBatches)) {
                            Map<TableBucket, ReadyWriteBatch> recordsByBucket =
                                    recordsByBucket(requestGroup);
                            if (gateway == null) {
                                handleWriteRequestException(unavailableException, recordsByBucket);
                            } else {
                                sendProduceLogRequestAndHandleResponse(
                                        gateway,
                                        makeProduceLogRequest(
                                                tableId, acks, maxRequestTimeoutMs, requestGroup),
                                        tableId,
                                        recordsByBucket);
                            }
                        }
                    } else {
                        List<List<ReadyWriteBatch>> requestGroups =
                                packPutKvRequestGroups(writeBatches);
                        if (requestGroups.size() > 1) {
                            // Keep the existing table-level validation across request boundaries.
                            // A single group is validated by makePutKvRequest below.
                            validatePutKvWriteBatches(writeBatches);
                        }
                        for (List<ReadyWriteBatch> requestGroup : requestGroups) {
                            Map<TableBucket, ReadyWriteBatch> recordsByBucket =
                                    recordsByBucket(requestGroup);
                            if (gateway == null) {
                                handleWriteRequestException(unavailableException, recordsByBucket);
                            } else {
                                sendPutKvRequestAndHandleResponse(
                                        gateway,
                                        makePutKvRequest(
                                                tableId, acks, maxRequestTimeoutMs, requestGroup),
                                        tableId,
                                        recordsByBucket);
                            }
                        }
                    }
                });
    }

    /**
     * Packs log batches into request groups while preserving their input order.
     *
     * <p>Each {@link TableBucket} can appear at most once in a group because both the server
     * request decoder and the client response handler correlate log batches by table bucket.
     */
    @VisibleForTesting
    static List<List<ReadyWriteBatch>> packProduceLogRequestGroups(List<ReadyWriteBatch> batches) {
        List<List<ReadyWriteBatch>> groups = new ArrayList<>();
        List<ReadyWriteBatch> currentGroup = null;
        Set<TableBucket> tableBuckets = new HashSet<>();
        for (ReadyWriteBatch batch : batches) {
            checkArgument(batch.writeBatch().isLogBatch(), "batch must be a log batch");
            if (currentGroup == null || tableBuckets.contains(batch.tableBucket())) {
                currentGroup = new ArrayList<>();
                groups.add(currentGroup);
                tableBuckets = new HashSet<>();
            }
            currentGroup.add(batch);
            tableBuckets.add(batch.tableBucket());
        }
        return groups;
    }

    /**
     * Packs KV batches into request groups while preserving their input order.
     *
     * <p>A group contains either normal batches or historical batches. Each {@link TableBucket} can
     * appear at most once because a put-KV response identifies a batch only by table bucket and
     * does not carry the original partition name. Historical batches for different table buckets
     * may stay in the same group even when their original partition names differ.
     */
    @VisibleForTesting
    static List<List<ReadyWriteBatch>> packPutKvRequestGroups(List<ReadyWriteBatch> batches) {
        List<List<ReadyWriteBatch>> groups = new ArrayList<>();
        List<ReadyWriteBatch> currentGroup = null;
        // PutKv responses and the server-side request decoder both identify records only by
        // TableBucket. Track the buckets already used by the current RPC to prevent one batch
        // from overwriting another when different original queues share a historical bucket.
        Set<TableBucket> tableBuckets = new HashSet<>();
        boolean currentGroupHistorical = false;

        for (ReadyWriteBatch batch : batches) {
            checkArgument(!batch.writeBatch().isLogBatch(), "batch must be a KV batch");
            KvWriteBatch kvWriteBatch = (KvWriteBatch) batch.writeBatch();
            boolean historical = kvWriteBatch.getOriginalPartitionName() != null;
            // A PutKv RPC must contain only one write kind because the server dispatches the whole
            // request as normal or historical. It must also contain each TableBucket at most once
            // so every response can be correlated with exactly one batch.
            if (currentGroup == null
                    || historical != currentGroupHistorical
                    || tableBuckets.contains(batch.tableBucket())) {
                currentGroup = new ArrayList<>();
                groups.add(currentGroup);
                // A new RPC has its own response-correlation scope, so the same TableBucket may be
                // used again in this group.
                tableBuckets = new HashSet<>();
                currentGroupHistorical = historical;
            }
            // Append greedily to preserve the input order within and across request groups.
            currentGroup.add(batch);
            tableBuckets.add(batch.tableBucket());
        }
        return groups;
    }

    private static Map<TableBucket, ReadyWriteBatch> recordsByBucket(
            List<ReadyWriteBatch> batches) {
        Map<TableBucket, ReadyWriteBatch> recordsByBucket = new HashMap<>();
        for (ReadyWriteBatch batch : batches) {
            checkArgument(
                    recordsByBucket.put(batch.tableBucket(), batch) == null,
                    "Duplicate table bucket %s in one write request.",
                    batch.tableBucket());
        }
        return recordsByBucket;
    }

    /**
     * Check whether the given batches are log batches. We assume all the batches are of the same
     * type.
     *
     * @param batches the batches to check, must not be empty.
     * @return true if the given batches are log batches, false if they are kv batches.
     */
    private boolean isLogBatches(List<ReadyWriteBatch> batches) {
        checkArgument(!batches.isEmpty(), "batches must not be empty");
        ReadyWriteBatch batch = batches.get(0);
        return batch.writeBatch().isLogBatch();
    }

    private void sendProduceLogRequestAndHandleResponse(
            TabletServerGateway gateway,
            ProduceLogRequest request,
            long tableId,
            Map<TableBucket, ReadyWriteBatch> recordsByBucket) {
        long startTime = System.currentTimeMillis();
        gateway.produceLog(request)
                .whenComplete(
                        (produceLogResponse, e) -> {
                            writerMetricGroup.setSendLatencyInMs(
                                    System.currentTimeMillis() - startTime);
                            if (e != null) {
                                handleWriteRequestException(e, recordsByBucket);
                            } else {
                                handleProduceLogResponse(
                                        produceLogResponse, tableId, recordsByBucket);
                            }
                        });
    }

    private void sendPutKvRequestAndHandleResponse(
            TabletServerGateway gateway,
            PutKvRequest request,
            long tableId,
            Map<TableBucket, ReadyWriteBatch> recordsByBucket) {
        long startTime = System.currentTimeMillis();
        gateway.putKv(request)
                .whenComplete(
                        (putKvResponse, e) -> {
                            writerMetricGroup.setSendLatencyInMs(
                                    System.currentTimeMillis() - startTime);
                            if (e != null) {
                                handleWriteRequestException(e, recordsByBucket);
                            } else {
                                handlePutKvResponse(putKvResponse, tableId, recordsByBucket);
                            }
                        });
    }

    private void handleProduceLogResponse(
            ProduceLogResponse response,
            long tableId,
            Map<TableBucket, ReadyWriteBatch> recordsByBucket) {
        Set<PhysicalTablePath> invalidMetadataTablesSet = new HashSet<>();
        for (PbProduceLogRespForBucket logRespForBucket : response.getBucketsRespsList()) {
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            logRespForBucket.hasPartitionId()
                                    ? logRespForBucket.getPartitionId()
                                    : null,
                            logRespForBucket.getBucketId());
            ReadyWriteBatch writeBatch = recordsByBucket.get(tb);
            if (logRespForBucket.hasErrorCode()) {
                Set<PhysicalTablePath> invalidMetadataTables =
                        handleWriteBatchException(
                                writeBatch, ApiError.fromErrorMessage(logRespForBucket));
                invalidMetadataTablesSet.addAll(invalidMetadataTables);
            } else {
                completeBatch(writeBatch);
            }
        }
        metadataUpdater.invalidPhysicalTableBucketMeta(invalidMetadataTablesSet);
    }

    private void handlePutKvResponse(
            PutKvResponse putKvResponse,
            long tableId,
            Map<TableBucket, ReadyWriteBatch> recordsByBucket) {
        Set<PhysicalTablePath> invalidMetadataTablesSet = new HashSet<>();
        for (PbPutKvRespForBucket respForBucket : putKvResponse.getBucketsRespsList()) {
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            respForBucket.hasPartitionId() ? respForBucket.getPartitionId() : null,
                            respForBucket.getBucketId());
            ReadyWriteBatch writeBatch = recordsByBucket.get(tb);
            if (respForBucket.hasErrorCode()) {
                Set<PhysicalTablePath> invalidMetadataTables =
                        handleWriteBatchException(
                                writeBatch, ApiError.fromErrorMessage(respForBucket));
                invalidMetadataTablesSet.addAll(invalidMetadataTables);
            } else {
                // Get log end offset (LEO) from response for KV batches
                long logEndOffset =
                        respForBucket.hasLogEndOffset() ? respForBucket.getLogEndOffset() : -1;
                completeBatch(writeBatch, tb, logEndOffset);
            }
        }
        metadataUpdater.invalidPhysicalTableBucketMeta(invalidMetadataTablesSet);
    }

    private void handleWriteRequestException(
            Throwable t, Map<TableBucket, ReadyWriteBatch> recordsByBucket) {
        ApiError error = ApiError.fromThrowable(t);

        // if batch failed because of retrievable exception, we need to retry send all those
        // batches.
        Set<PhysicalTablePath> invalidMetadataTablesSet = new HashSet<>();
        for (ReadyWriteBatch batch : recordsByBucket.values()) {
            Set<PhysicalTablePath> invalidMetadataTables = handleWriteBatchException(batch, error);
            invalidMetadataTablesSet.addAll(invalidMetadataTables);
        }

        metadataUpdater.invalidPhysicalTableBucketMeta(invalidMetadataTablesSet);
    }

    /** Handle the exception and return actual targets whose bucket metadata is invalid. */
    private Set<PhysicalTablePath> handleWriteBatchException(
            ReadyWriteBatch readyWriteBatch, ApiError error) {
        Set<PhysicalTablePath> invalidMetadataTables = new HashSet<>();
        WriteBatch writeBatch = readyWriteBatch.writeBatch();
        if (error.error() == Errors.DUPLICATE_SEQUENCE_EXCEPTION) {
            // If we have received a duplicate batch sequence error, it means that the batch
            // sequence has advanced beyond the sequence of the current batch.
            // The only thing we can do is to return success to the user.
            completeBatch(readyWriteBatch);
        } else if (error.error() == Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION
                && idempotenceManager.idempotenceEnabled()
                && idempotenceManager.isAlreadyCommitted(
                        writeBatch, readyWriteBatch.tableBucket())) {
            // The batch received OUT_OF_ORDER_SEQUENCE_EXCEPTION but its sequence is already
            // <= lastAckedBatchSequence, which means it was successfully written on the server
            // but the response was lost. Complete it as success to avoid infinite retry loop.
            LOG.warn(
                    "Batch for table-bucket {} with sequence {} received "
                            + "OUT_OF_ORDER_SEQUENCE_EXCEPTION but has already been committed "
                            + "(lastAckedBatchSequence={}). Treating as success due to lost response.",
                    readyWriteBatch.tableBucket(),
                    writeBatch.batchSequence(),
                    idempotenceManager.lastAckedBatchSequence(readyWriteBatch.tableBucket()));
            completeBatch(readyWriteBatch);
        } else if (canRetry(readyWriteBatch, error.error())) {
            // if batch failed because of retrievable exception, we need to retry send all those
            // batches.
            LOG.warn(
                    "Get error write response on table bucket {}, retrying ({} attempts left). Error: {}",
                    readyWriteBatch.tableBucket(),
                    retries - writeBatch.attempts(),
                    error.formatErrMsg());

            if (error.exception() instanceof HistoricalPartitionThrottledException
                    && writeBatch instanceof KvWriteBatch
                    && ((KvWriteBatch) writeBatch).getOriginalPartitionName() != null) {
                long retryDelayMs = historicalThrottleBackoff.backoff(writeBatch.attempts());
                writeBatch.setNextRetryTimeMs(accumulator.currentTimeMs() + retryDelayMs);
            }

            if (!idempotenceManager.idempotenceEnabled()) {
                reEnqueueBatch(readyWriteBatch);
            } else if (idempotenceManager.hasWriterId(writeBatch.writerId())) {
                // If idempotence is enabled only retry the request if the current writer id is
                // the same as the writer id of the batch.
                LOG.debug(
                        "Retrying batch to table-bucket {}, Batch sequence : {}",
                        readyWriteBatch.tableBucket(),
                        writeBatch.batchSequence());
                reEnqueueBatch(readyWriteBatch);
            } else {
                Exception exception =
                        Errors.UNKNOWN_WRITER_ID_EXCEPTION.exception(
                                String.format(
                                        "Attempted to retry sending a batch but the writer id has changed from %s "
                                                + "to %s in the mean time. This batch will be dropped.",
                                        writeBatch.writerId(), idempotenceManager.writerId()));
                failBatch(readyWriteBatch, exception, false);
            }

            if (error.exception() instanceof InvalidMetadataException) {
                if (error.exception() instanceof UnknownTableOrBucketException) {
                    LOG.warn(
                            "Received unknown table or bucket error in write request on bucket {}. The table-bucket may not exist.",
                            readyWriteBatch.tableBucket());
                } else {
                    LOG.warn(
                            "Received invalid metadata error in write request on bucket {}. "
                                    + "Going to request metadata update.",
                            readyWriteBatch.tableBucket(),
                            error.exception());
                }
                invalidMetadataTables.add(readyWriteBatch.targetPhysicalTablePath());
            }
        } else {
            LOG.warn(
                    "Get error write response on table bucket {}, fail. Error: {}",
                    readyWriteBatch.tableBucket(),
                    error.formatErrMsg());
            // tell the user the result of their request. We only adjust batch sequence if the
            // batch didn't exhaust its retries -- if it did, we don't know whether the batch
            // sequence was accepted or not, and thus it is not safe to reassign the sequence.
            failBatch(readyWriteBatch, error.exception(), writeBatch.attempts() < this.retries);
        }
        return invalidMetadataTables;
    }

    private void updateWriterMetrics(Map<Integer, List<ReadyWriteBatch>> batches) {
        batches.values()
                .forEach(
                        batchList -> {
                            for (ReadyWriteBatch readyWriteBatch : batchList) {
                                WriteBatch batch = readyWriteBatch.writeBatch();
                                // update table metrics.
                                int recordCount = batch.getRecordCount();
                                writerMetricGroup.recordsSendTotal().inc(recordCount);
                                writerMetricGroup.setBatchQueueTimeMs(batch.getQueueTimeMs());
                                writerMetricGroup
                                        .bytesSendTotal()
                                        .inc(batch.estimatedSizeInBytes());

                                writerMetricGroup.recordPerBatch().update(recordCount);
                                writerMetricGroup
                                        .bytesPerBatch()
                                        .update(batch.estimatedSizeInBytes());
                            }
                        });
    }

    /** Closes the sender without sending out any pending messages. */
    public void forceClose() {
        forceClose = true;
        initiateClose();
    }

    /** Start closing the sender (won't actually complete until all data is sent out). */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        accumulator.close();
        running = false;
    }

    /**
     * Releases all resources held by the accumulator (Arrow writer pool, buffer allocator, etc.).
     *
     * <p>In production, this is called at the end of {@link #run()} after both the main loop and
     * the shutdown drain loop have completed, so no further drain operations will touch the Arrow
     * allocator.
     *
     * <p>This method is idempotent: repeated calls are harmless because the underlying {@link
     * RecordAccumulator#destroyResources()} tolerates multiple invocations.
     */
    void destroyResources() {
        accumulator.destroyResources();
    }
}
