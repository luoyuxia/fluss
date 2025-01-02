/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.client.metadata.TestingMetadataUpdater;
import com.alibaba.fluss.client.metrics.TestingWriterMetricGroup;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.exception.TimeoutException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.entity.ProduceLogResultForBucket;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.tablet.TestTabletServerGateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.record.TestData.DATA1_PHYSICAL_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.utils.RpcMessageUtils.getProduceLogData;
import static com.alibaba.fluss.server.utils.RpcMessageUtils.makeProduceLogResponse;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for {@link Sender}. */
final class SenderTest {
    private static final int TOTAL_MEMORY_SIZE = 1024 * 1024;
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final int BATCH_SIZE = 16 * 1024;
    private static final int PAGE_SIZE = 256;
    private static final int REQUEST_TIMEOUT = 5000;
    private static final short ACKS_ALL = -1;
    private static final int MAX_INFLIGHT_REQUEST_PER_BUCKET = 5;

    private final TableBucket tb1 = new TableBucket(DATA1_TABLE_ID, 0);
    private TestingMetadataUpdater metadataUpdater;
    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private TestingWriterMetricGroup writerMetricGroup;

    // TODO add more tests as kafka SenderTest.

    @BeforeEach
    public void setup() {
        metadataUpdater = initializeMetadataUpdater();
        writerMetricGroup = TestingWriterMetricGroup.newInstance();
        sender = setupWithIdempotenceState();
    }

    @Test
    void testSimple() throws Exception {
        long offset = 0;
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future::complete);
        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(1);
        finishProduceLogRequest(tb1, 0, createProduceLogResponse(tb1, offset, 1));

        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(future.get()).isNull();
    }

    @Test
    void testRetries() throws Exception {
        // create a sender with retries = 1.
        int maxRetries = 1;
        Sender sender1 =
                setupWithIdempotenceState(
                        new IdempotenceManager(
                                false,
                                MAX_INFLIGHT_REQUEST_PER_BUCKET,
                                metadataUpdater.newRandomTabletServerClient()),
                        maxRetries,
                        0);
        // do a successful retry.
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future::complete);
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);
        long offset = 0;
        finishProduceLogRequest(tb1, 0, createProduceLogResponse(tb1, offset, 1));

        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(future.get()).isNull();

        // do an unsuccessful retry.
        future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future::complete);
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);

        // timeout error can retry send.
        finishProduceLogRequest(tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);

        // Even if timeout error can retry send, but the retry number > maxRetries, which will
        // return error.
        finishProduceLogRequest(tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        sender1.runOnce();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(future.get())
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining(Errors.REQUEST_TIME_OUT.message());
    }

    @Test
    void testInitWriterIdRequest() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.hasWriterId(0L)).isTrue();
        assertThat(idempotenceManager.writerId()).isEqualTo(0L);
    }

    @Test
    void testCanRetryWithoutIdempotence() throws Exception {
        // do a successful retry.
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future::complete);
        sender.runOnce();
        assertThat(sender.numOfInFlightBatches(tb1)).isEqualTo(1);
        assertThat(future.isDone()).isFalse();

        ApiMessage firstRequest = getRequest(tb1, 0);
        assertThat(firstRequest).isInstanceOf(ProduceLogRequest.class);
        assertThat(hasIdempotentRecords(tb1, (ProduceLogRequest) firstRequest)).isFalse();
        // first complete with retriable error.
        finishProduceLogRequest(tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        sender.runOnce();
        assertThat(future.isDone()).isFalse();

        // second retry complete.
        finishProduceLogRequest(tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender.runOnce();
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isNull();
    }

    @Test
    void testIdempotenceWithMultipleInflightBatch() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future1::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future2::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(2);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();

        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce(); // receive response 0.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();
        assertThat(future2.isDone()).isFalse();

        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce(); // receive response 1.
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get()).isNull();
    }

    @Test
    void testIdempotenceWithMaxInflightBatch() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        for (int i = 0; i < MAX_INFLIGHT_REQUEST_PER_BUCKET - 1; i++) {
            CompletableFuture<Exception> future = new CompletableFuture<>();
            appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future::complete);
            sender1.runOnce();
            assertThat(idempotenceManager.inflightBatchSize(tb1)).isEqualTo(i + 1);
            assertThat(idempotenceManager.canSendMortRequests(tb1)).isTrue();
        }

        // add one batch to make the inflight request size equal to max.
        CompletableFuture<Exception> future = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.inflightBatchSize(tb1))
                .isEqualTo(MAX_INFLIGHT_REQUEST_PER_BUCKET);
        assertThat(idempotenceManager.canSendMortRequests(tb1)).isFalse();

        // add one more batch, it will not be drained from accumulator.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future1::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.inflightBatchSize(tb1))
                .isEqualTo(MAX_INFLIGHT_REQUEST_PER_BUCKET);
        assertThat(accumulator.ready(metadataUpdater.getCluster()).readyNodes.size()).isEqualTo(1);

        // finish the first batch, the latest batch will be drained from the accumulator.
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce(); // receive response 0.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(idempotenceManager.inflightBatchSize(tb1))
                .isEqualTo(MAX_INFLIGHT_REQUEST_PER_BUCKET);
        assertThat(accumulator.ready(metadataUpdater.getCluster()).readyNodes.size()).isEqualTo(0);
    }

    @Test
    void testIdempotenceWithMultipleInflightBatchesRetriedInOrder() throws Exception {
        // Send multiple in flight requests, retry them all one at a time, in the correct order.
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future1::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future2::complete);
        sender1.runOnce();

        // Send third ProduceLogRequest.
        CompletableFuture<Exception> future3 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future3::complete);
        sender1.runOnce();

        // finish batch one with retriable error.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        sender1.runOnce(); // receive response 0

        // Queue the forth request, it shouldn't sent until the first 3 complete.
        CompletableFuture<Exception> future4 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future4::complete);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        finishIdempotentProduceLogRequest(
                1, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce(); // re send request 1, receive response 2

        finishIdempotentProduceLogRequest(
                2, tb1, 0, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));
        sender1.runOnce(); // receive response 3

        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        sender1.runOnce(); // Do nothing, we are reduced to one in flight request during retries.

        // the batch for request 4 shouldn't have been drained, and hence the batch sequence should
        // not have been incremented.
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(3);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        sender1.runOnce(); // receive response 1
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();

        sender1.runOnce(); // send request 2
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);

        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
        sender1.runOnce(); // receive response 2
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get()).isNull();

        sender1.runOnce(); // send request 3
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(2);

        finishIdempotentProduceLogRequest(2, tb1, 0, createProduceLogResponse(tb1, 2L, 3L));
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(1);
        sender1.runOnce(); // receive response 3
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(2));
        assertThat(future3.isDone()).isTrue();
        assertThat(future3.get()).isNull();

        finishIdempotentProduceLogRequest(3, tb1, 0, createProduceLogResponse(tb1, 3L, 4L));
        sender1.runOnce(); // receive response 4
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(3));
        assertThat(future4.isDone()).isTrue();
        assertThat(future4.get()).isNull();
        assertThat(sender1.numOfInFlightBatches(tb1)).isEqualTo(0);
    }

    @Test
    void testCorrectHandlingOfOutOfOrderResponses() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future1::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future2::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(2);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();

        // first finish second ProduceLogRequest with the out-of-order exception.
        finishIdempotentProduceLogRequest(
                1, tb1, 1, createProduceLogResponse(tb1, Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION));

        sender1.runOnce(); // receive response 1.
        Deque<WriteBatch> queuedBatches = accumulator.getDeque(DATA1_PHYSICAL_TABLE_PATH, tb1);

        // Make sure that we are queueing the second batch first.
        assertThat(queuedBatches.size()).isEqualTo(1);
        assertThat(queuedBatches.peek().batchSequence()).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // receive response 0
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        // Make sure we requeued both batches in the correct order.
        assertThat(queuedBatches.size()).isEqualTo(2);
        assertThat(queuedBatches.peek().batchSequence()).isEqualTo(0);
        assertThat(queuedBatches.peekLast().batchSequence()).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();
        sender1.runOnce(); // send request 0
        sender1.runOnce(); // don't do anything, only one inflight allowed once we are retrying.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Make sure that the requests are sent in order, even though the previous responses were
        // not in order.
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce(); // receive response 0.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();

        // send request 1.
        finishIdempotentProduceLogRequest(1, tb1, 0, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce(); // receive response 1.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get()).isNull();
    }

    @Test
    void testCorrectHandlingOfOutOfOrderResponsesWhenSecondSucceeds() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // Send first ProduceLogRequest.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future1::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future2::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(2);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();

        // first finish second ProduceLogRequest with success.
        finishIdempotentProduceLogRequest(1, tb1, 1, createProduceLogResponse(tb1, 1L, 2L));
        sender1.runOnce(); // receive response 1
        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get()).isNull();
        assertThat(future1.isDone()).isFalse();
        Deque<WriteBatch> queuedBatches = accumulator.getDeque(DATA1_PHYSICAL_TABLE_PATH, tb1);

        assertThat(queuedBatches.size()).isEqualTo(0);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));

        // then finish second ProduceLogRequest with error.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.REQUEST_TIME_OUT));
        // Make sure we requeued both batches in the correct order.
        assertThat(queuedBatches.size()).isEqualTo(1);
        assertThat(queuedBatches.peek().batchSequence()).isEqualTo(0);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        sender1.runOnce(); // resend request 1.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));

        // Make sure we handle the out of order successful responses correctly.
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce(); // receive response 0.
        assertThat(queuedBatches.size()).isEqualTo(0);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();
    }

    @Test
    void testCorrectHandlingOfDuplicateSequenceError() throws Exception {
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        // first finish second ProduceLogRequest with success.
        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future1::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();

        // Send second ProduceLogRequest.
        CompletableFuture<Exception> future2 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future2::complete);
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(2);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isNotPresent();
        assertThat(future1.isDone()).isFalse();
        assertThat(future2.isDone()).isFalse();

        // first finish second ProduceLogRequest with success.
        finishIdempotentProduceLogRequest(1, tb1, 1, createProduceLogResponse(tb1, 1000L, 1001L));
        sender.runOnce(); // receive response 1
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));

        // then finish second ProduceLogRequest with  duplicate batch sequence error.
        finishIdempotentProduceLogRequest(
                0, tb1, 0, createProduceLogResponse(tb1, Errors.DUPLICATE_SEQUENCE_EXCEPTION));
        sender.runOnce(); // receive response 0.

        // Make sure that the last ack'd sequence doesn't change.
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(1));
    }

    @Test
    void testSequenceNumberIncrement() throws Exception {
        int maxRetries = 10;
        IdempotenceManager idempotenceManager = createIdempotenceManager(true);
        Sender sender1 = setupWithIdempotenceState(idempotenceManager, maxRetries, 0);
        sender1.runOnce();
        assertThat(idempotenceManager.isWriterIdValid()).isTrue();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(0);

        CompletableFuture<Exception> future1 = new CompletableFuture<>();
        appendToAccumulator(tb1, row(DATA1_ROW_TYPE, new Object[] {1, "a"}), future1::complete);
        sender1.runOnce();
        finishIdempotentProduceLogRequest(0, tb1, 0, createProduceLogResponse(tb1, 0L, 1L));
        sender1.runOnce();
        assertThat(idempotenceManager.nextSequence(tb1)).isEqualTo(1);
        assertThat(idempotenceManager.lastAckedBatchSequence(tb1)).isEqualTo(Optional.of(0));
        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get()).isNull();
    }

    private TestingMetadataUpdater initializeMetadataUpdater() {
        return new TestingMetadataUpdater(
                Collections.singletonMap(DATA1_TABLE_PATH, DATA1_TABLE_INFO));
    }

    private void appendToAccumulator(TableBucket tb, InternalRow row, WriteCallback writeCallback)
            throws Exception {
        accumulator.append(
                new WriteRecord(DATA1_PHYSICAL_TABLE_PATH, WriteKind.APPEND, row, null),
                writeCallback,
                metadataUpdater.getCluster(),
                tb.getBucket(),
                false);
    }

    private ApiMessage getRequest(TableBucket tb, int index) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(metadataUpdater.leaderFor(tb));
        return gateway.getRequest(index);
    }

    private void finishProduceLogRequest(TableBucket tb, int index, ProduceLogResponse response) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(metadataUpdater.leaderFor(tb));
        gateway.response(index, response);
    }

    private void finishIdempotentProduceLogRequest(
            int batchSequence, TableBucket tb, int index, ProduceLogResponse response) {
        TestTabletServerGateway gateway =
                (TestTabletServerGateway)
                        metadataUpdater.newTabletServerClientForNode(metadataUpdater.leaderFor(tb));
        ApiMessage request = getRequest(tb1, index);
        assertThat(request).isInstanceOf(ProduceLogRequest.class);
        assertThat(hasIdempotentRecords(tb1, (ProduceLogRequest) request)).isTrue();
        assertBatchSequenceEquals(tb1, (ProduceLogRequest) request, batchSequence);
        gateway.response(index, response);
    }

    private ProduceLogResponse createProduceLogResponse(
            TableBucket tb, long baseOffset, long endOffset) {
        return makeProduceLogResponse(
                Collections.singletonList(
                        new ProduceLogResultForBucket(tb, baseOffset, endOffset)));
    }

    private ProduceLogResponse createProduceLogResponse(TableBucket tb, Errors error) {
        return makeProduceLogResponse(
                Collections.singletonList(new ProduceLogResultForBucket(tb, error.toApiError())));
    }

    private Sender setupWithIdempotenceState() {
        return setupWithIdempotenceState(createIdempotenceManager(false));
    }

    private Sender setupWithIdempotenceState(IdempotenceManager idempotenceManager) {
        return setupWithIdempotenceState(idempotenceManager, Integer.MAX_VALUE, 0);
    }

    private Sender setupWithIdempotenceState(
            IdempotenceManager idempotenceManager, int reties, int batchTimeoutMs) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, new MemorySize(TOTAL_MEMORY_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(BATCH_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(PAGE_SIZE));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT, Duration.ofMillis(batchTimeoutMs));
        accumulator = new RecordAccumulator(conf, idempotenceManager, writerMetricGroup);
        return new Sender(
                accumulator,
                REQUEST_TIMEOUT,
                MAX_REQUEST_SIZE,
                ACKS_ALL,
                reties,
                metadataUpdater,
                idempotenceManager,
                writerMetricGroup);
    }

    private IdempotenceManager createIdempotenceManager(boolean idempotenceEnabled) {
        return new IdempotenceManager(
                idempotenceEnabled,
                MAX_INFLIGHT_REQUEST_PER_BUCKET,
                metadataUpdater.newRandomTabletServerClient());
    }

    private static boolean hasIdempotentRecords(TableBucket tb, ProduceLogRequest request) {
        MemoryLogRecords memoryLogRecords = getProduceLogData(request).get(tb);
        return memoryLogRecords.batchIterator().next().writerId() != LogRecordBatch.NO_WRITER_ID;
    }

    private static void assertBatchSequenceEquals(
            TableBucket tb, ProduceLogRequest request, int expectedBatchSequence) {
        MemoryLogRecords memoryLogRecords = getProduceLogData(request).get(tb);
        assertThat(memoryLogRecords.batchIterator().next().batchSequence())
                .isEqualTo(expectedBatchSequence);
    }
}
