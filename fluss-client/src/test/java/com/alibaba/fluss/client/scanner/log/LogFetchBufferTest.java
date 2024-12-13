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

package com.alibaba.fluss.client.scanner.log;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogFetchBuffer}. */
public class LogFetchBufferTest {

    private final TableBucket tableBucket1 = new TableBucket(1, 0);
    private final TableBucket tableBucket2 = new TableBucket(1, 1);
    private final TableBucket tableBucket3 = new TableBucket(1, 2);
    private LogScannerStatus logScannerStatus;
    private LogRecordReadContext readContext;

    @BeforeEach
    void setup() {
        Map<TableBucket, Long> scanBuckets = new HashMap<>();
        scanBuckets.put(tableBucket1, 0L);
        scanBuckets.put(tableBucket2, 0L);
        scanBuckets.put(tableBucket3, 0L);
        logScannerStatus = new LogScannerStatus();
        logScannerStatus.assignScanBuckets(scanBuckets);
        readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
    }

    @AfterEach
    void afterEach() {
        if (readContext != null) {
            readContext.close();
            readContext = null;
        }
    }

    @Test
    void testBasicPeekAndPoll() throws Exception {
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            CompletedFetch completedFetch = makeCompletedFetch(tableBucket1);
            assertThat(logFetchBuffer.isEmpty()).isTrue();
            logFetchBuffer.add(completedFetch);
            assertThat(logFetchBuffer.isEmpty()).isFalse();
            assertThat(logFetchBuffer.peek()).isNotNull();
            assertThat(logFetchBuffer.peek()).isEqualTo(completedFetch);
            assertThat(logFetchBuffer.poll()).isEqualTo(completedFetch);
            assertThat(logFetchBuffer.peek()).isNull();
        }
    }

    @Test
    void testCloseClearsData() throws Exception {
        LogFetchBuffer logFetchBuffer = null;
        try {
            logFetchBuffer = new LogFetchBuffer();
            assertThat(logFetchBuffer.nextInLineFetch()).isNull();
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            logFetchBuffer.add(makeCompletedFetch(tableBucket1));
            assertThat(logFetchBuffer.isEmpty()).isFalse();

            logFetchBuffer.setNextInLineFetch(makeCompletedFetch(tableBucket1));
            assertThat(logFetchBuffer.nextInLineFetch()).isNotNull();
        } finally {
            if (logFetchBuffer != null) {
                logFetchBuffer.close();
            }
        }

        assertThat(logFetchBuffer.nextInLineFetch()).isNull();
        assertThat(logFetchBuffer.isEmpty()).isTrue();
    }

    @Test
    void testBufferedBuckets() throws Exception {
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            logFetchBuffer.setNextInLineFetch(makeCompletedFetch(tableBucket1));
            logFetchBuffer.add(makeCompletedFetch(tableBucket2));
            logFetchBuffer.add(makeCompletedFetch(tableBucket3));
            assertThat(logFetchBuffer.bufferedBuckets())
                    .containsExactlyInAnyOrder(tableBucket1, tableBucket2, tableBucket3);

            logFetchBuffer.setNextInLineFetch(null);
            assertThat(logFetchBuffer.bufferedBuckets())
                    .containsExactlyInAnyOrder(tableBucket2, tableBucket3);

            logFetchBuffer.poll();
            assertThat(logFetchBuffer.bufferedBuckets()).containsExactlyInAnyOrder(tableBucket3);

            logFetchBuffer.poll();
            assertThat(logFetchBuffer.bufferedBuckets()).isEmpty();
        }
    }

    @Test
    void testAddAllAndRetainAll() throws Exception {
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            logFetchBuffer.setNextInLineFetch(makeCompletedFetch(tableBucket1));
            logFetchBuffer.addAll(
                    Arrays.asList(
                            makeCompletedFetch(tableBucket2), makeCompletedFetch(tableBucket3)));
            logFetchBuffer.pend(makePendingFetch(tableBucket1));
            logFetchBuffer.pend(makePendingFetch(tableBucket2));
            logFetchBuffer.pend(makePendingFetch(tableBucket3));
            // TODO these tests need to add back after remove the hack logic in
            // LogFetchBuffer#bufferedBuckets()

            //            assertThat(logFetchBuffer.bufferedBuckets())
            //                    .containsExactlyInAnyOrder(tableBucket1, tableBucket2,
            // tableBucket3);
            assertThat(logFetchBuffer.pendedBuckets())
                    .containsExactlyInAnyOrder(tableBucket1, tableBucket2, tableBucket3);

            logFetchBuffer.retainAll(new HashSet<>(Arrays.asList(tableBucket2, tableBucket3)));
            //            assertThat(logFetchBuffer.bufferedBuckets())
            //                    .containsExactlyInAnyOrder(tableBucket2, tableBucket3);
            assertThat(logFetchBuffer.pendedBuckets())
                    .containsExactlyInAnyOrder(tableBucket2, tableBucket3);

            logFetchBuffer.retainAll(Collections.singleton(tableBucket3));
            //
            // assertThat(logFetchBuffer.bufferedBuckets()).containsExactlyInAnyOrder(tableBucket3);
            assertThat(logFetchBuffer.pendedBuckets()).containsExactlyInAnyOrder(tableBucket3);

            logFetchBuffer.retainAll(Collections.emptySet());
            assertThat(logFetchBuffer.bufferedBuckets()).isEmpty();
            assertThat(logFetchBuffer.pendedBuckets()).isEmpty();
        }
    }

    @Test
    void testWakeup() throws Exception {
        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            final Thread waitingThread =
                    new Thread(
                            () -> {
                                try {
                                    logFetchBuffer.awaitNotEmpty(
                                            System.nanoTime() + Duration.ofMinutes(1).toNanos());
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            waitingThread.start();
            logFetchBuffer.wakeup();
            waitingThread.join(Duration.ofSeconds(30).toMillis());
            assertThat(waitingThread.isAlive()).isFalse();
        }
    }

    @Test
    void testPendFetches() throws Exception {
        ExecutorService service = Executors.newSingleThreadExecutor();

        try (LogFetchBuffer logFetchBuffer = new LogFetchBuffer()) {
            Callable<Boolean> await =
                    () ->
                            logFetchBuffer.awaitNotEmpty(
                                    System.nanoTime() + Duration.ofMinutes(1).toNanos());

            assertThat(logFetchBuffer.isEmpty()).isTrue();
            AtomicBoolean completed1 = new AtomicBoolean(false);
            logFetchBuffer.pend(makePendingFetch(tableBucket1, completed1));
            // pending fetches are not counted as completed fetches.
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            logFetchBuffer.add(makeCompletedFetch(tableBucket3));
            // competed fetches will be pended if there is any pending fetches
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            AtomicBoolean completed2 = new AtomicBoolean(false);
            logFetchBuffer.pend(makePendingFetch(tableBucket2, completed2));
            logFetchBuffer.pend(makePendingFetch(tableBucket3));
            logFetchBuffer.pend(makePendingFetch(tableBucket3));

            Future<Boolean> signal =
                    service.submit(() -> await(logFetchBuffer, Duration.ofSeconds(1)));
            logFetchBuffer.tryComplete();
            // nothing happen
            assertThat(logFetchBuffer.isEmpty()).isTrue();
            // no condition signal
            assertThat(signal.get()).isFalse();

            signal = service.submit(() -> await(logFetchBuffer, Duration.ofMinutes(1)));
            completed1.set(true);
            logFetchBuffer.tryComplete();
            assertThat(signal.get()).isTrue();
            assertThat(logFetchBuffer.isEmpty()).isFalse();
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket1);
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket3);
            assertThat(logFetchBuffer.isEmpty()).isTrue();

            signal = service.submit(() -> await(logFetchBuffer, Duration.ofMinutes(1)));
            completed2.set(true);
            logFetchBuffer.tryComplete();
            assertThat(signal.get()).isTrue();
            assertThat(logFetchBuffer.isEmpty()).isFalse();
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket2);
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket3);
            assertThat(logFetchBuffer.poll().tableBucket).isEqualTo(tableBucket3);
            assertThat(logFetchBuffer.isEmpty()).isTrue();
        }
    }

    private boolean await(LogFetchBuffer buffer, Duration waitTime) throws InterruptedException {
        return buffer.awaitNotEmpty(System.nanoTime() + waitTime.toNanos());
    }

    private DefaultCompletedFetch makeCompletedFetch(TableBucket tableBucket) throws Exception {
        return new DefaultCompletedFetch(
                tableBucket,
                new FetchLogResultForBucket(tableBucket, genMemoryLogRecordsByObject(DATA1), 10L),
                readContext,
                logScannerStatus,
                true,
                0L,
                null,
                null);
    }

    private PendingFetch makePendingFetch(TableBucket tableBucket) throws Exception {
        return new CompletedPendingFetch(makeCompletedFetch(tableBucket));
    }

    private PendingFetch makePendingFetch(TableBucket tableBucket, AtomicBoolean completed)
            throws Exception {
        DefaultCompletedFetch completedFetch = makeCompletedFetch(tableBucket);
        return new PendingFetch() {
            @Override
            public TableBucket tableBucket() {
                return tableBucket;
            }

            @Override
            public boolean isCompleted() {
                return completed.get();
            }

            @Override
            public CompletedFetch toCompletedFetch() {
                return completedFetch;
            }
        };
    }
}
