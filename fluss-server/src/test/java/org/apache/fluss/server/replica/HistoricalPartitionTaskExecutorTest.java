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

package org.apache.fluss.server.replica;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HistoricalPartitionTaskExecutor}. */
class HistoricalPartitionTaskExecutorTest {

    @Test
    void testSerialPerBucketAndParallelAcrossBuckets() {
        Queue<Runnable> scheduled = new ArrayDeque<>();
        try (HistoricalPartitionTaskExecutor executor =
                new HistoricalPartitionTaskExecutor(scheduled::add)) {
            TableBucket bucket0 = new TableBucket(1L, 10L, 0);
            TableBucket bucket1 = new TableBucket(1L, 10L, 1);
            List<String> order = new ArrayList<>();

            CompletableFuture<Integer> first =
                    executor.submit(
                            bucket0,
                            () -> {
                                order.add("bucket0-first");
                                return 1;
                            });
            CompletableFuture<Integer> second =
                    executor.submit(
                            bucket0,
                            () -> {
                                order.add("bucket0-second");
                                return 2;
                            });
            CompletableFuture<Integer> other =
                    executor.submit(
                            bucket1,
                            () -> {
                                order.add("bucket1");
                                return 3;
                            });

            assertThat(scheduled).hasSize(2);
            scheduled.poll().run();
            assertThat(first).isCompletedWithValue(1);
            assertThat(second).isNotDone();
            assertThat(scheduled).hasSize(2);

            scheduled.poll().run();
            scheduled.poll().run();
            assertThat(other).isCompletedWithValue(3);
            assertThat(second).isCompletedWithValue(2);
            assertThat(order)
                    .containsExactlyElementsOf(
                            Arrays.asList("bucket0-first", "bucket1", "bucket0-second"));
        }
    }

    @Test
    void testCancelRejectsUntilReset() {
        Queue<Runnable> scheduled = new ArrayDeque<>();
        try (HistoricalPartitionTaskExecutor executor =
                new HistoricalPartitionTaskExecutor(scheduled::add)) {
            TableBucket bucket = new TableBucket(1L, 10L, 0);

            executor.cancel(bucket, new IllegalStateException("cancelled"));
            CompletableFuture<Integer> rejected = executor.submit(bucket, () -> 1);
            assertThatThrownBy(rejected::join)
                    .hasRootCauseInstanceOf(java.util.concurrent.RejectedExecutionException.class);

            executor.reset(bucket);
            CompletableFuture<Integer> accepted = executor.submit(bucket, () -> 2);
            scheduled.poll().run();
            assertThat(accepted).isCompletedWithValue(2);
        }
    }

    @Test
    void testResetWhileCurrentTaskIsFinishing() {
        Queue<Runnable> scheduled = new ArrayDeque<>();
        try (HistoricalPartitionTaskExecutor executor =
                new HistoricalPartitionTaskExecutor(scheduled::add)) {
            TableBucket bucket = new TableBucket(1L, 10L, 0);
            List<CompletableFuture<Integer>> next = new ArrayList<>();

            CompletableFuture<Integer> current =
                    executor.submit(
                            bucket,
                            () -> {
                                executor.cancel(bucket, new IllegalStateException("cancelled"));
                                executor.reset(bucket);
                                next.add(executor.submit(bucket, () -> 2));
                                return 1;
                            });

            scheduled.poll().run();
            scheduled.poll().run();
            assertThat(current).isCompletedWithValue(1);
            assertThat(next)
                    .singleElement()
                    .satisfies(future -> assertThat(future).isCompletedWithValue(2));
        }
    }

    @Test
    void testAwaitTerminationTimesOutUntilScheduledStepStops() throws Exception {
        Queue<Runnable> scheduled = new ArrayDeque<>();
        try (HistoricalPartitionTaskExecutor executor =
                new HistoricalPartitionTaskExecutor(scheduled::add)) {
            TableBucket bucket = new TableBucket(1L, 10L, 0);

            executor.submit(bucket, () -> 1);
            executor.close();

            assertThat(executor.awaitTermination(0, TimeUnit.MILLISECONDS)).isFalse();
            scheduled.poll().run();
            assertThat(executor.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
        }
    }
}
