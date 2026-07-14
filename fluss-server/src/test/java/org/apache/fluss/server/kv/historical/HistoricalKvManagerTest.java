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

package org.apache.fluss.server.kv.historical;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HistoricalKvManager}. */
class HistoricalKvManagerTest {

    private @TempDir File tempDir;

    @Test
    void testHandleReuseIsolationAndBucketInvalidation() throws Exception {
        try (HistoricalKvManager manager = createManager()) {
            TableBucket bucket0 = new TableBucket(1L, 10L, 0);
            TableBucket bucket1 = new TableBucket(1L, 10L, 1);
            File kvDir0 = new File(tempDir, "partition/kv-0");
            File kvDir1 = new File(tempDir, "partition/kv-1");
            HistoricalKvHandle handle0 = manager.getOrCreate(bucket0, kvDir0);
            assertThat(manager.getOrCreate(bucket0, kvDir0)).isSameAs(handle0);
            HistoricalKvHandle handle1 = manager.getOrCreate(bucket1, kvDir1);

            assertThat(handle1).isNotSameAs(handle0);
            assertThat(handle0.getDirectory().getName()).isEqualTo("kv-0");
            assertThat(handle1.getDirectory().getName()).isEqualTo("kv-1");
            assertThat(manager.size()).isEqualTo(2);

            manager.invalidateBucket(bucket0);
            assertThat(manager.getIfPresent(bucket0)).isNotPresent();
            assertThat(manager.getIfPresent(bucket1)).contains(handle1);
            assertThat(handle0.getDirectory()).doesNotExist();
            assertThat(handle1.getDirectory()).exists();
        }
    }

    @Test
    void testInvalidateTable() throws Exception {
        try (HistoricalKvManager manager = createManager()) {
            TableBucket table1Bucket0 = new TableBucket(1L, 10L, 0);
            TableBucket table1Bucket1 = new TableBucket(1L, 10L, 1);
            TableBucket table2Bucket0 = new TableBucket(2L, 20L, 0);
            manager.getOrCreate(table1Bucket0, new File(tempDir, "table1/partition/kv-0"));
            manager.getOrCreate(table1Bucket1, new File(tempDir, "table1/partition/kv-1"));
            HistoricalKvHandle remaining =
                    manager.getOrCreate(table2Bucket0, new File(tempDir, "table2/partition/kv-0"));

            manager.invalidateTable(1L);
            assertThat(manager.getIfPresent(table1Bucket0)).isNotPresent();
            assertThat(manager.getIfPresent(table1Bucket1)).isNotPresent();
            assertThat(manager.getIfPresent(table2Bucket0)).contains(remaining);
            assertThat(manager.size()).isEqualTo(1);
        }
    }

    @Test
    void testCreateAfterClose() throws Exception {
        HistoricalKvManager manager = createManager();
        TableBucket bucket = new TableBucket(1L, 10L, 0);
        manager.getOrCreate(bucket, new File(tempDir, "first/kv-0"));

        manager.close();
        manager.close();
        assertThat(manager.size()).isZero();
        assertThatThrownBy(() -> manager.getOrCreate(bucket, new File(tempDir, "first/kv-0")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testConcurrentGetOrCreateReturnsOneHandle() throws Exception {
        HistoricalKvManager manager = createManager();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        TableBucket bucket = new TableBucket(1L, 10L, 0);
        File kvTabletDir = new File(tempDir, "partition/kv-0");
        try {
            List<Future<HistoricalKvHandle>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(() -> manager.getOrCreate(bucket, kvTabletDir)));
            }

            HistoricalKvHandle expected = futures.get(0).get();
            for (Future<HistoricalKvHandle> future : futures) {
                assertThat(future.get()).isSameAs(expected);
            }
            assertThat(manager.size()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
            manager.close();
        }
    }

    @Test
    void testRejectNonPartitionedBucket() {
        try (HistoricalKvManager manager = createManager()) {
            assertThatThrownBy(
                            () ->
                                    manager.getOrCreate(
                                            new TableBucket(1L, 0),
                                            new File(tempDir, "table/kv-0")))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testRecoveryHandleIsHiddenUntilReady() throws Exception {
        try (HistoricalKvManager manager = createManager()) {
            TableBucket bucket = new TableBucket(1L, 10L, 0);
            HistoricalKvHandle handle =
                    manager.createForRecovery(bucket, new File(tempDir, "partition/kv-0"));

            assertThat(manager.getIfPresent(bucket)).isNotPresent();
            assertThat(manager.readyHandles()).isEmpty();

            manager.markReady(bucket, handle);
            assertThat(manager.getIfPresent(bucket)).contains(handle);
            assertThat(manager.readyHandles()).containsExactly(handle);
        }
    }

    @Test
    void testGetOrCreateDoesNotPublishRecoveringHandle() throws Exception {
        try (HistoricalKvManager manager = createManager()) {
            TableBucket bucket = new TableBucket(1L, 10L, 0);
            File directory = new File(tempDir, "partition/kv-0");
            manager.createForRecovery(bucket, directory);

            assertThatThrownBy(() -> manager.getOrCreate(bucket, directory))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("being recovered");
            assertThat(manager.getIfPresent(bucket)).isNotPresent();
        }
    }

    private HistoricalKvManager createManager() {
        return new HistoricalKvManager(
                new Configuration(),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                KvManager.getDefaultRateLimiter(),
                new ManualClock());
    }
}
