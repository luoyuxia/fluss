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
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.KvStateLookupResult.Status;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HistoricalKvHandle}. */
class HistoricalKvHandleTest {

    private @TempDir File tempDir;

    @Test
    void testPutUpdateDeleteAndTombstoneLookup() throws Exception {
        HistoricalKvHandle handle = createHandle(new ManualClock());
        HistoricalKvStateAccessor accessor = new HistoricalKvStateAccessor(handle, "dt=2025-01-01");
        byte[] primaryKey = new byte[] {1};
        byte[] value1 = new byte[] {1, 1};
        byte[] value2 = new byte[] {2, 2};

        try {
            handle.withWriteLock(() -> accessor.insert(accessor.encodeKey(primaryKey), value1, 0));
            assertLookup(handle, accessor, primaryKey, Status.PRESENT, value1);

            handle.withWriteLock(() -> accessor.update(accessor.encodeKey(primaryKey), value2, 1));
            assertLookup(handle, accessor, primaryKey, Status.PRESENT, value2);

            handle.withWriteLock(() -> accessor.delete(accessor.encodeKey(primaryKey), 2));
            assertLookup(handle, accessor, primaryKey, Status.DELETED, null);

            handle.withWriteLock(() -> accessor.flush(3));
            assertLookup(handle, accessor, primaryKey, Status.DELETED, null);
            assertThat(handle.getRocksDBKv().get(accessor.encodeKey(primaryKey).get())).isEmpty();
            assertLookup(handle, accessor, new byte[] {9}, Status.NOT_FOUND, null);
        } finally {
            handle.drop();
        }
    }

    @Test
    void testOriginalPartitionsAreIsolated() throws Exception {
        HistoricalKvHandle handle = createHandle(new ManualClock());
        HistoricalKvStateAccessor first = new HistoricalKvStateAccessor(handle, "dt=2025-01-01");
        HistoricalKvStateAccessor second = new HistoricalKvStateAccessor(handle, "dt=2025-01-02");
        byte[] primaryKey = new byte[] {1};

        try {
            handle.withWriteLock(
                    () -> {
                        first.insert(first.encodeKey(primaryKey), new byte[] {1}, 0);
                        second.insert(second.encodeKey(primaryKey), new byte[] {2}, 1);
                    });

            assertLookup(handle, first, primaryKey, Status.PRESENT, new byte[] {1});
            assertLookup(handle, second, primaryKey, Status.PRESENT, new byte[] {2});
        } finally {
            handle.drop();
        }
    }

    @Test
    void testTruncateRestoresPreviousState() throws Exception {
        HistoricalKvHandle handle = createHandle(new ManualClock());
        HistoricalKvStateAccessor accessor = new HistoricalKvStateAccessor(handle, "dt=2025-01-01");
        byte[] primaryKey = new byte[] {1};
        byte[] baseValue = new byte[] {1};

        try {
            handle.withWriteLock(
                    () -> {
                        accessor.insert(accessor.encodeKey(primaryKey), baseValue, 0);
                        accessor.update(accessor.encodeKey(primaryKey), new byte[] {2}, 1);
                        accessor.delete(accessor.encodeKey(primaryKey), 2);
                        accessor.truncateTo(1, TruncateReason.ERROR);
                    });
            assertLookup(handle, accessor, primaryKey, Status.PRESENT, baseValue);

            handle.withWriteLock(
                    () -> {
                        accessor.update(accessor.encodeKey(primaryKey), new byte[] {3}, 1);
                        accessor.truncateTo(1, TruncateReason.DUPLICATED);
                    });
            assertLookup(handle, accessor, primaryKey, Status.PRESENT, baseValue);

            byte[] newKey = new byte[] {2};
            handle.withWriteLock(
                    () -> {
                        accessor.insert(accessor.encodeKey(newKey), new byte[] {4}, 1);
                        accessor.truncateTo(1, TruncateReason.ERROR);
                    });
            assertLookup(handle, accessor, newKey, Status.NOT_FOUND, null);
        } finally {
            handle.drop();
        }
    }

    @Test
    void testRejectEmptyValue() throws Exception {
        HistoricalKvHandle handle = createHandle(new ManualClock());
        HistoricalKvStateAccessor accessor = new HistoricalKvStateAccessor(handle, "dt=2025-01-01");
        try {
            assertThatThrownBy(
                            () ->
                                    accessor.insert(
                                            accessor.encodeKey(new byte[] {1}), new byte[0], 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(
                            () ->
                                    accessor.update(
                                            accessor.encodeKey(new byte[] {1}), new byte[0], 0))
                    .isInstanceOf(IllegalArgumentException.class);
        } finally {
            handle.drop();
        }
    }

    @Test
    void testAccessTimeAndLifecycle() throws Exception {
        ManualClock clock = new ManualClock(10);
        HistoricalKvHandle handle = createHandle(clock);
        HistoricalKvStateAccessor accessor = new HistoricalKvStateAccessor(handle, "dt=2025-01-01");
        assertThat(handle.getLastAccessTime()).isEqualTo(10);

        clock.advanceTime(5, TimeUnit.MILLISECONDS);
        handle.withReadLock(() -> accessor.lookup(accessor.encodeKey(new byte[] {1})));
        assertThat(handle.getLastAccessTime()).isEqualTo(15);

        handle.close();
        assertThatThrownBy(
                        () ->
                                handle.withReadLock(
                                        () -> accessor.lookup(accessor.encodeKey(new byte[] {1}))))
                .isInstanceOf(IllegalStateException.class);

        handle.drop();
        assertThat(handle.getDirectory()).doesNotExist();
    }

    private HistoricalKvHandle createHandle(ManualClock clock) throws Exception {
        return HistoricalKvHandle.create(
                new TableBucket(1L, 10L, 0),
                new File(tempDir, "historical-kv-0"),
                new Configuration(),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                KvManager.getDefaultRateLimiter(),
                clock);
    }

    private static void assertLookup(
            HistoricalKvHandle handle,
            HistoricalKvStateAccessor accessor,
            byte[] primaryKey,
            Status expectedStatus,
            byte[] expectedValue)
            throws Exception {
        KvStateLookupResult result =
                handle.withReadLock(() -> accessor.lookup(accessor.encodeKey(primaryKey)));
        assertThat(result.status()).isEqualTo(expectedStatus);
        assertThat(result.value()).isEqualTo(expectedValue);
    }
}
