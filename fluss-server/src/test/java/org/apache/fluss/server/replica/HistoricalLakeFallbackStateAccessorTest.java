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

import org.apache.fluss.server.kv.KvStateAccessor;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HistoricalLakeFallbackStateAccessor}. */
class HistoricalLakeFallbackStateAccessorTest {

    private static final byte[] PRIMARY_KEY = new byte[] {1, 2};
    private static final Key ENCODED_KEY = Key.of(new byte[] {3, 4});

    @Test
    void testLocalPresentDoesNotFallBackToLake() throws Exception {
        byte[] localValue = new byte[] {5};
        TestingStateAccessor localAccessor =
                new TestingStateAccessor(KvStateLookupResult.present(localValue));
        AtomicInteger lakeLookups = new AtomicInteger();
        HistoricalLakeFallbackStateAccessor accessor =
                new HistoricalLakeFallbackStateAccessor(
                        localAccessor,
                        ignored -> {
                            lakeLookups.incrementAndGet();
                            return new byte[] {6};
                        });

        assertThat(accessor.lookup(PRIMARY_KEY, ENCODED_KEY))
                .isEqualTo(KvStateLookupResult.present(localValue));
        assertThat(lakeLookups).hasValue(0);
        assertThat(localAccessor.primaryKey).hasValue(PRIMARY_KEY);
        assertThat(localAccessor.encodedKey).hasValue(ENCODED_KEY);
    }

    @Test
    void testLocalDeleteDoesNotFallBackToLake() throws Exception {
        AtomicInteger lakeLookups = new AtomicInteger();
        HistoricalLakeFallbackStateAccessor accessor =
                new HistoricalLakeFallbackStateAccessor(
                        new TestingStateAccessor(KvStateLookupResult.deleted()),
                        ignored -> {
                            lakeLookups.incrementAndGet();
                            return new byte[] {6};
                        });

        assertThat(accessor.lookup(PRIMARY_KEY, ENCODED_KEY))
                .isEqualTo(KvStateLookupResult.deleted());
        assertThat(lakeLookups).hasValue(0);
    }

    @Test
    void testLocalMissUsesOriginalKeyForLakeLookup() throws Exception {
        AtomicReference<byte[]> lakeKey = new AtomicReference<>();
        byte[] lakeValue = new byte[] {7};
        HistoricalLakeFallbackStateAccessor accessor =
                new HistoricalLakeFallbackStateAccessor(
                        new TestingStateAccessor(KvStateLookupResult.notFound()),
                        key -> {
                            lakeKey.set(key);
                            return lakeValue;
                        });

        assertThat(accessor.lookup(PRIMARY_KEY, ENCODED_KEY))
                .isEqualTo(KvStateLookupResult.present(lakeValue));
        assertThat(lakeKey).hasValue(PRIMARY_KEY);
    }

    @Test
    void testLakeMissRemainsNotFound() throws Exception {
        HistoricalLakeFallbackStateAccessor accessor =
                new HistoricalLakeFallbackStateAccessor(
                        new TestingStateAccessor(KvStateLookupResult.notFound()), ignored -> null);

        assertThat(accessor.lookup(PRIMARY_KEY, ENCODED_KEY))
                .isEqualTo(KvStateLookupResult.notFound());
    }

    private static final class TestingStateAccessor implements KvStateAccessor {
        private final KvStateLookupResult lookupResult;
        private final AtomicReference<byte[]> primaryKey = new AtomicReference<>();
        private final AtomicReference<Key> encodedKey = new AtomicReference<>();

        private TestingStateAccessor(KvStateLookupResult lookupResult) {
            this.lookupResult = lookupResult;
        }

        @Override
        public Key encodeKey(byte[] primaryKey) {
            return ENCODED_KEY;
        }

        @Override
        public KvStateLookupResult lookup(Key encodedKey) {
            this.encodedKey.set(encodedKey);
            return lookupResult;
        }

        @Override
        public KvStateLookupResult lookup(byte[] primaryKey, Key encodedKey) {
            this.primaryKey.set(primaryKey);
            this.encodedKey.set(encodedKey);
            return lookupResult;
        }

        @Override
        public void insert(Key key, byte[] value, long logOffset) {}

        @Override
        public void update(Key key, byte[] value, long logOffset) {}

        @Override
        public void delete(Key key, long logOffset) {}

        @Override
        public void truncateTo(long logOffset, TruncateReason reason) {}

        @Override
        public int flush(long exclusiveLogOffset) {
            return 0;
        }
    }
}
