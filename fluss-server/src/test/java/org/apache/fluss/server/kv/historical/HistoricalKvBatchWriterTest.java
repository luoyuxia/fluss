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

import org.apache.fluss.server.kv.KvBatchWriter;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HistoricalKvBatchWriter}. */
class HistoricalKvBatchWriterTest {

    @Test
    void testDeleteIsWrittenAsTombstone() throws Exception {
        TestingBatchWriter delegate = new TestingBatchWriter();
        HistoricalKvBatchWriter writer = new HistoricalKvBatchWriter(delegate);
        byte[] key = new byte[] {1};
        byte[] value = new byte[] {2};

        writer.put(key, value);
        assertThat(delegate.lastKey).isEqualTo(key);
        assertThat(delegate.lastValue).isEqualTo(value);
        assertThat(delegate.deleteCount).isZero();

        writer.delete(key);
        assertThat(delegate.lastKey).isEqualTo(key);
        assertThat(delegate.lastValue).isEmpty();
        assertThat(delegate.deleteCount).isZero();

        writer.flush();
        writer.close();
        assertThat(delegate.flushCount).isEqualTo(1);
        assertThat(delegate.closeCount).isEqualTo(1);
    }

    @Test
    void testRejectEmptyNormalValue() {
        HistoricalKvBatchWriter writer = new HistoricalKvBatchWriter(new TestingBatchWriter());

        assertThatThrownBy(() -> writer.put(new byte[] {1}, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Historical KV values must not be empty");
    }

    private static final class TestingBatchWriter implements KvBatchWriter {
        private byte[] lastKey;
        private byte[] lastValue;
        private int deleteCount;
        private int flushCount;
        private int closeCount;

        @Override
        public void put(@Nonnull byte[] key, @Nonnull byte[] value) {
            lastKey = key;
            lastValue = value;
        }

        @Override
        public void delete(@Nonnull byte[] key) {
            deleteCount++;
        }

        @Override
        public void flush() throws IOException {
            flushCount++;
        }

        @Override
        public void close() {
            closeCount++;
        }
    }
}
