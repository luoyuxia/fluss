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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A KV batch writer that persists deletes as historical tombstones. */
final class HistoricalKvBatchWriter implements KvBatchWriter {

    private static final byte[] TOMBSTONE_VALUE = new byte[0];

    private final KvBatchWriter delegate;

    HistoricalKvBatchWriter(KvBatchWriter delegate) {
        this.delegate = checkNotNull(delegate, "delegate must not be null");
    }

    @Override
    public void put(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException {
        checkArgument(value.length > 0, "Historical KV values must not be empty");
        delegate.put(key, value);
    }

    @Override
    public void delete(@Nonnull byte[] key) throws IOException {
        delegate.put(key, TOMBSTONE_VALUE);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    static boolean isTombstone(@Nullable byte[] value) {
        return value != null && value.length == 0;
    }
}
