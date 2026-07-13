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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.server.kv.KvStateAccessor;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * State accessor for one original partition stored in a historical KV handle.
 *
 * <p>Each operation is individually protected by the handle lock. A caller that processes a whole
 * write batch must additionally wrap all state access and rollback in one {@link
 * HistoricalKvHandle#withWriteLock(org.apache.fluss.utils.function.ThrowingRunnable)} scope.
 */
@Internal
public final class HistoricalKvStateAccessor implements KvStateAccessor {

    private final HistoricalKvHandle handle;
    private final String originalPartitionName;

    /** Creates an accessor for one original partition in the given historical state handle. */
    public HistoricalKvStateAccessor(HistoricalKvHandle handle, String originalPartitionName) {
        this.handle = checkNotNull(handle, "handle must not be null");
        this.originalPartitionName =
                checkNotNull(originalPartitionName, "originalPartitionName must not be null");
        checkArgument(!originalPartitionName.isEmpty(), "originalPartitionName must not be empty");
    }

    @Override
    public Key encodeKey(byte[] primaryKey) {
        return Key.of(HistoricalKvKeyCodec.encode(originalPartitionName, primaryKey));
    }

    @Override
    public KvStateLookupResult lookup(Key key) throws IOException {
        return handle.withReadLock(() -> handle.lookup(key));
    }

    @Override
    public void insert(Key key, byte[] value, long logOffset) {
        checkArgument(value.length > 0, "Historical KV insert value must not be empty");
        handle.withWriteLock(() -> handle.insert(key, value, logOffset));
    }

    @Override
    public void update(Key key, @Nullable byte[] value, long logOffset) {
        checkNotNull(value, "Historical KV update value must not be null");
        checkArgument(value.length > 0, "Historical KV update value must not be empty");
        handle.withWriteLock(() -> handle.update(key, value, logOffset));
    }

    @Override
    public void delete(Key key, long logOffset) {
        handle.withWriteLock(() -> handle.delete(key, logOffset));
    }

    @Override
    public void truncateTo(long logOffset, TruncateReason reason) {
        handle.withWriteLock(() -> handle.truncateTo(logOffset, reason));
    }

    @Override
    public int flush(long exclusiveLogOffset) throws IOException {
        return handle.withWriteLock(() -> handle.flush(exclusiveLogOffset));
    }
}
