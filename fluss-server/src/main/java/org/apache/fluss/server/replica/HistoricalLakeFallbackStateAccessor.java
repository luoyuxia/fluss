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
import org.apache.fluss.server.kv.KvStateLookupResult.Status;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Adds Paimon fallback to a local historical state accessor. */
final class HistoricalLakeFallbackStateAccessor implements KvStateAccessor {

    private final KvStateAccessor localAccessor;
    private final LakeValueLookup lakeValueLookup;

    HistoricalLakeFallbackStateAccessor(
            KvStateAccessor localAccessor, LakeValueLookup lakeValueLookup) {
        this.localAccessor = checkNotNull(localAccessor, "localAccessor must not be null");
        this.lakeValueLookup = checkNotNull(lakeValueLookup, "lakeValueLookup must not be null");
    }

    @Override
    public Key encodeKey(byte[] primaryKey) {
        return localAccessor.encodeKey(primaryKey);
    }

    @Override
    public KvStateLookupResult lookup(Key encodedKey) throws IOException {
        return localAccessor.lookup(encodedKey);
    }

    @Override
    public KvStateLookupResult lookup(byte[] primaryKey, Key encodedKey) throws Exception {
        KvStateLookupResult localResult = localAccessor.lookup(primaryKey, encodedKey);
        if (localResult.status() != Status.NOT_FOUND) {
            return localResult;
        }

        byte[] lakeValue = lakeValueLookup.lookup(primaryKey);
        return lakeValue == null
                ? KvStateLookupResult.notFound()
                : KvStateLookupResult.present(lakeValue);
    }

    @Override
    public void insert(Key key, byte[] value, long logOffset) {
        localAccessor.insert(key, value, logOffset);
    }

    @Override
    public void update(Key key, @Nullable byte[] value, long logOffset) {
        localAccessor.update(key, value, logOffset);
    }

    @Override
    public void delete(Key key, long logOffset) {
        localAccessor.delete(key, logOffset);
    }

    @Override
    public void truncateTo(long logOffset, TruncateReason reason) {
        localAccessor.truncateTo(logOffset, reason);
    }

    @Override
    public int flush(long exclusiveLogOffset) throws IOException {
        return localAccessor.flush(exclusiveLogOffset);
    }

    @FunctionalInterface
    interface LakeValueLookup {
        @Nullable
        byte[] lookup(byte[] primaryKey) throws Exception;
    }
}
