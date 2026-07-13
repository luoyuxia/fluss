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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;

import javax.annotation.Nullable;

import java.io.IOException;

/** Accessor for the local state used while processing KV records. */
@Internal
public interface KvStateAccessor {

    /** Encodes the primary key for this state. */
    Key encodeKey(byte[] primaryKey);

    /** Looks up an encoded key from the prewrite buffer and underlying KV storage. */
    KvStateLookupResult lookup(Key key) throws IOException;

    /**
     * Looks up a key while retaining its original representation for optional external fallback.
     *
     * <p>The default implementation uses only the encoded key.
     *
     * @param primaryKey original primary key
     * @param encodedKey key encoded for the local state
     */
    default KvStateLookupResult lookup(byte[] primaryKey, Key encodedKey) throws Exception {
        return lookup(encodedKey);
    }

    /** Adds an insert mutation to the prewrite buffer. */
    void insert(Key key, byte[] value, long logOffset);

    /** Adds an update mutation to the prewrite buffer. */
    void update(Key key, @Nullable byte[] value, long logOffset);

    /** Adds a delete mutation to the prewrite buffer. */
    void delete(Key key, long logOffset);

    /** Truncates pending mutations to the given log offset. */
    void truncateTo(long logOffset, TruncateReason reason);

    /** Flushes pending mutations below the exclusive log offset. */
    int flush(long exclusiveLogOffset) throws IOException;
}
