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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Codec for keys stored in historical KV state. */
@Internal
public final class HistoricalKvKeyCodec {

    private HistoricalKvKeyCodec() {}

    /**
     * Encodes an original partition name and primary key into one unambiguous storage key.
     *
     * <p>The key layout is a four-byte big-endian UTF-8 partition-name length, followed by the
     * partition-name bytes and original primary-key bytes.
     */
    public static byte[] encode(String originalPartitionName, byte[] originalPrimaryKey) {
        checkNotNull(originalPartitionName, "originalPartitionName must not be null");
        checkArgument(!originalPartitionName.isEmpty(), "originalPartitionName must not be empty");
        checkNotNull(originalPrimaryKey, "originalPrimaryKey must not be null");

        byte[] partitionNameBytes = originalPartitionName.getBytes(StandardCharsets.UTF_8);
        long encodedLength =
                Integer.BYTES + (long) partitionNameBytes.length + originalPrimaryKey.length;
        checkArgument(encodedLength <= Integer.MAX_VALUE, "The encoded historical key is too long");

        return ByteBuffer.allocate((int) encodedLength)
                .putInt(partitionNameBytes.length)
                .put(partitionNameBytes)
                .put(originalPrimaryKey)
                .array();
    }
}
