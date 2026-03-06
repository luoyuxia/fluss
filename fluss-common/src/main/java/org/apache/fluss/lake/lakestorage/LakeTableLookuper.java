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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

/**
 * An interface for performing point lookups against lake storage for expired partitions.
 *
 * <p>When a partition has expired in Fluss and its data has been tiered to lake storage, this
 * lookuper can be used to perform point lookups directly against the lake storage.
 *
 * <p>Each instance is bound to a specific table and caches per-table resources (e.g., catalog
 * connections, table metadata) for efficient repeated lookups.
 *
 * <p>The key bytes passed to {@link #lookup} are already encoded in the lake storage's native
 * format (e.g., Paimon BinaryRow format for Paimon tables) by the client-side encoder, so
 * implementations can use them directly without re-encoding.
 *
 * @since 0.10
 */
@PublicEvolving
public interface LakeTableLookuper {

    /**
     * Lookup a single key from lake storage for an expired partition.
     *
     * <p>The key bytes are already encoded in the lake storage's native key format by the
     * client-side encoder (e.g., Paimon BinaryRow format). Implementations can wrap them directly
     * as the lake storage's key type without decode/re-encode.
     *
     * <p>The returned value bytes should be encoded with the a schema ID so the client can
     * correctly decode them.
     *
     * @param key the encoded key bytes to lookup, in the lake storage's native key format
     * @param context the lookup context containing partition, bucket, and schema information
     * @return the encoded value bytes, or null if the key is not found
     * @throws Exception if the lookup fails
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    /** Context for a lake table lookup, containing the partition name, bucket id and schema id. */
    class LookupContext {

        private final @Nullable String partitionName;
        private final int bucketId;
        private final int schemaId;

        public LookupContext(@Nullable String partitionName, int bucketId, int schemaId) {
            this.partitionName = partitionName;
            this.bucketId = bucketId;
            this.schemaId = schemaId;
        }

        /** Returns the partition name, or null for non-partitioned tables. */
        @Nullable
        public String getPartitionName() {
            return partitionName;
        }

        /** Returns the bucket id. */
        public int getBucketId() {
            return bucketId;
        }

        /** Returns the latest schema id of the table. */
        public int getSchemaId() {
            return schemaId;
        }
    }
}
