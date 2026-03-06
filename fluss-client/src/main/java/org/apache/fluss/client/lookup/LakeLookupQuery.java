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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Class to represent a Lake Lookup operation for looking up data from lake storage (e.g., Paimon)
 * when the partition doesn't exist in Fluss anymore.
 *
 * <p>This is used when a partition has been expired and dropped from Fluss, but the data is still
 * available in the lake storage.
 */
@Internal
public class LakeLookupQuery extends AbstractLookupQuery<byte[]> {

    private final CompletableFuture<byte[]> future;
    private final long tableId;
    private final String partitionName;
    private final int bucketId;

    /**
     * Creates a LakeLookupQuery.
     *
     * @param tablePath the table path
     * @param tableId the table id
     * @param partitionName the partition name (may be null for non-partitioned tables)
     * @param bucketId the bucket id
     * @param key the encoded key bytes
     */
    LakeLookupQuery(
            TablePath tablePath,
            long tableId,
            @Nullable String partitionName,
            int bucketId,
            byte[] key) {
        // For lake lookup, we don't have a partitionId since the partition doesn't exist in Fluss
        // We use a TableBucket with null partitionId
        super(tablePath, null, key);
        this.future = new CompletableFuture<>();
        this.tableId = tableId;
        this.partitionName = partitionName;
        this.bucketId = bucketId;
    }

    @Override
    public LookupType lookupType() {
        return LookupType.LAKE_LOOKUP;
    }

    @Override
    public CompletableFuture<byte[]> future() {
        return future;
    }

    /**
     * Gets the table id for lake lookup.
     *
     * @return the table id
     */
    public long tableId() {
        return tableId;
    }

    /**
     * Gets the partition name for lake lookup.
     *
     * @return the partition name, or null if the table is not partitioned
     */
    @Nullable
    public String partitionName() {
        return partitionName;
    }

    /**
     * Gets the bucket id for lake lookup.
     *
     * @return the bucket id
     */
    public int bucketId() {
        return bucketId;
    }
}
