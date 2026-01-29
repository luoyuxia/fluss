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

package org.apache.fluss.server.lake;

import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Interface for reading historical partition data from the data lake.
 *
 * <p>Historical partitions are partitions that have expired in Fluss but still exist in the data
 * lake (e.g., Paimon). This interface provides methods to lookup data from the lake for such
 * partitions.
 *
 * <p>This interface is designed to be implemented by lake-specific modules (e.g.,
 * fluss-lake-paimon) and used by the server to handle lookups for historical partitions
 * transparently.
 */
public interface LakeHistoricalPartitionReader extends Closeable {

    /**
     * Lookup a key from the data lake for a historical partition.
     *
     * @param tablePath the table path
     * @param partitionName the partition name
     * @param bucket the bucket id
     * @param keyBytes the key bytes to lookup
     * @return the value bytes if found, null otherwise
     * @throws IOException if an I/O error occurs
     */
    @Nullable
    byte[] lookup(TablePath tablePath, String partitionName, int bucket, byte[] keyBytes)
            throws IOException;

    /**
     * Batch lookup for multiple keys from the data lake for a historical partition.
     *
     * @param tablePath the table path
     * @param partitionName the partition name
     * @param bucket the bucket id
     * @param keys list of keys to lookup
     * @return list of values corresponding to the keys (null for not found keys)
     * @throws IOException if an I/O error occurs
     */
    List<byte[]> batchLookup(
            TablePath tablePath, String partitionName, int bucket, List<byte[]> keys)
            throws IOException;

    /**
     * Check if the reader supports the given table path.
     *
     * @param tablePath the table path
     * @return true if the reader can handle lookups for this table
     */
    boolean supports(TablePath tablePath);
}
