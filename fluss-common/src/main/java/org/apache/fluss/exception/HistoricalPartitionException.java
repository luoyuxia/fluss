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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Exception thrown when an operation targets a historical partition.
 *
 * <p>Historical partitions are partitions in datalake-enabled tables where:
 *
 * <ul>
 *   <li>Fluss data (logs/KV) has been cleaned up
 *   <li>Partition metadata is retained
 *   <li>Data is still available in the data lake (e.g., Paimon)
 * </ul>
 *
 * <p>Operations that target historical partitions may need special handling to route to the data
 * lake instead of local storage.
 *
 * @since 0.8
 */
@PublicEvolving
public class HistoricalPartitionException extends ApiException {

    private static final long serialVersionUID = 1L;

    private final long partitionId;
    private final String partitionName;

    public HistoricalPartitionException(String message) {
        this(message, -1L, null);
    }

    public HistoricalPartitionException(String message, long partitionId, String partitionName) {
        super(message);
        this.partitionId = partitionId;
        this.partitionName = partitionName;
    }

    public HistoricalPartitionException(String message, Throwable cause) {
        this(message, cause, -1L, null);
    }

    public HistoricalPartitionException(
            String message, Throwable cause, long partitionId, String partitionName) {
        super(message, cause);
        this.partitionId = partitionId;
        this.partitionName = partitionName;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getPartitionName() {
        return partitionName;
    }
}
