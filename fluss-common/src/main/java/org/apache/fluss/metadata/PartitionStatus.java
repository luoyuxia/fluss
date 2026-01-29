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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * The status of a partition in Fluss.
 *
 * <p>For datalake-enabled auto-partitioned tables, partitions go through the following lifecycle:
 *
 * <ul>
 *   <li>ACTIVE: Normal partition with data in Fluss (logs/KV)
 *   <li>HISTORICAL: Fluss data has been cleaned up, metadata retained, lake data available. This
 *       partition can still receive late data which will be tiered to the lake.
 * </ul>
 *
 * @since 0.8
 */
@PublicEvolving
public enum PartitionStatus {

    /**
     * Active partition with data stored in Fluss.
     *
     * <p>This is the normal state for partitions. Data is stored in LogStore/KvStore and may be
     * tiered to the data lake.
     */
    ACTIVE((byte) 0),

    /**
     * Historical partition where Fluss data has been cleaned up.
     *
     * <p>For datalake-enabled tables:
     *
     * <ul>
     *   <li>Partition metadata is retained
     *   <li>Fluss data (logs/KV) has been cleaned up
     *   <li>Data that was tiered to the lake is still available
     *   <li>Late data writes can be accepted and will be tiered to the lake
     *   <li>Lookups for PK tables will query the lake directly
     * </ul>
     *
     * <p>The partition will be fully cleaned up when it expires in the lake (e.g., Paimon's
     * partition.expiration-time).
     */
    HISTORICAL((byte) 1);

    private final byte value;

    PartitionStatus(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static PartitionStatus fromValue(byte value) {
        switch (value) {
            case 0:
                return ACTIVE;
            case 1:
                return HISTORICAL;
            default:
                throw new IllegalArgumentException("Unknown PartitionStatus value: " + value);
        }
    }

    /**
     * Check if this partition is a historical partition.
     *
     * @return true if this is a historical partition
     */
    public boolean isHistorical() {
        return this == HISTORICAL;
    }

    /**
     * Check if this partition is an active partition.
     *
     * @return true if this is an active partition
     */
    public boolean isActive() {
        return this == ACTIVE;
    }
}
