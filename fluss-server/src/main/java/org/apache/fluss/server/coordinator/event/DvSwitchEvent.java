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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.TableBucket;

import java.util.Set;

/**
 * An event to trigger the DV Switch phase: send DvReadableSwitch to tablet servers that host
 * buckets for the given table, so they update their readable offset.
 */
public class DvSwitchEvent implements CoordinatorEvent {

    private final long tableId;
    private final long snapshotId;
    private final Set<TableBucket> tableBuckets;
    private final int retryCount;

    public DvSwitchEvent(long tableId, long snapshotId, Set<TableBucket> tableBuckets) {
        this(tableId, snapshotId, tableBuckets, 0);
    }

    public DvSwitchEvent(
            long tableId, long snapshotId, Set<TableBucket> tableBuckets, int retryCount) {
        this.tableId = tableId;
        this.snapshotId = snapshotId;
        this.tableBuckets = tableBuckets;
        this.retryCount = retryCount;
    }

    public long getTableId() {
        return tableId;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public Set<TableBucket> getTableBuckets() {
        return tableBuckets;
    }

    public int getRetryCount() {
        return retryCount;
    }
}
