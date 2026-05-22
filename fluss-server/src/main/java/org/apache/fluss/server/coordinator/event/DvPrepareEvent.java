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

import org.apache.fluss.server.entity.DvPrepareData;
import org.apache.fluss.server.zk.data.lake.LakeTable;

import javax.annotation.Nullable;

/**
 * An event to trigger the DV Prepare phase: send DvPrepare to tablet servers that host buckets for
 * the given table. After all servers ACK, the snapshot is marked as readable in ZK and a {@link
 * DvSwitchEvent} is enqueued.
 */
public class DvPrepareEvent implements CoordinatorEvent {

    private final long tableId;
    private final DvPrepareData dvPrepare;
    private final LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata;
    @Nullable private final Long earliestSnapshotIDToKeep;
    private final int retryCount;

    public DvPrepareEvent(
            long tableId,
            DvPrepareData dvPrepare,
            LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata,
            @Nullable Long earliestSnapshotIDToKeep) {
        this(tableId, dvPrepare, lakeSnapshotMetadata, earliestSnapshotIDToKeep, 0);
    }

    public DvPrepareEvent(
            long tableId,
            DvPrepareData dvPrepare,
            LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata,
            @Nullable Long earliestSnapshotIDToKeep,
            int retryCount) {
        this.tableId = tableId;
        this.dvPrepare = dvPrepare;
        this.lakeSnapshotMetadata = lakeSnapshotMetadata;
        this.earliestSnapshotIDToKeep = earliestSnapshotIDToKeep;
        this.retryCount = retryCount;
    }

    public long getTableId() {
        return tableId;
    }

    public DvPrepareData getDvPrepare() {
        return dvPrepare;
    }

    public LakeTable.LakeSnapshotMetadata getLakeSnapshotMetadata() {
        return lakeSnapshotMetadata;
    }

    @Nullable
    public Long getEarliestSnapshotIDToKeep() {
        return earliestSnapshotIDToKeep;
    }

    public int getRetryCount() {
        return retryCount;
    }
}
