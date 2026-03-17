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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;

import javax.annotation.Nullable;

/** The data for request {@link NotifyKvSnapshotOffsetRequest}. */
public class NotifyKvSnapshotOffsetData {
    private final TableBucket tableBucket;
    @Nullable private final Long minRetainOffset;
    @Nullable private final Long dvReadableSnapshotId;
    @Nullable private final Long dvReadableTieredOffset;
    private final int coordinatorEpoch;

    public NotifyKvSnapshotOffsetData(
            TableBucket tableBucket,
            @Nullable Long minRetainOffset,
            @Nullable Long dvReadableSnapshotId,
            @Nullable Long dvReadableTieredOffset,
            int coordinatorEpoch) {
        this.tableBucket = tableBucket;
        this.minRetainOffset = minRetainOffset;
        this.dvReadableSnapshotId = dvReadableSnapshotId;
        this.dvReadableTieredOffset = dvReadableTieredOffset;
        this.coordinatorEpoch = coordinatorEpoch;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public @Nullable Long getMinRetainOffset() {
        return minRetainOffset;
    }

    public @Nullable Long getDvReadableSnapshotId() {
        return dvReadableSnapshotId;
    }

    public @Nullable Long getDvReadableTieredOffset() {
        return dvReadableTieredOffset;
    }

    public boolean hasDvReadableSwitch() {
        return dvReadableSnapshotId != null && dvReadableTieredOffset != null;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    @Override
    public String toString() {
        return "NotifyKvSnapshotOffsetData{"
                + "tableBucket="
                + tableBucket
                + ", minRetainOffset="
                + minRetainOffset
                + ", dvReadableSnapshotId="
                + dvReadableSnapshotId
                + ", dvReadableTieredOffset="
                + dvReadableTieredOffset
                + ", coordinatorEpoch="
                + coordinatorEpoch
                + '}';
    }
}
