/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.source;

import com.alibaba.fluss.utils.Projection;

import javax.annotation.Nullable;

/** A context for fetch data from lake. */
public class FetchContext {

    private final long fetchStartOffset;
    private final long logEndOffsetOfSnapshot;
    @Nullable private final String partitionName;
    private final int bucket;

    private final long lakeSnapshotId;
    @Nullable private final Projection projection;

    public FetchContext(
            @Nullable String partitionName,
            int bucket,
            long lakeSnapshotId,
            long fetchStartOffset,
            long logEndOffsetOfSnapshot,
            @Nullable Projection projection) {
        this.partitionName = partitionName;
        this.bucket = bucket;
        this.lakeSnapshotId = lakeSnapshotId;
        this.fetchStartOffset = fetchStartOffset;
        this.projection = projection;
        this.logEndOffsetOfSnapshot = logEndOffsetOfSnapshot;
    }

    public FetchContext(
            int bucket,
            long lakeSnapshotId,
            long fetchOffset,
            long logEndOffsetOfSnapshot,
            @Nullable Projection projection) {
        this(null, bucket, lakeSnapshotId, fetchOffset, logEndOffsetOfSnapshot, projection);
    }

    public String partitionName() {
        return partitionName;
    }

    public int bucket() {
        return bucket;
    }

    public long fetchStartOffset() {
        return fetchStartOffset;
    }

    public long lakeSnapshotId() {
        return lakeSnapshotId;
    }

    @Nullable
    public Projection projection() {
        return projection;
    }

    public long logEndOffsetOfSnapshot() {
        return logEndOffsetOfSnapshot;
    }
}
