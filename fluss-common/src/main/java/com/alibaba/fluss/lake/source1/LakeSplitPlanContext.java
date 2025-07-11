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

package com.alibaba.fluss.lake.source1;

import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.predicate.Predicate;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Contextual information for planning data splits in a datalake. Contains necessary metadata and
 * filtering criteria for split planning operations.
 */
public class LakeSplitPlanContext {

    private final long snapshotId;

    @Nullable private final Predicate predicate;
    @Nullable private final Integer bucket;
    @Nullable private final List<ResolvedPartitionSpec> partitionSpecs;

    public LakeSplitPlanContext(
            long snapshotId,
            Predicate predicate,
            Integer bucket,
            List<ResolvedPartitionSpec> partitionSpecs) {
        this.snapshotId = snapshotId;
        this.predicate = predicate;
        this.bucket = bucket;
        this.partitionSpecs = partitionSpecs;
    }

    /**
     * Returns the snapshot id for which that the datalake should plan. The datalake must use the
     * snapshot id to plan splits.
     *
     * @return the snapshot ID
     */
    public long snapshotId() {
        return snapshotId;
    }

    /**
     * Returns the optional filter predicate for split planning. The predicate may be used for rows
     * pruning or other optimizations.
     *
     * @return the filter predicate, or {@code null} if not specified
     */
    @Nullable
    public Predicate predicate() {
        return predicate;
    }

    /**
     * Returns the optional target bucket ID for bucket-aware planning. When specified, only splits
     * belonging to this bucket should be returned.
     *
     * @return the target bucket ID, or {@code null} if not bucket-specific
     */
    @Nullable
    public Integer bucket() {
        return bucket;
    }

    /**
     * Returns the optional list of partition specifications to filter by. When specified, only
     * splits matching these partitions should be returned.
     *
     * @return the list of partition specs, or {@code null} if not partition-specific
     */
    @Nullable
    public List<ResolvedPartitionSpec> partitionSpecs() {
        return partitionSpecs;
    }
}
