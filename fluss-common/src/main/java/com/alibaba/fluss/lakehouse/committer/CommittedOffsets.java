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

package com.alibaba.fluss.lakehouse.committer;

import com.alibaba.fluss.utils.types.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** The already commit bucket and offsets. */
public class CommittedOffsets {

    private final long snapshotId;

    public CommittedOffsets(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    // <partition_name, bucket> -> log offset, partition_name will be null if it's not a partition
    // bucket
    private final Map<Tuple2<String, Integer>, Long> committedOffsets = new HashMap<>();

    public long getSnapshotId() {
        return snapshotId;
    }

    public void addBucket(int bucketId, long offset) {
        committedOffsets.put(Tuple2.of(null, bucketId), offset);
    }

    public void addPartitionBucket(String partitionName, int bucketId, long offset) {
        committedOffsets.put(Tuple2.of(partitionName, bucketId), offset);
    }

    public Map<Tuple2<String, Integer>, Long> getCommitedOffsets() {
        return committedOffsets;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommittedOffsets that = (CommittedOffsets) o;
        return snapshotId == that.snapshotId
                && Objects.equals(committedOffsets, that.committedOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, committedOffsets);
    }
}
