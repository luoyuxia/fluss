/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.committer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * The result of a lake commit operation, containing the committed snapshot ID and the readable
 * snapshot information.
 *
 * <p>For most implementations, the readable snapshot is the same as the committed snapshot, and the
 * log end offsets are the same as the tiered offsets from TieringCommitOperator.
 *
 * <p>For Paimon DV tables, the readable snapshot may be different from the committed snapshot, and
 * the log end offsets may be different as well (based on compaction status).
 *
 * @since 0.9
 */
@PublicEvolving
public class LakeCommitResult {

    // The snapshot ID that was just committed
    private final long committedSnapshotId;

    // The readable snapshot ID, for most case, readableSnapshotId is same to committedSnapshotId
    // null if we don't know the actual latest readableSnapshotId during commit
    @Nullable private final Long readableSnapshotId;

    // The log end offsets for the readable snapshot
    // null if we don't know the actual readableSnapshotId
    @Nullable private final Map<TableBucket, Long> readableLogEndOffsets;

    private LakeCommitResult(
            long committedSnapshotId,
            @Nullable Long readableSnapshotId,
            @Nullable Map<TableBucket, Long> readableLogEndOffsets) {
        this.committedSnapshotId = committedSnapshotId;
        this.readableSnapshotId = readableSnapshotId;
        this.readableLogEndOffsets = readableLogEndOffsets;
    }

    public static LakeCommitResult committableIsReadable(long committedSnapshotId) {
        return new LakeCommitResult(committedSnapshotId, committedSnapshotId, null);
    }

    public static LakeCommitResult unknownReadableSnapshot(long committedSnapshotId) {
        return new LakeCommitResult(committedSnapshotId, null, null);
    }

    public static LakeCommitResult withReadableSnapshot(
            long committedSnapshotId,
            long readableSnapshotId,
            Map<TableBucket, Long> readableLogEndOffsets) {
        return new LakeCommitResult(committedSnapshotId, readableSnapshotId, readableLogEndOffsets);
    }

    public long getCommittedSnapshotId() {
        return committedSnapshotId;
    }

    @Nullable
    public Long getReadableSnapshotId() {
        return readableSnapshotId;
    }

    @Nullable
    public Map<TableBucket, Long> getReadableLogEndOffsets() {
        return readableLogEndOffsets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LakeCommitResult that = (LakeCommitResult) o;
        return committedSnapshotId == that.committedSnapshotId
                && Objects.equals(readableSnapshotId, that.readableSnapshotId)
                && Objects.equals(readableLogEndOffsets, that.readableLogEndOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(committedSnapshotId, readableSnapshotId, readableLogEndOffsets);
    }

    @Override
    public String toString() {
        return "LakeCommitResult{"
                + "committedSnapshotId="
                + committedSnapshotId
                + ", readableSnapshotId="
                + readableSnapshotId
                + ", readableLogEndOffsets="
                + readableLogEndOffsets
                + '}';
    }
}
