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

package org.apache.fluss.flink.lake.split;

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** A split mixing Lake snapshot and Fluss log. */
public class LakeSnapshotAndFlussLogSplit extends SourceSplitBase {

    public static final byte LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND = -2;

    // may be null when no lake snapshot data for the bucket
    @Nullable private final List<LakeSplit> lakeSnapshotSplits;

    /**
     * The index of the current split in the lake snapshot splits to be read, enable to skip read
     * lake split via this lake split index.
     */
    private int currentLakeSplitIndex;
    /** The records to skip when reading a lake split. */
    private long recordToSkip;

    /** Whether the lake split has been finished. */
    private boolean isLakeSplitFinished;

    private long startingOffset;
    private final long stoppingOffset;

    /**
     * Override partition ID for log subscription. When set, the log scanner subscribes to this
     * partition (e.g., __historical__) instead of the tableBucket's own partition.
     */
    @Nullable private final Long logPartitionId;

    /**
     * Include filter: only keep log records whose auto-partition key value equals this string. Used
     * for expired partition splits that subscribe to __historical__ log.
     */
    @Nullable private final String logIncludePartition;

    /**
     * Exclude filter: skip log records whose auto-partition key value is in this list. Used for
     * __historical__ splits to avoid duplicating records already handled by expired partition
     * splits.
     */
    @Nullable private final List<String> logExcludePartitions;

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable List<LakeSplit> snapshotSplits,
            long startingOffset,
            long stoppingOffset) {
        this(
                tableBucket,
                partitionName,
                snapshotSplits,
                startingOffset,
                stoppingOffset,
                0,
                0,
                // if lake splits is null, no lake splits, also means LakeSplitFinished
                snapshotSplits == null,
                null,
                null,
                null);
    }

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable List<LakeSplit> snapshotSplits,
            long startingOffset,
            long stoppingOffset,
            long recordsToSkip,
            int currentLakeSplitIndex,
            boolean isLakeSplitFinished) {
        this(
                tableBucket,
                partitionName,
                snapshotSplits,
                startingOffset,
                stoppingOffset,
                recordsToSkip,
                currentLakeSplitIndex,
                isLakeSplitFinished,
                null,
                null,
                null);
    }

    public LakeSnapshotAndFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable List<LakeSplit> snapshotSplits,
            long startingOffset,
            long stoppingOffset,
            long recordsToSkip,
            int currentLakeSplitIndex,
            boolean isLakeSplitFinished,
            @Nullable Long logPartitionId,
            @Nullable String logIncludePartition,
            @Nullable List<String> logExcludePartitions) {
        super(tableBucket, partitionName);
        this.lakeSnapshotSplits = snapshotSplits;
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.recordToSkip = recordsToSkip;
        this.currentLakeSplitIndex = currentLakeSplitIndex;
        this.isLakeSplitFinished = isLakeSplitFinished;
        this.logPartitionId = logPartitionId;
        this.logIncludePartition = logIncludePartition;
        this.logExcludePartitions =
                logExcludePartitions != null
                        ? Collections.unmodifiableList(logExcludePartitions)
                        : null;
    }

    public LakeSnapshotAndFlussLogSplit updateWithRecordsToSkip(long recordsToSkip) {
        this.recordToSkip = recordsToSkip;
        return this;
    }

    public LakeSnapshotAndFlussLogSplit updateWithCurrentLakeSplitIndex(int currentLakeSplitIndex) {
        this.currentLakeSplitIndex = currentLakeSplitIndex;
        return this;
    }

    public LakeSnapshotAndFlussLogSplit updateWithStartingOffset(long startingOffset) {
        this.startingOffset = startingOffset;
        return this;
    }

    public LakeSnapshotAndFlussLogSplit updateWithLakeSplitFinished(boolean isLakeSplitFinished) {
        this.isLakeSplitFinished = isLakeSplitFinished;
        return this;
    }

    public long getRecordsToSkip() {
        return recordToSkip;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public boolean isLakeSplitFinished() {
        return isLakeSplitFinished;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset >= 0 ? Optional.of(stoppingOffset) : Optional.empty();
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    public boolean isStreaming() {
        return !getStoppingOffset().isPresent();
    }

    protected byte splitKind() {
        return LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
    }

    @Override
    public String splitId() {
        return toSplitId("lake-hybrid-snapshot-log-", tableBucket);
    }

    @Nullable
    public List<LakeSplit> getLakeSplits() {
        return lakeSnapshotSplits;
    }

    public int getCurrentLakeSplitIndex() {
        return currentLakeSplitIndex;
    }

    @Nullable
    public Long getLogPartitionId() {
        return logPartitionId;
    }

    @Nullable
    public String getLogIncludePartition() {
        return logIncludePartition;
    }

    @Nullable
    public List<String> getLogExcludePartitions() {
        return logExcludePartitions;
    }

    @Override
    public String toString() {
        return "LakeSnapshotAndFlussLogSplit{"
                + "lakeSnapshotSplits="
                + lakeSnapshotSplits
                + ", recordToSkip="
                + recordToSkip
                + ", currentLakeSplitIndex="
                + currentLakeSplitIndex
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", logPartitionId="
                + logPartitionId
                + ", logIncludePartition='"
                + logIncludePartition
                + '\''
                + ", logExcludePartitions="
                + logExcludePartitions
                + '}';
    }
}
