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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.metadata.LakeTieringTaskType;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * A tiering split for bootstrap-upgrade tasks. It reads from a lake snapshot to generate bootstrap
 * SST files for primary key table buckets.
 */
public class TieringBootstrapSplit extends TieringSplit {

    private static final String TIERING_BOOTSTRAP_SPLIT_PREFIX = "tiering-bootstrap-split-";

    /** The lake snapshot id to read from. */
    private final long snapshotId;

    public TieringBootstrapSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            int numberOfSplits,
            boolean skipCurrentRound,
            long tieringEpoch,
            @Nullable String remoteDataDir) {
        super(
                tablePath,
                tableBucket,
                partitionName,
                numberOfSplits,
                skipCurrentRound,
                LakeTieringTaskType.BOOTSTRAP_UPGRADE,
                tieringEpoch,
                remoteDataDir);
        this.snapshotId = snapshotId;
    }

    public TieringBootstrapSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long snapshotId,
            int numberOfSplits,
            boolean skipCurrentRound,
            long tieringEpoch) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                snapshotId,
                numberOfSplits,
                skipCurrentRound,
                tieringEpoch,
                null);
    }

    @Override
    public String splitId() {
        return toSplitId(TIERING_BOOTSTRAP_SPLIT_PREFIX, this.tableBucket);
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String toString() {
        return "TieringBootstrapSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", numberOfSplits="
                + numberOfSplits
                + ", skipCurrentRound="
                + skipCurrentRound
                + ", snapshotId="
                + snapshotId
                + '}';
    }

    @Override
    public TieringBootstrapSplit copy(int numberOfSplits) {
        return new TieringBootstrapSplit(
                tablePath,
                tableBucket,
                partitionName,
                snapshotId,
                numberOfSplits,
                skipCurrentRound,
                tieringEpoch,
                remoteDataDir);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringBootstrapSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        TieringBootstrapSplit that = (TieringBootstrapSplit) object;
        return snapshotId == that.snapshotId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), snapshotId);
    }
}
