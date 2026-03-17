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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/** The table split for tiering service. It's used to describe the log data of a table bucket. */
public class TieringLogSplit extends TieringSplit {

    private static final String TIERING_LOG_SPLIT_PREFIX = "tiering-log-split-";

    private final long startingOffset;
    private final long stoppingOffset;
    @Nullable private final Map<String, byte[]> lakeDvSnapshot;
    @Nullable private final Map<Long, byte[]> logDvSnapshot;

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                UNKNOWN_NUMBER_OF_SPLITS,
                false,
                null,
                null);
    }

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            int numberOfSplits) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                false,
                null,
                null);
    }

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            int numberOfSplits,
            boolean skipCurrentRound) {
        this(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                skipCurrentRound,
                null,
                null);
    }

    public TieringLogSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            int numberOfSplits,
            boolean skipCurrentRound,
            @Nullable Map<String, byte[]> lakeDvSnapshot,
            @Nullable Map<Long, byte[]> logDvSnapshot) {
        super(tablePath, tableBucket, partitionName, numberOfSplits, skipCurrentRound);
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.lakeDvSnapshot = lakeDvSnapshot;
        this.logDvSnapshot = logDvSnapshot;
    }

    @Override
    public String splitId() {
        return toSplitId(TIERING_LOG_SPLIT_PREFIX, this.tableBucket);
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public long getStoppingOffset() {
        return stoppingOffset;
    }

    @Nullable
    public Map<String, byte[]> getLakeDvSnapshot() {
        return lakeDvSnapshot;
    }

    @Nullable
    public Map<Long, byte[]> getLogDvSnapshot() {
        return logDvSnapshot;
    }

    @Override
    public String toString() {
        return "TieringLogSplit{"
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
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + '}';
    }

    @Override
    public TieringLogSplit copy(int numberOfSplits) {
        return new TieringLogSplit(
                tablePath,
                tableBucket,
                partitionName,
                startingOffset,
                stoppingOffset,
                numberOfSplits,
                skipCurrentRound,
                lakeDvSnapshot,
                logDvSnapshot);
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringLogSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        TieringLogSplit that = (TieringLogSplit) object;
        return startingOffset == that.startingOffset
                && stoppingOffset == that.stoppingOffset
                && byteMapEquals(lakeDvSnapshot, that.lakeDvSnapshot)
                && byteMapEquals(logDvSnapshot, that.logDvSnapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                startingOffset,
                stoppingOffset,
                byteMapHashCode(lakeDvSnapshot),
                byteMapHashCode(logDvSnapshot));
    }

    private static <K> boolean byteMapEquals(
            @Nullable Map<K, byte[]> left, @Nullable Map<K, byte[]> right) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null || left.size() != right.size()) {
            return false;
        }
        for (Map.Entry<K, byte[]> entry : left.entrySet()) {
            if (!right.containsKey(entry.getKey())
                    || !Arrays.equals(entry.getValue(), right.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static <K> int byteMapHashCode(@Nullable Map<K, byte[]> map) {
        if (map == null) {
            return 0;
        }
        int result = 1;
        for (Map.Entry<K, byte[]> entry : map.entrySet()) {
            result = 31 * result + Objects.hashCode(entry.getKey());
            result = 31 * result + Arrays.hashCode(entry.getValue());
        }
        return result;
    }
}
