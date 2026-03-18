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

package org.apache.fluss.flink.lake.split;

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.DataFilePathUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** A DV-aware split for reading one lake snapshot file. */
public class DvAwareLakeSnapshotSplit extends SourceSplitBase {

    public static final byte DV_AWARE_LAKE_SNAPSHOT_SPLIT_KIND = -4;

    private final LakeSplit lakeSplit;
    private final int splitIndex;
    private final long recordsToSkip;
    private final Map<String, byte[]> lakeDvSnapshot;

    public DvAwareLakeSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            LakeSplit lakeSplit,
            int splitIndex,
            @Nullable byte[] deletionVector) {
        this(
                tableBucket,
                partitionName,
                lakeSplit,
                splitIndex,
                0L,
                deletionVector == null
                        ? Collections.emptyMap()
                        : Collections.singletonMap(
                                lakeSplit.dataFilePath() == null
                                        ? "__single_dv__"
                                        : DataFilePathUtils.normalizeDataFilePath(
                                                lakeSplit.dataFilePath()),
                                deletionVector));
    }

    public DvAwareLakeSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            LakeSplit lakeSplit,
            int splitIndex,
            long recordsToSkip,
            @Nullable byte[] deletionVector) {
        this(
                tableBucket,
                partitionName,
                lakeSplit,
                splitIndex,
                recordsToSkip,
                deletionVector == null
                        ? Collections.emptyMap()
                        : Collections.singletonMap(
                                lakeSplit.dataFilePath() == null
                                        ? "__single_dv__"
                                        : DataFilePathUtils.normalizeDataFilePath(
                                                lakeSplit.dataFilePath()),
                                deletionVector));
    }

    public DvAwareLakeSnapshotSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            LakeSplit lakeSplit,
            int splitIndex,
            long recordsToSkip,
            @Nullable Map<String, byte[]> lakeDvSnapshot) {
        super(tableBucket, partitionName);
        this.lakeSplit = lakeSplit;
        this.splitIndex = splitIndex;
        this.recordsToSkip = recordsToSkip;
        this.lakeDvSnapshot = normalizeLakeDvSnapshot(lakeDvSnapshot);
    }

    public LakeSplit getLakeSplit() {
        return lakeSplit;
    }

    public int getSplitIndex() {
        return splitIndex;
    }

    public long getRecordsToSkip() {
        return recordsToSkip;
    }

    public Map<String, byte[]> getLakeDvSnapshot() {
        return lakeDvSnapshot;
    }

    @Nullable
    public byte[] getDeletionVector() {
        String dataFilePath = lakeSplit.dataFilePath();
        if (dataFilePath != null) {
            byte[] value =
                    lakeDvSnapshot.get(DataFilePathUtils.normalizeDataFilePath(dataFilePath));
            if (value != null || lakeDvSnapshot.size() != 1) {
                return value;
            }
        }
        return lakeDvSnapshot.size() == 1 ? lakeDvSnapshot.values().iterator().next() : null;
    }

    @Override
    public String splitId() {
        return toSplitId(
                        "lake-dv-snapshot-",
                        new TableBucket(
                                tableBucket.getTableId(),
                                tableBucket.getPartitionId(),
                                lakeSplit.bucket()))
                + "-"
                + splitIndex;
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    @Override
    public byte splitKind() {
        return DV_AWARE_LAKE_SNAPSHOT_SPLIT_KIND;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DvAwareLakeSnapshotSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        DvAwareLakeSnapshotSplit that = (DvAwareLakeSnapshotSplit) object;
        return splitIndex == that.splitIndex
                && recordsToSkip == that.recordsToSkip
                && Objects.equals(lakeSplit, that.lakeSplit)
                && deepEquals(lakeDvSnapshot, that.lakeDvSnapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                lakeSplit,
                splitIndex,
                recordsToSkip,
                deepHashCode(lakeDvSnapshot));
    }

    @Override
    public String toString() {
        return "DvAwareLakeSnapshotSplit{"
                + "lakeSplit="
                + lakeSplit
                + ", splitIndex="
                + splitIndex
                + ", recordsToSkip="
                + recordsToSkip
                + ", deletionVectorLength="
                + (getDeletionVector() == null ? 0 : getDeletionVector().length)
                + ", tableBucket="
                + tableBucket
                + '}';
    }

    private static Map<String, byte[]> normalizeLakeDvSnapshot(
            @Nullable Map<String, byte[]> lakeDvSnapshot) {
        if (lakeDvSnapshot == null || lakeDvSnapshot.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, byte[]> normalizedSnapshot = new LinkedHashMap<>(lakeDvSnapshot.size());
        for (Map.Entry<String, byte[]> entry : lakeDvSnapshot.entrySet()) {
            normalizedSnapshot.put(
                    DataFilePathUtils.normalizeDataFilePath(entry.getKey()), entry.getValue());
        }
        return normalizedSnapshot;
    }

    private static boolean deepEquals(Map<String, byte[]> left, Map<String, byte[]> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (Map.Entry<String, byte[]> entry : left.entrySet()) {
            if (!Arrays.equals(entry.getValue(), right.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static int deepHashCode(Map<String, byte[]> map) {
        int result = 1;
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            result = 31 * result + Objects.hash(entry.getKey(), Arrays.hashCode(entry.getValue()));
        }
        return result;
    }
}
