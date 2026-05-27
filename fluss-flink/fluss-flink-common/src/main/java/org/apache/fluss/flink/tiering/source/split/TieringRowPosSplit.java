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
import java.util.Objects;

/**
 * A split for scanning row positions (__rowid) from compaction output files in the lake.
 *
 * <p>Created by the Enumerator from a {@link
 * org.apache.fluss.flink.tiering.event.RowPosRequestEvent}. Each split contains a batch of files
 * for the same bucket. The Reader deserializes the contained lake splits, reads the files, and
 * extracts __rowid values to build RowId-to-FilePos mappings.
 */
public class TieringRowPosSplit extends TieringSplit {

    public static final byte TIERING_ROW_POS_SPLIT_FLAG = 3;

    private static final String TIERING_ROW_POS_SPLIT_PREFIX = "tiering-row-pos-split-";

    /** File IDs assigned by the Enumerator, one per file in this batch. */
    private final int[] fileIds;

    /** Serialized lake split bytes, one per file in this batch. Parallel to {@code fileIds}. */
    private final byte[][] serializedLakeSplits;

    /** Total expected results for this scan round, used by the Committer for coordination. */
    private final int totalExpectedResults;

    public TieringRowPosSplit(
            TablePath tablePath,
            TableBucket tableBucket,
            @Nullable String partitionName,
            int[] fileIds,
            byte[][] serializedLakeSplits,
            int totalExpectedResults) {
        super(tablePath, tableBucket, partitionName, totalExpectedResults, false);
        this.fileIds = fileIds;
        this.serializedLakeSplits = serializedLakeSplits;
        this.totalExpectedResults = totalExpectedResults;
    }

    @Override
    public String splitId() {
        return toSplitId(TIERING_ROW_POS_SPLIT_PREFIX, this.tableBucket) + "-f" + fileIds[0];
    }

    public int[] getFileIds() {
        return fileIds;
    }

    public byte[][] getSerializedLakeSplits() {
        return serializedLakeSplits;
    }

    public int getTotalExpectedResults() {
        return totalExpectedResults;
    }

    @Override
    public TieringRowPosSplit copy(int numberOfSplits) {
        return new TieringRowPosSplit(
                tablePath,
                tableBucket,
                partitionName,
                fileIds,
                serializedLakeSplits,
                numberOfSplits);
    }

    @Override
    public String toString() {
        return "TieringRowPosSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", fileIds="
                + Arrays.toString(fileIds)
                + ", fileCount="
                + serializedLakeSplits.length
                + ", totalExpectedResults="
                + totalExpectedResults
                + '}';
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof TieringRowPosSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        TieringRowPosSplit that = (TieringRowPosSplit) object;
        return totalExpectedResults == that.totalExpectedResults
                && Arrays.equals(fileIds, that.fileIds)
                && Arrays.deepEquals(serializedLakeSplits, that.serializedLakeSplits);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), totalExpectedResults);
        result = 31 * result + Arrays.hashCode(fileIds);
        result = 31 * result + Arrays.deepHashCode(serializedLakeSplits);
        return result;
    }
}
