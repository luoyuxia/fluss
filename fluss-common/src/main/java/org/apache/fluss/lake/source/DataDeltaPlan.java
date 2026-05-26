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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Result of delta planning: splits to scan and deleted files to clean up.
 *
 * <p>Each split contains exactly one compaction output file. This ensures the Reader can determine
 * the fileName directly from the split metadata via {@link LakeSplit#fileName()}. Reader reads each
 * split (via {@link LakeSource#createRecordReader}), scans the {@code __rowid} column to get each
 * row's rowId, combined with the fileName from the split, producing RowId to FilePos entries for
 * the SST index.
 *
 * <p>Deleted files are compaction input files that were merged away. Fluss DV cleanup uses this
 * list to remove stale FileDict and RowPosIndex entries.
 *
 * @param <Split> The type of data split
 * @since 0.8
 */
@PublicEvolving
public class DataDeltaPlan<Split extends LakeSplit> {

    /** Compaction output files, each split contains exactly one file. */
    private final List<Split> splits;

    /**
     * Files deleted by compaction, indexed by (partitionName, bucketId). For non-partitioned
     * tables, partitionName is null. Paimon does not know TableBucket (no tableId/partitionId),
     * only partition name.
     */
    private final Map<String, Map<Integer, List<String>>> deletedFiles;

    public DataDeltaPlan(List<Split> splits, Map<String, Map<Integer, List<String>>> deletedFiles) {
        this.splits = splits;
        this.deletedFiles = deletedFiles;
    }

    /** Returns the splits to scan. Each split contains exactly one compaction output file. */
    public List<Split> getSplits() {
        return splits;
    }

    /**
     * Returns the files deleted by compaction, indexed by (partitionName, bucketId). Fluss DV
     * cleanup uses this to remove stale FileDict and RowPosIndex entries.
     */
    public Map<String, Map<Integer, List<String>>> getDeletedFiles() {
        return deletedFiles;
    }

    /**
     * Returns the deleted file names for a specific partition and bucket.
     *
     * @param partitionName the partition name, or null for non-partitioned tables
     * @param bucketId the bucket id
     * @return the deleted file names, or an empty list if none
     */
    public List<String> getDeletedFiles(String partitionName, int bucketId) {
        Map<Integer, List<String>> bucketFiles = deletedFiles.get(partitionName);
        if (bucketFiles == null) {
            return Collections.emptyList();
        }
        return bucketFiles.getOrDefault(bucketId, Collections.emptyList());
    }
}
