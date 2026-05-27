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

package org.apache.fluss.flink.tiering.source;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * Carries row position scan results from Reader back to Committer through the data stream.
 *
 * <p>Each instance corresponds to one batch of compaction output files for a single bucket (one
 * {@link org.apache.fluss.flink.tiering.source.split.TieringRowPosSplit}). Contains per-file
 * results with __rowid values for building DV index entries.
 */
public class RowPosResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Per-file scan results. Each entry corresponds to one compaction output file. */
    private final List<FileRowPos> fileResults;

    /** The partition name, or null for non-partitioned tables. */
    @Nullable private final String partitionName;

    /** The bucket ID this result belongs to. */
    private final int bucketId;

    public RowPosResult(
            List<FileRowPos> fileResults, @Nullable String partitionName, int bucketId) {
        this.fileResults = fileResults;
        this.partitionName = partitionName;
        this.bucketId = bucketId;
    }

    public List<FileRowPos> getFileResults() {
        return fileResults;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    public int getBucketId() {
        return bucketId;
    }

    /**
     * Holds __rowid values for a single compaction output file. The {@code rowIds} array contains
     * __rowid values in row order, where the array index is the row position within the file.
     */
    public static class FileRowPos implements Serializable {

        private static final long serialVersionUID = 1L;

        /** The file ID assigned by the Enumerator. */
        private final int fileId;

        /** The file name from the deserialized LakeSplit. */
        private final String fileName;

        /**
         * The __rowid values in row order. Uses {@code long[]} instead of {@code List<Long>} for
         * memory efficiency — arrays can contain millions of entries.
         */
        private final long[] rowIds;

        public FileRowPos(int fileId, String fileName, long[] rowIds) {
            this.fileId = fileId;
            this.fileName = fileName;
            this.rowIds = rowIds;
        }

        public int getFileId() {
            return fileId;
        }

        public String getFileName() {
            return fileName;
        }

        public long[] getRowIds() {
            return rowIds;
        }
    }
}
