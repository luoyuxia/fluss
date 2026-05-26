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

import java.util.List;
import java.util.Map;

/**
 * Result of scanning one {@link LakeSplit}. Contains the new fileId to fileName mappings and the
 * generated SST file metadata.
 *
 * @since 0.8
 */
@PublicEvolving
public class RowPosResult {

    private final int bucketId;

    /** New fileId to fileName mappings for compaction output files. */
    private final Map<Integer, String> newFileDictEntries;

    /** Metadata of generated SST files containing RowId to FilePos entries. */
    private final List<SstMeta> sstMetas;

    public RowPosResult(
            int bucketId, Map<Integer, String> newFileDictEntries, List<SstMeta> sstMetas) {
        this.bucketId = bucketId;
        this.newFileDictEntries = newFileDictEntries;
        this.sstMetas = sstMetas;
    }

    /** Returns the bucket id this result belongs to. */
    public int getBucketId() {
        return bucketId;
    }

    /** Returns the new fileId to fileName mappings. */
    public Map<Integer, String> getNewFileDictEntries() {
        return newFileDictEntries;
    }

    /** Returns the metadata of generated SST files. */
    public List<SstMeta> getSstMetas() {
        return sstMetas;
    }

    /** Metadata for a generated SST file. */
    public static class SstMeta {

        private final String fileName;
        private final long fileSize;

        public SstMeta(String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        /** Returns the SST file name. */
        public String getFileName() {
            return fileName;
        }

        /** Returns the SST file size in bytes. */
        public long getFileSize() {
            return fileSize;
        }
    }
}
