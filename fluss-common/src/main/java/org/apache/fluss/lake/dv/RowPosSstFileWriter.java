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

package org.apache.fluss.lake.dv;

import org.apache.fluss.annotation.Internal;

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Writes sorted RowId to FilePos entries into RocksDB SST files.
 *
 * <p>Entries must be provided in ascending RowId order (BigEndian encoding ensures correct RocksDB
 * sorting). When the number of entries exceeds {@link #MAX_ENTRIES_PER_SST}, they are automatically
 * split across multiple SST files.
 */
@Internal
public class RowPosSstFileWriter implements Closeable {

    public static final int MAX_ENTRIES_PER_SST = 1_000_000;

    private final String outputDir;

    public RowPosSstFileWriter(String outputDir) {
        this.outputDir = outputDir;
    }

    /**
     * Writes sorted entries into one or more SST files.
     *
     * @param sortedEntries entries sorted by RowId in ascending order
     * @return metadata for each generated SST file; empty list if input is empty
     */
    public List<SstFileMeta> write(List<RowPosEntry> sortedEntries) throws IOException {
        if (sortedEntries.isEmpty()) {
            return Collections.emptyList();
        }

        List<SstFileMeta> generatedFiles = new ArrayList<>();
        int totalEntries = sortedEntries.size();
        int offset = 0;
        int fileIndex = 0;

        while (offset < totalEntries) {
            int end = Math.min(offset + MAX_ENTRIES_PER_SST, totalEntries);
            String fileName = "sst_" + fileIndex + ".sst";
            String filePath = new File(outputDir, fileName).getPath();

            try (EnvOptions envOptions = new EnvOptions();
                    Options options = new Options();
                    SstFileWriter writer = new SstFileWriter(envOptions, options)) {
                writer.open(filePath);
                for (int i = offset; i < end; i++) {
                    RowPosEntry entry = sortedEntries.get(i);
                    writer.put(FilePos.encodeRowId(entry.getRowId()), entry.getFilePos().encode());
                }
                writer.finish();
            } catch (RocksDBException e) {
                throw new IOException("Failed to write SST file: " + filePath, e);
            }

            long fileSize = new File(filePath).length();
            generatedFiles.add(new SstFileMeta(fileName, fileSize));
            offset = end;
            fileIndex++;
        }

        return generatedFiles;
    }

    @Override
    public void close() {
        // No persistent resources to close; SstFileWriter is closed per write() call.
    }

    /** An immutable entry: (rowId, filePos). */
    public static class RowPosEntry implements Comparable<RowPosEntry> {
        private final long rowId;
        private final FilePos filePos;

        public RowPosEntry(long rowId, FilePos filePos) {
            this.rowId = rowId;
            this.filePos = filePos;
        }

        public long getRowId() {
            return rowId;
        }

        public FilePos getFilePos() {
            return filePos;
        }

        @Override
        public int compareTo(RowPosEntry other) {
            return Long.compare(this.rowId, other.rowId);
        }
    }

    /** Metadata for a generated SST file. */
    public static class SstFileMeta {
        private final String fileName;
        private final long fileSize;

        public SstFileMeta(String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        public String getFileName() {
            return fileName;
        }

        public long getFileSize() {
            return fileSize;
        }
    }
}
