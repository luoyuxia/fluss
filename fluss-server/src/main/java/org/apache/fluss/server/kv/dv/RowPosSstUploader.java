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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Uploads per-round RowPos SST files and index to remote storage.
 *
 * <p>The upload layout is:
 *
 * <pre>
 * {remoteLakeTableSnapshotDir}/rowPos/{roundUuid}/
 *   ├── index.json              (written last for atomic visibility)
 *   ├── {bucketId}/sst_0.sst
 *   ├── {bucketId}/sst_1.sst
 *   └── ...
 * </pre>
 *
 * <p>The {@code index.json} is always written <b>last</b>, so its presence guarantees that all SST
 * files for the round have been fully uploaded.
 */
@Internal
public class RowPosSstUploader {

    private static final String ROW_POS_DIR = "rowPos";
    private static final String INDEX_FILE = "index.json";
    private static final int UPLOAD_BUFFER_SIZE = 16 * 1024;

    private final FsPath remoteLakeTableSnapshotDir;

    public RowPosSstUploader(FsPath remoteLakeTableSnapshotDir) {
        this.remoteLakeTableSnapshotDir = remoteLakeTableSnapshotDir;
    }

    /**
     * Uploads one round of SST data for all buckets.
     *
     * <p>Steps:
     *
     * <ol>
     *   <li>Generate a round UUID
     *   <li>Upload each bucket's SST files to {@code rowPos/{roundUuid}/{bucketId}/}
     *   <li>Write {@code index.json} last (atomic visibility guarantee)
     * </ol>
     *
     * @param bucketSstMap mapping from bucketId to local SST data
     * @return the generated round UUID
     */
    public String uploadRound(Map<Integer, BucketSstData> bucketSstMap) throws IOException {
        String roundUuid = UUID.randomUUID().toString();
        FsPath roundDir = new FsPath(remoteLakeTableSnapshotDir, ROW_POS_DIR + "/" + roundUuid);

        FileSystem fs = roundDir.getFileSystem();
        fs.mkdirs(roundDir);

        // Build index while uploading SST files
        Map<Integer, List<RowPosSstIndex.SstFileEntry>> indexEntries = new HashMap<>();

        for (Map.Entry<Integer, BucketSstData> entry : bucketSstMap.entrySet()) {
            int bucketId = entry.getKey();
            BucketSstData data = entry.getValue();

            FsPath bucketDir = new FsPath(roundDir, String.valueOf(bucketId));
            fs.mkdirs(bucketDir);

            List<RowPosSstIndex.SstFileEntry> fileEntries = new ArrayList<>();
            for (RowPosSstFileWriter.SstFileMeta meta : data.getSstMetas()) {
                FsPath remoteSstPath = new FsPath(bucketDir, meta.getFileName());
                File localFile = new File(data.getLocalSstDir(), meta.getFileName());
                uploadFile(fs, localFile, remoteSstPath);
                fileEntries.add(
                        new RowPosSstIndex.SstFileEntry(meta.getFileName(), meta.getFileSize()));
            }
            indexEntries.put(bucketId, fileEntries);
        }

        // Write index.json last for atomic visibility
        RowPosSstIndex index = new RowPosSstIndex(indexEntries);
        FsPath indexPath = new FsPath(roundDir, INDEX_FILE);
        byte[] indexBytes = index.toJsonBytes();
        try (FSDataOutputStream out = fs.create(indexPath, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(indexBytes);
        }

        return roundUuid;
    }

    private void uploadFile(FileSystem fs, File localFile, FsPath remotePath) throws IOException {
        FileInputStream fis = null;
        FSDataOutputStream out = null;
        try {
            fis = new FileInputStream(localFile);
            out = fs.create(remotePath, FileSystem.WriteMode.NO_OVERWRITE);
            byte[] buffer = new byte[UPLOAD_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        } finally {
            IOUtils.closeQuietly(fis);
            IOUtils.closeQuietly(out);
        }
    }

    /** Local SST data for one bucket. */
    public static class BucketSstData {
        private final String localSstDir;
        private final List<RowPosSstFileWriter.SstFileMeta> sstMetas;

        public BucketSstData(String localSstDir, List<RowPosSstFileWriter.SstFileMeta> sstMetas) {
            this.localSstDir = localSstDir;
            this.sstMetas = sstMetas;
        }

        public String getLocalSstDir() {
            return localSstDir;
        }

        public List<RowPosSstFileWriter.SstFileMeta> getSstMetas() {
            return sstMetas;
        }
    }
}
