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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.RowPosSstFileWriter;
import org.apache.fluss.lake.dv.RowPosSstIndex;
import org.apache.fluss.lake.dv.RowPosSstUploader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowPosSstUploader} and {@link RowPosSstDownloader}. */
class RowPosSstUploadDownloadTest {

    @TempDir Path tempDir;
    private DvRocksDB dvRocksDB;

    @BeforeEach
    void setUp() throws Exception {
        dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString());
    }

    @AfterEach
    void tearDown() {
        if (dvRocksDB != null) {
            dvRocksDB.close();
        }
    }

    @Test
    void testUploadAndDownloadRoundTrip() throws Exception {
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        String localSstDir = tempDir.resolve("local_sst").toString();
        new File(localSstDir).mkdirs();

        // Generate SST files locally
        List<RowPosSstFileWriter.RowPosEntry> entries =
                Arrays.asList(entry(1L, 1, 10L), entry(2L, 1, 20L), entry(3L, 2, 30L));

        List<RowPosSstFileWriter.SstFileMeta> metas;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir)) {
            metas = writer.write(entries);
        }

        // Upload
        long snapshotId = 100L;
        RowPosSstUploader uploader = new RowPosSstUploader(remoteDir);
        Map<Integer, RowPosSstUploader.BucketSstData> bucketSstMap = new HashMap<>();
        bucketSstMap.put(0, new RowPosSstUploader.BucketSstData(localSstDir, metas));
        uploader.upload(snapshotId, bucketSstMap);

        // Download to a different local directory
        String downloadDir = tempDir.resolve("download").toString();
        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        List<String> localPaths = downloader.downloadBucketSst(snapshotId, null, 0, downloadDir);

        assertThat(localPaths).hasSize(1);

        // Ingest and verify
        dvRocksDB.rowPosIndex().ingestExternalFile(localPaths);
        assertThat(dvRocksDB.rowPosIndex().get(1L)).isEqualTo(new FilePos(1, 10L));
        assertThat(dvRocksDB.rowPosIndex().get(2L)).isEqualTo(new FilePos(1, 20L));
        assertThat(dvRocksDB.rowPosIndex().get(3L)).isEqualTo(new FilePos(2, 30L));
    }

    @Test
    void testMultipleBuckets() throws Exception {
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());

        // Bucket 0 SST
        String localSstDir0 = tempDir.resolve("local_sst_0").toString();
        new File(localSstDir0).mkdirs();
        List<RowPosSstFileWriter.SstFileMeta> metas0;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir0)) {
            metas0 = writer.write(Arrays.asList(entry(1L, 1, 10L), entry(2L, 1, 20L)));
        }

        // Bucket 1 SST
        String localSstDir1 = tempDir.resolve("local_sst_1").toString();
        new File(localSstDir1).mkdirs();
        List<RowPosSstFileWriter.SstFileMeta> metas1;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir1)) {
            metas1 = writer.write(Collections.singletonList(entry(100L, 5, 500L)));
        }

        // Upload both buckets
        long snapshotId = 200L;
        RowPosSstUploader uploader = new RowPosSstUploader(remoteDir);
        Map<Integer, RowPosSstUploader.BucketSstData> bucketSstMap = new HashMap<>();
        bucketSstMap.put(0, new RowPosSstUploader.BucketSstData(localSstDir0, metas0));
        bucketSstMap.put(1, new RowPosSstUploader.BucketSstData(localSstDir1, metas1));
        uploader.upload(snapshotId, bucketSstMap);

        // Download and verify bucket 0
        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        String downloadDir0 = tempDir.resolve("download_0").toString();
        List<String> paths0 = downloader.downloadBucketSst(snapshotId, null, 0, downloadDir0);
        assertThat(paths0).hasSize(1);

        dvRocksDB.rowPosIndex().ingestExternalFile(paths0);
        assertThat(dvRocksDB.rowPosIndex().get(1L)).isEqualTo(new FilePos(1, 10L));
        assertThat(dvRocksDB.rowPosIndex().get(2L)).isEqualTo(new FilePos(1, 20L));

        // Download and verify bucket 1
        String downloadDir1 = tempDir.resolve("download_1").toString();
        List<String> paths1 = downloader.downloadBucketSst(snapshotId, null, 1, downloadDir1);
        assertThat(paths1).hasSize(1);

        dvRocksDB.rowPosIndex().ingestExternalFile(paths1);
        assertThat(dvRocksDB.rowPosIndex().get(100L)).isEqualTo(new FilePos(5, 500L));
    }

    @Test
    void testBucketWithNoSst() throws Exception {
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());

        // Upload with only bucket 0
        String localSstDir = tempDir.resolve("local_sst").toString();
        new File(localSstDir).mkdirs();
        List<RowPosSstFileWriter.SstFileMeta> metas;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir)) {
            metas = writer.write(Collections.singletonList(entry(1L, 1, 10L)));
        }

        long snapshotId = 300L;
        RowPosSstUploader uploader = new RowPosSstUploader(remoteDir);
        Map<Integer, RowPosSstUploader.BucketSstData> bucketSstMap = new HashMap<>();
        bucketSstMap.put(0, new RowPosSstUploader.BucketSstData(localSstDir, metas));
        uploader.upload(snapshotId, bucketSstMap);

        // Try to download non-existing bucket 99
        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        String downloadDir = tempDir.resolve("download").toString();
        List<String> paths = downloader.downloadBucketSst(snapshotId, null, 99, downloadDir);
        assertThat(paths).isEmpty();
    }

    @Test
    void testIndexReadAfterUpload() throws Exception {
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());

        // Upload with buckets 0 and 2
        String localSstDir0 = tempDir.resolve("local_sst_0").toString();
        new File(localSstDir0).mkdirs();
        List<RowPosSstFileWriter.SstFileMeta> metas0;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir0)) {
            metas0 = writer.write(Collections.singletonList(entry(1L, 1, 10L)));
        }

        String localSstDir2 = tempDir.resolve("local_sst_2").toString();
        new File(localSstDir2).mkdirs();
        List<RowPosSstFileWriter.SstFileMeta> metas2;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir2)) {
            metas2 = writer.write(Arrays.asList(entry(10L, 2, 100L), entry(20L, 2, 200L)));
        }

        long snapshotId = 400L;
        RowPosSstUploader uploader = new RowPosSstUploader(remoteDir);
        Map<Integer, RowPosSstUploader.BucketSstData> bucketSstMap = new HashMap<>();
        bucketSstMap.put(0, new RowPosSstUploader.BucketSstData(localSstDir0, metas0));
        bucketSstMap.put(2, new RowPosSstUploader.BucketSstData(localSstDir2, metas2));
        uploader.upload(snapshotId, bucketSstMap);

        // Read index directly
        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        RowPosSstIndex index = downloader.readIndex(snapshotId, null);

        assertThat(index.getBucketIds()).containsExactlyInAnyOrder(0, 2);
        assertThat(index.getFiles(0)).hasSize(1);
        assertThat(index.getFiles(0).get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(index.getFiles(2)).hasSize(1);
        assertThat(index.getFiles(2).get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(index.getFiles(1)).isEmpty();
    }

    /** Helper to create a RowPosEntry using the fluss-common FilePos. */
    private static RowPosSstFileWriter.RowPosEntry entry(long rowId, int fileId, long rowPosition) {
        return new RowPosSstFileWriter.RowPosEntry(
                rowId, new org.apache.fluss.lake.dv.FilePos(fileId, rowPosition));
    }
}
