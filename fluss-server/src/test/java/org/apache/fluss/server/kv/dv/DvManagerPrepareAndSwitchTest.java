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
import org.apache.fluss.lake.dv.RowPosSstUploader;
import org.apache.fluss.server.entity.DvPositionReportData;

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

/** Tests for {@link DvManager} Prepare and Readable Switch logic. */
class DvManagerPrepareAndSwitchTest {

    @TempDir Path tempDir;

    private DvRocksDB dvRocksDB;
    private DvRWLock dvRWLock;
    private DvManager dvManager;

    @BeforeEach
    void setUp() throws Exception {
        dvRocksDB = DvRocksDB.open(tempDir.resolve("dv").toString());
        dvRWLock = new DvRWLock();
        dvManager = new DvManager(dvRocksDB, dvRWLock);
    }

    @AfterEach
    void tearDown() {
        dvManager.close();
        dvRocksDB.close();
    }

    @Test
    void testPrepareWritesFileDictAndCachesPendingState() throws Exception {
        // Upload SST to simulate remote storage
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        uploadTestSst(
                remoteDir, 1L, 0, Arrays.asList(sstEntry(10L, 1, 100L), sstEntry(20L, 2, 200L)));

        // Pre-populate FileDict with an old file mapping (for oldFiles resolution)
        dvRocksDB.fileDict().put(99, "/old/file.parquet");

        Map<Integer, String> newFileDictEntries = new HashMap<>();
        newFileDictEntries.put(1, "/data/f1.parquet");
        newFileDictEntries.put(2, "/data/f2.parquet");
        DvPositionReportData.DvBucketOffset bucketOffset =
                new DvPositionReportData.DvBucketOffset(
                        500L, newFileDictEntries, Collections.singletonList("/old/file.parquet"));

        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        String localTempDir = tempDir.resolve("local_sst").toString();

        dvManager.handlePrepare(0, bucketOffset, downloader, 1L, localTempDir);

        // Verify FileDict entries written
        assertThat(dvRocksDB.fileDict().getFilePath(1)).isEqualTo("/data/f1.parquet");
        assertThat(dvRocksDB.fileDict().getFilePath(2)).isEqualTo("/data/f2.parquet");

        // Verify pending readable offset cached
        assertThat(dvManager.getPendingReadableOffset(0)).isEqualTo(500L);

        // Verify readableSnapshotId NOT yet updated (that's Switch's job)
        assertThat(dvManager.getReadableSnapshotId()).isEqualTo(-1L);
    }

    @Test
    void testPrepareIdempotency() throws Exception {
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        uploadTestSst(remoteDir, 1L, 0, Collections.singletonList(sstEntry(10L, 1, 100L)));

        Map<Integer, String> newFileDictEntries = Collections.singletonMap(1, "/data/f1.parquet");
        DvPositionReportData.DvBucketOffset bucketOffset =
                new DvPositionReportData.DvBucketOffset(
                        500L, newFileDictEntries, Collections.emptyList());

        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        String localTempDir = tempDir.resolve("local_sst").toString();

        // First prepare
        dvManager.handlePrepare(0, bucketOffset, downloader, 1L, localTempDir);

        // Second prepare with same snapshotId should skip (idempotent)
        dvManager.handlePrepare(0, bucketOffset, downloader, 1L, localTempDir);

        // FileDict should still have the entry
        assertThat(dvRocksDB.fileDict().getFilePath(1)).isEqualTo("/data/f1.parquet");
    }

    @Test
    void testReadableSwitchIngestAndResolve() throws Exception {
        // Setup: upload SST with RowPos data
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        uploadTestSst(
                remoteDir, 1L, 0, Arrays.asList(sstEntry(10L, 1, 100L), sstEntry(20L, 2, 200L)));

        // Simulate pending deletes from handleChangelogSynced
        dvRocksDB.pendingDeletes().putPending(10L); // rowId 10: pending, will be resolved
        dvRocksDB.pendingDeletes().putPending(20L); // rowId 20: pending, will be resolved
        dvRocksDB.pendingDeletes().putPending(5L); // rowId 5: < readableOffset, orphan

        Map<Integer, String> newFileDictEntries = new HashMap<>();
        newFileDictEntries.put(1, "/data/f1.parquet");
        newFileDictEntries.put(2, "/data/f2.parquet");
        DvPositionReportData.DvBucketOffset bucketOffset =
                new DvPositionReportData.DvBucketOffset(
                        15L, newFileDictEntries, Collections.emptyList());

        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        String localTempDir = tempDir.resolve("local_sst").toString();

        // Prepare
        dvManager.handlePrepare(0, bucketOffset, downloader, 1L, localTempDir);

        // Readable Switch
        dvManager.handleReadableSwitch(0, 1L, 15L);

        // Verify readableSnapshotId updated
        assertThat(dvManager.getReadableSnapshotId()).isEqualTo(1L);
        assertThat(dvManager.getSnapshotStartLogOffset()).isEqualTo(15L);

        // Verify rowId 10 resolved: RowPosIndex deleted, LakeDv marked, PendingDeletes resolved
        assertThat(dvRocksDB.rowPosIndex().get(10L)).isNull();
        assertThat(dvRocksDB.lakeDv().get(1).contains(100L)).isTrue();
        PendingDeletes.PendingDeleteEntry entry10 = dvRocksDB.pendingDeletes().get(10L);
        assertThat(entry10).isNotNull();
        assertThat(entry10.isPending()).isFalse();
        assertThat(entry10.getFilePos()).isEqualTo(new FilePos(1, 100L));

        // Verify rowId 5 deleted (orphan: 5 < 15)
        assertThat(dvRocksDB.pendingDeletes().get(5L)).isNull();

        // Verify rowId 20 kept (20 >= readableOffset 15, so RowPos hit → resolved)
        PendingDeletes.PendingDeleteEntry entry20 = dvRocksDB.pendingDeletes().get(20L);
        assertThat(entry20).isNotNull();
        assertThat(entry20.isPending()).isFalse();
        assertThat(entry20.getFilePos()).isEqualTo(new FilePos(2, 200L));

        // Verify pending readable offset consumed
        assertThat(dvManager.getPendingReadableOffset(0)).isEqualTo(-1L);
    }

    @Test
    void testReadableSwitchIdempotency() throws Exception {
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        uploadTestSst(remoteDir, 1L, 0, Collections.singletonList(sstEntry(10L, 1, 100L)));

        DvPositionReportData.DvBucketOffset bucketOffset =
                new DvPositionReportData.DvBucketOffset(
                        500L,
                        Collections.singletonMap(1, "/data/f1.parquet"),
                        Collections.emptyList());

        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        String localTempDir = tempDir.resolve("local_sst").toString();

        dvManager.handlePrepare(0, bucketOffset, downloader, 1L, localTempDir);
        dvManager.handleReadableSwitch(0, 1L, 500L);

        assertThat(dvManager.getReadableSnapshotId()).isEqualTo(1L);

        // Second switch with same snapshotId should skip (idempotent)
        dvManager.handleReadableSwitch(0, 1L, 500L);

        assertThat(dvManager.getReadableSnapshotId()).isEqualTo(1L);
    }

    @Test
    void testReadableSwitchCleansOldFiles() throws Exception {
        // Pre-populate LakeDv and PendingDeletes for old file
        FilePos oldFilePos = new FilePos(99, 10L);
        dvRocksDB.lakeDv().markDeleted(99, 10L);
        dvRocksDB.lakeDv().markDeleted(99, 20L);
        dvRocksDB.pendingDeletes().put(100L, oldFilePos);

        // Also add an old FileDict entry
        dvRocksDB.fileDict().put(99, "/old/file.parquet");

        // Upload SST
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        uploadTestSst(remoteDir, 1L, 0, Collections.singletonList(sstEntry(50L, 1, 500L)));

        Map<Integer, String> newFileDictEntries = Collections.singletonMap(1, "/data/f1.parquet");
        DvPositionReportData.DvBucketOffset bucketOffset =
                new DvPositionReportData.DvBucketOffset(
                        200L, newFileDictEntries, Collections.singletonList("/old/file.parquet"));

        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        String localTempDir = tempDir.resolve("local_sst").toString();

        dvManager.handlePrepare(0, bucketOffset, downloader, 1L, localTempDir);
        dvManager.handleReadableSwitch(0, 1L, 200L);

        // Old file LakeDv entry should be deleted
        assertThat(dvRocksDB.lakeDv().get(99)).isNull();

        // PendingDeletes entry pointing to old file should be deleted
        assertThat(dvRocksDB.pendingDeletes().get(100L)).isNull();

        // FileDict entries for old file (fileId=99) should be deleted
        assertThat(dvRocksDB.fileDict().getFilePath(99)).isNull();
        assertThat(dvRocksDB.fileDict().getFileId("/old/file.parquet")).isEqualTo(-1);

        // FileDict entries for new file (fileId=1) should be kept
        assertThat(dvRocksDB.fileDict().getFilePath(1)).isEqualTo("/data/f1.parquet");
    }

    @Test
    void testReadableSwitchCleansLogDv() throws Exception {
        // Pre-populate LogDv with entries below and above readableOffset
        dvRocksDB.logDv().markDeleted(5L);
        dvRocksDB.logDv().markDeleted(100L);
        dvRocksDB.logDv().markDeleted(2000L);

        // Upload empty SST (no bucket 0 data)
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        uploadTestSst(remoteDir, 1L, 0, Collections.singletonList(sstEntry(50L, 1, 500L)));

        DvPositionReportData.DvBucketOffset bucketOffset =
                new DvPositionReportData.DvBucketOffset(
                        1500L, Collections.emptyMap(), Collections.emptyList());

        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        String localTempDir = tempDir.resolve("local_sst").toString();

        dvManager.handlePrepare(0, bucketOffset, downloader, 1L, localTempDir);
        dvManager.handleReadableSwitch(0, 1L, 1500L);

        // LogDv entries below readableOffset cleaned (range-based)
        // 5 is in range [0, 1024), range end 1024 <= 1500, so cleaned
        assertThat(dvRocksDB.logDv().isDeleted(5L)).isFalse();
        assertThat(dvRocksDB.logDv().isDeleted(100L)).isFalse();

        // 2000 is in range [1024, 2048), range end 2048 > 1500, so NOT cleaned
        assertThat(dvRocksDB.logDv().isDeleted(2000L)).isTrue();
    }

    @Test
    void testEndToEndChangelogThenPrepareAndSwitch() throws Exception {
        // Step 1: Simulate write path producing pending deletes
        // rowId 10 and 20 have no RowPosIndex → pending
        dvManager.handleChangelogSynced(Arrays.asList(new DvEntry(10L), new DvEntry(20L)));

        // Verify pending
        assertThat(dvRocksDB.pendingDeletes().get(10L).isPending()).isTrue();
        assertThat(dvRocksDB.pendingDeletes().get(20L).isPending()).isTrue();

        // Step 2: Prepare — SST with RowPos for rowId 10 (but not 20)
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        uploadTestSst(remoteDir, 1L, 0, Collections.singletonList(sstEntry(10L, 1, 100L)));

        DvPositionReportData.DvBucketOffset bucketOffset =
                new DvPositionReportData.DvBucketOffset(
                        15L,
                        Collections.singletonMap(1, "/data/f1.parquet"),
                        Collections.emptyList());

        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        dvManager.handlePrepare(
                0, bucketOffset, downloader, 1L, tempDir.resolve("local_sst").toString());

        // Step 3: Readable Switch
        dvManager.handleReadableSwitch(0, 1L, 15L);

        // rowId 10: resolved (SST hit)
        PendingDeletes.PendingDeleteEntry entry10 = dvRocksDB.pendingDeletes().get(10L);
        assertThat(entry10.isPending()).isFalse();
        assertThat(entry10.getFilePos()).isEqualTo(new FilePos(1, 100L));
        assertThat(dvRocksDB.lakeDv().get(1).contains(100L)).isTrue();

        // rowId 20: still pending (no SST data, rowId 20 >= readableOffset 15)
        PendingDeletes.PendingDeleteEntry entry20 = dvRocksDB.pendingDeletes().get(20L);
        assertThat(entry20.isPending()).isTrue();

        // LogDv marks still present (cleanup offset 15 is in range [0,1024))
        assertThat(dvRocksDB.logDv().isDeleted(10L)).isTrue();
        assertThat(dvRocksDB.logDv().isDeleted(20L)).isTrue();
    }

    @Test
    void testEmptySstPrepareAndSwitch() throws Exception {
        // Upload SST with data only for bucket 1, but we'll prepare bucket 0
        FsPath remoteDir = new FsPath(tempDir.resolve("remote").toUri());
        uploadTestSst(remoteDir, 1L, 1, Collections.singletonList(sstEntry(10L, 1, 100L)));

        DvPositionReportData.DvBucketOffset bucketOffset =
                new DvPositionReportData.DvBucketOffset(
                        500L, Collections.emptyMap(), Collections.emptyList());

        RowPosSstDownloader downloader = new RowPosSstDownloader(remoteDir);
        dvManager.handlePrepare(
                0, bucketOffset, downloader, 1L, tempDir.resolve("local_sst").toString());

        // Switch with no SST and no PendingDeletes — pure state update
        dvManager.handleReadableSwitch(0, 1L, 500L);

        assertThat(dvManager.getReadableSnapshotId()).isEqualTo(1L);
        assertThat(dvManager.getSnapshotStartLogOffset()).isEqualTo(500L);
    }

    // ---- Helpers ----

    private void uploadTestSst(
            FsPath remoteDir,
            long snapshotId,
            int bucketId,
            List<RowPosSstFileWriter.RowPosEntry> entries)
            throws Exception {
        String localSstDir = tempDir.resolve("gen_sst_" + bucketId).toString();
        new File(localSstDir).mkdirs();

        List<RowPosSstFileWriter.SstFileMeta> metas;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir)) {
            metas = writer.write(entries);
        }

        RowPosSstUploader uploader = new RowPosSstUploader(remoteDir);
        Map<Integer, RowPosSstUploader.BucketSstData> bucketSstMap = new HashMap<>();
        bucketSstMap.put(bucketId, new RowPosSstUploader.BucketSstData(localSstDir, metas));
        uploader.upload(snapshotId, bucketSstMap);
    }

    /** Helper to create a RowPosEntry using the fluss-common FilePos. */
    private static RowPosSstFileWriter.RowPosEntry sstEntry(
            long rowId, int fileId, long rowPosition) {
        return new RowPosSstFileWriter.RowPosEntry(
                rowId, new org.apache.fluss.lake.dv.FilePos(fileId, rowPosition));
    }
}
