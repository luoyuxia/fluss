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

import org.apache.fluss.flink.tiering.source.split.TieringBootstrapSplit;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonEncoding;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.rocksdb.Checkpoint;
import org.rocksdb.FlushOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Writes bootstrap SST files from lake snapshot data. Reads records from a lake source,
 * deduplicates them by primary key (keeping only the latest value), and produces RocksDB SST files.
 *
 * <p>Internally uses a temporary RocksDB instance as a disk-backed write buffer so that arbitrarily
 * large datasets can be processed without risking OOM. RocksDB handles deduplication (later puts
 * win), sorting, and memory management (memtable flushes to disk automatically).
 *
 * <p><b>Incremental upload optimization:</b> To overlap CPU work (reading + encoding records) with
 * I/O (uploading to remote storage), a background monitor thread periodically detects newly created
 * SST files via {@link RocksDB#getLiveFilesMetaData()} and uploads them asynchronously. L0 files
 * are skipped (high compaction probability); deeper levels are uploaded first (more stable). On
 * {@link #flush()}, a checkpoint is created and diffed against already-uploaded files to handle the
 * remaining uploads and clean up any compacted-away SSTs from remote storage.
 */
final class BootstrapSstWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(BootstrapSstWriter.class);

    /** Fixed snapshot ID used for bootstrap snapshots. */
    static final long BOOTSTRAP_SNAPSHOT_ID = 1L;

    /** Name of the snapshot metadata file, must match server-side CompletedSnapshotJsonSerde. */
    private static final String SNAPSHOT_METADATA_FILE_NAME = "_METADATA";

    /** Number of threads used for parallel file uploads. */
    private static final int UPLOAD_PARALLELISM = 4;

    /** Interval in milliseconds between background SST file monitoring checks. */
    private static final long MONITOR_INTERVAL_MS = 5_000;

    private final TableInfo tableInfo;
    private final TieringBootstrapSplit split;
    private final RowSerializer rowSerializer;
    private final KeyEncoder primaryKeyEncoder;
    private final short schemaId;

    /** Temporary RocksDB instance used as a disk-backed write buffer. */
    private final RocksDB tempDb;

    private final Options tempDbOptions;
    private final WriteOptions tempWriteOptions;
    private final Path tempDbDir;

    /** Remote shared directory for SST files, computed eagerly for incremental uploads. */
    @Nullable private final FsPath remoteKvSharedDir;

    /** Remote snapshot directory for metadata files. */
    @Nullable private final FsPath remoteSnapshotDir;

    /** Thread pool for parallel file uploads. */
    private final ExecutorService uploadExecutor;

    /** Single-thread scheduled executor for periodic SST monitoring. */
    private final ScheduledExecutorService monitorExecutor;

    /**
     * Tracks already-uploaded SST files: original SST filename to remote file info. Written by
     * monitor thread and flush(), read by flush(). Thread-safe.
     */
    private final Map<String, SnapshotFileInfo> uploadedSstFiles;

    /** Pending upload futures from the background monitor. */
    private final ConcurrentLinkedQueue<CompletableFuture<?>> pendingUploadFutures;

    /** Number of rows written (incremented on put, decremented on delete). */
    private long rowCount;

    BootstrapSstWriter(TableInfo tableInfo, TieringBootstrapSplit split) throws IOException {
        if (!tableInfo.hasPrimaryKey()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Bootstrap-upgrade SST generation requires primary-key table, but got %s.",
                            tableInfo.getTablePath()));
        }
        this.tableInfo = tableInfo;
        this.split = split;
        DataType[] rowFieldTypes =
                tableInfo.getSchema().getColumns().stream()
                        .map(Schema.Column::getDataType)
                        .toArray(DataType[]::new);
        this.rowSerializer = new RowSerializer(rowFieldTypes, BinaryRow.BinaryRowFormat.COMPACTED);
        this.primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        this.schemaId = (short) tableInfo.getSchemaId();

        // Open RocksDB with defaults (compaction enabled for natural SST management).
        RocksDB.loadLibrary();
        this.tempDbDir = Files.createTempDirectory("fluss-bootstrap-rocksdb-");
        this.tempDbOptions = new Options().setCreateIfMissing(true);
        this.tempWriteOptions = new WriteOptions().setDisableWAL(true);
        try {
            this.tempDb = RocksDB.open(tempDbOptions, tempDbDir.toString());
        } catch (RocksDBException e) {
            tempWriteOptions.close();
            tempDbOptions.close();
            FileUtils.deleteDirectoryQuietly(tempDbDir.toFile());
            throw new IOException("Failed to open temp RocksDB for bootstrap SST.", e);
        }

        // Compute remote paths eagerly so the background monitor knows where to upload.
        String remoteDataDir = split.getRemoteDataDir();
        if (remoteDataDir != null) {
            FsPath remoteKvDir = new FsPath(remoteDataDir + "/kv");
            PhysicalTablePath physicalPath =
                    PhysicalTablePath.of(split.getTablePath(), split.getPartitionName());
            TableBucket tableBucket = split.getTableBucket();
            FsPath remoteKvTabletDir =
                    FlussPaths.remoteKvTabletDir(remoteKvDir, physicalPath, tableBucket);
            this.remoteKvSharedDir = FlussPaths.remoteKvSharedDir(remoteKvTabletDir);
            this.remoteSnapshotDir =
                    FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, BOOTSTRAP_SNAPSHOT_ID);
        } else {
            this.remoteKvSharedDir = null;
            this.remoteSnapshotDir = null;
        }

        // Incremental upload infrastructure.
        this.uploadExecutor = Executors.newFixedThreadPool(UPLOAD_PARALLELISM);
        this.uploadedSstFiles = MapUtils.newConcurrentHashMap();
        this.pendingUploadFutures = new ConcurrentLinkedQueue<>();

        // Start background SST monitor.
        this.monitorExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> new Thread(r, "bootstrap-sst-monitor"));
        if (remoteKvSharedDir != null) {
            monitorExecutor.scheduleWithFixedDelay(
                    this::checkAndUploadNewSsts,
                    MONITOR_INTERVAL_MS,
                    MONITOR_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);
        }
    }

    void write(LogRecord record) throws IOException {
        byte[] keyBytes = primaryKeyEncoder.encodeKey(record.getRow());
        try {
            if (record.getChangeType() == ChangeType.DELETE) {
                tempDb.delete(tempWriteOptions, keyBytes);
                rowCount--;
            } else {
                BinaryRow binaryRow = rowSerializer.toBinaryRow(record.getRow());
                byte[] valueBytes = ValueEncoder.encodeValue(schemaId, binaryRow);
                tempDb.put(tempWriteOptions, keyBytes, valueBytes);
                rowCount++;
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to write record to temp RocksDB.", e);
        }
    }

    /**
     * Background monitor task: detects newly created SST files via {@link
     * RocksDB#getLiveFilesMetaData()} and uploads them asynchronously. L0 files are skipped (likely
     * to be compacted soon). Deeper levels are uploaded with higher priority (more stable).
     */
    private void checkAndUploadNewSsts() {
        try {
            List<LiveFileMetaData> liveFiles = tempDb.getLiveFilesMetaData();
            // Sort by level descending: deeper levels are more stable, upload first.
            liveFiles.sort(Comparator.comparingInt(LiveFileMetaData::level).reversed());

            for (LiveFileMetaData file : liveFiles) {
                // RocksDB may return filename with leading "/", e.g. "/000005.sst".
                String origName = file.fileName();
                if (origName.startsWith("/")) {
                    origName = origName.substring(1);
                }
                // Skip L0 (high compaction probability) and already-submitted files.
                if (file.level() == 0 || uploadedSstFiles.containsKey(origName)) {
                    continue;
                }
                submitSstUpload(origName, file.size());
            }
        } catch (Exception e) {
            LOG.warn("Background SST monitor check failed.", e);
        }
    }

    /** Submits an SST file for background upload to the remote shared/ directory. */
    private void submitSstUpload(String origSstName, long fileSize) {
        String uuidFileName = UUID.randomUUID() + ".sst";
        FsPath remotePath = new FsPath(remoteKvSharedDir, uuidFileName);
        Path localPath = tempDbDir.resolve(origSstName);

        // Record immediately to prevent duplicate submissions.
        uploadedSstFiles.put(
                origSstName, new SnapshotFileInfo(remotePath.toString(), uuidFileName, fileSize));

        CompletableFuture<?> future =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                uploadFile(localPath, remotePath);
                            } catch (IOException e) {
                                // Remove from map so flush() diff will re-upload from checkpoint.
                                uploadedSstFiles.remove(origSstName);
                                throw new RuntimeException(e);
                            }
                        },
                        uploadExecutor);
        pendingUploadFutures.add(future);
    }

    /**
     * Flushes the temp RocksDB, creates a checkpoint, and completes the snapshot upload by diffing
     * checkpoint SSTs against already-uploaded files.
     *
     * <p>The diff determines:
     *
     * <ul>
     *   <li><b>New SSTs</b>: in checkpoint but not yet uploaded (remaining L0 files and any missed
     *       files) - uploaded from the checkpoint directory.
     *   <li><b>Stale SSTs</b>: uploaded but no longer in checkpoint (compacted away) - deleted from
     *       remote storage.
     * </ul>
     *
     * <p>After all SST files are on remote, metadata files (MANIFEST, CURRENT, OPTIONS) and a
     * {@code _METADATA} JSON file are uploaded.
     *
     * @return the snapshot location path (e.g., {@code hdfs://.../{bucket}/snap-1}), or {@code
     *     null} if no data was written.
     */
    @Nullable
    String flush() throws IOException {
        // 1. Flush final memtable.
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            tempDb.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new IOException("Failed to flush temp RocksDB.", e);
        }

        // 2. Stop the background monitor.
        monitorExecutor.shutdown();

        // 3. Wait for pending background uploads (best-effort, failures handled at diff time).
        waitForPendingUploads();

        if (remoteKvSharedDir == null) {
            LOG.warn(
                    "Remote data dir is not configured; cannot upload bootstrap snapshot "
                            + "for table={}, bucket={}.",
                    tableInfo.getTablePath(),
                    split.getTableBucket());
            return null;
        }

        // 4. Create checkpoint for a consistent view of SST + metadata files.
        Path checkpointDir = Files.createTempDirectory("fluss-bootstrap-checkpoint-");
        try (Checkpoint checkpoint = Checkpoint.create(tempDb)) {
            checkpoint.createCheckpoint(checkpointDir.toString());
        } catch (RocksDBException e) {
            FileUtils.deleteDirectoryQuietly(checkpointDir.toFile());
            throw new IOException("Failed to create RocksDB checkpoint.", e);
        }

        try {
            // 5. Categorize checkpoint files into SST and metadata.
            Set<String> checkpointSstNames = new HashSet<>();
            List<Path> checkpointSstPaths = new ArrayList<>();
            List<Path> checkpointMetadataPaths = new ArrayList<>();
            try (Stream<Path> stream = Files.list(checkpointDir)) {
                stream.forEach(
                        p -> {
                            String name = p.getFileName().toString();
                            if (name.endsWith(".sst")) {
                                checkpointSstNames.add(name);
                                checkpointSstPaths.add(p);
                            } else {
                                checkpointMetadataPaths.add(p);
                            }
                        });
            }

            if (checkpointSstPaths.isEmpty()) {
                return null;
            }

            // 6. Diff: new SSTs (in checkpoint but not uploaded / size mismatch).
            List<Path> newSstPaths = new ArrayList<>();
            for (Path sstPath : checkpointSstPaths) {
                String name = sstPath.getFileName().toString();
                SnapshotFileInfo uploaded = uploadedSstFiles.get(name);
                if (uploaded == null) {
                    newSstPaths.add(sstPath);
                } else if (uploaded.fileSize != Files.size(sstPath)) {
                    // Size mismatch (partial upload from monitor): delete stale, re-upload.
                    deleteRemoteFileQuietly(new FsPath(uploaded.remotePath));
                    uploadedSstFiles.remove(name);
                    newSstPaths.add(sstPath);
                }
            }

            // 7. Diff: stale SSTs (uploaded but not in checkpoint, compacted away).
            List<SnapshotFileInfo> staleSsts = new ArrayList<>();
            for (Map.Entry<String, SnapshotFileInfo> entry : uploadedSstFiles.entrySet()) {
                if (!checkpointSstNames.contains(entry.getKey())) {
                    staleSsts.add(entry.getValue());
                }
            }

            // 8. Upload new SSTs from checkpoint dir (parallel).
            uploadNewSstFilesFromCheckpoint(newSstPaths);

            // 9. Delete stale remote SSTs (best-effort).
            deleteStaleRemoteSsts(staleSsts);

            // 10. Upload metadata files (MANIFEST, CURRENT, OPTIONS) from checkpoint.
            List<SnapshotFileInfo> privateFiles = uploadMetadataFiles(checkpointMetadataPaths);

            // 11. Collect all shared file info for _METADATA.
            List<SnapshotFileInfo> sharedFiles = new ArrayList<>();
            long totalSize = 0;
            for (String sstName : checkpointSstNames) {
                SnapshotFileInfo info = uploadedSstFiles.get(sstName);
                if (info != null) {
                    sharedFiles.add(info);
                    totalSize += info.fileSize;
                }
            }
            for (SnapshotFileInfo info : privateFiles) {
                totalSize += info.fileSize;
            }

            // 12. Write _METADATA JSON.
            TableBucket tableBucket = split.getTableBucket();
            byte[] metadataJson =
                    buildSnapshotMetadataJson(
                            tableBucket,
                            BOOTSTRAP_SNAPSHOT_ID,
                            remoteSnapshotDir,
                            sharedFiles,
                            privateFiles,
                            totalSize,
                            rowCount);
            FsPath metadataPath = new FsPath(remoteSnapshotDir, SNAPSHOT_METADATA_FILE_NAME);
            uploadBytes(metadataJson, metadataPath);

            LOG.info(
                    "Bootstrap snapshot uploaded: table={}, bucket={}, partition={}, "
                            + "sstCount={} (preUploaded={}, new={}), metadataCount={}, "
                            + "staleDeleted={}, totalBytes={}, snapshotPath={}.",
                    tableInfo.getTablePath(),
                    split.getTableBucket(),
                    split.getPartitionName(),
                    sharedFiles.size(),
                    sharedFiles.size() - newSstPaths.size(),
                    newSstPaths.size(),
                    privateFiles.size(),
                    staleSsts.size(),
                    totalSize,
                    remoteSnapshotDir);
            return remoteSnapshotDir.toString();
        } finally {
            FileUtils.deleteDirectoryQuietly(checkpointDir.toFile());
        }
    }

    /** Waits for all pending background uploads, logging failures without propagating them. */
    private void waitForPendingUploads() {
        List<CompletableFuture<?>> futures = new ArrayList<>(pendingUploadFutures);
        pendingUploadFutures.clear();
        if (futures.isEmpty()) {
            return;
        }
        try {
            FutureUtils.waitForAll(futures).get();
        } catch (ExecutionException e) {
            LOG.warn("Some background SST uploads failed; will re-upload at diff time.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for background SST uploads.", e);
        }
    }

    /** Uploads new SST files from the checkpoint directory in parallel. */
    private void uploadNewSstFilesFromCheckpoint(List<Path> newSstPaths) throws IOException {
        if (newSstPaths.isEmpty()) {
            return;
        }
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (Path sstPath : newSstPaths) {
            long size = Files.size(sstPath);
            String origName = sstPath.getFileName().toString();
            String uuidFileName = UUID.randomUUID() + ".sst";
            FsPath remotePath = new FsPath(remoteKvSharedDir, uuidFileName);
            uploadedSstFiles.put(
                    origName, new SnapshotFileInfo(remotePath.toString(), uuidFileName, size));
            futures.add(
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    uploadFile(sstPath, remotePath);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            },
                            uploadExecutor));
        }
        waitForAllOrThrow(futures, "Failed to upload new SST files from checkpoint.");
    }

    /** Uploads metadata files (MANIFEST, CURRENT, OPTIONS) from checkpoint in parallel. */
    private List<SnapshotFileInfo> uploadMetadataFiles(List<Path> metadataPaths)
            throws IOException {
        List<SnapshotFileInfo> results = new ArrayList<>();
        if (metadataPaths.isEmpty()) {
            return results;
        }
        List<CompletableFuture<SnapshotFileInfo>> futures = new ArrayList<>();
        for (Path metadataPath : metadataPaths) {
            long size = Files.size(metadataPath);
            String fileName = metadataPath.getFileName().toString();
            FsPath remotePath = new FsPath(remoteSnapshotDir, fileName);
            futures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    uploadFile(metadataPath, remotePath);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                return new SnapshotFileInfo(remotePath.toString(), fileName, size);
                            },
                            uploadExecutor));
        }
        try {
            FutureUtils.waitForAll(futures).get();
            for (CompletableFuture<SnapshotFileInfo> f : futures) {
                results.add(f.get());
            }
        } catch (ExecutionException e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            t = ExceptionUtils.stripException(t, RuntimeException.class);
            if (t instanceof IOException) {
                throw (IOException) t;
            }
            throw new IOException("Failed to upload metadata files.", t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while uploading metadata files.", e);
        }
        return results;
    }

    /** Deletes stale remote SST files that were compacted away (best-effort). */
    private void deleteStaleRemoteSsts(List<SnapshotFileInfo> staleSsts) {
        for (SnapshotFileInfo stale : staleSsts) {
            deleteRemoteFileQuietly(new FsPath(stale.remotePath));
        }
    }

    private static void deleteRemoteFileQuietly(FsPath remotePath) {
        try {
            remotePath.getFileSystem().delete(remotePath, false);
        } catch (IOException e) {
            LOG.warn("Failed to delete stale remote file: {}", remotePath, e);
        }
    }

    /** Waits for all futures to complete, throwing IOException on failure. */
    private static void waitForAllOrThrow(List<CompletableFuture<?>> futures, String errorMsg)
            throws IOException {
        try {
            FutureUtils.waitForAll(futures).get();
        } catch (ExecutionException e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            t = ExceptionUtils.stripException(t, RuntimeException.class);
            if (t instanceof IOException) {
                throw (IOException) t;
            }
            throw new IOException(errorMsg, t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted: " + errorMsg, e);
        }
    }

    @Override
    public void close() {
        monitorExecutor.shutdownNow();
        uploadExecutor.shutdownNow();
        tempDb.close();
        tempWriteOptions.close();
        tempDbOptions.close();
        FileUtils.deleteDirectoryQuietly(tempDbDir.toFile());
    }

    /** Uploads a local file to the specified remote path. */
    private static void uploadFile(Path localFile, FsPath remotePath) throws IOException {
        FileSystem remoteFs = remotePath.getFileSystem();
        try (FSDataOutputStream out = remoteFs.create(remotePath, FileSystem.WriteMode.OVERWRITE);
                java.io.InputStream in = Files.newInputStream(localFile)) {
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
        }
    }

    /** Uploads raw bytes to the specified remote path. */
    private static void uploadBytes(byte[] data, FsPath remotePath) throws IOException {
        FileSystem remoteFs = remotePath.getFileSystem();
        try (FSDataOutputStream out = remoteFs.create(remotePath, FileSystem.WriteMode.OVERWRITE)) {
            out.write(data);
        }
    }

    /**
     * Builds the _METADATA JSON in the same format as the server-side {@code
     * CompletedSnapshotJsonSerde}. This allows the coordinator to read the bootstrap snapshot using
     * the standard {@code CompletedSnapshotHandle.retrieveCompleteSnapshot()} path.
     */
    private static byte[] buildSnapshotMetadataJson(
            TableBucket tableBucket,
            long snapshotId,
            FsPath snapshotLocation,
            List<SnapshotFileInfo> sharedFiles,
            List<SnapshotFileInfo> privateFiles,
            long totalSize,
            long rowCount)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectMapper mapper = new ObjectMapper();
        try (JsonGenerator gen = mapper.createGenerator(bos, JsonEncoding.UTF8)) {
            gen.writeStartObject();
            gen.writeNumberField("version", 1);
            gen.writeNumberField("table_id", tableBucket.getTableId());
            if (tableBucket.getPartitionId() != null) {
                gen.writeNumberField("partition_id", tableBucket.getPartitionId());
            }
            gen.writeNumberField("bucket_id", tableBucket.getBucket());
            gen.writeNumberField("snapshot_id", snapshotId);
            gen.writeStringField("snapshot_location", snapshotLocation.toString());

            // kv_snapshot_handle
            gen.writeObjectFieldStart("kv_snapshot_handle");
            writeFileHandlesArray(gen, "shared_file_handles", sharedFiles);
            writeFileHandlesArray(gen, "private_file_handles", privateFiles);
            gen.writeNumberField("snapshot_incremental_size", totalSize);
            gen.writeEndObject();

            gen.writeNumberField("log_offset", 0L);
            if (rowCount > 0) {
                gen.writeNumberField("row_count", rowCount);
            }
            gen.writeEndObject();
        }
        return bos.toByteArray();
    }

    private static void writeFileHandlesArray(
            JsonGenerator gen, String fieldName, List<SnapshotFileInfo> files) throws IOException {
        gen.writeArrayFieldStart(fieldName);
        for (SnapshotFileInfo file : files) {
            gen.writeStartObject();
            gen.writeObjectFieldStart("kv_file_handle");
            gen.writeStringField("path", file.remotePath);
            gen.writeNumberField("size", file.fileSize);
            gen.writeEndObject();
            gen.writeStringField("local_path", file.localName);
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }

    /** Metadata for a file uploaded as part of the bootstrap snapshot. */
    private static final class SnapshotFileInfo {
        final String remotePath;
        final String localName;
        final long fileSize;

        SnapshotFileInfo(String remotePath, String localName, long fileSize) {
            this.remotePath = remotePath;
            this.localName = localName;
            this.fileSize = fileSize;
        }
    }
}
