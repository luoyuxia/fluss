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

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * DvManager is the unified entry point for all Deletion Vector operations. It manages the DV state
 * including RowPosIndex, LakeDv, LogDv, PendingDeletes, and FileDict.
 */
public class DvManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DvManager.class);

    // Concurrent control for DV operations
    // Write lock: changelog sync, position report, readable switch
    // Read lock: union read
    private final ReadWriteLock dvLock = new ReentrantReadWriteLock();

    // Core DV components
    private final DvRocksDB dvRocksDB;
    private final RowPosIndex rowPosIndex;
    private final LakeDv lakeDv;
    private final LogDv logDv;
    private final PendingDeletes pendingDeletes;
    private final FileDict fileDict;

    // Snapshot state
    @Nullable
    private Map<Integer, RoaringBitmap> snapshotBitmap; // LakeDv snapshot copy (in-memory)

    private long currentReadableSnapshotId = -1;
    private long currentReadableSnapshotTieredOffset = -1;

    /**
     * Creates a DvManager instance.
     *
     * @param dvRocksDBPath the path to the DV RocksDB directory
     * @throws IOException if initialization fails
     */
    public DvManager(File dvRocksDBPath) throws IOException {
        this.dvRocksDB = DvRocksDB.open(dvRocksDBPath);
        this.rowPosIndex = new RowPosIndex(dvRocksDB);
        this.lakeDv = new LakeDv(dvRocksDB);
        this.logDv = new LogDv(dvRocksDB);
        this.pendingDeletes = new PendingDeletes(dvRocksDB);
        this.fileDict = new FileDict(dvRocksDB);
    }

    /**
     * Handle changelog synced entries. For each old RowId from -U/-D records: 1. Check RowPosIndex
     * and pendingRowPos for the oldRowId 2. If found in either, mark the corresponding file
     * position as deleted in LakeDv 3. If not found in either, add to PendingDeletes 4. Mark the
     * oldRowId as deleted in LogDv
     *
     * @param deletedRowIds list of old RowIds from -U/-D changelog records
     */
    public void handleChangelogSynced(List<Long> deletedRowIds) {
        dvLock.writeLock().lock();
        try {
            for (long oldRowId : deletedRowIds) {
                // Step 1: Check RowPosIndex
                FilePos filePos = rowPosIndex.get(oldRowId);
                if (filePos != null) {
                    // Found in RowPosIndex: mark deleted in LakeDv, remove from RowPosIndex
                    lakeDv.markDeleted(filePos.getFileId(), filePos.getRowPosition());
                    rowPosIndex.delete(oldRowId);
                } else {
                    // Check pendingRowPos
                    FilePos pendingFilePos = rowPosIndex.getPending(oldRowId);
                    if (pendingFilePos != null) {
                        // Found in pendingRowPos: mark deleted in LakeDv, remove from pendingRowPos
                        lakeDv.markDeleted(
                                pendingFilePos.getFileId(), pendingFilePos.getRowPosition());
                        rowPosIndex.deletePending(oldRowId);
                    } else {
                        // Not found in either: add to PendingDeletes
                        pendingDeletes.add(oldRowId);
                    }
                }
                // Step 2: Mark in LogDv
                logDv.markDeleted(oldRowId);
            }
        } finally {
            dvLock.writeLock().unlock();
        }
    }

    /**
     * Handle position report from Tiering Writer. Updates RowPosIndex, LakeDv, and PendingDeletes
     * based on reported positions.
     *
     * @param positionReport file_path -> list of (rowId, rowPosition)
     * @param splitLastTieredOffset the last tiered offset before this split
     * @param splitLatestOffset the latest offset in this split
     * @param materializedDvFiles DV files that were materialized in this tiering round
     * @param actualSnapshotId the actual Iceberg snapshot ID after commit
     * @return true if the report was accepted, false if expired
     */
    public boolean handlePositionReport(
            Map<String, List<long[]>> positionReport,
            long splitLastTieredOffset,
            long splitLatestOffset,
            List<String> materializedDvFiles,
            long actualSnapshotId) {

        dvLock.writeLock().lock();
        try {
            if (splitLatestOffset <= currentReadableSnapshotTieredOffset) {
                return false;
            }

            if (currentReadableSnapshotId < 0) {
                handleInitialBuild(positionReport, actualSnapshotId, splitLatestOffset);
                return true;
            }

            for (Map.Entry<String, List<long[]>> entry : positionReport.entrySet()) {
                String filePath = entry.getKey();
                int fileId = fileDict.getOrCreateFileId(filePath);

                for (long[] rowPosition : entry.getValue()) {
                    long rowId = rowPosition[0];
                    int rowPos = (int) rowPosition[1];
                    FilePos filePos = FilePos.of(fileId, rowPos);

                    if (pendingDeletes.contains(rowId)) {
                        lakeDv.markDeleted(fileId, rowPos);
                    } else if (rowId > splitLastTieredOffset && rowId <= splitLatestOffset) {
                        rowPosIndex.putPending(rowId, filePos);
                    } else {
                        FilePos existingPos = rowPosIndex.get(rowId);
                        if (existingPos != null) {
                            rowPosIndex.putPending(rowId, filePos);
                        } else {
                            FilePos pendingPos = rowPosIndex.getPending(rowId);
                            if (pendingPos != null) {
                                rowPosIndex.putPending(rowId, filePos);
                            } else {
                                lakeDv.markDeleted(fileId, rowPos);
                            }
                        }
                    }
                }
            }

            if (snapshotBitmap != null && materializedDvFiles != null) {
                Map<Integer, RoaringBitmap> filteredSnapshot = new HashMap<>();
                for (String dvFile : materializedDvFiles) {
                    int fileId = fileDict.getOrCreateFileId(dvFile);
                    RoaringBitmap bitmap = snapshotBitmap.get(fileId);
                    if (bitmap != null) {
                        filteredSnapshot.put(fileId, bitmap);
                    }
                }
                snapshotBitmap = filteredSnapshot;
            }

            return true;
        } finally {
            dvLock.writeLock().unlock();
        }
    }

    /**
     * Handle readable switch notification. Atomically migrates pendingRowPos to RowPosIndex, cleans
     * up old files, clears PendingDeletes, and applies bitmap diff cleanup.
     *
     * @param newReadableSnapshotId the new DV-readable snapshot ID
     * @param newTieredOffset the tiered offset of the new readable snapshot
     * @param oldFileIds file IDs that exist in old readable snapshot but not in new one
     */
    public void handleReadableSwitch(
            long newReadableSnapshotId, long newTieredOffset, List<Integer> oldFileIds) {
        dvLock.writeLock().lock();
        try {
            // Step a: Migrate pendingRowPos to RowPosIndex
            rowPosIndex.migratePendingToIndex();

            // Step b: Clear pendingRowPos
            rowPosIndex.clearPending();

            // Step c: Delete LakeDv entries for old files
            if (oldFileIds != null) {
                for (int fileId : oldFileIds) {
                    lakeDv.deleteFile(fileId);
                }
            }

            // Step d: PendingDeletes cleanup
            pendingDeletes.cleanupRange(newTieredOffset);

            // Step e: Apply bitmap diff cleanup
            if (snapshotBitmap != null && !snapshotBitmap.isEmpty()) {
                lakeDv.applyBitmapDiff(snapshotBitmap);
                snapshotBitmap = null;
            }

            // Step f: Update current readable snapshot
            this.currentReadableSnapshotId = newReadableSnapshotId;
            this.currentReadableSnapshotTieredOffset = newTieredOffset;
        } finally {
            dvLock.writeLock().unlock();
        }
    }

    /**
     * Handle initial build after first tiering completion. Writes all reported positions directly
     * to RowPosIndex (not pendingRowPos). Sets the snapshot as the first DV-readable snapshot.
     *
     * @param positionReport file_path -> list of (rowId, rowPosition)
     * @param snapshotId the snapshot ID to set as first readable
     * @param tieredOffset the tiered offset of this snapshot
     */
    public void handleInitialBuild(
            Map<String, List<long[]>> positionReport, long snapshotId, long tieredOffset) {
        dvLock.writeLock().lock();
        try {
            // Write all positions directly to RowPosIndex
            for (Map.Entry<String, List<long[]>> entry : positionReport.entrySet()) {
                String filePath = entry.getKey();
                int fileId = fileDict.getOrCreateFileId(filePath);

                for (long[] rowPosition : entry.getValue()) {
                    long rowId = rowPosition[0];
                    int rowPos = (int) rowPosition[1];
                    FilePos filePos = FilePos.of(fileId, rowPos);
                    rowPosIndex.put(rowId, filePos);
                }
            }

            // Set as first DV-readable snapshot
            this.currentReadableSnapshotId = snapshotId;
            this.currentReadableSnapshotTieredOffset = tieredOffset;
        } finally {
            dvLock.writeLock().unlock();
        }
    }

    /**
     * Get DV data for union read for the requested readable snapshot.
     *
     * @param requestedSnapshotId the snapshot id seen by the caller
     * @param dataFiles the data files referenced by the readable snapshot
     * @param logEndOffset current log end offset of the tablet
     * @return union read DV result
     */
    public DvReadResult getDvForUnionRead(
            long requestedSnapshotId, List<String> dataFiles, long logEndOffset) {
        dvLock.readLock().lock();
        try {
            if (requestedSnapshotId != currentReadableSnapshotId) {
                return new DvReadResult(
                        new LinkedHashMap<>(),
                        new LinkedHashMap<>(),
                        logEndOffset,
                        true,
                        currentReadableSnapshotId);
            }

            Map<String, byte[]> lakeDvResult = new LinkedHashMap<>();
            for (String filePath : dataFiles) {
                Integer fileId = fileDict.getFileId(filePath);
                if (fileId == null) {
                    continue;
                }

                RoaringBitmap bitmap = lakeDv.getDeletedBitmap(fileId);
                if (bitmap.isEmpty()) {
                    continue;
                }

                bitmap.runOptimize();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                bitmap.serialize(dos);
                dos.flush();
                lakeDvResult.put(filePath, baos.toByteArray());
            }

            Map<Long, byte[]> logDvResult = new LinkedHashMap<>();
            if (logEndOffset > currentReadableSnapshotTieredOffset) {
                Map<Long, RoaringBitmap> deletedBitmaps =
                        logDv.getDeletedBitmap(currentReadableSnapshotTieredOffset, logEndOffset);
                for (Map.Entry<Long, RoaringBitmap> entry : deletedBitmaps.entrySet()) {
                    RoaringBitmap bitmap = entry.getValue();
                    if (bitmap.isEmpty()) {
                        continue;
                    }
                    bitmap.runOptimize();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    bitmap.serialize(dos);
                    dos.flush();
                    logDvResult.put(entry.getKey(), baos.toByteArray());
                }
            }

            return new DvReadResult(
                    lakeDvResult, logDvResult, logEndOffset, false, currentReadableSnapshotId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize union read DV data", e);
        } finally {
            dvLock.readLock().unlock();
        }
    }

    /**
     * Snapshot split-scoped LogDv for tiering log split generation.
     *
     * @return Map of base offset to serialized RoaringBitmap
     */
    public Map<Long, byte[]> snapshotLogDv(long startOffset, long endOffset) {
        dvLock.readLock().lock();
        try {
            Map<Long, byte[]> result = new LinkedHashMap<>();
            long normalizedStartOffset = Math.max(0L, startOffset);
            long normalizedEndOffset = Math.max(normalizedStartOffset, endOffset);
            if (normalizedStartOffset >= normalizedEndOffset) {
                return result;
            }

            Map<Long, RoaringBitmap> deletedBitmaps =
                    logDv.getDeletedBitmap(normalizedStartOffset, normalizedEndOffset);
            for (Map.Entry<Long, RoaringBitmap> entry : deletedBitmaps.entrySet()) {
                RoaringBitmap bitmap = entry.getValue();
                if (bitmap.isEmpty()) {
                    continue;
                }
                bitmap.runOptimize();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                bitmap.serialize(dos);
                dos.flush();
                result.put(entry.getKey(), baos.toByteArray());
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize LogDv snapshot", e);
        } finally {
            dvLock.readLock().unlock();
        }
    }

    /**
     * Snapshot LakeDv for tiering split generation. Takes a snapshot of current LakeDv, resolves
     * file_id to file_path via FileDict, and stores the snapshot bitmap in memory for later diff
     * cleanup.
     *
     * @return Map of file_path to serialized RoaringBitmap
     */
    public Map<String, byte[]> snapshotLakeDv() {
        dvLock.writeLock().lock();
        try {
            Map<Integer, RoaringBitmap> allEntries = lakeDv.getAllEntries();
            // Store snapshot bitmap for later diff cleanup
            this.snapshotBitmap = new HashMap<>();
            for (Map.Entry<Integer, RoaringBitmap> entry : allEntries.entrySet()) {
                this.snapshotBitmap.put(entry.getKey(), entry.getValue().clone());
            }
            // Convert file_id to file_path
            Map<String, byte[]> result = new HashMap<>();
            for (Map.Entry<Integer, RoaringBitmap> entry : allEntries.entrySet()) {
                String filePath = fileDict.getFilePath(entry.getKey());
                if (filePath != null) {
                    RoaringBitmap bitmap = entry.getValue();
                    bitmap.runOptimize();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    bitmap.serialize(dos);
                    dos.flush();
                    result.put(filePath, baos.toByteArray());
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize LakeDv snapshot", e);
        } finally {
            dvLock.writeLock().unlock();
        }
    }

    // ========== Getter methods ==========

    public ReadWriteLock getDvLock() {
        return dvLock;
    }

    public RowPosIndex getRowPosIndex() {
        return rowPosIndex;
    }

    public LakeDv getLakeDv() {
        return lakeDv;
    }

    public LogDv getLogDv() {
        return logDv;
    }

    public PendingDeletes getPendingDeletes() {
        return pendingDeletes;
    }

    public FileDict getFileDict() {
        return fileDict;
    }

    public long getCurrentReadableSnapshotId() {
        return currentReadableSnapshotId;
    }

    public long getCurrentReadableSnapshotTieredOffset() {
        return currentReadableSnapshotTieredOffset;
    }

    // ========== Setter methods ==========

    public void setSnapshotBitmap(@Nullable Map<Integer, RoaringBitmap> snapshotBitmap) {
        this.snapshotBitmap = snapshotBitmap;
    }

    public void setCurrentReadableSnapshotId(long snapshotId) {
        this.currentReadableSnapshotId = snapshotId;
    }

    public void setCurrentReadableSnapshotTieredOffset(long offset) {
        this.currentReadableSnapshotTieredOffset = offset;
    }

    @Override
    public void close() throws IOException {
        dvRocksDB.close();
    }

    /** Result of getDvForUnionRead. */
    public static class DvReadResult {
        private final Map<String, byte[]> lakeDv; // file_path -> serialized bitmap
        private final Map<Long, byte[]> logDv; // base_offset -> serialized bitmap
        private final long logEndOffset;
        private final boolean isStale;
        private final long currentReadableSnapshot;

        public DvReadResult(
                Map<String, byte[]> lakeDv,
                Map<Long, byte[]> logDv,
                long logEndOffset,
                boolean isStale,
                long currentReadableSnapshot) {
            this.lakeDv = lakeDv;
            this.logDv = logDv;
            this.logEndOffset = logEndOffset;
            this.isStale = isStale;
            this.currentReadableSnapshot = currentReadableSnapshot;
        }

        public Map<String, byte[]> getLakeDv() {
            return lakeDv;
        }

        public Map<Long, byte[]> getLogDv() {
            return logDv;
        }

        public long getLogEndOffset() {
            return logEndOffset;
        }

        public boolean isStale() {
            return isStale;
        }

        public long getCurrentReadableSnapshot() {
            return currentReadableSnapshot;
        }
    }
}
