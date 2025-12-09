/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.zk.data.lake;

import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.FlussPaths;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.metrics.registry.MetricRegistry.LOG;

/** The helper to handle {@link LakeTable}. */
public class LakeTableHelper {

    private final ZooKeeperClient zkClient;
    private final String remoteDataDir;

    public LakeTableHelper(ZooKeeperClient zkClient, String remoteDataDir) {
        this.zkClient = zkClient;
        this.remoteDataDir = remoteDataDir;
    }

    /**
     * Upserts a lake table snapshot for the given table.
     *
     * <p>This method merges the new snapshot with the existing one (if any) and stores it (data in
     * remote file, the remote file path in ZK). It appends the new snapshot to the existing list of
     * snapshots. If tableReadableTableSnapshot is provided, it will update the readable offsets for
     * the corresponding snapshot and delete all snapshots before that snapshot.
     *
     * @param tableId the table ID
     * @param tablePath the table path
     * @param lakeTableSnapshot the new snapshot to upsert
     * @param tableReadableTableSnapshot the readable snapshot to update (nullable)
     * @param minSnapshotIdToKeep the minimum snapshot ID to keep, snapshots before this ID will be
     *     deleted (nullable)
     * @throws Exception if the operation fails
     */
    public void upsertLakeTable(
            long tableId,
            TablePath tablePath,
            LakeTableSnapshot lakeTableSnapshot,
            @Nullable LakeTableSnapshot tableReadableTableSnapshot,
            @Nullable Long minSnapshotIdToKeep)
            throws Exception {
        LakeTable previousLakeTable = zkClient.getLakeTable(tableId).orElse(null);
        if (previousLakeTable != null) {
            LakeTableSnapshot previousLatestLakeSnapshot =
                    previousLakeTable.getLatestTableSnapshot();
            LakeTableSnapshot previousLatestLakeReadableSnapshot =
                    previousLakeTable.getLatestReadableTableSnapshot();

            // lake latest tiered snapshot
            lakeTableSnapshot = mergeLakeTable(previousLatestLakeSnapshot, lakeTableSnapshot);

            // if readable snapshot id equals to tiered snapshot id,
            // set readable table snapshot to tiered snapshot
            if (tableReadableTableSnapshot != null) {
                if (tableReadableTableSnapshot.getSnapshotId()
                        == lakeTableSnapshot.getSnapshotId()) {
                    tableReadableTableSnapshot = lakeTableSnapshot;
                } else {
                    if (previousLatestLakeReadableSnapshot != null) {
                        // Merge with previous readable snapshot to preserve offsets for buckets
                        // that might not be in the new readable offsets
                        tableReadableTableSnapshot =
                                mergeLakeTable(
                                        previousLatestLakeReadableSnapshot,
                                        tableReadableTableSnapshot);
                    }
                }
            }
        }

        // store the lake table snapshot into a file (tiered offsets)
        FsPath lakeTableSnapshotFsPath =
                storeLakeTableSnapshot(tableId, tablePath, lakeTableSnapshot);

        LakeTable.LakeSnapshotMetadata newLakeSnapshotMetadata;
        if (tableReadableTableSnapshot == lakeTableSnapshot) {
            newLakeSnapshotMetadata =
                    new LakeTable.LakeSnapshotMetadata(
                            lakeTableSnapshot.getSnapshotId(),
                            lakeTableSnapshotFsPath,
                            lakeTableSnapshotFsPath);
        } else {
            newLakeSnapshotMetadata =
                    new LakeTable.LakeSnapshotMetadata(
                            lakeTableSnapshot.getSnapshotId(), lakeTableSnapshotFsPath, null);
        }

        // Get existing snapshot metadata list or create a new one
        List<LakeTable.LakeSnapshotMetadata> snapshotMetadataList =
                previousLakeTable == null || previousLakeTable.getLakeSnapshotMetadata() == null
                        ? new ArrayList<>()
                        : new ArrayList<>(previousLakeTable.getLakeSnapshotMetadata());

        // Append the new snapshot metadata
        snapshotMetadataList.add(newLakeSnapshotMetadata);

        // If tableReadableTableSnapshot is provided, update the corresponding snapshot's
        // readableOffsetsFilePath and delete older snapshots
        if (tableReadableTableSnapshot != null) {
            long readableSnapshotId = tableReadableTableSnapshot.getSnapshotId();
            // Store the readable snapshot to a file
            FsPath readableOffsetsFilePath =
                    storeLakeTableSnapshot(tableId, tablePath, tableReadableTableSnapshot);

            // Find the snapshot with matching snapshotId and update its readableOffsetsFilePath
            boolean found = false;
            for (int i = 0; i < snapshotMetadataList.size(); i++) {
                LakeTable.LakeSnapshotMetadata metadata = snapshotMetadataList.get(i);
                if (metadata.getSnapshotId() == readableSnapshotId) {
                    // Create a new metadata with updated readableOffsetsFilePath
                    LakeTable.LakeSnapshotMetadata updatedMetadata =
                            new LakeTable.LakeSnapshotMetadata(
                                    metadata.getSnapshotId(),
                                    metadata.getTieredOffsetsFilePath(),
                                    readableOffsetsFilePath);
                    snapshotMetadataList.set(i, updatedMetadata);
                    found = true;
                }
            }
            if (!found) {
                // shouldn't happened
                LOG.warn(
                        "Readable snapshot {} not found in existing snapshots for table {}",
                        readableSnapshotId,
                        tableId);
            }
        }

        // Delete snapshots before minSnapshotIdToKeep if provided
        if (minSnapshotIdToKeep != null) {
            // Use iterator to safely remove elements while iterating
            Iterator<LakeTable.LakeSnapshotMetadata> iterator = snapshotMetadataList.iterator();
            while (iterator.hasNext()) {
                LakeTable.LakeSnapshotMetadata metadata = iterator.next();
                if (metadata.getSnapshotId() >= minSnapshotIdToKeep) {
                    // All subsequent snapshots will have larger IDs, so we can stop here
                    break;
                }
                // This snapshot should be deleted
                LOG.info(
                        "Deleting snapshot {} for table {} (minSnapshotIdToKeep: {})",
                        metadata.getSnapshotId(),
                        tableId,
                        minSnapshotIdToKeep);
                // Discard the snapshot files
                metadata.discard();
                // Remove from the list using iterator (safe removal)
                iterator.remove();
            }
        }

        // Create new LakeTable with updated snapshot metadata list
        LakeTable lakeTable = new LakeTable(snapshotMetadataList);
        try {
            zkClient.upsertLakeTable(tableId, lakeTable, previousLakeTable != null);
        } catch (Exception e) {
            LOG.warn("Failed to upsert lake table snapshot to zk.", e);
            // discard the new lake snapshot metadata
            newLakeSnapshotMetadata.discard();
            // todo: discard new readable metadata
            throw e;
        }
    }

    private LakeTableSnapshot mergeLakeTable(
            LakeTableSnapshot previousLakeTableSnapshot, LakeTableSnapshot newLakeTableSnapshot) {
        // Merge current snapshot with previous one since the current snapshot request
        // may not carry all buckets for the table. It typically only carries buckets
        // that were written after the previous commit.

        // merge log end offsets, current will override the previous
        Map<TableBucket, Long> bucketLogEndOffset =
                new HashMap<>(previousLakeTableSnapshot.getBucketLogEndOffset());
        bucketLogEndOffset.putAll(newLakeTableSnapshot.getBucketLogEndOffset());

        return new LakeTableSnapshot(newLakeTableSnapshot.getSnapshotId(), bucketLogEndOffset);
    }

    private FsPath storeLakeTableSnapshot(
            long tableId, TablePath tablePath, LakeTableSnapshot lakeTableSnapshot)
            throws Exception {
        // get the remote file path to store the lake table snapshot information
        FsPath remoteLakeTableSnapshotManifestPath =
                FlussPaths.remoteLakeTableSnapshotManifestPath(remoteDataDir, tablePath, tableId);
        // check whether the parent directory exists, if not, create the directory
        FileSystem fileSystem = remoteLakeTableSnapshotManifestPath.getFileSystem();
        if (!fileSystem.exists(remoteLakeTableSnapshotManifestPath.getParent())) {
            fileSystem.mkdirs(remoteLakeTableSnapshotManifestPath.getParent());
        }
        // serialize table snapshot to json bytes, and write to file
        byte[] jsonBytes = LakeTableSnapshotJsonSerde.toJson(lakeTableSnapshot);
        try (FSDataOutputStream outputStream =
                fileSystem.create(
                        remoteLakeTableSnapshotManifestPath, FileSystem.WriteMode.OVERWRITE)) {
            outputStream.write(jsonBytes);
        }
        return remoteLakeTableSnapshotManifestPath;
    }
}
