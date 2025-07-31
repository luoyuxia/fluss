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

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import com.alibaba.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/** Implementation of {@link LakeCommitter} for Paimon. */
public class PaimonLakeCommitter implements LakeCommitter<PaimonWriteResult, PaimonCommittable> {

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;
    private FileStoreCommit fileStoreCommit;
    private final TablePath tablePath;
    private static final ThreadLocal<Long> currentCommitSnapshotId = new ThreadLocal<>();
    private static final String FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY = "fluss-offsets";

    private static final JsonFactory JACKSON_FACTORY = new JsonFactory();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private List<PaimonBucketOffset> bucketOffsets;

    public PaimonLakeCommitter(PaimonCatalogProvider paimonCatalogProvider, TablePath tablePath)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        this.fileStoreTable = getTable(tablePath);
        this.tablePath = tablePath;
    }

    @Override
    public PaimonCommittable toCommittable(List<PaimonWriteResult> paimonWriteResults)
            throws IOException {
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        for (PaimonWriteResult paimonWriteResult : paimonWriteResults) {
            committable.addFileCommittable(paimonWriteResult.commitMessage());
        }
        bucketOffsets =
                paimonWriteResults.stream()
                        .map(PaimonWriteResult::bucketOffset)
                        .collect(Collectors.toList());
        return new PaimonCommittable(committable);
    }

    @Override
    public long commit(PaimonCommittable committable, Map<String, String> properties)
            throws IOException {
        ManifestCommittable manifestCommittable = committable.manifestCommittable();
        properties.forEach(manifestCommittable::addProperty);

        try {
            fileStoreCommit =
                    fileStoreTable
                            .store()
                            .newCommit(FLUSS_LAKE_TIERING_COMMIT_USER, fileStoreTable);
            fileStoreCommit.commit(manifestCommittable, false);
            Long commitSnapshotId = currentCommitSnapshotId.get();
            currentCommitSnapshotId.remove();

            return checkNotNull(commitSnapshotId, "Paimon committed snapshot id must be non-null.");
        } catch (Throwable t) {
            if (fileStoreCommit != null) {
                // if any error happen while commit, abort the commit to clean committable
                fileStoreCommit.abort(manifestCommittable.fileCommittables());
            }
            throw new IOException(t);
        }
    }

    @Override
    public void abort(PaimonCommittable committable) throws IOException {
        fileStoreCommit =
                fileStoreTable.store().newCommit(FLUSS_LAKE_TIERING_COMMIT_USER, fileStoreTable);
        fileStoreCommit.abort(committable.manifestCommittable().fileCommittables());
    }

    @Nullable
    @Override
    public CommittedLakeSnapshot getMissingLakeSnapshot(@Nullable Long latestLakeSnapshotIdOfFluss)
            throws IOException {
        Snapshot latestLakeSnapshotOfLake =
                getCommittedLatestSnapshotOfLake(FLUSS_LAKE_TIERING_COMMIT_USER);
        if (latestLakeSnapshotOfLake == null) {
            return null;
        }

        // we get the latest snapshot committed by fluss,
        // but the latest snapshot is not greater than latestLakeSnapshotIdOfFluss, no any missing
        // snapshot, return directly
        if (latestLakeSnapshotIdOfFluss != null
                && latestLakeSnapshotOfLake.id() <= latestLakeSnapshotIdOfFluss) {
            return null;
        }

        CommittedLakeSnapshot committedLakeSnapshot =
                new CommittedLakeSnapshot(latestLakeSnapshotOfLake.id());

        if (latestLakeSnapshotOfLake.properties() == null) {
            throw new IOException("Failed to load committed lake snapshot properties from Paimon.");
        }
        String property =
                latestLakeSnapshotOfLake.properties().get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
        if (property != null) {
            for (JsonNode node : OBJECT_MAPPER.readTree(property)) {
                PaimonBucketOffset bucketOffset =
                        PaimonBucketOffsetJsonSerde.INSTANCE.deserialize(node);
                if (bucketOffset.getPartitionId() != null) {
                    committedLakeSnapshot.addPartitionBucket(
                            bucketOffset.getPartitionId(),
                            bucketOffset.getBucket(),
                            bucketOffset.getLogOffset());
                } else {
                    committedLakeSnapshot.addBucket(
                            bucketOffset.getBucket(), bucketOffset.getLogOffset());
                }
            }
        }
        return committedLakeSnapshot;
    }

    @Nullable
    private Snapshot getCommittedLatestSnapshotOfLake(String commitUser) throws IOException {
        // get the latest snapshot commited by fluss or latest commited id
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long userCommittedSnapshotIdOrLatestCommitId =
                fileStoreTable
                        .snapshotManager()
                        .pickOrLatest((snapshot -> snapshot.commitUser().equals(commitUser)));
        // no any snapshot, return null directly
        if (userCommittedSnapshotIdOrLatestCommitId == null) {
            return null;
        }

        // pick the snapshot
        Snapshot snapshot = snapshotManager.tryGetSnapshot(userCommittedSnapshotIdOrLatestCommitId);

        if (!snapshot.commitUser().equals(commitUser)) {
            // the snapshot is still not commited by Fluss, return directly
            return null;
        }
        return snapshot;
    }

    @Override
    public void close() throws Exception {
        try {
            if (fileStoreCommit != null) {
                fileStoreCommit.close();
            }
            if (paimonCatalog != null) {
                paimonCatalog.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close PaimonLakeCommitter.", e);
        }
    }

    private FileStoreTable getTable(TablePath tablePath) throws IOException {
        try {
            FileStoreTable table =
                    (FileStoreTable)
                            paimonCatalog
                                    .getTable(toPaimon(tablePath))
                                    .copy(
                                            Collections.singletonMap(
                                                    CoreOptions.COMMIT_CALLBACKS.key(),
                                                    PaimonLakeCommitter.PaimonCommitCallback.class
                                                            .getName()));

            return table;
        } catch (Exception e) {
            throw new IOException("Failed to get table " + tablePath + " in Paimon.", e);
        }
    }

    /** A {@link CommitCallback} to save paimon commit snapshot info. */
    public static class PaimonCommitCallback implements CommitCallback {

        @Override
        public void call(
                List<ManifestEntry> list, List<IndexManifestEntry> indexFiles, Snapshot snapshot) {
            currentCommitSnapshotId.set(snapshot.id());
        }

        @Override
        public void retry(ManifestCommittable manifestCommittable) {
            // do-nothing
        }

        @Override
        public void close() throws Exception {
            // do-nothing
        }
    }
}
