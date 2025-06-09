/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.lakehouse.committer.CommittedOffsets;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.metadata.TablePath;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static com.alibaba.fluss.metadata.ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/** Implementation of {@link LakeCommitter} for Paimon. */
public class PaimonLakeCommitter implements LakeCommitter<PaimonWriteResult, PaimonCommittable> {

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;
    private FileStoreCommit fileStoreCommit;

    public PaimonLakeCommitter(PaimonCatalogProvider paimonCatalogProvider, TablePath tablePath)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.get();
        this.fileStoreTable = getTable(tablePath);
    }

    @Override
    public PaimonCommittable toCommitable(List<PaimonWriteResult> paimonWriteResults)
            throws IOException {
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        for (PaimonWriteResult paimonWriteResult : paimonWriteResults) {
            committable.addFileCommittable(paimonWriteResult.commitMessage());
        }
        return new PaimonCommittable(committable);
    }

    @Override
    public long commit(PaimonCommittable committable) throws IOException {
        ManifestCommittable manifestCommittable = committable.manifestCommittable();
        PaimonCommitCallback paimonCommitCallback = new PaimonCommitCallback();
        try {
            fileStoreCommit =
                    fileStoreTable
                            .store()
                            .newCommit(
                                    FLUSS_LAKE_TIERING_COMMIT_USER,
                                    Collections.singletonList(paimonCommitCallback));
            fileStoreCommit.commit(manifestCommittable, Collections.emptyMap());
            return checkNotNull(
                    paimonCommitCallback.commitSnapshotId,
                    "Paimon committed snapshot id must be non-null.");
        } catch (Throwable t) {
            if (fileStoreCommit != null) {
                // if any error happen while commit, abort the commit to clean committable
                fileStoreCommit.abort(manifestCommittable.fileCommittables());
            }
            throw new IOException(t);
        }
    }

    @Nullable
    @Override
    public CommittedOffsets getMissingCommittedOffsets(@Nullable Long knownSnapshotId)
            throws IOException {
        // get the fluss committed snapshot id
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long flussCommittedSnapshotIdOrLatestCommitId =
                fileStoreTable
                        .snapshotManager()
                        .pickOrLatest(
                                (snapshot ->
                                        snapshot.commitUser()
                                                .equals(FLUSS_LAKE_TIERING_COMMIT_USER)));
        // no any snapshot
        if (flussCommittedSnapshotIdOrLatestCommitId == null) {
            return null;
        }

        Snapshot snapshot =
                snapshotManager.tryGetSnapshot(flussCommittedSnapshotIdOrLatestCommitId);
        if (!snapshot.commitUser().equals(FLUSS_LAKE_TIERING_COMMIT_USER)) {
            // the snapshot is still not commited by Fluss
            return null;
        }

        // then it should be commit by Fluss

        // but the latest snapshot if not greater than knownSnapshotId, no any missing
        // snapshot, return directly
        if (knownSnapshotId != null && snapshot.id() <= knownSnapshotId) {
            return null;
        }

        CommittedOffsets committedOffsets = new CommittedOffsets(snapshot.id());
        ScanMode scanMode =
                fileStoreTable.primaryKeys().isEmpty() ? ScanMode.DELTA : ScanMode.CHANGELOG;

        Iterator<ManifestEntry> manifestEntryIterator =
                fileStoreTable
                        .store()
                        .newScan()
                        .withSnapshot(snapshot.id())
                        .withKind(scanMode)
                        .readFileIterator();
        while (manifestEntryIterator.hasNext()) {
            updateCommittedOffsets(committedOffsets, manifestEntryIterator.next());
        }
        return committedOffsets;
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
            throw new IOException("Fail to close PaimonLakeCommitter.", e);
        }
    }

    private FileStoreTable getTable(TablePath tablePath) throws IOException {
        try {
            return (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
        } catch (Exception e) {
            throw new IOException("Fail to get table " + tablePath + " in Paimon.");
        }
    }

    private static class PaimonCommitCallback implements CommitCallback {

        private Long commitSnapshotId = null;

        @Override
        public void call(List<ManifestEntry> list, Snapshot snapshot) {
            this.commitSnapshotId = snapshot.id();
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

    private void updateCommittedOffsets(
            CommittedOffsets committedOffsets, ManifestEntry manifestEntry) {
        // always get bucket, log_offset from statistic
        DataFileMeta dataFileMeta = manifestEntry.file();
        BinaryRow maxStatisticRow = dataFileMeta.valueStats().maxValues();

        // the system columns orders are: __bucket, __offset, __timestampAdd commentMore actions
        int fieldCount = maxStatisticRow.getFieldCount();
        int bucketId = maxStatisticRow.getInt(fieldCount - 3);
        long offset = maxStatisticRow.getLong(fieldCount - 2);

        String partition = null;
        BinaryRow partitionRow = manifestEntry.partition();
        if (partitionRow.getFieldCount() > 0) {
            List<String> partitionFields = new ArrayList<>(partitionRow.getFieldCount());
            for (int i = 0; i < partitionRow.getFieldCount(); i++) {
                partitionFields.add(partitionRow.getString(i).toString());
            }
            partition = String.join(PARTITION_SPEC_SEPARATOR, partitionFields);
        }

        if (partition == null) {
            committedOffsets.addBucket(bucketId, offset);
        } else {
            committedOffsets.addPartitionBucket(partition, bucketId, offset);
        }
    }
}
