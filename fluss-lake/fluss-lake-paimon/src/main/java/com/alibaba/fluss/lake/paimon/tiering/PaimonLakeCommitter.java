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

import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.metadata.TablePath;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.alibaba.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;
import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/** Implementation of {@link LakeCommitter} for Paimon. */
public class PaimonLakeCommitter implements LakeCommitter<PaimonWriteResult, PaimonCommittable> {

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;
    private BatchTableCommit batchTableCommit;

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
    public void commit(PaimonCommittable committable) throws IOException {
        batchTableCommit = fileStoreTable.newCommit(FLUSS_LAKE_TIERING_COMMIT_USER);
        List<CommitMessage> commitMessages = committable.manifestCommittable().fileCommittables();
        batchTableCommit.commit(commitMessages);
    }

    public void getLatestKnownSnapshotAndOffset() throws Exception {
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        Long flussCommittedSnapshotIdOrLatestCommitId =
                fileStoreTable
                        .snapshotManager()
                        .pickOrLatest(
                                (snapshot ->
                                        snapshot.commitUser()
                                                .equals(FLUSS_LAKE_TIERING_COMMIT_USER)));
        if (flussCommittedSnapshotIdOrLatestCommitId == null) {
            return;
        }
        Snapshot snapshot =
                snapshotManager.tryGetSnapshot(flussCommittedSnapshotIdOrLatestCommitId);
        if (!snapshot.commitUser().equals(FLUSS_LAKE_TIERING_COMMIT_USER)) {
            // the snapshot is still not commited by Fluss
            return;
        }

        Map<CommittedBucket, Long> committedBucketOffset = new HashMap<>();
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
            ManifestEntry manifestEntry = manifestEntryIterator.next();
            CommittedBucketLogOffset committedBucketLogOffset =
                    CommittedBucketLogOffset.fromManifestEntry(manifestEntry);
            committedBucketOffset.put(
                    committedBucketLogOffset.committedBucket, committedBucketLogOffset.logOffset);
        }

        System.out.println(committedBucketOffset);
    }

    @Override
    public void close() throws Exception {
        try {
            if (batchTableCommit != null) {
                batchTableCommit.close();
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

    private static class CommittedBucket {
        @Nullable private final String partitionName;
        private final int bucketId;

        public CommittedBucket(@Nullable BinaryRow partition, int bucketId) {
            this.partitionName = toPartitionName(partition);
            this.bucketId = bucketId;
        }

        @Nullable
        private String toPartitionName(BinaryRow partition) {
            if (partition.getFieldCount() == 0) {
                return null;
            }
            // todo: support multiple partitions
            return partition.getString(0).toString();
        }

        @Override
        public String toString() {
            return "CommittedBucket{"
                    + "partitionName='"
                    + partitionName
                    + '\''
                    + ", bucketId="
                    + bucketId
                    + '}';
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof CommittedBucket)) {
                return false;
            }
            CommittedBucket that = (CommittedBucket) object;
            return bucketId == that.bucketId && Objects.equals(partitionName, that.partitionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionName, bucketId);
        }
    }

    private static class CommittedBucketLogOffset {
        private final CommittedBucket committedBucket;
        private final long logOffset;

        static CommittedBucketLogOffset fromManifestEntry(ManifestEntry manifestEntry) {
            // always get bucket, log_offset from status
            DataFileMeta dataFileMeta = manifestEntry.file();
            BinaryRow maxStatisticRow = dataFileMeta.valueStats().maxValues();

            // the system columns orders are: __bucket, __offset, __timestamp
            int fieldCount = maxStatisticRow.getFieldCount();
            int bucketId = maxStatisticRow.getInt(fieldCount - 3);
            long offset = maxStatisticRow.getLong(fieldCount - 2);

            return new CommittedBucketLogOffset(
                    new CommittedBucket(manifestEntry.partition(), bucketId), offset);
        }

        private CommittedBucketLogOffset(CommittedBucket committedBucket, long logOffset) {
            this.committedBucket = committedBucket;
            this.logOffset = logOffset;
        }

        @Override
        public String toString() {
            return "CommittedBucketLogOffset{"
                    + "committedBucket="
                    + committedBucket
                    + ", logOffset="
                    + logOffset
                    + '}';
        }
    }
}
