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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableCommitImpl;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;

/** Implementation of {@link LakeCommitter} for Paimon. */
public class PaimonCommitter implements LakeCommitter<PaimonWriteResult, ManifestCommittable> {

    private final Catalog paimonCatalog;
    private final FileStoreTable fileStoreTable;

    private TableCommitImpl tableCommit;

    public PaimonCommitter(PaimonCatalogProvider paimonCatalogProvider, TablePath tablePath)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.getCatalog();
        this.fileStoreTable = getTable(tablePath);
    }

    @Override
    public ManifestCommittable toCommitable(List<PaimonWriteResult> paimonWriteResults)
            throws IOException {
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        for (PaimonWriteResult paimonWriteResult : paimonWriteResults) {
            paimonWriteResult.getCommitMessage().ifPresent(committable::addFileCommittable);
        }
        return committable;
    }

    @Override
    public void commit(ManifestCommittable committable) {
        tableCommit = fileStoreTable.newCommit("fluss_tiering_service");
        tableCommit.commit(committable);
    }

    @Override
    public void close() throws Exception {
        if (tableCommit != null) {
            tableCommit.close();
        }
    }

    private FileStoreTable getTable(TablePath tablePath) throws IOException {
        FileStoreTable table;
        try {
            table =
                    (FileStoreTable)
                            paimonCatalog.getTable(
                                    Identifier.create(
                                            tablePath.getDatabaseName(), tablePath.getTableName()));
        } catch (Catalog.TableNotExistException e) {
            throw new IOException("The table  " + tablePath + " doesn't exist in Paimon", e);
        }
        return table;
    }
}
