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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.lake.paimon.fs.FlussFileIoLoader;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lakehouse.writer.CommitterInitContext;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.lakehouse.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.function.Function;

/** Implementation of {@link LakeTieringFactory} for Paimon . */
public class PaimonLakeTieringFactory
        implements LakeTieringFactory<PaimonWriteResult, ManifestCommittable> {

    private final Catalog paimonCatalog;

    public PaimonLakeTieringFactory(
            Configuration paimonConfig, Function<URI, FileSystem> fsProvider) {
        this.paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                Options.fromMap(paimonConfig.toMap()),
                                new FlussFileIoLoader(fsProvider)));
    }

    @Override
    public LakeWriter<PaimonWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        FileStoreTable table = getTable(writerInitContext.tablePath());
        TableBucket tableBucket = writerInitContext.tableBucket();
        BinaryRow paimonPartition = toPaimonPartition(writerInitContext.partition());

        if (table.primaryKeys().isEmpty()) {
            // append only writer
        } else {
            // mergetree writer
        }

        //noinspection unchecked
        TableWriteImpl<InternalRow> writer =
                (TableWriteImpl<InternalRow>) table.newWrite("fluss_tiering_service");
        return new PaimonLakeWriter(writer, paimonPartition, tableBucket.getBucket());
    }

    private BinaryRow toPaimonPartition(@Nullable String partition) {
        if (partition == null) {
            return BinaryRow.EMPTY_ROW;
        }
        BinaryRow paimonPartition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(paimonPartition);
        writer.writeString(0, BinaryString.fromString(partition));
        writer.complete();
        return paimonPartition;
    }

    @Override
    public SimpleVersionedSerializer<PaimonWriteResult> getWriteResultSerializer() {
        return null;
    }

    @Override
    public LakeCommitter<PaimonWriteResult, ManifestCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        FileStoreTable table = getTable(committerInitContext.tablePath());
        return new PaimonCommitter(table);
    }

    @Override
    public SimpleVersionedSerializer<ManifestCommittable> getCommitableSerializer() {
        return null;
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
