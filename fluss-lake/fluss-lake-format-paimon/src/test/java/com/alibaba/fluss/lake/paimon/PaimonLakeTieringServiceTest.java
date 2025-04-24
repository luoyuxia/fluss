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

package com.alibaba.fluss.lake.paimon;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.local.LocalFileSystem;
import com.alibaba.fluss.lake.paimon.tiering.PaimonLakeTieringFactory;
import com.alibaba.fluss.lake.paimon.tiering.PaimonWriteResult;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.writer.FileSystemProvider;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.lakehouse.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.row.GenericRow;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class PaimonLakeTieringServiceTest {

    @Test
    void testTieringToPaimon(@TempDir Path paimonTempDir) throws Exception {
        Map<String, String> paimonConf = new HashMap<>();
        paimonConf.put("metastore", "filesystem");
        paimonConf.put("warehouse", paimonTempDir.toString());

        Catalog catalog =
                CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(paimonConf)));
        String db = "db";
        String table = "table1";

        catalog.createDatabase(db, true);
        Identifier identifier = Identifier.create(db, table);

        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .option(CoreOptions.BUCKET_KEY.key(), "a")
                        .option(CoreOptions.BUCKET.key(), "3")
                        .build(),
                true);

        Function<URI, FileSystem> fsProvider = uri -> LocalFileSystem.getSharedInstance();

        PaimonLakeTieringFactory paimonLakeTieringFactory =
                new PaimonLakeTieringFactory(
                        Configuration.fromMap(paimonConf),
                        new FileSystemProvider() {
                            @Override
                            public FileSystem getFileSystem(URI uri) {
                                return LocalFileSystem.getSharedInstance();
                            }
                        });
        LakeWriter<PaimonWriteResult> lakeWriter =
                paimonLakeTieringFactory.createLakeWriter(
                        new WriterInitContext() {
                            @Override
                            public TablePath tablePath() {
                                return TablePath.of(db, table);
                            }

                            @Override
                            public TableBucket tableBucket() {
                                return new TableBucket(1, 0);
                            }

                            @Nullable
                            @Override
                            public String partition() {
                                return null;
                            }
                        });

        LakeCommitter<PaimonWriteResult, ManifestCommittable> paimonLakeCommitter =
                paimonLakeTieringFactory.createLakeCommitter(() -> TablePath.of(db, table));

        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 1);
        genericRow.setField(1, 2);
        GenericRecord genericRecord =
                new GenericRecord(
                        1L, System.currentTimeMillis(), ChangeType.APPEND_ONLY, genericRow);
        lakeWriter.write(genericRecord);

        PaimonWriteResult paimonWriteResult = lakeWriter.complete();

        ManifestCommittable committable =
                paimonLakeCommitter.toCommitable(Collections.singletonList(paimonWriteResult));
        paimonLakeCommitter.commit(committable);

        Table table1 = catalog.getTable(identifier);
        RecordReader<InternalRow> reader =
                table1.newReadBuilder()
                        .newRead()
                        .createReader(table1.newReadBuilder().newScan().plan());

        reader.forEachRemaining(t -> System.out.println(t.getInt(0) + " " + t.getInt(1)));
    }

    @Test
    void testPkTieringToPaimon(@TempDir Path paimonTempDir) throws Exception {
        Map<String, String> paimonConf = new HashMap<>();
        paimonConf.put("metastore", "filesystem");
        paimonConf.put("warehouse", paimonTempDir.toString());

        Catalog catalog =
                CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(paimonConf)));
        String db = "db";
        String table = "table1";

        catalog.createDatabase(db, true);
        Identifier identifier = Identifier.create(db, table);

        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .primaryKey("a")
                        .option(CoreOptions.BUCKET_KEY.key(), "a")
                        .option(CoreOptions.BUCKET.key(), "3")
                        .build(),
                true);

        Function<URI, FileSystem> fsProvider = uri -> LocalFileSystem.getSharedInstance();

        PaimonLakeTieringFactory paimonLakeTieringFactory =
                new PaimonLakeTieringFactory(
                        Configuration.fromMap(paimonConf),
                        new FileSystemProvider() {
                            @Override
                            public FileSystem getFileSystem(URI uri) {
                                return LocalFileSystem.getSharedInstance();
                            }
                        });
        LakeWriter<PaimonWriteResult> lakeWriter =
                paimonLakeTieringFactory.createLakeWriter(
                        new WriterInitContext() {
                            @Override
                            public TablePath tablePath() {
                                return TablePath.of(db, table);
                            }

                            @Override
                            public TableBucket tableBucket() {
                                return new TableBucket(1, 0);
                            }

                            @Nullable
                            @Override
                            public String partition() {
                                return null;
                            }
                        });

        LakeCommitter<PaimonWriteResult, ManifestCommittable> paimonLakeCommitter =
                paimonLakeTieringFactory.createLakeCommitter(() -> TablePath.of(db, table));

        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 1);
        genericRow.setField(1, 6);
        GenericRecord genericRecord =
                new GenericRecord(1L, System.currentTimeMillis(), ChangeType.INSERT, genericRow);
        lakeWriter.write(genericRecord);
        lakeWriter.write(genericRecord);

        genericRow.setField(1, 5);
        genericRecord =
                new GenericRecord(1L, System.currentTimeMillis(), ChangeType.INSERT, genericRow);
        lakeWriter.write(genericRecord);

        PaimonWriteResult paimonWriteResult = lakeWriter.complete();

        ManifestCommittable committable =
                paimonLakeCommitter.toCommitable(Collections.singletonList(paimonWriteResult));
        paimonLakeCommitter.commit(committable);

        Table table1 = catalog.getTable(identifier);
        RecordReader<InternalRow> reader =
                table1.newReadBuilder()
                        .newRead()
                        .createReader(table1.newReadBuilder().newScan().plan());

        reader.forEachRemaining(t -> System.out.println(t.getInt(0) + " " + t.getInt(1)));

        List<ManifestEntry> manifestEntries =
                ((FileStoreTable) table1).store().newScan().plan().files();
        System.out.println(manifestEntries);
    }
}
