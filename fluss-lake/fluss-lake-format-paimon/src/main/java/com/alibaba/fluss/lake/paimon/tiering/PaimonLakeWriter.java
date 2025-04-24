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

import com.alibaba.fluss.lake.paimon.tiering.append.AppendOnlyWriter;
import com.alibaba.fluss.lake.paimon.tiering.mergetree.MergeTreeWriter;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecord;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;

import java.io.IOException;

/** Implementation of {@link LakeWriter} for Paimon. */
public class PaimonLakeWriter implements LakeWriter<PaimonWriteResult> {

    private final Catalog paimonCatalog;
    private final RecordWriter recordWriter;
    private final BinaryRow partition;
    private final int bucket;

    public PaimonLakeWriter(
            PaimonCatalogProvider paimonCatalogProvider,
            TablePath tablePath,
            BinaryRow partition,
            int bucket)
            throws IOException {
        this.paimonCatalog = paimonCatalogProvider.getCatalog();
        FileStoreTable table = getTable(tablePath);
        this.recordWriter =
                table.primaryKeys().isEmpty()
                        ? new AppendOnlyWriter(table)
                        : new MergeTreeWriter(table);
        this.partition = partition;
        this.bucket = bucket;
    }

    @Override
    public void write(LogRecord record) throws IOException {
        try {
            recordWriter.write(partition, bucket, record);
        } catch (Exception e) {
            throw new IOException("Fail to write Fluss record into Paimon.");
        }
    }

    @Override
    public PaimonWriteResult complete() throws IOException {
        try {
            // prepare commit
            CommitMessage commitMessage = recordWriter.complete();
            return new PaimonWriteResult(commitMessage);
        } catch (Exception e) {
            throw new IOException("Fail to complete the paimon writer.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            paimonCatalog.close();
            recordWriter.close();
        } catch (Exception e) {
            throw new IOException("Fail to close Paimon RecordWriter.");
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
            try {
                paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
                paimonCatalog.createTable(
                        Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName()),
                        Schema.newBuilder()
                                .column("c1", DataTypes.INT())
                                .column("c2", DataTypes.INT())
                                .build(),
                        true);
                table =
                        (FileStoreTable)
                                paimonCatalog.getTable(
                                        Identifier.create(
                                                tablePath.getDatabaseName(),
                                                tablePath.getTableName()));
            } catch (Exception e1) {
                throw new IOException("The table  " + tablePath + " doesn't exist in Paimon", e1);
            }
            //            throw new IOException("The table  " + tablePath + " doesn't exist in
            // Paimon", e);
        }
        return table;
    }
}
