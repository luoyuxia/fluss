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

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.iceberg.tiering.IcebergCatalogProvider;
import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.apache.fluss.lake.iceberg.utils.IcebergConversions.toIceberg;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test to verify compaction via {@link IcebergRewriteDataFiles}. */
class IcebergRewriteTest {
    private @TempDir File tempWarehouseDir;
    private Catalog icebergCatalog;

    @BeforeEach
    void setUp() {
        Configuration configuration = new Configuration();
        configuration.setString("warehouse", "file://" + tempWarehouseDir);
        configuration.setString("type", "hadoop");
        configuration.setString("name", "test");
        IcebergCatalogProvider provider = new IcebergCatalogProvider(configuration);
        icebergCatalog = provider.get();
    }

    @Test
    void testSingleBucketRewrite() throws Exception {
        TablePath tablePath = TablePath.of("iceberg", "compact_table");
        createTable(tablePath);
        Table icebergTable = icebergCatalog.loadTable(toIceberg(tablePath));
        int bucket = 0;
        appendTinyFilesWithRowsAndBucket(icebergTable, 2, 3, 1000, bucket);
        icebergTable.refresh();

        int filesBefore = countDataFiles(icebergTable);
        // We expect exactly 2 file for this no-op compaction scenario
        // since rewrite only happen when small files >= 3
        assertThat(filesBefore).isEqualTo(2);

        IcebergRewriteDataFiles icebergRewriteDataFiles =
                createIcebergRewriteDataFiles(icebergTable, bucket);

        // do rewrite, rewrite result should be null since no rewrite should happen
        RewriteDataFileResult rewriteDataFileResult = icebergRewriteDataFiles.execute();
        assertThat(rewriteDataFileResult).isNull();

        // append some files again, now, should rewrite
        appendTinyFilesWithRowsAndBucket(icebergTable, 10, 3, 2000, bucket);

        long rowsBefore = countRows(icebergTable);

        icebergRewriteDataFiles = createIcebergRewriteDataFiles(icebergTable, bucket);
        rewriteDataFileResult = icebergRewriteDataFiles.execute();

        // verify the rewrite result
        assertThat(rewriteDataFileResult).isNotNull();
        assertThat(rewriteDataFileResult.deletedDataFiles()).hasSize(12);
        assertThat(rewriteDataFileResult.addedDataFiles()).hasSize(1);

        // commit
        commitRewrite(icebergTable, rewriteDataFileResult);
        icebergTable.refresh();
        // only one file now
        assertThat(countDataFiles(icebergTable)).isEqualTo(1);

        // try compact again, should do nothing
        icebergRewriteDataFiles = createIcebergRewriteDataFiles(icebergTable, bucket);
        rewriteDataFileResult = icebergRewriteDataFiles.execute();
        assertThat(rewriteDataFileResult).isNull();

        // compact shouldn't change row counts
        long rowsAfter = countRows(icebergTable);
        assertThat(rowsAfter).isEqualTo(rowsBefore);
    }

    private IcebergRewriteDataFiles createIcebergRewriteDataFiles(Table table, int bucket) {
        table.refresh();
        return new IcebergRewriteDataFiles(table, null, new TableBucket(0, bucket), 1);
    }

    private void commitRewrite(Table table, RewriteDataFileResult rewriteDataFileResult) {
        table.refresh();
        RewriteFiles rewriteFiles = table.newRewrite();
        rewriteDataFileResult.deletedDataFiles().forEach(rewriteFiles::deleteFile);
        rewriteDataFileResult.addedDataFiles().forEach(rewriteFiles::addFile);
        rewriteFiles.commit();
    }

    private void createTable(TablePath tablePath) {
        Namespace namespace = Namespace.of(tablePath.getDatabaseName());
        SupportsNamespaces ns = (SupportsNamespaces) icebergCatalog;
        if (!ns.namespaceExists(namespace)) {
            ns.createNamespace(namespace);
        }

        Schema schema =
                new Schema(
                        Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
                        Types.NestedField.optional(2, "c2", Types.StringType.get()),
                        Types.NestedField.optional(3, "c3", Types.StringType.get()));

        TableIdentifier tableId =
                TableIdentifier.of(tablePath.getDatabaseName(), tablePath.getTableName());
        icebergCatalog.createTable(tableId, schema);
    }

    private static int countDataFiles(Table table) throws IOException {
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask ignored : tasks) {
                count++;
            }
        }
        return count;
    }

    private static long countRows(Table table) {
        long cnt = 0L;
        try (CloseableIterable<Record> it = IcebergGenerics.read(table).build()) {
            for (Record ignored : it) {
                cnt++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return cnt;
    }

    private static int countFilesForBucket(Table table, int bucket) throws IOException {
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().filter(Expressions.equal(BUCKET_COLUMN_NAME, bucket)).planFiles()) {
            for (FileScanTask ignored : tasks) {
                count++;
            }
        }
        return count;
    }

    private static void appendTinyFilesWithRowsAndBucket(
            Table table, int files, int rowsPerFile, int baseOffset, int bucket) throws Exception {
        DataFileSet toAppend = DataFileSet.create();
        for (int i = 0; i < files; i++) {
            toAppend.add(
                    writeTinyDataFile(table, rowsPerFile, baseOffset + (i * rowsPerFile), bucket));
        }
        AppendFiles append = table.newAppend();
        for (DataFile f : toAppend) {
            append.appendFile(f);
        }
        append.commit();
    }

    private static DataFile writeTinyDataFile(Table table, int rows, int startOffset, int bucket)
            throws Exception {
        try (TaskWriter<Record> taskWriter =
                TaskWriterFactory.createTaskWriter(table, null, bucket)) {
            for (int i = 0; i < rows; i++) {
                Record r = org.apache.iceberg.data.GenericRecord.create(table.schema());
                r.setField("c1", i);
                r.setField("c2", "v_" + i);
                r.setField("c3", "g");
                taskWriter.write(r);
            }
            DataFile[] dataFiles = taskWriter.dataFiles();
            checkState(dataFiles.length == 1);
            return dataFiles[0];
        }
    }
}
