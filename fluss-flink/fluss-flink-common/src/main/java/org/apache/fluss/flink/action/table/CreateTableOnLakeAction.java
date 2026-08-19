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

package org.apache.fluss.flink.action.table;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.action.Action;
import org.apache.fluss.flink.tiering.committer.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkFileIOLoader;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Action to create a Fluss table on an existing Paimon log table. */
@Internal
public class CreateTableOnLakeAction implements Action {

    private final Configuration flussConfiguration;
    private final Configuration paimonConfiguration;
    private final TablePath tablePath;
    private final Map<String, String> tableProperties;

    CreateTableOnLakeAction(
            Configuration flussConfiguration,
            Configuration paimonConfiguration,
            TablePath tablePath,
            Map<String, String> tableProperties) {
        this.flussConfiguration = new Configuration(flussConfiguration);
        this.paimonConfiguration = new Configuration(paimonConfiguration);
        this.tablePath = tablePath;
        this.tableProperties =
                Collections.unmodifiableMap(new HashMap<String, String>(tableProperties));
    }

    @Override
    public void run() throws Exception {
        Catalog paimonCatalog = createPaimonCatalog();
        try {
            FileStoreTable paimonTable = getPaimonTable(paimonCatalog);
            if (!paimonTable.primaryKeys().isEmpty()) {
                throw new UnsupportedOperationException(
                        String.format(
                                "Creating a Fluss table on Paimon primary-key table %s is not supported yet.",
                                tablePath));
            }

            Optional<Snapshot> snapshot = paimonTable.latestSnapshot();
            TableInfo tableInfo;
            try (Connection connection = ConnectionFactory.createConnection(flussConfiguration);
                    Admin admin = connection.getAdmin()) {
                tableInfo = admin.createTableOnLake(tablePath, tableProperties).get();
            }

            if (snapshot.isPresent()) {
                try {
                    commitLakeSnapshot(tableInfo, snapshot.get().id());
                } catch (Exception e) {
                    throw new IOException(
                            String.format(
                                    "Fluss table %s was created with table ID %d, but failed to register Paimon snapshot %d.",
                                    tablePath, tableInfo.getTableId(), snapshot.get().id()),
                            e);
                }
            }
            System.out.printf(
                    "Create table on lake succeeded for %s, table_id=%d%n",
                    tablePath, tableInfo.getTableId());
        } finally {
            paimonCatalog.close();
        }
    }

    Configuration getFlussConfiguration() {
        return new Configuration(flussConfiguration);
    }

    Configuration getPaimonConfiguration() {
        return new Configuration(paimonConfiguration);
    }

    TablePath getTablePath() {
        return tablePath;
    }

    Map<String, String> getTableProperties() {
        return tableProperties;
    }

    private Catalog createPaimonCatalog() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = CreateTableOnLakeAction.class.getClassLoader();
        }
        return CatalogFactory.createCatalog(
                CatalogContext.create(
                        Options.fromMap(paimonConfiguration.toMap()),
                        null,
                        new FlinkFileIOLoader()),
                classLoader);
    }

    private FileStoreTable getPaimonTable(Catalog paimonCatalog) throws Exception {
        Table table =
                paimonCatalog.getTable(
                        Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName()));
        if (!(table instanceof FileStoreTable)) {
            throw new IllegalArgumentException(
                    String.format("Paimon table %s is not a file store table.", tablePath));
        }
        return (FileStoreTable) table;
    }

    private void commitLakeSnapshot(TableInfo tableInfo, long snapshotId) throws Exception {
        Map<TableBucket, Long> emptyOffsets = Collections.emptyMap();
        try (FlussTableLakeSnapshotCommitter committer =
                new FlussTableLakeSnapshotCommitter(flussConfiguration)) {
            committer.open();
            String offsetsPath =
                    committer.prepareLakeSnapshot(
                            tableInfo.getTableId(), tableInfo.getTablePath(), emptyOffsets);
            committer.commit(
                    tableInfo.getTableId(),
                    tableInfo.getTablePath(),
                    LakeCommitResult.committedIsReadable(snapshotId),
                    offsetsPath,
                    emptyOffsets,
                    Collections.emptyMap());
        }
    }
}
