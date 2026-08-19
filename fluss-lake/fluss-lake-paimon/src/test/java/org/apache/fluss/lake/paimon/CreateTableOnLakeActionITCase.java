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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.action.Action;
import org.apache.fluss.flink.action.ActionLoader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.server.utils.LakeStorageUtils.extractLakeProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT coverage for the create_table_on_lake Action. */
class CreateTableOnLakeActionITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static final String DATABASE = "fluss";

    private static Catalog paimonCatalog;
    private static String paimonWarehouse;

    private Connection connection;
    private Admin admin;

    @BeforeEach
    void setUp() {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testCreateTableOnLakeActionCommitsInitialSnapshot() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "action_existing_log_table");
        createExistingPaimonLogTable(tablePath, 4, "id");
        FileStoreTable paimonTable =
                (FileStoreTable)
                        paimonCatalog.getTable(
                                Identifier.create(DATABASE, tablePath.getTableName()));
        writeData(paimonTable);
        long snapshotId = paimonTable.latestSnapshot().get().id();

        runAction(tablePath);

        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(lakeSnapshot.getTableBucketsOffset()).isEmpty();
    }

    @Test
    void testCreateTableOnLakeActionSkipsSnapshotForEmptyTable() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "action_empty_log_table");
        createExistingPaimonLogTable(tablePath, -1, null);

        runAction(tablePath, "bucket.num=2", "team=storage");

        assertThat(admin.getTableInfo(tablePath).get().getNumBuckets()).isEqualTo(2);
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .cause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    @Test
    void testCreateTableOnLakeActionRejectsPrimaryKeyTable() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, "action_primary_key_table");
        createExistingPaimonPrimaryKeyTable(tablePath);

        assertThatThrownBy(() -> runAction(tablePath))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("primary-key table");
        assertThat(admin.tableExists(tablePath).get()).isFalse();
    }

    private static Configuration initConfig() {
        Configuration configuration = new Configuration();
        configuration.setString("datalake.format", "paimon");
        configuration.setString("datalake.paimon.metastore", "filesystem");
        try {
            paimonWarehouse = Files.createTempDirectory("fluss-create-table-action").toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create Paimon warehouse.", e);
        }
        configuration.setString("datalake.paimon.warehouse", paimonWarehouse);
        configuration.setString("datalake.paimon.cache-enabled", "false");
        paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(
                                Options.fromMap(extractLakeProperties(configuration))));
        return configuration;
    }

    private void runAction(TablePath tablePath, String... tableProperties) throws Exception {
        String[] args =
                new String[] {
                    "create_table_on_lake",
                    "--table",
                    tablePath.toString(),
                    "--fluss.bootstrap.servers",
                    FLUSS_CLUSTER_EXTENSION.getBootstrapServers(),
                    "--datalake.format",
                    "paimon",
                    "--datalake.paimon.metastore",
                    "filesystem",
                    "--datalake.paimon.warehouse",
                    paimonWarehouse
                };
        int baseLength = args.length;
        args = Arrays.copyOf(args, baseLength + tableProperties.length * 2);
        for (int i = 0; i < tableProperties.length; i++) {
            args[baseLength + i * 2] = "--table-conf";
            args[baseLength + i * 2 + 1] = tableProperties[i];
        }

        Optional<Action> action = ActionLoader.createAction(args);
        assertThat(action).isPresent();
        action.get().build();
        action.get().run();
    }

    private static void createExistingPaimonLogTable(
            TablePath tablePath, int bucketCount, String bucketKey) throws Exception {
        paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.INT(), "identifier")
                        .column("name", org.apache.paimon.types.DataTypes.STRING())
                        .column("__bucket", org.apache.paimon.types.DataTypes.INT())
                        .column("__offset", org.apache.paimon.types.DataTypes.BIGINT())
                        .column(
                                "__timestamp",
                                org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS())
                        .option(CoreOptions.BUCKET.key(), String.valueOf(bucketCount))
                        .option("fluss.owner", "lake");
        if (bucketKey != null) {
            schemaBuilder.option(CoreOptions.BUCKET_KEY.key(), bucketKey);
        }
        paimonCatalog.createTable(
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName()),
                schemaBuilder.build(),
                false);
    }

    private static void createExistingPaimonPrimaryKeyTable(TablePath tablePath) throws Exception {
        paimonCatalog.createDatabase(tablePath.getDatabaseName(), true);
        Schema schema =
                Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.INT().notNull())
                        .column("name", org.apache.paimon.types.DataTypes.STRING())
                        .column("__bucket", org.apache.paimon.types.DataTypes.INT())
                        .column("__offset", org.apache.paimon.types.DataTypes.BIGINT())
                        .column(
                                "__timestamp",
                                org.apache.paimon.types.DataTypes.TIMESTAMP_LTZ_MILLIS())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "4")
                        .build();
        paimonCatalog.createTable(
                Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName()),
                schema,
                false);
    }

    private static void writeData(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (int i = 0; i < 10; i++) {
                write.write(
                        GenericRow.of(
                                i,
                                BinaryString.fromString("row-" + i),
                                0,
                                (long) i,
                                Timestamp.fromEpochMillis(System.currentTimeMillis())));
            }
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
    }
}
