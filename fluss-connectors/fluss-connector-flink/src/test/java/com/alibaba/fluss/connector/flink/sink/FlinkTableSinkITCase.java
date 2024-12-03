/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.connector.flink.sink;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase.assertResultsIgnoreOrder;
import static com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase.waitUntilPartitions;
import static com.alibaba.fluss.server.testutils.FlussClusterExtension.BUILTIN_DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlinkTableSink}. */
class FlinkTableSinkITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private static final String CATALOG_NAME = "testcatalog";
    private static final String DEFAULT_DB = "defaultdb";
    static StreamExecutionEnvironment env;
    static StreamTableEnvironment tEnv;
    static TableEnvironment tBatchEnv;

    @BeforeAll
    static void beforeAll() {
        // open a catalog so that we can get table from the catalog
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        String bootstrapServers = String.join(",", flussConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // create table environment
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        tEnv = StreamTableEnvironment.create(env);
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);

        // create batch table environment
        tBatchEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tBatchEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tBatchEnv.executeSql("use catalog " + CATALOG_NAME);
        tBatchEnv
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    }

    @BeforeEach
    void before() {
        // create database
        tEnv.executeSql("create database " + DEFAULT_DB);
        tEnv.useDatabase(DEFAULT_DB);
        tBatchEnv.useDatabase(DEFAULT_DB);
    }

    @AfterEach
    void after() {
        tEnv.useDatabase(BUILTIN_DATABASE);
        tEnv.executeSql(String.format("drop database %s cascade", DEFAULT_DB));
    }

    @Test
    void testAppendLog() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null, b bigint, c string) with "
                        + "('bucket.num' = '3')");
        tEnv.executeSql(
                        "INSERT INTO sink_test(a, b, c) "
                                + "VALUES (1, 3501, 'Tim'), "
                                + "(2, 3502, 'Fabian'), "
                                + "(3, 3503, 'coco'), "
                                + "(4, 3504, 'jerry'), "
                                + "(5, 3505, 'piggy'), "
                                + "(6, 3506, 'stave')")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Tim]", "+I[2, 3502, Fabian]",
                        "+I[3, 3503, coco]", "+I[4, 3504, jerry]",
                        "+I[5, 3505, piggy]", "+I[6, 3506, stave]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testAppendLogWithBucketKey() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null, b bigint, c string) with "
                        + "('bucket.num' = '3', 'bucket.key' = 'c')");
        tEnv.executeSql(
                        "INSERT INTO sink_test(a, b, c) "
                                + "VALUES (1, 3501, 'Tim'), "
                                + "(2, 3502, 'Fabian'), "
                                + "(3, 3503, 'Tim'), "
                                + "(4, 3504, 'jerry'), "
                                + "(5, 3505, 'piggy'), "
                                + "(7, 3507, 'Fabian'), "
                                + "(8, 3508, 'stave'), "
                                + "(9, 3509, 'Tim'), "
                                + "(10, 3510, 'coco'), "
                                + "(11, 3511, 'stave'), "
                                + "(12, 3512, 'Tim')")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        //noinspection ArraysAsListWithZeroOrOneArgument
        List<List<String>> expectedGroups =
                Arrays.asList(
                        Arrays.asList(
                                "+I[1, 3501, Tim]",
                                "+I[3, 3503, Tim]",
                                "+I[9, 3509, Tim]",
                                "+I[12, 3512, Tim]"),
                        Arrays.asList("+I[2, 3502, Fabian]", "+I[7, 3507, Fabian]"),
                        Arrays.asList("+I[4, 3504, jerry]"),
                        Arrays.asList("+I[5, 3505, piggy]"),
                        Arrays.asList("+I[8, 3508, stave]", "+I[11, 3511, stave]"),
                        Arrays.asList("+I[10, 3510, coco]"));

        List<String> expectedRows =
                expectedGroups.stream().flatMap(List::stream).collect(Collectors.toList());

        List<String> actual = new ArrayList<>(expectedRows.size());
        for (int i = 0; i < expectedRows.size(); i++) {
            actual.add(rowIter.next().toString());
        }
        rowIter.close();
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRows);

        // check data with the same bucket key should be read in sequence.
        for (List<String> expected : expectedGroups) {
            if (expected.size() <= 1) {
                continue;
            }
            int prevIndex = actual.indexOf(expected.get(0));
            for (int i = 1; i < expected.size(); i++) {
                int index = actual.indexOf(expected.get(i));
                assertThat(index).isGreaterThan(prevIndex);
                prevIndex = index;
            }
        }
    }

    @Test
    void testAppendLogWithRoundRobin() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null, b bigint, c string) with "
                        + "('bucket.num' = '3', 'client.writer.bucket.no-key-assigner' = 'round_robin')");
        tEnv.executeSql(
                        "INSERT INTO sink_test(a, b, c) "
                                + "VALUES (1, 3501, 'Tim'), "
                                + "(2, 3502, 'Fabian'), "
                                + "(3, 3503, 'coco'), "
                                + "(4, 3504, 'jerry'), "
                                + "(5, 3505, 'piggy'), "
                                + "(6, 3506, 'stave')")
                .await();

        Map<Integer, List<String>> rows = new HashMap<>();
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        Connection conn = ConnectionFactory.createConnection(clientConf);
        try (Table table = conn.getTable(TablePath.of(DEFAULT_DB, "sink_test"))) {
            LogScanner logScanner = table.getLogScanner(new LogScan());

            logScanner.subscribeFromBeginning(0);
            logScanner.subscribeFromBeginning(1);
            logScanner.subscribeFromBeginning(2);
            long scanned = 0;
            while (scanned < 6) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (TableBucket bucket : scanRecords.buckets()) {
                    List<String> rowsBucket =
                            rows.computeIfAbsent(bucket.getBucket(), k -> new ArrayList<>());
                    for (ScanRecord record : scanRecords.records(bucket)) {
                        InternalRow row = record.getRow();
                        rowsBucket.add(
                                Row.of(row.getInt(0), row.getLong(1), row.getString(2).toString())
                                        .toString());
                    }
                }
                scanned += scanRecords.count();
            }
        }
        List<String> expectedRows0 = Arrays.asList("+I[1, 3501, Tim]", "+I[4, 3504, jerry]");
        List<String> expectedRows1 = Arrays.asList("+I[2, 3502, Fabian]", "+I[5, 3505, piggy]");
        List<String> expectedRows2 = Arrays.asList("+I[3, 3503, coco]", "+I[6, 3506, stave]");
        assertThat(rows.values())
                .containsExactlyInAnyOrder(expectedRows0, expectedRows1, expectedRows2);
    }

    @Test
    void testAppendLogWithMultiBatch() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null, b bigint, c string) with "
                        + "('bucket.num' = '3')");
        int batchSize = 3;
        for (int i = 0; i < batchSize; i++) {
            tEnv.executeSql(
                            "INSERT INTO sink_test(a, b, c) "
                                    + "VALUES (1, 3501, 'Tim'), "
                                    + "(2, 3502, 'Fabian'), "
                                    + "(3, 3503, 'coco'), "
                                    + "(4, 3504, 'jerry'), "
                                    + "(5, 3505, 'piggy'), "
                                    + "(6, 3506, 'stave')")
                    .await();
        }

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        List<String> expectedRows = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            expectedRows.addAll(
                    Arrays.asList(
                            "+I[1, 3501, Tim]", "+I[2, 3502, Fabian]",
                            "+I[3, 3503, coco]", "+I[4, 3504, jerry]",
                            "+I[5, 3505, piggy]", "+I[6, 3506, stave]"));
        }
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testPut() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null primary key not enforced, b bigint, c string) with('bucket.num' = '3')");
        tEnv.executeSql(
                        "INSERT INTO sink_test(a, b, c) "
                                + "VALUES (1, 3501, 'Tim'), "
                                + "(2, 3502, 'Fabian'), "
                                + "(3, 3503, 'coco'), "
                                + "(4, 3504, 'jerry'), "
                                + "(5, 3505, 'piggy'), "
                                + "(6, 3506, 'stave')")
                .await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Tim]", "+I[2, 3502, Fabian]",
                        "+I[3, 3503, coco]", "+I[4, 3504, jerry]",
                        "+I[5, 3505, piggy]", "+I[6, 3506, stave]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testPartialUpsert() throws Exception {
        tEnv.executeSql(
                "create table sink_test (a int not null primary key not enforced, b bigint, c string) with('bucket.num' = '3')");

        // partial insert
        tEnv.executeSql("INSERT INTO sink_test(a, b) VALUES (1, 111), (2, 222)").await();
        tEnv.executeSql("INSERT INTO sink_test(c, a) VALUES ('c1', 1), ('c2', 2)").await();

        CloseableIterator<Row> rowIter = tEnv.executeSql("select * from sink_test").collect();

        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 111, null]",
                        "+I[2, 222, null]",
                        "-U[1, 111, null]",
                        "+U[1, 111, c1]",
                        "-U[2, 222, null]",
                        "+U[2, 222, c2]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // partial delete
        org.apache.flink.table.api.Table changeLogTable =
                tEnv.fromChangelogStream(
                        env.fromElements(
                                        Row.ofKind(
                                                org.apache.flink.types.RowKind.INSERT,
                                                1,
                                                333L,
                                                "c11"),
                                        Row.ofKind(
                                                org.apache.flink.types.RowKind.DELETE,
                                                1,
                                                333L,
                                                "c11"))
                                .returns(Types.ROW(Types.INT, Types.LONG, Types.STRING)));
        tEnv.createTemporaryView("changeLog", changeLogTable);

        // check the target fields in row 1 is set to null
        tEnv.executeSql("INSERT INTO sink_test(a, b) SELECT f0, f1 FROM changeLog").await();
        expectedRows =
                Arrays.asList(
                        "-U[1, 111, c1]", "+U[1, 333, c1]", "-U[1, 333, c1]", "+U[1, null, c1]");
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // check the row 1 will be deleted finally since all the fields in the row are set to null
        tEnv.executeSql("INSERT INTO sink_test(a, c) SELECT f0, f2 FROM changeLog").await();
        expectedRows = Arrays.asList("-U[1, null, c1]", "+U[1, null, c11]", "-D[1, null, c11]");
        assertResultsIgnoreOrder(rowIter, expectedRows, true);
    }

    @Test
    void testInsertWithoutSpecifiedCols() {
        tEnv.executeSql("create table sink_insert_all (a int, b bigint, c string)");
        tEnv.executeSql("create table source_insert_all (a int, b bigint, c string)");
        // we just use explain to reduce test time
        String expectPlan =
                "== Abstract Syntax Tree ==\n"
                        + "LogicalSink(table=[testcatalog.defaultdb.sink_insert_all], fields=[a, b, c])\n"
                        + "+- LogicalProject(a=[$0], b=[$1], c=[$2])\n"
                        + "   +- LogicalTableScan(table=[[testcatalog, defaultdb, source_insert_all]])\n"
                        + "\n"
                        + "== Optimized Physical Plan ==\n"
                        + "Sink(table=[testcatalog.defaultdb.sink_insert_all], fields=[a, b, c])\n"
                        + "+- TableSourceScan(table=[[testcatalog, defaultdb, source_insert_all]], fields=[a, b, c])\n"
                        + "\n"
                        + "== Optimized Execution Plan ==\n"
                        + "Sink(table=[testcatalog.defaultdb.sink_insert_all], fields=[a, b, c])\n"
                        + "+- TableSourceScan(table=[[testcatalog, defaultdb, source_insert_all]], fields=[a, b, c])\n";
        assertThat(tEnv.explainSql("insert into sink_insert_all select * from source_insert_all"))
                .isEqualTo(expectPlan);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testWritePartitionedTable(boolean isPrimaryKeyTable) throws Exception {
        String tableName =
                isPrimaryKeyTable
                        ? "partitioned_primary_key_table_sink"
                        : "partitioned_log_table_sink";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);
        tEnv.executeSql(
                String.format(
                        "create table %s ("
                                + "a int not null,"
                                + " b bigint, "
                                + "c string"
                                + (isPrimaryKeyTable ? ", primary key (a, c) NOT ENFORCED" : "")
                                + ")"
                                + " partitioned by (c) "
                                + "with ('table.auto-partition.enabled' = 'true',"
                                + " 'table.auto-partition.time-unit' = 'year')",
                        tableName));

        Collection<String> partitions =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath)
                        .values();

        InsertAndExpectValues insertAndExpectValues = rowsToInsertInto(partitions);

        List<String> insertValues = insertAndExpectValues.insertValues;
        List<String> expectedRows = insertAndExpectValues.expectedRows;

        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s(a, b, c) " + "VALUES %s",
                                tableName, String.join(", ", insertValues)))
                .await();

        CloseableIterator<Row> rowIter =
                tEnv.executeSql(String.format("select * from %s", tableName)).collect();
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // create two partitions, write data to the new partitions
        List<String> newPartitions = Arrays.asList("2000", "2001");

        FlinkTestBase.createPartitions(
                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, newPartitions);

        // insert into the new partition again, check we can read the data
        // in new partitions
        insertAndExpectValues = rowsToInsertInto(newPartitions);
        insertValues = insertAndExpectValues.insertValues;
        expectedRows = insertAndExpectValues.expectedRows;
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s(a, b, c) " + "VALUES %s",
                                tableName, String.join(", ", insertValues)))
                .await();
        assertResultsIgnoreOrder(rowIter, expectedRows, false);

        // test insert static partitions
        tEnv.executeSql(
                        String.format(
                                "INSERT INTO %s PARTITION (c = 2000) values (22, 2222), (33, 3333)",
                                tableName))
                .await();
        assertResultsIgnoreOrder(
                rowIter, Arrays.asList("+I[22, 2222, 2000]", "+I[33, 3333, 2000]"), true);
    }

    @Test
    void testDeleteAndUpdateStmtOnPkTable() throws Exception {
        String tableName = "pk_table_delete_test";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint, "
                                + " c string,"
                                + " primary key (a) not enforced"
                                + ")",
                        tableName));
        // test delete without data.
        tBatchEnv.executeSql("DELETE FROM " + tableName + " WHERE a = 5").await();

        List<String> insertValues =
                Arrays.asList(
                        "(1, 3501, 'Beijing')",
                        "(2, 3502, 'Shanghai')",
                        "(3, 3503, 'Berlin')",
                        "(4, 3504, 'Seattle')",
                        "(5, 3505, 'Boston')",
                        "(6, 3506, 'London')");
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s(a,b,c) VALUES %s",
                                tableName, String.join(", ", insertValues)))
                .await();

        // test delete row5
        tBatchEnv.executeSql("DELETE FROM " + tableName + " WHERE a = 5").await();
        CloseableIterator<Row> rowIter =
                tBatchEnv
                        .executeSql(String.format("select * from %s WHERE a = 5", tableName))
                        .collect();
        assertThat(rowIter.hasNext()).isFalse();

        // test update row4
        tBatchEnv.executeSql("UPDATE " + tableName + " SET c = 'New York' WHERE a = 4").await();
        CloseableIterator<Row> row4 =
                tBatchEnv
                        .executeSql(String.format("select * from %s WHERE a = 4", tableName))
                        .collect();
        List<String> expected = Collections.singletonList("+I[4, 3504, New York]");
        assertResultsIgnoreOrder(row4, expected, true);

        // use stream env to assert changelogs
        CloseableIterator<Row> changelogIter =
                tEnv.executeSql(
                                String.format(
                                        "select * from %s /*+ OPTIONS('scan.startup.mode' = 'earliest') */",
                                        tableName))
                        .collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, Beijing]",
                        "+I[2, 3502, Shanghai]",
                        "+I[3, 3503, Berlin]",
                        "+I[4, 3504, Seattle]",
                        "+I[5, 3505, Boston]",
                        "+I[6, 3506, London]",
                        "-D[5, 3505, Boston]",
                        "-U[4, 3504, Seattle]",
                        "+U[4, 3504, New York]");
        assertResultsIgnoreOrder(changelogIter, expectedRows, true);
    }

    @Test
    void testDeleteAndUpdateStmtOnPartitionedPkTable() throws Exception {
        String tableName = "partitioned_pk_table_delete_test";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint, "
                                + " c string,"
                                + " primary key (a, c) not enforced"
                                + ") partitioned by (c)"
                                + " with ('table.auto-partition.enabled' = 'true',"
                                + " 'table.auto-partition.time-unit' = 'year')",
                        tableName));
        Collection<String> partitions =
                waitUntilPartitions(
                                FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                                TablePath.of(DEFAULT_DB, tableName))
                        .values();
        String partition = partitions.iterator().next();
        List<String> insertValues =
                Arrays.asList(
                        "(1, 3501, '" + partition + "')",
                        "(2, 3502, '" + partition + "')",
                        "(3, 3503, '" + partition + "')",
                        "(4, 3504, '" + partition + "')",
                        "(5, 3505, '" + partition + "')",
                        "(6, 3506, '" + partition + "')");
        tBatchEnv
                .executeSql(
                        String.format(
                                "INSERT INTO %s(a,b,c) VALUES %s",
                                tableName, String.join(", ", insertValues)))
                .await();

        // test delete row5
        tBatchEnv
                .executeSql("DELETE FROM " + tableName + " WHERE a = 5 AND c = '" + partition + "'")
                .await();
        CloseableIterator<Row> rowIter =
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "select * from %s WHERE a = 5 AND c = '%s'",
                                        tableName, partition))
                        .collect();
        assertThat(rowIter.hasNext()).isFalse();

        // test update row4
        tBatchEnv
                .executeSql(
                        "UPDATE "
                                + tableName
                                + " SET b = 4004 WHERE a = 4 AND c = '"
                                + partition
                                + "'")
                .await();
        CloseableIterator<Row> row4 =
                tBatchEnv
                        .executeSql(
                                String.format(
                                        "select * from %s WHERE a = 4 AND c = '%s'",
                                        tableName, partition))
                        .collect();
        List<String> expected = Collections.singletonList("+I[4, 4004, " + partition + "]");
        assertResultsIgnoreOrder(row4, expected, true);

        // use stream env to assert changelogs
        CloseableIterator<Row> changelogIter =
                tEnv.executeSql(
                                String.format(
                                        "select * from %s /*+ OPTIONS('scan.startup.mode' = 'earliest') */",
                                        tableName))
                        .collect();
        List<String> expectedRows =
                Arrays.asList(
                        "+I[1, 3501, " + partition + "]",
                        "+I[2, 3502, " + partition + "]",
                        "+I[3, 3503, " + partition + "]",
                        "+I[4, 3504, " + partition + "]",
                        "+I[5, 3505, " + partition + "]",
                        "+I[6, 3506, " + partition + "]",
                        "-D[5, 3505, " + partition + "]",
                        "-U[4, 3504, " + partition + "]",
                        "+U[4, 4004, " + partition + "]");
        assertResultsIgnoreOrder(changelogIter, expectedRows, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testUnsupportedDeleteAndUpdateStmtOnLogTable(boolean isPartitionedTable) {
        String tableName =
                isPartitionedTable ? "partitioned_log_table_delete_test" : "log_table_delete_test";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint, "
                                + " c string"
                                + ")"
                                + (isPartitionedTable ? " partitioned by (c) " : "")
                                + "with ('table.auto-partition.enabled' = 'true',"
                                + " 'table.auto-partition.time-unit' = 'year')",
                        tableName));
        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("DELETE FROM " + tableName + " WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Log Table doesn't support DELETE and UPDATE statements.");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql(
                                                "UPDATE "
                                                        + tableName
                                                        + " SET c = 'New York' WHERE a = 4")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Log Table doesn't support DELETE and UPDATE statements.");
    }

    @Test
    void testUnsupportedDeleteAndUpdateStmtOnPartialPK() {
        // test primary-key table
        String t1 = "t1";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint not null, "
                                + " c string,"
                                + " primary key (a, b) not enforced"
                                + ")",
                        t1));
        assertThatThrownBy(() -> tBatchEnv.executeSql("DELETE FROM " + t1 + " WHERE a = 1").await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Currently, Fluss table only supports DELETE statement with conditions on primary key.");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("UPDATE " + t1 + " SET b = 4004 WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Updates to primary keys are not supported, primaryKeys ([a, b]), updatedColumns ([b])");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql(
                                                "UPDATE " + t1 + " SET c = 'New York' WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Currently, Fluss table only supports UPDATE statement with conditions on primary key.");

        // test partitioned primary-key table
        String t2 = "t2";
        tBatchEnv.executeSql(
                String.format(
                        "create table %s ("
                                + " a int not null,"
                                + " b bigint not null, "
                                + " c string,"
                                + " primary key (a, c) not enforced"
                                + ") partitioned by (c)"
                                + " with ('table.auto-partition.enabled' = 'true',"
                                + " 'table.auto-partition.time-unit' = 'year')",
                        t2));
        assertThatThrownBy(() -> tBatchEnv.executeSql("DELETE FROM " + t2 + " WHERE a = 1").await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Currently, Fluss table only supports DELETE statement with conditions on primary key.");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("UPDATE " + t2 + " SET c = '2028' WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Updates to primary keys are not supported, primaryKeys ([a, c]), updatedColumns ([c])");

        assertThatThrownBy(
                        () ->
                                tBatchEnv
                                        .executeSql("UPDATE " + t2 + " SET b = 4004 WHERE a = 1")
                                        .await())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Currently, Fluss table only supports UPDATE statement with conditions on primary key.");
    }

    private InsertAndExpectValues rowsToInsertInto(Collection<String> partitions) {
        List<String> insertValues = new ArrayList<>();
        List<String> expectedValues = new ArrayList<>();
        for (String partition : partitions) {
            insertValues.addAll(
                    Arrays.asList(
                            "(1, 3501, '" + partition + "')",
                            "(2, 3502, '" + partition + "')",
                            "(3, 3503, '" + partition + "')",
                            "(4, 3504, '" + partition + "')",
                            "(5, 3505, '" + partition + "')",
                            "(6, 3506, '" + partition + "')"));
            expectedValues.addAll(
                    Arrays.asList(
                            "+I[1, 3501, " + partition + "]",
                            "+I[2, 3502, " + partition + "]",
                            "+I[3, 3503, " + partition + "]",
                            "+I[4, 3504, " + partition + "]",
                            "+I[5, 3505, " + partition + "]",
                            "+I[6, 3506, " + partition + "]"));
        }
        return new InsertAndExpectValues(insertValues, expectedValues);
    }

    private static class InsertAndExpectValues {
        private final List<String> insertValues;
        private final List<String> expectedRows;

        public InsertAndExpectValues(List<String> insertValues, List<String> expectedRows) {
            this.insertValues = insertValues;
            this.expectedRows = expectedRows;
        }
    }
}
