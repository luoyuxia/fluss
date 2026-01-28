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

package org.apache.fluss.lake.iceberg.flink;

import org.apache.fluss.config.AutoPartitionTimeUnit;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsExactOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertResultsIgnoreOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.assertRowResultsIgnoreOrder;
import static org.apache.fluss.flink.source.testutils.FlinkRowAssertionsUtils.collectRowsWithTimeout;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test union read log table with full type. */
public class FlinkUnionReadLogTableITCase extends FlinkUnionReadTestBase {
    @TempDir public static File savepointDir;

    @BeforeAll
    protected static void beforeAll() {
        FlinkUnionReadTestBase.beforeAll();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReadLogTableFullType(boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);
        List<Row> writtenRows = new ArrayList<>();
        long tableId = prepareLogTable(t1, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        // wait until records has been synced
        waitUntilBucketSynced(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // now, start to read the log table, which will read iceberg
        // may read fluss or not, depends on the log offset of iceberg snapshot
        List<Row> actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());

        assertThat(actual).containsExactlyInAnyOrderElementsOf(writtenRows);

        // cancel the tiering job
        jobClient.cancel().get();

        // write some log data again
        writtenRows.addAll(writeRows(t1, 3, isPartitioned));

        // query the log table again and check the data
        // it should read both iceberg snapshot and fluss log
        actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select * from " + tableName).collect());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(writtenRows);

        // test project push down
        actual =
                CollectionUtil.iteratorToList(
                        batchTEnv.executeSql("select f_byte from " + tableName).collect());
        List<Row> expected =
                writtenRows.stream()
                        .map(row -> Row.of(row.getField(1)))
                        .collect(Collectors.toList());
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        if (isPartitioned) {
            // get first partition
            String partition = waitUntilPartitions(t1).values().iterator().next();
            String sqlWithPartitionFilter =
                    "select * FROM " + tableName + " WHERE p = '" + partition + "'";

            String plan = batchTEnv.explainSql(sqlWithPartitionFilter);

            // check if the plan contains partition filter
            // check filter push down
            assertThat(plan)
                    .contains("TableSourceScan(")
                    .contains("LogicalFilter(condition=[=($16, _UTF-16LE'" + partition + "'")
                    .contains("filter=[=(p, _UTF-16LE'" + partition + "'");

            List<Row> expectedFiltered =
                    writtenRows.stream()
                            .filter(r -> partition.equals(r.getField(16)))
                            .collect(Collectors.toList());

            List<Row> actualFiltered =
                    CollectionUtil.iteratorToList(
                            batchTEnv.executeSql(sqlWithPartitionFilter).collect());

            assertThat(actualFiltered).containsExactlyInAnyOrderElementsOf(expectedFiltered);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testReadLogTableInStreamMode(boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "stream_logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);
        List<Row> writtenRows = new LinkedList<>();
        long tableId = prepareLogTable(t1, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        // wait until records has been synced
        waitUntilBucketSynced(t1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        // now, start to read the log table, which will read iceberg
        // may read fluss or not, depends on the log offset of iceberg snapshot
        CloseableIterator<Row> actual =
                streamTEnv.executeSql("select * from " + tableName).collect();
        assertResultsIgnoreOrder(
                actual, writtenRows.stream().map(Row::toString).collect(Collectors.toList()), true);

        // cancel the tiering job
        jobClient.cancel().get();

        // write some log data again
        writtenRows.addAll(writeRows(t1, 3, isPartitioned));

        // query the log table again and check the data
        // it should read both iceberg snapshot and fluss log
        actual =
                streamTEnv
                        .executeSql(
                                "select * from "
                                        + tableName
                                        + " /*+ OPTIONS('scan.partition.discovery.interval'='100ms') */")
                        .collect();
        if (isPartitioned) {
            // we write to a new partition to verify partition discovery
            writtenRows.addAll(writeFullTypeRows(t1, 10, "3027"));
        }
        assertResultsIgnoreOrder(
                actual, writtenRows.stream().map(Row::toString).collect(Collectors.toList()), true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUnionReadLogTableFailover(boolean isPartitioned) throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName1 =
                "restore_logTable_" + (isPartitioned ? "partitioned" : "non_partitioned");
        String resultTableName =
                "result_table" + (isPartitioned ? "partitioned" : "non_partitioned");

        TablePath table1 = TablePath.of(DEFAULT_DB, tableName1);
        TablePath resultTable = TablePath.of(DEFAULT_DB, resultTableName);
        List<Row> writtenRows = new LinkedList<>();
        long tableId = prepareLogTable(table1, DEFAULT_BUCKET_NUM, isPartitioned, writtenRows);
        // wait until records has been synced
        waitUntilBucketSynced(table1, tableId, DEFAULT_BUCKET_NUM, isPartitioned);

        StreamTableEnvironment streamTEnv = buildStreamTEnv(null);
        // now, start to read the log table to write to a fluss result table
        // may read fluss or not, depends on the log offset of iceberg snapshot
        createFullTypeLogTable(resultTable, DEFAULT_BUCKET_NUM, isPartitioned, false);
        TableResult insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName1);

        CloseableIterator<Row> actual =
                streamTEnv.executeSql("select * from " + resultTableName).collect();
        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, writtenRows, false);
        } else {
            assertResultsExactOrder(actual, writtenRows, false);
        }

        // now, stop the job with save point
        String savepointPath =
                insertResult
                        .getJobClient()
                        .get()
                        .stopWithSavepoint(
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get();

        // re buildStreamTEnv
        streamTEnv = buildStreamTEnv(savepointPath);
        insertResult =
                streamTEnv.executeSql(
                        "insert into " + resultTableName + " select * from " + tableName1);

        // write some log data again
        List<Row> rows = writeRows(table1, 3, isPartitioned);
        if (isPartitioned) {
            assertRowResultsIgnoreOrder(actual, rows, true);
        } else {
            assertResultsExactOrder(actual, rows, true);
        }

        // cancel jobs
        insertResult.getJobClient().get().cancel().get();
        jobClient.cancel().get();
    }

    @Test
    void testPartitionTimestampModeOnLakeTable() throws Exception {
        // Start tiering job
        JobClient jobClient = buildTieringJob(execEnv);

        String tableName = "partition_timestamp_lake_test";
        TablePath tablePath = TablePath.of(DEFAULT_DB, tableName);

        // Create auto-partitioned log table with datalake enabled
        long tableId = createLogTable(tablePath, DEFAULT_BUCKET_NUM, true);

        // Wait for partitions to be created
        Map<Long, String> partitionNameByIds = waitUntilPartitions(tablePath);
        assertThat(partitionNameByIds.size()).isGreaterThanOrEqualTo(2);

        // Get sorted partition names (e.g., "2025", "2026")
        List<String> sortedPartitions =
                partitionNameByIds.values().stream().sorted().collect(Collectors.toList());
        String olderPartition = sortedPartitions.get(0);
        String newerPartition = sortedPartitions.get(1);

        // Write data to partitions
        List<InternalRow> rows = new ArrayList<>();
        // Write to older partition
        for (int i = 0; i < 3; i++) {
            rows.add(row(i, "old_" + i, olderPartition));
        }
        // Write to newer partition
        for (int i = 10; i < 13; i++) {
            rows.add(row(i, "new_" + i, newerPartition));
        }
        writeRows(tablePath, rows, true);

        // Wait until data is synced to lake
        waitUntilBucketSynced(tablePath, tableId, DEFAULT_BUCKET_NUM, true);

        // Cancel tiering job to create a scenario where we read from both lake and Fluss
        jobClient.cancel().get();

        // Write more data after tiering stopped (this will only be in Fluss)
        List<InternalRow> moreRows = new ArrayList<>();
        for (int i = 20; i < 23; i++) {
            moreRows.add(row(i, "fluss_" + i, newerPartition));
        }
        writeRows(tablePath, moreRows, true);

        // Use partition-timestamp mode with a timestamp that maps to the newer partition
        // This should filter to: partition >= newerPartition
        String timestampInNewerPartition = newerPartition + "-06-15 12:00:00";
        String query =
                String.format(
                        "SELECT a, b, c FROM %s "
                                + "/*+ OPTIONS('scan.startup.mode' = 'partition-timestamp', "
                                + "'scan.startup.timestamp' = '%s', "
                                + "'scan.partition.discovery.interval' = '100ms') */",
                        tableName, timestampInNewerPartition);

        CloseableIterator<Row> iterator = streamTEnv.executeSql(query).collect();

        // Expected: only data from newer partition (from both lake and Fluss)
        List<String> expected = new ArrayList<>();
        // From lake (rows 10-12)
        for (int i = 10; i < 13; i++) {
            expected.add(String.format("+I[%d, new_%d, %s]", i, i, newerPartition));
        }
        // From Fluss (rows 20-22)
        for (int i = 20; i < 23; i++) {
            expected.add(String.format("+I[%d, fluss_%d, %s]", i, i, newerPartition));
        }

        // First, verify initial data is consumed
        List<String> actual = collectRowsWithTimeout(iterator, expected.size(), false);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);

        // Now add a new partition that matches the filter (>= newerPartition)
        // This tests that newly discovered partitions are also consumed
        String newPartition = "3000"; // A future partition that's >= newerPartition
        List<InternalRow> newPartitionRows = new ArrayList<>();
        for (int i = 30; i < 33; i++) {
            newPartitionRows.add(row(i, "new_partition_" + i, newPartition));
        }
        writeRows(tablePath, newPartitionRows, true);

        // Expected data from the newly added partition
        List<String> expectedNewPartition = new ArrayList<>();
        for (int i = 30; i < 33; i++) {
            expectedNewPartition.add(
                    String.format("+I[%d, new_partition_%d, %s]", i, i, newPartition));
        }

        // Verify that data from the new partition is also consumed
        List<String> actualNewPartition =
                collectRowsWithTimeout(iterator, expectedNewPartition.size(), true);
        assertThat(actualNewPartition).containsExactlyInAnyOrderElementsOf(expectedNewPartition);
    }

    protected long createLogTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(bucketNum, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        if (isPartitioned) {
            schemaBuilder.column("c", DataTypes.STRING());
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("c");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private long prepareLogTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, List<Row> flinkRows)
            throws Exception {
        // createFullTypeLogTable creates a datalake-enabled table with a partition column.
        long t1Id = createFullTypeLogTable(tablePath, bucketNum, isPartitioned);
        if (isPartitioned) {
            Map<Long, String> partitionNameById = waitUntilPartitions(tablePath);
            for (String partition :
                    partitionNameById.values().stream().sorted().collect(Collectors.toList())) {
                for (int i = 0; i < 3; i++) {
                    flinkRows.addAll(writeFullTypeRows(tablePath, 10, partition));
                }
            }
        } else {
            for (int i = 0; i < 3; i++) {
                flinkRows.addAll(writeFullTypeRows(tablePath, 10, null));
            }
        }
        return t1Id;
    }

    protected long createFullTypeLogTable(TablePath tablePath, int bucketNum, boolean isPartitioned)
            throws Exception {
        return createFullTypeLogTable(tablePath, bucketNum, isPartitioned, true);
    }

    protected long createFullTypeLogTable(
            TablePath tablePath, int bucketNum, boolean isPartitioned, boolean lakeEnabled)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("f_boolean", DataTypes.BOOLEAN())
                        .column("f_byte", DataTypes.TINYINT())
                        .column("f_short", DataTypes.SMALLINT())
                        .column("f_int", DataTypes.INT())
                        .column("f_long", DataTypes.BIGINT())
                        .column("f_float", DataTypes.FLOAT())
                        .column("f_double", DataTypes.DOUBLE())
                        .column("f_string", DataTypes.STRING())
                        .column("f_decimal1", DataTypes.DECIMAL(5, 2))
                        .column("f_decimal2", DataTypes.DECIMAL(20, 0))
                        .column("f_timestamp_ltz1", DataTypes.TIMESTAMP_LTZ(3))
                        .column("f_timestamp_ltz2", DataTypes.TIMESTAMP_LTZ(6))
                        .column("f_timestamp_ntz1", DataTypes.TIMESTAMP(3))
                        .column("f_timestamp_ntz2", DataTypes.TIMESTAMP(6))
                        .column("f_binary", DataTypes.BINARY(4))
                        .column(
                                "f_row",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f_nested_int", DataTypes.INT()),
                                        DataTypes.FIELD("f_nested_string", DataTypes.STRING())));

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(bucketNum, "f_int")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        if (lakeEnabled) {
            tableBuilder
                    .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                    .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));
        }

        if (isPartitioned) {
            schemaBuilder.column("p", DataTypes.STRING());
            tableBuilder.property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true);
            tableBuilder.partitionedBy("p");
            tableBuilder.property(
                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, AutoPartitionTimeUnit.YEAR);
        }
        tableBuilder.schema(schemaBuilder.build());
        return createTable(tablePath, tableBuilder.build());
    }

    private List<Row> writeFullTypeRows(
            TablePath tablePath, int rowCount, @Nullable String partition) throws Exception {
        List<InternalRow> rows = new ArrayList<>();
        List<Row> flinkRows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            if (partition == null) {
                rows.add(
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_" + i,
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                row(10, "nested_string")));

                flinkRows.add(
                        Row.of(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_" + i,
                                new java.math.BigDecimal("9.00"),
                                new java.math.BigDecimal("1000"),
                                Instant.ofEpochMilli(1698235273400L),
                                Instant.ofEpochMilli(1698235273400L).plusNanos(7000),
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(1698235273501L), ZoneId.of("UTC")),
                                LocalDateTime.ofInstant(
                                                Instant.ofEpochMilli(1698235273501L),
                                                ZoneId.of("UTC"))
                                        .plusNanos(8000),
                                new byte[] {5, 6, 7, 8},
                                Row.of(10, "nested_string")));
            } else {
                rows.add(
                        row(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_" + i,
                                Decimal.fromUnscaledLong(900, 5, 2),
                                Decimal.fromBigDecimal(new java.math.BigDecimal(1000), 20, 0),
                                TimestampLtz.fromEpochMillis(1698235273400L),
                                TimestampLtz.fromEpochMillis(1698235273400L, 7000),
                                TimestampNtz.fromMillis(1698235273501L),
                                TimestampNtz.fromMillis(1698235273501L, 8000),
                                new byte[] {5, 6, 7, 8},
                                row(10, "nested_string"),
                                partition));

                flinkRows.add(
                        Row.of(
                                true,
                                (byte) 100,
                                (short) 200,
                                30,
                                400L,
                                500.1f,
                                600.0d,
                                "another_string_" + i,
                                new java.math.BigDecimal("9.00"),
                                new java.math.BigDecimal("1000"),
                                Instant.ofEpochMilli(1698235273400L),
                                Instant.ofEpochMilli(1698235273400L).plusNanos(7000),
                                LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(1698235273501L), ZoneId.of("UTC")),
                                LocalDateTime.ofInstant(
                                                Instant.ofEpochMilli(1698235273501L),
                                                ZoneId.of("UTC"))
                                        .plusNanos(8000),
                                new byte[] {5, 6, 7, 8},
                                Row.of(10, "nested_string"),
                                partition));
            }
        }
        writeRows(tablePath, rows, true);
        return flinkRows;
    }

    private List<Row> writeRows(TablePath tablePath, int rowCount, boolean isPartitioned)
            throws Exception {
        if (isPartitioned) {
            List<Row> rows = new ArrayList<>();
            for (String partition : waitUntilPartitions(tablePath).values()) {
                rows.addAll(writeFullTypeRows(tablePath, rowCount, partition));
            }
            return rows;
        } else {
            return writeFullTypeRows(tablePath, rowCount, null);
        }
    }
}
