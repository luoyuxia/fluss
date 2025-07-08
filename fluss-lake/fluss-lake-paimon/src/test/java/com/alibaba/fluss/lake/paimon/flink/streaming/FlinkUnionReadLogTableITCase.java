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

package com.alibaba.fluss.lake.paimon.flink.streaming;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.lake.paimon.testutils.FlinkPaimonTieringTestBase;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.testutils.DataTestUtils.row;

class FlinkUnionReadLogTableITCase extends FlinkPaimonTieringTestBase {

    StreamTableEnvironment streamTableEnv;

    @BeforeAll
    protected static void beforeAll() {
        FlinkPaimonTieringTestBase.beforeAll();
    }

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        streamTableEnv =
                StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        // crate catalog using sql
        streamTableEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        streamTableEnv.executeSql("use catalog " + CATALOG_NAME);
        streamTableEnv.executeSql("use " + DEFAULT_DB);
    }

    @Test
    void testStreamUnionReadLogTable() throws Exception {
        // first of all, start tiering
        JobClient jobClient = buildTieringJob(execEnv);
        String tableName = "log_table";
        TablePath t1 = TablePath.of(DEFAULT_DB, tableName);

        List<Row> writtenRows = new ArrayList<>();
        long tableId = prepareLogTable(t1, 3, writtenRows);
        // wait until records has been synced
        waitUtilBucketSynced(t1, tableId, 3, false);

        jobClient.cancel().get();

        writeRows(t1, 10, writtenRows);

        streamTableEnv.executeSql("select * from " + tableName).print();
    }

    private long prepareLogTable(TablePath tablePath, int bucketNum, List<Row> flinkRows)
            throws Exception {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().column("a", DataTypes.INT()).column("b", DataTypes.STRING());

        TableDescriptor.Builder tableBuilder =
                TableDescriptor.builder()
                        .distributedBy(bucketNum, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, Duration.ofMillis(500));

        tableBuilder.schema(schemaBuilder.build());
        long t1Id = createTable(tablePath, tableBuilder.build());
        writeRows(tablePath, 0, flinkRows);
        return t1Id;
    }

    private void writeRows(TablePath tablePath, int startPos, List<Row> flinkRows)
            throws Exception {
        List<InternalRow> flussRows = new ArrayList<>();
        for (int i = startPos; i < startPos + 10; i++) {
            flinkRows.add(Row.of(i, "row" + i));
            flussRows.add(row(i, "row" + i));
        }
        writeRows(tablePath, flussRows, true);
    }
}
