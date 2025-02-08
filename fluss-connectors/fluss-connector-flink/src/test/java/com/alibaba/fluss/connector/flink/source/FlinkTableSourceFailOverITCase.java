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

package com.alibaba.fluss.connector.flink.source;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.RestoreMode;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static com.alibaba.fluss.connector.flink.FlinkConnectorOptions.BOOTSTRAP_SERVERS;
import static com.alibaba.fluss.connector.flink.source.testutils.FlinkTestBase.waitUntilPartitions;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;

class FlinkTableSourceFailOverITCase {

    private static final String CATALOG_NAME = "testcatalog";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(
                            new com.alibaba.fluss.config.Configuration()
                                    .set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofDays(1)))
                    .setNumOfTabletServers(3)
                    .build();

    @TempDir File checkpointDir;
    @TempDir File savepointDir;

    protected static Connection conn;
    protected static Admin admin;

    protected static com.alibaba.fluss.config.Configuration clientConf;

    @BeforeAll
    protected static void beforeAll() {
        clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @Test
    void t1() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;
        final int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final MiniClusterResourceFactory clusterFactory =
                new MiniClusterResourceFactory(
                        numTaskManagers,
                        numSlotsPerTaskManager,
                        getFileBasedCheckpointsConfig(savepointDir));

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(parallelism);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss-aplus', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);

        tEnv.executeSql(
                "create table test_partitioned("
                        + "a int, b string, c varchar, primary key(a, c) not enforced"
                        + ") partitioned by (c) "
                        + "with ("
                        + "'table.auto-partition.enabled' = 'true',"
                        + "'table.auto-partition.time-unit' = 'day',"
                        + "'scan.partition.discovery.interval' = '100ms',"
                        + "'table.auto-partition.num-precreate' = '1')");

        TablePath tablePath = TablePath.of("fluss", "test_partitioned");
        Map<Long, String> partitionNameById =
                waitUntilPartitions(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(), tablePath, 1);

        Collection<String> partitions = partitionNameById.values();

        // prepare table data
        try (com.alibaba.fluss.client.table.Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.getUpsertWriter();
            RowType rowType = table.getDescriptor().getSchema().toRowType();
            for (int i = 1; i <= 3; i++) {
                for (String partition : partitions) {
                    Object[] values = new Object[] {i, "address" + i, partition};
                    upsertWriter.upsert(compactedRow(rowType, values));
                    // make sure every bucket has records
                    upsertWriter.flush();
                }
            }
        }

        Table table =
                tEnv.sqlQuery(
                        "select * from test_partitioned /*+ OPTIONS('scan.startup.mode' = 'latest') */");
        tEnv.toChangelogStream(table).addSink(new DiscardingSink<>());

        JobGraph jobGraph = execEnv.getStreamGraph().getJobGraph();

        JobID jobId = jobGraph.getJobID();

        MiniClusterWithClientResource cluster = clusterFactory.get();
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();

        String savePointPath;

        try {
            client.submitJob(jobGraph).get();
            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
            Thread.sleep(5000);

            // now, stop the job with save point
            savePointPath =
                    client.cancelWithSavepoint(jobId, null, SavepointFormatType.CANONICAL).get();

            execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            execEnv.setParallelism(parallelism);
            tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
            // crate catalog using sql
            tEnv.executeSql(
                    String.format(
                            "create catalog %s with ('type' = 'fluss-aplus', '%s' = '%s')",
                            CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
            tEnv.executeSql("use catalog " + CATALOG_NAME);
            table =
                    tEnv.sqlQuery(
                            "select * from test_partitioned  /*+ OPTIONS('scan.startup.mode' = 'latest') */");
            tEnv.toChangelogStream(table).addSink(new DiscardingSink<>());
            jobGraph = execEnv.getStreamGraph().getJobGraph();
            SavepointRestoreSettings savepointRestoreSettings =
                    SavepointRestoreSettings.forPath(savePointPath, false, RestoreMode.CLAIM.CLAIM);
            jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);
            client.submitJob(jobGraph).get();
            jobId = jobGraph.getJobID();

            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
            Thread.sleep(5000);
        } finally {
            cluster.after();
        }
    }

    @Test
    void testRestore() throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;
        final int parallelism = numTaskManagers * numSlotsPerTaskManager;

        final MiniClusterResourceFactory clusterFactory =
                new MiniClusterResourceFactory(
                        numTaskManagers,
                        numSlotsPerTaskManager,
                        getFileBasedCheckpointsConfig(savepointDir));

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(parallelism);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
        String bootstrapServers = String.join(",", clientConf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        // crate catalog using sql
        tEnv.executeSql(
                String.format(
                        "create catalog %s with ('type' = 'fluss-aplus', '%s' = '%s')",
                        CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
        tEnv.executeSql("use catalog " + CATALOG_NAME);

        tEnv.executeSql(
                "create table test_partitioned("
                        + "a int, b varchar"
                        + ") partitioned by (b) "
                        + "with ("
                        + "'table.auto-partition.enabled' = 'true',"
                        + "'table.auto-partition.time-unit' = 'year',"
                        + "'scan.partition.discovery.interval' = '100ms',"
                        + "'table.auto-partition.num-precreate' = '1')");

        tEnv.executeSql(
                "create temporary table test_partitioned_sink(a int, b varchar) with ('connector' = 'blackhole')");

        Table table = tEnv.sqlQuery("select * from test_partitioned");
        tEnv.toDataStream(table).addSink(new DiscardingSink<>());

        JobGraph jobGraph = execEnv.getStreamGraph().getJobGraph();

        JobID jobId = jobGraph.getJobID();

        MiniClusterWithClientResource cluster = clusterFactory.get();
        cluster.before();
        ClusterClient<?> client = cluster.getClusterClient();

        String savePointPath;

        try {
            client.submitJob(jobGraph).get();
            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);

            // create a partition manually
            FlinkTestBase.createPartitions(
                    FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                    TablePath.of("fluss", "test_partitioned"),
                    Collections.singletonList("2000"));
            // wait from a while to wait the source discovery the partition changes.
            Thread.sleep(3000);

            // drop a partition manually,
            FlinkTestBase.dropPartitions(
                    FLUSS_CLUSTER_EXTENSION.getZooKeeperClient(),
                    TablePath.of("fluss", "test_partitioned"),
                    Collections.singleton("2000"));

            // wait from a while to wait the source unsubscribe the partition
            Thread.sleep(3000);

            // now, stop the job with save point
            savePointPath =
                    client.cancelWithSavepoint(jobId, null, SavepointFormatType.CANONICAL).get();

            execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            execEnv.setParallelism(parallelism);
            tEnv = StreamTableEnvironment.create(execEnv, EnvironmentSettings.inStreamingMode());
            // crate catalog using sql
            tEnv.executeSql(
                    String.format(
                            "create catalog %s with ('type' = 'fluss-aplus', '%s' = '%s')",
                            CATALOG_NAME, BOOTSTRAP_SERVERS.key(), bootstrapServers));
            tEnv.executeSql("use catalog " + CATALOG_NAME);
            table = tEnv.sqlQuery("select * from test_partitioned");
            tEnv.toDataStream(table).addSink(new DiscardingSink<>());
            jobGraph = execEnv.getStreamGraph().getJobGraph();
            SavepointRestoreSettings savepointRestoreSettings =
                    SavepointRestoreSettings.forPath(savePointPath, false, RestoreMode.CLAIM.CLAIM);
            jobGraph.setSavepointRestoreSettings(savepointRestoreSettings);
            client.submitJob(jobGraph).get();
            jobId = jobGraph.getJobID();

            waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
        } finally {
            cluster.after();
        }
    }

    private Configuration getFileBasedCheckpointsConfig(File savepointDir) {
        return getFileBasedCheckpointsConfig(savepointDir.toURI().toString());
    }

    private Configuration getFileBasedCheckpointsConfig(final String savepointDir) {
        final Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
        return config;
    }

    private static class MiniClusterResourceFactory {
        private final int numTaskManagers;
        private final int numSlotsPerTaskManager;
        private final Configuration config;

        private MiniClusterResourceFactory(
                int numTaskManagers, int numSlotsPerTaskManager, Configuration config) {
            this.numTaskManagers = numTaskManagers;
            this.numSlotsPerTaskManager = numSlotsPerTaskManager;
            this.config = config;
        }

        MiniClusterWithClientResource get() {
            return new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(config)
                            .setNumberTaskManagers(numTaskManagers)
                            .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                            .build());
        }
    }
}
