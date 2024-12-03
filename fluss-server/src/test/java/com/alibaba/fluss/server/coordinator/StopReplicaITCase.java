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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.testutils.RpcMessageTestUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_INFO_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for stop replica. */
public class StopReplicaITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;
    private CoordinatorGateway coordinatorGateway;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStopReplica(boolean isPkTable) throws Exception {
        TablePath tablePath = isPkTable ? DATA1_TABLE_PATH_PK : DATA1_TABLE_PATH;
        TableInfo tableInfo = isPkTable ? DATA1_TABLE_INFO_PK : DATA1_TABLE_INFO;

        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableInfo.getTableDescriptor());
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        List<Integer> isr = waitAndGetIsr(tb);
        List<Path> tableDirs = assertReplicaExistAndGetTableOrPartitionDirs(tb, isr, isPkTable);
        // drop table.
        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        retryUtilReplicaNotExist(tb, isr, tableDirs);
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        // create this table again.
        tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableInfo.getTableDescriptor());
        TableBucket tb1 = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb1);

        isr = waitAndGetIsr(tb1);
        tableDirs = assertReplicaExistAndGetTableOrPartitionDirs(tb1, isr, isPkTable);
        // drop table.
        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        // create this table even if the drop table operation may not be completed.
        tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableInfo.getTableDescriptor());
        TableBucket tb2 = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb2);

        List<Integer> isr2 = waitAndGetIsr(tb2);
        List<Path> tableDirs2 = assertReplicaExistAndGetTableOrPartitionDirs(tb2, isr2, isPkTable);
        // drop table.
        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        retryUtilReplicaNotExist(tb, isr, tableDirs);
        retryUtilReplicaNotExist(tb, isr2, tableDirs2);
    }

    private List<Integer> waitAndGetIsr(TableBucket tb) {
        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofMinutes(1),
                        "leaderAndIsr is not ready");
        return leaderAndIsr.isr();
    }

    private List<Path> assertReplicaExistAndGetTableOrPartitionDirs(
            TableBucket tableBucket, List<Integer> isr, boolean isKvTable) {
        List<Path> tableDirs = new ArrayList<>();
        for (int replicaId : isr) {
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(replicaId).getReplicaManager();
            assertThat(replicaManager.getReplica(tableBucket))
                    .isInstanceOf(ReplicaManager.OnlineReplica.class);
            Replica replica = replicaManager.getReplicaOrException(tableBucket);
            Path dir = replica.getTabletParentDir();
            assertThat(dir).exists();
            tableDirs.add(dir);

            assertThat(replica.getLogTablet().getLogDir()).exists();
            if (isKvTable) {
                // wait the replica become leader, so that we can get the kv tablet
                replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(replica.getTableBucket());
                assertThat(replica.getKvTablet()).isNotNull();
                assertThat(replica.getKvTablet().getKvTabletDir()).exists();
            }
        }

        return tableDirs;
    }

    private void retryUtilReplicaNotExist(
            TableBucket tableBucket, List<Integer> isr, List<Path> tableDirs) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    // the local table dir should be removed
                    tableDirs.forEach(tableDir -> assertThat(tableDir).doesNotExist());

                    // Replicas in TabletServers should be shutdown
                    isr.forEach(
                            replicaId -> {
                                ReplicaManager replicaManager =
                                        FLUSS_CLUSTER_EXTENSION
                                                .getTabletServerById(replicaId)
                                                .getReplicaManager();
                                assertThat(replicaManager.getReplica(tableBucket))
                                        .isInstanceOf(ReplicaManager.NoneReplica.class);
                            });

                    // at last, when all Replicas are shutdown, the zk data should be removed.
                    assertThat(zkClient.getTableAssignment(tableBucket.getTableId())).isEmpty();
                    assertThat(zkClient.getLeaderAndIsr(tableBucket)).isEmpty();
                });
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }
}
