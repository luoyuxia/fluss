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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.LakeTieringTaskType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PbLakeTieringTableInfo;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.BootstrapUpgradeState;
import org.apache.fluss.server.zk.data.BootstrapUpgradeStatus;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newAlterTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for bootstrap lifecycle transitions on tiering heartbeat path. */
class BootstrapUpgradeLifecycleITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(Configuration.fromMap(getDataLakeFormat()))
                    .build();

    private static CoordinatorGateway coordinatorGateway;

    @BeforeAll
    static void beforeAll() {
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
    }

    @Test
    void testBootstrapTaskDispatchedFromZkState() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_bootstrap_lake_table");
        createLakeTable(tablePath, Duration.ofMillis(100));

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();
        FLUSS_CLUSTER_EXTENSION
                .getZooKeeperClient()
                .upsertBootstrapUpgradeState(
                        tableId,
                        new BootstrapUpgradeState(
                                BootstrapUpgradeStatus.IN_PROGRESS, "dt=2026-03-23"),
                        false);

        PbLakeTieringTableInfo bootstrapTask =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait bootstrap task dispatched from znode state.");
        assertThat(bootstrapTask.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());
        assertThat(bootstrapTask.getHoldPartition()).isEqualTo("dt=2026-03-23");
    }

    @Test
    void testBootstrapZnodeInitializedAndEnqueuedOnCreateTable() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_bootstrap_create_table");
        createBootstrapLakeTable(tablePath, "dt=2026-03-23", Duration.ofMinutes(5));

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        BootstrapUpgradeState bootstrapUpgradeState =
                waitValue(
                        () ->
                                FLUSS_CLUSTER_EXTENSION
                                        .getZooKeeperClient()
                                        .getBootstrapUpgradeState(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait bootstrap-upgrade znode initialized.");
        assertThat(bootstrapUpgradeState.getStatus()).isEqualTo(BootstrapUpgradeStatus.IN_PROGRESS);
        assertThat(bootstrapUpgradeState.getHoldPartition()).isEqualTo("dt=2026-03-23");

        PbLakeTieringTableInfo bootstrapTask =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait bootstrap task after create table.");
        assertThat(bootstrapTask.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());
        assertThat(bootstrapTask.getHoldPartition()).isEqualTo("dt=2026-03-23");
    }

    @Test
    void testBootstrapCompleteFallsBackToNormalTieringTask() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_bootstrap_complete_fallback");
        createBootstrapLakeTable(tablePath, "dt=2026-03-27", Duration.ofMillis(100));

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        PbLakeTieringTableInfo bootstrapTask =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait bootstrap tiering task.");
        assertThat(bootstrapTask.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());
        assertThat(bootstrapTask.getHoldPartition()).isEqualTo("dt=2026-03-27");

        LakeTieringHeartbeatRequest finishRequest = new LakeTieringHeartbeatRequest();
        finishRequest
                .addFinishedTable()
                .setTableId(tableId)
                .setCoordinatorEpoch(INITIAL_COORDINATOR_EPOCH)
                .setTieringEpoch((int) bootstrapTask.getTieringEpoch());
        LakeTieringHeartbeatResponse finishResponse =
                coordinatorGateway.lakeTieringHeartbeat(finishRequest).get();
        assertThat(finishResponse.getFinishedTableRespsList()).hasSize(1);
        assertThat(finishResponse.getFinishedTableRespAt(0).hasError()).isFalse();

        BootstrapUpgradeState completeState =
                waitValue(
                        () ->
                                FLUSS_CLUSTER_EXTENSION
                                        .getZooKeeperClient()
                                        .getBootstrapUpgradeState(tableId)
                                        .filter(
                                                s ->
                                                        s.getStatus()
                                                                == BootstrapUpgradeStatus.COMPLETE),
                        Duration.ofMinutes(1),
                        "Fail to wait bootstrap-upgrade state to become COMPLETE.");
        assertThat(completeState.getHoldPartition()).isEqualTo("dt=2026-03-27");

        PbLakeTieringTableInfo normalTask =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait normal tiering task after bootstrap completion.");
        assertThat(normalTask.getTaskType()).isEqualTo(LakeTieringTaskType.NORMAL_TIERING.code());
        assertThat(normalTask.hasHoldPartition()).isFalse();
    }

    @Test
    void testBootstrapStateRemainsInProgressOnFailureAndReassigned() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_bootstrap_fail_retry");
        createBootstrapLakeTable(tablePath, "dt=2026-03-26", Duration.ofMinutes(5));

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        PbLakeTieringTableInfo assignedTable =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait initial bootstrap task for failure retry.");
        assertThat(assignedTable.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());

        LakeTieringHeartbeatRequest failRequest = new LakeTieringHeartbeatRequest();
        failRequest
                .addFailedTable()
                .setTableId(tableId)
                .setCoordinatorEpoch(INITIAL_COORDINATOR_EPOCH)
                .setTieringEpoch((int) assignedTable.getTieringEpoch());
        LakeTieringHeartbeatResponse failResponse =
                coordinatorGateway.lakeTieringHeartbeat(failRequest).get();
        assertThat(failResponse.getFailedTableRespsList()).hasSize(1);
        assertThat(failResponse.getFailedTableRespsList().get(0).hasError()).isFalse();

        BootstrapUpgradeState stateAfterFailure =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getBootstrapUpgradeState(tableId)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Bootstrap-upgrade state should exist after failure."));
        assertThat(stateAfterFailure.getStatus()).isEqualTo(BootstrapUpgradeStatus.IN_PROGRESS);
        assertThat(stateAfterFailure.getHoldPartition()).isEqualTo("dt=2026-03-26");

        PbLakeTieringTableInfo reassignedTable =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait reassigned bootstrap task after failure.");
        assertThat(reassignedTable.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());
        assertThat(reassignedTable.getTieringEpoch())
                .isEqualTo(assignedTable.getTieringEpoch() + 1);
    }

    @Test
    void testDropTableDeletesBootstrapState() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_bootstrap_drop_table");
        createBootstrapLakeTable(tablePath, "dt=2026-03-25", Duration.ofMinutes(5));

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        waitValue(
                () ->
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getBootstrapUpgradeState(tableId),
                Duration.ofMinutes(1),
                "Fail to wait bootstrap-upgrade znode initialized.");

        adminGateway
                .dropTable(
                        newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();

        waitValue(
                () -> {
                    if (FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getBootstrapUpgradeState(tableId)
                            .isPresent()) {
                        return Optional.empty();
                    }
                    return Optional.of(Boolean.TRUE);
                },
                Duration.ofMinutes(1),
                "Fail to wait bootstrap-upgrade znode deleted after drop table.");
    }

    @Test
    void testDisableDataLakeDeletesBootstrapState() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_disable_datalake_delete_bootstrap_state");
        createBootstrapLakeTable(tablePath, "dt=2026-03-28", Duration.ofMinutes(5));

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        waitValue(
                () ->
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getBootstrapUpgradeState(tableId),
                Duration.ofMinutes(1),
                "Fail to wait bootstrap-upgrade znode initialized.");

        Map<String, String> setProperties = new HashMap<>();
        setProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "false");
        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                setProperties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();

        waitValue(
                () -> {
                    if (FLUSS_CLUSTER_EXTENSION
                            .getZooKeeperClient()
                            .getBootstrapUpgradeState(tableId)
                            .isPresent()) {
                        return Optional.empty();
                    }
                    return Optional.of(Boolean.TRUE);
                },
                Duration.ofMinutes(1),
                "Fail to wait bootstrap-upgrade znode deleted after disabling datalake.");
    }

    @Test
    void testEnableDataLakeWithBootstrapConfigInitializesAndDispatchesBootstrap() throws Exception {
        TablePath tablePath =
                TablePath.of("fluss", "test_enable_datalake_bootstrap_initialization");
        createNonLakeTableWithBootstrapConfig(tablePath);

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        Map<String, String> setProperties = new HashMap<>();
        setProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        setProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "100 ms");
        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                setProperties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();

        BootstrapUpgradeState bootstrapState =
                waitValue(
                        () ->
                                FLUSS_CLUSTER_EXTENSION
                                        .getZooKeeperClient()
                                        .getBootstrapUpgradeState(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait bootstrap-upgrade znode initialized after enabling datalake.");
        assertThat(bootstrapState.getStatus()).isEqualTo(BootstrapUpgradeStatus.IN_PROGRESS);
        assertThat(bootstrapState.getHoldPartition()).isEqualTo("dt=2026-03-29");

        PbLakeTieringTableInfo bootstrapTask =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait bootstrap task after enabling datalake.");
        assertThat(bootstrapTask.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());
        assertThat(bootstrapTask.getHoldPartition()).isEqualTo("dt=2026-03-29");
    }

    @Test
    void testEnableDataLakeWithoutBootstrapConfigStaysNormalTiering() throws Exception {
        TablePath tablePath =
                TablePath.of("fluss", "test_enable_datalake_without_bootstrap_config");
        createNonLakeTableWithoutBootstrapConfig(tablePath);

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        Map<String, String> setProperties = new HashMap<>();
        setProperties.put(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true");
        setProperties.put(ConfigOptions.TABLE_DATALAKE_FRESHNESS.key(), "100 ms");
        adminGateway
                .alterTable(
                        newAlterTableRequest(
                                tablePath,
                                setProperties,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                false))
                .get();

        // no bootstrap config means no bootstrap-upgrade znode should be created
        assertThat(FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getBootstrapUpgradeState(tableId))
                .isEmpty();

        PbLakeTieringTableInfo normalTask =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait normal tiering task after enabling datalake without bootstrap.");
        assertThat(normalTask.getTaskType()).isEqualTo(LakeTieringTaskType.NORMAL_TIERING.code());
        assertThat(normalTask.hasHoldPartition()).isFalse();
    }

    @Test
    void testCoordinatorRestartReEnqueuesBootstrapInProgressTable() throws Exception {
        TablePath tablePath =
                TablePath.of("fluss", "test_bootstrap_reenqueue_after_coordinator_restart");
        createBootstrapLakeTable(tablePath, "dt=2026-03-30", Duration.ofMinutes(5));

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        waitValue(
                () ->
                        FLUSS_CLUSTER_EXTENSION
                                .getZooKeeperClient()
                                .getBootstrapUpgradeState(tableId),
                Duration.ofMinutes(1),
                "Fail to wait bootstrap-upgrade znode initialized before restart.");

        FLUSS_CLUSTER_EXTENSION.stopCoordinatorServer();
        FLUSS_CLUSTER_EXTENSION.startCoordinatorServer();
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();

        PbLakeTieringTableInfo bootstrapTaskAfterRestart =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait bootstrap task after coordinator restart.");
        assertThat(bootstrapTaskAfterRestart.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());
        assertThat(bootstrapTaskAfterRestart.getHoldPartition()).isEqualTo("dt=2026-03-30");
    }

    @Test
    void testForceFinishKeepsBootstrapInProgressAndReassignsBootstrapTask() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_bootstrap_force_finish_reassign");
        createBootstrapLakeTable(tablePath, "dt=2026-03-31", Duration.ofMinutes(5));

        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long tableId =
                adminGateway.getTableInfo(newGetTableInfoRequest(tablePath)).get().getTableId();

        PbLakeTieringTableInfo assignedTask =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait initial bootstrap task for force-finish case.");
        assertThat(assignedTask.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());

        LakeTieringHeartbeatRequest forceFinishRequest = new LakeTieringHeartbeatRequest();
        forceFinishRequest.addForceFinishedTables(tableId);
        forceFinishRequest
                .addFinishedTable()
                .setTableId(tableId)
                .setCoordinatorEpoch(INITIAL_COORDINATOR_EPOCH)
                .setTieringEpoch((int) assignedTask.getTieringEpoch());
        LakeTieringHeartbeatResponse forceFinishResponse =
                coordinatorGateway.lakeTieringHeartbeat(forceFinishRequest).get();
        assertThat(forceFinishResponse.getFinishedTableRespsList()).hasSize(1);
        assertThat(forceFinishResponse.getFinishedTableRespAt(0).hasError()).isFalse();

        BootstrapUpgradeState stateAfterForceFinish =
                FLUSS_CLUSTER_EXTENSION
                        .getZooKeeperClient()
                        .getBootstrapUpgradeState(tableId)
                        .orElseThrow(
                                () ->
                                        new AssertionError(
                                                "Bootstrap-upgrade state should exist after force finish."));
        assertThat(stateAfterForceFinish.getStatus()).isEqualTo(BootstrapUpgradeStatus.IN_PROGRESS);
        assertThat(stateAfterForceFinish.getHoldPartition()).isEqualTo("dt=2026-03-31");

        PbLakeTieringTableInfo reassignedBootstrapTask =
                waitValue(
                        () -> requestTableFor(tableId),
                        Duration.ofMinutes(1),
                        "Fail to wait reassigned bootstrap task after force finish.");
        assertThat(reassignedBootstrapTask.getTaskType())
                .isEqualTo(LakeTieringTaskType.BOOTSTRAP_UPGRADE.code());
        assertThat(reassignedBootstrapTask.getTieringEpoch())
                .isEqualTo(assignedTask.getTieringEpoch() + 1);
    }

    private static Optional<PbLakeTieringTableInfo> requestTableFor(long expectedTableId)
            throws Exception {
        LakeTieringHeartbeatRequest heartbeatRequest = new LakeTieringHeartbeatRequest();
        heartbeatRequest.setRequestTable(true);
        LakeTieringHeartbeatResponse response =
                coordinatorGateway.lakeTieringHeartbeat(heartbeatRequest).get();
        if (!response.hasTieringTable()) {
            return Optional.empty();
        }
        PbLakeTieringTableInfo tableInfo = response.getTieringTable();
        if (tableInfo.getTableId() != expectedTableId) {
            return Optional.empty();
        }
        return Optional.of(tableInfo);
    }

    private static void createBootstrapLakeTable(
            TablePath tablePath, String holdPartition, Duration freshness) throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property("table.datalake.enabled", "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, freshness)
                        .customProperty("table.datalake.bootstrap.enabled", "true")
                        .customProperty("table.datalake.bootstrap.cutover-partition", holdPartition)
                        .build();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
    }

    private static void createLakeTable(TablePath tablePath, Duration freshness) throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property("table.datalake.enabled", "true")
                        .property(ConfigOptions.TABLE_DATALAKE_FRESHNESS, freshness)
                        .build();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
    }

    private static void createNonLakeTableWithBootstrapConfig(TablePath tablePath)
            throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property("table.datalake.enabled", "false")
                        .customProperty("table.datalake.bootstrap.enabled", "true")
                        .customProperty(
                                "table.datalake.bootstrap.cutover-partition", "dt=2026-03-29")
                        .build();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
    }

    private static void createNonLakeTableWithoutBootstrapConfig(TablePath tablePath)
            throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .property("table.datalake.enabled", "false")
                        .build();
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();
    }

    private static Map<String, String> getDataLakeFormat() {
        Map<String, String> datalakeFormat = new HashMap<>();
        datalakeFormat.put(ConfigOptions.DATALAKE_FORMAT.key(), DataLakeFormat.PAIMON.toString());
        return datalakeFormat;
    }
}
