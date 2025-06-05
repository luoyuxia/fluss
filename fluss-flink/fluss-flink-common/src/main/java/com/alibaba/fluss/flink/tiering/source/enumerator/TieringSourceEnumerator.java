/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.source.enumerator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.metrics.FlinkMetricRegistry;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplit;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplitGenerator;
import com.alibaba.fluss.flink.tiering.source.state.TieringSourceEnumeratorState;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import com.alibaba.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import com.alibaba.fluss.rpc.messages.PbHeartbeatReqForTable;
import com.alibaba.fluss.rpc.messages.PbLakeTieringTableInfo;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.utils.types.Tuple2;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * An implementation of {@link SplitEnumerator} used to request {@link TieringSplit} from Fluss
 * Cluster.
 *
 * <p>The enumerator is responsible for:
 *
 * <ul>
 *   <li>Register the Tiering Service job that the current TieringSourceEnumerator belongs to with
 *       the Fluss Cluster when the Flink Tiering job starts up.
 *   <li>Request Fluss table splits from Fluss Cluster and assigns to SourceReader to tier.
 *   <li>Un-Register the Tiering Service job that the current TieringSourceEnumerator belongs to
 *       with the Fluss Cluster when the Flink Tiering job shutdown as much as possible.
 * </ul>
 */
public class TieringSourceEnumerator
        implements SplitEnumerator<TieringSplit, TieringSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(TieringSourceEnumerator.class);

    private final Configuration flussConf;
    private final SplitEnumeratorContext<TieringSplit> context;
    private final SplitEnumeratorMetricGroup enumeratorMetricGroup;
    private final long pollTieringTableIntervalMs;
    private final List<TieringSplit> pendingSplits;
    private final Set<Integer> readersAwaitingSplit;

    // lazily instantiated
    private RpcClient rpcClient;
    private CoordinatorGateway coordinatorGateway;
    private Connection connection;
    private Admin flussAdmin;
    private TieringSplitGenerator splitGenerator;

    private int flussCoordinatorEpoch;

    public TieringSourceEnumerator(
            Configuration flussConf,
            SplitEnumeratorContext<TieringSplit> context,
            long pollTieringTableIntervalMs) {
        this.flussConf = flussConf;
        this.context = context;
        this.enumeratorMetricGroup = context.metricGroup();
        this.pendingSplits = new ArrayList<>();
        this.readersAwaitingSplit = new TreeSet<>();
        this.pollTieringTableIntervalMs = pollTieringTableIntervalMs;
    }

    @Override
    public void start() {
        connection = ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        FlinkMetricRegistry metricRegistry = new FlinkMetricRegistry(enumeratorMetricGroup);
        ClientMetricGroup clientMetricGroup =
                new ClientMetricGroup(metricRegistry, "LakeTieringService");
        this.rpcClient = RpcClient.create(flussConf, clientMetricGroup);
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
        this.splitGenerator = new TieringSplitGenerator(flussAdmin);

        LOG.info("Starting register Tiering Service to Fluss Coordinator...");
        try {
            LakeTieringHeartbeatResponse heartbeatResponse =
                    HeartBeatHelper.waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(
                                    HeartBeatHelper.registerHeartBeat()));
            this.flussCoordinatorEpoch = heartbeatResponse.getCoordinatorEpoch();
            LOG.info(
                    "Register Tiering Service to Fluss Coordinator(epoch={}) success.",
                    flussCoordinatorEpoch);

        } catch (Exception e) {
            LOG.error("Register Tiering Service failed due to ", e);
            throw new FlinkRuntimeException("Register Tiering Service failed due to ", e);
        }
        this.context.callAsync(
                this::requestTieringTableSplitsViaHeartBeat,
                this::generateAndAssignSplits,
                0,
                pollTieringTableIntervalMs);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader may be failed, skip this request.
            return;
        }
        LOG.info("TiringSourceReader {} requests split.", subtaskId);
        readersAwaitingSplit.add(subtaskId);
        this.context.callAsync(
                this::requestTieringTableSplitsViaHeartBeat, this::generateAndAssignSplits);
    }

    @Override
    public void addSplitsBack(List<TieringSplit> splits, int subtaskId) {
        readersAwaitingSplit.add(subtaskId);
        pendingSplits.addAll(splits);
        this.context.callAsync(
                this::requestTieringTableSplitsViaHeartBeat, this::generateAndAssignSplits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader: {} to Tiering Source enumerator.", subtaskId);
        if (context.registeredReaders().containsKey(subtaskId)) {
            readersAwaitingSplit.add(subtaskId);
        }
    }

    private void generateAndAssignSplits(
            Tuple2<Long, TablePath> tieringTable, Throwable throwable) {
        checkState(throwable == null, "Failed to request tiering table due to:", throwable);
        if (tieringTable != null) {
            generateTieringSplits(tieringTable);
        }
        assignSplits();
    }

    private void assignSplits() {
        if (!readersAwaitingSplit.isEmpty()) {
            final Integer[] readers = readersAwaitingSplit.toArray(new Integer[0]);
            for (Integer nextAwaitingReader : readers) {
                if (!context.registeredReaders().containsKey(nextAwaitingReader)) {
                    readersAwaitingSplit.remove(nextAwaitingReader);
                    continue;
                }
                if (!pendingSplits.isEmpty()) {
                    TieringSplit tieringSplit = pendingSplits.remove(0);
                    context.assignSplit(tieringSplit, nextAwaitingReader);
                    readersAwaitingSplit.remove(nextAwaitingReader);
                }
            }
        }
    }

    private Tuple2<Long, TablePath> requestTieringTableSplitsViaHeartBeat() {
        if (pendingSplits.isEmpty() && !readersAwaitingSplit.isEmpty()) {
            LakeTieringHeartbeatResponse heartbeatResponse =
                    HeartBeatHelper.waitHeartbeatResponse(
                            coordinatorGateway.lakeTieringHeartbeat(
                                    HeartBeatHelper.requestTieringTableHeartBeat()));
            if (heartbeatResponse.hasTieringTable()) {
                PbLakeTieringTableInfo tieringTable = heartbeatResponse.getTieringTable();
                return Tuple2.of(
                        tieringTable.getTableId(),
                        TablePath.of(
                                tieringTable.getTablePath().getDatabaseName(),
                                tieringTable.getTablePath().getTableName()));
            } else {
                LOG.info("No available Tiering table found, will poll later.");
            }
        }
        return null;
    }

    private void generateTieringSplits(Tuple2<Long, TablePath> tieringTable)
            throws FlinkRuntimeException {
        if (tieringTable == null) {
            return;
        }
        long start = System.currentTimeMillis();
        LOG.info("Generate Tiering splits for table {}.", tieringTable.f1);
        try {
            List<TieringSplit> tieringSplits = splitGenerator.generateTableSplits(tieringTable.f1);
            LOG.info(
                    "Generate Tiering {} splits for table {} with cost {}ms.",
                    tieringSplits.size(),
                    tieringTable.f1,
                    System.currentTimeMillis() - start);
            if (tieringSplits.isEmpty()) {
                LOG.info(
                        "Generate Tiering splits for table {} is empty, no need to tier data.",
                        tieringTable.f1.getTableName());
                reportFinishedTieringTable(tieringTable.f0);

            } else {
                pendingSplits.addAll(tieringSplits);
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Generate Tiering splits for table %s failed due to:", tieringTable.f1),
                    e);
        }
    }

    @Override
    public TieringSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        // do nothing, the downstream lake commiter will snapshot the state to Fluss Cluster
        return new TieringSourceEnumeratorState();
    }

    @Override
    public void close() throws IOException {
        if (rpcClient != null) {
            reportFailedTieringTable();
            try {
                LOG.info("Closing Tiering Source Enumerator of at {}.", System.currentTimeMillis());
                rpcClient.close();
            } catch (Exception e) {
                LOG.error("Failed to close Tiering Source enumerator.", e);
            }
        }
        try {
            if (flussAdmin != null) {
                LOG.info("Closing Fluss Admin client...");
                flussAdmin.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close Fluss Admin client.", e);
        }
        try {
            if (connection != null) {
                LOG.info("Closing Fluss connection...");
                connection.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close Fluss connection.", e);
        }
    }

    private void reportFailedTieringTable() throws FlinkRuntimeException {
        try {
            HeartBeatHelper.waitHeartbeatResponse(
                    coordinatorGateway.lakeTieringHeartbeat(
                            HeartBeatHelper.failedTieringTableHeartBeat(
                                    pendingSplits, flussCoordinatorEpoch)));
            LOG.info("Report failed table to Fluss Coordinator success");

        } catch (Exception e) {
            LOG.error("Errors happens when report failed table to Fluss cluster.", e);
            throw new FlinkRuntimeException(
                    "Errors happens when report failed table to Fluss cluster.", e);
        }
    }

    private void reportFinishedTieringTable(long tableId) throws FlinkRuntimeException {
        try {
            HeartBeatHelper.waitHeartbeatResponse(
                    coordinatorGateway.lakeTieringHeartbeat(
                            HeartBeatHelper.finishedTieringTableHeartBeat(
                                    tableId, flussCoordinatorEpoch)));
            LOG.info("Report finished table to Fluss Coordinator success");
        } catch (Exception e) {
            LOG.error("Errors happens when report finished table to Fluss cluster.", e);
            throw new FlinkRuntimeException(
                    "Errors happens when report finished table to Fluss cluster.", e);
        }
    }

    @VisibleForTesting
    int getFlussCoordinatorEpoch() {
        return flussCoordinatorEpoch;
    }

    /** A helper class to build heartbeat request. */
    @VisibleForTesting
    static class HeartBeatHelper {

        @VisibleForTesting
        static LakeTieringHeartbeatRequest registerHeartBeat() {
            return new LakeTieringHeartbeatRequest();
        }

        @VisibleForTesting
        static LakeTieringHeartbeatRequest requestTieringTableHeartBeat() {
            LakeTieringHeartbeatRequest heartbeatRequest = new LakeTieringHeartbeatRequest();
            heartbeatRequest.setRequestTable(true);
            return heartbeatRequest;
        }

        @VisibleForTesting
        static LakeTieringHeartbeatRequest finishedTieringTableHeartBeat(
                long tableId, int coordinatorEpoch) {
            Set<PbHeartbeatReqForTable> finishedTables = new HashSet<>();
            finishedTables.add(
                    new PbHeartbeatReqForTable()
                            .setTableId(tableId)
                            .setCoordinatorEpoch(coordinatorEpoch)
                            .setTieringEpoch(System.currentTimeMillis()));
            return new LakeTieringHeartbeatRequest().addAllFinishedTables(finishedTables);
        }

        @VisibleForTesting
        static LakeTieringHeartbeatRequest failedTieringTableHeartBeat(
                List<TieringSplit> pendingSplits, int coordinatorEpoch) {
            Set<PbHeartbeatReqForTable> failedTables = new HashSet<>();
            if (!pendingSplits.isEmpty()) {
                failedTables =
                        pendingSplits.stream()
                                .map(s -> s.getTableBucket().getTableId())
                                .collect(Collectors.toSet())
                                .stream()
                                .map(
                                        t ->
                                                new PbHeartbeatReqForTable()
                                                        .setTableId(t)
                                                        .setCoordinatorEpoch(coordinatorEpoch)
                                                        .setTieringEpoch(
                                                                System.currentTimeMillis()))
                                .collect(Collectors.toSet());
            }
            return new LakeTieringHeartbeatRequest().addAllFailedTables(failedTables);
        }

        @VisibleForTesting
        static LakeTieringHeartbeatResponse waitHeartbeatResponse(
                CompletableFuture<LakeTieringHeartbeatResponse> responseCompletableFuture) {
            try {
                return responseCompletableFuture.get();
            } catch (Exception e) {
                LOG.error("Failed to wait heartbeat response due to ", e);
                throw new FlinkRuntimeException("Failed to wait heartbeat response due to ", e);
            }
        }
    }
}
