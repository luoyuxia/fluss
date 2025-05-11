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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.FencedTieringEpochException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.InvalidCoordinatorException;
import com.alibaba.fluss.exception.TableNotExistException;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.entity.LakeTieringTableInfo;
import com.alibaba.fluss.server.utils.timer.DefaultTimer;
import com.alibaba.fluss.server.utils.timer.Timer;
import com.alibaba.fluss.server.utils.timer.TimerTask;
import com.alibaba.fluss.server.zk.ZkSequenceIDCounter;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.ZkData.LakeTableTieringEpochZNode;
import com.alibaba.fluss.utils.clock.Clock;
import com.alibaba.fluss.utils.clock.SystemClock;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A manager to manage the tables to be tiered.
 *
 * <p>For a lake table to be tiered, when created, it wil be put into this manager and scheduled to
 * be tiered by tiering services.
 *
 * <p>There are five states for the table to be tiered:
 *
 * <ul>
 *   <li>New: when a new lake table is created
 *   <li>Initialized: when the coordinator server is restarted, the state of all existing lake table
 *       will be Initialized state
 *   <li>Scheduled: when the lake table is waiting for a period of time to be tiered
 *   <li>Pending: when the lake table is waiting for tiering service to request the table
 *   <li>Tiering: when the lake table is being tiered by tiering service
 *   <li>Tiered: when the lake table finish one round of tiering
 *   <li>Failed: when the lake table tiering failed
 * </ul>
 *
 * <p>The state machine of table to be tiered is as follows:
 *
 * <pre>{@code
 * New -> Scheduled
 * Initialized --> |the interval from last lake snapshot is not less than tiering interval| Pending
 * Initialized --> |the interval from last lake snapshot is less than tiering interval| Scheduled
 * Scheduled --> |after waiting for a period of tiering interval| Pending
 * Pending --> |lake tiering service request table| Tiering
 * Tiering --> |once lake tiering finish| Tiered
 * Tiering --> |lake tiering heartbeat timout| Pending
 * Tiering --> |lake tiering report tiering| Failed
 * Tiered -> Scheduled
 * Failed -> Pending
 * }</pre>
 */
public class LakeTableTieringManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTableTieringManager.class);

    protected static final long TIERING_SERVICE_TIMEOUT_MS = 2 * 60 * 1000; // 2 minutes

    private final ZooKeeperClient zkClient;
    private final Timer lakeTieringScheduleTimer;
    private final ScheduledExecutorService lakeTieringServiceTimeoutChecker;
    private final Clock clock;
    private final Queue<Long> pendingTieringTables;
    private final LakeTieringExpiredOperationReaper expirationReaper;

    // the tiering state of the table to be tiered,
    // from table_id -> tiering state
    private final Map<Long, TieringState> tieringStates;

    // the live tables that are tiering,
    // from table_id -> last heartbeat time by the tiering service
    private final Map<Long, Long> liveTieringTableIds;

    // table_id -> table path
    private final Map<Long, TablePath> tablePathById;

    // table_id -> tiering interval
    private final Map<Long, Long> tieringIntervalByTableId;

    // the table id -> ZkSequenceIDCounter for table tiering epoch
    private final Map<Long, ZkSequenceIDCounter> tableTierEpochZkCounter;
    // cache table_id -> table tiering epoch
    private final Map<Long, Long> tableTierEpoch;

    private final Lock lock = new ReentrantLock();

    public LakeTableTieringManager(ZooKeeperClient zkClient) {
        this(
                zkClient,
                new DefaultTimer("delay lake tiering", 1_000, 20),
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("fluss-lake-tiering-timeout-checker")),
                SystemClock.getInstance());
    }

    @VisibleForTesting
    protected LakeTableTieringManager(
            ZooKeeperClient zkClient,
            Timer lakeTieringScheduleTimer,
            ScheduledExecutorService lakeTieringServiceTimeoutChecker,
            Clock clock) {
        this.zkClient = zkClient;
        this.lakeTieringScheduleTimer = lakeTieringScheduleTimer;
        this.lakeTieringServiceTimeoutChecker = lakeTieringServiceTimeoutChecker;
        this.clock = clock;
        this.pendingTieringTables = new ArrayDeque<>();
        this.tieringStates = new HashMap<>();
        this.liveTieringTableIds = new HashMap<>();
        this.tablePathById = new HashMap<>();
        this.tieringIntervalByTableId = new HashMap<>();
        this.expirationReaper = new LakeTieringExpiredOperationReaper();
        expirationReaper.start();
        this.lakeTieringServiceTimeoutChecker.scheduleWithFixedDelay(
                this::checkTieringServiceTimeout, 0, 30, TimeUnit.SECONDS);
        this.tableTierEpochZkCounter = new HashMap<>();
        this.tableTierEpoch = new HashMap<>();
    }

    public void initWithLakeTables(
            List<Tuple2<TableInfo, Long>> tableInfoAndLastLakeSnapshotTimestamp) {
        inLock(
                lock,
                () -> {
                    for (Tuple2<TableInfo, Long> tableInfoAndLastLakeTime :
                            tableInfoAndLastLakeSnapshotTimestamp) {
                        TableInfo tableInfo = tableInfoAndLastLakeTime.f0;
                        long tableId = tableInfo.getTableId();
                        long lastLakeSnapshotTimestamp = tableInfoAndLastLakeTime.f1;
                        putLakeTable(tableInfo);

                        doHandleStateChange(tableInfo.getTableId(), TieringState.Initialized);

                        // the interval from last lake snapshot is greater than the tiering
                        // interval, put into pending directly
                        long currentTime = clock.milliseconds();
                        long tieringInterval = tieringIntervalByTableId.get(tableInfo.getTableId());
                        if (currentTime - lastLakeSnapshotTimestamp >= tieringInterval) {
                            doHandleStateChange(tableId, TieringState.Pending);
                        } else {
                            // otherwise, schedule it to be tiered after the remain time interval
                            doHandleStateChange(
                                    tableId,
                                    TieringState.Scheduled,
                                    tieringInterval - (currentTime - lastLakeSnapshotTimestamp));
                        }
                    }
                });
    }

    public void addNewLakeTable(TableInfo tableInfo) {
        inLock(
                lock,
                () -> {
                    putLakeTable(tableInfo);
                    doHandleStateChange(tableInfo.getTableId(), TieringState.New);
                    // schedule it to be tiered after the tiering interval
                    doHandleStateChange(tableInfo.getTableId(), TieringState.Scheduled);
                });
    }

    private void putLakeTable(TableInfo tableInfo) {
        long tableId = tableInfo.getTableId();
        tablePathById.put(tableId, tableInfo.getTablePath());
        tieringIntervalByTableId.put(
                tableId, tableInfo.getTableConfig().getDataLakeTieringInterval().toMillis());
        ZkSequenceIDCounter counter =
                new ZkSequenceIDCounter(
                        zkClient.getCuratorClient(), LakeTableTieringEpochZNode.path(tableId));
        tableTierEpochZkCounter.put(tableId, counter);
        try {
            tableTierEpoch.put(tableId, counter.get());
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Fail to get current tier epoch for table id: %d of table path: %s",
                            tableId, tableInfo.getTablePath()));
        }
    }

    private void scheduleTableTiering(long tableId, @Nullable Long delayMs) {
        Long tieringInterval = tieringIntervalByTableId.get(tableId);
        if (tieringInterval == null) {
            // the table has been dropped, return directly
            return;
        }
        lakeTieringScheduleTimer.add(
                new DelayedTiering(tableId, delayMs == null ? tieringInterval : delayMs));
    }

    public void removeLakeTable(long tableId) {
        inLock(
                lock,
                () -> {
                    tablePathById.remove(tableId);
                    tieringIntervalByTableId.remove(tableId);
                    tieringStates.remove(tableId);
                    liveTieringTableIds.remove(tableId);

                    tableTierEpochZkCounter.remove(tableId);
                    tableTierEpoch.remove(tableId);
                });
    }

    @VisibleForTesting
    protected void checkTieringServiceTimeout() {
        inLock(
                lock,
                () -> {
                    for (Map.Entry<Long, Long> tableIdAndLastTimeHeartbeat :
                            liveTieringTableIds.entrySet()) {
                        Long tableId = tableIdAndLastTimeHeartbeat.getKey();
                        long lastHeartbeat = tableIdAndLastTimeHeartbeat.getValue();
                        if (isTieringServiceTimeout(lastHeartbeat)) {
                            // change it to pending to wait to be requested by other
                            // lake tiering service
                            doHandleStateChange(tableId, TieringState.Pending);
                        }
                    }
                });
    }

    @Nullable
    public LakeTieringTableInfo requestTable() {
        return inLock(
                lock,
                () -> {
                    Long tableId = pendingTieringTables.poll();
                    // no any pending table, return directly
                    if (tableId == null) {
                        return null;
                    }
                    TablePath tablePath = tablePathById.get(tableId);
                    // the table has been dropped, request again
                    if (tablePath == null) {
                        return requestTable();
                    }
                    long tieringEpoch = increaseTieringEpoch(tableId);
                    doHandleStateChange(tableId, TieringState.Tiering);
                    return new LakeTieringTableInfo(tableId, tablePath, tieringEpoch);
                });
    }

    private boolean isTieringServiceTimeout(long lastHeartbeat) {
        return clock.milliseconds() - lastHeartbeat >= TIERING_SERVICE_TIMEOUT_MS;
    }

    private long increaseTieringEpoch(long tableId) {
        long tieringEpoch;
        try {
            tieringEpoch = tableTierEpochZkCounter.get(tableId).incrementAndGet();
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Fail to increase current tier epoch for table id: %d of table path: %s",
                            tableId, tablePathById.get(tableId)));
        }
        tableTierEpoch.put(tableId, tieringEpoch);
        return tieringEpoch;
    }

    public void finishTableTiering(long tableId, long tieredEpoch) {
        inLock(
                lock,
                () -> {
                    validateTieringServiceRequest(tableId, tieredEpoch);
                    // to tiered state firstly
                    doHandleStateChange(tableId, TieringState.Tiered);
                    // then to scheduled state to enable other tiering service can pick it
                    doHandleStateChange(tableId, TieringState.Scheduled);
                });
    }

    public void reportTieringFail(long tableId, long tieringEpoch) {
        inLock(
                lock,
                () -> {
                    validateTieringServiceRequest(tableId, tieringEpoch);
                    // to fail state firstly
                    doHandleStateChange(tableId, TieringState.Failed);
                    // then to pending state to enable other tiering service can pick it
                    doHandleStateChange(tableId, TieringState.Pending);
                });
    }

    public void renewTieringHeartbeat(long tableId, long tieringEpoch) {
        inLock(
                lock,
                () -> {
                    validateTieringServiceRequest(tableId, tieringEpoch);
                    TieringState tieringState = tieringStates.get(tableId);
                    if (tieringState != TieringState.Tiering) {
                        throw new IllegalStateException(
                                String.format(
                                        "The table %d to renew tiering heartbeat must in Tiering state, but in %s state.",
                                        tableId, tieringState));
                    }
                    liveTieringTableIds.put(tableId, clock.milliseconds());
                });
    }

    private void validateTieringServiceRequest(long tableId, long tieringEpoch) {
        Long currentEpoch = tableTierEpoch.get(tableId);
        // the table has been dropped, return false
        if (currentEpoch == null) {
            throw new TableNotExistException("The table " + tableId + " doesn't exist.");
        }
        if (tieringEpoch < currentEpoch) {
            throw new FencedTieringEpochException(
                    String.format(
                            "The tiering epoch %d is lower than current epoch %d in coordinator for table %d.",
                            tieringEpoch, currentEpoch, tableId));
        }
        if (tieringEpoch > currentEpoch) {
            throw new InvalidCoordinatorException(
                    String.format(
                            "Coordinator Server has been moved, "
                                    + "the tiering epoch in request is %d for table %d, but the tiering epoch in the target coordinator server is %d.",
                            tieringEpoch, tableId, currentEpoch));
        }
    }

    private void doHandleStateChange(long tableId, TieringState targetState) {
        doHandleStateChange(tableId, targetState, null);
    }

    /**
     * Handle the state change of the lake table to be tiered. The core state transitions for the
     * state machine are as follows:
     *
     * <p>New -> Scheduled:
     *
     * <p>-- When the lake table is newly created, do: schedule a timer to wait for a tiering
     * interval configured in table, transmit the table to Pending
     *
     * <p>Initialized -> Pending:
     *
     * <p>Initialized -> Scheduled：
     *
     * <p>-- When the coordinator server is restarted, for all existing lake table, if the interval
     * from last lake snapshot is not less than tiering interval, do: transmit to Pending, otherwise
     * Scheduled which will wait for {@code delay} time to be Pending.
     *
     * <p>Scheduled -> Pending
     *
     * <p>-- The time interval to wait has passed, do: transmit to Pending state
     *
     * <p>Tiering -> Pending
     *
     * <p>-- When the tiering service is timeout, do: transmit to Pending state
     *
     * <p>Pending -> Tiering
     *
     * <p>-- When the table is assigned to a tiering service after tiering service request the
     * table, do: transmit to Tiering state
     */
    private void doHandleStateChange(long tableId, TieringState targetState, @Nullable Long delay) {
        TieringState currentState = tieringStates.get(tableId);
        if (!isValidStateTransition(currentState, targetState)) {
            LOG.error(
                    "Fail to change state for table {} from {} to {} as it's not a valid state change.",
                    tableId,
                    currentState,
                    targetState);
            return;
        }
        switch (targetState) {
            case New:
            case Initialized:
                // do nothing
                break;
            case Scheduled:
                scheduleTableTiering(tableId, delay);
                break;
            case Pending:
                pendingTieringTables.add(tableId);
                break;
            case Tiering:
                liveTieringTableIds.put(tableId, clock.milliseconds());
                break;
            case Tiered:
            case Failed:
                liveTieringTableIds.remove(tableId);
                // do nothing
                break;
        }
        doStateChange(tableId, currentState, targetState);
    }

    private boolean isValidStateTransition(
            @Nullable TieringState curState, TieringState targetState) {
        if (targetState == TieringState.New || targetState == TieringState.Initialized) {
            // when target state is new or Initialized, it's valid when current state is null
            return curState == null;
        }
        return targetState.validPreviousStates().contains(curState);
    }

    private void doStateChange(long tableId, TieringState fromState, TieringState toState) {
        tieringStates.put(tableId, toState);
        LOG.debug(
                "Successfully changed tiering state for table {} from {} to {}.",
                tableId,
                fromState,
                fromState);
    }

    @Override
    public void close() throws Exception {
        lakeTieringServiceTimeoutChecker.shutdown();
        expirationReaper.initiateShutdown();
        // improve shutdown time by waking up any ShutdownableThread(s) blocked on poll by
        // sending a no-op.
        lakeTieringScheduleTimer.add(
                new TimerTask(0) {
                    @Override
                    public void run() {}
                });
        try {
            expirationReaper.awaitShutdown();
        } catch (InterruptedException e) {
            throw new FlussRuntimeException(
                    "Error while shutdown lake tiering expired operation manager", e);
        }

        lakeTieringScheduleTimer.shutdown();
    }

    private class DelayedTiering extends TimerTask {

        private final long tableId;

        public DelayedTiering(long tableId, long delayMs) {
            super(delayMs);
            this.tableId = tableId;
        }

        @Override
        public void run() {
            inLock(
                    lock,
                    () ->
                            // to pending state
                            doHandleStateChange(tableId, TieringState.Pending));
        }
    }

    private class LakeTieringExpiredOperationReaper extends ShutdownableThread {

        public LakeTieringExpiredOperationReaper() {
            super("LakeTieringExpiredOperationReaper", false);
        }

        @Override
        public void doWork() throws Exception {
            advanceClock();
        }

        private void advanceClock() throws InterruptedException {
            lakeTieringScheduleTimer.advanceClock(200L);
        }
    }

    private enum TieringState {
        // When a new lake table is created, the state will be New
        New {
            @Override
            public Set<TieringState> validPreviousStates() {
                return Collections.emptySet();
            }
        },
        // When the coordinator server is restarted, the state of existing lake table
        // will be Initialized
        Initialized {
            @Override
            public Set<TieringState> validPreviousStates() {
                return Collections.emptySet();
            }
        },
        // When the lake table is waiting to be tiered, such as waiting for the period of tiering
        // interval, the state will be Scheduled
        Scheduled {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(New, Initialized, Tiered);
            }
        },
        // When the period of tiering interval has passed, but no any tiering service requesting
        // table, the state will be Pending
        Pending {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Scheduled, Initialized, Tiering, Failed);
            }
        },
        // When one tiering service is tiering the table, the state will be Tiering
        Tiering {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Pending);
            }
        },

        // When one tiering service has tiered the table, the state will be TIERED
        Tiered {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Tiering);
            }
        },
        // When one tiering service fail to tier the table, the state will be Failed
        Failed {
            @Override
            public Set<TieringState> validPreviousStates() {
                return EnumSet.of(Tiering);
            }
        };

        abstract Set<TieringState> validPreviousStates();
    }
}
