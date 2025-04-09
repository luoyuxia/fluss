/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.replica.fetcher;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.replica.ReplicaTestBase;
import com.alibaba.fluss.server.replica.fetcher.ReplicaFetcherManager.ServerIdAndFetcherId;
import com.alibaba.fluss.server.tablet.TestTabletServerGateway;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ReplicaFetcherManager}. */
class ReplicaFetcherManagerTest extends ReplicaTestBase {

    private ServerNode leader;
    private ReplicaFetcherThread fetcherThread;

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
        // with local test leader end point.
        leader = new ServerNode(1, "localhost", 9099, ServerType.COORDINATOR);
        fetcherThread =
                new ReplicaFetcherThread(
                        "test-fetcher-thread",
                        replicaManager,
                        new RemoteLeaderEndpoint(
                                conf,
                                TABLET_SERVER_ID,
                                leader.id(),
                                replicaManager,
                                new TestTabletServerGateway(false)),
                        (int)
                                conf.get(ConfigOptions.LOG_REPLICA_FETCH_BACKOFF_INTERVAL)
                                        .toMillis());
    }

    @Test
    void testAddAndRemoveBucket() {
        int numFetchers = 2;
        ReplicaFetcherManager fetcherManager =
                new TestingReplicaFetcherManager(TABLET_SERVER_ID, replicaManager, fetcherThread);

        long fetchOffset = 0L;
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 0);

        // make this table bucket as follower to make sure the fetcher thread can deal with the
        // fetched data from leader.
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH,
                Collections.singletonList(
                        new NotifyLeaderAndIsrData(
                                PhysicalTablePath.of(DATA1_TABLE_PATH),
                                tb,
                                Arrays.asList(leader.id(), TABLET_SERVER_ID),
                                new LeaderAndIsr(
                                        leader.id(),
                                        LeaderAndIsr.INITIAL_LEADER_EPOCH,
                                        Arrays.asList(leader.id(), TABLET_SERVER_ID),
                                        INITIAL_COORDINATOR_EPOCH,
                                        LeaderAndIsr.INITIAL_BUCKET_EPOCH))),
                result -> {});

        InitialFetchStatus initialFetchStatus =
                new InitialFetchStatus(DATA1_TABLE_ID, leader.id(), fetchOffset);

        Map<TableBucket, InitialFetchStatus> initialFetchStateMap = new HashMap<>();
        initialFetchStateMap.put(tb, initialFetchStatus);
        fetcherManager.addFetcherForBuckets(initialFetchStateMap);
        Map<ReplicaFetcherManager.ServerIdAndFetcherId, ReplicaFetcherThread> fetcherThreadMap =
                fetcherManager.getFetcherThreadMap();
        assertThat(fetcherThreadMap.size()).isEqualTo(1);
        ReplicaFetcherThread thread =
                fetcherThreadMap.get(new ServerIdAndFetcherId(1, tb.hashCode() % numFetchers));
        assertThat(thread).isEqualTo(fetcherThread);
        assertThat(fetcherThread.fetchStatus(tb).isPresent()).isTrue();

        fetcherManager.removeFetcherForBuckets(Collections.singleton(tb));
        // the fetcher thread will not be moved out from the map.
        assertThat(fetcherThreadMap.size()).isEqualTo(1);
        thread = fetcherThreadMap.get(new ServerIdAndFetcherId(1, tb.hashCode() % numFetchers));

        assertThat(thread).isEqualTo(fetcherThread);
        assertThat(fetcherThread.fetchStatus(tb).isPresent()).isFalse();

        fetcherManager.shutdownIdleFetcherThreads();
        assertThat(fetcherThreadMap.size()).isEqualTo(0);
    }

    private class TestingReplicaFetcherManager extends ReplicaFetcherManager {

        private final ReplicaFetcherThread fetcherThread;

        public TestingReplicaFetcherManager(
                int serverId, ReplicaManager replicaManager, ReplicaFetcherThread fetcherThread) {
            super(new Configuration(), null, serverId, replicaManager, (id) -> Optional.of(leader));
            this.fetcherThread = fetcherThread;
        }

        @Override
        public ReplicaFetcherThread createFetcherThread(int fetcherId, int leaderId) {
            return fetcherThread;
        }
    }
}
