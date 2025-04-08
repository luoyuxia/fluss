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

package com.alibaba.fluss.server.tablet;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.exception.NetworkException;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The ITCase for tablet server failover. */
class TabletServerFailOverITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIOExceptionShouldStopTabletServer(boolean isPkTable) throws Exception {
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        Schema schema =
                isPkTable
                        ? Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .primaryKey("a")
                                .build()
                        : Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();

        TablePath tablePath =
                TablePath.of("fluss", "test_ioexception_table_" + (isPkTable ? "pk" : "no_pk"));
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // delete the active segment, which will cause IOException when append log/changelog
        FLUSS_CLUSTER_EXTENSION
                .waitAndGetLeaderReplica(tb)
                .getLogTablet()
                .activeLogSegment()
                .deleteIfExists();

        // should get NetworkException: since the leader server is shutdown
        assertThatThrownBy(() -> writeData(leaderGateWay, tableId, isPkTable))
                .cause()
                .isInstanceOf(NetworkException.class);

        // should only has 2 tablet servers
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);

        // restart the shutdown server
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leader, true);
    }

    private ApiMessage writeData(
            TabletServerGateway tabletServerGateway, long tableId, boolean isPkTable)
            throws Exception {
        if (isPkTable) {
            return tabletServerGateway
                    .putKv(
                            newPutKvRequest(
                                    tableId, 0, 1, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                    .get();
        } else {
            return tabletServerGateway
                    .produceLog(
                            newProduceLogRequest(tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                    .get();
        }
    }
}
