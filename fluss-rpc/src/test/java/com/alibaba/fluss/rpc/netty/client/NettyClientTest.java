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

package com.alibaba.fluss.rpc.netty.client;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.util.NOPMetricsGroup;
import com.alibaba.fluss.rpc.TestingGatewayService;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.GetTableRequest;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.PbLookupReqForBucket;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.rpc.netty.server.NettyServer;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.utils.NetUtils;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.utils.NetUtils.getAvailablePort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link NettyClient}. */
final class NettyClientTest {

    private Configuration conf;
    private NettyClient nettyClient;
    private ServerNode serverNode;
    private NettyServer nettyServer;
    private TestingGatewayService service;

    @BeforeEach
    public void setup() throws Exception {
        conf = new Configuration();
        // 3 worker threads is enough for this test
        conf.setInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS, 3);
        nettyClient = new NettyClient(conf, TestingClientMetricGroup.newInstance());
        buildNettyServer(1);
    }

    @AfterEach
    public void cleanup() throws Exception {
        if (nettyServer != null) {
            nettyServer.close();
        }
        if (nettyClient != null) {
            nettyClient.close();
        }
    }

    @Test
    void testSendIncompleteRequest() {
        GetTableRequest getTableRequest = new GetTableRequest();

        // get table request without table path.
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.GET_TABLE, getTableRequest)
                                        .get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Failed to encode request for 'GET_TABLE(1007)'")
                .hasRootCauseMessage("Some required fields are missing");
    }

    @Test
    void testSendRequestToWrongServerType() {
        LookupRequest lookupRequest = new LookupRequest().setTableId(1);
        PbLookupReqForBucket pbLookupReqForBucket =
                new PbLookupReqForBucket().setPartitionId(1).setBucketId(1);
        pbLookupReqForBucket.addKey("key".getBytes());
        lookupRequest.addAllBucketsReqs(Collections.singleton(pbLookupReqForBucket));

        // LookupRequest isn't support by Coordinator server, See ApiManager for details. In this
        // case, we will get an exception.
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.LOOKUP, lookupRequest)
                                        .get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("The server does not support LOOKUP(1017)")
                .hasRootCauseMessage("The server does not support LOOKUP(1017)");
    }

    @Test
    void testRequestsProcessedInOrder() throws Exception {
        int numRequests = 100;
        List<CompletableFuture<ApiMessage>> futures = new ArrayList<>();
        for (int i = 0; i < numRequests; i++) {
            ApiVersionsRequest request =
                    new ApiVersionsRequest()
                            .setClientSoftwareName("testing_client" + i)
                            .setClientSoftwareVersion("1.0");
            futures.add(nettyClient.sendRequest(serverNode, ApiKeys.API_VERSIONS, request));
        }
        FutureUtils.waitForAll(futures).get();
        // we have one more api version request for rpc handshake.
        assertThat(service.getProcessorThreadNames()).hasSize(numRequests + 1);
        Set<String> deduplicatedThreadNames = new HashSet<>(service.getProcessorThreadNames());
        // there should only one thread to process the requests
        // since all requests are from the same client.
        assertThat(deduplicatedThreadNames).hasSize(1);
    }

    @Test
    void testServerDisconnection() throws Exception {
        ApiVersionsRequest request =
                new ApiVersionsRequest()
                        .setClientSoftwareName("testing_client_100")
                        .setClientSoftwareVersion("1.0");
        nettyClient.sendRequest(serverNode, ApiKeys.API_VERSIONS, request).get();
        assertThat(nettyClient.connections().size()).isEqualTo(1);
        assertThat(nettyClient.connections().get(serverNode.uid()).getServerNode())
                .isEqualTo(serverNode);

        // close the netty server.
        nettyServer.close();
        assertThatThrownBy(
                        () ->
                                nettyClient
                                        .sendRequest(serverNode, ApiKeys.API_VERSIONS, request)
                                        .get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Disconnected from node")
                .hasRootCauseMessage("Connection refused");
        assertThat(nettyClient.connections().size()).isEqualTo(0);

        // restart the netty server.
        buildNettyServer(1);
        nettyClient.sendRequest(serverNode, ApiKeys.API_VERSIONS, request).get();
        assertThat(nettyClient.connections().size()).isEqualTo(1);
        assertThat(nettyClient.connections().get(serverNode.uid()).getServerNode())
                .isEqualTo(serverNode);
    }

    private void buildNettyServer(int serverId) throws Exception {
        try (NetUtils.Port availablePort = getAvailablePort()) {
            serverNode =
                    new ServerNode(
                            serverId, "localhost", availablePort.getPort(), ServerType.COORDINATOR);
            service = new TestingGatewayService();
            nettyServer =
                    new NettyServer(
                            conf,
                            serverNode.host(),
                            String.valueOf(serverNode.port()),
                            service,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(
                                    NOPMetricsGroup.newInstance()));
            nettyServer.start();
        }
    }
}
