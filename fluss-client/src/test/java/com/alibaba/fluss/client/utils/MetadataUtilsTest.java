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

package com.alibaba.fluss.client.utils;

import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.RetryCountExceededException;
import com.alibaba.fluss.exception.StaleMetadataException;
import com.alibaba.fluss.exception.StorageException;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbServerNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link MetadataUtils}. */
class MetadataUtilsTest {

    @Mock private AdminReadOnlyGateway gateway;
    @Mock private RpcClient rpcClient;

    private Cluster originCluster;
    private Configuration configuration;
    private TablePath testTablePath;
    private Set<TablePath> tablePaths;

    @BeforeEach
    void setUp() {
        // Create a test cluster
        Map<Integer, ServerNode> aliveTabletServers = new HashMap<>();
        aliveTabletServers.put(1, new ServerNode(1, "localhost", 8080, ServerType.TABLET_SERVER));
        aliveTabletServers.put(2, new ServerNode(2, "localhost", 8081, ServerType.TABLET_SERVER));

        ServerNode coordinatorServer = new ServerNode(0, "localhost", 8082, ServerType.COORDINATOR);

        originCluster =
                new Cluster(
                        aliveTabletServers,
                        coordinatorServer,
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>(),
                        new HashMap<>());

        // Create test configuration with default values
        configuration = new Configuration();
        configuration.set(ConfigOptions.CLIENT_METADATA_MAX_RETRIES, 5);
        configuration.set(ConfigOptions.CLIENT_METADATA_TIMEOUT, Duration.ofSeconds(30));
        configuration.set(ConfigOptions.CLIENT_METADATA_RETRY_BACKOFF, Duration.ofMillis(100));

        testTablePath = new TablePath("test_db", "test_table");
        tablePaths = Collections.singleton(testTablePath);

        gateway = mock(AdminReadOnlyGateway.class);
        rpcClient = mock(RpcClient.class);
    }

    @Test
    void testSendMetadataRequestAndRebuildClusterWithTimeoutWithRetry_SuccessOnFirstAttempt()
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        MetadataResponse mockResponse = createMockMetadataResponse();
        when(gateway.metadata(any(MetadataRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        try (MockedStatic<GatewayClientProxy> gatewayClientProxyMock =
                Mockito.mockStatic(GatewayClientProxy.class)) {
            gatewayClientProxyMock
                    .when(() -> GatewayClientProxy.createGatewayProxy(any(), any(), any()))
                    .thenReturn(gateway);

            // When
            Cluster result =
                    MetadataUtils.sendMetadataRequestAndRebuildClusterWithRetry(
                            originCluster, rpcClient, tablePaths, null, null, configuration);

            // Then
            assertThat(result).isNotNull();
            assertThat(result.getAliveTabletServerList()).hasSize(1);
            verify(gateway, times(1)).metadata(any(MetadataRequest.class));
        }
    }

    @Test
    void testSendMetadataRequestAndRebuildClusterWithTimeoutWithRetry_SuccessAfterRetries()
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        MetadataResponse mockResponse = createMockMetadataResponse();

        // Fail twice with retriable exception, then succeed
        CompletableFuture<MetadataResponse> failedFuture1 = new CompletableFuture<>();
        failedFuture1.completeExceptionally(new StorageException("Retriable error"));
        CompletableFuture<MetadataResponse> failedFuture2 = new CompletableFuture<>();
        failedFuture2.completeExceptionally(new StorageException("Retriable error"));

        when(gateway.metadata(any(MetadataRequest.class)))
                .thenReturn(failedFuture1)
                .thenReturn(failedFuture2)
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        try (MockedStatic<GatewayClientProxy> gatewayClientProxyMock =
                Mockito.mockStatic(GatewayClientProxy.class)) {
            gatewayClientProxyMock
                    .when(() -> GatewayClientProxy.createGatewayProxy(any(), any(), any()))
                    .thenReturn(gateway);

            // When
            Cluster result =
                    MetadataUtils.sendMetadataRequestAndRebuildClusterWithRetry(
                            originCluster, rpcClient, tablePaths, null, null, configuration);

            // Then
            assertThat(result).isNotNull();
            assertThat(result.getAliveTabletServerList()).hasSize(1);
            verify(gateway, times(3)).metadata(any(MetadataRequest.class));
        }
    }

    @Test
    void testSendMetadataRequestAndRebuildClusterWithRetry_StaleMetadataExceptionRetryWithTimeout()
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        MetadataResponse mockResponse = createMockMetadataResponse();

        // Fail with StaleMetadataException, then succeed
        CompletableFuture<MetadataResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new StaleMetadataException("Stale metadata"));

        when(gateway.metadata(any(MetadataRequest.class)))
                .thenReturn(failedFuture)
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        try (MockedStatic<GatewayClientProxy> gatewayClientProxyMock =
                Mockito.mockStatic(GatewayClientProxy.class)) {
            gatewayClientProxyMock
                    .when(() -> GatewayClientProxy.createGatewayProxy(any(), any(), any()))
                    .thenReturn(gateway);

            // When
            Cluster result =
                    MetadataUtils.sendMetadataRequestAndRebuildClusterWithRetry(
                            originCluster, rpcClient, tablePaths, null, null, configuration);

            // Then
            assertThat(result).isNotNull();
            assertThat(result.getAliveTabletServerList()).hasSize(1);
            verify(gateway, times(2)).metadata(any(MetadataRequest.class));
        }
    }

    @Test
    void testSendMetadataRequestAndRebuildClusterWithRetry_TimeoutExceptionRetry()
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        MetadataResponse mockResponse = createMockMetadataResponse();

        // Fail with TimeoutException, then succeed
        CompletableFuture<MetadataResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new TimeoutException("Timeout"));

        when(gateway.metadata(any(MetadataRequest.class)))
                .thenReturn(failedFuture)
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        try (MockedStatic<GatewayClientProxy> gatewayClientProxyMock =
                Mockito.mockStatic(GatewayClientProxy.class)) {
            gatewayClientProxyMock
                    .when(() -> GatewayClientProxy.createGatewayProxy(any(), any(), any()))
                    .thenReturn(gateway);

            // When
            Cluster result =
                    MetadataUtils.sendMetadataRequestAndRebuildClusterWithRetry(
                            originCluster, rpcClient, tablePaths, null, null, configuration);

            // Then
            assertThat(result).isNotNull();
            assertThat(result.getAliveTabletServerList()).hasSize(1);
            verify(gateway, times(2)).metadata(any(MetadataRequest.class));
        }
    }

    @Test
    void testSendMetadataRequestAndRebuildClusterWithTimeoutWithRetry_MaxRetriesExceeded() {
        // Given
        CompletableFuture<MetadataResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new StorageException("Retriable error"));

        when(gateway.metadata(any(MetadataRequest.class))).thenReturn(failedFuture);

        try (MockedStatic<GatewayClientProxy> gatewayClientProxyMock =
                Mockito.mockStatic(GatewayClientProxy.class)) {
            gatewayClientProxyMock
                    .when(() -> GatewayClientProxy.createGatewayProxy(any(), any(), any()))
                    .thenReturn(gateway);

            // When & Then
            assertThatThrownBy(
                            () ->
                                    MetadataUtils.sendMetadataRequestAndRebuildClusterWithRetry(
                                            originCluster,
                                            rpcClient,
                                            tablePaths,
                                            null,
                                            null,
                                            configuration))
                    .isInstanceOf(RetryCountExceededException.class)
                    .hasMessageContaining("Metadata request failed after all retries");

            // Verify that the method was called maxRetries + 1 times (initial attempt + retries)
            verify(gateway, times(6)).metadata(any(MetadataRequest.class));
        }
    }

    @Test
    void
            testSendMetadataRequestAndRebuildClusterWithTimeoutWithRetry_NonRetriableExceptionThrownImmediately() {
        // Given
        CompletableFuture<MetadataResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Non-retriable error"));

        when(gateway.metadata(any(MetadataRequest.class))).thenReturn(failedFuture);

        try (MockedStatic<GatewayClientProxy> gatewayClientProxyMock =
                Mockito.mockStatic(GatewayClientProxy.class)) {
            gatewayClientProxyMock
                    .when(() -> GatewayClientProxy.createGatewayProxy(any(), any(), any()))
                    .thenReturn(gateway);

            // When & Then
            assertThatThrownBy(
                            () ->
                                    MetadataUtils.sendMetadataRequestAndRebuildClusterWithRetry(
                                            originCluster,
                                            rpcClient,
                                            tablePaths,
                                            null,
                                            null,
                                            configuration))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Non-retriable error");

            // Verify that the method was called only once (no retry for non-retriable exceptions)
            verify(gateway, times(1)).metadata(any(MetadataRequest.class));
        }
    }

    @Test
    void testSendMetadataRequestAndRebuildClusterWithTimeoutWithRetry_WithNullConfiguration()
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        MetadataResponse mockResponse = createMockMetadataResponse();
        when(gateway.metadata(any(MetadataRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResponse));

        try (MockedStatic<GatewayClientProxy> gatewayClientProxyMock =
                Mockito.mockStatic(GatewayClientProxy.class)) {
            gatewayClientProxyMock
                    .when(() -> GatewayClientProxy.createGatewayProxy(any(), any(), any()))
                    .thenReturn(gateway);

            // When
            Cluster result =
                    MetadataUtils.sendMetadataRequestAndRebuildClusterWithRetry(
                            originCluster, rpcClient, tablePaths, null, null, null);

            // Then
            assertThat(result).isNotNull();
            assertThat(result.getAliveTabletServerList()).hasSize(1);
            verify(gateway, times(1)).metadata(any(MetadataRequest.class));
        }
    }

    @Test
    void testSendMetadataRequestAndRebuildClusterWithTimeoutWithRetry_InterruptedException() {
        // Given
        CompletableFuture<MetadataResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new InterruptedException("Interrupted"));

        when(gateway.metadata(any(MetadataRequest.class))).thenReturn(failedFuture);

        try (MockedStatic<GatewayClientProxy> gatewayClientProxyMock =
                Mockito.mockStatic(GatewayClientProxy.class)) {
            gatewayClientProxyMock
                    .when(() -> GatewayClientProxy.createGatewayProxy(any(), any(), any()))
                    .thenReturn(gateway);

            // When & Then
            assertThatThrownBy(
                            () ->
                                    MetadataUtils.sendMetadataRequestAndRebuildClusterWithRetry(
                                            originCluster,
                                            rpcClient,
                                            tablePaths,
                                            null,
                                            null,
                                            configuration))
                    .isInstanceOf(InterruptedException.class)
                    .hasMessageContaining("Interrupted");

            // Verify that the method was called only once (no retry for InterruptedException)
            verify(gateway, times(1)).metadata(any(MetadataRequest.class));
        }
    }

    private MetadataResponse createMockMetadataResponse() {
        MetadataResponse response = new MetadataResponse();

        // Add a tablet server
        PbServerNode tabletServer =
                new PbServerNode().setNodeId(1).setHost("localhost").setPort(8080);
        response.addAllTabletServers(Collections.singletonList(tabletServer));

        return response;
    }
}
