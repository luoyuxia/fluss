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

package com.alibaba.fluss.server;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcGateway;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.rpc.netty.client.NettyClient;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.server.cli.CommandLineOptions;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.tablet.TabletServer;
import com.alibaba.fluss.server.utils.TestProcessBuilder;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.testutils.common.CommonTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Base integration process test for {@link CoordinatorServer} and {@link TabletServer}. */
public abstract class ServerITCaseBase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final String SERVER_STARTED_MARKER = "Successfully start Netty server";

    protected abstract ServerNode getServerNode();

    protected abstract Class<? extends RpcGateway> getRpcGatewayClass();

    protected abstract Class<? extends ServerBase> getServerClass();

    protected abstract Configuration getServerConfig();

    @Test
    void testRunServerUsingProcess(@TempDir Path tempFolder) throws Exception {
        Path yamlFile = tempFolder.resolve("server.yaml");
        generateYamlFile(yamlFile, getServerConfig());
        final Configuration configuration = new Configuration();
        configuration.setString(
                CommandLineOptions.CONFIG_DIR_OPTION.getLongOpt(),
                tempFolder.toAbsolutePath().toString());

        boolean success = false;
        TestProcessBuilder.TestProcess serverProcess = null;

        try {
            serverProcess =
                    new TestProcessBuilder(getServerClass().getName())
                            .addConfigAsMainClassArgs(configuration)
                            .addMainClassArg(
                                    String.format(
                                            "-D%s=%s",
                                            ConfigOptions.ZOOKEEPER_ADDRESS.key(),
                                            ZOO_KEEPER_EXTENSION_WRAPPER
                                                    .getCustomExtension()
                                                    .getConnectString()))
                            .start();

            // now, wait until server startup
            waitUntilServerStartup(serverProcess);

            // test connection
            testConnectionToServer();

            serverProcess.getProcess().destroy();

            serverProcess.getProcess().waitFor();

            success = true;
        } finally {
            if (serverProcess != null) {
                if (!success) {
                    TestProcessBuilder.TestProcess.printProcessLog(
                            getServerNode().serverType().toString(), serverProcess);
                }
                serverProcess.destroy();
            }
        }
    }

    private void waitUntilServerStartup(TestProcessBuilder.TestProcess process) {
        CommonTestUtils.waitUtil(
                () ->
                        process.getProcessOutput().toString().contains(SERVER_STARTED_MARKER)
                                || !process.getErrorOutput().toString().isEmpty(),
                Duration.ofMinutes(2),
                null);
        String errorMsg = process.getErrorOutput().toString();
        if (!errorMsg.isEmpty()) {
            throw new IllegalStateException("Server process failed to start: " + errorMsg);
        }
    }

    private void testConnectionToServer() throws Exception {
        try (NettyClient client =
                new NettyClient(new Configuration(), TestingClientMetricGroup.newInstance())) {
            RpcGateway gateway =
                    GatewayClientProxy.createGatewayProxy(
                            this::getServerNode, client, getRpcGatewayClass());
            ApiVersionsResponse response =
                    gateway.apiVersions(
                                    new ApiVersionsRequest()
                                            .setClientSoftwareVersion("1.0.0")
                                            .setClientSoftwareName("test"))
                            .get();
            ApiManager apiManager = new ApiManager(getServerNode().serverType());
            assertThat(response.getApiVersionsCount()).isEqualTo(apiManager.enabledApis().size());
        }
    }

    private static void generateYamlFile(Path yamlFile, Configuration configuration)
            throws Exception {
        final List<String> configurationLines =
                configuration.toMap().entrySet().stream()
                        .map(entry -> entry.getKey() + ": " + entry.getValue())
                        .collect(Collectors.toList());
        Files.write(yamlFile, configurationLines);
    }
}
