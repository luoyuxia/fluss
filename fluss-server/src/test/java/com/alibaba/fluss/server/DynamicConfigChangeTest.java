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

import com.alibaba.fluss.server.coordinator.CoordinatorService;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.zk.ZooKeeperClient;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.config.ConfigOptions.DATALAKE_FORMAT;
import static com.alibaba.fluss.metadata.DataLakeFormat.PAIMON;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DynamicConfigManager}. */
public class DynamicConfigChangeTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(2).build();

    protected static ZooKeeperClient zookeeperClient;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testConfigChange() throws Exception {
        CoordinatorService coordinatorService =
                FLUSS_CLUSTER_EXTENSION.getCoordinatorServer().getCoordinatorService();
        assertThat(coordinatorService.getDataLakeFormat()).isNull();
        zookeeperClient.upsertServerEntityConfig(
                Collections.singletonMap(DATALAKE_FORMAT.key(), "paimon"));
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(coordinatorService.getDataLakeFormat()).isEqualTo(PAIMON));
        zookeeperClient.upsertServerEntityConfig(Collections.emptyMap());
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(coordinatorService.getDataLakeFormat()).isNull());

        // if zookeeper with an invalid config, coordinator should ignore it.
        Map<String, String> config = new HashMap<>();
        config.put(DATALAKE_FORMAT.key(), "paimon");
        config.put("un_support_key", "value");
        zookeeperClient.upsertServerEntityConfig(config);

        //        ZkNodeChangeNotificationWatcher.NotificationHandler
        // configChangedNotificationHandler =
        // coordinatorService.dynamicConfigManager.getConfigChangedNotificationHandler();
        //        configChangedNotificationHandler.processNotification(new byte[0]);
        //        assertThat(coordinatorService.getDataLakeFormat()).isNull();

    }
}
