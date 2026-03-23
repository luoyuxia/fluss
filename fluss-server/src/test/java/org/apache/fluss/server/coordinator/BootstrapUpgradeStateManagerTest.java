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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BootstrapUpgradeStatus;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link BootstrapUpgradeStateManager}. */
class BootstrapUpgradeStateManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zooKeeperClient;
    private static BootstrapUpgradeStateManager stateManager;

    @BeforeAll
    static void beforeAll() {
        zooKeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        stateManager = new BootstrapUpgradeStateManager(zooKeeperClient);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        zooKeeperClient.close();
    }

    @Test
    void testInitializeInProgressIsIdempotentForSamePartition() {
        long tableId = 1L;
        String holdPartition = "dt=2026-03-30";

        stateManager.initializeInProgress(tableId, holdPartition);
        stateManager.initializeInProgress(tableId, holdPartition);

        assertThat(stateManager.get(tableId)).isPresent();
        assertThat(stateManager.get(tableId).get().getStatus())
                .isEqualTo(BootstrapUpgradeStatus.IN_PROGRESS);
        assertThat(stateManager.get(tableId).get().getHoldPartition()).isEqualTo(holdPartition);
    }

    @Test
    void testInitializeInProgressRejectsConflictingPartition() {
        long tableId = 2L;
        stateManager.initializeInProgress(tableId, "dt=2026-03-30");

        assertThatThrownBy(() -> stateManager.initializeInProgress(tableId, "dt=2026-03-31"))
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("Bootstrap-upgrade state already exists");
    }
}
