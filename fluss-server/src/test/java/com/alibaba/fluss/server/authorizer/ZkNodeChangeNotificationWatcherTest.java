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

package com.alibaba.fluss.server.authorizer;

import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.data.ZkData;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ZkNodeChangeNotificationWatcher }. */
public class ZkNodeChangeNotificationWatcherTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @Test
    void testZkNodeChangeNotifications() throws Exception {
        String seqNodeRoot = ZkData.AclChangesNode.path();
        String seqNodePrefix = ZkData.AclChangeNotificationNode.prefix();
        ZooKeeperClient zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        TestingNotificationHandler handler = new TestingNotificationHandler();

        long startTime = System.currentTimeMillis();
        ManualClock clock = new ManualClock(startTime);
        // Step 1: Insert initial notifications before starting the watcher
        ArrayList<Resource> expectedRegistrations =
                new ArrayList<>(
                        Arrays.asList(
                                Resource.cluster(),
                                Resource.database("test_database1"),
                                Resource.table("test_database2", "test_table")));
        for (Resource resource : expectedRegistrations) {
            zookeeperClient.insertAclChangeNotification(resource);
        }

        ZkNodeChangeNotificationWatcher aclChangeWatcher =
                new ZkNodeChangeNotificationWatcher(
                        zookeeperClient,
                        seqNodeRoot,
                        seqNodePrefix,
                        Duration.ofMinutes(5).toMillis(),
                        handler,
                        clock);
        aclChangeWatcher.start();
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(handler.resourceNotifications)
                                .containsExactlyInAnyOrderElementsOf(expectedRegistrations));
        // Verify that all initial notifications are processed
        List<String> nodesBeforeStart = zookeeperClient.getChildren(seqNodeRoot);
        assertThat(nodesBeforeStart).hasSize(3);

        // Step 2: Insert a new notification after the watcher has started
        Resource newNoticedResource = Resource.table("test_database3", "test_table");
        zookeeperClient.insertAclChangeNotification(newNoticedResource);
        expectedRegistrations.add(newNoticedResource);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(handler.resourceNotifications)
                                .containsExactlyInAnyOrderElementsOf(expectedRegistrations));
        // Verify that the new notification is processed
        List<String> nodesAfterStart = zookeeperClient.getChildren(ZkData.AclChangesNode.path());
        assertThat(nodesAfterStart).hasSize(4);

        // Step 3: Test purging of obsolete notifications
        long maxCtimeBeforeStart = 0;
        for (String node : nodesBeforeStart) {
            maxCtimeBeforeStart =
                    Math.max(
                            zookeeperClient.getState(seqNodeRoot + "/" + node).get().getCtime(),
                            maxCtimeBeforeStart);
        }
        // Advance the clock to make the initial notifications obsolete
        clock.advanceTime(
                maxCtimeBeforeStart - startTime + Duration.ofMinutes(5).toMillis(),
                TimeUnit.MILLISECONDS);
        // Insert a new notification to trigger the purging of obsolete notificatios.
        zookeeperClient.insertAclChangeNotification(newNoticedResource);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zookeeperClient.getChildren(seqNodeRoot)).hasSize(2));
    }

    private static class TestingNotificationHandler
            implements ZkNodeChangeNotificationWatcher.NotificationHandler {
        public List<Resource> resourceNotifications = new ArrayList<>();

        @Override
        public void processNotification(byte[] notification) {
            Resource resource = ZkData.AclChangeNotificationNode.decode(notification);
            resourceNotifications.add(resource);
        }
    }
}
