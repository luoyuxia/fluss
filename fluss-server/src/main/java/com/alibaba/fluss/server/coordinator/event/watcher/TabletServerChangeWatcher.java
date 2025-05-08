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

package com.alibaba.fluss.server.coordinator.event.watcher;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.server.coordinator.event.DeadTabletServerEvent;
import com.alibaba.fluss.server.coordinator.event.EventManager;
import com.alibaba.fluss.server.coordinator.event.NewTabletServerEvent;
import com.alibaba.fluss.server.metadata.ServerInfo;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.TabletServerRegistration;
import com.alibaba.fluss.server.zk.data.ZkData.ServerIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.ServerIdsZNode;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.utils.ZKPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A watcher to watch the tablet server changes(new/delete) in zookeeper. */
public class TabletServerChangeWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(TabletServerChangeWatcher.class);
    private final CuratorCache curatorCache;

    private volatile boolean running;

    private final EventManager eventManager;

    public TabletServerChangeWatcher(ZooKeeperClient zooKeeperClient, EventManager eventManager) {
        this.curatorCache =
                CuratorCache.build(zooKeeperClient.getCuratorClient(), ServerIdsZNode.path());
        this.eventManager = eventManager;
        this.curatorCache.listenable().addListener(new TabletServerChangeListener());
    }

    public void start() {
        running = true;
        curatorCache.start();
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        LOG.info("Stopping TableChangeWatcher");
        curatorCache.close();
    }

    private final class TabletServerChangeListener implements CuratorCacheListener {

        @Override
        public void event(Type type, ChildData oldData, ChildData newData) {
            if (newData != null) {
                LOG.debug("Received {} event (path: {})", type, newData.getPath());
            } else {
                LOG.debug("Received {} event", type);
            }

            switch (type) {
                case NODE_CREATED:
                    {
                        if (newData != null && newData.getData().length > 0) {
                            int serverId = getServerIdFromEvent(newData);
                            TabletServerRegistration registration =
                                    ServerIdZNode.decode(newData.getData());
                            LOG.info("Received CHILD_ADDED event for server {}.", serverId);
                            eventManager.put(
                                    new NewTabletServerEvent(
                                            new ServerInfo(
                                                    serverId,
                                                    registration.getRack(),
                                                    registration.getEndpoints(),
                                                    ServerType.TABLET_SERVER)));
                        }
                        break;
                    }
                case NODE_DELETED:
                    {
                        if (oldData != null && oldData.getData().length > 0) {
                            int serverId = getServerIdFromEvent(oldData);
                            LOG.info("Received CHILD_REMOVED event for server {}.", serverId);
                            eventManager.put(new DeadTabletServerEvent(serverId));
                        }
                        break;
                    }
                default:
                    break;
            }
        }
    }

    private int getServerIdFromEvent(ChildData data) {
        try {
            return Integer.parseInt(ZKPaths.getNodeFromPath(data.getPath()));
        } catch (NumberFormatException e) {
            throw new FlussRuntimeException(
                    "Invalid server id in zookeeper path: " + data.getPath(), e);
        }
    }
}
