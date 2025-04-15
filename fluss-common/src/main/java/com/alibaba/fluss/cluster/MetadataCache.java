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

package com.alibaba.fluss.cluster;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.PhysicalTablePath;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The metadata cache to cache the cluster metadata info.
 *
 * <p>Note : For this interface we only support cache cluster server node info.
 */
@Internal
public interface MetadataCache {

    /**
     * Get the coordinator server node.
     *
     * @return the coordinator server node
     */
    Optional<ServerNode> getCoordinatorServer(String listenerName);

    /**
     * Check whether the tablet server id related tablet server node is alive.
     *
     * @param serverId the tablet server id
     * @return true if the server is alive, false otherwise
     */
    boolean isAliveTabletServer(int serverId);

    /**
     * Get the tablet server.
     *
     * @param serverId the tablet server id
     * @return the tablet server node
     */
    Optional<ServerNode> getTabletServer(int serverId, String listenerName);

    /**
     * Get all alive tablet server nodes.
     *
     * @return all alive tablet server nodes
     */
    Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName);

    Set<Integer> getAliveTabletServerIds();

    PhysicalTablePath getTablePath(long tableId);

    /** Get ids of all alive tablet server nodes. */
    default int[] getLiveServerIds() {
        Set<Integer> aliveTabletServerIds = getAliveTabletServerIds();
        int[] server = new int[aliveTabletServerIds.size()];
        Iterator<Integer> iterator = aliveTabletServerIds.iterator();
        for (int i = 0; i < aliveTabletServerIds.size(); i++) {
            server[i] = iterator.next();
        }
        return server;
    }
}
