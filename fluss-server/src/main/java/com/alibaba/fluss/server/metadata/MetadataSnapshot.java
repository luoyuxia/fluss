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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** A snapshot of metadata of Fluss cluster that caches the necessary metadata the server needs. */
public class MetadataSnapshot {

    @Nullable private final ServerNode coordinatorServer;
    private final Map<Integer, ServerNode> aliveTabletServersById;

    private final Map<TableBucket, Integer> bucketLeaders;

    public MetadataSnapshot(
            Map<Integer, ServerNode> aliveTabletServersById,
            @Nullable ServerNode coordinatorServer) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServersById = Collections.unmodifiableMap(aliveTabletServersById);
        this.bucketLeaders = new HashMap<>();
    }

    public MetadataSnapshot(
            Map<Integer, ServerNode> aliveTabletServersById,
            @Nullable ServerNode coordinatorServer,
            Map<TableBucket, Integer> bucketLeaders) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServersById = aliveTabletServersById;
        this.bucketLeaders = bucketLeaders;
    }

    public MetadataSnapshot updateBucketLeaders(Map<TableBucket, Integer> newBucketLeaders) {
        Map<TableBucket, Integer> oldBucketLeaders = new HashMap<>(bucketLeaders);
        oldBucketLeaders.putAll(newBucketLeaders);
        return new MetadataSnapshot(aliveTabletServersById, coordinatorServer, oldBucketLeaders);
    }

    public MetadataSnapshot updateClusterServers(
            Map<Integer, ServerNode> aliveTabletServersById,
            @Nullable ServerNode coordinatorServer) {
        return new MetadataSnapshot(aliveTabletServersById, coordinatorServer, bucketLeaders);
    }

    public Map<Integer, ServerNode> getAliveTabletServers() {
        return aliveTabletServersById;
    }

    /** Get alive tablet server by id. */
    public Optional<ServerNode> getAliveTabletServerById(int serverId) {
        return Optional.ofNullable(aliveTabletServersById.get(serverId));
    }

    public Integer getLeader(TableBucket tableBucket) {
        return bucketLeaders.get(tableBucket);
    }

    @Nullable
    public ServerNode getCoordinatorServer() {
        return coordinatorServer;
    }

    /** Create an empty cluster instance with no nodes and no table-buckets. */
    public static MetadataSnapshot empty() {
        return new MetadataSnapshot(Collections.emptyMap(), null);
    }
}
