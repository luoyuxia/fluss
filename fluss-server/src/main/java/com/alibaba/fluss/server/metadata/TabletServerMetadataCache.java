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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.tablet.TabletServer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.server.metadata.PartitionMetadata.PARTITION_DURATION_DELETE_ID;
import static com.alibaba.fluss.server.metadata.PartitionMetadata.PARTITION_DURATION_DELETE_NAME;
import static com.alibaba.fluss.server.metadata.TableMetadata.TABLE_DURATION_DELETE_ID;
import static com.alibaba.fluss.server.metadata.TableMetadata.TABLE_DURATION_DELETE_PATH;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/** The implement of {@link ServerMetadataCache} for {@link TabletServer}. */
public class TabletServerMetadataCache implements ServerMetadataCache {

    private final Lock bucketMetadataLock = new ReentrantLock();

    /**
     * This is cache state. every Cluster instance is immutable, and updates (performed under a
     * lock) replace the value with a completely new one. this means reads (which are not under any
     * lock) need to grab the value of this ONCE and retain that read copy for the duration of their
     * operation.
     *
     * <p>multiple reads of this value risk getting different snapshots.
     */
    @GuardedBy("bucketMetadataLock")
    private volatile ServerMetadataSnapshot serverMetadataSnapshot;

    public TabletServerMetadataCache() {
        // no coordinator server address while creating.
        this.serverMetadataSnapshot = ServerMetadataSnapshot.empty();
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        Set<TabletServerInfo> aliveTabletServersById =
                serverMetadataSnapshot.getAliveTabletServerInfos();
        for (TabletServerInfo tabletServerInfo : aliveTabletServersById) {
            if (tabletServerInfo.getId() == serverId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<ServerNode> getTabletServer(int serverId, String listenerName) {
        return serverMetadataSnapshot.getAliveTabletServersById(serverId, listenerName);
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName) {
        return serverMetadataSnapshot.getAliveTabletServers(listenerName);
    }

    @Override
    public @Nullable ServerNode getCoordinatorServer(String listenerName) {
        return serverMetadataSnapshot.getCoordinatorServer(listenerName);
    }

    @Override
    public Set<TabletServerInfo> getAliveTabletServerInfos() {
        return serverMetadataSnapshot.getAliveTabletServerInfos();
    }

    @Override
    public Optional<TablePath> getTablePath(long tableId) {
        return serverMetadataSnapshot.getTablePath(tableId);
    }

    @Override
    public Optional<PhysicalTablePath> getPhysicalTablePath(long partitionId) {
        return serverMetadataSnapshot.getPhysicalTablePath(partitionId);
    }

    @Override
    public Optional<TableInfo> getTableInfo(long tableId) {
        return serverMetadataSnapshot.getTableInfo(tableId);
    }

    @Override
    public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;
        long tableId = snapshot.getTableId(tablePath);
        if (tableId == TableInfo.UNKNOWN_TABLE_ID) {
            return Optional.empty();
        } else {
            Optional<TableInfo> tableInfoOpt = snapshot.getTableInfo(tableId);
            if (tableInfoOpt.isPresent()) {
                TableInfo tableInfo = tableInfoOpt.get();
                List<BucketMetadata> bucketMetadataList = new ArrayList<>();
                snapshot.getBucketMetadata(tableId)
                        .forEach(
                                (bucketId, bucketMetadata) ->
                                        bucketMetadataList.add(bucketMetadata));
                return Optional.of(new TableMetadata(tableInfo, bucketMetadataList));
            } else {
                return Optional.empty();
            }
        }
    }

    @Override
    public Optional<PartitionMetadata> getPartitionMetadata(PhysicalTablePath partitionPath) {
        TablePath tablePath =
                new TablePath(partitionPath.getDatabaseName(), partitionPath.getTableName());
        String partitionName = partitionPath.getPartitionName();
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;

        long tableId = snapshot.getTableId(tablePath);
        Optional<Long> partitionIdOpt = snapshot.getPartitionId(partitionPath);
        if (partitionIdOpt.isPresent()) {
            long partitionId = partitionIdOpt.get();
            List<BucketMetadata> bucketMetadataList = new ArrayList<>();
            snapshot.getBucketMetadata(new TablePartition(tableId, partitionId))
                    .forEach((bucketId, bucketMetadata) -> bucketMetadataList.add(bucketMetadata));
            return Optional.of(
                    new PartitionMetadata(tableId, partitionName, partitionId, bucketMetadataList));
        } else {
            return Optional.empty();
        }
    }

    public void updateClusterMetadata(ClusterMetadata clusterMetadata) {
        inLock(
                bucketMetadataLock,
                () -> {
                    // 1. update coordinator server.
                    ServerInfo coordinatorServer = clusterMetadata.getCoordinatorServer();

                    // 2. Update the alive table servers. We always use the new alive table servers
                    // to replace the old alive table servers.
                    HashMap<Integer, ServerInfo> newAliveTableServers = new HashMap<>();
                    Set<ServerInfo> aliveTabletServers = clusterMetadata.getAliveTabletServers();
                    for (ServerInfo tabletServer : aliveTabletServers) {
                        newAliveTableServers.put(tabletServer.id(), tabletServer);
                    }

                    // 3. update table metadata. Always partial update.
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getTableIdByPath());
                    Map<Long, TableInfo> tableInfoByTableId =
                            new HashMap<>(serverMetadataSnapshot.getTableInfoByTableId());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMap =
                            new HashMap<>(serverMetadataSnapshot.getBucketMetadataMap());

                    for (TableMetadata tableMetadata : clusterMetadata.getTableMetadataList()) {
                        TableInfo tableInfo = tableMetadata.getTableInfo();
                        TablePath tablePath = tableInfo.getTablePath();
                        long tableId = tableInfo.getTableId();
                        if (tableId == TABLE_DURATION_DELETE_ID) {
                            Long removedTableId = tableIdByPath.remove(tablePath);
                            tableInfoByTableId.remove(removedTableId);
                            bucketMetadataMap.remove(removedTableId);
                        } else if (tablePath == TABLE_DURATION_DELETE_PATH) {
                            serverMetadataSnapshot
                                    .getTablePath(tableId)
                                    .ifPresent(tableIdByPath::remove);
                            tableInfoByTableId.remove(tableId);
                            bucketMetadataMap.remove(tableId);
                        } else {
                            tableIdByPath.put(tablePath, tableId);
                            tableInfoByTableId.put(tableId, tableInfo);
                            tableMetadata
                                    .getBucketMetadataList()
                                    .forEach(
                                            bucketMetadata ->
                                                    bucketMetadataMap
                                                            .computeIfAbsent(
                                                                    tableId, k -> new HashMap<>())
                                                            .put(
                                                                    bucketMetadata.getBucketId(),
                                                                    bucketMetadata));
                        }
                    }

                    Map<Long, TablePath> newPathByTableId = new HashMap<>();
                    tableIdByPath.forEach(
                            ((tablePath, tableId) -> newPathByTableId.put(tableId, tablePath)));

                    // 4. update partition metadata. Always partial update.
                    Map<PhysicalTablePath, Long> partitionsIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getPartitionIdByPath());
                    Map<TablePartition, Map<Integer, BucketMetadata>>
                            bucketMetadataMapForPartitionTable =
                                    new HashMap<>(
                                            serverMetadataSnapshot
                                                    .getBucketMetadataMapForPartitionedTable());

                    for (PartitionMetadata partitionMetadata :
                            clusterMetadata.getPartitionMetadataList()) {
                        long tableId = partitionMetadata.getTableId();
                        TablePath tablePath = newPathByTableId.get(tableId);
                        String partitionName = partitionMetadata.getPartitionName();
                        PhysicalTablePath physicalTablePath =
                                PhysicalTablePath.of(tablePath, partitionName);
                        long partitionId = partitionMetadata.getPartitionId();
                        if (partitionId == PARTITION_DURATION_DELETE_ID) {
                            long removedPartitionId = partitionsIdByPath.remove(physicalTablePath);
                            bucketMetadataMapForPartitionTable.remove(
                                    new TablePartition(tableId, removedPartitionId));
                        } else if (partitionName.equals(PARTITION_DURATION_DELETE_NAME)) {
                            serverMetadataSnapshot
                                    .getPhysicalTablePath(partitionId)
                                    .ifPresent(partitionsIdByPath::remove);
                            bucketMetadataMapForPartitionTable.remove(
                                    new TablePartition(tableId, partitionId));
                        } else {
                            partitionsIdByPath.put(physicalTablePath, partitionId);
                            partitionMetadata
                                    .getBucketMetadataList()
                                    .forEach(
                                            bucketMetadata ->
                                                    bucketMetadataMapForPartitionTable
                                                            .computeIfAbsent(
                                                                    new TablePartition(
                                                                            tableId, partitionId),
                                                                    k -> new HashMap<>())
                                                            .put(
                                                                    bucketMetadata.getBucketId(),
                                                                    bucketMetadata));
                        }
                    }

                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    coordinatorServer,
                                    newAliveTableServers,
                                    tableIdByPath,
                                    newPathByTableId,
                                    partitionsIdByPath,
                                    tableInfoByTableId,
                                    bucketMetadataMap,
                                    bucketMetadataMapForPartitionTable);
                });
    }
}
