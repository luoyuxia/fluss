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
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.PbBucketMetadata;
import com.alibaba.fluss.rpc.messages.PbClusterServerMetadata;
import com.alibaba.fluss.rpc.messages.PbServerNode;
import com.alibaba.fluss.rpc.messages.PbTableBucketMetadata;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/** The default implement of {@link ServerMetadataCache}. */
public class ServerMetadataCacheImpl extends AbstractServerMetadataCache {
    private final Lock bucketMetadataLock = new ReentrantLock();

    public ServerMetadataCacheImpl() {
        super();
    }

    @Override
    public void updateMetadata(UpdateMetadataRequest updateMetadataRequest) {
        inLock(
                bucketMetadataLock,
                () -> {
                    MetadataSnapshot newMetadataSnapshot = metadataSnapshot;
                    if (updateMetadataRequest.hasClusterServerMetadata()) {
                        PbClusterServerMetadata pbClusterServerMetadata =
                                updateMetadataRequest.getClusterServerMetadata();
                        ServerNode coordinatorServer =
                                pbClusterServerMetadata.hasCoordinatorServer()
                                        ? fromPbServerNode(
                                                pbClusterServerMetadata.getCoordinatorServer(),
                                                ServerType.COORDINATOR)
                                        : null;

                        // 2. Update the alive table servers. We always use the new alive table
                        // servers
                        // to replace the old alive table servers.
                        HashMap<Integer, ServerNode> newAliveTableServers = new HashMap<>();
                        for (PbServerNode pbServerNode :
                                pbClusterServerMetadata.getTabletServersList()) {
                            newAliveTableServers.put(
                                    pbServerNode.getNodeId(),
                                    fromPbServerNode(pbServerNode, ServerType.TABLET_SERVER));
                        }
                        newMetadataSnapshot =
                                newMetadataSnapshot.updateClusterServers(
                                        newAliveTableServers, coordinatorServer);
                    }

                    Map<TableBucket, Integer> bucketLeaders = new HashMap<>();
                    for (PbTableBucketMetadata pbTableBucketMetadata :
                            updateMetadataRequest.getBucketMetadatasList()) {
                        long tableId = pbTableBucketMetadata.getTableId();
                        Long partitionId =
                                pbTableBucketMetadata.hasPartitionId()
                                        ? pbTableBucketMetadata.getPartitionId()
                                        : null;
                        for (PbBucketMetadata pbBucketMetadata :
                                pbTableBucketMetadata.getBucketMetadatasList()) {
                            bucketLeaders.put(
                                    new TableBucket(
                                            tableId, partitionId, pbBucketMetadata.getBucketId()),
                                    pbBucketMetadata.getLeaderId());
                        }
                    }

                    if (!bucketLeaders.isEmpty()) {
                        newMetadataSnapshot =
                                newMetadataSnapshot.updateBucketLeaders(bucketLeaders);
                    }

                    if (newMetadataSnapshot != metadataSnapshot) {
                        metadataSnapshot = newMetadataSnapshot;
                    }
                });
    }

    @Nullable
    @Override
    public Integer getLeader(TableBucket tableBucket) {
        return metadataSnapshot.getLeader(tableBucket);
    }

    private ServerNode fromPbServerNode(PbServerNode pbServerNode, ServerType serverType) {
        return new ServerNode(
                pbServerNode.getNodeId(),
                pbServerNode.getHost(),
                pbServerNode.getPort(),
                serverType);
    }
}
