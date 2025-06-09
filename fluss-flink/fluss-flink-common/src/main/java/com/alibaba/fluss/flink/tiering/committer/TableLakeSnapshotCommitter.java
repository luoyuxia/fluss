/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.FlussConnection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.committer.CommittedOffsets;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import com.alibaba.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.types.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Committer to commit {@link TableLakeSnapshot} of lake to Fluss. */
public class TableLakeSnapshotCommitter implements AutoCloseable {

    private final Configuration flussConf;

    private CoordinatorGateway coordinatorGateway;
    private FlussConnection connection;
    private Admin admin;

    public TableLakeSnapshotCommitter(Configuration flussConf) {
        this.flussConf = flussConf;
    }

    public void open() {
        // init coordinator gateway
        connection = (FlussConnection) ConnectionFactory.createConnection(flussConf);
        RpcClient rpcClient = connection.getRpcClient();
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        admin = connection.getAdmin();
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
    }

    public void commit(TableLakeSnapshot tableLakeSnapshot) throws IOException {
        try {
            CommitLakeTableSnapshotRequest request =
                    toCommitLakeTableSnapshotRequest(tableLakeSnapshot);
            coordinatorGateway.commitLakeTableSnapshot(request).get();
        } catch (Exception e) {
            throw new IOException(
                    String.format(
                            "Fail to commit table lake snapshot %s to Fluss.", tableLakeSnapshot),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    public void commit(
            TablePath tablePath,
            long tableId,
            boolean isPartitioned,
            CommittedOffsets toCommitOffsets)
            throws Exception {
        TableLakeSnapshot tableLakeSnapshot =
                new TableLakeSnapshot(tableId, toCommitOffsets.getSnapshotId());
        Map<String, Long> partitionIdByName = new HashMap<>();
        if (isPartitioned) {
            List<PartitionInfo> partitionInfos = admin.listPartitionInfos(tablePath).get();
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitionIdByName.put(
                        partitionInfo.getPartitionName(), partitionInfo.getPartitionId());
            }
        }
        for (Map.Entry<Tuple2<String, Integer>, Long> entry :
                toCommitOffsets.getCommitedOffsets().entrySet()) {
            Tuple2<String, Integer> partitionBucket = entry.getKey();
            TableBucket tableBucket;
            if (partitionBucket.f0 == null) {
                tableBucket = new TableBucket(tableId, partitionBucket.f1);
            } else {
                String partitionName = partitionBucket.f0;
                // todo: what if partition rename, drop + create?
                Long partitionId = partitionIdByName.get(partitionName);
                if (partitionId != null) {
                    tableBucket = new TableBucket(tableId, partitionId, partitionBucket.f1);
                } else {
                    // let's skip the bucket
                    continue;
                }
            }
            tableLakeSnapshot.addBucketOffset(tableBucket, entry.getValue());
        }

        commit(tableLakeSnapshot);
    }

    private CommitLakeTableSnapshotRequest toCommitLakeTableSnapshotRequest(
            TableLakeSnapshot tableLakeSnapshot) {
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo pbLakeTableSnapshotInfo =
                commitLakeTableSnapshotRequest.addTablesReq();

        pbLakeTableSnapshotInfo.setTableId(tableLakeSnapshot.tableId());
        pbLakeTableSnapshotInfo.setSnapshotId(tableLakeSnapshot.snapshotId());
        for (Map.Entry<TableBucket, Long> bucketEndOffsetEntry :
                tableLakeSnapshot.logEndOffsets().entrySet()) {
            PbLakeTableOffsetForBucket pbLakeTableOffsetForBucket =
                    pbLakeTableSnapshotInfo.addBucketsReq();
            TableBucket tableBucket = bucketEndOffsetEntry.getKey();
            long endOffset = bucketEndOffsetEntry.getValue();
            if (tableBucket.getPartitionId() != null) {
                pbLakeTableOffsetForBucket.setPartitionId(tableBucket.getPartitionId());
            }
            pbLakeTableOffsetForBucket.setBucketId(tableBucket.getBucket());
            pbLakeTableOffsetForBucket.setLogEndOffset(endOffset);
        }
        return commitLakeTableSnapshotRequest;
    }

    @Override
    public void close() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
