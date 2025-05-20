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

package com.alibaba.fluss.flink.laketiering.committer;

import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.FlussConnection;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.LakeTableSnapshotNotExistException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import com.alibaba.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import com.alibaba.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/** Committer to commit {@link TableLakeSnapshot} of lake to Fluss. */
public class TableLakeSnapshotCommitter implements AutoCloseable {

    private final Configuration flussConf;

    private CoordinatorGateway coordinatorGateway;
    private FlussConnection connection;
    private Admin flussAdmin;

    public TableLakeSnapshotCommitter(Configuration flussConf) {
        this.flussConf = flussConf;
    }

    public void open() {
        // init admin client
        connection = (FlussConnection) ConnectionFactory.createConnection(flussConf);
        flussAdmin = connection.getAdmin();
        RpcClient rpcClient = connection.getRpcClient();
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
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
                            "Fail to commit table lake snapshot %s to Fluss", tableLakeSnapshot),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    @Nullable
    public Long getLatestLakeSnapshot(TablePath tablePath) throws IOException {
        try {
            LakeSnapshot lakeSnapshot = flussAdmin.getLatestLakeSnapshot(tablePath).get();
            return lakeSnapshot.getSnapshotId();
        } catch (Exception e) {
            Throwable throwable = ExceptionUtils.stripExecutionException(e);
            if (throwable instanceof LakeTableSnapshotNotExistException) {
                return null;
            } else {
                throw new IOException(
                        String.format(
                                "Fail to get latest lake snapshot for table %s",
                                tablePath.getTableName()),
                        throwable);
            }
        }
    }

    public TableInfo getTableInfo(TablePath tablePath) throws IOException {
        try {
            return flussAdmin.getTableInfo(tablePath).get();
        } catch (Exception e) {
            throw new IOException(
                    String.format("Fail to get table info for table %s", tablePath.getTableName()),
                    ExceptionUtils.stripExecutionException(e));
        }
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
        if (flussAdmin != null) {
            flussAdmin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
