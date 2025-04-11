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

package com.alibaba.fluss.server.tablet;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.LimitScanRequest;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.LookupResponse;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import com.alibaba.fluss.rpc.messages.PrefixLookupRequest;
import com.alibaba.fluss.rpc.messages.PrefixLookupResponse;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.rpc.messages.PutKvResponse;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.messages.StopReplicaResponse;
import com.alibaba.fluss.server.RpcServiceBase;
import com.alibaba.fluss.server.coordinator.MetadataManager;
import com.alibaba.fluss.server.entity.FetchData;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.log.ListOffsetsParam;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.zk.ZooKeeperClient;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.server.log.FetchParams.DEFAULT_MAX_WAIT_MS_WHEN_MIN_BYTES_ENABLE;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getListOffsetsData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifyLakeTableOffset;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifyLeaderAndIsrRequestData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifyRemoteLogOffsetsData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifySnapshotOffsetData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getProduceLogData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getPutKvData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getStopReplicaData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getTargetColumns;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeInitWriterResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeLimitScanResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeListOffsetsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeLookupResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makePrefixLookupResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makePutKvResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeStopReplicaResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toLookupData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toPrefixLookupData;

/** An RPC Gateway service for tablet server. */
public final class TabletService extends RpcServiceBase implements TabletServerGateway {

    private final String serviceName;
    private final ReplicaManager replicaManager;

    public TabletService(
            int serverId,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            ReplicaManager replicaManager,
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager) {
        super(remoteFileSystem, ServerType.TABLET_SERVER, zkClient, metadataCache, metadataManager);
        this.serviceName = "server-" + serverId;
        this.replicaManager = replicaManager;
    }

    @Override
    public String name() {
        return serviceName;
    }

    @Override
    public void shutdown() {}

    @Override
    public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
        CompletableFuture<ProduceLogResponse> response = new CompletableFuture<>();
        Map<TableBucket, MemoryLogRecords> produceLogData = getProduceLogData(request);
        replicaManager.appendRecordsToLog(
                request.getTimeoutMs(),
                request.getAcks(),
                produceLogData,
                bucketResponseMap -> response.complete(makeProduceLogResponse(bucketResponseMap)));
        return response;
    }

    @Override
    public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
        CompletableFuture<FetchLogResponse> response = new CompletableFuture<>();
        Map<TableBucket, FetchData> fetchLogData = getFetchLogData(request);
        FetchParams fetchParams;
        if (request.hasMinBytes()) {
            fetchParams =
                    new FetchParams(
                            request.getFollowerServerId(),
                            request.getMaxBytes(),
                            request.getMinBytes(),
                            request.hasMaxWaitMs()
                                    ? request.getMaxWaitMs()
                                    : DEFAULT_MAX_WAIT_MS_WHEN_MIN_BYTES_ENABLE);
        } else {
            fetchParams = new FetchParams(request.getFollowerServerId(), request.getMaxBytes());
        }
        replicaManager.fetchLogRecords(
                fetchParams,
                fetchLogData,
                fetchResponseMap -> response.complete(makeFetchLogResponse(fetchResponseMap)));
        return response;
    }

    @Override
    public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
        CompletableFuture<PutKvResponse> response = new CompletableFuture<>();
        Map<TableBucket, KvRecordBatch> putKvData = getPutKvData(request);
        replicaManager.putRecordsToKv(
                request.getTimeoutMs(),
                request.getAcks(),
                putKvData,
                getTargetColumns(request),
                bucketResponseMap -> response.complete(makePutKvResponse(bucketResponseMap)));
        return response;
    }

    @Override
    public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
        CompletableFuture<LookupResponse> response = new CompletableFuture<>();
        Map<TableBucket, List<byte[]>> lookupData = toLookupData(request);
        replicaManager.lookups(lookupData, value -> response.complete(makeLookupResponse(value)));
        return response;
    }

    @Override
    public CompletableFuture<PrefixLookupResponse> prefixLookup(PrefixLookupRequest request) {
        CompletableFuture<PrefixLookupResponse> response = new CompletableFuture<>();
        replicaManager.prefixLookups(
                toPrefixLookupData(request),
                value -> response.complete(makePrefixLookupResponse(value)));
        return response;
    }

    @Override
    public CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request) {
        CompletableFuture<LimitScanResponse> response = new CompletableFuture<>();
        replicaManager.limitScan(
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId()),
                request.getLimit(),
                value -> response.complete(makeLimitScanResponse(value)));
        return response;
    }

    @Override
    public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest) {
        CompletableFuture<NotifyLeaderAndIsrResponse> response = new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                notifyLeaderAndIsrRequest.getCoordinatorEpoch(),
                getNotifyLeaderAndIsrRequestData(notifyLeaderAndIsrRequest),
                result -> response.complete(makeNotifyLeaderAndIsrResponse(result)));
        return response;
    }

    @Override
    public CompletableFuture<StopReplicaResponse> stopReplica(
            StopReplicaRequest stopReplicaRequest) {
        CompletableFuture<StopReplicaResponse> response = new CompletableFuture<>();
        replicaManager.stopReplicas(
                stopReplicaRequest.getCoordinatorEpoch(),
                getStopReplicaData(stopReplicaRequest),
                result -> response.complete(makeStopReplicaResponse(result)));
        return response;
    }

    @Override
    public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
        CompletableFuture<ListOffsetsResponse> response = new CompletableFuture<>();
        Set<TableBucket> tableBuckets = getListOffsetsData(request);
        replicaManager.listOffsets(
                new ListOffsetsParam(
                        request.getFollowerServerId(),
                        request.hasOffsetType() ? request.getOffsetType() : null,
                        request.hasStartTimestamp() ? request.getStartTimestamp() : null),
                tableBuckets,
                (responseList) -> response.complete(makeListOffsetsResponse(responseList)));
        return response;
    }

    @Override
    public CompletableFuture<InitWriterResponse> initWriter(InitWriterRequest request) {
        CompletableFuture<InitWriterResponse> response = new CompletableFuture<>();
        response.complete(makeInitWriterResponse(metadataManager.initWriterId()));
        return response;
    }

    @Override
    public CompletableFuture<NotifyRemoteLogOffsetsResponse> notifyRemoteLogOffsets(
            NotifyRemoteLogOffsetsRequest request) {
        CompletableFuture<NotifyRemoteLogOffsetsResponse> response = new CompletableFuture<>();
        replicaManager.notifyRemoteLogOffsets(
                getNotifyRemoteLogOffsetsData(request), response::complete);
        return response;
    }

    @Override
    public CompletableFuture<NotifyKvSnapshotOffsetResponse> notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetRequest request) {
        CompletableFuture<NotifyKvSnapshotOffsetResponse> response = new CompletableFuture<>();
        replicaManager.notifyKvSnapshotOffset(
                getNotifySnapshotOffsetData(request), response::complete);
        return response;
    }

    @Override
    public CompletableFuture<NotifyLakeTableOffsetResponse> notifyLakeTableOffset(
            NotifyLakeTableOffsetRequest request) {
        CompletableFuture<NotifyLakeTableOffsetResponse> response = new CompletableFuture<>();
        replicaManager.notifyLakeTableOffset(getNotifyLakeTableOffset(request), response::complete);
        return response;
    }
}
