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

package com.alibaba.fluss.server;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.KvSnapshotNotExistException;
import com.alibaba.fluss.exception.LakeTableSnapshotNotExistException;
import com.alibaba.fluss.exception.NonPrimaryKeyTableException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.exception.SecurityDisabledException;
import com.alibaba.fluss.exception.SecurityTokenException;
import com.alibaba.fluss.exception.TableNotPartitionedException;
import com.alibaba.fluss.exception.UnknownTableOrBucketException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.metadata.DatabaseInfo;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.RpcGatewayService;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
import com.alibaba.fluss.rpc.messages.GetDatabaseInfoRequest;
import com.alibaba.fluss.rpc.messages.GetDatabaseInfoResponse;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotMetadataRequest;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import com.alibaba.fluss.rpc.messages.GetLatestKvSnapshotsRequest;
import com.alibaba.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import com.alibaba.fluss.rpc.messages.GetLatestLakeSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import com.alibaba.fluss.rpc.messages.GetTableInfoRequest;
import com.alibaba.fluss.rpc.messages.GetTableInfoResponse;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.GetTableSchemaResponse;
import com.alibaba.fluss.rpc.messages.ListAclsRequest;
import com.alibaba.fluss.rpc.messages.ListAclsResponse;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosRequest;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosResponse;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.PbApiVersion;
import com.alibaba.fluss.rpc.messages.PbTablePath;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.messages.TableExistsResponse;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.server.authorizer.Authorizer;
import com.alibaba.fluss.server.coordinator.CoordinatorService;
import com.alibaba.fluss.server.coordinator.MetadataManager;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.metadata.PartitionMetadata;
import com.alibaba.fluss.server.metadata.ServerMetadataCache;
import com.alibaba.fluss.server.metadata.TableMetadata;
import com.alibaba.fluss.server.tablet.TabletService;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.BucketSnapshot;
import com.alibaba.fluss.server.zk.data.LakeTableSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toAclFilter;
import static com.alibaba.fluss.security.acl.Resource.TABLE_SPLITTER;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.buildMetadataResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeGetLatestKvSnapshotsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeGetLatestLakeSnapshotResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeKvSnapshotMetadataResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeListAclsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toGetFileSystemSecurityTokenResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toListPartitionInfosResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toPhysicalTablePath;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toTablePath;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * An RPC service basic implementation that implements the common RPC methods of {@link
 * CoordinatorService} and {@link TabletService}.
 */
public abstract class RpcServiceBase extends RpcGatewayService implements AdminReadOnlyGateway {
    private static final Logger LOG = LoggerFactory.getLogger(RpcServiceBase.class);

    private static final long TOKEN_EXPIRATION_TIME_MS = 60 * 1000;

    private final FileSystem remoteFileSystem;
    private final ServerType provider;
    private final ApiManager apiManager;
    protected final ZooKeeperClient zkClient;
    protected final ServerMetadataCache metadataCache;
    protected final MetadataManager metadataManager;
    protected final Authorizer authorizer;

    private long tokenLastUpdateTimeMs = 0;
    private ObtainedSecurityToken securityToken = null;

    public RpcServiceBase(
            FileSystem remoteFileSystem,
            ServerType provider,
            ZooKeeperClient zkClient,
            ServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer) {
        this.remoteFileSystem = remoteFileSystem;
        this.provider = provider;
        this.apiManager = new ApiManager(provider);
        this.zkClient = zkClient;
        this.metadataCache = metadataCache;
        this.metadataManager = metadataManager;
        this.authorizer = authorizer;
    }

    @Override
    public ServerType providerType() {
        return provider;
    }

    @Override
    public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
        Set<ApiKeys> apiKeys = apiManager.enabledApis();
        List<PbApiVersion> apiVersions = new ArrayList<>();
        for (ApiKeys api : apiKeys) {
            apiVersions.add(
                    new PbApiVersion()
                            .setApiKey(api.id)
                            .setMinVersion(api.lowestSupportedVersion)
                            .setMaxVersion(api.highestSupportedVersion));
        }
        ApiVersionsResponse response = new ApiVersionsResponse();
        response.addAllApiVersions(apiVersions);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<ListDatabasesResponse> listDatabases(ListDatabasesRequest request) {
        ListDatabasesResponse response = new ListDatabasesResponse();
        Collection<String> databaseNames = metadataManager.listDatabases();

        if (authorizer != null) {
            Collection<Resource> authorizedDatabase =
                    authorizer.filterByAuthorized(
                            currentSession(),
                            OperationType.DESCRIBE,
                            databaseNames.stream()
                                    .map(Resource::database)
                                    .collect(Collectors.toList()));
            databaseNames =
                    authorizedDatabase.stream().map(Resource::getName).collect(Collectors.toList());
        }

        response.addAllDatabaseNames(databaseNames);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetDatabaseInfoResponse> getDatabaseInfo(
            GetDatabaseInfoRequest request) {
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.DESCRIBE,
                    Resource.database(request.getDatabaseName()));
        }

        GetDatabaseInfoResponse response = new GetDatabaseInfoResponse();
        DatabaseInfo databaseInfo = metadataManager.getDatabase(request.getDatabaseName());
        response.setDatabaseJson(databaseInfo.getDatabaseDescriptor().toJsonBytes())
                .setCreatedTime(databaseInfo.getCreatedTime())
                .setModifiedTime(databaseInfo.getModifiedTime());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<DatabaseExistsResponse> databaseExists(DatabaseExistsRequest request) {
        DatabaseExistsResponse response = new DatabaseExistsResponse();
        boolean exists = metadataManager.databaseExists(request.getDatabaseName());
        response.setExists(exists);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
        ListTablesResponse response = new ListTablesResponse();
        List<String> tableNames = metadataManager.listTables(request.getDatabaseName());
        if (authorizer != null) {
            List<Resource> resources =
                    tableNames.stream()
                            .map(t -> Resource.table(request.getDatabaseName(), t))
                            .collect(Collectors.toList());
            Collection<Resource> authorizedTable =
                    authorizer.filterByAuthorized(
                            currentSession(), OperationType.DESCRIBE, resources);
            tableNames =
                    authorizedTable.stream()
                            .map(resource -> resource.getName().split(TABLE_SPLITTER)[1])
                            .collect(Collectors.toList());
        }

        response.addAllTableNames(tableNames);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        if (authorizer != null) {
            authorizer.authorize(
                    currentSession(),
                    OperationType.DESCRIBE,
                    Resource.table(tablePath.getDatabaseName(), tablePath.getTableName()));
        }

        GetTableInfoResponse response = new GetTableInfoResponse();
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        response.setTableJson(tableInfo.toTableDescriptor().toJsonBytes())
                .setSchemaId(tableInfo.getSchemaId())
                .setTableId(tableInfo.getTableId())
                .setCreatedTime(tableInfo.getCreatedTime())
                .setModifiedTime(tableInfo.getModifiedTime());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetTableSchemaResponse> getTableSchema(GetTableSchemaRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        final SchemaInfo schemaInfo;
        if (request.hasSchemaId()) {
            schemaInfo = metadataManager.getSchemaById(tablePath, request.getSchemaId());
        } else {
            schemaInfo = metadataManager.getLatestSchema(tablePath);
        }
        GetTableSchemaResponse response = new GetTableSchemaResponse();
        response.setSchemaId(schemaInfo.getSchemaId());
        response.setSchemaJson(schemaInfo.getSchema().toJsonBytes());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<TableExistsResponse> tableExists(TableExistsRequest request) {
        TableExistsResponse response = new TableExistsResponse();
        boolean exists = metadataManager.tableExists(toTablePath(request.getTablePath()));
        response.setExists(exists);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        String listenerName = currentListenerName();

        List<PbTablePath> pbTablePaths = request.getTablePathsList();
        List<TableMetadata> tableMetadata = new ArrayList<>();
        for (PbTablePath pbTablePath : pbTablePaths) {
            if (authorizer == null
                    || authorizer.isAuthorized(
                            currentSession(),
                            OperationType.DESCRIBE,
                            Resource.table(
                                    pbTablePath.getDatabaseName(), pbTablePath.getTableName()))) {
                tableMetadata.add(
                        metadataCache
                                .getTableMetadata(toTablePath(pbTablePath))
                                .orElseThrow(
                                        () ->
                                                new UnknownTableOrBucketException(
                                                        "Table not exist in server cache for table path: "
                                                                + pbTablePath)));
            }
        }

        List<PartitionMetadata> partitionMetadata = new ArrayList<>();
        List<PhysicalTablePath> partitions = new ArrayList<>();
        request.getPartitionsPathsList()
                .forEach(
                        pbPhysicalTablePath ->
                                partitions.add(toPhysicalTablePath(pbPhysicalTablePath)));
        long[] partitionIds = request.getPartitionsIds();
        for (long partitionId : partitionIds) {
            partitions.add(
                    metadataCache
                            .getPhysicalTablePath(partitionId)
                            .orElseThrow(
                                    () ->
                                            new UnknownTableOrBucketException(
                                                    "Partition not exist in server cache for partition id: "
                                                            + partitionId)));
        }

        for (PhysicalTablePath partitionPath : partitions) {
            if (authorizer == null
                    || authorizer.isAuthorized(
                            currentSession(),
                            OperationType.DESCRIBE,
                            Resource.table(
                                    partitionPath.getDatabaseName(),
                                    partitionPath.getTableName()))) {
                partitionMetadata.add(
                        metadataCache
                                .getPartitionMetadata(partitionPath)
                                .orElseThrow(
                                        () ->
                                                new UnknownTableOrBucketException(
                                                        "Partition not exist in server cache for partition path: "
                                                                + partitionPath)));
            }
        }

        return CompletableFuture.completedFuture(
                buildMetadataResponse(
                        metadataCache.getCoordinatorServer(listenerName),
                        new HashSet<>(
                                metadataCache.getAllAliveTabletServers(listenerName).values()),
                        tableMetadata,
                        partitionMetadata));
    }

    @Override
    public CompletableFuture<GetLatestKvSnapshotsResponse> getLatestKvSnapshots(
            GetLatestKvSnapshotsRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        // get table info
        TableInfo tableInfo = metadataManager.getTable(tablePath);

        boolean hasPrimaryKey = tableInfo.hasPrimaryKey();
        boolean hasPartitionName = request.hasPartitionName();
        boolean isPartitioned = tableInfo.isPartitioned();
        if (!hasPrimaryKey) {
            throw new NonPrimaryKeyTableException(
                    "Table '"
                            + tablePath
                            + "' is not a primary key table, so it doesn't have any kv snapshots.");
        } else if (hasPartitionName && !isPartitioned) {
            throw new TableNotPartitionedException(
                    "Table '" + tablePath + "' is not a partitioned table.");
        } else if (!hasPartitionName && isPartitioned) {
            throw new PartitionNotExistException(
                    "Table '"
                            + tablePath
                            + "' is a partitioned table, but partition name is not provided.");
        }

        try {
            // get table id
            long tableId = tableInfo.getTableId();
            int numBuckets = tableInfo.getNumBuckets();
            Long partitionId =
                    hasPartitionName ? getPartitionId(tablePath, request.getPartitionName()) : null;
            Map<Integer, Optional<BucketSnapshot>> snapshots;
            if (partitionId != null) {
                snapshots = zkClient.getPartitionLatestBucketSnapshot(partitionId);
            } else {
                snapshots = zkClient.getTableLatestBucketSnapshot(tableId);
            }
            return CompletableFuture.completedFuture(
                    makeGetLatestKvSnapshotsResponse(tableId, partitionId, snapshots, numBuckets));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long getPartitionId(TablePath tablePath, String partitionName) {
        Optional<TablePartition> optTablePartition;
        try {
            optTablePartition = zkClient.getPartition(tablePath, partitionName);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to get latest kv snapshots for table '%s'", tablePath),
                    e);
        }
        if (!optTablePartition.isPresent()) {
            throw new PartitionNotExistException(
                    String.format(
                            "The partition '%s' of table '%s' does not exist.",
                            partitionName, tablePath));
        }
        return optTablePartition.get().getPartitionId();
    }

    @Override
    public CompletableFuture<GetKvSnapshotMetadataResponse> getKvSnapshotMetadata(
            GetKvSnapshotMetadataRequest request) {
        TableBucket tableBucket =
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId());
        long snapshotId = request.getSnapshotId();

        Optional<BucketSnapshot> snapshot;
        try {
            snapshot = zkClient.getTableBucketSnapshot(tableBucket, snapshotId);
            checkState(snapshot.isPresent(), "Kv snapshot not found");
            CompletedSnapshot completedSnapshot =
                    snapshot.get().toCompletedSnapshotHandle().retrieveCompleteSnapshot();
            return CompletableFuture.completedFuture(
                    makeKvSnapshotMetadataResponse(completedSnapshot));
        } catch (Exception e) {
            throw new KvSnapshotNotExistException(
                    String.format(
                            "Failed to get kv snapshot metadata for table bucket %s and snapshot id %s. Error: %s",
                            tableBucket, snapshotId, e.getMessage()));
        }
    }

    @Override
    public CompletableFuture<GetFileSystemSecurityTokenResponse> getFileSystemSecurityToken(
            GetFileSystemSecurityTokenRequest request) {
        // TODO: add ACL for per-table in https://github.com/alibaba/fluss/issues/752
        try {
            // In order to avoid repeatedly obtaining security token, cache it for a while.
            long currentTimeMs = System.currentTimeMillis();
            if (securityToken == null
                    || currentTimeMs - tokenLastUpdateTimeMs > TOKEN_EXPIRATION_TIME_MS) {
                securityToken = remoteFileSystem.obtainSecurityToken();
                tokenLastUpdateTimeMs = currentTimeMs;
            }

            return CompletableFuture.completedFuture(
                    toGetFileSystemSecurityTokenResponse(
                            remoteFileSystem.getUri().getScheme(), securityToken));
        } catch (Exception e) {
            throw new SecurityTokenException(
                    "Failed to get file access security token: " + e.getMessage());
        }
    }

    @Override
    public CompletableFuture<ListPartitionInfosResponse> listPartitionInfos(
            ListPartitionInfosRequest request) {
        TablePath tablePath = toTablePath(request.getTablePath());
        Map<String, Long> partitionNameAndIds = metadataManager.listPartitions(tablePath);
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        return CompletableFuture.completedFuture(
                toListPartitionInfosResponse(partitionKeys, partitionNameAndIds));
    }

    @Override
    public CompletableFuture<GetLatestLakeSnapshotResponse> getLatestLakeSnapshot(
            GetLatestLakeSnapshotRequest request) {
        // get table info
        TablePath tablePath = toTablePath(request.getTablePath());
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        // get table id
        long tableId = tableInfo.getTableId();

        Optional<LakeTableSnapshot> optLakeTableSnapshot;
        try {
            optLakeTableSnapshot = zkClient.getLakeTableSnapshot(tableId);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to get lake table snapshot for table: %s, table id: %d",
                            tablePath, tableId),
                    e);
        }

        if (!optLakeTableSnapshot.isPresent()) {
            throw new LakeTableSnapshotNotExistException(
                    String.format(
                            "Lake table snapshot not exist for table: %s, table id: %d",
                            tablePath, tableId));
        }

        LakeTableSnapshot lakeTableSnapshot = optLakeTableSnapshot.get();
        return CompletableFuture.completedFuture(
                makeGetLatestLakeSnapshotResponse(tableId, lakeTableSnapshot));
    }

    @Override
    public CompletableFuture<ListAclsResponse> listAcls(ListAclsRequest request) {
        if (authorizer == null) {
            throw new SecurityDisabledException("No Authorizer is configured.");
        }
        AclBindingFilter aclBindingFilter = toAclFilter(request.getAclFilter());
        try {
            Collection<AclBinding> acls = authorizer.listAcls(currentSession(), aclBindingFilter);
            return CompletableFuture.completedFuture(makeListAclsResponse(acls));
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to list acls for resource: ", aclBindingFilter), e);
        }
    }
}
