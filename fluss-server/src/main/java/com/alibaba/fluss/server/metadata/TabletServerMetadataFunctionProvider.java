package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.RpcServiceBase;
import com.alibaba.fluss.server.coordinator.MetadataManager;
import com.alibaba.fluss.server.zk.ZooKeeperClient;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The metadata function provider for tablet server. */
public class TabletServerMetadataFunctionProvider implements MetadataFunctionProvider {

    private final ZooKeeperClient zkClient;

    private final TabletServerMetadataCache metadataCache;

    private final MetadataManager metadataManager;

    public TabletServerMetadataFunctionProvider(
            ZooKeeperClient zkClient,
            TabletServerMetadataCache metadataCache,
            MetadataManager metadataManager) {
        this.zkClient = zkClient;
        this.metadataCache = metadataCache;
        this.metadataManager = metadataManager;
    }

    @Override
    public Optional<TableMetadata> getTableMetadataFromCache(TablePath tablePath) {
        return Optional.ofNullable(metadataCache.getTableMetadata(tablePath));
    }

    @Override
    public CompletableFuture<TableMetadata> getTableMetadataFromZk(TablePath tablePath) {
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        return RpcServiceBase.getTableMetadataFromZkAsync(
                        zkClient, tablePath, tableInfo.getTableId(), tableInfo.isPartitioned())
                .thenApply(
                        bucketMetadataList -> {
                            TableMetadata tableMetadata =
                                    new TableMetadata(tableInfo, bucketMetadataList);
                            // Update local cache after successfully fetching from ZooKeeper
                            metadataCache.updateTableMetadata(tableMetadata);
                            return tableMetadata;
                        });
    }

    @Override
    public Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId) {
        return metadataCache.getPhysicalTablePath(partitionId);
    }

    @Override
    public Optional<PartitionMetadata> getPartitionMetadataFromCache(
            PhysicalTablePath physicalTablePath) {
        return Optional.ofNullable(metadataCache.getPartitionMetadata(physicalTablePath));
    }

    @Override
    public CompletableFuture<PartitionMetadata> getPartitionMetadataFromZk(
            PhysicalTablePath physicalTablePath) {
        return RpcServiceBase.getPartitionMetadataFromZkAsync(physicalTablePath, zkClient)
                .thenApply(
                        partitionMetadata -> {
                            // Update local cache after successfully fetching from ZooKeeper
                            metadataCache.updatePartitionMetadata(partitionMetadata);
                            return partitionMetadata;
                        });
    }
}
