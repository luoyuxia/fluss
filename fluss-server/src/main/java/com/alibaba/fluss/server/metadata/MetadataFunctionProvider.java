package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TablePath;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** MetadataFunctionProvider. */
public interface MetadataFunctionProvider {

    Optional<TableMetadata> getTableMetadataFromCache(TablePath tablePath);

    CompletableFuture<TableMetadata> getTableMetadataFromZk(TablePath tablePath);

    Optional<PhysicalTablePath> getPhysicalTablePathFromCache(long partitionId);

    Optional<PartitionMetadata> getPartitionMetadataFromCache(PhysicalTablePath physicalTablePath);

    CompletableFuture<PartitionMetadata> getPartitionMetadataFromZk(
            PhysicalTablePath physicalTablePath);
}
