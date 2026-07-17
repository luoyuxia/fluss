/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.write;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.HistoricalPartitionResolver;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.fluss.utils.ExceptionUtils.stripCompletionException;
import static org.apache.fluss.utils.PartitionUtils.isHistoricalLookupCandidatePartition;
import static org.apache.fluss.utils.PartitionUtils.toHistoricalPartitionSpec;
import static org.apache.fluss.utils.PartitionUtils.validateAutoPartitionTime;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Resolves write targets and creates missing dynamic or historical partitions when needed. */
@ThreadSafe
public class DynamicPartitionCreator {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionCreator.class);

    private final MetadataUpdater metadataUpdater;
    private final boolean dynamicPartitionEnabled;
    private final Admin admin;
    private final Consumer<Throwable> fatalErrorHandler;
    private final HistoricalPartitionResolver historicalPartitionResolver;

    private final Set<PhysicalTablePath> inflightPartitionsToCreate = ConcurrentHashMap.newKeySet();
    // Original partitions that have already been confirmed to use historical write routing. The
    // table ID prevents a dropped and recreated table from reusing an old routing decision.
    private final ConcurrentMap<PhysicalTablePath, Long> confirmedHistoricalPartitions =
            new ConcurrentHashMap<>();

    public DynamicPartitionCreator(
            MetadataUpdater metadataUpdater,
            Admin admin,
            boolean dynamicPartitionEnabled,
            Consumer<Throwable> fatalErrorHandler) {
        this(
                metadataUpdater,
                admin,
                dynamicPartitionEnabled,
                fatalErrorHandler,
                new HistoricalPartitionResolver(metadataUpdater, admin));
    }

    DynamicPartitionCreator(
            MetadataUpdater metadataUpdater,
            Admin admin,
            boolean dynamicPartitionEnabled,
            Consumer<Throwable> fatalErrorHandler,
            HistoricalPartitionResolver historicalPartitionResolver) {
        this.metadataUpdater = metadataUpdater;
        this.admin = admin;
        this.dynamicPartitionEnabled = dynamicPartitionEnabled;
        this.fatalErrorHandler = fatalErrorHandler;
        this.historicalPartitionResolver = historicalPartitionResolver;
    }

    ResolvedWriteTarget resolveWriteTarget(
            PhysicalTablePath physicalTablePath, TableInfo tableInfo) {
        String partitionName = physicalTablePath.getPartitionName();
        if (partitionName == null) {
            // no need to check and create partition
            return ResolvedWriteTarget.normal(physicalTablePath);
        }

        // First check the cached metadata, and force an update only if the original partition is
        // missing.
        Optional<Long> partitionIdOpt = metadataUpdater.getPartitionId(physicalTablePath);
        if (partitionIdOpt.isPresent()) {
            confirmedHistoricalPartitions.remove(physicalTablePath);
            return ResolvedWriteTarget.normal(physicalTablePath);
        }

        Long confirmedTableId = confirmedHistoricalPartitions.get(physicalTablePath);
        if (confirmedTableId != null) {
            if (confirmedTableId == tableInfo.getTableId()
                    && isHistoricalLookupCandidatePartition(
                            tableInfo, partitionName, Instant.now())) {
                // The original partition was already confirmed missing. Resolve the historical
                // partition again so an invalidated target is refreshed before the next write.
                return resolveHistoricalWriteTarget(physicalTablePath, tableInfo);
            }
            confirmedHistoricalPartitions.remove(physicalTablePath, confirmedTableId);
        }

        if (inflightPartitionsToCreate.contains(physicalTablePath)) {
            // If the partition is already in inflightPartitionsToCreate, skip creating it.
            LOG.debug("Partition {} is already being created, skipping.", physicalTablePath);
            return ResolvedWriteTarget.normal(physicalTablePath);
        }

        if (forceCheckPartitionExist(physicalTablePath)) {
            // If the partition exists after the forced metadata update, skip creating it.
            LOG.debug("Partition {} already exists, skipping.", physicalTablePath);
            return ResolvedWriteTarget.normal(physicalTablePath);
        }

        if (isHistoricalLookupCandidatePartition(tableInfo, partitionName, Instant.now())) {
            ResolvedWriteTarget resolvedWriteTarget =
                    resolveHistoricalWriteTarget(physicalTablePath, tableInfo);
            confirmedHistoricalPartitions.put(physicalTablePath, tableInfo.getTableId());
            return resolvedWriteTarget;
        }

        if (!dynamicPartitionEnabled) {
            throw new PartitionNotExistException(
                    String.format("Table partition '%s' does not exist.", physicalTablePath));
        }

        // Validate only the normal dynamic-create path. Eligible expired partitions have already
        // been redirected to the historical system partition above.
        List<String> partitionKeys = tableInfo.getPartitionKeys();
        AutoPartitionStrategy autoPartitionStrategy =
                tableInfo.getTableConfig().getAutoPartitionStrategy();
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
        validateAutoPartitionTime(
                resolvedPartitionSpec.toPartitionSpec(), partitionKeys, autoPartitionStrategy);

        // Create the normal partition if it does not exist. add() ensures that only one thread
        // owns the asynchronous create operation.
        if (inflightPartitionsToCreate.add(physicalTablePath)) {
            LOG.info("Dynamically creating partition for {}", physicalTablePath);
            createPartition(physicalTablePath, partitionKeys);
        } else {
            // Another thread started creating the same partition after the earlier contains()
            // check.
            LOG.debug("Partition {} is already being created, skipping.", physicalTablePath);
        }
        return ResolvedWriteTarget.normal(physicalTablePath);
    }

    /** Resolves a historical target after the original partition is confirmed to be missing. */
    private ResolvedWriteTarget resolveHistoricalWriteTarget(
            PhysicalTablePath physicalTablePath, TableInfo tableInfo) {
        String originalPartitionName = physicalTablePath.getPartitionName();
        checkArgument(originalPartitionName != null, "Partition name shouldn't be null.");

        ResolvedPartitionSpec historicalPartitionSpec =
                toHistoricalPartitionSpec(tableInfo, originalPartitionName);
        PhysicalTablePath historicalPartitionPath =
                PhysicalTablePath.of(
                        tableInfo.getTablePath(), historicalPartitionSpec.getPartitionName());
        long historicalPartitionId = waitForHistoricalPartition(tableInfo, originalPartitionName);
        return ResolvedWriteTarget.historical(
                historicalPartitionPath, originalPartitionName, historicalPartitionId);
    }

    private boolean forceCheckPartitionExist(PhysicalTablePath physicalTablePath) {
        // force an IO to check whether the partition exists
        try {
            return metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        } catch (PartitionNotExistException e) {
            return false;
        }
    }

    private long waitForHistoricalPartition(TableInfo tableInfo, String originalPartitionName) {
        try {
            return historicalPartitionResolver
                    .resolveHistoricalPartitionId(tableInfo, originalPartitionName)
                    .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException(
                    "Interrupted while resolving historical partition for " + originalPartitionName,
                    e);
        } catch (ExecutionException e) {
            Throwable cause = stripCompletionException(e.getCause());
            if (cause instanceof RuntimeException || cause instanceof Error) {
                ExceptionUtils.rethrow(cause);
            }
            throw new FlussRuntimeException(
                    "Failed to resolve historical partition for " + originalPartitionName, cause);
        }
    }

    private void createPartition(PhysicalTablePath physicalTablePath, List<String> partitionKeys) {
        String partitionName = physicalTablePath.getPartitionName();
        TablePath tablePath = physicalTablePath.getTablePath();
        checkArgument(partitionName != null, "Partition name shouldn't be null.");
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);

        admin.createPartition(tablePath, resolvedPartitionSpec.toPartitionSpec(), true)
                .whenComplete(
                        (ignore, throwable) -> {
                            if (throwable != null) {
                                // If encounter TooManyPartitionsException or
                                // TooManyBucketsException, we should set
                                // cachedCreatePartitionException to make the next createPartition
                                // call failed.
                                onPartitionCreationFailed(physicalTablePath, throwable);
                            } else {
                                onPartitionCreationSuccess(physicalTablePath);
                            }
                        });
    }

    private void onPartitionCreationSuccess(PhysicalTablePath physicalTablePath) {
        inflightPartitionsToCreate.remove(physicalTablePath);
        // TODO: trigger to update metadata here when metadataUpdater supports async update
        // metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        LOG.info("Successfully created partition {}", physicalTablePath);
    }

    private void onPartitionCreationFailed(
            PhysicalTablePath physicalTablePath, Throwable throwable) {
        inflightPartitionsToCreate.remove(physicalTablePath);
        fatalErrorHandler.accept(
                new FlussRuntimeException(
                        "Failed to dynamically create partition " + physicalTablePath,
                        stripCompletionException(throwable)));
    }
}
