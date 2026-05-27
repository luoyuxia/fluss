/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.paimon.utils.FlussToPaimonPredicateConverter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/**
 * Paimon Lake format implementation of {@link org.apache.fluss.lake.source.LakeSource} for reading
 * paimon table.
 */
public class PaimonLakeSource implements LakeSource<PaimonSplit> {
    private static final long serialVersionUID = 1L;

    private final Configuration paimonConfig;
    private final TablePath tablePath;

    private @Nullable int[][] project;
    private @Nullable org.apache.paimon.predicate.Predicate predicate;

    public PaimonLakeSource(Configuration paimonConfig, TablePath tablePath) {
        this.paimonConfig = paimonConfig;
        this.tablePath = tablePath;
    }

    @Override
    public void withProject(int[][] project) {
        this.project = project;
    }

    @Override
    public void withLimit(int limit) {
        throw new UnsupportedOperationException("Not impl.");
    }

    @Override
    public FilterPushDownResult withFilters(List<Predicate> predicates) {
        List<Predicate> unConsumedPredicates = new ArrayList<>();
        List<Predicate> consumedPredicates = new ArrayList<>();
        List<org.apache.paimon.predicate.Predicate> converted = new ArrayList<>();
        RowType rowType = getRowType(tablePath);
        for (Predicate predicate : predicates) {
            Optional<org.apache.paimon.predicate.Predicate> optPredicate =
                    FlussToPaimonPredicateConverter.convert(rowType, predicate);
            if (optPredicate.isPresent()) {
                consumedPredicates.add(predicate);
                converted.add(optPredicate.get());
            } else {
                unConsumedPredicates.add(predicate);
            }
        }
        if (!converted.isEmpty()) {
            predicate = PredicateBuilder.and(converted);
        }
        return FilterPushDownResult.of(consumedPredicates, unConsumedPredicates);
    }

    @Override
    public Planner<PaimonSplit> createPlanner(PlannerContext plannerContext) {
        return new PaimonSplitPlanner(
                paimonConfig, tablePath, predicate, plannerContext.snapshotId());
    }

    @Override
    public RecordReader createRecordReader(ReaderContext<PaimonSplit> context) throws IOException {
        try (Catalog catalog = getCatalog()) {
            FileStoreTable fileStoreTable = getTable(catalog, tablePath);
            if (fileStoreTable.primaryKeys().isEmpty()) {
                return new PaimonRecordReader(
                        fileStoreTable, context.lakeSplit(), project, predicate);
            } else {
                return new PaimonSortedRecordReader(
                        fileStoreTable, context.lakeSplit(), project, predicate);
            }
        } catch (Exception e) {
            throw new IOException("Fail to create record reader.", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<PaimonSplit> getSplitSerializer() {
        return new PaimonSplitSerializer();
    }

    @Nullable
    @Override
    public DataDeltaPlan<PaimonSplit> planDelta(long fromSnapshotId) {
        try (Catalog catalog = getCatalog()) {
            FileStoreTable fileStoreTable = getTable(catalog, tablePath);

            if (!fileStoreTable.coreOptions().deletionVectorsEnabled()) {
                return null;
            }

            SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
            Long latestSnapshotId = snapshotManager.latestSnapshotId();
            if (latestSnapshotId == null || latestSnapshotId <= fromSnapshotId) {
                return null;
            }

            // Find the latest COMPACT snapshot in (fromSnapshotId, latestSnapshotId]
            Long latestCompactSnapshotId = null;
            for (long sid = latestSnapshotId; sid > fromSnapshotId; sid--) {
                Snapshot snapshot = snapshotManager.tryGetSnapshot(sid);
                if (snapshot != null && snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
                    latestCompactSnapshotId = sid;
                    break;
                }
            }
            if (latestCompactSnapshotId == null) {
                return null;
            }

            boolean isBucketUnAware = fileStoreTable.bucketMode() == BucketMode.BUCKET_UNAWARE;
            List<String> partitionKeys = fileStoreTable.partitionKeys();
            List<PaimonSplit> newSplits = new ArrayList<>();
            Map<String, Map<Integer, List<String>>> deletedFiles = new HashMap<>();

            // Check if fromSnapshotId is a valid existing snapshot.
            // If not (e.g., 0 or -1 for first-time call, or expired), do a full scan
            // at latestCompactSnapshotId. Otherwise, use incremental DIFF scan.
            Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
            boolean hasValidFromSnapshot =
                    earliestSnapshotId != null && fromSnapshotId >= earliestSnapshotId;

            if (hasValidFromSnapshot) {
                // Use Paimon's incremental DIFF scan to compute file diff between two
                // snapshots. Returns DataSplit with:
                //   dataFiles = files in compactSnapshot but not in fromSnapshot (new)
                //   beforeFiles = files in fromSnapshot but not in compactSnapshot (deleted)
                // DV info is automatically associated by Paimon.
                Map<String, String> diffOptions = new HashMap<>();
                diffOptions.put(
                        CoreOptions.INCREMENTAL_BETWEEN.key(),
                        fromSnapshotId + "," + latestCompactSnapshotId);
                diffOptions.put(
                        CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(),
                        CoreOptions.IncrementalBetweenScanMode.DIFF.toString());
                FileStoreTable diffTable = fileStoreTable.copy(diffOptions);
                List<Split> diffSplits = diffTable.newScan().plan().splits();

                for (Split split : diffSplits) {
                    DataSplit diffSplit = (DataSplit) split;
                    String partitionName = toPartitionName(diffSplit.partition(), partitionKeys);
                    int bucket = diffSplit.bucket();

                    // dataFiles = new files (in compact but not in from)
                    List<DataFileMeta> newFiles = diffSplit.dataFiles();
                    List<DeletionFile> dvFiles = diffSplit.deletionFiles().orElse(null);
                    buildSingleFileSplits(
                            newFiles,
                            dvFiles,
                            latestCompactSnapshotId,
                            diffSplit.partition(),
                            bucket,
                            diffSplit.bucketPath(),
                            isBucketUnAware,
                            diffSplit.rawConvertible(),
                            newSplits);

                    // beforeFiles = deleted files (in from but not in compact)
                    List<DataFileMeta> beforeFiles = diffSplit.beforeFiles();
                    if (!beforeFiles.isEmpty()) {
                        List<String> deleted = new ArrayList<>(beforeFiles.size());
                        for (DataFileMeta file : beforeFiles) {
                            deleted.add(file.fileName());
                        }
                        deletedFiles
                                .computeIfAbsent(partitionName, k -> new HashMap<>())
                                .put(bucket, deleted);
                    }
                }
            } else {
                // No valid base snapshot - do a full scan at latestCompactSnapshotId.
                // All files are "new", no files are "deleted".
                Map<String, String> scanOptions = new HashMap<>();
                scanOptions.put(
                        CoreOptions.SCAN_SNAPSHOT_ID.key(),
                        String.valueOf(latestCompactSnapshotId));
                FileStoreTable scanTable = fileStoreTable.copy(scanOptions);
                List<Split> splits = scanTable.newScan().plan().splits();

                for (Split split : splits) {
                    DataSplit dataSplit = (DataSplit) split;
                    List<DataFileMeta> dataFiles = dataSplit.dataFiles();
                    List<DeletionFile> dvFiles = dataSplit.deletionFiles().orElse(null);
                    buildSingleFileSplits(
                            dataFiles,
                            dvFiles,
                            latestCompactSnapshotId,
                            dataSplit.partition(),
                            dataSplit.bucket(),
                            dataSplit.bucketPath(),
                            isBucketUnAware,
                            dataSplit.rawConvertible(),
                            newSplits);
                }
            }

            if (newSplits.isEmpty() && deletedFiles.isEmpty()) {
                return null;
            }

            return new DataDeltaPlan<>(newSplits, deletedFiles);
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan delta for " + tablePath, e);
        }
    }

    /**
     * Builds single-file PaimonSplits from a list of data files and their associated DV files. Each
     * data file becomes its own PaimonSplit.
     */
    private static void buildSingleFileSplits(
            List<DataFileMeta> dataFiles,
            @Nullable List<DeletionFile> dvFiles,
            long snapshotId,
            BinaryRow partition,
            int bucket,
            String bucketPath,
            boolean isBucketUnAware,
            boolean rawConvertible,
            List<PaimonSplit> outSplits) {
        for (int i = 0; i < dataFiles.size(); i++) {
            DataFileMeta fileMeta = dataFiles.get(i);
            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withSnapshot(snapshotId)
                            .withPartition(partition)
                            .withBucket(bucket)
                            .rawConvertible(rawConvertible)
                            .withBucketPath(bucketPath)
                            .withDataFiles(Collections.singletonList(fileMeta));
            if (dvFiles != null && i < dvFiles.size()) {
                DeletionFile dFile = dvFiles.get(i);
                if (dFile != null) {
                    builder.withDataDeletionFiles(Collections.singletonList(dFile));
                }
            }
            outSplits.add(new PaimonSplit(builder.build(), isBucketUnAware));
        }
    }

    /**
     * Converts a Paimon partition BinaryRow to a partition name string. Returns null for
     * non-partitioned tables.
     */
    @Nullable
    private static String toPartitionName(BinaryRow partition, List<String> partitionKeys) {
        if (partition.getFieldCount() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partition.getFieldCount(); i++) {
            if (i > 0) {
                sb.append("/");
            }
            sb.append(partitionKeys.get(i)).append("=").append(partition.getString(i));
        }
        return sb.toString();
    }

    private Catalog getCatalog() {
        return CatalogFactory.createCatalog(
                CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
    }

    private FileStoreTable getTable(Catalog catalog, TablePath tablePath) throws Exception {
        return (FileStoreTable) catalog.getTable(toPaimon(tablePath));
    }

    private RowType getRowType(TablePath tablePath) {
        try (Catalog catalog = getCatalog()) {
            FileStoreTable fileStoreTable = getTable(catalog, tablePath);
            return fileStoreTable.rowType();
        } catch (Exception e) {
            throw new RuntimeException("Fail to get row type of " + tablePath, e);
        }
    }
}
