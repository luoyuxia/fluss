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
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/**
 * Paimon Lake format implementation of {@link org.apache.fluss.lake.source.LakeSource} for reading
 * paimon table.
 */
public class PaimonLakeSource implements LakeSource<PaimonSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeSource.class);
    private static final long serialVersionUID = 1L;

    private final Configuration paimonConfig;
    private final TablePath tablePath;

    private @Nullable int[][] project;
    private @Nullable org.apache.paimon.predicate.Predicate predicate;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    public Optional<Tuple2<Long, Map<TableBucket, Long>>> preferSnapshot(
            long tableId, long snapshotId) throws Exception {
        try (Catalog catalog = getCatalog()) {
            FileStoreTable fileStoreTable = getTable(catalog, tablePath);
            if (Options.fromMap(fileStoreTable.options())
                    .get(CoreOptions.DELETION_VECTORS_ENABLED)) {
                long currentId = snapshotId + 1;
                long lastCompactSnapshotId = -1;
                SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
                while (true) {
                    if (!snapshotManager.snapshotExists(currentId)) {
                        // 快照不存在，停止
                        break;
                    }

                    Snapshot snapshot = snapshotManager.snapshot(currentId);
                    if (snapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
                        // 是 COMPACT 快照，记录并继续
                        lastCompactSnapshotId = currentId;
                        currentId++;
                    } else {
                        // 遇到非 COMPACT 快照，停止（要求连续）
                        break;
                    }
                }

                if (lastCompactSnapshotId != -1) {
                    LOG.info(
                            "Found the last consecutive compaction snapshot: {}, use this as prefer snapshot.",
                            lastCompactSnapshotId);
                    return Optional.of(Tuple2.of(lastCompactSnapshotId, null));
                } else {
                    LOG.info(
                            "Can't find the next compacted snapshot for tiered snapshot {}, try to find by previous",
                            snapshotId);
                    Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
                    if (earliestSnapshotId == null) {
                        return Optional.empty();
                    } else {
                        LOG.warn(
                                "No any next compacted snapshot for tiered snapshot {}, try to fall back to not read lake.",
                                snapshotId);
                        return Optional.empty();
                        //                        for (long previousSnapshotId = snapshotId - 1;
                        //                                previousSnapshotId >= earliestSnapshotId;
                        //                                previousSnapshotId--) {
                        //                            Snapshot previousSnapshot =
                        //
                        // snapshotManager.snapshot(previousSnapshotId);
                        //                            Snapshot nextSnapshot =
                        //
                        // snapshotManager.snapshot(previousSnapshotId + 1);
                        //                            if (previousSnapshot.commitKind() ==
                        // Snapshot.CommitKind.APPEND
                        //                                    && nextSnapshot.commitKind() ==
                        // Snapshot.CommitKind.COMPACT) {
                        //                                Map<TableBucket, Long> logEndOffsets = new
                        // HashMap<>();
                        //                                Map<String, String> lakeSnapshotProperties
                        // =
                        //                                        previousSnapshot.properties();
                        //                                String flussOffsetProperties =
                        //                                        lakeSnapshotProperties.get(
                        //
                        // FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);
                        //
                        //                                for (JsonNode node :
                        //
                        // OBJECT_MAPPER.readTree(flussOffsetProperties)) {
                        //                                    BucketOffset bucketOffset =
                        //
                        // BucketOffsetJsonSerde.INSTANCE.deserialize(node);
                        //                                    if (bucketOffset.getPartitionId() !=
                        // null) {
                        //                                        logEndOffsets.put(
                        //                                                new TableBucket(
                        //                                                        tableId,
                        //
                        // bucketOffset.getPartitionId(),
                        //
                        // bucketOffset.getBucket()),
                        //
                        // bucketOffset.getLogOffset());
                        //                                    } else {
                        //                                        logEndOffsets.put(
                        //                                                new TableBucket(tableId,
                        // bucketOffset.getBucket()),
                        //
                        // bucketOffset.getLogOffset());
                        //                                    }
                        //                                }
                        //                                LOG.info(
                        //                                        "Find the nearest compacted
                        // snapshot {} for tiered snapshot {}, use the compacted snapshot, the
                        // offsets are {}.",
                        //                                        nextSnapshot.id(),
                        //                                        snapshotId,
                        //                                        logEndOffsets);
                        //                                return
                        // Optional.of(Tuple2.of(nextSnapshot.id(), logEndOffsets));
                        //                            }
                        //                        }
                        //                        // can't find any valid snapshot, return empty
                        //                        return Optional.empty();
                    }
                }
            } else {
                return Optional.of(Tuple2.of(snapshotId, null));
            }
        }
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
