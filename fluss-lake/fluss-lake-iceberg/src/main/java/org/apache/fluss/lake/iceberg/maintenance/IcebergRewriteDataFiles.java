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

package org.apache.fluss.lake.iceberg.maintenance;

import org.apache.fluss.lake.iceberg.tiering.writer.TaskWriterFactory;
import org.apache.fluss.metadata.TableBucket;

import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenericReader;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.DataFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Concrete implementation for Fluss's Iceberg integration. Handles bin-packing compaction of small
 * files into larger ones.
 *
 * <p>For tables with bucket transform in the partition spec (PK tables), this uses partition
 * filtering combined with file-level bucket filtering. Iceberg expressions cannot filter directly
 * on bucket transform values, so we filter files by checking their partition data at the bucket
 * field index.
 *
 * <p>For log tables without bucket transform, this uses partition filtering combined with a file
 * distribution strategy. Files within a partition are distributed across bucket writers using
 * modulo assignment (file_index % total_buckets == bucket_id). This ensures each bucket compacts a
 * distinct subset of files with no overlap.
 */
public class IcebergRewriteDataFiles {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergRewriteDataFiles.class);

    // TODO: make compaction strategy configurable
    private static final int MIN_FILES_TO_COMPACT = 3;

    private final Table table;
    private final String partition;
    private final TableBucket bucket;
    private final int totalBuckets;
    private final Expression partitionFilter;
    private final boolean isPkTable;
    private long targetSizeInBytes = 128 * 1024 * 1024; // 128MB default

    public IcebergRewriteDataFiles(
            Table table, @Nullable String partition, TableBucket bucket, int totalBuckets) {
        this.table = table;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        // PK tables have identifier fields and use bucket transform in Iceberg
        this.isPkTable = !table.schema().identifierFieldIds().isEmpty();

        // Build partition filter for identity partition fields only
        // Bucket filtering is done at file level since Iceberg expressions cannot filter
        // directly on bucket transform values
        this.partitionFilter = toPartitionFilterExpression(table, partition);
    }

    /**
     * Build filter expression for identity partition fields only.
     *
     * <p>Note: Bucket filtering cannot be done via Iceberg expressions because expressions filter
     * on row values, not partition transform results. For bucket filtering, we use file-level
     * filtering by checking partition data.
     *
     * @param table the Iceberg table
     * @param partitionName the partition name (nullable for non-partitioned tables)
     * @return the partition filter expression
     */
    private static Expression toPartitionFilterExpression(
            Table table, @Nullable String partitionName) {
        Expression expression = Expressions.alwaysTrue();

        // Add identity partition filters
        if (partitionName != null) {
            List<PartitionField> partitionFields = table.spec().fields();
            int partitionIndex = 0;
            String[] partitionArr =
                    partitionName.split(
                            "\\"
                                    + org.apache.fluss.metadata.ResolvedPartitionSpec
                                            .PARTITION_SPEC_SEPARATOR);
            for (String partitionValue : partitionArr) {
                PartitionField field = partitionFields.get(partitionIndex++);
                String columnName = table.schema().findColumnName(field.sourceId());
                expression =
                        Expressions.and(expression, Expressions.equal(columnName, partitionValue));
            }
        }

        return expression;
    }

    public IcebergRewriteDataFiles targetSizeInBytes(long targetSize) {
        this.targetSizeInBytes = targetSize;
        return this;
    }

    private List<CombinedScanTask> planRewriteFileGroups(long snapshotId) throws IOException {
        List<FileScanTask> fileScanTasks;
        if (isPkTable) {
            // PK table: use partition filter for partition pruning,
            // then filter files by bucket at file level
            fileScanTasks = scanFilesWithBucketFilter(snapshotId);
        } else {
            // Log table: use partition filter first, then distribute
            // files within the partition across buckets using modulo assignment
            fileScanTasks = scanFilesWithDistribution(snapshotId);
        }

        // the files < targetSizeInBytes is less than MIN_FILES_TO_COMPACT, don't compact
        if (fileScanTasks.stream()
                        .filter(fileScanTask -> fileScanTask.length() < targetSizeInBytes)
                        .count()
                < MIN_FILES_TO_COMPACT) {
            // return empty file group
            return Collections.emptyList();
        }

        // do package now
        BinPacking.ListPacker<FileScanTask> packer =
                new BinPacking.ListPacker<>(targetSizeInBytes, 1, false);
        return packer.pack(fileScanTasks, ContentScanTask::length).stream()
                .filter(tasks -> tasks.size() > 1)
                .map(BaseCombinedScanTask::new)
                .collect(Collectors.toList());
    }

    /**
     * Scan files using partition filter and file-level bucket filtering. Used for tables with
     * bucket transform (PK tables).
     *
     * <p>Iceberg expressions cannot filter directly on bucket transform values, so we apply
     * partition filter first for partition pruning, then filter files by checking the bucket value
     * in each file's partition data.
     */
    private List<FileScanTask> scanFilesWithBucketFilter(long snapshotId) throws IOException {
        List<FileScanTask> fileScanTasks = new ArrayList<>();
        PartitionSpec spec = table.spec();
        // The bucket transform is the last partition field
        int bucketFieldIndex = spec.fields().size() - 1;
        int targetBucket = bucket.getBucket();

        try (CloseableIterable<FileScanTask> tasks =
                table.newScan()
                        .useSnapshot(snapshotId)
                        .includeColumnStats()
                        .filter(partitionFilter)
                        .ignoreResiduals()
                        .planFiles()) {
            for (FileScanTask task : tasks) {
                // Get the bucket value from the file's partition data
                StructLike partitionData = task.file().partition();
                int fileBucket = partitionData.get(bucketFieldIndex, Integer.class);
                if (fileBucket == targetBucket) {
                    fileScanTasks.add(task);
                }
            }
        }
        return fileScanTasks;
    }

    /**
     * Scan files with partition filter and distribute them across buckets using modulo assignment.
     * Used for log tables without bucket transform.
     *
     * <p>First applies partition filter (identity transform) to get files for this partition, then
     * distributes files across buckets. Files are sorted by path for deterministic ordering, then
     * assigned to buckets: file at index i is assigned to bucket (i % totalBuckets).
     *
     * <p>This ensures:
     *
     * <ul>
     *   <li>All files within the partition are covered across all bucket writers
     *   <li>No overlap between different bucket writers
     *   <li>Deterministic assignment for reproducibility
     * </ul>
     */
    private List<FileScanTask> scanFilesWithDistribution(long snapshotId) throws IOException {
        List<FileScanTask> allFileScanTasks = new ArrayList<>();
        // Apply partition filter to get files only for this partition
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan()
                        .useSnapshot(snapshotId)
                        .includeColumnStats()
                        .filter(partitionFilter)
                        .ignoreResiduals()
                        .planFiles()) {
            tasks.forEach(allFileScanTasks::add);
        }

        // Sort files by path for deterministic ordering
        allFileScanTasks.sort(Comparator.comparing(task -> task.file().path().toString()));

        // Select files for this bucket using modulo assignment
        int bucketId = bucket.getBucket();
        return IntStream.range(0, allFileScanTasks.size())
                .filter(i -> i % totalBuckets == bucketId)
                .mapToObj(allFileScanTasks::get)
                .collect(Collectors.toList());
    }

    @Nullable
    public RewriteDataFileResult execute() {
        try {
            // plan the file groups to be rewrite
            Snapshot snapshot = table.currentSnapshot();
            // if no snapshot, just return
            if (snapshot == null) {
                return null;
            }
            List<CombinedScanTask> tasksToRewrite = planRewriteFileGroups(snapshot.snapshotId());
            if (tasksToRewrite.isEmpty()) {
                return null;
            }
            LOG.info("Start to rewrite files {}.", tasksToRewrite);
            DataFileSet deletedDataFiles = DataFileSet.create();
            DataFileSet addedDataFiles = DataFileSet.create();
            for (CombinedScanTask combinedScanTask : tasksToRewrite) {
                addedDataFiles.addAll(rewriteFileGroup(combinedScanTask));
                deletedDataFiles.addAll(
                        combinedScanTask.files().stream()
                                .map(ContentScanTask::file)
                                .collect(Collectors.toList()));
            }
            LOG.info("Finish rewriting files from {} to {}.", deletedDataFiles, addedDataFiles);
            return new RewriteDataFileResult(
                    snapshot.snapshotId(), deletedDataFiles, addedDataFiles);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Fail to compact bucket %s of table %s.", bucket, table.name()),
                    e);
        }
    }

    private DataFileSet rewriteFileGroup(CombinedScanTask combinedScanTask) throws IOException {
        try (CloseableIterable<Record> records = readDataFile(combinedScanTask);
                TaskWriter<Record> taskWriter =
                        TaskWriterFactory.createTaskWriter(table, partition, bucket.getBucket())) {
            for (Record record : records) {
                taskWriter.write(record);
            }
            WriteResult rewriteResult = taskWriter.complete();
            checkState(
                    rewriteResult.deleteFiles().length == 0,
                    "the delete files should be empty, but got "
                            + Arrays.toString(rewriteResult.deleteFiles()));
            return DataFileSet.of(Arrays.asList(rewriteResult.dataFiles()));
        }
    }

    private CloseableIterable<Record> readDataFile(CombinedScanTask combinedScanTask) {
        IcebergGenericReader reader = new IcebergGenericReader(table.newScan(), true);
        return reader.open(combinedScanTask);
    }
}
