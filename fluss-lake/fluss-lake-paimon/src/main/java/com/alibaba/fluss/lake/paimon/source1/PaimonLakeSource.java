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

package com.alibaba.fluss.lake.paimon.source1;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lake.source1.LakeRecords;
import com.alibaba.fluss.lake.source1.LakeSource;
import com.alibaba.fluss.lake.source1.LakeSplitPlanContext;
import com.alibaba.fluss.lake.source1.LakeSplitReadContext;
import com.alibaba.fluss.lake.source1.SortedView;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.utils.CloseableIterator;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toChangeType;
import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimon;
import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimonPartitionBinaryRow;
import static com.alibaba.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static com.alibaba.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** */
public class PaimonLakeSource implements LakeSource<PaimonSplit> {

    private final Configuration paimonConfig;
    private final TablePath tablePath;

    public PaimonLakeSource(Configuration paimonConfig, TablePath tablePath) {
        this.paimonConfig = paimonConfig;
        this.tablePath = tablePath;
    }

    @Override
    public List<PaimonSplit> plan(LakeSplitPlanContext context) throws IOException {
        List<PaimonSplit> splits = new ArrayList<>();
        try {
            try (Catalog catalog = getCatalog()) {
                FileStoreTable fileStoreTable = getTable(catalog, tablePath, context.snapshotId());
                // if primary key table, only generate only splits
                // to do batch sort merge
                if (!fileStoreTable.primaryKeys().isEmpty()) {
                    // todo: may need make it passed in context
                    fileStoreTable.copy(
                            Collections.singletonMap(
                                    CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(),
                                    // we set a max size to make sure only one splits
                                    MemorySize.MAX_VALUE.toString()));
                }

                InnerTableScan tableScan = fileStoreTable.newScan();
                if (context.bucket() != null) {
                    tableScan =
                            tableScan.withBucketFilter((b) -> Objects.equals(b, context.bucket()));
                }

                if (context.partitionSpecs() != null) {
                    tableScan =
                            tableScan.withPartitionFilter(
                                    toPartitionRows(checkNotNull(context.partitionSpecs())));
                }

                for (Split split : tableScan.plan().splits()) {
                    DataSplit dataSplit = (DataSplit) split;
                    splits.add(new PaimonSplit(dataSplit));
                }
            }
        } catch (Exception e) {
            throw new IOException("Fail to plan paimon splits.");
        }

        return splits;
    }

    @Override
    public LakeRecords read(LakeSplitReadContext<PaimonSplit> context) throws IOException {
        try {
            try (Catalog catalog = getCatalog()) {
                FileStoreTable fileStoreTable = getTable(catalog, tablePath);

                Comparator<com.alibaba.fluss.row.InternalRow> rowComparator = null;

                if (!fileStoreTable.primaryKeys().isEmpty()) {
                    KeyValueFileStore keyValueFileStore =
                            (KeyValueFileStore) fileStoreTable.store();
                    rowComparator =
                            toFlussRowComparator(
                                    fileStoreTable.rowType(), keyValueFileStore.newKeyComparator());
                }

                ReadBuilder readBuilder = fileStoreTable.newReadBuilder();
                if (context.getProjectColumns() != null) {
                    readBuilder =
                            project(
                                    readBuilder,
                                    fileStoreTable.rowType(),
                                    context.getProjectColumns());
                }

                TableRead tableRead = readBuilder.newRead();

                RecordReader<InternalRow> recordReader =
                        tableRead.createReader(context.getLakeSplit().dataSplit());

                CloseableIterator<LogRecord> records =
                        new PaimonRowAsFlussRecordIterator(
                                recordReader.toCloseableIterator(), readBuilder.readType());

                if (rowComparator == null) {
                    return () -> records;
                } else {
                    return new LakeRecordsWithOrder(records, rowComparator);
                }
            }
        } catch (Exception e) {
            throw new IOException("Fail to read paimon splits.", e);
        }
    }

    private static class LakeRecordsWithOrder implements LakeRecords, SortedView {

        private final CloseableIterator<LogRecord> records;
        private final Comparator<com.alibaba.fluss.row.InternalRow> rowComparator;

        public LakeRecordsWithOrder(
                CloseableIterator<LogRecord> records,
                Comparator<com.alibaba.fluss.row.InternalRow> rowComparator) {
            this.records = records;
            this.rowComparator = rowComparator;
        }

        @Override
        public Comparator<com.alibaba.fluss.row.InternalRow> order() {
            return rowComparator;
        }

        @Override
        public CloseableIterator<LogRecord> getLakeRecords() {
            return records;
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

    private FileStoreTable getTable(Catalog catalog, TablePath tablePath, long snapshotId)
            throws Exception {
        return (FileStoreTable)
                catalog.getTable(toPaimon(tablePath))
                        .copy(
                                Collections.singletonMap(
                                        CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                        String.valueOf(snapshotId)));
    }

    private FileStoreTable getTable(Catalog catalog, TablePath tablePath) throws Exception {
        return (FileStoreTable) catalog.getTable(toPaimon(tablePath));
    }

    private List<BinaryRow> toPartitionRows(List<ResolvedPartitionSpec> partitionSpecs) {
        List<BinaryRow> rows = new ArrayList<>(partitionSpecs.size());
        for (ResolvedPartitionSpec partitionSpec : partitionSpecs) {
            rows.add(
                    toPaimonPartitionBinaryRow(
                            partitionSpec.getPartitionKeys(), partitionSpec.getPartitionName()));
        }
        return rows;
    }

    private ReadBuilder project(ReadBuilder readBuilder, RowType rowType, String[] projectCols) {
        int[] project = new int[projectCols.length];
        for (int i = 0; i < project.length; i++) {
            project[i] = rowType.getFieldIndex(projectCols[i]);
        }
        return readBuilder.withProjection(project);
    }

    private Comparator<com.alibaba.fluss.row.InternalRow> toFlussRowComparator(
            RowType rowType, Comparator<InternalRow> paimonRowcomparator) {
        return (row1, row2) ->
                paimonRowcomparator.compare(
                        new FlussRowAsPaimonRow(row1, rowType),
                        new FlussRowAsPaimonRow(row2, rowType));
    }

    private static class PaimonRowAsFlussRecordIterator implements CloseableIterator<LogRecord> {

        private final org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator;

        private final ProjectedRow flussRow;

        private final int logOffsetColIndex;
        private final int timestampColIndex;

        public PaimonRowAsFlussRecordIterator(
                org.apache.paimon.utils.CloseableIterator<InternalRow> paimonRowIterator,
                RowType paimonRowType) {
            this.paimonRowIterator = paimonRowIterator;
            this.logOffsetColIndex = paimonRowType.getFieldIndex(OFFSET_COLUMN_NAME);
            this.timestampColIndex = paimonRowType.getFieldIndex(TIMESTAMP_COLUMN_NAME);

            int[] project = IntStream.range(0, paimonRowType.getFieldCount() - 3).toArray();
            flussRow = ProjectedRow.from(project);
        }

        @Override
        public void close() {
            try {
                paimonRowIterator.close();
            } catch (Exception e) {
                throw new RuntimeException("Fail to close iterator.", e);
            }
        }

        @Override
        public boolean hasNext() {
            return paimonRowIterator.hasNext();
        }

        @Override
        public LogRecord next() {
            InternalRow paimonRow = paimonRowIterator.next();
            ChangeType changeType = toChangeType(paimonRow.getRowKind());
            long offset = paimonRow.getLong(logOffsetColIndex);
            long timestamp = paimonRow.getTimestamp(timestampColIndex, 6).getMillisecond();

            LogRecord logRecord =
                    new GenericRecord(
                            offset,
                            timestamp,
                            changeType,
                            flussRow.replaceRow(new PaimonRowAsFlussRow(paimonRow)));
            System.out.println("logRecord: " + logRecord);
            return logRecord;
        }
    }
}
