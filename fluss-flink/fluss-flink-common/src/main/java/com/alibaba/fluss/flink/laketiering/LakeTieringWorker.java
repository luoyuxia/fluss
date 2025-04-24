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

package com.alibaba.fluss.flink.laketiering;

import com.alibaba.fluss.flink.source.split.TablePathLogSplit;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.lakehouse.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.GenericRecord;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.GenericRow;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/** . */
public class LakeTieringWorker<WriteResult>
        implements SplitReader<ReadPos<WriteResult>, TablePathLogSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTieringWorker.class);

    private final LakeTieringFactory<WriteResult, ?> lakeTieringFactory;

    private final Queue<TablePathLogSplit> splits;

    private final Map<TableBucket, LakeWriter<WriteResult>> lakeWriters;

    @Nullable private TablePathLogSplit currentSplit;
    @Nullable private LakeWriter<WriteResult> currentWriter;

    public LakeTieringWorker(LakeTieringFactory<WriteResult, ?> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.splits = new ArrayDeque<>();
        this.lakeWriters = new HashMap<>();
    }

    @Override
    public RecordsWithSplitIds<ReadPos<WriteResult>> fetch() throws IOException {
        checkSplitOrStartNext();
        if (currentWriter != null) {
            List<LogRecord> logRecords = genLogRecords();
            for (LogRecord logRecord : logRecords) {
                currentWriter.write(logRecord);
            }
            WriteResult writeResult = currentWriter.complete();
            TableBucketWriteResult<WriteResult> tableBucketWriteResult =
                    new TableBucketWriteResult<>(
                            currentSplit.getTablePath(),
                            currentSplit.getTableBucket(),
                            writeResult);
            currentWriter = null;
            return new FlinkRecordsWithSplitIds<>(
                    currentSplit.splitId(), new ReadPos<>(0, 0, tableBucketWriteResult));
        } else {
            return new FlinkRecordsWithSplitIds<>();
        }
    }

    private void checkSplitOrStartNext() {
        if (currentSplit != null) {
            return;
        }

        TablePathLogSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            return;
        }

        currentSplit = nextSplit;
        currentWriter = getLakeWriter(nextSplit);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TablePathLogSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        for (TablePathLogSplit sourceSplitBase : splitsChanges.splits()) {
            LOG.info("add split {}", sourceSplitBase.splitId());
            splits.add(sourceSplitBase);
        }
    }

    private List<LogRecord> genLogRecords() {
        List<LogRecord> logRecords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            GenericRow genericRow = new GenericRow(2);
            genericRow.setField(0, i);
            genericRow.setField(1, i + 1);
            logRecords.add(
                    new GenericRecord(
                            i, System.currentTimeMillis(), ChangeType.INSERT, genericRow));
        }
        return logRecords;
    }

    private LakeWriter<WriteResult> getLakeWriter(TablePathLogSplit split) {
        TableBucket tableBucket = split.getTableBucket();
        String partition = split.getPartitionName();
        TablePath tablePath = split.getTablePath();
        return lakeWriters.computeIfAbsent(
                tableBucket,
                (tb) -> {
                    WriterInitContext writerInitContext =
                            new WriterInitContextImpl(tablePath, tb, partition);
                    try {
                        return lakeTieringFactory.createLakeWriter(writerInitContext);
                    } catch (IOException e) {
                        throw new FlinkRuntimeException(
                                String.format(
                                        "Fail to create lake writer for bucket %s of table %s.",
                                        tb, tablePath),
                                e);
                    }
                });
    }

    @Override
    public void wakeUp() {
        //
    }

    @Override
    public void close() throws Exception {
        //
    }

    private static class WriterInitContextImpl implements WriterInitContext {

        private final TablePath tablePath;
        private final TableBucket tableBucket;
        private final String partition;

        public WriterInitContextImpl(
                TablePath tablePath, TableBucket tableBucket, @Nullable String partition) {
            this.tablePath = tablePath;
            this.tableBucket = tableBucket;
            this.partition = partition;
        }

        @Override
        public TablePath tablePath() {
            return tablePath;
        }

        @Override
        public TableBucket tableBucket() {
            return tableBucket;
        }

        @Nullable
        @Override
        public String partition() {
            return partition;
        }
    }

    private static class FlinkRecordsWithSplitIds<WriteResult>
            implements RecordsWithSplitIds<ReadPos<WriteResult>> {

        /** The finished splits. */
        private final Set<String> finishedSplits;

        @Nullable private String splitId;
        @Nullable private final ReadPos<WriteResult> recordsForSplit;
        @Nullable private ReadPos<WriteResult> recordsForSplitCurrent;

        private FlinkRecordsWithSplitIds() {
            this(null);
        }

        private FlinkRecordsWithSplitIds(@Nullable String splitId) {
            this(splitId, null, Collections.emptySet());
        }

        private FlinkRecordsWithSplitIds(String splitId, ReadPos<WriteResult> recordsForSplit) {
            this(splitId, recordsForSplit, Collections.emptySet());
        }

        private FlinkRecordsWithSplitIds(
                @Nullable String splitId,
                @Nullable ReadPos<WriteResult> recordsForSplit,
                Set<String> finishedSplits) {
            this.splitId = splitId;
            this.finishedSplits = finishedSplits;
            this.recordsForSplit = recordsForSplit;
        }

        @Nullable
        @Override
        public String nextSplit() {
            // move the split one (from current value to null)
            final String nextSplit = this.splitId;
            this.splitId = null;

            // move the iterator, from null to value (if first move) or to null (if second move)
            this.recordsForSplitCurrent = nextSplit != null ? this.recordsForSplit : null;

            return nextSplit;
        }

        @Nullable
        @Override
        public ReadPos<WriteResult> nextRecordFromSplit() {
            if (recordsForSplitCurrent != null) {
                ReadPos<WriteResult> record = this.recordsForSplitCurrent;
                this.recordsForSplitCurrent = null;
                return record;
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
