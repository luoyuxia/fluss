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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.flink.lake.split.DvAwareLakeSnapshotSplit;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.PositionedRecord;
import org.apache.fluss.lake.source.PositionedRecordReader;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DvAwareLakeSnapshotSplitScanner}. */
class DvAwareLakeSnapshotSplitScannerTest {

    @Test
    void testApplyLakeDv() throws Exception {
        TestLakeSplit lakeSplit = new TestLakeSplit(0, "file-1.parquet");
        TestingLakeSource lakeSource = new TestingLakeSource();
        lakeSource.put(
                lakeSplit,
                new TestingPositionedRecordReader(
                        Arrays.asList(
                                positionedRecord(0L, "lake-a"),
                                positionedRecord(1L, "lake-b"),
                                positionedRecord(2L, "lake-c"))));

        DvAwareLakeSnapshotSplit split =
                new DvAwareLakeSnapshotSplit(
                        new TableBucket(1L, null, 0), null, lakeSplit, 0, serializeBitmap(1));

        BatchScanner scanner = new DvAwareLakeSnapshotSplitScanner(lakeSource, split);
        try {
            List<String> actual = collectFirstColumn(scanner);
            assertThat(actual).containsExactly("lake-a", "lake-c");
        } finally {
            scanner.close();
        }
    }

    private static List<String> collectFirstColumn(BatchScanner scanner) throws IOException {
        List<String> values = new ArrayList<>();
        CloseableIterator<InternalRow> batch;
        while ((batch = scanner.pollBatch(Duration.ofMillis(1))) != null) {
            while (batch.hasNext()) {
                values.add(batch.next().getString(0).toString());
            }
            batch.close();
        }
        return values;
    }

    private static PositionedRecord positionedRecord(long rowPosition, String value) {
        return new PositionedRecord(
                new GenericRecord(
                        0L, 0L, ChangeType.INSERT, GenericRow.of(BinaryString.fromString(value))),
                rowPosition);
    }

    private static byte[] serializeBitmap(int... values) throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int value : values) {
            bitmap.add(value);
        }
        bitmap.runOptimize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream outputStream = new DataOutputStream(baos)) {
            bitmap.serialize(outputStream);
        }
        return baos.toByteArray();
    }

    private static class TestLakeSplit implements LakeSplit {
        private final int bucket;
        private final String dataFilePath;

        private TestLakeSplit(int bucket, String dataFilePath) {
            this.bucket = bucket;
            this.dataFilePath = dataFilePath;
        }

        @Override
        public int bucket() {
            return bucket;
        }

        @Override
        public List<String> partition() {
            return Collections.emptyList();
        }

        @Override
        public String dataFilePath() {
            return dataFilePath;
        }
    }

    private static class TestingPositionedRecordReader implements PositionedRecordReader {
        private final List<PositionedRecord> records;

        private TestingPositionedRecordReader(List<PositionedRecord> records) {
            this.records = records;
        }

        @Override
        public CloseableIterator<LogRecord> read() {
            List<LogRecord> logRecords = new ArrayList<>(records.size());
            for (PositionedRecord record : records) {
                logRecords.add(record.record());
            }
            return CloseableIterator.wrap(logRecords.iterator());
        }

        @Override
        public CloseableIterator<PositionedRecord> readWithPosition() {
            return CloseableIterator.wrap(records.iterator());
        }
    }

    private static class TestingLakeSource implements LakeSource<LakeSplit> {
        private final Map<LakeSplit, PositionedRecordReader> readers = new HashMap<>();

        private void put(LakeSplit split, PositionedRecordReader reader) {
            readers.put(split, reader);
        }

        @Override
        public void withProject(int[][] project) {}

        @Override
        public void withLimit(int limit) {}

        @Override
        public FilterPushDownResult withFilters(
                List<org.apache.fluss.predicate.Predicate> predicates) {
            return FilterPushDownResult.of(Collections.emptyList(), predicates);
        }

        @Override
        public Planner<LakeSplit> createPlanner(PlannerContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PositionedRecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            return readers.get(context.lakeSplit());
        }

        @Override
        public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
            throw new UnsupportedOperationException();
        }
    }
}
