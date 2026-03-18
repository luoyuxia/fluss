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

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.flink.lake.split.DvAwareFlussLogSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link DvAwareFlussLogSplitScanner}. */
class DvAwareFlussLogSplitScannerTest {

    @Test
    void testApplyLogDvAndRetainOffsets() throws Exception {
        Table table = mock(Table.class);
        Scan scan = mock(Scan.class);
        TestingLogScanner logScanner = new TestingLogScanner();
        when(table.newScan()).thenReturn(scan);
        when(scan.project(any(int[].class))).thenReturn(scan);
        when(scan.createLogScanner()).thenReturn(logScanner);

        TableBucket tableBucket = new TableBucket(1L, null, 0);
        DvAwareFlussLogSplit split =
                new DvAwareFlussLogSplit(
                        tableBucket,
                        null,
                        1000L,
                        1005L,
                        Collections.singletonMap(1000L, serializeBitmapBytes()));

        logScanner.addBatch(
                tableBucket,
                scanRecord(1000L, ChangeType.INSERT, "log-a"),
                scanRecord(1001L, ChangeType.UPDATE_AFTER, "log-b"),
                scanRecord(1002L, ChangeType.DELETE, "log-delete"),
                scanRecord(1003L, ChangeType.UPDATE_BEFORE, "log-before"),
                scanRecord(1004L, ChangeType.INSERT, "log-c"));

        BatchScanner scanner = new DvAwareFlussLogSplitScanner(table, split, new int[] {0});
        try {
            List<String> actual = collectFirstColumn(scanner);
            assertThat(actual).containsExactly("log-a", "log-c");
            assertThat(logScanner.subscribedOffset).isEqualTo(1000L);
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

    private static byte[] serializeBitmapBytes() throws IOException {
        org.roaringbitmap.RoaringBitmap bitmap = new org.roaringbitmap.RoaringBitmap();
        bitmap.add(1);
        bitmap.runOptimize();
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream outputStream = new java.io.DataOutputStream(baos)) {
            bitmap.serialize(outputStream);
        }
        return baos.toByteArray();
    }

    private static ScanRecord scanRecord(long offset, ChangeType changeType, String value) {
        return new ScanRecord(
                offset, 0L, changeType, GenericRow.of(BinaryString.fromString(value)));
    }

    private static class TestingLogScanner implements LogScanner {
        private final Deque<ScanRecords> batches = new ArrayDeque<>();
        private long subscribedOffset = Long.MIN_VALUE;

        private void addBatch(TableBucket tableBucket, ScanRecord... records) {
            batches.add(
                    new ScanRecords(Collections.singletonMap(tableBucket, Arrays.asList(records))));
        }

        @Override
        public ScanRecords poll(Duration timeout) {
            return batches.isEmpty() ? ScanRecords.EMPTY : batches.removeFirst();
        }

        @Override
        public void subscribe(int bucket, long offset) {
            this.subscribedOffset = offset;
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            this.subscribedOffset = offset;
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {}

        @Override
        public void unsubscribe(int bucket) {}

        @Override
        public void wakeup() {}

        @Override
        public void close() {}
    }
}
