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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;

import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileLogInputStream}. */
public class FileLogInputStreamTest extends LogTestBase {
    private @TempDir File tempDir;

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testWriteTo(byte recordBatchMagic) throws Exception {
        try (FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            recordBatchMagic,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(recordBatchMagic);

            LogRecordBatch recordBatch = batch.loadFullBatch();

            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> iterator = recordBatch.records(readContext)) {
                assertThat(iterator.hasNext()).isTrue();
                LogRecord record = iterator.next();
                assertThat(record.getRow().getFieldCount()).isEqualTo(2);
                assertThat(iterator.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testV2FormatWithStatistics() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_v2.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);
            assertThat(batch.getRecordCount()).isEqualTo(TestData.DATA1.size());

            // Test statistics reading with ReadContext
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {

                // Test getStatistics method
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isPresent();

                LogRecordBatchStatistics statistics = statisticsOpt.get();
                assertThat(statistics.getMinValues()).isNotNull();
                assertThat(statistics.getMaxValues()).isNotNull();
                assertThat(statistics.getNullCounts()).isNotNull();

                // Verify statistics content for DATA1
                assertThat(statistics.getMinValues().getInt(0)).isEqualTo(1); // min id
                assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(10); // max id
                assertThat(statistics.getNullCounts()[0]).isEqualTo(0); // no nulls

                // Test that statistics are cached (lazy loading)
                Optional<LogRecordBatchStatistics> statisticsOpt2 =
                        batch.getStatistics(readContext);
                assertThat(statisticsOpt2).isPresent();
                assertThat(statisticsOpt2.get()).isSameAs(statisticsOpt.get());
            }

            // Test that records can still be read correctly
            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> iterator = batch.records(readContext)) {
                assertThat(iterator.hasNext()).isTrue();
                int recordCount = 0;
                while (iterator.hasNext()) {
                    LogRecord record = iterator.next();
                    assertThat(record).isNotNull();
                    recordCount++;
                }
                assertThat(recordCount).isEqualTo(TestData.DATA1.size());
            }
        }
    }

    @Test
    void testV2FormatWithoutStatistics() throws Exception {
        // Create test data without statistics using V1 format (which doesn't support statistics)
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_v1_no_stats.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            LOG_MAGIC_VALUE_V1,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V1);

            // Test that getStatistics returns empty when magic version doesn't support statistics
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isEmpty();
            }
        }
    }

    @Test
    void testGetStatisticsWithNullContext() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_null_context.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // Test that getStatistics returns empty when context is null
            Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(null);
            assertThat(statisticsOpt).isEmpty();
        }
    }

    @Test
    void testGetStatisticsWithInvalidSchemaId() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_invalid_schema.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // Test that getStatistics returns empty when schema is not found in context
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, 999)) { // Invalid schema ID
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isEmpty();
            }
        }
    }

    @Test
    void testOffsetCalculation() {
        // Test offset calculations
        System.out.println("LOG_OVERHEAD: " + LogRecordBatchFormat.LOG_OVERHEAD);
        System.out.println(
                "V2_ATTRIBUTES_OFFSET: "
                        + LogRecordBatchFormat.attributeOffset(
                                LogRecordBatchFormat.LOG_MAGIC_VALUE_V2));
        System.out.println(
                "V2_STATISTICS_LENGTH_OFFSET: "
                        + LogRecordBatchFormat.statisticsLengthOffset(
                                LogRecordBatchFormat.LOG_MAGIC_VALUE_V2));

        // Calculate relative offsets
        int attributesOffset =
                LogRecordBatchFormat.attributeOffset(LogRecordBatchFormat.LOG_MAGIC_VALUE_V2)
                        - LogRecordBatchFormat.LOG_OVERHEAD;
        int statisticsLengthOffset =
                LogRecordBatchFormat.statisticsLengthOffset(LogRecordBatchFormat.LOG_MAGIC_VALUE_V2)
                        - LogRecordBatchFormat.LOG_OVERHEAD;

        System.out.println("Relative attributes offset: " + attributesOffset);
        System.out.println("Relative statistics length offset: " + statisticsLengthOffset);

        // Verify calculations
        assertThat(attributesOffset).isEqualTo(19); // 31 - 12 = 19
        assertThat(statisticsLengthOffset).isEqualTo(40); // 52 - 12 = 40
    }

    @Test
    void testStatisticsCreation() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Get the batch
        LogRecordBatch memoryBatch = memoryLogRecords.batches().iterator().next();
        assertThat(memoryBatch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

        // Test that the memory batch has statistics
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
            Optional<LogRecordBatchStatistics> memoryStatsOpt =
                    memoryBatch.getStatistics(readContext);
            assertThat(memoryStatsOpt).isPresent();

            LogRecordBatchStatistics memoryStats = memoryStatsOpt.get();
            System.out.println("Memory batch statistics: " + memoryStats);

            // Verify statistics content
            assertThat(memoryStats.getMinValues().getInt(0)).isEqualTo(1);
            assertThat(memoryStats.getMaxValues().getInt(0)).isEqualTo(10);
            assertThat(memoryStats.getMinValues().getString(1).toString()).isEqualTo("a");
            assertThat(memoryStats.getMaxValues().getString(1).toString()).isEqualTo("j");
        }
    }

    @Test
    void testGetBytesViewWithoutStatisticsV2WithStats() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_v2_with_stats.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

            // Verify original batch has statistics
            assertThat(batch.statisticsSizeInBytes()).isGreaterThan(0);

            // Get bytes view without statistics
            BytesView bytesViewWithoutStats = batch.getBytesViewWithoutStatistics();
            assertThat(bytesViewWithoutStats).isNotNull();

            // Verify the returned view is a MultiBytesView
            assertThat(bytesViewWithoutStats).isInstanceOf(MultiBytesView.class);

            // Create a new batch from the bytes view without statistics
            ByteBuf byteBuf = bytesViewWithoutStats.getByteBuf();
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(0, bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            // Create a new batch from the modified data
            DefaultLogRecordBatch newBatch = new DefaultLogRecordBatch();
            newBatch.pointTo(MemorySegment.wrap(buffer.array()), 0);

            // Verify the new batch has no statistics
            assertThat(newBatch.statisticsSizeInBytes()).isEqualTo(0);

            // Verify the new batch still has the same magic version
            assertThat(newBatch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

            // Verify the new batch has the same record count
            assertThat(newBatch.getRecordCount()).isEqualTo(batch.getRecordCount());

            // Verify the new batch has the same schema ID
            assertThat(newBatch.schemaId()).isEqualTo(batch.schemaId());

            // Verify the new batch has the same base log offset
            assertThat(newBatch.baseLogOffset()).isEqualTo(batch.baseLogOffset());

            // Verify the new batch size is smaller (without statistics)
            assertThat(newBatch.sizeInBytes()).isLessThan(batch.sizeInBytes());

            // Verify the size difference equals the original statistics size
            int statisticsSize = batch.statisticsSizeInBytes();
            assertThat(batch.sizeInBytes() - newBatch.sizeInBytes()).isEqualTo(statisticsSize);
        }
    }

    @Test
    void testGetBytesViewWithoutStatisticsV2WithoutStats() throws Exception {
        // Create test data without statistics using V2 format
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_v2_without_stats.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            LOG_MAGIC_VALUE_V2,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

            // Verify original batch has no statistics
            assertThat(batch.statisticsSizeInBytes()).isEqualTo(0);

            // Get bytes view without statistics
            BytesView bytesViewWithoutStats = batch.getBytesViewWithoutStatistics();
            assertThat(bytesViewWithoutStats).isNotNull();

            // Create a new batch from the bytes view
            ByteBuf byteBuf = bytesViewWithoutStats.getByteBuf();
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(0, bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            DefaultLogRecordBatch newBatch = new DefaultLogRecordBatch();
            newBatch.pointTo(MemorySegment.wrap(bytes), 0);

            // Verify the new batch still has no statistics
            assertThat(newBatch.statisticsSizeInBytes()).isEqualTo(0);

            // Verify the new batch has the same size (no change since no statistics were present)
            assertThat(newBatch.sizeInBytes()).isEqualTo(batch.sizeInBytes());

            // Verify the new batch has the same magic version
            assertThat(newBatch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testGetBytesViewWithoutStatisticsV0V1(byte recordBatchMagic) throws Exception {
        // Create test data using V0/V1 format (which don't support statistics)
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_v0v1.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            recordBatchMagic,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(recordBatchMagic);

            // Verify original batch has no statistics
            assertThat(batch.statisticsSizeInBytes()).isEqualTo(0);

            // Get bytes view without statistics
            BytesView bytesViewWithoutStats = batch.getBytesViewWithoutStatistics();
            assertThat(bytesViewWithoutStats).isNotNull();

            // Create a new batch from the bytes view
            ByteBuf byteBuf = bytesViewWithoutStats.getByteBuf();
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(0, bytes);

            DefaultLogRecordBatch newBatch = new DefaultLogRecordBatch();
            newBatch.pointTo(MemorySegment.wrap(bytes), 0);

            // Verify the new batch still has no statistics
            assertThat(newBatch.statisticsSizeInBytes()).isEqualTo(0);

            // Verify the new batch has the same size (no change since no statistics were present)
            assertThat(newBatch.sizeInBytes()).isEqualTo(batch.sizeInBytes());

            // Verify the new batch has the same magic version
            assertThat(newBatch.magic()).isEqualTo(recordBatchMagic);
        }
    }

    @Test
    void testGetBytesViewWithoutStatisticsHeaderModification() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_header_modification.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

            // Verify original batch has statistics
            assertThat(batch.statisticsSizeInBytes()).isGreaterThan(0);

            // Get bytes view without statistics
            BytesView bytesViewWithoutStats = batch.getBytesViewWithoutStatistics();
            assertThat(bytesViewWithoutStats).isNotNull();

            // Create a new batch from the bytes view
            ByteBuf byteBuf = bytesViewWithoutStats.getByteBuf();
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(0, bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            DefaultLogRecordBatch newBatch = new DefaultLogRecordBatch();
            newBatch.pointTo(MemorySegment.wrap(buffer.array()), 0);

            // Verify the new batch has no statistics
            assertThat(newBatch.statisticsSizeInBytes()).isEqualTo(0);

            // Verify the statistics flag is cleared in attributes
            // The statistics flag should be bit 1 (0x02)
            // Note: attributes() method is private, so we can't test it directly
            // Instead, we verify the effect by checking statisticsSizeInBytes() == 0

            // Verify the statistics length field is set to 0
            // Since we can't access private fields directly, we verify through public methods
            assertThat(newBatch.statisticsSizeInBytes()).isEqualTo(0);

            // Verify the length field is updated correctly
            int originalLength = batch.batchSize;
            int newLength = newBatch.sizeInBytes() - LogRecordBatchFormat.LOG_OVERHEAD;
            int statisticsSize = batch.statisticsSizeInBytes();
            assertThat(newLength).isEqualTo(originalLength - statisticsSize);
        }
    }

    @Test
    void testGetBytesViewWithoutStatisticsDataIntegrity() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_data_integrity.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // Get bytes view without statistics
            BytesView bytesViewWithoutStats = batch.getBytesViewWithoutStatistics();
            assertThat(bytesViewWithoutStats).isNotNull();

            // Create a new batch from the bytes view
            ByteBuf byteBuf = bytesViewWithoutStats.getByteBuf();
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(0, bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            DefaultLogRecordBatch newBatch = new DefaultLogRecordBatch();
            newBatch.pointTo(MemorySegment.wrap(buffer.array()), 0);

            // Verify that records can still be read correctly
            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> iterator = newBatch.records(readContext)) {
                assertThat(iterator.hasNext()).isTrue();
                int recordCount = 0;
                while (iterator.hasNext()) {
                    LogRecord record = iterator.next();
                    assertThat(record).isNotNull();
                    assertThat(record.getRow().getFieldCount()).isEqualTo(2);
                    recordCount++;
                }
                assertThat(recordCount).isEqualTo(TestData.DATA1.size());
            }

            // Verify that the batch is still valid
            assertThat(newBatch.isValid()).isTrue();
        }
    }

    @Test
    void testGetBytesViewWithoutStatisticsMultipleBatches() throws Exception {
        // Create test data with multiple batches, some with statistics, some without
        MemoryLogRecords memoryLogRecords1 =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        MemoryLogRecords memoryLogRecords2 =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        TestData.ANOTHER_DATA1, DATA1_ROW_TYPE, 10L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_multiple_batches.tmp"))) {
            fileLogRecords.append(memoryLogRecords1);
            fileLogRecords.append(memoryLogRecords2);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            // Test first batch
            FileLogInputStream.FileChannelLogRecordBatch batch1 = logInputStream.nextBatch();
            assertThat(batch1).isNotNull();
            assertThat(batch1.statisticsSizeInBytes()).isGreaterThan(0);

            BytesView bytesView1 = batch1.getBytesViewWithoutStatistics();
            assertThat(bytesView1).isNotNull();

            ByteBuf byteBuf1 = bytesView1.getByteBuf();
            byte[] bytes1 = new byte[byteBuf1.readableBytes()];
            byteBuf1.getBytes(0, bytes1);
            ByteBuffer buffer1 = ByteBuffer.wrap(bytes1);

            DefaultLogRecordBatch newBatch1 = new DefaultLogRecordBatch();
            newBatch1.pointTo(MemorySegment.wrap(buffer1.array()), 0);
            assertThat(newBatch1.statisticsSizeInBytes()).isEqualTo(0);

            // Test second batch
            FileLogInputStream.FileChannelLogRecordBatch batch2 = logInputStream.nextBatch();
            assertThat(batch2).isNotNull();
            assertThat(batch2.statisticsSizeInBytes()).isGreaterThan(0);

            BytesView bytesView2 = batch2.getBytesViewWithoutStatistics();
            assertThat(bytesView2).isNotNull();

            ByteBuf byteBuf2 = bytesView2.getByteBuf();
            byte[] bytes2 = new byte[byteBuf2.readableBytes()];
            byteBuf2.getBytes(0, bytes2);
            ByteBuffer buffer2 = ByteBuffer.wrap(bytes2);

            DefaultLogRecordBatch newBatch2 = new DefaultLogRecordBatch();
            newBatch2.pointTo(MemorySegment.wrap(buffer2.array()), 0);
            assertThat(newBatch2.statisticsSizeInBytes()).isEqualTo(0);

            // Verify both batches have different base log offsets
            assertThat(newBatch1.baseLogOffset()).isNotEqualTo(newBatch2.baseLogOffset());
            assertThat(newBatch1.baseLogOffset()).isEqualTo(0L);
            assertThat(newBatch2.baseLogOffset()).isEqualTo(10L);
        }
    }
}
