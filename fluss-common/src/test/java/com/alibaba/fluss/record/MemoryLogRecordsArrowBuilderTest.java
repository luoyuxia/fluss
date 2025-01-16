/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.memory.LazyMemorySegmentPool;
import com.alibaba.fluss.memory.ManagedPagedOutputView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.arrow.ArrowWriter;
import com.alibaba.fluss.row.arrow.ArrowWriterPool;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.assertMemoryRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MemoryLogRecordsArrowBuilder}. */
public class MemoryLogRecordsArrowBuilderTest {
    private BufferAllocator allocator;
    private ArrowWriterPool provider;
    private Configuration conf;

    @BeforeEach
    void setup() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.provider = new ArrowWriterPool(allocator);
        this.conf = new Configuration();
    }

    @AfterEach
    void tearDown() {
        provider.close();
        allocator.close();
    }

    @Test
    void testAppendWithEmptyRecord() throws Exception {
        int maxSizeInBytes = 1024;
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L,
                        DEFAULT_SCHEMA_ID,
                        maxSizeInBytes,
                        DATA1_ROW_TYPE,
                        CompressionUtil.CodecType.NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder = createMemoryLogRecordsArrowBuilder(writer, 10, 100);
        assertThat(builder.isFull()).isFalse();
        assertThat(builder.getMaxSizeInBytes()).isEqualTo(maxSizeInBytes);
        builder.close();
        builder.serialize();
        builder.setWriterState(1L, 0);
        MemoryLogRecords records =
                MemoryLogRecords.pointToByteBuffer(builder.build().getByteBuf().nioBuffer());
        Iterator<LogRecordBatch> iterator = records.batches().iterator();
        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch batch = iterator.next();
        assertThat(batch.getRecordCount()).isEqualTo(0);
        assertThat(batch.sizeInBytes()).isEqualTo(48);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testAppend() throws Exception {
        int maxSizeInBytes = 1024;
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L,
                        DEFAULT_SCHEMA_ID,
                        maxSizeInBytes,
                        DATA1_ROW_TYPE,
                        CompressionUtil.CodecType.NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder = createMemoryLogRecordsArrowBuilder(writer, 10, 1024);
        List<RowKind> rowKinds =
                DATA1.stream().map(row -> RowKind.APPEND_ONLY).collect(Collectors.toList());
        List<InternalRow> rows =
                DATA1.stream()
                        .map(object -> row(DATA1_ROW_TYPE, object))
                        .collect(Collectors.toList());
        List<Object[]> expectedResult = new ArrayList<>();
        while (!builder.isFull()) {
            int rndIndex = RandomUtils.nextInt(0, DATA1.size());
            builder.append(rowKinds.get(rndIndex), rows.get(rndIndex));
            expectedResult.add(DATA1.get(rndIndex));
        }
        assertThat(builder.isFull()).isTrue();
        assertThatThrownBy(() -> builder.append(RowKind.APPEND_ONLY, rows.get(0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "The arrow batch size is full and it shouldn't accept writing new rows, it's a bug.");

        builder.setWriterState(1L, 0);
        builder.close();
        assertThatThrownBy(() -> builder.append(RowKind.APPEND_ONLY, rows.get(0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Tried to append a record, but MemoryLogRecordsArrowBuilder is closed for record appends");
        builder.serialize();
        assertThat(builder.isClosed()).isTrue();
        MemoryLogRecords records =
                MemoryLogRecords.pointToByteBuffer(builder.build().getByteBuf().nioBuffer());
        assertLogRecordsEquals(DATA1_ROW_TYPE, records, expectedResult);
    }

    @ParameterizedTest
    @MethodSource("codecTypes")
    void testCompression(CompressionUtil.CodecType codec) throws Exception {
        int maxSizeInBytes = 1024;
        // create a compression-able data set.
        List<Object[]> dataSet =
                Arrays.asList(
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "},
                        new Object[] {1, "                        "});

        // first create an un-compression batch.
        ArrowWriter writer1 =
                provider.getOrCreateWriter(
                        1L,
                        DEFAULT_SCHEMA_ID,
                        maxSizeInBytes,
                        DATA1_ROW_TYPE,
                        CompressionUtil.CodecType.NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                createMemoryLogRecordsArrowBuilder(writer1, 10, 1024);
        for (Object[] data : dataSet) {
            builder.append(RowKind.APPEND_ONLY, row(DATA1_ROW_TYPE, data));
        }
        builder.close();
        builder.serialize();
        MemoryLogRecords records1 =
                MemoryLogRecords.pointToByteBuffer(builder.build().getByteBuf().nioBuffer());
        int sizeInBytes1 = records1.sizeInBytes();
        assertLogRecordsEquals(DATA1_ROW_TYPE, records1, dataSet);

        // second create a compression batch.
        ArrowWriter writer2 =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, maxSizeInBytes, DATA1_ROW_TYPE, codec);
        MemoryLogRecordsArrowBuilder builder2 =
                createMemoryLogRecordsArrowBuilder(writer2, 10, 1024);
        for (Object[] data : dataSet) {
            builder2.append(RowKind.APPEND_ONLY, row(DATA1_ROW_TYPE, data));
        }
        builder2.close();
        builder2.serialize();
        MemoryLogRecords records2 =
                MemoryLogRecords.pointToByteBuffer(builder2.build().getByteBuf().nioBuffer());

        int sizeInBytes2 = records2.sizeInBytes();
        assertLogRecordsEquals(DATA1_ROW_TYPE, records2, dataSet);

        // compare the size of two batches.
        assertThat(sizeInBytes1).isGreaterThan(sizeInBytes2);
    }

    @Test
    void testIllegalArgument() {
        int maxSizeInBytes = 1024;
        assertThatThrownBy(
                        () -> {
                            try (ArrowWriter writer =
                                    provider.getOrCreateWriter(
                                            1L,
                                            DEFAULT_SCHEMA_ID,
                                            maxSizeInBytes,
                                            DATA1_ROW_TYPE,
                                            CompressionUtil.CodecType.NO_COMPRESSION)) {
                                createMemoryLogRecordsArrowBuilder(writer, 10, 30);
                            }
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The size of first segment of pagedOutputView is too small, need at least 48 bytes.");
    }

    @Test
    void testClose() throws Exception {
        int maxSizeInBytes = 1024;
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L,
                        DEFAULT_SCHEMA_ID,
                        1024,
                        DATA1_ROW_TYPE,
                        CompressionUtil.CodecType.NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder = createMemoryLogRecordsArrowBuilder(writer, 10, 1024);
        List<RowKind> rowKinds =
                DATA1.stream().map(row -> RowKind.APPEND_ONLY).collect(Collectors.toList());
        List<InternalRow> rows =
                DATA1.stream()
                        .map(object -> row(DATA1_ROW_TYPE, object))
                        .collect(Collectors.toList());
        while (!builder.isFull()) {
            int rndIndex = RandomUtils.nextInt(0, DATA1.size());
            builder.append(rowKinds.get(rndIndex), rows.get(rndIndex));
        }
        assertThat(builder.isFull()).isTrue();

        String tableSchemaId = 1L + "-" + 1 + "-" + "NO_COMPRESSION";
        assertThat(provider.freeWriters().size()).isEqualTo(0);
        int sizeInBytesBeforeClose = builder.getSizeInBytes();
        builder.close();
        builder.serialize();
        builder.setWriterState(1L, 0);
        MemoryLogRecords.pointToByteBuffer(builder.build().getByteBuf().nioBuffer());
        assertThat(provider.freeWriters().get(tableSchemaId).size()).isEqualTo(1);
        int sizeInBytes = builder.getSizeInBytes();
        assertThat(sizeInBytes).isEqualTo(sizeInBytesBeforeClose);
        // get writer again, writer will be initial.
        ArrowWriter writer1 =
                provider.getOrCreateWriter(
                        1L,
                        DEFAULT_SCHEMA_ID,
                        maxSizeInBytes,
                        DATA1_ROW_TYPE,
                        CompressionUtil.CodecType.NO_COMPRESSION);
        assertThat(provider.freeWriters().get(tableSchemaId).size()).isEqualTo(0);

        // Even if the writer has re-initialized, the sizeInBytes should be the same.
        assertThat(builder.getSizeInBytes()).isEqualTo(sizeInBytes);

        writer.close();
        writer1.close();
    }

    @Test
    void testOverrideLastOffset() throws Exception {
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L,
                        DEFAULT_SCHEMA_ID,
                        1024 * 10,
                        DATA1_ROW_TYPE,
                        CompressionUtil.CodecType.NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                createMemoryLogRecordsArrowBuilder(writer, 10, 1024 * 10);
        List<RowKind> rowKinds =
                DATA1.stream().map(row -> RowKind.APPEND_ONLY).collect(Collectors.toList());
        List<InternalRow> rows =
                DATA1.stream()
                        .map(object -> row(DATA1_ROW_TYPE, object))
                        .collect(Collectors.toList());
        for (int i = 0; i < DATA1.size(); i++) {
            builder.append(rowKinds.get(i), rows.get(i));
        }

        // override lastLogOffset smaller than record counts.
        assertThatThrownBy(() -> builder.overrideLastLogOffset(3L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The override lastLogOffset is less than recordCount + baseLogOffset,"
                                + " which will cause the logOffsetDelta to be negative");

        // override lastLogOffset larger than record counts.
        builder.overrideLastLogOffset(15L);
        builder.close();
        builder.serialize();
        MemoryLogRecords memoryLogRecords =
                MemoryLogRecords.pointToByteBuffer(builder.build().getByteBuf().nioBuffer());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(10);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(15);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(16);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        assertMemoryRecordsEquals(
                DATA1_ROW_TYPE, memoryLogRecords, Collections.singletonList(DATA1));

        // test empty record batch.
        ArrowWriter writer2 =
                provider.getOrCreateWriter(
                        1L,
                        DEFAULT_SCHEMA_ID,
                        1024 * 10,
                        DATA1_ROW_TYPE,
                        CompressionUtil.CodecType.NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder2 =
                createMemoryLogRecordsArrowBuilder(writer2, 10, 1024 * 10);
        builder2.overrideLastLogOffset(0);
        builder2.close();
        builder2.serialize();
        memoryLogRecords =
                MemoryLogRecords.pointToByteBuffer(builder2.build().getByteBuf().nioBuffer());
        iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(0);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(1);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createArrowReadContext(
                                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }
    }

    private static List<CompressionUtil.CodecType> codecTypes() {
        return Arrays.asList(CompressionUtil.CodecType.LZ4_FRAME, CompressionUtil.CodecType.ZSTD);
    }

    private MemoryLogRecordsArrowBuilder createMemoryLogRecordsArrowBuilder(
            ArrowWriter writer, int maxPages, int pageSizeInBytes) throws IOException {
        conf.set(
                ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE,
                new MemorySize((long) maxPages * pageSizeInBytes));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(pageSizeInBytes));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(pageSizeInBytes));
        return MemoryLogRecordsArrowBuilder.builder(
                0L,
                DEFAULT_SCHEMA_ID,
                writer,
                new ManagedPagedOutputView(LazyMemorySegmentPool.create(conf)));
    }
}
