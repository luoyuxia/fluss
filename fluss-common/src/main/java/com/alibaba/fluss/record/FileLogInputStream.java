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

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.FileRegionBytesView;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;
import com.alibaba.fluss.utils.FileUtils;
import com.alibaba.fluss.utils.crc.Crc32C;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.Optional;

import static com.alibaba.fluss.record.LogRecordBatchFormat.BASE_OFFSET_OFFSET;
import static com.alibaba.fluss.record.LogRecordBatchFormat.HEADER_SIZE_UP_TO_MAGIC;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static com.alibaba.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static com.alibaba.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static com.alibaba.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A log input stream which is backed by a {@link FileChannel}. */
public class FileLogInputStream
        implements LogInputStream<FileLogInputStream.FileChannelLogRecordBatch> {
    private static final Logger LOG = LoggerFactory.getLogger(FileLogInputStream.class);

    private int position;
    private final int end;
    private final FileLogRecords fileRecords;
    private final ByteBuffer logHeaderBuffer = ByteBuffer.allocate(HEADER_SIZE_UP_TO_MAGIC);

    /** Create a new log input stream over the FileChannel. */
    FileLogInputStream(FileLogRecords records, int start, int end) {
        this.fileRecords = records;
        this.position = start;
        this.end = end;
        this.logHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public FileChannelLogRecordBatch nextBatch() throws IOException {
        FileChannel channel = fileRecords.channel();
        if (position >= end - HEADER_SIZE_UP_TO_MAGIC) {
            return null;
        }

        logHeaderBuffer.rewind();
        FileUtils.readFullyOrFail(channel, logHeaderBuffer, position, "log header");

        logHeaderBuffer.rewind();
        long offset = logHeaderBuffer.getLong(BASE_OFFSET_OFFSET);
        int length = logHeaderBuffer.getInt(LENGTH_OFFSET);

        if (position > end - LOG_OVERHEAD - length) {
            return null;
        }

        byte magic = logHeaderBuffer.get(MAGIC_OFFSET);
        FileChannelLogRecordBatch batch =
                new FileChannelLogRecordBatch(offset, magic, fileRecords, position, length);

        position += batch.sizeInBytes();
        return batch;
    }

    /**
     * Log entry backed by an underlying FileChannel. This allows iteration over the record batches
     * without needing to read the record data into memory until it is needed. The downside is that
     * entries will generally no longer be readable when the underlying channel is closed.
     */
    public static class FileChannelLogRecordBatch implements LogRecordBatch {
        protected final long offset;
        protected final byte magic;
        protected final FileLogRecords fileRecords;
        protected final int position;
        protected final int batchSize;

        private LogRecordBatch fullBatch;
        private LogRecordBatch batchHeader;
        private LogRecordBatchStatistics statistics;

        // Cache for statistics to avoid repeated parsing
        private Optional<LogRecordBatchStatistics> cachedStatistics = null;

        FileChannelLogRecordBatch(
                long offset, byte magic, FileLogRecords fileRecords, int position, int batchSize) {
            this.offset = offset;
            this.magic = magic;
            this.fileRecords = fileRecords;
            this.position = position;
            this.batchSize = batchSize;
        }

        @Override
        public long checksum() {
            return loadBatchHeader().checksum();
        }

        @Override
        public short schemaId() {
            return loadBatchHeader().schemaId();
        }

        @Override
        public long baseLogOffset() {
            return offset;
        }

        public int position() {
            return position;
        }

        public BytesView getBytesView() {
            return new FileRegionBytesView(fileRecords.channel(), position, sizeInBytes());
        }

        /**
         * Get a bytes view without statistics information. For V2+ batches that contain statistics,
         * this method will rewrite the header to clear the statistics flag and length, and exclude
         * the statistics data from the returned view.
         *
         * @return A MultiBytesView containing the batch data without statistics
         * @throws IOException if reading from the file fails
         */
        public BytesView getBytesViewWithoutStatistics() throws IOException {
            FileChannel channel = fileRecords.channel();
            MultiBytesView.Builder builder = MultiBytesView.builder();

            // Read the original log header
            ByteBuffer logHeaderBuffer = ByteBuffer.allocate(headerSize());
            logHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
            FileUtils.readFullyOrFail(channel, logHeaderBuffer, position, "log header");
            logHeaderBuffer.rewind();

            // Get the original batch size
            int originalBatchSizeInBytes = LOG_OVERHEAD + logHeaderBuffer.getInt(LENGTH_OFFSET);

            // For V2+ versions, clear statistics flag and length
            if (magic >= LogRecordBatchFormat.LOG_MAGIC_VALUE_V2) {
                // Clear statistics flag in attributes
                int attributeOffset = LogRecordBatchFormat.attributeOffset(magic);
                byte attributes = logHeaderBuffer.get(attributeOffset);
                attributes &= ~LogRecordBatchFormat.STATISTICS_FLAG_MASK; // clear statistics flag
                logHeaderBuffer.put(attributeOffset, attributes);

                // Clear statistics length field
                int statsLengthOffset = LogRecordBatchFormat.statisticsLengthOffset(magic);
                logHeaderBuffer.putInt(statsLengthOffset, 0);
            }

            // Calculate new batch size (excluding statistics)
            int statisticsLength = loadBatchHeader().statisticsSizeInBytes();
            int newBatchSizeInBytes = originalBatchSizeInBytes - statisticsLength;

            // Update the length field in the header
            logHeaderBuffer.position(LENGTH_OFFSET);
            logHeaderBuffer.putInt(newBatchSizeInBytes - LOG_OVERHEAD);

            // Create new header bytes
            logHeaderBuffer.rewind();
            byte[] newHeader = new byte[headerSize()];
            logHeaderBuffer.get(newHeader);

            // Build the MultiBytesView
            builder.addBytes(newHeader);

            // Add the data portion (excluding statistics)
            int dataSize = newBatchSizeInBytes - headerSize();
            if (dataSize > 0) {
                builder.addBytes(channel, position + headerSize(), dataSize);
            }

            // Recalculate CRC for the modified batch
            MultiBytesView result = builder.build();
            ByteBuf byteBuf = result.getByteBuf();
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(0, bytes);

            // Calculate new CRC from schemaId to end of batch
            int schemaIdOffset = LogRecordBatchFormat.schemaIdOffset(magic);
            long newCrc =
                    Crc32C.compute(bytes, schemaIdOffset, newBatchSizeInBytes - schemaIdOffset);

            // Update CRC in the header
            int crcOffset = LogRecordBatchFormat.crcOffset(magic);
            // Write CRC in little-endian order
            bytes[crcOffset] = (byte) (newCrc & 0xFF);
            bytes[crcOffset + 1] = (byte) ((newCrc >> 8) & 0xFF);
            bytes[crcOffset + 2] = (byte) ((newCrc >> 16) & 0xFF);
            bytes[crcOffset + 3] = (byte) ((newCrc >> 24) & 0xFF);

            // Create final result with updated CRC
            return new MemorySegmentBytesView(MemorySegment.wrap(bytes), 0, newBatchSizeInBytes);
        }

        @Override
        public byte magic() {
            return magic;
        }

        @Override
        public long commitTimestamp() {
            return loadBatchHeader().commitTimestamp();
        }

        @Override
        public long nextLogOffset() {
            return lastLogOffset() + 1;
        }

        @Override
        public long writerId() {
            return loadBatchHeader().writerId();
        }

        @Override
        public int batchSequence() {
            return loadBatchHeader().batchSequence();
        }

        @Override
        public int leaderEpoch() {
            return loadBatchHeader().leaderEpoch();
        }

        @Override
        public long lastLogOffset() {
            return loadBatchHeader().lastLogOffset();
        }

        @Override
        public int getRecordCount() {
            return loadBatchHeader().getRecordCount();
        }

        @Override
        public CloseableIterator<LogRecord> records(ReadContext context) {
            return loadFullBatch().records(context);
        }

        @Override
        public boolean isValid() {
            return loadFullBatch().isValid();
        }

        @Override
        public void ensureValid() {
            loadFullBatch().ensureValid();
        }

        @Override
        public int sizeInBytes() {
            return LOG_OVERHEAD + batchSize;
        }

        private LogRecordBatch toMemoryRecordBatch(ByteBuffer buffer) {
            DefaultLogRecordBatch records = new DefaultLogRecordBatch();
            records.pointTo(MemorySegment.wrap(buffer.array()), 0);
            return records;
        }

        private int headerSize() {
            return recordBatchHeaderSize(magic);
        }

        protected LogRecordBatch loadFullBatch() {
            if (fullBatch == null) {
                batchHeader = null;
                fullBatch = loadBatchWithSize(sizeInBytes(), "full record batch");
            }
            return fullBatch;
        }

        protected LogRecordBatch loadBatchHeader() {
            if (fullBatch != null) {
                return fullBatch;
            }

            if (batchHeader == null) {
                batchHeader = loadBatchWithSize(headerSize(), "record batch header");
            }

            return batchHeader;
        }

        protected ByteBuffer loadByteBufferWithSize(int size, int position, String description) {
            FileChannel channel = fileRecords.channel();
            try {
                return FileUtils.loadByteBufferFromFile(channel, size, position, description);
            } catch (IOException e) {
                throw new FlussRuntimeException(
                        "Failed to load record batch at position "
                                + position
                                + " from "
                                + fileRecords,
                        e);
            }
        }

        private LogRecordBatch loadBatchWithSize(int size, String description) {
            return toMemoryRecordBatch(loadByteBufferWithSize(size, position, description));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FileChannelLogRecordBatch that = (FileChannelLogRecordBatch) o;

            FileChannel channel = fileRecords == null ? null : fileRecords.channel();
            FileChannel thatChannel = that.fileRecords == null ? null : that.fileRecords.channel();

            return offset == that.offset
                    && position == that.position
                    && batchSize == that.batchSize
                    && Objects.equals(channel, thatChannel);
        }

        @Override
        public int hashCode() {
            FileChannel channel = fileRecords == null ? null : fileRecords.channel();

            int result = Long.hashCode(offset);
            result = 31 * result + (channel != null ? channel.hashCode() : 0);
            result = 31 * result + position;
            result = 31 * result + batchSize;
            return result;
        }

        @Override
        public String toString() {
            return "FileChannelLogRecordBatch(magic: "
                    + magic
                    + ", offset: "
                    + offset
                    + ", size: "
                    + batchSize
                    + ")";
        }

        @Override
        public Optional<LogRecordBatchStatistics> getStatistics(ReadContext context) {
            if (context == null) {
                return Optional.empty();
            }

            // Return cached statistics if already parsed
            if (cachedStatistics != null) {
                return cachedStatistics;
            }

            if (magic < LogRecordBatchFormat.LOG_MAGIC_VALUE_V2) {
                // Statistics are only available in V2 and later
                cachedStatistics = Optional.empty();
                return cachedStatistics;
            }

            try {
                // Load and parse statistics
                if (statistics != null) {
                    cachedStatistics = Optional.of(statistics);
                    return cachedStatistics;
                }

                RowType rowType = context.getRowType(schemaId());
                if (rowType == null) {
                    cachedStatistics = Optional.empty();
                    return cachedStatistics;
                }

                int statisticsLength = loadBatchHeader().statisticsSizeInBytes();

                if (statisticsLength <= 0) {
                    cachedStatistics = Optional.empty();
                    return cachedStatistics;
                }

                int statisticsDataOffset = sizeInBytes() - statisticsLength;

                ByteBuffer statisticsData =
                        loadByteBufferWithSize(
                                statisticsLength, position + statisticsDataOffset, "statistics");

                // Parse statistics directly from byte buffer without creating heap objects
                statistics =
                        LogRecordBatchStatisticsParser.parseStatistics(
                                statisticsData.array(), rowType);
                cachedStatistics = Optional.ofNullable(statistics);
                return cachedStatistics;
            } catch (Exception e) {
                // If loading statistics fails, log the error and return empty
                LOG.warn("Failed to load statistics for record batch at position {}", position, e);
                cachedStatistics = Optional.empty();
                return cachedStatistics;
            }
        }

        @Override
        public int statisticsSizeInBytes() {
            return loadBatchHeader().statisticsSizeInBytes();
        }
    }
}
