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
import com.alibaba.fluss.memory.MemorySegmentInputView;
import com.alibaba.fluss.types.RowType;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.alibaba.fluss.record.LogRecordBatchFormat.STATISTICS_VERSION;

/**
 * Parser for LogRecordBatchStatistics that reads statistics directly from memory without creating
 * intermediate heap objects.
 */
public class LogRecordBatchStatisticsParser {

    // Use the constant from LogRecordBatchFormat

    /**
     * Parse statistics from a memory segment.
     *
     * @param segment The memory segment containing the statistics data
     * @param position The position in the segment to start reading from
     * @param size The size of the statistics data
     * @param rowType The row type for interpreting the data
     * @return The parsed statistics as a ByteViewLogRecordBatchStatistics, or null if parsing fails
     */
    public static ByteViewLogRecordBatchStatistics parseStatistics(
            MemorySegment segment, int position, int size, RowType rowType) {
        if (size <= 0) {
            return null;
        }

        try {
            MemorySegmentInputView inputView = new MemorySegmentInputView(segment, position);
            int currentPos = position;

            // Read version
            byte version = inputView.readByte();
            currentPos += 1;
            if (version != STATISTICS_VERSION) {
                return null; // Unsupported version
            }

            // Read field count
            int fieldCount = inputView.readShort();
            currentPos += 2;
            if (fieldCount != rowType.getFieldCount()) {
                return null; // Field count mismatch
            }

            // Read null counts array
            Long[] nullCounts = new Long[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                nullCounts[i] = inputView.readLong();
                currentPos += 8;
            }

            // Read min values size
            int minValuesSize = inputView.readInt();
            currentPos += 4;

            // Calculate min values offset
            int minValuesOffset = currentPos - position;

            // Skip min values data
            if (minValuesSize > 0) {
                // Skip the data by reading bytes
                for (int i = 0; i < minValuesSize; i++) {
                    inputView.readByte();
                }
                currentPos += minValuesSize;
            }

            // Read max values size
            int maxValuesSize = inputView.readInt();
            currentPos += 4;

            // Calculate max values offset
            int maxValuesOffset = currentPos - position;

            // Skip max values data
            if (maxValuesSize > 0) {
                // Skip the data by reading bytes
                for (int i = 0; i < maxValuesSize; i++) {
                    inputView.readByte();
                }
                currentPos += maxValuesSize;
            }

            return new ByteViewLogRecordBatchStatistics(
                    segment,
                    position,
                    size,
                    rowType,
                    nullCounts,
                    minValuesOffset,
                    maxValuesOffset,
                    minValuesSize,
                    maxValuesSize);

        } catch (IOException e) {
            return null; // Parsing failed
        }
    }

    /**
     * Parse statistics from a byte array.
     *
     * @param data The byte array containing the statistics data
     * @param rowType The row type for interpreting the data
     * @return The parsed statistics as a ByteViewLogRecordBatchStatistics, or null if parsing fails
     */
    public static ByteViewLogRecordBatchStatistics parseStatistics(byte[] data, RowType rowType) {
        if (data == null || data.length == 0) {
            return null;
        }

        MemorySegment segment = MemorySegment.wrap(data);
        return parseStatistics(segment, 0, data.length, rowType);
    }

    /**
     * Parse statistics from a ByteBuffer.
     *
     * @param buffer The ByteBuffer containing the statistics data
     * @param rowType The row type for interpreting the data
     * @return The parsed statistics as a ByteViewLogRecordBatchStatistics, or null if parsing fails
     */
    public static ByteViewLogRecordBatchStatistics parseStatistics(
            ByteBuffer buffer, RowType rowType) {
        if (buffer == null || buffer.remaining() == 0) {
            return null;
        }

        MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
        return parseStatistics(segment, 0, buffer.remaining(), rowType);
    }

    /**
     * Check if the given memory segment contains valid statistics.
     *
     * @param segment The memory segment to check
     * @param position The position to start reading from
     * @param size The size of the data to check
     * @param rowType The row type to validate against
     * @return true if the segment contains valid statistics
     */
    public static boolean isValidStatistics(
            MemorySegment segment, int position, int size, RowType rowType) {
        if (segment == null || size < 3) {
            return false;
        }

        try {
            MemorySegmentInputView inputView = new MemorySegmentInputView(segment, position);

            // Check version
            byte version = inputView.readByte();
            if (version != STATISTICS_VERSION) {
                return false;
            }

            // Check field count
            int fieldCount = inputView.readShort();
            return fieldCount == rowType.getFieldCount();

        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Check if the given data contains valid statistics.
     *
     * @param data The byte array to check
     * @param rowType The row type to validate against
     * @return true if the data contains valid statistics
     */
    public static boolean isValidStatistics(byte[] data, RowType rowType) {
        if (data == null || data.length < 3) {
            return false;
        }

        MemorySegment segment = MemorySegment.wrap(data);
        return isValidStatistics(segment, 0, data.length, rowType);
    }

    /**
     * Check if the given ByteBuffer contains valid statistics.
     *
     * @param buffer The ByteBuffer to check
     * @param rowType The row type to validate against
     * @return true if the buffer contains valid statistics
     */
    public static boolean isValidStatistics(ByteBuffer buffer, RowType rowType) {
        if (buffer == null || buffer.remaining() < 3) {
            return false;
        }

        MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
        return isValidStatistics(segment, 0, buffer.remaining(), rowType);
    }

    /**
     * Get the size of statistics data from the header.
     *
     * @param segment The memory segment containing the statistics data
     * @param position The position to start reading from
     * @param size The size of the data to check
     * @return The size of the statistics data, or -1 if invalid
     */
    public static int getStatisticsSize(MemorySegment segment, int position, int size) {
        if (segment == null || size < 3) {
            return -1;
        }

        try {
            MemorySegmentInputView inputView = new MemorySegmentInputView(segment, position);
            int currentPos = position;

            // Skip version
            inputView.readByte(); // version
            currentPos += 1;

            // Read field count
            int fieldCount = inputView.readShort();
            currentPos += 2;

            // Skip null counts (8 bytes per field)
            int nullCountsSize = fieldCount * 8;
            for (int i = 0; i < nullCountsSize; i++) {
                inputView.readByte();
            }
            currentPos += nullCountsSize;

            // Read min values size
            int minValuesSize = inputView.readInt();
            currentPos += 4;

            // Skip min values
            if (minValuesSize > 0) {
                for (int i = 0; i < minValuesSize; i++) {
                    inputView.readByte();
                }
                currentPos += minValuesSize;
            }

            // Read max values size
            int maxValuesSize = inputView.readInt();
            currentPos += 4;

            // Skip max values
            if (maxValuesSize > 0) {
                for (int i = 0; i < maxValuesSize; i++) {
                    inputView.readByte();
                }
                currentPos += maxValuesSize;
            }

            return currentPos - position;

        } catch (IOException e) {
            return -1;
        }
    }

    /**
     * Get the size of statistics data from the header.
     *
     * @param data The byte array containing the statistics data
     * @return The size of the statistics data, or -1 if invalid
     */
    public static int getStatisticsSize(byte[] data) {
        if (data == null || data.length < 3) {
            return -1;
        }

        MemorySegment segment = MemorySegment.wrap(data);
        return getStatisticsSize(segment, 0, data.length);
    }

    /**
     * Get the size of statistics data from the header.
     *
     * @param buffer The ByteBuffer containing the statistics data
     * @return The size of the statistics data, or -1 if invalid
     */
    public static int getStatisticsSize(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < 3) {
            return -1;
        }

        MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
        return getStatisticsSize(segment, 0, buffer.remaining());
    }
}
