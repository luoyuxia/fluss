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
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.compacted.CompactedRowDeserializer;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.io.IOException;

/**
 * Optimized serializer for {@link LogRecordBatchStatistics} using CompactedRow for better space
 * efficiency.
 *
 * <p>The serialization format is as follows:
 *
 * <ul>
 *   <li>Version (1 byte): Format version for future compatibility
 *   <li>Field Count (2 bytes): Number of fields in the statistics
 *   <li>Null Counts (8 * fieldCount bytes): Array of null counts for each field
 *   <li>Min Values CompactedRow Size (4 bytes): Size of min values CompactedRow
 *   <li>Min Values CompactedRow (variable): Serialized min values using CompactedRow format
 *   <li>Max Values CompactedRow Size (4 bytes): Size of max values CompactedRow
 *   <li>Max Values CompactedRow (variable): Serialized max values using CompactedRow format
 * </ul>
 */
public class LogRecordBatchStatisticsSerializer {

    private static final byte STATISTICS_VERSION = 2; // Increment version for new format

    /**
     * Serializes the given statistics to a byte array using CompactedRow for optimal space
     * efficiency.
     *
     * @param statistics The statistics to serialize
     * @param rowType The row type for the statistics
     * @return The serialized statistics as a byte array
     * @throws IOException If serialization fails
     */
    public static byte[] serialize(LogRecordBatchStatistics statistics, RowType rowType)
            throws IOException {
        if (statistics == null) {
            return new byte[0];
        }

        // Estimate size: version(1) + fieldCount(2) + nullCounts(8*fieldCount) + minRow + maxRow
        int estimatedSize =
                3 + 8 * rowType.getFieldCount() + 1024; // Conservative estimate for rows
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(estimatedSize);

        // Write version
        outputView.writeByte(STATISTICS_VERSION);

        // Write field count
        int fieldCount = rowType.getFieldCount();
        outputView.writeShort(fieldCount);

        // Write null counts array
        Long[] nullCounts = statistics.getNullCounts();
        for (int i = 0; i < fieldCount; i++) {
            long nullCount = nullCounts != null ? nullCounts[i] : 0L;
            outputView.writeLong(nullCount);
        }

        // Write min values as CompactedRow
        CompactedRow minValues = (CompactedRow) statistics.getMinValues();
        if (minValues != null) {
            int minRowSize = minValues.getSizeInBytes();
            outputView.writeInt(minRowSize);
            outputView.write(
                    minValues.getSegment().getHeapMemory(), minValues.getOffset(), minRowSize);
        } else {
            // Write empty CompactedRow
            outputView.writeInt(0);
        }

        // Write max values as CompactedRow
        CompactedRow maxValues = (CompactedRow) statistics.getMaxValues();
        if (maxValues != null) {
            int maxRowSize = maxValues.getSizeInBytes();
            outputView.writeInt(maxRowSize);
            outputView.write(
                    maxValues.getSegment().getHeapMemory(), maxValues.getOffset(), maxRowSize);
        } else {
            // Write empty CompactedRow
            outputView.writeInt(0);
        }

        return outputView.getCopyOfBuffer();
    }

    /**
     * Deserializes statistics from a byte array using CompactedRow format.
     *
     * @param data The serialized statistics data
     * @param rowType The row type for the statistics
     * @return The deserialized statistics, or null if data is empty
     * @throws IOException If deserialization fails
     */
    public static LogRecordBatchStatistics deserialize(byte[] data, RowType rowType)
            throws IOException {
        if (data == null || data.length == 0) {
            return null;
        }

        MemorySegment segment = MemorySegment.wrap(data);
        MemorySegmentInputView inputView = new MemorySegmentInputView(segment);

        // Read version
        byte version = inputView.readByte();
        if (version != STATISTICS_VERSION) {
            throw new IOException("Unsupported statistics version: " + version);
        }

        // Read field count
        int fieldCount = inputView.readShort();
        if (fieldCount != rowType.getFieldCount()) {
            throw new IOException(
                    "Field count mismatch: expected "
                            + rowType.getFieldCount()
                            + ", got "
                            + fieldCount);
        }

        // Read null counts array
        Long[] nullCounts = new Long[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            nullCounts[i] = inputView.readLong();
        }

        // Read min values CompactedRow
        CompactedRow minValues = null;
        int minRowSize = inputView.readInt();
        if (minRowSize > 0) {
            DataType[] fieldTypes = new DataType[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                fieldTypes[i] = rowType.getTypeAt(i);
            }
            CompactedRowDeserializer deserializer = new CompactedRowDeserializer(fieldTypes);
            byte[] minRowData = new byte[minRowSize];
            inputView.readFully(minRowData, 0, minRowSize);
            minValues = CompactedRow.from(fieldTypes, minRowData, deserializer);
        }

        // Read max values CompactedRow
        CompactedRow maxValues = null;
        int maxRowSize = inputView.readInt();
        if (maxRowSize > 0) {
            DataType[] fieldTypes = new DataType[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                fieldTypes[i] = rowType.getTypeAt(i);
            }
            CompactedRowDeserializer deserializer = new CompactedRowDeserializer(fieldTypes);
            byte[] maxRowData = new byte[maxRowSize];
            inputView.readFully(maxRowData, 0, maxRowSize);
            maxValues = CompactedRow.from(fieldTypes, maxRowData, deserializer);
        }

        return new DefaultLogRecordBatchStatistics(minValues, maxValues, nullCounts);
    }
}
