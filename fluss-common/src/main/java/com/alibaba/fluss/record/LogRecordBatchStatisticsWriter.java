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

import com.alibaba.fluss.memory.OutputView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.compacted.CompactedRowWriter;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.io.IOException;

import static com.alibaba.fluss.record.LogRecordBatchFormat.STATISTICS_VERSION;

/**
 * Writer for LogRecordBatchStatistics that writes statistics directly to memory without creating
 * intermediate heap objects.
 */
public class LogRecordBatchStatisticsWriter {

    private final RowType rowType;
    private final int fieldCount;
    private final DataType[] fieldTypes;
    private final CompactedRowWriter.FieldWriter[] fieldWriters;

    public LogRecordBatchStatisticsWriter(RowType rowType) {
        this.rowType = rowType;
        this.fieldCount = rowType.getFieldCount();
        this.fieldTypes = new DataType[fieldCount];
        this.fieldWriters = new CompactedRowWriter.FieldWriter[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            this.fieldTypes[i] = rowType.getTypeAt(i);
            this.fieldWriters[i] = CompactedRowWriter.createFieldWriter(fieldTypes[i]);
        }
    }

    /**
     * Write statistics to an OutputView. This method handles cross-segment scenarios safely by
     * using OutputView's automatic segment management.
     *
     * @param minValues The minimum values as InternalRow, can be null
     * @param maxValues The maximum values as InternalRow, can be null
     * @param nullCounts The null counts array
     * @param outputView The target output view
     * @return The number of bytes written
     * @throws IOException If writing fails
     */
    public int writeStatistics(
            InternalRow minValues, InternalRow maxValues, Long[] nullCounts, OutputView outputView)
            throws IOException {
        // Calculate the expected size of statistics data
        int expectedSize = calculateStatisticsSize(minValues, maxValues, nullCounts);

        // Write version
        outputView.writeByte(STATISTICS_VERSION);

        // Write field count
        outputView.writeShort(fieldCount);

        // Write null counts array
        for (int i = 0; i < fieldCount; i++) {
            long nullCount = nullCounts != null ? nullCounts[i] : 0L;
            outputView.writeLong(nullCount);
        }

        // Write min values
        if (minValues != null) {
            CompactedRow minCompactedRow = convertToCompactedRow(minValues);
            int minRowSize = minCompactedRow.getSizeInBytes();
            outputView.writeInt(minRowSize);
            outputView.write(
                    minCompactedRow.getSegment().getHeapMemory(),
                    minCompactedRow.getOffset(),
                    minRowSize);
        } else {
            outputView.writeInt(0);
        }

        // Write max values
        if (maxValues != null) {
            CompactedRow maxCompactedRow = convertToCompactedRow(maxValues);
            int maxRowSize = maxCompactedRow.getSizeInBytes();
            outputView.writeInt(maxRowSize);
            outputView.write(
                    maxCompactedRow.getSegment().getHeapMemory(),
                    maxCompactedRow.getOffset(),
                    maxRowSize);
        } else {
            outputView.writeInt(0);
        }

        // Return the calculated size for all OutputView implementations
        return expectedSize;
    }

    /**
     * Calculate the expected size of statistics data in bytes.
     *
     * @param minValues The minimum values
     * @param maxValues The maximum values
     * @param nullCounts The null counts
     * @return The expected size in bytes
     */
    private int calculateStatisticsSize(
            InternalRow minValues, InternalRow maxValues, Long[] nullCounts) {
        int size = 0;

        // Version (1 byte)
        size += 1;

        // Field count (2 bytes)
        size += 2;

        // Null counts array (8 bytes per field)
        size += fieldCount * 8;

        // Min values size (4 bytes) + min values data
        size += 4;
        if (minValues != null) {
            // If it's already a CompactedRow, get size directly
            if (minValues instanceof CompactedRow) {
                size += ((CompactedRow) minValues).getSizeInBytes();
            } else {
                // Estimate size based on field count and types
                size += estimateRowSize(minValues);
            }
        }

        // Max values size (4 bytes) + max values data
        size += 4;
        if (maxValues != null) {
            // If it's already a CompactedRow, get size directly
            if (maxValues instanceof CompactedRow) {
                size += ((CompactedRow) maxValues).getSizeInBytes();
            } else {
                // Estimate size based on field count and types
                size += estimateRowSize(maxValues);
            }
        }

        return size;
    }

    /**
     * Estimate the size of a row in bytes.
     *
     * @param row The row to estimate
     * @return Estimated size in bytes
     */
    private int estimateRowSize(InternalRow row) {
        int size = 0;
        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                size += 1; // null flag
            } else {
                DataType fieldType = rowType.getTypeAt(i);
                size += estimateFieldSize(fieldType, row, i);
            }
        }
        return size;
    }

    /**
     * Estimate the size of a field in bytes.
     *
     * @param fieldType The field type
     * @param row The row containing the field
     * @param fieldIndex The field index
     * @return Estimated size in bytes
     */
    private int estimateFieldSize(DataType fieldType, InternalRow row, int fieldIndex) {
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return 1;
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
                return 4;
            case BIGINT:
                return 8;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 8;
            case STRING:
                // Estimate string size (this is approximate)
                return 4 + row.getString(fieldIndex).getSizeInBytes();
            case DECIMAL:
                return 16; // Approximate size for decimal
            default:
                return 8; // Default estimate
        }
    }

    /**
     * Convert an InternalRow to CompactedRow format. If the input is already a CompactedRow, it
     * will be returned directly. Otherwise, it will be converted to CompactedRow format.
     *
     * @param row The row to convert
     * @return The CompactedRow
     */
    private CompactedRow convertToCompactedRow(InternalRow row) {
        // If the input is already a CompactedRow, return it directly
        if (row instanceof CompactedRow) {
            return (CompactedRow) row;
        }

        // Otherwise, convert to CompactedRow format
        CompactedRowWriter writer = new CompactedRowWriter(fieldCount);
        writer.reset();

        for (int i = 0; i < fieldCount; i++) {
            if (row.isNullAt(i)) {
                writer.setNullAt(i);
            } else {
                fieldWriters[i].writeField(writer, i, row);
            }
        }

        CompactedRow compactedRow =
                new CompactedRow(fieldCount, null); // Deserializer not needed for writing
        compactedRow.pointTo(writer.segment(), 0, writer.position());
        return compactedRow;
    }

    /**
     * Get the row type used by this writer.
     *
     * @return The row type
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * Get the field count.
     *
     * @return The number of fields
     */
    public int getFieldCount() {
        return fieldCount;
    }
}
