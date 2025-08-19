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
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.compacted.CompactedRowDeserializer;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

/**
 * Byte view implementation of LogRecordBatchStatistics that provides zero-copy access to statistics
 * data without creating heap objects or copying data.
 */
public class ByteViewLogRecordBatchStatistics implements LogRecordBatchStatistics {

    private final MemorySegment segment;
    private final int position;
    private final int size;
    private final RowType rowType;
    private final DataType[] fieldTypes;
    private final int fieldCount;

    // Cached null counts for quick access
    private final Long[] nullCounts;

    // Offsets for min/max values in the byte array
    private final int minValuesOffset;
    private final int maxValuesOffset;
    private final int minValuesSize;
    private final int maxValuesSize;

    // Lazy-loaded deserializers
    private CompactedRowDeserializer minDeserializer;
    private CompactedRowDeserializer maxDeserializer;

    // Cached CompactedRow objects
    private CompactedRow cachedMinRow;
    private CompactedRow cachedMaxRow;

    public ByteViewLogRecordBatchStatistics(
            MemorySegment segment,
            int position,
            int size,
            RowType rowType,
            Long[] nullCounts,
            int minValuesOffset,
            int maxValuesOffset,
            int minValuesSize,
            int maxValuesSize) {
        this.segment = segment;
        this.position = position;
        this.size = size;
        this.rowType = rowType;
        this.fieldCount = rowType.getFieldCount();
        this.fieldTypes = new DataType[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            this.fieldTypes[i] = rowType.getTypeAt(i);
        }
        this.nullCounts = nullCounts;
        this.minValuesOffset = minValuesOffset;
        this.maxValuesOffset = maxValuesOffset;
        this.minValuesSize = minValuesSize;
        this.maxValuesSize = maxValuesSize;
    }

    @Override
    public InternalRow getMinValues() {
        if (minValuesSize <= 0) {
            return null;
        }

        // Return cached row if already created
        if (cachedMinRow != null) {
            return cachedMinRow;
        }

        // Create deserializer if needed
        if (minDeserializer == null) {
            minDeserializer = new CompactedRowDeserializer(fieldTypes);
        }

        // Create and cache the CompactedRow
        cachedMinRow = new CompactedRow(fieldCount, minDeserializer);
        cachedMinRow.pointTo(segment, position + minValuesOffset, minValuesSize);
        return cachedMinRow;
    }

    @Override
    public InternalRow getMaxValues() {
        if (maxValuesSize <= 0) {
            return null;
        }

        // Return cached row if already created
        if (cachedMaxRow != null) {
            return cachedMaxRow;
        }

        // Create deserializer if needed
        if (maxDeserializer == null) {
            maxDeserializer = new CompactedRowDeserializer(fieldTypes);
        }

        // Create and cache the CompactedRow
        cachedMaxRow = new CompactedRow(fieldCount, maxDeserializer);
        cachedMaxRow.pointTo(segment, position + maxValuesOffset, maxValuesSize);
        return cachedMaxRow;
    }

    @Override
    public Long[] getNullCounts() {
        return nullCounts;
    }

    /**
     * Get the null count for a specific field.
     *
     * @param fieldIndex The field index
     * @return The null count for the field
     */
    public long getNullCount(int fieldIndex) {
        return nullCounts[fieldIndex];
    }

    /**
     * Get the underlying memory segment.
     *
     * @return The memory segment
     */
    public MemorySegment getSegment() {
        return segment;
    }

    /**
     * Get the position in the memory segment.
     *
     * @return The position
     */
    public int getPosition() {
        return position;
    }

    /**
     * Get the total size of the statistics data.
     *
     * @return The size in bytes
     */
    public int getSize() {
        return size;
    }

    /**
     * Check if minimum values are available.
     *
     * @return true if minimum values are available
     */
    public boolean hasMinValues() {
        return minValuesSize > 0;
    }

    /**
     * Check if maximum values are available.
     *
     * @return true if maximum values are available
     */
    public boolean hasMaxValues() {
        return maxValuesSize > 0;
    }

    /**
     * Get the field count.
     *
     * @return The number of fields
     */
    public int getFieldCount() {
        return fieldCount;
    }

    /**
     * Get the field type at the specified index.
     *
     * @param index The field index
     * @return The field type
     */
    public DataType getFieldType(int index) {
        return fieldTypes[index];
    }

    /**
     * Get the row type for this statistics.
     *
     * @return The row type
     */
    public RowType getRowType() {
        return rowType;
    }

    @Override
    public String toString() {
        return "ByteViewLogRecordBatchStatistics{"
                + "fieldCount="
                + fieldCount
                + ", hasMinValues="
                + hasMinValues()
                + ", hasMaxValues="
                + hasMaxValues()
                + ", size="
                + size
                + '}';
    }
}
