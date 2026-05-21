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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.MurmurHashUtils;

import java.io.IOException;

import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_LENGTH;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is an immutable log record and can be directly persisted. The schema is as follows:
 *
 * <ul>
 *   <li>Length => int32
 *   <li>Attributes => Int8
 *   <li>Value => {@link InternalRow}
 * </ul>
 *
 * <p>The current record attributes are depicted below:
 *
 * <p>----------- | ChangeType (0-3) | Unused (4-7) |---------------
 *
 * <p>The offset compute the difference relative to the base offset and of the batch that this
 * record is contained in.
 *
 * @since 0.1
 */
@PublicEvolving
public class IndexedLogRecord implements LogRecord {

    private static final int ATTRIBUTES_LENGTH = 1;

    private final long logOffset;
    private final long timestamp;
    private final DataType[] fieldTypes;

    private MemorySegment segment;
    private int offset;
    private int sizeInBytes;

    // DV support: RowId and the number of bytes consumed by its varint encoding
    private long rowId = NO_ROW_ID;
    private int rowIdSize = 0;

    IndexedLogRecord(long logOffset, long timestamp, DataType[] fieldTypes) {
        this.logOffset = logOffset;
        this.fieldTypes = fieldTypes;
        this.timestamp = timestamp;
    }

    void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        this.segment = segment;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexedLogRecord that = (IndexedLogRecord) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
    }

    @Override
    public long logOffset() {
        return logOffset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public ChangeType getChangeType() {
        byte attributes = segment.get(offset + LENGTH_LENGTH);
        return ChangeType.fromByteValue(attributes);
    }

    @Override
    public long getRowId() {
        return rowId;
    }

    @Override
    public InternalRow getRow() {
        int rowOffset = LENGTH_LENGTH + ATTRIBUTES_LENGTH + rowIdSize;
        return LogRecord.deserializeInternalRow(
                sizeInBytes - rowOffset,
                segment,
                offset + rowOffset,
                fieldTypes,
                LogFormat.INDEXED);
    }

    /** Write the record to input `target` and return its size. */
    public static int writeTo(OutputView outputView, ChangeType changeType, IndexedRow row)
            throws IOException {
        int sizeInBytes = calculateSizeInBytes(row);

        // TODO using varint instead int to reduce storage size.
        // write record total bytes size.
        outputView.writeInt(sizeInBytes);

        // write attributes.
        outputView.writeByte(changeType.toByteValue());

        // write internal row.
        serializeInternalRow(outputView, row);

        return sizeInBytes + LENGTH_LENGTH;
    }

    /** Write the record with a RowId (for DV-enabled tables) and return its size. */
    public static int writeTo(
            OutputView outputView, ChangeType changeType, IndexedRow row, long rowId)
            throws IOException {
        int varintSize = ValueEncoder.unsignedVarLongSize(rowId);
        int sizeInBytes = ATTRIBUTES_LENGTH + varintSize + row.getSizeInBytes();

        // write record total bytes size.
        outputView.writeInt(sizeInBytes);

        // write attributes.
        outputView.writeByte(changeType.toByteValue());

        // write varint RowId.
        writeUnsignedVarLong(outputView, rowId);

        // write internal row.
        serializeInternalRow(outputView, row);

        return sizeInBytes + LENGTH_LENGTH;
    }

    public static IndexedLogRecord readFrom(
            MemorySegment segment,
            int position,
            long logOffset,
            long logTimestamp,
            DataType[] colTypes) {
        return readFrom(segment, position, logOffset, logTimestamp, colTypes, false);
    }

    public static IndexedLogRecord readFrom(
            MemorySegment segment,
            int position,
            long logOffset,
            long logTimestamp,
            DataType[] colTypes,
            boolean dvEnabled) {
        int sizeInBytes = segment.getInt(position);
        IndexedLogRecord logRecord = new IndexedLogRecord(logOffset, logTimestamp, colTypes);
        logRecord.pointTo(segment, position, sizeInBytes + LENGTH_LENGTH);
        if (dvEnabled) {
            int rowIdOffset = position + LENGTH_LENGTH + ATTRIBUTES_LENGTH;
            logRecord.rowId = readUnsignedVarLong(segment, rowIdOffset);
            logRecord.rowIdSize = varintSizeFromSegment(segment, rowIdOffset);
        }
        return logRecord;
    }

    public static int sizeOf(BinaryRow row) {
        int sizeInBytes = calculateSizeInBytes(row);
        return sizeInBytes + LENGTH_LENGTH;
    }

    private static int calculateSizeInBytes(BinaryRow row) {
        int size = 1; // always one byte for attributes
        size += row.getSizeInBytes();
        return size;
    }

    /** Write an unsigned varint long to the output view. */
    static void writeUnsignedVarLong(OutputView outputView, long value) throws IOException {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            outputView.writeByte((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        outputView.writeByte((int) (value & 0x7F));
    }

    /** Read an unsigned varint long from a MemorySegment at the given position. */
    static long readUnsignedVarLong(MemorySegment segment, int position) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = segment.get(position++);
            result |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }

    /** Return the number of bytes consumed by a varint at the given position in a MemorySegment. */
    static int varintSizeFromSegment(MemorySegment segment, int position) {
        int size = 0;
        while ((segment.get(position + size) & 0x80) != 0) {
            size++;
        }
        return size + 1;
    }

    private static void serializeInternalRow(OutputView outputView, IndexedRow row)
            throws IOException {
        IndexedRowWriter.serializeIndexedRow(row, outputView);
    }
}
