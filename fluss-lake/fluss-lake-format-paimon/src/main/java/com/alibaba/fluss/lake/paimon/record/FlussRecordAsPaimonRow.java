/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.record;

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.TimestampLtz;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

/** Fluss record as paimon row. */
public class FlussRecordAsPaimonRow implements InternalRow {

    private final LogRecord logRecord;
    private final int rowFieldCount;
    private final com.alibaba.fluss.row.InternalRow internalRow;

    public FlussRecordAsPaimonRow(LogRecord logRecord) {
        this.logRecord = logRecord;
        this.internalRow = logRecord.getRow();
        this.rowFieldCount = internalRow.getFieldCount();
    }

    @Override
    public int getFieldCount() {
        // plus two fields: offset, timestamp
        return rowFieldCount + 2;
    }

    @Override
    public RowKind getRowKind() {
        switch (logRecord.getChangeType()) {
            case APPEND_ONLY:
            case INSERT:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new IllegalArgumentException(
                        "Unsupported change type: " + logRecord.getChangeType());
        }
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        // do nothing
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < rowFieldCount) {
            return internalRow.isNullAt(pos);
        }
        // is the last two fields: offset, timestamp which are never null
        return false;
    }

    @Override
    public boolean getBoolean(int pos) {
        return internalRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return internalRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return internalRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return internalRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        // if it points to the last two fields
        if (pos == getFieldCount() - 1) {
            return logRecord.timestamp();
        } else if (pos == getFieldCount() - 2) {
            return logRecord.logOffset();
        }
        //  the origin RowData
        return internalRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return internalRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return internalRow.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(internalRow.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        com.alibaba.fluss.row.Decimal flussDecimal = internalRow.getDecimal(pos, precision, scale);
        if (flussDecimal.isCompact()) {
            return Decimal.fromUnscaledLong(flussDecimal.toUnscaledLong(), precision, scale);
        } else {
            return Decimal.fromBigDecimal(flussDecimal.toBigDecimal(), precision, scale);
        }
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        // it's timestamp field
        if (pos == getFieldCount() - 1) {
            return Timestamp.fromEpochMillis(logRecord.timestamp());
        }
        if (TimestampLtz.isCompact(precision)) {
            return Timestamp.fromEpochMillis(
                    internalRow.getTimestampLtz(pos, precision).getEpochMillisecond());
        } else {
            TimestampLtz timestampLtz = internalRow.getTimestampLtz(pos, precision);
            return Timestamp.fromEpochMillis(
                    timestampLtz.getEpochMillisecond(), timestampLtz.getNanoOfMillisecond());
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        return internalRow.getBytes(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException(
                "getArray is not support for Fluss record currently.");
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException(
                "getMap is not support for Fluss record currently.");
    }

    @Override
    public InternalRow getRow(int pos, int pos1) {
        throw new UnsupportedOperationException(
                "getRow is not support for Fluss record currently.");
    }
}
