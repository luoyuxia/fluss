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

    public FlussRecordAsPaimonRow(LogRecord logRecord) {
        this.logRecord = logRecord;
    }

    @Override
    public int getFieldCount() {
        return 0;
    }

    @Override
    public RowKind getRowKind() {
        return null;
    }

    @Override
    public void setRowKind(RowKind rowKind) {}

    @Override
    public boolean isNullAt(int i) {
        return false;
    }

    @Override
    public boolean getBoolean(int i) {
        return false;
    }

    @Override
    public byte getByte(int i) {
        return 0;
    }

    @Override
    public short getShort(int i) {
        return 0;
    }

    @Override
    public int getInt(int i) {
        return 0;
    }

    @Override
    public long getLong(int i) {
        return 0;
    }

    @Override
    public float getFloat(int i) {
        return 0;
    }

    @Override
    public double getDouble(int i) {
        return 0;
    }

    @Override
    public BinaryString getString(int i) {
        return null;
    }

    @Override
    public Decimal getDecimal(int i, int i1, int i2) {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int i, int i1) {
        return null;
    }

    @Override
    public byte[] getBinary(int i) {
        return new byte[0];
    }

    @Override
    public InternalArray getArray(int i) {
        return null;
    }

    @Override
    public InternalMap getMap(int i) {
        return null;
    }

    @Override
    public InternalRow getRow(int i, int i1) {
        return null;
    }
}
