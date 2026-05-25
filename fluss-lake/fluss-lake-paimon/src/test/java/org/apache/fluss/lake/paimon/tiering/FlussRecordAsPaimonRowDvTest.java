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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;

import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.ChangeType.INSERT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlussRecordAsPaimonRow} with DV enabled. */
class FlussRecordAsPaimonRowDvTest {

    @Test
    void testRowidFieldPopulated() {
        int tableBucket = 0;
        // 1 business column + 3 system columns (__bucket, __offset, __timestamp) + __rowid
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.BooleanType(),
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3),
                        new org.apache.paimon.types.BigIntType());

        FlussRecordAsPaimonRow row = new FlussRecordAsPaimonRow(tableBucket, tableRowType, true);
        long logOffset = 42L;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, true);
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        row.setFlussRecord(logRecord);

        // __rowid is the 5th field (index 4), should return logOffset
        assertThat(row.getLong(4)).isEqualTo(logOffset);
        assertThat(row.isNullAt(4)).isFalse();
    }

    @Test
    void testFieldCountWithDv() {
        // 2 business columns + 3 system columns + 1 __rowid = 6
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.VarCharType(),
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3),
                        new org.apache.paimon.types.BigIntType());

        FlussRecordAsPaimonRow row = new FlussRecordAsPaimonRow(0, tableRowType, true);
        long logOffset = 10L;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 1);
        genericRow.setField(1, null);
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        row.setFlussRecord(logRecord);

        assertThat(row.getFieldCount()).isEqualTo(6);
    }

    @Test
    void testBusinessFieldCountWithDv() {
        // 3 business columns + 3 system columns + 1 __rowid = 7
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.VarCharType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3),
                        new org.apache.paimon.types.BigIntType());

        FlussRecordAsPaimonRow row = new FlussRecordAsPaimonRow(0, tableRowType, true);
        long logOffset = 99L;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(3);
        genericRow.setField(0, 1);
        genericRow.setField(1, null);
        genericRow.setField(2, 100L);
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        row.setFlussRecord(logRecord);

        // System columns at indices 3, 4, 5 and __rowid at 6
        assertThat(row.getInt(3)).isEqualTo(0); // __bucket
        assertThat(row.getLong(4)).isEqualTo(logOffset); // __offset
        assertThat(row.getLong(6)).isEqualTo(logOffset); // __rowid
    }

    @Test
    void testBackwardCompatNonDv() {
        // Verify the 2-arg constructor works as before
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.BooleanType(),
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow row = new FlussRecordAsPaimonRow(0, tableRowType);
        long logOffset = 7L;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, true);
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        row.setFlussRecord(logRecord);

        assertThat(row.getFieldCount()).isEqualTo(4);
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getInt(1)).isEqualTo(0); // __bucket
        assertThat(row.getLong(2)).isEqualTo(logOffset); // __offset
    }
}
