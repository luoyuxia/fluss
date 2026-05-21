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

package org.apache.fluss.row.encode;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowDeserializer;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ValueEncoder} and {@link ValueDecoder} RowId methods. */
class ValueEncoderTest {

    private static final short SCHEMA_ID = 1;
    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField("a", DataTypes.INT()),
                            new DataField("b", DataTypes.STRING())));
    private static final DataType[] FIELD_TYPES = ROW_TYPE.getChildren().toArray(new DataType[0]);
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .build();

    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, 127L, 128L, 16383L, 16384L, 2097151L, 268435455L, Long.MAX_VALUE})
    void testEncodeAndExtractRowId(long rowId) {
        BinaryRow row = createTestRow(42, "hello");
        byte[] encoded = ValueEncoder.encodeValueWithRowId(SCHEMA_ID, row, rowId);

        long extractedRowId = ValueEncoder.extractRowId(encoded);
        assertThat(extractedRowId).isEqualTo(rowId);
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, 127L, 128L, 16383L, 16384L, 2097151L, 268435455L, Long.MAX_VALUE})
    void testRowIdVarIntSize(long rowId) {
        BinaryRow row = createTestRow(42, "hello");
        byte[] encoded = ValueEncoder.encodeValueWithRowId(SCHEMA_ID, row, rowId);

        int varintSize = ValueEncoder.rowIdVarIntSize(encoded);
        assertThat(varintSize).isEqualTo(ValueEncoder.unsignedVarLongSize(rowId));
    }

    @Test
    void testUnsignedVarLongSize() {
        // 1 byte: 0 ~ 127
        assertThat(ValueEncoder.unsignedVarLongSize(0L)).isEqualTo(1);
        assertThat(ValueEncoder.unsignedVarLongSize(127L)).isEqualTo(1);
        // 2 bytes: 128 ~ 16383
        assertThat(ValueEncoder.unsignedVarLongSize(128L)).isEqualTo(2);
        assertThat(ValueEncoder.unsignedVarLongSize(16383L)).isEqualTo(2);
        // 3 bytes: 16384 ~ 2097151
        assertThat(ValueEncoder.unsignedVarLongSize(16384L)).isEqualTo(3);
        assertThat(ValueEncoder.unsignedVarLongSize(2097151L)).isEqualTo(3);
        // 4 bytes
        assertThat(ValueEncoder.unsignedVarLongSize(2097152L)).isEqualTo(4);
        assertThat(ValueEncoder.unsignedVarLongSize(268435455L)).isEqualTo(4);
        // 5 bytes
        assertThat(ValueEncoder.unsignedVarLongSize(268435456L)).isEqualTo(5);
        // max value needs 9 bytes
        assertThat(ValueEncoder.unsignedVarLongSize(Long.MAX_VALUE)).isEqualTo(9);
    }

    @Test
    void testEncodeDecodeRoundTripWithValueDecoder() {
        BinaryRow row = createTestRow(42, "hello");
        long rowId = 12345L;
        byte[] encoded = ValueEncoder.encodeValueWithRowId(SCHEMA_ID, row, rowId);

        ValueDecoder decoder =
                new ValueDecoder(
                        new TestingSchemaGetter((int) SCHEMA_ID, SCHEMA), KvFormat.COMPACTED);
        BinaryValue decoded = decoder.decodeValueSkippingRowId(encoded);

        assertThat(decoded.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(decoded.row.getInt(0)).isEqualTo(42);
        assertThat(decoded.row.getString(1).toString()).isEqualTo("hello");
    }

    @Test
    void testBinaryValueEncodeValueWithRowId() {
        CompactedRow row = createTestRow(100, "world");
        BinaryValue binaryValue = new BinaryValue(SCHEMA_ID, row);
        long rowId = 999L;

        byte[] encoded = binaryValue.encodeValueWithRowId(rowId);
        long extractedRowId = ValueEncoder.extractRowId(encoded);
        assertThat(extractedRowId).isEqualTo(rowId);

        ValueDecoder decoder =
                new ValueDecoder(
                        new TestingSchemaGetter((int) SCHEMA_ID, SCHEMA), KvFormat.COMPACTED);
        BinaryValue decoded = decoder.decodeValueSkippingRowId(encoded);
        assertThat(decoded.row.getInt(0)).isEqualTo(100);
        assertThat(decoded.row.getString(1).toString()).isEqualTo("world");
    }

    @Test
    void testOldFormatStillWorks() {
        BinaryRow row = createTestRow(10, "abc");
        byte[] oldEncoded = ValueEncoder.encodeValue(SCHEMA_ID, row);

        ValueDecoder decoder =
                new ValueDecoder(
                        new TestingSchemaGetter((int) SCHEMA_ID, SCHEMA), KvFormat.COMPACTED);
        BinaryValue decoded = decoder.decodeValue(oldEncoded);

        assertThat(decoded.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(decoded.row.getInt(0)).isEqualTo(10);
        assertThat(decoded.row.getString(1).toString()).isEqualTo("abc");
    }

    @Test
    void testDvFormatLargerThanOldFormat() {
        BinaryRow row = createTestRow(42, "hello");
        byte[] oldEncoded = ValueEncoder.encodeValue(SCHEMA_ID, row);
        byte[] dvEncoded = ValueEncoder.encodeValueWithRowId(SCHEMA_ID, row, 0L);
        // DV format has at least 1 extra byte for varint RowId=0
        assertThat(dvEncoded.length).isGreaterThan(oldEncoded.length);
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 1L, 127L, 128L, 16383L, 16384L, 2097151L, 268435455L, Long.MAX_VALUE})
    void testEncodeDecodeRoundTripForVariousRowIds(long rowId) {
        BinaryRow row = createTestRow(7, "test");
        byte[] encoded = ValueEncoder.encodeValueWithRowId(SCHEMA_ID, row, rowId);

        // verify RowId
        assertThat(ValueEncoder.extractRowId(encoded)).isEqualTo(rowId);

        // verify row data
        ValueDecoder decoder =
                new ValueDecoder(
                        new TestingSchemaGetter((int) SCHEMA_ID, SCHEMA), KvFormat.COMPACTED);
        BinaryValue decoded = decoder.decodeValueSkippingRowId(encoded);
        assertThat(decoded.schemaId).isEqualTo(SCHEMA_ID);
        assertThat(decoded.row.getInt(0)).isEqualTo(7);
        assertThat(decoded.row.getString(1).toString()).isEqualTo("test");
    }

    private static CompactedRow createTestRow(int intVal, String strVal) {
        CompactedRowWriter writer = new CompactedRowWriter(FIELD_TYPES.length);
        writer.writeInt(intVal);
        writer.writeString(org.apache.fluss.row.BinaryString.fromString(strVal));
        byte[] bytes = writer.toBytes();
        return CompactedRow.from(FIELD_TYPES, bytes, new CompactedRowDeserializer(FIELD_TYPES));
    }
}
