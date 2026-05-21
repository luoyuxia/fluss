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

import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.utils.UnsafeUtils;

/** An encoder to encode {@link BinaryRow} with a schema id as value to be stored in kv store. */
public class ValueEncoder {

    public static final int SCHEMA_ID_LENGTH = 2;

    /**
     * Encode the {@code row} with a {@code schemaId} to a byte array value to be expected persisted
     * to kv store.
     *
     * @param schemaId the schema id of the row
     * @param row the row to encode
     */
    public static byte[] encodeValue(short schemaId, BinaryRow row) {
        byte[] values = new byte[SCHEMA_ID_LENGTH + row.getSizeInBytes()];
        UnsafeUtils.putShort(values, 0, schemaId);
        row.copyTo(values, SCHEMA_ID_LENGTH);
        return values;
    }

    /**
     * Encode the {@code row} with a {@code schemaId} and a {@code rowId} to a byte array value.
     * Used for DV-enabled tables. The format is: {@code [RowId (varint)][SchemaId (2B)][BinaryRow
     * (variable)]}.
     *
     * @param schemaId the schema id of the row
     * @param row the row to encode
     * @param rowId the RowId (log offset of the +I/+U record) to embed
     */
    public static byte[] encodeValueWithRowId(short schemaId, BinaryRow row, long rowId) {
        int varintLen = unsignedVarLongSize(rowId);
        byte[] values = new byte[varintLen + SCHEMA_ID_LENGTH + row.getSizeInBytes()];
        int offset = putUnsignedVarLong(values, 0, rowId);
        UnsafeUtils.putShort(values, offset, schemaId);
        row.copyTo(values, offset + SCHEMA_ID_LENGTH);
        return values;
    }

    /**
     * Extract the RowId from the beginning of a DV-enabled value byte array. The RowId is stored as
     * an unsigned varint prefix.
     */
    public static long extractRowId(byte[] valueBytes) {
        return getUnsignedVarLong(valueBytes, 0);
    }

    /**
     * Return the number of bytes occupied by the varint-encoded RowId at the start of the given
     * value byte array.
     */
    public static int rowIdVarIntSize(byte[] valueBytes) {
        int pos = 0;
        while ((valueBytes[pos] & 0x80) != 0) {
            pos++;
        }
        return pos + 1;
    }

    // ---- Unsigned varint (LEB128) encoding/decoding ----

    /** Compute the number of bytes needed to encode the given long as an unsigned varint. */
    public static int unsignedVarLongSize(long value) {
        int size = 1;
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    /**
     * Write the given long as an unsigned varint into the byte array at the given offset. Returns
     * the new offset after writing.
     */
    static int putUnsignedVarLong(byte[] buf, int offset, long value) {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            buf[offset++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[offset++] = (byte) (value & 0x7F);
        return offset;
    }

    /** Read an unsigned varint-encoded long from the byte array starting at the given offset. */
    static long getUnsignedVarLong(byte[] buf, int offset) {
        long result = 0;
        int shift = 0;
        byte b;
        do {
            b = buf[offset++];
            result |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return result;
    }
}
