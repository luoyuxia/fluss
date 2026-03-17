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
    public static final int ROW_ID_LENGTH = 8;

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
     * Encode the {@code row} with a {@code schemaId} and {@code rowId} to a byte array value for
     * deletion vector mode. The format is: [RowId (8 bytes)][schemaId (2 bytes)][BinaryRow].
     *
     * @param rowId the row id to encode
     * @param schemaId the schema id of the row
     * @param row the row to encode
     */
    public static byte[] encodeValueWithRowId(long rowId, short schemaId, BinaryRow row) {
        byte[] values = new byte[ROW_ID_LENGTH + SCHEMA_ID_LENGTH + row.getSizeInBytes()];
        UnsafeUtils.putLong(values, 0, rowId);
        UnsafeUtils.putShort(values, ROW_ID_LENGTH, schemaId);
        row.copyTo(values, ROW_ID_LENGTH + SCHEMA_ID_LENGTH);
        return values;
    }

    /**
     * Extract the row id from the value bytes in deletion vector mode.
     *
     * @param value the value bytes
     * @return the row id
     */
    public static long extractRowId(byte[] value) {
        return UnsafeUtils.getLong(value, 0);
    }

    /**
     * Extract the schema id from the value bytes in deletion vector mode. The schema id is located
     * after the 8-byte row id.
     *
     * @param value the value bytes
     * @return the schema id
     */
    public static short extractSchemaIdFromDvValue(byte[] value) {
        return UnsafeUtils.getShort(value, ROW_ID_LENGTH);
    }

    /**
     * Get the offset of the binary row data in the value bytes for deletion vector mode.
     *
     * @return the offset of the binary row data
     */
    public static int extractValueBytesOffset() {
        return ROW_ID_LENGTH + SCHEMA_ID_LENGTH;
    }
}
