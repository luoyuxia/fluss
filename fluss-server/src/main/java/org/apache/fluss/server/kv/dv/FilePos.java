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

package org.apache.fluss.server.kv.dv;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A value object that combines file_id (high 4 bytes) and row_position (low 4 bytes) into an 8-byte
 * long.
 */
public class FilePos {
    private final long value;

    private FilePos(long value) {
        this.value = value;
    }

    /**
     * Creates a FilePos from file_id and row_position.
     *
     * @param fileId the file ID (high 4 bytes)
     * @param rowPosition the row position within the file (low 4 bytes)
     * @return a new FilePos instance
     */
    public static FilePos of(int fileId, int rowPosition) {
        long value = ((long) fileId << 32) | (rowPosition & 0xFFFFFFFFL);
        return new FilePos(value);
    }

    /**
     * Creates a FilePos from a long value.
     *
     * @param value the long value containing file_id and row_position
     * @return a new FilePos instance
     */
    public static FilePos fromLong(long value) {
        return new FilePos(value);
    }

    /**
     * Creates a FilePos from a byte array.
     *
     * @param bytes the byte array (must be 8 bytes)
     * @return a new FilePos instance
     */
    public static FilePos fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length != 8) {
            throw new IllegalArgumentException("Byte array must be exactly 8 bytes");
        }
        long value = ByteBuffer.wrap(bytes).getLong();
        return new FilePos(value);
    }

    /**
     * Gets the file ID from this FilePos.
     *
     * @return the file ID (high 4 bytes)
     */
    public int getFileId() {
        return (int) (value >>> 32);
    }

    /**
     * Gets the row position from this FilePos.
     *
     * @return the row position (low 4 bytes)
     */
    public int getRowPosition() {
        return (int) value;
    }

    /**
     * Converts this FilePos to a long value.
     *
     * @return the long value
     */
    public long toLong() {
        return value;
    }

    /**
     * Converts this FilePos to a byte array.
     *
     * @return a byte array of 8 bytes
     */
    public byte[] toBytes() {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FilePos filePos = (FilePos) o;
        return value == filePos.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "FilePos{fileId=" + getFileId() + ", rowPosition=" + getRowPosition() + "}";
    }
}
