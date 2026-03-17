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

import org.roaringbitmap.RoaringBitmap;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Log Deletion Vector that manages deleted offsets in log ranges. Uses LogDv CF with range-based
 * key partitioning.
 */
public class LogDv {
    private static final int RANGE_SIZE = 1000;
    private final DvRocksDB dvRocksDB;

    public LogDv(DvRocksDB dvRocksDB) {
        this.dvRocksDB = Objects.requireNonNull(dvRocksDB, "dvRocksDB cannot be null");
    }

    /**
     * Encodes a range key into an 8-byte big-endian array.
     *
     * @param rangeKey the range key
     * @return the encoded byte array
     */
    private byte[] encodeRangeKey(long rangeKey) {
        return ByteBuffer.allocate(8).putLong(rangeKey).array();
    }

    /**
     * Decodes a byte array to a range key.
     *
     * @param bytes the byte array
     * @return the range key
     */
    private long decodeRangeKey(byte[] bytes) {
        if (bytes == null || bytes.length != 8) {
            throw new IllegalArgumentException("Range key bytes must be exactly 8 bytes");
        }
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * Serializes a RoaringBitmap to a byte array.
     *
     * @param bitmap the bitmap to serialize
     * @return the serialized byte array
     */
    private byte[] serializeBitmap(RoaringBitmap bitmap) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            bitmap.serialize(new DataOutputStream(baos));
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize bitmap", e);
        }
    }

    /**
     * Deserializes a byte array to a RoaringBitmap.
     *
     * @param bytes the byte array
     * @return the deserialized bitmap
     */
    private RoaringBitmap deserializeBitmap(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new RoaringBitmap();
        }
        try {
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
            return bitmap;
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize bitmap", e);
        }
    }

    /**
     * Marks an offset as deleted.
     *
     * @param offset the offset to mark as deleted
     */
    public void markDeleted(long offset) {
        long rangeKey = offset / RANGE_SIZE;
        byte[] key = encodeRangeKey(rangeKey);

        try {
            byte[] existingValue = dvRocksDB.get(dvRocksDB.getLogDvCfHandle(), key);
            RoaringBitmap bitmap =
                    existingValue != null ? deserializeBitmap(existingValue) : new RoaringBitmap();

            long rangeStartOffset = rangeKey * RANGE_SIZE;
            int bitmapPosition = (int) (offset - rangeStartOffset);
            bitmap.add(bitmapPosition);

            byte[] newValue = serializeBitmap(bitmap);
            dvRocksDB.put(dvRocksDB.getLogDvCfHandle(), key, newValue);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to mark offset as deleted: " + offset, e);
        }
    }

    /**
     * Gets deleted bitmaps for the given offset range.
     *
     * @param startOffset the start offset (inclusive)
     * @param endOffset the end offset (exclusive)
     * @return a map of base offset to deleted bitmap
     */
    public Map<Long, RoaringBitmap> getDeletedBitmap(long startOffset, long endOffset) {
        Map<Long, RoaringBitmap> result = new HashMap<>();

        long startRangeKey = startOffset / RANGE_SIZE;
        long endRangeKey = (endOffset - 1) / RANGE_SIZE;

        for (long rangeKey = startRangeKey; rangeKey <= endRangeKey; rangeKey++) {
            byte[] key = encodeRangeKey(rangeKey);
            try {
                byte[] value = dvRocksDB.get(dvRocksDB.getLogDvCfHandle(), key);
                if (value != null && value.length > 0) {
                    RoaringBitmap bitmap = deserializeBitmap(value);
                    long baseOffset = rangeKey * RANGE_SIZE;
                    result.put(baseOffset, bitmap);
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(
                        "Failed to get deleted bitmap for range: " + rangeKey, e);
            }
        }

        return result;
    }

    /**
     * Cleans up ranges whose end offset is less than the given start log offset.
     *
     * @param startLogOffset the start log offset
     */
    public void cleanup(long startLogOffset) {
        long maxRangeKeyToCleanup = (startLogOffset - 1) / RANGE_SIZE;

        try (RocksIterator iterator = dvRocksDB.newIterator(dvRocksDB.getLogDvCfHandle())) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                long rangeKey = decodeRangeKey(iterator.key());
                long rangeEndOffset = (rangeKey + 1) * RANGE_SIZE;

                if (rangeEndOffset < startLogOffset) {
                    byte[] key = iterator.key();
                    dvRocksDB.delete(dvRocksDB.getLogDvCfHandle(), key);
                    iterator.seekToFirst(); // Reset after deletion
                } else {
                    iterator.next();
                }
            }
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to cleanup log DV", e);
        }
    }
}
