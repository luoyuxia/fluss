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
 * Lake Deletion Vector that manages deleted row positions for files. Uses LakeDv CF with file ID as
 * key.
 */
public class LakeDv {
    private final DvRocksDB dvRocksDB;

    public LakeDv(DvRocksDB dvRocksDB) {
        this.dvRocksDB = Objects.requireNonNull(dvRocksDB, "dvRocksDB cannot be null");
    }

    /**
     * Encodes a file ID into a 4-byte big-endian array.
     *
     * @param fileId the file ID
     * @return the encoded byte array
     */
    private byte[] encodeFileId(int fileId) {
        return ByteBuffer.allocate(4).putInt(fileId).array();
    }

    /**
     * Decodes a byte array to a file ID.
     *
     * @param bytes the byte array
     * @return the file ID
     */
    private int decodeFileId(byte[] bytes) {
        if (bytes == null || bytes.length != 4) {
            throw new IllegalArgumentException("File ID bytes must be exactly 4 bytes");
        }
        return ByteBuffer.wrap(bytes).getInt();
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
     * Marks a row position as deleted for a given file.
     *
     * @param fileId the file ID
     * @param rowPosition the row position to mark as deleted
     */
    public void markDeleted(int fileId, int rowPosition) {
        byte[] key = encodeFileId(fileId);

        try {
            byte[] existingValue = dvRocksDB.get(dvRocksDB.getLakeDvCfHandle(), key);
            RoaringBitmap bitmap =
                    existingValue != null ? deserializeBitmap(existingValue) : new RoaringBitmap();

            bitmap.add(rowPosition);

            byte[] newValue = serializeBitmap(bitmap);
            dvRocksDB.put(dvRocksDB.getLakeDvCfHandle(), key, newValue);
        } catch (RocksDBException e) {
            throw new RuntimeException(
                    "Failed to mark row position as deleted for file: " + fileId, e);
        }
    }

    /**
     * Gets the deleted bitmap for a given file.
     *
     * @param fileId the file ID
     * @return the deleted bitmap (empty if not found)
     */
    public RoaringBitmap getDeletedBitmap(int fileId) {
        byte[] key = encodeFileId(fileId);
        try {
            byte[] value = dvRocksDB.get(dvRocksDB.getLakeDvCfHandle(), key);
            return value != null ? deserializeBitmap(value) : new RoaringBitmap();
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to get deleted bitmap for file: " + fileId, e);
        }
    }

    /**
     * Gets all entries in the lake deletion vector.
     *
     * @return a map of file ID to deleted bitmap
     */
    public Map<Integer, RoaringBitmap> getAllEntries() {
        Map<Integer, RoaringBitmap> entries = new HashMap<>();
        try (RocksIterator iterator = dvRocksDB.newIterator(dvRocksDB.getLakeDvCfHandle())) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                int fileId = decodeFileId(iterator.key());
                RoaringBitmap bitmap = deserializeBitmap(iterator.value());
                entries.put(fileId, bitmap);
                iterator.next();
            }
        }
        return entries;
    }

    /**
     * Creates a deep copy snapshot of all entries.
     *
     * @return a map of file ID to cloned deleted bitmap
     */
    public Map<Integer, RoaringBitmap> snapshot() {
        Map<Integer, RoaringBitmap> snapshot = new HashMap<>();
        try (RocksIterator iterator = dvRocksDB.newIterator(dvRocksDB.getLakeDvCfHandle())) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                int fileId = decodeFileId(iterator.key());
                RoaringBitmap bitmap = deserializeBitmap(iterator.value());
                snapshot.put(fileId, bitmap.clone());
                iterator.next();
            }
        }
        return snapshot;
    }

    /**
     * Deletes a file from the lake deletion vector.
     *
     * @param fileId the file ID to delete
     */
    public void deleteFile(int fileId) {
        byte[] key = encodeFileId(fileId);
        try {
            dvRocksDB.delete(dvRocksDB.getLakeDvCfHandle(), key);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to delete file: " + fileId, e);
        }
    }

    /**
     * Applies bitmap diff by computing current AND NOT snapshot for each file. If the result is
     * empty, the file entry is deleted.
     *
     * @param snapshotBitmap the snapshot bitmap to apply diff
     */
    public void applyBitmapDiff(Map<Integer, RoaringBitmap> snapshotBitmap) {
        for (Map.Entry<Integer, RoaringBitmap> entry : snapshotBitmap.entrySet()) {
            int fileId = entry.getKey();
            RoaringBitmap snapshot = entry.getValue();

            RoaringBitmap current = getDeletedBitmap(fileId);
            current.andNot(snapshot);

            if (current.isEmpty()) {
                deleteFile(fileId);
            } else {
                byte[] key = encodeFileId(fileId);
                try {
                    byte[] newValue = serializeBitmap(current);
                    dvRocksDB.put(dvRocksDB.getLakeDvCfHandle(), key, newValue);
                } catch (RocksDBException e) {
                    throw new RuntimeException(
                            "Failed to apply bitmap diff for file: " + fileId, e);
                }
            }
        }
    }
}
