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

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * File dictionary that encodes file paths to file IDs and vice versa. Uses bidirectional mapping in
 * the FileDict CF.
 */
public class FileDict {
    private final DvRocksDB dvRocksDB;
    private int nextFileId;

    private static final byte FORWARD_PREFIX = 'F';
    private static final byte REVERSE_PREFIX = 'R';

    public FileDict(DvRocksDB dvRocksDB) {
        this.dvRocksDB = Objects.requireNonNull(dvRocksDB, "dvRocksDB cannot be null");
        this.nextFileId = initializeNextFileId();
    }

    /**
     * Initializes nextFileId by scanning the DB to find the maximum file ID.
     *
     * @return the next available file ID
     */
    private int initializeNextFileId() {
        int maxFileId = -1;
        try (RocksIterator iterator = dvRocksDB.newIterator(dvRocksDB.getFileDictCfHandle())) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (key.length > 0 && key[0] == REVERSE_PREFIX) {
                    int fileId = ByteBuffer.wrap(key, 1, 4).getInt();
                    if (fileId > maxFileId) {
                        maxFileId = fileId;
                    }
                }
                iterator.next();
            }
        }
        return maxFileId + 1;
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
     * Decodes a 4-byte big-endian array to a file ID.
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
     * Gets or creates a file ID for the given file path.
     *
     * @param filePath the file path
     * @return the file ID
     */
    public int getOrCreateFileId(String filePath) {
        Objects.requireNonNull(filePath, "filePath cannot be null");
        byte[] filePathBytes = filePath.getBytes(StandardCharsets.UTF_8);

        // Try to get existing file ID
        byte[] forwardKey = new byte[1 + filePathBytes.length];
        forwardKey[0] = FORWARD_PREFIX;
        System.arraycopy(filePathBytes, 0, forwardKey, 1, filePathBytes.length);

        try {
            byte[] existingValue = dvRocksDB.get(dvRocksDB.getFileDictCfHandle(), forwardKey);
            if (existingValue != null) {
                return decodeFileId(existingValue);
            }

            // Create new file ID
            int fileId = nextFileId++;
            byte[] fileIdBytes = encodeFileId(fileId);

            // Store forward mapping
            dvRocksDB.put(dvRocksDB.getFileDictCfHandle(), forwardKey, fileIdBytes);

            // Store reverse mapping
            byte[] reverseKey = new byte[1 + fileIdBytes.length];
            reverseKey[0] = REVERSE_PREFIX;
            System.arraycopy(fileIdBytes, 0, reverseKey, 1, fileIdBytes.length);
            dvRocksDB.put(dvRocksDB.getFileDictCfHandle(), reverseKey, filePathBytes);

            return fileId;
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to get or create file ID for: " + filePath, e);
        }
    }

    /**
     * Gets the file ID for a given file path.
     *
     * @param filePath the file path
     * @return the file ID, or null if not found
     */
    public Integer getFileId(String filePath) {
        Objects.requireNonNull(filePath, "filePath cannot be null");
        byte[] filePathBytes = filePath.getBytes(StandardCharsets.UTF_8);
        byte[] forwardKey = new byte[1 + filePathBytes.length];
        forwardKey[0] = FORWARD_PREFIX;
        System.arraycopy(filePathBytes, 0, forwardKey, 1, filePathBytes.length);

        try {
            byte[] value = dvRocksDB.get(dvRocksDB.getFileDictCfHandle(), forwardKey);
            return value == null ? null : decodeFileId(value);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to get file ID for: " + filePath, e);
        }
    }

    /**
     * Gets the file path for a given file ID.
     *
     * @param fileId the file ID
     * @return the file path, or null if not found
     */
    public String getFilePath(int fileId) {
        byte[] fileIdBytes = encodeFileId(fileId);
        byte[] reverseKey = new byte[1 + fileIdBytes.length];
        reverseKey[0] = REVERSE_PREFIX;
        System.arraycopy(fileIdBytes, 0, reverseKey, 1, fileIdBytes.length);

        try {
            byte[] value = dvRocksDB.get(dvRocksDB.getFileDictCfHandle(), reverseKey);
            return value != null ? new String(value, StandardCharsets.UTF_8) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to get file path for file ID: " + fileId, e);
        }
    }

    /**
     * Gets all entries in the file dictionary.
     *
     * @return a map of file ID to file path
     */
    public Map<Integer, String> getAllEntries() {
        Map<Integer, String> entries = new HashMap<>();
        try (RocksIterator iterator = dvRocksDB.newIterator(dvRocksDB.getFileDictCfHandle())) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (key.length > 0 && key[0] == REVERSE_PREFIX && key.length == 5) {
                    int fileId = decodeFileId(new byte[] {key[1], key[2], key[3], key[4]});
                    String filePath = new String(iterator.value(), StandardCharsets.UTF_8);
                    entries.put(fileId, filePath);
                }
                iterator.next();
            }
        }
        return entries;
    }
}
