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
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Encapsulates RowPosIndex CF and PendingRowPos CF operations. Manages the mapping from rowId to
 * FilePos for both the main index and pending entries.
 */
public class RowPosIndex {
    private final DvRocksDB dvRocksDB;

    public RowPosIndex(DvRocksDB dvRocksDB) {
        this.dvRocksDB = Objects.requireNonNull(dvRocksDB, "dvRocksDB cannot be null");
    }

    /**
     * Encodes a rowId into a byte array using big-endian order.
     *
     * @param rowId the row ID to encode
     * @return the encoded byte array
     */
    private byte[] encodeRowId(long rowId) {
        return ByteBuffer.allocate(8).putLong(rowId).array();
    }

    /**
     * Gets the FilePos for a given rowId from the RowPosIndex CF.
     *
     * @param rowId the row ID
     * @return the FilePos, or null if not found
     */
    public FilePos get(long rowId) {
        try {
            byte[] key = encodeRowId(rowId);
            byte[] value = dvRocksDB.get(dvRocksDB.getRowPosIndexCfHandle(), key);
            return value != null ? FilePos.fromBytes(value) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to get FilePos for rowId: " + rowId, e);
        }
    }

    /**
     * Puts a FilePos for a given rowId into the RowPosIndex CF.
     *
     * @param rowId the row ID
     * @param filePos the FilePos to store
     */
    public void put(long rowId, FilePos filePos) {
        checkNotNull(filePos, "filePos cannot be null");
        try {
            byte[] key = encodeRowId(rowId);
            byte[] value = filePos.toBytes();
            dvRocksDB.put(dvRocksDB.getRowPosIndexCfHandle(), key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to put FilePos for rowId: " + rowId, e);
        }
    }

    /**
     * Deletes a rowId from the RowPosIndex CF.
     *
     * @param rowId the row ID to delete
     */
    public void delete(long rowId) {
        try {
            byte[] key = encodeRowId(rowId);
            dvRocksDB.delete(dvRocksDB.getRowPosIndexCfHandle(), key);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to delete rowId: " + rowId, e);
        }
    }

    /**
     * Gets the pending FilePos for a given rowId from the PendingRowPos CF.
     *
     * @param rowId the row ID
     * @return the pending FilePos, or null if not found
     */
    public FilePos getPending(long rowId) {
        try {
            byte[] key = encodeRowId(rowId);
            byte[] value = dvRocksDB.get(dvRocksDB.getPendingRowPosCfHandle(), key);
            return value != null ? FilePos.fromBytes(value) : null;
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to get pending FilePos for rowId: " + rowId, e);
        }
    }

    /**
     * Puts a pending FilePos for a given rowId into the PendingRowPos CF.
     *
     * @param rowId the row ID
     * @param filePos the FilePos to store
     */
    public void putPending(long rowId, FilePos filePos) {
        checkNotNull(filePos, "filePos cannot be null");
        try {
            byte[] key = encodeRowId(rowId);
            byte[] value = filePos.toBytes();
            dvRocksDB.put(dvRocksDB.getPendingRowPosCfHandle(), key, value);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to put pending FilePos for rowId: " + rowId, e);
        }
    }

    /**
     * Deletes a pending rowId from the PendingRowPos CF.
     *
     * @param rowId the row ID to delete
     */
    public void deletePending(long rowId) {
        try {
            byte[] key = encodeRowId(rowId);
            dvRocksDB.delete(dvRocksDB.getPendingRowPosCfHandle(), key);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to delete pending rowId: " + rowId, e);
        }
    }

    /**
     * Migrates all pending entries from PendingRowPos CF to RowPosIndex CF, then clears the
     * PendingRowPos CF.
     */
    public void migratePendingToIndex() {
        try (RocksIterator iterator = dvRocksDB.newIterator(dvRocksDB.getPendingRowPosCfHandle())) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                dvRocksDB.put(dvRocksDB.getRowPosIndexCfHandle(), key, value);
                iterator.next();
            }
            clearPending();
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to migrate pending entries to index", e);
        }
    }

    /** Clears all entries from the PendingRowPos CF. */
    public void clearPending() {
        try {
            byte[] beginKey = new byte[0];
            byte[] endKey =
                    new byte[] {
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
                    };
            dvRocksDB.deleteRange(dvRocksDB.getPendingRowPosCfHandle(), beginKey, endKey);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to clear pending entries", e);
        }
    }
}
