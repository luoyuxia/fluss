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

import java.nio.ByteBuffer;
import java.util.Objects;

/** Manages pending deletes using PendingDeletes CF. Tracks row IDs that are pending deletion. */
public class PendingDeletes {
    private final DvRocksDB dvRocksDB;
    private static final byte[] EMPTY_VALUE = new byte[0];

    public PendingDeletes(DvRocksDB dvRocksDB) {
        this.dvRocksDB = Objects.requireNonNull(dvRocksDB, "dvRocksDB cannot be null");
    }

    /**
     * Encodes a row ID into an 8-byte big-endian array.
     *
     * @param rowId the row ID
     * @return the encoded byte array
     */
    private byte[] encodeRowId(long rowId) {
        return ByteBuffer.allocate(8).putLong(rowId).array();
    }

    /**
     * Adds a row ID to the pending deletes.
     *
     * @param rowId the row ID to add
     */
    public void add(long rowId) {
        try {
            byte[] key = encodeRowId(rowId);
            dvRocksDB.put(dvRocksDB.getPendingDeletesCfHandle(), key, EMPTY_VALUE);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to add pending delete for rowId: " + rowId, e);
        }
    }

    /**
     * Checks if a row ID is in the pending deletes.
     *
     * @param rowId the row ID to check
     * @return true if the row ID is pending deletion, false otherwise
     */
    public boolean contains(long rowId) {
        try {
            byte[] key = encodeRowId(rowId);
            byte[] value = dvRocksDB.get(dvRocksDB.getPendingDeletesCfHandle(), key);
            return value != null;
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to check pending delete for rowId: " + rowId, e);
        }
    }

    /**
     * Removes a row ID from the pending deletes.
     *
     * @param rowId the row ID to remove
     */
    public void remove(long rowId) {
        try {
            byte[] key = encodeRowId(rowId);
            dvRocksDB.delete(dvRocksDB.getPendingDeletesCfHandle(), key);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to remove pending delete for rowId: " + rowId, e);
        }
    }

    /**
     * Cleans up pending deletes up to and including the given max row ID.
     *
     * @param maxRowId the maximum row ID to cleanup (inclusive)
     */
    public void cleanupRange(long maxRowId) {
        try {
            byte[] beginKey = encodeRowId(0L);
            byte[] endKey = encodeRowId(maxRowId + 1);
            dvRocksDB.deleteRange(dvRocksDB.getPendingDeletesCfHandle(), beginKey, endKey);
        } catch (RocksDBException e) {
            throw new RuntimeException(
                    "Failed to cleanup pending deletes range up to: " + maxRowId, e);
        }
    }
}
