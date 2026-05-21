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

import org.apache.fluss.utils.CloseableIterator;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Operations wrapper for the PendingDeletes Column Family.
 *
 * <p>PendingDeletes stores entries for rows that have been deleted but whose positions may not yet
 * be resolved. Each entry maps a RowId (8-byte BigEndian long) to either:
 *
 * <ul>
 *   <li><b>Known position</b>: FilePos varint-encoded bytes (at least 2 bytes)
 *   <li><b>Pending (position unknown)</b>: empty byte array ({@code new byte[0]})
 * </ul>
 *
 * <p>The empty byte array is distinguishable from a varint-encoded FilePos because varint encoding
 * produces at least 2 bytes.
 */
public class PendingDeletes {

    private static final byte[] PENDING_MARKER = new byte[0];

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;
    private final WriteOptions writeOptions;

    PendingDeletes(RocksDB db, ColumnFamilyHandle cfHandle, WriteOptions writeOptions) {
        this.db = db;
        this.cfHandle = cfHandle;
        this.writeOptions = writeOptions;
    }

    /** Writes a pending delete entry with a known file position. */
    public void put(long rowId, FilePos filePos) throws IOException {
        try {
            db.put(cfHandle, writeOptions, encodeRowId(rowId), filePos.encode());
        } catch (RocksDBException e) {
            throw new IOException("Failed to put PendingDeletes entry for rowId " + rowId, e);
        }
    }

    /** Writes a pending marker (position unknown) for the given rowId. */
    public void putPending(long rowId) throws IOException {
        try {
            db.put(cfHandle, writeOptions, encodeRowId(rowId), PENDING_MARKER);
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to put PendingDeletes pending marker for rowId " + rowId, e);
        }
    }

    /** Gets the pending delete entry for the given rowId. Returns null if not found. */
    @Nullable
    public PendingDeleteEntry get(long rowId) throws IOException {
        try {
            byte[] value = db.get(cfHandle, encodeRowId(rowId));
            if (value == null) {
                return null;
            }
            return decodePendingDeleteEntry(rowId, value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to get PendingDeletes entry for rowId " + rowId, e);
        }
    }

    /** Deletes the pending delete entry for the given rowId. */
    public void delete(long rowId) throws IOException {
        try {
            db.delete(cfHandle, writeOptions, encodeRowId(rowId));
        } catch (RocksDBException e) {
            throw new IOException("Failed to delete PendingDeletes entry for rowId " + rowId, e);
        }
    }

    /** Adds a put operation with a known file position to the given WriteBatch. */
    public void put(WriteBatch batch, long rowId, FilePos filePos) throws IOException {
        try {
            batch.put(cfHandle, encodeRowId(rowId), filePos.encode());
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add PendingDeletes put to WriteBatch for rowId " + rowId, e);
        }
    }

    /** Adds a pending marker to the given WriteBatch. */
    public void putPending(WriteBatch batch, long rowId) throws IOException {
        try {
            batch.put(cfHandle, encodeRowId(rowId), PENDING_MARKER);
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add PendingDeletes pending marker to WriteBatch for rowId " + rowId,
                    e);
        }
    }

    /** Adds a delete operation to the given WriteBatch. */
    public void delete(WriteBatch batch, long rowId) throws IOException {
        try {
            batch.delete(cfHandle, encodeRowId(rowId));
        } catch (RocksDBException e) {
            throw new IOException(
                    "Failed to add PendingDeletes delete to WriteBatch for rowId " + rowId, e);
        }
    }

    /**
     * Returns a closeable iterator over all PendingDeletes entries. The caller must close the
     * iterator when done to release underlying RocksDB resources.
     */
    public CloseableIterator<PendingDeleteEntry> iterator() {
        ReadOptions readOptions = new ReadOptions();
        RocksIterator rocksIterator = db.newIterator(cfHandle, readOptions);
        rocksIterator.seekToFirst();

        return new CloseableIterator<PendingDeleteEntry>() {
            @Override
            public boolean hasNext() {
                return rocksIterator.isValid();
            }

            @Override
            public PendingDeleteEntry next() {
                if (!rocksIterator.isValid()) {
                    throw new NoSuchElementException();
                }
                long rowId = decodeRowId(rocksIterator.key());
                PendingDeleteEntry entry = decodePendingDeleteEntry(rowId, rocksIterator.value());
                rocksIterator.next();
                return entry;
            }

            @Override
            public void close() {
                rocksIterator.close();
                readOptions.close();
            }
        };
    }

    private static PendingDeleteEntry decodePendingDeleteEntry(long rowId, byte[] value) {
        if (value.length == 0) {
            return new PendingDeleteEntry(rowId, null);
        }
        return new PendingDeleteEntry(rowId, FilePos.decode(value));
    }

    /** Encodes a rowId as an 8-byte BigEndian key. */
    static byte[] encodeRowId(long rowId) {
        byte[] key = new byte[8];
        key[0] = (byte) (rowId >>> 56);
        key[1] = (byte) (rowId >>> 48);
        key[2] = (byte) (rowId >>> 40);
        key[3] = (byte) (rowId >>> 32);
        key[4] = (byte) (rowId >>> 24);
        key[5] = (byte) (rowId >>> 16);
        key[6] = (byte) (rowId >>> 8);
        key[7] = (byte) rowId;
        return key;
    }

    /** Decodes a rowId from an 8-byte BigEndian key. */
    static long decodeRowId(byte[] key) {
        return ((long) (key[0] & 0xFF) << 56)
                | ((long) (key[1] & 0xFF) << 48)
                | ((long) (key[2] & 0xFF) << 40)
                | ((long) (key[3] & 0xFF) << 32)
                | ((long) (key[4] & 0xFF) << 24)
                | ((long) (key[5] & 0xFF) << 16)
                | ((long) (key[6] & 0xFF) << 8)
                | ((long) (key[7] & 0xFF));
    }

    /** An immutable entry representing a pending delete. */
    public static class PendingDeleteEntry {

        private final long rowId;
        private final @Nullable FilePos filePos;

        PendingDeleteEntry(long rowId, @Nullable FilePos filePos) {
            this.rowId = rowId;
            this.filePos = filePos;
        }

        public long getRowId() {
            return rowId;
        }

        /** Returns the file position, or null if the position is still pending. */
        @Nullable
        public FilePos getFilePos() {
            return filePos;
        }

        /** Returns true if the position is still pending (unknown). */
        public boolean isPending() {
            return filePos == null;
        }

        @Override
        public String toString() {
            return "PendingDeleteEntry{rowId="
                    + rowId
                    + ", filePos="
                    + (filePos == null ? "PENDING" : filePos)
                    + '}';
        }
    }
}
