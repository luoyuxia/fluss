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

import org.apache.fluss.rocksdb.RocksDBOperationUtils;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** A RocksDB instance for Deletion Vector (DV) with 6 column families. */
public class DvRocksDB implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DvRocksDB.class);

    // ------------------------------------------------------------------------
    //  Column Family Names
    // ------------------------------------------------------------------------
    public static final byte[] CF_ROW_POS_INDEX = "row_pos_index".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_PENDING_ROW_POS =
            "pending_row_pos".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_LOG_DV = "log_dv".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_LAKE_DV = "lake_dv".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_FILE_DICT = "file_dict".getBytes(StandardCharsets.UTF_8);
    public static final byte[] CF_PENDING_DELETES =
            "pending_deletes".getBytes(StandardCharsets.UTF_8);

    // ------------------------------------------------------------------------
    //  Fields
    // ------------------------------------------------------------------------
    private final RocksDB db;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;
    private final ColumnFamilyHandle rowPosIndexHandle;
    private final ColumnFamilyHandle pendingRowPosHandle;
    private final ColumnFamilyHandle logDvHandle;
    private final ColumnFamilyHandle lakeDvHandle;
    private final ColumnFamilyHandle fileDictHandle;
    private final ColumnFamilyHandle pendingDeletesHandle;
    private final WriteOptions writeOptions;
    private final DBOptions dbOptions;
    private boolean closed;

    private DvRocksDB(
            RocksDB db,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            ColumnFamilyHandle rowPosIndexHandle,
            ColumnFamilyHandle pendingRowPosHandle,
            ColumnFamilyHandle logDvHandle,
            ColumnFamilyHandle lakeDvHandle,
            ColumnFamilyHandle fileDictHandle,
            ColumnFamilyHandle pendingDeletesHandle,
            WriteOptions writeOptions,
            DBOptions dbOptions) {
        this.db = db;
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
        this.rowPosIndexHandle = rowPosIndexHandle;
        this.pendingRowPosHandle = pendingRowPosHandle;
        this.logDvHandle = logDvHandle;
        this.lakeDvHandle = lakeDvHandle;
        this.fileDictHandle = fileDictHandle;
        this.pendingDeletesHandle = pendingDeletesHandle;
        this.writeOptions = writeOptions;
        this.dbOptions = dbOptions;
        this.closed = false;
    }

    /**
     * Open a DvRocksDB instance at the given path.
     *
     * @param dvRocksDBPath the directory path for RocksDB
     * @return a new DvRocksDB instance
     * @throws IOException if opening the database fails
     */
    public static DvRocksDB open(File dvRocksDBPath) throws IOException {
        // Ensure RocksDB native library is loaded
        RocksDBKvBuilder.ensureRocksDBIsLoaded(System.getProperty("java.io.tmpdir"));

        // Create DB options
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);
        dbOptions.setCreateMissingColumnFamilies(true);

        // Create column family descriptors
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>();

        // Default column family
        ColumnFamilyOptions defaultCfOptions = new ColumnFamilyOptions();
        columnFamilyDescriptors.add(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultCfOptions));
        columnFamilyOptions.add(defaultCfOptions);

        // Custom column families
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(CF_ROW_POS_INDEX, cfOptions));
        columnFamilyOptions.add(cfOptions);

        cfOptions = new ColumnFamilyOptions();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(CF_PENDING_ROW_POS, cfOptions));
        columnFamilyOptions.add(cfOptions);

        cfOptions = new ColumnFamilyOptions();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(CF_LOG_DV, cfOptions));
        columnFamilyOptions.add(cfOptions);

        cfOptions = new ColumnFamilyOptions();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(CF_LAKE_DV, cfOptions));
        columnFamilyOptions.add(cfOptions);

        cfOptions = new ColumnFamilyOptions();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(CF_FILE_DICT, cfOptions));
        columnFamilyOptions.add(cfOptions);

        cfOptions = new ColumnFamilyOptions();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(CF_PENDING_DELETES, cfOptions));
        columnFamilyOptions.add(cfOptions);

        // Open RocksDB
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        RocksDB db =
                RocksDBOperationUtils.openDB(
                        dvRocksDBPath.getAbsolutePath(),
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        dbOptions,
                        false);

        // Create write options with WAL disabled
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setDisableWAL(true);

        return new DvRocksDB(
                db,
                columnFamilyHandles.get(0),
                columnFamilyHandles.get(1),
                columnFamilyHandles.get(2),
                columnFamilyHandles.get(3),
                columnFamilyHandles.get(4),
                columnFamilyHandles.get(5),
                columnFamilyHandles.get(6),
                writeOptions,
                dbOptions);
    }

    // ------------------------------------------------------------------------
    //  Getters for Column Family Handles
    // ------------------------------------------------------------------------
    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    public ColumnFamilyHandle getRowPosIndexCfHandle() {
        return rowPosIndexHandle;
    }

    public ColumnFamilyHandle getPendingRowPosCfHandle() {
        return pendingRowPosHandle;
    }

    public ColumnFamilyHandle getLogDvCfHandle() {
        return logDvHandle;
    }

    public ColumnFamilyHandle getLakeDvCfHandle() {
        return lakeDvHandle;
    }

    public ColumnFamilyHandle getFileDictCfHandle() {
        return fileDictHandle;
    }

    public ColumnFamilyHandle getPendingDeletesCfHandle() {
        return pendingDeletesHandle;
    }

    // ------------------------------------------------------------------------
    //  Basic Operations
    // ------------------------------------------------------------------------
    public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        return db.get(columnFamilyHandle, key);
    }

    public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value)
            throws RocksDBException {
        db.put(columnFamilyHandle, writeOptions, key, value);
    }

    public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        db.delete(columnFamilyHandle, writeOptions, key);
    }

    public RocksIterator newIterator(ColumnFamilyHandle columnFamilyHandle) {
        return db.newIterator(columnFamilyHandle);
    }

    public void deleteRange(ColumnFamilyHandle columnFamilyHandle, byte[] beginKey, byte[] endKey)
            throws RocksDBException {
        db.deleteRange(columnFamilyHandle, writeOptions, beginKey, endKey);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            IOUtils.closeQuietly(rowPosIndexHandle);
            IOUtils.closeQuietly(pendingRowPosHandle);
            IOUtils.closeQuietly(logDvHandle);
            IOUtils.closeQuietly(lakeDvHandle);
            IOUtils.closeQuietly(fileDictHandle);
            IOUtils.closeQuietly(pendingDeletesHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);
            IOUtils.closeQuietly(db);
            IOUtils.closeQuietly(writeOptions);
            IOUtils.closeQuietly(dbOptions);
            LOG.info("DvRocksDB closed successfully.");
        }
    }
}
