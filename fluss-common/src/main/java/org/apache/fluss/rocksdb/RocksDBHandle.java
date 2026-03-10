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

package org.apache.fluss.rocksdb;

import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility for creating a RocksDB instance either from scratch or from restored local data. */
public class RocksDBHandle implements AutoCloseable {

    private final boolean isReadOnly;
    private final DBOptions dbOptions;

    private final String dbPath;

    private RocksDB db;

    private ColumnFamilyHandle defaultColumnFamilyHandle;
    private final Map<String, ColumnFamilyHandle> extraColumnFamilyHandles = new HashMap<>();

    private final ColumnFamilyOptions defaultColumnFamilyOptions;

    public RocksDBHandle(
            File instanceRocksDBPath,
            DBOptions dbOptions,
            ColumnFamilyOptions defaultColumnFamilyOptions,
            boolean isReadOnly) {
        this.dbPath = instanceRocksDBPath.getAbsolutePath();
        this.dbOptions = dbOptions;
        this.defaultColumnFamilyOptions = defaultColumnFamilyOptions;
        this.isReadOnly = isReadOnly;
    }

    public RocksDBHandle(
            File instanceRocksDBPath,
            DBOptions dbOptions,
            ColumnFamilyOptions defaultColumnFamilyOptions) {
        this(instanceRocksDBPath, dbOptions, defaultColumnFamilyOptions, false);
    }

    public void openDB() throws IOException {
        loadDb();
    }

    private void loadDb() throws IOException {
        List<byte[]> existingColumnFamilies = listExistingColumnFamilies();
        if (existingColumnFamilies.isEmpty()) {
            existingColumnFamilies = Collections.singletonList(RocksDB.DEFAULT_COLUMN_FAMILY);
        }
        boolean hasDefaultCf =
                existingColumnFamilies.stream()
                        .anyMatch(cfName -> Arrays.equals(cfName, RocksDB.DEFAULT_COLUMN_FAMILY));
        if (!hasDefaultCf) {
            existingColumnFamilies = new ArrayList<>(existingColumnFamilies);
            existingColumnFamilies.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                existingColumnFamilies.stream()
                        .map(
                                cfName ->
                                        Arrays.equals(cfName, RocksDB.DEFAULT_COLUMN_FAMILY)
                                                ? new ColumnFamilyDescriptor(
                                                        cfName, defaultColumnFamilyOptions)
                                                : new ColumnFamilyDescriptor(cfName))
                        .collect(Collectors.toList());
        List<ColumnFamilyHandle> openedHandles = new ArrayList<>(columnFamilyDescriptors.size());
        db =
                RocksDBOperationUtils.openDB(
                        dbPath, columnFamilyDescriptors, openedHandles, dbOptions, isReadOnly);

        defaultColumnFamilyHandle = null;
        for (int i = 0; i < openedHandles.size(); i++) {
            byte[] cfNameBytes = existingColumnFamilies.get(i);
            ColumnFamilyHandle cfHandle = openedHandles.get(i);
            if (Arrays.equals(cfNameBytes, RocksDB.DEFAULT_COLUMN_FAMILY)) {
                defaultColumnFamilyHandle = cfHandle;
            } else {
                extraColumnFamilyHandles.put(
                        new String(cfNameBytes, StandardCharsets.UTF_8), cfHandle);
            }
        }
        if (defaultColumnFamilyHandle == null) {
            throw new IOException("Default column family handle is missing after opening RocksDB.");
        }
    }

    private List<byte[]> listExistingColumnFamilies() {
        try (Options options = new Options()) {
            return RocksDB.listColumnFamilies(options, dbPath);
        } catch (RocksDBException e) {
            return Collections.emptyList();
        }
    }

    public RocksDB getDb() {
        return db;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    public Map<String, ColumnFamilyHandle> getExtraColumnFamilyHandles() {
        return extraColumnFamilyHandles;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        for (ColumnFamilyHandle cfHandle : extraColumnFamilyHandles.values()) {
            IOUtils.closeQuietly(cfHandle);
        }
        extraColumnFamilyHandles.clear();
        IOUtils.closeQuietly(db);
        // Making sure the already created column family options will be closed
        IOUtils.closeQuietly(defaultColumnFamilyOptions);
    }
}
