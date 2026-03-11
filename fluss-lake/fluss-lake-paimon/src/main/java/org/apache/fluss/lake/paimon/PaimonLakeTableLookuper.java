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

package org.apache.fluss.lake.paimon;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.lake.paimon.utils.PaimonRowAsFlussRow;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toFlussRowType;

/**
 * Paimon implementation of {@link LakeTableLookuper}.
 *
 * <p>Each instance is bound to a specific table and caches per-table resources (Catalog,
 * FileStoreTable, LocalTableQuery, etc.) for efficient repeated lookups.
 *
 * <p>Performs point lookups against Paimon lake storage for expired partitions using {@link
 * LocalTableQuery}. The key bytes passed to {@link #lookup} are already encoded in Paimon's
 * BinaryRow format by the client-side {@code PaimonKeyEncoder}, so they can be directly wrapped as
 * a Paimon BinaryRow without decode/re-encode.
 */
public class PaimonLakeTableLookuper implements LakeTableLookuper {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeTableLookuper.class);

    private final Configuration paimonConfig;
    private final TablePath tablePath;

    // Lazily initialized and cached per-table resources
    private volatile int cachedSchemaId = -1;
    private Catalog catalog;
    private FileStoreTable fileStoreTable;
    private RowType flussRowType;
    private int numPkFields;
    private List<String> partitionKeys;
    private InternalRow.FieldGetter[] fieldGetters;
    private LocalTableQuery tableQuery;

    // Cache of (partitionName, bucketId) pairs that have already been refreshed
    private final Set<Tuple2<String, Integer>> refreshedBuckets = new HashSet<>();

    public PaimonLakeTableLookuper(Configuration paimonConfig, TablePath tablePath) {
        this.paimonConfig = paimonConfig;
        this.tablePath = tablePath;
    }

    @Nullable
    @Override
    public byte[] lookup(byte[] key, LookupContext context) throws Exception {
        ensureInitialized(context.getSchemaId());

        String partitionName = context.getPartitionName();
        int bucketId = context.getBucketId();

        // Create partition BinaryRow
        BinaryRow partition = createPartitionBinaryRow(partitionKeys, partitionName);

        // Refresh files only for new (partitionName, bucketId) pairs
        Tuple2<String, Integer> bucketKey = Tuple2.of(partitionName, bucketId);
        if (!refreshedBuckets.contains(bucketKey)) {
            for (Split split :
                    fileStoreTable
                            .newScan()
                            // todo: partition consider multiple fields
                            .withPartitionFilter(
                                    Collections.singletonMap(partitionKeys.get(0), partitionName))
                            .withBucket(bucketId)
                            .plan()
                            .splits()) {
                DataSplit dataSplit = (DataSplit) split;
                tableQuery.refreshFiles(
                        partition, dataSplit.bucket(), emptyList(), dataSplit.dataFiles());
            }
            refreshedBuckets.add(bucketKey);
        }

        // The key bytes are already encoded in Paimon's BinaryRow format by PaimonKeyEncoder,
        // so we can directly wrap them as a Paimon BinaryRow without decode/re-encode.
        BinaryRow keyRow = new BinaryRow(numPkFields);
        keyRow.pointTo(MemorySegment.wrap(key), 0, key.length);

        // Perform lookup
        org.apache.paimon.data.InternalRow valueRow =
                tableQuery.lookup(partition, bucketId, keyRow);

        if (valueRow != null) {
            // Wrap Paimon value row as Fluss InternalRow using adapter
            PaimonRowAsFlussRow flussValueRow = new PaimonRowAsFlussRow(valueRow);
            return encodeValueRow(flussValueRow, flussRowType, fieldGetters, context.getSchemaId());
        } else {
            return null;
        }
    }

    /**
     * Lazily initialize and cache per-table resources. Re-initializes when schema ID changes to
     * ensure the Paimon FileStoreTable schema stays in sync with the Fluss schema.
     */
    private synchronized void ensureInitialized(int schemaId) throws Exception {
        if (cachedSchemaId == schemaId) {
            return;
        }

        // Close previous LocalTableQuery if re-initializing due to schema change
        if (tableQuery != null) {
            tableQuery.close();
            refreshedBuckets.clear();
        }

        if (catalog == null) {
            catalog =
                    CatalogFactory.createCatalog(
                            CatalogContext.create(Options.fromMap(paimonConfig.toMap())));
        }

        org.apache.paimon.catalog.Identifier identifier =
                org.apache.paimon.catalog.Identifier.create(
                        tablePath.getDatabaseName(), tablePath.getTableName());
        Table table = catalog.getTable(identifier);

        if (!(table instanceof FileStoreTable)) {
            throw new IllegalStateException("Table " + tablePath + " is not a FileStoreTable");
        }

        fileStoreTable = (FileStoreTable) table;

        if (fileStoreTable.primaryKeys().isEmpty()) {
            throw new IllegalStateException(
                    "Table "
                            + tablePath
                            + " is not a primary key table, lake lookup not supported");
        }

        // Convert Paimon RowType to Fluss RowType
        org.apache.paimon.types.RowType paimonFullRowType = fileStoreTable.rowType();
        flussRowType = toFlussRowType(paimonFullRowType);
        List<String> primaryKeyFields = fileStoreTable.schema().trimmedPrimaryKeys();

        // Store the number of PK fields for wrapping key bytes as BinaryRow
        numPkFields = primaryKeyFields.size();

        partitionKeys = fileStoreTable.partitionKeys();

        // Prepare field getters for value encoding
        int valueFieldCount = flussRowType.getFieldCount() - 3;
        fieldGetters = new InternalRow.FieldGetter[valueFieldCount];
        for (int i = 0; i < valueFieldCount; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(flussRowType.getTypeAt(i), i);
        }

        // Create LocalTableQuery (cached and reused across lookups)
        tableQuery = fileStoreTable.newLocalTableQuery();
        tableQuery.withIOManager(IOManager.create(System.getProperty("java.io.tmpdir")));

        cachedSchemaId = schemaId;
    }

    /**
     * Create a partition BinaryRow from partition name.
     *
     * <p>For single partition key tables, the partition name is the partition value. For
     * non-partitioned tables, returns an empty BinaryRow.
     */
    private BinaryRow createPartitionBinaryRow(List<String> partitionKeys, String partitionName) {
        // For single partition key, partition name equals partition value
        // todo: consider support multiple partitions
        BinaryRow partition = new BinaryRow(partitionKeys.size());
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeString(0, org.apache.paimon.data.BinaryString.fromString(partitionName));
        writer.complete();
        return partition;
    }

    /** Encode value row to bytes (excluding system columns). */
    private byte[] encodeValueRow(
            InternalRow flussValueRow,
            RowType flussRowType,
            InternalRow.FieldGetter[] fieldGetters,
            int schemaId) {
        try {
            int fieldCount = fieldGetters.length;
            CompactedRowEncoder compactedRowEncoder =
                    new CompactedRowEncoder(flussRowType.getFieldTypes().toArray(new DataType[0]));
            compactedRowEncoder.startNewRow();

            for (int i = 0; i < fieldCount; i++) {
                Object value = fieldGetters[i].getFieldOrNull(flussValueRow);
                compactedRowEncoder.encodeField(i, value);
            }

            org.apache.fluss.row.BinaryRow binaryRow = compactedRowEncoder.finishRow();
            return ValueEncoder.encodeValue((short) schemaId, binaryRow);
        } catch (Exception e) {
            LOG.warn("Failed to encode value row", e);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        tableQuery.close();
    }
}
