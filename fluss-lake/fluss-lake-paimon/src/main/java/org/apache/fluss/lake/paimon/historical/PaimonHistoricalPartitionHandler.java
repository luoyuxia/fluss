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

package org.apache.fluss.lake.paimon.historical;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/**
 * Paimon-specific handler for historical partition operations.
 *
 * <p>This class provides functionality for:
 * <ul>
 *   <li>Checking if a partition has expired in Paimon (based on partition.expiration-time)</li>
 *   <li>Looking up keys from Paimon for changelog generation in PK tables</li>
 *   <li>Scanning historical partition data from Paimon for union read</li>
 * </ul>
 *
 * <p>Currently hardcoded for Paimon as it's the only lake format supporting partition TTL.
 * If Iceberg/Hudi support is needed later, extract a LakeHistoricalPartitionHandler interface.
 */
public class PaimonHistoricalPartitionHandler implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonHistoricalPartitionHandler.class);

    private final Catalog paimonCatalog;
    private final TablePath tablePath;
    private final FileStoreTable paimonTable;

    public PaimonHistoricalPartitionHandler(Catalog paimonCatalog, TablePath tablePath)
            throws Exception {
        this.paimonCatalog = paimonCatalog;
        this.tablePath = tablePath;
        this.paimonTable = (FileStoreTable) paimonCatalog.getTable(toPaimon(tablePath));
    }

    /**
     * Check if a partition has expired in Paimon.
     *
     * <p>Uses Paimon's partition.expiration-time configuration to determine if a partition
     * should be considered expired.
     *
     * @param partitionName the partition name to check
     * @return true if the partition has expired in Paimon, false otherwise
     */
    public boolean isPartitionExpiredInPaimon(String partitionName) {
        try {
            // Get partition expiration time from Paimon table options
            Duration expirationTime = paimonTable.coreOptions().partitionExpireTime();
            if (expirationTime == null) {
                // No expiration configured, partition never expires in Paimon
                return false;
            }

            // Check if partition exists in Paimon
            List<String> existingPartitions = getExistingPartitions();
            if (!existingPartitions.contains(partitionName)) {
                // Partition doesn't exist in Paimon, consider it expired
                return true;
            }

            // TODO: Check actual partition creation time vs expiration time
            // For now, if partition exists in Paimon, it's not expired
            return false;
        } catch (Exception e) {
            LOG.warn("Failed to check partition expiration for {}, treating as not expired", 
                    partitionName, e);
            return false;
        }
    }

    /**
     * Get all partitions that exist in Paimon.
     *
     * @return list of partition names
     */
    public List<String> getExistingPartitions() throws Exception {
        List<String> partitionNames = new ArrayList<>();
        List<BinaryRow> partitions = paimonTable.newSnapshotReader().partitions().partitions();
        List<String> partitionKeys = paimonTable.partitionKeys();
        
        for (BinaryRow partition : partitions) {
            // Convert BinaryRow to partition name string
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < partitionKeys.size(); i++) {
                if (i > 0) {
                    sb.append("$");
                }
                sb.append(partition.getString(i).toString());
            }
            partitionNames.add(sb.toString());
        }
        return partitionNames;
    }

    /**
     * Lookup a key from Paimon for changelog generation.
     *
     * <p>This is used when writing to a historical partition of a PK table.
     * We need to lookup the existing value to determine if it's an INSERT or UPDATE.
     *
     * @param partitionName the partition name
     * @param bucket the bucket id
     * @param keyBytes the key bytes to lookup
     * @return the value bytes if found, null otherwise
     */
    @Nullable
    public byte[] lookup(String partitionName, int bucket, byte[] keyBytes) throws IOException {
        try {
            // Build partition predicate
            List<String> partitionKeys = paimonTable.partitionKeys();
            Map<String, String> partitionSpec = parsePartitionName(partitionName, partitionKeys);
            
            // Create read builder with partition and bucket filter
            ReadBuilder readBuilder = paimonTable.newReadBuilder();
            
            // Add partition filter
            RowType rowType = paimonTable.rowType();
            PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
            List<Predicate> predicates = new ArrayList<>();
            
            for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
                int fieldIndex = rowType.getFieldIndex(entry.getKey());
                if (fieldIndex >= 0) {
                    predicates.add(predicateBuilder.equal(fieldIndex, entry.getValue()));
                }
            }
            
            // Add bucket filter using __bucket system column
            int bucketFieldIndex = rowType.getFieldIndex("__bucket");
            if (bucketFieldIndex >= 0) {
                predicates.add(predicateBuilder.equal(bucketFieldIndex, bucket));
            }
            
            if (!predicates.isEmpty()) {
                readBuilder.withFilter(PredicateBuilder.and(predicates));
            }
            
            // Read and find matching key
            List<Split> splits = readBuilder.newScan().plan().splits();
            TableRead tableRead = readBuilder.newRead();
            
            // For now, we'll use a simpler approach to find the key
            // In a real implementation, we would need to deserialize the key bytes
            // and compare with the primary key columns
            for (Split split : splits) {
                try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader = 
                        tableRead.createReader(split)) {
                    org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow> batch;
                    while ((batch = reader.readBatch()) != null) {
                        org.apache.paimon.data.InternalRow row;
                        while ((row = batch.next()) != null) {
                            // Extract the key portion from the row and compare with keyBytes
                            // This is a simplified implementation - in practice, we need to know the schema
                            // and which columns constitute the primary key
                            byte[] rowKey = extractKeyFromRow(row, rowType, tableInfo.getPrimaryKeyColumns());
                            if (java.util.Arrays.equals(rowKey, keyBytes)) {
                                // Found the key, return the value (remaining part of the row)
                                byte[] rowValue = extractValueFromRow(row, rowType, tableInfo.getPrimaryKeyColumns());
                                batch.releaseBatch();
                                return rowValue;
                            }
                        }
                        batch.releaseBatch();
                    }
                }
            }
            
            // Key not found
            return null;
        } catch (Exception e) {
            throw new IOException("Failed to lookup key from Paimon for partition " + partitionName, e);
        }
    }

    /**
     * Batch lookup for better performance.
     *
     * @param partitionName the partition name
     * @param bucket the bucket id
     * @param keys list of keys to lookup
     * @return map of key -> value for found keys
     */
    public Map<byte[], byte[]> batchLookup(String partitionName, int bucket, List<byte[]> keys) 
            throws IOException {
        // For now, delegate to individual lookups
        // TODO: Optimize with batch read
        Map<byte[], byte[]> results = new HashMap<>();
        for (byte[] key : keys) {
            byte[] value = lookup(partitionName, bucket, key);
            if (value != null) {
                results.put(key, value);
            }
        }
        return results;
    }

    /**
     * Scan historical partition data from Paimon.
     *
     * <p>Used for union read scenarios where we need to read data from Paimon
     * for historical partitions.
     *
     * @param partitionName the partition name
     * @param bucket the bucket id
     * @return iterator over the rows in the partition/bucket
     */
    public CloseableIterator<InternalRow> scan(String partitionName, int bucket) throws IOException {
        try {
            // Build partition predicate
            List<String> partitionKeys = paimonTable.partitionKeys();
            Map<String, String> partitionSpec = parsePartitionName(partitionName, partitionKeys);
            
            ReadBuilder readBuilder = paimonTable.newReadBuilder();
            
            // Add partition and bucket filter
            RowType rowType = paimonTable.rowType();
            PredicateBuilder predicateBuilder = new PredicateBuilder(rowType);
            List<Predicate> predicates = new ArrayList<>();
            
            for (Map.Entry<String, String> entry : partitionSpec.entrySet()) {
                int fieldIndex = rowType.getFieldIndex(entry.getKey());
                if (fieldIndex >= 0) {
                    predicates.add(predicateBuilder.equal(fieldIndex, entry.getValue()));
                }
            }
            
            int bucketFieldIndex = rowType.getFieldIndex("__bucket");
            if (bucketFieldIndex >= 0) {
                predicates.add(predicateBuilder.equal(bucketFieldIndex, bucket));
            }
            
            if (!predicates.isEmpty()) {
                readBuilder.withFilter(PredicateBuilder.and(predicates));
            }
            
            List<Split> splits = readBuilder.newScan().plan().splits();
            TableRead tableRead = readBuilder.newRead();
            
            // TODO: Return proper CloseableIterator that wraps Paimon reader
            // For now, return empty iterator as placeholder
            return CloseableIterator.emptyIterator();
        } catch (Exception e) {
            throw new IOException("Failed to scan Paimon for partition " + partitionName, e);
        }
    }

    /**
     * Parse partition name to partition spec map.
     *
     * @param partitionName the partition name (e.g., "2024-01-01" or "2024-01-01$A")
     * @param partitionKeys the partition keys
     * @return map of partition key -> value
     */
    private Map<String, String> parsePartitionName(String partitionName, List<String> partitionKeys) {
        if (partitionKeys.isEmpty()) {
            return Collections.emptyMap();
        }
        
        Map<String, String> spec = new HashMap<>();
        String[] parts = partitionName.split("\\$");
        for (int i = 0; i < Math.min(parts.length, partitionKeys.size()); i++) {
            spec.put(partitionKeys.get(i), parts[i]);
        }
        return spec;
    }

    private byte[] extractKeyFromRow(org.apache.paimon.data.InternalRow row, RowType rowType, java.util.List<String> primaryKeyColumns) {
        // This is a simplified implementation that would need to be fleshed out based on
        // the actual table schema and key serialization format
        // For now, return a dummy implementation
        return new byte[0];
    }
    
    private byte[] extractValueFromRow(org.apache.paimon.data.InternalRow row, RowType rowType, java.util.List<String> primaryKeyColumns) {
        // This is a simplified implementation that would need to be fleshed out based on
        // the actual table schema and value serialization format
        // For now, return a dummy implementation
        return new byte[0];
    }
    
    @Override
    public void close() throws IOException {
        try {
            if (paimonCatalog != null) {
                paimonCatalog.close();
            }
        } catch (Exception e) {
            throw new IOException("Failed to close PaimonHistoricalPartitionHandler", e);
        }
    }
}
