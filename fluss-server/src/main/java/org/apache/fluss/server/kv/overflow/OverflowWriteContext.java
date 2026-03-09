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

package org.apache.fluss.server.kv.overflow;

import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.PartitionUtils;

import org.rocksdb.ColumnFamilyHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Manages per-partition write contexts for overflow partitions.
 *
 * <p>When records from different original partitions land in a single overflow tablet, their
 * primary keys may collide (e.g., two records with the same pk but from different partitions). This
 * context maintains a separate {@link KvPreWriteBuffer} and RocksDB column family per original
 * partition to avoid key collisions.
 */
@NotThreadSafe
public class OverflowWriteContext implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OverflowWriteContext.class);

    private static final String CF_PREFIX = "overflow_";

    private final RocksDBKv rocksDBKv;
    private final long writeBatchSize;
    private final TabletServerMetricGroup serverMetricGroup;

    /**
     * Optional lake table lookuper for point lookups against Paimon when data is not found in
     * buffer or RocksDB. Historical data may have been tiered to the data lake.
     */
    private final @Nullable LakeTableLookuper lakeLookuper;

    /** Bucket ID for Paimon lookup context. */
    private final int bucketId;

    /** Per-partition contexts, keyed by partition name (e.g., "2020" or "us$2020"). */
    private final Map<String, PartitionKvContext> partitionContexts = new HashMap<>();

    /** Field getters to extract partition column values from rows. */
    private final InternalRow.FieldGetter[] partitionFieldGetters;

    /** Data type roots for partition columns, used for value-to-string conversion. */
    private final DataTypeRoot[] partitionTypeRoots;

    public OverflowWriteContext(
            RocksDBKv rocksDBKv,
            long writeBatchSize,
            TabletServerMetricGroup serverMetricGroup,
            @Nullable LakeTableLookuper lakeLookuper,
            int bucketId,
            RowType rowType,
            List<String> partitionKeys) {
        this.rocksDBKv = rocksDBKv;
        this.writeBatchSize = writeBatchSize;
        this.serverMetricGroup = serverMetricGroup;
        this.lakeLookuper = lakeLookuper;
        this.bucketId = bucketId;

        int numPartitionKeys = partitionKeys.size();
        this.partitionFieldGetters = new InternalRow.FieldGetter[numPartitionKeys];
        this.partitionTypeRoots = new DataTypeRoot[numPartitionKeys];

        for (int i = 0; i < numPartitionKeys; i++) {
            int colIndex = rowType.getFieldIndex(partitionKeys.get(i));
            DataType dataType = rowType.getTypeAt(colIndex);
            this.partitionFieldGetters[i] = InternalRow.createFieldGetter(dataType, colIndex);
            this.partitionTypeRoots[i] = dataType.getTypeRoot();
        }
    }

    /**
     * Extracts the original partition name from a row using the partition column getters.
     *
     * @param row the row containing partition column values
     * @return the partition name (e.g., "2020" or "us$2020")
     */
    public String extractPartitionName(InternalRow row) {
        if (partitionFieldGetters.length == 1) {
            Object value = partitionFieldGetters[0].getFieldOrNull(row);
            return PartitionUtils.convertValueOfType(value, partitionTypeRoots[0]);
        }
        StringJoiner joiner = new StringJoiner(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR);
        for (int i = 0; i < partitionFieldGetters.length; i++) {
            Object value = partitionFieldGetters[i].getFieldOrNull(row);
            joiner.add(PartitionUtils.convertValueOfType(value, partitionTypeRoots[i]));
        }
        return joiner.toString();
    }

    /**
     * Gets or creates a per-partition context (buffer + CF) for the given partition name.
     *
     * @param partitionName the partition name (e.g., "2020")
     * @return the partition's write context
     */
    public PartitionKvContext getOrCreatePartitionContext(String partitionName) throws IOException {
        PartitionKvContext ctx = partitionContexts.get(partitionName);
        if (ctx != null) {
            return ctx;
        }

        String cfName = CF_PREFIX + partitionName;
        ColumnFamilyHandle cfHandle = rocksDBKv.getOrCreateColumnFamily(cfName);
        KvPreWriteBuffer buffer =
                new KvPreWriteBuffer(
                        rocksDBKv.newWriteBatch(
                                writeBatchSize,
                                serverMetricGroup.kvFlushCount(),
                                serverMetricGroup.kvFlushLatencyHistogram()),
                        cfHandle,
                        serverMetricGroup);
        ctx = new PartitionKvContext(partitionName, cfHandle, buffer);
        partitionContexts.put(partitionName, ctx);
        return ctx;
    }

    /**
     * Flushes all per-partition buffers up to the given log offset.
     *
     * @return the total row count difference across all partitions
     */
    public int flushAll(long exclusiveUpToLogOffset) throws IOException {
        int totalRowCountDiff = 0;
        for (PartitionKvContext ctx : partitionContexts.values()) {
            totalRowCountDiff += ctx.getBuffer().flush(exclusiveUpToLogOffset);
        }
        return totalRowCountDiff;
    }

    /**
     * Truncates all per-partition buffers to the given log sequence number.
     *
     * @param targetLogSequenceNumber the lower bound of the log sequence number truncated to
     * @param truncateReason the reason to truncate
     */
    public void truncateAll(
            long targetLogSequenceNumber, KvPreWriteBuffer.TruncateReason truncateReason) {
        for (PartitionKvContext ctx : partitionContexts.values()) {
            ctx.getBuffer().truncateTo(targetLogSequenceNumber, truncateReason);
        }
    }

    /** Returns the internal map of partition contexts (for testing). */
    public Map<String, PartitionKvContext> getPartitionContexts() {
        return partitionContexts;
    }

    /** Returns whether this context has a lake lookuper for Paimon fallback. */
    public boolean hasLakeLookuper() {
        return lakeLookuper != null;
    }

    /**
     * Looks up a key in the data lake (Paimon) for the given partition.
     *
     * @param key the encoded primary key bytes
     * @param partitionName the original partition name
     * @param schemaId the schema ID for encoding the returned value
     * @return the encoded value bytes, or null if not found
     */
    @Nullable
    public byte[] lookupInLake(byte[] key, String partitionName, int schemaId) {
        if (lakeLookuper == null) {
            return null;
        }
        try {
            LakeTableLookuper.LookupContext context =
                    new LakeTableLookuper.LookupContext(partitionName, bucketId, schemaId);
            return lakeLookuper.lookup(key, context);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to look up key in lake for partition '{}', returning null.",
                    partitionName,
                    e);
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        for (PartitionKvContext ctx : partitionContexts.values()) {
            ctx.getBuffer().close();
        }
        partitionContexts.clear();
    }

    /**
     * Holds the per-partition write state: a {@link KvPreWriteBuffer} and a {@link
     * ColumnFamilyHandle} for a single original partition within the overflow tablet.
     */
    public static class PartitionKvContext {
        private final String partitionName;
        private final ColumnFamilyHandle columnFamilyHandle;
        private final KvPreWriteBuffer buffer;

        PartitionKvContext(
                String partitionName,
                ColumnFamilyHandle columnFamilyHandle,
                KvPreWriteBuffer buffer) {
            this.partitionName = partitionName;
            this.columnFamilyHandle = columnFamilyHandle;
            this.buffer = buffer;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public ColumnFamilyHandle getColumnFamilyHandle() {
            return columnFamilyHandle;
        }

        public KvPreWriteBuffer getBuffer() {
            return buffer;
        }
    }
}
