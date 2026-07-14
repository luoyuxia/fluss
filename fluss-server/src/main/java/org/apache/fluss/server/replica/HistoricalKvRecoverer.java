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

package org.apache.fluss.server.replica;

import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.kv.RemoteLogFetcher;
import org.apache.fluss.server.kv.historical.HistoricalKvHandle;
import org.apache.fluss.server.kv.historical.HistoricalKvManager;
import org.apache.fluss.server.kv.historical.HistoricalKvStateAccessor;
import org.apache.fluss.server.kv.snapshot.SnapshotContext;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.storage.LocalDiskManager;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.PartitionUtils.convertValueOfType;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Lazily rebuilds disposable historical KV state from remote and local WAL.
 *
 * <p>Recovery starts from the lake log end offset, or the log start offset when no lake offset is
 * available, and replays through the local log end offset. The handle remains hidden until the
 * complete range has been recovered. Records before the high watermark are flushed to RocksDB;
 * later records remain in the pre-write buffer. A failed recovery drops the incomplete handle.
 */
final class HistoricalKvRecoverer {

    private final HistoricalKvManager historicalKvManager;
    private final SnapshotContext snapshotContext;
    private final LocalDiskManager localDiskManager;

    HistoricalKvRecoverer(
            HistoricalKvManager historicalKvManager,
            SnapshotContext snapshotContext,
            LocalDiskManager localDiskManager) {
        this.historicalKvManager =
                checkNotNull(historicalKvManager, "historicalKvManager must not be null");
        this.snapshotContext = checkNotNull(snapshotContext, "snapshotContext must not be null");
        this.localDiskManager = checkNotNull(localDiskManager, "localDiskManager must not be null");
    }

    /** Rebuilds one historical bucket and publishes its handle after recovery completes. */
    void recover(Replica replica) throws Exception {
        localDiskManager.ensureWritable();
        LogTablet logTablet = replica.getLogTablet();
        long lakeEnd = logTablet.getLakeLogEndOffset();
        // Data before lakeEnd is already available through lake fallback, so only rebuild the WAL
        // tail that has not been tiered yet.
        long startOffset = lakeEnd >= 0 ? lakeEnd : logTablet.logStartOffset();
        long committedEnd = logTablet.getHighWatermark();
        long localEnd = logTablet.localLogEndOffset();

        checkArgument(
                startOffset <= localEnd,
                "Historical recovery start offset %s is after local end offset %s for %s.",
                startOffset,
                localEnd,
                replica.getTableBucket());
        checkArgument(
                committedEnd <= localEnd,
                "Historical high watermark %s is after local end offset %s for %s.",
                committedEnd,
                localEnd,
                replica.getTableBucket());

        HistoricalKvHandle handle =
                historicalKvManager.createForRecovery(
                        replica.getTableBucket(), replica.getKvTabletDir());
        try {
            if (startOffset < localEnd) {
                handle.withWriteLock(
                        () -> replay(replica, handle, startOffset, committedEnd, localEnd));
            }
            // The high watermark may have advanced during replay. Flush once more with its latest
            // value before exposing the recovered handle.
            handle.withWriteLock(
                    () -> {
                        HistoricalKvStateAccessor flushAccessor =
                                new HistoricalKvStateAccessor(handle, "__recovery_flush__");
                        flushAccessor.flush(replica.getLogHighWatermark());
                    });
            historicalKvManager.markReady(replica.getTableBucket(), handle);
        } catch (Throwable t) {
            try {
                historicalKvManager.invalidateBucket(replica.getTableBucket());
            } catch (Throwable cleanupFailure) {
                t.addSuppressed(cleanupFailure);
            }
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            throw new KvStorageException(
                    "Failed to recover historical KV state for " + replica.getTableBucket(), t);
        }
    }

    /**
     * Replays the continuous range {@code [startOffset, localEnd)}, reading the retained prefix
     * from remote WAL when {@code startOffset} precedes the local log start offset.
     */
    private void replay(
            Replica replica,
            HistoricalKvHandle handle,
            long startOffset,
            long committedEnd,
            long localEnd)
            throws Exception {
        LogTablet logTablet = replica.getLogTablet();
        long localStart = logTablet.localLogStartOffset();
        RecoveryContext context = new RecoveryContext(replica, handle);
        long nextOffset = startOffset;

        try (RemoteLogFetcher remoteLogFetcher =
                new RemoteLogFetcher(
                        replica.getRemoteLogManager(),
                        replica.getTableBucket(),
                        logTablet.getLogDir(),
                        snapshotContext.remoteLogPrefetchNumInRecoverKv(),
                        snapshotContext.remoteLogDownloadThreadsInRecoverKv())) {
            while (nextOffset < localEnd) {
                Iterable<LogRecordBatch> batches;
                long readEnd;
                if (nextOffset < localStart) {
                    readEnd = Math.min(localStart, localEnd);
                    batches = remoteLogFetcher.fetch(nextOffset, readEnd);
                } else {
                    readEnd = localEnd;
                    LogRecords records =
                            logTablet
                                    .read(
                                            nextOffset,
                                            snapshotContext.maxFetchLogSizeInRecoverKv(),
                                            FetchIsolation.LOG_END,
                                            true,
                                            null,
                                            null)
                                    .getRecords();
                    if (records == MemoryLogRecords.EMPTY) {
                        break;
                    }
                    batches = records.batches();
                }
                nextOffset = applyBatches(batches, nextOffset, readEnd, committedEnd, context);
            }
        }
    }

    /**
     * Applies batches within {@code [startOffset, endOffset)} and returns the next exclusive
     * offset. Mutations before {@code committedEnd} are flushed while later mutations remain in the
     * pre-write buffer.
     */
    private long applyBatches(
            Iterable<LogRecordBatch> batches,
            long startOffset,
            long endOffset,
            long committedEnd,
            RecoveryContext context)
            throws Exception {
        long nextOffset = startOffset;
        for (LogRecordBatch batch : batches) {
            if (batch.nextLogOffset() <= nextOffset) {
                continue;
            }
            context.apply(batch, nextOffset, endOffset);
            nextOffset = Math.min(batch.nextLogOffset(), endOffset);
            if (nextOffset <= committedEnd) {
                context.flush(nextOffset);
            } else if (context.flushedOffset < committedEnd && committedEnd > startOffset) {
                context.flush(committedEnd);
            }
            if (nextOffset >= endOffset) {
                break;
            }
        }
        return nextOffset;
    }

    /** Caches schema-specific decoders and partition accessors for one recovery pass. */
    private static final class RecoveryContext {
        private final Replica replica;
        private final HistoricalKvHandle handle;
        private final TableInfo tableInfo;
        private final SchemaGetter schemaGetter;
        private final KvFormat kvFormat;
        private final Map<Integer, RecoverySchema> schemas = new HashMap<>();
        private final Map<String, HistoricalKvStateAccessor> accessors = new HashMap<>();

        private long flushedOffset = -1L;

        private RecoveryContext(Replica replica, HistoricalKvHandle handle) {
            this.replica = replica;
            this.handle = handle;
            this.tableInfo = replica.getTableInfo();
            this.schemaGetter = replica.schemaGetter();
            this.kvFormat = tableInfo.getTableConfig().getKvFormat();
        }

        /** Rebuilds composite-key mutations from the records in one WAL batch. */
        private void apply(LogRecordBatch batch, long startOffset, long endOffset)
                throws Exception {
            RecoverySchema recoverySchema =
                    schemas.computeIfAbsent(
                            (int) batch.schemaId(), this::createRecoverySchemaUnchecked);
            try (LogRecordReadContext readContext =
                            createReadContext(recoverySchema, batch.schemaId());
                    CloseableIterator<LogRecord> records = batch.records(readContext)) {
                while (records.hasNext()) {
                    LogRecord record = records.next();
                    if (record.logOffset() < startOffset || record.logOffset() >= endOffset) {
                        continue;
                    }
                    ChangeType changeType = record.getChangeType();
                    // UPDATE_BEFORE only carries the old value; UPDATE_AFTER rebuilds final state.
                    if (changeType == ChangeType.UPDATE_BEFORE) {
                        continue;
                    }
                    InternalRow row = record.getRow();
                    String partitionName = recoverySchema.partitionName(row);
                    HistoricalKvStateAccessor accessor =
                            accessors.computeIfAbsent(
                                    partitionName,
                                    ignored ->
                                            new HistoricalKvStateAccessor(handle, partitionName));
                    byte[] key = recoverySchema.keyEncoder.encodeKey(row);
                    org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key encodedKey =
                            accessor.encodeKey(key);
                    if (changeType == ChangeType.DELETE) {
                        // Preserve a tombstone so a local miss cannot resurrect an old lake row.
                        accessor.delete(encodedKey, record.logOffset());
                    } else {
                        byte[] value =
                                ValueEncoder.encodeValue(
                                        batch.schemaId(), recoverySchema.toKvRow(row));
                        if (changeType == ChangeType.INSERT) {
                            accessor.insert(encodedKey, value, record.logOffset());
                        } else {
                            accessor.update(encodedKey, value, record.logOffset());
                        }
                    }
                }
            }
        }

        /** Flushes the handle-wide pre-write state through the given exclusive offset. */
        private void flush(long exclusiveOffset) throws Exception {
            if (accessors.isEmpty()) {
                return;
            }
            accessors.values().iterator().next().flush(exclusiveOffset);
            flushedOffset = Math.max(flushedOffset, exclusiveOffset);
        }

        /** Creates a decoder matching the replica log format and the batch schema. */
        private LogRecordReadContext createReadContext(
                RecoverySchema recoverySchema, short schemaId) {
            if (replica.getLogFormat() == org.apache.fluss.metadata.LogFormat.ARROW) {
                return LogRecordReadContext.createArrowReadContext(
                        recoverySchema.rowType, (int) schemaId, schemaGetter);
            }
            return LogRecordReadContext.createCompactedRowReadContext(
                    recoverySchema.rowType, (int) schemaId);
        }

        private RecoverySchema createRecoverySchemaUnchecked(Integer schemaId) {
            try {
                return new RecoverySchema(schemaGetter.getSchema(schemaId), tableInfo, kvFormat);
            } catch (Exception e) {
                throw new KvStorageException(
                        "Failed to initialize schema "
                                + schemaId
                                + " while recovering "
                                + replica.getTableBucket(),
                        e);
            }
        }
    }

    /** Holds the encoders and partition getters required to replay one schema version. */
    private static final class RecoverySchema {
        private final RowType rowType;
        private final KeyEncoder keyEncoder;
        private final RowEncoder rowEncoder;
        private final InternalRow.FieldGetter[] fieldGetters;
        private final InternalRow.FieldGetter[] partitionGetters;
        private final DataType[] partitionTypes;
        private final List<String> partitionKeys;
        private final KvFormat kvFormat;

        private RecoverySchema(Schema schema, TableInfo tableInfo, KvFormat kvFormat) {
            this.rowType = schema.getRowType();
            this.kvFormat = kvFormat;
            this.keyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            rowType,
                            tableInfo.getPhysicalPrimaryKeys(),
                            tableInfo.getTableConfig(),
                            tableInfo.isDefaultBucketKey());
            DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
            this.rowEncoder = RowEncoder.create(kvFormat, dataTypes);
            this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
            for (int i = 0; i < fieldGetters.length; i++) {
                fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
            }

            this.partitionKeys = tableInfo.getPartitionKeys();
            this.partitionGetters = new InternalRow.FieldGetter[partitionKeys.size()];
            this.partitionTypes = new DataType[partitionKeys.size()];
            for (int i = 0; i < partitionKeys.size(); i++) {
                int fieldIndex = rowType.getFieldIndex(partitionKeys.get(i));
                checkArgument(
                        fieldIndex >= 0,
                        "Partition column %s is absent from recovery schema.",
                        partitionKeys.get(i));
                partitionTypes[i] = rowType.getTypeAt(fieldIndex);
                partitionGetters[i] = InternalRow.createFieldGetter(partitionTypes[i], fieldIndex);
            }
        }

        /** Reconstructs the original partition name from the row's partition columns. */
        private String partitionName(InternalRow row) {
            List<String> values = new ArrayList<>(partitionKeys.size());
            for (int i = 0; i < partitionKeys.size(); i++) {
                Object value = partitionGetters[i].getFieldOrNull(row);
                values.add(
                        convertValueOfType(
                                checkNotNull(
                                        value,
                                        "Partition column %s must not be null",
                                        partitionKeys.get(i)),
                                partitionTypes[i].getTypeRoot()));
            }
            return new ResolvedPartitionSpec(partitionKeys, values).getPartitionName();
        }

        /** Converts a log row to the table's configured KV value representation. */
        private BinaryRow toKvRow(InternalRow row) {
            if (kvFormat == KvFormat.INDEXED && row instanceof IndexedRow) {
                return (IndexedRow) row;
            }
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldGetters.length; i++) {
                rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
            }
            return rowEncoder.finishRow();
        }
    }
}
