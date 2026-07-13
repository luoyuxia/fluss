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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.kv.scan.OpenScanResult;
import org.apache.fluss.server.kv.scan.ScannerContext;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotDataUploader;
import org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot;
import org.apache.fluss.server.kv.snapshot.TabletState;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** A kv tablet which presents a unified view of kv storage. */
@ThreadSafe
public final class KvTablet {
    private static final Logger LOG = LoggerFactory.getLogger(KvTablet.class);
    private static final long ROW_COUNT_DISABLED = -1;

    private final PhysicalTablePath physicalPath;
    private final TableBucket tableBucket;

    private final File kvTabletDir;
    private final long writeBatchSize;
    private final RocksDBKv rocksDBKv;
    private final KvPreWriteBuffer kvPreWriteBuffer;
    private final KvStateAccessor kvStateAccessor;
    private final KvWriteProcessor kvWriteProcessor;
    private final TabletServerMetricGroup serverMetricGroup;

    // A lock that guards all modifications to the kv.
    private final ReadWriteLock kvLock = new ReentrantReadWriteLock();
    private final AutoIncrementManager autoIncrementManager;

    // RocksDB statistics accessor for this tablet
    @Nullable private final RocksDBStatistics rocksDBStatistics;

    /**
     * The kv data in pre-write buffer whose log offset is less than the flushedLogOffset has been
     * flushed into kv.
     */
    private volatile long flushedLogOffset = 0;

    private volatile long rowCount;

    @GuardedBy("kvLock")
    private volatile boolean isClosed = false;

    private KvTablet(
            PhysicalTablePath physicalPath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            TabletServerMetricGroup serverMetricGroup,
            RocksDBKv rocksDBKv,
            long writeBatchSize,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            @Nullable RocksDBStatistics rocksDBStatistics,
            AutoIncrementManager autoIncrementManager) {
        this.physicalPath = physicalPath;
        this.tableBucket = tableBucket;
        this.kvTabletDir = kvTabletDir;
        this.rocksDBKv = rocksDBKv;
        this.writeBatchSize = writeBatchSize;
        this.serverMetricGroup = serverMetricGroup;
        this.kvPreWriteBuffer = new KvPreWriteBuffer(createKvBatchWriter(), serverMetricGroup);
        this.kvStateAccessor = new NormalKvStateAccessor(kvPreWriteBuffer, rocksDBKv);
        this.kvWriteProcessor =
                new KvWriteProcessor(
                        tableBucket,
                        logTablet,
                        new ArrowWriterPool(arrowBufferAllocator),
                        memorySegmentPool,
                        kvFormat,
                        rowMerger,
                        arrowCompressionInfo,
                        schemaGetter,
                        changelogImage,
                        autoIncrementManager);
        this.rocksDBStatistics = rocksDBStatistics;
        this.autoIncrementManager = autoIncrementManager;
        // disable row count for WAL image mode.
        this.rowCount = changelogImage == ChangelogImage.WAL ? ROW_COUNT_DISABLED : 0L;
    }

    public static KvTablet create(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File kvTabletDir,
            Configuration serverConf,
            TabletServerMetricGroup serverMetricGroup,
            BufferAllocator arrowBufferAllocator,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            RateLimiter sharedRateLimiter,
            AutoIncrementManager autoIncrementManager)
            throws IOException {
        RocksDBKv kv = buildRocksDBKv(serverConf, kvTabletDir, sharedRateLimiter);

        // Create RocksDB statistics accessor (will be registered to TableMetricGroup by Replica)
        // Pass ResourceGuard to ensure thread-safe access during concurrent close operations
        // Pass ColumnFamilyHandle for column family specific properties like num-files-at-level0
        // Pass Cache for accurate block cache memory tracking
        RocksDBStatistics rocksDBStatistics =
                new RocksDBStatistics(
                        kv.getDb(),
                        kv.getStatistics(),
                        kv.getResourceGuard(),
                        kv.getDefaultColumnFamilyHandle(),
                        kv.getBlockCache());

        return new KvTablet(
                tablePath,
                tableBucket,
                logTablet,
                kvTabletDir,
                serverMetricGroup,
                kv,
                serverConf.get(ConfigOptions.KV_WRITE_BATCH_SIZE).getBytes(),
                arrowBufferAllocator,
                memorySegmentPool,
                kvFormat,
                rowMerger,
                arrowCompressionInfo,
                schemaGetter,
                changelogImage,
                rocksDBStatistics,
                autoIncrementManager);
    }

    private static RocksDBKv buildRocksDBKv(
            Configuration configuration, File kvDir, RateLimiter sharedRateLimiter)
            throws IOException {
        // Enable statistics to support RocksDB statistics collection
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(configuration, kvDir, true, sharedRateLimiter);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                        kvDir,
                        rocksDBResourceContainer,
                        rocksDBResourceContainer.getColumnOptions());
        return rocksDBKvBuilder.build();
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public TablePath getTablePath() {
        return physicalPath.getTablePath();
    }

    public long getAutoIncrementCacheSize() {
        return autoIncrementManager.getAutoIncrementCacheSize();
    }

    public void updateAutoIncrementIDRange(AutoIncIDRange newRange) {
        autoIncrementManager.updateIDRange(newRange);
    }

    @Nullable
    public String getPartitionName() {
        return physicalPath.getPartitionName();
    }

    public File getKvTabletDir() {
        return kvTabletDir;
    }

    /**
     * Get RocksDB statistics accessor for this tablet.
     *
     * @return the RocksDB statistics accessor, or null if not available
     */
    @Nullable
    public RocksDBStatistics getRocksDBStatistics() {
        return rocksDBStatistics;
    }

    void setFlushedLogOffset(long flushedLogOffset) {
        this.flushedLogOffset = flushedLogOffset;
    }

    void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    // row_count is volatile, so it's safe to read without lock
    public long getRowCount() {
        if (rowCount == ROW_COUNT_DISABLED) {
            throw new InvalidTableException(
                    String.format(
                            "Row count is disabled for this table '%s'. This usually happens when the table is"
                                    + "created before v0.9 or the changelog image is set to WAL, "
                                    + "as maintaining row count in WAL mode is costly and not necessary for most use cases. "
                                    + "If you want to enable row count, please set changelog image to FULL.",
                            getTablePath()));
        }
        return rowCount;
    }

    /**
     * Get the current state of the tablet, including the log offset, row count and auto-increment
     * ID range. This is used for snapshot and recovery to capture the state of the tablet at a
     * specific log offset.
     *
     * <p>Note: this method must be called under the kvLock to ensure the consistency between the
     * returned state and the log offset.
     */
    @GuardedBy("kvLock")
    public TabletState getTabletState() {
        return new TabletState(
                flushedLogOffset,
                rowCount == ROW_COUNT_DISABLED ? null : rowCount,
                autoIncrementManager.getCurrentIDRanges());
    }

    /**
     * Put the KvRecordBatch into the kv storage with default DEFAULT mode.
     *
     * <p>This is a convenience method that calls {@link #putAsLeader(KvRecordBatch, int[],
     * MergeMode)} with {@link MergeMode#DEFAULT}.
     *
     * @param kvRecords the kv records to put into
     * @param targetColumns the target columns to put, null if put all columns
     */
    public LogAppendInfo putAsLeader(KvRecordBatch kvRecords, @Nullable int[] targetColumns)
            throws Exception {
        return putAsLeader(kvRecords, targetColumns, MergeMode.DEFAULT);
    }

    /**
     * Put the KvRecordBatch into the kv storage, and return the appended wal log info.
     *
     * <p>Schema Evolution Handling:
     *
     * <p>We don't allow shema of input kv records to be larger than the latest schema id known by
     * the tablet. Besides, we currently only support ADD COLUMN LAST operation, so the input row or
     * old row must have same or fewer columns than latest schema. This helps to simplify the schema
     * change handling.
     *
     * <p>1. We write the kv records into KvStore without converting it into latest schema for
     * performance consideration. We have mechanisms that writer client dynamically use latest
     * schema for writing records.
     *
     * <p>2. We always use the latest schema for writing WAL logs, because it anyway happens
     * deserialization&serialization to convert the compacted format into Arrow format.
     *
     * @param kvRecords the kv records to put into
     * @param targetColumns the target columns to put, null if put all columns
     * @param mergeMode the merge mode (DEFAULT or OVERWRITE)
     */
    public LogAppendInfo putAsLeader(
            KvRecordBatch kvRecords, @Nullable int[] targetColumns, MergeMode mergeMode)
            throws Exception {
        return inWriteLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return kvWriteProcessor.putAsLeader(
                            kvRecords, targetColumns, mergeMode, kvStateAccessor);
                });
    }

    public void flush(long exclusiveUpToLogOffset, FatalErrorHandler fatalErrorHandler) {
        // todo: need to introduce a backpressure mechanism
        // to avoid too much records in kvPreWriteBuffer
        inWriteLock(
                kvLock,
                () -> {
                    // when kv manager is closed which means kv tablet is already closed,
                    // but the tablet server may still handle fetch log request from follower
                    // as the tablet rpc service is closed asynchronously, then update the watermark
                    // and then flush the pre-write buffer.

                    // In such case, if the tablet is already closed, we won't flush pre-write
                    // buffer, just warning it.
                    if (isClosed) {
                        LOG.warn(
                                "The kv tablet for {} is already closed, ignore flushing kv pre-write buffer.",
                                tableBucket);
                    } else {
                        try {
                            int rowCountDiff = kvStateAccessor.flush(exclusiveUpToLogOffset);
                            if (exclusiveUpToLogOffset > flushedLogOffset) {
                                flushedLogOffset = exclusiveUpToLogOffset;
                            }
                            if (rowCount != ROW_COUNT_DISABLED) {
                                // row count is enabled, we update the row count after flush.
                                long currentRowCount = rowCount;
                                rowCount = currentRowCount + rowCountDiff;
                            }
                        } catch (Throwable t) {
                            fatalErrorHandler.onFatalError(
                                    new KvStorageException("Failed to flush kv pre-write buffer."));
                        }
                    }
                });
    }

    /** put key,value,logOffset into pre-write buffer directly. */
    void putToPreWriteBuffer(
            ChangeType changeType, byte[] key, @Nullable byte[] value, long logOffset) {
        KvPreWriteBuffer.Key wrapKey = KvPreWriteBuffer.Key.of(key);
        if (changeType == ChangeType.DELETE && value == null) {
            kvStateAccessor.delete(wrapKey, logOffset);
        } else if (changeType == ChangeType.INSERT) {
            kvStateAccessor.insert(wrapKey, value, logOffset);
        } else if (changeType == ChangeType.UPDATE_AFTER) {
            kvStateAccessor.update(wrapKey, value, logOffset);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported change type for putToPreWriteBuffer: " + changeType);
        }
    }

    /**
     * Get a executor that executes submitted runnable tasks with preventing any concurrent
     * modification to this tablet.
     *
     * @return An executor that wraps task execution within the lock for all modification to this
     *     tablet.
     */
    public Executor getGuardedExecutor() {
        return runnable -> inWriteLock(kvLock, runnable::run);
    }

    public List<byte[]> multiGet(List<byte[]> keys) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.multiGet(keys);
                });
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.prefixLookup(prefixKey);
                });
    }

    public List<byte[]> limitScan(int limit) throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    return rocksDBKv.limitScan(limit);
                });
    }

    /**
     * Opens a new full-scan session under the {@code kvLock} read lock. Returns an empty-bucket
     * result (context = {@code null}, all RocksDB resources released internally) when the bucket
     * has no rows. The returned {@link ScannerContext} is unregistered; the caller owns
     * registration and close.
     *
     * @param limit row-count cap across all batches ({@code ≤ 0} means unlimited)
     * @throws IOException if RocksDB is shutting down
     */
    public OpenScanResult openScan(String scannerId, long limit, long initialAccessTimeMs)
            throws IOException {
        return inReadLock(
                kvLock,
                () -> {
                    rocksDBKv.checkIfRocksDBClosed();
                    ResourceGuard.Lease lease = rocksDBKv.getResourceGuard().acquireResource();
                    Snapshot snapshot = null;
                    ReadOptions readOptions = null;
                    RocksIterator iterator = null;
                    boolean success = false;
                    try {
                        snapshot = rocksDBKv.getDb().getSnapshot();
                        // Capture under kvLock so the offset matches the data visible through
                        // the snapshot.
                        long capturedLogOffset = flushedLogOffset;
                        readOptions = new ReadOptions().setSnapshot(snapshot);
                        iterator =
                                rocksDBKv
                                        .getDb()
                                        .newIterator(
                                                rocksDBKv.getDefaultColumnFamilyHandle(),
                                                readOptions);
                        iterator.seekToFirst();
                        if (!iterator.isValid()) {
                            return new OpenScanResult(null, capturedLogOffset);
                        }
                        ScannerContext context =
                                new ScannerContext(
                                        scannerId,
                                        tableBucket,
                                        rocksDBKv,
                                        iterator,
                                        readOptions,
                                        snapshot,
                                        lease,
                                        limit,
                                        capturedLogOffset,
                                        initialAccessTimeMs);
                        success = true;
                        return new OpenScanResult(context, capturedLogOffset);
                    } finally {
                        if (!success) {
                            IOUtils.closeQuietly(iterator);
                            IOUtils.closeQuietly(readOptions);
                            if (snapshot != null) {
                                try {
                                    rocksDBKv.getDb().releaseSnapshot(snapshot);
                                } catch (Throwable t) {
                                    LOG.warn("Error releasing RocksDB snapshot.", t);
                                }
                                IOUtils.closeQuietly(snapshot);
                            }
                            IOUtils.closeQuietly(lease);
                        }
                    }
                });
    }

    public KvBatchWriter createKvBatchWriter() {
        return rocksDBKv.newWriteBatch(
                writeBatchSize,
                serverMetricGroup.kvFlushCount(),
                serverMetricGroup.kvFlushLatencyHistogram());
    }

    public void close() throws Exception {
        LOG.info("close kv tablet {} for table {}.", tableBucket, physicalPath);
        inWriteLock(
                kvLock,
                () -> {
                    if (isClosed) {
                        return;
                    }
                    // Note: RocksDB metrics lifecycle is managed by TableMetricGroup
                    // No need to close it here
                    if (rocksDBKv != null) {
                        rocksDBKv.close();
                    }
                    isClosed = true;
                });
    }

    /** Completely delete the kv directory and all contents form the file system with no delay. */
    public void drop() throws Exception {
        inWriteLock(
                kvLock,
                () -> {
                    // first close the kv.
                    close();
                    // then delete the directory.
                    FileUtils.deleteDirectory(kvTabletDir);
                });
    }

    public RocksIncrementalSnapshot createIncrementalSnapshot(
            Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            long lastCompletedSnapshotId) {
        return new RocksIncrementalSnapshot(
                uploadedSstFiles,
                rocksDBKv.getDb(),
                rocksDBKv.getResourceGuard(),
                kvSnapshotDataUploader,
                kvTabletDir,
                lastCompletedSnapshotId);
    }

    // only for testing.
    @VisibleForTesting
    KvPreWriteBuffer getKvPreWriteBuffer() {
        return kvPreWriteBuffer;
    }

    // only for testing.
    @VisibleForTesting
    public RocksDBKv getRocksDBKv() {
        return rocksDBKv;
    }
}
