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

package org.apache.fluss.server.kv.historical;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.KvBatchWriter;
import org.apache.fluss.server.kv.KvStateLookupResult;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.Key;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.function.SupplierWithException;
import org.apache.fluss.utils.function.ThrowingRunnable;

import org.rocksdb.RateLimiter;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.fluss.utils.Preconditions.checkState;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/** Disposable local KV state for one historical table bucket. */
@Internal
@ThreadSafe
public final class HistoricalKvHandle implements AutoCloseable {

    private final TableBucket tableBucket;
    private final File directory;
    private final RocksDBKv rocksDBKv;
    private final KvPreWriteBuffer preWriteBuffer;
    private final Clock clock;
    private final ReadWriteLock stateLock = new ReentrantReadWriteLock();

    private volatile long lastAccessTime;

    @GuardedBy("stateLock")
    private boolean closed;

    private HistoricalKvHandle(
            TableBucket tableBucket,
            File directory,
            RocksDBKv rocksDBKv,
            KvPreWriteBuffer preWriteBuffer,
            Clock clock) {
        this.tableBucket = tableBucket;
        this.directory = directory;
        this.rocksDBKv = rocksDBKv;
        this.preWriteBuffer = preWriteBuffer;
        this.clock = clock;
        this.lastAccessTime = clock.milliseconds();
    }

    static HistoricalKvHandle create(
            TableBucket tableBucket,
            File directory,
            Configuration configuration,
            TabletServerMetricGroup serverMetricGroup,
            RateLimiter sharedRateLimiter,
            Clock clock)
            throws IOException {
        FileUtils.deleteDirectory(directory);

        RocksDBKv rocksDBKv = null;
        KvPreWriteBuffer preWriteBuffer = null;
        try {
            RocksDBResourceContainer resourceContainer =
                    new RocksDBResourceContainer(
                            configuration, directory, false, sharedRateLimiter);
            RocksDBKvBuilder builder =
                    new RocksDBKvBuilder(
                            directory, resourceContainer, resourceContainer.getColumnOptions());
            rocksDBKv = builder.build();

            KvBatchWriter batchWriter =
                    rocksDBKv.newWriteBatch(
                            configuration.get(ConfigOptions.KV_WRITE_BATCH_SIZE).getBytes(),
                            serverMetricGroup.kvFlushCount(),
                            serverMetricGroup.kvFlushLatencyHistogram());
            preWriteBuffer =
                    new KvPreWriteBuffer(
                            new HistoricalKvBatchWriter(batchWriter), serverMetricGroup);
            return new HistoricalKvHandle(tableBucket, directory, rocksDBKv, preWriteBuffer, clock);
        } catch (Exception e) {
            if (preWriteBuffer != null) {
                try {
                    preWriteBuffer.close();
                } catch (Exception closeException) {
                    e.addSuppressed(closeException);
                }
            }
            if (rocksDBKv != null) {
                try {
                    rocksDBKv.close();
                } catch (Exception closeException) {
                    e.addSuppressed(closeException);
                }
            }
            FileUtils.deleteDirectoryQuietly(directory);
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException(
                    "Failed to create historical KV state for " + tableBucket + '.', e);
        }
    }

    /** Returns the historical table bucket represented by this handle. */
    public TableBucket getTableBucket() {
        return tableBucket;
    }

    /** Returns this handle's RocksDB base directory. */
    public File getDirectory() {
        return directory;
    }

    /** Returns the time of the latest state access in milliseconds. */
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    /** Executes a local lookup while preventing the state from being closed or deleted. */
    public <T, E extends Exception> T withReadLock(SupplierWithException<T, E> action) throws E {
        return inReadLock(
                stateLock,
                () -> {
                    checkOpen();
                    touch();
                    return action.get();
                });
    }

    /** Executes a state mutation while holding the lock for the complete operation. */
    public <T, E extends Exception> T withWriteLock(SupplierWithException<T, E> action) throws E {
        return inWriteLock(
                stateLock,
                () -> {
                    checkOpen();
                    touch();
                    return action.get();
                });
    }

    /** Executes a state mutation while holding the lock for the complete operation. */
    public <E extends Exception> void withWriteLock(ThrowingRunnable<E> action) throws E {
        inWriteLock(
                stateLock,
                () -> {
                    checkOpen();
                    touch();
                    action.run();
                });
    }

    @GuardedBy("stateLock")
    KvStateLookupResult lookup(Key key) throws IOException {
        KvPreWriteBuffer.Value bufferedValue = preWriteBuffer.get(key);
        if (bufferedValue != null) {
            byte[] value = bufferedValue.get();
            return value == null
                    ? KvStateLookupResult.deleted()
                    : KvStateLookupResult.present(value);
        }

        byte[] value = rocksDBKv.get(key.get());
        if (value == null) {
            return KvStateLookupResult.notFound();
        }
        return HistoricalKvBatchWriter.isTombstone(value)
                ? KvStateLookupResult.deleted()
                : KvStateLookupResult.present(value);
    }

    @GuardedBy("stateLock")
    void insert(Key key, byte[] value, long logOffset) {
        preWriteBuffer.insert(key, value, logOffset);
    }

    @GuardedBy("stateLock")
    void update(Key key, byte[] value, long logOffset) {
        preWriteBuffer.update(key, value, logOffset);
    }

    @GuardedBy("stateLock")
    void delete(Key key, long logOffset) {
        preWriteBuffer.delete(key, logOffset);
    }

    @GuardedBy("stateLock")
    void truncateTo(long logOffset, TruncateReason reason) {
        preWriteBuffer.truncateTo(logOffset, reason);
    }

    @GuardedBy("stateLock")
    int flush(long exclusiveLogOffset) throws IOException {
        return preWriteBuffer.flush(exclusiveLogOffset);
    }

    /** Closes the state without deleting its directory. */
    @Override
    public void close() throws Exception {
        inWriteLock(stateLock, this::closeUnderLock);
    }

    /** Closes the state and deletes its directory. */
    public void drop() throws Exception {
        inWriteLock(
                stateLock,
                () -> {
                    Exception closeFailure = null;
                    try {
                        closeUnderLock();
                    } catch (Exception e) {
                        closeFailure = e;
                    }

                    try {
                        FileUtils.deleteDirectory(directory);
                    } catch (Exception deleteFailure) {
                        if (closeFailure != null) {
                            closeFailure.addSuppressed(deleteFailure);
                        } else {
                            closeFailure = deleteFailure;
                        }
                    }

                    if (closeFailure != null) {
                        throw closeFailure;
                    }
                });
    }

    @GuardedBy("stateLock")
    private void closeUnderLock() throws Exception {
        if (closed) {
            return;
        }
        closed = true;

        Exception closeFailure = null;
        try {
            preWriteBuffer.close();
        } catch (Exception e) {
            closeFailure = e;
        }

        try {
            rocksDBKv.close();
        } catch (Exception e) {
            if (closeFailure != null) {
                closeFailure.addSuppressed(e);
            } else {
                closeFailure = e;
            }
        }

        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    @GuardedBy("stateLock")
    private void checkOpen() {
        checkState(!closed, "Historical KV state for %s is already closed", tableBucket);
        rocksDBKv.checkIfRocksDBClosed();
    }

    private void touch() {
        lastAccessTime = clock.milliseconds();
    }

    @VisibleForTesting
    RocksDBKv getRocksDBKv() {
        return rocksDBKv;
    }

    @VisibleForTesting
    KvPreWriteBuffer getPreWriteBuffer() {
        return preWriteBuffer;
    }
}
