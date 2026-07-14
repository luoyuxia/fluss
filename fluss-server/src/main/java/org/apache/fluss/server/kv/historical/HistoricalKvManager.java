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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.function.FunctionWithException;

import org.rocksdb.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Manages disposable historical KV state by table bucket. */
@Internal
@ThreadSafe
public final class HistoricalKvManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalKvManager.class);

    private final Configuration configuration;
    private final TabletServerMetricGroup serverMetricGroup;
    private final RateLimiter sharedRateLimiter;
    private final Clock clock;
    private final Object lifecycleLock = new Object();

    @GuardedBy("lifecycleLock")
    private final Map<TableBucket, HistoricalKvHandle> handles = new HashMap<>();

    @GuardedBy("lifecycleLock")
    private final Map<TableBucket, HistoricalKvHandle> readyHandles = new HashMap<>();

    @GuardedBy("lifecycleLock")
    private boolean closed;

    /** Creates a historical KV manager with the shared server RocksDB resources. */
    public HistoricalKvManager(
            Configuration configuration,
            TabletServerMetricGroup serverMetricGroup,
            RateLimiter sharedRateLimiter,
            Clock clock) {
        this.configuration = checkNotNull(configuration, "configuration must not be null");
        this.serverMetricGroup =
                checkNotNull(serverMetricGroup, "serverMetricGroup must not be null");
        this.sharedRateLimiter =
                checkNotNull(sharedRateLimiter, "sharedRateLimiter must not be null");
        this.clock = checkNotNull(clock, "clock must not be null");
    }

    /** Gets or lazily creates historical state in the given KV tablet directory. */
    public HistoricalKvHandle getOrCreate(TableBucket tableBucket, File kvTabletDir)
            throws IOException {
        checkNotNull(tableBucket, "tableBucket must not be null");
        checkNotNull(kvTabletDir, "kvTabletDir must not be null");
        checkArgument(
                tableBucket.getPartitionId() != null,
                "Historical KV state requires a partitioned table bucket");

        synchronized (lifecycleLock) {
            checkState(!closed, "HistoricalKvManager is already closed");
            HistoricalKvHandle existing = handles.get(tableBucket);
            if (existing != null) {
                checkState(
                        readyHandles.get(tableBucket) == existing,
                        "Historical KV state for %s is being recovered",
                        tableBucket);
                return existing;
            }

            HistoricalKvHandle created =
                    HistoricalKvHandle.create(
                            tableBucket,
                            kvTabletDir,
                            configuration,
                            serverMetricGroup,
                            sharedRateLimiter,
                            clock);
            handles.put(tableBucket, created);
            readyHandles.put(tableBucket, created);
            return created;
        }
    }

    /** Creates an empty handle that remains hidden until recovery completes. */
    public HistoricalKvHandle createForRecovery(TableBucket tableBucket, File kvTabletDir)
            throws IOException {
        checkNotNull(tableBucket, "tableBucket must not be null");
        checkNotNull(kvTabletDir, "kvTabletDir must not be null");
        checkArgument(
                tableBucket.getPartitionId() != null,
                "Historical KV state requires a partitioned table bucket");

        synchronized (lifecycleLock) {
            checkState(!closed, "HistoricalKvManager is already closed");
            HistoricalKvHandle previous = handles.remove(tableBucket);
            readyHandles.remove(tableBucket, previous);
            if (previous != null) {
                dropHandle(previous);
            }
            HistoricalKvHandle created =
                    HistoricalKvHandle.create(
                            tableBucket,
                            kvTabletDir,
                            configuration,
                            serverMetricGroup,
                            sharedRateLimiter,
                            clock);
            handles.put(tableBucket, created);
            return created;
        }
    }

    /** Publishes a completely recovered handle as ready for online access. */
    public void markReady(TableBucket tableBucket, HistoricalKvHandle handle) {
        synchronized (lifecycleLock) {
            checkState(!closed, "HistoricalKvManager is already closed");
            checkState(
                    handles.get(tableBucket) == handle,
                    "Historical KV handle for %s was replaced during recovery",
                    tableBucket);
            readyHandles.put(tableBucket, handle);
        }
    }

    /** Returns the historical state for a table bucket if it has been created. */
    public Optional<HistoricalKvHandle> getIfPresent(TableBucket tableBucket) {
        synchronized (lifecycleLock) {
            return Optional.ofNullable(readyHandles.get(tableBucket));
        }
    }

    /** Executes a read against the current ready handle under lifecycle-safe lock ordering. */
    public <T, E extends Exception> Optional<T> withReadyHandleReadLock(
            TableBucket tableBucket, FunctionWithException<HistoricalKvHandle, T, E> action)
            throws E {
        HistoricalKvHandle handle;
        synchronized (lifecycleLock) {
            handle = readyHandles.get(tableBucket);
            if (handle == null) {
                return Optional.empty();
            }
            handle.acquireReadLock();
        }
        try {
            return Optional.ofNullable(action.apply(handle));
        } finally {
            handle.releaseReadLock();
        }
    }

    /** Flushes a ready handle while preventing concurrent lifecycle removal. */
    public boolean flushIfReady(TableBucket tableBucket, long exclusiveLogOffset)
            throws IOException {
        HistoricalKvHandle handle;
        synchronized (lifecycleLock) {
            handle = readyHandles.get(tableBucket);
            if (handle == null) {
                return false;
            }
            handle.acquireWriteLock();
        }
        try {
            handle.flush(exclusiveLogOffset);
            return true;
        } finally {
            handle.releaseWriteLock();
        }
    }

    /** Removes the expected ready handle when no lookup currently holds its read lock. */
    public boolean tryInvalidateBucket(TableBucket tableBucket, HistoricalKvHandle expectedHandle) {
        synchronized (lifecycleLock) {
            if (readyHandles.get(tableBucket) != expectedHandle
                    || !expectedHandle.tryAcquireWriteLock()) {
                return false;
            }
            readyHandles.remove(tableBucket, expectedHandle);
            handles.remove(tableBucket, expectedHandle);
        }

        try {
            expectedHandle.dropUnderWriteLock();
        } catch (Exception e) {
            throw new KvStorageException(
                    "Failed to drop historical KV state for " + tableBucket, e);
        } finally {
            expectedHandle.releaseWriteLock();
        }
        return true;
    }

    /** Returns a snapshot of handles that are ready for online access. */
    public List<HistoricalKvHandle> readyHandles() {
        synchronized (lifecycleLock) {
            return new ArrayList<>(readyHandles.values());
        }
    }

    /** Closes and deletes the historical state for a table bucket. */
    public void invalidateBucket(TableBucket tableBucket) {
        synchronized (lifecycleLock) {
            HistoricalKvHandle handle = handles.remove(tableBucket);
            readyHandles.remove(tableBucket, handle);
            if (handle != null) {
                dropHandle(handle);
            }
        }
    }

    /** Closes and deletes all historical state for a table. */
    public void invalidateTable(long tableId) {
        synchronized (lifecycleLock) {
            List<HistoricalKvHandle> removed = new ArrayList<>();
            Iterator<Map.Entry<TableBucket, HistoricalKvHandle>> iterator =
                    handles.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TableBucket, HistoricalKvHandle> entry = iterator.next();
                if (entry.getKey().getTableId() == tableId) {
                    removed.add(entry.getValue());
                    readyHandles.remove(entry.getKey(), entry.getValue());
                    iterator.remove();
                }
            }
            dropHandles(removed);
        }
    }

    /** Closes and deletes every historical state managed by this instance. */
    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            List<HistoricalKvHandle> removed = new ArrayList<>(handles.values());
            handles.clear();
            readyHandles.clear();
            dropHandles(removed);
        }
    }

    private void dropHandles(List<HistoricalKvHandle> removed) {
        KvStorageException firstFailure = null;
        for (HistoricalKvHandle handle : removed) {
            try {
                handle.drop();
            } catch (Exception e) {
                LOG.warn("Failed to drop historical KV state for {}.", handle.getTableBucket(), e);
                if (firstFailure == null) {
                    firstFailure =
                            new KvStorageException(
                                    "Failed to drop historical KV state for "
                                            + handle.getTableBucket(),
                                    e);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private void dropHandle(HistoricalKvHandle handle) {
        try {
            handle.drop();
        } catch (Exception e) {
            throw new KvStorageException(
                    "Failed to drop historical KV state for " + handle.getTableBucket(), e);
        }
    }

    @VisibleForTesting
    int size() {
        synchronized (lifecycleLock) {
            return handles.size();
        }
    }
}
