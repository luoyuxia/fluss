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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.exception.WakeupException;
import com.alibaba.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * {@link LogFetchBuffer} buffers up {@link CompletedFetch the results} from the tablet server
 * responses as they are received. It's essentially a wrapper around a {@link java.util.Queue} of
 * {@link CompletedFetch}. There is at most one {@link LogFetchBuffer} per bucket in the queue.
 *
 * <p>Note: this class is thread-safe with the intention that {@link CompletedFetch the data} will
 * be created by a background thread and consumed by the application thread.
 */
@ThreadSafe
@Internal
public class LogFetchBuffer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LogFetchBuffer.class);

    private final Lock lock = new ReentrantLock();
    private final Condition notEmptyCondition = lock.newCondition();
    private final AtomicBoolean wokenup = new AtomicBoolean(false);

    @GuardedBy("lock")
    private final Map<TableBucket, LinkedList<CompletedFetch>> completedFetches;

    @GuardedBy("lock")
    private final LinkedList<TableBucket> completeTableBuckets;

    @GuardedBy("lock")
    private final Map<TableBucket, LinkedList<PendingFetch>> pendingFetches;

    @GuardedBy("lock")
    private @Nullable CompletedFetch nextInLineFetch;

    public LogFetchBuffer() {
        this.completedFetches = new HashMap<>();
        this.completeTableBuckets = new LinkedList<>();
        this.pendingFetches = new HashMap<>();
    }

    /**
     * Returns {@code true} if there are no completed fetches pending to return to the user.
     *
     * @return {@code true} if the buffer is empty, {@code false} otherwise
     */
    boolean isEmpty() {
        return inLock(lock, completedFetches::isEmpty);
    }

    void pend(PendingFetch pendingFetch) {
        inLock(lock, () -> addPendingBucket(pendingFetch));
    }

    /**
     * Tries to complete the pending fetches in order, convert them into completed fetches in the
     * buffer.
     */
    void tryComplete(TableBucket tableBucket) {
        inLock(
                lock,
                () -> {
                    boolean hasCompleted = false;
                    LinkedList<PendingFetch> tableBucketPendingFetches =
                            pendingFetches.get(tableBucket);
                    while (tableBucketPendingFetches != null
                            && !tableBucketPendingFetches.isEmpty()) {
                        PendingFetch pendingFetch = tableBucketPendingFetches.peek();
                        if (pendingFetch.isCompleted()) {
                            CompletedFetch completedFetch = pendingFetch.toCompletedFetch();
                            addCompleteBucket(completedFetch);
                            tableBucketPendingFetches.poll();
                            if (tableBucketPendingFetches.isEmpty()) {
                                pendingFetches.remove(tableBucket);
                            }
                            hasCompleted = true;
                        } else {
                            break;
                        }
                    }
                    if (hasCompleted) {
                        notEmptyCondition.signalAll();
                    }
                });
    }

    void add(CompletedFetch completedFetch) {
        inLock(
                lock,
                () -> {
                    LinkedList<PendingFetch> bucketPendingFetches =
                            pendingFetches.get(completedFetch.tableBucket);
                    if (bucketPendingFetches == null || bucketPendingFetches.isEmpty()) {
                        addCompleteBucket(completedFetch);
                        notEmptyCondition.signalAll();
                    } else {
                        addPendingBucket(new CompletedPendingFetch(completedFetch));
                    }
                });
    }

    void addAll(Collection<CompletedFetch> completedFetches) {
        if (completedFetches == null || completedFetches.isEmpty()) {
            return;
        }
        inLock(
                lock,
                () -> {
                    if (pendingFetches.isEmpty()) {
                        completedFetches.forEach(this::addCompleteBucket);
                        notEmptyCondition.signalAll();
                    } else {
                        completedFetches.forEach(
                                cf -> addPendingBucket(new CompletedPendingFetch(cf)));
                    }
                });
    }

    CompletedFetch nextInLineFetch() {
        return inLock(lock, () -> nextInLineFetch);
    }

    void setNextInLineFetch(@Nullable CompletedFetch nextInLineFetch) {
        inLock(lock, () -> this.nextInLineFetch = nextInLineFetch);
    }

    CompletedFetch peek() {
        return inLock(
                lock,
                () -> {
                    if (completeTableBuckets.isEmpty()) {
                        return null;
                    }
                    return completedFetches.get(completeTableBuckets.peek()).peek();
                });
    }

    CompletedFetch poll() {
        return inLock(
                lock,
                () -> {
                    if (completeTableBuckets.isEmpty()) {
                        return null;
                    }
                    TableBucket tb = completeTableBuckets.poll();
                    LinkedList<CompletedFetch> tableBucketCompleteFetched =
                            completedFetches.get(tb);
                    CompletedFetch completeFetch = tableBucketCompleteFetched.poll();
                    if (tableBucketCompleteFetched.isEmpty()) {
                        removeCompleteBucket(tb);
                    } else {
                        // put back to the queue as last.
                        completeTableBuckets.addLast(tb);
                    }
                    return completeFetch;
                });
    }

    /**
     * Allows the caller to await presence of data in the buffer. The method will block, returning
     * only under one of the following conditions:
     *
     * <ol>
     *   <li>The buffer was already non-empty on entry
     *   <li>The buffer was populated during the wait
     *   <li>The time out
     *   <li>The thread was interrupted
     * </ol>
     *
     * @param deadlineNanos the deadline time to wait util
     * @return false if the waiting time detectably elapsed before return from the method, else true
     */
    boolean awaitNotEmpty(long deadlineNanos) throws InterruptedException {
        return inLock(
                lock,
                () -> {
                    while (isEmpty()) {
                        if (wokenup.compareAndSet(true, false)) {
                            throw new WakeupException("The await is wakeup.");
                        }
                        long remainingNanos = deadlineNanos - System.nanoTime();
                        if (remainingNanos <= 0) {
                            // false for timeout
                            return false;
                        }

                        if (notEmptyCondition.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                            // true for signal
                            return true;
                        }
                    }

                    // true for wakeup or we have data to return
                    return true;
                });
    }

    public void wakeup() {
        wokenup.set(true);
        inLock(lock, notEmptyCondition::signalAll);
    }

    /**
     * Updates the buffer to retain only the fetch data that corresponds to the given buckets. Any
     * previously {@link CompletedFetch fetched data} and {@link PendingFetch pending fetch} is
     * removed if its bucket is not in the given set of buckets.
     *
     * @param buckets {@link Set} of {@link TableBucket}s for which any buffered data should be kept
     */
    void retainAll(Set<TableBucket> buckets) {
        inLock(
                lock,
                () -> {
                    maybeDrain(buckets);
                    if (maybeDrain(buckets, nextInLineFetch)) {
                        nextInLineFetch = null;
                    }

                    Set<TableBucket> pendingBuckets = new HashSet<>(pendingFetches.keySet());
                    pendingBuckets.forEach(
                            k -> {
                                if (!buckets.contains(k)) {
                                    pendingFetches.remove(k);
                                }
                            });
                });
    }

    /**
     * Drains (i.e. <em>removes</em>) the contents of the given {@link CompletedFetch} as its data
     * should not be returned to the user.
     */
    private boolean maybeDrain(Set<TableBucket> buckets) {
        boolean hasDrain = false;
        for (TableBucket bucket : buckets) {
            if (completedFetches.containsKey(bucket)) {
                completedFetches
                        .get(bucket)
                        .forEach((completedFetch) -> maybeDrain(buckets, completedFetch));
                removeCompleteBucket(bucket);
                hasDrain = true;
            }
        }
        return hasDrain;
    }

    /**
     * Drains (i.e. <em>removes</em>) the contents of the given {@link CompletedFetch} as its data
     * should not be returned to the user.
     */
    private boolean maybeDrain(Set<TableBucket> buckets, CompletedFetch completedFetch) {
        if (completedFetch != null && !buckets.contains(completedFetch.tableBucket)) {
            LOG.debug(
                    "Removing {} from buffered fetch data as it is not in the set of buckets to retain ({})",
                    completedFetch.tableBucket,
                    buckets);
            completedFetch.drain();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Return the set of {@link TableBucket buckets} for which we have data in the buffer.
     *
     * @return {@link TableBucket bucket} set
     */
    @Nullable
    Set<TableBucket> bufferedBuckets() {
        return inLock(
                lock,
                () -> {
                    // If there are any pending fetches which have not been added to
                    // completedFetches, we will return null. For example, a possible scenario is
                    // that the remote log downloader can not download remote log as soon as
                    // possible. In this case, we can't return any buckets to avoid OOM cause by the
                    // frequently fetch log request send to server to fetch log back, which the
                    // fetch data can not consume timely and will be buffered in memory.
                    // TODO this is a hack logic to avoid OOM, we should fix it later to refactor
                    // the remote log download logic.
                    if (!pendingFetches.isEmpty()) {
                        return null;
                    }

                    final Set<TableBucket> buckets = new HashSet<>(completedFetches.keySet());
                    if (nextInLineFetch != null && !nextInLineFetch.isConsumed()) {
                        buckets.add(nextInLineFetch.tableBucket);
                    }
                    return buckets;
                });
    }

    /** Return the set of {@link TableBucket buckets} for which we have pending fetches. */
    Set<TableBucket> pendedBuckets() {
        return inLock(lock, pendingFetches::keySet);
    }

    void addCompleteBucket(CompletedFetch completedFetch) {
        LinkedList<CompletedFetch> tableBucketCompletedFetches;
        if (!completedFetches.containsKey(completedFetch.tableBucket)) {
            tableBucketCompletedFetches = new LinkedList<>();
            completeTableBuckets.addLast(completedFetch.tableBucket);
        } else {
            tableBucketCompletedFetches = completedFetches.get(completedFetch.tableBucket);
        }
        tableBucketCompletedFetches.add(completedFetch);
        completedFetches.put(completedFetch.tableBucket, tableBucketCompletedFetches);
    }

    void addPendingBucket(PendingFetch pendingFetch) {
        LinkedList<PendingFetch> tableBucketPendingFetches;
        if (!pendingFetches.containsKey(pendingFetch.tableBucket())) {
            tableBucketPendingFetches = new LinkedList<>();
        } else {
            tableBucketPendingFetches = pendingFetches.get(pendingFetch.tableBucket());
        }

        tableBucketPendingFetches.add(pendingFetch);
        pendingFetches.put(pendingFetch.tableBucket(), tableBucketPendingFetches);
    }

    void removeCompleteBucket(TableBucket tableBucket) {
        completedFetches.remove(tableBucket);
        completeTableBuckets.remove(tableBucket);
    }

    @Override
    public void close() throws Exception {
        inLock(lock, () -> retainAll(Collections.emptySet()));
    }
}
