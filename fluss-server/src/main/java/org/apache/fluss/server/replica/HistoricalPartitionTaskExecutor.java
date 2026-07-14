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

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Executes historical partition tasks serially per table bucket.
 *
 * <p>Only one task for a table bucket may run at a time, while tasks for different buckets may run
 * concurrently when the underlying executor permits it. A task failure is reported through its
 * future and does not stop later tasks in the same queue.
 *
 * <p>Cancelling a bucket rejects queued and new tasks but does not interrupt its running task. The
 * bucket must be {@link #reset(TableBucket) reset} before accepting new work. Closing the executor
 * follows the same drain rule: queued tasks are rejected and termination waits for running tasks.
 */
final class HistoricalPartitionTaskExecutor implements AutoCloseable {

    private final Object lock = new Object();
    private final Executor executor;
    private final Map<TableBucket, BucketQueue> queues = new HashMap<>();
    private final Set<TableBucket> cancelledBuckets = new HashSet<>();
    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private boolean closed;

    /** Creates a per-bucket task executor backed by the given shared executor. */
    HistoricalPartitionTaskExecutor(Executor executor) {
        this.executor = checkNotNull(executor, "executor must not be null");
    }

    /** Enqueues a task after existing work for the same table bucket. */
    <T> CompletableFuture<T> submit(TableBucket tableBucket, Callable<T> callable) {
        checkNotNull(tableBucket, "tableBucket must not be null");
        checkNotNull(callable, "callable must not be null");

        QueuedTask<T> task = new QueuedTask<>(callable);
        BucketQueue queue;
        boolean schedule;
        synchronized (lock) {
            if (closed) {
                task.future.completeExceptionally(
                        new RejectedExecutionException(
                                "Historical partition task executor is closed."));
                return task.future;
            }
            if (cancelledBuckets.contains(tableBucket)) {
                task.future.completeExceptionally(
                        new RejectedExecutionException(
                                "Historical partition task queue is cancelled for "
                                        + tableBucket
                                        + '.'));
                return task.future;
            }
            queue = queues.computeIfAbsent(tableBucket, ignored -> new BucketQueue());
            queue.tasks.add(task);
            // Only the submitter that observes an idle queue schedules its runner. Mark the queue
            // running under lock so concurrent submitters only enqueue their tasks.
            schedule = !queue.running;
            queue.running = true;
        }

        if (schedule) {
            scheduleNext(tableBucket, queue);
        }
        return task.future;
    }

    /**
     * Rejects queued and future tasks for a bucket without interrupting the task currently running.
     */
    void cancel(TableBucket tableBucket, Throwable cause) {
        List<QueuedTask<?>> cancelledTasks = new ArrayList<>();
        synchronized (lock) {
            cancelledBuckets.add(tableBucket);
            BucketQueue queue = queues.get(tableBucket);
            if (queue == null) {
                queue = new BucketQueue();
                queue.cancelled = true;
                queues.put(tableBucket, queue);
            } else {
                queue.cancelled = true;
                QueuedTask<?> task;
                while ((task = queue.tasks.poll()) != null) {
                    cancelledTasks.add(task);
                }
            }
        }
        cancelledTasks.forEach(task -> task.future.completeExceptionally(cause));
    }

    /** Allows a previously cancelled bucket to accept tasks again. */
    void reset(TableBucket tableBucket) {
        synchronized (lock) {
            cancelledBuckets.remove(tableBucket);
            BucketQueue queue = queues.get(tableBucket);
            if (queue != null) {
                if (queue.running) {
                    queue.cancelled = false;
                } else if (queue.tasks.isEmpty()) {
                    queues.remove(tableBucket, queue);
                }
            }
        }
    }

    /** Rejects queued work and begins waiting for already running tasks to finish. */
    @Override
    public void close() {
        List<QueuedTask<?>> cancelledTasks = new ArrayList<>();
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            cancelledBuckets.clear();
            for (BucketQueue queue : queues.values()) {
                queue.cancelled = true;
                QueuedTask<?> task;
                while ((task = queue.tasks.poll()) != null) {
                    cancelledTasks.add(task);
                }
            }
            maybeCompleteTermination();
        }
        RejectedExecutionException cause =
                new RejectedExecutionException("Historical partition task executor is closed.");
        cancelledTasks.forEach(task -> task.future.completeExceptionally(cause));
    }

    /**
     * Waits up to the given timeout for all running tasks to finish after {@link #close()}.
     *
     * @return whether all running tasks finished before the timeout
     */
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        checkNotNull(unit, "unit must not be null");
        try {
            terminationFuture.get(timeout, unit);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("Historical task executor termination failed.", e);
        }
    }

    /** Schedules the next queue step without holding the state lock. */
    private void scheduleNext(TableBucket tableBucket, BucketQueue queue) {
        try {
            executor.execute(() -> runNext(tableBucket, queue));
        } catch (RuntimeException e) {
            rejectQueue(tableBucket, queue, e);
        }
    }

    /** Runs one task and schedules another step if the bucket still has pending work. */
    private void runNext(TableBucket tableBucket, BucketQueue queue) {
        QueuedTask<?> task;
        synchronized (lock) {
            // The queue may have been cancelled or replaced after this step was scheduled.
            if (queues.get(tableBucket) != queue || queue.cancelled) {
                markQueueIdle(tableBucket, queue);
                return;
            }
            task = queue.tasks.poll();
            if (task == null) {
                markQueueIdle(tableBucket, queue);
                return;
            }
        }

        // Do not hold the state lock while running user code.
        task.run();

        synchronized (lock) {
            if (queue.cancelled || queue.tasks.isEmpty()) {
                markQueueIdle(tableBucket, queue);
                return;
            }
        }

        // Schedule outside lock because a direct executor may invoke runNext immediately.
        scheduleNext(tableBucket, queue);
    }

    /** Fails all pending work when the backing executor rejects a queue step. */
    private void rejectQueue(TableBucket tableBucket, BucketQueue queue, RuntimeException cause) {
        List<QueuedTask<?>> rejectedTasks = new ArrayList<>();
        synchronized (lock) {
            if (queues.get(tableBucket) != queue) {
                return;
            }
            QueuedTask<?> task;
            while ((task = queue.tasks.poll()) != null) {
                rejectedTasks.add(task);
            }
            markQueueIdle(tableBucket, queue);
        }
        rejectedTasks.forEach(task -> task.future.completeExceptionally(cause));
    }

    /** Marks a queue idle, removes it once drained, and rechecks executor termination. */
    @GuardedBy("lock")
    private void markQueueIdle(TableBucket tableBucket, BucketQueue queue) {
        queue.running = false;
        if (queue.tasks.isEmpty()) {
            queues.remove(tableBucket, queue);
        }
        maybeCompleteTermination();
    }

    /**
     * Completes termination after close once no bucket has a running task.
     *
     * <p>This method is called whenever a queue may have transitioned to idle. Before close it is a
     * no-op; after close, the last running queue completes the termination future.
     */
    @GuardedBy("lock")
    private void maybeCompleteTermination() {
        if (!closed) {
            return;
        }
        for (BucketQueue queue : queues.values()) {
            if (queue.running) {
                return;
            }
        }
        terminationFuture.complete(null);
    }

    /** Mutable scheduling state for one table bucket. */
    private static final class BucketQueue {
        private final Queue<QueuedTask<?>> tasks = new ArrayDeque<>();
        private boolean running;
        private boolean cancelled;
    }

    /** Couples one callable with the future used to report its result. */
    private static final class QueuedTask<T> {
        private final Callable<T> callable;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        private QueuedTask(Callable<T> callable) {
            this.callable = callable;
        }

        /** Captures both checked exceptions and fatal task failures in the result future. */
        private void run() {
            try {
                future.complete(callable.call());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        }
    }
}
