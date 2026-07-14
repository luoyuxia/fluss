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
import org.apache.fluss.server.kv.historical.HistoricalKvHandle;
import org.apache.fluss.server.kv.historical.HistoricalKvManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Coordinates lazy recovery and lifecycle invalidation for historical KV state. */
final class HistoricalKvLifecycleManager {

    private final Object lock = new Object();
    private final HistoricalKvManager historicalKvManager;
    private final HistoricalPartitionTaskExecutor taskExecutor;
    private final HistoricalKvRecoverer recoverer;
    private final Map<TableBucket, CompletableFuture<Void>> recoveryFutures = new HashMap<>();

    HistoricalKvLifecycleManager(
            HistoricalKvManager historicalKvManager,
            HistoricalPartitionTaskExecutor taskExecutor,
            HistoricalKvRecoverer recoverer) {
        this.historicalKvManager =
                checkNotNull(historicalKvManager, "historicalKvManager must not be null");
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.recoverer = checkNotNull(recoverer, "recoverer must not be null");
    }

    CompletableFuture<Void> ensureRecovered(Replica replica) {
        TableBucket tableBucket = replica.getTableBucket();
        if (historicalKvManager.getIfPresent(tableBucket).isPresent()) {
            return CompletableFuture.completedFuture(null);
        }

        synchronized (lock) {
            CompletableFuture<Void> current = recoveryFutures.get(tableBucket);
            if (current != null) {
                return current;
            }
            CompletableFuture<Void> recovery =
                    taskExecutor.submit(
                            tableBucket,
                            () -> {
                                recoverIfNeeded(replica);
                                return null;
                            });
            recoveryFutures.put(tableBucket, recovery);
            recovery.whenComplete(
                    (ignored, error) -> {
                        synchronized (lock) {
                            recoveryFutures.remove(tableBucket, recovery);
                        }
                    });
            return recovery;
        }
    }

    void recoverIfNeeded(Replica replica) throws Exception {
        if (historicalKvManager.getIfPresent(replica.getTableBucket()).isPresent()) {
            return;
        }
        replica.recoverHistoricalKv(() -> recoverer.recover(replica));
    }

    void resetBucket(TableBucket tableBucket) {
        taskExecutor.reset(tableBucket);
    }

    void invalidateBucket(TableBucket tableBucket) {
        taskExecutor.cancel(
                tableBucket,
                new CancellationException(
                        "Historical partition lifecycle changed for " + tableBucket + '.'));
        historicalKvManager.invalidateBucket(tableBucket);
    }

    CompletableFuture<Boolean> cleanupIfFullyTiered(
            Replica replica,
            HistoricalKvHandle expectedHandle,
            boolean requireIdle,
            long nowMs,
            long idleTimeoutMs) {
        return taskExecutor.submit(
                replica.getTableBucket(),
                () -> {
                    if (!replica.isLeader() || !replica.isHistoricalPartition()) {
                        return false;
                    }
                    HistoricalKvHandle current =
                            historicalKvManager.getIfPresent(replica.getTableBucket()).orElse(null);
                    if (current == null
                            || current != expectedHandle
                            || replica.getLakeLogEndOffset() < replica.getLocalLogEndOffset()) {
                        return false;
                    }
                    if (requireIdle && nowMs - current.getLastAccessTime() < idleTimeoutMs) {
                        return false;
                    }
                    return historicalKvManager.tryInvalidateBucket(
                            replica.getTableBucket(), current);
                });
    }
}
