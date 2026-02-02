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

package org.apache.fluss.server.replica.changelog;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.utils.function.ThrowingRunnable;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manager for handling changelog processors for primary-key tables.
 *
 * <p>This manager coordinates all the changelog processors for primary-key tables,
 * ensuring proper lifecycle management and coordination with tiering services.
 */
public class PkTableChangelogManager implements Closeable {

    @GuardedBy("processorsLock")
    private final Map<TableBucket, PkTableChangelogProcessor> processors = new HashMap<>();

    private final Map<TableBucket, ReentrantLock> bucketLocks = new ConcurrentHashMap<>();
    private final RemoteLogManager remoteLogManager;

    public PkTableChangelogManager(RemoteLogManager remoteLogManager) {
        this.remoteLogManager = remoteLogManager;
    }

    /**
     * Get or create a changelog processor for the given replica.
     *
     * @param replica the replica
     * @return the changelog processor for the replica
     */
    public PkTableChangelogProcessor getOrCreateProcessor(Replica replica) {
        TableBucket tableBucket = replica.getTableBucket();
        ReentrantLock lock = getBucketLock(tableBucket);

        lock.lock();
        try {
            PkTableChangelogProcessor processor = processors.get(tableBucket);
            if (processor == null) {
                processor = new PkTableChangelogProcessor(replica, remoteLogManager);
                processors.put(tableBucket, processor);
            }
            return processor;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove the changelog processor for the given replica.
     *
     * @param replica the replica
     */
    public void removeProcessor(Replica replica) {
        TableBucket tableBucket = replica.getTableBucket();
        ReentrantLock lock = getBucketLock(tableBucket);

        lock.lock();
        try {
            PkTableChangelogProcessor processor = processors.remove(tableBucket);
            if (processor != null) {
                processor.close();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Execute an operation with the processor for the given replica.
     *
     * @param replica the replica
     * @param operation the operation to execute
     */
    public void executeWithProcessor(Replica replica, ThrowingRunnable<Exception> operation) throws Exception {
        TableBucket tableBucket = replica.getTableBucket();
        ReentrantLock lock = getBucketLock(tableBucket);

        lock.lock();
        try {
            operation.run();
        } finally {
            lock.unlock();
        }
    }

    private ReentrantLock getBucketLock(TableBucket tableBucket) {
        return bucketLocks.computeIfAbsent(tableBucket, k -> new ReentrantLock());
    }

    @Override
    public void close() {
        for (PkTableChangelogProcessor processor : processors.values()) {
            processor.close();
        }
        processors.clear();
        bucketLocks.clear();
    }
}