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

package org.apache.fluss.server.kv.dv;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A read-write lock wrapper for controlling concurrent access to DV data structures.
 *
 * <p>Write lock holders: changelog sync write path (DvManager batch processing -U/-D), Prepare
 * Phase 2 (write FileDict + store SST paths), Readable Switch (Ingest SST + batch resolve +
 * cleanup).
 *
 * <p>Read lock holders: Union Read (read LakeDv + LogDv snapshots).
 */
public class DvRWLock {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }
}
