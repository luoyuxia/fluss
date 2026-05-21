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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DvRWLock}. */
class DvRWLockTest {

    @Test
    void testConcurrentReaders() throws Exception {
        DvRWLock lock = new DvRWLock();
        int numReaders = 5;
        CountDownLatch allReadersAcquired = new CountDownLatch(numReaders);
        CountDownLatch releaseAll = new CountDownLatch(1);
        AtomicInteger concurrentReaders = new AtomicInteger(0);
        AtomicInteger maxConcurrentReaders = new AtomicInteger(0);

        for (int i = 0; i < numReaders; i++) {
            new Thread(
                            () -> {
                                lock.readLock();
                                try {
                                    int current = concurrentReaders.incrementAndGet();
                                    maxConcurrentReaders.updateAndGet(
                                            max -> Math.max(max, current));
                                    allReadersAcquired.countDown();
                                    releaseAll.await(5, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                } finally {
                                    concurrentReaders.decrementAndGet();
                                    lock.readUnlock();
                                }
                            })
                    .start();
        }

        assertThat(allReadersAcquired.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(maxConcurrentReaders.get()).isEqualTo(numReaders);
        releaseAll.countDown();
    }

    @Test
    void testWriteLockExclusive() throws Exception {
        DvRWLock lock = new DvRWLock();
        CountDownLatch writerAcquired = new CountDownLatch(1);
        CountDownLatch writerRelease = new CountDownLatch(1);
        AtomicBoolean readerAcquiredDuringWrite = new AtomicBoolean(false);

        // Writer thread
        Thread writer =
                new Thread(
                        () -> {
                            lock.writeLock();
                            try {
                                writerAcquired.countDown();
                                writerRelease.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                lock.writeUnlock();
                            }
                        });
        writer.start();

        assertThat(writerAcquired.await(5, TimeUnit.SECONDS)).isTrue();

        // Try to acquire read lock while write lock is held
        CountDownLatch readerAttempted = new CountDownLatch(1);
        Thread reader =
                new Thread(
                        () -> {
                            readerAttempted.countDown();
                            lock.readLock();
                            try {
                                readerAcquiredDuringWrite.set(true);
                            } finally {
                                lock.readUnlock();
                            }
                        });
        reader.start();

        readerAttempted.await(5, TimeUnit.SECONDS);
        // Give reader thread time to attempt lock acquisition
        Thread.sleep(100);
        // Reader should be blocked
        assertThat(readerAcquiredDuringWrite.get()).isFalse();

        // Release writer, reader should then proceed
        writerRelease.countDown();
        writer.join(5000);
        reader.join(5000);
        assertThat(readerAcquiredDuringWrite.get()).isTrue();
    }
}
