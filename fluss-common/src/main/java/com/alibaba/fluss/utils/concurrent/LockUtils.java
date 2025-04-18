/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils.concurrent;

import com.alibaba.fluss.utils.function.SupplierWithException;
import com.alibaba.fluss.utils.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/** Utils for {@link Lock}. */
public class LockUtils {

    private static final AtomicLong atomicLong = new AtomicLong(0);

    private static final Logger LOG = LoggerFactory.getLogger(LockUtils.class);

    public static <E extends Exception> void inLock(Lock lock, ThrowingRunnable<E> runnable)
            throws E {
        lock.lock();
        long id = atomicLong.incrementAndGet();
        LOG.info(
                "lock id {}, current stacktrace {}",
                id,
                Arrays.toString(Thread.currentThread().getStackTrace()));
        try {
            runnable.run();
        } finally {
            lock.unlock();
            LOG.info("exit lock id {}", id);
        }
    }

    public static <T, E extends Exception> T inLock(Lock lock, SupplierWithException<T, E> action)
            throws E {
        lock.lock();
        long id = atomicLong.incrementAndGet();
        LOG.info(
                "lock id {}, current stacktrace {}",
                id,
                Arrays.toString(Thread.currentThread().getStackTrace()));
        try {
            return action.get();
        } finally {
            lock.unlock();
            LOG.info("exit lock id {}", id);
        }
    }

    public static <E extends Exception> void inReadLock(
            ReadWriteLock lock, ThrowingRunnable<E> runnable) throws E {
        inLock(lock.readLock(), runnable);
    }

    public static <T, E extends Exception> T inReadLock(
            ReadWriteLock lock, SupplierWithException<T, E> action) throws E {
        return inLock(lock.readLock(), action);
    }

    public static <E extends Exception> void inWriteLock(
            ReadWriteLock lock, ThrowingRunnable<E> runnable) throws E {
        inLock(lock.writeLock(), runnable);
    }

    public static <T, E extends Exception> T inWriteLock(
            ReadWriteLock lock, SupplierWithException<T, E> action) throws E {
        return inLock(lock.writeLock(), action);
    }
}
