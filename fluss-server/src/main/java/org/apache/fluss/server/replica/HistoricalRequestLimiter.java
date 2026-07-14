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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Shared admission control for historical lookup and write bucket requests. */
final class HistoricalRequestLimiter {

    private final Semaphore permits;

    HistoricalRequestLimiter(Configuration conf) {
        this(conf.get(ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS));
    }

    HistoricalRequestLimiter(int maxRequests) {
        checkArgument(
                maxRequests > 0,
                "%s must be greater than 0.",
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
        this.permits = new Semaphore(maxRequests);
    }

    Optional<Permit> tryAcquire() {
        return permits.tryAcquire() ? Optional.of(new Permit(permits)) : Optional.empty();
    }

    /** A permit that releases its slot at most once. */
    static final class Permit implements AutoCloseable {

        private final Semaphore permits;
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit(Semaphore permits) {
            this.permits = permits;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                permits.release();
            }
        }
    }
}
