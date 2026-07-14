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

import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.log.LogAppendInfo;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** I/O-executor entry for focused historical PK write processing. */
final class HistoricalPkWriteManager {

    private final HistoricalPkWriteProcessor processor;
    private final HistoricalPartitionTaskExecutor taskExecutor;
    private final HistoricalKvLifecycleManager lifecycleManager;

    HistoricalPkWriteManager(
            HistoricalPkWriteProcessor processor,
            HistoricalPartitionTaskExecutor taskExecutor,
            HistoricalKvLifecycleManager lifecycleManager) {
        this.processor = checkNotNull(processor, "processor must not be null");
        this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor must not be null");
        this.lifecycleManager = checkNotNull(lifecycleManager, "lifecycleManager must not be null");
    }

    CompletableFuture<PutKvResultForBucket> put(
            Replica replica,
            PutKvDataForBucket putData,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            int requiredAcks) {
        return taskExecutor
                .submit(
                        putData.tableBucket(),
                        () -> {
                            try {
                                lifecycleManager.recoverIfNeeded(replica);
                                LogAppendInfo appendInfo =
                                        processor.process(
                                                replica,
                                                putData,
                                                targetColumns,
                                                mergeMode,
                                                requiredAcks);
                                return new PutKvResultForBucket(
                                        putData.tableBucket(), appendInfo.lastOffset() + 1);
                            } catch (Throwable t) {
                                return new PutKvResultForBucket(
                                        putData.tableBucket(), ApiError.fromThrowable(t));
                            }
                        })
                .exceptionally(
                        error ->
                                new PutKvResultForBucket(
                                        putData.tableBucket(), ApiError.fromThrowable(error)));
    }
}
