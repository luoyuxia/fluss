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

import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.PutKvDataForBucket;
import org.apache.fluss.server.kv.historical.HistoricalKvHandle;
import org.apache.fluss.server.kv.historical.HistoricalKvManager;
import org.apache.fluss.server.kv.historical.HistoricalKvStateAccessor;
import org.apache.fluss.server.log.LogAppendInfo;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Processes historical primary-key writes without exposing an online RPC dispatch. */
final class HistoricalPkWriteProcessor {

    private final HistoricalKvManager historicalKvManager;
    private final HistoricalLakeLookupManager lakeLookupManager;

    HistoricalPkWriteProcessor(
            HistoricalKvManager historicalKvManager,
            HistoricalLakeLookupManager lakeLookupManager) {
        this.historicalKvManager =
                checkNotNull(historicalKvManager, "historicalKvManager must not be null");
        this.lakeLookupManager =
                checkNotNull(lakeLookupManager, "lakeLookupManager must not be null");
    }

    LogAppendInfo process(
            Replica replica,
            PutKvDataForBucket putData,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            int requiredAcks)
            throws Exception {
        return replica.putHistoricalRecordsToLeader(
                requiredAcks,
                () -> {
                    TableInfo tableInfo = replica.getTableInfo();
                    String originalPartitionName =
                            checkNotNull(
                                    putData.originalPartitionName(),
                                    "originalPartitionName must not be null");
                    ResolvedPartitionSpec originalPartitionSpec =
                            ResolvedPartitionSpec.fromPartitionName(
                                    tableInfo.getPartitionKeys(), originalPartitionName);
                    HistoricalKvHandle handle =
                            historicalKvManager.getOrCreate(
                                    putData.tableBucket(), replica.getKvTabletDir());
                    HistoricalKvStateAccessor localAccessor =
                            new HistoricalKvStateAccessor(handle, originalPartitionName);
                    HistoricalLakeFallbackStateAccessor stateAccessor =
                            new HistoricalLakeFallbackStateAccessor(
                                    localAccessor,
                                    primaryKey ->
                                            lakeLookupManager.lookupValue(
                                                    tableInfo,
                                                    originalPartitionSpec,
                                                    putData.tableBucket().getBucket(),
                                                    primaryKey));

                    return handle.withWriteLock(
                            () ->
                                    replica.getOrCreateHistoricalKvWriteProcessor()
                                            .putAsLeader(
                                                    putData.records(),
                                                    targetColumns,
                                                    mergeMode,
                                                    stateAccessor));
                });
    }
}
