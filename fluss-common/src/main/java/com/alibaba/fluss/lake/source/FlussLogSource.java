/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.source;

import com.alibaba.fluss.exception.InvalidTimestampException;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Provides access to log-based data(data of Fluss log table, change log of Fluss primary key table)
 * in lake storage. Supports time-based lookup log and log record fetching operations.
 */
public interface FlussLogSource {

    /**
     * Finds the log offset corresponding to the specified timestamp for the Fluss data of given
     * {@code partitionName} and {@code bucket} in the lake snapshot. It should return
     *
     * @param partitionName target partition (null for un-partitioned table)
     * @param bucket target bucket
     * @param timestamp the target timestamp to lookup
     * @param lakeSnapshotId the lake snapshot id to lookup from
     * @return the first offset that bigger than or equal given {@code timestamp}
     * @throws InvalidTimestampException if the {@code timestamp} is larger than max timestamp or
     *     less than min timestamp in the lake
     */
    long lookupLogOffsetByTimeStamp(
            @Nullable String partitionName, int bucket, long timestamp, long lakeSnapshotId)
            throws InvalidTimestampException;

    /**
     * Fetches log records for the Fluss data in datalake.
     *
     * @param fetchContext the context to fetch in datalake
     * @return An iterator of LogRecords
     */
    CloseableIterator<LogRecord> fetchLogRecords(FetchContext fetchContext) throws IOException;
}
