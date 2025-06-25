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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LakeLogRecords;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.rpc.protocol.ApiError;

/**
 * {@link LakeCompletedFetch} is a {@link CompletedFetch} that represents a completed fetch that the
 * log records are fetched from datalake log storage.
 */
public class LakeCompletedFetch extends CompletedFetch {

    public LakeCompletedFetch(
            TableBucket tableBucket,
            LakeLogRecords lakeLogRecords,
            long highWatermark,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrc,
            long fetchOffset) {
        super(
                tableBucket,
                ApiError.NONE,
                lakeLogRecords.sizeInBytes(),
                highWatermark,
                lakeLogRecords.batches().iterator(),
                readContext,
                logScannerStatus,
                isCheckCrc,
                fetchOffset);
    }
}
