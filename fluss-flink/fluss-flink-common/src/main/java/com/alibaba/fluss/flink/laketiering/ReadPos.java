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

package com.alibaba.fluss.flink.laketiering;

/** . */
public class ReadPos<WriteResult> {

    // the read records count include this record when read this record
    private final long readRecordsCount;

    private final long offset;

    public TableBucketWriteResult<WriteResult> writeResult;

    public ReadPos(
            long readRecordsCount, long offset, TableBucketWriteResult<WriteResult> writeResult) {
        this.readRecordsCount = readRecordsCount;
        this.offset = offset;
        this.writeResult = writeResult;
    }

    public long getReadRecordsCount() {
        return readRecordsCount;
    }

    public long getOffset() {
        return offset;
    }

    public TableBucketWriteResult<WriteResult> getWriteResult() {
        return writeResult;
    }
}
