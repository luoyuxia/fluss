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

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.metadata.TableBucket;

import java.util.HashMap;
import java.util.Map;

/** A lake snapshot for a table. */
public class TableLakeSnapshot {

    private final long tableId;

    private final long snapshotId;

    private final Map<TableBucket, Long> logEndOffsets;

    public TableLakeSnapshot(long tableId, long snapshotId) {
        this(tableId, snapshotId, new HashMap<>());
    }

    public TableLakeSnapshot(long tableId, long snapshotId, Map<TableBucket, Long> logEndOffsets) {
        this.tableId = tableId;
        this.snapshotId = snapshotId;
        this.logEndOffsets = logEndOffsets;
    }

    public long tableId() {
        return tableId;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public Map<TableBucket, Long> logEndOffsets() {
        return logEndOffsets;
    }

    public void addBucketOffset(TableBucket bucket, long offset) {
        logEndOffsets.put(bucket, offset);
    }
}
