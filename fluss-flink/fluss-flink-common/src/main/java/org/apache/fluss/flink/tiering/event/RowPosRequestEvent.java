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

package org.apache.fluss.flink.tiering.event;

import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.List;
import java.util.Map;

/**
 * SourceEvent sent from Committer to Enumerator requesting row position scans for compaction output
 * files.
 *
 * <p>Splits are pre-serialized by the Committer using {@code LakeSource.getSplitSerializer()}. This
 * decouples the Enumerator (in JobManager coordinator thread) from lake-format-specific classes.
 *
 * <p>The map structure groups splits by partition and bucket, so the Enumerator knows
 * partition/bucket context for creating {@code TieringRowPosSplit} with proper {@code TableBucket}
 * without deserializing the splits.
 *
 * <p>Deleted files from {@code DataDeltaPlan} are NOT included in this event — they are stored and
 * handled locally by the Committer.
 */
public class RowPosRequestEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final long tableId;
    private final TablePath tablePath;
    private final int nextFileId;

    /**
     * Serialized lake splits grouped by partition name and bucket ID. Key: partition name (null key
     * for non-partitioned tables). Value: map from bucket ID to list of serialized LakeSplit bytes.
     */
    private final Map<String, Map<Integer, List<byte[]>>> serializedSplitsByPartitionAndBucket;

    public RowPosRequestEvent(
            long tableId,
            TablePath tablePath,
            int nextFileId,
            Map<String, Map<Integer, List<byte[]>>> serializedSplitsByPartitionAndBucket) {
        this.tableId = tableId;
        this.tablePath = tablePath;
        this.nextFileId = nextFileId;
        this.serializedSplitsByPartitionAndBucket = serializedSplitsByPartitionAndBucket;
    }

    public long getTableId() {
        return tableId;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public int getNextFileId() {
        return nextFileId;
    }

    public Map<String, Map<Integer, List<byte[]>>> getSerializedSplitsByPartitionAndBucket() {
        return serializedSplitsByPartitionAndBucket;
    }
}
