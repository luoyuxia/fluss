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

package org.apache.fluss.flink.lake;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * A lightweight carrier class holding DV snapshot data for a single bucket. This data is fetched by
 * the Enumerator and attached to {@link
 * org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit} for the Reader to apply DV
 * filtering.
 *
 * <p>Serialization/deserialization is handled by {@link LakeSplitSerializer}, not by this class
 * itself.
 */
@Internal
public class DvSnapshotInfo {

    /** Per-file deletion vectors: filePath to serialized Roaring64Bitmap of deleted positions. */
    private final Map<String, byte[]> lakeDv;

    /** Serialized Roaring64Bitmap of deleted log offsets. Null if empty. */
    @Nullable private final byte[] logDvBitmap;

    /**
     * The log end offset at snapshot time. LogDv applies to [snapshotStartOffset, logEndOffset).
     */
    private final long logEndOffset;

    /** The log start offset of the DV snapshot. */
    private final long snapshotStartOffset;

    public DvSnapshotInfo(
            Map<String, byte[]> lakeDv,
            @Nullable byte[] logDvBitmap,
            long logEndOffset,
            long snapshotStartOffset) {
        this.lakeDv = lakeDv;
        this.logDvBitmap = logDvBitmap;
        this.logEndOffset = logEndOffset;
        this.snapshotStartOffset = snapshotStartOffset;
    }

    /** Returns per-file deletion vectors: filePath to serialized Roaring64Bitmap. */
    public Map<String, byte[]> getLakeDv() {
        return lakeDv;
    }

    /** Returns the serialized Roaring64Bitmap of deleted log offsets. Null if empty. */
    @Nullable
    public byte[] getLogDvBitmap() {
        return logDvBitmap;
    }

    /** Returns the log end offset at snapshot time. */
    public long getLogEndOffset() {
        return logEndOffset;
    }

    /** Returns the log start offset of the DV snapshot. */
    public long getSnapshotStartOffset() {
        return snapshotStartOffset;
    }
}
