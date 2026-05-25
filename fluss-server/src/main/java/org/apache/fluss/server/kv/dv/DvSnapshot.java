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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * A point-in-time snapshot of the Deletion Vectors for a single bucket, used for union read.
 *
 * <p>Contains:
 *
 * <ul>
 *   <li><b>lakeDvEntries</b>: per-file deletion vectors (filePath → serialized Roaring64Bitmap of
 *       deleted row positions)
 *   <li><b>logDvBitmap</b>: serialized Roaring64Bitmap of deleted log offsets within
 *       [snapshotStartOffset, logEndOffset)
 *   <li><b>logEndOffset</b>: the log end offset at snapshot time
 *   <li><b>snapshotStartOffset</b>: the log start offset of the DV snapshot
 * </ul>
 */
@Internal
public class DvSnapshot {

    private final Map<String, byte[]> lakeDvEntries;
    @Nullable private final byte[] logDvBitmap;
    private final long logEndOffset;
    private final long snapshotStartOffset;

    public DvSnapshot(
            Map<String, byte[]> lakeDvEntries,
            @Nullable byte[] logDvBitmap,
            long logEndOffset,
            long snapshotStartOffset) {
        this.lakeDvEntries = lakeDvEntries;
        this.logDvBitmap = logDvBitmap;
        this.logEndOffset = logEndOffset;
        this.snapshotStartOffset = snapshotStartOffset;
    }

    /** Returns per-file deletion vectors: filePath to serialized Roaring64Bitmap. */
    public Map<String, byte[]> getLakeDvEntries() {
        return lakeDvEntries;
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
