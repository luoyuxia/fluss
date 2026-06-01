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

package org.apache.fluss.flink.source.split;

import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** The split for log. It's used to describe the log data of a table bucket. */
public class LogSplit extends SourceSplitBase {

    public static final long NO_STOPPING_OFFSET = LogScanner.NO_STOPPING_OFFSET;

    private static final String LOG_SPLIT_PREFIX = "log-";

    private final long startingOffset;
    private final long stoppingOffset;

    /**
     * Serialized Roaring64Bitmap of deleted log offsets for DV batch read. Null means no DV
     * filtering is needed.
     */
    @Nullable private final byte[] logDvBitmap;

    public LogSplit(TableBucket tableBucket, @Nullable String partitionName, long startingOffset) {
        this(tableBucket, partitionName, startingOffset, NO_STOPPING_OFFSET, null);
    }

    public LogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset) {
        this(tableBucket, partitionName, startingOffset, stoppingOffset, null);
    }

    public LogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            @Nullable byte[] logDvBitmap) {
        super(tableBucket, partitionName);
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.logDvBitmap = logDvBitmap;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset >= 0 ? Optional.of(stoppingOffset) : Optional.empty();
    }

    /**
     * Returns the serialized Roaring64Bitmap of deleted log offsets. Null means no DV filtering is
     * needed.
     */
    @Nullable
    public byte[] getLogDvBitmap() {
        return logDvBitmap;
    }

    /** Returns true if this is a DV batch read split (log DV data is available). */
    public boolean isDvBatchRead() {
        return logDvBitmap != null;
    }

    @Override
    protected byte splitKind() {
        return LOG_SPLIT_FLAG;
    }

    @Override
    public String splitId() {
        return toSplitId(LOG_SPLIT_PREFIX, tableBucket);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LogSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        LogSplit logSplit = (LogSplit) object;
        return startingOffset == logSplit.startingOffset
                && stoppingOffset == logSplit.stoppingOffset
                && Arrays.equals(logDvBitmap, logSplit.logDvBitmap);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), startingOffset, stoppingOffset);
        result = 31 * result + Arrays.hashCode(logDvBitmap);
        return result;
    }

    @Override
    public String toString() {
        return "LogSplit{"
                + "tableBucket="
                + tableBucket
                + ", partitionName='"
                + partitionName
                + '\''
                + ", startingOffset="
                + startingOffset
                + ", stoppingOffset="
                + stoppingOffset
                + ", hasDvBitmap="
                + (logDvBitmap != null)
                + '}';
    }
}
