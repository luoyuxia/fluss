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

package org.apache.fluss.flink.lake.split;

import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** A DV-aware split for reading a bounded log range without sort-merge. */
public class DvAwareFlussLogSplit extends SourceSplitBase {

    public static final byte DV_AWARE_FLUSS_LOG_SPLIT_KIND = -5;

    private final long startingOffset;
    private final long stoppingOffset;
    private final Map<Long, byte[]> logDvSnapshot;

    public DvAwareFlussLogSplit(
            TableBucket tableBucket,
            @Nullable String partitionName,
            long startingOffset,
            long stoppingOffset,
            @Nullable Map<Long, byte[]> logDvSnapshot) {
        super(tableBucket, partitionName);
        this.startingOffset = startingOffset;
        this.stoppingOffset = stoppingOffset;
        this.logDvSnapshot =
                logDvSnapshot == null ? Collections.emptyMap() : new LinkedHashMap<>(logDvSnapshot);
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    public Optional<Long> getStoppingOffset() {
        return stoppingOffset >= 0 ? Optional.of(stoppingOffset) : Optional.empty();
    }

    public Map<Long, byte[]> getLogDvSnapshot() {
        return logDvSnapshot;
    }

    @Override
    public String splitId() {
        return toSplitId("lake-dv-log-", tableBucket);
    }

    @Override
    public boolean isLakeSplit() {
        return true;
    }

    @Override
    public byte splitKind() {
        return DV_AWARE_FLUSS_LOG_SPLIT_KIND;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DvAwareFlussLogSplit)) {
            return false;
        }
        if (!super.equals(object)) {
            return false;
        }
        DvAwareFlussLogSplit that = (DvAwareFlussLogSplit) object;
        return startingOffset == that.startingOffset
                && stoppingOffset == that.stoppingOffset
                && deepEquals(logDvSnapshot, that.logDvSnapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(), startingOffset, stoppingOffset, deepHashCode(logDvSnapshot));
    }

    private static boolean deepEquals(Map<Long, byte[]> left, Map<Long, byte[]> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (Map.Entry<Long, byte[]> entry : left.entrySet()) {
            if (!java.util.Arrays.equals(entry.getValue(), right.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static int deepHashCode(Map<Long, byte[]> map) {
        int result = 1;
        for (Map.Entry<Long, byte[]> entry : map.entrySet()) {
            result =
                    31 * result
                            + Objects.hash(
                                    entry.getKey(), java.util.Arrays.hashCode(entry.getValue()));
        }
        return result;
    }
}
