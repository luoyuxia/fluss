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

package org.apache.fluss.flink.lake.state;

import org.apache.fluss.flink.lake.split.DvAwareFlussLogSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;

/** The state of {@link DvAwareFlussLogSplit}. */
public class DvAwareFlussLogSplitState extends SourceSplitState {

    private final DvAwareFlussLogSplit split;
    private long nextOffset;

    public DvAwareFlussLogSplitState(DvAwareFlussLogSplit split) {
        super(split);
        this.split = split;
        this.nextOffset = split.getStartingOffset();
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    @Override
    public SourceSplitBase toSourceSplit() {
        return new DvAwareFlussLogSplit(
                split.getTableBucket(),
                split.getPartitionName(),
                nextOffset,
                split.getStoppingOffset().orElse(LogSplit.NO_STOPPING_OFFSET),
                split.getLogDvSnapshot());
    }
}
