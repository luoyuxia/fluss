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

package org.apache.fluss.record;

import org.apache.fluss.memory.MemorySegment;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * An on-memory RowId vector for Arrow-format batches of DV-enabled tables. Each RowId is stored as
 * a fixed 8-byte big-endian long.
 */
public class RowIdVector {

    private final MemorySegment segment;
    private final int position;
    private final int recordCount;

    public RowIdVector(MemorySegment segment, int position, int recordCount) {
        checkArgument(position >= 0, "position must be >= 0");
        checkArgument(recordCount >= 0, "recordCount must be >= 0");
        this.segment = segment;
        this.position = position;
        this.recordCount = recordCount;
    }

    /** Get the RowId at i-th position. */
    public long getRowId(int i) {
        checkArgument(i >= 0 && i < recordCount, "i must be in [0, %s), but is %s", recordCount, i);
        return segment.getLong(position + i * Long.BYTES);
    }

    public int sizeInBytes() {
        return recordCount * Long.BYTES;
    }
}
