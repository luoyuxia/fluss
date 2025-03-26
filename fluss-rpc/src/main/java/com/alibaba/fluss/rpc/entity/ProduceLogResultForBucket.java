/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc.entity;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.Errors;

/** Result of {@link ProduceLogRequest} for each table bucket. */
@Internal
public class ProduceLogResultForBucket extends WriteResultForBucket {
    private final long baseOffset;
    private final long writerId;
    private final int batchSequence;

    public ProduceLogResultForBucket(
            TableBucket tableBucket,
            long baseOffset,
            long endOffset,
            long writerId,
            int batchSequence) {
        this(tableBucket, baseOffset, endOffset, writerId, batchSequence, ApiError.NONE);
    }

    public ProduceLogResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, -1L, -1L, -1L, -1, error);
    }

    private ProduceLogResultForBucket(
            TableBucket tableBucket,
            long baseOffset,
            long endOffset,
            long writerId,
            int batchSequence,
            ApiError error) {
        super(tableBucket, endOffset, error);
        this.baseOffset = baseOffset;
        this.writerId = writerId;
        this.batchSequence = batchSequence;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getWriterId() {
        return writerId;
    }

    public int getBatchSequence() {
        return batchSequence;
    }

    @Override
    public <T extends WriteResultForBucket> T copy(Errors newError) {
        //noinspection unchecked
        return (T)
                new ProduceLogResultForBucket(
                        tableBucket,
                        baseOffset,
                        getWriteLogEndOffset(),
                        writerId,
                        batchSequence,
                        newError.toApiError());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ProduceLogResultForBucket that = (ProduceLogResultForBucket) o;
        return baseOffset == that.baseOffset
                && writerId == that.writerId
                && batchSequence == that.batchSequence;
    }
}
