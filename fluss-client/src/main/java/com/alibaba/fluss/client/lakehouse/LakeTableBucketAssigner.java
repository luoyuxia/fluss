/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.lakehouse;

import com.alibaba.fluss.client.lakehouse.paimon.PaimonBucketAssigner;
import com.alibaba.fluss.client.write.StaticBucketAssigner;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import java.util.List;

import static com.alibaba.fluss.client.lakehouse.paimon.PaimonBucketAssigner.DATA_LAKE_PAIMON;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/** A bucket assigner for table with data lake enabled. */
public class LakeTableBucketAssigner implements StaticBucketAssigner {

    // the bucket extractor of bucket, fluss will use the the bucket
    // that paimon assign to align with paimon when data lake is enabled
    // todo: make it pluggable
    private final PaimonBucketAssigner paimonBucketAssigner;

    public LakeTableBucketAssigner(
            String lakeType, RowType rowType, List<String> bucketKey, int bucketNum) {
        checkArgument(
                lakeType.equalsIgnoreCase(DATA_LAKE_PAIMON),
                "Currently, only %s is supported as the datalake type of a table.",
                DATA_LAKE_PAIMON);
        this.paimonBucketAssigner = new PaimonBucketAssigner(rowType, bucketKey, bucketNum);
    }

    public int assignBucket(InternalRow row) {
        return paimonBucketAssigner.assignBucket(row);
    }
}
