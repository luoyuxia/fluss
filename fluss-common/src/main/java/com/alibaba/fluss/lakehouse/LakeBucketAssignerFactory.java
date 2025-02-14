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

package com.alibaba.fluss.lakehouse;

import com.alibaba.fluss.lakehouse.paimon.PaimonBucketAssigner;

/** A factory to create {@link LakeBucketAssigner} for different datalake formats. */
public class LakeBucketAssignerFactory {

    public static LakeBucketAssigner createLakeBucketAssigner(
            DataLakeFormat dataLakeFormat, int numBuckets) {
        if (dataLakeFormat == DataLakeFormat.PAIMON) {
            return new PaimonBucketAssigner(numBuckets);
        }
        throw new UnsupportedOperationException(
                String.format("Unsupported data lake format: %s", dataLakeFormat));
    }
}
