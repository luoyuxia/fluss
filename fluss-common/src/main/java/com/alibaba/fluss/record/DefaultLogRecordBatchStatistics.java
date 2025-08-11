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

package com.alibaba.fluss.record;

import com.alibaba.fluss.row.InternalRow;

/** Default implementation of {@link LogRecordBatchStatistics}. */
public class DefaultLogRecordBatchStatistics implements LogRecordBatchStatistics {

    private final InternalRow minValues;
    private final InternalRow maxValues;
    private final Long[] nullCounts;

    public DefaultLogRecordBatchStatistics(
            InternalRow minValues, InternalRow maxValues, Long[] nullCounts) {
        this.minValues = minValues;
        this.maxValues = maxValues;
        this.nullCounts = nullCounts;
    }

    @Override
    public InternalRow getMinValues() {
        return minValues;
    }

    @Override
    public InternalRow getMaxValues() {
        return maxValues;
    }

    @Override
    public Long[] getNullCounts() {
        return nullCounts;
    }

    @Override
    public String toString() {
        return "DefaultLogRecordBatchStatistics{"
                + ", minValues="
                + minValues
                + ", maxValues="
                + maxValues
                + ", nullCounts="
                + nullCounts
                + '}';
    }
}
