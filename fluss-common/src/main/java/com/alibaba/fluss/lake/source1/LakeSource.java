/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.source1;

import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * A generic interface for lake data sources, defining how to plan splits and read data. Any
 * datalake format supporting to read from the data tiered in lake as Fluss records should implement
 * this method.
 *
 * @param <Split> The type of data split, which must extend {@link LakeSplit}
 */
public interface LakeSource<Split extends LakeSplit> extends Serializable {

    /**
     * Plans data splits based on the given context and returns a list of splits. This method is
     * typically used in the task assignment phase of distributed computing frameworks (e.g., Flink,
     * Spark).
     *
     * @param context The split planning context, providing necessary information (e.g., snapshot
     *     id, filters, etc.)
     * @return A list of planned data splits
     */
    List<Split> plan(LakeSplitPlanContext context) throws IOException;

    /**
     * Reads data from the specified split and returns a closeable iterator of records. This method
     * is usually invoked by task executors for actual data reading.
     *
     * @param context The read context, containing split information, read configuration, etc.
     * @return A closeable iterator for traversing the data records
     */
    LakeRecords read(LakeSplitReadContext<Split> context) throws IOException;

    /**
     * Returns the serializer for the data split, used to transfer split information in distributed
     * environment.
     *
     * @return The serializer for the split
     */
    SimpleVersionedSerializer<Split> getSplitSerializer();
}
