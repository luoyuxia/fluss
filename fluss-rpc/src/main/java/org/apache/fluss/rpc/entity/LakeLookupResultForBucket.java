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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.rpc.messages.LakeLookupRequest;
import org.apache.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

import java.util.List;

/** The result of {@link LakeLookupRequest} for each partition bucket. */
public class LakeLookupResultForBucket {

    private final @Nullable String partitionName;
    private final int bucketId;
    private final @Nullable List<byte[]> values;
    private final ApiError error;

    public LakeLookupResultForBucket(
            @Nullable String partitionName, int bucketId, List<byte[]> values) {
        this(partitionName, bucketId, values, ApiError.NONE);
    }

    public LakeLookupResultForBucket(@Nullable String partitionName, int bucketId, ApiError error) {
        this(partitionName, bucketId, null, error);
    }

    private LakeLookupResultForBucket(
            @Nullable String partitionName,
            int bucketId,
            @Nullable List<byte[]> values,
            ApiError error) {
        this.partitionName = partitionName;
        this.bucketId = bucketId;
        this.values = values;
        this.error = error;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    public int getBucketId() {
        return bucketId;
    }

    public @Nullable List<byte[]> getValues() {
        return values;
    }

    /** Returns true if the request is failed. */
    public boolean failed() {
        return error.isFailure();
    }

    public int getErrorCode() {
        return error.error().code();
    }

    public @Nullable String getErrorMessage() {
        return error.message();
    }

    public ApiError getError() {
        return error;
    }
}
