/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.client.write;

import org.apache.fluss.metadata.PhysicalTablePath;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The resolved physical target and routing context for a write record. */
final class ResolvedWriteTarget {

    private final PhysicalTablePath physicalTablePath;
    private final @Nullable String originalPartitionName;
    private final @Nullable Long partitionId;

    private ResolvedWriteTarget(
            PhysicalTablePath physicalTablePath,
            @Nullable String originalPartitionName,
            @Nullable Long partitionId) {
        this.physicalTablePath =
                checkNotNull(physicalTablePath, "physicalTablePath must not be null");
        this.originalPartitionName = originalPartitionName;
        this.partitionId = partitionId;
    }

    static ResolvedWriteTarget normal(PhysicalTablePath physicalTablePath) {
        return new ResolvedWriteTarget(physicalTablePath, null, null);
    }

    static ResolvedWriteTarget historical(
            PhysicalTablePath physicalTablePath, String originalPartitionName, long partitionId) {
        checkNotNull(originalPartitionName, "originalPartitionName must not be null");
        checkArgument(!originalPartitionName.isEmpty(), "originalPartitionName must not be empty");
        return new ResolvedWriteTarget(physicalTablePath, originalPartitionName, partitionId);
    }

    PhysicalTablePath physicalTablePath() {
        return physicalTablePath;
    }

    boolean isHistorical() {
        return originalPartitionName != null;
    }

    @Nullable
    String originalPartitionName() {
        return originalPartitionName;
    }

    long partitionId() {
        return checkNotNull(partitionId, "Historical partition id must not be null");
    }
}
