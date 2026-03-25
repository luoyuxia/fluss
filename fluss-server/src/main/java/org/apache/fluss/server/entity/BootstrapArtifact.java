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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

/** Bootstrap artifact metadata for a bucket. */
public class BootstrapArtifact {
    private final TableBucket tableBucket;
    @Nullable private final String partitionName;
    @Nullable private final String snapshotPath;

    public BootstrapArtifact(
            TableBucket tableBucket,
            @Nullable String partitionName,
            @Nullable String snapshotPath) {
        this.tableBucket = tableBucket;
        this.partitionName = partitionName;
        this.snapshotPath = snapshotPath;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    @Nullable
    public String getSnapshotPath() {
        return snapshotPath;
    }
}
