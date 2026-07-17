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

package org.apache.fluss.client.write;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;

/**
 * The {@link WriteBatch} already ready in sender. The difference with {@link WriteBatch} is that
 * the partitionId of the tableBucket in ReadyWriteBatch has already been determined in the dynamic
 * partition create scenario
 */
public class ReadyWriteBatch {
    private final TableBucket tableBucket;
    private final WriteBatch writeBatch;
    // The physical metadata path of this RPC destination. It is the batch path for a normal write.
    // For a historical write, the batch remains keyed by its original partition path while this
    // field points to the historical system partition path.
    private final PhysicalTablePath targetPhysicalTablePath;

    public ReadyWriteBatch(TableBucket tableBucket, WriteBatch writeBatch) {
        this(tableBucket, writeBatch, writeBatch.physicalTablePath());
    }

    ReadyWriteBatch(
            TableBucket tableBucket,
            WriteBatch writeBatch,
            PhysicalTablePath targetPhysicalTablePath) {
        this.tableBucket = tableBucket;
        this.writeBatch = writeBatch;
        this.targetPhysicalTablePath = targetPhysicalTablePath;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public WriteBatch writeBatch() {
        return writeBatch;
    }

    PhysicalTablePath targetPhysicalTablePath() {
        return targetPhysicalTablePath;
    }
}
