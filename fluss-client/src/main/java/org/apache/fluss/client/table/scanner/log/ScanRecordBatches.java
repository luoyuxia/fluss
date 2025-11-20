/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FlussArrowRecordBatch;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** . */
public class ScanRecordBatches {

    public static final ScanRecordBatches EMPTY = new ScanRecordBatches(Collections.emptyMap());

    private final Map<TableBucket, List<FlussArrowRecordBatch>> records;

    public ScanRecordBatches(Map<TableBucket, List<FlussArrowRecordBatch>> records) {
        this.records = records;
    }

    /**
     * Get just the records for the given bucketId.
     *
     * @param scanBucket The bucket to get records for
     */
    public List<FlussArrowRecordBatch> records(TableBucket scanBucket) {
        List<FlussArrowRecordBatch> recs = records.get(scanBucket);
        if (recs == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(recs);
    }

    /**
     * Get the bucket ids which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (maybe empty if no data was
     *     returned)
     */
    public Set<TableBucket> buckets() {
        return Collections.unmodifiableSet(records.keySet());
    }
}
