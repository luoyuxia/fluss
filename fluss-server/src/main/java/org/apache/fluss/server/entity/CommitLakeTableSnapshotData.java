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
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import java.util.Map;
import java.util.Objects;

/** The data for request {@link CommitLakeTableSnapshotRequest}. */
public class CommitLakeTableSnapshotData {

    private final Map<Long, LakeTableSnapshot> lakeTableSnapshots;
    private final Map<TableBucket, Long> tableBucketsMaxTieredTimestamp;
    private final Map<Long, LakeTableSnapshot> readableLakeTableSnapshots;
    private final Map<Long, Long> minSnapshotIdToKeepByTableId;

    public CommitLakeTableSnapshotData(
            Map<Long, LakeTableSnapshot> lakeTableSnapshots,
            Map<TableBucket, Long> tableBucketsMaxTieredTimestamp,
            Map<Long, LakeTableSnapshot> readableLakeTableSnapshots,
            Map<Long, Long> minSnapshotIdToKeepByTableId) {
        this.lakeTableSnapshots = lakeTableSnapshots;
        this.tableBucketsMaxTieredTimestamp = tableBucketsMaxTieredTimestamp;
        this.readableLakeTableSnapshots = readableLakeTableSnapshots;
        this.minSnapshotIdToKeepByTableId = minSnapshotIdToKeepByTableId;
    }

    public Map<Long, LakeTableSnapshot> getLakeTableSnapshot() {
        return lakeTableSnapshots;
    }

    public Map<TableBucket, Long> getTableBucketsMaxTieredTimestamp() {
        return tableBucketsMaxTieredTimestamp;
    }

    public Map<Long, LakeTableSnapshot> getReadableLakeTableSnapshots() {
        return readableLakeTableSnapshots;
    }

    public Map<Long, Long> getMinSnapshotIdToKeepByTableId() {
        return minSnapshotIdToKeepByTableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommitLakeTableSnapshotData that = (CommitLakeTableSnapshotData) o;
        return Objects.equals(lakeTableSnapshots, that.lakeTableSnapshots)
                && Objects.equals(
                        tableBucketsMaxTieredTimestamp, that.tableBucketsMaxTieredTimestamp)
                && Objects.equals(readableLakeTableSnapshots, that.readableLakeTableSnapshots)
                && Objects.equals(minSnapshotIdToKeepByTableId, that.minSnapshotIdToKeepByTableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                lakeTableSnapshots,
                tableBucketsMaxTieredTimestamp,
                readableLakeTableSnapshots,
                minSnapshotIdToKeepByTableId);
    }

    @Override
    public String toString() {
        return "CommitLakeTableSnapshotData{"
                + "lakeTableSnapshots="
                + lakeTableSnapshots
                + ", tableBucketsMaxTieredTimestamp="
                + tableBucketsMaxTieredTimestamp
                + '}';
    }
}
