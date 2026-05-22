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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for DV data classes. */
class DvDataClassesTest {

    @Test
    void testDvPositionReportData() {
        Map<Integer, String> dictEntries0 = new HashMap<>();
        dictEntries0.put(1, "/sst/f1.sst");
        dictEntries0.put(2, "/sst/f2.sst");

        Map<Integer, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                0,
                new DvPositionReportData.DvBucketOffset(
                        100L, dictEntries0, Arrays.asList("/sst/old1.sst")));
        bucketOffsets.put(
                1,
                new DvPositionReportData.DvBucketOffset(
                        200L, Collections.emptyMap(), Collections.emptyList()));

        DvPositionReportData data = new DvPositionReportData(bucketOffsets);

        assertThat(data.getBucketOffsets()).hasSize(2);
        assertThat(data.getBucketOffsets().get(0).getReadableOffset()).isEqualTo(100L);
        assertThat(data.getBucketOffsets().get(0).getNewFileDictEntries()).hasSize(2);
        assertThat(data.getBucketOffsets().get(0).getNewFileDictEntries().get(1))
                .isEqualTo("/sst/f1.sst");
        assertThat(data.getBucketOffsets().get(0).getOldFiles()).contains("/sst/old1.sst");
        assertThat(data.getBucketOffsets().get(1).getReadableOffset()).isEqualTo(200L);
        assertThat(data.getBucketOffsets().get(1).getNewFileDictEntries()).isEmpty();
        assertThat(data.getBucketOffsets().get(1).getOldFiles()).isEmpty();
    }

    @Test
    void testDvBucketOffset() {
        Map<Integer, String> dictEntries = new HashMap<>();
        dictEntries.put(5, "/sst/f5.sst");

        DvPositionReportData.DvBucketOffset offset =
                new DvPositionReportData.DvBucketOffset(
                        500L, dictEntries, Arrays.asList("/sst/old.sst"));

        assertThat(offset.getReadableOffset()).isEqualTo(500L);
        assertThat(offset.getNewFileDictEntries()).hasSize(1);
        assertThat(offset.getNewFileDictEntries().get(5)).isEqualTo("/sst/f5.sst");
        assertThat(offset.getOldFiles()).hasSize(1).contains("/sst/old.sst");
    }

    @Test
    void testDvPrepareData() {
        Map<Integer, String> dictEntries = new HashMap<>();
        dictEntries.put(10, "/dict/f10.sst");

        Map<Integer, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                3,
                new DvPositionReportData.DvBucketOffset(
                        500L, dictEntries, Arrays.asList("/dict/old.sst")));

        DvPrepareData data = new DvPrepareData(42L, 7L, bucketOffsets);

        assertThat(data.getTableId()).isEqualTo(42L);
        assertThat(data.getReadableSnapshotId()).isEqualTo(7L);
        assertThat(data.getBucketOffsets()).hasSize(1);
        assertThat(data.getBucketOffsets().get(3).getReadableOffset()).isEqualTo(500L);
        assertThat(data.getBucketOffsets().get(3).getNewFileDictEntries().get(10))
                .isEqualTo("/dict/f10.sst");
        assertThat(data.getBucketOffsets().get(3).getOldFiles()).contains("/dict/old.sst");
    }

    @Test
    void testDvPrepareDataFilterByBuckets() {
        Map<Integer, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                0,
                new DvPositionReportData.DvBucketOffset(
                        100L, Collections.singletonMap(1, "/sst/f1"), Arrays.asList("/old1")));
        bucketOffsets.put(
                1,
                new DvPositionReportData.DvBucketOffset(
                        200L, Collections.singletonMap(2, "/sst/f2"), Arrays.asList("/old2")));
        bucketOffsets.put(
                2,
                new DvPositionReportData.DvBucketOffset(
                        300L, Collections.emptyMap(), Collections.emptyList()));

        DvPrepareData data = new DvPrepareData(42L, 7L, bucketOffsets);

        DvPrepareData filtered = data.filterByBuckets(new HashSet<>(Arrays.asList(0, 2)));

        assertThat(filtered.getTableId()).isEqualTo(42L);
        assertThat(filtered.getReadableSnapshotId()).isEqualTo(7L);
        assertThat(filtered.getBucketOffsets()).hasSize(2);
        assertThat(filtered.getBucketOffsets()).containsKey(0);
        assertThat(filtered.getBucketOffsets()).containsKey(2);
        assertThat(filtered.getBucketOffsets()).doesNotContainKey(1);
        assertThat(filtered.getBucketOffsets().get(0).getNewFileDictEntries().get(1))
                .isEqualTo("/sst/f1");
        assertThat(filtered.getBucketOffsets().get(0).getOldFiles()).contains("/old1");
    }

    @Test
    void testDvReadableSwitchData() {
        DvReadableSwitchData data = new DvReadableSwitchData(5, 100L, 10L);

        assertThat(data.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(data.getTableId()).isEqualTo(100L);
        assertThat(data.getReadableSnapshotId()).isEqualTo(10L);
    }

    @Test
    void testCommitLakeTableSnapshotsDataWithDvReport() {
        Map<Integer, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                0,
                new DvPositionReportData.DvBucketOffset(
                        100L,
                        Collections.singletonMap(1, "/sst/f1.sst"),
                        Arrays.asList("/sst/old.sst")));

        DvPositionReportData dvReport = new DvPositionReportData(bucketOffsets);

        CommitLakeTableSnapshotsData.Builder builder = CommitLakeTableSnapshotsData.builder();
        builder.addTableSnapshot(42L, null, null, null, null, dvReport);

        CommitLakeTableSnapshotsData data = builder.build();
        CommitLakeTableSnapshotsData.CommitLakeTableSnapshot snapshot =
                data.getCommitLakeTableSnapshotByTableId().get(42L);

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.getDvPositionReport()).isNotNull();
        assertThat(snapshot.getDvPositionReport().getBucketOffsets()).hasSize(1);
    }

    @Test
    void testCommitLakeTableSnapshotsDataWithoutDvReport() {
        CommitLakeTableSnapshotsData.Builder builder = CommitLakeTableSnapshotsData.builder();
        builder.addTableSnapshot(42L, null, null, null, null);

        CommitLakeTableSnapshotsData data = builder.build();
        CommitLakeTableSnapshotsData.CommitLakeTableSnapshot snapshot =
                data.getCommitLakeTableSnapshotByTableId().get(42L);

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.getDvPositionReport()).isNull();
    }

    @Test
    void testNotifyLakeTableOffsetDataWithDvPrepare() {
        Map<Integer, DvPositionReportData.DvBucketOffset> bucketOffsets = new HashMap<>();
        bucketOffsets.put(
                0,
                new DvPositionReportData.DvBucketOffset(
                        100L, Collections.emptyMap(), Collections.emptyList()));

        DvPrepareData dvPrepare = new DvPrepareData(42L, 7L, bucketOffsets);

        NotifyLakeTableOffsetData data =
                new NotifyLakeTableOffsetData(5, new HashMap<>(), dvPrepare);

        assertThat(data.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(data.getDvPrepare()).isNotNull();
        assertThat(data.getDvPrepare().getTableId()).isEqualTo(42L);
        assertThat(data.getDvPrepare().getReadableSnapshotId()).isEqualTo(7L);
    }

    @Test
    void testNotifyLakeTableOffsetDataWithoutDvPrepare() {
        NotifyLakeTableOffsetData data = new NotifyLakeTableOffsetData(5, new HashMap<>());

        assertThat(data.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(data.getDvPrepare()).isNull();
    }
}
