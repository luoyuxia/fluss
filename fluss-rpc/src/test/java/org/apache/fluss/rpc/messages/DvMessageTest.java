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

package org.apache.fluss.rpc.messages;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for DV-related proto messages serde round-trip. */
class DvMessageTest {

    @Test
    void testPbDvPositionReportRoundTrip() {
        PbDvPositionReport original = new PbDvPositionReport();
        PbDvBucketOffset bo0 = original.addBucketOffset().setBucketId(0).setReadableOffset(100L);
        bo0.addNewFileDictEntry().setFileId(1).setFilePath("/data/sst/file1.sst");
        bo0.addNewFileDictEntry().setFileId(2).setFilePath("/data/sst/file2.sst");
        bo0.addOldFile("/data/sst/old1.sst");
        bo0.addOldFile("/data/sst/old2.sst");

        original.addBucketOffset().setBucketId(1).setReadableOffset(200L);

        byte[] bytes = original.toByteArray();
        PbDvPositionReport parsed = new PbDvPositionReport();
        parsed.parseFrom(bytes);

        assertThat(parsed.getBucketOffsetsCount()).isEqualTo(2);

        PbDvBucketOffset parsedBo0 = parsed.getBucketOffsetAt(0);
        assertThat(parsedBo0.getBucketId()).isEqualTo(0);
        assertThat(parsedBo0.getReadableOffset()).isEqualTo(100L);
        assertThat(parsedBo0.getNewFileDictEntriesCount()).isEqualTo(2);
        assertThat(parsedBo0.getNewFileDictEntryAt(0).getFileId()).isEqualTo(1);
        assertThat(parsedBo0.getNewFileDictEntryAt(0).getFilePath())
                .isEqualTo("/data/sst/file1.sst");
        assertThat(parsedBo0.getOldFilesCount()).isEqualTo(2);
        assertThat(parsedBo0.getOldFileAt(0)).isEqualTo("/data/sst/old1.sst");

        PbDvBucketOffset parsedBo1 = parsed.getBucketOffsetAt(1);
        assertThat(parsedBo1.getBucketId()).isEqualTo(1);
        assertThat(parsedBo1.getReadableOffset()).isEqualTo(200L);
        assertThat(parsedBo1.getNewFileDictEntriesCount()).isEqualTo(0);
        assertThat(parsedBo1.getOldFilesCount()).isEqualTo(0);
    }

    @Test
    void testPbDvPrepareRoundTrip() {
        PbDvPrepare original = new PbDvPrepare();
        original.setTableId(42L);
        original.setReadableSnapshotId(7L);
        PbDvBucketOffset bo = original.addBucketOffset().setBucketId(3).setReadableOffset(500L);
        bo.addNewFileDictEntry().setFileId(10).setFilePath("/data/dict/f10.sst");
        bo.addOldFile("/data/dict/old.sst");

        byte[] bytes = original.toByteArray();
        PbDvPrepare parsed = new PbDvPrepare();
        parsed.parseFrom(bytes);

        assertThat(parsed.getTableId()).isEqualTo(42L);
        assertThat(parsed.getReadableSnapshotId()).isEqualTo(7L);
        assertThat(parsed.getBucketOffsetsCount()).isEqualTo(1);

        PbDvBucketOffset parsedBo = parsed.getBucketOffsetAt(0);
        assertThat(parsedBo.getBucketId()).isEqualTo(3);
        assertThat(parsedBo.getReadableOffset()).isEqualTo(500L);
        assertThat(parsedBo.getNewFileDictEntriesCount()).isEqualTo(1);
        assertThat(parsedBo.getNewFileDictEntryAt(0).getFileId()).isEqualTo(10);
        assertThat(parsedBo.getOldFilesCount()).isEqualTo(1);
        assertThat(parsedBo.getOldFileAt(0)).isEqualTo("/data/dict/old.sst");
    }

    @Test
    void testDvReadableSwitchRequestRoundTrip() {
        DvReadableSwitchRequest original = new DvReadableSwitchRequest();
        original.setCoordinatorEpoch(5);
        original.setTableId(100L);
        original.setReadableSnapshotId(10L);

        byte[] bytes = original.toByteArray();
        DvReadableSwitchRequest parsed = new DvReadableSwitchRequest();
        parsed.parseFrom(bytes);

        assertThat(parsed.getCoordinatorEpoch()).isEqualTo(5);
        assertThat(parsed.getTableId()).isEqualTo(100L);
        assertThat(parsed.getReadableSnapshotId()).isEqualTo(10L);
    }

    @Test
    void testDvReadableSwitchResponseRoundTrip() {
        DvReadableSwitchResponse original = new DvReadableSwitchResponse();
        byte[] bytes = original.toByteArray();
        DvReadableSwitchResponse parsed = new DvReadableSwitchResponse();
        parsed.parseFrom(bytes);
        // Empty message - just verify no exceptions
        assertThat(parsed).isNotNull();
    }

    @Test
    void testLakeTableSnapshotMetadataWithDvPositionReport() {
        PbLakeTableSnapshotMetadata metadata = new PbLakeTableSnapshotMetadata();
        metadata.setTableId(1L);
        metadata.setSnapshotId(5L);
        metadata.setTieredBucketOffsetsFilePath("/path/tiered");

        PbDvPositionReport dvReport = metadata.setDvPositionReport();
        PbDvBucketOffset bo = dvReport.addBucketOffset().setBucketId(0).setReadableOffset(50L);
        bo.addNewFileDictEntry().setFileId(1).setFilePath("/sst/f1");

        byte[] bytes = metadata.toByteArray();
        PbLakeTableSnapshotMetadata parsed = new PbLakeTableSnapshotMetadata();
        parsed.parseFrom(bytes);

        assertThat(parsed.getTableId()).isEqualTo(1L);
        assertThat(parsed.getSnapshotId()).isEqualTo(5L);
        assertThat(parsed.hasDvPositionReport()).isTrue();
        PbDvPositionReport parsedDv = parsed.getDvPositionReport();
        assertThat(parsedDv.getBucketOffsetsCount()).isEqualTo(1);
        assertThat(parsedDv.getBucketOffsetAt(0).getNewFileDictEntriesCount()).isEqualTo(1);
    }

    @Test
    void testLakeTableSnapshotMetadataWithoutDvPositionReport() {
        PbLakeTableSnapshotMetadata metadata = new PbLakeTableSnapshotMetadata();
        metadata.setTableId(1L);
        metadata.setSnapshotId(5L);
        metadata.setTieredBucketOffsetsFilePath("/path/tiered");

        byte[] bytes = metadata.toByteArray();
        PbLakeTableSnapshotMetadata parsed = new PbLakeTableSnapshotMetadata();
        parsed.parseFrom(bytes);

        assertThat(parsed.hasDvPositionReport()).isFalse();
    }

    @Test
    void testNotifyLakeTableOffsetRequestWithDvPrepare() {
        NotifyLakeTableOffsetRequest request = new NotifyLakeTableOffsetRequest();
        request.setCoordinatorEpoch(3);

        PbDvPrepare dvPrepare = request.setDvPrepare();
        dvPrepare.setTableId(42L);
        dvPrepare.setReadableSnapshotId(7L);
        dvPrepare
                .addBucketOffset()
                .setBucketId(0)
                .setReadableOffset(100L)
                .addNewFileDictEntry()
                .setFileId(1)
                .setFilePath("/sst/f1");

        byte[] bytes = request.toByteArray();
        NotifyLakeTableOffsetRequest parsed = new NotifyLakeTableOffsetRequest();
        parsed.parseFrom(bytes);

        assertThat(parsed.getCoordinatorEpoch()).isEqualTo(3);
        assertThat(parsed.hasDvPrepare()).isTrue();
        assertThat(parsed.getDvPrepare().getTableId()).isEqualTo(42L);
        assertThat(parsed.getDvPrepare().getReadableSnapshotId()).isEqualTo(7L);
        assertThat(parsed.getDvPrepare().getBucketOffsetsCount()).isEqualTo(1);
    }

    @Test
    void testNotifyLakeTableOffsetRequestWithoutDvPrepare() {
        NotifyLakeTableOffsetRequest request = new NotifyLakeTableOffsetRequest();
        request.setCoordinatorEpoch(3);

        byte[] bytes = request.toByteArray();
        NotifyLakeTableOffsetRequest parsed = new NotifyLakeTableOffsetRequest();
        parsed.parseFrom(bytes);

        assertThat(parsed.getCoordinatorEpoch()).isEqualTo(3);
        assertThat(parsed.hasDvPrepare()).isFalse();
    }
}
