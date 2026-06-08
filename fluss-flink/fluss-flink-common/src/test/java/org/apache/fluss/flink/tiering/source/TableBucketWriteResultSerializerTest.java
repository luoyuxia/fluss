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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableBucketWriteResultSerializer}. */
class TableBucketWriteResultSerializerTest {

    private static final TableBucketWriteResultSerializer<TestingWriteResult>
            tableBucketWriteResultSerializer =
                    new TableBucketWriteResultSerializer<>(new TestingWriteResultSerializer());

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSerializeAndDeserialize(boolean isPartitioned) throws Exception {
        // verify when writeResult is not null
        TestingWriteResult testingWriteResult = new TestingWriteResult(2);
        TablePath tablePath = TablePath.of("db1", "tb1");
        TableBucket tableBucket =
                isPartitioned ? new TableBucket(1, 1000L, 2) : new TableBucket(1, 2);
        String partitionName = isPartitioned ? "partition1" : null;
        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, partitionName, testingWriteResult, 10, 30L, 20);

        // test serialize and deserialize
        byte[] serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        TestingWriteResult deserializedWriteResult = deserialized.writeResult();
        assertThat(deserializedWriteResult).isNotNull();
        assertThat(deserializedWriteResult.getWriteResult())
                .isEqualTo(testingWriteResult.getWriteResult());
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(20);

        // verify when writeResult is null
        tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, partitionName, null, 20, 30L, 30);
        serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);
        assertThat(deserialized.tablePath()).isEqualTo(tablePath);
        assertThat(deserialized.tableBucket()).isEqualTo(tableBucket);
        assertThat(deserialized.partitionName()).isEqualTo(partitionName);
        assertThat(deserialized.writeResult()).isNull();
        assertThat(deserialized.numberOfWriteResults()).isEqualTo(30);
    }

    @Test
    void testSerializeAndDeserializeWithRowPosResult() throws Exception {
        TablePath tablePath = TablePath.of("db1", "tb1");
        TableBucket tableBucket = new TableBucket(1, 100L, 2);

        // RowPosResult with multiple SST files
        Map<Integer, String> fileDictEntries = new HashMap<>();
        fileDictEntries.put(10, "file1.parquet");
        fileDictEntries.put(11, "file2.parquet");
        RowPosResult rowPosResult =
                new RowPosResult(
                        2,
                        100L,
                        "partition1",
                        fileDictEntries,
                        Arrays.asList(
                                new RowPosResult.SstMeta("sst_0.sst", 12345),
                                new RowPosResult.SstMeta("sst_1.sst", 67890)));

        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, "partition1", null, -1, -1, 3, rowPosResult);

        byte[] serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.isRowPosResult()).isTrue();
        RowPosResult deserializedRowPos = deserialized.getRowPosResult();
        assertThat(deserializedRowPos).isNotNull();
        assertThat(deserializedRowPos.getBucketId()).isEqualTo(2);
        assertThat(deserializedRowPos.getPartitionId()).isEqualTo(100L);
        assertThat(deserializedRowPos.getPartitionName()).isEqualTo("partition1");
        assertThat(deserializedRowPos.getNewFileDictEntries()).hasSize(2);
        assertThat(deserializedRowPos.getNewFileDictEntries().get(10)).isEqualTo("file1.parquet");
        assertThat(deserializedRowPos.getNewFileDictEntries().get(11)).isEqualTo("file2.parquet");
        assertThat(deserializedRowPos.getSstMetas()).hasSize(2);
        assertThat(deserializedRowPos.getSstMetas().get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(deserializedRowPos.getSstMetas().get(0).getFileSize()).isEqualTo(12345);
        assertThat(deserializedRowPos.getSstMetas().get(1).getFileName()).isEqualTo("sst_1.sst");
        assertThat(deserializedRowPos.getSstMetas().get(1).getFileSize()).isEqualTo(67890);
    }

    @Test
    void testSerializeAndDeserializeWithSingleSstRowPosResult() throws Exception {
        TablePath tablePath = TablePath.of("db1", "tb1");
        TableBucket tableBucket = new TableBucket(1, 2);

        // single-SST RowPosResult (non-partitioned)
        Map<Integer, String> fileDictEntries = new HashMap<>();
        fileDictEntries.put(5, "output.parquet");
        RowPosResult rowPosResult =
                new RowPosResult(
                        2,
                        null,
                        null,
                        fileDictEntries,
                        Collections.singletonList(new RowPosResult.SstMeta("sst_0.sst", 1000)));

        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath, tableBucket, null, null, -1, -1, 1, rowPosResult);

        byte[] serialized = tableBucketWriteResultSerializer.serialize(tableBucketWriteResult);
        TableBucketWriteResult<TestingWriteResult> deserialized =
                tableBucketWriteResultSerializer.deserialize(
                        tableBucketWriteResultSerializer.getVersion(), serialized);

        assertThat(deserialized.isRowPosResult()).isTrue();
        RowPosResult deserializedRowPos = deserialized.getRowPosResult();
        assertThat(deserializedRowPos).isNotNull();
        assertThat(deserializedRowPos.getBucketId()).isEqualTo(2);
        assertThat(deserializedRowPos.getPartitionId()).isNull();
        assertThat(deserializedRowPos.getPartitionName()).isNull();
        assertThat(deserializedRowPos.getNewFileDictEntries()).hasSize(1);
        assertThat(deserializedRowPos.getNewFileDictEntries().get(5)).isEqualTo("output.parquet");
        assertThat(deserializedRowPos.getSstMetas()).hasSize(1);
        assertThat(deserializedRowPos.getSstMetas().get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(deserializedRowPos.getSstMetas().get(0).getFileSize()).isEqualTo(1000);
    }
}
