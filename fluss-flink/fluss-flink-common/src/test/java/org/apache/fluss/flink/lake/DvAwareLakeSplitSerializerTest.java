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

package org.apache.fluss.flink.lake;

import org.apache.fluss.flink.lake.split.DvAwareFlussLogSplit;
import org.apache.fluss.flink.lake.split.DvAwareLakeSnapshotSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.fluss.flink.lake.split.DvAwareFlussLogSplit.DV_AWARE_FLUSS_LOG_SPLIT_KIND;
import static org.apache.fluss.flink.lake.split.DvAwareLakeSnapshotSplit.DV_AWARE_LAKE_SNAPSHOT_SPLIT_KIND;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for DV-aware dedicated lake/log split serde. */
class DvAwareLakeSplitSerializerTest {

    private static final byte[] TEST_DATA = "test-lake-split".getBytes();
    private static final LakeSplit LAKE_SPLIT =
            new TestingLakeSplit(0, Collections.singletonList("2025-08-18"));

    private final TableBucket tableBucket = new TableBucket(0, 1L, 0);
    private final LakeSplitSerializer serializer =
            new LakeSplitSerializer(new TestSimpleVersionedSerializer());

    @Test
    void testSerializeAndDeserializeDvAwareLakeSnapshotSplit() throws IOException {
        DvAwareLakeSnapshotSplit originalSplit =
                new DvAwareLakeSnapshotSplit(
                        tableBucket, "2025-08-18", LAKE_SPLIT, 3, 2L, new byte[] {1, 2, 3});

        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(
                        DV_AWARE_LAKE_SNAPSHOT_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(deserializedSplit).isInstanceOf(DvAwareLakeSnapshotSplit.class);
        DvAwareLakeSnapshotSplit result = (DvAwareLakeSnapshotSplit) deserializedSplit;
        assertThat(result.getLakeSplit()).isEqualTo(LAKE_SPLIT);
        assertThat(result.getSplitIndex()).isEqualTo(3);
        assertThat(result.getRecordsToSkip()).isEqualTo(2L);
        assertThat(result.getDeletionVector()).containsExactly(1, 2, 3);
    }

    @Test
    void testSerializeAndDeserializeDvAwareFlussLogSplit() throws IOException {
        DvAwareFlussLogSplit originalSplit =
                new DvAwareFlussLogSplit(
                        tableBucket,
                        "2025-08-18",
                        100L,
                        200L,
                        Collections.singletonMap(100L, new byte[] {4, 5, 6}));

        DataOutputSerializer output = new DataOutputSerializer(256);
        serializer.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(
                        DV_AWARE_FLUSS_LOG_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(deserializedSplit).isInstanceOf(DvAwareFlussLogSplit.class);
        DvAwareFlussLogSplit result = (DvAwareFlussLogSplit) deserializedSplit;
        assertThat(result.getStartingOffset()).isEqualTo(100L);
        assertThat(result.getStoppingOffset()).hasValue(200L);
        assertThat(result.getLogDvSnapshot()).containsOnlyKeys(100L);
        assertThat(result.getLogDvSnapshot().get(100L)).containsExactly(4, 5, 6);
    }

    private static class TestSimpleVersionedSerializer
            implements SimpleVersionedSerializer<LakeSplit> {

        @Override
        public byte[] serialize(LakeSplit split) {
            return TEST_DATA;
        }

        @Override
        public LakeSplit deserialize(int version, byte[] serialized) {
            return LAKE_SPLIT;
        }

        @Override
        public int getVersion() {
            return 1;
        }
    }
}
