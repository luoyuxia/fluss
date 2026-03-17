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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for serialization and deserialization of {@link TieringSnapshotSplit} and {@link
 * TieringLogSplit}.
 */
class TieringSplitSerializerTest {

    private static final TieringSplitSerializer serializer = TieringSplitSerializer.INSTANCE;
    private static final TableBucket tableBucket = new TableBucket(1, 2);
    private static final TablePath tablePath = TablePath.of("test_db", "test_table");
    private static final TableBucket partitionedTableBucket = new TableBucket(1, 100L, 2);
    private static final TablePath partitionedTablePath =
            TablePath.of("test_db", "test_partitioned_table");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringSnapshotSplitSerde(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        TieringSnapshotSplit tieringSplit =
                new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 10);

        byte[] serialized = serializer.serialize(tieringSplit);
        TieringSnapshotSplit deserializedSplit =
                (TieringSnapshotSplit) serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringSnapshotSplitStringExpression(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        String expectedSplitId =
                isPartitionedTable
                        ? "tiering-snapshot-split-1-p100-2"
                        : "tiering-snapshot-split-1-2";
        assertThat(new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 20).splitId())
                .isEqualTo(expectedSplitId);

        String expectedSplitString =
                isPartitionedTable
                        ? "TieringSnapshotSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', numberOfSplits=30, skipCurrentRound=false, snapshotId=0, logOffsetOfSnapshot=200, lakeDvSnapshot=null}"
                        : "TieringSnapshotSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', numberOfSplits=30, skipCurrentRound=false, snapshotId=0, logOffsetOfSnapshot=200, lakeDvSnapshot=null}";
        assertThat(new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 30).toString())
                .isEqualTo(expectedSplitString);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringLogSplitSerde(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        TieringLogSplit tieringSplit =
                new TieringLogSplit(path, bucket, partitionName, 100, 200, 40);

        byte[] serialized = serializer.serialize(tieringSplit);
        TieringLogSplit deserializedSplit =
                (TieringLogSplit) serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }

    @Test
    void testTieringLogSplitSerdeWithLakeAndLogDvSnapshots() throws Exception {
        Map<String, byte[]> lakeDvSnapshot = Map.of("file-1.parquet", new byte[] {1, 2, 3});
        Map<Long, byte[]> logDvSnapshot = Map.of(1000L, new byte[] {4, 5, 6});
        TieringLogSplit tieringSplit =
                new TieringLogSplit(
                        tablePath,
                        tableBucket,
                        null,
                        100,
                        200,
                        40,
                        false,
                        lakeDvSnapshot,
                        logDvSnapshot);

        byte[] serialized = serializer.serialize(tieringSplit);
        TieringLogSplit deserializedSplit =
                (TieringLogSplit) serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
        assertThat(deserializedSplit.getLakeDvSnapshot()).containsOnlyKeys("file-1.parquet");
        assertThat(deserializedSplit.getLakeDvSnapshot().get("file-1.parquet"))
                .containsExactly(1, 2, 3);
        assertThat(deserializedSplit.getLogDvSnapshot()).containsOnlyKeys(1000L);
        assertThat(deserializedSplit.getLogDvSnapshot().get(1000L)).containsExactly(4, 5, 6);
    }

    @Test
    void testDeserializeVersion2TieringLogSplitWithoutLakeDvSnapshot() throws Exception {
        Map<Long, byte[]> logDvSnapshot = Map.of(1000L, new byte[] {7, 8, 9});
        byte[] serialized = serializeVersion2TieringLogSplit(logDvSnapshot);

        TieringLogSplit deserializedSplit = (TieringLogSplit) serializer.deserialize(2, serialized);

        assertThat(deserializedSplit.getLakeDvSnapshot()).isNull();
        assertThat(deserializedSplit.getLogDvSnapshot()).containsOnlyKeys(1000L);
        assertThat(deserializedSplit.getLogDvSnapshot().get(1000L)).containsExactly(7, 8, 9);
        assertThat(deserializedSplit.getStartingOffset()).isEqualTo(100L);
        assertThat(deserializedSplit.getStoppingOffset()).isEqualTo(200L);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringLogSplitStringExpression(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        String expectedSplitId =
                isPartitionedTable ? "tiering-log-split-1-p100-2" : "tiering-log-split-1-2";
        assertThat(new TieringLogSplit(path, bucket, partitionName, 100, 200, 3).splitId())
                .isEqualTo(expectedSplitId);

        String expectedSplitString =
                isPartitionedTable
                        ? "TieringLogSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', numberOfSplits=2, skipCurrentRound=false, startingOffset=100, stoppingOffset=200}"
                        : "TieringLogSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', numberOfSplits=2, skipCurrentRound=false, startingOffset=100, stoppingOffset=200}";
        assertThat(new TieringLogSplit(path, bucket, partitionName, 100, 200, 2).toString())
                .isEqualTo(expectedSplitString);
    }

    @Test
    void testSkipCurrentRoundSerde() throws Exception {
        TieringSnapshotSplit snapshotSplitWithSkipCurrentRound =
                new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 200L, 10, true);
        byte[] serialized = serializer.serialize(snapshotSplitWithSkipCurrentRound);
        TieringSnapshotSplit deserializedSnapshotSplit =
                (TieringSnapshotSplit) serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSnapshotSplit).isEqualTo(snapshotSplitWithSkipCurrentRound);

        TieringLogSplit logSplitWithSkipCurrentRound =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, true);
        serialized = serializer.serialize(logSplitWithSkipCurrentRound);
        TieringLogSplit deserializedLogSplit =
                (TieringLogSplit) serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedLogSplit).isEqualTo(logSplitWithSkipCurrentRound);

        TieringSnapshotSplit snapshotSplit =
                new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 200L, 10, false);
        assertThat(snapshotSplit.shouldSkipCurrentRound()).isFalse();
        snapshotSplit.skipCurrentRound();
        assertThat(snapshotSplit.shouldSkipCurrentRound()).isTrue();

        serialized = serializer.serialize(snapshotSplit);
        deserializedSnapshotSplit =
                (TieringSnapshotSplit) serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSnapshotSplit).isEqualTo(snapshotSplit);

        TieringLogSplit logSplit =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, false);
        assertThat(logSplit.shouldSkipCurrentRound()).isFalse();
        logSplit.skipCurrentRound();
        assertThat(logSplit.shouldSkipCurrentRound()).isTrue();

        serialized = serializer.serialize(logSplit);
        deserializedLogSplit =
                (TieringLogSplit) serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedLogSplit).isEqualTo(logSplit);
    }

    private static byte[] serializeVersion2TieringLogSplit(Map<Long, byte[]> logDvSnapshot)
            throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeByte((byte) 2);
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());
        out.writeLong(tableBucket.getTableId());
        out.writeInt(tableBucket.getBucket());
        out.writeBoolean(false);
        out.writeInt(40);
        out.writeBoolean(false);
        out.writeLong(100L);
        out.writeLong(200L);
        out.writeBoolean(true);
        out.writeInt(logDvSnapshot.size());
        for (Map.Entry<Long, byte[]> entry : logDvSnapshot.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }
        return out.getCopyOfBuffer();
    }
}
