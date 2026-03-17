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

import org.apache.fluss.flink.tiering.source.TieringSource;
import org.apache.fluss.flink.tiering.source.enumerator.TieringSourceEnumerator;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A serializer for the {@link TieringSplit}.
 *
 * <p>This serializer is only used to serialize and deserialize splits sent from {@link
 * TieringSourceEnumerator} to {@link TieringSource} for network transmission. Therefore, it does
 * not need to consider compatibility.
 */
public class TieringSplitSerializer implements SimpleVersionedSerializer<TieringSplit> {

    public static final TieringSplitSerializer INSTANCE = new TieringSplitSerializer();

    private static final int VERSION_0 = 0;
    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int VERSION_3 = 3;

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final byte TIERING_SNAPSHOT_SPLIT_FLAG = 1;
    private static final byte TIERING_LOG_SPLIT_FLAG = 2;

    private static final int CURRENT_VERSION = VERSION_3;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TieringSplit split) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        byte splitKind = split.splitKind();
        out.writeByte(splitKind);

        TablePath tablePath = split.getTablePath();
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());

        TableBucket tableBucket = split.getTableBucket();
        out.writeLong(tableBucket.getTableId());
        out.writeInt(tableBucket.getBucket());

        if (split.getTableBucket().getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(split.getTableBucket().getPartitionId());
            out.writeUTF(split.getPartitionName());
        } else {
            out.writeBoolean(false);
        }

        out.writeInt(split.getNumberOfSplits());
        out.writeBoolean(split.shouldSkipCurrentRound());
        if (split.isTieringSnapshotSplit()) {
            TieringSnapshotSplit tieringSnapshotSplit = split.asTieringSnapshotSplit();
            out.writeLong(tieringSnapshotSplit.getSnapshotId());
            out.writeLong(tieringSnapshotSplit.getLogOffsetOfSnapshot());
            writeStringByteMap(out, tieringSnapshotSplit.getLakeDvSnapshot());
        } else {
            TieringLogSplit tieringLogSplit = split.asTieringLogSplit();
            out.writeLong(tieringLogSplit.getStartingOffset());
            out.writeLong(tieringLogSplit.getStoppingOffset());
            writeStringByteMap(out, tieringLogSplit.getLakeDvSnapshot());
            writeLongByteMap(out, tieringLogSplit.getLogDvSnapshot());
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TieringSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_0
                && version != VERSION_1
                && version != VERSION_2
                && version != VERSION_3) {
            throw new IOException("Unknown version " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        byte splitKind = in.readByte();

        String databaseName = in.readUTF();
        String tableName = in.readUTF();
        TablePath tablePath = new TablePath(databaseName, tableName);

        long tableId = in.readLong();
        int bucketId = in.readInt();

        Long partitionId = null;
        String partitionName = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
            partitionName = in.readUTF();
        }
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        int numberOfSplits = in.readInt();
        boolean skipCurrentRound = in.readBoolean();

        if (splitKind == TIERING_SNAPSHOT_SPLIT_FLAG) {
            long snapshotId = in.readLong();
            long logOffsetOfSnapshot = in.readLong();
            Map<String, byte[]> lakeDvSnapshot = readStringByteMap(in);
            return new TieringSnapshotSplit(
                    tablePath,
                    tableBucket,
                    partitionName,
                    snapshotId,
                    logOffsetOfSnapshot,
                    numberOfSplits,
                    skipCurrentRound,
                    lakeDvSnapshot);
        } else {
            long startingOffset = in.readLong();
            long stoppingOffset = in.readLong();
            Map<String, byte[]> lakeDvSnapshot = null;
            if (version >= VERSION_3) {
                lakeDvSnapshot = readStringByteMap(in);
            }
            Map<Long, byte[]> logDvSnapshot = null;
            if (version >= VERSION_2) {
                logDvSnapshot = readLongByteMap(in);
            }
            return new TieringLogSplit(
                    tablePath,
                    tableBucket,
                    partitionName,
                    startingOffset,
                    stoppingOffset,
                    numberOfSplits,
                    skipCurrentRound,
                    lakeDvSnapshot,
                    logDvSnapshot);
        }
    }

    private static void writeStringByteMap(
            DataOutputSerializer out, Map<String, byte[]> stringByteMap) throws IOException {
        if (stringByteMap != null) {
            out.writeBoolean(true);
            out.writeInt(stringByteMap.size());
            for (Map.Entry<String, byte[]> entry : stringByteMap.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeInt(entry.getValue().length);
                out.write(entry.getValue());
            }
        } else {
            out.writeBoolean(false);
        }
    }

    private static Map<String, byte[]> readStringByteMap(DataInputDeserializer in)
            throws IOException {
        Map<String, byte[]> stringByteMap = null;
        boolean hasStringByteMap = in.readBoolean();
        if (hasStringByteMap) {
            int mapSize = in.readInt();
            stringByteMap = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                String key = in.readUTF();
                int valueLength = in.readInt();
                byte[] value = new byte[valueLength];
                in.readFully(value);
                stringByteMap.put(key, value);
            }
        }
        return stringByteMap;
    }

    private static void writeLongByteMap(DataOutputSerializer out, Map<Long, byte[]> longByteMap)
            throws IOException {
        if (longByteMap != null) {
            out.writeBoolean(true);
            out.writeInt(longByteMap.size());
            for (Map.Entry<Long, byte[]> entry : longByteMap.entrySet()) {
                out.writeLong(entry.getKey());
                out.writeInt(entry.getValue().length);
                out.write(entry.getValue());
            }
        } else {
            out.writeBoolean(false);
        }
    }

    private static Map<Long, byte[]> readLongByteMap(DataInputDeserializer in) throws IOException {
        Map<Long, byte[]> longByteMap = null;
        boolean hasLongByteMap = in.readBoolean();
        if (hasLongByteMap) {
            int mapSize = in.readInt();
            longByteMap = new HashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                long key = in.readLong();
                int valueLength = in.readInt();
                byte[] value = new byte[valueLength];
                in.readFully(value);
                longByteMap.put(key, value);
            }
        }
        return longByteMap;
    }
}
