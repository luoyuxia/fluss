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

package org.apache.fluss.flink.lake;

import org.apache.fluss.flink.lake.split.DvAwareFlussLogSplit;
import org.apache.fluss.flink.lake.split.DvAwareLakeSnapshotSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.flink.lake.split.DvAwareFlussLogSplit.DV_AWARE_FLUSS_LOG_SPLIT_KIND;
import static org.apache.fluss.flink.lake.split.DvAwareLakeSnapshotSplit.DV_AWARE_LAKE_SNAPSHOT_SPLIT_KIND;
import static org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit.LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
import static org.apache.fluss.flink.lake.split.LakeSnapshotSplit.LAKE_SNAPSHOT_SPLIT_KIND;

/** A serializer for lake split. */
public class LakeSplitSerializer {

    private final SimpleVersionedSerializer<LakeSplit> sourceSplitSerializer;

    public LakeSplitSerializer(SimpleVersionedSerializer<LakeSplit> sourceSplitSerializer) {
        this.sourceSplitSerializer = sourceSplitSerializer;
    }

    public void serialize(DataOutputSerializer out, SourceSplitBase split) throws IOException {
        out.writeInt(sourceSplitSerializer.getVersion());
        if (split instanceof LakeSnapshotSplit) {
            LakeSnapshotSplit lakeSplit = (LakeSnapshotSplit) split;
            out.writeInt(lakeSplit.getSplitIndex());
            byte[] serializeBytes = sourceSplitSerializer.serialize(lakeSplit.getLakeSplit());
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
        } else if (split instanceof DvAwareLakeSnapshotSplit) {
            DvAwareLakeSnapshotSplit lakeSplit = (DvAwareLakeSnapshotSplit) split;
            out.writeInt(lakeSplit.getSplitIndex());
            byte[] serializeBytes = sourceSplitSerializer.serialize(lakeSplit.getLakeSplit());
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
            out.writeLong(lakeSplit.getRecordsToSkip());
            writeStringByteMap(out, lakeSplit.getLakeDvSnapshot());
        } else if (split instanceof DvAwareFlussLogSplit) {
            DvAwareFlussLogSplit logSplit = (DvAwareFlussLogSplit) split;
            out.writeLong(logSplit.getStartingOffset());
            out.writeLong(logSplit.getStoppingOffset().orElse(LogSplit.NO_STOPPING_OFFSET));
            writeLongByteMap(out, logSplit.getLogDvSnapshot());
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            LakeSnapshotAndFlussLogSplit lakeSnapshotAndFlussLogSplit =
                    (LakeSnapshotAndFlussLogSplit) split;
            writeLakeSplits(out, lakeSnapshotAndFlussLogSplit.getLakeSplits());
            out.writeLong(lakeSnapshotAndFlussLogSplit.getStartingOffset());
            out.writeLong(
                    lakeSnapshotAndFlussLogSplit
                            .getStoppingOffset()
                            .orElse(LogSplit.NO_STOPPING_OFFSET));
            out.writeLong(lakeSnapshotAndFlussLogSplit.getRecordsToSkip());
            out.writeInt(lakeSnapshotAndFlussLogSplit.getCurrentLakeSplitIndex());
            out.writeBoolean(lakeSnapshotAndFlussLogSplit.isLakeSplitFinished());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported split type: " + split.getClass().getName());
        }
    }

    public SourceSplitBase deserialize(
            byte splitKind,
            TableBucket tableBucket,
            @Nullable String partition,
            DataInputDeserializer input)
            throws IOException {
        int version = input.readInt();
        if (splitKind == LAKE_SNAPSHOT_SPLIT_KIND) {
            int splitIndex = input.readInt();
            byte[] serializeBytes = new byte[input.readInt()];
            input.readFully(serializeBytes);
            LakeSplit lakeSplit = sourceSplitSerializer.deserialize(version, serializeBytes);
            return new LakeSnapshotSplit(tableBucket, partition, lakeSplit, splitIndex);
        } else if (splitKind == DV_AWARE_LAKE_SNAPSHOT_SPLIT_KIND) {
            int splitIndex = input.readInt();
            byte[] serializeBytes = new byte[input.readInt()];
            input.readFully(serializeBytes);
            LakeSplit lakeSplit = sourceSplitSerializer.deserialize(version, serializeBytes);
            long recordsToSkip = input.readLong();
            Map<String, byte[]> lakeDvSnapshot = readStringByteMap(input);
            return new DvAwareLakeSnapshotSplit(
                    tableBucket, partition, lakeSplit, splitIndex, recordsToSkip, lakeDvSnapshot);
        } else if (splitKind == DV_AWARE_FLUSS_LOG_SPLIT_KIND) {
            long startingOffset = input.readLong();
            long stoppingOffset = input.readLong();
            Map<Long, byte[]> logDvSnapshot = readLongByteMap(input);
            return new DvAwareFlussLogSplit(
                    tableBucket, partition, startingOffset, stoppingOffset, logDvSnapshot);
        } else if (splitKind == LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND) {
            List<LakeSplit> lakeSplits = readLakeSplits(input, version);
            long startingOffset = input.readLong();
            long stoppingOffset = input.readLong();
            long recordsToSkip = input.readLong();
            int splitIndex = input.readInt();
            boolean isLakeSplitFinished = input.readBoolean();
            return new LakeSnapshotAndFlussLogSplit(
                    tableBucket,
                    partition,
                    lakeSplits,
                    startingOffset,
                    stoppingOffset,
                    recordsToSkip,
                    splitIndex,
                    isLakeSplitFinished);
        } else {
            throw new UnsupportedOperationException("Unsupported split kind: " + splitKind);
        }
    }

    private void writeLakeSplits(DataOutputSerializer out, @Nullable List<LakeSplit> lakeSplits)
            throws IOException {
        if (lakeSplits == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        out.writeInt(lakeSplits.size());
        for (LakeSplit lakeSplit : lakeSplits) {
            byte[] serializeBytes = sourceSplitSerializer.serialize(lakeSplit);
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
        }
    }

    @Nullable
    private List<LakeSplit> readLakeSplits(DataInputDeserializer input, int version)
            throws IOException {
        if (!input.readBoolean()) {
            return null;
        }

        int lakeSplitSize = input.readInt();
        List<LakeSplit> lakeSplits = new ArrayList<>(lakeSplitSize);
        for (int i = 0; i < lakeSplitSize; i++) {
            byte[] serializeBytes = new byte[input.readInt()];
            input.readFully(serializeBytes);
            lakeSplits.add(sourceSplitSerializer.deserialize(version, serializeBytes));
        }
        return lakeSplits;
    }

    private static void writeStringByteMap(
            DataOutputSerializer out, @Nullable Map<String, byte[]> stringByteMap)
            throws IOException {
        if (stringByteMap == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        out.writeInt(stringByteMap.size());
        for (Map.Entry<String, byte[]> entry : stringByteMap.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }
    }

    @Nullable
    private static Map<String, byte[]> readStringByteMap(DataInputDeserializer in)
            throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        int mapSize = in.readInt();
        Map<String, byte[]> stringByteMap = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String key = in.readUTF();
            int valueLength = in.readInt();
            byte[] value = new byte[valueLength];
            in.readFully(value);
            stringByteMap.put(key, value);
        }
        return stringByteMap;
    }

    private static void writeLongByteMap(
            DataOutputSerializer out, @Nullable Map<Long, byte[]> longByteMap) throws IOException {
        if (longByteMap == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        out.writeInt(longByteMap.size());
        for (Map.Entry<Long, byte[]> entry : longByteMap.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }
    }

    @Nullable
    private static Map<Long, byte[]> readLongByteMap(DataInputDeserializer in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        int mapSize = in.readInt();
        Map<Long, byte[]> longByteMap = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            long key = in.readLong();
            int valueLength = in.readInt();
            byte[] value = new byte[valueLength];
            in.readFully(value);
            longByteMap.put(key, value);
        }
        return longByteMap;
    }

    private static void writeByteArray(DataOutputSerializer out, @Nullable byte[] value)
            throws IOException {
        if (value == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeInt(value.length);
        out.write(value);
    }

    @Nullable
    private static byte[] readByteArray(DataInputDeserializer in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        byte[] value = new byte[in.readInt()];
        in.readFully(value);
        return value;
    }
}
