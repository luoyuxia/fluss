/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.source.split;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** . */
public class TablePathSplitSerializer implements SimpleVersionedSerializer<TablePathLogSplit> {

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int VERSION_0 = 0;

    private static final int CURRENT_VERSION = VERSION_0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TablePathLogSplit tablePathLogSplit) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        // write table path
        TablePath tablePath = tablePathLogSplit.getTablePath();
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());
        // write common part
        serializeSourceSplitBase(out, tablePathLogSplit);
        // write starting offset
        out.writeLong(tablePathLogSplit.getStartingOffset());
        // write stopping offset
        out.writeLong(tablePathLogSplit.getStoppingOffset().orElse(LogSplit.NO_STOPPING_OFFSET));

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TablePathLogSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION_0) {
            throw new IOException("Unknown version " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        TablePath tablePath = new TablePath(in.readUTF(), in.readUTF());
        // deserialize split bucket
        long tableId = in.readLong();
        Long partitionId = null;
        String partitionName = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
            partitionName = in.readUTF();
        }
        int bucketId = in.readInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
        long startingOffset = in.readLong();
        long stoppingOffset = in.readLong();
        return new TablePathLogSplit(
                tablePath, tableBucket, partitionName, startingOffset, stoppingOffset);
    }

    private void serializeSourceSplitBase(DataOutputSerializer out, SourceSplitBase sourceSplitBase)
            throws IOException {
        // write bucket
        TableBucket tableBucket = sourceSplitBase.getTableBucket();
        out.writeLong(tableBucket.getTableId());
        // write partition
        if (sourceSplitBase.getTableBucket().getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(sourceSplitBase.getTableBucket().getPartitionId());
            out.writeUTF(sourceSplitBase.getPartitionName());
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(tableBucket.getBucket());
    }
}
