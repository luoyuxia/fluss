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

package com.alibaba.fluss.flink.laketiering;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** todo. */
public class TableBucketWriteResultSerializer<WriteResult>
        implements SimpleVersionedSerializer<TableBucketWriteResult<WriteResult>> {

    private static final int CURRENT_VERSION = 1;

    private final com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer<WriteResult>
            writeResultSerializer;

    public TableBucketWriteResultSerializer(
            com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer<WriteResult>
                    writeResultSerializer) {
        this.writeResultSerializer = writeResultSerializer;
    }

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(TableBucketWriteResult<WriteResult> tableBucketWriteResult)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out)) {
            // serialize table path
            TablePath tablePath = tableBucketWriteResult.tablePath();
            view.writeUTF(tablePath.getDatabaseName());
            view.writeUTF(tablePath.getTableName());

            // serialize bucket
            TableBucket tableBucket = tableBucketWriteResult.tableBucket();
            view.writeLong(tableBucket.getTableId());
            // write partition
            if (tableBucket.getPartitionId() != null) {
                view.writeBoolean(true);
                view.writeLong(tableBucket.getPartitionId());
            } else {
                view.writeBoolean(false);
            }
            view.writeInt(tableBucket.getBucket());

            // serialize write result
            WriteResult writeResult = tableBucketWriteResult.writeResult();
            byte[] serializeBytes = writeResultSerializer.serialize(writeResult);
            view.writeInt(serializeBytes.length);
            view.write(serializeBytes);

            return out.toByteArray();
        }
    }

    @Override
    public TableBucketWriteResult<WriteResult> deserialize(int version, byte[] serialized)
            throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in)) {
            // deserialize table path
            String databaseName = view.readUTF();
            String tableName = view.readUTF();
            TablePath tablePath = new TablePath(databaseName, tableName);

            // deserialize bucket
            long tableId = view.readLong();
            Long partitionId = null;
            if (view.readBoolean()) {
                partitionId = view.readLong();
            }
            int bucketId = view.readInt();
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

            // deserialize write result
            int writeResultLength = view.readInt();
            byte[] writeResultBytes = new byte[writeResultLength];
            view.readFully(writeResultBytes);
            WriteResult writeResult = writeResultSerializer.deserialize(version, writeResultBytes);

            return new TableBucketWriteResult<>(tablePath, tableBucket, writeResult);
        }
    }
}
