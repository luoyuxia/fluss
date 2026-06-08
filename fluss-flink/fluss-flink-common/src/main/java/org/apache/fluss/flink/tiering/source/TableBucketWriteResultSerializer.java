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

import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The serializer for {@link TableBucketWriteResult}. */
public class TableBucketWriteResultSerializer<WriteResult>
        implements SimpleVersionedSerializer<TableBucketWriteResult<WriteResult>> {

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int CURRENT_VERSION = 1;

    private final org.apache.fluss.lake.serializer.SimpleVersionedSerializer<WriteResult>
            writeResultSerializer;

    public TableBucketWriteResultSerializer(
            org.apache.fluss.lake.serializer.SimpleVersionedSerializer<WriteResult>
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
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        // serialize table path
        TablePath tablePath = tableBucketWriteResult.tablePath();
        out.writeUTF(tablePath.getDatabaseName());
        out.writeUTF(tablePath.getTableName());

        // serialize bucket
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        out.writeLong(tableBucket.getTableId());
        // write partition
        if (tableBucket.getPartitionId() != null) {
            out.writeBoolean(true);
            out.writeLong(tableBucket.getPartitionId());
            out.writeUTF(tableBucketWriteResult.partitionName());
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(tableBucket.getBucket());

        // serialize write result
        WriteResult writeResult = tableBucketWriteResult.writeResult();
        if (writeResult == null) {
            // write -1 to mark write result as null
            out.writeInt(-1);
        } else {
            byte[] serializeBytes = writeResultSerializer.serialize(writeResult);
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
        }

        // serialize log end offset
        out.writeLong(tableBucketWriteResult.logEndOffset());

        // serialize max timestamp
        out.writeLong(tableBucketWriteResult.maxTimestamp());

        // serialize number of write results
        out.writeInt(tableBucketWriteResult.numberOfWriteResults());

        // serialize RowPosResult (SstMeta-based format)
        RowPosResult rowPosResult = tableBucketWriteResult.getRowPosResult();
        if (rowPosResult != null) {
            out.writeBoolean(true);
            out.writeInt(rowPosResult.getBucketId());
            // serialize partition info
            if (rowPosResult.getPartitionId() != null) {
                out.writeBoolean(true);
                out.writeLong(rowPosResult.getPartitionId());
                out.writeUTF(rowPosResult.getPartitionName());
            } else {
                out.writeBoolean(false);
            }
            // serialize newFileDictEntries
            Map<Integer, String> newFileDictEntries = rowPosResult.getNewFileDictEntries();
            out.writeInt(newFileDictEntries.size());
            for (Map.Entry<Integer, String> entry : newFileDictEntries.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            // serialize sstMetas
            List<RowPosResult.SstMeta> sstMetas = rowPosResult.getSstMetas();
            out.writeInt(sstMetas.size());
            for (RowPosResult.SstMeta meta : sstMetas) {
                out.writeUTF(meta.getFileName());
                out.writeLong(meta.getFileSize());
            }
        } else {
            out.writeBoolean(false);
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TableBucketWriteResult<WriteResult> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != CURRENT_VERSION && version != 0) {
            throw new IOException("Unknown version " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        // deserialize table path
        String databaseName = in.readUTF();
        String tableName = in.readUTF();
        TablePath tablePath = new TablePath(databaseName, tableName);

        // deserialize bucket
        long tableId = in.readLong();
        Long partitionId = null;
        String partitionName = null;
        if (in.readBoolean()) {
            partitionId = in.readLong();
            partitionName = in.readUTF();
        }
        int bucketId = in.readInt();
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);

        // deserialize write result
        int writeResultLength = in.readInt();
        WriteResult writeResult;
        if (writeResultLength >= 0) {
            byte[] writeResultBytes = new byte[writeResultLength];
            in.readFully(writeResultBytes);
            writeResult =
                    writeResultSerializer.deserialize(
                            writeResultSerializer.getVersion(), writeResultBytes);
        } else {
            writeResult = null;
        }

        // deserialize log end offset
        long logEndOffset = in.readLong();
        // deserialize max timestamp
        long maxTimestamp = in.readLong();
        // deserialize number of write results
        int numberOfWriteResults = in.readInt();

        // deserialize RowPosResult (SstMeta-based format)
        RowPosResult rowPosResult = null;
        if (in.readBoolean()) {
            int rowPosBucketId = in.readInt();
            // deserialize partition info (version >= 1)
            Long rowPosPartitionId = null;
            String rowPosPartitionName = null;
            if (version >= 1 && in.readBoolean()) {
                rowPosPartitionId = in.readLong();
                rowPosPartitionName = in.readUTF();
            }
            // deserialize newFileDictEntries
            int dictSize = in.readInt();
            Map<Integer, String> newFileDictEntries = new HashMap<>(dictSize);
            for (int d = 0; d < dictSize; d++) {
                int fileId = in.readInt();
                String fileName = in.readUTF();
                newFileDictEntries.put(fileId, fileName);
            }
            // deserialize sstMetas
            int sstMetaCount = in.readInt();
            List<RowPosResult.SstMeta> sstMetas = new ArrayList<>(sstMetaCount);
            for (int s = 0; s < sstMetaCount; s++) {
                String sstFileName = in.readUTF();
                long sstFileSize = in.readLong();
                sstMetas.add(new RowPosResult.SstMeta(sstFileName, sstFileSize));
            }
            rowPosResult =
                    new RowPosResult(
                            rowPosBucketId,
                            rowPosPartitionId,
                            rowPosPartitionName,
                            newFileDictEntries,
                            sstMetas);
        }

        return new TableBucketWriteResult<>(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                logEndOffset,
                maxTimestamp,
                numberOfWriteResults,
                rowPosResult);
    }
}
