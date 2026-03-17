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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** The serializer for {@link TableBucketWriteResult}. */
public class TableBucketWriteResultSerializer<WriteResult>
        implements SimpleVersionedSerializer<TableBucketWriteResult<WriteResult>> {

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int CURRENT_VERSION = 3;

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
        if (CURRENT_VERSION >= 3) {
            out.writeInt(writeResult == null ? -1 : writeResultSerializer.getVersion());
        }
        if (writeResult == null) {
            // write -1 to mark write result as null
            out.writeInt(-1);
        } else {
            byte[] serializeBytes = writeResultSerializer.serialize(writeResult);
            out.writeInt(serializeBytes.length);
            out.write(serializeBytes);
        }

        // serialize split start offset
        out.writeLong(tableBucketWriteResult.splitStartOffset());

        // serialize log end offset
        out.writeLong(tableBucketWriteResult.logEndOffset());

        // serialize max timestamp
        out.writeLong(tableBucketWriteResult.maxTimestamp());

        // serialize number of write results
        out.writeInt(tableBucketWriteResult.numberOfWriteResults());

        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TableBucketWriteResult<WriteResult> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version <= 0 || version > CURRENT_VERSION) {
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
        WriteResult writeResult;
        int writeResultVersion = version >= 3 ? in.readInt() : -1;
        int writeResultLength = in.readInt();
        if (writeResultLength >= 0) {
            byte[] writeResultBytes = new byte[writeResultLength];
            in.readFully(writeResultBytes);
            writeResult = deserializeWriteResult(version, writeResultVersion, writeResultBytes);
        } else {
            writeResult = null;
        }

        long splitStartOffset = version >= 2 ? in.readLong() : -1L;
        // deserialize log end offset
        long logEndOffset = in.readLong();
        // deserialize max timestamp
        long maxTimestamp = in.readLong();
        // deserialize number of write results
        int numberOfWriteResults = in.readInt();
        return new TableBucketWriteResult<>(
                tablePath,
                tableBucket,
                partitionName,
                writeResult,
                splitStartOffset,
                logEndOffset,
                maxTimestamp,
                numberOfWriteResults);
    }

    private WriteResult deserializeWriteResult(
            int serializerVersion, int embeddedWriteResultVersion, byte[] writeResultBytes)
            throws IOException {
        if (embeddedWriteResultVersion >= 0) {
            return writeResultSerializer.deserialize(embeddedWriteResultVersion, writeResultBytes);
        }

        IOException firstIo = null;
        RuntimeException firstRuntime = null;
        int currentWriteResultVersion = writeResultSerializer.getVersion();
        try {
            return writeResultSerializer.deserialize(currentWriteResultVersion, writeResultBytes);
        } catch (IOException e) {
            firstIo = e;
        } catch (RuntimeException e) {
            firstRuntime = e;
        }

        if (serializerVersion != currentWriteResultVersion) {
            try {
                return writeResultSerializer.deserialize(serializerVersion, writeResultBytes);
            } catch (IOException | RuntimeException fallback) {
                if (firstIo != null) {
                    fallback.addSuppressed(firstIo);
                }
                if (firstRuntime != null) {
                    fallback.addSuppressed(firstRuntime);
                }
                if (fallback instanceof IOException) {
                    throw (IOException) fallback;
                }
                throw new IOException("Failed to deserialize nested write result.", fallback);
            }
        }

        if (firstIo != null) {
            throw firstIo;
        }
        throw new IOException("Failed to deserialize nested write result.", firstRuntime);
    }
}
