/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.iceberg.io.WriteResult;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Serializer for {@link IcebergWriteResult}. */
public class IcebergWriteResultSerializer implements SimpleVersionedSerializer<IcebergWriteResult> {

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;
    private static final int VERSION_3 = 3;
    private static final int VERSION_4 = 4;
    private static final int CURRENT_VERSION = VERSION_4;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(IcebergWriteResult icebergWriteResult) throws IOException {
        byte[] writeResultBytes =
                InstantiationUtils.serializeObject(icebergWriteResult.getWriteResult());

        RewriteDataFileResult rewriteDataFileResult = icebergWriteResult.rewriteDataFileResult();
        byte[] rewriteResultBytes =
                rewriteDataFileResult == null
                        ? null
                        : InstantiationUtils.serializeObject(rewriteDataFileResult);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(writeResultBytes.length);
                DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(writeResultBytes.length);
            dos.write(writeResultBytes);

            boolean hasRewrite = rewriteResultBytes != null;
            dos.writeBoolean(hasRewrite);
            if (hasRewrite) {
                dos.writeInt(rewriteResultBytes.length);
                dos.write(rewriteResultBytes);
            }

            byte[] positionReportBytes =
                    InstantiationUtils.serializeObject(icebergWriteResult.getPositionReport());
            dos.writeInt(positionReportBytes.length);
            dos.write(positionReportBytes);

            dos.writeLong(icebergWriteResult.getBaseSnapshotId());

            byte[] materializedDvFilesBytes =
                    InstantiationUtils.serializeObject(icebergWriteResult.getMaterializedDvFiles());
            dos.writeInt(materializedDvFilesBytes.length);
            dos.write(materializedDvFilesBytes);
            return baos.toByteArray();
        }
    }

    @Override
    public IcebergWriteResult deserialize(int version, byte[] serialized) throws IOException {
        WriteResult writeResult;
        RewriteDataFileResult rewriteDataFileResult;
        Map<String, List<long[]>> positionReport = null;
        long baseSnapshotId = -1L;
        List<String> materializedDvFiles = null;
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serialized))) {
            int wrLen = dis.readInt();
            if (wrLen < 0 || wrLen > serialized.length) {
                throw new IOException(
                        "Corrupted serialization: invalid WriteResult length " + wrLen);
            }
            byte[] wrBytes = new byte[wrLen];
            dis.readFully(wrBytes);
            writeResult =
                    InstantiationUtils.deserializeObject(wrBytes, getClass().getClassLoader());

            boolean hasCompaction = dis.readBoolean();
            if (hasCompaction) {
                int crLen = dis.readInt();
                if (crLen < 0 || crLen > serialized.length) {
                    throw new IOException(
                            "Corrupted serialization: invalid compactionResult length " + crLen);
                }
                byte[] crBytes = new byte[crLen];
                dis.readFully(crBytes);
                rewriteDataFileResult =
                        InstantiationUtils.deserializeObject(crBytes, getClass().getClassLoader());
            } else {
                rewriteDataFileResult = null;
            }

            if (version >= VERSION_2 && dis.available() > 0) {
                int positionReportLen = dis.readInt();
                byte[] positionReportBytes = new byte[positionReportLen];
                dis.readFully(positionReportBytes);
                positionReport =
                        InstantiationUtils.deserializeObject(
                                positionReportBytes, getClass().getClassLoader());

                if (version <= VERSION_3) {
                    int locallyDeletedRowIdsLen = dis.readInt();
                    byte[] locallyDeletedRowIdsBytes = new byte[locallyDeletedRowIdsLen];
                    dis.readFully(locallyDeletedRowIdsBytes);
                    InstantiationUtils.deserializeObject(
                            locallyDeletedRowIdsBytes, getClass().getClassLoader());
                }

                baseSnapshotId = dis.readLong();
            }

            if (version >= VERSION_3 && dis.available() > 0) {
                int materializedDvFilesLen = dis.readInt();
                byte[] materializedDvFilesBytes = new byte[materializedDvFilesLen];
                dis.readFully(materializedDvFilesBytes);
                materializedDvFiles =
                        InstantiationUtils.deserializeObject(
                                materializedDvFilesBytes, getClass().getClassLoader());
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not deserialize IcebergWriteResult.", e);
        }

        return new IcebergWriteResult(
                writeResult,
                rewriteDataFileResult,
                positionReport,
                baseSnapshotId,
                materializedDvFiles);
    }
}
