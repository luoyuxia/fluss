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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** The {@link SimpleVersionedSerializer} for {@link PaimonWriteResult}. */
public class PaimonWriteResultSerializer implements SimpleVersionedSerializer<PaimonWriteResult> {

    private static final int CURRENT_VERSION = 1;

    private final CommitMessageSerializer messageSer = new CommitMessageSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PaimonWriteResult paimonWriteResult) throws IOException {
        List<CommitMessage> commitMessages = paimonWriteResult.commitMessages();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(commitMessages.size());
        for (CommitMessage commitMessage : commitMessages) {
            byte[] serialized = messageSer.serialize(commitMessage);
            out.writeInt(serialized.length);
            out.write(serialized);
        }
        out.flush();
        return baos.toByteArray();
    }

    @Override
    public PaimonWriteResult deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting PaimonWriteResult version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
        int count = in.readInt();
        List<CommitMessage> commitMessages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = in.readInt();
            byte[] msgBytes = new byte[len];
            in.readFully(msgBytes);
            commitMessages.add(messageSer.deserialize(messageSer.getVersion(), msgBytes));
        }
        return new PaimonWriteResult(commitMessages);
    }
}
