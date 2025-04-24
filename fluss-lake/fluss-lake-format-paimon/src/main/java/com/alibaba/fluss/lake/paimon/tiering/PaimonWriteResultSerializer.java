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

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.IOException;

/** . */
public class PaimonWriteResultSerializer implements SimpleVersionedSerializer<PaimonWriteResult> {

    private final CommitMessageSerializer messageSer = new CommitMessageSerializer();

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PaimonWriteResult writeResult) throws IOException {
        if (!writeResult.getCommitMessage().isPresent()) {
            return new byte[0];
        }
        CommitMessage commitMessage = writeResult.getCommitMessage().get();
        return messageSer.serialize(commitMessage);
    }

    @Override
    public PaimonWriteResult deserialize(int version, byte[] serialized) throws IOException {
        CommitMessage commitMessage = null;
        if (serialized.length != 0) {
            commitMessage = messageSer.deserialize(version, serialized);
        }
        return new PaimonWriteResult(commitMessage);
    }
}
