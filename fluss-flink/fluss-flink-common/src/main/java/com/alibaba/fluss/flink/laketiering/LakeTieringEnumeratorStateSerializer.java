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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/** todo. */
class LakeTieringEnumeratorStateSerializer
        implements SimpleVersionedSerializer<LakeTieringEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(LakeTieringEnumeratorState lakeTieringEnumeratorState)
            throws IOException {
        return new byte[0];
    }

    @Override
    public LakeTieringEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        return new LakeTieringEnumeratorState();
    }
}
