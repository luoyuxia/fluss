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

import com.alibaba.fluss.record.LogRecord;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

import java.io.IOException;

public interface RecordWriter {

    void write(BinaryRow partition, int bucket, LogRecord logRecord) throws IOException;

    @Nullable
    CommitMessage complete() throws IOException;

    void close() throws Exception;
}
