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

import com.alibaba.fluss.flink.source.split.LogSplitState;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/** The Flink emitter to emit {@link TableBucketWriteResult} to downstream operator. */
public class TableBucketWriteResultEmitter<WriteResult>
        implements RecordEmitter<
                TableBucketWriteResult<WriteResult>,
                TableBucketWriteResult<WriteResult>,
                LogSplitState> {
    @Override
    public void emitRecord(
            TableBucketWriteResult<WriteResult> writeResult,
            SourceOutput<TableBucketWriteResult<WriteResult>> sourceOutput,
            LogSplitState logSplitState)
            throws Exception {
        sourceOutput.collect(writeResult);
    }
}
