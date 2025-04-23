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

import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.record.LogRecord;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.CommitMessage;

import java.io.IOException;

/** Implementation of {@link LakeWriter} for Paimon. */
public class PaimonLakeWriter implements LakeWriter<PaimonWriteResult> {

    private final RecordWriter recordWriter;
    private final BinaryRow partition;
    private final int bucket;

    public PaimonLakeWriter(RecordWriter recordWriter, BinaryRow partition, int bucket) {
        this.recordWriter = recordWriter;
        this.partition = partition;
        this.bucket = bucket;
    }

    @Override
    public void write(LogRecord record) throws IOException {
        try {
            recordWriter.write(partition, bucket, record);
        } catch (Exception e) {
            throw new IOException("Fail to write Fluss record into Paimon.");
        }
    }

    @Override
    public PaimonWriteResult complete() throws IOException {
        try {
            // prepare commit
            CommitMessage commitMessage = recordWriter.complete();
            return new PaimonWriteResult(commitMessage);
        } catch (Exception e) {
            throw new IOException("Fail to complete the paimon writer.");
        }
    }

    @Override
    public void close() throws IOException {
        try {
            recordWriter.close();
        } catch (Exception e) {
            throw new IOException("Fail to close Paimon RecordWriter.");
        }
    }
}
