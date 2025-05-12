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

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecord;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.util.List;

import static com.alibaba.fluss.lake.paimon.utils.PaimonConversions.toPaimonBinaryRow;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** A base interface to write {@link LogRecord} to Paimon. */
public abstract class RecordWriter<T> implements AutoCloseable {

    protected final TableWriteImpl<T> tableWrite;
    protected final int bucket;
    @Nullable protected final BinaryRow partition;
    protected final FlussRecordAsPaimonRow flussRecordAsPaimonRow;

    public RecordWriter(
            TableWriteImpl<T> tableWrite, TableBucket tableBucket, @Nullable String partition) {
        this.tableWrite = tableWrite;
        this.bucket = tableBucket.getBucket();
        this.partition = toPaimonBinaryRow(partition);
        this.flussRecordAsPaimonRow = new FlussRecordAsPaimonRow(tableBucket.getBucket());
    }

    public abstract void write(LogRecord record) throws Exception;

    CommitMessage complete() throws Exception {
        List<CommitMessage> commitMessages = tableWrite.prepareCommit();
        checkState(commitMessages.size() == 1, "The size of CommitMessage must be 1.");
        return commitMessages.get(0);
    }

    public void close() throws Exception {
        tableWrite.close();
    }
}
