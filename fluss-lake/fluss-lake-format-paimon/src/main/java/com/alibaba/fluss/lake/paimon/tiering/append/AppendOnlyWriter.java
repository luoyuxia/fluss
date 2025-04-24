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

package com.alibaba.fluss.lake.paimon.tiering.append;

import com.alibaba.fluss.lake.paimon.record.FlussRecordAsPaimonRow;
import com.alibaba.fluss.lake.paimon.tiering.RecordWriter;
import com.alibaba.fluss.record.LogRecord;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/** . */
public class AppendOnlyWriter implements RecordWriter {

    private final TableWriteImpl<InternalRow> tableWrite;

    public AppendOnlyWriter(FileStoreTable fileStoreTable) {
        //noinspection unchecked
        this.tableWrite =
                (TableWriteImpl<InternalRow>) fileStoreTable.newWrite("fluss_lake_tiering_service");
    }

    @Override
    public void write(BinaryRow partition, int bucket, LogRecord logRecord) throws IOException {
        try {
            tableWrite.getWrite().write(partition, bucket, new FlussRecordAsPaimonRow(logRecord));
        } catch (Exception e) {
            throw new IOException("Fail to write fluss record.", e);
        }
    }

    @Override
    @Nullable
    public CommitMessage complete() throws IOException {
        try {
            List<CommitMessage> commitMessages = tableWrite.prepareCommit();
            checkState(
                    commitMessages.size() <= 1,
                    "The size of CommitMessage must not be greater than 1");
            return commitMessages.get(0);
        } catch (Exception e) {
            throw new IOException("Fail to complete the paimon writer.", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (tableWrite != null) {
            tableWrite.close();
        }
    }
}
