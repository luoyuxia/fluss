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

package com.alibaba.fluss.lake.paimon.tiering.mergetree;

import com.alibaba.fluss.lake.paimon.record.FlussRecordAsPaimonRow;
import com.alibaba.fluss.lake.paimon.tiering.RecordWriter;
import com.alibaba.fluss.record.LogRecord;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.lake.paimon.record.FlussRecordAsPaimonRow.toRowKind;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/** Merge tree writer. */
public class MergeTreeWriter implements RecordWriter {

    private final TableWriteImpl<KeyValue> keyValueWriter;
    private final KeyValue keyValue = new KeyValue();
    private final RowKeyExtractor rowKeyExtractor;

    public MergeTreeWriter(FileStoreTable fileStoreTable) {
        fileStoreTable =
                fileStoreTable.copy(
                        Collections.singletonMap(
                                CoreOptions.CHANGELOG_PRODUCER.key(),
                                CoreOptions.ChangelogProducer.INPUT.name()));
        //noinspection unchecked
        this.keyValueWriter =
                (TableWriteImpl<KeyValue>) fileStoreTable.newWrite("fluss_lake_tiering_service");
        this.rowKeyExtractor = fileStoreTable.createRowKeyExtractor();
    }

    @Override
    public void write(BinaryRow partition, int bucket, LogRecord logRecord) throws IOException {
        try {
            InternalRow row = new FlussRecordAsPaimonRow(logRecord);
            rowKeyExtractor.setRecord(row);
            keyValue.replace(
                    rowKeyExtractor.trimmedPrimaryKey(),
                    KeyValue.UNKNOWN_SEQUENCE,
                    toRowKind(logRecord.getChangeType()),
                    row);
            keyValueWriter.getWrite().write(partition, bucket, keyValue);
        } catch (Exception e) {
            throw new IOException("Fail to write fluss record into paimon.", e);
        }
    }

    @Nullable
    @Override
    public CommitMessage complete() throws IOException {
        try {
            List<CommitMessage> commitMessages = keyValueWriter.prepareCommit();
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
        if (keyValueWriter != null) {
            keyValueWriter.close();
        }
    }
}
