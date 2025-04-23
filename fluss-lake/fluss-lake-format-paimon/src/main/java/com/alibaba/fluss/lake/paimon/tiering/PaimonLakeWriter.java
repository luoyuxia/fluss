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

import com.alibaba.fluss.lake.paimon.record.FlussRecordAsPaimonRow;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.record.LogRecord;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link LakeWriter} for Paimon. */
public class PaimonLakeWriter implements LakeWriter<PaimonWriteResult> {

    private final RecordWriter<InternalRow> recordWriter;
    private final BinaryRow partition;
    private final int bucket;

    public PaimonLakeWriter(RecordWriter<InternalRow> recordWriter, String partition, int bucket) {
        this.recordWriter = recordWriter;
        this.partition = new BinaryRow(1);
        this.bucket = bucket;
    }

    @Override
    public void write(LogRecord record) throws IOException {
        try {
            recordWriter.write(new FlussRecordAsPaimonRow(record));
        } catch (Exception e) {
            throw new IOException("Fail to write Fluss record into Paimon.");
        }
    }

    @Override
    public PaimonWriteResult complete() throws IOException {
        try {
            // prepare commit
            CommitIncrement increment = recordWriter.prepareCommit(true);
            List<IndexFileMeta> newIndexFiles = new ArrayList<>();
            CompactDeletionFile compactDeletionFile = increment.compactDeletionFile();
            if (compactDeletionFile != null) {
                compactDeletionFile.getOrCompute().ifPresent(newIndexFiles::add);
            }
            CommitMessageImpl committable =
                    new CommitMessageImpl(
                            partition,
                            bucket,
                            increment.newFilesIncrement(),
                            increment.compactIncrement(),
                            new IndexIncrement(newIndexFiles));
            return new PaimonWriteResult(committable);
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
