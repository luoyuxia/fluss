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

import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.writer.CommitterInitContext;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

/** . */
public class LakeTieringCommitterOperator<WriteResult, Committable>
        extends AbstractStreamOperator<Committable>
        implements OneInputStreamOperator<TableBucketWriteResult<WriteResult>, Committable> {

    private static final long serialVersionUID = 1L;

    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;

    private List<WriteResult> writeResults = new ArrayList<>();

    public LakeTieringCommitterOperator(
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
    }

    @Override
    public void processElement(StreamRecord<TableBucketWriteResult<WriteResult>> streamRecord)
            throws Exception {
        TableBucketWriteResult<WriteResult> tableBucketWriteResult = streamRecord.getValue();
        WriteResult writeResult = tableBucketWriteResult.writeResult();
        writeResults.add(writeResult);

        LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter(
                        new CommitterInitContext() {
                            @Override
                            public TablePath tablePath() {
                                return tableBucketWriteResult.tablePath();
                            }
                        });
        System.out.println("write result: " + writeResult);
        if (writeResults.size() == 1) {
            Committable committable = lakeCommitter.toCommitable(writeResults);
            lakeCommitter.commit(committable);
            output.collect(new StreamRecord<>(committable));
        }
    }
}
