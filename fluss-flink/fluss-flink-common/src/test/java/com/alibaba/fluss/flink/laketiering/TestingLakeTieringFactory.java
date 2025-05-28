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

import com.alibaba.fluss.flink.laketiering.committer.TestingCommittable;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.lakehouse.writer.WriterInitContext;
import com.alibaba.fluss.record.LogRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An implementation of {@link LakeTieringFactory} for testing purpose. */
public class TestingLakeTieringFactory
        implements LakeTieringFactory<TestingWriteResult, TestingCommittable> {

    @Override
    public LakeWriter<TestingWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new TestingLakeWriter();
    }

    @Override
    public SimpleVersionedSerializer<TestingWriteResult> getWriteResultSerializer() {
        return new TestingWriteResultSerializer();
    }

    @Override
    public LakeCommitter<TestingWriteResult, TestingCommittable> createLakeCommitter()
            throws IOException {
        return new TestingLakeCommitter();
    }

    @Override
    public SimpleVersionedSerializer<TestingCommittable> getCommitableSerializer() {
        throw new UnsupportedOperationException("Method getCommitableSerializer is not supported.");
    }

    private static final class TestingLakeWriter implements LakeWriter<TestingWriteResult> {

        private int writtenRecords;

        @Override
        public void write(LogRecord record) throws IOException {
            writtenRecords += 1;
        }

        @Override
        public TestingWriteResult complete() throws IOException {
            return new TestingWriteResult(writtenRecords);
        }

        @Override
        public void close() throws IOException {}
    }

    private static final class TestingLakeCommitter
            implements LakeCommitter<TestingWriteResult, TestingCommittable> {

        private long currentSnapshot = 0;

        @Override
        public TestingCommittable toCommitable(List<TestingWriteResult> testingWriteResults)
                throws IOException {
            List<Integer> writeResults = new ArrayList<>();
            for (TestingWriteResult testingWriteResult : testingWriteResults) {
                writeResults.add(testingWriteResult.getWriteResult());
            }
            return new TestingCommittable(writeResults);
        }

        @Override
        public long commit(TestingCommittable committable) throws IOException {
            return currentSnapshot++;
        }

        @Override
        public void close() throws Exception {}
    }
}
