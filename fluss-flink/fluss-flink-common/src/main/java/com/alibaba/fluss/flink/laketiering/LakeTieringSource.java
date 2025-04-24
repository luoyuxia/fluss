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

import com.alibaba.fluss.flink.source.split.TablePathLogSplit;
import com.alibaba.fluss.flink.source.split.TablePathSplitSerializer;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/** . */
public class LakeTieringSource<WriteResult>
        implements Source<
                TableBucketWriteResult<WriteResult>,
                TablePathLogSplit,
                LakeTieringEnumeratorState> {

    private final LakeTieringFactory<WriteResult, ?> lakeTieringFactory;

    public LakeTieringSource(LakeTieringFactory<WriteResult, ?> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<TablePathLogSplit, LakeTieringEnumeratorState> createEnumerator(
            SplitEnumeratorContext<TablePathLogSplit> splitEnumeratorContext) {
        return new LakeTieringSourceEnumerator(null, splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<TablePathLogSplit, LakeTieringEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<TablePathLogSplit> splitEnumeratorContext,
            LakeTieringEnumeratorState lakeTieringEnumeratorState) {
        return new LakeTieringSourceEnumerator(null, splitEnumeratorContext);
    }

    @Override
    public SimpleVersionedSerializer<TablePathLogSplit> getSplitSerializer() {
        return new TablePathSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<LakeTieringEnumeratorState>
            getEnumeratorCheckpointSerializer() {
        return new LakeTieringEnumeratorStateSerializer();
    }

    @Override
    public SourceReader<TableBucketWriteResult<WriteResult>, TablePathLogSplit> createReader(
            SourceReaderContext sourceReaderContext) {
        return new LakeTieringSourceReader<>(lakeTieringFactory, sourceReaderContext);
    }
}
