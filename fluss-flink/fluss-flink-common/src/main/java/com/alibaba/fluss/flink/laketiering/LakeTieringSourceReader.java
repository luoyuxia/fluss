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
import com.alibaba.fluss.flink.source.split.TablePathSourceSplitState;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import java.util.Map;

/** todo. */
public class LakeTieringSourceReader<WriteResult>
        extends SingleThreadMultiplexSourceReaderBase<
                ReadPos<WriteResult>,
                TableBucketWriteResult<WriteResult>,
                TablePathLogSplit,
                TablePathSourceSplitState> {

    public LakeTieringSourceReader(
            LakeTieringFactory<WriteResult, ?> lakeTieringFactory, SourceReaderContext context) {
        super(
                () -> new LakeTieringWorker<>(lakeTieringFactory),
                new TableBucketWriteResultEmitter<>(),
                context.getConfiguration(),
                context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, TablePathSourceSplitState> finishedSplitIds) {
        // on nothing
    }

    @Override
    protected TablePathSourceSplitState initializedState(TablePathLogSplit split) {
        return new TablePathSourceSplitState(split);
    }

    @Override
    protected TablePathLogSplit toSplitType(String splitId, TablePathSourceSplitState splitState) {
        return splitState.toSourceSplit();
    }
}
