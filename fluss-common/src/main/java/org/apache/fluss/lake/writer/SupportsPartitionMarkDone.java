/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.writer;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Optional factory capability for marking idle lake partitions done.
 *
 * @param <WriteResult> the write result type
 * @param <CommittableT> the committable type accepted by the corresponding lake committer
 */
@Internal
public interface SupportsPartitionMarkDone<WriteResult, CommittableT>
        extends LakeTieringFactory<WriteResult, CommittableT> {

    /** Creates a lake committer that also prepares partition mark-done maintenance. */
    @Override
    Committer<WriteResult, CommittableT> createLakeCommitter(CommitterInitContext context)
            throws IOException;

    /**
     * A lake committer supporting partition mark-done maintenance within its own lifecycle.
     *
     * @param <WriteResult> the write result type
     * @param <CommittableT> the committable type
     */
    @Internal
    interface Committer<WriteResult, CommittableT>
            extends LakeCommitter<WriteResult, CommittableT> {

        /** Returns whether mark-done is enabled for this committer, without additional I/O. */
        boolean isPartitionMarkDoneEnabled();

        /**
         * Runs idempotent mark-done actions and prepares the state to persist for an empty round.
         *
         * <p>The caller prepares fresh offsets metadata only when this returns a committable, then
         * persists it through {@link LakeCommitter#commit} on this committer. Actions may be
         * retried after a failure.
         *
         * @return a committable containing the updated state, or null if no commit is needed
         */
        @Nullable
        CommittableT markPartitionsDone() throws IOException;
    }
}
