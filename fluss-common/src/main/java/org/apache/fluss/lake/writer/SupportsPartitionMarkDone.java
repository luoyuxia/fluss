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

        /**
         * Executes partition mark-done processing and attaches the resulting state to the given
         * committable.
         *
         * <p>If the job or table settings disable mark-done, this method returns {@code false}
         * without running actions or modifying the committable.
         *
         * <p>The caller invokes this once per round, after snapshot recovery and before preparing
         * offsets metadata. Empty rounds pass a committable created from an empty write-result
         * list. When enabled, the resulting state must also be attached when unchanged, since a
         * data or offsets commit may still be required. This method does not commit a lake
         * snapshot.
         *
         * <p>The return value describes only changes to mark-done state. The caller decides whether
         * to commit based on both this result and the round's data or offsets progress. Mark-done
         * actions may be retried if subsequent persistence fails and must therefore be idempotent.
         *
         * @param committable the current round's committable to update
         * @return whether the mark-done state changed and requires persistence
         * @throws IOException if preparation fails
         */
        boolean preparePartitionMarkDone(CommittableT committable) throws IOException;
    }
}
