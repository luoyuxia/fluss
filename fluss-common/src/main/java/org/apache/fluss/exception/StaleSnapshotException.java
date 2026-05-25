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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.Internal;

/**
 * Exception thrown when a DV snapshot request references a snapshot ID that does not match the
 * current readable snapshot on the TabletServer.
 *
 * <p>Two cases:
 *
 * <ul>
 *   <li>{@code requestedSnapshotId > currentSnapshotId}: The TabletServer has not yet completed the
 *       Readable Switch for the requested snapshot. The client should backoff and retry.
 *   <li>{@code requestedSnapshotId < currentSnapshotId}: The requested snapshot has been superseded
 *       by a newer one. The client should refresh its LakeSnapshot and re-plan.
 * </ul>
 */
@Internal
public class StaleSnapshotException extends FlussException {
    private static final long serialVersionUID = 1L;

    private final long currentSnapshotId;
    private final long requestedSnapshotId;

    public StaleSnapshotException(long currentSnapshotId, long requestedSnapshotId) {
        super(
                String.format(
                        "Stale snapshot: requested %d, current %d",
                        requestedSnapshotId, currentSnapshotId));
        this.currentSnapshotId = currentSnapshotId;
        this.requestedSnapshotId = requestedSnapshotId;
    }

    /** Returns the current readable snapshot ID on the TabletServer. */
    public long getCurrentSnapshotId() {
        return currentSnapshotId;
    }

    /** Returns the snapshot ID that was requested by the client. */
    public long getRequestedSnapshotId() {
        return requestedSnapshotId;
    }
}
