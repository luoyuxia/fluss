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

package com.alibaba.fluss.lake.source;

/** The lake log fetch info. */
public class LakeLogFetchInfo {

    private final long snapshotId;

    private final long lakeLogEndOffset;

    public LakeLogFetchInfo(long snapshotId, long lakeLogEndOffset) {
        this.snapshotId = snapshotId;
        this.lakeLogEndOffset = lakeLogEndOffset;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long lakeLogEndOffset() {
        return lakeLogEndOffset;
    }
}
