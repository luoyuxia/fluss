/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.lake.state;

import org.apache.fluss.flink.lake.split.DvAwareLakeSnapshotSplit;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DvAwareLakeSnapshotSplitState}. */
class DvAwareLakeSnapshotSplitStateTest {

    @Test
    void testToSourceSplitRestoresProgress() {
        DvAwareLakeSnapshotSplit split =
                new DvAwareLakeSnapshotSplit(
                        new TableBucket(1L, 2L, 3),
                        "p1",
                        new TestingLakeSplit(3, Collections.singletonList("p1")),
                        4,
                        new byte[] {1, 2});

        DvAwareLakeSnapshotSplitState state = new DvAwareLakeSnapshotSplitState(split);
        state.setRecordsToSkip(7L);

        DvAwareLakeSnapshotSplit restoredSplit = (DvAwareLakeSnapshotSplit) state.toSourceSplit();

        assertThat(restoredSplit.getSplitIndex()).isEqualTo(4);
        assertThat(restoredSplit.getRecordsToSkip()).isEqualTo(7L);
        assertThat(restoredSplit.getDeletionVector()).containsExactly(1, 2);
    }
}
