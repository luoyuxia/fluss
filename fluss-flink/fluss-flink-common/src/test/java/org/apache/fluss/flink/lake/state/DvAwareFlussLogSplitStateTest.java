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

import org.apache.fluss.flink.lake.split.DvAwareFlussLogSplit;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DvAwareFlussLogSplitState}. */
class DvAwareFlussLogSplitStateTest {

    @Test
    void testToSourceSplitRestoresProgress() {
        DvAwareFlussLogSplit split =
                new DvAwareFlussLogSplit(
                        new TableBucket(1L, 2L, 3),
                        "p1",
                        100L,
                        200L,
                        Collections.singletonMap(100L, new byte[] {3, 4}));

        DvAwareFlussLogSplitState state = new DvAwareFlussLogSplitState(split);
        state.setNextOffset(123L);

        DvAwareFlussLogSplit restoredSplit = (DvAwareFlussLogSplit) state.toSourceSplit();

        assertThat(restoredSplit.getStartingOffset()).isEqualTo(123L);
        assertThat(restoredSplit.getStoppingOffset()).hasValue(200L);
        assertThat(restoredSplit.getLogDvSnapshot()).containsOnlyKeys(100L);
        assertThat(restoredSplit.getLogDvSnapshot().get(100L)).containsExactly(3, 4);
    }
}
