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

package org.apache.fluss.server.kv.dv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DvManager}. */
class DvManagerTest {

    @TempDir private File tempDir;

    @Test
    void testHandlePositionReportAndUnionRead() throws Exception {
        try (DvManager dvManager = new DvManager(tempDir)) {
            Map<String, List<long[]>> initialBuild = new HashMap<>();
            initialBuild.put("file-a.parquet", Collections.singletonList(new long[] {1L, 3L}));
            dvManager.handleInitialBuild(initialBuild, 10L, 1L);

            Map<String, List<long[]>> positionReport = new HashMap<>();
            positionReport.put(
                    "file-b.parquet", Arrays.asList(new long[] {2L, 7L}, new long[] {3L, 9L}));
            assertThat(
                            dvManager.handlePositionReport(
                                    positionReport, 1L, 3L, Collections.emptyList(), 11L))
                    .isTrue();

            Integer fileId = dvManager.getFileDict().getFileId("file-b.parquet");
            assertThat(fileId).isNotNull();
            FilePos pendingFilePos = dvManager.getRowPosIndex().getPending(2L);
            assertThat(pendingFilePos).isNotNull();
            assertThat(pendingFilePos.getFileId()).isEqualTo(fileId);
            assertThat(pendingFilePos.getRowPosition()).isEqualTo(7);

            dvManager.handleReadableSwitch(11L, 3L, Collections.emptyList());
            dvManager.handleChangelogSynced(Collections.singletonList(2L));

            DvManager.DvReadResult staleResult =
                    dvManager.getDvForUnionRead(
                            10L, Collections.singletonList("file-b.parquet"), 3L);
            assertThat(staleResult.isStale()).isTrue();
            assertThat(staleResult.getCurrentReadableSnapshot()).isEqualTo(11L);

            DvManager.DvReadResult readResult =
                    dvManager.getDvForUnionRead(
                            11L, Collections.singletonList("file-b.parquet"), 3L);
            assertThat(readResult.isStale()).isFalse();
            assertThat(readResult.getLakeDv()).containsKey("file-b.parquet");

            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.deserialize(
                    new DataInputStream(
                            new ByteArrayInputStream(
                                    readResult.getLakeDv().get("file-b.parquet"))));
            assertThat(bitmap.contains(7)).isTrue();
        }
    }
}
