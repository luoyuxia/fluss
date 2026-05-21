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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowPosSstIndex}. */
class RowPosSstIndexTest {

    @Test
    void testSerializeDeserialize() {
        Map<Integer, List<RowPosSstIndex.SstFileEntry>> bucketFiles = new HashMap<>();
        bucketFiles.put(
                0,
                Arrays.asList(
                        new RowPosSstIndex.SstFileEntry("sst_0.sst", 12345),
                        new RowPosSstIndex.SstFileEntry("sst_1.sst", 67890)));
        bucketFiles.put(
                3, Collections.singletonList(new RowPosSstIndex.SstFileEntry("sst_0.sst", 11111)));

        RowPosSstIndex original = new RowPosSstIndex(bucketFiles);
        byte[] json = original.toJsonBytes();
        RowPosSstIndex restored = RowPosSstIndex.fromJsonBytes(json);

        assertThat(restored.getBucketIds()).containsExactlyInAnyOrder(0, 3);
        assertThat(restored.getFiles(0)).isEqualTo(original.getFiles(0));
        assertThat(restored.getFiles(3)).isEqualTo(original.getFiles(3));
    }

    @Test
    void testGetFiles() {
        Map<Integer, List<RowPosSstIndex.SstFileEntry>> bucketFiles = new HashMap<>();
        bucketFiles.put(
                0, Collections.singletonList(new RowPosSstIndex.SstFileEntry("sst_0.sst", 100)));

        RowPosSstIndex index = new RowPosSstIndex(bucketFiles);

        // Existing bucket
        List<RowPosSstIndex.SstFileEntry> files = index.getFiles(0);
        assertThat(files).hasSize(1);
        assertThat(files.get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(files.get(0).getFileSize()).isEqualTo(100);

        // Non-existing bucket returns empty list
        assertThat(index.getFiles(999)).isEmpty();
    }

    @Test
    void testGetBucketIds() {
        Map<Integer, List<RowPosSstIndex.SstFileEntry>> bucketFiles = new HashMap<>();
        bucketFiles.put(0, Collections.<RowPosSstIndex.SstFileEntry>emptyList());
        bucketFiles.put(5, Collections.<RowPosSstIndex.SstFileEntry>emptyList());
        bucketFiles.put(12, Collections.<RowPosSstIndex.SstFileEntry>emptyList());

        RowPosSstIndex index = new RowPosSstIndex(bucketFiles);
        assertThat(index.getBucketIds()).containsExactlyInAnyOrder(0, 5, 12);
    }

    @Test
    void testEmptyIndex() {
        Map<Integer, List<RowPosSstIndex.SstFileEntry>> empty = Collections.emptyMap();
        RowPosSstIndex index = new RowPosSstIndex(empty);

        byte[] json = index.toJsonBytes();
        RowPosSstIndex restored = RowPosSstIndex.fromJsonBytes(json);

        assertThat(restored.getBucketIds()).isEmpty();
        assertThat(restored.getFiles(0)).isEmpty();
    }

    @Test
    void testMultipleBucketsMultipleFiles() {
        Map<Integer, List<RowPosSstIndex.SstFileEntry>> bucketFiles = new HashMap<>();
        bucketFiles.put(
                0,
                Arrays.asList(
                        new RowPosSstIndex.SstFileEntry("sst_0.sst", 1000),
                        new RowPosSstIndex.SstFileEntry("sst_1.sst", 2000),
                        new RowPosSstIndex.SstFileEntry("sst_2.sst", 3000)));
        bucketFiles.put(
                1,
                Arrays.asList(
                        new RowPosSstIndex.SstFileEntry("sst_0.sst", 4000),
                        new RowPosSstIndex.SstFileEntry("sst_1.sst", 5000)));
        bucketFiles.put(
                2, Collections.singletonList(new RowPosSstIndex.SstFileEntry("sst_0.sst", 6000)));

        RowPosSstIndex original = new RowPosSstIndex(bucketFiles);
        byte[] json = original.toJsonBytes();
        RowPosSstIndex restored = RowPosSstIndex.fromJsonBytes(json);

        assertThat(restored.getBucketIds()).containsExactlyInAnyOrder(0, 1, 2);
        assertThat(restored.getFiles(0)).hasSize(3);
        assertThat(restored.getFiles(1)).hasSize(2);
        assertThat(restored.getFiles(2)).hasSize(1);

        // Verify content
        assertThat(restored.getFiles(0).get(2).getFileName()).isEqualTo("sst_2.sst");
        assertThat(restored.getFiles(0).get(2).getFileSize()).isEqualTo(3000);
        assertThat(restored.getFiles(2).get(0).getFileSize()).isEqualTo(6000);
    }
}
