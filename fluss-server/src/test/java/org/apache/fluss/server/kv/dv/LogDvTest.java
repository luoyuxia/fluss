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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogDv}. */
class LogDvTest {

    @TempDir Path tempDir;
    private DvRocksDB dvRocksDB;

    @BeforeEach
    void setup() throws Exception {
        dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString());
    }

    @AfterEach
    void teardown() {
        if (dvRocksDB != null) {
            dvRocksDB.close();
        }
    }

    @Test
    void testMarkDeletedAndIsDeleted() throws Exception {
        LogDv logDv = dvRocksDB.logDv();

        logDv.markDeleted(100L);
        assertThat(logDv.isDeleted(100L)).isTrue();
        assertThat(logDv.isDeleted(101L)).isFalse();
    }

    @Test
    void testIsDeletedNonExistent() throws Exception {
        assertThat(dvRocksDB.logDv().isDeleted(999L)).isFalse();
    }

    @Test
    void testMarkDeletedMultipleInSameRange() throws Exception {
        LogDv logDv = dvRocksDB.logDv();

        // All within the first range [0, 1024)
        logDv.markDeleted(0L);
        logDv.markDeleted(500L);
        logDv.markDeleted(1023L);

        assertThat(logDv.isDeleted(0L)).isTrue();
        assertThat(logDv.isDeleted(500L)).isTrue();
        assertThat(logDv.isDeleted(1023L)).isTrue();
        assertThat(logDv.isDeleted(1L)).isFalse();
    }

    @Test
    void testMarkDeletedCrossRange() throws Exception {
        LogDv logDv = dvRocksDB.logDv();

        // First range [0, 1024)
        logDv.markDeleted(100L);
        // Second range [1024, 2048)
        logDv.markDeleted(1500L);
        // Third range [2048, 3072)
        logDv.markDeleted(2500L);

        assertThat(logDv.isDeleted(100L)).isTrue();
        assertThat(logDv.isDeleted(1500L)).isTrue();
        assertThat(logDv.isDeleted(2500L)).isTrue();
        // Other offsets in same ranges should not be deleted
        assertThat(logDv.isDeleted(200L)).isFalse();
        assertThat(logDv.isDeleted(1600L)).isFalse();
    }

    @Test
    void testSnapshotSingleRange() throws Exception {
        LogDv logDv = dvRocksDB.logDv();

        logDv.markDeleted(10L);
        logDv.markDeleted(20L);
        logDv.markDeleted(30L);

        Roaring64Bitmap snapshot = logDv.snapshot(0L, 1024L);
        assertThat(snapshot.getLongCardinality()).isEqualTo(3);
        assertThat(snapshot.contains(10L)).isTrue();
        assertThat(snapshot.contains(20L)).isTrue();
        assertThat(snapshot.contains(30L)).isTrue();
    }

    @Test
    void testSnapshotCrossRange() throws Exception {
        LogDv logDv = dvRocksDB.logDv();

        logDv.markDeleted(500L);
        logDv.markDeleted(1500L);
        logDv.markDeleted(2500L);

        Roaring64Bitmap snapshot = logDv.snapshot(0L, 3072L);
        assertThat(snapshot.getLongCardinality()).isEqualTo(3);
        assertThat(snapshot.contains(500L)).isTrue();
        assertThat(snapshot.contains(1500L)).isTrue();
        assertThat(snapshot.contains(2500L)).isTrue();
    }

    @Test
    void testSnapshotFiltersByRange() throws Exception {
        LogDv logDv = dvRocksDB.logDv();

        logDv.markDeleted(500L);
        logDv.markDeleted(1500L);
        logDv.markDeleted(2500L);

        // Only include offsets in [1000, 2000)
        Roaring64Bitmap snapshot = logDv.snapshot(1000L, 2000L);
        assertThat(snapshot.getLongCardinality()).isEqualTo(1);
        assertThat(snapshot.contains(1500L)).isTrue();
    }

    @Test
    void testSnapshotEmpty() throws Exception {
        Roaring64Bitmap snapshot = dvRocksDB.logDv().snapshot(0L, 1024L);
        assertThat(snapshot.getLongCardinality()).isEqualTo(0);
    }

    @Test
    void testCleanup() throws Exception {
        LogDv logDv = dvRocksDB.logDv();

        // Create entries in three ranges
        logDv.markDeleted(100L); // range [0, 1024)
        logDv.markDeleted(1500L); // range [1024, 2048)
        logDv.markDeleted(2500L); // range [2048, 3072)

        // Clean up ranges where rangeStart + RANGE_SIZE <= 2048
        // This should remove range [0, 1024) and range [1024, 2048)
        logDv.cleanup(2048L);

        assertThat(logDv.isDeleted(100L)).isFalse();
        assertThat(logDv.isDeleted(1500L)).isFalse();
        assertThat(logDv.isDeleted(2500L)).isTrue();
    }

    @Test
    void testCleanupPartial() throws Exception {
        LogDv logDv = dvRocksDB.logDv();

        logDv.markDeleted(100L); // range [0, 1024)
        logDv.markDeleted(1500L); // range [1024, 2048)

        // Only clean up range [0, 1024) (rangeStart + RANGE_SIZE = 1024 <= 1024)
        logDv.cleanup(1024L);

        assertThat(logDv.isDeleted(100L)).isFalse();
        assertThat(logDv.isDeleted(1500L)).isTrue();
    }

    @Test
    void testLargeOffset() throws Exception {
        LogDv logDv = dvRocksDB.logDv();
        long largeOffset = 1_000_000_000L;

        logDv.markDeleted(largeOffset);
        assertThat(logDv.isDeleted(largeOffset)).isTrue();
        assertThat(logDv.isDeleted(largeOffset + 1)).isFalse();
    }
}
