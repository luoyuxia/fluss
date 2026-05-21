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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileDictAllocator}. */
class FileDictAllocatorTest {

    @Test
    void testAllocateNewPaths() {
        FileDictAllocator allocator = new FileDictAllocator(0);
        assertThat(allocator.allocate("data/file_a.parquet")).isEqualTo(0);
        assertThat(allocator.allocate("data/file_b.parquet")).isEqualTo(1);
        assertThat(allocator.allocate("data/file_c.parquet")).isEqualTo(2);
    }

    @Test
    void testAllocateIdempotent() {
        FileDictAllocator allocator = new FileDictAllocator(0);
        int id1 = allocator.allocate("data/file_a.parquet");
        int id2 = allocator.allocate("data/file_a.parquet");
        assertThat(id1).isEqualTo(id2);
        assertThat(allocator.getNextFileId()).isEqualTo(1);
    }

    @Test
    void testGetNewEntries() {
        FileDictAllocator allocator = new FileDictAllocator(0);
        allocator.allocate("data/file_a.parquet");
        allocator.allocate("data/file_b.parquet");

        Map<Integer, String> newEntries = allocator.getNewEntries();
        assertThat(newEntries).hasSize(2);
        assertThat(newEntries.get(0)).isEqualTo("data/file_a.parquet");
        assertThat(newEntries.get(1)).isEqualTo("data/file_b.parquet");
    }

    @Test
    void testResetNewEntries() {
        FileDictAllocator allocator = new FileDictAllocator(0);
        allocator.allocate("data/file_a.parquet");
        allocator.allocate("data/file_b.parquet");

        allocator.resetNewEntries();

        // New entries should be empty after reset
        assertThat(allocator.getNewEntries()).isEmpty();

        // Subsequent allocations continue from correct nextFileId
        assertThat(allocator.allocate("data/file_c.parquet")).isEqualTo(2);
        assertThat(allocator.getNewEntries()).hasSize(1);
        assertThat(allocator.getNewEntries().get(2)).isEqualTo("data/file_c.parquet");

        // Previously allocated paths are still cached
        assertThat(allocator.allocate("data/file_a.parquet")).isEqualTo(0);
        // Cached hit should not be in new entries
        assertThat(allocator.getNewEntries()).hasSize(1);
    }

    @Test
    void testRestoreFromNextFileId() {
        FileDictAllocator allocator = new FileDictAllocator(5);
        assertThat(allocator.allocate("data/file_x.parquet")).isEqualTo(5);
        assertThat(allocator.allocate("data/file_y.parquet")).isEqualTo(6);
    }

    @Test
    void testGetNextFileId() {
        FileDictAllocator allocator = new FileDictAllocator(0);
        assertThat(allocator.getNextFileId()).isEqualTo(0);

        allocator.allocate("data/file_a.parquet");
        assertThat(allocator.getNextFileId()).isEqualTo(1);

        allocator.allocate("data/file_b.parquet");
        assertThat(allocator.getNextFileId()).isEqualTo(2);

        // Idempotent call should not increment
        allocator.allocate("data/file_a.parquet");
        assertThat(allocator.getNextFileId()).isEqualTo(2);
    }
}
