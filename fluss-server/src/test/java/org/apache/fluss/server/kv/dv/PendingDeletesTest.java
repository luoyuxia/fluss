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

import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.WriteBatch;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PendingDeletes}. */
class PendingDeletesTest {

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
    void testPutKnownAndGet() throws Exception {
        PendingDeletes pd = dvRocksDB.pendingDeletes();
        FilePos pos = new FilePos(10, 100L);

        pd.put(1L, pos);

        PendingDeletes.PendingDeleteEntry entry = pd.get(1L);
        assertThat(entry).isNotNull();
        assertThat(entry.isPending()).isFalse();
        assertThat(entry.getRowId()).isEqualTo(1L);
        assertThat(entry.getFilePos()).isEqualTo(pos);
    }

    @Test
    void testPutPendingAndGet() throws Exception {
        PendingDeletes pd = dvRocksDB.pendingDeletes();

        pd.putPending(1L);

        PendingDeletes.PendingDeleteEntry entry = pd.get(1L);
        assertThat(entry).isNotNull();
        assertThat(entry.isPending()).isTrue();
        assertThat(entry.getRowId()).isEqualTo(1L);
        assertThat(entry.getFilePos()).isNull();
    }

    @Test
    void testGetNonExistent() throws Exception {
        assertThat(dvRocksDB.pendingDeletes().get(999L)).isNull();
    }

    @Test
    void testDelete() throws Exception {
        PendingDeletes pd = dvRocksDB.pendingDeletes();

        pd.putPending(1L);
        pd.delete(1L);
        assertThat(pd.get(1L)).isNull();
    }

    @Test
    void testResolvePending() throws Exception {
        PendingDeletes pd = dvRocksDB.pendingDeletes();

        // Initially pending
        pd.putPending(1L);
        assertThat(pd.get(1L).isPending()).isTrue();

        // Resolve with known position
        pd.put(1L, new FilePos(10, 100L));
        PendingDeletes.PendingDeleteEntry entry = pd.get(1L);
        assertThat(entry.isPending()).isFalse();
        assertThat(entry.getFilePos()).isEqualTo(new FilePos(10, 100L));
    }

    @Test
    void testWriteBatch() throws Exception {
        PendingDeletes pd = dvRocksDB.pendingDeletes();

        try (WriteBatch batch = new WriteBatch()) {
            pd.put(batch, 1L, new FilePos(10, 100L));
            pd.putPending(batch, 2L);
            pd.putPending(batch, 3L);
            dvRocksDB.write(batch);
        }

        assertThat(pd.get(1L).isPending()).isFalse();
        assertThat(pd.get(2L).isPending()).isTrue();
        assertThat(pd.get(3L).isPending()).isTrue();
    }

    @Test
    void testWriteBatchDelete() throws Exception {
        PendingDeletes pd = dvRocksDB.pendingDeletes();

        pd.putPending(1L);
        pd.putPending(2L);

        try (WriteBatch batch = new WriteBatch()) {
            pd.delete(batch, 1L);
            dvRocksDB.write(batch);
        }

        assertThat(pd.get(1L)).isNull();
        assertThat(pd.get(2L)).isNotNull();
    }

    @Test
    void testIterator() throws Exception {
        PendingDeletes pd = dvRocksDB.pendingDeletes();

        pd.put(1L, new FilePos(10, 100L));
        pd.putPending(2L);
        pd.put(3L, new FilePos(20, 200L));

        List<PendingDeletes.PendingDeleteEntry> entries = new ArrayList<>();
        try (CloseableIterator<PendingDeletes.PendingDeleteEntry> it = pd.iterator()) {
            while (it.hasNext()) {
                entries.add(it.next());
            }
        }

        assertThat(entries).hasSize(3);
        // Entries are in BigEndian key order (rowId order)
        assertThat(entries.get(0).getRowId()).isEqualTo(1L);
        assertThat(entries.get(0).isPending()).isFalse();
        assertThat(entries.get(0).getFilePos()).isEqualTo(new FilePos(10, 100L));

        assertThat(entries.get(1).getRowId()).isEqualTo(2L);
        assertThat(entries.get(1).isPending()).isTrue();

        assertThat(entries.get(2).getRowId()).isEqualTo(3L);
        assertThat(entries.get(2).isPending()).isFalse();
    }

    @Test
    void testIteratorEmpty() throws Exception {
        try (CloseableIterator<PendingDeletes.PendingDeleteEntry> it =
                dvRocksDB.pendingDeletes().iterator()) {
            assertThat(it.hasNext()).isFalse();
        }
    }

    @Test
    void testPendingDeleteEntryToString() {
        PendingDeletes.PendingDeleteEntry pending = new PendingDeletes.PendingDeleteEntry(1L, null);
        assertThat(pending.toString()).contains("PENDING");

        PendingDeletes.PendingDeleteEntry known =
                new PendingDeletes.PendingDeleteEntry(1L, new FilePos(10, 100L));
        assertThat(known.toString()).contains("fileId=10");
    }
}
