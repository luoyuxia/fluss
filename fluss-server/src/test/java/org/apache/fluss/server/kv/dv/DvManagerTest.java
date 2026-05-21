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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DvManager}. */
class DvManagerTest {

    @TempDir Path tempDir;

    private DvRocksDB dvRocksDB;
    private DvRWLock dvRWLock;
    private DvManager dvManager;

    @BeforeEach
    void setUp() throws Exception {
        dvRocksDB = DvRocksDB.open(tempDir.resolve("dv").toString());
        dvRWLock = new DvRWLock();
        dvManager = new DvManager(dvRocksDB, dvRWLock);
    }

    @AfterEach
    void tearDown() {
        dvManager.close();
        dvRocksDB.close();
    }

    @Test
    void testRowPosIndexMiss() throws Exception {
        // RowPosIndex has no mapping for these rowIds -> should produce pending markers
        long rowId1 = 100L;
        long rowId2 = 200L;
        List<DvEntry> entries = Arrays.asList(new DvEntry(rowId1), new DvEntry(rowId2));

        dvManager.handleChangelogSynced(entries);

        // Verify LogDv has deletion marks
        assertThat(dvRocksDB.logDv().isDeleted(rowId1)).isTrue();
        assertThat(dvRocksDB.logDv().isDeleted(rowId2)).isTrue();

        // Verify PendingDeletes has pending markers (no resolved position)
        PendingDeletes.PendingDeleteEntry entry1 = dvRocksDB.pendingDeletes().get(rowId1);
        assertThat(entry1).isNotNull();
        assertThat(entry1.isPending()).isTrue();

        PendingDeletes.PendingDeleteEntry entry2 = dvRocksDB.pendingDeletes().get(rowId2);
        assertThat(entry2).isNotNull();
        assertThat(entry2.isPending()).isTrue();

        // Verify RowPosIndex is still empty (nothing to delete)
        assertThat(dvRocksDB.rowPosIndex().get(rowId1)).isNull();
        assertThat(dvRocksDB.rowPosIndex().get(rowId2)).isNull();

        // Verify LakeDv has no entries (no file positions to mark)
        assertThat(dvRocksDB.lakeDv().getAll()).isEmpty();
    }

    @Test
    void testRowPosIndexHit() throws Exception {
        // Pre-populate RowPosIndex with known positions
        long rowId1 = 100L;
        long rowId2 = 200L;
        FilePos filePos1 = new FilePos(1, 10L);
        FilePos filePos2 = new FilePos(2, 20L);

        dvRocksDB.rowPosIndex().put(rowId1, filePos1);
        dvRocksDB.rowPosIndex().put(rowId2, filePos2);

        List<DvEntry> entries = Arrays.asList(new DvEntry(rowId1), new DvEntry(rowId2));
        dvManager.handleChangelogSynced(entries);

        // Verify LogDv has deletion marks
        assertThat(dvRocksDB.logDv().isDeleted(rowId1)).isTrue();
        assertThat(dvRocksDB.logDv().isDeleted(rowId2)).isTrue();

        // Verify RowPosIndex entries are deleted (consumed)
        assertThat(dvRocksDB.rowPosIndex().get(rowId1)).isNull();
        assertThat(dvRocksDB.rowPosIndex().get(rowId2)).isNull();

        // Verify LakeDv has per-file deletion marks
        assertThat(dvRocksDB.lakeDv().get(1).contains(10L)).isTrue();
        assertThat(dvRocksDB.lakeDv().get(2).contains(20L)).isTrue();

        // Verify PendingDeletes has resolved entries (with file positions)
        PendingDeletes.PendingDeleteEntry entry1 = dvRocksDB.pendingDeletes().get(rowId1);
        assertThat(entry1).isNotNull();
        assertThat(entry1.isPending()).isFalse();
        assertThat(entry1.getFilePos()).isEqualTo(filePos1);

        PendingDeletes.PendingDeleteEntry entry2 = dvRocksDB.pendingDeletes().get(rowId2);
        assertThat(entry2).isNotNull();
        assertThat(entry2.isPending()).isFalse();
        assertThat(entry2.getFilePos()).isEqualTo(filePos2);
    }

    @Test
    void testMixedHitAndMiss() throws Exception {
        // rowId 100: has RowPosIndex mapping (hit)
        // rowId 200: no RowPosIndex mapping (miss)
        long hitRowId = 100L;
        long missRowId = 200L;
        FilePos hitFilePos = new FilePos(5, 50L);

        dvRocksDB.rowPosIndex().put(hitRowId, hitFilePos);

        List<DvEntry> entries = Arrays.asList(new DvEntry(hitRowId), new DvEntry(missRowId));
        dvManager.handleChangelogSynced(entries);

        // Both should have LogDv marks
        assertThat(dvRocksDB.logDv().isDeleted(hitRowId)).isTrue();
        assertThat(dvRocksDB.logDv().isDeleted(missRowId)).isTrue();

        // Hit: RowPosIndex deleted, LakeDv marked, PendingDeletes resolved
        assertThat(dvRocksDB.rowPosIndex().get(hitRowId)).isNull();
        assertThat(dvRocksDB.lakeDv().get(5).contains(50L)).isTrue();
        PendingDeletes.PendingDeleteEntry hitEntry = dvRocksDB.pendingDeletes().get(hitRowId);
        assertThat(hitEntry).isNotNull();
        assertThat(hitEntry.isPending()).isFalse();
        assertThat(hitEntry.getFilePos()).isEqualTo(hitFilePos);

        // Miss: RowPosIndex still empty, no LakeDv for other files, PendingDeletes pending
        assertThat(dvRocksDB.rowPosIndex().get(missRowId)).isNull();
        PendingDeletes.PendingDeleteEntry missEntry = dvRocksDB.pendingDeletes().get(missRowId);
        assertThat(missEntry).isNotNull();
        assertThat(missEntry.isPending()).isTrue();
    }

    @Test
    void testEmptyEntries() throws Exception {
        // Pre-populate some data to verify no-op behavior
        dvRocksDB.rowPosIndex().put(999L, new FilePos(1, 1L));

        dvManager.handleChangelogSynced(Collections.<DvEntry>emptyList());

        // Nothing should have changed
        assertThat(dvRocksDB.rowPosIndex().get(999L)).isEqualTo(new FilePos(1, 1L));
        assertThat(dvRocksDB.lakeDv().getAll()).isEmpty();
        assertThat(dvRocksDB.pendingDeletes().get(999L)).isNull();
    }

    @Test
    void testMultipleEntriesForSameFile() throws Exception {
        // Two rows in the same file at different positions
        long rowId1 = 100L;
        long rowId2 = 200L;
        FilePos filePos1 = new FilePos(1, 10L);
        FilePos filePos2 = new FilePos(1, 20L);

        dvRocksDB.rowPosIndex().put(rowId1, filePos1);
        dvRocksDB.rowPosIndex().put(rowId2, filePos2);

        List<DvEntry> entries = Arrays.asList(new DvEntry(rowId1), new DvEntry(rowId2));
        dvManager.handleChangelogSynced(entries);

        // LakeDv for file 1 should contain both positions
        assertThat(dvRocksDB.lakeDv().get(1).contains(10L)).isTrue();
        assertThat(dvRocksDB.lakeDv().get(1).contains(20L)).isTrue();
    }

    @Test
    void testMultipleBatches() throws Exception {
        // First batch
        long rowId1 = 100L;
        dvRocksDB.rowPosIndex().put(rowId1, new FilePos(1, 10L));
        dvManager.handleChangelogSynced(Collections.singletonList(new DvEntry(rowId1)));

        // Second batch
        long rowId2 = 200L;
        dvManager.handleChangelogSynced(Collections.singletonList(new DvEntry(rowId2)));

        // First entry: hit (resolved)
        assertThat(dvRocksDB.logDv().isDeleted(rowId1)).isTrue();
        PendingDeletes.PendingDeleteEntry entry1 = dvRocksDB.pendingDeletes().get(rowId1);
        assertThat(entry1).isNotNull();
        assertThat(entry1.isPending()).isFalse();

        // Second entry: miss (pending)
        assertThat(dvRocksDB.logDv().isDeleted(rowId2)).isTrue();
        PendingDeletes.PendingDeleteEntry entry2 = dvRocksDB.pendingDeletes().get(rowId2);
        assertThat(entry2).isNotNull();
        assertThat(entry2.isPending()).isTrue();
    }
}
