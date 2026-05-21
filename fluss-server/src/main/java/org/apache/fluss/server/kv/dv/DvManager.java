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

import org.apache.fluss.annotation.Internal;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Core state machine managing Deletion Vector writes during the KvTablet write path.
 *
 * <p>When a -U (update-before) or -D (delete) record is produced, this manager:
 *
 * <ol>
 *   <li>Marks the old record's changelog offset in LogDv (so tiering/union-read can skip superseded
 *       records)
 *   <li>Looks up RowPosIndex to see if the old record's lake position is known
 *   <li>If known: marks LakeDv, writes resolved PendingDeletes, deletes RowPosIndex entry
 *   <li>If unknown: writes pending PendingDeletes for future resolution
 * </ol>
 */
@Internal
public class DvManager implements Closeable {

    private final DvRocksDB dvRocksDB;
    private final DvRWLock dvRWLock;

    public DvManager(DvRocksDB dvRocksDB, DvRWLock dvRWLock) {
        this.dvRocksDB = dvRocksDB;
        this.dvRWLock = dvRWLock;
    }

    /**
     * Processes collected -U/-D entries after changelog append succeeds. This is the main entry
     * point called from KvTablet's putAsLeader after the WAL batch is synced.
     *
     * @param entries the DV entries to process, each representing a superseded +I/+U record
     */
    public void handleChangelogSynced(List<DvEntry> entries) throws IOException {
        dvRWLock.writeLock();
        try {
            for (DvEntry entry : entries) {
                long oldRowId = entry.getOldRowId();

                // 1. Mark LogDv: the old +I/+U offset is now superseded
                dvRocksDB.logDv().markDeleted(oldRowId);

                // 2. Point-get RowPosIndex to check if old record's lake position is known
                FilePos filePos = dvRocksDB.rowPosIndex().get(oldRowId);

                if (filePos != null) {
                    // Hit: old record's lake position is known
                    // Mark LakeDv (per-file deletion bitmap)
                    dvRocksDB.lakeDv().markDeleted(filePos.fileId(), filePos.rowPosition());
                    // Delete RowPosIndex entry (consumed)
                    dvRocksDB.rowPosIndex().delete(oldRowId);
                    // Write PendingDeletes with resolved position
                    dvRocksDB.pendingDeletes().put(oldRowId, filePos);
                } else {
                    // Miss: position unknown (data not yet tiered or compacted)
                    dvRocksDB.pendingDeletes().putPending(oldRowId);
                }
            }
        } finally {
            dvRWLock.writeUnlock();
        }
    }

    @Override
    public void close() {
        // DvRocksDB lifecycle is managed externally by KvTablet
    }
}
