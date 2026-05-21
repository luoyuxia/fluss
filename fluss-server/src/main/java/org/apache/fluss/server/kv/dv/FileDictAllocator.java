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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Allocates monotonically increasing file IDs for Paimon data file paths.
 *
 * <p>Runs on the TieringService side. The {@code nextFileId} counter is persisted via Paimon
 * snapshot properties so that IDs are never reused across restarts.
 *
 * <p>Within a single session, the allocator caches path-to-ID mappings so that repeated calls with
 * the same path return the same ID (idempotent). New allocations since the last {@link
 * #resetNewEntries()} are tracked separately for reporting to the CoordinatorServer.
 */
@Internal
public class FileDictAllocator {

    private int nextFileId;
    private final Map<String, Integer> sessionCache;
    private Map<Integer, String> newEntries;

    /**
     * Creates an allocator starting from the given nextFileId.
     *
     * @param nextFileId 0 for a fresh start; restored value on recovery
     */
    public FileDictAllocator(int nextFileId) {
        this.nextFileId = nextFileId;
        this.sessionCache = new HashMap<>();
        this.newEntries = new HashMap<>();
    }

    /**
     * Allocates a file ID for the given file path.
     *
     * <p>Idempotent within the same session: repeated calls with the same path return the same ID.
     * New paths are assigned the next available ID and recorded in {@link #getNewEntries()}.
     */
    public int allocate(String filePath) {
        Integer existing = sessionCache.get(filePath);
        if (existing != null) {
            return existing;
        }
        int fileId = nextFileId++;
        sessionCache.put(filePath, fileId);
        newEntries.put(fileId, filePath);
        return fileId;
    }

    /**
     * Returns new (fileId, filePath) mappings allocated since construction or the last {@link
     * #resetNewEntries()} call.
     */
    public Map<Integer, String> getNewEntries() {
        return Collections.unmodifiableMap(newEntries);
    }

    /** Returns the current nextFileId value (for persistence to snapshot properties). */
    public int getNextFileId() {
        return nextFileId;
    }

    /**
     * Resets the new entries tracker. The session cache and nextFileId counter are preserved so
     * that subsequent allocations continue from the correct ID.
     */
    public void resetNewEntries() {
        newEntries = new HashMap<>();
    }
}
