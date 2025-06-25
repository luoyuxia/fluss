/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.utils.CloseableIterator;

import java.util.Collections;

/**
 * An implementation of {@link LogRecords} used to bridge the records in datalake to {@link
 * LogRecords} that fluss log scanner deserves.
 */
public class LakeLogRecords implements LogRecords {

    private final long fetchOffset;
    private final long nextFetchOffset;
    private final CloseableIterator<LogRecord> logRecords;

    public LakeLogRecords(
            long fetchOffset, long nextFetchOffset, CloseableIterator<LogRecord> logRecords) {
        this.fetchOffset = fetchOffset;
        this.nextFetchOffset = nextFetchOffset;
        this.logRecords = logRecords;
    }

    @Override
    public int sizeInBytes() {
        // actually, we don't really care the size in bytes during client fetch log,
        // so in here, return 0 directly
        return 0;
    }

    @Override
    public Iterable<LogRecordBatch> batches() {
        return Collections.singletonList(
                new LakeLogRecordBatch(fetchOffset, nextFetchOffset, logRecords));
    }

    private static class LakeLogRecordBatch implements LogRecordBatch {

        private final long baseFetchOffset;
        private final long nextFetchOffset;
        private final CloseableIterator<LogRecord> logRecords;

        private LakeLogRecordBatch(
                long baseFetchOffset,
                long nextFetchOffset,
                CloseableIterator<LogRecord> logRecords) {
            this.baseFetchOffset = baseFetchOffset;
            this.nextFetchOffset = nextFetchOffset;
            this.logRecords = logRecords;
        }

        @Override
        public boolean isValid() {
            // always valid
            return true;
        }

        @Override
        public void ensureValid() {
            // do nothing
        }

        @Override
        public long checksum() {
            // don't care
            return 0;
        }

        @Override
        public short schemaId() {
            // don't care
            return 0;
        }

        @Override
        public long baseLogOffset() {
            return baseFetchOffset;
        }

        @Override
        public long lastLogOffset() {
            return nextLogOffset() - 1;
        }

        @Override
        public long nextLogOffset() {
            return nextFetchOffset;
        }

        @Override
        public byte magic() {
            // don't care
            return 0;
        }

        @Override
        public long commitTimestamp() {
            // don't care
            return 0;
        }

        @Override
        public long writerId() {
            // don't care
            return 0;
        }

        @Override
        public int batchSequence() {
            // don't care
            return 0;
        }

        @Override
        public int sizeInBytes() {
            // don't care
            return 0;
        }

        @Override
        public int getRecordCount() {
            // don't care, just return 0 directly
            return 0;
        }

        @Override
        public CloseableIterator<LogRecord> records(ReadContext context) {
            return logRecords;
        }
    }
}
