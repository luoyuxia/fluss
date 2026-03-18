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

package org.apache.fluss.flink.lake.reader;

import org.apache.fluss.lake.source.PositionedRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

/** Filters lake records by row position using split-scoped lake DV. */
class LakeDvFilterIterator implements CloseableIterator<LogRecord> {

    private final CloseableIterator<PositionedRecord> positionedRecords;
    private final RoaringBitmap deletedRows;
    @Nullable private PositionedRecord nextRecord;

    LakeDvFilterIterator(
            CloseableIterator<PositionedRecord> positionedRecords, RoaringBitmap deletedRows) {
        this.positionedRecords = positionedRecords;
        this.deletedRows = deletedRows;
    }

    @Override
    public void close() {
        positionedRecords.close();
    }

    @Override
    public boolean hasNext() {
        if (nextRecord != null) {
            return true;
        }
        while (positionedRecords.hasNext()) {
            PositionedRecord candidate = positionedRecords.next();
            if (!deletedRows.contains((int) candidate.rowPosition())) {
                nextRecord = candidate;
                return true;
            }
        }
        return false;
    }

    @Override
    public LogRecord next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException();
        }
        PositionedRecord result = nextRecord;
        nextRecord = null;
        return result.record();
    }
}
