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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LakeDvFilterIterator}. */
class LakeDvFilterIteratorTest {

    @Test
    void testFiltersDeletedLakeRowsByPosition() {
        List<PositionedRecord> positionedRecords =
                Arrays.asList(
                        positionedRecord(0, "a"),
                        positionedRecord(1, "b"),
                        positionedRecord(2, "c"));
        RoaringBitmap deletedRows = new RoaringBitmap();
        deletedRows.add(1);

        LakeDvFilterIterator iterator =
                new LakeDvFilterIterator(
                        CloseableIterator.wrap(positionedRecords.iterator()), deletedRows);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getRow().getString(0).toString()).isEqualTo("a");
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next().getRow().getString(0).toString()).isEqualTo("c");
        assertThat(iterator.hasNext()).isFalse();
    }

    private static PositionedRecord positionedRecord(long position, String value) {
        LogRecord record =
                new GenericRecord(
                        0L, 0L, ChangeType.INSERT, GenericRow.of(BinaryString.fromString(value)));
        return new PositionedRecord(record, position);
    }
}
