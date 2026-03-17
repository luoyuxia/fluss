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

package org.apache.fluss.lake.iceberg.tiering.writer;

import org.apache.fluss.lake.iceberg.tiering.RecordWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.record.LogRecord;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link RecordWriter} for DV (Deletion Vector) mode.
 *
 * <p>Snapshot splits still materialize Puffin DV from {@code lakeDvSnapshot}. For log splits, this
 * writer applies split-scoped {@code logDvSnapshot} up front, so same-split write-then-delete rows
 * are filtered before they are written to Iceberg.
 */
public class DvTaskWriter extends RecordWriter {

    /** Position report: file_path -> list of (rowId, rowPosition). */
    private final Map<String, List<long[]>> positionReport;

    /** Split-scoped log dv snapshot: base_offset -> roaring bitmap. */
    private final Map<Long, RoaringBitmap> splitLogDvSnapshot;

    public DvTaskWriter(
            Table icebergTable, WriterInitContext writerInitContext, TaskWriter<Record> taskWriter)
            throws IOException {
        super(
                taskWriter,
                icebergTable.schema(),
                writerInitContext.tableInfo().getRowType(),
                writerInitContext.tableBucket());
        this.positionReport = new HashMap<>();
        this.splitLogDvSnapshot = deserializeLogDvSnapshot(writerInitContext.logDvSnapshot());
    }

    @Override
    public void write(LogRecord record) throws Exception {
        flussRecordAsIcebergRecord.setFlussRecord(record);
        switch (record.getChangeType()) {
            case INSERT:
            case UPDATE_AFTER:
                if (isDeletedInSplitLogDv(record.logOffset())) {
                    return;
                }
                GenericRecordAppendOnlyWriter appendOnlyWriter =
                        (GenericRecordAppendOnlyWriter) taskWriter;
                appendOnlyWriter.write(flussRecordAsIcebergRecord);
                String filePath = appendOnlyWriter.currentFilePath();
                long rowPosition = appendOnlyWriter.currentFileRowCount() - 1;
                if (filePath != null && rowPosition >= 0) {
                    addPositionEntry(filePath, record.logOffset(), (int) rowPosition);
                }
                break;
            case UPDATE_BEFORE:
            case DELETE:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown row kind: " + record.getChangeType());
        }
    }

    public void addPositionEntry(String filePath, long rowId, int rowPosition) {
        positionReport
                .computeIfAbsent(filePath, key -> new ArrayList<>())
                .add(new long[] {rowId, rowPosition});
    }

    public Map<String, List<long[]>> getPositionReport() {
        return positionReport;
    }

    private boolean isDeletedInSplitLogDv(long rowId) {
        if (splitLogDvSnapshot.isEmpty()) {
            return false;
        }
        long baseOffset = (rowId / 1000L) * 1000L;
        RoaringBitmap bitmap = splitLogDvSnapshot.get(baseOffset);
        return bitmap != null && bitmap.contains((int) (rowId - baseOffset));
    }

    private static Map<Long, RoaringBitmap> deserializeLogDvSnapshot(
            Map<Long, byte[]> serializedLogDvSnapshot) throws IOException {
        if (serializedLogDvSnapshot == null || serializedLogDvSnapshot.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Long, RoaringBitmap> result = new LinkedHashMap<>(serializedLogDvSnapshot.size());
        for (Map.Entry<Long, byte[]> entry : serializedLogDvSnapshot.entrySet()) {
            RoaringBitmap bitmap = new RoaringBitmap();
            try (DataInputStream dataInputStream =
                    new DataInputStream(new ByteArrayInputStream(entry.getValue()))) {
                bitmap.deserialize(dataInputStream);
            }
            result.put(entry.getKey(), bitmap);
        }
        return result;
    }
}
