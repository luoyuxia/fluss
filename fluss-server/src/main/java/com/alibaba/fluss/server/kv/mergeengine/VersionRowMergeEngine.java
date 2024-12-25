/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv.mergeengine;

import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.metadata.MergeEngine;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.server.kv.partialupdate.PartialUpdater;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKv;
import com.alibaba.fluss.server.kv.wal.WalBuilder;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;

/** A wrapper for version merge engine. */
public class VersionRowMergeEngine extends RowMergeEngine {

    private final MergeEngine mergeEngine;
    private final Schema schema;
    private final InternalRow.FieldGetter[] currentFieldGetters;

    public VersionRowMergeEngine(
            PartialUpdater partialUpdater,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            KvPreWriteBuffer kvPreWriteBuffer,
            RocksDBKv rocksDBKv,
            Schema schema,
            short schemaId,
            int appendedRecordCount,
            long logOffset,
            MergeEngine mergeEngine) {
        super(
                partialUpdater,
                valueDecoder,
                walBuilder,
                kvPreWriteBuffer,
                rocksDBKv,
                schema,
                schemaId,
                appendedRecordCount,
                logOffset);
        this.mergeEngine = mergeEngine;
        this.schema = schema;
        RowType currentRowType = schema.toRowType();
        this.currentFieldGetters = new InternalRow.FieldGetter[currentRowType.getFieldCount()];
        for (int i = 0; i < currentRowType.getFieldCount(); i++) {
            currentFieldGetters[i] = InternalRow.createFieldGetter(currentRowType.getTypeAt(i), i);
        }
    }

    @Override
    protected void update(KvPreWriteBuffer.Key key, BinaryRow oldRow, BinaryRow newRow)
            throws Exception {
        RowType rowType = schema.toRowType();
        if (checkVersionMergeEngine(rowType, oldRow, newRow)) {
            return;
        }
        super.update(key, oldRow, newRow);
    }

    private boolean checkVersionMergeEngine(RowType rowType, BinaryRow oldRow, BinaryRow newRow) {
        if (!checkNewRowVersion(mergeEngine, rowType, oldRow, newRow)) {
            // When the specified field version is less
            // than the version number of the old
            // record, do not update
            return true;
        }
        return false;
    }

    // Check row version.
    private boolean checkNewRowVersion(
            MergeEngine mergeEngine, RowType rowType, BinaryRow oldRow, BinaryRow newRow) {
        int fieldIndex = rowType.getFieldIndex(mergeEngine.getColumn());
        if (fieldIndex == -1) {
            throw new IllegalArgumentException(
                    String.format(
                            "When the merge engine is set to version, the column %s does not exist.",
                            mergeEngine.getColumn()));
        }
        InternalRow.FieldGetter fieldGetter = currentFieldGetters[fieldIndex];
        Object oldValue = fieldGetter.getFieldOrNull(oldRow);
        if (oldValue == null) {
            throw new RuntimeException(
                    String.format(
                            "When the merge engine is set to version, the column %s old value cannot be null.",
                            mergeEngine.getColumn()));
        }
        Object newValue = fieldGetter.getFieldOrNull(newRow);
        if (newValue == null) {
            throw new RuntimeException(
                    String.format(
                            "When the merge engine is set to version, the column %s new value cannot be null.",
                            mergeEngine.getColumn()));
        }

        DataType dataType = rowType.getTypeAt(fieldIndex);

        if (dataType instanceof BigIntType) {
            return (Long) newValue > (Long) oldValue;
        } else if (dataType instanceof IntType) {
            return (Integer) newValue > (Integer) oldValue;
        } else if (dataType instanceof TimestampType || dataType instanceof TimeType) {
            return ((TimestampNtz) newValue).getMillisecond()
                    > ((TimestampNtz) oldValue).getMillisecond();
        } else if (dataType instanceof LocalZonedTimestampType) {
            return ((TimestampLtz) newValue).toEpochMicros()
                    > ((TimestampLtz) oldValue).toEpochMicros();
        } else {
            throw new FlussRuntimeException("Unsupported data type: " + dataType.asSummaryString());
        }
    }
}
