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
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;

/**
 * The version row merge engine for primary key table. The update will only occur if the new value
 * of the specified version field is greater than the old value.
 *
 * @since 0.6
 */
public class VersionRowMergeEngine implements RowMergeEngine {

    private final MergeEngine mergeEngine;
    private final InternalRow.FieldGetter[] currentFieldGetters;
    private final RowType rowType;

    public VersionRowMergeEngine(Schema schema, MergeEngine mergeEngine) {
        this.mergeEngine = mergeEngine;
        this.rowType = schema.toRowType();
        this.currentFieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            currentFieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
    }

    @Override
    public BinaryRow merge(BinaryRow oldRow, BinaryRow newRow) {
        int fieldIndex = rowType.getFieldIndex(mergeEngine.getColumn());
        if (fieldIndex == -1) {
            throw new IllegalArgumentException(
                    String.format(
                            "When the merge engine is set to version, the column %s does not exist.",
                            mergeEngine.getColumn()));
        }
        InternalRow.FieldGetter fieldGetter = currentFieldGetters[fieldIndex];
        Object oldValue = fieldGetter.getFieldOrNull(oldRow);
        Object newValue = fieldGetter.getFieldOrNull(newRow);
        // If the old value is null, simply overwrite it with the new value.
        if (oldValue == null) return newRow;
        // If the new value is empty, ignore it directly.
        if (newValue == null) return null;
        DataType dataType = rowType.getTypeAt(fieldIndex);
        return getValueComparator(dataType).isGreaterThan(newValue, oldValue) ? newRow : null;
    }

    @Override
    public boolean shouldSkipDeletion(BinaryRow newRow) {
        return true;
    }

    private ValueComparator getValueComparator(DataType dataType) {
        if (dataType instanceof BigIntType) {
            return (left, right) -> (Long) left > (Long) right;
        }
        if (dataType instanceof IntType || dataType instanceof TimeType) {
            return (left, right) -> (Integer) left > (Integer) right;
        }
        if (dataType instanceof TimestampType) {
            return (left, right) ->
                    ((TimestampNtz) left).getMillisecond()
                            > ((TimestampNtz) right).getMillisecond();
        }
        if (dataType instanceof LocalZonedTimestampType) {
            return (left, right) ->
                    ((TimestampLtz) left).toEpochMicros() > ((TimestampLtz) right).toEpochMicros();
        }
        throw new FlussRuntimeException("Unsupported data type: " + dataType.asSummaryString());
    }

    interface ValueComparator {
        boolean isGreaterThan(Object left, Object right);
    }
}
