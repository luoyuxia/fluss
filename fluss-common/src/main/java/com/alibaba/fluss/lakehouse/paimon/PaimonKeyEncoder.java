/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.lakehouse.paimon;

import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;

import java.util.List;

/** An implementation of {@link KeyEncoder} to follow Paimon's encoding strategy. */
public class PaimonKeyEncoder implements KeyEncoder {

    private final InternalRow.FieldGetter[] fieldGetters;

    private final PaimonBinaryRowWriter.FieldWriter[] fieldEncoders;

    private final PaimonBinaryRowWriter paimonBinaryRowWriter;

    public PaimonKeyEncoder(RowType rowType, List<String> keys) {
        int[] bucketKeyIndex = getBucketKeyIndex(rowType, keys);
        DataType[] encodeDataTypes = new DataType[bucketKeyIndex.length];
        for (int i = 0; i < bucketKeyIndex.length; i++) {
            encodeDataTypes[i] = rowType.getTypeAt(bucketKeyIndex[i]);
        }

        // for get fields from internal row
        fieldGetters = new InternalRow.FieldGetter[bucketKeyIndex.length];
        // for encode fields
        fieldEncoders = new PaimonBinaryRowWriter.FieldWriter[bucketKeyIndex.length];
        for (int i = 0; i < bucketKeyIndex.length; i++) {
            DataType fieldDataType = encodeDataTypes[i];
            fieldGetters[i] = InternalRow.createFieldGetter(fieldDataType, bucketKeyIndex[i]);
            fieldEncoders[i] = PaimonBinaryRowWriter.createFieldWriter(fieldDataType);
        }
        paimonBinaryRowWriter = new PaimonBinaryRowWriter(bucketKeyIndex.length);
    }

    private int[] getBucketKeyIndex(RowType rowType, List<String> bucketKey) {
        int[] bucketKeyIndex = new int[bucketKey.size()];
        for (int i = 0; i < bucketKey.size(); i++) {
            bucketKeyIndex[i] = rowType.getFieldIndex(bucketKey.get(i));
        }
        return bucketKeyIndex;
    }

    @Override
    public byte[] encodeKey(InternalRow row) {
        paimonBinaryRowWriter.reset();
        // always be RowKind.INSERT
        paimonBinaryRowWriter.writeRowKind(RowKind.INSERT);
        // iterate all the fields of the row, and encode each field
        for (int i = 0; i < fieldGetters.length; i++) {
            fieldEncoders[i].writeField(
                    paimonBinaryRowWriter, i, fieldGetters[i].getFieldOrNull(row));
        }
        return paimonBinaryRowWriter.toBytes();
    }
}
