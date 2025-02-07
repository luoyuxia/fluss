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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HashBucketAssigner}. */
class HashBucketAssignerTest {

    @Test
    void testBucketAssign() {
        final RowType rowType =
                DataTypes.ROW(
                        new DataField("a", DataTypes.INT()),
                        new DataField("b", DataTypes.INT()),
                        new DataField("c", DataTypes.STRING()),
                        new DataField("d", DataTypes.BIGINT()));

        // Suppose a, b are primary keys.
        int[] pkIndices = {0, 1};
        KeyEncoder keyEncoder = new KeyEncoder(rowType, pkIndices);
        IndexedRow row1 = row(rowType, new Object[] {1, 1, "2", 3L});
        IndexedRow row2 = row(rowType, new Object[] {1, 1, "3", 4L});
        IndexedRow row3 = row(rowType, new Object[] {1, 2, "4", 5L});
        IndexedRow row4 = row(rowType, new Object[] {1, 1, "4", 5L});

        HashBucketAssigner hashBucketAssigner = new HashBucketAssigner(3, keyEncoder);

        int bucket1 = hashBucketAssigner.assignBucket(row1);
        int bucket2 = hashBucketAssigner.assignBucket(row2);
        int bucket3 = hashBucketAssigner.assignBucket(row3);
        int bucket4 = hashBucketAssigner.assignBucket(row4);

        assertThat(bucket1).isEqualTo(bucket2);
        assertThat(bucket1).isNotEqualTo(bucket3);
        assertThat(bucket3).isNotEqualTo(bucket4);
        assertThat(bucket1 < 3).isTrue();
        assertThat(bucket2 < 3).isTrue();
        assertThat(bucket3 < 3).isTrue();
        assertThat(bucket4 < 3).isTrue();
    }

    @Test
    void testBucketForRowKey() {
        final RowType rowType =
                DataTypes.ROW(
                        new DataField("a", DataTypes.INT()),
                        new DataField("b", DataTypes.INT()),
                        new DataField("c", DataTypes.STRING()),
                        new DataField("d", DataTypes.BIGINT()));

        List<byte[]> keyList = new ArrayList<>();
        int rowCount = 3000;
        int[] pkIndices = {0, 1, 2};
        KeyEncoder keyEncoder = new KeyEncoder(rowType, pkIndices);
        for (int i = 0; i < rowCount; i++) {
            IndexedRow row =
                    row(
                            rowType,
                            new Object[] {i, rowCount - i, String.valueOf(rowCount - i), (long) i});
            keyList.add(keyEncoder.encode(row));
        }

        for (int bucketNumber = 3; bucketNumber < 10; bucketNumber++) {
            HashBucketAssigner hashBucketAssigner =
                    new HashBucketAssigner(bucketNumber, keyEncoder);
            for (byte[] key : keyList) {
                int bucket = hashBucketAssigner.assignBucket(key);
                assertThat(bucket >= 0).isTrue();
                assertThat(bucket < bucketNumber).isTrue();
            }
        }
    }
}
