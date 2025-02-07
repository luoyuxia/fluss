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

package com.alibaba.fluss.client.lakehouse;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alibaba.fluss.client.lakehouse.paimon.PaimonBucketAssigner.DATA_LAKE_PAIMON;
import static com.alibaba.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LakeTableBucketAssigner} . */
class LakeTableBucketAssignerTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testPrimaryKeyTableBucketAssign(boolean isPartitioned) {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "c")
                        .build();
        // bucket key
        List<String> bucketKey =
                isPartitioned ? Collections.singletonList("a") : Arrays.asList("a", "c");

        RowType rowType = schema.getRowType();
        InternalRow row1 = compactedRow(rowType, new Object[] {1, "2", "a"});
        InternalRow row2 = compactedRow(rowType, new Object[] {1, "3", "b"});

        InternalRow row3 = compactedRow(rowType, new Object[] {2, "4", "a"});
        InternalRow row4 = compactedRow(rowType, new Object[] {2, "4", "b"});

        LakeTableBucketAssigner lakeTableBucketAssigner =
                new LakeTableBucketAssigner(DATA_LAKE_PAIMON, schema.getRowType(), bucketKey, 3);

        int row1Bucket = lakeTableBucketAssigner.assignBucket(row1);
        int row2Bucket = lakeTableBucketAssigner.assignBucket(row2);
        int row3Bucket = lakeTableBucketAssigner.assignBucket(row3);
        int row4Bucket = lakeTableBucketAssigner.assignBucket(row4);

        if (isPartitioned) {
            // bucket key is the column 'a'
            assertThat(row1Bucket).isEqualTo(row2Bucket);
            assertThat(row3Bucket).isEqualTo(row4Bucket);
            assertThat(row1Bucket).isNotEqualTo(row3Bucket);
        } else {
            // bucket key is the column 'a', 'c'
            assertThat(row1Bucket).isNotEqualTo(row2Bucket);
            assertThat(row3Bucket).isNotEqualTo(row4Bucket);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLogTableBucketAssign(boolean isPartitioned) {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING())
                        .build();
        List<String> bucketKey =
                isPartitioned ? Collections.singletonList("a") : Arrays.asList("a", "c");
        LakeTableBucketAssigner lakeTableBucketAssigner =
                new LakeTableBucketAssigner(DATA_LAKE_PAIMON, schema.getRowType(), bucketKey, 3);

        RowType rowType = schema.getRowType();
        InternalRow row1 = compactedRow(rowType, new Object[] {1, "2", "a"});
        InternalRow row2 = compactedRow(rowType, new Object[] {1, "3", "a"});

        InternalRow row3 = compactedRow(rowType, new Object[] {2, "2", "b"});
        InternalRow row4 = compactedRow(rowType, new Object[] {2, "3", "b"});

        int row1Bucket = lakeTableBucketAssigner.assignBucket(row1);
        int row2Bucket = lakeTableBucketAssigner.assignBucket(row2);
        int row3Bucket = lakeTableBucketAssigner.assignBucket(row3);
        int row4Bucket = lakeTableBucketAssigner.assignBucket(row4);

        assertThat(row1Bucket).isEqualTo(row2Bucket);
        assertThat(row3Bucket).isEqualTo(row4Bucket);
        assertThat(row1Bucket).isNotEqualTo(row3Bucket);
    }
}
