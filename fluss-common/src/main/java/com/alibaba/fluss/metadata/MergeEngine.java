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

package com.alibaba.fluss.metadata;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.Sets;
import com.alibaba.fluss.types.BigIntType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.IntType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** The merge engine for primary key table. */
public class MergeEngine {

    public static final Set<String> VERSION_SUPPORTED_DATA_TYPES =
            Sets.newHashSet(
                    BigIntType.class.getName(),
                    IntType.class.getName(),
                    TimestampType.class.getName(),
                    TimeType.class.getName(),
                    LocalZonedTimestampType.class.getName());
    private final Type type;
    private final String column;

    private MergeEngine(Type type) {
        this(type, null);
    }

    private MergeEngine(Type type, String column) {
        this.type = type;
        this.column = column;
    }

    public static MergeEngine create(Map<String, String> properties) {
        return create(properties, null);
    }

    public static MergeEngine create(Map<String, String> properties, RowType rowType) {
        return create(Configuration.fromMap(properties), rowType);
    }

    public static MergeEngine create(Configuration options, RowType rowType) {
        if (options == null) {
            return null;
        }
        MergeEngine.Type type = options.get(ConfigOptions.TABLE_MERGE_ENGINE);
        if (type == null) {
            return null;
        }

        switch (type) {
            case FIRST_ROW:
                return new MergeEngine(Type.FIRST_ROW);
            case VERSION:
                String column = options.get(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN);
                if (column == null) {
                    throw new IllegalArgumentException(
                            "When the merge engine is set to version, the 'table.merge-engine.version.column' cannot be empty.");
                }
                if (rowType != null) {
                    int fieldIndex = rowType.getFieldIndex(column);
                    if (fieldIndex == -1) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "When the merge engine is set to version, the column %s does not exist.",
                                        column));
                    }
                    DataType dataType = rowType.getTypeAt(fieldIndex);
                    if (!VERSION_SUPPORTED_DATA_TYPES.contains(dataType.getClass().getName())) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "The merge engine column is not support type %s .",
                                        dataType.asSummaryString()));
                    }
                }
                return new MergeEngine(Type.VERSION, column);
            default:
                throw new UnsupportedOperationException("Unsupported merge engine: " + type);
        }
    }

    public Type getType() {
        return type;
    }

    public String getColumn() {
        return column;
    }

    public enum Type {
        FIRST_ROW("first_row"),
        VERSION("version");
        private final String value;

        Type(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MergeEngine that = (MergeEngine) o;
        return type == that.type && Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, column);
    }
}
