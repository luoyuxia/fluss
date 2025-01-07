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

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * The merge engine for primary key table.
 *
 * @since 0.6
 */
public class MergeEngine {

    private final Type type;

    /** When merge engine type is version, column cannot be null. */
    @Nullable private final String column;

    private MergeEngine(Type type) {
        this(type, null);
    }

    private MergeEngine(Type type, String column) {
        this.type = type;
        this.column = column;
    }

    public static MergeEngine create(Map<String, String> properties) {
        return create(Configuration.fromMap(properties));
    }

    private static MergeEngine create(Configuration options) {
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
                            String.format(
                                    "When the merge engine is set to version, the '%s' must be set.",
                                    ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
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
