/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.source1;

import com.alibaba.fluss.predicate.Predicate;

import javax.annotation.Nullable;

/**
 * Contextual information for reading data from a specific {@link LakeSplit} in a datalake system.
 * Contains the target data split along with optional filtering and projection criteria.
 *
 * @param <Split> The type of lake split this context operates on, must extend {@link LakeSplit}
 */
public class LakeSplitReadContext<Split extends LakeSplit> {

    private final Split lakeSplit;

    private final @Nullable Predicate predicate;

    private final @Nullable String[] projectColumns;

    public LakeSplitReadContext(
            Split lakeSplit, @Nullable Predicate predicate, @Nullable String[] projectColumns) {
        this.lakeSplit = lakeSplit;
        this.predicate = predicate;
        this.projectColumns = projectColumns;
    }

    /**
     * Returns the target lake split to be read.
     *
     * @return the non-null lake split instance
     */
    public Split getLakeSplit() {
        return lakeSplit;
    }

    /**
     * Returns the optional filter predicate for row-level filtering. When present, only rows
     * matching this predicate should be returned.
     *
     * @return the filter predicate, or null if no filtering should be applied
     */
    @Nullable
    public Predicate getPredicate() {
        return predicate;
    }

    /**
     * Returns the optional column projection list. When present, only these columns should be
     * included in the result (pushdown projection). When null, all columns should be returned (full
     * projection).
     *
     * @return array of column names to project, or null for full projection
     */
    @Nullable
    public String[] getProjectColumns() {
        return projectColumns;
    }
}
