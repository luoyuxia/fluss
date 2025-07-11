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

import com.alibaba.fluss.row.InternalRow;

import java.util.Comparator;

/**
 * Represents a <strong>verified</strong> sorted view of records with a defined ordering.
 * Implementations must guarantee all records comply with the returned comparator's order.
 *
 * <p>This is a marker interface that implies strict ordering consistency. Any implementation
 * claiming to be a {@code SortedView} must maintain invariants:
 *
 * <ul>
 *   <li>For any two records a and b, a precedes b iff {@code order().compare(a, b) < 0}
 *   <li>The ordering must be consistent across all record accesses
 * </ul>
 *
 * <p>Note: This is mainly used for union read primary key table since we will do sort merge records
 * in lake and fluss. The records in primary key table for lake may should implement this method for
 * union read with a better performance.
 */
public interface SortedView {

    /**
     * Returns the definitive comparator that governs record ordering.
     *
     * @return a non-null, consistent comparator that defines the total order of records. The
     *     comparator must satisfy the general contract of {@link Comparator#compare(Object,
     *     Object)}.
     */
    Comparator<InternalRow> order();
}
