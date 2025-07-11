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

import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.utils.CloseableIterator;

/**
 * Represents a collection of records read from a data lake storage system. Provides iterator-based
 * access to the underlying records with automatic resource management.
 *
 * <p>Implementations must ensure the returned iterator is thread-safe if accessed concurrently.
 */
public interface LakeRecords {

    /**
     * Retrieves a closeable iterator for traversing the lake records.
     *
     * @return a non-null {@link CloseableIterator} that must be closed after use to release
     *     underlying resources. The iterator may be empty but never null.
     * @see CloseableIterator#close()
     */
    CloseableIterator<LogRecord> getLakeRecords();
}
