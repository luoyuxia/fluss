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

package com.alibaba.fluss.lake;

import com.alibaba.fluss.exception.DatabaseAlreadyExistException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.TableInfo;

public interface MetadataLakeApplier extends AutoCloseable {

    void applyDatabaseCreated(String databaseName, DatabaseDescriptor databaseDescriptor)
            throws DatabaseAlreadyExistException;

    void applyTableCreated(TableInfo flussTable)
            throws DatabaseAlreadyExistException, TableAlreadyExistException;

    @Override
    default void close() throws Exception {
        // default do nothing
    }
}
