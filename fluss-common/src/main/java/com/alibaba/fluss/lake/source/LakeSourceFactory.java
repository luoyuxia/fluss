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

package com.alibaba.fluss.lake.source;

import com.alibaba.fluss.metadata.TablePath;

/**
 * Factory interface for creating data sources from datalake storage. Enables creation of sources
 * with different access patterns.
 */
public interface LakeSourceFactory {

    /**
     * Creates a log source for accessing log data of Fluss(data of Fluss log table, change log of
     * Fluss primary key table).
     *
     * @param tablePath table path for the target table
     * @return Created FlussLogSource instance
     */
    FlussLogSource createFlussLogSource(TablePath tablePath);
}
