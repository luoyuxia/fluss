/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.lake;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

/**
 * Factory for creating {@link LakeHistoricalPartitionReader} instances.
 *
 * <p>This factory is used by the server to obtain readers for historical partitions in the data
 * lake. The factory can be optionally set on the server; if not set, historical partition lookups
 * will fail with an appropriate error.
 */
public interface LakeHistoricalPartitionReaderFactory {

    /**
     * Create a reader for the given table.
     *
     * @param tablePath the table path
     * @param conf the configuration
     * @return the reader, or null if not supported
     */
    @Nullable
    LakeHistoricalPartitionReader createReader(TablePath tablePath, Configuration conf);

    /**
     * Check if this factory can create a reader for the given table.
     *
     * @param tablePath the table path
     * @return true if this factory can create a reader
     */
    boolean supports(TablePath tablePath);
}
