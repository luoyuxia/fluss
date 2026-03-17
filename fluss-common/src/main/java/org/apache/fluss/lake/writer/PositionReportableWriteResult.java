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

package org.apache.fluss.lake.writer;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Marker interface for lake write results that can report row positions for DV bookkeeping. */
public interface PositionReportableWriteResult {

    /** Returns file-path keyed row position report: file path -> list of (rowId, rowPosition). */
    @Nullable
    Map<String, List<long[]>> getPositionReport();

    /** Returns referenced data files whose logical DVs were materialized to physical DVs. */
    @Nullable
    default List<String> getMaterializedDvFiles() {
        return null;
    }
}
