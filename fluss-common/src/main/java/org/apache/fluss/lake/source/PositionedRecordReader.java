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

package org.apache.fluss.lake.source;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;

/** A {@link RecordReader} that can also expose per-row positions for DV-aware filtering. */
@PublicEvolving
public interface PositionedRecordReader extends RecordReader {

    /**
     * Read a {@link LakeSplit} into a closeable iterator with row-position metadata.
     *
     * @return the closeable iterator of positioned records
     * @throws IOException if an I/O error occurs
     */
    CloseableIterator<PositionedRecord> readWithPosition() throws IOException;
}
