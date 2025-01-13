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

package com.alibaba.fluss.server.kv.wal;

import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.row.InternalRow;

/** The interface to build write-ahead-log batch ({@link MemoryLogRecords}) for kv store. */
public interface WalBuilder {

    void append(RowKind rowKind, InternalRow row) throws Exception;

    MemoryLogRecords build() throws Exception;

    void setWriterState(long writerId, int batchSequence);

    void overrideLastLogOffset(long lastLogOffset);

    void deallocate();
}
