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

package com.alibaba.fluss.server.kv.mergeengine;

import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.server.kv.partialupdate.PartialUpdater;
import com.alibaba.fluss.server.kv.prewrite.KvPreWriteBuffer;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKv;
import com.alibaba.fluss.server.kv.wal.WalBuilder;

/** A wrapper for first row merge engine. */
public class FirstRowMergeEngine extends RowMergeEngine {

    public FirstRowMergeEngine(
            PartialUpdater partialUpdater,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            KvPreWriteBuffer kvPreWriteBuffer,
            RocksDBKv rocksDBKv,
            Schema schema,
            short schemaId,
            int appendedRecordCount,
            long logOffset) {
        super(
                partialUpdater,
                valueDecoder,
                walBuilder,
                kvPreWriteBuffer,
                rocksDBKv,
                schema,
                schemaId,
                appendedRecordCount,
                logOffset);
    }

    @Override
    protected void deleteOrUpdate(KvPreWriteBuffer.Key key, byte[] oldValue) throws Exception {
        // When Merge engine is "first_row", We don't need to do any update and delete operations.
    }

    @Override
    protected void update(KvPreWriteBuffer.Key key, BinaryRow oldRow, BinaryRow newRow)
            throws Exception {
        // When Merge engine is "first_row", We don't need to do any update operations.
    }
}
