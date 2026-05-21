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

package org.apache.fluss.server.kv.dv;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.SstFileWriter;
import org.rocksdb.WriteBatch;

import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowPosIndex}. */
class RowPosIndexTest {

    @TempDir Path tempDir;
    private DvRocksDB dvRocksDB;

    @BeforeEach
    void setup() throws Exception {
        dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString());
    }

    @AfterEach
    void teardown() {
        if (dvRocksDB != null) {
            dvRocksDB.close();
        }
    }

    @Test
    void testPutAndGet() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        FilePos pos = new FilePos(10, 100L);

        index.put(1L, pos);
        assertThat(index.get(1L)).isEqualTo(pos);
    }

    @Test
    void testGetNonExistent() throws Exception {
        assertThat(dvRocksDB.rowPosIndex().get(999L)).isNull();
    }

    @Test
    void testPutOverwrite() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();

        index.put(1L, new FilePos(10, 100L));
        index.put(1L, new FilePos(20, 200L));
        assertThat(index.get(1L)).isEqualTo(new FilePos(20, 200L));
    }

    @Test
    void testDelete() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();

        index.put(1L, new FilePos(10, 100L));
        index.delete(1L);
        assertThat(index.get(1L)).isNull();
    }

    @Test
    void testWriteBatch() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();

        try (WriteBatch batch = new WriteBatch()) {
            index.put(batch, 1L, new FilePos(10, 100L));
            index.put(batch, 2L, new FilePos(20, 200L));
            index.put(batch, 3L, new FilePos(30, 300L));
            dvRocksDB.write(batch);
        }

        assertThat(index.get(1L)).isEqualTo(new FilePos(10, 100L));
        assertThat(index.get(2L)).isEqualTo(new FilePos(20, 200L));
        assertThat(index.get(3L)).isEqualTo(new FilePos(30, 300L));
    }

    @Test
    void testWriteBatchDelete() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();

        index.put(1L, new FilePos(10, 100L));
        index.put(2L, new FilePos(20, 200L));

        try (WriteBatch batch = new WriteBatch()) {
            index.delete(batch, 1L);
            dvRocksDB.write(batch);
        }

        assertThat(index.get(1L)).isNull();
        assertThat(index.get(2L)).isEqualTo(new FilePos(20, 200L));
    }

    @Test
    void testIngestExternalFile() throws Exception {
        RowPosIndex index = dvRocksDB.rowPosIndex();
        String sstPath = tempDir.resolve("test.sst").toString();

        // Create SST file with sorted entries
        try (EnvOptions envOptions = new EnvOptions();
                Options options = new Options();
                SstFileWriter writer = new SstFileWriter(envOptions, options)) {
            writer.open(sstPath);
            // Keys must be in sorted order (BigEndian encoding ensures this)
            writer.put(RowPosIndex.encodeRowId(1L), new FilePos(10, 100L).encode());
            writer.put(RowPosIndex.encodeRowId(2L), new FilePos(10, 200L).encode());
            writer.put(RowPosIndex.encodeRowId(3L), new FilePos(20, 300L).encode());
            writer.finish();
        }

        index.ingestExternalFile(Collections.singletonList(sstPath));

        assertThat(index.get(1L)).isEqualTo(new FilePos(10, 100L));
        assertThat(index.get(2L)).isEqualTo(new FilePos(10, 200L));
        assertThat(index.get(3L)).isEqualTo(new FilePos(20, 300L));
    }

    @Test
    void testEncodeDecodeRowId() {
        assertThat(RowPosIndex.decodeRowId(RowPosIndex.encodeRowId(0L))).isEqualTo(0L);
        assertThat(RowPosIndex.decodeRowId(RowPosIndex.encodeRowId(1L))).isEqualTo(1L);
        assertThat(RowPosIndex.decodeRowId(RowPosIndex.encodeRowId(Long.MAX_VALUE)))
                .isEqualTo(Long.MAX_VALUE);
        assertThat(RowPosIndex.decodeRowId(RowPosIndex.encodeRowId(-1L))).isEqualTo(-1L);
    }
}
