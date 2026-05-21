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
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.rocksdb.WriteBatch;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LakeDv}. */
class LakeDvTest {

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
        LakeDv lakeDv = dvRocksDB.lakeDv();
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        bitmap.add(10L);
        bitmap.add(20L);
        bitmap.add(30L);

        lakeDv.put(1, bitmap);

        Roaring64Bitmap result = lakeDv.get(1);
        assertThat(result).isNotNull();
        assertThat(result.contains(10L)).isTrue();
        assertThat(result.contains(20L)).isTrue();
        assertThat(result.contains(30L)).isTrue();
        assertThat(result.getLongCardinality()).isEqualTo(3);
    }

    @Test
    void testGetNonExistent() throws Exception {
        assertThat(dvRocksDB.lakeDv().get(999)).isNull();
    }

    @Test
    void testMarkDeletedAccumulation() throws Exception {
        LakeDv lakeDv = dvRocksDB.lakeDv();

        lakeDv.markDeleted(1, 10L);
        lakeDv.markDeleted(1, 20L);
        lakeDv.markDeleted(1, 30L);

        Roaring64Bitmap result = lakeDv.get(1);
        assertThat(result).isNotNull();
        assertThat(result.getLongCardinality()).isEqualTo(3);
        assertThat(result.contains(10L)).isTrue();
        assertThat(result.contains(20L)).isTrue();
        assertThat(result.contains(30L)).isTrue();
    }

    @Test
    void testMarkDeletedIdempotent() throws Exception {
        LakeDv lakeDv = dvRocksDB.lakeDv();

        lakeDv.markDeleted(1, 10L);
        lakeDv.markDeleted(1, 10L);

        Roaring64Bitmap result = lakeDv.get(1);
        assertThat(result.getLongCardinality()).isEqualTo(1);
    }

    @Test
    void testDelete() throws Exception {
        LakeDv lakeDv = dvRocksDB.lakeDv();

        lakeDv.markDeleted(1, 10L);
        lakeDv.delete(1);
        assertThat(lakeDv.get(1)).isNull();
    }

    @Test
    void testDeleteWriteBatch() throws Exception {
        LakeDv lakeDv = dvRocksDB.lakeDv();

        lakeDv.markDeleted(1, 10L);
        lakeDv.markDeleted(2, 20L);

        try (WriteBatch batch = new WriteBatch()) {
            lakeDv.delete(batch, 1);
            dvRocksDB.write(batch);
        }

        assertThat(lakeDv.get(1)).isNull();
        assertThat(lakeDv.get(2)).isNotNull();
    }

    @Test
    void testGetAll() throws Exception {
        LakeDv lakeDv = dvRocksDB.lakeDv();

        lakeDv.markDeleted(1, 10L);
        lakeDv.markDeleted(1, 20L);
        lakeDv.markDeleted(2, 30L);
        lakeDv.markDeleted(3, 40L);

        Map<Integer, Roaring64Bitmap> all = lakeDv.getAll();
        assertThat(all).hasSize(3);
        assertThat(all.get(1).getLongCardinality()).isEqualTo(2);
        assertThat(all.get(2).contains(30L)).isTrue();
        assertThat(all.get(3).contains(40L)).isTrue();
    }

    @Test
    void testGetAllEmpty() throws Exception {
        Map<Integer, Roaring64Bitmap> all = dvRocksDB.lakeDv().getAll();
        assertThat(all).isEmpty();
    }

    @Test
    void testEncodeDecodeFileId() {
        assertThat(LakeDv.decodeFileId(LakeDv.encodeFileId(0))).isEqualTo(0);
        assertThat(LakeDv.decodeFileId(LakeDv.encodeFileId(1))).isEqualTo(1);
        assertThat(LakeDv.decodeFileId(LakeDv.encodeFileId(Integer.MAX_VALUE)))
                .isEqualTo(Integer.MAX_VALUE);
    }
}
