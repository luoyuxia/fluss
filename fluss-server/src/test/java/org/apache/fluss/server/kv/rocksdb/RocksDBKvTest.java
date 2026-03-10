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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyHandle;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.server.kv.rocksdb.RocksDBKv}. */
class RocksDBKvTest {

    @Test
    void testRocksDbKv(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                        instanceBasePath,
                        rocksDBResourceContainer,
                        rocksDBResourceContainer.getColumnOptions());

        try (RocksDBKv rocksDBKv = rocksDBKvBuilder.build()) {
            // put the k/v
            byte[] key = new byte[] {1, 2, 3};
            byte[] val = new byte[] {1, 2};
            rocksDBKv.put(key, val);
            assertThat(rocksDBKv.get(key)).isEqualTo(val);
            // put with a different value
            byte[] val1 = new byte[] {1};
            rocksDBKv.put(key, val1);
            assertThat(rocksDBKv.get(key)).isEqualTo(val1);
            // delete the key
            rocksDBKv.delete(key);
            assertThat(rocksDBKv.get(key)).isNull();

            // test multi get
            byte[] key2 = new byte[] {1, 2, 3, 4};
            byte[] val2 = new byte[] {1, 2, 3};
            rocksDBKv.put(key2, val2);

            assertThat(rocksDBKv.multiGet(Arrays.asList(key, key2))).containsExactly(null, val2);
        }
    }

    @Test
    void testReopenWithExistingColumnFamilies(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        byte[] key = new byte[] {9, 9, 9};
        byte[] value = new byte[] {7, 7};
        String cfName = "overflow_2020";

        RocksDBResourceContainer firstResourceContainer =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder firstBuilder =
                new RocksDBKvBuilder(
                        instanceBasePath,
                        firstResourceContainer,
                        firstResourceContainer.getColumnOptions());
        try (RocksDBKv rocksDBKv = firstBuilder.build()) {
            ColumnFamilyHandle cf = rocksDBKv.getOrCreateColumnFamily(cfName);
            rocksDBKv.put(cf, key, value);
        }

        RocksDBResourceContainer secondResourceContainer =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder secondBuilder =
                new RocksDBKvBuilder(
                        instanceBasePath,
                        secondResourceContainer,
                        secondResourceContainer.getColumnOptions());
        try (RocksDBKv rocksDBKv = secondBuilder.build()) {
            ColumnFamilyHandle cf = rocksDBKv.getOrCreateColumnFamily(cfName);
            assertThat(rocksDBKv.get(cf, key)).isEqualTo(value);
        }
    }
}
