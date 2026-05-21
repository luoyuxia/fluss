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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RowPosSstFileWriter}. */
class RowPosSstFileWriterTest {

    @TempDir Path tempDir;
    private DvRocksDB dvRocksDB;

    @BeforeEach
    void setUp() throws Exception {
        dvRocksDB = DvRocksDB.open(tempDir.resolve("db").toString());
    }

    @AfterEach
    void tearDown() {
        if (dvRocksDB != null) {
            dvRocksDB.close();
        }
    }

    @Test
    void testWriteEmptyEntries() throws Exception {
        String outputDir = tempDir.resolve("sst_output").toString();
        new File(outputDir).mkdirs();

        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(outputDir)) {
            List<RowPosSstFileWriter.SstFileMeta> metas =
                    writer.write(Collections.<RowPosSstFileWriter.RowPosEntry>emptyList());
            assertThat(metas).isEmpty();
        }
    }

    @Test
    void testWriteSingleEntry() throws Exception {
        String outputDir = tempDir.resolve("sst_output").toString();
        new File(outputDir).mkdirs();

        List<RowPosSstFileWriter.RowPosEntry> entries =
                Collections.singletonList(
                        new RowPosSstFileWriter.RowPosEntry(1L, new FilePos(10, 100L)));

        List<RowPosSstFileWriter.SstFileMeta> metas;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(outputDir)) {
            metas = writer.write(entries);
        }

        assertThat(metas).hasSize(1);
        assertThat(metas.get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(metas.get(0).getFileSize()).isGreaterThan(0);

        // Verify by ingesting into RowPosIndex
        String sstPath = new File(outputDir, "sst_0.sst").getPath();
        dvRocksDB.rowPosIndex().ingestExternalFile(Collections.singletonList(sstPath));
        assertThat(dvRocksDB.rowPosIndex().get(1L)).isEqualTo(new FilePos(10, 100L));
    }

    @Test
    void testWriteMultipleEntries() throws Exception {
        String outputDir = tempDir.resolve("sst_output").toString();
        new File(outputDir).mkdirs();

        List<RowPosSstFileWriter.RowPosEntry> entries =
                Arrays.asList(
                        new RowPosSstFileWriter.RowPosEntry(1L, new FilePos(1, 10L)),
                        new RowPosSstFileWriter.RowPosEntry(2L, new FilePos(1, 20L)),
                        new RowPosSstFileWriter.RowPosEntry(3L, new FilePos(2, 30L)),
                        new RowPosSstFileWriter.RowPosEntry(100L, new FilePos(5, 500L)));

        List<RowPosSstFileWriter.SstFileMeta> metas;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(outputDir)) {
            metas = writer.write(entries);
        }

        assertThat(metas).hasSize(1);

        // Verify by ingesting
        String sstPath = new File(outputDir, "sst_0.sst").getPath();
        dvRocksDB.rowPosIndex().ingestExternalFile(Collections.singletonList(sstPath));
        assertThat(dvRocksDB.rowPosIndex().get(1L)).isEqualTo(new FilePos(1, 10L));
        assertThat(dvRocksDB.rowPosIndex().get(2L)).isEqualTo(new FilePos(1, 20L));
        assertThat(dvRocksDB.rowPosIndex().get(3L)).isEqualTo(new FilePos(2, 30L));
        assertThat(dvRocksDB.rowPosIndex().get(100L)).isEqualTo(new FilePos(5, 500L));
    }

    @Test
    void testWriteEntriesMustBeSorted() throws Exception {
        String outputDir = tempDir.resolve("sst_output").toString();
        new File(outputDir).mkdirs();

        // Unsorted entries should cause RocksDB to throw
        List<RowPosSstFileWriter.RowPosEntry> entries =
                Arrays.asList(
                        new RowPosSstFileWriter.RowPosEntry(3L, new FilePos(1, 30L)),
                        new RowPosSstFileWriter.RowPosEntry(1L, new FilePos(1, 10L)));

        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(outputDir)) {
            assertThatThrownBy(() -> writer.write(entries)).isInstanceOf(IOException.class);
        }
    }

    @Test
    void testSplitAcrossMultipleSstFiles() throws Exception {
        String outputDir = tempDir.resolve("sst_output").toString();
        new File(outputDir).mkdirs();

        // Create entries exceeding MAX_ENTRIES_PER_SST
        int totalEntries = RowPosSstFileWriter.MAX_ENTRIES_PER_SST + 100;
        List<RowPosSstFileWriter.RowPosEntry> entries = new ArrayList<>(totalEntries);
        for (long i = 0; i < totalEntries; i++) {
            entries.add(new RowPosSstFileWriter.RowPosEntry(i, new FilePos(1, i)));
        }

        List<RowPosSstFileWriter.SstFileMeta> metas;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(outputDir)) {
            metas = writer.write(entries);
        }

        assertThat(metas).hasSize(2);
        assertThat(metas.get(0).getFileName()).isEqualTo("sst_0.sst");
        assertThat(metas.get(1).getFileName()).isEqualTo("sst_1.sst");

        // Ingest both SST files and verify some entries
        List<String> sstPaths =
                Arrays.asList(
                        new File(outputDir, "sst_0.sst").getPath(),
                        new File(outputDir, "sst_1.sst").getPath());
        dvRocksDB.rowPosIndex().ingestExternalFile(sstPaths);

        assertThat(dvRocksDB.rowPosIndex().get(0L)).isEqualTo(new FilePos(1, 0L));
        assertThat(dvRocksDB.rowPosIndex().get((long) RowPosSstFileWriter.MAX_ENTRIES_PER_SST - 1))
                .isEqualTo(new FilePos(1, RowPosSstFileWriter.MAX_ENTRIES_PER_SST - 1));
        assertThat(dvRocksDB.rowPosIndex().get((long) RowPosSstFileWriter.MAX_ENTRIES_PER_SST))
                .isEqualTo(new FilePos(1, RowPosSstFileWriter.MAX_ENTRIES_PER_SST));
        assertThat(dvRocksDB.rowPosIndex().get((long) totalEntries - 1))
                .isEqualTo(new FilePos(1, totalEntries - 1));
    }
}
