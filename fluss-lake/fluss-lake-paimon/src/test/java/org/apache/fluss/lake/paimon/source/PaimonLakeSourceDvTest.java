/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.source.RowWithPosResult;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.writeAndCommitData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PaimonLakeSource} deletion vector related functionality. */
class PaimonLakeSourceDvTest {

    private static final String DEFAULT_DB = "test_db";

    @TempDir private Path tempDir;
    @TempDir private File compactionTempDir;
    private Catalog paimonCatalog;
    private Configuration paimonConfig;

    @BeforeEach
    void setUp() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("warehouse", tempDir.resolve("warehouse").toString());
        options.put("metastore", "filesystem");
        paimonCatalog =
                CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(options)));
        paimonCatalog.createDatabase(DEFAULT_DB, false);

        paimonConfig = new Configuration();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            paimonConfig.setString(entry.getKey(), entry.getValue());
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
    }

    @Test
    void testPlanDeltaReturnsNull() throws Exception {
        // Case 1: DV not enabled
        Identifier noDvId = Identifier.create(DEFAULT_DB, "no_dv");
        Schema noDvSchema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .option("bucket", "1")
                        .build();
        paimonCatalog.createTable(noDvId, noDvSchema, false);
        FileStoreTable noDvTable = (FileStoreTable) paimonCatalog.getTable(noDvId);
        writeAndCommitData(noDvTable, Collections.singletonMap(0, generateRows(0, 3)));
        compact(noDvTable, 0);
        assertThat(newLakeSource("no_dv").planDelta(0)).isNull();

        // Case 2: No COMPACT snapshot
        Identifier noCompactId = Identifier.create(DEFAULT_DB, "no_compact");
        FileStoreTable noCompactTable = createDvTable(noCompactId, 1);
        writeAndCommitData(noCompactTable, Collections.singletonMap(0, generateRows(0, 3)));
        assertThat(newLakeSource("no_compact").planDelta(0)).isNull();

        // Case 3: No new snapshot after fromSnapshotId
        Identifier noNewId = Identifier.create(DEFAULT_DB, "no_new");
        FileStoreTable noNewTable = createDvTable(noNewId, 1);
        writeAndCommitData(noNewTable, Collections.singletonMap(0, generateRows(0, 3)));
        compact(noNewTable, 0);
        long latest = noNewTable.snapshotManager().latestSnapshotId();
        assertThat(newLakeSource("no_new").planDelta(latest)).isNull();

        // Case 4: Non-existent table throws
        assertThatThrownBy(() -> newLakeSource("non_existent").planDelta(0))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testPlanDeltaFullScan() throws Exception {
        Identifier tableId = Identifier.create(DEFAULT_DB, "full_scan");
        FileStoreTable table = createDvTableWithSystemColumns(tableId);

        // Write 5 rows and compact
        writeAndCommitData(table, Collections.singletonMap(0, generateRowsWithSystemColumns(0, 5)));
        compact(table, 0);

        // planDelta from 0: full scan path (no valid base snapshot)
        PaimonLakeSource lakeSource = newLakeSource("full_scan");
        DataDeltaPlan<PaimonSplit> deltaPlan = lakeSource.planDelta(0);
        assertThat(deltaPlan).isNotNull();
        assertThat(deltaPlan.getSplits()).isNotEmpty();

        // Verify split structure and read all records
        Set<Integer> readC1Values = new HashSet<>();
        for (PaimonSplit split : deltaPlan.getSplits()) {
            // Each split contains exactly one file
            assertThat(split.dataSplit().dataFiles()).hasSize(1);
            assertThat(split.fileName()).isEqualTo(split.dataSplit().dataFiles().get(0).fileName());
            assertThat(split.bucket()).isEqualTo(0);

            // Read via reader
            RecordReader reader = lakeSource.createRecordReader(() -> split);
            try (CloseableIterator<LogRecord> it = reader.read()) {
                while (it.hasNext()) {
                    LogRecord record = it.next();
                    int c1 = record.getRow().getInt(0);
                    readC1Values.add(c1);
                    assertThat(record.getRow().getString(1).toString()).isEqualTo("val" + c1);
                    assertThat(record.logOffset()).isEqualTo(c1);
                }
            }
        }
        assertThat(readC1Values).containsExactlyInAnyOrder(0, 1, 2, 3, 4);
    }

    @Test
    void testPlanDeltaIncrementalDiff() throws Exception {
        Identifier tableId = Identifier.create(DEFAULT_DB, "incr_diff");
        FileStoreTable table = createDvTableWithSystemColumns(tableId);

        // Round 1: write batch1 + compact
        long now = System.currentTimeMillis();
        writeAndCommitData(table, Collections.singletonMap(0, generateRowsWithSystemColumns(0, 3)));
        compact(table, 0);
        long firstCompactSnapshot = table.snapshotManager().latestSnapshotId();

        // Round 2: write batch2 (overlapping keys 2 + new keys 3..5) + compact
        List<GenericRow> batch2 = new ArrayList<>();
        for (int i = 2; i < 6; i++) {
            batch2.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("updated_" + i),
                            0,
                            (long) (i + 10),
                            Timestamp.fromEpochMillis(now)));
        }
        writeAndCommitData(table, Collections.singletonMap(0, batch2));
        compact(table, 0);

        // Incremental diff from firstCompactSnapshot
        PaimonLakeSource lakeSource = newLakeSource("incr_diff");
        DataDeltaPlan<PaimonSplit> deltaPlan = lakeSource.planDelta(firstCompactSnapshot);
        assertThat(deltaPlan).isNotNull();
        assertThat(deltaPlan.getSplits()).isNotEmpty();

        // Deleted files should reference files from the first compact snapshot
        Map<String, Map<Integer, List<String>>> deletedFiles = deltaPlan.getDeletedFiles();
        assertThat(deletedFiles).containsKey(null);
        assertThat(deletedFiles.get(null)).containsKey(0);
        assertThat(deletedFiles.get(null).get(0)).isNotEmpty();

        // Read all incremental splits
        Set<Integer> readC1Values = new HashSet<>();
        for (PaimonSplit split : deltaPlan.getSplits()) {
            RecordReader reader = lakeSource.createRecordReader(() -> split);
            try (CloseableIterator<LogRecord> it = reader.read()) {
                while (it.hasNext()) {
                    LogRecord record = it.next();
                    readC1Values.add(record.getRow().getInt(0));
                }
            }
        }
        assertThat(readC1Values).isNotEmpty();
    }

    @Test
    void testPlanDeltaDeletionVectorFiltering() throws Exception {
        // Test DV filtering with large data. Use changelog-producer=lookup so Paimon
        // uses ForceUpLevel0Compaction which can generate DVs on existing files.
        Identifier tableId = Identifier.create(DEFAULT_DB, "dv_filtering");
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("__bucket", DataTypes.INT())
                        .column("__offset", DataTypes.BIGINT())
                        .column("__timestamp", DataTypes.TIMESTAMP(6))
                        .primaryKey("c1")
                        .option("bucket", "1")
                        .option("deletion-vectors.enabled", "true")
                        .option("changelog-producer", "lookup")
                        .build();
        paimonCatalog.createTable(tableId, schema, false);
        FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(tableId);

        // Step 1: Write 1000 rows and compact to create a large L5 file
        writeAndCommitData(
                table, Collections.singletonMap(0, generateRowsWithSystemColumns(0, 1000)));
        compact(table, 0);

        // Step 2: Write updates for 10 keys, then compact.
        // With lookup changelog-producer, ForceUpLevel0Compaction picks only L0
        // and creates DVs on the existing L5 file via lookup merge.
        long now = System.currentTimeMillis();
        List<GenericRow> updates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            updates.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("updated_" + i),
                            0,
                            (long) (i + 10000),
                            Timestamp.fromEpochMillis(now)));
        }
        writeAndCommitData(table, Collections.singletonMap(0, updates));
        compact(table, 0);

        // planDelta from 0
        PaimonLakeSource lakeSource = newLakeSource("dv_filtering");
        DataDeltaPlan<PaimonSplit> deltaPlan = lakeSource.planDelta(0);
        assertThat(deltaPlan).isNotNull();

        // Check for DV files in splits
        boolean hasDvFiles = false;
        for (PaimonSplit split : deltaPlan.getSplits()) {
            DataSplit ds = split.dataSplit();
            List<DeletionFile> dvFiles = ds.deletionFiles().orElse(null);
            if (dvFiles != null) {
                for (DeletionFile df : dvFiles) {
                    if (df != null) {
                        hasDvFiles = true;
                        break;
                    }
                }
            }
        }
        assertThat(hasDvFiles)
                .as("Lookup compaction should produce deletion vector files")
                .isTrue();

        // Read all splits: DV filtering should ensure each key appears once
        // with the latest value
        Map<Integer, String> readData = new HashMap<>();
        for (PaimonSplit split : deltaPlan.getSplits()) {
            RecordReader reader = lakeSource.createRecordReader(() -> split);
            try (CloseableIterator<LogRecord> it = reader.read()) {
                while (it.hasNext()) {
                    LogRecord record = it.next();
                    int c1 = record.getRow().getInt(0);
                    String c2 = record.getRow().getString(1).toString();
                    assertThat(readData).doesNotContainKey(c1);
                    readData.put(c1, c2);
                }
            }
        }

        // All 1000 keys present
        assertThat(readData).hasSize(1000);
        // Updated keys have new values
        for (int i = 0; i < 10; i++) {
            assertThat(readData.get(i)).isEqualTo("updated_" + i);
        }
        // Non-updated keys have original values
        for (int i = 10; i < 1000; i++) {
            assertThat(readData.get(i)).isEqualTo("val" + i);
        }
    }

    @Test
    void testPlanDeltaWithPartitionedTable() throws Exception {
        Identifier tableId = Identifier.create(DEFAULT_DB, "partitioned");
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .primaryKey("c1", "pt")
                        .partitionKeys("pt")
                        .option("bucket", "1")
                        .option("deletion-vectors.enabled", "true")
                        .build();
        paimonCatalog.createTable(tableId, schema, false);
        FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(tableId);

        // Write data to partition "p1"
        List<GenericRow> rows = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            rows.add(
                    GenericRow.of(
                            i, BinaryString.fromString("v" + i), BinaryString.fromString("p1")));
        }
        writeAndCommitData(table, Collections.singletonMap(0, rows));

        // Compact partition "p1"
        org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper compactHelper =
                new org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper(
                        table, compactionTempDir);
        compactHelper
                .compactBucket(
                        org.apache.paimon.data.BinaryRow.singleColumn(
                                BinaryString.fromString("p1")),
                        0)
                .commit();

        DataDeltaPlan<PaimonSplit> deltaPlan = newLakeSource("partitioned").planDelta(0);
        assertThat(deltaPlan).isNotNull();
        assertThat(deltaPlan.getSplits()).isNotEmpty();
        for (PaimonSplit split : deltaPlan.getSplits()) {
            assertThat(split.partition()).containsExactly("p1");
            assertThat(split.fileName()).isNotNull();
        }
    }

    @Test
    void testReadWithPosReflectsPhysicalPositionWithDv() throws Exception {
        // Create DV table with lookup changelog-producer so that
        // ForceUpLevel0Compaction generates DVs on existing files.
        Identifier tableId = Identifier.create(DEFAULT_DB, "readwithpos_dv");
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("__bucket", DataTypes.INT())
                        .column("__offset", DataTypes.BIGINT())
                        .column("__timestamp", DataTypes.TIMESTAMP(6))
                        .primaryKey("c1")
                        .option("bucket", "1")
                        .option("deletion-vectors.enabled", "true")
                        .option("changelog-producer", "lookup")
                        .build();
        paimonCatalog.createTable(tableId, schema, false);
        FileStoreTable table = (FileStoreTable) paimonCatalog.getTable(tableId);

        // Step 1: Write 10 rows (c1=0..9) and compact to create a single data file.
        // Rows are sorted by PK (c1) in the file, so c1=i is at physical position i.
        writeAndCommitData(
                table, Collections.singletonMap(0, generateRowsWithSystemColumns(0, 10)));
        compact(table, 0);

        // Step 2: Update c1=2,5,7. With lookup changelog-producer,
        // ForceUpLevel0Compaction creates DVs on the original file for these keys.
        long now = System.currentTimeMillis();
        List<GenericRow> updates = new ArrayList<>();
        for (int c1 : new int[] {2, 5, 7}) {
            updates.add(
                    GenericRow.of(
                            c1,
                            BinaryString.fromString("updated_" + c1),
                            0,
                            (long) (c1 + 100),
                            Timestamp.fromEpochMillis(now)));
        }
        writeAndCommitData(table, Collections.singletonMap(0, updates));
        compact(table, 0);

        // Step 3: Use planDelta to get single-file splits, separate DV and non-DV splits
        PaimonLakeSource lakeSource = newLakeSource("readwithpos_dv");
        DataDeltaPlan<PaimonSplit> deltaPlan = lakeSource.planDelta(0);
        assertThat(deltaPlan).isNotNull();

        PaimonSplit dvSplit = null;
        PaimonSplit nonDvSplit = null;
        for (PaimonSplit split : deltaPlan.getSplits()) {
            if (hasDeletionFiles(split)) {
                dvSplit = split;
            } else {
                nonDvSplit = split;
            }
        }
        assertThat(dvSplit).as("Expected a split with DV files").isNotNull();
        assertThat(nonDvSplit).as("Expected a split without DV files").isNotNull();

        // Step 4a: Read DV split with readWithPos.
        // DV filtering is applied, so DV-deleted rows (c1=2,5,7) are skipped.
        // Positions reflect physical file positions with gaps for deleted rows.
        final PaimonSplit dvSplitToRead = dvSplit;
        RecordReader dvReader = lakeSource.createRecordReader(() -> dvSplitToRead);
        List<Long> dvPositions = new ArrayList<>();
        List<Integer> dvC1Values = new ArrayList<>();
        try (CloseableIterator<RowWithPosResult> iter = dvReader.readWithPos()) {
            while (iter.hasNext()) {
                RowWithPosResult rwp = iter.next();
                dvPositions.add(rwp.getPos());
                dvC1Values.add(rwp.getRow().getInt(0));
            }
        }

        assertThat(dvC1Values)
                .as("DV split should return non-deleted rows only")
                .containsExactly(0, 1, 3, 4, 6, 8, 9);
        assertThat(dvPositions)
                .as("DV split positions should have gaps for deleted rows")
                .containsExactly(0L, 1L, 3L, 4L, 6L, 8L, 9L);

        // Step 4b: Read non-DV split with readWithPos.
        // No DV filtering, positions should be sequential with no gaps.
        final PaimonSplit nonDvSplitToRead = nonDvSplit;
        RecordReader nonDvReader = lakeSource.createRecordReader(() -> nonDvSplitToRead);
        List<Long> nonDvPositions = new ArrayList<>();
        List<Integer> nonDvC1Values = new ArrayList<>();
        try (CloseableIterator<RowWithPosResult> iter = nonDvReader.readWithPos()) {
            while (iter.hasNext()) {
                RowWithPosResult rwp = iter.next();
                nonDvPositions.add(rwp.getPos());
                nonDvC1Values.add(rwp.getRow().getInt(0));
            }
        }

        assertThat(nonDvC1Values)
                .as("Non-DV split should contain the updated rows")
                .containsExactlyInAnyOrder(2, 5, 7);
        assertThat(nonDvPositions)
                .as("Non-DV split positions should be sequential with no gaps")
                .containsExactly(0L, 1L, 2L);
    }

    // ---- helpers ----

    private static boolean hasDeletionFiles(PaimonSplit split) {
        List<DeletionFile> dvFiles = split.dataSplit().deletionFiles().orElse(null);
        if (dvFiles != null) {
            for (DeletionFile df : dvFiles) {
                if (df != null) {
                    return true;
                }
            }
        }
        return false;
    }

    private PaimonLakeSource newLakeSource(String tableName) {
        return new PaimonLakeSource(paimonConfig, TablePath.of(DEFAULT_DB, tableName));
    }

    private void compact(FileStoreTable table, int bucket) throws Exception {
        org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper compactHelper =
                new org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper(
                        table, compactionTempDir);
        compactHelper.compactBucket(bucket).commit();
    }

    private FileStoreTable createDvTable(Identifier tableIdentifier, int numBuckets)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .primaryKey("c1")
                        .option("bucket", String.valueOf(numBuckets))
                        .option("deletion-vectors.enabled", "true")
                        .build();
        paimonCatalog.createTable(tableIdentifier, schema, false);
        return (FileStoreTable) paimonCatalog.getTable(tableIdentifier);
    }

    private FileStoreTable createDvTableWithSystemColumns(Identifier tableIdentifier)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("__bucket", DataTypes.INT())
                        .column("__offset", DataTypes.BIGINT())
                        .column("__timestamp", DataTypes.TIMESTAMP(6))
                        .primaryKey("c1")
                        .option("bucket", "1")
                        .option("deletion-vectors.enabled", "true")
                        .build();
        paimonCatalog.createTable(tableIdentifier, schema, false);
        return (FileStoreTable) paimonCatalog.getTable(tableIdentifier);
    }

    private static List<GenericRow> generateRows(int from, int to) {
        List<GenericRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("value" + i)));
        }
        return rows;
    }

    private static List<GenericRow> generateRowsWithSystemColumns(int from, int to) {
        long now = System.currentTimeMillis();
        List<GenericRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(
                    GenericRow.of(
                            i,
                            BinaryString.fromString("val" + i),
                            0, // __bucket
                            (long) i, // __offset
                            Timestamp.fromEpochMillis(now)));
        }
        return rows;
    }
}
