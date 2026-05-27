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

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.source.TableBucketWriteResult;
import org.apache.fluss.flink.tiering.source.TieringSplitReader;
import org.apache.fluss.flink.tiering.source.split.TieringRowPosSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSplit;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.DataDeltaPlan;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.dv.DvRocksDB;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
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
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.lake.paimon.utils.PaimonTestUtils.writeAndCommitData;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link TieringSplitReader} processing a {@link TieringRowPosSplit} produces valid SST
 * files that can be ingested by a RocksDB RowPosIndex.
 *
 * <p>This test uses a real Paimon table with DV support to produce realistic splits with deletion
 * vectors. The splits are serialized and wrapped in a {@link TieringRowPosSplit}, then processed by
 * {@link TieringSplitReader} which reads the files, generates SSTs, and uploads them. The test
 * verifies the SSTs can be ingested and queried via {@link DvRocksDB}.
 */
class RowPosSstGenerationTest {

    private static final String DEFAULT_DB = "test_db";

    @TempDir private Path tempDir;
    @TempDir private File compactionTempDir;
    private Catalog paimonCatalog;
    private Configuration paimonConfig;
    private DvRocksDB dvRocksDB;

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

        dvRocksDB = DvRocksDB.open(tempDir.resolve("rocksdb").toString());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (dvRocksDB != null) {
            dvRocksDB.close();
        }
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
    }

    @Test
    void testSstGenerationFromReadWithPos() throws Exception {
        // 1. Create a DV-enabled Paimon table, write 10 rows, compact to create base file.
        Identifier tableId = Identifier.create(DEFAULT_DB, "sst_ingest");
        FileStoreTable table = createDvTableWithSystemColumns(tableId);
        writeAndCommitData(
                table, Collections.singletonMap(0, generateRowsWithSystemColumns(0, 10)));
        compact(table, 0);

        // 2. Update 3 keys (c1=2,5,7) to generate DVs on the original file.
        long now = System.currentTimeMillis();
        List<GenericRow> updates = new ArrayList<>();
        for (int c1 : new int[] {2, 5, 7}) {
            updates.add(
                    GenericRow.of(
                            c1,
                            BinaryString.fromString("updated_" + c1),
                            0,
                            (long) (c1 + 100),
                            Timestamp.fromEpochMillis(now),
                            (long) (c1 + 100)));
        }
        writeAndCommitData(table, Collections.singletonMap(0, updates));
        compact(table, 0);

        // 3. planDelta to get compaction output splits, serialize via getSplitSerializer().
        TablePath tablePath = TablePath.of(DEFAULT_DB, "sst_ingest");
        PaimonLakeSource planSource = newLakeSource("sst_ingest");
        DataDeltaPlan<PaimonSplit> deltaPlan = planSource.planDelta(0);
        assertThat(deltaPlan).isNotNull();
        assertThat(deltaPlan.getSplits()).isNotEmpty();

        SimpleVersionedSerializer<PaimonSplit> splitSerializer = planSource.getSplitSerializer();
        List<PaimonSplit> splits = deltaPlan.getSplits();
        int[] fileIds = new int[splits.size()];
        byte[][] serializedLakeSplits = new byte[splits.size()][];
        for (int i = 0; i < splits.size(); i++) {
            fileIds[i] = i;
            serializedLakeSplits[i] = splitSerializer.serialize(splits.get(i));
        }

        // 4. Build a TieringRowPosSplit with serialized splits, fileIds, flussColumnCount=2.
        long compactSnapshotId = deltaPlan.getCompactSnapshotId();
        FsPath remoteBasePath = new FsPath(tempDir.resolve("remote").toUri());
        String remoteUploadBasePath = remoteBasePath.toString();
        int flussColumnCount = 2; // c1, c2

        TableBucket tableBucket = new TableBucket(1, null, 0);
        TieringRowPosSplit rowPosSplit =
                new TieringRowPosSplit(
                        tablePath,
                        tableBucket,
                        null,
                        fileIds,
                        serializedLakeSplits,
                        1,
                        compactSnapshotId,
                        remoteUploadBasePath,
                        flussColumnCount);

        // 5. Create a TieringSplitReader with a factory that returns PaimonLakeSource.
        //    connection=null (not used for RowPos), tieringMetrics=null (not used for RowPos).
        LakeTieringFactory<Object, Object> factory = createTestFactory();
        TieringSplitReader<Object> reader = new TieringSplitReader<>(null, factory, null);

        // 6. Process the split via TieringSplitReader.
        List<TieringSplit> splitList = new ArrayList<>();
        splitList.add(rowPosSplit);
        reader.handleSplitsChanges(new SplitsAddition<>(splitList));
        RecordsWithSplitIds<TableBucketWriteResult<Object>> records = reader.fetch();

        // 7. Extract RowPosResult and verify SST metadata.
        String splitIdResult = records.nextSplit();
        assertThat(splitIdResult).isNotNull();
        TableBucketWriteResult<Object> writeResult = records.nextRecordFromSplit();
        assertThat(writeResult).isNotNull();
        assertThat(writeResult.isRowPosResult()).isTrue();

        RowPosResult rowPosResult = writeResult.getRowPosResult();
        assertThat(rowPosResult).isNotNull();
        assertThat(rowPosResult.getBucketId()).isEqualTo(0);
        assertThat(rowPosResult.getSstMetas()).isNotEmpty();
        for (RowPosResult.SstMeta meta : rowPosResult.getSstMetas()) {
            assertThat(meta.getFileSize()).isGreaterThan(0);
        }

        // 8. Download uploaded SSTs from remote path, ingest into DvRocksDB.
        String uploadedSstDir =
                tempDir.resolve("remote")
                        .resolve("rowPos")
                        .resolve(String.valueOf(compactSnapshotId))
                        .resolve("0")
                        .toString();
        List<String> sstPaths = new ArrayList<>();
        for (RowPosResult.SstMeta meta : rowPosResult.getSstMetas()) {
            File sstFile = new File(uploadedSstDir, meta.getFileName());
            assertThat(sstFile).exists();
            sstPaths.add(sstFile.getPath());
        }
        dvRocksDB.rowPosIndex().ingestExternalFile(sstPaths);

        // 9. Verify entries for expected rowIds.
        // DV-filtered file: rowIds 0,1,3,4,6,8,9 (original rows not deleted by DV)
        // Update file: rowIds 102,105,107 (updated rows with __rowid = c1 + 100)
        long[] expectedRowIds = {0, 1, 3, 4, 6, 8, 9, 102, 105, 107};
        Map<Integer, List<Long>> positionsByFile = new HashMap<>();
        for (long rowId : expectedRowIds) {
            org.apache.fluss.server.kv.dv.FilePos fp = dvRocksDB.rowPosIndex().get(rowId);
            assertThat(fp).as("RowPosIndex should contain entry for rowId " + rowId).isNotNull();
            positionsByFile
                    .computeIfAbsent(fp.fileId(), k -> new ArrayList<>())
                    .add(fp.rowPosition());
        }

        // Verify deleted rowIds (2, 5, 7) are not in the index.
        for (long deletedRowId : new long[] {2, 5, 7}) {
            assertThat(dvRocksDB.rowPosIndex().get(deletedRowId))
                    .as("Deleted rowId " + deletedRowId + " should not be in the index")
                    .isNull();
        }

        // The DV-filtered file should have positions with gaps at 2, 5, 7.
        boolean foundDvGaps = false;
        for (List<Long> positions : positionsByFile.values()) {
            Collections.sort(positions);
            if (positions.equals(Arrays.asList(0L, 1L, 3L, 4L, 6L, 8L, 9L))) {
                foundDvGaps = true;
                break;
            }
        }
        assertThat(foundDvGaps)
                .as("DV-filtered positions should have gaps at deleted rows (2, 5, 7)")
                .isTrue();

        reader.close();
    }

    // ---- helpers ----

    /**
     * Creates a minimal {@link LakeTieringFactory} for testing that only supports {@link
     * LakeTieringFactory#createLakeSource(TablePath)} via {@link PaimonLakeSource}.
     */
    private LakeTieringFactory<Object, Object> createTestFactory() {
        return new LakeTieringFactory<Object, Object>() {
            @Override
            public LakeWriter<Object> createLakeWriter(WriterInitContext writerInitContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SimpleVersionedSerializer<Object> getWriteResultSerializer() {
                throw new UnsupportedOperationException();
            }

            @Override
            public LakeCommitter<Object, Object> createLakeCommitter(
                    CommitterInitContext committerInitContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SimpleVersionedSerializer<Object> getCommittableSerializer() {
                throw new UnsupportedOperationException();
            }

            @Override
            public LakeSource<?> createLakeSource(TablePath path) {
                return new PaimonLakeSource(paimonConfig, path);
            }
        };
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

    private FileStoreTable createDvTableWithSystemColumns(Identifier tableIdentifier)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("c1", DataTypes.INT())
                        .column("c2", DataTypes.STRING())
                        .column("__bucket", DataTypes.INT())
                        .column("__offset", DataTypes.BIGINT())
                        .column("__timestamp", DataTypes.TIMESTAMP(6))
                        .column("__rowid", DataTypes.BIGINT())
                        .primaryKey("c1")
                        .option("bucket", "1")
                        .option("deletion-vectors.enabled", "true")
                        .option("changelog-producer", "lookup")
                        .build();
        paimonCatalog.createTable(tableIdentifier, schema, false);
        return (FileStoreTable) paimonCatalog.getTable(tableIdentifier);
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
                            Timestamp.fromEpochMillis(now), // __timestamp
                            (long) i)); // __rowid
        }
        return rows;
    }
}
