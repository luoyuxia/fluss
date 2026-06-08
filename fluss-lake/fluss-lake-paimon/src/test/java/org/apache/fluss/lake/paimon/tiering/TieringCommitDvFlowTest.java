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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.flink.adapter.StreamOperatorParametersAdapter;
import org.apache.fluss.flink.tiering.committer.CommittableMessage;
import org.apache.fluss.flink.tiering.committer.TieringCommitOperator;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.event.RowPosRequestEvent;
import org.apache.fluss.flink.tiering.source.TableBucketWriteResult;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.lake.dv.FilePos;
import org.apache.fluss.lake.dv.RowPosSstFileWriter;
import org.apache.fluss.lake.dv.RowPosSstUploader;
import org.apache.fluss.lake.paimon.utils.PaimonTestUtils;
import org.apache.fluss.lake.source.RowPosResult;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.GetDvSnapshotResponse;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link TieringCommitOperator}'s DV scan flow using real Paimon compaction
 * and real {@link PaimonLakeCommitter}.
 *
 * <p>Exercises the full flow (write → compact → write with DV scan):
 *
 * <ul>
 *   <li>Round 1: Tier data through the operator (no compaction yet, no DV scan)
 *   <li>Compact: trigger real Paimon compaction (single L0 → L5, clean state)
 *   <li>Round 2: Tier data again — operator detects compaction via planDelta(), triggers DV scan,
 *       processes RowPos results, and commits with both tiered and readable snapshots
 * </ul>
 *
 * <p>This test uses a dedicated Fluss cluster with datalake enabled (Paimon). The Paimon test data
 * is written to a separate warehouse from the cluster's, so we can use a simple schema without
 * system columns.
 */
class TieringCommitDvFlowTest {

    private static final String DV_DB = "fluss";
    private static final String DV_TABLE = "test_dv_commit";

    /** Warehouse path used by the Fluss cluster's datalake integration. */
    private static String clusterWarehousePath;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initClusterConfig())
                    .setNumOfTabletServers(3)
                    .build();

    private static Configuration initClusterConfig() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_MAX_RETAINED_SNAPSHOTS, Integer.MAX_VALUE);
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        conf.setString("datalake.paimon.metastore", "filesystem");
        try {
            clusterWarehousePath =
                    Files.createTempDirectory("fluss-test-dv-cluster")
                            .resolve("warehouse")
                            .toString();
        } catch (Exception e) {
            throw new FlussRuntimeException("Failed to create cluster warehouse path", e);
        }
        conf.setString("datalake.paimon.warehouse", clusterWarehousePath);
        return conf;
    }

    private static Connection conn;
    private static Admin admin;

    /** Temp directory for the test Paimon warehouse (separate from cluster warehouse). */
    @TempDir private Path tempDir;

    @TempDir private File compactionTempDir;

    private Catalog paimonCatalog;
    private Configuration paimonConfig;
    private TieringCommitOperator<PaimonWriteResult, PaimonCommittable> committerOperator;
    private MockOperatorEventGateway mockOperatorEventGateway;
    private StreamOperatorParameters<CommittableMessage<PaimonCommittable>> parameters;

    @BeforeAll
    static void beforeAll() {
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @BeforeEach
    void beforeEach() throws Exception {
        // Set up Paimon catalog at the test warehouse (separate from cluster's warehouse)
        Map<String, String> options = new HashMap<>();
        options.put("warehouse", tempDir.resolve("warehouse").toString());
        options.put("metastore", "filesystem");
        paimonCatalog =
                CatalogFactory.createCatalog(CatalogContext.create(Options.fromMap(options)));
        paimonCatalog.createDatabase(DV_DB, false);

        paimonConfig = new Configuration();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            paimonConfig.setString(entry.getKey(), entry.getValue());
        }

        // Set up Flink operator parameters
        mockOperatorEventGateway = new MockOperatorEventGateway();
        MockOperatorEventDispatcher mockOperatorEventDispatcher =
                new MockOperatorEventDispatcher(mockOperatorEventGateway);
        // Use a no-serialize output — PaimonCommittable wraps ManifestCommittable which
        // is not deserializable by MockOutput's InstantiationUtil.clone().
        MockOutput<CommittableMessage<PaimonCommittable>> noSerializeOutput =
                new MockOutput<CommittableMessage<PaimonCommittable>>(new ArrayList<>()) {
                    @Override
                    public void collect(
                            StreamRecord<CommittableMessage<PaimonCommittable>> element) {
                        // skip serialization
                    }
                };
        parameters =
                StreamOperatorParametersAdapter.create(
                        new SourceOperatorStreamTask<String>(new DummyEnvironment()),
                        new MockStreamConfig(new org.apache.flink.configuration.Configuration(), 1),
                        noSerializeOutput,
                        null,
                        mockOperatorEventDispatcher,
                        null);
    }

    @AfterEach
    void afterEach() throws Exception {
        if (committerOperator != null) {
            committerOperator.close();
        }
        if (paimonCatalog != null) {
            paimonCatalog.close();
        }
    }

    @Test
    void testDvCommitFlow() throws Exception {
        // ----- Setup: Create Paimon DV-enabled PK table -----
        Identifier paimonTableId = Identifier.create(DV_DB, DV_TABLE);
        org.apache.paimon.schema.Schema paimonSchema =
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("id", org.apache.paimon.types.DataTypes.INT())
                        .column("name", org.apache.paimon.types.DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("deletion-vectors.enabled", "true")
                        .build();
        paimonCatalog.createTable(paimonTableId, paimonSchema, false);
        FileStoreTable paimonTable = (FileStoreTable) paimonCatalog.getTable(paimonTableId);

        // ----- Setup: Create Fluss DV-enabled table -----
        TablePath tablePath = TablePath.of(DV_DB, DV_TABLE);
        TableDescriptor dvDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .primaryKey("id")
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .build())
                        .distributedBy(1, "id")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED, true)
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED, true)
                        .build();
        long tableId = createTable(tablePath, dvDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        // ----- Setup: Create operator with real PaimonLakeTieringFactory -----
        Configuration flussConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        flussConf.set(ConfigOptions.REMOTE_DATA_DIR, FLUSS_CLUSTER_EXTENSION.getRemoteDataDir());

        Configuration dataLakeConfig = new Configuration();
        dataLakeConfig.setString("warehouse", tempDir.resolve("warehouse").toString());
        dataLakeConfig.setString("metastore", "filesystem");

        committerOperator =
                new TieringCommitOperator<>(
                        parameters,
                        flussConf,
                        new Configuration(),
                        new PaimonLakeTieringFactory(paimonConfig),
                        dataLakeConfig,
                        "paimon");
        committerOperator.open();

        // ----- Write Round 1 data to Fluss log (rows 0, 1, 2) -----
        // Real log entries are needed so the tablet server's DV Prepare can scan
        // the log and build logDv/lakeDv during the Readable Switch phase.
        org.apache.fluss.client.table.Table flussTable = conn.getTable(tablePath);
        org.apache.fluss.client.table.writer.UpsertWriter upsertWriter =
                flussTable.newUpsert().createWriter();
        upsertWriter.upsert(flussRow(0, "value0")).get();
        upsertWriter.upsert(flussRow(1, "value1")).get();
        UpsertResult round1Last = upsertWriter.upsert(flussRow(2, "value2")).get();
        long round1LogEndOffset = round1Last.getLogEndOffset();

        // ----- Round 1: Tier rows 0-2 (no compaction yet -> no DV scan) -----
        // Paimon: snapshot 1 (APPEND, single L0 file)
        PaimonWriteResult wr1 = prepareWriteResult(paimonTable, generateRows(0, 3));
        committerOperator.processElement(
                new StreamRecord<>(
                        new TableBucketWriteResult<>(
                                tablePath, tb, null, wr1, round1LogEndOffset, 100L, 1)));
        // No compaction -> operator commits directly, sends FinishedTieringEvent
        assertLastEventIs(FinishedTieringEvent.class);

        // Verify: Fluss tiered snapshot 1 exists with round1LogEndOffset
        LakeSnapshot afterRound1 = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(afterRound1.getSnapshotId()).isEqualTo(1L);
        assertThat(afterRound1.getTableBucketsOffset())
                .isEqualTo(Collections.singletonMap(tb, round1LogEndOffset));

        // ----- Compact: single L0 -> L5, clean state with no L0 files -----
        // Following PaimonLakeSourceDvTest pattern: compact after each write to ensure
        // each compaction handles only one L0 file (Paimon lookup compaction limitation).
        // Paimon: snapshot 2 (COMPACT, all files at L5)
        FileStoreTable latestTable = (FileStoreTable) paimonCatalog.getTable(paimonTableId);
        compact(latestTable, 0);

        assertThat(latestTable.snapshotManager().latestSnapshotId()).isEqualTo(2L);
        assertThat(latestTable.snapshotManager().tryGetSnapshot(2).commitKind())
                .isEqualTo(org.apache.paimon.Snapshot.CommitKind.COMPACT);

        // Get actual Paimon data file name from compacted snapshot 2.
        // This name must match what planDelta returns as deletedFiles in Round 3.
        String round1PaimonFileName = getFirstDataFileName(latestTable, 2L, 0);

        // ----- Write Round 2 data to Fluss log (rows 2, 3, 4, 5 + update row 3) -----
        // Row id=2 overlaps with Round 1. The KV tablet's handleChangelogSynced marks
        // the old +I's log offset in logDv and creates a PendingDeletes entry. During
        // the later Readable Switch, batchResolvePendingDeletes resolves this entry
        // against RowPosIndex and marks the corresponding Paimon file position in lakeDv.
        upsertWriter.upsert(flussRow(2, "value2_updated")).get();
        UpsertResult r3First = upsertWriter.upsert(flussRow(3, "value3")).get();
        upsertWriter.upsert(flussRow(4, "value4")).get();
        upsertWriter.upsert(flussRow(5, "value5")).get();
        // Update row id=3 again so the first id=3 write (at offset >= readableOffset)
        // is superseded. handleChangelogSynced marks the old offset in logDv, and since
        // it is >= readableOffset it survives cleanup during handleReadableSwitch.
        UpsertResult round2Last = upsertWriter.upsert(flussRow(3, "value3_updated")).get();
        long round2LogEndOffset = round2Last.getLogEndOffset();
        // The offset of the first id=3 write that is now superseded
        long row3SupersededOffset = r3First.getLogEndOffset() - 1;

        // ----- Round 2: Tier rows 2-5 -> DV scan triggered -----
        // planDelta(1) detects COMPACT snapshot 2, triggers RowPos scan.
        // Paimon: snapshot 3 (APPEND) will be created during deferred commit.
        // Write the same records as the Fluss log so Paimon compaction produces
        // correct values (matches real TieringWriter behavior).
        List<GenericRow> round2PaimonRows =
                Arrays.asList(
                        GenericRow.of(2, BinaryString.fromString("value2_updated")),
                        GenericRow.of(3, BinaryString.fromString("value3")),
                        GenericRow.of(4, BinaryString.fromString("value4")),
                        GenericRow.of(5, BinaryString.fromString("value5")),
                        GenericRow.of(3, BinaryString.fromString("value3_updated")));
        PaimonWriteResult wr2 = prepareWriteResult(paimonTable, round2PaimonRows);
        committerOperator.processElement(
                new StreamRecord<>(
                        new TableBucketWriteResult<>(
                                tablePath, tb, null, wr2, round2LogEndOffset, 200L, 1)));

        // planDelta(1) detects COMPACT snapshot 2 -> RowPosRequestEvent sent, commit deferred
        RowPosRequestEvent rowPosEvent = assertLastEventIs(RowPosRequestEvent.class);
        assertThat(rowPosEvent.getTableId()).isEqualTo(tableId);

        // Tiered snapshot still 1 (round 2 not committed yet)
        LakeSnapshot beforeRowPos = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(beforeRowPos.getSnapshotId()).isEqualTo(1L);

        // ----- Create and upload real SST file (simulating what the Reader does) -----
        // The SST maps log offsets of rows in the compacted Paimon data to Paimon file
        // positions. Row id=0 was at log offset 0, id=1 at offset 1, id=2 at offset 2.
        // fileId=0 is the first allocated ID (operator starts nextFileId from 0).
        long compactSnapshotId = 2L;
        String remoteDataDir = FLUSS_CLUSTER_EXTENSION.getRemoteDataDir();
        FsPath remoteLakeDir =
                FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId);
        RowPosSstUploader uploader = new RowPosSstUploader(remoteLakeDir);

        // Write a real RocksDB SST with RowId(=logOffset) -> FilePos entries
        File localSstDir = Files.createTempDirectory("sst-upload").toFile();
        List<RowPosSstFileWriter.SstFileMeta> sstMetas;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir.getPath())) {
            sstMetas =
                    writer.write(
                            Arrays.asList(
                                    new RowPosSstFileWriter.RowPosEntry(0L, new FilePos(0, 0L)),
                                    new RowPosSstFileWriter.RowPosEntry(1L, new FilePos(0, 1L)),
                                    new RowPosSstFileWriter.RowPosEntry(2L, new FilePos(0, 2L))));
        }
        assertThat(sstMetas).hasSize(1);

        // Upload SST file to remote (Reader's job; index.json is written by the operator)
        uploader.uploadBucketSsts(
                compactSnapshotId,
                null,
                0,
                new RowPosSstUploader.BucketSstData(localSstDir.getPath(), sstMetas));

        // ----- Send RowPos result -> deferred commit completes -----
        List<RowPosResult.SstMeta> rowPosSstMetas = new ArrayList<>();
        for (RowPosSstFileWriter.SstFileMeta meta : sstMetas) {
            rowPosSstMetas.add(new RowPosResult.SstMeta(meta.getFileName(), meta.getFileSize()));
        }
        RowPosResult rowPos =
                new RowPosResult(
                        0,
                        null,
                        null,
                        Collections.singletonMap(0, round1PaimonFileName),
                        rowPosSstMetas);
        committerOperator.processElement(
                new StreamRecord<>(
                        new TableBucketWriteResult<>(tablePath, tb, null, null, 0, 0, 0, rowPos)));
        assertLastEventIs(FinishedTieringEvent.class);

        // ----- Verify: tiered snapshot -----
        // Paimon snapshot 3 (APPEND), offset = round2LogEndOffset
        CommonTestUtils.retry(
                Duration.ofSeconds(30),
                () -> {
                    LakeSnapshot tieredSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
                    assertThat(tieredSnapshot.getSnapshotId()).isEqualTo(3L);
                    assertThat(tieredSnapshot.getTableBucketsOffset())
                            .isEqualTo(Collections.singletonMap(tb, round2LogEndOffset));
                });

        // ----- Verify: readable snapshot -----
        // Paimon snapshot 2 (COMPACT), offset = round1LogEndOffset.
        // The DV Prepare → mark readable flow is async (coordinator sends DvPrepare to
        // tablet servers, waits for ACKs, then marks snapshot readable in ZK).
        CommonTestUtils.retry(
                Duration.ofSeconds(30),
                () -> {
                    LakeSnapshot readableSnapshot = admin.getReadableLakeSnapshot(tablePath).get();
                    assertThat(readableSnapshot.getSnapshotId()).isEqualTo(2L);
                    assertThat(readableSnapshot.getTableBucketsOffset())
                            .isEqualTo(Collections.singletonMap(tb, round1LogEndOffset));
                });

        // ----- Verify: DV snapshot for overlapping row id=2 -----
        // After DV Prepare + Readable Switch, the tablet server should have:
        // - lakeDv: the L5 data file with position 2 marked as deleted, because
        //   row id=2's old +I (at log offset 2) was resolved via RowPosIndex → FilePos(0, 2)
        // - logDv: empty — the superseded log offset 2 < readableOffset, so it was
        //   cleaned up during handleReadableSwitch
        CommonTestUtils.retry(
                Duration.ofSeconds(30),
                () -> {
                    GetDvSnapshotResponse dvResp =
                            admin.getDvSnapshot(tablePath, tableId, null, 0, 2L).get();
                    assertThat(dvResp.getSnapshotStartOffset()).isEqualTo(round1LogEndOffset);

                    // lakeDv: the L5 data file should have position 2 marked as deleted.
                    // Row id=2 was originally at log offset 2 → SST maps it to FilePos(0, 2).
                    // During handleReadableSwitch, batchResolvePendingDeletes resolved this
                    // entry and marked file_id=0 / position=2 in lakeDv.
                    assertThat(dvResp.getLakeDvEntriesCount()).isEqualTo(1);
                    assertThat(dvResp.getLakeDvEntryAt(0).getFilePath())
                            .isEqualTo(round1PaimonFileName);
                    byte[] bitmapBytes = dvResp.getLakeDvEntryAt(0).getDeletedPositionsBitmap();
                    assertThat(bitmapBytes).isNotNull();
                    Roaring64Bitmap lakeBitmap = new Roaring64Bitmap();
                    lakeBitmap.deserialize(java.nio.ByteBuffer.wrap(bitmapBytes));
                    // Only position 2 should be deleted (row id=2 overlap)
                    assertThat(lakeBitmap.getLongCardinality()).isEqualTo(1L);
                    assertThat(lakeBitmap.contains(2L)).isTrue();

                    // logDv: row id=3's first write (at row3SupersededOffset) was
                    // superseded by the second write. Since that offset >= readableOffset,
                    // it survives cleanup and appears in the logDv bitmap.
                    // (Row id=2's old offset 2 < readableOffset, so it was cleaned up.)
                    assertThat(dvResp.getLogDvBitmap()).isNotNull();
                    Roaring64Bitmap logBitmap = new Roaring64Bitmap();
                    logBitmap.deserialize(java.nio.ByteBuffer.wrap(dvResp.getLogDvBitmap()));
                    assertThat(logBitmap.getLongCardinality()).isEqualTo(1L);
                    assertThat(logBitmap.contains(row3SupersededOffset)).isTrue();
                });

        // ----- Round 3: Second compaction → oldFiles cleanup -----
        // Compact the L0 from snapshot 3 (rows 2-5) → snapshot 4 (COMPACT).
        // The previous APPEND before this COMPACT is snapshot 3, which IS registered
        // as a Fluss lake snapshot. This is required by DvTableReadableSnapshotRetriever
        // which needs to look up tiered offsets for the previous APPEND snapshot.
        latestTable = (FileStoreTable) paimonCatalog.getTable(paimonTableId);
        compact(latestTable, 0);

        assertThat(latestTable.snapshotManager().latestSnapshotId()).isEqualTo(4L);
        assertThat(latestTable.snapshotManager().tryGetSnapshot(4).commitKind())
                .isEqualTo(org.apache.paimon.Snapshot.CommitKind.COMPACT);

        // Get actual Paimon data file name from compacted snapshot 4 (new compacted file).
        String round3PaimonFileName = getFirstDataFileName(latestTable, 4L, 0);

        // ----- Write Round 3 data to Fluss log (rows 6, 7) -----
        upsertWriter.upsert(flussRow(6, "value6")).get();
        UpsertResult round3Last = upsertWriter.upsert(flussRow(7, "value7")).get();
        long round3LogEndOffset = round3Last.getLogEndOffset();

        // ----- Round 3: Tier rows 6-7 → planDelta(3) detects COMPACT snapshot 4 -----
        // DIFF between snapshot 3 and snapshot 4:
        //   dataFiles: new L5 files (compaction output, rows 0-5)
        //   beforeFiles: old L5 from snapshot 2 + old L0 from snapshot 3
        // The old L5 files become deletedFiles/oldFiles.
        PaimonWriteResult wr3 = prepareWriteResult(paimonTable, generateRows(6, 8));
        committerOperator.processElement(
                new StreamRecord<>(
                        new TableBucketWriteResult<>(
                                tablePath, tb, null, wr3, round3LogEndOffset, 300L, 1)));

        // planDelta(3) detects COMPACT snapshot 4 → RowPosRequestEvent sent, commit deferred
        RowPosRequestEvent rowPosEvent3 = assertLastEventIs(RowPosRequestEvent.class);
        assertThat(rowPosEvent3.getTableId()).isEqualTo(tableId);

        // ----- Create and upload SST for Round 3 compacted files -----
        // The compacted file uses fileId=1 (Round 1 used fileId=0; the operator tracks
        // nextFileId and increments it for each new compacted file).
        // Map the original log offsets (0, 1, 2) to positions in the new file.
        long compactSnapshotId3 = 4L;
        File localSstDir3 = Files.createTempDirectory("sst-upload-3").toFile();
        List<RowPosSstFileWriter.SstFileMeta> sstMetas3;
        try (RowPosSstFileWriter writer = new RowPosSstFileWriter(localSstDir3.getPath())) {
            List<RowPosSstFileWriter.RowPosEntry> entries3 = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                entries3.add(
                        new RowPosSstFileWriter.RowPosEntry((long) i, new FilePos(1, (long) i)));
            }
            sstMetas3 = writer.write(entries3);
        }

        uploader.uploadBucketSsts(
                compactSnapshotId3,
                null,
                0,
                new RowPosSstUploader.BucketSstData(localSstDir3.getPath(), sstMetas3));

        // ----- Send RowPos result → deferred commit completes -----
        List<RowPosResult.SstMeta> rowPosSstMetas3 = new ArrayList<>();
        for (RowPosSstFileWriter.SstFileMeta meta : sstMetas3) {
            rowPosSstMetas3.add(new RowPosResult.SstMeta(meta.getFileName(), meta.getFileSize()));
        }
        RowPosResult rowPos3 =
                new RowPosResult(
                        0,
                        null,
                        null,
                        Collections.singletonMap(1, round3PaimonFileName),
                        rowPosSstMetas3);
        committerOperator.processElement(
                new StreamRecord<>(
                        new TableBucketWriteResult<>(tablePath, tb, null, null, 0, 0, 0, rowPos3)));
        assertLastEventIs(FinishedTieringEvent.class);

        // ----- Verify: tiered snapshot advances to 5 (APPEND) -----
        CommonTestUtils.retry(
                Duration.ofSeconds(30),
                () -> {
                    LakeSnapshot tieredSnapshot3 = admin.getLatestLakeSnapshot(tablePath).get();
                    assertThat(tieredSnapshot3.getSnapshotId()).isEqualTo(5L);
                    assertThat(tieredSnapshot3.getTableBucketsOffset())
                            .isEqualTo(Collections.singletonMap(tb, round3LogEndOffset));
                });

        // ----- Verify: readable snapshot advances to 4 (COMPACT) -----
        CommonTestUtils.retry(
                Duration.ofSeconds(30),
                () -> {
                    LakeSnapshot readableSnapshot3 = admin.getReadableLakeSnapshot(tablePath).get();
                    assertThat(readableSnapshot3.getSnapshotId()).isEqualTo(4L);
                    assertThat(readableSnapshot3.getTableBucketsOffset())
                            .isEqualTo(Collections.singletonMap(tb, round2LogEndOffset));
                });

        // ----- Verify: DV snapshot after Round 3 — old file cleaned up -----
        // After DV Prepare + Readable Switch for snapshot 4:
        // - round1PaimonFileName (fileId=0) was in deletedFiles from planDelta(3)
        // - cleanupOldFiles() should have removed:
        //   * LakeDv entries for fileId=0 (the position-2 deletion from Round 2)
        //   * PendingDeletes entries pointing to fileId=0
        //   * FileDict entries for fileId=0 (this PR)
        // - round3PaimonFileName (fileId=1) is the new compacted file
        // - row3SupersededOffset < round2LogEndOffset → cleaned up from logDv
        CommonTestUtils.retry(
                Duration.ofSeconds(30),
                () -> {
                    GetDvSnapshotResponse dvResp3 =
                            admin.getDvSnapshot(tablePath, tableId, null, 0, 4L).get();
                    assertThat(dvResp3.getSnapshotStartOffset()).isEqualTo(round2LogEndOffset);

                    // lakeDv: should be empty. The only entry (fileId=0, position 2)
                    // was cleaned up by cleanupOldFiles. Rows 6-7 are new inserts
                    // with no overlapping lake data, so no new lakeDv entries.
                    assertThat(dvResp3.getLakeDvEntriesCount()).isEqualTo(0);

                    // logDv: should be empty. row3SupersededOffset < round2LogEndOffset
                    // was cleaned up during handleReadableSwitch. Rows 6-7 have no
                    // superseded writes, so no new logDv entries.
                    byte[] logDvBytes3 = dvResp3.getLogDvBitmap();
                    if (logDvBytes3 != null && logDvBytes3.length > 0) {
                        Roaring64Bitmap logBitmap3 = new Roaring64Bitmap();
                        logBitmap3.deserialize(java.nio.ByteBuffer.wrap(logDvBytes3));
                        assertThat(logBitmap3.getLongCardinality()).isEqualTo(0L);
                    }
                });

        flussTable.close();
    }

    // ---- helper methods ----

    private long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, true).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    /**
     * Writes rows to the Paimon table in write-only mode (skips lookup which requires IOManager)
     * and returns the uncommitted PaimonWriteResult. The operator's PaimonLakeCommitter will commit
     * this result into a new Paimon snapshot.
     */
    private PaimonWriteResult prepareWriteResult(FileStoreTable table, List<GenericRow> rows)
            throws Exception {
        Map<String, String> writeOnlyOpts = new HashMap<>();
        writeOnlyOpts.put(CoreOptions.WRITE_ONLY.key(), "true");
        FileStoreTable writeOnlyTable = table.copy(writeOnlyOpts);
        try (BatchTableWrite batchWrite = writeOnlyTable.newBatchWriteBuilder().newWrite()) {
            for (GenericRow row : rows) {
                batchWrite.write(row, 0);
            }
            List<CommitMessage> commitMessages = batchWrite.prepareCommit();
            assertThat(commitMessages).isNotEmpty();
            return new PaimonWriteResult(commitMessages.get(0));
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T assertLastEventIs(Class<T> expectedClass) {
        List<OperatorEvent> events = mockOperatorEventGateway.getEventsSent();
        assertThat(events).isNotEmpty();
        SourceEventWrapper lastWrapper = (SourceEventWrapper) events.get(events.size() - 1);
        if (lastWrapper.getSourceEvent() instanceof FailedTieringEvent) {
            FailedTieringEvent event = (FailedTieringEvent) lastWrapper.getSourceEvent();
            System.out.println(event.failReason());
        }
        assertThat(lastWrapper.getSourceEvent()).isInstanceOf(expectedClass);
        return (T) lastWrapper.getSourceEvent();
    }

    /** Creates a Fluss InternalRow for the (id INT, name STRING) schema. */
    private static org.apache.fluss.row.InternalRow flussRow(int id, String name) {
        return org.apache.fluss.row.GenericRow.of(
                id, org.apache.fluss.row.BinaryString.fromString(name));
    }

    private static List<GenericRow> generateRows(int from, int to) {
        List<GenericRow> rows = new ArrayList<>();
        for (int i = from; i < to; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("value" + i)));
        }
        return rows;
    }

    /** Returns the first data file name from the given Paimon snapshot for the specified bucket. */
    private static String getFirstDataFileName(FileStoreTable table, long snapshotId, int bucket) {
        Map<String, String> scanOpts = new HashMap<>();
        scanOpts.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId));
        FileStoreTable scanTable = table.copy(scanOpts);
        for (org.apache.paimon.table.source.Split split : scanTable.newScan().plan().splits()) {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.bucket() == bucket && !dataSplit.dataFiles().isEmpty()) {
                return dataSplit.dataFiles().get(0).fileName();
            }
        }
        throw new AssertionError(
                "No data file found for snapshot " + snapshotId + " bucket " + bucket);
    }

    /** Compacts the specified bucket using {@link PaimonTestUtils.CompactHelper}. */
    private void compact(FileStoreTable table, int bucket) throws Exception {
        new org.apache.fluss.lake.paimon.utils.PaimonTestUtils.CompactHelper(
                        table, compactionTempDir)
                .compactBucket(bucket)
                .commit();
    }

    // ---- inner classes ----

    /** Mock {@link OperatorEventDispatcher} for testing. */
    private static class MockOperatorEventDispatcher implements OperatorEventDispatcher {

        private final OperatorEventGateway operatorEventGateway;

        MockOperatorEventDispatcher(MockOperatorEventGateway mockOperatorEventGateway) {
            this.operatorEventGateway = mockOperatorEventGateway;
        }

        @Override
        public void registerEventHandler(
                OperatorID operatorID, OperatorEventHandler operatorEventHandler) {
            // no-op
        }

        @Override
        public OperatorEventGateway getOperatorEventGateway(OperatorID operatorID) {
            return operatorEventGateway;
        }
    }
}
