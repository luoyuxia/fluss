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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Materializes logical deletion vectors into Iceberg Puffin DV files. */
final class IcebergDeletionVectorMaterializer {

    private IcebergDeletionVectorMaterializer() {}

    static MaterializationResult materialize(
            Table icebergTable,
            int bucketId,
            WriteResult writeResult,
            @Nullable Map<String, byte[]> lakeDvSnapshot)
            throws IOException {
        long baseSnapshotId =
                icebergTable.currentSnapshot() == null
                        ? -1L
                        : icebergTable.currentSnapshot().snapshotId();

        Map<String, DataFileContext> dataFilesByPath =
                collectDataFiles(icebergTable, baseSnapshotId, writeResult.dataFiles());
        Map<String, LinkedHashSet<Long>> deletePositionsByFile = new LinkedHashMap<>();

        if (lakeDvSnapshot != null) {
            for (Map.Entry<String, byte[]> entry : lakeDvSnapshot.entrySet()) {
                if (!dataFilesByPath.containsKey(entry.getKey())) {
                    continue;
                }
                deserializeBitmap(entry.getValue())
                        .forEach(
                                (int pos) ->
                                        deletePositionsByFile
                                                .computeIfAbsent(
                                                        entry.getKey(),
                                                        key -> new LinkedHashSet<>())
                                                .add((long) pos));
            }
        }

        if (deletePositionsByFile.isEmpty()) {
            return new MaterializationResult(writeResult, baseSnapshotId, null);
        }

        OutputFileFactory outputFileFactory =
                OutputFileFactory.builderFor(icebergTable, bucketId, 0)
                        .format(FileFormat.PUFFIN)
                        .suffix("dv")
                        .build();
        BaseDeleteLoader deleteLoader =
                new BaseDeleteLoader(
                        deleteFile -> icebergTable.io().newInputFile(deleteFile.location()));
        BaseDVFileWriter dvFileWriter =
                new BaseDVFileWriter(
                        outputFileFactory,
                        filePath -> {
                            DataFileContext context = dataFilesByPath.get(filePath);
                            if (context == null || context.deleteFiles.isEmpty()) {
                                return PositionDeleteIndex.empty();
                            }
                            return deleteLoader.loadPositionDeletes(context.deleteFiles, filePath);
                        });

        List<String> materializedDvFiles = new ArrayList<>(deletePositionsByFile.size());
        for (Map.Entry<String, LinkedHashSet<Long>> entry : deletePositionsByFile.entrySet()) {
            DataFileContext context = dataFilesByPath.get(entry.getKey());
            if (context == null) {
                continue;
            }
            for (Long position : entry.getValue()) {
                dvFileWriter.delete(entry.getKey(), position, context.spec, context.partition);
            }
            materializedDvFiles.add(entry.getKey());
        }

        dvFileWriter.close();
        DeleteWriteResult deleteWriteResult = dvFileWriter.result();
        WriteResult combinedWriteResult =
                WriteResult.builder()
                        .add(writeResult)
                        .addDeleteFiles(deleteWriteResult.deleteFiles())
                        .addReferencedDataFiles(deleteWriteResult.referencedDataFiles())
                        .addRewrittenDeleteFiles(deleteWriteResult.rewrittenDeleteFiles())
                        .build();
        return new MaterializationResult(combinedWriteResult, baseSnapshotId, materializedDvFiles);
    }

    private static Map<String, DataFileContext> collectDataFiles(
            Table icebergTable, long baseSnapshotId, DataFile[] newlyWrittenDataFiles)
            throws IOException {
        Map<String, DataFileContext> dataFilesByPath = new LinkedHashMap<>();
        if (baseSnapshotId > 0) {
            try (CloseableIterable<FileScanTask> fileScanTasks =
                    icebergTable.newScan().useSnapshot(baseSnapshotId).planFiles()) {
                for (FileScanTask fileScanTask : fileScanTasks) {
                    DataFile dataFile = fileScanTask.file();
                    dataFilesByPath.put(
                            dataFile.location(),
                            new DataFileContext(
                                    icebergTable.specs().get(dataFile.specId()),
                                    dataFile.partition(),
                                    fileScanTask.deletes()));
                }
            }
        }

        for (DataFile dataFile : newlyWrittenDataFiles) {
            dataFilesByPath.put(
                    dataFile.location(),
                    new DataFileContext(
                            icebergTable.specs().get(dataFile.specId()),
                            dataFile.partition(),
                            java.util.Collections.emptyList()));
        }
        return dataFilesByPath;
    }

    private static RoaringBitmap deserializeBitmap(byte[] serializedBitmap) throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        try (DataInputStream dataInputStream =
                new DataInputStream(new ByteArrayInputStream(serializedBitmap))) {
            bitmap.deserialize(dataInputStream);
        }
        return bitmap;
    }

    static final class MaterializationResult {
        private final WriteResult writeResult;
        private final long baseSnapshotId;
        @Nullable private final List<String> materializedDvFiles;

        private MaterializationResult(
                WriteResult writeResult,
                long baseSnapshotId,
                @Nullable List<String> materializedDvFiles) {
            this.writeResult = writeResult;
            this.baseSnapshotId = baseSnapshotId;
            this.materializedDvFiles = materializedDvFiles;
        }

        WriteResult writeResult() {
            return writeResult;
        }

        long baseSnapshotId() {
            return baseSnapshotId;
        }

        @Nullable
        List<String> materializedDvFiles() {
            return materializedDvFiles;
        }
    }

    private static final class DataFileContext {
        private final PartitionSpec spec;
        @Nullable private final StructLike partition;
        private final List<DeleteFile> deleteFiles;

        private DataFileContext(
                PartitionSpec spec, @Nullable StructLike partition, List<DeleteFile> deleteFiles) {
            this.spec = spec;
            this.partition = partition;
            this.deleteFiles = deleteFiles;
        }
    }
}
