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

import org.apache.fluss.lake.iceberg.maintenance.RewriteDataFileResult;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.DeleteFileSet;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The committable that derived from {@link IcebergWriteResult} to commit to Iceberg. */
public class IcebergCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataFileSet dataFiles;
    private final DeleteFileSet deleteFiles;
    private final DeleteFileSet rewrittenDeleteFiles;
    private final List<RewriteDataFileResult> rewriteDataFiles;
    @Nullable private final Map<String, List<long[]>> positionReport;
    private final long baseSnapshotId;
    @Nullable private final List<String> materializedDvFiles;

    private IcebergCommittable(
            DataFileSet dataFiles,
            DeleteFileSet deleteFiles,
            DeleteFileSet rewrittenDeleteFiles,
            List<RewriteDataFileResult> rewriteDataFiles,
            @Nullable Map<String, List<long[]>> positionReport,
            long baseSnapshotId,
            @Nullable List<String> materializedDvFiles) {
        this.dataFiles = dataFiles;
        this.deleteFiles = deleteFiles;
        this.rewrittenDeleteFiles = rewrittenDeleteFiles;
        this.rewriteDataFiles = rewriteDataFiles;
        this.positionReport = positionReport;
        this.baseSnapshotId = baseSnapshotId;
        this.materializedDvFiles = materializedDvFiles;
    }

    public DataFileSet getDataFiles() {
        return dataFiles;
    }

    public DeleteFileSet getDeleteFiles() {
        return deleteFiles;
    }

    public DeleteFileSet getRewrittenDeleteFiles() {
        return rewrittenDeleteFiles;
    }

    public List<RewriteDataFileResult> rewriteDataFileResults() {
        return rewriteDataFiles;
    }

    @Nullable
    public Map<String, List<long[]>> getPositionReport() {
        return positionReport;
    }

    public long getBaseSnapshotId() {
        return baseSnapshotId;
    }

    @Nullable
    public List<String> getMaterializedDvFiles() {
        return materializedDvFiles;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link IcebergCommittable}. */
    public static class Builder {
        private final DataFileSet dataFiles = DataFileSet.create();
        private final DeleteFileSet deleteFiles = DeleteFileSet.create();
        private final DeleteFileSet rewrittenDeleteFiles = DeleteFileSet.create();
        private final List<RewriteDataFileResult> rewriteDataFileResults = new ArrayList<>();
        @Nullable private Map<String, List<long[]>> positionReport;
        private long baseSnapshotId = -1;
        @Nullable private List<String> materializedDvFiles;

        public Builder addDataFile(DataFile dataFile) {
            this.dataFiles.add(dataFile);
            return this;
        }

        public Builder addDeleteFile(DeleteFile deleteFile) {
            this.deleteFiles.add(deleteFile);
            return this;
        }

        public Builder addRewrittenDeleteFile(DeleteFile deleteFile) {
            this.rewrittenDeleteFiles.add(deleteFile);
            return this;
        }

        public Builder addRewriteDataFileResult(RewriteDataFileResult rewriteDataFileResult) {
            this.rewriteDataFileResults.add(rewriteDataFileResult);
            return this;
        }

        public Builder positionReport(@Nullable Map<String, List<long[]>> positionReport) {
            this.positionReport = positionReport;
            return this;
        }

        public Builder addPositionReportEntry(String filePath, List<long[]> positions) {
            if (this.positionReport == null) {
                this.positionReport = new HashMap<>();
            }
            this.positionReport
                    .computeIfAbsent(filePath, key -> new ArrayList<>())
                    .addAll(positions);
            return this;
        }

        public Builder baseSnapshotId(long baseSnapshotId) {
            this.baseSnapshotId = baseSnapshotId;
            return this;
        }

        public Builder materializedDvFiles(@Nullable List<String> materializedDvFiles) {
            this.materializedDvFiles = materializedDvFiles;
            return this;
        }

        public Builder addMaterializedDvFile(String dvFilePath) {
            if (this.materializedDvFiles == null) {
                this.materializedDvFiles = new ArrayList<>();
            }
            this.materializedDvFiles.add(dvFilePath);
            return this;
        }

        public IcebergCommittable build() {
            return new IcebergCommittable(
                    dataFiles,
                    deleteFiles,
                    rewrittenDeleteFiles,
                    rewriteDataFileResults,
                    positionReport,
                    baseSnapshotId,
                    materializedDvFiles);
        }
    }

    @Override
    public String toString() {
        return "IcebergCommittable{"
                + "dataFiles="
                + dataFiles
                + ", deleteFiles="
                + deleteFiles
                + ", rewrittenDeleteFiles="
                + rewrittenDeleteFiles
                + ", rewriteDataFiles="
                + rewriteDataFiles
                + ", positionReport="
                + positionReport
                + ", baseSnapshotId="
                + baseSnapshotId
                + ", materializedDvFiles="
                + materializedDvFiles
                + '}';
    }
}
