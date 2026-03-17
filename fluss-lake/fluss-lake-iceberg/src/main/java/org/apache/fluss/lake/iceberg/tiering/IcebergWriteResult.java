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
import org.apache.fluss.lake.writer.PositionReportableWriteResult;

import org.apache.iceberg.io.WriteResult;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** The write result of Iceberg lake writer to pass to committer to commit. */
public class IcebergWriteResult implements Serializable, PositionReportableWriteResult {

    private static final long serialVersionUID = 1L;

    private final WriteResult writeResult;
    @Nullable private final RewriteDataFileResult rewriteDataFileResult;
    @Nullable private final Map<String, List<long[]>> positionReport;
    private final long baseSnapshotId;
    @Nullable private final List<String> materializedDvFiles;

    public IcebergWriteResult(
            WriteResult writeResult, @Nullable RewriteDataFileResult rewriteDataFileResult) {
        this(writeResult, rewriteDataFileResult, null, -1L, null);
    }

    public IcebergWriteResult(
            WriteResult writeResult,
            @Nullable RewriteDataFileResult rewriteDataFileResult,
            @Nullable Map<String, List<long[]>> positionReport,
            long baseSnapshotId) {
        this(writeResult, rewriteDataFileResult, positionReport, baseSnapshotId, null);
    }

    public IcebergWriteResult(
            WriteResult writeResult,
            @Nullable RewriteDataFileResult rewriteDataFileResult,
            @Nullable Map<String, List<long[]>> positionReport,
            long baseSnapshotId,
            @Nullable List<String> materializedDvFiles) {
        this.writeResult = writeResult;
        this.rewriteDataFileResult = rewriteDataFileResult;
        this.positionReport = positionReport;
        this.baseSnapshotId = baseSnapshotId;
        this.materializedDvFiles = materializedDvFiles;
    }

    public WriteResult getWriteResult() {
        return writeResult;
    }

    @Nullable
    public RewriteDataFileResult rewriteDataFileResult() {
        return rewriteDataFileResult;
    }

    @Override
    @Nullable
    public Map<String, List<long[]>> getPositionReport() {
        return positionReport;
    }

    public long getBaseSnapshotId() {
        return baseSnapshotId;
    }

    @Override
    @Nullable
    public List<String> getMaterializedDvFiles() {
        return materializedDvFiles;
    }

    @Override
    public String toString() {
        return "IcebergWriteResult{"
                + "dataFiles="
                + writeResult.dataFiles().length
                + ", deleteFiles="
                + writeResult.deleteFiles().length
                + (rewriteDataFileResult != null
                        ? (", rewriteDataFiles=" + rewriteDataFileResult)
                        : "")
                + (positionReport != null
                        ? (", dvPositionReportSize=" + positionReport.size())
                        : "")
                + (materializedDvFiles != null
                        ? (", materializedDvFiles=" + materializedDvFiles.size())
                        : "")
                + '}';
    }
}
