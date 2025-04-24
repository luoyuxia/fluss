/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lakehouse.writer.CommitterInitContext;
import com.alibaba.fluss.lakehouse.writer.FileSystemProvider;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.lakehouse.writer.WriterInitContext;
import com.alibaba.fluss.metadata.TableBucket;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.manifest.ManifestCommittable;

import javax.annotation.Nullable;

import java.io.IOException;

/** Implementation of {@link LakeTieringFactory} for Paimon . */
public class PaimonLakeTieringFactory
        implements LakeTieringFactory<PaimonWriteResult, ManifestCommittable> {

    private static final long serialVersionUID = 1L;

    private final PaimonCatalogProvider paimonCatalogProvider;

    public PaimonLakeTieringFactory(Configuration paimonConfig, FileSystemProvider fsProvider) {
        this.paimonCatalogProvider = new PaimonCatalogProvider(paimonConfig, fsProvider);
    }

    @Override
    public LakeWriter<PaimonWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        TableBucket tableBucket = writerInitContext.tableBucket();
        BinaryRow paimonPartition = toPaimonPartition(writerInitContext.partition());
        return new PaimonLakeWriter(
                paimonCatalogProvider,
                writerInitContext.tablePath(),
                paimonPartition,
                tableBucket.getBucket());
    }

    private BinaryRow toPaimonPartition(@Nullable String partition) {
        if (partition == null) {
            return BinaryRow.EMPTY_ROW;
        }
        BinaryRow paimonPartition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(paimonPartition);
        writer.writeString(0, BinaryString.fromString(partition));
        writer.complete();
        return paimonPartition;
    }

    @Override
    public SimpleVersionedSerializer<PaimonWriteResult> getWriteResultSerializer() {
        return new PaimonWriteResultSerializer();
    }

    @Override
    public LakeCommitter<PaimonWriteResult, ManifestCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return new PaimonCommitter(paimonCatalogProvider, committerInitContext.tablePath());
    }

    @Override
    public SimpleVersionedSerializer<ManifestCommittable> getCommitableSerializer() {
        return null;
    }
}
