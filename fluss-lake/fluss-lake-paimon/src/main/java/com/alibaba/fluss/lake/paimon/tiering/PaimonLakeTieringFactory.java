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
import com.alibaba.fluss.lakehouse.committer.CommitterInitContext;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.serializer.SimpleVersionedSerializer;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.lakehouse.writer.WriterInitContext;

import java.io.IOException;

/** Implementation of {@link LakeTieringFactory} for Paimon . */
public class PaimonLakeTieringFactory
        implements LakeTieringFactory<PaimonWriteResult, PaimonCommittable> {

    public static final String FLUSS_LAKE_TIERING_COMMIT_USER = "fluss_lake_tiering_committer";

    private static final long serialVersionUID = 1L;

    private final PaimonCatalogProvider paimonCatalogProvider;

    public PaimonLakeTieringFactory(Configuration paimonConfig) {
        this.paimonCatalogProvider = new PaimonCatalogProvider(paimonConfig);
    }

    @Override
    public LakeWriter<PaimonWriteResult> createLakeWriter(WriterInitContext writerInitContext)
            throws IOException {
        return new PaimonLakeWriter(paimonCatalogProvider, writerInitContext);
    }

    @Override
    public SimpleVersionedSerializer<PaimonWriteResult> getWriteResultSerializer() {
        return new PaimonWriteResultSerializer();
    }

    @Override
    public LakeCommitter<PaimonWriteResult, PaimonCommittable> createLakeCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return new PaimonLakeCommitter(paimonCatalogProvider, committerInitContext.tablePath());
    }

    @Override
    public SimpleVersionedSerializer<PaimonCommittable> getCommitableSerializer() {
        return new PaimonCommittableSerializer();
    }
}
