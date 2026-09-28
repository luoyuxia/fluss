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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.paimon.tiering.markdone.PaimonPartitionMarkDone;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.lake.writer.SupportsPartitionMarkDone;
import org.apache.fluss.lake.writer.WriterInitContext;
import org.apache.fluss.metadata.TableInfo;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon;

/** Implementation of {@link LakeTieringFactory} for Paimon . */
public class PaimonLakeTieringFactory
        implements SupportsPartitionMarkDone<PaimonWriteResult, PaimonCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonLakeTieringFactory.class);

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
    public SupportsPartitionMarkDone.Committer<PaimonWriteResult, PaimonCommittable>
            createLakeCommitter(CommitterInitContext committerInitContext) throws IOException {
        return new PaimonLakeCommitter(paimonCatalogProvider, committerInitContext);
    }

    @Override
    public SimpleVersionedSerializer<PaimonCommittable> getCommittableSerializer() {
        return new PaimonCommittableSerializer();
    }

    @Override
    public boolean isPartitionMarkDoneEnabled(TableInfo tableInfo) {
        if (!tableInfo.isPartitioned()) {
            return false;
        }
        try (Catalog catalog = paimonCatalogProvider.get()) {
            FileStoreTable table =
                    (FileStoreTable) catalog.getTable(toPaimon(tableInfo.getLakeTablePath()));
            return PaimonPartitionMarkDone.isEnabled(table, tableInfo);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to check partition mark-done for table {}, skipping this round.",
                    tableInfo.getTablePath(),
                    e);
            return false;
        }
    }
}
