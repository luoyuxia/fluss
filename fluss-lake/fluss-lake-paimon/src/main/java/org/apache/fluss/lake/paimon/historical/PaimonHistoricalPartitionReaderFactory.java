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

package org.apache.fluss.lake.paimon.historical;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.lake.LakeHistoricalPartitionReader;
import org.apache.fluss.server.lake.LakeHistoricalPartitionReaderFactory;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Factory for creating {@link PaimonHistoricalPartitionReader} instances.
 *
 * <p>This factory creates readers for historical partitions in Paimon tables.
 */
public class PaimonHistoricalPartitionReaderFactory implements LakeHistoricalPartitionReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonHistoricalPartitionReaderFactory.class);

    private final Catalog paimonCatalog;

    public PaimonHistoricalPartitionReaderFactory(Catalog paimonCatalog) {
        this.paimonCatalog = paimonCatalog;
    }

    @Override
    @Nullable
    public LakeHistoricalPartitionReader createReader(TablePath tablePath, Configuration conf) {
        try {
            // Create Paimon handler for this table
            PaimonHistoricalPartitionHandler paimonHandler = new PaimonHistoricalPartitionHandler(
                    paimonCatalog, tablePath);
            return new PaimonHistoricalPartitionReader(paimonHandler);
        } catch (Exception e) {
            LOG.warn("Failed to create Paimon historical partition reader for table {}", tablePath, e);
            return null;
        }
    }

    @Override
    public boolean supports(TablePath tablePath) {
        try {
            // Check if table exists in Paimon catalog
            return paimonCatalog.tableExists(org.apache.fluss.lake.paimon.utils.PaimonConversions.toPaimon(tablePath));
        } catch (Exception e) {
            LOG.warn("Failed to check if table {} exists in Paimon", tablePath, e);
            return false;
        }
    }
}