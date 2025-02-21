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

package com.alibaba.fluss.lake.paimon;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lake.LakeCatalog;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;

import java.util.List;

/** A Paimon implementation of {@link LakeCatalog}. */
public class PaimonLakeCatalog implements LakeCatalog {

    private final Catalog paimonCatalog;

    public PaimonLakeCatalog(Configuration configuration) {
        this.paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(configuration.toMap())));
    }

    @Override
    public void createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws TableAlreadyExistException {
        // then, create the table
        Identifier paimonPath = toPaimonIdentifier(tablePath);
        Schema paimonSchema = toPaimonSchema(tableDescriptor);
        try {
            // not ignore if table exists
            paimonCatalog.createTable(paimonPath, paimonSchema, false);
        } catch (Catalog.DatabaseNotExistException e) {
            // create database
            createDatabase(tablePath.getDatabaseName());
        } catch (Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
        }
    }

    private void createDatabase(String databaseName) {
        try {
            // ignore if exists
            paimonCatalog.createDatabase(databaseName, true);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            // do nothing, shouldn't throw since ignoreIfExists
        }
    }

    private Identifier toPaimonIdentifier(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private Schema toPaimonSchema(TableDescriptor tableDescriptor) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        Options options = new Options();

        // When bucket key is undefined, it should use dynamic bucket (bucket = -1) mode.
        List<String> bucketKeys = tableDescriptor.getBucketKeys();
        if (bucketKeys != null) {
            int numBuckets =
                    tableDescriptor
                            .getTableDistribution()
                            .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Bucket count should be set."));
            options.set(CoreOptions.BUCKET, numBuckets);
            options.set(CoreOptions.BUCKET_KEY, String.join(",", bucketKeys));
        } else {
            options.set(CoreOptions.BUCKET, CoreOptions.BUCKET.defaultValue());
        }

        // set schema
        for (com.alibaba.fluss.metadata.Schema.Column column :
                tableDescriptor.getSchema().getColumns()) {
            schemaBuilder.column(
                    column.getName(),
                    column.getDataType().accept(FlussDataTypeToPaimonDataType.INSTANCE),
                    column.getComment().orElse(null));
        }

        // set pk
        if (tableDescriptor.getSchema().getPrimaryKey().isPresent()) {
            schemaBuilder.primaryKey(
                    tableDescriptor.getSchema().getPrimaryKey().get().getColumnNames());
            options.set(
                    CoreOptions.CHANGELOG_PRODUCER.key(),
                    CoreOptions.ChangelogProducer.INPUT.toString());
        }
        // set partition keys
        schemaBuilder.partitionKeys(tableDescriptor.getPartitionKeys());

        // set custom properties to paimon schema
        tableDescriptor.getCustomProperties().forEach(options::set);
        schemaBuilder.options(options.toMap());
        return schemaBuilder.build();
    }

    @Override
    public void close() throws Exception {
        paimonCatalog.close();
    }
}
