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

import com.alibaba.fluss.client.lakehouse.paimon.FlussDataTypeToPaimonDataType;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.DatabaseAlreadyExistException;
import com.alibaba.fluss.exception.DatabaseNotExistException;
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lake.MetadataLakeApplier;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;

/** . */
public class PaimonMetadataLakeApplier implements MetadataLakeApplier {

    private final Catalog paimonCatalog;

    public PaimonMetadataLakeApplier(Configuration configuration) {
        this.paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(configuration.toMap())));
    }

    @Override
    public void applyDatabaseCreated(String databaseName, DatabaseDescriptor databaseDescriptor)
            throws DatabaseAlreadyExistException {
        // not ignore if database exists
        try {
            paimonCatalog.createDatabase(databaseName, false);
        } catch (Catalog.DatabaseAlreadyExistException e) {
            throw new DatabaseAlreadyExistException(
                    "Database " + databaseName + " does not exist.");
        }
    }

    @Override
    public void applyTableCreated(TableInfo flussTable)
            throws DatabaseAlreadyExistException, TableAlreadyExistException {
        TablePath tablePath = flussTable.getTablePath();
        // then, create the table
        try {
            // not ignore if database exists
            paimonCatalog.createTable(
                    toPaimonIdentifier(tablePath), toPaimonSchema(flussTable), false);
        } catch (Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(
                    "Database " + tablePath.getDatabaseName() + " does not exist.");
        } catch (Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistException("Table " + tablePath + " already exists.");
        }
    }

    private Identifier toPaimonIdentifier(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private Schema toPaimonSchema(TableInfo flussTable) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        Options options = new Options();

        // When bucket key is undefined, it should use dynamic bucket (bucket = -1) mode.
        if (flussTable.hasBucketKey()) {
            options.set(CoreOptions.BUCKET, flussTable.getNumBuckets());
            options.set(CoreOptions.BUCKET_KEY, String.join(",", flussTable.getBucketKeys()));
        } else {
            options.set(CoreOptions.BUCKET, CoreOptions.BUCKET.defaultValue());
        }

        // set schema
        for (com.alibaba.fluss.metadata.Schema.Column column :
                flussTable.getSchema().getColumns()) {
            schemaBuilder.column(
                    column.getName(),
                    column.getDataType().accept(FlussDataTypeToPaimonDataType.INSTANCE),
                    column.getComment().orElse(null));
        }

        // set pk
        if (flussTable.getSchema().getPrimaryKey().isPresent()) {
            schemaBuilder.primaryKey(flussTable.getSchema().getPrimaryKey().get().getColumnNames());
            options.set(
                    CoreOptions.CHANGELOG_PRODUCER.key(),
                    CoreOptions.ChangelogProducer.INPUT.toString());
        }
        // set partition keys
        schemaBuilder.partitionKeys(flussTable.getPartitionKeys());

        // set custom properties to paimon schema
        flussTable.getCustomProperties().toMap().forEach(options::set);
        schemaBuilder.options(options.toMap());
        return schemaBuilder.build();
    }

    @Override
    public void close() throws Exception {
        paimonCatalog.close();
    }
}
