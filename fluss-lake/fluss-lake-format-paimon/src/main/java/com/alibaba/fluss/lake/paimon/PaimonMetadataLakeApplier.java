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
import com.alibaba.fluss.exception.TableAlreadyExistException;
import com.alibaba.fluss.lake.MetadataLakeApplier;
import com.alibaba.fluss.metadata.TableInfo;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;

/** . */
public class PaimonMetadataLakeApplier implements MetadataLakeApplier {

    private Catalog paimonCatalog;

    public PaimonMetadataLakeApplier(Configuration configuration) {
        this.paimonCatalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(Options.fromMap(configuration.toMap())));
    }

    @Override
    public void applyTableCreated(TableInfo flussTable) throws TableAlreadyExistException {
        //        Identifier identifier = toPaimonIdentifier(flussTable.getTablePath());
        //        // if database not exists, create it
        //        try {
        //            paimonCatalog.getDatabase(identifier.getDatabaseName());
        //        } catch (Catalog.DatabaseNotExistException ignored) {
        //            paimonCatalog.createDatabase(identifier.getDatabaseName(), true);
        //        }
        //
        //        // then, create the table
        //        paimonCatalog.createTable(identifier, toPaimonSchema(flussTable), true);
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
        } else {
            //            // for log table, need to set offset and timestamp
            //            schemaBuilder.column(OFFSET_COLUMN_NAME, DataTypes.BIGINT());
            //            // we use timestamp_ltz type
            //            schemaBuilder.column(TIMESTAMP_COLUMN_NAME,
            // DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
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
