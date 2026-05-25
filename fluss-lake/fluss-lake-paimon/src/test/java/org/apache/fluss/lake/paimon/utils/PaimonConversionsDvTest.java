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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.lake.paimon.PaimonLakeCatalog;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for DV-related schema generation in {@link PaimonConversions}. */
class PaimonConversionsDvTest {

    @Test
    void testToPaimonSchemaDvEnabled() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .property(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED.key(), "true")
                        .build();

        org.apache.paimon.schema.Schema paimonSchema =
                PaimonConversions.toPaimonSchema(tableDescriptor);

        List<String> fieldNames =
                paimonSchema.fields().stream()
                        .map(org.apache.paimon.types.DataField::name)
                        .collect(java.util.stream.Collectors.toList());

        // Should contain __rowid column after the 3 system columns
        assertThat(fieldNames).contains(PaimonLakeCatalog.ROWID_COLUMN_NAME);
        // Order: id, name, __bucket, __offset, __timestamp, __rowid
        assertThat(fieldNames)
                .containsExactly(
                        "id",
                        "name",
                        "__bucket",
                        "__offset",
                        "__timestamp",
                        PaimonLakeCatalog.ROWID_COLUMN_NAME);
    }

    @Test
    void testToPaimonSchemaDvDisabled() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .build();

        org.apache.paimon.schema.Schema paimonSchema =
                PaimonConversions.toPaimonSchema(tableDescriptor);

        List<String> fieldNames =
                paimonSchema.fields().stream()
                        .map(org.apache.paimon.types.DataField::name)
                        .collect(java.util.stream.Collectors.toList());

        // Should NOT contain __rowid column
        assertThat(fieldNames).doesNotContain(PaimonLakeCatalog.ROWID_COLUMN_NAME);
        // Order: id, name, __bucket, __offset, __timestamp
        assertThat(fieldNames).containsExactly("id", "name", "__bucket", "__offset", "__timestamp");
    }

    @Test
    void testToPaimonSchemaRowidColumnConflict() {
        Schema schema =
                Schema.newBuilder()
                        .column(PaimonLakeCatalog.ROWID_COLUMN_NAME, DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .build();

        assertThatThrownBy(() -> PaimonConversions.toPaimonSchema(tableDescriptor))
                .isInstanceOf(InvalidTableException.class)
                .hasMessageContaining(PaimonLakeCatalog.ROWID_COLUMN_NAME)
                .hasMessageContaining("conflicts with a system column name");
    }
}
