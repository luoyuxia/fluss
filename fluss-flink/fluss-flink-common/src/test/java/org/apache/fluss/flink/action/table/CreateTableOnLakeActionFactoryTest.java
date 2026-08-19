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

package org.apache.fluss.flink.action.table;

import org.apache.fluss.flink.action.Action;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CreateTableOnLakeActionFactory}. */
class CreateTableOnLakeActionFactoryTest {

    private final CreateTableOnLakeActionFactory factory = new CreateTableOnLakeActionFactory();

    @Test
    void testCreateWithTieringStyleConfigurations() {
        Optional<Action> optionalAction =
                factory.create(
                        params(
                                "--table",
                                "my_db.my_table",
                                "--fluss.bootstrap.servers",
                                "localhost:9123",
                                "--fluss.client.request-timeout",
                                "30s",
                                "--datalake.format",
                                "paimon",
                                "--datalake.paimon.metastore",
                                "filesystem",
                                "--datalake.paimon.warehouse",
                                "/tmp/paimon",
                                "--table-conf",
                                "bucket.num=16",
                                "--table-conf",
                                "owner=storage"));

        assertThat(optionalAction).isPresent();
        CreateTableOnLakeAction action = (CreateTableOnLakeAction) optionalAction.get();
        assertThat(action.getTablePath()).isEqualTo(TablePath.of("my_db", "my_table"));
        assertThat(action.getFlussConfiguration().toMap())
                .containsEntry("bootstrap.servers", "localhost:9123")
                .containsEntry("client.request-timeout", "30s");
        assertThat(action.getPaimonConfiguration().toMap())
                .containsEntry("metastore", "filesystem")
                .containsEntry("warehouse", "/tmp/paimon");
        assertThat(action.getTableProperties())
                .containsEntry("bucket.num", "16")
                .containsEntry("owner", "storage");
    }

    @Test
    void testRequireFlussBootstrapServers() {
        assertThatThrownBy(
                        () ->
                                factory.create(
                                        params(
                                                "--table",
                                                "my_db.my_table",
                                                "--datalake.format",
                                                "paimon")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("--fluss.bootstrap.servers is required");
    }

    @Test
    void testRejectUnsupportedLakeFormat() {
        assertThatThrownBy(
                        () ->
                                factory.create(
                                        params(
                                                "--table",
                                                "my_db.my_table",
                                                "--fluss.bootstrap.servers",
                                                "localhost:9123",
                                                "--datalake.format",
                                                "iceberg")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only supports Paimon");
    }

    @Test
    void testRejectInvalidTablePath() {
        assertThatThrownBy(
                        () ->
                                factory.create(
                                        params(
                                                "--table",
                                                "my_table",
                                                "--fluss.bootstrap.servers",
                                                "localhost:9123",
                                                "--datalake.format",
                                                "paimon")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("--table must use the form database.table");
    }

    @Test
    void testRejectDuplicateTableProperty() {
        assertThatThrownBy(
                        () ->
                                factory.create(
                                        params(
                                                "--table",
                                                "my_db.my_table",
                                                "--fluss.bootstrap.servers",
                                                "localhost:9123",
                                                "--datalake.format",
                                                "paimon",
                                                "--table-conf",
                                                "bucket.num=16",
                                                "--table-conf",
                                                "bucket.num=32")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate --table-conf key: bucket.num");
    }

    private static MultipleParameterToolAdapter params(String... args) {
        return MultipleParameterToolAdapter.fromArgs(args);
    }
}
