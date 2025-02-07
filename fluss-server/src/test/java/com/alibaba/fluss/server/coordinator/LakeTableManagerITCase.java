/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.TableConfig;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;

import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newCreateTableRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newGetTableInfoRequest;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for creating/dropping table for Fluss with lake storage configured . */
class LakeTableManagerITCase {

    private static final String LAKE_STORAGE = "Paimon";

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(
                            Configuration.fromMap(
                                    Collections.singletonMap(
                                            ConfigOptions.LAKEHOUSE_STORAGE.key(), LAKE_STORAGE)))
                    .build();

    @Test
    void testCreateTable() throws Exception {
        AdminGateway adminGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("f1", DataTypes.INT()).build())
                        .build();

        TablePath tablePath = TablePath.of("fluss", "test_lake_table");

        // create the table
        adminGateway.createTable(newCreateTableRequest(tablePath, tableDescriptor, false)).get();

        TableConfig tableConfig =
                new TableConfig(
                        Configuration.fromMap(
                                TableDescriptor.fromJsonBytes(
                                                adminGateway
                                                        .getTableInfo(
                                                                newGetTableInfoRequest(tablePath))
                                                        .get()
                                                        .getTableJson())
                                        .getProperties()));
        // verify the data lake type is set correctly
        assertThat(tableConfig.getDataLakeType()).isEqualTo(LAKE_STORAGE);
    }
}
