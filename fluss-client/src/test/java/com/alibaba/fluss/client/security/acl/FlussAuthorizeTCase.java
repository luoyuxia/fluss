/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.client.security.acl;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.batch.BatchScanner;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.utils.ClientRpcMessageUtils;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.DataLakeFormat;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.security.acl.AccessControlEntry;
import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceFilter;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.Lists;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.security.acl.AccessControlEntry.WILD_CARD_HOST;
import static com.alibaba.fluss.security.acl.FlussPrincipal.WILD_CARD_PRINCIPAL;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** It case to test authorization of admin operation、read and write operation. */
public class FlussAuthorizeTCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setCoordinatorServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setTabletServerListeners("FLUSS://localhost:0, CLIENT://localhost:0")
                    .setClusterConf(initConfig())
                    .build();

    private Connection rootConn;
    private Admin rootAdmin;
    private Connection guestConn;
    private Admin guestAdmin;
    private Configuration guestConf;

    @BeforeEach
    protected void setup() throws Exception {
        Configuration conf = FLUSS_CLUSTER_EXTENSION.getClientConfig("CLIENT");
        conf.set(ConfigOptions.CLIENT_SECURITY_PROTOCOL, "username_password");
        Configuration rootConf = new Configuration(conf);
        rootConf.setString("client.security.username_password.username", "root");
        rootConf.setString("client.security.username_password.password", "password");
        rootConn = ConnectionFactory.createConnection(rootConf);
        rootAdmin = rootConn.getAdmin();

        guestConf = new Configuration(conf);
        guestConf.setString("client.security.username_password.username", "guest");
        guestConf.setString("client.security.username_password.password", "password2");
        guestConn = ConnectionFactory.createConnection(guestConf);
        guestAdmin = guestConn.getAdmin();

        // prepare default database and table
        rootAdmin
                .createDatabase(
                        DATA1_TABLE_PATH_PK.getDatabaseName(), DatabaseDescriptor.EMPTY, true)
                .get();
        rootAdmin.createTable(DATA1_TABLE_PATH_PK, DATA1_TABLE_DESCRIPTOR_PK, true).get();
    }

    @AfterEach
    protected void teardown() throws Exception {
        if (rootAdmin != null) {
            rootAdmin.dropAcls(Collections.singletonList(AclBindingFilter.ANY)).all().get();
            rootAdmin.close();
            rootAdmin = null;
        }

        if (rootConn != null) {
            rootConn.close();
            rootConn = null;
        }

        if (guestAdmin != null) {
            guestAdmin.close();
            guestAdmin = null;
        }

        if (guestConn != null) {
            guestConn.close();
            guestConn = null;
        }
    }

    @Test
    void testAclOperation() throws Exception {
        // test whether have authorization to operate list acls.
        assertThatThrownBy(() -> guestAdmin.listAcls(AclBindingFilter.ANY).get())
                .hasMessageContaining(
                        "Principal FlussPrincipal{name='guest', type='USER'} have no authorization to operate DESCRIBE on resource Resource{type=CLUSTER, name='fluss-cluster'}");
        rootAdmin
                .createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        Resource.cluster(),
                                        new AccessControlEntry(
                                                WILD_CARD_PRINCIPAL,
                                                WILD_CARD_HOST,
                                                OperationType.DESCRIBE,
                                                PermissionType.ALLOW))))
                .all()
                .get();
        assertThat(guestAdmin.listAcls(AclBindingFilter.ANY).get()).hasSize(1);

        // test whether have authorization to operate create and drop acls.
        FlussPrincipal user1 = new FlussPrincipal("USER", "test_ufu");
        AclBinding user1AclBinding =
                new AclBinding(
                        Resource.table("test_db", "person"),
                        new AccessControlEntry(
                                user1, "*", OperationType.CREATE, PermissionType.ALLOW));
        List<AclBinding> aclBindings =
                Arrays.asList(
                        user1AclBinding,
                        new AclBinding(
                                Resource.database("test_db2"),
                                new AccessControlEntry(
                                        new FlussPrincipal("ROLE", "test_role"),
                                        "127.0.0.1",
                                        OperationType.DROP,
                                        PermissionType.ANY)),
                        new AclBinding(
                                Resource.cluster(),
                                new AccessControlEntry(
                                        new FlussPrincipal("ROLE", "test_role"),
                                        "127.0.0.1",
                                        OperationType.DROP,
                                        PermissionType.ALLOW)));
        assertThatThrownBy(() -> guestAdmin.createAcls(aclBindings).all().get())
                .hasMessageContaining(
                        "Principal FlussPrincipal{name='guest', type='USER'} have no authorization to operate ALTER on resource Resource{type=CLUSTER, name='fluss-cluster'}");
        rootAdmin
                .createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        Resource.cluster(),
                                        new AccessControlEntry(
                                                WILD_CARD_PRINCIPAL,
                                                WILD_CARD_HOST,
                                                OperationType.ALTER,
                                                PermissionType.ALLOW))))
                .all()
                .get();
        guestAdmin.createAcls(aclBindings).all().get();

        assertThat(
                        guestAdmin
                                .listAcls(
                                        new AclBindingFilter(
                                                ResourceFilter.ANY,
                                                new AccessControlEntryFilter(
                                                        user1,
                                                        null,
                                                        OperationType.ANY,
                                                        PermissionType.ALLOW)))
                                .get())
                .containsExactlyInAnyOrderElementsOf(Collections.singleton(user1AclBinding));

        Collection<AclBinding> allAclBinds = rootAdmin.listAcls(AclBindingFilter.ANY).get();
        assertThat(guestAdmin.dropAcls(Collections.singletonList(AclBindingFilter.ANY)).all().get())
                .containsExactlyInAnyOrderElementsOf(allAclBinds);
        assertThat(rootAdmin.listAcls(AclBindingFilter.ANY).get()).isEmpty();
    }

    @Test
    void testAlterDatabase() throws Exception {
        assertThatThrownBy(
                        () ->
                                guestAdmin
                                        .createDatabase(
                                                "test-database1", DatabaseDescriptor.EMPTY, false)
                                        .get())
                .hasMessageContaining(
                        "Principal FlussPrincipal{name='guest', type='USER'} have no authorization to operate CREATE on resource Resource{type=CLUSTER, name='fluss-cluster'}");
        rootAdmin
                .createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        Resource.cluster(),
                                        new AccessControlEntry(
                                                new FlussPrincipal("guest", "USER"),
                                                "*",
                                                OperationType.CREATE,
                                                PermissionType.ALLOW))))
                .all()
                .get();
        guestAdmin.createDatabase("test-database2", DatabaseDescriptor.EMPTY, false).get();
        assertThat(rootAdmin.databaseExists("test-database1").get()).isFalse();
        assertThat(rootAdmin.databaseExists("test-database2").get()).isTrue();
    }

    @Test
    void testListDatabases() throws ExecutionException, InterruptedException {
        assertThat(guestAdmin.listDatabases().get())
                .containsExactlyInAnyOrderElementsOf(Collections.emptyList());
        assertThat(rootAdmin.listDatabases().get())
                .containsExactlyInAnyOrderElementsOf(
                        Lists.newArrayList("fluss", DATA1_TABLE_PATH_PK.getDatabaseName()));

        rootAdmin
                .createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        Resource.database("fluss"),
                                        new AccessControlEntry(
                                                new FlussPrincipal("guest", "USER"),
                                                "*",
                                                OperationType.DESCRIBE,
                                                PermissionType.ALLOW))))
                .all()
                .get();
        assertThat(guestAdmin.listDatabases().get()).isEqualTo(Collections.singletonList("fluss"));

        rootAdmin
                .createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        Resource.cluster(),
                                        new AccessControlEntry(
                                                new FlussPrincipal("guest", "USER"),
                                                "*",
                                                OperationType.ALL,
                                                PermissionType.ALLOW))))
                .all()
                .get();
        assertThat(guestAdmin.listDatabases().get())
                .containsExactlyInAnyOrderElementsOf(
                        Lists.newArrayList("fluss", DATA1_TABLE_PATH_PK.getDatabaseName()));
    }

    @Test
    void testAlterTable() throws Exception {
        assertThatThrownBy(
                        () ->
                                guestAdmin
                                        .createTable(
                                                DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false)
                                        .get())
                .hasMessageContaining(
                        "Principal FlussPrincipal{name='guest', type='USER'} have no authorization to operate CREATE on resource Resource{type=DATABASE, name='test_db_1'}");
        assertThat(rootAdmin.tableExists(DATA1_TABLE_PATH).get()).isFalse();
        rootAdmin
                .createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        Resource.database(DATA1_TABLE_PATH.getDatabaseName()),
                                        new AccessControlEntry(
                                                new FlussPrincipal("guest", "USER"),
                                                "*",
                                                OperationType.CREATE,
                                                PermissionType.ALLOW))))
                .all()
                .get();
        guestAdmin.createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false).get();
        assertThat(rootAdmin.tableExists(DATA1_TABLE_PATH).get()).isTrue();
    }

    @Test
    void testListTables() throws Exception {
        assertThat(guestAdmin.listTables(DATA1_TABLE_PATH_PK.getDatabaseName()).get())
                .isEqualTo(Collections.emptyList());
        rootAdmin
                .createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        Resource.database(DATA1_TABLE_PATH_PK.getDatabaseName()),
                                        new AccessControlEntry(
                                                new FlussPrincipal("guest", "USER"),
                                                "*",
                                                OperationType.DESCRIBE,
                                                PermissionType.ALLOW))))
                .all()
                .get();
        assertThat(guestAdmin.listTables(DATA1_TABLE_PATH_PK.getDatabaseName()).get())
                .isEqualTo(Collections.singletonList(DATA1_TABLE_PATH_PK.getTableName()));
    }

    @Test
    void testGetMetaInfo() throws Exception {
        MetadataRequest metadataRequest =
                ClientRpcMessageUtils.makeMetadataRequest(
                        Collections.singleton(DATA1_TABLE_PATH_PK), null, null);

        try (RpcClient rpcClient =
                RpcClient.create(guestConf, TestingClientMetricGroup.newInstance())) {
            AdminGateway guestGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode("CLIENT"),
                            rpcClient,
                            AdminGateway.class);

            //
            // assertThat(guestGateway.metadata(metadataRequest).get().getTableMetadatasList())
            //                    .isEmpty();

            // if add acl to allow guest read any resource, it will allow to get metadata.
            rootAdmin
                    .createAcls(
                            Collections.singletonList(
                                    new AclBinding(
                                            Resource.table(
                                                    DATA1_TABLE_PATH_PK.getDatabaseName(),
                                                    DATA1_TABLE_PATH_PK.getTableName()),
                                            new AccessControlEntry(
                                                    new FlussPrincipal("guest", "USER"),
                                                    "*",
                                                    OperationType.DESCRIBE,
                                                    PermissionType.ALLOW))))
                    .all()
                    .get();

            assertThat(guestGateway.metadata(metadataRequest).get().getTableMetadatasList())
                    .hasSize(1);
        }
    }

    @Test
    void testProduceAndConsumer() throws Exception {
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1).build();
        rootAdmin.createTable(DATA1_TABLE_PATH, descriptor, false).get();
        // create acl to allow guest write.
        rootAdmin
                .createAcls(
                        Collections.singletonList(
                                new AclBinding(
                                        Resource.table(
                                                DATA1_TABLE_PATH.getDatabaseName(),
                                                DATA1_TABLE_PATH.getTableName()),
                                        new AccessControlEntry(
                                                new FlussPrincipal("guest", "USER"),
                                                "*",
                                                OperationType.WRITE,
                                                PermissionType.ALLOW))))
                .all()
                .get();
        try (Table table = guestConn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row(1, "a")).get();

            try (BatchScanner batchScanner =
                    table.newScan()
                            .limit(1)
                            .createBatchScanner(
                                    new TableBucket(table.getTableInfo().getTableId(), 0))) {
                assertThatThrownBy(() -> batchScanner.pollBatch(Duration.ofMinutes(1)))
                        .hasMessageContaining(
                                "Principal FlussPrincipal{name='guest', type='USER'} have no authorization to operate READ on resource Resource{type=TABLE, name='test_db_1.test_non_pk_table_1'}");
            }
            rootAdmin
                    .createAcls(
                            Collections.singletonList(
                                    new AclBinding(
                                            Resource.table(
                                                    DATA1_TABLE_PATH.getDatabaseName(),
                                                    DATA1_TABLE_PATH.getTableName()),
                                            new AccessControlEntry(
                                                    new FlussPrincipal("guest", "USER"),
                                                    "*",
                                                    OperationType.READ,
                                                    PermissionType.ALLOW))))
                    .all()
                    .get();

            // wait for acl notify to tablet server.
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            catchThrowable(
                                                    (() -> {
                                                        try (BatchScanner batchScanner =
                                                                table.newScan()
                                                                        .limit(1)
                                                                        .createBatchScanner(
                                                                                new TableBucket(
                                                                                        table.getTableInfo()
                                                                                                .getTableId(),
                                                                                        0))) {
                                                            CloseableIterator<InternalRow>
                                                                    internalRowCloseableIterator =
                                                                            batchScanner.pollBatch(
                                                                                    Duration
                                                                                            .ofMinutes(
                                                                                                    1));
                                                            assertThat(internalRowCloseableIterator)
                                                                    .hasNext();
                                                            assertThat(
                                                                            internalRowCloseableIterator
                                                                                    .next())
                                                                    .isEqualTo(row(1, "a"));
                                                        }
                                                    })))
                                    .doesNotThrowAnyException());
        }
    }

    @Test
    void testInitWriter() throws Exception {
        try (RpcClient rpcClient =
                RpcClient.create(guestConf, TestingClientMetricGroup.newInstance())) {
            TabletServerGateway guestTabletServerGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> FLUSS_CLUSTER_EXTENSION.getTabletServerNodes("CLIENT").get(0),
                            rpcClient,
                            TabletServerGateway.class);
            assertThatThrownBy(
                            () ->
                                    guestTabletServerGateway
                                            .initWriter(new InitWriterRequest())
                                            .get())
                    .hasMessageContaining(
                            "Principal FlussPrincipal{name='guest', type='USER'} have no authorization to operate IDEMPOTENT_WRITE on resource Resource{type=CLUSTER, name='fluss-cluster'}");
            // if add acl to allow guest write any resource, it will allow to INIT_WRITER.
            rootAdmin
                    .createAcls(
                            Collections.singletonList(
                                    new AclBinding(
                                            Resource.database(DATA1_TABLE_PATH.getDatabaseName()),
                                            new AccessControlEntry(
                                                    new FlussPrincipal("guest", "USER"),
                                                    "*",
                                                    OperationType.WRITE,
                                                    PermissionType.ALLOW))))
                    .all()
                    .get();

            // wait for acl notify to tablet server.
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            catchThrowable(
                                                    (() ->
                                                            assertThat(
                                                                            guestTabletServerGateway
                                                                                    .initWriter(
                                                                                            new InitWriterRequest())
                                                                                    .get()
                                                                                    .hasWriterId())
                                                                    .isTrue())))
                                    .doesNotThrowAnyException());
        }
    }

    @Test
    void testGetFileSystemSecurityToken() throws Exception {
        try (RpcClient rpcClient =
                RpcClient.create(guestConf, TestingClientMetricGroup.newInstance())) {
            AdminGateway guestGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> FLUSS_CLUSTER_EXTENSION.getCoordinatorServerNode("CLIENT"),
                            rpcClient,
                            AdminGateway.class);
            assertThatThrownBy(
                            () ->
                                    guestGateway
                                            .getFileSystemSecurityToken(
                                                    new GetFileSystemSecurityTokenRequest())
                                            .get())
                    .hasMessageContaining(
                            "Principal FlussPrincipal{name='guest', type='USER'} have no authorization to operate FILESYSTEM_TOKEN on resource Resource{type=CLUSTER, name='fluss-cluster'}");
            // if add acl to allow guest read any resource, it will allow to FILESYSTEM_TOKEN.
            rootAdmin
                    .createAcls(
                            Collections.singletonList(
                                    new AclBinding(
                                            Resource.table(
                                                    DATA1_TABLE_PATH.getDatabaseName(),
                                                    DATA1_TABLE_PATH.getTableName()),
                                            new AccessControlEntry(
                                                    new FlussPrincipal("guest", "USER"),
                                                    "*",
                                                    OperationType.READ,
                                                    PermissionType.ALLOW))))
                    .all()
                    .get();
            assertThat(
                            guestGateway
                                    .getFileSystemSecurityToken(
                                            new GetFileSystemSecurityTokenRequest())
                                    .get()
                                    .getToken())
                    .isEmpty();
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.KV_SNAPSHOT_INTERVAL, Duration.ofSeconds(1));
        // set a shorter max lag time to make tests in FlussFailServerTableITCase faster
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(10));
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);

        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE, MemorySize.parse("1mb"));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse("1kb"));

        // set security information.
        conf.setString(
                ConfigOptions.SERVER_SECURITY_PROTOCOL_MAP.key(), "CLIENT:username_password");
        conf.setString("security.username_password.credentials", "root:password,guest:password2");
        conf.set(ConfigOptions.SUPER_USERS, "USER:root");
        conf.set(ConfigOptions.AUTHORIZER_PLUGIN_TYPE, "zookeeper");
        return conf;
    }
}
