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

package com.alibaba.fluss.server.authorizer;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.netty.server.Session;
import com.alibaba.fluss.security.acl.AccessControlEntry;
import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceFilter;
import com.alibaba.fluss.security.acl.ResourceType;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.ZooKeeperUtils;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.fluss.security.acl.OperationType.CREATE;
import static com.alibaba.fluss.security.acl.OperationType.DROP;
import static com.alibaba.fluss.security.acl.OperationType.READ;
import static com.alibaba.fluss.security.acl.OperationType.WRITE;
import static com.alibaba.fluss.security.acl.ResourceType.DATABASE;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ZooKeeperBasedAuthorizer}. */
public class ZooKeeperBasedAuthorizerTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private ZooKeeperBasedAuthorizer authorizer;
    private ZooKeeperBasedAuthorizer authorizer2;
    private ZooKeeperClient zooKeeperClient;

    @BeforeEach
    void setUp() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        configuration.setString(ConfigOptions.SUPER_USERS, "USER:root");
        zooKeeperClient = ZooKeeperUtils.startZookeeperClient(configuration, new NOPErrorHandler());
        authorizer = new ZooKeeperBasedAuthorizer(configuration);
        authorizer2 = new ZooKeeperBasedAuthorizer(configuration);
        authorizer.startup();
        authorizer2.startup();
    }

    @AfterEach
    void tearDown() {
        authorizer.dropAcls(Collections.singletonList(AclBindingFilter.ANY));
        authorizer.close();
        authorizer2.close();
        zooKeeperClient.close();
    }

    @Test
    void testSimpleAclOperation() {
        assertThat(authorizer.listAcls(AclBindingFilter.ANY)).isEmpty();
        List<AclBinding> database1Acls =
                Arrays.asList(
                        createAclBinding(Resource.database("database1"), "user1", "host-1", CREATE),
                        createAclBinding(Resource.database("database1"), "user2", "host-1", DROP));

        List<AclBinding> database2Acls =
                Arrays.asList(
                        createAclBinding(Resource.database("database2"), "user1", "host-1", CREATE),
                        createAclBinding(Resource.database("database2"), "user2", "host-1", DROP));

        authorizer.addAcls(database1Acls);
        authorizer.addAcls(database2Acls);
        // list by database
        assertThat(
                        authorizer.listAcls(
                                new AclBindingFilter(
                                        new ResourceFilter(DATABASE, "database2"),
                                        AccessControlEntryFilter.ANY)))
                .containsExactlyInAnyOrderElementsOf(database2Acls);
        // list by user
        assertThat(
                        authorizer.listAcls(
                                createAclBindingFilter(
                                        ResourceFilter.ANY,
                                        new FlussPrincipal("user1", "USER"),
                                        null,
                                        OperationType.ANY)))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(database1Acls.get(0), database2Acls.get(0)));
        // list by operation
        assertThat(
                        authorizer.listAcls(
                                createAclBindingFilter(
                                        ResourceFilter.ANY,
                                        FlussPrincipal.ANY,
                                        null,
                                        OperationType.CREATE)))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(database1Acls.get(0), database2Acls.get(0)));

        authorizer.dropAcls(
                Collections.singletonList(
                        new AclBindingFilter(
                                new ResourceFilter(DATABASE, "database2"),
                                AccessControlEntryFilter.ANY)));
        assertThat(authorizer.listAcls(AclBindingFilter.ANY))
                .containsExactlyInAnyOrderElementsOf(database1Acls);
    }

    @Test
    void testSimpleAuthorizations() throws Exception {
        Session session = createSession("user1", "192.168.1.1");
        List<Action> actions =
                Arrays.asList(
                        new Action(Resource.table("database1", "foo"), READ),
                        new Action(Resource.database("database2"), WRITE));
        assertThat(authorizer.authorize(session, actions)).containsExactly(false, false);
        authorizer.addAcls(
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("database2"),
                                new AccessControlEntry(
                                        session.getPrincipal(),
                                        "192.168.1.1",
                                        WRITE,
                                        PermissionType.ALLOW))));
        assertThat(authorizer.authorize(session, actions)).containsExactly(false, true);
        authorizer.addAcls(
                Collections.singletonList(
                        new AclBinding(
                                Resource.table("database1", "foo"),
                                new AccessControlEntry(
                                        session.getPrincipal(), "*", READ, PermissionType.ALLOW))));
        assertThat(authorizer.authorize(session, actions)).containsExactly(true, true);
        authorizer.dropAcls(
                Collections.singletonList(
                        new AclBindingFilter(
                                new ResourceFilter(ResourceType.TABLE, "database1.foo"),
                                new AccessControlEntryFilter(
                                        null, null, READ, PermissionType.ANY))));
        assertThat(authorizer.authorize(session, actions)).containsExactly(false, true);
    }

    @Test
    void testAuthorizerNoZkConfig() {
        Configuration configuration = new Configuration();
        ZooKeeperBasedAuthorizer zooKeeperBasedAuthorizer =
                new ZooKeeperBasedAuthorizer(configuration);
        assertThatThrownBy(zooKeeperBasedAuthorizer::startup)
                .hasMessageContaining(
                        "No valid ZooKeeper quorum has been specified. You can specify the quorum via the configuration key 'zookeeper.address");
    }

    @Test
    void testSuperUserHasAccess() throws Exception {
        Session normalUserSession = createSession("user1", "192.168.1.1");
        Session superUserSession = createSession("root", "192.168.1.1");
        assertThat(
                        authorizer.authorize(
                                normalUserSession,
                                Collections.singletonList(
                                        new Action(Resource.database("database1"), READ))))
                .containsExactly(false);
        assertThat(
                        authorizer.authorize(
                                superUserSession,
                                Collections.singletonList(
                                        new Action(Resource.database("database1"), READ))))
                .containsExactly(true);
    }

    @Test
    void testAclWithOperationAll() throws Exception {
        Session session = createSession("user1", "192.168.1.1");
        List<Action> actions =
                Arrays.asList(
                        new Action(Resource.database("database1"), READ),
                        new Action(Resource.database("database1"), WRITE));
        assertThat(authorizer.authorize(session, actions)).containsExactly(false, false);
        authorizer.addAcls(
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("database1"),
                                new AccessControlEntry(
                                        session.getPrincipal(),
                                        "192.168.1.1",
                                        OperationType.ALL,
                                        PermissionType.ALLOW))));
        assertThat(authorizer.authorize(session, actions)).containsExactly(true, true);
    }

    @Test
    void testHostAddressAclValidation() throws Exception {
        FlussPrincipal principal = new FlussPrincipal("user1", "USER");
        Session session1 = createSession("user1", "192.168.1.1");
        Session session2 = createSession("user1", "192.168.1.2");
        List<Action> actions =
                Collections.singletonList(new Action(Resource.database("database1"), READ));
        assertThat(authorizer.authorize(session1, actions)).containsExactly(false);
        assertThat(authorizer.authorize(session2, actions)).containsExactly(false);
        authorizer.addAcls(
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("database1"),
                                new AccessControlEntry(
                                        principal,
                                        session1.getInetAddress().getHostAddress(),
                                        READ,
                                        PermissionType.ALLOW))));
        assertThat(authorizer.authorize(session1, actions)).containsExactly(true);
        assertThat(authorizer.authorize(session2, actions)).containsExactly(false);
        authorizer.addAcls(
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("database1"),
                                new AccessControlEntry(
                                        principal, "*", READ, PermissionType.ALLOW))));
        assertThat(authorizer.authorize(session1, actions)).containsExactly(true);
        assertThat(authorizer.authorize(session2, actions)).containsExactly(true);
    }

    /** Test ACL inheritance, as described in {@link OperationType}. */
    @Test
    void testAclInheritanceOnOperationType() throws Exception {
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(),
                OperationType.ALL,
                Arrays.asList(
                        READ,
                        WRITE,
                        OperationType.CREATE,
                        OperationType.DROP,
                        OperationType.ALTER,
                        OperationType.DESCRIBE,
                        OperationType.IDEMPOTENT_WRITE,
                        OperationType.FILESYSTEM_TOKEN));
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(),
                OperationType.CREATE,
                Collections.singleton(OperationType.DESCRIBE));
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(),
                OperationType.DROP,
                Collections.singleton(OperationType.DESCRIBE));
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(),
                OperationType.ALTER,
                Collections.singleton(OperationType.DESCRIBE));

        // when we allow READ on any resource, we also allow DESCRIBE and FILESYSTEM_TOKEN on
        // cluster.
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(),
                READ,
                Arrays.asList(OperationType.DESCRIBE, OperationType.FILESYSTEM_TOKEN));
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(),
                WRITE,
                Arrays.asList(OperationType.DESCRIBE, OperationType.IDEMPOTENT_WRITE));
        testOperationTypeImplicationsOfAllow(
                Resource.database("database1"),
                READ,
                Arrays.asList(OperationType.DESCRIBE, OperationType.FILESYSTEM_TOKEN));
        testOperationTypeImplicationsOfAllow(
                Resource.table("database2", "table1"),
                WRITE,
                Arrays.asList(OperationType.DESCRIBE, OperationType.IDEMPOTENT_WRITE));
    }

    private void testOperationTypeImplicationsOfAllow(
            Resource resource, OperationType parentOp, Collection<OperationType> allowedOps)
            throws Exception {
        Session session = createSession("user1", "192.168.1.1");
        AccessControlEntry accessControlEntry =
                new AccessControlEntry(session.getPrincipal(), "*", parentOp, PermissionType.ALLOW);
        authorizer.addAcls(Collections.singletonList(new AclBinding(resource, accessControlEntry)));
        Arrays.asList(OperationType.values())
                .forEach(
                        op -> {
                            if (allowedOps.contains(op) || op == parentOp) {
                                assertThat(authorizer.authorize(session, op, resource)).isTrue();
                            } else if (op != OperationType.ANY) {
                                assertThat(authorizer.authorize(session, op, resource)).isFalse();
                            }
                        });
        authorizer.dropAcls(
                Collections.singletonList(
                        new AclBindingFilter(
                                new ResourceFilter(resource.getType(), resource.getName()),
                                new AccessControlEntryFilter(
                                        session.getPrincipal(),
                                        "*",
                                        parentOp,
                                        PermissionType.ALLOW))));
        authorizer.dropAcls(
                Collections.singletonList(
                        new AclBindingFilter(
                                new ResourceFilter(
                                        Resource.cluster().getType(), Resource.cluster().getName()),
                                new AccessControlEntryFilter(
                                        session.getPrincipal(),
                                        "*",
                                        OperationType.IDEMPOTENT_WRITE,
                                        PermissionType.ALLOW))));
        authorizer.dropAcls(
                Collections.singletonList(
                        new AclBindingFilter(
                                new ResourceFilter(
                                        Resource.cluster().getType(), Resource.cluster().getName()),
                                new AccessControlEntryFilter(
                                        session.getPrincipal(),
                                        "*",
                                        OperationType.FILESYSTEM_TOKEN,
                                        PermissionType.ALLOW))));
    }

    @Test
    void testAclInheritanceOnResourceType() throws Exception {
        testResourceTypeImplicationsOfAllow(
                ResourceType.CLUSTER, Arrays.asList(DATABASE, ResourceType.TABLE));
        testResourceTypeImplicationsOfAllow(DATABASE, Collections.singleton(ResourceType.TABLE));
    }

    private void testResourceTypeImplicationsOfAllow(
            ResourceType parentType, Collection<ResourceType> allowedTypes) throws Exception {
        FlussPrincipal user = new FlussPrincipal("user1", "USER");
        Session session = createSession("user1", "192.168.1.1");
        AccessControlEntry accessControlEntry =
                new AccessControlEntry(user, "*", READ, PermissionType.ALLOW);
        authorizer.addAcls(
                Collections.singletonList(
                        new AclBinding(mockResource(parentType), accessControlEntry)));
        Arrays.asList(ResourceType.values())
                .forEach(
                        resourceType -> {
                            if (allowedTypes.contains(resourceType) || resourceType == parentType) {
                                assertThat(
                                                authorizer.authorize(
                                                        session, READ, mockResource(resourceType)))
                                        .isTrue();
                            } else if (resourceType != ResourceType.ANY) {
                                assertThat(
                                                authorizer.authorize(
                                                        session, READ, mockResource(resourceType)))
                                        .isFalse();
                            }
                        });
        authorizer.dropAcls(
                Collections.singletonList(
                        new AclBindingFilter(
                                new ResourceFilter(
                                        mockResource(parentType).getType(),
                                        mockResource(parentType).getName()),
                                new AccessControlEntryFilter(
                                        user, "*", READ, PermissionType.ALLOW))));
    }

    private Resource mockResource(ResourceType resourceType) {
        switch (resourceType) {
            case CLUSTER:
                return Resource.cluster();
            case DATABASE:
                return Resource.database("database1");
            case TABLE:
                return Resource.table("database1", "table1");
            default:
                return null;
        }
    }

    @Test
    void testLoadCache() throws Exception {
        Resource resource1 = Resource.database("foo-" + UUID.randomUUID());
        Set<AccessControlEntry> acls1 =
                Collections.singleton(
                        new AccessControlEntry(
                                new FlussPrincipal("user1", "User"),
                                "host-1",
                                READ,
                                PermissionType.ANY));
        addAcls(authorizer, resource1, acls1);

        Resource resource2 = Resource.database("foo-" + UUID.randomUUID());
        Set<AccessControlEntry> acls2 =
                Collections.singleton(
                        new AccessControlEntry(
                                new FlussPrincipal("user2", "User"),
                                "host-1",
                                READ,
                                PermissionType.ANY));
        addAcls(authorizer, resource2, acls2);

        // delete acl change notifications to test initial load.
        zooKeeperClient.deleteAclChangeNotifications();
        authorizer2.startup();
        assertThat(listAcls(authorizer2, resource1)).isEqualTo(acls1);
        assertThat(listAcls(authorizer2, resource2)).isEqualTo(acls2);

        // test update cache later
        final Set<AccessControlEntry> acls3 =
                new HashSet<>(
                        Arrays.asList(
                                new AccessControlEntry(
                                        new FlussPrincipal("user2", "User"),
                                        "host-1",
                                        READ,
                                        PermissionType.ANY),
                                new AccessControlEntry(
                                        new FlussPrincipal("user3", "User"),
                                        "host-2",
                                        OperationType.IDEMPOTENT_WRITE,
                                        PermissionType.ANY)));
        addAcls(authorizer, resource2, acls3);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, resource2)).isEqualTo(acls3);
                });
    }

    // Authorizing the empty resource is not supported because we create a znode with the resource
    // name.
    @Test
    void testEmptyAclThrowsException() {
        assertThatThrownBy(
                        () ->
                                addAcls(
                                        authorizer,
                                        Resource.database(""),
                                        Collections.singleton(
                                                new AccessControlEntry(
                                                        new FlussPrincipal("user1", "User"),
                                                        "host-1",
                                                        READ,
                                                        PermissionType.ANY))))
                .hasRootCauseExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Path must not end with / character");
    }

    @Test
    void testLocalConcurrentModificationOfResourceAcls() {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        FlussPrincipal user1 = new FlussPrincipal("user1", "User");
        AccessControlEntry acl1 = new AccessControlEntry(user1, "host-1", READ, PermissionType.ANY);
        FlussPrincipal user2 = new FlussPrincipal("user2", "User");
        AccessControlEntry acl2 = new AccessControlEntry(user2, "host-2", READ, PermissionType.ANY);
        addAcls(authorizer, commonResource, Collections.singleton(acl1));
        addAcls(authorizer, commonResource, Collections.singleton(acl2));
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer, commonResource))
                            .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2)));
                });
    }

    @Test
    void testDistributedConcurrentModificationOfResourceAcls() {
        Resource commonResource = Resource.database("test");
        FlussPrincipal user1 = new FlussPrincipal("user1", "User");
        AccessControlEntry acl1 = new AccessControlEntry(user1, "host-1", READ, PermissionType.ANY);
        FlussPrincipal user2 = new FlussPrincipal("user2", "User");
        AccessControlEntry acl2 = new AccessControlEntry(user2, "host-2", READ, PermissionType.ANY);
        // Add on each instance
        addAcls(authorizer, commonResource, Collections.singleton(acl1));
        addAcls(authorizer2, commonResource, Collections.singleton(acl2));

        FlussPrincipal user3 = new FlussPrincipal("user3", "User");
        AccessControlEntry acl3 = new AccessControlEntry(user3, "host-3", READ, PermissionType.ANY);

        // Add on one instance and delete on another
        addAcls(authorizer, commonResource, Collections.singleton(acl3));
        removeAcls(authorizer2, commonResource, Collections.singleton(acl3));

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer, commonResource))
                            .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2)));
                });

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, commonResource))
                            .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2)));
                });
    }

    @Test
    void testHighConcurrencyModificationOfResourceAcls() throws Exception {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        Set<AccessControlEntry> acls =
                IntStream.range(0, 50)
                        .mapToObj(
                                i ->
                                        new AccessControlEntry(
                                                new FlussPrincipal(String.valueOf(i), "User"),
                                                "host-1",
                                                READ,
                                                PermissionType.ANY))
                        .collect(Collectors.toSet());

        List<Runnable> concurrentFunctions =
                acls.stream()
                        .map(
                                acl ->
                                        (Runnable)
                                                () -> {
                                                    if (Integer.parseInt(
                                                                            acl.getPrincipal()
                                                                                    .getName())
                                                                    % 2
                                                            == 0) {
                                                        addAcls(
                                                                authorizer,
                                                                commonResource,
                                                                Collections.singleton(acl));
                                                    } else {
                                                        addAcls(
                                                                authorizer2,
                                                                commonResource,
                                                                Collections.singleton(acl));
                                                    }

                                                    if (Integer.parseInt(
                                                                            acl.getPrincipal()
                                                                                    .getName())
                                                                    % 10
                                                            == 0) {
                                                        removeAcls(
                                                                authorizer2,
                                                                commonResource,
                                                                Collections.singleton(acl));
                                                    }
                                                })
                        .collect(Collectors.toList());

        Set<AccessControlEntry> expectedAcls =
                acls.stream()
                        .filter(acl -> Integer.parseInt(acl.getPrincipal().getName()) % 10 != 0)
                        .collect(Collectors.toSet());
        assertConcurrent(concurrentFunctions, 30 * 1000);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(listAcls(authorizer, commonResource)).isEqualTo(expectedAcls));
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, commonResource)).isEqualTo(expectedAcls);
                });
    }

    @Test
    void testHighConcurrencyDeletionOfResourceAcls() {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        AccessControlEntry acl =
                new AccessControlEntry(
                        new FlussPrincipal("user1", "User"), "host-1", READ, PermissionType.ANY);
        List<Runnable> concurrentFunctions =
                IntStream.range(0, 50)
                        .mapToObj(
                                i ->
                                        (Runnable)
                                                () -> {
                                                    addAcls(
                                                            authorizer,
                                                            commonResource,
                                                            Collections.singleton(acl));
                                                    removeAcls(
                                                            authorizer2,
                                                            commonResource,
                                                            Collections.singleton(acl));
                                                })
                        .collect(Collectors.toList());
        assertConcurrent(concurrentFunctions, 30 * 1000);
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer, commonResource))
                            .isEqualTo(Collections.emptySet());
                });

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, commonResource))
                            .isEqualTo(Collections.emptySet());
                });
    }

    // todo : add acl作为base测试类

    void addAcls(Authorizer authorizer, Resource resource, Set<AccessControlEntry> entries) {
        List<AclBinding> aclBindings =
                entries.stream()
                        .map(entry -> new AclBinding(resource, entry))
                        .collect(Collectors.toList());
        authorizer
                .addAcls(aclBindings)
                .forEach(
                        result -> {
                            if (result.exception().isPresent()) {
                                throw result.exception().get();
                            }
                        });
    }

    Set<AccessControlEntry> listAcls(Authorizer authorizer, Resource resource) {
        AclBindingFilter aclBindingFilter =
                new AclBindingFilter(
                        new ResourceFilter(resource.getType(), resource.getName()),
                        AccessControlEntryFilter.ANY);
        Collection<AclBinding> aclBindings = authorizer.listAcls(aclBindingFilter);
        return aclBindings.stream()
                .map(AclBinding::getAccessControlEntry)
                .collect(Collectors.toSet());
    }

    void removeAcls(Authorizer authorizer, Resource resource, Set<AccessControlEntry> entries) {
        List<AclBindingFilter> aclBindings =
                entries.stream()
                        .map(
                                entry ->
                                        new AclBindingFilter(
                                                new ResourceFilter(
                                                        resource.getType(), resource.getName()),
                                                new AccessControlEntryFilter(
                                                        entry.getPrincipal(),
                                                        entry.getHost(),
                                                        entry.getOperationType(),
                                                        entry.getPermissionType())))
                        .collect(Collectors.toList());
        authorizer
                .dropAcls(aclBindings)
                .forEach(
                        result -> {
                            if (result.exception().isPresent()) {
                                throw result.exception().get();
                            }
                        });
    }

    /**
     * Asserts that a list of tasks can be executed concurrently within a given timeout.
     *
     * @param tasks the list of tasks to execute
     * @param timeoutMs the timeout in milliseconds
     */
    private void assertConcurrent(List<Runnable> tasks, long timeoutMs) {
        ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
        List<Future<?>> futures = tasks.stream().map(executor::submit).collect(Collectors.toList());

        executor.shutdown();
        try {
            boolean completed = executor.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
            assertThat(completed).isTrue();
            for (Future<?> future : futures) {
                future.get(); // Ensure no exceptions were thrown
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Should support many concurrent calls"
                            + " - Exception during concurrent execution",
                    e);
        }
    }

    private Session createSession(String username, String host) throws Exception {
        return new Session(
                (byte) 1,
                "FLUSS",
                false,
                InetAddress.getByName(host),
                new FlussPrincipal(username, "USER"));
    }

    private AclBinding createAclBinding(
            ResourceType resourceType, String username, String host, OperationType operation) {
        return new AclBinding(
                mockResource(resourceType), createAclEntry(username, host, operation));
    }

    private AclBinding createAclBinding(
            Resource resource, String username, String host, OperationType operation) {
        return new AclBinding(resource, createAclEntry(username, host, operation));
    }

    private AccessControlEntry createAclEntry(
            String username, String host, OperationType operation) {
        return new AccessControlEntry(
                new FlussPrincipal(username, "USER"), host, operation, PermissionType.ALLOW);
    }

    private AclBindingFilter createAclBindingFilter(
            ResourceFilter resourceFilter,
            FlussPrincipal flussPrincipal,
            String host,
            OperationType operation) {
        return new AclBindingFilter(
                resourceFilter,
                new AccessControlEntryFilter(
                        flussPrincipal, host, operation, PermissionType.ALLOW));
    }
}
