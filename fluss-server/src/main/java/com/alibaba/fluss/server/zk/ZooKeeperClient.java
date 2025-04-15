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

package com.alibaba.fluss.server.zk;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePartition;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceType;
import com.alibaba.fluss.server.zk.data.BucketSnapshot;
import com.alibaba.fluss.server.zk.data.CoordinatorAddress;
import com.alibaba.fluss.server.zk.data.DatabaseRegistration;
import com.alibaba.fluss.server.zk.data.LakeTableSnapshot;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.RemoteLogManifestHandle;
import com.alibaba.fluss.server.zk.data.ResourceAcl;
import com.alibaba.fluss.server.zk.data.TableAssignment;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.server.zk.data.TabletServerRegistration;
import com.alibaba.fluss.server.zk.data.ZkData.AclChangeNotificationNode;
import com.alibaba.fluss.server.zk.data.ZkData.AclChangesNode;
import com.alibaba.fluss.server.zk.data.ZkData.BucketIdsZNode;
import com.alibaba.fluss.server.zk.data.ZkData.BucketRemoteLogsZNode;
import com.alibaba.fluss.server.zk.data.ZkData.BucketSnapshotIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.BucketSnapshotsZNode;
import com.alibaba.fluss.server.zk.data.ZkData.CoordinatorZNode;
import com.alibaba.fluss.server.zk.data.ZkData.DatabaseZNode;
import com.alibaba.fluss.server.zk.data.ZkData.DatabasesZNode;
import com.alibaba.fluss.server.zk.data.ZkData.LakeTableZNode;
import com.alibaba.fluss.server.zk.data.ZkData.LeaderAndIsrZNode;
import com.alibaba.fluss.server.zk.data.ZkData.PartitionIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.PartitionSequenceIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.PartitionZNode;
import com.alibaba.fluss.server.zk.data.ZkData.PartitionsZNode;
import com.alibaba.fluss.server.zk.data.ZkData.ResourceAclNode;
import com.alibaba.fluss.server.zk.data.ZkData.SchemaZNode;
import com.alibaba.fluss.server.zk.data.ZkData.SchemasZNode;
import com.alibaba.fluss.server.zk.data.ZkData.ServerIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.ServerIdsZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TableIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TableSequenceIdZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TableZNode;
import com.alibaba.fluss.server.zk.data.ZkData.TablesZNode;
import com.alibaba.fluss.server.zk.data.ZkData.WriterIdZNode;
import com.alibaba.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import com.alibaba.fluss.shaded.zookeeper3.org.apache.zookeeper.data.Stat;
import com.alibaba.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * This class includes methods for write/read various metadata (leader address, tablet server
 * registration, table assignment, table, schema) in Zookeeper.
 */
@Internal
public class ZooKeeperClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClient.class);

    private final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper;

    private final CuratorFramework zkClient;
    private final ZkSequenceIDCounter tableIdCounter;
    private final ZkSequenceIDCounter partitionIdCounter;
    private final ZkSequenceIDCounter writerIdCounter;

    public ZooKeeperClient(CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper) {
        this.curatorFrameworkWrapper = curatorFrameworkWrapper;
        this.zkClient = curatorFrameworkWrapper.asCuratorFramework();
        this.tableIdCounter = new ZkSequenceIDCounter(zkClient, TableSequenceIdZNode.path());
        this.partitionIdCounter =
                new ZkSequenceIDCounter(zkClient, PartitionSequenceIdZNode.path());
        this.writerIdCounter = new ZkSequenceIDCounter(zkClient, WriterIdZNode.path());
    }

    public Optional<byte[]> getOrEmpty(String path) throws Exception {
        try {
            return Optional.of(zkClient.getData().forPath(path));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Coordinator server
    // --------------------------------------------------------------------------------------------

    /** Register a coordinator leader server to ZK. */
    public void registerCoordinatorLeader(CoordinatorAddress coordinatorAddress) throws Exception {
        String path = CoordinatorZNode.path();
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, CoordinatorZNode.encode(coordinatorAddress));
        LOG.info("Registered leader {} at path {}.", coordinatorAddress, path);
    }

    /** Get the leader address registered in ZK. */
    public Optional<CoordinatorAddress> getCoordinatorAddress() throws Exception {
        Optional<byte[]> bytes = getOrEmpty(CoordinatorZNode.path());
        return bytes.map(CoordinatorZNode::decode);
    }

    // --------------------------------------------------------------------------------------------
    // Tablet server
    // --------------------------------------------------------------------------------------------

    /** Register a tablet server to ZK. */
    public void registerTabletServer(
            int tabletServerId, TabletServerRegistration tabletServerRegistration)
            throws Exception {
        String path = ServerIdZNode.path(tabletServerId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, ServerIdZNode.encode(tabletServerRegistration));
        LOG.info(
                "Registered tablet server {} at path {} with registration {}.",
                tabletServerId,
                path,
                tabletServerRegistration);
    }

    /** Get the tablet server registered in ZK. */
    public Optional<TabletServerRegistration> getTabletServer(int tabletServerId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(ServerIdZNode.path(tabletServerId));
        return bytes.map(ServerIdZNode::decode);
    }

    /** Gets the list of sorted server Ids. */
    public int[] getSortedTabletServerList() throws Exception {
        List<String> tabletServers = getChildren(ServerIdsZNode.path());
        return tabletServers.stream().mapToInt(Integer::parseInt).sorted().toArray();
    }

    // --------------------------------------------------------------------------------------------
    // Tablet assignments
    // --------------------------------------------------------------------------------------------

    /** Register table assignment to ZK. */
    public void registerTableAssignment(long tableId, TableAssignment tableAssignment)
            throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, TableIdZNode.encode(tableAssignment));
        LOG.info("Registered table assignment {} for table id {}.", tableAssignment, tableId);
    }

    /** Register partition assignment to ZK. */
    public void registerPartitionAssignment(
            long partitionId, PartitionAssignment partitionAssignment) throws Exception {
        String path = PartitionIdZNode.path(partitionId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, PartitionIdZNode.encode(partitionAssignment));
    }

    /** Get the table assignment in ZK. */
    public Optional<TableAssignment> getTableAssignment(long tableId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(TableIdZNode.path(tableId));
        return bytes.map(
                data ->
                        // we'll put a laketable node under TableIdZNode,
                        // so it won't be Optional#empty
                        // but will with a zero-length array
                        data.length == 0 ? null : TableIdZNode.decode(data));
    }

    /** Get the partition assignment in ZK. */
    public Optional<PartitionAssignment> getPartitionAssignment(long partitionId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(PartitionIdZNode.path(partitionId));
        return bytes.map(PartitionIdZNode::decode);
    }

    public void updateTableAssignment(long tableId, TableAssignment tableAssignment)
            throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.setData().forPath(path, TableIdZNode.encode(tableAssignment));
        LOG.info("Updated table assignment {} for table id {}.", tableAssignment, tableId);
    }

    public void deleteTableAssignment(long tableId) throws Exception {
        String path = TableIdZNode.path(tableId);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table assignment for table id {}.", tableId);
    }

    public void deletePartitionAssignment(long partitionId) throws Exception {
        String path = PartitionIdZNode.path(partitionId);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table assignment for partition id {}.", partitionId);
    }

    // --------------------------------------------------------------------------------------------
    // Table state
    // --------------------------------------------------------------------------------------------

    /** Register bucket LeaderAndIsr to ZK. */
    public void registerLeaderAndIsr(TableBucket tableBucket, LeaderAndIsr leaderAndIsr)
            throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, LeaderAndIsrZNode.encode(leaderAndIsr));
        LOG.info("Registered {} for bucket {} in Zookeeper.", leaderAndIsr, tableBucket);
    }

    /** Get the bucket LeaderAndIsr in ZK. */
    public Optional<LeaderAndIsr> getLeaderAndIsr(TableBucket tableBucket) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(LeaderAndIsrZNode.path(tableBucket));
        return bytes.map(LeaderAndIsrZNode::decode);
    }

    public void updateLeaderAndIsr(TableBucket tableBucket, LeaderAndIsr leaderAndIsr)
            throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.setData().forPath(path, LeaderAndIsrZNode.encode(leaderAndIsr));
        LOG.info("Updated {} for bucket {} in Zookeeper.", leaderAndIsr, tableBucket);
    }

    public void deleteLeaderAndIsr(TableBucket tableBucket) throws Exception {
        String path = LeaderAndIsrZNode.path(tableBucket);
        zkClient.delete().forPath(path);
        LOG.info("Deleted LeaderAndIsr for bucket {} in Zookeeper.", tableBucket);
    }

    // --------------------------------------------------------------------------------------------
    // Database
    // --------------------------------------------------------------------------------------------
    public void registerDatabase(String database, DatabaseRegistration databaseRegistration)
            throws Exception {
        String path = DatabaseZNode.path(database);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, DatabaseZNode.encode(databaseRegistration));
        LOG.info("Registered database {}", database);
    }

    /** Get the database in ZK. */
    public Optional<DatabaseRegistration> getDatabase(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        Optional<byte[]> bytes = getOrEmpty(path);
        return bytes.map(DatabaseZNode::decode);
    }

    public void deleteDatabase(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public boolean databaseExists(String database) throws Exception {
        String path = DatabaseZNode.path(database);
        return zkClient.checkExists().forPath(path) != null;
    }

    public List<String> listDatabases() throws Exception {
        return getChildren(DatabasesZNode.path());
    }

    public List<String> listTables(String databaseName) throws Exception {
        return getChildren(TablesZNode.path(databaseName));
    }

    // --------------------------------------------------------------------------------------------
    // Table
    // --------------------------------------------------------------------------------------------

    /** generate a table id . */
    public long getTableIdAndIncrement() throws Exception {
        return tableIdCounter.getAndIncrement();
    }

    public long getPartitionIdAndIncrement() throws Exception {
        return partitionIdCounter.getAndIncrement();
    }

    /** Register table to ZK metadata. */
    public void registerTable(TablePath tablePath, TableRegistration tableRegistration)
            throws Exception {
        registerTable(tablePath, tableRegistration, true);
    }

    /**
     * Register table to ZK metadata.
     *
     * @param needCreateNode when register a table to zk, whether need to create the node of the
     *     path. In the case that we first register the schema to a path which will be the children
     *     path of the path to store the table, then register the table, we won't need to create the
     *     node again.
     */
    public void registerTable(
            TablePath tablePath, TableRegistration tableRegistration, boolean needCreateNode)
            throws Exception {
        String path = TableZNode.path(tablePath);
        byte[] tableBytes = TableZNode.encode(tableRegistration);
        if (needCreateNode) {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, tableBytes);
        } else {
            zkClient.setData().forPath(path, tableBytes);
        }
        LOG.info(
                "Registered table {} for database {}",
                tablePath.getTableName(),
                tablePath.getDatabaseName());
    }

    /** Get the table in ZK. */
    public Optional<TableRegistration> getTable(TablePath tablePath) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(TableZNode.path(tablePath));
        return bytes.map(TableZNode::decode);
    }

    /** Update the table in ZK. */
    public void updateTable(TablePath tablePath, TableRegistration tableRegistration)
            throws Exception {
        String path = TableZNode.path(tablePath);
        zkClient.setData().forPath(path, TableZNode.encode(tableRegistration));
        LOG.info(
                "Updated table {} for database {}",
                tablePath.getTableName(),
                tablePath.getDatabaseName());
    }

    /** Delete the table in ZK. */
    public void deleteTable(TablePath tablePath) throws Exception {
        String path = TableZNode.path(tablePath);
        zkClient.delete().deletingChildrenIfNeeded().forPath(path);
        LOG.info("Deleted table {}.", tablePath);
    }

    public boolean tableExist(TablePath tablePath) throws Exception {
        String path = TableZNode.path(tablePath);
        Stat stat = zkClient.checkExists().forPath(path);
        // when we create a table, we will first create a node with
        // path 'table_path/schemas/schema_id' to store the schema, so we can't use the path of
        // table 'table_path' exist or not to check the table exist or not.
        return stat != null && stat.getDataLength() > 0;
    }

    /** Get the partitions of a table in ZK. */
    public Set<String> getPartitions(TablePath tablePath) throws Exception {
        String path = PartitionsZNode.path(tablePath);
        return new HashSet<>(getChildren(path));
    }

    /** Get the partition and the id for the partitions of a table in ZK. */
    public Map<String, Long> getPartitionNameAndIds(TablePath tablePath) throws Exception {
        Map<String, Long> partitions = new HashMap<>();
        for (String partitionName : getPartitions(tablePath)) {
            Optional<TablePartition> optPartition = getPartition(tablePath, partitionName);
            optPartition.ifPresent(
                    partition -> partitions.put(partitionName, partition.getPartitionId()));
        }
        return partitions;
    }

    /** Get the id and name for the partitions of a table in ZK. */
    public Map<Long, String> getPartitionIdAndNames(TablePath tablePath) throws Exception {
        Map<Long, String> partitionIdAndNames = new HashMap<>();
        for (String partitionName : getPartitions(tablePath)) {
            Optional<TablePartition> optPartition = getPartition(tablePath, partitionName);
            optPartition.ifPresent(
                    partition ->
                            partitionIdAndNames.put(partition.getPartitionId(), partitionName));
        }
        return partitionIdAndNames;
    }

    /** Get a partition of a table in ZK. */
    public Optional<TablePartition> getPartition(TablePath tablePath, String partitionName)
            throws Exception {
        String path = PartitionZNode.path(tablePath, partitionName);
        return getOrEmpty(path).map(PartitionZNode::decode);
    }

    /** Get partition num of a table in ZK. */
    public int getPartitionNumber(TablePath tablePath) throws Exception {
        String path = PartitionsZNode.path(tablePath);
        Stat stat = zkClient.checkExists().forPath(path);
        if (stat == null) {
            return 0;
        }
        return stat.getNumChildren();
    }

    /** Create a partition for a table in ZK. */
    public void registerPartition(
            TablePath tablePath, long tableId, String partitionName, long partitionId)
            throws Exception {
        String path = PartitionZNode.path(tablePath, partitionName);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, PartitionZNode.encode(new TablePartition(tableId, partitionId)));
    }

    /** Delete a partition for a table in ZK. */
    public void deletePartition(TablePath tablePath, String partitionName) throws Exception {
        String path = PartitionZNode.path(tablePath, partitionName);
        zkClient.delete().forPath(path);
    }

    // --------------------------------------------------------------------------------------------
    // Schema
    // --------------------------------------------------------------------------------------------

    /** Register schema to ZK metadata and return the schema id. */
    public int registerSchema(TablePath tablePath, Schema schema) throws Exception {
        int currentSchemaId = getCurrentSchemaId(tablePath);
        // increase schema id.
        currentSchemaId++;
        String path = SchemaZNode.path(tablePath, currentSchemaId);
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, SchemaZNode.encode(schema));
        LOG.info("Registered new schema version {} for table {}.", currentSchemaId, tablePath);
        return currentSchemaId;
    }

    /** Get the specific schema by schema id in ZK metadata. */
    public Optional<SchemaInfo> getSchemaById(TablePath tablePath, int schemaId) throws Exception {
        Optional<byte[]> bytes = getOrEmpty(SchemaZNode.path(tablePath, schemaId));
        return bytes.map(b -> new SchemaInfo(SchemaZNode.decode(b), schemaId));
    }

    /** Gets the current schema id of the given table in ZK metadata. */
    public int getCurrentSchemaId(TablePath tablePath) throws Exception {
        Optional<Integer> currentSchemaId =
                getChildren(SchemasZNode.path(tablePath)).stream()
                        .map(Integer::parseInt)
                        .reduce(Math::max);
        return currentSchemaId.orElse(0);
    }

    // --------------------------------------------------------------------------------------------
    // Table Bucket snapshot
    // --------------------------------------------------------------------------------------------
    public void registerTableBucketSnapshot(TableBucket tableBucket, BucketSnapshot snapshot)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshot.getSnapshotId());
        zkClient.create()
                .creatingParentsIfNeeded()
                .forPath(path, BucketSnapshotIdZNode.encode(snapshot));
    }

    public void deleteTableBucketSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshotId);
        zkClient.delete().forPath(path);
    }

    public OptionalLong getTableBucketLatestSnapshotId(TableBucket tableBucket) throws Exception {
        String path = BucketSnapshotsZNode.path(tableBucket);
        return getChildren(path).stream().mapToLong(Long::parseLong).max();
    }

    public Optional<BucketSnapshot> getTableBucketSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception {
        String path = BucketSnapshotIdZNode.path(tableBucket, snapshotId);
        return getOrEmpty(path).map(BucketSnapshotIdZNode::decode);
    }

    /** Get the latest snapshot of the table bucket. */
    public Optional<BucketSnapshot> getTableBucketLatestSnapshot(TableBucket tableBucket)
            throws Exception {
        OptionalLong latestSnapshotId = getTableBucketLatestSnapshotId(tableBucket);
        if (latestSnapshotId.isPresent()) {
            return getTableBucketSnapshot(tableBucket, latestSnapshotId.getAsLong());
        } else {
            return Optional.empty();
        }
    }

    public List<Tuple2<BucketSnapshot, Long>> getTableBucketAllSnapshotAndIds(
            TableBucket tableBucket) throws Exception {
        String path = BucketSnapshotsZNode.path(tableBucket);
        List<Tuple2<BucketSnapshot, Long>> snapshotAndIds = new ArrayList<>();
        for (String snapshotId : getChildren(path)) {
            long snapshotIdLong = Long.parseLong(snapshotId);
            Optional<BucketSnapshot> optionalTableBucketSnapshot =
                    getTableBucketSnapshot(tableBucket, snapshotIdLong);
            optionalTableBucketSnapshot.ifPresent(
                    snapshot -> snapshotAndIds.add(Tuple2.of(snapshot, snapshotIdLong)));
        }
        return snapshotAndIds;
    }

    /**
     * Get all the latest snapshot for the buckets of the table. If no any buckets found for the
     * table in zk, return empty. The key of the map is the bucket id, the value is the optional
     * latest snapshot, empty if there is no snapshot for the kv bucket.
     */
    public Map<Integer, Optional<BucketSnapshot>> getTableLatestBucketSnapshot(long tableId)
            throws Exception {
        Optional<TableAssignment> optTableAssignment = getTableAssignment(tableId);
        if (!optTableAssignment.isPresent()) {
            return Collections.emptyMap();
        } else {
            TableAssignment tableAssignment = optTableAssignment.get();
            return getBucketSnapshots(tableId, null, tableAssignment);
        }
    }

    public Map<Integer, Optional<BucketSnapshot>> getPartitionLatestBucketSnapshot(long partitionId)
            throws Exception {
        Optional<PartitionAssignment> optPartitionAssignment = getPartitionAssignment(partitionId);
        if (!optPartitionAssignment.isPresent()) {
            return Collections.emptyMap();
        } else {
            return getBucketSnapshots(
                    optPartitionAssignment.get().getTableId(),
                    partitionId,
                    optPartitionAssignment.get());
        }
    }

    private Map<Integer, Optional<BucketSnapshot>> getBucketSnapshots(
            long tableId, @Nullable Long partitionId, TableAssignment tableAssignment)
            throws Exception {
        Map<Integer, Optional<BucketSnapshot>> snapshots = new HashMap<>();
        // first, put as empty for all buckets
        for (Integer bucket : tableAssignment.getBuckets()) {
            snapshots.put(bucket, Optional.empty());
        }

        // get the bucket ids
        String bucketIdsPath =
                partitionId == null
                        ? BucketIdsZNode.pathOfTable(tableId)
                        : BucketIdsZNode.pathOfPartition(partitionId);
        // iterate all buckets
        for (String bucketIdStr : getChildren(bucketIdsPath)) {
            // get the bucket id
            int bucketId = Integer.parseInt(bucketIdStr);
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            // get the snapshot node for the bucket
            String bucketSnapshotPath = BucketSnapshotsZNode.path(tableBucket);
            // get all the snapshots for the bucket
            List<String> bucketSnapshots = getChildren(bucketSnapshotPath);

            Optional<Long> optLatestSnapshotId =
                    bucketSnapshots.stream().map(Long::parseLong).reduce(Math::max);
            Optional<BucketSnapshot> optTableBucketSnapshot = Optional.empty();
            if (optLatestSnapshotId.isPresent()) {
                optTableBucketSnapshot =
                        getTableBucketSnapshot(tableBucket, optLatestSnapshotId.get());
            }
            snapshots.put(bucketId, optTableBucketSnapshot);
        }
        return snapshots;
    }

    // --------------------------------------------------------------------------------------------
    // Writer
    // --------------------------------------------------------------------------------------------

    /** generate an unique id for writer. */
    public long getWriterIdAndIncrement() throws Exception {
        return writerIdCounter.getAndIncrement();
    }

    // --------------------------------------------------------------------------------------------
    // Remote log manifest handler
    // --------------------------------------------------------------------------------------------

    /**
     * Register or update the remote log manifest handle to zookeeper.
     *
     * <p>Note: If there is already a remote log manifest for the given table bucket, it will be
     * overwritten.
     */
    public void upsertRemoteLogManifestHandle(
            TableBucket tableBucket, RemoteLogManifestHandle remoteLogManifestHandle)
            throws Exception {
        String path = BucketRemoteLogsZNode.path(tableBucket);
        if (getRemoteLogManifestHandle(tableBucket).isPresent()) {
            zkClient.setData().forPath(path, BucketRemoteLogsZNode.encode(remoteLogManifestHandle));
        } else {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, BucketRemoteLogsZNode.encode(remoteLogManifestHandle));
        }
    }

    public Optional<RemoteLogManifestHandle> getRemoteLogManifestHandle(TableBucket tableBucket)
            throws Exception {
        String path = BucketRemoteLogsZNode.path(tableBucket);
        return getOrEmpty(path).map(BucketRemoteLogsZNode::decode);
    }

    public void upsertLakeTableSnapshot(long tableId, LakeTableSnapshot lakeTableSnapshot)
            throws Exception {
        String path = LakeTableZNode.path(tableId);
        Optional<LakeTableSnapshot> optLakeTableSnapshot = getLakeTableSnapshot(tableId);
        if (optLakeTableSnapshot.isPresent()) {
            // we need to merge current lake table snapshot with previous
            // since the current lake table snapshot request won't carry all
            // the bucket for the table. It will only carry the bucket that is written
            // after the previous commit
            LakeTableSnapshot previous = optLakeTableSnapshot.get();

            // merge log startup offset, current will override the previous
            Map<TableBucket, Long> bucketLogStartOffset =
                    new HashMap<>(previous.getBucketLogStartOffset());
            bucketLogStartOffset.putAll(lakeTableSnapshot.getBucketLogStartOffset());

            // merge log end offsets, current will override the previous
            Map<TableBucket, Long> bucketLogEndOffset =
                    new HashMap<>(previous.getBucketLogEndOffset());
            bucketLogEndOffset.putAll(lakeTableSnapshot.getBucketLogEndOffset());

            lakeTableSnapshot =
                    new LakeTableSnapshot(
                            lakeTableSnapshot.getSnapshotId(),
                            lakeTableSnapshot.getTableId(),
                            bucketLogStartOffset,
                            bucketLogEndOffset);
            zkClient.setData().forPath(path, LakeTableZNode.encode(lakeTableSnapshot));
        } else {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .forPath(path, LakeTableZNode.encode(lakeTableSnapshot));
        }
    }

    public Optional<LakeTableSnapshot> getLakeTableSnapshot(long tableId) throws Exception {
        String path = LakeTableZNode.path(tableId);
        return getOrEmpty(path).map(LakeTableZNode::decode);
    }

    /**
     * Register or update the acl registration to zookeeper.
     *
     * <p>Note: If there is already an acl registration for the given resources and the acl version
     * is smaller than new one, it will be overwritten.(we need to compare the version before
     * upsert)
     */
    public void upsertResourceAcl(Resource resource, ResourceAcl newResourceAcl) throws Exception {
        String path = ResourceAclNode.path(resource);
        Optional<ResourceAcl> oldAclRegistration = getResourceAcl(resource);
        if (oldAclRegistration.isPresent()) {
            zkClient.setData().forPath(path, ResourceAclNode.encode(newResourceAcl));
        } else {
            LOG.info("insert acl node {} with value {}", resource, newResourceAcl);
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, ResourceAclNode.encode(newResourceAcl));
            LOG.info(
                    "update acl node {} from {} into {}",
                    resource,
                    oldAclRegistration,
                    newResourceAcl);
        }
    }

    /**
     * Retrieves the ACL (Access Control List) for a specific resource from ZooKeeper.
     *
     * @param resource the resource to query
     * @return an Optional containing the ResourceAcl if it exists, or empty if not found
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public Optional<ResourceAcl> getResourceAcl(Resource resource) throws Exception {
        String path = ResourceAclNode.path(resource);
        return getOrEmpty(path).map(ResourceAclNode::decode);
    }

    /**
     * Retrieves all resources of a specific type and their corresponding ACLs from ZooKeeper.
     *
     * @param resourceType the type of resource to query
     * @return a list of child node names representing resources of the given type
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public List<String> listResourcesByType(ResourceType resourceType) throws Exception {
        String path = ResourceAclNode.path(resourceType);
        return getChildren(path);
    }

    /**
     * Deletes the ACL (Access Control List) for a specific resource from ZooKeeper.
     *
     * @param resource the resource whose ACL should be deleted
     * @throws Exception if there is an error accessing ZooKeeper
     */
    public void deleteResourceAcl(Resource resource) throws Exception {
        String path = ResourceAclNode.path(resource);
        zkClient.delete().forPath(path);
    }

    public void insertAclChangeNotification(Resource resource) throws Exception {
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                .forPath(
                        AclChangeNotificationNode.pathPrefix(),
                        AclChangeNotificationNode.encode(resource));
        LOG.info("add acl change notification for resource {}  ", resource);
    }

    public void deleteAclChangeNotifications() throws Exception {
        List<String> children = getChildren(AclChangesNode.path());
        for (String child : children) {
            zkClient.delete().forPath(AclChangesNode.path() + "/" + child);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Utils
    // --------------------------------------------------------------------------------------------

    /**
     * Gets all the child nodes at a given zk node path.
     *
     * @param path the path to list children
     * @return list of child node names
     */
    public List<String> getChildren(String path) throws Exception {
        try {
            return zkClient.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return Collections.emptyList();
        }
    }

    /** Gets the data and stat of a given zk node path. */
    public Optional<Stat> getState(String path) throws Exception {
        try {
            Stat stat = zkClient.checkExists().forPath(path);
            LOG.info("stat of path {} is {}", path, stat);
            return Optional.of(stat);
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        }
    }

    /** delete a path. */
    public void deletePath(String path) throws Exception {
        try {
            zkClient.delete().forPath(path);
        } catch (KeeperException.NoNodeException ignored) {
        }
    }

    public CuratorFramework getCuratorClient() {
        return zkClient;
    }

    /** Close the underlying ZooKeeperClient. */
    @Override
    public void close() {
        LOG.info("Closing...");
        if (curatorFrameworkWrapper != null) {
            curatorFrameworkWrapper.close();
        }
    }
}
