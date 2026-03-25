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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BootstrapUpgradeState;
import org.apache.fluss.server.zk.data.BootstrapUpgradeStatus;

import javax.annotation.Nullable;

import java.util.Optional;

/** Coordinator-side manager for bootstrap-upgrade state access. */
public class BootstrapUpgradeStateManager {

    private final ZooKeeperClient zooKeeperClient;

    public BootstrapUpgradeStateManager(ZooKeeperClient zooKeeperClient) {
        this.zooKeeperClient = zooKeeperClient;
    }

    /** Initializes bootstrap-upgrade state with {@link BootstrapUpgradeStatus#IN_PROGRESS}. */
    public void initializeInProgress(long tableId, String holdPartition) {
        initializeInProgress(tableId, holdPartition, null);
    }

    /** Initializes bootstrap-upgrade state with {@link BootstrapUpgradeStatus#IN_PROGRESS}. */
    public void initializeInProgress(
            long tableId, String holdPartition, @Nullable Long holdPartitionId) {
        try {
            Optional<BootstrapUpgradeState> existing =
                    zooKeeperClient.getBootstrapUpgradeState(tableId);
            if (existing.isPresent()) {
                BootstrapUpgradeState existingState = existing.get();
                if (existingState.getStatus() == BootstrapUpgradeStatus.IN_PROGRESS
                        && existingState.getHoldPartition().equals(holdPartition)) {
                    // idempotent retries should not fail create-table/alter-table event processing
                    return;
                }
                throw new FlussRuntimeException(
                        String.format(
                                "Bootstrap-upgrade state already exists for table %d: %s",
                                tableId, existingState));
            }
            upsert(
                    tableId,
                    new BootstrapUpgradeState(
                            BootstrapUpgradeStatus.IN_PROGRESS, holdPartition, holdPartitionId),
                    false);
        } catch (FlussRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to initialize bootstrap-upgrade state for table %d.", tableId),
                    e);
        }
    }

    /**
     * Updates the holdPartitionId on an existing IN_PROGRESS bootstrap-upgrade state. This is used
     * after partition metadata is created early to store the assigned partition ID.
     */
    public void updateHoldPartitionId(long tableId, long holdPartitionId) {
        try {
            Optional<BootstrapUpgradeState> existing =
                    zooKeeperClient.getBootstrapUpgradeState(tableId);
            if (!existing.isPresent()) {
                throw new FlussRuntimeException(
                        String.format(
                                "Bootstrap-upgrade state does not exist for table %d.", tableId));
            }
            BootstrapUpgradeState existingState = existing.get();
            upsert(
                    tableId,
                    new BootstrapUpgradeState(
                            existingState.getStatus(),
                            existingState.getHoldPartition(),
                            holdPartitionId),
                    true);
        } catch (FlussRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to update holdPartitionId for table %d.", tableId), e);
        }
    }

    /** Marks bootstrap-upgrade state as {@link BootstrapUpgradeStatus#COMPLETE}. */
    public void markComplete(long tableId) {
        try {
            BootstrapUpgradeState existing =
                    get(tableId)
                            .orElseThrow(
                                    () ->
                                            new FlussRuntimeException(
                                                    String.format(
                                                            "Bootstrap-upgrade state does not exist for table %d.",
                                                            tableId)));
            upsert(
                    tableId,
                    new BootstrapUpgradeState(
                            BootstrapUpgradeStatus.COMPLETE,
                            existing.getHoldPartition(),
                            existing.getHoldPartitionId()),
                    true);
        } catch (FlussRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to mark bootstrap-upgrade state COMPLETE for table %d.",
                            tableId),
                    e);
        }
    }

    /** Loads bootstrap-upgrade state for the given table. */
    public Optional<BootstrapUpgradeState> get(long tableId) {
        try {
            return zooKeeperClient.getBootstrapUpgradeState(tableId);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format("Failed to load bootstrap-upgrade state for table %d.", tableId),
                    e);
        }
    }

    /** Deletes bootstrap-upgrade state for the given table. */
    public void delete(long tableId) {
        try {
            zooKeeperClient.deleteBootstrapUpgradeState(tableId);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to delete bootstrap-upgrade state for table %d.", tableId),
                    e);
        }
    }

    /** Deletes bootstrap-upgrade state if present for the given table. */
    public void deleteIfPresent(long tableId) {
        try {
            if (zooKeeperClient.getBootstrapUpgradeState(tableId).isPresent()) {
                zooKeeperClient.deleteBootstrapUpgradeState(tableId);
            }
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to delete bootstrap-upgrade state for table %d.", tableId),
                    e);
        }
    }

    private void upsert(long tableId, BootstrapUpgradeState state, boolean isUpdate) {
        try {
            zooKeeperClient.upsertBootstrapUpgradeState(tableId, state, isUpdate);
        } catch (Exception e) {
            throw new FlussRuntimeException(
                    String.format(
                            "Failed to upsert bootstrap-upgrade state for table %d.", tableId),
                    e);
        }
    }
}
