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

package org.apache.fluss.server.zk.data;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The persisted bootstrap-upgrade state for a table. */
public class BootstrapUpgradeState {

    private final BootstrapUpgradeStatus status;
    private final String holdPartition;

    public BootstrapUpgradeState(BootstrapUpgradeStatus status, String holdPartition) {
        this.status = checkNotNull(status, "status must not be null.");
        this.holdPartition = checkNotNull(holdPartition, "holdPartition must not be null.");
    }

    public BootstrapUpgradeStatus getStatus() {
        return status;
    }

    public String getHoldPartition() {
        return holdPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BootstrapUpgradeState that = (BootstrapUpgradeState) o;
        return status == that.status && Objects.equals(holdPartition, that.holdPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, holdPartition);
    }

    @Override
    public String toString() {
        return "BootstrapUpgradeState{"
                + "status="
                + status
                + ", holdPartition='"
                + holdPartition
                + '\''
                + '}';
    }
}
