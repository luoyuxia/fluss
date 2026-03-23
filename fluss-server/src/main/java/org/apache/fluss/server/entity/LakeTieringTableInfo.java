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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.LakeTieringTaskType;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.fluss.metadata.LakeTieringTaskType.NORMAL_TIERING;

/** The info for the table assigned from Coordinator to lake tiering service to do tiering. */
public class LakeTieringTableInfo {

    private final long tableId;
    private final TablePath tablePath;
    private final long tieringEpoch;
    private final LakeTieringTaskType taskType;
    private final @Nullable String holdPartition;

    public LakeTieringTableInfo(long tableId, TablePath tablePath, long tieringEpoch) {
        this(tableId, tablePath, tieringEpoch, NORMAL_TIERING, null);
    }

    public LakeTieringTableInfo(
            long tableId,
            TablePath tablePath,
            long tieringEpoch,
            LakeTieringTaskType taskType,
            @Nullable String holdPartition) {
        this.tableId = tableId;
        this.tablePath = tablePath;
        this.tieringEpoch = tieringEpoch;
        this.taskType = taskType;
        this.holdPartition = holdPartition;
    }

    public long tableId() {
        return tableId;
    }

    public TablePath tablePath() {
        return tablePath;
    }

    public long tieringEpoch() {
        return tieringEpoch;
    }

    public LakeTieringTaskType taskType() {
        return taskType;
    }

    public @Nullable String holdPartition() {
        return holdPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LakeTieringTableInfo that = (LakeTieringTableInfo) o;
        return tableId == that.tableId
                && tieringEpoch == that.tieringEpoch
                && taskType == that.taskType
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(holdPartition, that.holdPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, tablePath, tieringEpoch, taskType, holdPartition);
    }

    @Override
    public String toString() {
        return "LakeTieringTableInfo{"
                + "tableId="
                + tableId
                + ", tablePath="
                + tablePath
                + ", tieringEpoch="
                + tieringEpoch
                + ", taskType="
                + taskType
                + ", holdPartition='"
                + holdPartition
                + '\''
                + '}';
    }
}
