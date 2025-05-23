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

package com.alibaba.fluss.lakehouse.paimon.source.event;

import com.alibaba.fluss.metadata.TablePath;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.Map;

/** A source event to represent tables are removed to send from enumerator to reader. */
public class TablesRemovedEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final Map<Long, TablePath> removedTables;

    public TablesRemovedEvent(Map<Long, TablePath> removedTables) {
        this.removedTables = removedTables;
    }

    public Map<Long, TablePath> getRemovedTables() {
        return removedTables;
    }

    @Override
    public String toString() {
        return "TablesRemovedEvent{" + "removedTables=" + removedTables + '}';
    }
}
