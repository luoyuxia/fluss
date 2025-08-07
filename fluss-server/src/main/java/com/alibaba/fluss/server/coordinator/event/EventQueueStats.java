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

package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.utils.MapUtils;

import java.util.HashMap;
import java.util.Map;

/** Maintains statistics about event types in the coordinator event queue. */
@Internal
public class EventQueueStats {
    private final Map<Class<? extends CoordinatorEvent>, Integer> eventTypeCounts;

    public EventQueueStats() {
        this.eventTypeCounts = MapUtils.newConcurrentHashMap();
    }

    /** Increment the count for a specific event type when it's added to the queue. */
    public void incrementEventCount(Class<? extends CoordinatorEvent> eventType) {
        eventTypeCounts.compute(eventType, (key, value) -> value == null ? 1 : value + 1);
    }

    /** Decrement the count for a specific event type when it's removed from the queue. */
    public void decrementEventCount(Class<? extends CoordinatorEvent> eventType) {
        eventTypeCounts.computeIfPresent(
                eventType,
                (key, value) -> {
                    int newValue = value - 1;
                    return newValue <= 0 ? null : newValue;
                });
    }

    /** Get a snapshot of current event type counts. */
    public Map<Class<? extends CoordinatorEvent>, Integer> getEventTypeCounts() {
        return new HashMap<>(eventTypeCounts);
    }

    /** Clear all statistics. */
    public void clear() {
        eventTypeCounts.clear();
    }
}
