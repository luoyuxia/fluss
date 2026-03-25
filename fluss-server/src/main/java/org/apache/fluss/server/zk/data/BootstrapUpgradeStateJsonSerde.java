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

import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;

/** Json serializer and deserializer for {@link BootstrapUpgradeState}. */
public class BootstrapUpgradeStateJsonSerde
        implements JsonSerializer<BootstrapUpgradeState>, JsonDeserializer<BootstrapUpgradeState> {

    public static final BootstrapUpgradeStateJsonSerde INSTANCE =
            new BootstrapUpgradeStateJsonSerde();

    private static final String VERSION_KEY = "version";
    private static final String STATUS = "status";
    private static final String HOLD_PARTITION = "hold_partition";
    private static final String HOLD_PARTITION_ID = "hold_partition_id";

    private static final int VERSION = 1;

    @Override
    public void serialize(BootstrapUpgradeState bootstrapUpgradeState, JsonGenerator generator)
            throws IOException {
        generator.writeStartObject();
        generator.writeNumberField(VERSION_KEY, VERSION);
        generator.writeStringField(STATUS, bootstrapUpgradeState.getStatus().name());
        generator.writeStringField(HOLD_PARTITION, bootstrapUpgradeState.getHoldPartition());
        if (bootstrapUpgradeState.getHoldPartitionId() != null) {
            generator.writeNumberField(
                    HOLD_PARTITION_ID, bootstrapUpgradeState.getHoldPartitionId());
        }
        generator.writeEndObject();
    }

    @Override
    public BootstrapUpgradeState deserialize(JsonNode node) {
        Long holdPartitionId = null;
        if (node.has(HOLD_PARTITION_ID)) {
            holdPartitionId = node.get(HOLD_PARTITION_ID).asLong();
        }
        return new BootstrapUpgradeState(
                BootstrapUpgradeStatus.valueOf(node.get(STATUS).asText()),
                node.get(HOLD_PARTITION).asText(),
                holdPartitionId);
    }
}
