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

import org.apache.fluss.utils.json.JsonSerdeTestBase;

/** Test for {@link BootstrapUpgradeStateJsonSerde}. */
class BootstrapUpgradeStateJsonSerdeTest extends JsonSerdeTestBase<BootstrapUpgradeState> {

    BootstrapUpgradeStateJsonSerdeTest() {
        super(BootstrapUpgradeStateJsonSerde.INSTANCE);
    }

    @Override
    protected BootstrapUpgradeState[] createObjects() {
        return new BootstrapUpgradeState[] {
            new BootstrapUpgradeState(BootstrapUpgradeStatus.IN_PROGRESS, "dt=2026-03-23"),
            new BootstrapUpgradeState(BootstrapUpgradeStatus.COMPLETE, "dt=2026-03-24")
        };
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {
            "{\"version\":1,\"status\":\"IN_PROGRESS\",\"hold_partition\":\"dt=2026-03-23\"}",
            "{\"version\":1,\"status\":\"COMPLETE\",\"hold_partition\":\"dt=2026-03-24\"}"
        };
    }
}
