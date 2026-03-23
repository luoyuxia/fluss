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

package org.apache.fluss.metadata;

/** The task type assigned to the lake tiering service. */
public enum LakeTieringTaskType {
    NORMAL_TIERING(0),
    BOOTSTRAP_UPGRADE(1);

    private final int code;

    LakeTieringTaskType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static LakeTieringTaskType fromCode(int code) {
        for (LakeTieringTaskType value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown lake tiering task type code: " + code);
    }
}
