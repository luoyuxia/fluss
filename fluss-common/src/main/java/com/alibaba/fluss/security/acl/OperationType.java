/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.security.acl;

/**
 * Enumeration representing operation types used in ACL (Access Control List) systems.
 *
 * <p><b>Permission Inheritance Rules:</b>
 *
 * <p>1. {@link #ALL} grants permission for all operations
 *
 * <p>2. {@link #READ}, {@link #WRITE}, {@link #CREATE}, {@link #DROP}, and {@link #ALTER}
 * implicitly include {@link #DESCRIBE}
 *
 * <p>3. When a user is granted {@link #WRITE} permission on any resource: - Automatically grants
 * {@link #IDEMPOTENT_WRITE} permission permissions on cluster.
 *
 * <p>4. When a user is granted {@link #READ} permission on any resource: - Automatically grants
 * {@link #FILESYSTEM_TOKEN} permission on cluster.
 */
public enum OperationType {
    /** In a filter, matches any OperationType. */
    ANY((byte) 1),
    ALL((byte) 2),
    READ((byte) 3),
    WRITE((byte) 4),
    CREATE((byte) 5),
    DROP((byte) 6),
    ALTER((byte) 7),
    DESCRIBE((byte) 8),
    IDEMPOTENT_WRITE((byte) 12),
    FILESYSTEM_TOKEN((byte) 500);

    private final byte code;

    OperationType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static OperationType fromCode(byte code) {
        for (OperationType operationType : OperationType.values()) {
            if (operationType.code == code) {
                return operationType;
            }
        }
        throw new IllegalArgumentException("Unknown operation type code: " + code);
    }
}
