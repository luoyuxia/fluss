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

package com.alibaba.fluss.server.authorizer;

import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;

/** The action to operator. */
public class Action {
    private final Resource resource;
    private final OperationType operation;

    public Action(Resource resource, OperationType operation) {
        this.resource = resource;
        this.operation = operation;
    }

    public Resource getResource() {
        return resource;
    }

    public OperationType getOperation() {
        return operation;
    }
}
