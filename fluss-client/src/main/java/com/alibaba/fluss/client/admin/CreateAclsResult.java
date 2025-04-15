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

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.rpc.messages.PbCreateAclRespInfo;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.security.acl.AclBinding;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toAclBinding;

/**
 * Represents the result of a batch ACL operation, managing asynchronous completion of individual
 * ACL operations.
 *
 * <p>This class tracks the execution status of multiple ACL operations (e.g., create/drop) by
 * associating each {@link AclBinding} with its corresponding {@link CompletableFuture}. It
 * processes RPC responses to complete or fail individual futures based on server-side results.
 *
 * @since 0.6
 */
public class CreateAclsResult {
    private final Map<AclBinding, CompletableFuture<Void>> futures;

    public CreateAclsResult(Map<AclBinding, CompletableFuture<Void>> futures) {
        this.futures = futures;
    }

    public CompletableFuture<Void> all() {
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]));
    }

    /**
     * Gets the map of ACL bindings to their associated futures.
     *
     * @return The map of ACL bindings to futures.
     */
    public Map<AclBinding, CompletableFuture<Void>> getFutures() {
        return futures;
    }

    /**
     * Completes individual futures based on RPC response information.
     *
     * <p>For each {@link PbCreateAclRespInfo} in the collection, Completes the future with success
     * or failure based on the response's error code.
     *
     * @param pbAclRespInfos Collection of protobuf response messages containing ACL operation
     *     results.
     */
    public void complete(List<PbCreateAclRespInfo> pbAclRespInfos) {
        pbAclRespInfos.forEach(
                pbAclRespInfo -> {
                    AclBinding aclBinding = toAclBinding(pbAclRespInfo.getAcl());
                    CompletableFuture<Void> future = futures.get(aclBinding);
                    if (pbAclRespInfo.hasErrorCode()
                            && pbAclRespInfo.getErrorCode() != Errors.NONE.code()) {
                        future.completeExceptionally(
                                Errors.forCode(pbAclRespInfo.getErrorCode())
                                        .exception(pbAclRespInfo.getErrorMessage()));
                    } else {
                        future.complete(null);
                    }
                });
    }

    /**
     * Marks all futures as exceptionally completed with the provided throwable.
     *
     * <p>This method propagates a common exception (e.g., network error) to all tracked futures.
     *
     * @param t The throwable to propagate to all futures
     */
    public void completeExceptionally(Throwable t) {
        futures.values().forEach(future -> future.completeExceptionally(t));
    }
}
