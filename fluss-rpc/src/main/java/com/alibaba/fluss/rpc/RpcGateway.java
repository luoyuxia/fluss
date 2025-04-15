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

package com.alibaba.fluss.rpc;

import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.messages.AuthenticateRequest;
import com.alibaba.fluss.rpc.messages.AuthenticateResponse;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.RPC;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.CompletableFuture;

/** Rpc gateway interface which has to be implemented by Rpc gateways. */
public interface RpcGateway {

    /** Returns the APIs and Versions of the RPC Gateway supported. */
    @RPC(api = ApiKeys.API_VERSIONS)
    CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request);

    /**
     * This method just to registers the AUTHENTICATE API in the API manager for client-side
     * request/response object generation.
     *
     * <p>This method does not handle the authentication logic itself. Instead, the {@link
     * AuthenticateRequest} is processed preemptively by {@link
     * com.alibaba.fluss.rpc.netty.server.NettyServerHandler} during the initial connection
     * handshake. The client uses this method definition to generate corresponding request/response
     * objects for API version compatibility.
     *
     * @param request The authenticate request (not used in this method's implementation).
     * @return Always returns {@code null} since the actual authentication handling occurs in {@link
     *     com.alibaba.fluss.rpc.netty.server.NettyServerHandler}.
     * @see com.alibaba.fluss.rpc.netty.server.NettyServerHandler#channelRead(ChannelHandlerContext,
     *     Object) For authentication processing implementation.
     */
    @RPC(api = ApiKeys.AUTHENTICATE)
    default CompletableFuture<AuthenticateResponse> authenticate(AuthenticateRequest request) {
        return CompletableFuture.completedFuture(new AuthenticateResponse());
    }
}
