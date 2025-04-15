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

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.exception.NetworkException;
import com.alibaba.fluss.record.send.Send;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.AuthenticateRequest;
import com.alibaba.fluss.rpc.messages.AuthenticateResponse;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.rpc.protocol.ApiMethod;
import com.alibaba.fluss.rpc.protocol.MessageCodec;
import com.alibaba.fluss.security.auth.ServerAuthenticator;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelFutureListener;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.timeout.IdleState;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import com.alibaba.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeErrorResponse;
import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeServerFailure;
import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeSuccessResponse;

/** Implementation of the channel handler to process inbound requests for RPC server. */
public final class NettyServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(NettyServerHandler.class);

    /**
     * Map from API key to inflight responses. In here, we need to use a Queue to store the inflight
     * responses of each API key, because fluss clients require the responses to be sent in order.
     */
    private final Map<Short, Deque<FlussRequest>> inflightResponseMap = new HashMap<>();

    private final RequestChannel requestChannel;
    private final ApiManager apiManager;
    private final String listenerName;
    private final RequestsMetrics requestsMetrics;
    private volatile ChannelHandlerContext ctx;
    private SocketAddress remoteAddress;

    private final ServerAuthenticator authenticator;

    private volatile ConnectionState state;

    public NettyServerHandler(
            RequestChannel requestChannel,
            ApiManager apiManager,
            String listenerName,
            RequestsMetrics requestsMetrics,
            ServerAuthenticator authenticator) {
        this.requestChannel = requestChannel;
        this.apiManager = apiManager;
        this.listenerName = listenerName;
        this.requestsMetrics = requestsMetrics;
        this.authenticator = authenticator;
        this.state = ConnectionState.START;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        CompletableFuture<ApiMessage> future = new CompletableFuture<>();
        ByteBuf buffer = (ByteBuf) msg;
        int frameLength = buffer.readInt();
        short apiKey = buffer.readShort();
        short apiVersion = buffer.readShort();
        int requestId = buffer.readInt();
        int messageSize = frameLength - MessageCodec.REQUEST_HEADER_LENGTH;

        boolean needRelease = false;
        try {
            ApiMethod api = apiManager.getApi(apiKey);
            if (api == null) {
                LOG.warn("Received unknown API key {}.", apiKey);
                needRelease = true;
                return;
            }

            ApiMessage requestMessage = api.getRequestConstructor().get();
            requestMessage.parseFrom(buffer, messageSize);
            // Most request types are parsed entirely into objects at this point. For those we can
            // release the underlying buffer.
            // However, some (like produce) retain a reference to the buffer. For those requests we
            // cannot release the buffer early, but only when request processing is done.
            if (!requestMessage.isLazilyParsed()) {
                needRelease = true;
            }

            FlussRequest request =
                    new FlussRequest(
                            apiKey,
                            apiVersion,
                            requestId,
                            api,
                            requestMessage,
                            buffer,
                            listenerName,
                            future);

            Deque<FlussRequest> inflightResponses =
                    inflightResponseMap.computeIfAbsent(apiKey, k -> new ArrayDeque<>());
            inflightResponses.addLast(request);
            future.whenCompleteAsync((r, t) -> sendResponse(ctx, apiKey), ctx.executor());
            if (state.isAuthenticating() && apiKey != ApiKeys.API_VERSIONS.id) {
                handleAuthenticateRequest(apiKey, requestMessage, future);
            } else {
                requestChannel.putRequest(request);
            }

            if (!state.isActive()) {
                LOG.warn("Received a request on an inactive channel: {}", remoteAddress);
                request.fail(new NetworkException("Channel is inactive"));
                needRelease = true;
            }
        } catch (Throwable t) {
            needRelease = true;
            LOG.error("Error while parsing request.", t);
            future.completeExceptionally(t);
        } finally {
            if (needRelease) {
                buffer.release();
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
        switchState(
                authenticator.isComplete()
                        ? ConnectionState.READY
                        : ConnectionState.AUTHENTICATING);

        // TODO: connection metrics (count, client tags, receive request avg idle time, etc.)
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                LOG.warn("Connection {} is idle, closing...", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // debug level to avoid too many logs if NLB(Network Load Balancer is mounted, see
        // more detail in #377
        // may revert to warn level if we found warn level is necessary
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Connection [{}] got exception in Netty server pipeline: \n{}",
                    ctx.channel().remoteAddress(),
                    ExceptionUtils.stringifyException(cause));
        }

        ByteBuf byteBuf = encodeServerFailure(ctx.alloc(), ApiError.fromThrowable(cause));
        ctx.writeAndFlush(byteBuf).addListener(ChannelFutureListener.CLOSE);
        close();
    }

    private void close() {
        switchState(ConnectionState.CLOSE);
        ctx.close();
        LOG.warn(
                "Close channel {} with {} pending requests.",
                remoteAddress,
                inflightResponseMap.size());
        inflightResponseMap.forEach((k, v) -> v.forEach(FlussRequest::cancel));
    }

    private void sendResponse(ChannelHandlerContext ctx, short apiKey) {
        FlussRequest request;
        Deque<FlussRequest> inflightResponses = inflightResponseMap.get(apiKey);
        while ((request = inflightResponses.peekFirst()) != null) {
            CompletableFuture<ApiMessage> f = request.getResponseFuture();
            boolean cancelled = request.cancelled();

            if (cancelled) {
                inflightResponses.pollFirst();
                request.releaseBuffer();
                continue;
            }

            boolean isDone = f.isDone();
            if (!isDone) {
                break;
            }

            inflightResponses.pollFirst();
            if (state.isActive()) {
                try {
                    ApiMessage response = f.join();
                    sendSuccessResponse(ctx, request, response);
                } catch (Throwable t) {
                    sendError(ctx, request, t);
                }
            } else {
                request.releaseBuffer();
            }
        }
    }

    private void sendSuccessResponse(
            ChannelHandlerContext ctx, FlussRequest request, ApiMessage responseMessage) {
        // TODO: use a memory managed allocator
        ByteBufAllocator alloc = ctx.alloc();
        try {
            Send send = encodeSuccessResponse(alloc, request.getRequestId(), responseMessage);
            send.writeTo(ctx);
            ctx.flush();
            long requestEndTimeMs = System.currentTimeMillis();
            updateRequestMetrics(request, requestEndTimeMs);
        } catch (Throwable t) {
            LOG.error("Failed to send response to client.", t);
            sendError(ctx, request, t);
        }
    }

    private void sendError(ChannelHandlerContext ctx, FlussRequest request, Throwable t) {
        ApiError error = ApiError.fromThrowable(t);
        // TODO: use a memory managed allocator
        ByteBufAllocator alloc = ctx.alloc();
        ByteBuf byteBuf = encodeErrorResponse(alloc, request.getRequestId(), error);
        ctx.writeAndFlush(byteBuf);

        getMetrics(request).ifPresent(metrics -> metrics.getErrorsCount().inc());
    }

    private void updateRequestMetrics(FlussRequest request, long requestEndTimeMs) {
        // get the metrics to be updated for this kind of request
        Optional<RequestsMetrics.Metrics> optMetrics = getMetrics(request);
        // no any metrics registered for the kind of request
        if (!optMetrics.isPresent()) {
            return;
        }

        // now, we need to update metrics
        RequestsMetrics.Metrics metrics = optMetrics.get();

        metrics.getRequestsCount().inc();
        metrics.getRequestBytes().update(request.getMessage().totalSize());

        // update metrics related to time
        long requestDequeTimeMs = request.getRequestDequeTimeMs();
        long requestCompletedTimeMs = request.getRequestCompletedTimeMs();
        metrics.getRequestQueueTimeMs().update(requestDequeTimeMs - request.getStartTimeMs());
        metrics.getRequestProcessTimeMs().update(requestCompletedTimeMs - requestDequeTimeMs);
        metrics.getResponseSendTimeMs().update(requestEndTimeMs - requestCompletedTimeMs);
        metrics.getTotalTimeMs().update(requestEndTimeMs - request.getStartTimeMs());
    }

    private Optional<RequestsMetrics.Metrics> getMetrics(FlussRequest request) {
        boolean isFromFollower = false;
        ApiMessage requestMessage = request.getMessage();
        if (request.getApiKey() == ApiKeys.FETCH_LOG.id) {
            // for fetch, we need to identify it's from client or follower
            FetchLogRequest fetchLogRequest = (FetchLogRequest) requestMessage;
            isFromFollower = fetchLogRequest.getFollowerServerId() >= 0;
        }
        return requestsMetrics.getMetrics(request.getApiKey(), isFromFollower);
    }

    @VisibleForTesting
    Deque<FlussRequest> inflightResponses(short apiKey) {
        return inflightResponseMap.get(apiKey);
    }

    private void handleAuthenticateRequest(
            short apiKey, ApiMessage requestMessage, CompletableFuture<ApiMessage> future) {
        if (apiKey != ApiKeys.AUTHENTICATE.id) {
            LOG.warn(
                    "Connection is still in the authentication process. Unable to handle API key: {}.",
                    apiKey);
            future.completeExceptionally(
                    new AuthenticationException(
                            "The connection has not completed authentication yet."));
            return;
        }

        AuthenticateRequest authenticateRequest = (AuthenticateRequest) requestMessage;
        if (!authenticator.protocol().equals(authenticateRequest.getProtocol())) {
            future.completeExceptionally(
                    new AuthenticationException(
                            String.format(
                                    "Authenticate protocol not match: protocol of server is %s while protocol of client is %s",
                                    authenticator.protocol(), authenticateRequest.getProtocol())));
            return;
        }

        AuthenticateResponse authenticateResponse = new AuthenticateResponse();
        if (!authenticator.isComplete()) {
            byte[] token = authenticateRequest.getToken();
            byte[] response =
                    !authenticator.isComplete()
                            ? authenticator.evaluateResponse(token)
                            : new byte[0];
            authenticateResponse.setChallenge(response);
        }
        future.complete(authenticateResponse);

        if (authenticator.isComplete()) {
            switchState(ConnectionState.READY);
        }
    }

    private void switchState(ConnectionState targetState) {
        LOG.info("switch state form {} to {}", state, targetState);
        state = targetState;
    }

    private enum ConnectionState {
        START,
        AUTHENTICATING,
        READY,
        CLOSE;

        public boolean isActive() {
            return this == AUTHENTICATING || this == READY;
        }

        public boolean isAuthenticating() {
            return this == AUTHENTICATING;
        }
    }
}
