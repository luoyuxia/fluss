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

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.CorruptedFrameException;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.DecoderException;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.fluss.shaded.netty4.io.netty.util.internal.ObjectUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteOrder;
import java.util.List;

/** hello docs. */
public class MyLengthFieldBasedFrameDecoder extends ByteToMessageDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(MyLengthFieldBasedFrameDecoder.class);

    private final ByteOrder byteOrder;
    private final int maxFrameLength;
    private final int lengthFieldOffset;
    private final int lengthFieldLength;
    private final int lengthFieldEndOffset;
    private final int lengthAdjustment;
    private final int initialBytesToStrip;
    private final boolean failFast;
    private boolean discardingTooLongFrame;
    private long tooLongFrameLength;
    private long bytesToDiscard;
    private int frameLengthInt;
    private final ServerNode serverNode;

    public MyLengthFieldBasedFrameDecoder(
            int maxFrameLength,
            int lengthFieldOffset,
            int lengthFieldLength,
            ServerNode serverNode) {
        this(maxFrameLength, lengthFieldOffset, lengthFieldLength, 0, 0, serverNode);
    }

    public MyLengthFieldBasedFrameDecoder(
            int maxFrameLength,
            int lengthFieldOffset,
            int lengthFieldLength,
            int lengthAdjustment,
            int initialBytesToStrip,
            ServerNode serverNode) {
        this(
                maxFrameLength,
                lengthFieldOffset,
                lengthFieldLength,
                lengthAdjustment,
                initialBytesToStrip,
                true,
                serverNode);
    }

    public MyLengthFieldBasedFrameDecoder(
            int maxFrameLength,
            int lengthFieldOffset,
            int lengthFieldLength,
            int lengthAdjustment,
            int initialBytesToStrip,
            boolean failFast,
            ServerNode serverNode) {
        this(
                ByteOrder.BIG_ENDIAN,
                maxFrameLength,
                lengthFieldOffset,
                lengthFieldLength,
                lengthAdjustment,
                initialBytesToStrip,
                failFast,
                serverNode);
    }

    public MyLengthFieldBasedFrameDecoder(
            ByteOrder byteOrder,
            int maxFrameLength,
            int lengthFieldOffset,
            int lengthFieldLength,
            int lengthAdjustment,
            int initialBytesToStrip,
            boolean failFast,
            ServerNode serverNode) {
        this.frameLengthInt = -1;
        this.byteOrder = (ByteOrder) ObjectUtil.checkNotNull(byteOrder, "byteOrder");
        ObjectUtil.checkPositive(maxFrameLength, "maxFrameLength");
        ObjectUtil.checkPositiveOrZero(lengthFieldOffset, "lengthFieldOffset");
        ObjectUtil.checkPositiveOrZero(initialBytesToStrip, "initialBytesToStrip");
        if (lengthFieldOffset > maxFrameLength - lengthFieldLength) {
            throw new IllegalArgumentException(
                    "maxFrameLength ("
                            + maxFrameLength
                            + ") must be equal to or greater than lengthFieldOffset ("
                            + lengthFieldOffset
                            + ") + lengthFieldLength ("
                            + lengthFieldLength
                            + ").");
        } else {
            this.maxFrameLength = maxFrameLength;
            this.lengthFieldOffset = lengthFieldOffset;
            this.lengthFieldLength = lengthFieldLength;
            this.lengthAdjustment = lengthAdjustment;
            this.lengthFieldEndOffset = lengthFieldOffset + lengthFieldLength;
            this.initialBytesToStrip = initialBytesToStrip;
            this.failFast = failFast;
        }

        this.serverNode = serverNode;
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOG.info("channelRead start of node {}", serverNode);
        super.channelRead(ctx, msg);
        LOG.info("channelRead end of node {}", serverNode);
    }

    protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
            throws Exception {
        LOG.info("decode start of node {}", serverNode);
        Object decoded = this.decode(ctx, in);
        if (decoded != null) {
            out.add(decoded);
        }
        LOG.info("decode end, send to handler {}", serverNode);
    }

    private void discardingTooLongFrame(ByteBuf in) {
        long bytesToDiscard = this.bytesToDiscard;
        int localBytesToDiscard = (int) Math.min(bytesToDiscard, (long) in.readableBytes());
        in.skipBytes(localBytesToDiscard);
        bytesToDiscard -= (long) localBytesToDiscard;
        this.bytesToDiscard = bytesToDiscard;
        this.failIfNecessary(false);
    }

    private static void failOnNegativeLengthField(
            ByteBuf in, long frameLength, int lengthFieldEndOffset) {
        in.skipBytes(lengthFieldEndOffset);
        throw new CorruptedFrameException("negative pre-adjustment length field: " + frameLength);
    }

    private static void failOnFrameLengthLessThanLengthFieldEndOffset(
            ByteBuf in, long frameLength, int lengthFieldEndOffset) {
        in.skipBytes(lengthFieldEndOffset);
        throw new CorruptedFrameException(
                "Adjusted frame length ("
                        + frameLength
                        + ") is less than lengthFieldEndOffset: "
                        + lengthFieldEndOffset);
    }

    private void exceededFrameLength(ByteBuf in, long frameLength) {
        long discard = frameLength - (long) in.readableBytes();
        this.tooLongFrameLength = frameLength;
        if (discard < 0L) {
            in.skipBytes((int) frameLength);
        } else {
            this.discardingTooLongFrame = true;
            this.bytesToDiscard = discard;
            in.skipBytes(in.readableBytes());
        }

        this.failIfNecessary(true);
    }

    private static void failOnFrameLengthLessThanInitialBytesToStrip(
            ByteBuf in, long frameLength, int initialBytesToStrip) {
        in.skipBytes((int) frameLength);
        throw new CorruptedFrameException(
                "Adjusted frame length ("
                        + frameLength
                        + ") is less than initialBytesToStrip: "
                        + initialBytesToStrip);
    }

    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        long frameLength = 0L;
        int readerIndex;
        if (this.frameLengthInt == -1) {
            if (this.discardingTooLongFrame) {
                this.discardingTooLongFrame(in);
            }

            if (in.readableBytes() < this.lengthFieldEndOffset) {
                return null;
            }

            readerIndex = in.readerIndex() + this.lengthFieldOffset;
            frameLength =
                    this.getUnadjustedFrameLength(
                            in, readerIndex, this.lengthFieldLength, this.byteOrder);
            if (frameLength < 0L) {
                failOnNegativeLengthField(in, frameLength, this.lengthFieldEndOffset);
            }

            frameLength += (long) (this.lengthAdjustment + this.lengthFieldEndOffset);
            if (frameLength < (long) this.lengthFieldEndOffset) {
                failOnFrameLengthLessThanLengthFieldEndOffset(
                        in, frameLength, this.lengthFieldEndOffset);
            }

            if (frameLength > (long) this.maxFrameLength) {
                this.exceededFrameLength(in, frameLength);
                return null;
            }

            this.frameLengthInt = (int) frameLength;
        }

        if (in.readableBytes() < this.frameLengthInt) {
            return null;
        } else {
            if (this.initialBytesToStrip > this.frameLengthInt) {
                failOnFrameLengthLessThanInitialBytesToStrip(
                        in, frameLength, this.initialBytesToStrip);
            }

            in.skipBytes(this.initialBytesToStrip);
            readerIndex = in.readerIndex();
            int actualFrameLength = this.frameLengthInt - this.initialBytesToStrip;
            ByteBuf frame = this.extractFrame(ctx, in, readerIndex, actualFrameLength);
            in.readerIndex(readerIndex + actualFrameLength);
            this.frameLengthInt = -1;
            return frame;
        }
    }

    protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
        buf = buf.order(order);
        long frameLength;
        switch (length) {
            case 1:
                frameLength = (long) buf.getUnsignedByte(offset);
                break;
            case 2:
                frameLength = (long) buf.getUnsignedShort(offset);
                break;
            case 3:
                frameLength = (long) buf.getUnsignedMedium(offset);
                break;
            case 4:
                frameLength = buf.getUnsignedInt(offset);
                break;
            case 5:
            case 6:
            case 7:
            default:
                throw new DecoderException(
                        "unsupported lengthFieldLength: "
                                + this.lengthFieldLength
                                + " (expected: 1, 2, 3, 4, or 8)");
            case 8:
                frameLength = buf.getLong(offset);
        }

        return frameLength;
    }

    private void failIfNecessary(boolean firstDetectionOfTooLongFrame) {
        if (this.bytesToDiscard == 0L) {
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0L;
            this.discardingTooLongFrame = false;
            if (!this.failFast || firstDetectionOfTooLongFrame) {
                this.fail(tooLongFrameLength);
            }
        } else if (this.failFast && firstDetectionOfTooLongFrame) {
            this.fail(this.tooLongFrameLength);
        }
    }

    protected ByteBuf extractFrame(
            ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
        return buffer.retainedSlice(index, length);
    }

    private void fail(long frameLength) {
        if (frameLength > 0L) {
            throw new TooLongFrameException(
                    "Adjusted frame length exceeds "
                            + this.maxFrameLength
                            + ": "
                            + frameLength
                            + " - discarded");
        } else {
            throw new TooLongFrameException(
                    "Adjusted frame length exceeds " + this.maxFrameLength + " - discarding");
        }
    }
}
