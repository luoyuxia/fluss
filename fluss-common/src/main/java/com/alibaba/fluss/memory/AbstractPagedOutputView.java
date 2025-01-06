/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.memory;

import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.utils.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The base class for all output views that are backed by multiple memory pages. This base class
 * contains all encoding methods to write data to a page and detect page boundary crossing. The
 * concrete subclasses must implement the methods to collect the current page and provide the next
 * memory page once the boundary is crossed.
 *
 * <p>The paging assumes that all memory segments are of the same size.
 */
public abstract class AbstractPagedOutputView implements OutputView, MemorySegmentWritable {

    /** the page size of each memory segments. */
    protected final int pageSize;

    private List<MemorySegmentBytesView> segmentBytesViewList;

    /** the current memory segment to write to. */
    private MemorySegment currentSegment;

    /** the offset in the current segment. */
    private int positionInSegment;

    /** Flag indicating whether throw BufferExhaustedException or wait for available segment. */
    protected boolean waitingSegment;

    protected AbstractPagedOutputView(MemorySegment initialSegment, int pageSize) {
        if (initialSegment == null) {
            throw new NullPointerException("Initial Segment may not be null");
        }
        Preconditions.checkArgument(
                initialSegment.size() == pageSize, "Initial segment size must match page size.");
        this.pageSize = pageSize;
        this.currentSegment = initialSegment;
        this.positionInSegment = 0;
        this.segmentBytesViewList = new ArrayList<>();
        this.waitingSegment = true;
    }

    // --------------------------------------------------------------------------------------------
    //                                  Page Management
    // --------------------------------------------------------------------------------------------

    /**
     * This method must return a segment. If no more segments are available, it must throw an {@link
     * java.io.EOFException}.
     *
     * @return The next memory segment.
     */
    protected abstract MemorySegment nextSegment() throws IOException;

    /** Recycle the given memories. */
    protected abstract void deallocate(List<MemorySegment> segments);

    /** Recycle all memory segments in this output view. */
    public void recycleAll() {
        List<MemorySegment> segments =
                getSegmentBytesViewList().stream()
                        .map(MemorySegmentBytesView::getMemorySegment)
                        .collect(Collectors.toList());
        deallocate(segments);
    }

    /** Recycle allocated memory segments via {@link #nextSegment()} in this output view. */
    public void recycleAllocated() {
        List<MemorySegment> segments =
                getSegmentBytesViewList().stream()
                        .map(MemorySegmentBytesView::getMemorySegment)
                        .collect(Collectors.toList());
        // remove the initialSegment which is not allocated by the output view.
        segments.remove(0);
        if (!segments.isEmpty()) {
            deallocate(segments);
        }
    }

    /**
     * Gets the segment bytes view list, which including current segment.
     *
     * @return The segment bytes view list.
     */
    public List<MemorySegmentBytesView> getSegmentBytesViewList() {
        List<MemorySegmentBytesView> views = new ArrayList<>(segmentBytesViewList);
        views.add(new MemorySegmentBytesView(currentSegment, 0, positionInSegment));
        return views;
    }

    /**
     * Gets the segment that the view currently writes to.
     *
     * @return The segment the view currently writes to.
     */
    public MemorySegment getCurrentSegment() {
        return this.currentSegment;
    }

    /**
     * Gets the current write position (the position where the next bytes will be written) in the
     * current memory segment.
     *
     * @return The current write offset in the current memory segment.
     */
    public int getCurrentPositionInSegment() {
        return this.positionInSegment;
    }

    /**
     * Gets the page size of the memory segments.
     *
     * @return the page size of the memory segments.
     */
    public int getPageSize() {
        return this.pageSize;
    }

    public void seekOutput(MemorySegment seg, int position, boolean waitingSegment) {
        this.currentSegment = seg;
        this.positionInSegment = position;
        this.waitingSegment = waitingSegment;
    }

    /**
     * Moves the output view to the next page. This method invokes internally the {@link
     * #nextSegment()} method to give the current memory segment to the concrete subclass'
     * implementation and obtain the next segment to write to. Writing will continue inside the new
     * segment after the header.
     *
     * @throws IOException Thrown, if the current segment could not be processed or a new segment
     *     could not be obtained.
     */
    public void advance() throws IOException {
        MemorySegmentBytesView segmentBytesView =
                new MemorySegmentBytesView(currentSegment, 0, positionInSegment);
        this.currentSegment = nextSegment();
        this.positionInSegment = 0;
        segmentBytesViewList.add(segmentBytesView);
    }

    /**
     * Clears the internal state. Any successive write calls will fail until either {@link
     * #advance()}.
     *
     * @see #advance()
     */
    public void clear() {
        this.currentSegment = null;
        this.positionInSegment = 0;
        this.segmentBytesViewList = new ArrayList<>();
        this.waitingSegment = true;
    }

    // --------------------------------------------------------------------------------------------
    //                               Data Output Specific methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void writeByte(int v) throws IOException {
        if (positionInSegment < pageSize) {
            currentSegment.put(positionInSegment++, (byte) v);
        } else {
            advance();
            writeByte(v);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int remaining = pageSize - positionInSegment;
        if (remaining >= len) {
            currentSegment.put(positionInSegment, b, off, len);
            positionInSegment += len;
        } else {
            if (remaining == 0) {
                advance();
                remaining = pageSize - positionInSegment;
            }
            while (true) {
                int toPut = Math.min(remaining, len);
                currentSegment.put(positionInSegment, b, off, toPut);
                off += toPut;
                len -= toPut;

                if (len > 0) {
                    positionInSegment = pageSize;
                    advance();
                    remaining = pageSize - positionInSegment;
                } else {
                    positionInSegment += toPut;
                    break;
                }
            }
        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        writeByte(v ? 1 : 0);
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (positionInSegment < pageSize - 1) {
            currentSegment.putShort(positionInSegment, (short) v);
            positionInSegment += 2;
        } else if (positionInSegment == pageSize) {
            advance();
            writeShort(v);
        } else {
            // TODO add some test to cover this.
            writeByte(v);
            writeByte(v >> 8);
        }
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (positionInSegment < pageSize - 3) {
            currentSegment.putInt(positionInSegment, v);
            positionInSegment += 4;
        } else if (positionInSegment == pageSize) {
            advance();
            writeInt(v);
        } else {
            writeByte(v);
            writeByte(v >> 8);
            writeByte(v >> 16);
            writeByte(v >> 24);
        }
    }

    public void writeUnsignedInt(long v) throws IOException {
        writeInt((int) (v & 0xffffffffL));
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (positionInSegment < pageSize - 7) {
            currentSegment.putLong(positionInSegment, v);
            positionInSegment += 8;
        } else if (positionInSegment == pageSize) {
            advance();
            writeLong(v);
        } else {
            writeByte((int) v);
            writeByte((int) (v >> 8));
            writeByte((int) (v >> 16));
            writeByte((int) (v >> 24));
            writeByte((int) (v >> 32));
            writeByte((int) (v >> 40));
            writeByte((int) (v >> 48));
            writeByte((int) (v >> 56));
        }
    }

    @Override
    public void writeFloat(float v) throws IOException {
        if (positionInSegment < pageSize - 3) {
            currentSegment.putFloat(positionInSegment, v);
            positionInSegment += 4;
        } else if (positionInSegment == pageSize) {
            advance();
            writeFloat(v);
        } else {
            writeInt(Float.floatToIntBits(v));
        }
    }

    @Override
    public void writeDouble(double v) throws IOException {
        if (positionInSegment < pageSize - 7) {
            currentSegment.putDouble(positionInSegment, v);
            positionInSegment += 8;
        } else if (positionInSegment == pageSize) {
            advance();
            writeDouble(v);
        } else {
            writeLong(Double.doubleToLongBits(v));
        }
    }

    @Override
    public void write(MemorySegment segment, int off, int len) throws IOException {
        int remaining = pageSize - positionInSegment;
        if (remaining >= len) {
            segment.copyTo(off, currentSegment, positionInSegment, len);
            positionInSegment += len;
        } else {
            if (remaining == 0) {
                advance();
                remaining = pageSize - positionInSegment;
            }

            while (true) {
                int toPut = Math.min(remaining, len);
                segment.copyTo(off, currentSegment, positionInSegment, toPut);
                off += toPut;
                len -= toPut;

                if (len > 0) {
                    positionInSegment = pageSize;
                    advance();
                    remaining = pageSize - positionInSegment;
                } else {
                    positionInSegment += toPut;
                    break;
                }
            }
        }
    }
}
