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

package com.alibaba.fluss.fs.pangu.stream;

import com.alibaba.fluss.fs.FSDataOutputStream;

import com.alibaba.ververica.pangu.common.stream.CommonPanguOutputStream;

import java.io.IOException;

/** Output stream for pangu. */
public class PanguDataOutputStream extends FSDataOutputStream {

    private final CommonPanguOutputStream commonPanguOutputStream;

    public PanguDataOutputStream(CommonPanguOutputStream commonPanguOutputStream) {
        this.commonPanguOutputStream = commonPanguOutputStream;
    }

    /**
     * Append data to file.
     *
     * @param b the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException I/O error.
     */
    @Override
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        commonPanguOutputStream.write(b, off, len);
    }

    @Override
    public long getPos() throws IOException {
        return commonPanguOutputStream.getPos();
    }

    @Override
    public synchronized void write(int b) throws IOException {
        commonPanguOutputStream.write(b);
    }

    /**
     * Pangu supports three-phase buffered write. See
     * https://pangu.aliyun-inc.com/PUBLIC/PanguDFS-Pangu_Client_internal_doc?menu=DFS304&path=articles%2Fwhat-is_how-to_buffer_write.
     *
     * <p>We use the pattern: pangu_open -> pangu_appendv -> pangu_flush_file -> pangu_appendv ->
     * pangu_sync_file -> pangu_close_file
     *
     * @throws IOException I/O error.
     */
    @Override
    public void flush() throws IOException {
        commonPanguOutputStream.flush();
    }

    @Override
    public void close() throws IOException {
        commonPanguOutputStream.close();
    }
}
