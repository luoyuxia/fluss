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

import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.ververica.pangu.common.stream.CommonPanguInputStream;

import java.io.IOException;

/**
 * Input stream for pangu. A wrapper to {@link
 * com.alibaba.ververica.pangu.common.stream.CommonPanguInputStream}.
 */
public class PanguDataInputStream extends FSDataInputStream {

    private final CommonPanguInputStream commonPanguInputStream;

    public PanguDataInputStream(CommonPanguInputStream commonPanguInputStream) {
        this.commonPanguInputStream = commonPanguInputStream;
    }

    @Override
    public void seek(long desired) {
        commonPanguInputStream.seek(desired);
    }

    @Override
    public long getPos() throws IOException {
        return commonPanguInputStream.getPos();
    }

    @Override
    public int read() throws IOException {
        return commonPanguInputStream.read();
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws IOException {
        return commonPanguInputStream.read(buf, off, len);
    }

    @Override
    public synchronized void close() throws IOException {
        commonPanguInputStream.close();
    }
}
