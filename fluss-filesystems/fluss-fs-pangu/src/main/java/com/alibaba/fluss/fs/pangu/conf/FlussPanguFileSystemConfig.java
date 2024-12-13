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

package com.alibaba.fluss.fs.pangu.conf;

import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.ververica.pangu.common.conf.FileSystemConfig;

import static com.alibaba.fluss.config.ConfigBuilder.key;

/** Config for Fluss file system (adapt to Pangu). */
public class FlussPanguFileSystemConfig {

    public static final ConfigOption<Integer> INPUT_STREAM_READ_BUFFER_SIZE =
            key("fs.dfs.io.buffer.size")
                    .intType()
                    .defaultValue(256 * 1024)
                    .withDescription(
                            "Size of the read buffer (io size) when fetching data from pangu file.");

    public static final ConfigOption<Integer> OUTPUT_STREAM_MAX_WRITE_BUFFER_SIZE =
            key("fs.dfs.io.write-buffer.size")
                    .intType()
                    .defaultValue(1048576)
                    .withDescription("Size of the write buffer when appending data to pangu file.");

    public static final ConfigOption<Boolean> BUFFER_RELEASE_AFTER_BATCH_READ =
            key("fs.dfs.buffer.release-after-batch-read")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to release the temp buffer after batch read (e.g. #read()).");

    public static final ConfigOption<Integer> DIRECT_BUFFER_POOL_SIZE =
            key("fs.dfs.buffer-pool.size")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The size of the buffer pool used by read and write.");

    public static FileSystemConfig createFsConfig(Configuration originalConf) {
        return FileSystemConfig.newBuilder()
                .setReadBufferSize(originalConf.getInt(INPUT_STREAM_READ_BUFFER_SIZE))
                .setMaxWriteBufferSize(originalConf.getInt(OUTPUT_STREAM_MAX_WRITE_BUFFER_SIZE))
                .setReleaseBufferAfterBatchRead(
                        originalConf.getBoolean(BUFFER_RELEASE_AFTER_BATCH_READ))
                .setBufferPoolSize(originalConf.getInt(DIRECT_BUFFER_POOL_SIZE))
                .build();
    }
}
