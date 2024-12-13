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
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.pangu.model.enums.PanguFileType;
import com.alibaba.pangu.model.enums.PanguIoAdvice;
import com.alibaba.pangu.model.enums.PanguQosFlow;
import com.alibaba.ververica.pangu.common.conf.PanguConfig;
import com.alibaba.ververica.pangu.common.util.PanguSdkMode;

import static com.alibaba.fluss.config.ConfigBuilder.key;

/**
 * Configs for Pangu, NOT file system, file system setting in {@link FlussPanguFileSystemConfig}.
 */
public class FlussPanguConfig {

    public static final ConfigOption<Long> WRITE_BUFFER_SIZE_LIMIT =
            key("fs.dfs.write-buffer-size-limit")
                    .longType()
                    .defaultValue(512 * 1024 * 1024L)
                    .withDescription("Buffer memory in bytes for write. In range [256MB, 8GB]");

    public static final ConfigOption<String> USER_DEFINED_FLAGS =
            key("fs.dfs.flags")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Flags for user defined, which could override default predefined options.");

    public static final ConfigOption<PanguQosFlow> QOS_FLOW =
            key("fs.dfs.qos.flow")
                    .enumType(PanguQosFlow.class)
                    .defaultValue(PanguQosFlow.PANGU_QOS_INVALID_FLOW)
                    .withDescription(
                            String.format(
                                    "The qos flow option when opening file. We have three candidates: %s (by default), %s and %s.",
                                    PanguQosFlow.PANGU_QOS_INVALID_FLOW,
                                    PanguQosFlow.PANGU_QOS_DEFAULT_HIGH_PRIORITY_FLOW,
                                    PanguQosFlow.PANGU_QOS_DEFAULT_LOW_PRIORITY_FLOW));

    public static final ConfigOption<PanguIoAdvice> IO_ADVICE =
            key("fs.dfs.io.advice")
                    .enumType(PanguIoAdvice.class)
                    .defaultValue(PanguIoAdvice.PANGU_IO_ADVICE_SEQUENTIAL)
                    .withDescription(
                            String.format(
                                    "The io advice when opening file. We have two candidates: %s (by default) and %s.",
                                    PanguIoAdvice.PANGU_IO_ADVICE_SEQUENTIAL,
                                    PanguIoAdvice.PANGU_IO_ADVICE_NORMAL));

    public static final ConfigOption<String> MEMORY_PREFETCH_LIMIT =
            key("fs.dfs.memory.prefetch.limit")
                    .stringType()
                    .defaultValue("512mb")
                    .withDescription(
                            "The prefetch memory limit when prefetching file. In new pangu client, this is no longer set by --pangu_client_PrefetchMemoryLimit, but with ReadaheadSizeLimit in at pangu_fs_create API");

    public static final ConfigOption<Boolean> SYNC_IO =
            key("fs.dfs.io.sync")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to use sync io mode to create file. This option should only be enabled on SSD DFS cluster.");

    public static final ConfigOption<Boolean> ENABLE_CHECKSUM =
            key("fs.dfs.checksum.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable checksum when appending data to pangu file.");

    public static final ConfigOption<PanguFileType> FILE_TYPE =
            key("fs.dfs.file.type")
                    .enumType(PanguFileType.class)
                    .defaultValue(PanguFileType.PANGU_FILE_TYPE_PERFORMANCE)
                    .withDescription(
                            String.format(
                                    "The file type when created. We have candidates: %s (by default), %s, %s and %s.",
                                    PanguFileType.PANGU_FILE_TYPE_PERFORMANCE,
                                    PanguFileType.PANGU_FILE_TYPE_DEFAULT,
                                    PanguFileType.PANGU_FILE_TYPE_STANDARD,
                                    PanguFileType.PANGU_FILE_TYPE_INLINEFILE));

    public static final ConfigOption<String> TYPE_INFO =
            key("fs.dfs.type.info")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Type info for file creation. Mainly for writing EC (erasure coding) files.\n"
                                    + "If fs.dfs.file.type is PANGU_FILE_TYPE_PERFORMANCE: ec_data=8 ec_parity=3 ec_packet_size_bits=15\n"
                                    + "If fs.dfs.file.type is PANGU_FILE_TYPE_STANDARD: ec_data=8 ec_parity=3 ec_packet_size_bits=20");

    public static final ConfigOption<PanguSdkMode> SDK_MODE =
            key("fs.dfs.sdk.mode")
                    .enumType(PanguSdkMode.class)
                    .defaultValue(PanguSdkMode.PANGU_SDK_CORE)
                    .withDescription(
                            String.format(
                                    "The SDK mode when creating file system. We have two options: %s (by default) and %s.",
                                    PanguSdkMode.PANGU_SDK_CORE, PanguSdkMode.PANGU_SDK_POV));

    public static final ConfigOption<Long> BLOCK_SIZE =
            key("fs.dfs.block.size")
                    .longType()
                    .defaultValue(128 * 1024 * 1024L)
                    .withDescription(
                            "The fake block size for filesystem. This option is target for input split.");

    public static final ConfigOption<String> LOG_CONFIG_FILE =
            key("fs.dfs.log.config.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The config file path for file system logs. If this option is "
                                    + "not configured, the \"apsara_log_conf.json\" in the resource folder will be "
                                    + "used as the configuration file by default; if the user configures this option,"
                                    + "the file in the user-defined path will be used as the configuration file.");

    public static final ConfigOption<Integer> LIST_DIRECTORY_ENTRY_COUNT_LIMIT =
            key("fs.dfs.list-directory.entry-count-limit")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Limit of entries in one list directory. If exceeds, list will try multiple times.");

    public static PanguConfig createPanguConfig(Configuration originalConf, boolean isMock) {
        return PanguConfig.newBuilder()
                .setMock(isMock)
                .setSdkMode(PanguSdkMode.valueOf(originalConf.getValue(FlussPanguConfig.SDK_MODE)))
                .setReadaheadSizeLimit(
                        MemorySize.parseBytes(
                                originalConf.getString(FlussPanguConfig.MEMORY_PREFETCH_LIMIT)))
                .setWriteBufferSizeLimit(
                        originalConf.getLong(FlussPanguConfig.WRITE_BUFFER_SIZE_LIMIT))
                .setQosFlow(PanguQosFlow.valueOf(originalConf.getValue(FlussPanguConfig.QOS_FLOW)))
                .setUseSyncIo(originalConf.getBoolean(FlussPanguConfig.SYNC_IO))
                .setIoAdvice(
                        PanguIoAdvice.valueOf(originalConf.getValue(FlussPanguConfig.IO_ADVICE)))
                .setFileType(
                        PanguFileType.valueOf(originalConf.getValue(FlussPanguConfig.FILE_TYPE)))
                .setBlockSize(originalConf.getLong(FlussPanguConfig.BLOCK_SIZE))
                .setEnableChecksum(originalConf.getBoolean(FlussPanguConfig.ENABLE_CHECKSUM))
                .setListDirEntryCountLimit(
                        originalConf.getInt(FlussPanguConfig.LIST_DIRECTORY_ENTRY_COUNT_LIMIT))
                .setUserDefinedFlags(originalConf.getString(FlussPanguConfig.USER_DEFINED_FLAGS))
                .setLogPath(originalConf.getString(FlussPanguConfig.LOG_CONFIG_FILE))
                .build();
    }
}
