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

package com.alibaba.fluss.fs.pangu;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig;
import com.alibaba.pangu.model.enums.PanguFileType;
import com.alibaba.pangu.model.enums.PanguIoAdvice;
import com.alibaba.pangu.model.enums.PanguQosFlow;
import com.alibaba.ververica.pangu.common.util.PanguSdkMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.BLOCK_SIZE;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.ENABLE_CHECKSUM;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.FILE_TYPE;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.IO_ADVICE;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.MEMORY_PREFETCH_LIMIT;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.QOS_FLOW;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.SDK_MODE;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.SYNC_IO;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.TYPE_INFO;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.WRITE_BUFFER_SIZE_LIMIT;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguFileSystemConfig.BUFFER_RELEASE_AFTER_BATCH_READ;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguFileSystemConfig.DIRECT_BUFFER_POOL_SIZE;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguFileSystemConfig.INPUT_STREAM_READ_BUFFER_SIZE;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguFileSystemConfig.OUTPUT_STREAM_MAX_WRITE_BUFFER_SIZE;
import static com.alibaba.ververica.pangu.common.conf.ApsaraLogConfig.DEFAULT_CONFIG_FILE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PanguFileSystem}. */
class PanguFileSystemTest {

    public Configuration configuration;
    public PanguFileSystem panguFs;

    static final URI TEST_URI;
    static final String USER_DEFINED_FLAGS =
            "--pangu_client2_InputStreamFastSplitMaxLength=524289 --nuwa_client_ConfigDirectory=/my/path ";

    static {
        try {
            TEST_URI = new URI("dfs://test/a/");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setup() {
        configuration = new Configuration();
        panguFs = new PanguFileSystem(TEST_URI);
    }

    @AfterEach
    void tearDown() {
        panguFs.closeNotLoadSo();
    }

    private static Map<String, String> extractExtraInfo(String extraInfo) {
        Map<String, String> extraInfoMap = new HashMap<>();

        String[] infos = extraInfo.split(" ");

        for (String info : infos) {
            info = info.trim();
            if (info.length() == 0) {
                continue;
            }
            String[] split = info.split("=");
            extraInfoMap.put(split[0], split[1]);
        }

        return extraInfoMap;
    }

    @Test
    void testGetUserDefinedFlag() {
        configuration.setString(FlussPanguConfig.USER_DEFINED_FLAGS, USER_DEFINED_FLAGS);
        String extraInfo = panguFs.initPanguFsNotLoadSo(configuration).getExtraInfo();
        System.out.println(extraInfo);
        Map<String, String> extraInfoMap = extractExtraInfo(extraInfo);

        assertThat(extraInfoMap.get("--pangu_client2_InputStreamFastSplitMaxLength"))
                .isEqualTo("524289");

        assertThat(extraInfoMap.get("--nuwa_client_ConfigDirectory")).isEqualTo("/my/path");
    }

    @Test
    void testGetMemoryPrefetchLimit() throws Exception {
        {
            assertThat(panguFs.initPanguFsNotLoadSo(configuration).getReadaheadSizeLimit())
                    .isEqualTo(MemorySize.parseBytes(MEMORY_PREFETCH_LIMIT.defaultValue()));
            panguFs.closeNotLoadSo();
        }
        {
            configuration.setString(MEMORY_PREFETCH_LIMIT, "1g");
            assertThat(panguFs.initPanguFsNotLoadSo(configuration).getReadaheadSizeLimit())
                    .isEqualTo(1024 * 1024 * 1024);
        }
    }

    @Test
    void testConfigureApsaraLogPath(@TempDir File tmpFile) throws Exception {
        {
            String actualLogConfPath = panguFs.initPanguFsNotLoadSo(configuration).getLogPath();
            assertThat(actualLogConfPath.substring(actualLogConfPath.lastIndexOf("/") + 1))
                    .isEqualTo(DEFAULT_CONFIG_FILE);
            panguFs.closeNotLoadSo();
        }
        {
            String userConfigLogPath = tmpFile.getAbsolutePath();
            configuration.setString(FlussPanguConfig.LOG_CONFIG_FILE, userConfigLogPath);
            assertThat(panguFs.initPanguFsNotLoadSo(configuration).getLogPath())
                    .isEqualTo(userConfigLogPath);
        }
    }

    @Test
    @Disabled("VVR-56617418")
    void testDefaultApsaraLogConf(@TempDir File tmpFile) throws IOException {
        // Setup log folder for pangu.
        String originLogFile = System.getProperty("log.file");
        String logFolder = tmpFile.toString();
        System.setProperty("log.file", logFolder.endsWith("/") ? logFolder : logFolder + "/");

        // Assert pangu uses correct log folder.
        String context =
                new String(
                        Files.readAllBytes(
                                Paths.get(
                                        panguFs.initPanguFsNotLoadSo(configuration).getLogPath())));
        assertThat(context.contains(logFolder)).isTrue();

        if (originLogFile != null) {
            System.setProperty("log.file", originLogFile);
        }
    }

    @Test
    void testDefaultConfig() {
        panguFs.initPanguFsNotLoadSo(configuration);

        assertThat(panguFs.getMaxWriteBufferSize())
                .isEqualTo((int) OUTPUT_STREAM_MAX_WRITE_BUFFER_SIZE.defaultValue());
        assertThat(panguFs.getReadBufferSize())
                .isEqualTo((int) INPUT_STREAM_READ_BUFFER_SIZE.defaultValue());
        assertThat(panguFs.isReleaseBufferAfterBatchRead())
                .isEqualTo(BUFFER_RELEASE_AFTER_BATCH_READ.defaultValue());
        assertThat(panguFs.getBufferPoolSize())
                .isEqualTo((int) DIRECT_BUFFER_POOL_SIZE.defaultValue());

        assertThat(panguFs.getReadaheadSizeLimit())
                .isEqualTo(MemorySize.parseBytes(MEMORY_PREFETCH_LIMIT.defaultValue()));
        assertThat(panguFs.getWriteBufferSizeLimit())
                .isEqualTo((long) WRITE_BUFFER_SIZE_LIMIT.defaultValue());
        assertThat(panguFs.getBlockSize()).isEqualTo((long) BLOCK_SIZE.defaultValue());
        assertThat(panguFs.getSdkMode()).isEqualTo(SDK_MODE.defaultValue());
        assertThat(panguFs.getQosFlow()).isEqualTo(QOS_FLOW.defaultValue());
        assertThat(panguFs.getFileType()).isEqualTo(FILE_TYPE.defaultValue());
        assertThat(panguFs.getTypeInfo()).isNull();
        assertThat(panguFs.getIoAdvice()).isEqualTo(IO_ADVICE.defaultValue());
        assertThat(panguFs.isUseSyncIo()).isEqualTo(SYNC_IO.defaultValue());
        assertThat(panguFs.getFileType()).isEqualTo(FILE_TYPE.defaultValue());
        assertThat(panguFs.isEnableChecksum()).isEqualTo(ENABLE_CHECKSUM.defaultValue());
    }

    @Test
    public void testInitializeCustomizedConfig() {
        configuration.setString(OUTPUT_STREAM_MAX_WRITE_BUFFER_SIZE.key(), "233");
        configuration.setString(INPUT_STREAM_READ_BUFFER_SIZE.key(), "233");
        configuration.setString(BUFFER_RELEASE_AFTER_BATCH_READ.key(), "false");
        configuration.setString(DIRECT_BUFFER_POOL_SIZE.key(), "233");

        configuration.setString(MEMORY_PREFETCH_LIMIT.key(), "233mb");
        configuration.setString(WRITE_BUFFER_SIZE_LIMIT.key(), "233");
        configuration.setString(BLOCK_SIZE.key(), "233");
        configuration.setString(SDK_MODE.key(), PanguSdkMode.PANGU_SDK_POV.name());
        configuration.setString(
                QOS_FLOW.key(), PanguQosFlow.PANGU_QOS_DEFAULT_HIGH_PRIORITY_FLOW.name());
        configuration.setString(FILE_TYPE.key(), PanguFileType.PANGU_FILE_TYPE_STANDARD.name());
        configuration.setString(TYPE_INFO.key(), "ec_data=8 ec_parity=3 ec_packet_size_bits=20");
        configuration.setString(IO_ADVICE.key(), PanguIoAdvice.PANGU_IO_ADVICE_NORMAL.name());
        configuration.setString(SYNC_IO.key(), "false");
        configuration.setString(FILE_TYPE.key(), PanguFileType.PANGU_FILE_TYPE_STANDARD.name());
        configuration.setString(ENABLE_CHECKSUM.key(), "true");

        panguFs.initPanguFsNotLoadSo(configuration);

        assertThat(panguFs.getMaxWriteBufferSize()).isEqualTo(233);
        assertThat(panguFs.getReadBufferSize()).isEqualTo(233);
        assertThat(panguFs.isReleaseBufferAfterBatchRead()).isFalse();
        assertThat(panguFs.getBufferPoolSize()).isEqualTo(233);

        assertThat(panguFs.getReadaheadSizeLimit()).isEqualTo(233 * 1024 * 1024);
        assertThat(panguFs.getWriteBufferSizeLimit()).isEqualTo(233);
        assertThat(panguFs.getBlockSize()).isEqualTo(233);
        assertThat(panguFs.getSdkMode()).isEqualTo(PanguSdkMode.PANGU_SDK_POV);
        assertThat(panguFs.getQosFlow())
                .isEqualTo(PanguQosFlow.PANGU_QOS_DEFAULT_HIGH_PRIORITY_FLOW);
        assertThat(panguFs.getIoAdvice()).isEqualTo(PanguIoAdvice.PANGU_IO_ADVICE_NORMAL);
        assertThat(panguFs.isUseSyncIo()).isFalse();
        assertThat(panguFs.getFileType()).isEqualTo(PanguFileType.PANGU_FILE_TYPE_STANDARD);
        assertThat(panguFs.isEnableChecksum()).isTrue();
        assertThat(panguFs.getFileType()).isEqualTo(PanguFileType.PANGU_FILE_TYPE_STANDARD);
        assertThat(panguFs.getTypeInfo()).isEqualTo("ec_data=8 ec_parity=3 ec_packet_size_bits=20");
    }
}
