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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.LakeStorageInfo;
import com.alibaba.fluss.metadata.DataLakeFormat;

import java.util.HashMap;
import java.util.Map;

/** Utils for Fluss lake storage. */
public class LakeStorageUtils {

    private static final String DATALAKE_PAIMON_PREFIX = "datalake.paimon.";

    public static LakeStorageInfo getLakeStorageInfo(Configuration configuration) {
        DataLakeFormat datalakeFormat = configuration.get(ConfigOptions.DATALAKE_FORMAT);
        if (datalakeFormat == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "The lakehouse storage is not set, please set it by %s",
                            ConfigOptions.DATALAKE_FORMAT.key()));
        }

        if (datalakeFormat != DataLakeFormat.PAIMON) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The lakehouse storage %s "
                                    + " is not supported. Only %s is supported.",
                            datalakeFormat, DataLakeFormat.PAIMON));
        }

        // currently, extract catalog config
        Map<String, String> datalakeConfig = new HashMap<>();
        Map<String, String> flussConfig = configuration.toMap();
        for (Map.Entry<String, String> configEntry : flussConfig.entrySet()) {
            String configKey = configEntry.getKey();
            String configValue = configEntry.getValue();
            if (configKey.startsWith(DATALAKE_PAIMON_PREFIX)) {
                datalakeConfig.put(
                        configKey.substring(DATALAKE_PAIMON_PREFIX.length()), configValue);
            }
        }
        return new LakeStorageInfo(datalakeFormat.toString(), datalakeConfig);
    }
}
