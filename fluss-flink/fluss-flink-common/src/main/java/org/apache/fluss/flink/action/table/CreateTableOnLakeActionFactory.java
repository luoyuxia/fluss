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

package org.apache.fluss.flink.action.table;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.provider.ConfigProviders;
import org.apache.fluss.flink.action.Action;
import org.apache.fluss.flink.action.ActionFactory;
import org.apache.fluss.flink.adapter.MultipleParameterToolAdapter;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.PropertiesUtils.extractAndRemovePrefix;

/** Factory for {@link CreateTableOnLakeAction}. */
@Internal
public class CreateTableOnLakeActionFactory implements ActionFactory {

    private static final String FLUSS_CONFIG_PREFIX = "fluss.";
    private static final String DATALAKE_CONFIG_PREFIX = "datalake.";
    private static final String TABLE = "table";
    private static final String TABLE_CONF = "table-conf";

    @Override
    public String identifier() {
        return "create_table_on_lake";
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        Map<String, String> parameterMap = resolveConfigProviders(params.toMap());
        TablePath tablePath = parseTablePath(parameterMap.get(TABLE));

        Map<String, String> flussConfigMap =
                extractAndRemovePrefix(parameterMap, FLUSS_CONFIG_PREFIX);
        String bootstrapServers = flussConfigMap.get(ConfigOptions.BOOTSTRAP_SERVERS.key());
        if (StringUtils.isNullOrWhitespaceOnly(bootstrapServers)) {
            throw new IllegalArgumentException(
                    "--fluss." + ConfigOptions.BOOTSTRAP_SERVERS.key() + " is required");
        }

        DataLakeFormat dataLakeFormat =
                Configuration.fromMap(parameterMap).get(ConfigOptions.DATALAKE_FORMAT);
        if (dataLakeFormat == null) {
            throw new IllegalArgumentException("--datalake.format is required");
        }
        if (dataLakeFormat != DataLakeFormat.PAIMON) {
            throw new IllegalArgumentException(
                    "Create table on lake currently only supports Paimon, but was "
                            + dataLakeFormat);
        }

        Map<String, String> paimonConfigMap =
                extractAndRemovePrefix(
                        parameterMap, DATALAKE_CONFIG_PREFIX + DataLakeFormat.PAIMON + ".");
        return Optional.<Action>of(
                new CreateTableOnLakeAction(
                        Configuration.fromMap(flussConfigMap),
                        Configuration.fromMap(paimonConfigMap),
                        tablePath,
                        parseTableProperties(params.getMultiParameter(TABLE_CONF))));
    }

    @Override
    public String help() {
        return "Usage: create_table_on_lake --table <database.table>\n"
                + "  --fluss.bootstrap.servers <host:port>\n"
                + "  --datalake.format paimon\n"
                + "  --datalake.paimon.<key> <value> ...\n"
                + "  [--table-conf <key>=<value>]...\n"
                + "\n"
                + "The Fluss and Paimon options use the same prefixes as the tiering service.\n"
                + "All --datalake.paimon.* options are passed to the Paimon catalog after the\n"
                + "prefix is removed. They must identify the same Paimon catalog configured in\n"
                + "the Fluss cluster. Repeat --table-conf to override Fluss table properties.\n"
                + "\n"
                + "Example:\n"
                + "  create_table_on_lake --table my_db.my_table \\\n"
                + "    --fluss.bootstrap.servers localhost:9123 \\\n"
                + "    --datalake.format paimon \\\n"
                + "    --datalake.paimon.metastore filesystem \\\n"
                + "    --datalake.paimon.warehouse /tmp/paimon \\\n"
                + "    --table-conf bucket.num=16";
    }

    private static Map<String, String> resolveConfigProviders(Map<String, String> parameterMap) {
        Configuration parameters = Configuration.fromMap(parameterMap);
        ConfigProviders.resolve(parameters);
        return new HashMap<>(parameters.toMap());
    }

    private static TablePath parseTablePath(@Nullable String table) {
        if (StringUtils.isNullOrWhitespaceOnly(table)) {
            throw new IllegalArgumentException("--table <database.table> is required");
        }
        String[] parts = table.trim().split("\\.", -1);
        if (parts.length != 2
                || StringUtils.isNullOrWhitespaceOnly(parts[0])
                || StringUtils.isNullOrWhitespaceOnly(parts[1])) {
            throw new IllegalArgumentException("--table must use the form database.table");
        }
        return TablePath.of(parts[0].trim(), parts[1].trim());
    }

    private static Map<String, String> parseTableProperties(
            @Nullable Collection<String> propertyValues) {
        if (propertyValues == null || propertyValues.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> properties = new LinkedHashMap<>();
        for (String property : propertyValues) {
            int separator = property.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException(
                        "--table-conf must use key=value format, but was: " + property);
            }
            String key = property.substring(0, separator).trim();
            String value = property.substring(separator + 1).trim();
            if (key.isEmpty() || value.isEmpty()) {
                throw new IllegalArgumentException("--table-conf key and value must not be empty");
            }
            if (properties.put(key, value) != null) {
                throw new IllegalArgumentException("Duplicate --table-conf key: " + key);
            }
        }
        return properties;
    }
}
