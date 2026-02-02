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

package org.apache.fluss.lake.paimon.catalog;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.util.Map;

/**
 * Factory for creating Paimon catalogs based on table configuration.
 */
public class PaimonCatalogFactory {

    /**
     * Creates a Paimon catalog for the given table based on its configuration.
     *
     * @param tableInfo The table information containing lake configuration
     * @return A configured Paimon catalog
     */
    public static Catalog createCatalog(TableInfo tableInfo) {
        Configuration tableConfig = tableInfo.getTableConfig().getConfiguration();
        
        // Extract Paimon catalog configuration from table config
        Options options = new Options();
        
        // Get the warehouse path from table configuration
        String warehousePath = tableConfig.getString(ConfigOptions.DATA_LAKE_WAREHOUSE_PATH, null);
        if (warehousePath != null) {
            options.setString(CatalogOptions.WAREHOUSE, warehousePath);
        }
        
        // Add any additional catalog options from table configuration
        for (Map.Entry<String, String> entry : tableConfig.toMap().entrySet()) {
            if (entry.getKey().startsWith("paimon.")) {
                // Extract the option name without the 'paimon.' prefix
                String optionName = entry.getKey().substring("paimon.".length());
                options.setString(optionName, entry.getValue());
            }
        }
        
        // Create and return the appropriate catalog based on configuration using CatalogFactory
        return org.apache.paimon.catalog.CatalogFactory.createCatalog(
            org.apache.paimon.catalog.CatalogContext.create(options)
        );
    }
}