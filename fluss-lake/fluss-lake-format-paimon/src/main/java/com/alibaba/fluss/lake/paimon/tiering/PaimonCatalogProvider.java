/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.tiering;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lake.paimon.fs.FlussFileIoLoader;
import com.alibaba.fluss.lakehouse.writer.FileSystemProvider;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

import java.io.Serializable;

public class PaimonCatalogProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Configuration paimonConfig;
    private final FileSystemProvider fsProvider;

    public PaimonCatalogProvider(Configuration paimonConfig, FileSystemProvider fsProvider) {
        this.paimonConfig = paimonConfig;
        this.fsProvider = fsProvider;
    }

    public Catalog getCatalog() {
        return CatalogFactory.createCatalog(
                CatalogContext.create(
                        Options.fromMap(paimonConfig.toMap()),
                        new FlussFileIoLoader(fsProvider),
                        new FlussFileIoLoader(fsProvider)));
    }
}
