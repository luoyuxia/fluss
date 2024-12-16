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

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PanguFileSystemPlugin}. */
class PanguFileSystemPluginTest {

    @Test
    public void testGettingSchema() throws Exception {
        PanguFileSystemPlugin panguFileSystemPlugin = new PanguFileSystemPlugin();
        Configuration configuration = new Configuration();
        panguFileSystemPlugin.create(new URI("dfs://test/a1"), configuration);
        assertThat(panguFileSystemPlugin.getScheme()).isEqualTo(PanguFileSystemPlugin.SCHEME);
    }
}
