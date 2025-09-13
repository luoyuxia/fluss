/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.lake.split;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

/** A split to count rt. */
public class CountRtSplit extends SourceSplitBase {

    public static final byte COUNT_RT_SPLIT_KIND = -10;

    private final TablePath tablePath;
    private final Configuration conf;

    public CountRtSplit(TablePath tablePath, Configuration config) {
        super(new TableBucket(-1L, -1), null);
        this.tablePath = tablePath;
        this.conf = config;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public Configuration getConf() {
        return conf;
    }

    @Override
    public byte splitKind() {
        return COUNT_RT_SPLIT_KIND;
    }

    @Override
    public String splitId() {
        return "count-rt-split-" + tablePath.toString();
    }
}
