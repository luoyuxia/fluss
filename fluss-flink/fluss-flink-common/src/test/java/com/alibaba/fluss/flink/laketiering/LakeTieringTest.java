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

package com.alibaba.fluss.flink.laketiering;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.lakestorage.LakeStorage;
import com.alibaba.fluss.lakehouse.lakestorage.LakeStoragePlugin;
import com.alibaba.fluss.lakehouse.lakestorage.LakeStoragePluginSetUp;
import com.alibaba.fluss.lakehouse.writer.LakeTieringContext;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.metadata.DataLakeFormat;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class LakeTieringTest {

    @Test
    void t1() throws Exception {
        StreamExecutionEnvironment env = build();
        env.execute();
    }

    private StreamExecutionEnvironment build() throws IOException {
        // then build the fluss to paimon job
        final StreamExecutionEnvironment execEnv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setParallelism(1);
        execEnv.setMaxParallelism(1);

        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        configuration.setString("datalake.paimon.warehouse", "/tmp/paimon");
        LakeStoragePlugin lakeStoragePlugin =
                LakeStoragePluginSetUp.fromConfiguration(configuration, null);

        Configuration lakeConfig = new Configuration();
        lakeConfig.setString("warehouse", "/tmp/paimon");

        LakeStorage lakeStorage = lakeStoragePlugin.createLakeStorage(lakeConfig);

        LakeTieringContext lakeTieringContext = new FlinkLakeTieringContext();

        LakeTieringFactory lakeTieringFactory =
                lakeStorage.createLakeTieringFactory(lakeTieringContext);

        LakeTieringSource<?> lakeTieringSource = new LakeTieringSource<>(lakeTieringFactory);

        DataStreamSource<?> tieringSource =
                execEnv.fromSource(
                        lakeTieringSource,
                        WatermarkStrategy.noWatermarks(),
                        "source",
                        new TableBucketWriteResultTypoInfo<>(
                                () -> lakeTieringFactory.getWriteResultSerializer()));
        tieringSource
                .transform(
                        "committer",
                        new CommittableTypeInfo<>(
                                () -> lakeTieringFactory.getCommitableSerializer()),
                        new LakeTieringCommitterOperator<>(lakeTieringFactory))
                .setParallelism(1)
                .setMaxParallelism(1)
                .sinkTo(new DiscardingSink<>());
        return execEnv;
    }
}
