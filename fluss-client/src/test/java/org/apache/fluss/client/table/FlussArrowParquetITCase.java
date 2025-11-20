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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecordBatches;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.FlussArrowRecordBatch;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.DataTestUtils.row;

public class FlussArrowParquetITCase extends ClientToServerITCaseBase {

    @Test
    void testAppendOnly() throws Exception {
        createTable(DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR, false);
        try (Table table = conn.getTable(DATA1_TABLE_PATH)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row(1, "a"));
            appendWriter.append(row(1, "a"));
            appendWriter.flush();
        }

        try (Table table = conn.getTable(DATA1_TABLE_PATH);
                LogScanner logScanner = table.newScan().createLogScanner(); ) {
            for (int i = 0; i < 3; i++) {
                logScanner.subscribeFromBeginning(i);
            }
            while (true) {
                ScanRecordBatches scanRecordBatches =
                        logScanner.pollScanRecordsBatches(Duration.ofSeconds(1));
                for (TableBucket bucket : scanRecordBatches.buckets()) {
                    List<FlussArrowRecordBatch> recordBatches = scanRecordBatches.records(bucket);

                    for (FlussArrowRecordBatch recordBatch : recordBatches) {

                        System.out.println(recordBatch.getSchemaRoot().contentToTSVString());
                    }
                }
                Thread.sleep(1000);
            }
        }
    }
}
