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

package org.apache.fluss.lake.paimon.historical;

import org.apache.fluss.client.TableSchema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;

import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit test for {@link PaimonHistoricalPartitionReader}. */
@ExtendWith(MockitoExtension.class)
class PaimonHistoricalPartitionReaderTest {

    @Mock
    private PaimonHistoricalPartitionHandler paimonHandler;

    @Mock
    private Catalog paimonCatalog;

    private PaimonHistoricalPartitionReader reader;

    private static final TablePath TABLE_PATH = TablePath.of("test_db", "test_table");
    private static final String PARTITION_NAME = "dt=2023-01-01";
    private static final DataType[] FIELD_TYPES = {DataTypes.INT(), DataTypes.STRING()};
    private static final String[] FIELD_NAMES = {"id", "name"};

    @BeforeEach
    void setUp() {
        reader = new PaimonHistoricalPartitionReader(paimonHandler, TABLE_PATH, FIELD_TYPES, FIELD_NAMES);
    }

    @Test
    void testLookupFound() throws Exception {
        // Create a test row
        InternalRow testRow = GenericRow.of(1, BinaryString.fromString("test_value"));

        // Mock the handler to return the test row
        when(paimonHandler.lookup(eq(PARTITION_NAME), any(InternalRow.class)))
                .thenReturn(testRow);

        // Perform lookup
        InternalRow result = reader.lookup(PARTITION_NAME, GenericRow.of(1));

        // Verify result
        assertThat(result).isNotNull();
        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.getString(1)).isEqualTo(BinaryString.fromString("test_value"));
    }

    @Test
    void testLookupNotFound() throws Exception {
        // Mock the handler to return null (not found)
        when(paimonHandler.lookup(eq(PARTITION_NAME), any(InternalRow.class)))
                .thenReturn(null);

        // Perform lookup
        InternalRow result = reader.lookup(PARTITION_NAME, GenericRow.of(1));

        // Verify result is null
        assertThat(result).isNull();
    }

    @Test
    void testBatchLookup() throws Exception {
        // Create test rows
        InternalRow row1 = GenericRow.of(1, BinaryString.fromString("value1"));
        InternalRow row2 = GenericRow.of(2, BinaryString.fromString("value2"));

        // Mock the handler to return the test rows
        when(paimonHandler.batchLookup(eq(PARTITION_NAME), any()))
                .thenReturn(java.util.Arrays.asList(row1, row2));

        // Perform batch lookup
        java.util.List<InternalRow> results = reader.batchLookup(
                PARTITION_NAME, 
                java.util.Arrays.asList(GenericRow.of(1), GenericRow.of(2)));

        // Verify results
        assertThat(results).isNotNull();
        assertThat(results).hasSize(2);
        assertThat(results.get(0).getInt(0)).isEqualTo(1);
        assertThat(results.get(0).getString(1)).isEqualTo(BinaryString.fromString("value1"));
        assertThat(results.get(1).getInt(0)).isEqualTo(2);
        assertThat(results.get(1).getString(1)).isEqualTo(BinaryString.fromString("value2"));
    }

    @Test
    void testBatchLookupEmpty() throws Exception {
        // Mock the handler to return empty list
        when(paimonHandler.batchLookup(eq(PARTITION_NAME), any()))
                .thenReturn(Collections.emptyList());

        // Perform batch lookup
        java.util.List<InternalRow> results = reader.batchLookup(
                PARTITION_NAME, 
                java.util.Arrays.asList(GenericRow.of(1), GenericRow.of(2)));

        // Verify results
        assertThat(results).isNotNull();
        assertThat(results).isEmpty();
    }

    @Test
    void testClose() throws Exception {
        // Mock the handler close method
        when(paimonHandler.close()).thenReturn();

        // Close the reader
        reader.close();

        // Verify handler was closed
        // This would be verified by mocking and checking if close was called
    }
}