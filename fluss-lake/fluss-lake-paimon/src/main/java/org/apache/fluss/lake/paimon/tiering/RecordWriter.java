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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.lake.paimon.source.FlussRowAsPaimonRow;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.PartitionUtils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkState;

/** A base interface to write {@link LogRecord} to Paimon. */
public abstract class RecordWriter<T> implements AutoCloseable {

    protected final TableWriteImpl<T> tableWrite;
    protected final RowType tableRowType;
    protected final int bucket;
    protected final List<String> partitionKeys;
    protected final BinaryRow partition;
    protected final boolean isHistoricalPartition;
    protected final FlussRecordAsPaimonRow flussRecordAsPaimonRow;

    public RecordWriter(
            TableWriteImpl<T> tableWrite,
            RowType tableRowType,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys,
            org.apache.fluss.types.RowType flussRowType) {
        this.tableWrite = tableWrite;
        this.tableRowType = tableRowType;
        this.bucket = tableBucket.getBucket();
        this.partitionKeys = partitionKeys;
        this.isHistoricalPartition =
                partition != null && PartitionUtils.isHistoricalPartitionName(partition);
        if (isHistoricalPartition || partition == null || partitionKeys.isEmpty()) {
            // For historical partitions, partition is resolved per-record from row data
            // since records from different original partitions coexist.
            // For non-partitioned tables, use empty row.
            this.partition = BinaryRow.EMPTY_ROW;
        } else {
            // eagerly resolve BinaryRow partition from partition name string
            this.partition = resolvePartition(partition, partitionKeys, flussRowType);
        }
        this.flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket.getBucket(), tableRowType);
    }

    public abstract void write(LogRecord record) throws Exception;

    List<CommitMessage> complete() throws Exception {
        return tableWrite.prepareCommit();
    }

    public void close() throws Exception {
        tableWrite.close();
    }

    /**
     * Returns the Paimon partition for the current record being written.
     *
     * <p>For historical partitions, extracts the partition from the row data since different
     * records may belong to different original partitions. For regular partitions, returns the
     * pre-resolved partition.
     *
     * <p>Must be called after {@link FlussRecordAsPaimonRow#setFlussRecord(LogRecord)}.
     */
    protected BinaryRow getPartitionForRecord() {
        if (isHistoricalPartition) {
            return tableWrite.getPartition(flussRecordAsPaimonRow);
        }
        return partition;
    }

    /**
     * Resolves a Paimon {@link BinaryRow} partition from the partition name string by parsing each
     * partition value to its typed Fluss representation, constructing a synthetic row, and
     * delegating to Paimon's partition extraction.
     */
    private BinaryRow resolvePartition(
            String partitionName,
            List<String> partitionKeys,
            org.apache.fluss.types.RowType flussRowType) {
        ResolvedPartitionSpec spec =
                ResolvedPartitionSpec.fromPartitionName(partitionKeys, partitionName);
        List<String> partitionValues = spec.getPartitionValues();

        // Build a GenericRow with partition column values at their correct positions.
        // The row field count must match the Paimon RowType (business columns + system columns)
        // so that FlussRowAsPaimonRow aligns with the Paimon schema.
        GenericRow partitionRow = new GenericRow(tableRowType.getFieldCount());

        for (int i = 0; i < partitionKeys.size(); i++) {
            String partitionKey = partitionKeys.get(i);
            int fieldIndex = flussRowType.getFieldIndex(partitionKey);
            checkState(
                    fieldIndex >= 0,
                    "Partition key '%s' not found in Fluss row type.",
                    partitionKey);
            DataTypeRoot typeRoot = flussRowType.getTypeAt(fieldIndex).getTypeRoot();
            Object typedValue = PartitionUtils.parseValueOfType(partitionValues.get(i), typeRoot);
            partitionRow.setField(fieldIndex, typedValue);
        }

        FlussRowAsPaimonRow paimonRow = new FlussRowAsPaimonRow(partitionRow, tableRowType);
        return tableWrite.getPartition(paimonRow);
    }
}
