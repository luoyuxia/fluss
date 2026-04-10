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

package org.apache.fluss.lake.paimon.tiering.append;

import org.apache.fluss.lake.paimon.tiering.RecordWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.LogRecord;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** A {@link RecordWriter} to write to Paimon's append-only table. */
public class AppendOnlyWriter extends RecordWriter<InternalRow> {

    private final FileStoreTable fileStoreTable;

    // System column field definitions, reused across batches
    private final Field bucketField;
    private final Field offsetField;
    private final Field timestampField;

    public AppendOnlyWriter(
            FileStoreTable fileStoreTable,
            TableBucket tableBucket,
            @Nullable String partition,
            List<String> partitionKeys) {
        //noinspection unchecked
        super(
                (TableWriteImpl<InternalRow>)
                        // todo: set ioManager to support write-buffer-spillable
                        fileStoreTable.newWrite(FLUSS_LAKE_TIERING_COMMIT_USER),
                fileStoreTable.rowType(),
                tableBucket,
                partition,
                partitionKeys); // Pass to parent
        this.fileStoreTable = fileStoreTable;
        this.bucketField =
                new Field(
                        TableDescriptor.BUCKET_COLUMN_NAME,
                        new FieldType(false, new ArrowType.Int(32, true), null),
                        null);
        this.offsetField =
                new Field(
                        TableDescriptor.OFFSET_COLUMN_NAME,
                        new FieldType(false, new ArrowType.Int(64, true), null),
                        null);
        this.timestampField =
                new Field(
                        TableDescriptor.TIMESTAMP_COLUMN_NAME,
                        new FieldType(
                                false, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null),
                        null);
    }

    @Override
    public void write(LogRecord record) throws Exception {
        flussRecordAsPaimonRow.setFlussRecord(record);

        // get partition once
        if (partition == null) {
            partition = tableWrite.getPartition(flussRecordAsPaimonRow);
        }

        // hacky, call internal method tableWrite.getWrite() to support
        // to write to given partition, otherwise, it'll always extract a partition from Paimon row
        // which may be costly
        int writtenBucket = bucket;
        // if bucket-unaware mode, we have to use bucket = 0 to write to follow paimon best practice
        if (fileStoreTable.store().bucketMode() == BucketMode.BUCKET_UNAWARE) {
            writtenBucket = 0;
        }
        tableWrite.getWrite().write(partition, writtenBucket, flussRecordAsPaimonRow);
    }

    /**
     * Writes an Arrow batch directly to Paimon Parquet files. Enriches the VectorSchemaRoot with
     * system columns (__bucket, __offset, __timestamp) and uses Paimon's {@link ArrowBundleRecords}
     * for efficient batch writing.
     *
     * <p>System column vectors are created using the batch's own allocator to ensure all vectors in
     * the enriched VectorSchemaRoot share the same allocator root. This is required by Arrow's C
     * Data Interface which validates allocator root identity during buffer association.
     */
    public void writeArrowBatch(ArrowBatchData arrowBatchData) throws Exception {
        int writtenBucket = bucket;
        if (fileStoreTable.store().bucketMode() == BucketMode.BUCKET_UNAWARE) {
            writtenBucket = 0;
        }

        VectorSchemaRoot originalRoot = arrowBatchData.getVectorSchemaRoot();
        BufferAllocator batchAllocator = arrowBatchData.getAllocator();
        long baseOffset = arrowBatchData.getBaseLogOffset();
        long timestamp = arrowBatchData.getTimestamp();
        int rowCount = originalRoot.getRowCount();

        // Create system column vectors using the same allocator as data vectors so that
        // all vectors share the same allocator root for Paimon's Arrow C Data serialization.
        try (IntVector batchBucketVector = new IntVector(bucketField, batchAllocator);
                BigIntVector batchOffsetVector = new BigIntVector(offsetField, batchAllocator);
                TimeStampMilliVector batchTimestampVector =
                        new TimeStampMilliVector(timestampField, batchAllocator)) {
            populateSystemColumns(
                    batchBucketVector,
                    batchOffsetVector,
                    batchTimestampVector,
                    bucket,
                    baseOffset,
                    timestamp,
                    rowCount);

            VectorSchemaRoot enrichedRoot =
                    buildEnrichedRoot(
                            originalRoot,
                            batchBucketVector,
                            batchOffsetVector,
                            batchTimestampVector);

            ArrowBundleRecords arrowBundleRecords =
                    new ArrowBundleRecords(enrichedRoot, tableRowType, false);

            // derive partition from the first row if not yet determined
            if (partition == null) {
                // todo: optimize how to get paimon partition
                InternalRow firstRow = arrowBundleRecords.iterator().next();
                partition = tableWrite.getPartition(firstRow);
            }

            tableWrite.writeBundle(partition, writtenBucket, arrowBundleRecords);
        }
    }

    /** Builds an enriched VectorSchemaRoot with original data vectors plus system columns. */
    private VectorSchemaRoot buildEnrichedRoot(
            VectorSchemaRoot originalRoot,
            IntVector batchBucketVector,
            BigIntVector batchOffsetVector,
            TimeStampMilliVector batchTimestampVector) {
        List<Field> originalFields = originalRoot.getSchema().getFields();
        List<Field> enrichedFields = new ArrayList<>(originalFields);
        enrichedFields.add(bucketField);
        enrichedFields.add(offsetField);
        enrichedFields.add(timestampField);
        Schema enrichedSchema = new Schema(enrichedFields);

        List<FieldVector> allVectors = new ArrayList<>();
        for (int i = 0; i < originalFields.size(); i++) {
            allVectors.add(originalRoot.getVector(i));
        }
        allVectors.add(batchBucketVector);
        allVectors.add(batchOffsetVector);
        allVectors.add(batchTimestampVector);

        return new VectorSchemaRoot(enrichedSchema, allVectors, originalRoot.getRowCount());
    }

    /** Populates system column vectors with bucket, offset, and timestamp values. */
    private static void populateSystemColumns(
            IntVector bucketVector,
            BigIntVector offsetVector,
            TimeStampMilliVector timestampVector,
            int bucket,
            long baseOffset,
            long timestamp,
            int rowCount) {
        bucketVector.allocateNew(rowCount);
        offsetVector.allocateNew(rowCount);
        timestampVector.allocateNew(rowCount);

        for (int i = 0; i < rowCount; i++) {
            bucketVector.set(i, bucket);
        }
        bucketVector.setValueCount(rowCount);

        for (int i = 0; i < rowCount; i++) {
            offsetVector.set(i, baseOffset + i);
        }
        offsetVector.setValueCount(rowCount);

        for (int i = 0; i < rowCount; i++) {
            timestampVector.set(i, timestamp);
        }
        timestampVector.setValueCount(rowCount);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
