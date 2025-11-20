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

import org.apache.fluss.lake.batch.ArrowRecordBatch;
import org.apache.fluss.lake.paimon.tiering.RecordWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.record.FlussArrowRecordBatch;
import org.apache.fluss.record.LogRecord;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampVector;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.lake.paimon.tiering.PaimonLakeTieringFactory.FLUSS_LAKE_TIERING_COMMIT_USER;

/** A {@link RecordWriter} to write to Paimon's append-only table. */
public class AppendOnlyWriter extends RecordWriter<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(AppendOnlyWriter.class);

    private final FileStoreTable fileStoreTable;

    // Reusable resources for enriched VectorSchemaRoot
    @Nullable private VectorSchemaRoot enrichedRoot;
    @Nullable private Schema enrichedSchema;
    private int originalFieldCount = -1;
    @Nullable private IntVector bucketVector;
    @Nullable private BigIntVector offsetVector;
    @Nullable private TimeStampVector timestampVector;

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

    @Override
    public void writeBatch(ArrowRecordBatch batch) throws Exception {
        // hacky, call internal method tableWrite.getWrite() to support
        // to write to given partition, otherwise, it'll always extract a partition from Paimon row
        // which may be costly
        int writtenBucket = bucket;
        // if bucket-unaware mode, we have to use bucket = 0 to write to follow paimon best practice
        if (fileStoreTable.store().bucketMode() == BucketMode.BUCKET_UNAWARE) {
            writtenBucket = 0;
        }

        FlussArrowRecordBatch flussBatch = batch.getFlussArrowRecordBatch();
        VectorSchemaRoot originalRoot = flussBatch.getSchemaRoot();
        long baseOffset = flussBatch.getBaseOffset();
        long timestamp = flussBatch.getTimestamp();
        int rowCount = originalRoot.getRowCount();

        long startMs = System.currentTimeMillis();
        // Initialize or reuse enriched VectorSchemaRoot
        ensureEnrichedRootInitialized(originalRoot);
        LOG.debug("Ensure enriched root initialize in {} ms", System.currentTimeMillis() - startMs);

        startMs = System.currentTimeMillis();
        // Update enriched root with new data
        updateEnrichedVectorSchemaRoot(writtenBucket, baseOffset, timestamp, rowCount);
        LOG.debug("Ensure enriched root update in {} ms", System.currentTimeMillis() - startMs);

        startMs = System.currentTimeMillis();
        tableWrite.writeBundle(
                partition,
                writtenBucket,
                new ArrowBundleRecords(enrichedRoot, tableRowType, false));
        LOG.debug("write bundle in {} ms", System.currentTimeMillis() - startMs);
    }

    /**
     * Ensures the enriched VectorSchemaRoot is initialized. Reuses system column vectors if schema
     * matches. Uses the same allocator as originalRoot to avoid transfer issues. Note: enrichedRoot
     * is recreated each time to reference the current originalRoot's vectors.
     */
    private void ensureEnrichedRootInitialized(VectorSchemaRoot originalRoot) {
        Schema originalSchema = originalRoot.getSchema();
        List<Field> originalFields = originalSchema.getFields();
        int currentFieldCount = originalFields.size();

        // Check if we need to recreate system column vectors (schema changed or first time)
        if (bucketVector == null || originalFieldCount != currentFieldCount) {
            // Clean up existing system column vectors if any
            if (bucketVector != null) {
                bucketVector.close();
                offsetVector.close();
                timestampVector.close();
            }

            // Create system column fields
            Field bucketField =
                    new Field(
                            TableDescriptor.BUCKET_COLUMN_NAME,
                            new FieldType(false, new ArrowType.Int(32, true), null),
                            null);
            Field offsetField =
                    new Field(
                            TableDescriptor.OFFSET_COLUMN_NAME,
                            new FieldType(false, new ArrowType.Int(64, true), null),
                            null);
            Field timestampField =
                    new Field(
                            TableDescriptor.TIMESTAMP_COLUMN_NAME,
                            new FieldType(
                                    false,
                                    new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Asia/Shanghai"),
                                    null),
                            null);

            // Create new schema with original fields + system columns
            List<Field> enrichedFields = new ArrayList<>(originalFields);
            enrichedFields.add(bucketField);
            enrichedFields.add(offsetField);
            enrichedFields.add(timestampField);
            enrichedSchema = new Schema(enrichedFields);

            // Use the same allocator as originalRoot to avoid transfer issues
            BufferAllocator originalAllocator =
                    originalRoot.getFieldVectors().get(0).getAllocator();

            // Create system column vectors using the same allocator
            bucketVector = new IntVector(bucketField, originalAllocator);
            offsetVector = new BigIntVector(offsetField, originalAllocator);
            timestampVector = new TimeStampMicroVector(timestampField, originalAllocator);

            originalFieldCount = currentFieldCount;
        }

        // Always recreate enrichedRoot to reference the current originalRoot's vectors
        // (originalRoot may be different for each batch, but we reuse system column vectors)
        List<FieldVector> allVectors = new ArrayList<>();
        for (int i = 0; i < currentFieldCount; i++) {
            allVectors.add(originalRoot.getVector(i));
        }
        allVectors.add(bucketVector);
        allVectors.add(offsetVector);
        allVectors.add(timestampVector);

        enrichedRoot = new VectorSchemaRoot(enrichedSchema, allVectors, originalRoot.getRowCount());
    }

    /**
     * Updates the enriched VectorSchemaRoot with new data. Original data columns are already
     * referenced directly, we only need to update system columns. Reuses allocated memory when
     * possible to avoid frequent allocation/deallocation.
     */
    private void updateEnrichedVectorSchemaRoot(
            int bucket, long baseOffset, long timestamp, int rowCount) {
        // Original data columns are already referenced directly in enrichedRoot,
        // so we don't need to copy them. Just update the row count.
        enrichedRoot.setRowCount(rowCount);

        // Allocate or reuse space for system columns
        // Only reallocate if current capacity is insufficient
        if (bucketVector.getValueCapacity() < rowCount) {
            bucketVector.allocateNew(rowCount);
        }
        if (offsetVector.getValueCapacity() < rowCount) {
            offsetVector.allocateNew(rowCount);
        }
        if (timestampVector.getValueCapacity() < rowCount) {
            timestampVector.allocateNew(rowCount);
        }

        // Fill __bucket column (all values are the same bucket)
        for (int i = 0; i < rowCount; i++) {
            bucketVector.set(i, bucket);
        }
        bucketVector.setValueCount(rowCount);

        // Fill __offset column (starting from baseOffset, incrementing by 1)
        for (int i = 0; i < rowCount; i++) {
            offsetVector.set(i, baseOffset + i);
        }
        offsetVector.setValueCount(rowCount);

        // Fill __timestamp column (all values are the same timestamp)
        // Convert timestamp from milliseconds to microseconds
        long timestampMicros = timestamp * 1000;
        if (timestampVector instanceof TimeStampMicroVector) {
            TimeStampMicroVector microVector = (TimeStampMicroVector) timestampVector;
            for (int i = 0; i < rowCount; i++) {
                microVector.set(i, timestampMicros);
            }
        } else {
            // Fallback for other timestamp vector types
            for (int i = 0; i < rowCount; i++) {
                timestampVector.set(i, timestampMicros);
            }
        }
        timestampVector.setValueCount(rowCount);
    }

    @Override
    public void close() throws Exception {
        // Clean up system column vectors
        // Note: enrichedRoot shares vectors with originalRoot, so we only close system vectors
        if (bucketVector != null) {
            bucketVector.close();
            bucketVector = null;
        }
        if (offsetVector != null) {
            offsetVector.close();
            offsetVector = null;
        }
        if (timestampVector != null) {
            timestampVector.close();
            timestampVector = null;
        }
        enrichedRoot = null;
        enrichedSchema = null;
        originalFieldCount = -1;

        // Call parent close
        super.close();
    }
}
