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

package org.apache.paimon.format.parquet;

import org.apache.arrow.dataset.file.ParquetWriter;
import org.apache.arrow.dataset.file.ParquetWriterProperties;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.paimon.arrow.ArrowUtils;
import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.common.ArrowBundleWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** A factory to create Parquet {@link FormatWriter}. */
public class AliParquetWriterFactory implements FormatWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AliParquetWriterFactory.class);
    private static final String DATASET_JNI_X86_64 =
            "arrow_dataset_jni/x86_64/libarrow_dataset_jni.so";
    private static final String DATASET_JNI_AARCH64 =
            "arrow_dataset_jni/aarch_64/libarrow_dataset_jni.so";

    private final RowType rowType;
    private final int writeBatchSize;
    private final int zstdLevel;
    private final Supplier<ArrowFormatCWriter> arrowFormatWriterSupplier;

    public AliParquetWriterFactory(
            RowType rowType,
            Supplier<ArrowFormatCWriter> arrowFormatWriterSupplier,
            int writeBatchSize,
            int zstdLevel) {
        this.rowType = rowType;
        this.arrowFormatWriterSupplier = arrowFormatWriterSupplier;
        this.writeBatchSize = writeBatchSize;
        this.zstdLevel = zstdLevel;
    }

    @Override
    public FormatWriter create(
            PositionOutputStream positionOutputStream, @Nullable String compression)
            throws IOException {
        ArrowFormatCWriter arrowFormatCWriter = arrowFormatWriterSupplier.get();
        ParquetWriter parquetWriter =
                initNativeWriter(
                        rowType, positionOutputStream, compression, writeBatchSize, zstdLevel);

        return new ArrowBundleWriter(positionOutputStream, parquetWriter, arrowFormatCWriter);
    }

    private ParquetWriter initNativeWriter(
            RowType rowType,
            PositionOutputStream positionOutputStream,
            String compression,
            int writeBatchSize,
            int compressionLevel)
            throws IOException {

        List<Field> fields = new ArrayList<>();
        for (DataField dataField : rowType.getFields()) {
            fields.add(
                    ArrowUtils.toArrowField(dataField.name(), dataField.id(), dataField.type(), 0));
        }

        ParquetWriterProperties parquetWriterProperties =
                ParquetWriterProperties.builder()
                        .compressionCodec(compression)
                        .compressionLevel(compressionLevel)
                        .writeBatchSize(writeBatchSize)
                        .build();

        Schema schema = new Schema(fields);
        try {
            return new ParquetWriter(positionOutputStream, schema, parquetWriterProperties);
        } catch (Throwable t) {
            ClassLoader classLoader = ParquetWriter.class.getClassLoader();
            LOG.error(
                    "Failed to initialize Arrow native parquet writer. os.name={}, os.arch={}, java.io.tmpdir={}, java.library.path={}, parquetWriterClassLoader={}, hasX86_64DatasetJni={}, hasAarch64DatasetJni={}, errorChain={}",
                    System.getProperty("os.name"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.io.tmpdir"),
                    System.getProperty("java.library.path"),
                    classLoader,
                    classLoader != null && classLoader.getResource(DATASET_JNI_X86_64) != null,
                    classLoader != null && classLoader.getResource(DATASET_JNI_AARCH64) != null,
                    formatThrowableChain(t),
                    t);
            throw new IOException(t);
        }
    }

    private static String formatThrowableChain(Throwable throwable) {
        StringBuilder builder = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (builder.length() > 0) {
                builder.append(" -> ");
            }
            builder.append(current.getClass().getName());
            String message = current.getMessage();
            if (message != null && !message.isEmpty()) {
                builder.append(": ").append(message);
            }
            current = current.getCause();
        }
        return builder.toString();
    }
}
