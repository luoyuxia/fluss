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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/** A factory to create Parquet {@link FormatWriter}. */
public class AliParquetWriterFactory implements FormatWriterFactory {

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
        return new ParquetWriter(positionOutputStream, schema, parquetWriterProperties);
    }
}
