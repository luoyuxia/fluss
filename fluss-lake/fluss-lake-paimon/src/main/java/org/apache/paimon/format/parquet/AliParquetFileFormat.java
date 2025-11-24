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

import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parquet {@link FileFormat} for native. */
public class AliParquetFileFormat extends ParquetFileFormat {

    private static final Logger LOG = LoggerFactory.getLogger(AliParquetFileFormat.class);

    private final FileFormatFactory.FormatContext formatContext;

    public AliParquetFileFormat(FileFormatFactory.FormatContext formatContext) {
        super(formatContext);
        this.formatContext = formatContext;
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        if (TypeCheckVisitor.supportNativeWrite(type)) {
            LOG.info("Use native write");
            return new AliParquetWriterFactory(
                    type,
                    () -> new ArrowFormatCWriter(type, formatContext.writeBatchSize(), true),
                    formatContext.writeBatchSize(),
                    formatContext.zstdLevel());
        } else {
            LOG.info("Use normal write");
            return super.createWriterFactory(type);
        }
    }

    /** Check if the type meet the native parquet requirement. */
    private static class TypeCheckVisitor implements DataTypeVisitor<Boolean> {

        private static final TypeCheckVisitor VISITOR = new TypeCheckVisitor();

        public static boolean supportNativeWrite(RowType type) {
            return type.accept(VISITOR);
        }

        @Override
        public Boolean visit(CharType charType) {
            return true;
        }

        @Override
        public Boolean visit(VarCharType varCharType) {
            return true;
        }

        @Override
        public Boolean visit(BooleanType booleanType) {
            return true;
        }

        @Override
        public Boolean visit(BinaryType binaryType) {
            return true;
        }

        @Override
        public Boolean visit(VarBinaryType varBinaryType) {
            return true;
        }

        @Override
        public Boolean visit(DecimalType decimalType) {
            return true;
        }

        @Override
        public Boolean visit(TinyIntType tinyIntType) {
            return true;
        }

        @Override
        public Boolean visit(SmallIntType smallIntType) {
            return true;
        }

        @Override
        public Boolean visit(IntType intType) {
            return true;
        }

        @Override
        public Boolean visit(BigIntType bigIntType) {
            return true;
        }

        @Override
        public Boolean visit(FloatType floatType) {
            return true;
        }

        @Override
        public Boolean visit(DoubleType doubleType) {
            return true;
        }

        @Override
        public Boolean visit(DateType dateType) {
            return true;
        }

        @Override
        public Boolean visit(TimeType timeType) {
            return true;
        }

        @Override
        public Boolean visit(TimestampType timestampType) {
            int precision = DataTypeChecks.getPrecision(timestampType);
            return precision <= 9;
        }

        @Override
        public Boolean visit(LocalZonedTimestampType localZonedTimestampType) {
            int precision = DataTypeChecks.getPrecision(localZonedTimestampType);
            return precision <= 9;
        }

        @Override
        public Boolean visit(VariantType variantType) {
            // native write does not support variant type at the moment.
            return false;
        }

        @Override
        public Boolean visit(ArrayType arrayType) {
            // NOTE: Arrow 14.0.0 arrow type serialize arrow type has a bug. If meet null, it will
            // fail to deserialize in c++ side. JDK 8 support up to arrow 14.0.0.
            // TODO: Enable array type when arrow 15.0.0 is binded.
            return false;
        }

        @Override
        public Boolean visit(MultisetType multisetType) {
            return multisetType.getElementType().accept(this);
        }

        @Override
        public Boolean visit(MapType mapType) {
            return mapType.getKeyType().accept(this) && mapType.getValueType().accept(this);
        }

        @Override
        public Boolean visit(RowType rowType) {
            for (DataType type : rowType.getFieldTypes()) {
                if (!type.accept(this)) {
                    return false;
                }
            }
            return true;
        }
    }
}
