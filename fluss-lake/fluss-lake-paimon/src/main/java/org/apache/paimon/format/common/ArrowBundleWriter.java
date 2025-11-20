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

package org.apache.paimon.format.common;

import org.apache.arrow.dataset.file.ParquetWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.BundleRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Arrow bundle writer. */
public class ArrowBundleWriter implements BundleFormatWriter {
    private static final Logger LOG = LoggerFactory.getLogger(ArrowBundleWriter.class);

    protected final PositionOutputStream underlyingStream;
    protected final ParquetWriter parquetWriter;
    private final ArrowFormatCWriter arrowFormatWriter;

    public ArrowBundleWriter(
            PositionOutputStream underlyingStream,
            ParquetWriter parquetWriter,
            ArrowFormatCWriter arrowFormatWriter) {
        this.underlyingStream = underlyingStream;
        this.parquetWriter = parquetWriter;
        this.arrowFormatWriter = arrowFormatWriter;
    }

    @Override
    public void writeBundle(BundleRecords bundleRecords) throws IOException {
        if (bundleRecords instanceof ArrowBundleRecords) {
            add(((ArrowBundleRecords) bundleRecords).getVectorSchemaRoot());
        } else {
            for (InternalRow row : bundleRecords) {
                addElement(row);
            }
        }
    }

    public void add(VectorSchemaRoot vsr) throws IOException {
        parquetWriter.write(vsr);
    }

    @Override
    public void addElement(InternalRow internalRow) throws IOException {
        if (!arrowFormatWriter.write(internalRow)) {
            flush();
            if (!arrowFormatWriter.write(internalRow)) {
                throw new RuntimeException("Exception happens while write to orc file");
            }
        }
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return suggestedCheck && (underlyingStream.getPos() > targetSize);
    }

    @Override
    public void close() throws IOException {
        flush();
        this.parquetWriter.close();
        this.arrowFormatWriter.close();
    }

    private void flush() throws IOException {
        if (!arrowFormatWriter.empty()) {
            arrowFormatWriter.flush();
            VectorSchemaRoot schemaRoot = arrowFormatWriter.getVectorSchemaRoot();
            this.parquetWriter.write(schemaRoot);
            arrowFormatWriter.reset();
        }
    }
}
