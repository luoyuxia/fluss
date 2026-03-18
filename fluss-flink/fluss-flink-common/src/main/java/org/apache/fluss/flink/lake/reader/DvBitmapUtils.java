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

package org.apache.fluss.flink.lake.reader;

import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Utilities for split-scoped DV bitmap deserialization and lookup. */
final class DvBitmapUtils {

    static final int LOG_DV_RANGE_SIZE = 1000;

    private DvBitmapUtils() {}

    static Map<String, RoaringBitmap> deserializeLakeDvSnapshot(
            @Nullable Map<String, byte[]> serializedLakeDvSnapshot) throws IOException {
        if (serializedLakeDvSnapshot == null || serializedLakeDvSnapshot.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, RoaringBitmap> result = new LinkedHashMap<>(serializedLakeDvSnapshot.size());
        for (Map.Entry<String, byte[]> entry : serializedLakeDvSnapshot.entrySet()) {
            result.put(entry.getKey(), deserializeBitmap(entry.getValue()));
        }
        return result;
    }

    static Map<Long, RoaringBitmap> deserializeLogDvSnapshot(
            @Nullable Map<Long, byte[]> serializedLogDvSnapshot) throws IOException {
        if (serializedLogDvSnapshot == null || serializedLogDvSnapshot.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Long, RoaringBitmap> result = new LinkedHashMap<>(serializedLogDvSnapshot.size());
        for (Map.Entry<Long, byte[]> entry : serializedLogDvSnapshot.entrySet()) {
            result.put(entry.getKey(), deserializeBitmap(entry.getValue()));
        }
        return result;
    }

    @Nullable
    static RoaringBitmap deserializeBitmap(@Nullable byte[] serializedBitmap) throws IOException {
        if (serializedBitmap == null || serializedBitmap.length == 0) {
            return null;
        }
        RoaringBitmap bitmap = new RoaringBitmap();
        try (DataInputStream inputStream =
                new DataInputStream(new ByteArrayInputStream(serializedBitmap))) {
            bitmap.deserialize(inputStream);
        }
        return bitmap;
    }

    static boolean isDeletedInLogDv(Map<Long, RoaringBitmap> logDvByBaseOffset, long rowId) {
        if (logDvByBaseOffset.isEmpty()) {
            return false;
        }
        long baseOffset = (rowId / LOG_DV_RANGE_SIZE) * LOG_DV_RANGE_SIZE;
        RoaringBitmap bitmap = logDvByBaseOffset.get(baseOffset);
        return bitmap != null && bitmap.contains((int) (rowId - baseOffset));
    }
}
