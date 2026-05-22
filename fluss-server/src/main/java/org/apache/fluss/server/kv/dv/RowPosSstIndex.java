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

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.utils.json.JsonDeserializer;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.JsonSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Unified index for one snapshot's RowPos SST files ({@code index.json}).
 *
 * <p>Records which buckets participated in a snapshot and lists the SST files for each bucket. This
 * replaces the need for separate cross-bucket index and per-bucket manifest files.
 */
@Internal
public class RowPosSstIndex {

    private static final int VERSION = 1;
    private static final String VERSION_KEY = "version";
    private static final String BUCKETS_KEY = "buckets";
    private static final String FILES_KEY = "files";
    private static final String NAME_KEY = "name";
    private static final String SIZE_KEY = "size";

    private static final RowPosSstIndexJsonSerde JSON_SERDE = new RowPosSstIndexJsonSerde();

    private final Map<Integer, List<SstFileEntry>> bucketFiles;

    public RowPosSstIndex(Map<Integer, List<SstFileEntry>> bucketFiles) {
        this.bucketFiles = bucketFiles;
    }

    /** Returns the set of bucket IDs that have SST files in this snapshot. */
    public Set<Integer> getBucketIds() {
        return Collections.unmodifiableSet(bucketFiles.keySet());
    }

    /** Returns the SST file list for the given bucket. Returns an empty list if not present. */
    public List<SstFileEntry> getFiles(int bucketId) {
        List<SstFileEntry> files = bucketFiles.get(bucketId);
        return files == null ? Collections.<SstFileEntry>emptyList() : files;
    }

    /** Serializes this index to JSON bytes. */
    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, JSON_SERDE);
    }

    /** Deserializes an index from JSON bytes. */
    public static RowPosSstIndex fromJsonBytes(byte[] bytes) {
        return JsonSerdeUtils.readValue(bytes, JSON_SERDE);
    }

    /** An SST file entry within the index. */
    public static class SstFileEntry {
        private final String fileName;
        private final long fileSize;

        public SstFileEntry(String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }

        public String getFileName() {
            return fileName;
        }

        public long getFileSize() {
            return fileSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SstFileEntry that = (SstFileEntry) o;
            return fileSize == that.fileSize && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileName, fileSize);
        }

        @Override
        public String toString() {
            return "SstFileEntry{name='" + fileName + "', size=" + fileSize + '}';
        }
    }

    // ---- JSON serialization / deserialization ----

    private static class RowPosSstIndexJsonSerde
            implements JsonSerializer<RowPosSstIndex>, JsonDeserializer<RowPosSstIndex> {

        @Override
        public void serialize(RowPosSstIndex index, JsonGenerator generator) throws IOException {
            generator.writeStartObject();
            generator.writeNumberField(VERSION_KEY, VERSION);

            generator.writeObjectFieldStart(BUCKETS_KEY);
            for (Map.Entry<Integer, List<SstFileEntry>> entry : index.bucketFiles.entrySet()) {
                generator.writeObjectFieldStart(String.valueOf(entry.getKey()));
                generator.writeArrayFieldStart(FILES_KEY);
                for (SstFileEntry file : entry.getValue()) {
                    generator.writeStartObject();
                    generator.writeStringField(NAME_KEY, file.getFileName());
                    generator.writeNumberField(SIZE_KEY, file.getFileSize());
                    generator.writeEndObject();
                }
                generator.writeEndArray();
                generator.writeEndObject();
            }
            generator.writeEndObject();

            generator.writeEndObject();
        }

        @Override
        public RowPosSstIndex deserialize(JsonNode node) {
            Map<Integer, List<SstFileEntry>> bucketFiles = new HashMap<>();
            JsonNode bucketsNode = node.get(BUCKETS_KEY);
            if (bucketsNode != null) {
                Iterator<String> fieldNames = bucketsNode.fieldNames();
                while (fieldNames.hasNext()) {
                    String bucketIdStr = fieldNames.next();
                    int bucketId = Integer.parseInt(bucketIdStr);
                    JsonNode bucketNode = bucketsNode.get(bucketIdStr);
                    JsonNode filesNode = bucketNode.get(FILES_KEY);

                    List<SstFileEntry> files = new ArrayList<>();
                    for (JsonNode fileNode : filesNode) {
                        String name = fileNode.get(NAME_KEY).asText();
                        long size = fileNode.get(SIZE_KEY).asLong();
                        files.add(new SstFileEntry(name, size));
                    }
                    bucketFiles.put(bucketId, files);
                }
            }
            return new RowPosSstIndex(bucketFiles);
        }
    }
}
