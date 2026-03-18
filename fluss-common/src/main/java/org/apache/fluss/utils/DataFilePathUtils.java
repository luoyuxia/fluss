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

package org.apache.fluss.utils;

import org.apache.fluss.fs.FsPath;

import java.net.URI;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Paths;
import java.util.Objects;

/** Utilities for canonicalizing data file paths used by DV bookkeeping. */
public final class DataFilePathUtils {

    private DataFilePathUtils() {}

    public static String normalizeDataFilePath(String filePath) {
        Objects.requireNonNull(filePath, "filePath cannot be null");

        try {
            URI uri = URI.create(filePath);
            String scheme = uri.getScheme();
            if (scheme == null) {
                return normalizeLocalPath(filePath);
            }
            if ("file".equalsIgnoreCase(scheme)
                    && (uri.getAuthority() == null
                            || uri.getAuthority().isEmpty()
                            || "localhost".equalsIgnoreCase(uri.getAuthority()))) {
                return normalizeLocalPath(Paths.get(uri).normalize().toString());
            }
        } catch (IllegalArgumentException | FileSystemNotFoundException e) {
            return normalizeLocalPath(filePath);
        }

        return new FsPath(filePath).toString();
    }

    private static String normalizeLocalPath(String filePath) {
        String normalizedPath = filePath.replace('\\', '/').replaceAll("/+", "/");
        if (normalizedPath.endsWith("/") && normalizedPath.length() > 1) {
            return normalizedPath.substring(0, normalizedPath.length() - 1);
        }
        return normalizedPath;
    }
}
