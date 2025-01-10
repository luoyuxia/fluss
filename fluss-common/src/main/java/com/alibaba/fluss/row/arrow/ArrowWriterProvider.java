/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.arrow;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.compression.CompressionUtil;
import com.alibaba.fluss.types.RowType;

/** The provider used for requesting and releasing {@link ArrowWriter}. */
@Internal
public interface ArrowWriterProvider extends AutoCloseable {
    ArrowWriter getOrCreateWriter(
            long tableId,
            int schemaId,
            int maxSizeInBytes,
            RowType schema,
            CompressionUtil.CodecType codecType);

    void recycleWriter(ArrowWriter arrowWriter);
}
