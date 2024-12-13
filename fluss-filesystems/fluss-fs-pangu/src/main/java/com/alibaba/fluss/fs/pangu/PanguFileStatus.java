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

package com.alibaba.fluss.fs.pangu;

import com.alibaba.fluss.fs.FileStatus;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.ververica.pangu.common.CommonPanguFileStatus;

import java.net.URISyntaxException;

/** Pangu entry status of directory or file. */
public class PanguFileStatus implements FileStatus {

    private final CommonPanguFileStatus commonPanguFileStatus;

    public PanguFileStatus(CommonPanguFileStatus commonPanguFileStatus) {
        this.commonPanguFileStatus = commonPanguFileStatus;
    }

    @Override
    public long getLen() {
        return commonPanguFileStatus.getLen();
    }

    @Override
    public boolean isDir() {
        return commonPanguFileStatus.isDir();
    }

    @Override
    public FsPath getPath() {
        try {
            return new FsPath(commonPanguFileStatus.getPath());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
