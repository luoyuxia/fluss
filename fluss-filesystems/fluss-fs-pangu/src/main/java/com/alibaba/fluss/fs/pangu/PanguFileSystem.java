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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileStatus;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.local.LocalFileSystem;
import com.alibaba.fluss.fs.pangu.stream.PanguDataInputStream;
import com.alibaba.fluss.fs.pangu.stream.PanguDataOutputStream;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.pangu.model.enums.PanguFileType;
import com.alibaba.pangu.model.enums.PanguIoAdvice;
import com.alibaba.pangu.model.enums.PanguQosFlow;
import com.alibaba.ververica.pangu.common.CommonPanguFileStatus;
import com.alibaba.ververica.pangu.common.CommonPanguFileSystem;
import com.alibaba.ververica.pangu.common.conf.PanguFileCreateConfig;
import com.alibaba.ververica.pangu.common.util.PanguSdkMode;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;

import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.TYPE_INFO;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguConfig.createPanguConfig;
import static com.alibaba.fluss.fs.pangu.conf.FlussPanguFileSystemConfig.createFsConfig;

/** Adapt Pangu client API to Fluss file system. */
public class PanguFileSystem extends FileSystem implements AutoCloseable {

    private static final ObtainedSecurityToken TOKEN =
            new ObtainedSecurityToken(
                    PanguFileSystemPlugin.SCHEME, new byte[0], null, Collections.emptyMap());

    private final URI fsUri;
    private final boolean isMock;

    private CommonPanguFileSystem commonPanguFs;

    private PanguFileCreateConfig panguFileCreateConfig;

    public PanguFileSystem(URI fsUri) {
        this.fsUri = fsUri;
        this.isMock = Boolean.parseBoolean(System.getProperty("pangu.sdk.mock"));
    }

    /** Initial pangu library (.so) with pangu_fs_create. */
    public PanguFileSystem initPanguFs(Configuration configuration) throws IOException {
        commonPanguFs =
                CommonPanguFileSystem.getInstance(
                        createPanguConfig(configuration, isMock),
                        createFsConfig(configuration),
                        fsUri);
        initPanguFileCreateConfig(configuration);
        return this;
    }

    @VisibleForTesting
    PanguFileSystem initPanguFsNotLoadSo(Configuration configuration) {
        commonPanguFs =
                CommonPanguFileSystem.getInstanceNotLoadSo(
                        createPanguConfig(configuration, isMock),
                        createFsConfig(configuration),
                        fsUri);
        initPanguFileCreateConfig(configuration);
        return this;
    }

    private void initPanguFileCreateConfig(Configuration configuration) {
        panguFileCreateConfig =
                PanguFileCreateConfig.newBuilder()
                        .setTypeInfo(configuration.getString(TYPE_INFO))
                        .build();
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() {
        return TOKEN;
    }

    @Override
    public URI getUri() {
        return fsUri;
    }

    /**
     * Return a file status object that represents the path.
     *
     * <p>Pangu use a slash ending to distinct directory or file. If the path is not end with slash
     * and not found for the first time, we will append a slash to get again.
     */
    @Override
    public FileStatus getFileStatus(FsPath f) throws IOException {
        return new PanguFileStatus(commonPanguFs.getFileStatus(f.toUri()));
    }

    @Override
    public FSDataInputStream open(FsPath f) throws IOException {
        return new PanguDataInputStream(
                commonPanguFs.open(f.toUri(), commonPanguFs.getFsConfig().getReadBufferSize()));
    }

    @Override
    public FileStatus[] listStatus(FsPath f) throws IOException {
        CommonPanguFileStatus[] commonPanguFileStatuses = commonPanguFs.listStatus(f.toUri());

        return Arrays.stream(commonPanguFileStatuses)
                .map(PanguFileStatus::new)
                .toArray(PanguFileStatus[]::new);
    }

    @Override
    public boolean delete(FsPath f, boolean recursive) throws IOException {
        return commonPanguFs.delete(f.toUri(), recursive);
    }

    /**
     * Recursively create directory. The behaviour of the API is not equal to {@link FileSystem},
     * but is aligned to {@link LocalFileSystem}. If dir f exists in the end (either created by
     * mkdirs or already-exist pre mkdirs), return true. If f is a file, return false. Otherwise,
     * throw exception.
     */
    @Override
    public boolean mkdirs(FsPath f) throws IOException {
        return commonPanguFs.mkdirs(f.toUri());
    }

    @Override
    public FSDataOutputStream create(FsPath f, WriteMode overwriteMode) throws IOException {
        switch (overwriteMode) {
            case OVERWRITE:
                return new PanguDataOutputStream(
                        commonPanguFs.create(
                                f.toUri(),
                                CommonPanguFileSystem.WriteMode.OVERWRITE,
                                panguFileCreateConfig));
            case NO_OVERWRITE:
                return new PanguDataOutputStream(
                        commonPanguFs.create(
                                f.toUri(),
                                CommonPanguFileSystem.WriteMode.NO_OVERWRITE,
                                panguFileCreateConfig));
            default:
                throw new UnsupportedOperationException("Unknown overwrite mode " + overwriteMode);
        }
    }

    @Override
    public boolean rename(FsPath src, FsPath dst) throws IOException {
        return commonPanguFs.rename(src.toUri(), dst.toUri());
    }

    @Override
    public void close() throws Exception {
        commonPanguFs.close();
    }

    @VisibleForTesting
    void closeNotLoadSo() {
        commonPanguFs.closeNoLoadSo();
    }

    @VisibleForTesting
    String getExtraInfo() {
        return commonPanguFs.getExtraInfo();
    }

    // ========================================================
    //  Getter for config testing.
    // ========================================================

    @VisibleForTesting
    int getReadBufferSize() {
        return commonPanguFs.getFsConfig().getReadBufferSize();
    }

    @VisibleForTesting
    int getMaxWriteBufferSize() {
        return commonPanguFs.getFsConfig().getMaxWriteBufferSize();
    }

    @VisibleForTesting
    boolean isReleaseBufferAfterBatchRead() {
        return commonPanguFs.getFsConfig().isReleaseBufferAfterBatchRead();
    }

    @VisibleForTesting
    int getBufferPoolSize() {
        return commonPanguFs.getFsConfig().getBufferPoolSize();
    }

    @VisibleForTesting
    PanguSdkMode getSdkMode() {
        return commonPanguFs.getPanguConfig().getSdkMode();
    }

    @VisibleForTesting
    PanguQosFlow getQosFlow() {
        return commonPanguFs.getPanguConfig().getQosFlow();
    }

    @VisibleForTesting
    PanguIoAdvice getIoAdvice() {
        return commonPanguFs.getPanguConfig().getIoAdvice();
    }

    @VisibleForTesting
    boolean isUseSyncIo() {
        return commonPanguFs.getPanguConfig().isUseSyncIo();
    }

    @VisibleForTesting
    PanguFileType getFileType() {
        return commonPanguFs.getPanguConfig().getFileType();
    }

    @Nullable
    @VisibleForTesting
    String getTypeInfo() {
        return panguFileCreateConfig.getTypeInfo();
    }

    @VisibleForTesting
    long getBlockSize() {
        return commonPanguFs.getPanguConfig().getBlockSize();
    }

    @VisibleForTesting
    boolean isEnableChecksum() {
        return commonPanguFs.getPanguConfig().isEnableChecksum();
    }

    // ========================================================
    //  Export config to outside.
    // ========================================================

    @VisibleForTesting
    public long getReadaheadSizeLimit() {
        return commonPanguFs.getPanguConfig().getReadaheadSizeLimit();
    }

    @VisibleForTesting
    public long getWriteBufferSizeLimit() {
        return commonPanguFs.getPanguConfig().getWriteBufferSizeLimit();
    }

    @VisibleForTesting
    public String getLogPath() {
        return commonPanguFs.getPanguConfig().getLogPath();
    }
}
