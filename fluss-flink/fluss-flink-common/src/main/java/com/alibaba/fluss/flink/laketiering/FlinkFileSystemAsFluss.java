/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.laketiering;

import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileStatus;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.net.URI;

/** . */
public class FlinkFileSystemAsFluss extends FileSystem {

    private final Path path;

    public FlinkFileSystemAsFluss(Path path) {
        this.path = path;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        throw new UnsupportedOperationException("Method obtainSecurityToken is not supported.");
    }

    @Override
    public URI getUri() {
        return path.toUri();
    }

    @Override
    public FileStatus getFileStatus(FsPath f) throws IOException {
        return new FlussFileStatue(fileSystem(f).getFileStatus(toPath(f)));
    }

    @Override
    public FSDataInputStream open(FsPath f) throws IOException {
        return new FlussFSDataInputStream(fileSystem(f).open(toPath(f)));
    }

    @Override
    public FileStatus[] listStatus(FsPath f) throws IOException {
        FileStatus[] fileStatuses = new FlussFileStatue[0];
        org.apache.flink.core.fs.FileStatus[] flinkFileStatuses =
                fileSystem(f).listStatus(toPath(f));
        if (flinkFileStatuses != null) {
            fileStatuses = new FileStatus[flinkFileStatuses.length];
            for (int i = 0; i < flinkFileStatuses.length; i++) {
                fileStatuses[i] = new FlussFileStatue(flinkFileStatuses[i]);
            }
        }
        return fileStatuses;
    }

    @Override
    public boolean delete(FsPath f, boolean recursive) throws IOException {
        return fileSystem(f).delete(toPath(f), recursive);
    }

    @Override
    public boolean mkdirs(FsPath f) throws IOException {
        return fileSystem(f).mkdirs(toPath(f));
    }

    @Override
    public FSDataOutputStream create(FsPath f, WriteMode overwriteMode) throws IOException {
        return new FlussFSDataOutputStream(
                fileSystem(f)
                        .create(
                                toPath(f),
                                overwriteMode == WriteMode.OVERWRITE
                                        ? org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE
                                        : org.apache.flink.core.fs.FileSystem.WriteMode
                                                .NO_OVERWRITE));
    }

    @Override
    public boolean rename(FsPath src, FsPath dst) throws IOException {
        return fileSystem(src).rename(toPath(src), toPath(dst));
    }

    private Path toPath(FsPath path) {
        return new Path(path.toUri());
    }

    private org.apache.flink.core.fs.FileSystem fileSystem(FsPath fsPath) throws IOException {
        return org.apache.flink.core.fs.FileSystem.get(fsPath.toUri());
    }

    private static class FlussFSDataOutputStream extends FSDataOutputStream {
        private final org.apache.flink.core.fs.FSDataOutputStream out;

        public FlussFSDataOutputStream(org.apache.flink.core.fs.FSDataOutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() throws IOException {
            return out.getPos();
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static class FlussFSDataInputStream extends FSDataInputStream {
        private final org.apache.flink.core.fs.FSDataInputStream in;

        public FlussFSDataInputStream(org.apache.flink.core.fs.FSDataInputStream in) {
            this.in = in;
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
        }

        @Override
        public int read() throws IOException {
            return in.read();
        }

        @Override
        public void seek(long pos) throws IOException {
            in.seek(pos);
        }
    }

    private static class FlussFileStatue implements FileStatus {

        private final org.apache.flink.core.fs.FileStatus fileStatus;

        public FlussFileStatue(org.apache.flink.core.fs.FileStatus fileStatus) {
            this.fileStatus = fileStatus;
        }

        @Override
        public long getLen() {
            return fileStatus.getLen();
        }

        @Override
        public boolean isDir() {
            return fileStatus.isDir();
        }

        @Override
        public FsPath getPath() {
            return new FsPath(fileStatus.getPath().toUri());
        }
    }
}
