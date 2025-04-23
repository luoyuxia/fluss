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

package com.alibaba.fluss.lake.paimon.fs;

import com.alibaba.fluss.fs.FSDataInputStream;
import com.alibaba.fluss.fs.FSDataOutputStream;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.util.Locale;

import static com.alibaba.fluss.fs.FileSystem.WriteMode;

/** . */
public class FlussFileIO implements FileIO {

    private final FileSystem fs;

    public FlussFileIO(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public boolean isObjectStore() {
        String scheme = fs.getUri().getScheme().toLowerCase(Locale.US);
        if (scheme.startsWith("s3")
                || scheme.startsWith("emr")
                || scheme.startsWith("oss")
                || scheme.startsWith("wasb")
                || scheme.startsWith("gs")) {
            // the Amazon S3 storage or Aliyun OSS storage or Azure Blob Storage
            // or Google Cloud Storage
            return true;
        } else if (scheme.startsWith("http") || scheme.startsWith("ftp")) {
            // file servers instead of file systems
            // they might actually be consistent, but we have no hard guarantees
            // currently to rely on that
            return true;
        } else {
            // the remainder should include hdfs, kosmos, ceph, ...
            // this also includes federated HDFS (viewfs).
            return false;
        }
    }

    @Override
    public void configure(CatalogContext catalogContext) {
        // nothing to do
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return new FlussSeekableInputStream(fs.open(fsPath(path)));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return new FlussPositionOutputStream(
                fs.create(fsPath(path), overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return new FlussFileStatus(fs.getFileStatus(fsPath(path)));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        FileStatus[] statuses = new FileStatus[0];
        com.alibaba.fluss.fs.FileStatus[] flussStatus = fs.listStatus(fsPath(path));
        if (flussStatus != null) {
            statuses = new FileStatus[flussStatus.length];
            for (int i = 0; i < flussStatus.length; i++) {
                statuses[i] = new FlussFileStatus(flussStatus[i]);
            }
        }
        return statuses;
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return fs.exists(fsPath(path));
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return fs.delete(fsPath(path), recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return fs.mkdirs(fsPath(path));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return fs.rename(fsPath(src), fsPath(dst));
    }

    private FsPath fsPath(Path path) {
        return new FsPath(path.toUri());
    }

    private static class FlussSeekableInputStream extends SeekableInputStream {

        private final FSDataInputStream in;

        public FlussSeekableInputStream(FSDataInputStream in) {
            this.in = in;
        }

        @Override
        public void seek(long seekPos) throws IOException {
            in.seek(seekPos);
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
        public int read(byte[] bytes, int off, int len) throws IOException {
            return in.read(bytes, off, len);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    private static class FlussPositionOutputStream extends PositionOutputStream {

        private final FSDataOutputStream out;

        public FlussPositionOutputStream(FSDataOutputStream out) {
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
        public void write(byte[] bytes) throws IOException {
            out.write(bytes);
        }

        @Override
        public void write(byte[] bytes, int off, int len) throws IOException {
            out.write(bytes, off, len);
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

    private static class FlussFileStatus implements FileStatus {

        private final com.alibaba.fluss.fs.FileStatus status;

        public FlussFileStatus(com.alibaba.fluss.fs.FileStatus status) {
            this.status = status;
        }

        @Override
        public long getLen() {
            return status.getLen();
        }

        @Override
        public boolean isDir() {
            return status.isDir();
        }

        @Override
        public Path getPath() {
            return new Path(status.getPath().toUri());
        }

        @Override
        public long getModificationTime() {
            throw new UnsupportedOperationException("Method getModificationTime is not supported.");
        }
    }
}
