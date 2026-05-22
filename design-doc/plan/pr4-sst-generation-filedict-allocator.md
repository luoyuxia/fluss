# PR 4: SST 生成基础设施 + FileDictAllocator

## 目标

构建 RowId → FilePos 映射的物化和传输基础设施。每轮 Paimon compaction 完成后，TieringService 扫描 compaction 输出文件，建立 RowId → FilePos 映射，生成 RocksDB SST 文件，上传到远程存储；TabletServer 在 Prepare 阶段下载 SST，在 Readable Switch 阶段 Ingest 到 RowPosIndex。

## 设计文档参考

- [fluss-paimon-deletion-vector-design.md](../fluss-paimon-deletion-vector-design.md) §5.2.2 Step 5, §5.2 FileDictAllocator, Appendix C
- [paimon-dv-implementation-plan.md](paimon-dv-implementation-plan.md) PR 4

---

## 远程存储目录结构

按 lake snapshot ID 组织，每个 snapshot 一个目录，内含统一索引和按 bucket 分组的 SST 文件：

```
{$remoteLakeTableSnapshotDir}/
├── metadata/
│   └── {UUID}.offsets              ← 已有（lake snapshot offset 文件）
└── rowPos/
    └── {snapshotId}/                ← 对应 lake snapshot ID，一轮 readable-switch 的数据
        ├── index.json              ← 统一索引：每 bucket 的文件列表 + 大小
        ├── 0/                      ← bucket 0
        │   ├── sst_0.sst
        │   └── sst_1.sst
        └── 1/                      ← bucket 1
            └── sst_0.sst
```

**设计要点**：
- **按 snapshot 分组**：一轮的所有数据在同一目录下，清理时整目录删除
- **index.json 统一索引**：避免 S3/OSS 的 list 操作（list 延迟高且有分页问题），Reader 只需 GET 一次 index.json 即可知道该 snapshot 包含哪些 bucket 和文件
- **index.json 最后写入**：原子可见性保证，index.json 存在即表示该 snapshot 所有 SST 已上传完毕
- **确定性 bucket 子目录**：目录名即 bucketId，无需额外 UUID

`remoteLakeTableSnapshotDir` 通过 `FlussPaths.remoteLakeTableSnapshotDir(remoteDataDir, tablePath, tableId)` 计算。

---

## 改动清单

### 1. 新增类：`RowPosSstFileWriter`
**文件**：`fluss-server/.../kv/dv/RowPosSstFileWriter.java`（新建）

封装 RocksDB 的 `SstFileWriter`，将排序好的 RowId → FilePos 条目写入 SST 文件。条目必须按 RowId 升序排列（BigEndian 编码保证 RocksDB 排序正确性）。

```java
public class RowPosSstFileWriter implements Closeable {

    private static final int MAX_ENTRIES_PER_SST = 1_000_000;

    private final String outputDir;        // 本地输出目录
    private final List<SstFileMeta> generatedFiles;

    public RowPosSstFileWriter(String outputDir);

    /**
     * 将排序好的条目写入一个或多个 SST 文件。
     * 条目必须按 RowId 升序排列。
     * 超过 MAX_ENTRIES_PER_SST 时自动拆分为多个 SST 文件。
     * 空列表返回空结果（不生成文件）。
     */
    public List<SstFileMeta> write(List<RowPosEntry> sortedEntries) throws IOException;

    @Override
    public void close();

    /** 不可变条目：(rowId, filePos)。 */
    public static class RowPosEntry implements Comparable<RowPosEntry> {
        private final long rowId;
        private final FilePos filePos;

        public RowPosEntry(long rowId, FilePos filePos);
        public long getRowId();
        public FilePos getFilePos();

        @Override
        public int compareTo(RowPosEntry other) {
            return Long.compare(this.rowId, other.rowId);
        }
    }

    /** 生成的 SST 文件元数据。 */
    public static class SstFileMeta {
        private final String fileName;
        private final long fileSize;

        public SstFileMeta(String fileName, long fileSize);
        public String getFileName();
        public long getFileSize();
    }
}
```

**实现要点**：
- 使用 `org.rocksdb.SstFileWriter` + `EnvOptions` + `Options`（参考 `RowPosIndexTest.testIngestExternalFile()` 的写法）
- Key 编码：`RowPosIndex.encodeRowId(rowId)`（8 字节 BigEndian）
- Value 编码：`filePos.encode()`（varint LEB128）
- SST 文件命名：`sst_0.sst`、`sst_1.sst`、...
- 空列表输入时不生成任何 SST 文件，返回空列表

---

### 2. 新增类：`FileDictAllocator`
**文件**：`fluss-server/.../kv/dv/FileDictAllocator.java`（新建）

维护单调递增的 `nextFileId` 计数器，为 Paimon 数据文件路径分配唯一的 file ID。运行在 TieringService 侧。

```java
public class FileDictAllocator {

    private int nextFileId;
    private final Map<String, Integer> sessionCache;   // path -> fileId（本 session 缓存）
    private final Map<Integer, String> newEntries;     // fileId -> path（本轮新分配）

    /**
     * 从指定的 nextFileId 开始创建。
     * 首次运行 nextFileId = 0；恢复时从 lake snapshot property 读取。
     */
    public FileDictAllocator(int nextFileId);

    /**
     * 为给定文件路径分配 file ID。
     * 同一 session 中对相同路径重复调用返回相同 ID（幂等）。
     * 新路径分配新 ID 并记录到 newEntries。
     */
    public int allocate(String filePath);

    /**
     * 返回自构造或上次 resetNewEntries() 以来新分配的 (fileId -> filePath) 映射。
     * 用于报告给 CoordinatorServer。
     */
    public Map<Integer, String> getNewEntries();

    /** 返回当前 nextFileId 值（用于持久化到 snapshot property）。 */
    public int getNextFileId();

    /** 重置 newEntries（新一轮开始时调用）。保留 sessionCache 和 nextFileId。 */
    public void resetNewEntries();
}
```

**持久化模型**：
- `nextFileId` 通过 Paimon snapshot property 持久化（key: `fluss.dv.nextFileId`）
- TieringService 启动时：从最近的 lake snapshot 读取 property → `new FileDictAllocator(restoredNextFileId)`
- 每次报告后：`nextFileId` 值包含在 snapshot metadata 中用于恢复

> 注：持久化的集成在 PR 9 中实现，本 PR 仅实现 FileDictAllocator 核心逻辑。

---

### 3. 新增类：`RowPosSstIndex`
**文件**：`fluss-server/.../kv/dv/RowPosSstIndex.java`（新建）

每轮 snapshot 的统一索引文件（`index.json`），记录该 snapshot 包含哪些 bucket 以及每个 bucket 的 SST 文件列表。合并了原来的 `BucketSstIndex`（跨 bucket 索引）和 `SstManifest`（per-bucket 文件清单）的职责。

```java
public class RowPosSstIndex {

    private final Map<Integer, List<SstFileEntry>> bucketFiles;  // bucketId -> SST 文件列表

    public RowPosSstIndex(Map<Integer, List<SstFileEntry>> bucketFiles);

    /** 返回该 snapshot 包含的所有 bucket ID。 */
    public Set<Integer> getBucketIds();

    /** 返回指定 bucket 的 SST 文件列表。bucket 不存在时返回空列表。 */
    public List<SstFileEntry> getFiles(int bucketId);

    /** 序列化为 JSON 字节。 */
    public byte[] toJsonBytes() throws IOException;

    /** 从 JSON 字节反序列化。 */
    public static RowPosSstIndex fromJsonBytes(byte[] bytes) throws IOException;

    /** SST 文件条目。 */
    public static class SstFileEntry {
        private final String fileName;
        private final long fileSize;

        public SstFileEntry(String fileName, long fileSize);
        public String getFileName();
        public long getFileSize();
    }
}
```

JSON 格式（`index.json`）：
```json
{
  "version": 1,
  "buckets": {
    "0": {
      "files": [
        {"name": "sst_0.sst", "size": 12345},
        {"name": "sst_1.sst", "size": 67890}
      ]
    },
    "1": {
      "files": [
        {"name": "sst_0.sst", "size": 11111}
      ]
    }
  }
}
```

---

### 4. 新增类：`RowPosSstUploader`
**文件**：`fluss-server/.../kv/dv/RowPosSstUploader.java`（新建）

将一轮 snapshot 的 SST 文件和 index 上传到远程存储。由 TieringService 在 SST 生成后调用。

```java
public class RowPosSstUploader {

    private static final String ROW_POS_DIR = "rowPos";
    private static final String INDEX_FILE = "index.json";
    private static final int UPLOAD_BUFFER_SIZE = 16 * 1024;  // 16KB

    private final FsPath remoteLakeTableSnapshotDir;

    public RowPosSstUploader(FsPath remoteLakeTableSnapshotDir);

    /**
     * 上传指定 snapshot 的所有 bucket SST 文件 + index.json：
     * 1. 对每个 bucket：上传 SST 文件到 rowPos/{snapshotId}/{bucketId}/
     * 2. 最后写入 index.json（原子可见性保证）
     *
     * @param snapshotId lake snapshot ID，用作远程目录名
     * @param bucketSstMap bucketId -> (localSstDir, sstMetas) 映射
     */
    public void upload(long snapshotId,
            Map<Integer, BucketSstData> bucketSstMap) throws IOException;

    /** 一个 bucket 的本地 SST 数据。 */
    public static class BucketSstData {
        private final String localSstDir;
        private final List<RowPosSstFileWriter.SstFileMeta> sstMetas;

        public BucketSstData(String localSstDir, List<RowPosSstFileWriter.SstFileMeta> sstMetas);
        public String getLocalSstDir();
        public List<RowPosSstFileWriter.SstFileMeta> getSstMetas();
    }
}
```

上传路径：
- SST 文件：`{remoteLakeTableSnapshotDir}/rowPos/{snapshotId}/{bucketId}/sst_0.sst`
- 索引文件：`{remoteLakeTableSnapshotDir}/rowPos/{snapshotId}/index.json`（最后写入）

---

### 5. 新增类：`RowPosSstDownloader`
**文件**：`fluss-server/.../kv/dv/RowPosSstDownloader.java`（新建）

从远程存储下载 SST 文件到本地临时目录。由 TabletServer 在 Prepare 阶段调用。

```java
public class RowPosSstDownloader {

    private static final String ROW_POS_DIR = "rowPos";
    private static final String INDEX_FILE = "index.json";
    private static final int DOWNLOAD_BUFFER_SIZE = 16 * 1024;  // 16KB

    private final FsPath remoteLakeTableSnapshotDir;

    public RowPosSstDownloader(FsPath remoteLakeTableSnapshotDir);

    /**
     * 下载指定 snapshot 中指定 bucket 的 SST 文件到本地目录：
     * 1. 读取 index.json，获取该 bucket 的文件列表
     * 2. 下载 index 中列出的所有 SST 文件
     *
     * @param snapshotId lake snapshot ID
     * @return 本地 SST 文件路径列表，bucket 不在 index 中时返回空列表
     */
    public List<String> downloadBucketSst(long snapshotId, int bucketId, String localDir)
            throws IOException;

    /**
     * 读取指定 snapshot 的 index.json。
     */
    public RowPosSstIndex readIndex(long snapshotId) throws IOException;
}
```

---

## 涉及文件列表

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-server/.../kv/dv/RowPosSstFileWriter.java` | 新建 | RocksDB SST 文件生成，封装排序条目写入 |
| `fluss-server/.../kv/dv/FileDictAllocator.java` | 新建 | 文件路径 → file ID 的单调递增分配器 |
| `fluss-server/.../kv/dv/RowPosSstIndex.java` | 新建 | 统一索引 JSON 序列化/反序列化（合并 bucket 索引 + 文件清单） |
| `fluss-server/.../kv/dv/RowPosSstUploader.java` | 新建 | SST + index.json 上传到远程存储 |
| `fluss-server/.../kv/dv/RowPosSstDownloader.java` | 新建 | 从远程存储下载 SST 文件到本地 |

## 复用的现有工具

| 工具 | 用途 |
|------|------|
| `RowPosIndex.encodeRowId(long)` / `decodeRowId(byte[])` | SST 文件的 key 编码 |
| `FilePos.encode()` / `FilePos.decode(byte[])` | SST 文件的 value 编码 |
| `RowPosIndex.ingestExternalFile(List<String>)` | SST 的消费端（PR 6 使用，本 PR 测试中验证） |
| `FlussPaths.remoteLakeTableSnapshotDir(...)` | 远程基础路径计算 |
| `FileSystem.get(URI)` / `FsPath` | 远程文件系统读写操作 |
| `org.rocksdb.SstFileWriter`、`EnvOptions`、`Options` | RocksDB SST 写入 API |
| `org.apache.fluss.shaded.jackson2.*` | JSON 序列化（index.json） |
| `IOUtils.copyBytes(...)` | 流拷贝工具 |

---

## 测试

### RowPosSstFileWriterTest
- **testWriteEmptyEntries**：空输入 → 不生成 SST 文件
- **testWriteSingleEntry**：一条条目 → 一个 SST 文件，通过 `RowPosIndex.ingestExternalFile()` + `get()` 验证
- **testWriteMultipleEntries**：N 条排序条目 → SST 文件，Ingest 后验证所有条目
- **testWriteEntriesMustBeSorted**：未排序输入 → RocksDB 报错
- **testSplitAcrossMultipleSstFiles**：条目数超过 MAX_ENTRIES_PER_SST → 多个 SST 文件，全部可 Ingest

### FileDictAllocatorTest
- **testAllocateNewPaths**：分配 3 个路径 → ID 0, 1, 2
- **testAllocateIdempotent**：同一路径分配两次 → 返回相同 ID
- **testGetNewEntries**：分配后 getNewEntries 返回全部新 (fileId, path) 对
- **testResetNewEntries**：reset 后 getNewEntries 为空，但后续分配从正确的 nextFileId 继续
- **testRestoreFromNextFileId**：以 nextFileId=5 创建 → 首次分配得到 ID 5
- **testGetNextFileId**：验证 nextFileId 正确递增

### RowPosSstIndexTest
- **testSerializeDeserialize**：JSON 往返序列化一致性
- **testGetFiles**：查找存在和不存在的 bucket
- **testGetBucketIds**：返回所有 bucket ID
- **testEmptyIndex**：空 bucket 映射
- **testMultipleBucketsMultipleFiles**：多 bucket、每 bucket 多文件

### RowPosSstUploadDownloadTest
- **testUploadAndDownloadRoundTrip**：生成 SST → 上传一轮 → 下载 → Ingest → 验证数据一致
- **testMultipleBuckets**：一轮包含多个 bucket 的 SST → 分别下载 → 验证
- **testBucketWithNoSst**：bucket 不在 index 中 → 下载返回空列表
- **testIndexReadAfterUpload**：上传后 readIndex → 验证 bucket + 文件映射正确
- **testIndexWrittenLast**：验证 index.json 是最后写入的文件（原子可见性）

使用 `@TempDir` 提供本地目录，用本地文件系统（`file://`）模拟远程存储。

---

## 前置依赖

- PR 1（DvRocksDB + 核心数据结构）—— RowPosIndex、FilePos、FileDict 编解码

---

## 验证

1. 编译：`mvn compile -pl fluss-server -am -DskipTests`
2. 格式化：`mvn spotless:apply -pl fluss-server`
3. 运行测试：`mvn test -pl fluss-server -Dtest="RowPosSstFileWriterTest,FileDictAllocatorTest,RowPosSstIndexTest,RowPosSstUploadDownloadTest"`
