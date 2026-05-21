# PR 1: DvRocksDB + 核心数据结构

## 目标

构建 DV 的存储层：一个带 5 个 Column Family 的独立 RocksDB 实例，以及各 CF 的操作封装类。

## 设计文档参考

- [fluss-paimon-deletion-vector-design.md](../fluss-paimon-deletion-vector-design.md) §3.2, §3.3, §3.4
- [fluss-deletion-vector-design-v3-en.md](../fluss-deletion-vector-design-v3-en.md) §3.2, §3.3, §3.4（Iceberg 版详细定义）

---

## 包结构

所有新增类放在 `fluss-server/src/main/java/org/apache/fluss/server/kv/dv/` 包下：

```
server/kv/dv/
├── DvRocksDB.java           // RocksDB 实例 + 5 个 CF 管理
├── FilePos.java             // (file_id, row_position) 不可变对象 + varint 编解码
├── RowPosIndex.java         // RowPosIndex CF 操作封装
├── LakeDv.java              // LakeDv CF 操作封装
├── LogDv.java               // LogDv CF 操作封装（分区式 bitmap）
├── FileDict.java            // FileDict CF 操作封装（双向映射）
├── PendingDeletes.java      // PendingDeletes CF 操作封装
└── DvRWLock.java            // 读写锁
```

---

## 改动清单

### 1. FilePos：不可变数据对象 + varint 编解码

**文件**：新建 `fluss-server/.../kv/dv/FilePos.java`

```java
public final class FilePos {
    private final int fileId;
    private final long rowPosition;

    public FilePos(int fileId, long rowPosition) { ... }

    // Getters
    public int fileId() { ... }
    public long rowPosition() { ... }

    // varint encoding/decoding
    public byte[] encode() { ... }
    public static FilePos decode(byte[] bytes) { ... }
    public static FilePos decode(byte[] bytes, int offset) { ... }

    // equals, hashCode, toString
}
```

**编码格式**：varint（Variable-Length Quantity）

每个字节的高 1 位为续位标志（1 = 后续还有字节，0 = 最后一个字节），低 7 位为有效数据。

- `file_id`：varint（unsigned int）
- `row_position`：varint（unsigned long）
- 顺序：`varint(file_id) || varint(row_position)`
- 典型大小：3–5 字节

| file_id 范围 | varint 字节数 | row_position 范围 | varint 字节数 | 总计 |
|---|---|---|---|---|
| < 128 | 1 | < 16384 | 2 | 3B |
| < 16384 | 2 | < 2M | 3 | 5B |
| < 2M | 3 | < 256M | 4 | 7B |

**varint 编解码**：

```java
// encode varint unsigned int
static void writevarint(int value, byte[] buf, int[] pos) {
    while ((value & 0xFFFFFF80) != 0) {
        buf[pos[0]++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
    }
    buf[pos[0]++] = (byte) (value & 0x7F);
}

// encode varint unsigned long
static void writevarint(long value, byte[] buf, int[] pos) {
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
        buf[pos[0]++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
    }
    buf[pos[0]++] = (byte) (value & 0x7F);
}

// decode varint unsigned int
static int readvarintInt(byte[] buf, int[] pos) {
    int result = 0;
    int shift = 0;
    byte b;
    do {
        b = buf[pos[0]++];
        result |= (b & 0x7F) << shift;
        shift += 7;
    } while ((b & 0x80) != 0);
    return result;
}

// decode varint unsigned long
static long readvarintLong(byte[] buf, int[] pos) {
    long result = 0;
    int shift = 0;
    byte b;
    do {
        b = buf[pos[0]++];
        result |= (long) (b & 0x7F) << shift;
        shift += 7;
    } while ((b & 0x80) != 0);
    return result;
}
```

---

### 2. DvRocksDB：独立 RocksDB 实例 + 5 个 CF

**文件**：新建 `fluss-server/.../kv/dv/DvRocksDB.java`

```java
public class DvRocksDB implements Closeable {

    // Column Family names
    static final String CF_ROW_POS_INDEX = "RowPosIndex";
    static final String CF_LOG_DV = "LogDv";
    static final String CF_LAKE_DV = "LakeDv";
    static final String CF_FILE_DICT = "FileDict";
    static final String CF_PENDING_DELETES = "PendingDeletes";

    private final RocksDB db;
    private final ColumnFamilyHandle cfRowPosIndex;
    private final ColumnFamilyHandle cfLogDv;
    private final ColumnFamilyHandle cfLakeDv;
    private final ColumnFamilyHandle cfFileDict;
    private final ColumnFamilyHandle cfPendingDeletes;

    // Sub-components (facade accessors)
    private final RowPosIndex rowPosIndex;
    private final LogDv logDv;
    private final LakeDv lakeDv;
    private final FileDict fileDict;
    private final PendingDeletes pendingDeletes;

    // ...
}
```

**创建流程**：

```java
public static DvRocksDB open(String dbPath, DBOptions dbOptions,
                              ColumnFamilyOptions cfOptions) {
    List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
        new ColumnFamilyDescriptor(CF_ROW_POS_INDEX.getBytes(), cfOptions),
        new ColumnFamilyDescriptor(CF_LOG_DV.getBytes(), cfOptions),
        new ColumnFamilyDescriptor(CF_LAKE_DV.getBytes(), cfOptions),
        new ColumnFamilyDescriptor(CF_FILE_DICT.getBytes(), cfOptions),
        new ColumnFamilyDescriptor(CF_PENDING_DELETES.getBytes(), cfOptions)
    );
    List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    RocksDB db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);
    // cfHandles[0] = default, [1] = RowPosIndex, ...
    return new DvRocksDB(db, cfHandles);
}
```

**为什么独立于 KvTablet 的 RocksDB**：

- DV 的 checkpoint/recovery 生命周期独立（绑定到 lake snapshot，而非 KV 数据）
- DV 支持 IngestExternalFile（KvTablet 的 RocksDB 不使用此特性）
- 可独立调优 compaction 策略和 block cache

**Checkpoint/Restore**：

```java
// Checkpoint
public void checkpoint(String checkpointPath) {
    Checkpoint checkpoint = Checkpoint.create(db);
    checkpoint.createCheckpoint(checkpointPath);
}

// Restore: 直接用 open() 打开 checkpoint 目录的副本
public static DvRocksDB restore(String checkpointPath, DBOptions dbOptions,
                                 ColumnFamilyOptions cfOptions) {
    return open(checkpointPath, dbOptions, cfOptions);
}
```

**IngestExternalFile**（用于 RowPosIndex SST Ingest）：

```java
public void ingestSstToRowPosIndex(List<String> sstPaths) {
    IngestExternalFileOptions options = new IngestExternalFileOptions();
    options.setMoveFiles(true);
    db.ingestExternalFile(cfRowPosIndex, sstPaths, options);
}
```

**Close**：

```java
@Override
public void close() {
    // 按 RocksDB 规范：先关 CF handles，再关 db
    cfPendingDeletes.close();
    cfFileDict.close();
    cfLakeDv.close();
    cfLogDv.close();
    cfRowPosIndex.close();
    db.close();
}
```

**RocksDB 选项**：

复用现有 `RocksDBResourceContainer` 的配置模式。DvRocksDB 使用独立的 DBOptions 和 ColumnFamilyOptions，可在后续 PR 中根据 DV 场景优化（如 RowPosIndex 适合 point-lookup 优化的 bloom filter、LakeDv 适合小 value 等）。本 PR 先使用默认配置。

---

### 3. RowPosIndex CF 操作封装

**文件**：新建 `fluss-server/.../kv/dv/RowPosIndex.java`

**Key 格式**：RowId，8 字节 BigEndian long

```java
static byte[] encodeRowId(long rowId) {
    byte[] key = new byte[8];
    // BigEndian encoding
    key[0] = (byte) (rowId >>> 56);
    key[1] = (byte) (rowId >>> 48);
    // ...
    return key;
}
```

**Value 格式**：FilePos varint 编码

**接口**：

```java
public class RowPosIndex {
    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;

    /** Point-get: rowId → FilePos，不存在返回 null。 */
    public FilePos get(long rowId) { ... }

    /** 写入 rowId → filePos。 */
    public void put(long rowId, FilePos filePos) { ... }

    /** 删除。 */
    public void delete(long rowId) { ... }

    /** WriteBatch 方式批量写入。 */
    public void put(WriteBatch batch, long rowId, FilePos filePos) { ... }
    public void delete(WriteBatch batch, long rowId) { ... }

    /** Ingest 外部 SST 文件到 RowPosIndex CF。 */
    public void ingestExternalFile(List<String> sstPaths) { ... }
}
```

---

### 4. LakeDv CF 操作封装

**文件**：新建 `fluss-server/.../kv/dv/LakeDv.java`

**Key 格式**：file_id，4 字节 BigEndian int

**Value 格式**：序列化的 `org.roaringbitmap.longlong.Roaring64Bitmap`

与 Iceberg DV 方案保持一致，使用 64 位 Roaring64Bitmap。row_position 通常远小于 2^32，此时 Roaring64Bitmap 退化为单个 32 位容器，开销极低。

**接口**：

```java
public class LakeDv {
    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;

    /** 获取指定 file 的 deleted position bitmap，不存在返回 null。 */
    public Roaring64Bitmap get(int fileId) { ... }

    /** 设置指定 file 的 deleted position bitmap（全量覆盖）。 */
    public void put(int fileId, Roaring64Bitmap bitmap) { ... }

    /** 为指定 file 追加一个 deleted position（read-modify-write）。 */
    public void markDeleted(int fileId, long rowPosition) { ... }

    /** 删除指定 file 的整个 LakeDv 条目。 */
    public void delete(int fileId) { ... }

    /** WriteBatch 方式。 */
    public void delete(WriteBatch batch, int fileId) { ... }

    /** 获取所有 LakeDv 条目（用于 union read）。 */
    public Map<Integer, Roaring64Bitmap> getAll() { ... }
}
```

**序列化**：复用现有 `RoaringBitmapUtils.serializeRoaringBitmap64()` / `deserializeRoaringBitmap64()`。

---

### 5. LogDv CF 操作封装

**文件**：新建 `fluss-server/.../kv/dv/LogDv.java`

**分区式 bitmap**：以固定区间（如 1024）划分 offset 范围，每个区间对应一个 RocksDB entry。

**Key 格式**：区间起始 offset，8 字节 BigEndian long

```
rangeStart = (offset / RANGE_SIZE) * RANGE_SIZE
key = BigEndian(rangeStart)
```

**Value 格式**：序列化的 `RoaringBitmap`（32 位即可，因为每个区间内的 offset 偏移量 < RANGE_SIZE）

**接口**：

```java
public class LogDv {
    static final int RANGE_SIZE = 1024;

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;

    /** 标记某个 offset 为已删除。 */
    public void markDeleted(long offset) {
        long rangeStart = (offset / RANGE_SIZE) * RANGE_SIZE;
        int offsetInRange = (int) (offset - rangeStart);
        // read-modify-write: get existing bitmap, set bit, put back
    }

    /** 检查某个 offset 是否已被标记删除。 */
    public boolean isDeleted(long offset) { ... }

    /** 快照：获取指定 offset 范围内的所有已删除 offset。 */
    public RoaringBitmap snapshot(long fromOffset, long toOffset) { ... }

    /** 清理过期的 LogDv 条目（rangeEnd < cleanupOffset 的区间全部删除）。 */
    public void cleanup(long cleanupOffset) { ... }
}
```

---

### 6. FileDict CF 操作封装

**文件**：新建 `fluss-server/.../kv/dv/FileDict.java`

**双向映射**：在同一个 CF 中存储两类 entry，通过 key 前缀区分。

**Key 格式**：
- 正向（path → id）：`prefix=0x00` + `file_path_bytes`
- 反向（id → path）：`prefix=0x01` + `BigEndian(file_id)` (4 bytes)

**Value 格式**：
- 正向：`BigEndian(file_id)` (4 bytes)
- 反向：`file_path_bytes`

**接口**：

```java
public class FileDict {
    private static final byte PREFIX_PATH_TO_ID = 0x00;
    private static final byte PREFIX_ID_TO_PATH = 0x01;

    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;

    /** 通过 file_path 查询 file_id，不存在返回 -1。 */
    public int getFileId(String filePath) { ... }

    /** 通过 file_id 查询 file_path，不存在返回 null。 */
    public String getFilePath(int fileId) { ... }

    /** 写入双向映射。 */
    public void put(int fileId, String filePath) { ... }

    /** WriteBatch 方式批量写入。 */
    public void put(WriteBatch batch, int fileId, String filePath) { ... }

    /** 批量写入多个 fileDict 条目。 */
    public void putAll(WriteBatch batch, Map<Integer, String> entries) { ... }
}
```

---

### 7. PendingDeletes CF 操作封装

**文件**：新建 `fluss-server/.../kv/dv/PendingDeletes.java`

**Key 格式**：RowId，8 字节 BigEndian long（与 RowPosIndex 相同）

**Value 格式**：两种：
- **位置已知**：FilePos varint 编码（与 RowPosIndex value 相同）
- **位置未知（pending）**：空字节数组 `new byte[0]`

> 使用空字节数组表示 `pending`，因为 FilePos varint 编码至少 2 字节，不会与空字节数组混淆。

**接口**：

```java
public class PendingDeletes {
    private final RocksDB db;
    private final ColumnFamilyHandle cfHandle;

    /** 写入已知位置的 pending delete。 */
    public void put(long rowId, FilePos filePos) { ... }

    /** 写入 pending 标记（位置未知）。 */
    public void putPending(long rowId) { ... }

    /** 获取 pending delete entry，不存在返回 null。 */
    public PendingDeleteEntry get(long rowId) { ... }

    /** 删除。 */
    public void delete(long rowId) { ... }

    /** WriteBatch 方式。 */
    public void put(WriteBatch batch, long rowId, FilePos filePos) { ... }
    public void putPending(WriteBatch batch, long rowId) { ... }
    public void delete(WriteBatch batch, long rowId) { ... }

    /** 遍历所有 PendingDeletes 条目。 */
    public CloseableIterator<PendingDeleteEntry> iterator() { ... }

    /** 不可变对象，表示一个 pending delete 条目。 */
    public static class PendingDeleteEntry {
        private final long rowId;
        private final @Nullable FilePos filePos; // null = pending

        public boolean isPending() { return filePos == null; }
    }
}
```

---

### 8. DvRWLock

**文件**：新建 `fluss-server/.../kv/dv/DvRWLock.java`

```java
public class DvRWLock {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void readLock() { lock.readLock().lock(); }
    public void readUnlock() { lock.readLock().unlock(); }
    public void writeLock() { lock.writeLock().lock(); }
    public void writeUnlock() { lock.writeLock().unlock(); }
}
```

封装 `java.util.concurrent.locks.ReentrantReadWriteLock`。写锁持有者：

- §4.2 changelog sync 写路径（DvManager 批量处理 -U/-D）
- §5.3 Prepare Phase 2（写 FileDict + 存储 SST 路径）
- §5.4 Readable Switch（Ingest SST + batch resolve + cleanup）

读锁持有者：

- §6 Union Read（读取 LakeDv + LogDv 快照）

---

## 涉及文件列表

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-server/.../kv/dv/FilePos.java` | 新建 | FilePos 不可变对象 + unsigned varint (LEB128) 编解码 |
| `fluss-server/.../kv/dv/DvRocksDB.java` | 新建 | RocksDB 实例管理，5 个 CF，checkpoint/restore，ingest |
| `fluss-server/.../kv/dv/RowPosIndex.java` | 新建 | RowPosIndex CF CRUD + ingest |
| `fluss-server/.../kv/dv/LakeDv.java` | 新建 | LakeDv CF CRUD + markDeleted + getAll |
| `fluss-server/.../kv/dv/LogDv.java` | 新建 | LogDv CF 分区式 bitmap + snapshot + cleanup |
| `fluss-server/.../kv/dv/FileDict.java` | 新建 | FileDict CF 双向映射（前缀区分） |
| `fluss-server/.../kv/dv/PendingDeletes.java` | 新建 | PendingDeletes CF CRUD + iterator |
| `fluss-server/.../kv/dv/DvRWLock.java` | 新建 | ReentrantReadWriteLock 封装 |

---

## 关键设计决策

### Key 编码统一用 BigEndian

RowId（8B long）和 file_id（4B int）的 key 均使用 BigEndian 编码。BigEndian 保证了 RocksDB 的字典序与数值序一致，使 range scan 和 prefix scan 行为正确。

### LakeDv 使用 64 位 Roaring64Bitmap

与 Iceberg DV 方案保持一致，使用 `Roaring64Bitmap`。虽然单文件 row_position 通常不超过 int 范围，但使用 64 位可以避免潜在的溢出风险，且在小值场景下 Roaring64Bitmap 退化为单个 32 位容器，开销极低。

### PendingDeletes 用空字节数组表示 pending

`pending` 标记使用 `new byte[0]`（空字节数组），FilePos varint 编码至少 2 字节，两者不会混淆。相比字符串 `"pending"` 更节省空间。

### FileDict 双向映射用 key 前缀区分

在同一个 CF 内使用 1 字节前缀（`0x00` = path→id，`0x01` = id→path）区分两个方向。使用 `WriteBatch` 原子写入两个方向的条目，保证一致性。

### DvRocksDB 与 KvTablet 的 RocksDB 完全独立

不共享 DBOptions、ColumnFamilyOptions、RateLimiter。DV 的 I/O pattern（大量 point-get + 定期 bulk ingest）与 KV 层（大量 put + scan）不同，独立配置更灵活。

---

## 测试

**文件**：`fluss-server/src/test/java/org/apache/fluss/server/kv/dv/` 下新建测试类

### FilePos 测试

- varint 编解码往返一致性（小值、边界值、大值）
- fileId = 0, rowPosition = 0 的边界情况
- fileId 最大值（Integer.MAX_VALUE）、rowPosition 最大值

### DvRocksDB 测试

- open + close 正常流程
- 5 个 CF 均可读写
- checkpoint 后 restore，数据一致
- ingestSstToRowPosIndex 正常工作

### RowPosIndex 测试

- put / get / delete 基本 CRUD
- get 不存在的 key → null
- WriteBatch 批量写入

### LakeDv 测试

- put / get / delete 基本 CRUD
- markDeleted 累加（多次 markDeleted 同一 fileId）
- getAll 返回所有条目
- delete 后 get → null

### LogDv 测试

- markDeleted + isDeleted 基本操作
- 跨区间（不同 RANGE_SIZE 区间）的标记和查询
- snapshot 指定 offset 范围
- cleanup 清理过期区间

### FileDict 测试

- put 后双向查询（getFileId + getFilePath）
- 不存在的查询 → -1 / null
- WriteBatch 批量写入一致性

### PendingDeletes 测试

- put known filePos / putPending / get / delete
- get 返回 PendingDeleteEntry，正确区分 pending vs known
- iterator 遍历所有条目
- delete 后 get → null

### DvRWLock 测试

- 多线程读锁并发
- 写锁互斥
- 写锁阻塞读锁

---

## 前置依赖

- PR 0（`table.deletion-vectors.enabled` 配置项）—— 逻辑上依赖，但 PR 1 的代码不直接引用 PR 0 的配置项，可以并行开发
- 现有 `RoaringBitmap` 依赖（`org.roaringbitmap:RoaringBitmap` 已在 pom.xml 中）
- 现有 `RocksDB` JNI 依赖（已在 pom.xml 中）
