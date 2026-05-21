# Deletion Vector — RocksDB 底层优化方案

本文档从 RocksDB 底层机制���发，针对 DV 方案的性能热点提出具体优化。与 [dv-performance-solutions.md](dv-performance-solutions.md) 的应用层优化互补。

---

## 1. Merge Operator 优化 LakeDv 写路径

### 当前问题

§6.2 对 LakeDv 的更新是 read-modify-write：

```
bitmap = db.get(lakeDvCF, fileId)       // point get（读）
bitmap.set(rowPosition)                  // 修改
db.put(lakeDvCF, fileId, bitmap)         // 写回
```

每次 delete/update 都要先读 bitmap 再写回。高频 delete 时 point get 成为热点。

### 优化：自定义 MergeOperator

```java
// 定义 MergeOperator
class BitmapMergeOperator extends MergeOperator {
    @Override
    public boolean fullMerge(byte[] key, byte[] existingValue,
                             List<byte[]> operands, byte[] result) {
        RoaringBitmap bitmap = (existingValue != null)
            ? deserialize(existingValue)
            : new RoaringBitmap();
        for (byte[] operand : operands) {
            int pos = decodePosition(operand);
            bitmap.add(pos);
        }
        result = serialize(bitmap);
        return true;
    }
}
```

写路径变为：

```java
// §6.2 不再需要先 get 再 put
db.merge(lakeDvCF, fileId, encodePosition(rowPosition));  // 一次写操作，无读
```

### 效果

- **写路径从 get + put（2 次 I/O）变为 merge（1 次写，0 次读）**
- merge 操作写入 memtable 后立即返回，实际合并延迟到 compaction 或读取时
- §6.2 的写锁临界区减少一次 point get
- 对 §7.3.1 步骤 7 反向扫中的 `LakeDv[hit.fileId] |= {hit.pos}` 同样适用

### 注意

- ���路径（union read clone LakeDv）的开销不变——RocksDB 读取时会触发 merge，返回合��后的 bitmap
- 如果 merge operand 堆积过多（高频写 + 低频 compaction），读延迟会增加。需要调优 `max_successive_merges` 或 `min_merge_operands` 触发自动 compaction

---

## 2. CompactionFilter 自动清理 PendingDeletes

### 当前问题

§7.3.3 需要显式全扫 PendingDeletes 清理 stale sentinel（`R < currentTieredOffset`）。这是写锁内的额外扫描成本。

### 优化：自定义 CompactionFilter

```java
class PendingDeletesCompactionFilter extends CompactionFilter {
    // 由 readable switch 时更新
    volatile long currentTieredOffset;

    @Override
    public Decision filterV2(int level, byte[] key, ValueType valueType,
                              byte[] existingValue) {
        long rowId = decodeRowId(key);
        if (rowId < currentTieredOffset) {
            return Decision.REMOVE;  // stale，在 compaction 时自动删除
        }
        return Decision.KEEP;
    }
}
```

### 效果

- **stale sentinel 清理从显式���描变为 compaction 副产品，零额外 I/O**
- §7.3.3 的临界区内不再需要扫 PendingDeletes 的 stale 部分
- 清理发生在 RocksDB 后台 compaction 线程，不���塞前台

### 注意

- CompactionFilter 的 `currentTieredOffset` 必须是 readable switch 完成后才更新，否则可能过早删除
- 清理是惰性的（等 compaction 触发），不是实时的。如果需要及时回收空间，可在 readable switch 后手动 `CompactRange(pendingDeletesCF, 0, currentTieredOffset)`

---

## 3. Generation Counter 替代 DropColumnFamily

### 当前问题

§7.3.3 readable switch 时 `DropColumnFamily(pendingRowPos) + CreateColumnFamily(pendingRowPos)`：
- 每次 Drop/Create 在 MANIFEST 中新增记录，长时间运行后 MANIFEST 膨胀
- Drop 触发后台文件删除，可能与正在进行的 compaction 冲突
- Create 新 CF 需要分配新的 CF ID，更新元数据

### 优化：key 前缀加 generation，Compaction Filter 延迟淘汰

```
pendingRowPos key 格式：{generation (8 bytes)}{RowId (8 bytes)}
```

```java
class GenerationCompactionFilter extends CompactionFilter {
    volatile long currentGeneration;

    @Override
    public Decision filterV2(int level, byte[] key, ...) {
        long gen = decodeGeneration(key);
        if (gen < currentGeneration) {
            return Decision.REMOVE;  // 旧 generation，后台清理
        }
        return Decision.KEEP;
    }
}
```

readable switch 时：
```java
// 不再 DropColumnFamily + CreateColumnFamily
// 只需递增 generation
currentGeneration++;
// 读操作自动用 currentGeneration 前缀，旧 generation 数据对读不可见
// 旧数据由 CompactionFilter 在后台清理
```

### 效果

- **MANIFEST 零增长**（CF 不再反复创建/删除）
- readable switch 从 Drop + Create（毫秒级元数据操作）降为 generation++ （纳秒级）
- 旧数据延迟清理，不阻塞前台

### 代价

- key 多 8 bytes 前缀（generation）
- 读操作需要带 generation 前缀做 Seek/Get（可封装为透明层）
- 旧 generation 数据在 compaction 前仍占磁盘空间

---

## 4. RocksDB Snapshot 支撑无锁反向扫

### 当前问题

Phase 2 拆锁后（见 dv-performance-solutions.md §1），无锁阶段扫描 PendingDeletes + 查 pendingRowPos 需要一致性视图。如果 §6.2 并发修改，可能看到不一致状态。

### 优化：用 RocksDB Snapshot 保证一致读

```java
// 第一次写锁内（Ingest 完成后）
lock()
  ingest(sst, pendingRowPos);
  writeFileDict();
  Snapshot snapshot = db.getSnapshot();  // 捕获 Ingest 后的一致状态
unlock()

// 无锁阶段：用 snapshot 扫描（§6.2 的并发写对 snapshot 不可见）
ReadOptions readOpts = new ReadOptions().setSnapshot(snapshot);
try {
    RocksIterator iter = db.newIterator(pendingDeletesCF, readOpts);
    for (iter.seekToFirst(); iter.isValid(); iter.next()) {
        byte[] value = db.get(pendingRowPosCF, readOpts, iter.key());
        if (value != null) batch.add(iter.key(), value);
    }
} finally {
    db.releaseSnapshot(snapshot);
}

// 第二次写锁内：apply batch（带验证）
lock()
  applyBatch(batch);
unlock()
```

### 效果

- **无锁阶段的一致性由 RocksDB 保证**，不依赖应用层逻辑
- snapshot 是 RocksDB 的轻量操作（仅记录一个 sequence number，O(1)）
- 扫描看到的是 Ingest 完成瞬间的精确状态

### 注意

- snapshot 持有期间，该 sequence number 之后的 SST 不会被 compaction ���理（防止 snapshot 引用的数据被删）
- 扫描要尽快完成并释放 snapshot，避免 hold 住过多 SST 文件

---

## 5. MultiGet 批量点查

### 当前问题

- §6.2 批量化后，每批 N 条 delete 需要对 RowPosIndex 和 pendingRowPos 各做 N 次 point get
- §7.3.1 反向扫中，每个 PendingDeletes entry 做一次 `pendingRowPos.get(R)`

逐条 Get 无法利用 I/O 合并和 CPU 缓存。

### 优化：RocksDB MultiGet

```java
// §6.2 批量：一次 MultiGet 替代 N 次 Get
List<byte[]> keys = batch.stream().map(d -> encodeRowId(d.oldRowId)).collect(toList());

// 查 RowPosIndex
List<byte[]> posResults = db.multiGetAsList(readOpts, rowPosIndexCF, keys);
// 查 pendingRowPos
List<byte[]> pendingResults = db.multiGetAsList(readOpts, pendingRowPosCF, keys);

// 反向扫同理：攒一批 PendingDeletes keys，批量查 pendingRowPos
```

### 效果

RocksDB MultiGet 的内部优��：
- **同一 SST 文件内的 key 合并为一次 I/O**（减少 read 系统调用）
- **Data Block 和 Index Block 的缓存命中率提升**（批量访问相邻 key 时 block cache 复用）
- **Bloom Filter 批量查询**（减少函数调用开销）
- 实测通常比逐条 Get 快 2-5 倍（取决于 key 分布和 cache 命中率）

---

## 6. Per-CF 差异化配置

### 当前问题

DvRocksDB 的六个 CF 访问模式差异极大，统一配置无法兼顾。

### 优化：按 CF 定制配置

```java
// RowPosIndex：point get 为主
ColumnFamilyOptions rowPosOpts = new ColumnFamilyOptions()
    .setTableFormatConfig(new BlockBasedTableConfig()
        .setFilterPolicy(new BloomFilter(10))     // 10 bits/key bloom filter
        .setWholeKeyFiltering(true)               // 全 key bloom，point get 最优
        .setBlockSize(4 * 1024))                  // 小 block，减少读放大
    .setMemtablePrefixBloomSizeRatio(0.1);        // memtable bloom，加速 memtable 查询

// pendingRowPos：Ingest + point get + 批量清理
ColumnFamilyOptions pendingOpts = new ColumnFamilyOptions()
    .setTableFormatConfig(new BlockBasedTableConfig()
        .setFilterPolicy(new BloomFilter(10)))
    .setLevel0FileNumCompactionTrigger(8)          // Ingest 后不急于 compact
    .setDisableAutoCompactions(false);

// PendingDeletes：point get + write + 全扫
ColumnFamilyOptions pdOpts = new ColumnFamilyOptions()
    .setTableFormatConfig(new BlockBasedTableConfig()
        .setFilterPolicy(new BloomFilter(10))      // point get 时的 bloom
        .setBlockSize(16 * 1024))                  // 大 block，顺序扫描友好
    .setCompactionFilter(pendingDeletesCompactionFilter);  // 自动清理 stale

// LakeDv：Merge Operator + read-modify-write
ColumnFamilyOptions lakeDvOpts = new ColumnFamilyOptions()
    .setMergeOperator(bitmapMergeOperator)
    .setTableFormatConfig(new BlockBasedTableConfig()
        .setBlockSize(8 * 1024));

// LogDv：range-based bitmap，读多写少
ColumnFamilyOptions logDvOpts = new ColumnFamilyOptions()
    .setTableFormatConfig(new BlockBasedTableConfig()
        .setBlockSize(16 * 1024))                  // 大 block，range scan 友好
    .setOptimizeFiltersForHits(true);              // 减少 bloom filter 内存

// FileDict：低频 point get + write
ColumnFamilyOptions fileDictOpts = new ColumnFamilyOptions()
    .setTableFormatConfig(new BlockBasedTableConfig()
        .setFilterPolicy(new BloomFilter(10)));
```

### 效果

每个 CF 针对自己的访问模式优化，避免"顺序扫描用了 point-get 优化的小 block"或"point-get 用了大 block 浪费缓存"的问题。

---

## 7. ReadOptions 调优扫描路径

### 当前问题

PendingDeletes 全扫和 §7.3.3 清理扫描使用默认 ReadOptions，可能污染 block cache、无 readahead。

### 优化

```java
// 扫描专用 ReadOptions
ReadOptions scanOpts = new ReadOptions()
    .setFillCache(false)           // 扫描结果不进 block cache，避免驱逐热数据
    .setReadaheadSize(2 * 1024 * 1024)  // 2MB readahead，顺序读预取
    .setVerifyChecksums(false)     // 扫描路径跳过校验（已由写入保证）
    .setTotalOrderSeek(true);      // 全序扫描（跳过 prefix bloom 限制）

// point get 专用 ReadOptions
ReadOptions getOpts = new ReadOptions()
    .setFillCache(true)            // point get 结果进 cache
    .setVerifyChecksums(true);
```

### 效果

- `fillCache=false`：扫描不���逐 block cache 中的热 key（RowPosIndex 的 point get 数据），保护 §6.2 的 cache 命中率
- `readaheadSize`：操作系统级预读，顺序扫描吞吐提升显著（RocksDB 内部 SST 文件是按 key 排序的，顺序扫描是顺序 I/O）
- 在 SSD 上效果相对有限，在 HDD 上效果显著

---

## 8. BlobDB 优化大 Bitmap 存储

### 当前问题

LakeDv 的 value �� Roaring Bitmap，delete-heavy 场景下单个文件的 bitmap 可能很大（上万 bits → 数 KB 甚至数十 KB）。大 value 导致：
- LSM compaction 写放大（大 value 在每一层都要重写）
- block cache 效率低（一个大 value 占满一个 block）

### 优化：启用 Integrated BlobDB

```java
ColumnFamilyOptions lakeDvOpts = new ColumnFamilyOptions()
    .setEnableBlobFiles(true)
    .setMinBlobSize(1024)              // value > 1KB 分离到 blob 文件
    .setBlobCompressionType(CompressionType.LZ4_COMPRESSION)
    .setEnableBlobGarbageCollection(true)
    .setBlobGarbageCollectionAgeCutoff(0.25);  // blob 文件 75% 过期时触发 GC
```

### 效果

- **LSM tree 只存 key + blob 引用**（固定大小），compaction 写放大大幅降低
- 大 bitmap 存在独立的 blob 文件中，顺序 I/O 读写
- blob GC 独立于 LSM compaction，互不干扰
- 适用于 LakeDv bitmap > 1KB 的场景；小 bitmap 仍 inline 存储

---

## 9. WriteBufferManager 统一内存管理

### 当前问题

六个 CF 各自维护 memtable，总内存占用不可控。某个 CF 的 memtable flush 可能挤压其他 CF 的 block cache。

### 优化

```java
// 全局内存预算
long totalMemory = 512 * 1024 * 1024;  // 512MB
Cache blockCache = new LRUCache(totalMemory * 0.6);  // 60% 给 block cache
WriteBufferManager wbm = new WriteBufferManager(
    totalMemory * 0.4,  // 40% 给所有 CF 的 memtable
    blockCache           // 从同一 cache 分配，避免双重计数
);

DBOptions dbOptions = new DBOptions()
    .setWriteBufferManager(wbm);
```

### 效果

- 所有 CF 的 memtable 共享内��预算，防止单个 CF 膨胀
- block cache 和 memtable 在同一内存池内动态平衡
- 避免 OOM 或不可预期的内存峰值

---

## 10. DeleteRange 优化 §7.3.3 清理

### 当前问题

如果不用 CompactionFilter（方案 2），§7.3.3 的 stale sentinel 清理需要逐条 delete。大量 stale entry 时写放大严重（每条 delete 写一个 tombstone）。

### 优化：用 DeleteRange 替代逐条 delete

```java
// 替代：逐条扫描 + delete
// for each stale entry: db.delete(pendingDeletesCF, key)

// 改为：一次 DeleteRange
byte[] beginKey = encodeRowId(0);
byte[] endKey = encodeRowId(currentTieredOffset);
db.deleteRange(pendingDeletesCF, beginKey, endKey);  // 一个 range tombstone
```

### 效果

- **一个 range tombstone 替代 N 个 point tombstone**，写放大从 O(N) 降为 O(1)
- 后续 compaction 时 range tombstone 一次性清理覆盖范围内的所有 entry
- WAL 只写一条记录而非 N 条

### 注意

- range tombstone 在读路径有额外开销（RocksDB 需要检查 range tombstone 是否覆盖读取的 key）
- 如果频繁 DeleteRange，碎片化的 range tombstone 可能拖慢读取。建议配合 `CompactRange` 定期清理

---

## 方案组合与优先��

### 推荐组合

```
§6.2 写路径：
  LakeDv 更新 → Merge Operator（方案 1）
  批量 point get → MultiGet（方案 5）

§7.3.1 Phase 2：
  无锁扫描 → RocksDB Snapshot（方案 4）
  批量查 pendingRowPos → MultiGet（方案 5）
  扫描 ReadOptions → fillCache=false + readahead（方案 7）

§7.3.3 Readable Switch���
  pendingRowPos 清理 → Generation Counter（方案 3）
  PendingDeletes stale 清理 → CompactionFilter（方案 2）或 DeleteRange（方案 10）

全局：
  Per-CF 差异化配置（方案 6）
  WriteBufferManager 统一内存（方案 9）
  LakeDv 大 bitmap → BlobDB（方案 8）
```

### 优先级

| 优先级 | 方案 | 改动 | 效果 |
|--------|------|------|------|
| **最高** | Merge Operator (LakeDv) | LakeDv CF 配置 + §6.2 写路径 | §6.2 消除一次 point get |
| **最高** | RocksDB Snapshot (Phase 2) | §7.3.1 拆锁实现 | 无锁扫描的正确性保证 |
| **高** | MultiGet | §6.2 批量 + 反向扫 | 2-5x 点查提速 |
| **高** | Generation Counter | pendingRowPos key 格式 + CompactionFilter | 消除 DropCF/CreateCF |
| **高** | Per-CF 配置 | DvRocksDB 初始化 | 每个 CF 最优化 |
| **中** | CompactionFilter (PendingDeletes) | PendingDeletes CF + §7.3.3 | stale 清理零额外 I/O |
| **中** | ReadOptions 调优 | 扫描路径 | 保护 cache + 预读提速 |
| **中** | WriteBufferManager | DB 级配置 | 内存可控 |
| **低** | BlobDB (LakeDv) | LakeDv CF 配置 | delete-heavy 时减少写放大 |
| **低** | DeleteRange | §7.3.3 清理路�� | 清理写放大 O(N)→O(1) |
