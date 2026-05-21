# Deletion Vector — RocksDB 源码级优化方案

本文档探讨在可以修改 RocksDB 源码（fork）的前提下，针对 DV 工作负载定制的深度优化。与 [dv-rocksdb-optimizations.md](dv-rocksdb-optimizations.md) 的 API 层优化互补。

---

## 1. TruncateColumnFamily：零 MANIFEST 开销的 CF 重置

### 痛点

readable switch 需要清空 pendingRowPos。当前做法是 `DropColumnFamily + CreateColumnFamily`，每次在 MANIFEST 中新增 CF 删除 + CF 创建记录，长时间运行后 MANIFEST 膨胀。Generation Counter 方案（API 层）虽然回避了这个问题，但在 key 上增加了 8 字节前缀。

### 改法

在 RocksDB 中新增 `TruncateColumnFamily` 操作：

```cpp
// db_impl.cc
Status DBImpl::TruncateColumnFamily(ColumnFamilyHandle* cf) {
  // 1. 将该 CF 当前所有 SST 文件标记为 obsolete（同 DropCF 的文件处理逻辑）
  // 2. 清空该 CF 的 memtable（flush 后丢弃，或直接丢弃）
  // 3. 重置该 CF 的 sequence number / version 为初始状态
  // 4. 在 MANIFEST 中写入一条 TruncateRecord（而非 Drop + Create 两条）
  // 5. CF ID 不变，元数据不变，仅数据清零

  auto* cfd = static_cast<ColumnFamilyHandleImpl*>(cf)->cfd();

  // Mark all files as obsolete
  VersionEdit edit;
  for (int level = 0; level < cfd->NumberLevels(); level++) {
    for (auto* file : cfd->current()->storage_info()->LevelFiles(level)) {
      edit.DeleteFile(level, file->fd.GetNumber());
    }
  }

  // Flush and discard memtable
  cfd->mem()->Reset();

  // Apply as single MANIFEST entry
  edit.SetColumnFamily(cfd->GetID());
  return versions_->LogAndApply(cfd, &edit, &mutex_);
}
```

### 调用方

```java
// §7.3.3 readable switch
// 替代：db.dropColumnFamily(pendingRowPos); pendingRowPos = db.createColumnFamily(...)
db.truncateColumnFamily(pendingRowPos);  // CF ID 不变，MANIFEST 只加一条记录
```

### 效果

- MANIFEST 增长从每次 2 条记录（Drop + Create）降为 1 条（Truncate）
- 无需重新分配 CF ID，减少内部元数据更新
- 后续代码中对 CF handle 的引用不需要刷新（CF ID 不变）
- 比 Generation Counter 更干净，key 格式不变

---

## 2. IngestExternalFile with AtomicSwap：原子迁移

### 痛点

readable switch 的核心操作：将 pendingRowPos 的 SST Ingest 到 RowPosIndex，然后清空 pendingRowPos。当前需要两步（Ingest + Drop/Truncate），中间有中间状态。

### 改法

扩展 IngestExternalFile，支持 atomic swap 语义：

```cpp
struct IngestExternalFileOptions {
  // ... existing fields ...

  // 新增：原子清空源 CF
  // Ingest files into target CF, atomically truncate source CF
  // 在同一个 MANIFEST edit 中完成两件事
  ColumnFamilyHandle* atomic_truncate_source = nullptr;
};
```

```cpp
// db_impl_compaction_flush.cc
Status DBImpl::IngestExternalFile(ColumnFamilyHandle* target_cf,
                                  const std::vector<std::string>& files,
                                  const IngestExternalFileOptions& opts) {
  // ... existing ingest logic for target_cf ...

  VersionEdit edit;
  // Add ingested files to target CF
  for (auto& file : ingested_files) {
    edit.AddFile(target_level, file);
  }

  if (opts.atomic_truncate_source) {
    // In the SAME VersionEdit, delete all files from source CF
    auto* source_cfd = GetCFD(opts.atomic_truncate_source);
    for (int level = 0; level < source_cfd->NumberLevels(); level++) {
      for (auto* f : source_cfd->current()->storage_info()->LevelFiles(level)) {
        edit.DeleteFile(level, f->fd.GetNumber());
      }
    }
  }

  // Single MANIFEST write: atomically add to target + truncate source
  return versions_->LogAndApply({target_cfd, source_cfd}, {&edit}, &mutex_);
}
```

### 调用方

```java
// §7.3.3 readable switch — 一步完成
IngestExternalFileOptions opts = new IngestExternalFileOptions();
opts.setAtomicTruncateSource(pendingRowPosCF);
db.ingestExternalFile(rowPosIndexCF, pendingSstFiles, opts);
// 原子完成：SST 加入 RowPosIndex + pendingRowPos 清空
```

### 效果

- **一次 MANIFEST write 完成两件事**，无中间状态
- 崩溃恢复时要么都完成、要么都没完成，不存在"Ingest 成功但 Truncate 没做"的半成品状态
- 消除了 §7.3.3 中 Ingest 和 DropCF 之间的竞态窗口

---

## 3. CrossCFGet / MultiCFGet：跨 CF 点查

### 痛点

§6.2 每条 delete 需要分别查 RowPosIndex 和 pendingRowPos：

```java
pos1 = db.get(rowPosIndexCF, rowId);      // 第一次 point get
pos2 = db.get(pendingRowPosCF, rowId);     // 第二次 point get
```

两次独立的 Get 无法共享 bloom filter 查询、block cache 查询的开销。

### 改法

新增 `MultiCFGet`——对同一个 key 在多个 CF 中查找：

```cpp
// db.h
struct MultiCFGetResult {
  Status status;
  std::string value;
  ColumnFamilyHandle* found_in;  // 在哪个 CF 中找到的
};

Status DB::MultiCFGet(const ReadOptions& options,
                      const std::vector<ColumnFamilyHandle*>& cfs,
                      const Slice& key,
                      std::vector<MultiCFGetResult>* results);

// 或者 short-circuit 版本：找到第一个就返回
Status DB::GetFirstFromCFs(const ReadOptions& options,
                           const std::vector<ColumnFamilyHandle*>& cfs,
                           const Slice& key,
                           std::string* value,
                           ColumnFamilyHandle** found_in);
```

内部实现优化：

```cpp
Status DBImpl::GetFirstFromCFs(const ReadOptions& opts,
                               const std::vector<ColumnFamilyHandle*>& cfs,
                               const Slice& key,
                               std::string* value,
                               ColumnFamilyHandle** found_in) {
  // 单次获取 DB mutex snapshot（而非每个 CF 各获取一次）
  auto* sv = GetSuperVersion();

  for (auto* cf : cfs) {
    auto* cfd = GetCFD(cf);
    // 复用同一个 snapshot 做查找
    // Bloom filter check → memtable check → SST check
    Status s = GetImpl(opts, cfd, key, value);
    if (s.ok()) {
      *found_in = cf;
      return s;
    }
  }
  return Status::NotFound();
}
```

### 调用方

```java
// §6.2 —— 一次调用替代两次 Get
MultiCFGetResult result = db.getFirstFromCFs(
    readOpts,
    List.of(rowPosIndexCF, pendingRowPosCF),  // 优先查 RowPosIndex
    rowIdKey
);
if (result.found()) {
    // result.value = position, result.foundIn = 哪个 CF
}
```

### 效果

- 单次 DB mutex / SuperVersion 获取（而非两次）
- short-circuit：RowPosIndex 命中就不查 pendingRowPos（大多数情况）
- bloom filter miss 的优化共享：如果 key 不存在于任何 CF，两次 bloom check 的元数据获取可以合并
- §6.2 热路径延迟降低约 30-50%（减少一次完整的 Get 调用开销）

---

## 4. JoinIterator：迭代 + 点查融合

### 痛点

§7.3.1 反向扫模式：迭代 PendingDeletes 的每个 entry，对每个 key 在 pendingRowPos 中做 point get。当前是两个独立操作，无法利用 key 的局部性。

### 改法

新增 `JoinIterator`——迭代一个 CF，自动对每个 key 在另一个 CF 中做 point lookup：

```cpp
class JoinIterator : public Iterator {
  Iterator* primary_;          // PendingDeletes iterator
  ColumnFamilyHandle* lookup_cf_;  // pendingRowPos CF
  DB* db_;
  ReadOptions lookup_opts_;

  // 预取队列：批量收集 primary 的 keys，一次性 MultiGet lookup
  static const int PREFETCH_BATCH = 64;
  std::deque<std::pair<std::string, std::string>> prefetch_buffer_;

 public:
  void Next() override {
    if (prefetch_buffer_.empty()) {
      // 批量预取下一批
      std::vector<Slice> keys;
      std::vector<std::string> primary_values;

      for (int i = 0; i < PREFETCH_BATCH && primary_->Valid(); i++) {
        keys.push_back(primary_->key());
        primary_values.push_back(primary_->value().ToString());
        primary_->Next();
      }

      // 一次 MultiGet 查 lookup_cf
      std::vector<std::string> lookup_values(keys.size());
      std::vector<Status> statuses = db_->MultiGet(
          lookup_opts_, lookup_cf_, keys, &lookup_values);

      for (int i = 0; i < keys.size(); i++) {
        if (statuses[i].ok()) {
          // 有匹配，放入结果
          prefetch_buffer_.emplace_back(
              keys[i].ToString(), lookup_values[i]);
        }
      }
    }
    // 从 prefetch_buffer 弹出下一个匹配结果
  }

  // key() 和 value() 返回 primary 的 key + lookup 的 value
  Slice key() override { return prefetch_buffer_.front().first; }
  Slice lookupValue() override { return prefetch_buffer_.front().second; }
};
```

### 调用方

```java
// §7.3.1 反向扫 —— 迭代 PendingDeletes，自动批量查 pendingRowPos
JoinIterator joinIter = db.newJoinIterator(
    pendingDeletesCF,    // 迭代源
    pendingRowPosCF,     // 每个 key 的 lookup 目标
    readOpts
);
for (joinIter.seekToFirst(); joinIter.isValid(); joinIter.next()) {
    // joinIter 只返回命中的（PendingDeletes 有且 pendingRowPos 也有）
    byte[] rowId = joinIter.key();
    byte[] position = joinIter.lookupValue();
    batch.add(rowId, position);
}
```

### 效果

- **迭代 + 点查融合为流水线**：迭代下一批 key 的同时，上一批的 MultiGet 结果已在内存中
- **MultiGet 批量查询**：64 个 key 一次 MultiGet，比逐个 Get 快 2-5x
- **只返回命中**：上层代码不需要处理 miss，逻辑更简洁
- PendingDeletes 有 10 万条但只有 100 条命中时，上层只看到 100 次迭代

---

## 5. WriteBatchWithIndex 的 Cross-CF Read 优化

### 痛点

§6.2 批量化后，一个 WriteBatch 内的多条 delete 可能互相影响。例如：
- delete(RowId=100) 从 pendingRowPos 删了 entry
- delete(RowId=200) 的 position 恰好在同一个 file 中

当前 WriteBatchWithIndex 的 `GetFromBatchAndDB` 只支持单 CF。

### 改法

扩展 WriteBatchWithIndex 支持 Cross-CF Read：

```cpp
class WriteBatchWithIndex {
  // 现有
  Status GetFromBatchAndDB(DB* db, const ReadOptions& opts,
                           ColumnFamilyHandle* cf, const Slice& key,
                           std::string* value);

  // 新增：先查 batch 中的写入（所有 CF），再查 DB
  Status GetFromBatchAndDBMultiCF(
      DB* db, const ReadOptions& opts,
      const std::vector<ColumnFamilyHandle*>& cfs,
      const Slice& key,
      std::string* value,
      ColumnFamilyHandle** found_in);
};
```

### 调用方

```java
WriteBatchWithIndex batch = new WriteBatchWithIndex();

for (Delete del : deleteBatch) {
    // 查询时能看到 batch 内先前 delete 的效果
    result = batch.getFromBatchAndDBMultiCF(
        db, readOpts,
        List.of(rowPosIndexCF, pendingRowPosCF),
        del.rowId
    );
    if (result.found()) {
        batch.merge(lakeDvCF, result.fileId, result.position);
        batch.delete(result.foundIn, del.rowId);
        batch.put(pendingDeletesCF, del.rowId, result.position);
    }
}
db.write(writeOpts, batch);  // 一次原子提交
```

### 效果

- 批内可见性：后面的 delete 能看到前面 delete 的结果（例如从 pendingRowPos 删除的 entry）
- 结合 MultiCFGet：batch 内查询也走 short-circuit 逻辑
- 单次 `db.write()` 原子提交所有 CF 的修改

---

## 6. Native Bitmap Value Type

### 痛点

LakeDv 的 value 是 Roaring Bitmap，但 RocksDB 只看到 opaque bytes。每次 Merge 需要完整反序列化 → 修改 → 序列化。Compaction 时如果堆积了 N 个 merge operand，需要反序列化 N 次。

### 改法

在 RocksDB 中内置 bitmap value type，支持原生位操作：

```cpp
// 新增 ColumnFamilyOptions
options.value_type = ValueType::ROARING_BITMAP;

// 原生操作 API
db->SetBit(cf, key, bit_position);           // O(1)，不需要先读
db->ClearBit(cf, key, bit_position);         // O(1)
db->AndNot(cf, key, other_bitmap);           // O(|bitmap|)，compaction 时
db->GetBitmap(cf, key, &bitmap);             // 读取完整 bitmap

// 内部存储：增量 delta 编码
// memtable 中存 delta ops: [SetBit(3), SetBit(7), SetBit(100)]
// flush 时合并为 Roaring Bitmap SST block
// compaction 时多个 delta 合并为紧凑 bitmap
```

### 实现要点

```cpp
// 在 memtable 中存增量操作（不是完整 bitmap）
struct BitmapDelta {
  enum Op { SET, CLEAR, AND_NOT };
  Op op;
  uint32_t position;  // for SET/CLEAR
  std::string bitmap;  // for AND_NOT
};

// flush 到 SST 时，合并所有 delta 为紧凑 Roaring Bitmap
// compaction 时，合并多个 SST 的 bitmap（Roaring Bitmap 的 OR 操作）
```

### 效果

- **SetBit 操作 O(1)**，memtable 只写一个 delta，不反序列化完整 bitmap
- compaction 时批量合并 delta → bitmap，摊销序列化开销
- 读取时 RocksDB 自动合并 memtable delta + SST bitmap，返回最终结果
- 比通用 Merge Operator 更高效（RocksDB 知道值的结构，可做更激进的优化）

---

## 7. Lazy Compaction for Temporary CFs

### 痛点

pendingRowPos 是短命 CF——数据 Ingest 进来，readable switch 后就全部迁移走。但 RocksDB 不知道这一点，仍然会对 pendingRowPos 触发后台 compaction（浪费 CPU 和 I/O）。

### 改法

新增 CF 属性：`temporary = true`，RocksDB 对该 CF 跳过后台 compaction：

```cpp
ColumnFamilyOptions opts;
opts.temporary = true;  // 新增属性

// 内部实现：
// - 跳过 Level0 → Level1 compaction
// - 跳过所有后台 compaction 调度
// - IngestExternalFile 直接放到 Level0，不触发任何 compaction
// - 读取时走正常的 multi-level 查找（Level0 文件数可能多，但 pendingRowPos 的生命周期短，可接受）
```

### 效果

- pendingRowPos 的后台 compaction CPU/IO 降为零
- 不影响正确性（数据在 readable switch 时通过 AtomicSwap Ingest 到 RowPosIndex）
- 减少 compaction 线程对其他 CF（RowPosIndex、PendingDeletes）的资源竞争

---

## 8. Batched Atomic Read-Modify-Write (RMW) Primitive

### 痛点

§6.2 的核心模式是：读两个 CF → 决策 → 写四个 CF。当前必须在外部锁内完成整个 RMW。如果 RocksDB 原生支持批量 RMW，可以不依赖外部锁。

### 改法

```cpp
// 新增 API：原子 RMW 事务（非通用事务，专为 RMW 优化）
class AtomicRMW {
  DB* db_;
  WriteBatch batch_;

 public:
  // 读阶段：获取一致性 snapshot，所有读共享同一 snapshot
  Status Get(ColumnFamilyHandle* cf, const Slice& key, std::string* value);
  Status GetFirstFromCFs(const std::vector<ColumnFamilyHandle*>& cfs,
                         const Slice& key, ...);

  // 写阶段：写入 WriteBatch
  void Put(ColumnFamilyHandle* cf, const Slice& key, const Slice& value);
  void Delete(ColumnFamilyHandle* cf, const Slice& key);
  void Merge(ColumnFamilyHandle* cf, const Slice& key, const Slice& value);

  // 提交：乐观并发——检查读集合有无冲突
  Status Commit();  // 如果读过的 key 被其他线程修改，返回 TryAgain
};
```

```cpp
// 内部实现：
// - Begin 时获取 sequence number S
// - 所有 Read 基于 snapshot S
// - Commit 时检查：对于每个读过的 (cf, key)，当前 sequence number
//   是否仍 == S 时的值。如果是 → 原子提交 WriteBatch。如果不是 → TryAgain
// - 比 TransactionDB 更轻量：不需要 lock manager，不需要 WritePrepared/WriteCommitted
```

### 调用方

```java
// §6.2 无外部锁版本
while (true) {
    AtomicRMW rmw = db.beginRMW();
    var pos = rmw.getFirstFromCFs(List.of(rowPosIndexCF, pendingRowPosCF), rowId);
    if (pos.found()) {
        rmw.merge(lakeDvCF, pos.fileId, pos.position);
        rmw.delete(pos.foundIn, rowId);
        rmw.put(pendingDeletesCF, rowId, pos.position);
    }
    if (rmw.commit().ok()) break;  // 成功
    // TryAgain → 重试（乐观并发，冲突极少——同一 RowId 几乎不会并发修改）
}
```

### 效果

- **消除 DvRWLock**（至少对 §6.2 路径）
- §6.2 delete 之间完全无锁并发
- 冲突率极低（同一 RowId 并发 delete 的概率接近零）
- §7.3.1 / §7.3.3 的写路径仍需某种同步（但可以用更细粒度的机制）

---

## 方案组合与改造路径

### 推荐分阶段实施

**Phase 1（小改动，大收益）**：

| 方案 | RocksDB 改动量 | 效果 |
|------|---------------|------|
| TruncateColumnFamily | ~200 行 | 消除 MANIFEST 膨胀 |
| MultiCFGet / GetFirstFromCFs | ~150 行 | §6.2 点查延迟 -30~50% |
| Temporary CF (skip compaction) | ~50 行（compaction scheduler 加 if 判断） | pendingRowPos 零后台 compaction |

**Phase 2（中等改动，关键优化）**：

| 方案 | RocksDB 改动量 | 效果 |
|------|---------------|------|
| IngestExternalFile + AtomicSwap | ~300 行 | readable switch 原子化 |
| JoinIterator | ~400 行 | 反向扫吞吐 2-5x |

**Phase 3（大改动，颠覆性优化）**：

| 方案 | RocksDB 改动量 | 效果 |
|------|---------------|------|
| Native Bitmap Value Type | ~2000 行 | LakeDv 读写全面提速 |
| Batched Atomic RMW | ~1000 行 | 消除 §6.2 外部锁 |

### 改造原则

1. **不改 RocksDB 的数据格式（SST / WAL / MANIFEST 格式兼容）**——保证升级不需要数据迁移
2. **新增 API 而非修改已有 API**——不影响 RocksDB 的其他使用场景
3. **TruncateColumnFamily 可以通过 VersionEdit 实现**——复用现有的 MANIFEST 机制，只是新增一种 edit type
4. **JoinIterator 可以作为上层封装**——不一定要改 RocksDB 源码，但放在 RocksDB 内部可以利用内部的 MultiGet 批量优化
