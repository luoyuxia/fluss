# Deletion Vector 基础设计

## 术语定义

### RowId

使用 RowId 来唯一标识一条 KV 数据。对于 PUT 的两条 KV 数据，即使 key 相同，RowId 也不同。我们用这条数据对应的 changelog 中 INSERT/UPDATE_AFTER 的 log offset 作为 RowId。

示例：

```
------KV------                ------LOG------
PUT (a, 'hey')    ==>   +I  (log_offset0, a, 'hey')           => rowid = 0
PUT (a, 'hello')  ==>   -U  (log_offset1, a, 'hey')
                         +U  (log_offset2, a, 'hello')         => rowid = 2
```

### FilePos

用来标记一条数据在数据湖中的位置，由**文件路径 + 数据在该文件中的 pos** 组成。

### RowPosIndex

一条 KV 数据最终会被同步到数据湖中，存在于数据湖的某个文件中。RowPosIndex 是 RowId → 所在文件和位置的映射。结构如下：

| RowId  | FilePosList                                                    |
|--------|----------------------------------------------------------------|
| rowId1 | `[{datafile1, pos1}, {datafile2, pos3}, {datafile3, pos10}]`   |
| rowId2 | `[{datafile2, pos2}, {datafile3, pos3}]`                       |

其中 value 是一个 FilePosList，包含多个 FilePos。因为相同的一条数据会在不同快照的不同 datafile 中出现。

存储方案：
- 用一个 RocksDB 来记录，考虑定时 checkpoint 到远程，恢复时从 checkpoint 对应的数据湖快照开始恢复。
- datafile 文件名包含 UUID，是很长的字符串，因此需要进行 **dictionary 编码**，将文件名转成 int 类型。编码映射关系记录在 RocksDB 的另一个列族中。

### LogDv

用来标记 Fluss changelog 中的数据被删除了。在 union read 场景中，client 需要读出 Fluss 中的 delta changelog，即 `[log_startoffset, log_endoffset]` 这段数据。通过 LogDv，client 可以知道这段数据中哪些 log 可以被跳过，避免将整段数据全部读出再在内存中 merge。

数据结构：

| offset_range       | del_bitmap   |
|--------------------|--------------|
| offset0 ~ offset9  | `bin{1}`     |
| offset10 ~ offset20| `bin{2, 5}`  |
| offset21 ~ offset30| `bin{1, 4}`  |

LogDv 的 key 是 changelog offset 的 range，取固定的 offset 间隔，这样 bitmap 的 bit 数就固定了。

**LogDv 处理逻辑示例：**

1. INSERT 一条数据 row1: `(key1, v1)`，rowid = 0，offset = 0
2. 之后 append 了一些数据，changelog 的 endoffset 为 5
3. 用 row2: `(key1, v2)` 更新了这条数据，row2 的 rowid = 6（UPDATE_AFTER 对应的 offset），offset = 6
4. 处理 `-U(key1, v1)` 时，发现被删除数据的 rowid 为 0，对应 offset 也是 0，找到 offset = 0 所在的 range `offset0 ~ offset9`，更新 bitmap 为 `{1}`，表示该 range 中第一条数据被删掉了

**Client 读取时：**

返回给 client 当前的 LogDv `{offset0 ~ offset9: {1}}`（针对当前 endoffset = 6 而言）。Client 从 offset 0 读到 offset 6，读 offset 0 时发现其在 LogDv 中，直接跳过。

**生命周期管理：**

- LogDv 比较小，可考虑只在内存中保存，不持久化。当数据湖 snapshot advance 后，所有小于数据湖最新 snapshot 对应的 `start_logoffset` 的 `offset_range` 条目都可以清理掉。
- 但为避免 OOM 风险，LogDv 保存在 RocksDB 中，和 LakeDv 保存在同一个 RocksDB 实例中，使用不同的列族。

### LakeDv

用来标记数据湖中的数据被删除了。在 union read 场景中，Fluss 需要把湖上的数据读出来，然后和 Fluss 的 delta changelog 做 merge。有了 LakeDv，读湖上数据时可以直接通过 LakeDv 跳过被删除的数据。

数据结构：

| file_name  | del_bitmap    |
|------------|---------------|
| data_file1 | `bin{3}`      |
| data_file2 | `bin{2, 10}`  |
| data_file3 | `bin{1, 4}`   |
| data_file4 | `bin{5}`      |

LakeDv 是 datafile → del_bitmap 的映射，del_bitmap 表示该文件中第几条 record 被标记为删除。Client 读湖上的 datafile 时，通过这个映射找到对应的 del_bitmap，直接跳过对应数据。

考虑到 datafile 数量可能较多，LakeDv 用 RocksDB 保存。

> **TODO**: 对于超大表，datafile list 可能很大，生成的 DV 也会很大，用户请求 DV 时耗时可能很长。需要考虑增量 DV，增量一般只有 3 分钟内的数据产生的 del_bitmap，文件数和 bitmap 大小都可控。

**LakeDv 更新逻辑：**

对于 `-D/-U` changelog，查 RowPosIndex 找到对应的 datafile list，更新 DV。同时记录下被更新的 datafile，记作 `update_datafiles`。返回的 LakeDv 就是 `update_datafiles` 对应的 DV。数据湖快照 advance 后，可以把快照对应的 logoffset 位点之前的 changelog 涉及到的 `update_datafiles` 清理掉。

> **TODO**: 再梳理一下这个流程。

另外，LakeDv 需要持久化到远程，否则恢复时需要 list 最新湖 snapshot 的所有 datafile 并更新 LakeDv，会很慢。

---

## 设计方案

### 实时数据写到 Fluss

1. 一条 KV 数据进入 Fluss，**获取 KvTablet 的写锁**
2. 用这条 KV 数据的 key 反查 KvTablet：
   - **查不到**：生成 `+I(value1, rowid1)`，写入 PrewriteBuffer，写入 changelog
   - **查到了**：
     - PUT → 生成 `-U(value1, rowid1)`, `+U(value2, rowid2)`
     - DELETE → 生成 `-D(value1, rowid1)`
     - 写入 PrewriteBuffer，写入 changelog
3. **释放 KvTablet 的写锁**，等待 changelog 同步成功

### Changelog 同步成功

1. **获取 KvTablet 的写锁**
2. Flush PrewriteBuffer 数据到 RocksDB
3. **获取 LakeDv 的写锁**
4. 遍历 PrewriteBuffer flush 下去的每一行 entry，如果是 `-U` / `-D` 的 entry：
   - a. 用对应 rowId（即 rowId1），从 RowPosIndex 查到这行数据对应的 `datafile_pos list`，对于每一个 `datafile_pos`：
     - i. 用 datafile 去反查 LakeDv，如果查到，得到 previous 的 posDv，将当前 pos merge 到 previous posDv 中，更新 LakeDv
     - ii. 如果查不到，说明这个 datafile 已经从 LakeDv 中清理掉了，do nothing
   - b. 在 RowPosIndex 中删除这个 rowId1
   - c. 用 rowId1 去更新 LogDv，表示 rowId1 的数据被删除了。Client 读 changelog 时，如果 offset 在 LogDv 中，可以跳过
5. **释放 LakeDv 的写锁**
6. 更新 `log_hw`

> **注意**：之前的逻辑是先更新 `log_hw` 再 flush。这会导致 DV 和 `log_hw` 不一致——返回的 `log_hw` 是更大的值，但 DV 还没更新到对应的值，会导致重复读出两条相同数据。

7. **释放 KvTablet 的写锁**

### 处理数据湖的 snapshot

LakeTieringService 生成新的数据湖快照后，通知 CoordinatorServer，CoordinatorServer 再通知 TabletServer。

已有 s2，新来 s3，则：

```
newFiles = snapshot_files(s3) - snapshot_files(s2)
oldFiles = snapshot_files(s2) - snapshot_files(s3)
```

**Step 1**：对于 s3 的 newFiles 中的每一个 file：

1. **获取 LakeDv 的写锁**
2. 为该文件初始化一个 `delete_bits`，遍历文件的每个 RowId，反查 RowPosIndex：
   - **没查到**：认为被删除了，将该数据 append 到 `delete_bits`
   - **查到了**：merge 当前的 filePos 到对应 rowId 的 filePosList 中（需要检查 filePosList 中的每个 file 是否过期——如果 LakeDv 中没有这个文件，该文件就过期了，可以从 filePosList 中删掉）。例如：当前数据对应 `file5:pos30`，RowPosIndex 中之前的映射是 `row2 → [file1:20, file0:10]`，如果 `file0` 在 LakeDv 中查不到，则 merge 后为 `row2 → [file1:20, file5:30]`
3. 将 `file:delete_bits` 更新到 LakeDv 中
4. **释放 LakeDv 的写锁**

**Step 2**：通知 CoordinatorServer s3 的 DV 已完成。CoordinatorServer 收齐所有通知后，将 s3 设置为 DV 可读（更新 LakeTableZNode），client 即可在读 s3 时使用 DV。如果超出最多保存的 snapshot 数量，CoordinatorServer 还会通知清理。

**Step 3**：假设 s2 要被清理，对于 oldFiles 的每一个 file：
- 从 LakeDv 中删除这个 oldFile

> **关于 Step 1 加锁的原因**：
>
> 如果不加锁，changelog 同步成功后 flush PrewriteBuffer 中有一条 `-D(rowid1)`，此时 RowPosIndex 还没删除这条数据。Step 1 从 RowPosIndex 中查到了这条数据，认为它是存活的，不会在 LakeDv 中标记为删除。但实际上对于这个快照的 LakeDv 而言，rowid1 应该被标记为删除。
>
> 加锁后：要么 changelog 同步成功流程先执行，LakeDv 会包含 rowid1；要么处理数据湖 snapshot 流程先执行，此时 LakeDv 不包含 rowid1，但 RowPosIndex 会记录下 rowid1 对应的 datafile，后续 changelog 同步成功时会在 LakeDv 中将其标记为删除。

### RowPosIndex & LogDv & LakeDv 的恢复流程

RowPosIndex、LogDv、LakeDv 都作为不同的列族，保存在另一个 RocksDB 中，记为 **DvRocksDB**。与 KvTablet 的 RocksDB 分开，这样与 KvTablet 的 checkpoint 流程解耦。

DvRocksDB 定期做 checkpoint，将 checkpoint 出的 SST 文件上传到远程。做 checkpoint 时记录：
- `restoreSnapshot`：当前数据湖 snapshot advance 到哪里
- `snapshotStartLogOffset`：该数据湖 snapshot 的 log start offset

**恢复步骤：**

1. 从远程拉取 SST 到本地，加载 DvRocksDB，从 `snapshotStartLogOffset` 开始读 changelog
2. 如果是 `-U/-D` 的 changelog，根据对应删除的 rowId：
   - 在 RowPosIndex 中找到 rowId 对应的 FilePosList
   - 遍历 FilePosList 的每个 FilePos，在 LakeDv 中找到 file 的 previous DV：
     - **找到了**：merge 当前 FilePos 到 previous DV，更新 LakeDv
     - **找不到**：说明该 file 在一个过期的 snapshot 中，忽略
3. 在 RowPosIndex 中删除这个 rowId
4. 比较 rowId 和 `snapshotStartLogOffset`：
   - **rowId < snapshotStartLogOffset**：直接忽略，要删除的行已在湖上快照中，通过 LakeDv 来屏蔽。读 delta log 时不会读到 rowId 对应的 changelog
   - **rowId >= snapshotStartLogOffset**：更新 LogDv，将 `offset = rowId` 的那条 changelog 标记为删除

恢复出来的 RowPosIndex、LogDv、LakeDv 都是针对 `restoreSnapshot` 的。如果有新 snapshot（记作 `newSnapshot`），则 `newSnapshot - restoreSnapshot` 即为 newFiles，`restoreSnapshot - newSnapshot` 即为 oldFiles，按上述"处理数据湖的 snapshot"流程处理。

### Client 通过 DV 进行 union read

1. Client 获得 DV 可见的最新 snapshot id，发送 union read 请求
2. Fluss list 该 snapshot 下的 datafile list
3. **获取 KvTablet 的读锁**（为了保证 LakeDv、LogDv 和 logEndOffset 一致。否则可能 LogDv 对应的是 `logoffset [0, 10]`，但返回 `logEndOffset = 12`，包含 `-U[key1, v1]` 和 `+U[key1, v2]`，读到 logEndOffset = 12 时会把 `+U[key1, v2]` 重复读出）
4. 获取当前 `logEndOffset`，从 LakeDv 中得到对应 datafile list 的 lakeDv，从 LogDv 中得到当前 snapshot 的 start offset 到 `logEndOffset` 的 logDv
5. **释放读锁**，给 client 返回 lakeDv、logDv、logEndOffset
6. Client 在数据湖 snapshot 上 apply lakeDv，fetch `[snapshot_start_offset, logEndOffset]` 这段 changelog 数据，apply logDv，读出实际数据

> **注意**：
> - 返回的 lakeDv 是对于数据湖全量数据的 DV，client 不需要再单独 apply Paimon 自己的 DV
> - 返回的 logDv 格式如下：

```json
{
  "logDv": [
    { "base_offset": "offset1",  "del_bits": "xxxx" },
    { "base_offset": "offset10", "del_bits": "xxxx" },
    { "base_offset": "offset20", "del_bits": "xxxx" }
  ]
}
```
