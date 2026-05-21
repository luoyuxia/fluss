# PR 2: KV State RowId + Changelog 格式扩展

## 目标

在 KV state value 中嵌入 RowId，在 changelog record 中携带 RowId，为 DV 写入路径提供数据基础。

## 设计文档参考

- [fluss-deletion-vector-design-v3-en.md](../fluss-deletion-vector-design-v3-en.md) §3.1, §10.1, §10.2
- [fluss-paimon-deletion-vector-design.md](../fluss-paimon-deletion-vector-design.md) §3.1, §10.1, §10.2（引用 Iceberg 版）

---

## 核心概念

### RowId 定义

RowId 唯一标识 KV 记录的一个**特定版本**（不是主键本身），其值为对应 `+I` / `+U` changelog 记录的 log offset。

| KV 操作            | Changelog 记录              | RowId             |
|------------------|---------------------------|-------------------|
| `PUT (key1, v1)` | `+I (offset=0, key1, v1)` | RowId = 0（第一版本）   |
| `PUT (key1, v2)` | `-U (offset=1, key1, v1)` | 引用 RowId = 0（旧版本） |
|                  | `+U (offset=2, key1, v2)` | RowId = 2（第二版本）   |
| `DELETE (key1)`  | `-D (offset=3, key1, v2)` | 引用 RowId = 2（旧版本） |

---

## 改动清单

### 1. KV State Value 格式：嵌入 RowId

**设计**：在 value 最前端插入 RowId，使用 unsigned varint（LEB128）编码。RowId 放在最前，这样更新/删除时可快速读取前几字节获取 oldRowId，无需解析 BinaryRow。

Varint 编码优势：RowId 实际上是 log offset，典型值远小于 2^63。varint 根据数值大小自适应字节数：

| RowId 范围 | varint 字节数 | 对比固定 8B 节省 |
|---|---|---|
| 0 ~ 127 | 1B | 7B |
| 128 ~ 16383 | 2B | 6B |
| 16384 ~ 2M | 3B | 5B |
| 2M ~ 268M | 4B | 4B |
| 268M ~ 34B | 5B | 3B |

PR 1 的 `FilePos.java` 已实现 unsigned varint encode/decode 工具方法，可直接复用。

```
旧格式: [SchemaId (2B)][BinaryRow (变长)]
新格式: [RowId (varint)][SchemaId (2B)][BinaryRow (变长)]
```

DV 开关关闭时走旧格式，无任何额外开销。

**文件**：修改 `fluss-common/.../row/encode/ValueEncoder.java`

```java
public class ValueEncoder {
    // 现有方法保留不变（非 DV 表使用）
    public static byte[] encodeValue(short schemaId, BinaryRow row) { ... }

    // 新增：DV 表使用（varint 编码 RowId）
    public static byte[] encodeValueWithRowId(short schemaId, BinaryRow row, long rowId) {
        int varintLen = computeUnsignedVarLongSize(rowId);
        byte[] values = new byte[varintLen + SCHEMA_ID_LENGTH + row.getSizeInBytes()];
        int offset = putUnsignedVarLong(values, 0, rowId);
        UnsafeUtils.putShort(values, offset, schemaId);
        row.copyTo(values, offset + SCHEMA_ID_LENGTH);
        return values;
    }

    // 快速提取 RowId（解析 varint 前缀）
    public static long extractRowId(byte[] valueBytes) {
        return getUnsignedVarLong(valueBytes, 0);
    }

    // 返回 RowId varint 占用的字节数（用于跳过 RowId 定位 SchemaId）
    public static int rowIdVarIntSize(byte[] valueBytes) {
        return computeUnsignedVarLongSizeFromBytes(valueBytes, 0);
    }
}
```

> 注：varint 工具方法可从 PR 1 的 `FilePos` 中提取为共享静态工具方法（如 `VarIntUtils`），或直接在 `ValueEncoder` 中内联实现。

**文件**：修改 `fluss-common/.../row/encode/ValueDecoder.java`

```java
public class ValueDecoder {
    // 现有方法保留不变
    public BinaryValue decodeValue(byte[] valueBytes) { ... }

    // 新增：解码 DV 格式，跳过 varint RowId
    public BinaryValue decodeValueSkippingRowId(byte[] valueBytes) {
        int rowIdLen = ValueEncoder.rowIdVarIntSize(valueBytes);
        MemorySegment memorySegment = MemorySegment.wrap(valueBytes);
        short schemaId = memorySegment.getShort(rowIdLen);
        RowDecoder rowDecoder = rowDecoders.computeIfAbsent(schemaId, ...);
        BinaryRow row = rowDecoder.decode(
                memorySegment,
                rowIdLen + SCHEMA_ID_LENGTH,
                valueBytes.length - rowIdLen - SCHEMA_ID_LENGTH);
        return new BinaryValue(schemaId, row);
    }
}
```

**文件**：修改 `fluss-common/.../record/BinaryValue.java`

```java
public class BinaryValue {
    // 新增：DV 模式 encode
    public byte[] encodeValueWithRowId(long rowId) {
        return ValueEncoder.encodeValueWithRowId(schemaId, row, rowId);
    }
}
```

---

### 2. Changelog 格式扩展：Record 携带 RowId

**设计**：DV 是表级属性——开了 DV 的表，所有 changelog record 一定有 RowId；关了就一定没有。因此不需要 per-record 的 `hasRowId` 标志位，读写端根据表的 `isDeletionVectorsEnabled()` 配置决定格式。Attributes 字节布局保持不变。

```
非 DV 表: [Length (4B)][Attributes (1B)][InternalRow (变长)]
DV 表:    [Length (4B)][Attributes (1B)][RowId (varint)][InternalRow (变长)]
```

Attributes 字节布局不变：

```
bit 0-3: ChangeType (0=+A, 1=+I, 2=-U, 3=+U, 4=-D)
bit 4-7: 保留（未使用）
```

四种 record 的 RowId 语义：
- `+I`/`+U`：RowId = 该记录自己的 log offset（新版本的标识）
- `-U`/`-D`：RowId = 旧版本的 RowId（从 KV state value 头部读取）

向后兼容：非 DV 表的 record 格式完全不变，消费端通过表 metadata 判断是否需要解析 RowId。

**⚠️ 重要约束：读取时 RowId 对行数据透明**

消费者通过 `LogRecord.getRow()` 获取的 InternalRow **不能包含 RowId 字节**。RowId 是 DV 的内部 metadata，只通过 `LogRecord.getRowId()` 单独获取。对所有三种格式：

- **Indexed / Compacted**：解析时先读 varint RowId，然后将 InternalRow 指向 RowId 之后的位置
- **Arrow**：RowIdVector 与 ArrowData 本身就是独立存储，天然隔离

这保证了现有消费端（CDC source、Flink connector 等）读到的 `getRow()` 内容与非 DV 表完全一致，无需任何适配。

#### 2a. LogRecord 接口扩展

**文件**：修改 `fluss-common/.../record/LogRecord.java`

```java
public interface LogRecord {
    // 现有方法不变
    long logOffset();
    long timestamp();
    ChangeType getChangeType();
    InternalRow getRow();

    // 新增：获取 RowId，非 DV record 返回 NO_ROW_ID
    long NO_ROW_ID = -1L;
    default long getRowId() { return NO_ROW_ID; }
}
```

#### 2b. IndexedLogRecord 格式调整

**文件**：修改 `fluss-common/.../record/IndexedLogRecord.java`

```java
public class IndexedLogRecord implements LogRecord {
    private long rowId = NO_ROW_ID;

    // pointTo() / readFrom() 需要新增 dvEnabled 参数（或在构造时传入）：
    // 1. 读取 Attributes byte → 解析 ChangeType（不变）
    // 2. 若 dvEnabled，解析 varint RowId，将 rowId 字段赋值
    // 3. InternalRow 的起始位置 = Attributes 之后 + varint RowId 字节数
    //    ⚠️ 关键：getRow() 返回的 InternalRow 不能包含 RowId 字节，
    //    RowId 仅通过 getRowId() 获取
    // 4. 若非 dvEnabled，直接读 InternalRow（原路径不变）
}
```

> 注：dvEnabled 如何传入有两种方式：(a) 在 LogRecordBatch / LogRecordReadContext 中携带；(b) 在 IndexedLogRecord 构造时注入。具体实现时选择对现有调用链侵入最小的方式。

#### 2c. CompactedLogRecord 格式调整

**文件**：修改 `fluss-common/.../record/CompactedLogRecord.java`

逻辑同 IndexedLogRecord。

#### 2d. MemoryLogRecordsIndexedBuilder 写入 RowId

**文件**：修改 `fluss-common/.../record/MemoryLogRecordsIndexedBuilder.java`

```java
// 新增方法
public void append(ChangeType changeType, IndexedRow row, long rowId) {
    // Attributes 不变（不需要设置 bit flag）
    // 在 Attributes 之后写入 RowId (varint 编码)
    // 写入 IndexedRow
}
```

#### 2e. MemoryLogRecordsCompactedBuilder 写入 RowId

**文件**：修改 `fluss-common/.../record/MemoryLogRecordsCompactedBuilder.java`

逻辑同 IndexedBuilder。

#### 2f. Arrow 格式

Arrow 使用列式 batch 格式，没有 per-record 的 `[Length][Attributes][Row]` 头部，而是用独立的 **ChangeTypeVector**（每条 record 1 字节）+ Arrow columnar data。RowId 采用类似方案：新增 **RowIdVector**，每条 record 固定 8 字节（BigEndian long）。

> 为什么 Arrow 格式用固定 8 字节而非 varint？ChangeTypeVector 已采用固定宽度（1B/record）的设计，支持按 index 随机访问。RowIdVector 沿用同样模式，固定 8 字节/record，无需额外 length prefix，size = recordCount × 8，reader 可直接定位 ArrowData 起始位置。

**DV 表的 Arrow batch 布局**：

```
非 DV:  [Header][Statistics?][ChangeTypes?][ArrowData]
DV 表:  [Header][Statistics?][ChangeTypes?][RowIdVector (recordCount × 8B)][ArrowData]
```

Reader 根据表的 `isDeletionVectorsEnabled()` 决定是否在 ChangeTypes 之后解析 RowIdVector。

**文件**：修改 `fluss-common/.../record/MemoryLogRecordsArrowBuilder.java`

```java
// 新增方法（DV 模式）
public void append(ChangeType changeType, InternalRow row, long rowId) {
    arrowWriter.writeRow(row);
    if (!appendOnly) {
        changeTypeWriter.writeChangeType(changeType);
    }
    rowIdWriter.writeRowId(rowId);  // 写入 8 字节 BigEndian long
}
```

构造时根据 dvEnabled 决定是否创建 RowIdVectorWriter。build() 时在 ChangeTypes 和 ArrowData 之间插入 RowIdVector bytes。

**文件**：新增 `fluss-common/.../record/RowIdVector.java`

```java
// 类比 ChangeTypeVector，固定 8 字节/record
public class RowIdVector {
    public long getRowId(int i) {
        return segment.getLong(position + i * Long.BYTES);
    }
    public int sizeInBytes() {
        return recordCount * Long.BYTES;
    }
}
```

**文件**：新增 `fluss-common/.../record/RowIdVectorWriter.java`

```java
// 类比 ChangeTypeVectorWriter
public class RowIdVectorWriter {
    public void writeRowId(long rowId) {
        segment.putLong(startPosition + recordsCount * Long.BYTES, rowId);
        recordsCount++;
    }
    public int sizeInBytes() {
        return recordsCount * Long.BYTES;
    }
}
```

**文件**：修改 `fluss-common/.../record/DefaultLogRecordBatch.java`

在 `columnRecordIterator()` 的 Arrow 读取路径中，DV 模式下在 ChangeTypeVector 之后解析 RowIdVector，然后将 RowId 注入 GenericRecord。

**文件**：修改 `fluss-server/.../kv/wal/ArrowWalBuilder.java`

```java
@Override
public void append(ChangeType changeType, InternalRow row, long rowId) throws Exception {
    recordsBuilder.append(changeType, row, rowId);
}
```

---

### 3. WalBuilder 接口扩展

**文件**：修改 `fluss-server/.../kv/wal/WalBuilder.java`

```java
public interface WalBuilder {
    // 现有方法保留
    void append(ChangeType changeType, InternalRow row) throws Exception;

    // 新增：DV 模式，携带 RowId
    default void append(ChangeType changeType, InternalRow row, long rowId) throws Exception {
        // 默认实现：忽略 rowId，兼容非 DV 场景
        append(changeType, row);
    }

    MemoryLogRecords build() throws Exception;
    void setWriterState(long writerId, int batchSequence);
    void deallocate();
}
```

**文件**：修改 `fluss-server/.../kv/wal/IndexWalBuilder.java`

```java
@Override
public void append(ChangeType changeType, InternalRow row, long rowId) throws Exception {
    recordsBuilder.append(changeType, (IndexedRow) row, rowId);
}
```

**文件**：修改 `fluss-server/.../kv/wal/CompactedWalBuilder.java`

类似 IndexWalBuilder，调用 `recordsBuilder.append(changeType, compactedRow, rowId)`。

---

### 4. KvTablet 写入路径调整

**文件**：修改 `fluss-server/.../kv/KvTablet.java`

#### 4a. 新增 DV 状态字段

```java
private final boolean dvEnabled;  // 从 TableInfo.isDeletionVectorsEnabled() 获取
```

#### 4b. 修改 applyInsert()

```java
private long applyInsert(..., long logOffset, ...) throws Exception {
    BinaryValue newValue = autoIncrementUpdater.updateAutoIncrementColumns(currentValue);
    if (dvEnabled) {
        walBuilder.append(ChangeType.INSERT, latestSchemaRow.replaceRow(newValue.row), logOffset);
        kvPreWriteBuffer.insert(key, newValue.encodeValueWithRowId(logOffset), logOffset);
    } else {
        walBuilder.append(ChangeType.INSERT, latestSchemaRow.replaceRow(newValue.row));
        kvPreWriteBuffer.insert(key, newValue.encodeValue(), logOffset);
    }
    return logOffset + 1;
}
```

#### 4c. 修改 applyUpdate()

```java
private long applyUpdate(..., long logOffset) throws Exception {
    if (changelogImage == ChangelogImage.FULL) {
        if (dvEnabled) {
            long oldRowId = ValueEncoder.extractRowId(oldValueBytes);
            walBuilder.append(ChangeType.UPDATE_BEFORE,
                    latestSchemaRow.replaceRow(oldValue.row), oldRowId);
            walBuilder.append(ChangeType.UPDATE_AFTER,
                    latestSchemaRow.replaceRow(newValue.row), logOffset + 1);
            kvPreWriteBuffer.update(key,
                    newValue.encodeValueWithRowId(logOffset + 1), logOffset + 1);
        } else {
            walBuilder.append(ChangeType.UPDATE_BEFORE,
                    latestSchemaRow.replaceRow(oldValue.row));
            walBuilder.append(ChangeType.UPDATE_AFTER,
                    latestSchemaRow.replaceRow(newValue.row));
            kvPreWriteBuffer.update(key, newValue.encodeValue(), logOffset + 1);
        }
        return logOffset + 2;
    } else {
        // WAL mode (DV 不使用 WAL mode，但保持兼容)
        // ...
    }
}
```

> 注：DV 要求 FULL changelog mode，但 WAL mode 分支仍保留以保持代码完整性。

#### 4d. 修改 applyDelete()

```java
private long applyDelete(..., long logOffset) throws Exception {
    if (dvEnabled) {
        long oldRowId = ValueEncoder.extractRowId(oldValueBytes);
        walBuilder.append(ChangeType.DELETE,
                latestSchemaRow.replaceRow(oldValue.row), oldRowId);
    } else {
        walBuilder.append(ChangeType.DELETE,
                latestSchemaRow.replaceRow(oldValue.row));
    }
    kvPreWriteBuffer.delete(key, logOffset);
    return logOffset + 1;
}
```

#### 4e. 修改 processUpsert() / processDeletion()

需要将 `oldValueBytes` 传递到 `applyUpdate()` / `applyDelete()`，以便 DV 模式下提取 oldRowId。

当前签名：
```java
applyDelete(key, oldValue, walBuilder, latestSchemaRow, logOffset)
applyUpdate(key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset)
```

DV 模式调整：
```java
applyDelete(key, oldValue, oldValueBytes, walBuilder, latestSchemaRow, logOffset)
applyUpdate(key, oldValue, newValue, oldValueBytes, walBuilder, latestSchemaRow, logOffset)
```

或者在 `processDeletion()` / `processUpsert()` 中提取 oldRowId 后传入：
```java
long oldRowId = dvEnabled ? ValueEncoder.extractRowId(oldValueBytes) : LogRecord.NO_ROW_ID;
applyDelete(key, oldValue, walBuilder, latestSchemaRow, logOffset, oldRowId)
```

两种方式等价，选择后者更清晰（不泄漏原始 bytes）。

#### 4f. 修改 valueDecoder 调用

```java
// 在 processDeletion() / processUpsert() 中
BinaryValue oldValue;
if (dvEnabled) {
    oldValue = valueDecoder.decodeValueSkippingRowId(oldValueBytes);
} else {
    oldValue = valueDecoder.decodeValue(oldValueBytes);
}
```

---

## 涉及文件列表

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-common/.../row/encode/ValueEncoder.java` | 修改 | 新增 `encodeValueWithRowId()`、`extractRowId()`、`rowIdVarIntSize()` + varint 工具方法 |
| `fluss-common/.../row/encode/ValueDecoder.java` | 修改 | 新增 `decodeValueSkippingRowId()` |
| `fluss-common/.../record/BinaryValue.java` | 修改 | 新增 `encodeValueWithRowId(long rowId)` |
| `fluss-common/.../record/LogRecord.java` | 修改 | 新增 `getRowId()` 默认方法、`NO_ROW_ID` 常量 |
| `fluss-common/.../record/IndexedLogRecord.java` | 修改 | DV 模式下解析 varint RowId |
| `fluss-common/.../record/CompactedLogRecord.java` | 修改 | 同 IndexedLogRecord |
| `fluss-common/.../record/MemoryLogRecordsIndexedBuilder.java` | 修改 | 新增带 rowId 的 append |
| `fluss-common/.../record/MemoryLogRecordsCompactedBuilder.java` | 修改 | 同 IndexedBuilder |
| `fluss-server/.../kv/wal/WalBuilder.java` | 修改 | 新增带 rowId 的 append 默认方法 |
| `fluss-server/.../kv/wal/IndexWalBuilder.java` | 修改 | 实现带 rowId 的 append |
| `fluss-server/.../kv/wal/CompactedWalBuilder.java` | 修改 | 实现带 rowId 的 append |
| `fluss-server/.../kv/wal/ArrowWalBuilder.java` | 修改 | 实现带 rowId 的 append |
| `fluss-common/.../record/MemoryLogRecordsArrowBuilder.java` | 修改 | 新增带 rowId 的 append，build 时写入 RowIdVector |
| `fluss-common/.../record/DefaultLogRecordBatch.java` | 修改 | Arrow 读取路径解析 RowIdVector |
| `fluss-common/.../record/RowIdVector.java` | 新增 | 读取端：固定 8B/record 的 RowId 向量 |
| `fluss-common/.../record/RowIdVectorWriter.java` | 新增 | 写入端：固定 8B/record 的 RowId 向量写入器 |
| `fluss-server/.../kv/KvTablet.java` | 修改 | 写入路径集成 RowId 编解码 |

---

## 关键设计决策

### RowId 放在 value 最前端（varint 编码）

RowId 放在 KV state value 的最前端（varint 编码），而不是 schemaId 之后。这样更新/删除时只需从首字节开始解析 varint 即可获取 oldRowId，无需解析变长的 BinaryRow。

使用 unsigned varint（LEB128）而非固定 8 字节的原因：
1. **空间节省显著**：RowId 是 log offset，绝大多数场景下值在 0~数十亿范围，varint 只需 1~5 字节（对比固定 8 字节节省 3~7 字节）
2. **解析开销极低**：varint decode 只需逐字节检查高位 bit，对 CPU 几乎无感知
3. **与 PR 1 一致**：PR 1 的 `FilePos` 已对 rowPosition（同为 long 类型）使用 varint 编码，保持风格统一
4. **跳过 RowId 同样简单**：`decodeValueSkippingRowId()` 只需先算出 varint 字节数再跳过

### Changelog RowId 由表级 DV 配置决定（无 per-record 标志位）

DV 是表级属性，开了 DV 的表所有 changelog record 一定携带 RowId，关了则一定没有。因此不需要在 Attributes 中引入 `hasRowId` bit flag：

1. **格式更简洁**：Attributes 字节保持原样，bit 4-7 完全保留
2. **解析更清晰**：不需要 per-record 分支判断，读端根据表 metadata 选择解析路径
3. **天然兼容**：非 DV 表的 record 格式完全不变，零侵入
4. **消费端已有 metadata**：CDC source、lake sync、follower 等消费者都能获取表的 `isDeletionVectorsEnabled()` 配置

### 所有四种 record 类型均携带 RowId

虽然 `+I`/`+U` 的 RowId 语义上等于其自身 log offset（可以推导），但统一所有四种 record 类型都携带 RowId：
1. 消费端无需按类型分支处理
2. 避免在所有消费路径中重复实现 "RowId = log offset" 的隐式约束
3. 为未来 RowId 与 log offset 解耦留余地
4. varint 编码下每条 record 仅 1~5 字节额外开销（通常 < 5% 总 payload）

### DV 开关全路径控制

所有修改均受 `isDeletionVectorsEnabled()` 开关控制。DV 关闭时：
- ValueEncoder/ValueDecoder 走旧路径，无额外开销
- WalBuilder 使用原始 `append(ChangeType, InternalRow)`
- KvTablet 不提取 RowId，不传递 RowId

### Arrow 格式使用固定 8 字节 RowIdVector

Arrow 格式是列式 batch，与 Indexed/Compacted 的行式 per-record 格式不同。Arrow 的 ChangeTypeVector 已采用固定宽度（1B/record），RowIdVector 沿用相同模式（8B/record），而非 Indexed/Compacted 中的 varint。原因：

1. **与 ChangeTypeVector 设计一致**：固定宽度向量，支持按 index 随机访问
2. **无需 length prefix**：size = recordCount × 8，reader 可直接计算 ArrowData 起始位置
3. **无需修改 batch header**：不用引入新的 magic version 来存储 varint vector 的总字节数

---

## 测试

### ValueEncoder/ValueDecoder 测试

- `encodeValueWithRowId()` + `extractRowId()` 往返一致性
- `encodeValueWithRowId()` + `decodeValueSkippingRowId()` 往返一致性
- RowId = 0（varint 1 字节）、RowId = 127（varint 1 字节边界）、RowId = 128（varint 2 字节）、RowId = Long.MAX_VALUE（varint 9 字节）边界值
- `rowIdVarIntSize()` 对各范围 RowId 返回正确字节数
- 旧格式 `encodeValue()` 的数据仍可用 `decodeValue()` 正常解码（向后兼容）

### Changelog Record 测试

- Indexed format（DV 模式）：写入带 RowId 的 record → 读取 → getRowId() 正确
- Compacted format（DV 模式）：同上
- 非 DV 模式：写入/读取 record → getRowId() 返回 NO_ROW_ID（原有行为不变）
- 四种 ChangeType 的 record 均正确携带 RowId
- varint 编码的 RowId 边界值测试（小值、大值）
- Arrow format（DV 模式）：写入带 RowId 的 batch → 读取 → getRowId() 正确
- Arrow format（非 DV 模式）：原有行为不变，无 RowIdVector
- RowIdVector / RowIdVectorWriter 单元测试

### KvTablet 集成测试

- INSERT：KV state value 包含 RowId = logOffset，changelog `+I` record 携带 RowId = logOffset
- UPDATE（FULL mode）：
  - `-U` record 携带 oldRowId（旧版本 RowId）
  - `+U` record 携带 newRowId = logOffset+1
  - 新 KV state value 包含 newRowId
- DELETE：`-D` record 携带 oldRowId
- DV 关闭时：KV state value 无 RowId，changelog record 无 RowId（原有行为不变）

---

## 前置依赖

- PR 0（`table.deletion-vectors.enabled` 配置项）—— KvTablet 需要读取此配置判断是否启用 DV
- PR 1 代码不直接依赖，但逻辑上 PR 2 提供的 RowId 是 PR 3（DvManager）的数据基础
