# Fluss Streamhouse 中的 Deletion Vector 支持

## 背景

Streamhouse 架构下，Fluss 作为实时层，Iceberg 作为历史层。对于主键表，更新和删除首先到达 Fluss，但同一行的旧版本可能已经 tiering 到 Iceberg 中。联合查询（union read）时，系统必须保证已删除或已更新的行不会从历史层重新出现。

## 问题

当前 tiering 写入 Iceberg 时，DELETE 和 UPDATE_BEFORE 通过 Iceberg v2 的 **equality delete** 处理。这个方案有以下缺陷：

- **小文件累积**：每轮 tiering 都会产生 equality delete 文件，随时间不断堆积。
- **读取合并开销大**：查询引擎需要将 equality delete 应用到所有历史 data file 上，读取性能持续劣化。
- **元数据膨胀**：manifest 条目随 delete 文件数量线性增长。

## 方案：三层 Deletion Vector

Fluss 通过在三个层次管理 deletion vector 来解决上述问题：

```
              Fluss (实时层)                    Iceberg (历史层)
  ┌───────────────────────────────┐     ┌──────────────────────────┐
  │  WAL (changelog)              │     │  Data Files (Parquet)    │
  │  ┌─────────────────────────┐  │     │                          │
  │  │  Log Deletion Vector    │  │     │  ┌────────────────────┐  │
  │  │  (热层内的删除/更新追踪) │  │     │  │ Iceberg Deletion   │  │
  │  └─────────────────────────┘  │     │  │ Vector (Puffin)    │  │
  │                               │     │  └────────────────────┘  │
  │  ┌─────────────────────────┐  │     │                          │
  │  │  Lake Deletion Vector   │──┼────►│  下一轮 tiering 时物化   │
  │  │  (跨层逻辑删除标记)     │  │     │                          │
  │  └─────────────────────────┘  │     │                          │
  └───────────────────────────────┘     └──────────────────────────┘
```

### 1. Iceberg Deletion Vector

标准的 Iceberg v3 deletion vector。Fluss tiering 数据到 Iceberg 时，删除操作物化为 Puffin 文件，其中包含 RoaringBitmap，精确指向 data file 中被删除行的 row position。完全替代 equality delete。

### 2. Log Deletion Vector

追踪 Fluss 实时 changelog 中的删除和更新。仅作用于仍在热层（WAL）中、尚未 tiering 到 Iceberg 的数据。

当一轮 tiering 完成后，下一轮 tiering 开始前，新到达的 DELETE 和 UPDATE 记录会持续写入 WAL。
这些变更对应的旧行可能存在于两个位置：同在 WAL 中的更早记录，或者已经 tiering 到 Iceberg 的历史数据。
Log Deletion Vector 负责前者——标记 WAL 内部已被后续操作覆盖或删除的行，确保联合查询时不会读到 WAL 中已过时的版本。后者（旧行已在 Iceberg 中）则由 Lake Deletion Vector 负责。

### 3. Lake Deletion Vector

连接实时层与历史层的桥梁。当 Fluss 收到一条针对已 tiering 到 Iceberg 的行的删除或更新时：

- Fluss 记录元数据，将 Iceberg 中对应的行逻辑标记为已删除。
- 该逻辑删除在联合查询时**立即生效**，无需等待下一次 Iceberg snapshot 写入。
- 这些逻辑删除会在下一轮 tiering commit 时物化为 Iceberg 中的物理 deletion vector。

## 联合查询语义

联合查询（Fluss 热数据 + Iceberg 历史数据）时，查询引擎应用所有相关的 deletion vector：

- **Log Deletion Vector** 屏蔽热层中已被删除或覆盖的行。
- **Lake Deletion Vector** 屏蔽 Iceberg 中已在 Fluss 侧被删除但尚未物化的行。
- **Iceberg Deletion Vector** 屏蔽 Iceberg 中已物化的删除行。

三层协作确保正确的 upsert 语义：UPDATE 产生最新值，DELETE 彻底移除该行，无论原始数据位于哪一层。

## 意义

这套机制是 Streamhouse 中主键表的基础构建块，在不牺牲查询性能和数据时效性的前提下，实现跨流式数据和历史数据的正确 upsert 语义。
