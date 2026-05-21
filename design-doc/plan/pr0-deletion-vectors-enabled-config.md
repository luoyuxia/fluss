# PR 0: `table.deletion-vectors.enabled` 配置项 + 前置校验

## 目标

引入 `table.deletion-vectors.enabled` 表级配置项，作为 Fluss 三层 DV 架构的总开关。该配置必须在建表时设置，建表后不可修改。

## 设计文档参考

[fluss-paimon-deletion-vector-design.md](../fluss-paimon-deletion-vector-design.md) §10.5

---

## 改动清单

### 1. 新增配置项定义

**文件**：`fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`

在 `TABLE_DATALAKE_AUTO_EXPIRE_SNAPSHOT`（约 line 1597）之后新增：

```java
public static final ConfigOption<Boolean> TABLE_DELETION_VECTORS_ENABLED =
        key("table.deletion-vectors.enabled")
                .booleanType()
                .defaultValue(false)
                .withDescription(
                        "Whether to enable Deletion Vector support for the table. "
                                + "Must be set at table creation time and cannot be changed afterwards. "
                                + "When enabled, Fluss maintains a three-layer DV architecture "
                                + "(Lake DV + Log DV + Paimon DV) for instant cross-layer deduplication "
                                + "during union reads. Disabled by default as it introduces additional "
                                + "storage, write path, and tiering overhead. "
                                + "Requires: primary key table, "
                                + "'table.datalake.enabled' = true, "
                                + "and FULL changelog image mode.");
```

### 2. 建表后不可变（ALTER TABLE 拒绝修改）

#### 2a. `table.deletion-vectors.enabled`（table property）

**文件**：`fluss-common/src/main/java/org/apache/fluss/config/FlussConfigUtils.java`

不需要改动。`ALTERABLE_TABLE_OPTIONS`（line 42-53）是可修改属性的白名单，`table.deletion-vectors.enabled` 不在其中，因此 `isAlterableTableOption()` 自动返回 false。

ALTER TABLE 尝试修改时，`TableDescriptorValidation.validateAlterTableProperties()`（line 160-171）会检测到这是一个非 alterable 的 `table.*` 属性，抛出 `InvalidAlterTableException`。无需额外代码。

#### 2b. `paimon.deletion-vectors.enabled`（custom property）

无需 Fluss 侧额外拦截。Paimon 本身将 `deletion-vectors.enabled` 视为不可变属性，ALTER TABLE 变更传播到 Paimon 时，Paimon 的 schema validation 会自动拒绝修改。

### 3. 建表时前置条件校验

**文件**：`fluss-server/src/main/java/org/apache/fluss/server/utils/TableDescriptorValidation.java`

在 `validateTableDescriptor()` 方法（line 86-130）中，在 `checkTableLakeFormatMatchesCluster`（line 129）之后新增校验方法调用：

```java
checkDeletionVectors(tableDescriptor, tableConf);
```

新增方法：

```java
private static void checkDeletionVectors(
        TableDescriptor tableDescriptor, Configuration tableConf) {
    if (!tableConf.get(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED)) {
        return;
    }

    // 1. 必须是主键表
    if (!tableDescriptor.hasPrimaryKey()) {
        throw new InvalidConfigException(
                "'table.deletion-vectors.enabled' can only be enabled on primary key tables.");
    }

    // 2. 必须开启 datalake
    if (!tableConf.get(ConfigOptions.TABLE_DATALAKE_ENABLED)) {
        throw new InvalidConfigException(
                "'table.deletion-vectors.enabled' requires 'table.datalake.enabled' = true.");
    }

    // 3. 必须使用 FULL changelog image mode
    ChangelogImage changelogImage = tableConf.get(ConfigOptions.TABLE_CHANGELOG_IMAGE);
    if (changelogImage != ChangelogImage.FULL) {
        throw new InvalidConfigException(
                "'table.deletion-vectors.enabled' requires FULL changelog image mode. "
                        + "Please set 'table.changelog.image' to 'FULL' or remove the setting "
                        + "(FULL is the default).");
    }
}
```

### 4. 运行时判断方法（放在 TableInfo 中）

**文件**：`fluss-common/src/main/java/org/apache/fluss/metadata/TableInfo.java`

在 `TableInfo` 中新增 `isDeletionVectorsEnabled()` 方法，供后续 PR 在各组件中使用。`TableInfo` 是运行时最常被传递的表元数据对象，且已有类似的 boolean check 方法（如 `hasPrimaryKey()`、`isPartitioned()`、`isAutoPartitioned()`、`isStatisticsEnabled()`），放在这里符合现有代码风格。

在 `isStatisticsEnabled()` 方法（约 line 241）之后新增：

```java
/** Returns true if deletion vectors is enabled for this table. */
public boolean isDeletionVectorsEnabled() {
    return properties.get(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED);
}
```

其中 `properties` 是 `TableInfo` 已有的 `Configuration` 字段（line 75），无需额外构造。

### 5. 强制联动 `paimon.deletion-vectors.enabled`

**文件**：`fluss-lake/fluss-lake-paimon/src/main/java/org/apache/fluss/lake/paimon/utils/PaimonConversions.java`

由于 Fluss 的 DV 不支持开启后再关闭（建表时确定，不可变），`paimon.deletion-vectors.enabled` 必须与 `table.deletion-vectors.enabled` 保持一致，用户不允许单独控制 Paimon 侧的 DV 开关。

在 `toPaimonSchema()` 方法中（约 line 233），在处理完 `properties` 和 `customProperties` 之后、在已有的 `if (options.get(CoreOptions.DELETION_VECTORS_ENABLED))` 校验之前，新增：

```java
// Fluss DV doesn't support toggle after creation, so Paimon DV must be consistent
Configuration flussConf = Configuration.fromMap(tableDescriptor.getProperties());
boolean flussDvEnabled = flussConf.get(ConfigOptions.TABLE_DELETION_VECTORS_ENABLED);
boolean paimonDvEnabled = options.get(CoreOptions.DELETION_VECTORS_ENABLED);

if (flussDvEnabled && !paimonDvEnabled) {
    // Auto-enable Paimon DV when Fluss DV is enabled
    options.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
} else if (!flussDvEnabled && paimonDvEnabled) {
    // Reject: Paimon DV requires Fluss DV
    throw new InvalidConfigException(
            "'paimon.deletion-vectors.enabled' requires 'table.deletion-vectors.enabled' = true. "
                    + "Fluss must maintain the three-layer DV architecture for Paimon DV to work.");
}
```

逻辑说明：
- `table.deletion-vectors.enabled = true` 且用户未设置 `paimon.deletion-vectors.enabled` → 自动补上
- `table.deletion-vectors.enabled = true` 且用户已设置 `paimon.deletion-vectors.enabled = true` → 无冲突，正常通过
- `table.deletion-vectors.enabled = false` 且用户设置了 `paimon.deletion-vectors.enabled = true` → 拒绝，因为 Paimon DV 文件依赖 Fluss 三层 DV 架构
- 后续已有的 partition key 类型校验逻辑不受影响

两个配置的关系：
- `table.deletion-vectors.enabled`（Fluss 侧）：总开关，控制 Fluss 是否维护三层 DV 架构（DvRocksDB、DvManager、SST、Prepare/Switch 等）
- `paimon.deletion-vectors.enabled`（Paimon 侧）：由 Fluss 侧总开关决定，不允许单独设置。开启后 Paimon compaction 会生成 DV 文件

---

## 涉及文件列表

| 文件 | 操作 | 说明 |
|------|------|------|
| `fluss-common/.../config/ConfigOptions.java` | 修改 | 新增 `TABLE_DELETION_VECTORS_ENABLED` |
| `fluss-server/.../utils/TableDescriptorValidation.java` | 修改 | 新增 `checkDeletionVectors()` |
| `fluss-common/.../metadata/TableInfo.java` | 修改 | 新增 `isDeletionVectorsEnabled()` 方法 |
| `fluss-lake/fluss-lake-paimon/.../utils/PaimonConversions.java` | 修改 | 强制联动 `paimon.deletion-vectors.enabled`，禁止单独设置 |

不需要修改的文件（利用现有机制）：
- `FlussConfigUtils.java`：`ALTERABLE_TABLE_OPTIONS` 白名单机制自动拒绝 ALTER `table.deletion-vectors.enabled`
- `MetadataManager.java`：已有的调用链无需改动
- `CoordinatorService.java`：已有的 `createTable` → `validateTableDescriptor` 调用链无需改动
- Paimon schema validation：自动拒绝 ALTER `paimon.deletion-vectors.enabled`

---

## 测试

本 PR 改动较简单（配置定义 + 校验 + 联动），不单独新增测试，后续 PR 的集成测试会覆盖。
