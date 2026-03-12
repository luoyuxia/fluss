# FileSystem Scheme Mapping

## 1. 背景

用户有内部优化的 OSS Hadoop FileSystem 实现，使用 `ossj://` 前缀。底层数据和标准 `oss://` 是同一份 OSS 存储，只是 Hadoop FileSystem 实现不同。

### 需求场景

- 路径是 `ossj://xxx`（来自外部元数据，不可更改）
- **Client A**（有 ossj 插件）：直接用自己的优化插件读取，不受影响
- **Client B**（无 ossj 插件）：通过配置 `client.fs.scheme-mapping.ossj = oss`，复用 `OSSFileSystemPlugin` 读取

### 可行性依据

参考先例：`S3FileSystemPlugin`（scheme `s3`）和 `S3AFileSystemPlugin`（scheme `s3a`）底层都是 `new S3AFileSystem()`，`S3AFileSystem` 接受 `s3://` URI 初始化，验证了 Hadoop FileSystem 不校验传入的 scheme。

## 2. 改动文件

共 4 个文件，均不涉及 Server 端改动或配置。

### 2.1 `fluss-common/.../fs/FileSystem.java`

#### 2.1.1 新增常量

```java
private static final String SCHEME_MAPPING_PREFIX = "fs.scheme-mapping.";
```

#### 2.1.2 在 `initialize()` 中注册 scheme mapping

在 `FS_PLUGINS.put(scheme, plugin)` 循环之后新增：

```java
// Register scheme aliases from configuration.
// e.g., fs.scheme-mapping.ossj = oss means "ossj" scheme uses the "oss" plugin.
for (String key : config.keySet()) {
    if (key.startsWith(SCHEME_MAPPING_PREFIX)) {
        String aliasScheme = key.substring(SCHEME_MAPPING_PREFIX.length());
        String targetScheme = config.getString(
                ConfigBuilder.key(key).stringType().noDefaultValue(), null);
        if (targetScheme != null) {
            FileSystemPlugin targetPlugin = FS_PLUGINS.get(targetScheme);
            if (targetPlugin != null && !FS_PLUGINS.containsKey(aliasScheme)) {
                FS_PLUGINS.put(aliasScheme, targetPlugin);
                LOG.info("Registered filesystem scheme mapping: {} -> {}",
                         aliasScheme, targetScheme);
            }
        }
    }
}
```

关键点：`!FS_PLUGINS.containsKey(aliasScheme)` 保证 Client A 自己 SPI 注册的 `ossj` plugin 不会被覆盖。

#### 2.1.3 新增 import

```java
import org.apache.fluss.config.ConfigBuilder;
```

### 2.2 `fluss-client/.../client/FlussConnection.java`

#### 问题

当前 `extractPrefix` 只过滤 key 前缀但不 strip，导致 `client.fs.*` 配置传入 `FileSystem.initialize()` 后 key 仍带 `client.` 前缀，scheme mapping 匹配不到。

#### 修复

```java
// 修复前：
FileSystem.initialize(
    Configuration.fromMap(
        extractPrefix(new HashMap<>(conf.toMap()), CLIENT_PREFIX + "fs.")),
    null);

// 修复后：
FileSystem.initialize(
    Configuration.fromMap(
        extractAndRemovePrefix(
            extractPrefix(new HashMap<>(conf.toMap()), CLIENT_PREFIX + "fs."),
            CLIENT_PREFIX)),
    null);
```

效果：`client.fs.scheme-mapping.ossj` → filter `client.fs.*` → strip `client.` → `fs.scheme-mapping.ossj`

#### 新增 import

```java
import static org.apache.fluss.utils.PropertiesUtils.extractAndRemovePrefix;
```

### 2.3 `fluss-client/.../client/token/SecurityTokenReceiverRepository.java`

#### 问题

Server 可能发出 scheme="ossj" 的 token，Client B 没有 `ossj` receiver，需要 fallback 到 `oss` receiver。

#### 方案

通过构造函数注入 `Configuration`，在初始化时解析 `fs.scheme-mapping.*` 配置，将 alias receiver 直接注册进 map。避免依赖 `FileSystem` 的静态状态，也避免运行时每次 fallback 查询。

```java
class SecurityTokenReceiverRepository {

    private static final Logger LOG =
            LoggerFactory.getLogger(SecurityTokenReceiverRepository.class);

    private static final String SCHEME_MAPPING_PREFIX = "fs.scheme-mapping.";

    private final Map<String, SecurityTokenReceiver> securityTokenReceivers;

    SecurityTokenReceiverRepository(Configuration config) {
        this.securityTokenReceivers = loadReceivers();
        registerAliasReceivers(config);
    }

    private void registerAliasReceivers(Configuration config) {
        for (String key : config.keySet()) {
            if (key.startsWith(SCHEME_MAPPING_PREFIX)) {
                String aliasScheme = key.substring(SCHEME_MAPPING_PREFIX.length());
                String targetScheme = config.getString(
                        ConfigBuilder.key(key).stringType().noDefaultValue(), null);
                if (targetScheme != null
                        && securityTokenReceivers.containsKey(targetScheme)
                        && !securityTokenReceivers.containsKey(aliasScheme)) {
                    securityTokenReceivers.put(aliasScheme,
                            securityTokenReceivers.get(targetScheme));
                    LOG.info("Registered token receiver alias: {} -> {}",
                             aliasScheme, targetScheme);
                }
            }
        }
    }

    // loadReceivers() 和 onNewTokensObtained() 保持不变
}
```

#### 新增 import

```java
import org.apache.fluss.config.ConfigBuilder;
import org.apache.fluss.config.Configuration;
```

### 2.4 `fluss-client/.../client/token/DefaultSecurityTokenManager.java`

#### 修改

将已有的 `configuration` 传递给 `SecurityTokenReceiverRepository` 构造函数：

```java
// 修改前：
this.securityTokenReceiverRepository = new SecurityTokenReceiverRepository();

// 修改后：
this.securityTokenReceiverRepository = new SecurityTokenReceiverRepository(configuration);
```

## 3. 不需要改的文件

| 文件 | 原因 |
|------|------|
| Server 端所有代码 | 零改动、零配置 |
| `OSSFileSystemPlugin.java` | scheme mapping 后 `create(URI("ossj://..."))` 被调用，内部 `AliyunOSSFileSystem` 直接工作 |
| `HadoopFileSystem.java` | `toHadoopPath` 保持 static，无需感知 scheme |
| `RpcServiceBase.java` | 无关 |

## 4. 测试

### 4.1 `FileSystemTest.java`

新增 2 个测试用例：

```java
@Test
void testSchemeMapping() throws Exception {
    Configuration config = new Configuration();
    config.setString("fs.scheme-mapping.test-alias", "file");
    FileSystem.initialize(config, null);

    // test-alias:///path should resolve to LocalFileSystem via mapping
    FileSystem fs = FileSystem.getUnguardedFileSystem(new URI("test-alias:///tmp/test"));
    assertThat(fs).isInstanceOf(LocalFileSystem.class);
}

@Test
void testSchemeMappingDoesNotOverrideExistingPlugin() throws Exception {
    Configuration config = new Configuration();
    config.setString("fs.scheme-mapping.file", "some-other");
    FileSystem.initialize(config, null);

    // "file" scheme still uses LocalFileSystem (not overridden)
    FileSystem fs = FileSystem.getUnguardedFileSystem(new URI("file:///tmp/test"));
    assertThat(fs).isInstanceOf(LocalFileSystem.class);
}
```

### 4.2 `SecurityTokenReceiverRepositoryTest.java`

新增测试：构造 `SecurityTokenReceiverRepository` 时传入含 `fs.scheme-mapping.ossj = oss` 的 Configuration，验证 `ossj` scheme 的 token 能正确路由到 `oss` 对应的 receiver。

## 5. 使用方式

**Client B（无 ossj 插件的客户端）：**

```properties
client.fs.scheme-mapping.ossj = oss
```

**Client A（有 ossj 插件的客户端）：**

无需任何配置，SPI 注册的 `ossj` plugin 优先生效。
