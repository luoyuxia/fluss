# FIP-28 Historical Partition Lookup PR 5 实施计划

## 目标

PR 5 为 historical lookup 增加第一版服务端流控，避免大量慢 lake lookup 无限制进入 `ioExecutor` 并影响 normal lookup 的服务端资源。

该 PR 合并后，historical lookup 在提交 lake lookup 前会经过一个独立的 server-side semaphore。超过上限时，server 返回明确的 throttle error；client 将该错误作为可重试错误处理，并按最小必要方式做 retry/backoff。

## 前置依赖

该计划默认 PR 4 已经完成并合入：

- Client 可以把 expired original partition lookup 路由到 `__historical__` partition。
- Lookup RPC 已携带 `PbLookupReqForBucket.partition_name`。
- `TabletService.lookup(...)` 已能识别 historical lookup request。
- `ReplicaManager.historicalLookups(...)` 已把 historical lookup 转给 `HistoricalLakeLookupManager`。
- `HistoricalLakeLookupManager` 已通过 `ioExecutor` 异步访问 lake table lookuper。

## 非目标

PR 5 第一版不做以下事情：

- 不增加 `client.lookup.historical-inflight-ratio`。
- 不拆分 `LookupSender` 的 normal/historical inflight permits。
- 不增加 dedicated historical lookup metrics。
- 不支持 historical write。
- 不改变 PR 4 的 historical lookup eligibility 规则。
- 不补 original partition existence check。
- 不改 normal lookup 的请求路径和本地 KV lookup 语义。

## 当前代码状态

Server 侧：

- `TabletService.lookup(...)` 通过 `hasHistoricalLookup(request)` 将携带 `partition_name` 的 lookup request 路由到 historical path。
- `ReplicaManager.historicalLookups(...)` 对每个 `LookupDataForBucket` 获取 replica，然后调用 `HistoricalLakeLookupManager.lookup(...)`。
- `HistoricalLakeLookupManager.lookup(...)` 当前直接 `CompletableFuture.supplyAsync(..., ioExecutor)`，没有 admission control。
- `ConfigOptions.NETTY_SERVER_MAX_QUEUED_REQUESTS` 已定义 `netty.server.max-queued-requests`，但 historical lookup 没有单独上限。

Client 侧：

- `LookupSender.handleLookupError(...)` 已有统一 retry 入口。
- 当前 retry 条件是 `exception instanceof RetriableException`。
- 当前 lookup retry 是立即 re-enqueue；如果要满足 throttle backoff，需要补一个很小的 delayed retry 机制。

## Step 1: 增加服务端 historical queued request 配置

修改：

- `fluss-common/src/main/java/org/apache/fluss/config/ConfigOptions.java`

新增配置：

```java
public static final ConfigOption<Integer> NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS =
        key("netty.server.max-queued-historical-requests")
                .intType()
                .defaultValue(50)
                .withDescription(
                        "The number of historical lookup requests allowed to wait for lake lookup "
                                + "processing before throttling them.");
```

实现要求：

- 放在 `NETTY_SERVER_MAX_QUEUED_REQUESTS` 附近，保持 netty server 配置分组。
- 默认值先保守设为 50。
- 在使用点校验该值必须大于 0。
- 不通过 `netty.server.max-queued-requests` ratio 推导 historical 容量。

测试：

- 增加配置读取或构造层测试，确认显式配置会覆盖默认值。
- 如果项目没有单独的 `ConfigOptions` 默认值测试，可以在 flow-control 单测中覆盖。

## Step 2: 增加明确的 historical lookup throttle error

目标：

- Server 返回明确 bucket-level `ApiError`。
- Client 能通过现有 retry 判断识别为 retriable。

推荐新增异常：

- `fluss-common/src/main/java/org/apache/fluss/exception/HistoricalLookupThrottledException.java`

```java
public class HistoricalLookupThrottledException extends RetriableException {
    public HistoricalLookupThrottledException(String message) {
        super(message);
    }
}
```

修改：

- `fluss-rpc/src/main/java/org/apache/fluss/rpc/protocol/Errors.java`

新增 error code：

```java
HISTORICAL_LOOKUP_THROTTLED(
        71,
        "Historical lookup is throttled because too many historical lookup requests are in flight.",
        HistoricalLookupThrottledException::new)
```

实现要求：

- error code 使用当前最大 code 后的下一个值。
- 新异常继承 `RetriableException`，这样 `LookupSender.canRetry(...)` 能先接入现有 retry 逻辑。
- 不需要 proto 变更；bucket response 里的 error code/message 仍走现有 `ErrorMessage`。

测试：

- `ApiErrorTest` 或 `Errors` 相关测试覆盖：
  - `ApiError.fromThrowable(new HistoricalLookupThrottledException(...))` 映射到新 error。
  - `ApiError.fromErrorMessage(...)` 能还原为同一 error。

## Step 3: 在 HistoricalLakeLookupManager 增加 admission control

推荐放置点：

- `fluss-server/src/main/java/org/apache/fluss/server/replica/HistoricalLakeLookupManager.java`

原因：

- 该类已经是 lake lookup 的唯一入口。
- admission 放这里可以保证只保护 historical lake lookup，不影响 normal lookup。
- `ReplicaManager` 仍负责找 replica 和组装 bucket result。

新增字段：

```java
private final Semaphore lookupPermits;
```

构造逻辑：

1. 从 `conf.get(ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS)` 读取容量。
2. 用 `checkArgument(maxQueuedHistoricalRequests > 0, ...)` 做配置校验。
3. 初始化 `new Semaphore(maxQueuedHistoricalRequests)`。

修改 `lookup(...)` 流程：

```java
CompletableFuture<LookupResultForBucket> lookup(
        LookupDataForBucket lookupData, TableInfo tableInfo) {
    TableBucket tableBucket = lookupData.tableBucket();
    if (!lookupPermits.tryAcquire()) {
        return CompletableFuture.completedFuture(
                new LookupResultForBucket(
                        tableBucket,
                        ApiError.fromThrowable(
                                new HistoricalLookupThrottledException(
                                        "Historical lookup is throttled for " + tableBucket))));
    }

    CompletableFuture<LookupResultForBucket> future =
            CompletableFuture.supplyAsync(() -> lookupInternal(lookupData, tableInfo), ioExecutor);
    future.whenComplete((ignored, error) -> lookupPermits.release());
    return future;
}
```

实现要求：

- `tryAcquire()` 失败时不能提交到 `ioExecutor`。
- 成功 acquire 后，无论 lookup 成功、失败还是 future 异常完成，都必须 release。
- 不要在 validation 失败前提前 release；用 `whenComplete` 覆盖所有 async 完成路径。
- `close()` 不需要额外 release；未完成的 future 完成时仍会 release。
- 第一版 permit 粒度按 bucket request 计算，即每个 `LookupDataForBucket` 占一个 permit。

## Step 4: 确认 ReplicaManager 和 TabletService 的错误传播

涉及类：

- `fluss-server/src/main/java/org/apache/fluss/server/replica/ReplicaManager.java`
- `fluss-server/src/main/java/org/apache/fluss/server/tablet/TabletService.java`

检查点：

- `HistoricalLakeLookupManager.lookup(...)` 返回 throttled `LookupResultForBucket` 后，`ReplicaManager.historicalLookups(...)` 应原样放入 result map。
- `TabletService.lookup(...)` 最终通过 `makeLookupResponse(...)` 写入 bucket-level error。
- mixed normal/historical request 仍按 PR 4 的 request-level 规则处理，不在 PR 5 改语义。

预期不需要大改：

- `ReplicaManager.historicalLookups(...)` 当前已对 completed future result 和 exceptional completion 做 bucket-level result 汇总。
- `TabletService.lookup(...)` 当前已经把 historical result 与 authorization error map 合并。

## Step 5: Client 侧接入 historical throttle retry/backoff

第一阶段：

- 因为 `HistoricalLookupThrottledException` 继承 `RetriableException`，`LookupSender.canRetry(...)` 会先把它作为可重试错误。
- 如果只要求 bounded retry，现有 `lookup.incrementRetries()` + `reEnqueueLookup(lookup)` 已可工作。

第二阶段，补最小 backoff：

- 当前 `LookupSender` 的 retry 是立即 re-enqueue，不是真正 backoff。
- 为避免 throttle 后立即打满 server，可以只对 `HistoricalLookupThrottledException` 增加 delayed retry。

推荐改法：

1. 在 `AbstractLookupQuery` 增加：

   ```java
   private long nextRetryTimeMs;

   public long nextRetryTimeMs() { return nextRetryTimeMs; }

   public void setNextRetryTimeMs(long nextRetryTimeMs) {
       this.nextRetryTimeMs = nextRetryTimeMs;
   }
   ```

2. 在 `LookupSender` 中增加 private `ExponentialBackoff historicalThrottleBackoff`：

   ```java
   new ExponentialBackoff(100L, 2, 5000L, 0.2)
   ```

3. 在 `handleLookupError(...)` 中：

   - 如果 `error.exception() instanceof HistoricalLookupThrottledException`：
     - `lookup.incrementRetries()`
     - 计算 backoff delay。
     - 设置 `lookup.setNextRetryTimeMs(System.currentTimeMillis() + delayMs)`。
     - `reEnqueueLookup(lookup)`。
   - 其他 retriable error 保持现有逻辑。

4. 在 `LookupQueue.drain()` 处理 `reEnqueuedLookupQueue` 时：

   - 如果 poll 到的 lookup 还没到 `nextRetryTimeMs`，把它放回 re-enqueued queue，并继续尝试从普通 `lookupQueue` drain。
   - 避免 sender thread 为 historical retry 睡眠，normal lookup 不能被 delayed historical lookup 阻塞。

实现注意：

- 不新增 client 配置。
- 不拆分 inflight semaphore。
- 不把 backoff 逻辑应用到 normal retriable lookup，避免改变现有 retry 行为。
- `maxRetries` 仍使用 `CLIENT_LOOKUP_MAX_RETRIES`。

## Step 6: Unit Tests

### Server admission tests

推荐新增或扩展：

- `fluss-server/src/test/java/org/apache/fluss/server/replica/HistoricalLakeLookupManagerTest.java`
- 或现有 `ReplicaManager` / `TabletService` historical lookup 测试。

覆盖 case：

1. `testHistoricalLookupThrottledWhenPermitsExhausted`
   - 配置 `netty.server.max-queued-historical-requests = 1`。
   - 用阻塞 lake lookuper 占住第一个 permit。
   - 第二个 historical lookup 返回 `HISTORICAL_LOOKUP_THROTTLED`。
   - 验证第二个请求没有进入 `ioExecutor` 或 lake lookuper。

2. `testHistoricalLookupReleasesPermitOnSuccess`
   - 第一次 lookup 成功后释放 permit。
   - 第二次 lookup 可以继续进入 lake lookup。

3. `testHistoricalLookupReleasesPermitOnFailure`
   - lake lookuper 抛异常。
   - permit 被释放，后续 lookup 不会被误 throttle。

4. `testNormalLookupDoesNotUseHistoricalPermits`
   - 占满 historical permits。
   - normal lookup 仍走 `ReplicaManager.lookups(...)` 并成功。

5. `testHistoricalLookupMaxQueuedRequestsUsesExplicitConfig`
   - 显式配置为 2 时，可以同时接纳两个 historical bucket requests。
   - 第三个才被 throttle。

### Client retry/backoff tests

推荐扩展：

- `fluss-client/src/test/java/org/apache/fluss/client/lookup/LookupSenderTest.java`

覆盖 case：

1. `testHistoricalLookupThrottleIsRetried`
   - gateway 第一次返回 `HISTORICAL_LOOKUP_THROTTLED`。
   - 第二次返回成功。
   - future 最终成功，`query.retries()` 为 1。

2. `testHistoricalLookupThrottleBackoffDoesNotBlockNormalLookup`
   - historical query 收到 throttle 并设置 delayed retry。
   - 同时提交 normal query。
   - normal query 不等待 historical backoff，到达后先完成。

3. `testHistoricalLookupThrottleFailsAfterMaxRetries`
   - gateway 一直返回 throttle。
   - 超过 `CLIENT_LOOKUP_MAX_RETRIES` 后 future exceptionally complete。

如果第一版只接入现有立即 retry，不补 delayed retry，则只保留第 1 和第 3 个测试，并在 PR 描述中明确 backoff 未实现。

## Step 7: Integration/Regression Tests

推荐在 PR4 的 E2E 基础上增加 focused case，而不是扩大完整 IT 覆盖：

- `HistoricalPartitionLookupITCase` 可增加一个小 case：
  - `netty.server.max-queued-historical-requests = 1`
  - 构造两个并发 historical lookups。
  - 一个被接纳，另一个可能 throttle 后由 client retry。
  - 最终两个 lookup 都返回正确结果。

如果该 IT 难以稳定制造 throttle，可以只做 server/client unit tests，不强求 E2E throttle。

## 验证命令

Focused tests：

```bash
./mvnw test -pl fluss-common -Dtest=ApiErrorTest
./mvnw test -pl fluss-client -Dtest=LookupSenderTest
./mvnw test -pl fluss-server -Dtest='*Historical*Lookup*Test'
```

受影响模块：

```bash
./mvnw verify -pl fluss-common,fluss-rpc,fluss-client,fluss-server
./mvnw spotless:check
```

如果同时调整了 Paimon E2E：

```bash
./mvnw -pl fluss-lake/fluss-lake-paimon -am -Dtest=HistoricalPartitionLookupITCase -DfailIfNoTests=false test
```

## 风险和注意点

- `LookupSender` 当前 retry 没有 delay。如果 PR 要严格满足 backoff，需要补 delayed re-enqueue；不要让 sender thread sleep，否则 normal lookup 会被 historical throttle 影响。
- `netty.server.max-queued-historical-requests` 名称是 request 语义，但第一版按 bucket request 计 permit。实现和文档需要保持一致，避免 reviewer 误解为按整包 `LookupRequest` 计数。
- Throttle error 必须是 retriable，否则 client 会直接失败。
- Permit release 必须覆盖 success、bucket-level failure、unexpected exceptional completion。
- 不新增 metrics，避免 PR5 范围扩大。

## 自审

- 范围符合 PR5 第一版：server admission + throttle retry/backoff。
- 没有重新引入 `client.lookup.historical-inflight-ratio`。
- 没有把 metrics 放回 PR5。
- 配置命名与现有 `netty.server.max-queued-requests` 保持一致。
- 测试覆盖了 throttle、release、normal lookup 不受影响和 client retry。
