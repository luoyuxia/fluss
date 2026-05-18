# Plan: Client-Side Exponential Backoff for HistoricalPartitionThrottledException

## Context

Server-side flow control rejects historical partition requests with `HistoricalPartitionThrottledException` when the historical request queue is full (configured via `server.historical-request-queue-ratio`). This prevents historical partition requests from starving normal partition requests on the server.

However, the client currently treats this as a generic `RetriableException` and retries immediately (no backoff). This causes:
- Busy-loop: client hammers server repeatedly, all requests keep getting rejected
- Wasted network/CPU resources
- No actual throttling effect on the client side

**Fix**: Add exponential backoff with jitter when client receives `HistoricalPartitionThrottledException`. Both write path (`Sender`) and lookup path (`LookupSender`) need this.

## Key Reusable Components

| Component | File | Usage |
|-----------|------|-------|
| `ExponentialBackoff` | `fluss-common/.../utils/ExponentialBackoff.java` | Calculate backoff intervals with jitter |
| `WriteBatch.attempts()` | `fluss-client/.../write/WriteBatch.java` | Track retry count for backoff calculation |
| `AbstractLookupQuery.retries()` | `fluss-client/.../lookup/AbstractLookupQuery.java` | Track retry count for lookup backoff |
| TODO at line 733 | `RecordAccumulator.java` | Existing placeholder for backoff check in drain |

## Design

### Approach: Per-Batch Backoff in Drain Phase

When `HistoricalPartitionThrottledException` is received:
1. Calculate backoff using `ExponentialBackoff` (initial=100ms, multiplier=2, max=5s, jitter=0.2)
2. Record `retryAfterMs` timestamp on the batch/lookup
3. In drain phase, skip batches/lookups whose backoff hasn't elapsed

This approach is minimal and targeted — only affects historical partition throttle, doesn't change general retry behavior.

## Files to Modify

| # | File | Change |
|---|------|--------|
| 1 | `fluss-client/.../write/WriteBatch.java` | Add `retryAfterMs` field + setter/getter |
| 2 | `fluss-client/.../write/Sender.java` | Set backoff on batch when `HISTORICAL_PARTITION_THROTTLED` |
| 3 | `fluss-client/.../write/RecordAccumulator.java` | Skip batches in backoff at drain (line 733 TODO) |
| 4 | `fluss-client/.../lookup/AbstractLookupQuery.java` | Add `retryAfterMs` field + setter/getter |
| 5 | `fluss-client/.../lookup/LookupSender.java` | Set backoff + skip lookups in backoff when draining |

## Detailed Changes

### 1. WriteBatch.java — Add backoff tracking

```java
// New field
private volatile long retryAfterMs = 0L;

// Set by Sender when throttled
void setRetryAfterMs(long retryAfterMs) {
    this.retryAfterMs = retryAfterMs;
}

// Checked by RecordAccumulator during drain
boolean isReadyForRetry(long nowMs) {
    return nowMs >= retryAfterMs;
}
```

### 2. Sender.java — Apply backoff on throttle

In `handleWriteBatchException()`, before calling `reEnqueueBatch()`, detect the throttle case:

```java
} else if (canRetry(readyWriteBatch, error.error())) {
    // Apply exponential backoff for historical partition throttle
    if (error.error().exception() instanceof HistoricalPartitionThrottledException) {
        long backoffMs = throttleBackoff.backoff(writeBatch.attempts());
        writeBatch.setRetryAfterMs(clock.milliseconds() + backoffMs);
        LOG.info(
                "Historical partition throttled for {}, backing off {}ms",
                readyWriteBatch.tableBucket(), backoffMs);
    }
    reEnqueueBatch(readyWriteBatch);
}
```

Add `ExponentialBackoff throttleBackoff` field to `Sender`:
```java
// initialInterval=100ms, multiplier=2, maxInterval=5000ms, jitter=0.2
private final ExponentialBackoff throttleBackoff = new ExponentialBackoff(100, 2, 5000, 0.2);
```

### 3. RecordAccumulator.java — Skip batches in backoff during drain

At the TODO on line 733:
```java
// Retry backoff check — skip batch if still in backoff period
if (!first.isReadyForRetry(clock.milliseconds())) {
    continue;
}
```

Also in `batchReady()` — a batch in backoff should not make its node "ready":
```java
// In bucketReady/batchReady: treat backoff batch as not ready
if (!first.isReadyForRetry(clock.milliseconds())) {
    long backoffLeftMs = first.getRetryAfterMs() - clock.milliseconds();
    nextReadyCheckDelayMs = Math.min(nextReadyCheckDelayMs, Math.max(backoffLeftMs, 0));
    // Don't mark node as ready
    continue;
}
```

This ensures `Sender.runOnce()` wakes up at the right time via `nextReadyCheckDelayMs`.

### 4. AbstractLookupQuery.java — Add backoff tracking

```java
private long retryAfterMs = 0L;

public void setRetryAfterMs(long retryAfterMs) {
    this.retryAfterMs = retryAfterMs;
}

public long getRetryAfterMs() {
    return retryAfterMs;
}
```

### 5. LookupSender.java — Apply backoff on throttle

In `handleLookupError()`:
```java
for (AbstractLookupQuery<?> lookup : lookups) {
    if (canRetry(lookup, error.exception())) {
        lookup.incrementRetries();
        // Apply backoff for historical partition throttle
        if (error.exception() instanceof HistoricalPartitionThrottledException) {
            long backoffMs = throttleBackoff.backoff(lookup.retries());
            lookup.setRetryAfterMs(clock.milliseconds() + backoffMs);
        }
        reEnqueueLookup(lookup);
    } else {
        lookup.future().completeExceptionally(error.exception());
    }
}
```

In the drain/send phase, skip lookups still in backoff:
```java
// When processing lookups from queue, skip those in backoff
long nowMs = clock.milliseconds();
if (lookup.getRetryAfterMs() > nowMs) {
    reEnqueueLookup(lookup);  // Put back, will be retried later
    continue;
}
```

Add `ExponentialBackoff throttleBackoff` field (same config as Sender).

## Backoff Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Initial interval | 100ms | Quick first retry, historical data isn't latency-critical |
| Multiplier | 2 | Standard doubling |
| Max interval | 5000ms | Cap at 5s to avoid excessive wait |
| Jitter | 0.2 | ±20% randomization to avoid thundering herd |

Sequence: ~100ms → ~200ms → ~400ms → ~800ms → ~1600ms → ~3200ms → 5000ms (capped)

## Testing Strategy

### Write Path Test — `SenderTest`

Use `ManualClock` (from `fluss-common/.../utils/clock/ManualClock.java`) instead of `SystemClock` to control time deterministically.

**Test: `testHistoricalPartitionThrottledBackoff`**:
```java
@Test
void testHistoricalPartitionThrottledBackoff() throws Exception {
    ManualClock clock = new ManualClock(System.currentTimeMillis());
    // Setup sender with ManualClock-based accumulator
    accumulator = new RecordAccumulator(conf, idempotenceManager, writerMetricGroup, clock);
    sender = new Sender(accumulator, ...);

    // Append a KV batch to a historical partition bucket
    appendToAccumulator(historicalBucket, row(...), callback);
    sender.runOnce();  // sends request

    // Simulate HISTORICAL_PARTITION_THROTTLED response
    finishRequest(historicalBucket, 0,
        createPutKvResponse(historicalBucket, Errors.HISTORICAL_PARTITION_THROTTLED));
    sender.runOnce();  // processes error, sets backoff, re-enqueues

    // Verify batch is NOT drained immediately (still in backoff)
    sender.runOnce();
    assertThat(sender.numOfInFlightBatches(historicalBucket)).isEqualTo(0);

    // Advance clock past initial backoff (~100ms)
    clock.advanceTime(150, TimeUnit.MILLISECONDS);

    // Now batch should be drained and sent
    sender.runOnce();
    assertThat(sender.numOfInFlightBatches(historicalBucket)).isEqualTo(1);
}
```

**Key**: Uses `ManualClock` to avoid flaky time-based assertions. The test is deterministic.

### Lookup Path Test — `LookupSenderTest`

**Test: `testHistoricalPartitionThrottledLookupBackoff`**:
```java
@Test
void testHistoricalPartitionThrottledLookupBackoff() throws Exception {
    AtomicInteger attemptCount = new AtomicInteger(0);
    gateway.setLookupHandler(request -> {
        int attempt = attemptCount.incrementAndGet();
        if (attempt <= 2) {
            return createFailedResponse(request,
                new HistoricalPartitionThrottledException("queue full"));
        }
        return createSuccessResponse(request, "value".getBytes());
    });

    LookupQuery query = new LookupQuery(...);
    lookupQueue.appendLookup(query);

    // Wait for completion — backoff should introduce delay
    byte[] result = query.future().get(10, TimeUnit.SECONDS);
    assertThat(result).isEqualTo("value".getBytes());
    assertThat(attemptCount.get()).isEqualTo(3);
    assertThat(query.retries()).isEqualTo(2);
}
```

For lookup, the test verifies that retries succeed with backoff. If `ManualClock` is not easily injectable into `LookupSender`, use real time and verify completion with reasonable timeout (backoff is short: ~100ms + ~200ms = ~300ms total before 3rd attempt).

### Existing Test Patterns to Follow

| File | Pattern |
|------|---------|
| `SenderTest.java:125-168` | `testRetries()` — uses `finishRequest()` with error codes |
| `SenderTest.java:968-986` | `setupWithIdempotenceState()` — constructs accumulator + sender |
| `LookupSenderTest.java:199-225` | `testRetriableExceptionTriggersRetry()` — handler-based retry test |

## Verification

```bash
# Format
./mvnw spotless:apply -pl fluss-client -q

# Compile
./mvnw test-compile -pl fluss-client -am -q

# Run Sender tests
./mvnw test -Dtest="org.apache.fluss.client.write.SenderTest" -pl fluss-client -DfailIfNoTests=false

# Run RecordAccumulator tests
./mvnw test -Dtest="org.apache.fluss.client.write.RecordAccumulatorTest" -pl fluss-client -DfailIfNoTests=false

# Run LookupSender tests
./mvnw test -Dtest="org.apache.fluss.client.lookup.LookupSenderTest" -pl fluss-client -DfailIfNoTests=false

# Run all client tests
./mvnw test -pl fluss-client -DfailIfNoTests=false
```
