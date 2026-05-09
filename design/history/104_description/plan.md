# Translation Plan: KAFKA-19249 — Replace Consumer#close(Duration) with Consumer#close(CloseOptions)

**AK commit:** `2dffe32c2a36dc40e0fbcec3d2438275b4f268be`
**AK branch:** trunk
**PR:** #104
**Rust branch:** `kafka-translate/2dffe32c2a36dc40e0fbcec3d2438275b4f268be`

---

## Summary of the Apache Kafka Commit

This is a **deprecation cleanup** commit. It replaces calls to the deprecated
`Consumer#close(Duration)` method (deprecated since Kafka 4.1) with the newer
`Consumer#close(CloseOptions)` API across both production and test code.

**Changed files:**

| File | Type |
|------|------|
| `clients/src/main/java/org/apache/kafka/clients/consumer/MockConsumer.java` | Production (test utility) |
| `core/src/test/scala/integration/kafka/api/IntegrationTestHarness.scala` | Test |
| `core/src/test/scala/integration/kafka/server/DynamicBrokerReconfigurationTest.scala` | Test |
| `storage/src/main/java/org/apache/kafka/server/log/remote/metadata/storage/ConsumerTask.java` | Production (server) |

**Nature of the change:**
- `consumer.close(Duration.ZERO)` becomes `consumer.close(CloseOptions.timeout(Duration.ZERO))`
- `consumer.close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS))` becomes
  `consumer.close(CloseOptions.timeout(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS)))`
- `consumer.close(Duration.ofSeconds(30))` becomes
  `consumer.close(CloseOptions.timeout(Duration.ofSeconds(30)))`
- The `@SuppressWarnings("deprecation")` annotation is removed from `ConsumerTask.closeConsumer()`.

---

## Rust Translation Analysis

### Does the consumer exist in Rust?

**No.** The Rust codebase does not currently have a consumer implementation.
Searching the `src/` directory reveals no consumer module, no `Consumer` trait,
no `MockConsumer`, and no `CloseOptions` type. The only consumer-related code
consists of:

- Protocol message types generated from JSON specs (request/response schemas)
- References to "consumer" in comments/docs within `common/internals/topic.rs`
  and `common/protocol/errors.rs`

The Rust client currently implements:
- Network client (`network_client.rs`)
- Metadata management
- Producer (`src/producer/`)
- Common infrastructure (serialization, config, security, protocol)

### Is there production code to translate?

**No.** None of the four files modified in this commit have Rust equivalents:

1. `MockConsumer.java` — No Rust `MockConsumer` exists.
2. `IntegrationTestHarness.scala` — No Rust integration test harness uses a consumer.
3. `DynamicBrokerReconfigurationTest.scala` — Server-side test, not applicable.
4. `ConsumerTask.java` — Server-side remote storage component, not applicable.

### What needs to be done?

**Nothing.** This commit is a **no-op** for the Rust translation because:

1. The consumer client is not yet implemented in Rust.
2. When the consumer is eventually implemented, it should use a `CloseOptions`
   pattern from the start (i.e., never introduce the deprecated `close(Duration)`
   signature).
3. The server-side files (`ConsumerTask`, integration test harness) are out of
   scope for a client library translation.

---

## Implementation Plan

### Phase 1 — No code changes required

This commit requires no Rust code changes. The translation is recorded as a
**description-only** entry to maintain commit history traceability.

---

## Design Note for Future Consumer Implementation

When the Rust consumer is eventually implemented, the `close` method should
accept a `CloseOptions` struct rather than a bare `Duration`:

```rust
/// Options for closing a consumer.
pub struct CloseOptions {
    /// Maximum time to wait for the consumer to close gracefully.
    pub timeout: Duration,
}

impl CloseOptions {
    pub fn timeout(timeout: Duration) -> Self {
        Self { timeout }
    }
}

pub trait Consumer {
    // ... other methods ...
    fn close(&self, options: CloseOptions) -> Result<(), KafkaError>;
}
```

This avoids ever introducing and then deprecating a `close(Duration)` API,
aligning with the current Java API direction from the start.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| (none) | — | No Rust code changes needed |

---

## Out of Scope

- Implementing the Rust consumer client.
- Implementing `CloseOptions` (deferred until consumer work begins).
- Server-side components (`ConsumerTask`, test harnesses).

---

## Definition of Done

- [x] Design document written and committed.
- [x] No code changes required — confirmed by analysis.
