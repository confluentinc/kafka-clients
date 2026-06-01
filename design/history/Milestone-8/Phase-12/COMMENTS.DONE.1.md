# Phase 12 Review — Resolved Comments

## Issue 3: `max_time_to_wait_ms` slot in `AsyncKafkaConsumer` is disconnected from the bg-task's `cached_max_time_to_wait_ms` — accessor returns 0 forever

**Commit**: `d3e9e15` (Phase 12 commit 3/N)
**File**: `src/consumer/async_kafka_consumer.rs:988` (creates fresh `Arc<AtomicI64>`), `src/consumer/internals/consumer_network_thread.rs:201, 252, 513` (bg-task creates its own `Arc<AtomicI64>` in `Self::new`).
**Severity**: **blocking** (called out by Actor — confirming it as a real bug)
**Java reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ApplicationEventHandler.java` — `maximumTimeToWait()` reads from a shared `AtomicLong` updated by the bg thread.

### Description

The production ctor creates `let max_time_to_wait_ms: Arc<AtomicI64> = Arc::new(AtomicI64::new(0));` (line 988) and stuffs it into `AsyncKafkaConsumerComponents`. The consumer's accessor `maximum_time_to_wait_ms()` (line 1324) reads from this slot.

Meanwhile, `ConsumerNetworkThread::new` (consumer_network_thread.rs:252) creates its OWN `cached_max_time_to_wait_ms: Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))` and writes to it after every `run_once` iteration (line 513). The two `Arc<AtomicI64>` instances point to different `AtomicI64` cells.

Result: the app-side `maximum_time_to_wait_ms()` accessor always returns the initial `0` (and even if it weren't 0, it would never observe bg-task updates).

### Resolution

Fixed by making `ConsumerNetworkThread::new` accept an `Arc<AtomicI64>` parameter (`cached_max_time_to_wait_ms`) — the production ctor in `AsyncKafkaConsumer::new` now constructs `Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))` and passes the SAME Arc into both `ConsumerNetworkThread::new` AND the consumer struct (via `AsyncKafkaConsumerComponents.max_time_to_wait_ms`). The bg ctor seeds the slot with `MAX_POLL_TIMEOUT_MS` (Java's `Long.MAX_VALUE` default is dropped to the safer "wake at least this often" bound — explicit comment in the ctor). All five Phase-10 test callsites updated to pass `Arc::new(AtomicI64::new(MAX_POLL_TIMEOUT_MS))`.

Fixup commit references `d3e9e15`.
