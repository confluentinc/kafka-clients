---
name: Phase 7f — KafkaProducerTest translations
description: ~24 Java tests landed; metadata.close production gap surfaced; Drop-as-CLOSE_COUNT pattern; cross-module MockClient access via pub(crate) tests module
type: project
---

Phase 7f translates the non-transactional / non-metrics / non-telemetry
subset of `KafkaProducerTest.java` (~24 tests).

## `tests` module visibility hoist

`pub(super) mod tests` → `pub(crate) mod tests` in
`producer/internals/sender.rs`, plus every `pub(super)` item inside it
flipped to `pub(crate)`. This makes `MockClientImpl` reachable from
`kafka_producer.rs::tests`, unblocking full broker-loopback tests
without copying 200+ LOC of MockClient into a second file. Items stay
`#[cfg(test)]`-gated so production builds expose nothing.

This is the right pattern any time a sibling test module needs the
mock-broker shim — don't duplicate.

## Drop-tracking serializers / interceptors / partitioners

Java `MockSerializer.CLOSE_COUNT.get()` style static counters don't
translate cleanly to Rust. The equivalent: a custom impl with an
`Arc<AtomicUsize>` that fires on `Drop`:

```rust
struct DropTrackingSerializer { drops: Arc<AtomicUsize> }
impl Serializer<String> for DropTrackingSerializer { ... }
impl Drop for DropTrackingSerializer {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
}
```

Then build the producer holding the serializer in a scope, close, drop,
and assert the counter is `2` (one each for key+value). The "post-close
+ producer drop" event is the Drop point because Phase 7e's
`Utils.closeQuietly` chain is currently a no-op (every plug-in has a
no-op default `close()`).

For Arc-held interceptors (`Arc<ProducerInterceptors>`), you must `drop(interceptors)`
the test's local Arc handle AFTER the producer drop to actually fire
the inner interceptor's Drop. Without that the Arc refcount stays at
1 (the test's reference) and no Drop fires.

## metadata.close() production gap

`KafkaProducer::close` did NOT call `metadata.close()`. Java's
mechanism is transitive: `Sender.run` → `client.close()` →
`NetworkClient.DefaultMetadataUpdater.close()` → `metadata.close()`.
Our `MockClient` doesn't implement that chain.

Phase 7f added `self.metadata.close()` to both the graceful-deadline
arm AND the force-close arm of `KafkaProducer::close_inner`. Without
this fix, `testCloseWhenWaitingForMetadataUpdate` hangs (a `send` 
blocked in `wait_on_metadata.await_update` never unblocks).

Phase 8 should move this call back into the `client.close()` chain
once `DefaultMetadataUpdater` is translated — until then the producer's
close path closes metadata directly.

## Test deviations from Java

* **Headers post-send**: Java asserts `record.headers().is_read_only()`
  after `send`. Rust's `send` takes the record by value — accessing
  `record.headers()` post-send is a compile-time error. The Rust
  ownership model gives the same guarantee Java's `setReadOnly` flag
  enforces at runtime. Document the deviation in the test rustdoc.

* **Null-topic tests** (`testNullTopicName`, `testPartitionsForWithNullTopic`):
  Rust types prevent null. `ProducerRecord::new` takes
  `impl Into<Arc<str>>` (no null repr); `partitions_for` takes `&str`
  (no null repr). SKIP with rationale.

* **`closeWithNegativeTimestampShouldThrow`**: `Duration::from_millis(-100)`
  is a compile error in Rust. SKIP.

* **`testConstructorWithNotStringKey`**: Java `Properties` allows
  non-String keys; Rust `HashMap<String, String>` enforces strings at
  the type level. SKIP.

* **`testConstructorFailureCloseResource` / `testConstructorWithInvalidMetricReporterClass`**:
  Java reflective metric-reporter loading not in Milestone-1. SKIP.

* **`testInterceptorConstructorConfigurationWithExceptionShouldCloseRemainingInstances`**:
  Java reflective interceptor.classes loading not in Milestone-1.
  Rust takes pre-built `ProducerInterceptors` instances directly. SKIP.

* **Parameterized tests `(isIdempotenceEnabled, true|false)`**:
  Milestone-1 rejects `enable.idempotence=true` upstream. Translate
  the `false` case only; document.

## Skip-rationale block

Every Java `@Test` not translated gets a 1-line skip comment in the
test module's "Java tests deliberately NOT translated" block at the
end of the tests mod. Format:

```
//  * testName (line N) — short rationale
```

This makes the deferred coverage discoverable for Phase 8 / 9 reviewers.

## `MockClient` flush 50-record harness

For `testFlushCompleteSendOfInflightBatches` to actually round-trip
through the full Sender + MockClient pipeline:

1. Build a `MockClientImpl` with `prepare_response` staging 50+
   produce responses (use a generous overshoot — the accumulator may
   batch fewer requests than records).
2. Pre-populate `ProducerMetadata` with the topic + topic_id BEFORE
   `KafkaProducer::new_for_test` (move the populated metadata in via
   `metadata: Some(pm)`).
3. Use `accumulator.append(...)` directly (bypassing
   `KafkaProducer::send`) for deterministic partition assignment. The
   test goal is "flush completes 50 inflight batches" — the partition
   factory under test in OTHER tests doesn't matter here.
4. `assert!(none.is_done())` before flush, `producer.flush().await`,
   `assert!(all.is_done())` after.

The Sender drives the produce requests in its spawned task; the staged
mock responses answer immediately, and the futures resolve.

## Lint gotchas hit during Phase 7f

* `clippy::let_and_return` — extracting an async block into a `let
  binding then returning the binding triggers the lint. Inline the
  block.
* `clippy::let_underscore_future` — `let _ = tokio::spawn(...)` warns
  even when discarding intentionally. Wrap in
  `tokio::time::timeout(d, handle).await` (await the JoinHandle for
  no-leak Drop discipline).
* `KafkaProducer` is not `Debug` (owns a `JoinHandle`); `expect_err`
  needs `T: Debug`. Use `let-else`:
  ```rust
  let Err(err) = ... else { panic!("expected Err"); };
  ```

## Slow-test pitfall

Tests that exercise `partitions_for` / `wait_on_metadata` with the
default 60_000ms `max.block.ms` will spin for a full minute under
mock conditions where metadata never gets healthy. Always inject a
small `max.block.ms` (50-100ms) for tests that expect a Timeout.
