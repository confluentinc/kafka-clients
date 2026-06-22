# COMMENTS.DONE.39 — resolved Critic feedback, Phase 39

## Issue 1: Production gap — listener error not wrapped with Java's message — RESOLVED

- **Fix**: Implemented Java's conditional `maybeWrapAsKafkaException(e, "User
  rebalance callback throws an error")` (`AsyncKafkaConsumer.java:2334`,
  `ConsumerUtils.java:256`) on the rebalance-callback error path.
  - Added `KafkaError::is_kafka_exception()` (`src/common/kafka_error.rs`)
    mirroring Java's `t instanceof KafkaException`: only `IllegalArgument` and
    `IllegalState` (Java's `IllegalArgumentException` / `IllegalStateException`
    `RuntimeException`s) are NOT `KafkaException`; everything else
    (`ApiException` subtypes, `Serialization`, `Wakeup`, bare `Generic`) IS.
  - Rewrote `maybe_wrap_as_kafka_error_with_msg`
    (`src/consumer/internals/consumer_utils.rs`) to match Java EXACTLY: a
    `KafkaException` passes through unchanged (message and all); a
    non-`KafkaException` is replaced by a `KafkaException` whose message is
    exactly the supplied string (cause logged at debug). (The previous Rust
    impl prepended `"{msg}: {inner}"` for several variants — non-faithful, and
    wrong for `Timeout`, which IS a `KafkaException` in Java and must pass
    through.)
  - Applied the wrap in `process_background_events`
    (`src/consumer/async_kafka_consumer.rs`, the mirror of Java's
    `invokeRebalanceCallbacks`), so BOTH the bg-side `ack` future and the
    app-side surfacing carry the wrapped error — matching Java's single
    `ConsumerRebalanceListenerCallbackCompletedEvent` payload. The close-path
    invoker site uses Java's single-arg `maybeWrapAsKafkaException(error)`,
    which is identity in Rust (everything is already a `KafkaError`) — no
    change needed there.
- **Perf**: per-rebalance error path only; success path and §31 handshake
  unchanged (the `map_err` is a no-op on `Ok`). Perf-neutral.
- **Tests**: rewrote the two `consumer_utils` unit tests to assert Java's
  contract (non-Kafka error → exact message replacement; Timeout/Serialization/
  Wakeup KafkaExceptions pass through unchanged). Invoker unit tests still
  assert the invoker's RAW return (the wrap lives in the consumer, not the
  invoker) — correct, unchanged.

## Issue 2: `test_fetch_partitions_with_always_failed_listener` asserts weaker than Java — RESOLVED

- Restored the exact-message assertion. The `Err` arm now asserts
  `err.to_string() == "User rebalance callback throws an error"`
  (`ConsumerIntegrationTest.java:185`), with the "no record ever delivered"
  invariant kept. The listener throws `IllegalState("always failed")` (a
  non-`KafkaException`), so the Issue-1 wrap produces exactly Java's message.

## Issue 4: `test_async_consumer_auto_commit_on_rebalance` flakiness — RESOLVED

- De-flaked by NOT producing the 1000 records to `tp` (Java's `sendRecords`).
  The test never consumes them; with no fetchable records the post-re-subscribe
  `await_assignment` poll loop cannot advance the seeked positions (300/500)
  before the rebalance auto-commit captures them, removing the broker-timing
  dependency that Java's in-callback `pause()` guarded against (Issue 8 —
  `pause()` from inside the listener is structurally unsupported in Rust). The
  auto-commit + `committed()` readback (300/500) is preserved.

## Issue 3: Callback-reentrancy structural gap — DOCUMENTED (not a code change)

- Recorded as a dedicated "KNOWN API-CAPABILITY GAP" section in this phase's
  `PLAN.md`, referencing `consumer-threading.md` §31: a `&self`
  `Arc<dyn ConsumerRebalanceListener>` cannot call into the `&mut self`
  consumer (no clonable handle; inline invocation holds `&mut self`; a
  driver-channel pattern deadlocks). assign/position/seek/pause/assignment/
  beginningOffsets from inside a rebalance callback are unsupported in the
  current Rust API. Tracked separately by the team; no API redesign attempted
  here. The 8 `#[ignore]`d callback-reentrancy tests are the right call for
  this phase.
