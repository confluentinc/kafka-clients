# Critic 46 — Phase M6 (AsyncConsumerMetrics) — RESOLVED

Reviewed commits 6df99e4, 0cc48c4, 2e23a47, 67805b6 against the Java contract
(`AsyncConsumerMetrics.java`, `AsyncConsumerMetricsTest.java`,
`ConsumerNetworkThreadTest.java`, `Metrics.java`, `NetworkClientDelegate.java`,
`ApplicationEventHandler.java`, `BackgroundEventHandler.java`,
`AsyncKafkaConsumer.java`).

Both issues addressed as fixups referencing the M6 commits.

---

## Issue 1: Two end-to-end `ConsumerNetworkThread` metric tests not translated; deferral comment is stale and the defer rationale is wrong

- **File**: `src/consumer/internals/consumer_network_thread.rs:1906-1911`
- **Severity**: Missing Requirement (DoD #3 — Java test not translated without a valid skip rationale)
- **Java Reference**: `ConsumerNetworkThreadTest.java:210-243` (`testRunOnceRecordTimeBetweenNetworkThreadPoll`) and `:245-288` (`testRunOnceRecordApplicationEventQueueSizeAndApplicationEventQueueTime`)
- **Description**: The deferral comment still claimed `AsyncConsumerMetrics` "is
  not yet translated" and that the bg-task emits `log::trace!` placeholders. After
  M6 this is false: `AsyncConsumerMetrics` *is* translated and `run_once` calls the
  real record methods. Public `metrics()` is not required (Java reads via
  `metrics.metric(metrics.metricName(...))` against a directly-constructed
  `Metrics`); `MockTime` already exists in the test module.

### Resolution

- Translated both Java tests into the `consumer_network_thread.rs` test module:
  - `run_once_records_time_between_network_thread_poll` — two `run_once`
    iterations 10ms apart on `MockTime`; asserts
    `time-between-network-thread-poll-{avg,max} == 10`. First iteration is skipped
    by Java's `lastPollTimeMs == 0` guard.
  - `run_once_records_application_event_queue_size_and_time` — enqueues one
    `AsyncPoll` event stamped at the current mock time, pre-bumps the size gauge to
    1, sleeps 10ms, runs one iteration; asserts `application-event-queue-size == 0`
    (drain reset) and `application-event-queue-time-{avg,max} == 10`.
- Both are `@ParameterizedTest`-faithful: looped over
  `[CONSUMER_METRIC_GROUP, CONSUMER_SHARE_METRIC_GROUP]` (Java's `groupNameProvider`).
- Metrics read directly from a locally-constructed `Arc<Metrics>` via
  `metrics.metric(metrics.metric_name_group(...))` — no public accessor added.
- Wired via the existing `set_async_consumer_metrics(...)` setter on the
  `make_thread_no_membership` fixture.
- Replaced the stale "not yet translated / log::trace!" comment with a pointer to
  the two new tests.

---

## Issue 2: `background-event-queue-size` is not reset to 0 on an empty drain, diverging from Java

- **File**: `src/consumer/async_kafka_consumer.rs:2323-2328`
- **Severity**: Behavior Mismatch (low — metric-value timing only)
- **Java Reference**: `BackgroundEventHandler.java:65-70` (`drainEvents`)
- **Description**: Java's `drainEvents()` records
  `recordBackgroundEventQueueSize(0)` UNCONDITIONALLY on every invocation (no
  `isEmpty()` early-return), so the gauge is continuously refreshed to 0 while
  idle. The Rust path only recorded 0 on the first drained event (`if !had_events`
  guard), so on an empty drain the gauge lingered at the last post-`add` peak.
  The application-event side is correct (Java's `processApplicationEvents` *does*
  early-return on empty), so only the background side needed the fix.

### Resolution

- Moved the `background_event_queue_size.store(0, SeqCst)` +
  `record_background_event_queue_size(0)` pair out of the `if !had_events` guard to
  the top of `process_background_events`, so it runs once per invocation regardless
  of whether any event was drained — matching `BackgroundEventHandler.drainEvents`.
  The app-event side was left as-is (already faithful to Java's early-return).
- The `record_background_event_queue_processing_time` call retains its `if had_events`
  guard (faithful to Java's `if (!events.isEmpty())` in `processBackgroundEvents`).
- Added `idle_drain_resets_background_event_queue_size_to_zero`: pre-seeds the gauge
  to a stale peak of 2, drains an empty channel, asserts both the shared `AtomicI64`
  and the registered `background-event-queue-size` metric read 0.

---

## Verification

- `cargo build` — clean.
- `cargo test --lib` — 2116 passed, 0 failed (3 new tests).
- `cargo xtask format-check` — clean.
- `cargo xtask lint` — no issues.
