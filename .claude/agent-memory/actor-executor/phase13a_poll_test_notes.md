---
name: phase13a-poll-test-notes
description: Phase 13a suite 4 PlaintextConsumerPollTest — 3/8 pass, Issues 8 and 9 filed (listener-can't-call-consumer, GroupIdNotFound before heartbeat, poll(ZERO) needs non-zero timeout)
metadata:
  type: project
---

Phase 13a suite 4 (`PlaintextConsumerPollTest`) translation outcome:

- 8 translated of 10 CONSUMER-arm tests (2 SKIPped for `consumer.metrics()` deferral); 4 multi-consumer SKIPped for harness deferral; 10 classic twins SKIPped.
- 3 pass: `max_poll_records`, `no_offset_for_partition_exception_on_poll_zero`, `poll_eventually_returns_records_with_zero_timeout`.
- 5 `#[ignore]`-gated on 2 newly-filed production issues:

## Issue 8 — listener can't call back into consumer
Structural Rust-vs-Java gap. `Box<dyn Consumer<K,V>>` is exclusively owned by the test driver task; listener is `Arc<dyn ConsumerRebalanceListener>` invoked on the same task synchronously inline during `poll()`. Java's pattern (listener captures `consumer` reference, calls `consumer.position()` + `consumer.commitSync()` from inside `onPartitionsRevoked`) is unrepresentable without a trait redesign — channel-based handshake deadlocks because the driver is blocked inside `poll()` awaiting the listener's future.

**How to apply:** When translating tests that have a listener calling back into the consumer (`commitSync` / `position` / `assignment` etc.), `#[ignore]` with Issue 8 rationale. Estimated fix is 50-100 LoC trait signature change.

## Issue 9 — GroupIdNotFound + poll-timer-start-point
Two related gaps surfaced together:
1. OffsetFetch dispatched before first heartbeat lands → `GroupIdNotFound` (broker hasn't created group yet). Affects subscribe path consumers.
2. `AbstractHeartbeatRequestManager.poll_timer_expires_at_ms` is armed at consumer construction, not first `poll()`. With `max.poll.interval.ms=1000`, consumer fences itself during the FIRST rebalance window.

Affected: tests with `max.poll.interval.ms=1000` + subscribe (`max_poll_interval_ms`, `max_poll_interval_ms_delay_in_assignment`, `max_poll_interval_ms_shorter_than_poll_timeout`, `recovery_on_poll_after_delayed_rebalance`).

**How to apply:** Fix is ~50-80 LoC, same shape as Issue 1's fix (retry classification on `GroupIdNotFound` in commit_request_manager).

## Translation patterns learned

### `poll(Duration::ZERO)` in Rust does NOT loop internally
Java's `poll(Duration.ZERO)` returns immediately but the JVM scheduler runs the bg thread concurrently. Rust's `poll(Duration::ZERO)` calls `check_inflight_poll → poll_for_fetches` once and exits because the inner loop is gated on `time.milliseconds() < poll_deadline_ms` (immediately false).

**How to apply:** Test patterns that need consumer state changes (OffsetFetch, position resolution) must use `poll(Duration::from_millis(50))` or similar, NOT `Duration::ZERO`. Add a translation-deviation rustdoc comment when changing from Java's `Duration.ZERO`.

### Rebalance listener trait shape
- `Arc<dyn ConsumerRebalanceListener>` is shared between subscribe-time installer and the bg-side invoker.
- Listener method `&self` plus `async_trait`. `Send + Sync + 'static` is the trait bound.
- Java's anonymous inner class with override pattern → Rust struct + impl.
- Counters: `Arc<AtomicUsize>` cloned into the listener and into the test handle.
- Sleep in listener via `tokio::time::sleep` — listener runs on caller's task per §31, so sleep is fine.

### Cluster config differences create separate cluster instances
Adding `KAFKA_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS=500` etc. to my poll test caused the cluster pool to materialize a separate 3-broker cluster (cluster_config.rs hash includes all server_properties). For consistent test isolation this is desirable, but it means the first test on the new cluster has cold-start overhead (~30s).
