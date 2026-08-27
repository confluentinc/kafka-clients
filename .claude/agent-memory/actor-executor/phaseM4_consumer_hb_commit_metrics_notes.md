---
name: phaseM4-consumer-hb-commit-metrics
description: Milestone-9 Phase M4 — KafkaConsumerMetrics/HeartbeatMetricsManager/OffsetCommitMetricsManager + wiring; ClosureMeasurable core helper; ThreadTime.nanoseconds(); poll/commit finally-via-inner-helper; post-construction setter plumbing for bg-task RMs; all INFO Java parity
metadata:
  type: project
---

Phase M4 (Actor 44) translated the three remaining consumer metric managers
and wired them. Commit `715618f` on `consumer-impl`. Builds on M1 (core) + M2
(windowed stats) + M3 (FetchMetricsManager + shared `Arc<Metrics>`). PLAN:
`design/history/Milestone-9-metrics/Phase-M4-consumer-heartbeat-commit-metrics/PLAN.md`.

**Files (Java `consumer/internals/metrics/*` -> Rust `consumer/internals/*`,
flat like M3, NOT a `metrics/` subfolder):**
- `kafka_consumer_metrics.rs` — KafkaConsumerMetrics + KafkaConsumerMetricsTest (4).
  NOTE the Java test is at `consumer/internals/KafkaConsumerMetricsTest.java`
  (NOT under `metrics/`).
- `heartbeat_metrics_manager.rs` — + HeartbeatMetricsManagerTest (1).
- `offset_commit_metrics_manager.rs` — + OffsetCommitMetricsManagerTest (1).
- `common/metrics/measurable.rs` — added `ClosureMeasurable` (+ export in mod.rs).

**Reusable patterns:**

- **ClosureMeasurable (core addition).** M1 added `ClosureGauge` (for `Gauge`
  lambdas). These managers use `Measurable` lambdas (`(config, now) -> double`),
  so the symmetric `ClosureMeasurable` was needed. Faithful to Java's
  `Measurable` functional-interface usage. Value-neutral. `add_metric` takes
  `Box<dyn Measurable>`.

- **Shared-scalar gauge state via Arc<AtomicI64>.** Java's `lastPollMs` /
  `lastHeartbeatMs` are plain `long` fields read inside the measurable lambda
  AND written by `recordPollStart`/`recordHeartbeatSentMs`. The Rust closure
  captures `Arc<AtomicI64>`; the record method writes the same Arc. Lets the
  record methods take `&self` (manager owned by app/bg task, no &mut needed).
  init: last_poll_ms=0 (Java returns -1 gauge until first poll), last_heartbeat_ms=-1.

- **Java try/finally -> inner-helper split.** poll()/commit_sync_internal()/
  committed_timeout() each record a metric in Java's `finally` on EVERY exit
  path (including errors). Rust has early `return Err`. Pattern: outer method =
  `ensure_open` + capture start + `let result = self.xxx_inner(...).await;` +
  record-metric + `result`. Inner helper holds the Java try-body verbatim.
  - poll: recordPollStart AFTER ensure_open but BEFORE subscription check
    (Java :841); recordPollEnd in finally (:882). poll_inner holds the loop.
  - commit_sync_internal: commit_start_ns captured at top; recordCommitSync in
    finally; commit_sync_inner holds the deadline/commit/wait body.
  - committed_timeout: start_ns AFTER ensure_open, BEFORE throwIfGroupIdNotDefined
    + empty check (so recordCommitted runs even for empty/error); committed_inner.
  - close: kafka_consumer_metrics.close() after the final reaper pass,
    mirroring Java's `closeQuietly(kafkaConsumerMetrics, ...)` (:1573).

- **Post-construction setter plumbing for bg-task RM metrics (preferred over
  ctor param).** Java constructs the metrics manager INSIDE the RM ctor
  (CommitRequestManager :172, AbstractHeartbeatRequestManager param). Adding a
  `&Arc<Metrics>` ctor param would touch ~25 RM-new test call sites + force a
  cross-manager construction order. Instead used the established `set_coordinator`
  pattern: `Mutex<Option<Arc<..>>>` slot on `CommitRequestManagerInner` +
  `set_offset_commit_metrics_manager`; `Option<Arc<..>>` field on
  `AbstractHeartbeatRequestManager` + `set_metrics_manager` on the (owned, not
  Arc) ConsumerHeartbeatRequestManager (made `mut hb` in the ctor). `None` =>
  recording is a no-op (value-neutral; the unit tests construct the manager
  directly and assert on it, never via the consumer). Consumer constructs all
  three managers from the SAME `metrics` Arc right after create_fetch_metrics_manager.

- **Latency threading through the async forwarder.** Java records
  `recordRequestLatency(response.requestLatencyMs())` inside the `whenComplete`
  lambda. In Rust the forwarder spawn must capture `client_response.request_latency_ms()`
  BEFORE `take_response_body()`. Commit path: thread it as a 4th arg to
  `handle_offset_commit_response`, record at top (success path only, Java :767).
  Heartbeat path: added `request_latency_ms` field to
  `PendingHeartbeatCompletion::Response`, record in `drain_pending_completions`
  before `on_response` (normal path, :299); for the `logResponse`/ignore path
  (which drops the envelope) clone `Option<Arc<HeartbeatMetricsManager>>` into
  the forwarder and record inline (:311). recordHeartbeatSentMs goes in
  `make_heartbeat_poll_result` (the makeHeartbeatRequest(currentTimeMs,…) analog)
  AND the poll-timer-expired leave branch (which builds its own PollResult).

- **ThreadTime.nanoseconds().** The `*-time-ns-total` sensors need nanoseconds.
  `ThreadTime` had only `milliseconds()`. Added `nanoseconds()` with a default
  (`milliseconds()*1_000_000`, fine for mock clocks); `SystemThreadTime` overrides
  with real `SystemTime::now().duration_since(UNIX_EPOCH).as_nanos()`.

- **Recording levels: ALL INFO, full Java parity (no DEBUG gating).** Every
  sensor created via `metrics.sensor(name)` (INFO default) and every metric via
  `metrics.add_metric`/`sensor.add`. Same decision as M3.

- **Meter(WindowedCount) construction.** Java `new Meter(new WindowedCount(),
  rate, total)` -> `Meter::with_stat(Arc::new(WindowedCount::new().into_sampled_stat()),
  rate, total)` via `sensor.add_compound(Box::new(...))`. commit/heartbeat
  total = 3.0 after 3 records (CumulativeSum +1.0/record), rate ≈ 0.1.

- **Test value-assert.** `metrics.metric(&mn).unwrap().metric_value().as_double()`
  (MetricValue::Double -> Option<f64>). MockTime at
  `crate::common::metrics::time::mock::MockTime` (test-cfg). `Metrics::with_time`
  takes `Arc<dyn Time>`; the heartbeat test keeps `Arc<MockTime>` for
  `.milliseconds()`/`.sleep()` and casts a clone for `with_time` — needs
  `use crate::common::metrics::Time` in the test module. cfg(test)-gate the
  `MetricName` import + the test-visible MetricName struct fields (only read in tests).

- **@RepeatedTest/Random -> deterministic loop.** Java's HeartbeatMetricsManagerTest
  uses `rand.nextInt(10)+1` seconds; translated as `for random_sleep_s in 1..=10`
  so the last-heartbeat-seconds-ago assertion is deterministic over the full range.

**Consumer struct/components.** Added `kafka_consumer_metrics: Arc<KafkaConsumerMetrics>`
to both AsyncKafkaConsumerComponents AND the struct + both ctors (prod ~L911,
test new_for_test ~L4542) + new_with_components passthrough. Constructed from
the shared `metrics` Arc.

**M5/M7 handoff:** rebalance/callback managers (M5) + public `metrics()` (M7)
register against the SAME registry — no re-plumb. The `Metrics` field is still
`#[allow(dead_code)]` until M7 exposes the accessor; the managers keep the
registry populated.
