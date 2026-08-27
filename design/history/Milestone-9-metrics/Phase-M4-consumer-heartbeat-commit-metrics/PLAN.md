# Phase M4 — KafkaConsumerMetrics + Heartbeat + OffsetCommit metrics

Actor 44. Branch `consumer-impl`. Builds on M1 (metrics core), M2 (windowed
stats), M3 (FetchMetricsManager + `AsyncKafkaConsumer.metrics: Arc<Metrics>`).

## Scope (3 managers + 1 closure-measurable core helper)

Java `clients/consumer/internals/metrics/*` → Rust
`consumer::internals::*` (M3 placed the fetch metrics flat in
`consumer/internals/`, NOT in a `metrics/` subfolder; follow that convention).

1. **`KafkaConsumerMetrics`** (`kafka_consumer_metrics.rs`). Sensors/metrics:
   - `last-poll-seconds-ago` (gauge / `Measurable` closure)
   - `time-between-poll` sensor → `time-between-poll-avg` (`Avg`),
     `time-between-poll-max` (`Max`)
   - `poll-idle-ratio-avg` sensor → `poll-idle-ratio-avg` (`Avg`)
   - `commit-sync-time-ns-total` sensor → metric of same name (`CumulativeSum`)
   - `committed-time-ns-total` sensor → metric of same name (`CumulativeSum`)
   - Group: `consumer-metrics` (`CONSUMER_METRIC_GROUP`).
   - Methods: `record_poll_start`, `record_poll_end`, `record_commit_sync`,
     `record_committed`, `close` (`AutoCloseable` → explicit `close()` +
     `removeMetric`/`removeSensor`).
   - State: `last_poll_ms`, `poll_start_ms`, `time_since_last_poll_ms`
     (mutated by `record_poll_start`/`record_poll_end`). The `last-poll`
     gauge reads `last_poll_ms`, which is shared with `record_poll_start`
     → `Arc<AtomicI64>` so the closure-gauge can read it (idiomatic-Rust
     concurrency for the shared scalar, value-neutral). The manager itself
     is owned by the consumer (app task), so `record_poll_*` take `&self`
     and write through the atomic.

2. **`HeartbeatMetricsManager`** (`heartbeat_metrics_manager.rs`). Sensors:
   - `heartbeat-latency` sensor → `heartbeat-response-time-max` (`Max`) +
     `Meter(WindowedCount)` → `heartbeat-rate` / `heartbeat-total`.
   - `last-heartbeat-seconds-ago` (gauge closure reading `last_heartbeat_ms`,
     `Arc<AtomicI64>` init -1).
   - Group: `consumer-coordinator-metrics`
     (`CONSUMER_METRIC_GROUP_PREFIX + COORDINATOR_METRICS_SUFFIX`).
   - Methods: `record_heartbeat_sent_ms`, `record_request_latency`.
   - Pub(crate) `MetricName` fields (`heartbeat_response_time_max`,
     `heartbeat_rate`, `heartbeat_total`, `last_heartbeat_seconds_ago`)
     `#[cfg(test)]`-visible — the Java test reads `metrics.metric(manager.X)`.

3. **`OffsetCommitMetricsManager`** (`offset_commit_metrics_manager.rs`).
   Sensor:
   - `commit-latency` sensor → `commit-latency-avg` (`Avg`),
     `commit-latency-max` (`Max`), `Meter(WindowedCount)` → `commit-rate` /
     `commit-total`.
   - Group: `consumer-coordinator-metrics`.
   - Method: `record_request_latency`.
   - Pub(crate) `MetricName` fields (test-visible).

### Core helper (justified production addition)
`ClosureMeasurable` in `common::metrics::measurable` — a `Measurable` backed
by a closure `Fn(&MetricConfig, i64) -> f64`. Faithful translation of Java's
`Measurable` functional-interface usage (`(config, now) -> ...` lambdas in
`KafkaConsumerMetrics` / `HeartbeatMetricsManager`). M1 already added
`ClosureGauge` (for `Gauge` lambdas); this is the symmetric helper for
`Measurable` lambdas. Value-neutral, no perf cost.

## Recording levels (Java parity — verified)
All three managers create sensors via `metrics.sensor(name)`
(`Metrics.sensor` → INFO default) and metrics via `metrics.addMetric` /
`sensor.add` (INFO). **Every sensor/metric is INFO** — no DEBUG gating in
Java, none in Rust. Matches the M3 "full Java parity, all INFO" decision.

## Wiring points (file:line, frequency)

- `KafkaConsumerMetrics` — owned by `AsyncKafkaConsumer` (constructed from the
  SAME `Arc<Metrics>` as the fetch manager). Java
  `AsyncKafkaConsumer.java:404,562,609`.
  - `record_poll_start(timer.currentTimeMs())` at top of `poll()`
    (`AsyncKafkaConsumer.java:841`) — **per-poll**.
  - `record_poll_end(timer.currentTimeMs())` in `poll()`'s `finally`
    (`:882`) — **per-poll**. Rust `poll()` has early returns; record end on
    every exit path (wrap body, record-end-then-return).
  - `record_commit_sync(time.nanoseconds() - commitStart)` in
    `commit_sync_internal`'s finally-equivalent (`:1721`) — **per-commit**.
  - `record_committed(time.nanoseconds() - start)` in `committed_timeout`'s
    finally-equivalent (`:1187`) — **per-committed-call**.
  - `close()` from `AsyncKafkaConsumer::close` (`:1573`
    `closeQuietly(kafkaConsumerMetrics, ...)`) — **per-close**.

- `HeartbeatMetricsManager` — owned by `AbstractHeartbeatRequestManager`
  (bg task). Java `AbstractHeartbeatRequestManager.java:285,299,311`.
  - `record_heartbeat_sent_ms(currentTimeMs)` in
    `make_heartbeat_poll_result` (the `makeHeartbeatRequest(currentTimeMs,…)`
    analog, `:285`) AND in the poll-timer-expired leave path (Java calls the
    same `makeHeartbeatRequest(currentTimeMs,true)` helper there) —
    **per-heartbeat-send**.
  - `record_request_latency(response.requestLatencyMs())` — Java records it
    in the `whenComplete` lambda for BOTH the normal (`:299`) and
    `logResponse`/ignore (`:311`) paths. Rust: capture
    `client_response.request_latency_ms()` in the forwarder spawn, thread it
    through `PendingHeartbeatCompletion::Response { latency_ms }`, record in
    `drain_pending_completions` (normal path). The ignore_response path drops
    the envelope, so its latency is recorded inline in the forwarder before
    dropping (faithful to Java's `logResponse` recording). — **per-heartbeat-response**.

- `OffsetCommitMetricsManager` — owned by `CommitRequestManagerInner` (bg
  task). Java `CommitRequestManager.java:172,767`.
  - `record_request_latency(response.requestLatencyMs())` at top of the
    commit `onResponse` (`:767`, success path only — NOT failure). Rust:
    capture `client_response.request_latency_ms()` in the commit-response
    forwarder spawn (commit_request_manager.rs:1580), thread to
    `handle_offset_commit_response`, record there. — **per-commit-response**.

## Metrics-ownership plumbing
- Heartbeat + commit managers register against the SAME `Arc<Metrics>` the
  consumer owns (M3's `create_fetch_metrics_manager` builds it). Rename that
  to `create_metrics(config) -> Arc<Metrics>` returning just the registry, and
  build the fetch manager + the two bg-task managers + KafkaConsumerMetrics
  from it. Plumb the heartbeat/commit metrics managers into their RMs the same
  way the coordinator/interceptor-hook are: **post-construction setters**
  (`set_metrics_manager`) using an `Option<Arc<…>>` slot — avoids changing the
  ~25 RM-`new` test call sites and the cross-manager construction order. When
  the slot is `None` (tests that don't wire metrics) recording is a no-op
  (value-neutral; the Java tests construct the manager directly and assert on
  it, which the translated Rust unit tests do too).

## Tests (translate; assert VALUES with MockTime)
- `KafkaConsumerMetricsTest` (4): `shouldRecordCommitSyncTime`,
  `shouldRecordCommittedTime`, `shouldRemoveMetricsOnClose`,
  `checkMetricsAfterCreation`. (Java file is at
  `consumer/internals/KafkaConsumerMetricsTest.java`, not under `metrics/`.)
- `HeartbeatMetricsManagerTest` (1): `testHeartbeatMetrics` — uses `MockTime`,
  asserts max=103, rate≈0.1, total=3, last-heartbeat-seconds-ago.
- `OffsetCommitMetricsManagerTest` (1): `testOffsetCommitMetrics` —
  avg=100, max=102, rate=0.1, total=3.
All as in-file `#[cfg(test)]` modules (managers are `pub(crate)`).

## Commits
1. `M4: ClosureMeasurable + KafkaConsumerMetrics + consumer wiring`
2. `M4: Heartbeat + OffsetCommit metrics managers + bg-task wiring`
3. (tests land with each manager; perf-light)

## Verify (DoD)
`cargo build` / `cargo test --lib` / `cargo xtask lint` / `cargo xtask
format-check` green after each commit. No per-record cost added (all hooks are
per-poll/-commit/-heartbeat).
