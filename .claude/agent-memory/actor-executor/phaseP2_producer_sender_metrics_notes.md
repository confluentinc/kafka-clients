---
name: phaseP2-producer-sender-metrics
description: M12 P2 producer Sender-side metrics — registry/SenderMetrics/gauges/throttle plumbing patterns
metadata:
  type: project
---

Milestone 12 Phase P2 (branch `producer-metrics`, Actor N=4): translated the
producer Sender-side metrics.

**Files:** `src/producer/internals/sender_metrics_registry.rs` (client-level
`MetricName`s eager + per-topic `MetricNameTemplate`s + `all_templates()`,
delegates sensor/get_sensor/add_metric to `Arc<Metrics>`), `producer_metrics.rs`
(thin wrapper), `SenderMetrics` inner struct + `throttle_time_sensor()` free fn
in `sender.rs`.

**Key translation decisions (all Java-faithful, cite lines):**
- **requests-in-flight gauge**: client moves into the sender task, so the gauge
  can't capture it like Java. `InFlightRequests.in_flight_request_count` is now
  `Arc<AtomicI32>` with `count_handle()`; `KafkaClient::in_flight_count_handle()`
  trait method (default = fresh zero handle; NetworkClient overrides). Gauge reads
  the shared atomic. MockClient untouched (uses default).
- **throttle plumbing**: `NetworkClient` gains `Option<Arc<Sensor>>` +
  `set_throttle_time_sensor()`; records `response.throttle_time_ms()` for EVERY
  response in `handle_completed_receives` (Java NetworkClient.java:1000). Producer
  factory sets it on the concrete NetworkClient before it moves into with_client.
- **`Sender.throttleTimeSensor` static → module fn** `throttle_time_sensor()` —
  a static-like assoc fn on generic `Sender<C>` can't infer C.
- **update_produce_request_metrics call site**: Java records at Sender.java:434
  after addToInflightBatches (which keeps refs); Rust `add_to_inflight_batches`
  MOVES batches, so record just BEFORE the move. Values identical.
- record_latency: only in the has_response success branch (not timeout/disconnect/
  version-mismatch/acks=0). record_errors: top of innermost
  `fail_batch_with_record_exceptions` (both response + expired paths route here,
  one record per batch). record_retries: at Reenqueue action; record_batch_split:
  after split_and_reenqueue (Java Sensor.record() no-arg = record(1.0)).
- SenderMetrics holds a `time_provider` and computes `now` like Java's `Time time`;
  records via `sensor.record_at(value, now)`.

**Test gotchas:**
- `testQuotaMetrics` needs real NetworkClient + MockSelector → placed in
  network_client.rs test module (harness lives there). MUST build `Metrics` on a
  shared `MockTime` (`crate::common::metrics::time::mock::MockTime` +
  `Metrics::with_config_reporters_time(cfg, vec![], Arc<dyn Time>)`) and drive the
  client from the SAME clock — else windowed Avg/Max measure at wall-clock while
  samples were recorded at mock-time-0 → NaN.
- `testSenderMetricsTemplates`: compare metric↔template on
  `(name, group, BTreeSet<tag_keys>)` tuples, NOT `MetricNameTemplate` in a HashSet
  — MetricNameTemplate's IndexSet hash is tag-order-sensitive.
- SenderTestContext now exposes `metrics: Arc<Metrics>` (built with client-id tag).

**MetricValue** is an enum (`MetricValue::Double(f64)`), no `as_double()`. Read a
metric via `KafkaMetric::metric_value()` (needs `crate::common::metric::Metric`
trait in scope).

Side-fix commit: consumer `metrics.recording.level` case-sensitive validation
(mirrors producer b3d22294).
