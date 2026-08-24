# Phase P2 — SenderMetricsRegistry + SenderMetrics + gauges + produce-throttle-time

Actor N=4, branch `producer-metrics`.

Translates the Sender-side producer metrics from Apache Kafka 4.2
(`clients/.../producer/internals/{SenderMetricsRegistry,ProducerMetrics}.java`
and `Sender.java`'s `SenderMetrics` inner class + `throttleTimeSensor`).

## Plan

1. Side-fix (separate commit, first): make `ConsumerConfig`'s
   `metrics.recording.level` validation case-sensitive, mirroring the producer
   fix `b3d22294`.
2. `src/producer/internals/sender_metrics_registry.rs` — `SenderMetricsRegistry`:
   eager client-level `MetricName`s + per-topic `MetricNameTemplate`s +
   `all_templates()`, delegating `sensor`/`get_sensor`/`add_metric` to the shared
   `Metrics`.
3. `src/producer/internals/producer_metrics.rs` — `ProducerMetrics` wrapping the
   registry.
4. `Sender.SenderMetrics` inner class in `sender.rs`: the nine sensors, the two
   constructor gauges, the lazy per-topic sensors, and the recording methods,
   wired at the Java-faithful call sites.
5. `produce-throttle-time` sensor via `throttle_time_sensor()`, recorded by the
   `NetworkClient` for every response.
6. Tests: `testQuotaMetrics`, `testSenderMetricsTemplates`, plus unit tests for
   `maybe_register_topic_metrics` / `update_produce_request_metrics`.

## Design choices

### requests-in-flight counter (shared atomic vs. capturing the client)

Java's `requests-in-flight` gauge is `(config, now) -> client.inFlightRequestCount()`
— the gauge closure captures the `KafkaClient`. In Rust the client `C` is *moved*
into the spawned sender task, so a gauge closure cannot borrow it. Chosen
solution: `InFlightRequests` already held an `AtomicI32` count; it is now
`Arc<AtomicI32>` with a `count_handle()` accessor. `NetworkClient` exposes it via
the new `KafkaClient::in_flight_count_handle()` trait method (default returns a
fresh always-zero handle; `NetworkClient` overrides to return the live shared
counter). `Sender::new` takes that handle from the client before the move and the
gauge reads the shared atomic. `MockClient` is untouched — it uses the default
zero handle, which is fine because no test asserts its `requests-in-flight` value.

### produce-throttle-time plumbing (NetworkClient-level optional sensor)

Java hands the `produce-throttle-time` sensor to the `NetworkClient` constructor
(`KafkaProducer.java:514,523` via `ClientUtils.createNetworkClient`), and
`NetworkClient.handleCompletedReceives` records **every** response's
`throttleTimeMs` into it (`NetworkClient.java:1000`). This is the exact behavior
`testQuotaMetrics` asserts (it records the ApiVersions throttle 400 as well as
the produce throttles 100/200/300).

Chosen approach: add an `Option<Arc<Sensor>>` field to `NetworkClient`, set via a
`set_throttle_time_sensor()` setter (rather than threading a new constructor
parameter through every call site), and record `response.throttle_time_ms()` for
every parsed response in `handle_completed_receives` — the direct analog of
Java's site. The producer factory (`KafkaProducer::from_config`) creates the
sensor with `throttle_time_sensor(&registry)` and sets it on the concrete
`NetworkClient` before the client moves into the generic sender task. The
consumer's fetch-throttle precedent left this as a "documented carry-over" and
never actually recorded from real responses; here it is fully wired because
`testQuotaMetrics` requires it. NetworkClient shared code stays unchanged for all
other callers (the field defaults to `None`).

### `throttle_time_sensor` as a module function, not a static method

Java's `Sender.throttleTimeSensor(registry)` is a `static` method. In Rust a
static-like associated function on the generic `Sender<C>` cannot infer `C` at
the call site, so it is a `pub(crate) fn throttle_time_sensor` in the `sender`
module. Documented at the definition.

### `update_produce_request_metrics` call-site reorder

Java calls `sensors.updateProduceRequestMetrics(batches)` at `Sender.java:434`,
*after* `addToInflightBatches` — but Java's `addToInflightBatches` keeps the
batch references in the `batches` map. Rust's `add_to_inflight_batches` *moves*
the batches out, so the recording is done immediately before that move. The
recorded values (estimated size, queue time, compression ratio, max record size,
record count) are unaffected by the reorder. Commented at the call site.

### testQuotaMetrics location

`testQuotaMetrics` uses a real `NetworkClient` + `MockSelector` request/response
harness that lives (test-private) in the `network_client.rs` test module. Rather
than duplicate that harness into the sender module, the translated test lives
alongside it and calls `sender::throttle_time_sensor`. It drives the metrics
registry and the client from a single shared `MockTime` so the windowed Avg/Max
stats are measured at the same clock the sensor records at (a wall-clock metrics
clock would read NaN because the mock-time samples fall outside the measured
window — this was observed and fixed).

## Self-review (Definition of Done)

- **DoD #1 (rules):** `internal` package → `pub(crate)` (registry, ProducerMetrics,
  SenderMetrics); constants exported only by defining file (`GROUP` reused from
  `kafka_producer_metrics`, `TOPIC_METRIC_GROUP_NAME` local); Apache 2.0 headers on
  new files; javadoc → rustdoc with Java line citations; `i32`/`f64` casts match
  Java's `int`/`double` recording.
- **DoD #2 (methods):** every `SenderMetricsRegistry` member (all client-level +
  topic-level accessors + `all_templates`/`sensor`/`get_sensor`/`add_metric`),
  `ProducerMetrics`, and every `SenderMetrics` method
  (`update_produce_request_metrics`, `record_latency`, `record_retries`,
  `record_errors`, `record_batch_split`, `maybe_register_topic_metrics`, both
  gauges) translated. `ProducerMetrics.main` (a docs-table emitter) is not
  translated; its `getAllTemplates` is exposed as a test-only `all_templates()`.
- **DoD #3 (tests):** `testQuotaMetrics` and `testSenderMetricsTemplates`
  translated faithfully (exact throttle values 400/100/200/300 → avg 250 / max
  400; template-set equality on name+group+tag-keys). Added unit tests for
  `maybe_register_topic_metrics` and `update_produce_request_metrics`, which are
  otherwise only exercised indirectly in Java. Side-fix adds
  `test_metrics_recording_level_validator` to `consumer_config`. Error-message
  content asserted in the recording-level test.
- **DoD #4 (blockers):** the in-flight shared-atomic handle and the NetworkClient
  throttle-sensor plumbing were the two blockers; both implemented.
- **DoD #5 (all tests pass):** `cargo test --lib` → 3193 passed, 0 failed.
- **DoD #6/#7 (no dupes / no extra types):** no duplicated classes; no
  Rust-invented types beyond the Java structure. The `TemplateKey` tuple in the
  test is a local hash-stable comparison key, not a new domain type.
- **DoD #8 (no TODO/FIXME):** none left. The one unreachable registration-error
  path in `update_produce_request_metrics` logs and continues (Java uses
  `Objects.requireNonNull`); it cannot fire at runtime because the sensor names
  are deterministic and registered by the preceding `maybe_register_topic_metrics`.
- **DoD #9 (make verify):** `cargo build`, `cargo test --lib`,
  `cargo xtask format`, `cargo xtask lint` all clean. C/Python surfaces are
  untouched by this phase.
- **DoD #10 (hot-path allocation audit):** all recording is per-drained-batch
  (`update_produce_request_metrics`) or per-response
  (`record_latency`/`record_retries`/`record_errors`), amortized over many
  records — NOT the per-record send path (CLAUDE.md §11). No per-record
  allocations, `String` clones, or `Box<dyn Future>` added on the send path. The
  per-topic name strings are built once per drained batch, exactly as Java does.
  The §27 consumer per-record allocation-budget test
  (`test_collect_fetch_per_record_allocation_budget`) still passes, and the
  producer send path (`RecordAccumulator`/`ProducerBatch`/`BufferPool`) is
  untouched.
- **DoD #11 (consumer trait surface):** N/A — this phase is producer-side.
