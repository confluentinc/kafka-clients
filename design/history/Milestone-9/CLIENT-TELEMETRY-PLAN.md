# Milestone 9: KIP-714 Client Metrics and Observability

## Overview

Translate the client side of KIP-714 (client metrics push) from the pinned
Apache Kafka 4.2.0 source (`kafka/` submodule @ `a18251ba`, tag `4.2.0`).
The client periodically pushes its metrics to the broker over two RPCs:

  - `GetTelemetrySubscriptions` — broker assigns a `ClientInstanceId` and
    returns the subscribed metric prefixes, push interval, and accepted
    compression types.
  - `PushTelemetry` — client serializes matching metrics as an OpenTelemetry
    `MetricsData` protobuf payload (optionally compressed) and pushes it on
    the assigned interval; a final "terminating" push happens on close.

Everything rides the existing `NetworkClient` poll loop — no new thread in
Java, therefore no new task in Rust (CLAUDE.md rule 9).

**Current Rust-side readiness (verified):**

  - `ApiKeys::GET_TELEMETRY_SUBSCRIPTIONS` / `PUSH_TELEMETRY` already exist.
  - All four `*Telemetry*Data` wire types are already generated from
    `generator/messages/` specs.
  - `ApiVersionsResponse` already filters telemetry APIs on
    `client_telemetry_enabled` (tests translated).
  - `network_client.rs` has an explicit "telemetry is deferred" note where
    the `TelemetrySender` integration belongs.
  - Nothing under `org.apache.kafka.common.metrics.*` or
    `org.apache.kafka.common.telemetry.*` is translated yet.

## The prerequisite: `common.metrics` has no Rust translation

KIP-714 does not produce metrics — it **exports the client's existing
metrics registry**. `ClientTelemetryReporter` *implements* `MetricsReporter`;
`KafkaMetricsCollector` receives `KafkaMetric` instances through reporter
callbacks (`init` / `metricChange` / `metricRemoval`) and reads
`metricValue()`, type-switching on `WindowedCount` / `CumulativeSum` to
classify sums vs. gauges. None of that framework exists in Rust today
(30 untranslated classes in `remaining_classes.txt`).

**Decision required — two options:**

  - **Option A (recommended): Phase 0 of this milestone translates the
    `common.metrics` framework core** (~1.9k lines core + ~1.6k lines
    `stats/`, sizes verified below). The registry, `Sensor`, and the stats
    hierarchy are translated fully (DoD #2 — no partial classes), with
    their Java test suites. What is deferred to a **follow-up milestone**
    is *instrumentation parity*: the producer/consumer call sites that
    record into sensors (`SenderMetrics`, `KafkaProducerMetrics`,
    `FetchMetricsManager`, `KafkaConsumerMetrics`, selector metrics, …).
    Until those land, pushes carry few metrics — which is wire-correct and
    honest (the broker matches subscriptions against whatever the client
    reports; librdkafka rolled its KIP-714 metric coverage out gradually
    the same way).
  - **Option B: land `common.metrics` as its own milestone first**, then
    do KIP-714. Cleaner separation, but serializes two milestones and the
    registry alone has no observable behavior until either JMX-equivalent
    reporting (excluded) or KIP-714 consumes it.

The rest of this plan assumes Option A.

## Scope

**In scope:**

  - `common.metrics` framework core + `stats/` (Phase 0, table below).
  - Request/response wrappers for both telemetry RPCs + `ConcreteRequest`
    / `ConcreteResponse` arms.
  - `common.telemetry` package: `ClientTelemetryState`, all
    `internals/` classes (reporter, sender, provider, utils, emitter,
    collector, naming, `LastValueTracker`, `MetricKey`).
  - OTLP protobuf types for `MetricsData` (vendored protos + `prost`; see
    Dependencies).
  - `NetworkClient` `TelemetrySender` integration (mirrors the existing
    `MetadataUpdater` pattern already translated in `network_client.rs`).
  - Config: `enable.metrics.push` (default `true`) in producer + consumer
    configs; `common_client_configs::telemetry_reporter` factory.
  - Public API: `client_instance_id(timeout)` on the `Producer` and
    `Consumer` traits (blocking in Java → `async fn` per rule 9.1).
  - Close path: `initiate_close()` → terminating push (producer
    `KafkaProducer::close`, consumer `AsyncKafkaConsumer::close`).
  - Payload compression reuse (gzip/lz4/snappy/zstd already in
    `common/compress` for record batches).
  - All Java unit tests for the above + byte-level OTLP fixture tests +
    integration test against a real broker with a configured metrics
    subscription.

**Out of scope (deferred or excluded):**

  - Instrumentation parity — recording sensors throughout producer/
    consumer/network internals. Follow-up milestone; tracked separately.
  - `JmxReporter` — JMX does not exist in Rust. The `metrics()` map
    accessor on producer/consumer (Java `Map<MetricName, ? extends Metric>`)
    is translated; JMX exposure is not.
  - Pluggable `metric.reporters` (reflective `Class.forName` loading).
    `MetricsReporter` stays a Rust trait; `ClientTelemetryReporter` is the
    only implementation, constructed directly when `enable.metrics.push`
    is true — same effective behavior as Java's default configuration.
  - Admin client `clientInstanceId` (no admin client in this repo yet).
  - Share-consumer telemetry (KIP-932 is out of scope per
    consumer-threading.md §20).
  - Server-side: `ClientMetricsManager`, broker configs, and the
    `kafka-client-metrics.sh` tooling (used *by* the integration test via
    the broker container, not translated).

## Rust-Specific Adaptations

| Java | Rust |
|------|------|
| `io.opentelemetry.proto:opentelemetry-proto:1.3.2-alpha` (shaded jar) | Vendored `.proto` files (same v1.3.2) compiled with `prost-build` (recommended; see Dependencies) |
| `MetricsReporter` loaded reflectively via `metric.reporters` config | `MetricsReporter` trait; `ClientTelemetryReporter` constructed directly by the `telemetry_reporter()` factory |
| `ReentrantReadWriteLock` guarding `DefaultClientTelemetrySender` state | `std::sync::RwLock` — critical sections are short and never await (rule 9.6.2) |
| `Condition subscriptionLoaded` + blocking `fetchClientInstanceId(Duration)` | `tokio::sync::watch<Option<Uuid>>`; `client_instance_id` is an `async fn` awaiting the watch with `tokio::time::timeout` (rule 9.1 — a `Condvar` would block the runtime) |
| `synchronized` on `configure` / `contextChange` / `updateMetricsLabels` | `std::sync::Mutex` around reporter mutable state |
| `ConcurrentHashMap` in `KafkaMetricsCollector` | `DashMap` (already a dependency) or `Mutex<HashMap>` — Actor's choice, no await under lock |
| `Predicate<? super MetricKeyable>` metric selector | `Box<dyn Fn(&MetricKey) -> bool + Send + Sync>` (prefix matching, same semantics) |
| `Time.SYSTEM` | existing `common/utils` time abstraction |
| `NetworkClient.TelemetrySender` (inner class) | struct in `network_client.rs`, `Option<TelemetrySender>` field — exactly parallel to the existing `external_metadata_updater: Option<Box<dyn MetadataUpdater>>` |

Threading note: the bg-task side (`time_to_next_update`, `create_request`,
`handle_response`) is only ever called from the single `NetworkClient` poll
loop; the app side (`client_instance_id`, `initiate_close`, reporter
callbacks from `Metrics`) crosses tasks. The lock split above mirrors
Java's read/write-lock discipline 1:1 so behavior-parity review stays easy.

Hot-path note (rule 11): telemetry collection is periodic (subscription
push interval), not per-record — no hot-path constraints on the collector.
`Sensor::record()` *will* sit on the send path once instrumentation lands,
so Phase 0 must translate `Sensor` with that in mind (Java's `record()` is
`synchronized`; the Rust translation keeps a `Mutex` now, and the
follow-up milestone owns any contention work — do not speculatively
optimize here).

## Dependencies (CLAUDE.md rule 1.2 — requires approval before adding)

`PushTelemetryRequest.metrics` is a serialized OpenTelemetry
`MetricsData` protobuf message. Two options:

  - **Vendored protos + `prost` (recommended).** Copy the three proto
    files (`common/v1/common.proto`, `resource/v1/resource.proto`,
    `metrics/v1/metrics.proto`) from opentelemetry-proto **v1.3.2** — the
    exact version Java pins and shades — into `proto/opentelemetry/`, and
    generate with `prost-build` from `build.rs` (which already runs the
    message generator). Promote `prost` from dev-dependency to dependency;
    add `prost-build` to build-dependencies. Mirrors Java's shading intent:
    version-pinned, no external crate churn, no tonic/transport baggage.
    The proto files are Apache-2.0; keep their headers.
  - Alternative: `opentelemetry-proto` crate. Fewer vendored files, but
    the crate tracks newer OTLP versions than Java's 1.3.2 pin and pulls
    feature-gated codegen deps.

No other new dependencies. Compression reuses existing crates.

## Implementation Phases

### Phase 0: `common.metrics` framework core

| Java Class | Rust Module | Lines |
|---|---|---|
| `MetricName` | `common/metric_name.rs` | 132 |
| `MetricNameTemplate` | `common/metric_name_template.rs` | small |
| `Metric` (interface) | `common/metric.rs` | small |
| `Metrics` | `common/metrics/metrics.rs` | 696 |
| `Sensor` | `common/metrics/sensor.rs` | 396 |
| `KafkaMetric` | `common/metrics/kafka_metric.rs` | 127 |
| `MetricConfig`, `Quota`, `QuotaViolationException` → `QuotaViolationError` | `common/metrics/...` | ~180 |
| `MetricValueProvider`, `Measurable`, `Gauge`, `Stat`, `MeasurableStat`, `CompoundStat` | `common/metrics/...` | ~215 |
| `MetricsReporter`, `MetricsContext`, `KafkaMetricsContext` | `common/metrics/...` | ~185 |
| `internals/MetricsUtils` | `common/metrics/internals/metrics_utils.rs` | small |
| `stats/*` (Avg, Min, Max, Value, Rate, SimpleRate, Meter, WindowedCount, WindowedSum, CumulativeCount, CumulativeSum, SampledStat, TokenBucket, Percentile(s), Histogram, Frequency/Frequencies) | `common/metrics/stats/` | 1577 total |

Tests: `MetricsTest`, `SensorTest`, `KafkaMetricsContextTest`, and the
`stats/` test files (`RateTest`, `MeterTest`, `SampledStatTest`,
`TokenBucketTest`, `FrequenciesTest`, `HistogramTest`, …). Translate the
full set (DoD #3).

Estimated: ~20 classes. This is the largest phase; it can be split
Phase 0a (interfaces + `MetricName`/`MetricConfig`/`KafkaMetric`) /
Phase 0b (`Metrics` + `Sensor` + `stats/`) at Actor's discretion.

### Phase 1: Telemetry RPC wrappers

| Java Class | Rust Module |
|---|---|
| `GetTelemetrySubscriptionsRequest` | `common/requests/get_telemetry_subscriptions_request.rs` |
| `GetTelemetrySubscriptionsResponse` | `common/requests/get_telemetry_subscriptions_response.rs` |
| `PushTelemetryRequest` | `common/requests/push_telemetry_request.rs` |
| `PushTelemetryResponse` | `common/requests/push_telemetry_response.rs` |

Plus `ConcreteRequest` / `ConcreteResponse` enum arms. Same mechanical
pattern as the Milestone-3 SASL wrappers. Wire types already generated.

Tests: the four `*Test.java` files under `common/requests` (verified
present at the pin), including error-count aggregation tests.

### Phase 2: State machine + metric identity types

| Java Class | Rust Module |
|---|---|
| `ClientTelemetryState` (166 lines, strict `validateTransition`) | `common/telemetry/client_telemetry_state.rs` |
| `MetricKey`, `MetricKeyable` | `common/telemetry/internals/metric_key.rs`, `metric_keyable.rs` |
| `MetricNamingStrategy`, `TelemetryMetricNamingConvention` | `common/telemetry/internals/...` |

Tests: `ClientTelemetryStateTest` (every legal and illegal transition —
error *messages* asserted per DoD #3), `TelemetryMetricNamingConventionTest`.

### Phase 3: OTLP layer

  - Vendor protos + wire `prost-build` into `build.rs` (per the approved
    dependency decision).
  - `SinglePointMetric` (147) — sum/gauge point construction with
    delta/cumulative `AggregationTemporality`.
  - `MetricsEmitter` (109), `ClientTelemetryEmitter` (60).
  - `LastValueTracker` (86) — delta computation between pushes.

Tests: `SinglePointMetricTest`, `ClientTelemetryEmitterTest`,
`LastValueTrackerTest`, plus `TestEmitter` (test helper). Add a
**byte-level OTLP fixture test**: serialize a known `MetricsData` from the
Java client (hex fixture) and assert byte-identical `prost` output — same
rationale as Milestone-3's SASL hex fixtures (round-trips alone can hide a
symmetric encoding bug).

### Phase 4: Metrics collection

| Java Class | Rust Module | Lines |
|---|---|---|
| `MetricsCollector` | `common/telemetry/internals/metrics_collector.rs` | 83 |
| `KafkaMetricsCollector` | `common/telemetry/internals/kafka_metrics_collector.rs` | 321 |

Depends on Phase 0 (`KafkaMetric`, `MeasurableStat` types for the
sum-vs-gauge type switch) and Phase 3 (emitter).
Tests: `KafkaMetricsCollectorTest`.

### Phase 5: Reporter and sender

| Java Class | Rust Module | Lines |
|---|---|---|
| `ClientTelemetryUtils` (subscription validation, compression, selector, `fetchClientInstanceId`) | `common/telemetry/internals/client_telemetry_utils.rs` | 247 |
| `ClientTelemetryProvider` (OTLP `Resource` labels: client_id, group_id, member_id, transactional_id, rack, …) | `common/telemetry/internals/client_telemetry_provider.rs` | 153 |
| `ClientTelemetrySender` (interface) | `common/telemetry/internals/client_telemetry_sender.rs` | 112 |
| `ClientTelemetryReporter` + inner `DefaultClientTelemetrySender` (state machine over `ClientTelemetryState`, subscription cache, interval/jitter timing, terminating-push handshake) | `common/telemetry/internals/client_telemetry_reporter.rs` | 1020 |

Tests: `ClientTelemetryUtilsTest`, `ClientTelemetryReporterTest`.
This phase carries the concurrency translation (RwLock/watch table above).

### Phase 6: NetworkClient + client wiring + public API

  - `TelemetrySender` struct in `network_client.rs`: `maybe_update(now)`
    feeding the selector poll timeout (min with metadata timeout — Java
    `NetworkClient.poll` line 643), least-loaded-node selection, response
    routing arms for both telemetry responses, failed/unsupported-version
    handling, `close()`. Removes the "telemetry is deferred" note and
    completes the omitted `NetworkClientTest` telemetry assertions.
  - `client_utils::create_network_client` overloads gain the sender param.
  - `common_client_configs`: `ENABLE_METRICS_PUSH_CONFIG` +
    `telemetry_reporter(client_id, config)` factory.
  - `ProducerConfig` / `ConsumerConfig`: `enable.metrics.push` key.
  - `KafkaProducer` / `AsyncKafkaConsumer`: hold
    `Option<ClientTelemetryReporter>`, pass `telemetry_sender()` into
    network-client construction, call `update_metrics_labels` on group/
    transactional state changes, `initiate_close()` in `close()` (before
    the close timeout wait, matching Java ordering).
  - `Producer` / `Consumer` traits: `async fn client_instance_id(timeout)`
    (+ `MockProducer` / `MockConsumer` parity with Java mocks' behavior).

Tests: `KafkaProducerTest` / `AsyncKafkaConsumerTest` telemetry cases,
`NetworkClientTest` telemetry cases, `CommonClientConfigs` factory test.

### Phase 7: Integration test

Docker broker (existing testcontainers infra) with a client-metrics
subscription created via the broker container's `kafka-client-metrics.sh`
(or `kafka-configs.sh`) before the client connects. Assert:

  1. Client transitions `SUBSCRIPTION_NEEDED → SUBSCRIPTION_IN_PROGRESS →
     PUSH_NEEDED → …` and `client_instance_id()` returns the
     broker-assigned UUID.
  2. Broker accepts at least one `PushTelemetry` (no error).
  3. Close performs the terminating push.
  4. `enable.metrics.push=false` → no telemetry RPCs issued (and
     `client_instance_id()` errors as in Java).

## Excluded Java Classes

| Java Class | Why excluded |
|---|---|
| `JmxReporter`, `JmxReporter.KafkaMbean` | JMX does not exist in Rust |
| `ClientTelemetry` interface (`clientReceiver()`) | broker-side receiver plumbing; client only needs `MetricsReporter` + sender |
| `ClientMetricsManager`, `ClientMetricsConfigs`, … (server module) | server-side |
| `ClientMetricsCommand` (tools) | CLI tooling, not client library |
| Reflective plugin loading (`AbstractConfig.getConfiguredInstances` for `metric.reporters`) | no dynamic class loading; trait + direct construction |
| `ShareConsumer` telemetry surfaces | KIP-932 out of scope (§20) |
| Admin `clientInstanceId` | no admin client translated yet |

## Class Summary

| Phase | New classes | Modified |
|---|---|---|
| 0: metrics core | ~20 (+16 stats) | 0 |
| 1: RPC wrappers | 4 | 2 (ConcreteRequest/Response) |
| 2: state + keys | 5 | 0 |
| 3: OTLP layer | 4 (+ generated protos) | 1 (build.rs) |
| 4: collector | 2 | 0 |
| 5: reporter/sender | 4 | 0 |
| 6: wiring | 1 (TelemetrySender) | ~8 (NetworkClient, ClientUtils, configs, producer, consumer, traits, mocks) |
| 7: integration | tests only | test infra |

## Open Questions (resolve before Actor starts)

1. **Approve the protobuf dependency** (vendored protos + promote `prost`
   to a production dependency + add `prost-build`) — rule 1.2 ask.
2. **Confirm Option A** (metrics framework as Phase 0 here; sensor
   instrumentation parity as its own follow-up milestone).
3. Verify the broker image used by the integration suite ships
   `kafka-client-metrics.sh` and that subscriptions can be created
   in-container (fallback: create the subscription via raw
   `AlterConfigs`-family RPC from the test).

## Adoption checklist (post-approval)

  - Add Milestone 9 to `design/history/MILESTONES.md` (file currently
    ends at Milestone 5 and is stale).
  - Regenerate `marked_classes.txt` / `remaining_classes.txt` — last
    refreshed 2026-04-22, before the Milestone-8 consumer landed.

## Java Source Reference (at `kafka/` pin `a18251ba`, tag 4.2.0)

- Telemetry: `kafka/clients/src/main/java/org/apache/kafka/common/telemetry/`
- Metrics: `kafka/clients/src/main/java/org/apache/kafka/common/metrics/`
- NetworkClient integration: `kafka/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java` (inner class `TelemetrySender`, lines ~1382–1500)
- Factory: `kafka/clients/src/main/java/org/apache/kafka/clients/CommonClientConfigs.java` (`telemetryReporter`)
- Requests: `kafka/clients/src/main/java/org/apache/kafka/common/requests/{GetTelemetrySubscriptions,PushTelemetry}*.java`
- Public API: `Producer.clientInstanceId`, `Consumer.clientInstanceId`, `KafkaProducer` lines ~1347, ~1533; `AsyncKafkaConsumer` lines ~406, ~521, ~1634
