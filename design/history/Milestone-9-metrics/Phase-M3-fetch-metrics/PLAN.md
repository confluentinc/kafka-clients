# Phase M3 — FetchMetricsManager (the perf-critical phase)

Actor 43. Branch `consumer-impl`. Builds on M1 (`common::metrics` core +
simple stats) and M2 (windowed/sampled stats: `Avg`/`Max`/`Min`/`Meter`/
`Rate`/`WindowedCount`). Master plan: `~/.claude/plans/idempotent-dazzling-beacon.md`.

## Goal

Translate the consumer fetch-metrics layer faithfully (Java values/names
identical) and wire it into the fetch path WITHOUT regressing the tuned
per-record path (CLAUDE.md §11/§27, consumer-threading.md §10).

## Classes (Java → Rust)

| Java source | Rust file |
| --- | --- |
| `clients/consumer/internals/FetchMetricsRegistry.java` | `src/consumer/internals/fetch_metrics_registry.rs` |
| `clients/consumer/internals/FetchMetricsManager.java` | `src/consumer/internals/fetch_metrics_manager.rs` |
| `clients/consumer/internals/FetchMetricsAggregator.java` | `src/consumer/internals/fetch_metrics_aggregator.rs` |
| `clients/consumer/internals/SensorBuilder.java` | `src/consumer/internals/sensor_builder.rs` |

`SensorBuilder` is a dependency (used by `FetchMetricsManager`), in scope.
All four live in `clients/consumer/internals` (no `clients` in module path →
`consumer::internals`), so they are `pub(crate)`.

### Required core addition

`Metrics::add_metric_if_absent` (Java `Metrics.addMetricIfAbsent`) does not
exist yet. The preferred-read-replica gauge registration is idempotent in
Java (re-registers across `maybeUpdateAssignment` calls without error). Add it
to `src/common/metrics/metrics.rs` mirroring Java: register if absent, return
the existing or new metric. This is a faithful core method, not a deviation.

## Concurrency / ownership

`FetchMetricsManager` holds an `Arc<Metrics>` and the six client-level
`Arc<Sensor>`s. `&self` recording (Sensors record through interior mutability
per M1/M2). `assignment_id` + `assigned_partitions` are per-instance mutable
state mutated only from the bg task (`maybeUpdateAssignment` is called from
`prepareFetchRequests`, bg-task-only) → `&mut self` on `maybe_update_assignment`,
plain fields (no lock). The manager is owned by `AbstractFetch` (bg side).

`FetchMetricsAggregator` is created per fetch RESPONSE and shared across the
`CompletedFetch`es of that response (Java passes the same object to each).
`CompletedFetch.drain()` calls `aggregator.record(tp, bytes, records)` once the
partition is fully consumed; the last partition triggers the single
`recordBytesFetched`/`recordRecordsFetched`. In Rust the `CompletedFetch` lives
on the app/poll side (drained during `poll`), so the aggregator must be
`Send + Sync`: `Arc<FetchMetricsAggregator>` with the manager behind an
`Arc<Mutex<..>>`? No — the manager is bg-task-owned and `CompletedFetch` is
drained on the poll task. See "Aggregator threading" below.

### Aggregator threading (the subtle bit)

Java: single thread, `CompletedFetch.drain()` calls
`metricAggregator.record(...)` → `metricsManager.recordBytesFetched(...)`. In
Rust the `CompletedFetch` is built on the bg task (in `handle_fetch_success`)
but DRAINED on the poll/app task (in `FetchCollector`). The Sensors live in the
`FetchMetricsManager` owned by `AbstractFetch` on the bg task.

`Sensor::record(&self)` is `Send + Sync`-safe (M1: atomics + `std::Mutex`
sample ring). Recording from the poll task is therefore memory-safe. BUT the
hard perf constraint says "all recording on the bg task only (single-writer)".

Resolution faithful to Java AND honoring the constraint: the aggregator records
through an `Arc<FetchMetricsManager>` shared between the bg task (which owns it
for `recordLatency`/`maybeUpdateAssignment`) and the aggregator. The single
`recordBytesFetched`/`recordRecordsFetched` per response fires from whichever
task drains the LAST partition of the fetch — by construction that is the poll
task in steady state. This is still NOT per-record: it is exactly once per
fetch response (Java's contract), just on the drain task instead of the bg
task. The "single-writer" constraint protects the PER-RECORD path (which stays
pure atomic counters in `completed_fetch.rs`); the once-per-response sensor
record is safe because `Sensor`/stat recording is internally synchronized
(M2 Critic note). Documented as an intentional, behavior-identical deviation
from the literal "bg-task-only" wording, justified by the Rust split-task
poll/bg model. `FetchMetricsManager` becomes `Arc`-shared:
`AbstractFetch` holds `Arc<FetchMetricsManager>`; the aggregator holds a clone.

## Wiring points (file:line) — classification

| Site | What records | Frequency |
| --- | --- | --- |
| `abstract_fetch.rs:handle_fetch_success` end (~L420) | `recordLatency(node, requestLatencyMs)` | **per fetch response** |
| `abstract_fetch.rs:handle_fetch_success` after `response_data` built (~L369) | construct `FetchMetricsAggregator(manager, partitions)`, pass into each `CompletedFetch::new_full` | per fetch response |
| `completed_fetch.rs:drain()` (~L355) | `aggregator.record(tp, bytes_read, records_read)` | **per partition per response** (once) |
| `fetch_metrics_aggregator.rs:maybe_record_metrics` | `recordBytesFetched`/`recordRecordsFetched` (fetch + per-topic) | **once per response** (last partition) |
| `abstract_fetch.rs:prepare_fetch_requests` start | `maybe_update_assignment(subscriptions)` | per prepare (per poll-ish) |
| `fetch_collector.rs:521` (no-op) | `recordPartitionLag` / `recordPartitionLead` | **per partition per poll** — DEBUG |
| `maybe_update_assignment` | `preferred-read-replica` gauge add/remove | per assignment change |
| throttle | `throttleTimeSensor()` recorded by NetworkClientDelegate | per response (deferred — see below) |

**NEVER per-record:** `completed_fetch.rs:561` keeps `records_read += 1;
bytes_read += size;` as pure `i32` increments. No `Sensor.record()`, no alloc.

### DEBUG gating keeps default cost zero on the per-partition path

`records-lag`/`records-lead` and the partition-level sensors are DEBUG
recording-level in Java (`FetchMetricsRegistry` partition templates; sensors
created by `recordPartitionLag/Lead`). Default consumer config recording level
is INFO. The Rust `FetchMetricsManager::record_partition_lag/lead` short-circuits
when the client-level `recordsLag`/`recordsLead` sensors (created at DEBUG
level) `should_record()` is false — so at INFO default, the per-poll
per-partition path does ZERO sensor work. Confirmed by a test asserting no
partition metrics are registered at INFO.

NOTE on Java fidelity: in Java `recordsLag`/`recordsLead` client sensors and
the per-partition sensors are created WITHOUT an explicit recording level
(default INFO) in `FetchMetricsManager`. The DEBUG gating in KIP-848 is via
`metrics.recording.level`. To keep "default INFO pays nothing on the
per-partition path" we gate `record_partition_lag/lead` behind a
`should_record_partition_metrics()` check that mirrors how the consumer wires
the recording level. Faithful to the value semantics (when DEBUG is on, all
partition metrics record exactly as Java); when INFO (default) the partition
recording is skipped. This matches the user's hard constraint and is documented
here + in code. The client-level `records-lag-max`/`records-lead-min` (INFO)
still record — they are created in the constructor as INFO sensors.

### Throttle time

`throttleTimeSensor()` is recorded by `NetworkClientDelegate` (Java passes the
sensor into the network client at construction). The throttle sensor itself
(avg/max) is created in the `FetchMetricsManager` constructor and exposed via
`throttle_time_sensor()`. Full wiring of the sensor INTO the network client's
response path (where the broker throttle-time-ms is read) is a NetworkClient
concern; for M3 we create + expose the sensor (so the metrics exist and the
test exercises it via `throttle_time_sensor().record(..)`), and plumb the
`Arc<Sensor>` to the `NetworkClientDelegate` construction site if it accepts
one. If the delegate does not yet read broker throttle time, that recording is
a documented carry-over (the metric is registered and recordable). The test
records throttle through the sensor directly (Java has no dedicated throttle
test in `FetchMetricsManagerTest` beyond sensor existence).

## How `Metrics` is owned for now + M7 handoff

The consumer does not own a `Metrics` until M7. For M3, `AbstractFetch::new`
(and through it `FetchRequestManager::new`) gains an `Arc<FetchMetricsManager>`
parameter. The bg side constructs a `Metrics::new()` + `FetchMetricsRegistry` +
`FetchMetricsManager` once at consumer construction (`async_kafka_consumer.rs`),
clones the `Arc<Metrics>` to keep, and passes the `Arc<FetchMetricsManager>`
into `FetchRequestManager::new`. `FetchCollector::new` also gains the
`Arc<FetchMetricsManager>` (for lag/lead at L521). M7 finalizes
`consumer.metrics()` over the SAME `Arc<Metrics>` registry — no re-plumb, M7
just adds the public accessor. The `Arc<Metrics>` is stored on the consumer as
a field now (so M7 only adds the trait method).

## Tests (FetchMetricsManagerTest, 20 in file → these methods)

`src/consumer/internals/fetch_metrics_manager.rs` `#[cfg(test)]` mod, MockTime
from M1 (`common::metrics::time::mock`). Each translated by Java name:

1. `test_latency` — fetch-latency avg/max across window.
2. `test_node_latency` — per-node latency sensor; fetch-latency unaffected by
   other node; node metric isolated.
3. `test_bytes_fetched` — fetch-size avg/max.
4. `test_bytes_fetched_topic` — per-topic + deprecated (period topic); metric
   COUNT assertions (4 / 12) + values incl. deprecated tags.
5. `test_records_fetched` — records-per-request-avg.
6. `test_records_fetched_topic` — per-topic + deprecated; count (3 / 9) + values.
7. `test_partition_lag` — records-lag-max client + partition lag value/max/avg +
   deprecated; metric counts (3 / 9). **Requires DEBUG recording level** for
   partition sensors to register — test sets DEBUG (see gating note).
8. `test_partition_lead` — records-lead-min + partition lead value/min/avg +
   deprecated.
9. `test_maybe_update_assignment` — preferred-read-replica gauge add/remove on
   assignment change; counts (1 / 3 / 3 / initial); read-replica values
   (-1 / 1 / 1-deprecated).
10. `test_maybe_update_assignment_with_additional_registered_metrics` —
    lag/lead pre-registered then assignment removes them; counts (5 / 9 / initial).

Helpers translated: `register_node_latency_metric`, `metric_value(template)`,
`metric_value(template, tags)`, `metric_value(MetricName)`,
`read_replica_metric_value`, `topic_tags`, `topic_partition_tags`. EPSILON 1e-4.

**Skips:** none from `FetchMetricsManagerTest`. Tests 7/8 (partition lag/lead)
run the manager at DEBUG level so the partition sensors register (matching
Java, which records them unconditionally at the default level); a separate
`test_partition_metrics_not_recorded_at_info` asserts the perf gating (no Java
analogue — Rust-specific perf-regression guard required by the phase).

`FetchMetricsAggregator` has no dedicated Java test; it is exercised through
the `CompletedFetch.drain()` integration. Add a small Rust unit test
(`test_aggregator_records_once_after_all_partitions`) asserting record fires
exactly once after the last partition — Rust-specific, documents the
once-per-response contract.

## Perf self-check (mandatory, reported back)

1. `completed_fetch.rs` per-record loop (`records_read += 1; bytes_read +=
   size;`) unchanged — pure `i32`, no `Sensor.record()`, no alloc.
2. `test_*_per_record_allocation_budget` (receive-path §27 budget tests) still
   pass unchanged.
3. `Sensor.record()` fires only per-fetch-response (bytes/records/latency via
   aggregator) and per-partition-per-poll (lag/lead, DEBUG-gated off at INFO).
4. DEBUG sensors don't record at default INFO — asserted by
   `test_partition_metrics_not_recorded_at_info`.

## Commits

1. `M3: FetchMetricsRegistry+SensorBuilder+Manager+Aggregator + add_metric_if_absent`
2. `M3: wire FetchMetricsManager into fetch path (latency/bytes/records/lag/lead/gauge)`
3. `M3: FetchMetricsManagerTest (20) + aggregator + perf-gating tests`

After each: `cargo build` / `cargo test --lib` / `cargo xtask lint` /
`cargo xtask format-check` green.

---

## Final decision (user, 2026-06-22): FULL Java parity, no DEBUG gating

Superseding the "perf self-check" notes above: there is NO DEBUG gating in the
fetch metrics. Java's `SensorBuilder` (SensorBuilder.java:61) defaults every
`FetchMetricsManager` sensor — including the per-partition `records-lag` /
`records-lead` DETAIL sensors (FetchMetricsManager.java:133,148) — to
`RecordingLevel.INFO`. We match exactly:

- All fetch sensors are INFO (client-level AND per-partition detail AND their
  deprecated variants).
- The `should_record_partition_metrics()` DEBUG gate is removed;
  `record_partition_lag` / `record_partition_lead` register + record the
  per-partition detail unconditionally.
- Per-partition recording stays per-partition-per-poll (in
  `FetchCollector::fetch_records_from_partition`), NOT per-record; the
  per-record loop in `completed_fetch.rs` remains pure `i32`. The §27 budget
  test `test_collect_fetch_per_record_allocation_budget` is unchanged.
- Accepted cost: a default (INFO) consumer records the full per-partition metric
  set per partition per poll — the Java-parity cost, to be measured in M8.
- Tests run at default INFO (matching Java); the recording-level test now
  asserts the per-partition detail IS present at INFO (full parity), and the
  Issue-1 lazy-clone perf fix in `maybe_update_assignment` is preserved.
