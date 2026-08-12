# Phase M5 — Rebalance + RebalanceCallback metrics

Actor 45. Builds on M1–M4 (`common::metrics` + Fetch/KafkaConsumer/Heartbeat/OffsetCommit
managers, all registering against the consumer's `Arc<Metrics>`).

## Scope (Java → Rust)

| Java class (`clients/consumer/internals/metrics`) | Rust |
| --- | --- |
| `RebalanceMetricsManager` (abstract base) | folded into `ConsumerRebalanceMetricsManager` — see deviation below |
| `ConsumerRebalanceMetricsManager` | `consumer::internals::consumer_rebalance_metrics_manager::ConsumerRebalanceMetricsManager` |
| `RebalanceCallbackMetricsManager` | `consumer::internals::rebalance_callback_metrics_manager::RebalanceCallbackMetricsManager` |

Module placement follows the M3/M4 precedent: managers live directly under
`src/consumer/internals/` (flat), not a `metrics/` subdir (no such subdir exists in this repo).

### Deviation: `RebalanceMetricsManager` abstract base folded in

Java's `RebalanceMetricsManager` is an abstract base with:
- a `protected String metricGroupName` + `createMetric(...)` helper,
- abstract `recordRebalanceStarted/Ended`, `rebalanceStarted`, and a default no-op
  `maybeRecordRebalanceFailed`.

It exists so `AbstractMembershipManager` can hold a polymorphic `RebalanceMetricsManager`
ref shared by `ConsumerRebalanceMetricsManager` (consumer/streams) and
`ShareRebalanceMetricsManager` (share). Per `consumer-threading.md` §20, Share and Streams
are OUT of scope, so only one concrete impl exists in the Rust client. Translating the base
as a trait would add a single-impl trait with no polymorphism — a struct not present in a
useful sense. We therefore fold the base's helper + the four methods directly onto
`ConsumerRebalanceMetricsManager`. If/when Share/Streams are translated, a
`RebalanceMetricsManager` trait can be extracted with no API change. (CLAUDE.md DoD §7:
new traits must be justified; here we justify NOT adding one.)

## Per-sensor recording levels (Java vs Rust — must match exactly)

Both Java managers call `metrics.sensor(name)` / `metrics.metricName(...)` / `addMetric(...)`
with NO explicit `RecordingLevel` → all default to **INFO** (`Sensor`'s default level is
`INFO`; `metricName` overloads without a level produce INFO metrics). There are NO
DEBUG-gated sensors in either manager. Rust mirrors: every sensor/metric created with the
default (INFO) level. No DEBUG gating, no perf-driven downgrade — matches Java 1:1.

| Sensor / metric | Java level | Rust level |
| --- | --- | --- |
| `rebalance-latency` (avg/max/total/total-count/rate-per-hour) | INFO | INFO |
| `failed-rebalance` (total/rate-per-hour) | INFO | INFO |
| `last-rebalance-seconds-ago` (gauge) | INFO | INFO |
| `assigned-partitions` (gauge) | INFO | INFO |
| `partition-{revoked,assigned,lost}-latency` (avg/max) | INFO | INFO |

## Concurrency / value-neutral idioms

- `recordRebalanceStarted/Ended` and `maybeRecordRebalanceFailed` are per-rebalance (one per
  reconcile cycle / heartbeat-failure), NOT per-record. Allocation-light, no hot path.
- `lastRebalanceEndMs`/`lastRebalanceStartMs`: Java plain `long` mutated from the membership
  state machine, read by the `last-rebalance-seconds-ago` gauge closure. The gauge closure
  runs on the metric-read path (a different thread). Rust: `lastRebalanceEndMs` in
  `Arc<AtomicI64>` (init -1) so the closure can read while the record path writes — same
  value-neutral swap M4 used for `last-heartbeat-ms`. `lastRebalanceStartMs` is read+written
  only on the record path (membership task), kept in the same `Arc<AtomicI64>` form for
  symmetry and to let `rebalance_started()` be a `&self` query.
- `assigned-partitions` gauge: Java captures the `SubscriptionState` ref and calls
  `numAssignedPartitions()` on the metric-read path. Rust: closure captures
  `Arc<Mutex<SubscriptionState>>`, locks briefly to read the count (metric-read path, low
  frequency, never per-record). Lock is dropped immediately; no `.await` involved.

## Wiring

### `ConsumerRebalanceMetricsManager` → membership state machine

Java records in `AbstractMembershipManager`:
- `transitionTo(...)`: `recordRebalanceEnded` when `isCompletingRebalance` (RECONCILING →
  STABLE|ACKNOWLEDGING); `recordRebalanceStarted` when `isStartingRebalance`
  (!RECONCILING → RECONCILING). (`AbstractMembershipManager.java:239-244`)
- `onHeartbeatFailure(retriable)`: `maybeRecordRebalanceFailed()` when `!retriable`.
  (`AbstractMembershipManager.java:301-304`)

Rust currently DROPS metrics in the membership manager (ctor comment). M5 threads an
`Option<Arc<ConsumerRebalanceMetricsManager>>` + a metrics-`Time` clock onto `MembershipInner`
(via `AbstractMembershipManager::new`) and records at the two faithful sites:
- `MembershipInner::transition_to` — add the `isCompletingRebalance`/`isStartingRebalance`
  gate + record (using the threaded clock's `milliseconds()`).
- `AbstractMembershipManager::on_heartbeat_failure` — honor the `retriable` flag (the Rust
  signature already takes it but ignored it) and call `maybe_record_rebalance_failed()` when
  `!retriable`.

`None` → no-op (tests that build a membership manager without metrics). Live path
(`async_kafka_consumer.rs` ctor) always sets it from the consumer's `Arc<Metrics>`.

### `RebalanceCallbackMetricsManager` → listener invoker

Java records in `ConsumerRebalanceListenerInvoker` AFTER the listener returns successfully
(latency = `time.milliseconds() - startMs`); on a thrown exception the record call is skipped
(`ConsumerRebalanceListenerInvoker.java:63-65,93-95,123-125`). Rust mirrors: the invoker
gets an `Option<RebalanceCallbackMetricsManager>` + a metrics-`Time` clock; `invoke_*` capture
`start` before the `.await`, and on the `Ok(())` arm record `now - start`. No recording on
error/wakeup arms.

§31 safety: recording is added only around the EXISTING invoke calls; the invocation
thread/handshake is unchanged, no lock held across the `.await`.

## Metrics ownership plumbing (M3 field + M4 setter pattern)

- Managers register against the consumer's `Arc<Metrics>` (M3's `metrics` field), same as M4.
- `RebalanceCallbackMetricsManager`: built in the consumer ctor; the invoker gains a
  `set_metrics(manager, time)` setter (M4 `set_*_metrics_manager` precedent). `None` in tests.
- `ConsumerRebalanceMetricsManager`: built in the consumer ctor; threaded into the membership
  manager ctor as `Option<Arc<...>>` + clock. `None` in the membership-manager unit tests.
- Confirm the live `AsyncKafkaConsumer::new` path constructs and wires both.

## Tests

- `ConsumerRebalanceMetricsManagerTest` (8) — inline `#[cfg(test)] mod tests`, MockTime + a
  real `SubscriptionState`, assert exact values (latency avg/max/total, rebalance-total,
  rate-per-hour, failed-total/rate, last-rebalance-seconds-ago, assigned-partitions, the
  started flag, mixed/consecutive scenarios).
- `RebalanceCallbackMetricsManagerTest` (1) — inline, assert per-callback avg/max.
- Skips: none. (The Java tests `mock(LogContext.class)` — Rust uses `log`, dropped.)

## Verification per commit
`cargo build` / `cargo test --lib` / `cargo xtask lint` / `cargo xtask format-check` green.
