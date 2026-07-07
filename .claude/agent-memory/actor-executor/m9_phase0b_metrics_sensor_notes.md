---
name: m9-phase0b-metrics-sensor-notes
description: Milestone-9 Phase 0b (Metrics registry + Sensor) — LANDED; ownership cycle, locking, stat dual-view, count-metric type, quota/token-bucket test gotchas
metadata:
  type: project
---

Phase 0b (`Metrics.java` + full `Sensor.java` + MetricsTest/SensorTest + the 3 un-deferred
FrequenciesTest testWithMetricsStrategy* cases) is **DONE** on `milestone-9-client-telemetry`
(2114 lib tests green). Builds on Phase 0a (see [[m9-phase0a-metrics-notes]]).

**Why these notes:** the Sensor↔Metrics ownership cycle and the record-vs-read locking discipline
are the hard, non-obvious calls; future phases (collector, telemetry reporter) instantiate Metrics.

**How to apply — decisions:**
- **Ownership cycle**: `Metrics` wraps `Arc<MetricsCore>`. `Sensor` holds `registry: Weak<MetricsCore>`
  (avoids the cycle — MetricsCore.sensors holds `Arc<Sensor>`). `metrics.sensor(name)` returns
  `Arc<Sensor>`. Sensor.add upgrades the Weak to call `core.register_metric`.
- **Registry maps = DashMap** (metrics: `DashMap<MetricName, Arc<KafkaMetric>>`, sensors, children).
  DashMap (not a global `Mutex<HashMap>`) is deliberate: it removes the global-registry lock so the
  sensor-lock → registry ordering can't invert with removeSensor. `children_sensors` keyed by parent
  NAME (Java keys by Sensor identity; names are unique so equivalent). Reporters: `Mutex<Vec<Arc<dyn
  MetricsReporter>>>` — cloned (Arc) before firing callbacks so callbacks fire OUTSIDE the lock
  (constraint: a reporter may re-enter the registry).
- **Deadlock invariant (testConcurrentReadUpdateReport / LockingReporter)**: `KafkaMetric::metric_value`
  locks ONLY the stat's own `Mutex` (+ its config Mutex) — never the sensor/registry/reporter. That
  makes the stat a leaf lock, so a synchronized reporter reading metricValue can't deadlock with
  registration. Preserve this.
- **Stat dual-view sharing** (`add(MetricName, MeasurableStat)`): `add_metric<S: MeasurableStat +
  'static>` — wrap once as `Arc<Mutex<S>>`, then coerce the SAME allocation twice:
  `let m: Arc<Mutex<dyn Measurable>> = c.clone(); let s: Arc<Mutex<dyn Stat>> = c.clone();`. `m` → the
  KafkaMetric's `MetricValueProvider::Measurable` (read), `s` → the sensor's StatAndConfig (record).
  Both views hit the same data. `Arc<Mutex<dyn A>>` can NOT be upcast to `Arc<Mutex<dyn B>>` (Mutex
  isn't CoerceUnsized across dyn) — the coercion MUST happen from the concrete `Arc<Mutex<S>>`, so
  add_metric is generic over the concrete S (not `Box<dyn MeasurableStat>`).
- **StatAndConfig.config is a live supplier** `Box<dyn Fn() -> MetricConfig + Send + Sync>`. For
  add(MetricName, stat) it clones the KafkaMetric's CURRENT config (`move || metric.config()`), so
  `metrics.metric(name).set_config(new)` is reflected on the next record (testUpdatingMetricConfig...).
  For add(CompoundStat) it's the static `statConfig`.
- **Sensor.record / check_quotas return `Result<(), QuotaViolationError>`** (Java throws
  QuotaViolationException). check_quotas is inlined into record_internal (std Mutex is non-reentrant —
  do NOT have record_internal call the public check_quotas). QuotaViolationError stays STANDALONE
  (impl Error) — NOT added to the KafkaError enum: no Errors protocol code corresponds to it and no
  Phase-0b caller needs the conversion. `QuotaViolationError` holds **`Arc<KafkaMetric>`** (not
  `KafkaMetric` by value): the metric is already shared through the sensor + registry, and by-value
  made the `Err` variant ~208 B, tripping clippy `result_large_err` on every `record*`/`check_quotas*`
  signature. The Arc is documented as the sharing mechanism (no cross-language meta-commentary).
- **Expiration**: `expire_sensors()` is a callable pub(crate) method (Java ExpireSensorTask.run) — tests
  drive it synchronously (MetricsTest.testRemoveInactiveMetrics, SensorTest.testExpiredSensor). The
  periodic 30s tokio task is spawned only when enable_expiration AND `Handle::try_current().is_ok()`
  (so plain `#[test]` constructing Metrics(expiration=true) doesn't panic; clients don't enable it).
- **Built-in `kafka-metrics-count` metric is a Measurable → `MetricValue::Double`**, NOT a Gauge/Int.
  Java's `addMetric(metricName(...), (config, now) -> metrics.size())` binds to `addMetric(MetricName,
  Measurable)` (the only functional-interface overload; `MetricValueProvider` is an empty marker), and
  the `int` widens to `double`, so `metricValue()` is a `Double`. Implemented as `MetricsCountMeasurable
  { core: Weak<MetricsCore> }` → `metrics.len() as f64`. **Phase 4 (KafkaMetricsCollector)**: this metric
  is measurable (not a bare gauge) and its value is a Double — treat it accordingly in the sum/gauge
  type-switch (it is neither WindowedCount nor CumulativeSum, so it classifies as a gauge-like value).
- **`test_support` module** (`#[cfg(test)] pub(crate) mod test_support` in `metrics/mod.rs`, mirroring
  the `stats::test_time` precedent): `MockClock` (yields a `TimeSource` via `time_source()`; `new()` /
  `with_auto_tick(ms)` / `with_start(ms)`) + `FakeMetricsReporter` (no-op). Shared by metrics.rs,
  sensor.rs, and stats/frequencies.rs tests.
- **TokenBucket is start-time-sensitive; Rate is not.** TokenBucket's first `refill` fills relative to
  epoch 0 (initial `lastUpdateMs = 0`), so a test starting the clock at 0 gets no initial burst
  (credits `0-30 = -30`), whereas Java's `MockTime(0, currentTimeMillis, 0)` fills the full burst first
  (`20-30 = -10`). SensorTest strict-quota tests therefore use `MockClock::with_start(wall_clock_ms())`.
  Rate's `windowSize` is relative to the oldest sample's `lastWindowMs` (set at first record), so it is
  start-independent — its test passes from 0, but uses the same wall-clock start for parity.
- **Concurrency stress tests (`test_concurrent_read_update{,_report}`) run 1000 iterations**, not
  Java's 10000: the harness uses `Mutex<VecDeque>` (Java's lock-free `ConcurrentLinkedDeque`), so heavy
  deque-lock contention in the unoptimized test profile made 10000 iters take ~280 s; 1000 keeps the
  full lib suite at ~13 s while still surfacing deadlocks/races immediately. They use `std::thread::scope`
  (record/read/report threads); a background panic or poisoned lock propagates at scope join.
- **Deviations**: `toHtmlTable` SKIPPED (depends on excluded JmxReporter.getMBeanName; untested; JMX
  doc utility). Java `new JmxReporter()` in test setup → substituted with FakeMetricsReporter (no-op).
  Mockito `mock(MeasurableStat)` config-verify tests → a `RecordingMeasurableStat` test double capturing
  each received config; compared by quota bound (Rust passes a *cloned* config, so identity comparison
  like Mockito's isn't meaningful — value comparison captures "each stat records/measures with its own
  config"). Many Java constructor/method overloads → distinct Rust names (no overloading).
