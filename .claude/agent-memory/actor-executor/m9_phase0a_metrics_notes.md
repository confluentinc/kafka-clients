---
name: m9-phase0a-metrics-notes
description: Milestone-9 Phase 0a (common.metrics core+stats) landed; key design decisions Phase 0b/4/5 must build on, and deferrals
metadata:
  type: project
---

Phase 0a translated `common.metrics` interfaces + `stats/` (everything EXCEPT `Metrics`,
full `Sensor`, `MetricsTest`, `SensorTest`, `SampleMetrics` — those are Phase 0b). Landed
as 6 commits on branch `milestone-9-client-telemetry` (see `git log --grep "Phase 0a (M9)"`).

**Why these notes:** the shared-mutable-stat topology and the erased-value modeling were the
hard calls; Phase 0b (Sensor/Metrics) and Phase 4 (KafkaMetricsCollector) must honor them.

**How to apply — decisions Phase 0b/4/5 must respect:**
- `MetricValueProvider` is an **enum** (`Measurable(Arc<Mutex<dyn Measurable>>)` |
  `Gauge(Arc<Mutex<dyn Gauge>>)`), NOT a trait — Rust has no `Object`/wildcard. Java's bare
  `MetricValueProvider` and `Gauge<T>` are folded into the `Gauge` arm. `MetricValue` enum
  (Double/Long/Int/Str) stands in for `Object metricValue()`.
- `KafkaMetric` drops Java's separate `lock` param — the provider's own `Mutex` is the lock.
  Sensor (0b) shares the SAME `Arc<Mutex<dyn Measurable>>` between the sensor stat-list and
  each metric; do not clone/re-wrap (use the enum variant directly, not `from_measurable`).
- `Stat::record`/`Measurable::measure` take `&mut self`; sharing is via `Arc<Mutex<..>>`
  (the Mutex replaces Java's per-Sensor lock). `Measurable` has `as_any`/`as_any_mut`
  (Java `instanceof` replacement) — needed by the Phase-4 collector's WindowedCount/
  CumulativeSum type-switch and by MeterTest's `(Rate)`/`(CumulativeSum)` downcasts.
- `WindowedCount` and `CumulativeCount` are **standalone structs**, not subclasses of their
  Sum counterparts. Phase 4's `instanceof WindowedSum`/`instanceof CumulativeSum` checks must
  test BOTH the sum and the count type.
- `CompoundStat::stats()` returns `Vec<NamedMeasurable{ Arc<Mutex<dyn Measurable>> }>` sharing
  the stat's mutable state. `Meter` shares its `rate`/`total` Arcs; `Percentiles`/`Frequencies`
  hold their sampled state in an inner `Arc<Mutex<...Inner>>` so the per-bucket measurables
  (which capture that Arc) stay in sync with `record()` — Java relied on lambdas capturing `this`.
- `SampledStat` is a trait over a `SampledStatBase` field; `impl_sampled_stat_traits!` macro
  (in `stats/sampled_stat.rs`) generates the Stat/Measurable/MeasurableStat delegations. `Sample`
  has an `Option<Histogram>` for the percentile/frequency samples.
- `MetricConfig` fluent setters are `with_*` (Rust can't overload getter/setter name);
  `with_samples` returns `Result` (Java `IllegalArgumentException`). Context labels are
  `HashMap<String, Option<String>>` (Java nullable map values).
- `MetricsReporter` trait: `&self` methods, `Arc<KafkaMetric>` params, no reflective
  Configurable/Reconfigurable surface — Phase 5 may refine the exact param shape.
- `QuotaViolationError` is standalone (impl `Error`), NOT yet a `KafkaError` enum variant —
  Phase 0b (Sensor) decides that integration (kafka_error.rs is shared code, left untouched).
- `TimeUnit` is a minimal native enum in `common/metrics/time_unit.rs` (subset of
  `java.util.concurrent.TimeUnit`); `pub(crate)` internals: `metrics_utils::{convert,get_tags}`
  (`get_tags` carries `#[cfg_attr(not(test), allow(dead_code))]` until Metrics/0b uses it).

**Deferred to Phase 0b:** `FrequenciesTest.testWithMetricsStrategy1/2/3` (need `Metrics`+`Sensor`).
Skipped (documented): `JmxReporterTest`, `KafkaMbeanTest`, `IntGaugeSuiteTest`,
`PluginMetricsImplTest` (JMX/instrumentation/KIP-877 out of scope). `KafkaMetricTest`'s
lambda-identity assertion adapted to a value check; null-provider test dropped (Rust types).
