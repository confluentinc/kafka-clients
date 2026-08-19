---
name: phaseM1-metrics-core-notes
description: Milestone-9 Phase M1 metrics-core foundation — module layout, hybrid concurrency choices, Sensor/Metrics circular-ref break, M2 carry-overs
metadata:
  type: project
---

Milestone-9 (consumer metrics) Phase M1 landed the `common::metrics` foundation
(commit c71c9ba on consumer-impl). Plan: `~/.claude/plans/idempotent-dazzling-beacon.md`;
phase docs under `design/history/Milestone-9-metrics/Phase-M1-metrics-core/`.

**Why:** Largest remaining Java-parity gap (#5) — consumer had no metrics framework.
M1 = core types + simple stats; M2 = windowed/sampled; M3 = FetchMetricsManager
(perf-critical); M4-M7 = consumer manager wiring + public `metrics()` API; M8 = perf
re-baseline.

**How to apply (module layout):**
- `org.apache.kafka.common.MetricName/MetricNameTemplate/Metric` → `src/common/{metric_name,metric_name_template,metric}.rs` (NOT under metrics/ — they are package `common`, not `common.metrics`). Re-exported from `common`.
- `org.apache.kafka.common.metrics.*` → `src/common/metrics/` (file-per-class, re-export from `metrics/mod.rs`). `internals/` subdir for MetricsUtils.
- `MetricValue` enum (Double/String/Long/Int) is my erasure of Java's `Object` from `Metric.metricValue()` — lives in `common/metric.rs`, re-exported by metrics for convenience.

**Hybrid concurrency decisions (value-identical to Java, reuse in M2):**
- Pure-counter stats store `f64::to_bits()` in `AtomicU64`: `Value`=store (last-wins),
  `CumulativeSum`=compare_exchange_weak add loop, `CumulativeCount`=wraps CumulativeSum
  recording 1.0. Lock-free; identical IEEE-754. `Stat::record(&self,...)` and
  `Measurable::measure(&self,...)` both take `&self` (interior mutability) — this is
  REQUIRED because Sensor holds `Vec<Box<dyn Stat>>` and records without `&mut`.
- M2 SampledStat needs multi-field mutation (ring buffer) → `std::Mutex` around the
  sample buffer (taken per-fetch, NEVER per-record).
- Registry = `Mutex<HashMap<MetricName,Arc<KafkaMetric>>>`; sensors/children maps same.
- KafkaMetric.config behind `Mutex<Arc<MetricConfig>>` (Java volatile + synchronized setter).

**Sensor↔Metrics circular reference break:** extracted `MetricsShared` (the metrics map
+ reporters) into `Arc<MetricsShared>`; `Sensor` holds `Option<Arc<MetricsShared>>`
(None = standalone sensor, mirrors Java's `null` registry in SensorTest). `Metrics`
owns the `Arc<MetricsShared>` + sensors/children maps. `register_metric` lives on
MetricsShared (called by both Metrics::add_metric and Sensor::add). No ownership cycle.

**Sensor.add MeasurableStat dual-view:** a `MeasurableStat` must be the SAME object both
as the recordable `Stat` in the sensor AND the `Measurable` provider in the KafkaMetric
(so a record reflects in the measured value). Held behind `Arc<dyn MeasurableStat>`,
wrapped in `StatArc`/`MeasurableArc` newtypes to expose each view.

**StatAndConfig:** M1 implements only the FromMetric supplier (stat reads
`metric.config()`); the `add(CompoundStat, fixed-config)` path is M2 with windowed stats.

**Time:** metrics core takes `Arc<dyn Time>` (minimal mirror of Java's `Time`:
`milliseconds()`/`nanoseconds()`). Did NOT reuse consumer's `ThreadTime` (pub(crate) to
consumer module, only exposes milliseconds()). Test `MockTime` in `time::mock`
(`#[cfg(test)]`), manually advanced via `sleep()`.

**Sensor-expiry:** no scheduler thread; exposed `Metrics::expire_sensors()` for explicit
driving (Java ExpireSensorTask equivalent). `children_sensors` keyed by parent NAME
(unique) not Sensor identity.

**Naming gotchas:** Java overloaded `record(double)` vs `record(double,long)` vs `record()`
→ Rust `record(value)` / `record_at(value, time_ms)` / `record_occurrence()` (can't
overload). Metrics::metricName overloads → `metric_name` / `metric_name_group` /
`metric_name_key_value` (returns Result on odd pairs).

**Panic vs Result:** `RecordingLevel::should_record` unknown config_id → panic (unreachable
internal invariant, Java IllegalStateException). `MetricConfig::with_samples(<1)` → panic
(builder programming error, Java IllegalArgumentException). `get_tags` odd pairs / sensor
circular-dep / duplicate-metric-name → Result (recoverable, surfaced to API).

**M2 carry-overs (documented in PLAN.md skips):** SampledStat/WindowedSum/WindowedCount/
Rate/SimpleRate/Avg/Max/Min/Meter; full MetricsTest.testSimpleStats (Avg/Max/Min/Rate/
Meter/Percentiles rows); quota ENFORCEMENT (checkQuotas) + TokenBucket; CompoundStat +
Sensor::add(CompoundStat); the SensorTest quota/Mockito-stat-verification tests;
MetricsTest concurrent-read-update. CumulativeCount was substituted for WindowedCount in
the hierarchical/remove tests (same record()==+1 semantics for those assertions).

**Out of scope (M1 + whole effort):** JmxReporter/MBean/MetricsContext (JVM-only),
KIP-714 telemetry/clientInstanceId, Percentiles/Frequencies, producer wiring.
