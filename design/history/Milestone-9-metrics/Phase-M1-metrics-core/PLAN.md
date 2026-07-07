# Phase M1 — Metrics core + simple stats (foundation)

Actor 41. Branch `consumer-impl`. Java package `org.apache.kafka.common.metrics`
→ Rust module `common::metrics` (`clients` MUST NOT appear). Foundation only —
no consumer wiring (that is Phases M3+).

## Hybrid approach (user-confirmed)

Translate the Java metrics core **faithfully** (same class names PascalCase→struct,
file-per-class, snake_case methods, same semantics/values, recording-level gating,
metric names/groups/tags), with **idiomatic Rust concurrency**: Java `synchronized`
→ the minimal Rust atomic/lock equivalent. **Nothing observable differs from Java.**
`double` → `f64`, `long` → `i64`, `int` → `i32`. No JMX — a `MetricsReporter` trait
(no-op default) replaces `JmxReporter`.

## Classes in scope (Java → Rust mapping)

| Java source | Rust file | Notes |
|---|---|---|
| `common/MetricName.java` | `common/metric_name.rs` | **public**. equals/hashCode use (name, group, tags) only — description excluded. |
| `common/MetricNameTemplate.java` | `common/metric_name_template.rs` | used by `Metrics::metric_instance`; tags = ordered set. |
| `common/Metric.java` | `metrics/metric.rs` | **public** read interface: `metric_name()` + `metric_value()`. |
| `metrics/MetricConfig.java` | `metrics/metric_config.rs` | quota kept as `Option<Quota>` placeholder (full quota = M2). |
| `metrics/Quota.java` | `metrics/quota.rs` | minimal (needed by MetricConfig + equality test). |
| `metrics/Stat.java` | `metrics/stat.rs` | `fn record(&self, &MetricConfig, f64, i64)`. |
| `metrics/Measurable.java` | `metrics/measurable.rs` | `fn measure(&self, &MetricConfig, i64) -> f64`. |
| `metrics/MeasurableStat.java` | `metrics/measurable_stat.rs` | `Stat + Measurable` supertrait. |
| `metrics/Gauge.java` | `metrics/gauge.rs` | value-provider producing a non-measurable value. |
| `metrics/MetricValueProvider.java` | `metrics/metric_value_provider.rs` | enum: `Measurable` or `Gauge` (Java erased generic over Double/T). |
| `metrics/KafkaMetric.java` | `metrics/kafka_metric.rs` | `metric_value()` / `measurable_value()`; impl `Metric`. |
| `metrics/Metrics.java` | `metrics/metrics.rs` | registry subset (consumer-relevant): `metric_name`, `sensor`, `add_metric`, `remove_metric`, `register_metric`, `metrics()`, `metric()`, `remove_sensor`, `get_sensor`, reporters, `metric_instance`, `ExpireSensorTask`. No scheduler thread / no JMX. |
| `metrics/Sensor.java` | `metrics/sensor.rs` | `record`, `shouldRecord`, parent/child chain, `add`, expiry, `RecordingLevel` enum. |
| `metrics/Sensor.RecordingLevel` | (in `sensor.rs`) | INFO/DEBUG/TRACE + `should_record(config_id)`, `for_id`, `for_name`. |
| `metrics/MetricsReporter.java` | `metrics/metrics_reporter.rs` | trait `init`/`metric_change`/`metric_removal`/`close`; no-op blanket-free default behaviour. NO JmxReporter. |
| `metrics/stats/Value.java` | `metrics/stats/value.rs` | instantaneous value. |
| `metrics/stats/CumulativeSum.java` | `metrics/stats/cumulative_sum.rs` | non-sampled total. |
| `metrics/stats/CumulativeCount.java` | `metrics/stats/cumulative_count.rs` | CumulativeSum that always records 1. |
| `metrics/internals/MetricsUtils.getTags` | `metrics/internals/metrics_utils.rs` (subset) | only `get_tags` (pairs→map). |

`MetricsContext` / `KafkaMetricsContext` / `Monitorable` / `PluginMetrics` /
`CompoundStat` / `NamedMeasurable`: **deferred** (see skips). `CompoundStat` is
needed only by `Meter`/`Percentiles` (M2); `Sensor::add(CompoundStat)` is deferred
to M2 with the windowed stats — the `MeasurableStat` `add` overload (the one the
consumer fetch/commit metrics use) is implemented now.

## Concurrency decisions (per class — why value-identical to Java)

- **`Value`** — `AtomicU64` holding `f64::to_bits`. `record` = `store` (last value
  wins, exactly Java's `this.value = value`); `measure` = `load`. Java guards the
  field with the sensor's `synchronized`; a single atomic store/load is the minimal
  equivalent and produces the identical observable value. `&self` record.
- **`CumulativeSum`** — `AtomicU64` holding `f64::to_bits` of the running total,
  updated with a `compare_exchange_weak` add loop. Identical arithmetic to Java's
  `total += value` (IEEE-754 `f64` addition); the CAS only serializes concurrent
  adds, exactly what Java's `synchronized` does. `&self` record.
- **`CumulativeCount`** — wraps a `CumulativeSum`, records `1.0` (Java extends
  CumulativeSum, overrides record to pass 1). Same atomic, same value.
- **`Sensor`** — stats held as `Vec<Box<dyn Stat>>` (cold registration under a
  `Mutex` for the add path; `record` iterates the snapshot). Because each `Stat`
  is internally lock-free (atomics), `record` needs no per-stat lock — it mirrors
  Java's `recordInternal` loop. `last_record_time` = `AtomicI64`. Parents = `Vec<Arc<Sensor>>`.
- **`Metrics`** — `Mutex<HashMap<MetricName, Arc<KafkaMetric>>>` for metrics,
  `Mutex<HashMap<String, Arc<Sensor>>>` for sensors, `Mutex<HashMap<.., Vec<Arc<Sensor>>>>`
  for children. Mirrors Java's `ConcurrentHashMap` + `synchronized` methods. The
  `synchronized` ordering in Java (Sensor→Metrics→Reporter) is preserved by lock
  acquisition order; reporter callbacks run outside the data locks where Java does.
- **`KafkaMetric`** — `config` behind `Mutex<Arc<MetricConfig>>` (Java `volatile` +
  `synchronized` setter). value read delegates to the stat's atomic — no extra lock
  needed because the stat is itself atomic (Java needed `metricLock` because the
  stat field was a plain Java field).

These swaps change only the JVM concurrency idiom; metric **values**, names, groups,
tags, and recording-level gating are bit-for-bit Java-identical.

## `now_ms` / time

Java `Stat.record`/`Measurable.measure` take `long now` directly — the **caller**
passes the time, so the metrics core needs no internal clock. `Sensor::record(value, now)`
takes `now` as a parameter (matching Java's `record(double, long)`); the convenience
`record(value)` that reads `time.milliseconds()` and `KafkaMetric.metricValue()`
(which reads `time`) need a `Time`. We thread an `Arc<dyn Time>` (a minimal local
`Time` trait: `milliseconds()`/`nanoseconds()`/`sleep()`) into `Metrics`/`Sensor`/
`KafkaMetric`, mirroring Java's `org.apache.kafka.common.utils.Time`. A `SystemTime`
impl + a test `MockTime` (manually advanced) are provided. This is the faithful
translation of Java's `Time` (the consumer's `ThreadTime` is `pub(crate)` to the
consumer module and only exposes `milliseconds()`, so it is not reusable from
`common`; mirroring Java's `Time` minimally is cleaner and is what Java does).

## Tests translated (Java test → Rust test)

From `common/metrics/SensorTest.java`:
- `testRecordLevelEnum` → `test_record_level_enum`
- `testShouldRecordForInfoLevelSensor` → `test_should_record_for_info_level_sensor`
- `testShouldRecordForDebugLevelSensor` → `test_should_record_for_debug_level_sensor`
- `testShouldRecordForTraceLevelSensor` → `test_should_record_for_trace_level_sensor`
- `testIdempotentAdd` → `test_idempotent_add` (uses CumulativeSum/Value in place of Avg/WindowedSum — see note)
- `shouldReturnPresenceOfMetrics` → `should_return_presence_of_metrics`
- `testExpiredSensor` → partially: expiry-on-`add` path with `Value` (the Avg/Meter
  parts need M2 stats; the expiry **mechanism** is what M1 owns).

From `common/metrics/MetricsTest.java` (M1-relevant subset):
- `testMetricName` → `test_metric_name` (metricName two-ways equality + odd keyValue → error)
- `testHierarchicalSensors` → `test_hierarchical_sensors` (uses CumulativeCount in place of WindowedCount — same `record()`==+1 semantics for this assertion)
- `testBadSensorHierarchy` → `test_bad_sensor_hierarchy`
- `testRemoveChildSensor` → `test_remove_child_sensor`
- `testRemoveSensor` → `test_remove_sensor` (CumulativeCount substituted for WindowedCount)
- `testRemoveMetric` → `test_remove_metric` (CumulativeCount substituted)
- `testRemoveInactiveMetrics` → `test_remove_inactive_metrics` (ExpireSensorTask + MockTime; CumulativeCount substituted)
- `testDuplicateMetricName` → `test_duplicate_metric_name`
- `testSimpleStats` → **split**: the M1 part `test_simple_stats_cumulative` asserts
  the `CumulativeSum` row (`s2.total == 5.0`) and `CumulativeCount`; the Avg/Max/Min/
  Rate/Meter/Percentiles/WindowedCount rows are M2 (skipped here, full method translated in M2).
- `testQuotasEquality` → `test_quotas_equality` (Quota.equals; needs minimal Quota).

Simple-stat behaviour tests (Java has no standalone Value/CumulativeSum test files —
these stats are exercised through MetricsTest; we add focused unit tests for value parity):
- `test_value_records_last` — Value returns the last recorded value.
- `test_cumulative_sum` — sum of recorded values.
- `test_cumulative_count` — counts invocations regardless of value.

### Skips (with rationale)

- **`MetricNameTest.java`** — does not exist in Apache Kafka 4.2 (MetricName equality
  is covered by `MetricsTest.testMetricName`, which IS translated). No file to skip;
  noted because the task brief referenced it.
- **`JmxReporterTest`, `KafkaMbeanTest`, `KafkaMetricsContextTest`** — JMX / MBean /
  metrics-context are out of scope (JVM-only; replaced by the `MetricsReporter` trait).
- **`KafkaMetricTest`** — exercises `Measurable`/`Gauge` value reads; the M1-relevant
  assertions (measurable vs gauge `metricValue`, `measurableValue` of a non-measurable
  returns 0) are folded into `kafka_metric.rs` unit tests. The Avg/Rate rows are M2.
- **`MetricsTest` rows needing M2 stats** — `testSimpleStats` (Avg/Max/Min/Rate/Meter/
  WindowedCount/Percentiles), `testTimeWindowing`, `testOldDataHasNoEffect`,
  `testSampledStat*`, `testRateWindowing`, `testSimpleRate`, `testPercentiles*`,
  `shouldPin*`, `testMetricInstances` (Meter), `testQuotas` (Rate). Translated in M2.
- **`testCheckQuotasInMultiThreads`, `testStrictQuotaEnforcement*`,
  `testRecordAndCheckQuotaUseMetricConfigOfEachStat`,
  `testUpdatingMetricConfigIsReflectedInTheSensor`** (SensorTest) — depend on
  `Rate`/`TokenBucket`/Mockito stat verification + full Quota enforcement; the
  Quota *equality* and *plumbing* land in M1, Quota *enforcement* (`checkQuotas`)
  + windowed stats land in M2.
- **`testConcurrentReadUpdate*`** (MetricsTest) — concurrency stress with Avg/Rate;
  M2 (the atomic-stat concurrency is unit-tested here instead).

## DoD

`cargo build`, `cargo test --lib` (new tests green), `cargo xtask lint`,
`cargo xtask format-check` all green after each commit. Public surface limited to
`MetricName`, `MetricNameTemplate`, `Metric`, `Metrics`, `Sensor`, `MetricConfig`,
`KafkaMetric`, the traits, `MetricsReporter`, and the simple stats — re-exported from
`common::metrics` per CLAUDE.md.

## Commit groups

1. `M1: metrics core types` — MetricName, MetricNameTemplate, Metric, MetricConfig,
   Quota, traits (Stat/Measurable/MeasurableStat/Gauge/MetricValueProvider),
   Time, KafkaMetric, MetricsReporter + module wiring.
2. `M1: Metrics registry + Sensor` — Metrics, Sensor, RecordingLevel, ExpireSensorTask,
   MetricsUtils.get_tags.
3. `M1: simple stats + tests` — Value, CumulativeSum, CumulativeCount + all translated tests.
