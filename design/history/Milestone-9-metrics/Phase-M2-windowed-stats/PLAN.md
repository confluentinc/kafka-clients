# Phase M2 — Windowed / sampled stats (value-parity math)

Actor 42. Branch `consumer-impl`. Java package `org.apache.kafka.common.metrics.stats`
→ Rust module `common::metrics::stats` (`clients` MUST NOT appear). Builds on the M1
metrics core (`common::metrics`) — does NOT disturb M1 types/tests.

## Hybrid approach (user-confirmed) — VALUE PARITY is the whole point

Translate the Java windowed/sampled stats **faithfully** so the computed VALUES are
bit-for-bit identical to Java (`double`→`f64`, `long`→`i64`). The windowing math
(`current()` selection, `oldest()`, `isComplete`, `advance`, `purgeObsoleteSamples`,
`Rate.windowSize`) is reproduced line-for-line. Idiomatic concurrency ONLY where it
does not change values: a `std::sync::Mutex` around the `SampledStat` sample ring +
cursor (multi-field mutation; taken per-fetch / per-partition, never per-record).

## Classes in scope (Java → Rust mapping)

| Java source | Rust file | Notes |
|---|---|---|
| `metrics/CompoundStat.java` (+ `NamedMeasurable`) | `metrics/compound_stat.rs` | `CompoundStat: Stat` with `fn stats() -> Vec<NamedMeasurable>`; `NamedMeasurable { name, stat }`. |
| `metrics/stats/SampledStat.java` | `metrics/stats/sampled_stat.rs` | abstract base. `Mutex<SampledStatInner { samples, current, time_window_ms }>`; the per-subclass `update`/`combine`/`initial_value` provided via a `SampledStatKind` trait object held by the base, mirroring Java's abstract methods. `current/oldest/advance/purge_obsolete_samples/is_complete` reproduced exactly. |
| `metrics/stats/WindowedSum.java` | `metrics/stats/windowed_sum.rs` | `SampledStatKind`: initial 0.0, update `+= value`, combine `Σ value`. |
| `metrics/stats/WindowedCount.java` | `metrics/stats/windowed_count.rs` | WindowedSum that updates with `1.0`. |
| `metrics/stats/Avg.java` | `metrics/stats/avg.rs` | initial 0.0, update `+= value`, combine `Σvalue/Σcount` else NaN. |
| `metrics/stats/Max.java` | `metrics/stats/max.rs` | initial -inf, update max, combine max else NaN. |
| `metrics/stats/Min.java` | `metrics/stats/min.rs` | initial f64::MAX, update min, combine min else NaN. |
| `metrics/stats/Rate.java` | `metrics/stats/rate.rs` | holds a `SampledStat` + `unit` + `time_window_ms`; `measure = stat.measure / convert(windowSize)`; `windowSize` reproduced exactly. `windowSize` is virtual (SimpleRate overrides) → modeled via a `RateKind` strategy or a method on Rate dispatched by an enum/flag. |
| `metrics/stats/SimpleRate.java` | `metrics/stats/simple_rate.rs` | overrides `windowSize` = `max(elapsed, config.timeWindowMs)`. |
| `metrics/stats/Meter.java` | `metrics/stats/meter.rs` | `CompoundStat`: shared `Rate` + `CumulativeSum`; `stats()` yields total then rate; record fans into both (total records 1.0 when rate stat is WindowedCount). |
| `metrics/internals/MetricsUtils.convert` + `TimeUnit` | `metrics/internals/metrics_utils.rs` (extend) | add `TimeUnit` enum (NANOSECONDS..DAYS) + `convert(time_ms, unit) -> f64`. `get_tags` already present from M1. |

### `Sensor` extension (faithful to Java `Sensor.add(CompoundStat)`)

Java `Sensor.add(CompoundStat, config)` adds ONE `StatAndConfig` holding the compound
stat with a CONSTANT config supplier (`() -> statConfig`), then registers one
`KafkaMetric` per `NamedMeasurable` child (each reading `statConfig`). On `record`, the
sensor records into the compound stat once; the compound stat fans out to its children
internally; each child metric `measure`s its own sub-stat. M1's `StatAndConfig` couples
one stat to one metric's config — M2 generalizes it to also support the
constant-config + decoupled-stat shape, and adds `Sensor::add_compound` /
`add_compound_with_config`. The sub-stats exposed via `NamedMeasurable` MUST be the SAME
objects the compound stat records into (shared via `Arc`) so a record is reflected in
the measured value.

### Concurrency (per class — why value-identical to Java)

- **`SampledStat`** — single `std::sync::Mutex<SampledStatInner>` guarding `samples`,
  `current`, `time_window_ms`. Java guards these with the sensor's `synchronized`; the
  Mutex is the minimal equivalent. Taken per-fetch/per-partition (NEVER per-record on
  the hot path — see CLAUDE.md §11/§27). No per-record `Vec` growth: the ring grows once
  to `samples()+1` then recycles in place (Java's `advance` reuses slots). `record` and
  `measure` take `&self`.
- **`Rate`/`SimpleRate`** — delegate to the inner `SampledStat`'s Mutex; Rate itself has
  only immutable config fields (`unit`, `time_window_ms`).
- **`Meter`** — `Arc<Rate>` + `Arc<CumulativeSum>`, both internally synchronized; `record`
  records into each. `stats()` returns `Measurable` views over the same `Arc`s.

These swaps change only the JVM concurrency idiom; metric **values** are bit-for-bit
Java-identical.

## Tests translated (Java test → Rust test)

### Dedicated stats test files
From `common/metrics/stats/`:
- `SampledStatTest.java`:
  - `testSampleIsPurgedIfDoesntOverlap` → `test_sample_is_purged_if_doesnt_overlap`
  - `testSampleIsKeptIfOverlaps` → `test_sample_is_kept_if_overlaps`
  - `testSampleIsKeptIfOverlapsAndExtra` → `test_sample_is_kept_if_overlaps_and_extra`
  - (`SampleCount` test helper subclass translated as a test-only `SampledStatKind`.)
- `RateTest.java`:
  - `testRateWithNoPriorAvailableSamples` (`@ParameterizedTest @CsvSource {1,1 / 1,11 / 11,1 / 11,11}`) → `test_rate_with_no_prior_available_samples` (loop over the 4 param rows).
  - `testRateIsConsistentAfterTheFirstWindow` → `test_rate_is_consistent_after_the_first_window`.
- `MeterTest.java`:
  - `testMeter` → `test_meter` (100×100ms loop; asserts total cumulative + windowed rate against the hand-computed sampled total).

### M1-deferred MetricsTest / SensorTest rows now CLOSED in M2

These were explicitly deferred in the M1 PLAN's "Skips" section; M2 owns them. The
windowing rows are translated as **stat-level unit tests** (the MetricsTest method drives
a stat directly via `MockTime`), which is the faithful and value-identical form:
- `MetricsTest.testTimeWindowing` → `windowed_count.rs::test_time_windowing`.
- `MetricsTest.testOldDataHasNoEffect` → `max.rs::test_old_data_has_no_effect`.
- `MetricsTest.testSampledStatReturnsNaNWhenNoValuesExist` → `sampled_stat.rs::test_sampled_stat_returns_nan_when_no_values_exist` (Max/Min/Avg).
- `MetricsTest.testSampledStatReturnsInitialValueWhenNoValuesExist` → `sampled_stat.rs::test_sampled_stat_returns_initial_value_when_no_values_exist` (WindowedCount/WindowedSum).
- `MetricsTest.testSimpleRate` → `simple_rate.rs::test_simple_rate` (full method; drives a `SimpleRate` directly with `MockTime`).
- `MetricsTest.testRateWindowing` → `metrics.rs` integration test `test_rate_windowing` (registry + `Sensor.add(Meter)` + windowSize cast; needs `add_compound`).
- `MetricsTest.testSimpleStats` → `metrics.rs` integration test `test_simple_stats` — the
  Avg/Max/Min/Rate/occurrences(Meter+WindowedCount)/count(WindowedCount)/CumulativeSum
  rows (M1 only asserted the CumulativeSum row). The **Percentiles** row is omitted
  (out of scope, see Skips) and noted in the test.

(The remaining M1-deferred SensorTest quota/Mockito rows — `testCheckQuotasInMultiThreads`,
`testStrictQuotaEnforcement*`, `testRecordAndCheckQuotaUseMetricConfigOfEachStat`,
`testUpdatingMetricConfigIsReflectedInTheSensor`, `testConcurrentReadUpdate*` — depend on
full **Quota enforcement** (`checkQuotas`/`QuotaViolationException`), which is NOT in this
phase's scope (windowed stats only). They remain deferred; see Skips. M2 unblocks the
*stats* they need but not quota *enforcement*.)

### Skips (with rationale)

- **`Percentiles`/`Frequencies`/`Histogram`** (`Percentiles.java`, `Frequencies.java`,
  `Histogram.java`, `Percentile.java`, `Frequency.java`, `PercentilesTest`,
  `FrequenciesTest`, `HistogramTest`) — the consumer does not use them (task brief).
  Documented out of scope. `MetricsTest.testPercentiles*`, `shouldPinSmallerValuesToMin`,
  `shouldPinLargerValuesToMax`, `testPercentilesWithRandomNumbersAndLinearBucketing`
  skipped accordingly.
- **`TokenBucket`** (`TokenBucket.java`) — used only by quota enforcement / share-consumer
  rate-limiting; the consumer's metric set does not use it and full Quota enforcement is
  out of M2 scope. Skipped (no dedicated test file in 4.2).
- **Quota-enforcement SensorTest/MetricsTest rows** (`testQuotas`, the
  `checkQuotas`/`QuotaViolation` rows) — require `Sensor.checkQuotas` + windowed-stat
  quota math; deferred with quota enforcement (a later phase). M2 lands the windowed
  *stats*, not quota *enforcement*.
- **`testConcurrentReadUpdate*`** (MetricsTest) — concurrency stress; the atomic/Mutex
  stat concurrency is exercised by the value-parity tests here. Deferred.
- **`testMetricInstances`** (MetricsTest) — exercises `metricInstance`/`MetricNameTemplate`
  tag inheritance, NOT M2 stats (it constructs no windowed stat). Belongs to the
  MetricName/template surface already in M1; not an M2 row.

## Rate / SampledStat value-parity verification (self-review)

Hand-verified against the Java expected literals:
1. `RateTest.testRateWithNoPriorAvailableSamples` row `1,1` (numSample=1,
   window=1s): record 50 at t=0, sleep 999ms, measure at t=999.
   `windowSize`: oldest.start=0 → totalElapsed=999ms; windowMs=1000;
   numFullWindows=0; minFullWindows=samples-1=0; no padding → max(999,1)=999ms.
   `convert(999, SECONDS)=0.999`. rate = 50/0.999 ≈ 50.05. The Java test computes
   `windowSize = convert(999, SECONDS) + 0*1 = 0.999`, `expected = 50/0.999`. MATCH.
2. `MetricsTest.testSimpleStats` Avg row: records 0..9 → Σ=45, count=10 →
   Avg=45/10=4.5. Java asserts 4.5. MATCH. Max=9, Min=0. Rate row: occurrences
   uses WindowedCount-Meter; `elapsedSecs = timeWindowMs*(samples-1)/1000 =
   30000*1/1000 = 30s` then +2s sleep = 32s? — Java recomputes per the method;
   reproduced by driving the real stat through the registry with `MockTime`.
3. `SampledStatTest.testSampleIsKeptIfOverlapsAndExtra`: 2 complete samples + 1 live,
   monitored window 2s, measured at 2.2s. `SampleCount.combine = Σ value` and each
   completed/overlapping sample has value 1 → 3. Java asserts 3. MATCH (validates
   `purgeObsoleteSamples` keeps the n+1 overlapping sample).

## DoD

`cargo build`, `cargo test --lib`, `cargo xtask lint`, `cargo xtask format-check` all
green after each commit group. Public surface: the stats + `CompoundStat`/`NamedMeasurable`
+ `TimeUnit` re-exported from `common::metrics` / `common::metrics::stats` per CLAUDE.md
(stats package is NOT under `internal`, so public is correct, matching Java's `public`
stats classes). No new public concepts beyond Java's.

## Commit groups

1. `M2: SampledStat + WindowedSum/Count` — TimeUnit+convert, CompoundStat/NamedMeasurable,
   SampledStat base + kind trait, WindowedSum, WindowedCount + their tests
   (SampledStatTest, testTimeWindowing, testSampledStat*Initial).
2. `M2: Rate/SimpleRate/Avg/Max/Min` — Rate, SimpleRate, Avg, Max, Min + RateTest,
   testSimpleRate, testOldDataHasNoEffect, testSampledStat*NaN.
3. `M2: Meter + CompoundStat sensor registration` — Meter + Sensor::add_compound + MeterTest.
4. `M2: integration tests` — MetricsTest.testSimpleStats + testRateWindowing (registry).
