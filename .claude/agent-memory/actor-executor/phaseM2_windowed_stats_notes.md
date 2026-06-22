---
name: phaseM2-windowed-stats
description: Milestone-9 Phase M2 windowed/sampled stats — SampledStatKind abstract-method seam, StatConfigSource for Sensor.add(CompoundStat), value-parity proof, into_sampled_stat seam, no-Any-downcast deviation
metadata:
  type: project
---

Phase M2 (Actor 42) translated `org.apache.kafka.common.metrics.stats` windowed/sampled
stats into `common::metrics::stats`, building on M1's metrics core. Commits: `2efeb96`
(stats + sensor), `d047996` (MetricsTest integration tests). All under `consumer-impl`.

**Why:** value-parity-critical phase — computed Rate/Avg/Max/Meter values must equal Java
to f64 precision after clock advances across sample windows. Windowing math reproduced
line-for-line; idiomatic concurrency only where it doesn't change values.

**How to apply (reusable patterns):**

- **Java abstract-method base → `Kind` trait object.** `SampledStat` is abstract in Java
  with subclass `update`/`combine` + ctor `initialValue`. Modelled as a concrete
  `SampledStat` struct holding `Box<dyn SampledStatKind>` (the abstract methods) + the
  `initial_value` field. `newSample` is NOT virtual in Java → stays on the struct. This
  is the go-to shape for Java abstract-class-with-template-methods.
- **`instanceof` checks → discriminator default methods on the Kind trait.** Meter does
  `rate.stat instanceof WindowedCount` / `instanceof WindowedSum`. Added
  `is_windowed_count()`/`is_windowed_sum()` to `SampledStatKind` (default `false`,
  overridden in WindowedSumKind/WindowedCountKind). WindowedCount extends WindowedSum in
  Java so its kind returns `true` for BOTH. SampledStat exposes `pub(crate)` accessors.
- **`std::sync::Mutex<inner>` for the sample ring; `&self` record/measure.** One Mutex
  guards `samples` Vec + `current` cursor + `time_window_ms`. Ring grows once to
  `samples()+1` then recycles in place (`advance` reuses slots) — no per-record Vec growth.
  Take it per-fetch/per-partition, never per-record (CLAUDE.md §11/§27).
- **`Sensor.add(CompoundStat)` ≠ `add(MeasurableStat)`.** Java's StatAndConfig holds a
  `Supplier<MetricConfig>`: for the MeasurableStat path it's `metric::config`; for the
  CompoundStat path it's a CONSTANT `() -> statConfig`, and the ONE recordable compound
  stat is decoupled from the per-child KafkaMetrics. Generalized M1's `StatAndConfig` to
  an enum `StatConfigSource { FromMetric(Arc<KafkaMetric>) | Constant(Arc<MetricConfig>) }`.
  Child measurables (NamedMeasurable.stat()) MUST be the SAME `Arc` objects the compound
  stat records into, so a record reflects in every child's measured value.
- **Wrapper-owns-private-SampledStat + `pub(crate) into_sampled_stat()` seam.** Rate/Meter
  hold an `Arc<SampledStat>` directly (Java `Rate.stat` is a `SampledStat`); the public
  wrappers (WindowedSum/WindowedCount) expose `into_sampled_stat(self)` for them. The
  count-Meter production path (`Meter::with_stat(WindowedCount::new().into_sampled_stat())`)
  lands in M3, so WindowedCount's seam carries `#[cfg_attr(not(test), allow(dead_code))]`
  with a comment — accepted convention for staged cross-phase API under `#![deny(warnings)]`.
- **`Rate.unitName()` Java quirk:** `name().substring(0, length-2).toLowerCase()` →
  "SECONDS" yields "secon" (NOT "second"). Faithful, not a bug — assert "secon".
- **SimpleRate composes a Rate** (Java extends Rate, overrides only `windowSize`); Rust has
  no inheritance, so SimpleRate re-implements `measure` calling its own `window_size`.

**Deviation (documented, not a bug):** Java's `testRateWindowing` casts
`(Rate) rateMetric.measurable()` to call `windowSize`. The erased `MetricValueProvider`
has no `Any` downcast seam, and adding one would touch every M1 `Measurable` impl. That
single assertion is NOT ported; it's covered indirectly (rate value = sampledValue/windowSize
pins the window) + dedicated `Rate::window_size` tests in rate.rs. If a future phase needs
the downcast, add `as_any` as a REQUIRED object-safe method (default-with-Sized-bound is not
callable through `dyn`).

**Value-parity proof:** testMeter (100-window loop, EPS 1e-7), testSimpleRate (exact
`1000/1.2`, `1000/1.4`...), RateTest CsvSource formula, testRateWindowing rate-expiry all
pass with exact Java literals. 55 metrics tests green (M1+M2).

**M3 carry-overs:** FetchMetricsManager wires the count-Meter (uses WindowedCount seam) and
per-fetch bytes/records/latency. TimeUnit + convert + MetricConfig::with_time_window now
available. CompoundStat/NamedMeasurable + Sensor::add_compound ready.
