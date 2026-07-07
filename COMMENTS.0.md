# Critic 0 — Milestone 9 Phase 0a (`common.metrics` foundation)

Review of commits 55ae22d, a747461, 2d941c6, de4221e, 28625b5, 5eb8905.
Java contract: Apache Kafka 4.2.0.

Two real issues found (both low severity — behavior/text divergences from Java,
no functional/algorithmic bug). The stats math itself is faithful; see the
"Verified clean" appendix.

---

## Issue 2: `Max`/`Min` use `f64::max`/`f64::min` (drop NaN); Java `Math.max`/`Math.min` propagate NaN
- **File**: `src/common/metrics/stats/max.rs:52,59`, `src/common/metrics/stats/min.rs:52,59`
- **Severity**: Behavior Mismatch (low; only manifests when a `NaN` value is recorded)
- **Java Reference**: `Max.java:34` (`Math.max(...)` in `update`) and `:43` (`combine`); `Min.java:34,43` (`Math.min(...)`)
- **Description**: Rust `f64::max`/`f64::min` implement IEEE `maxNum`/`minNum`: when one operand is `NaN` they return the **other** operand, i.e. `NaN` is silently ignored. Java's `Math.max`/`Math.min` **propagate** `NaN` (if either argument is `NaN` the result is `NaN`). Consequence: if a client records a `NaN` value (e.g. a ratio that divided by zero), Java's `Max`/`Min` sample value becomes `NaN` and `measure()` returns `NaN` (poisoned until the sample resets); the Rust translation drops the `NaN` and keeps reporting the max/min of the finite values. All finite-value behavior is identical; the initial values (`Max` = `NEG_INFINITY`, `Min` = `MAX` — the real 4.2.0 asymmetry) are correctly reproduced.
- **Expected**: match Java — a `NaN` operand poisons the running max/min (return `NaN`).
- **Actual**: `NaN` is ignored; the finite max/min is retained.

---

## Verified clean (no issue — recorded so the Actor need not re-litigate)

**Documented deviations — each verified genuinely behavior-preserving:**
- `LinearBinScheme::to_bin` clamping negatives to bin 0 instead of throwing (Java `Histogram.java:202`): **unreachable**. Its only caller path is `Percentiles`, whose `update` (`percentiles.rs:189-202`, Java `Percentiles.java:114-130`) bounds the value to `[min, max]` *before* `histogram.record`, and `LinearBinScheme` requires `min == 0.0`, so `to_bin` never sees a negative. `HistogramTest.java:106-134` does not test the negative-throw. Deviation is safe.
- `KafkaMetric` dropping the `lock` param: the lock only ever provided mutual exclusion, now subsumed by the provider's own `Mutex`. No method behavior depended on lock identity.
- `MetricValue` / `MetricValueProvider` enum folding of `Gauge<T>`: pre-approved; nothing in Phase 0a needs to distinguish a bare provider from a Gauge.
- `WindowedCount`/`CumulativeCount` no longer subtypes of the `*Sum` classes: `Meter` (`meter.rs:74,94`, Java `Meter.java:62,82`) correctly checks **both** `WindowedSum` and `WindowedCount`, and the `IllegalArgument` message ("Meter is supported only for WindowedCount or WindowedSum.") matches. `KafkaMetricsCollector`'s `instanceof CumulativeSum` is out of Phase-0a scope (not translated yet).
- `MetricConfig::with_samples` returning `Result`: message `"The number of samples must be at least 1."` is byte-exact with Java `MetricConfig.samples(int)`.

**Numeric parity — read line-by-line against Java, faithful:**
- `SampledStat` window advance / `purge_obsolete_samples` / `oldest` / `is_complete` / ring `advance` (mod `config.samples()+1`) — exact.
- `Rate::window_size` prorate-N-1-windows + `.max(1)`, and `SimpleRate` fixed-first-window (`max(elapsed, timeWindowMs)`) — exact; `SimpleRate::measure` correctly re-dispatches to its own `window_size` (Java virtual override).
- `Meter` Rate+Total composition, `stats()` order (total, rate), count-vs-sum total value — exact.
- `TokenBucket` refill/burst credit math and `Long.MAX_VALUE`-on-null-quota — exact.
- `Histogram` `record`/`value(quantile)`/`ConstantBinScheme`/`LinearBinScheme` `to_bin`/`from_bin` including float→double promotion in the quantile comparison — exact.
- `Percentiles::value` (outer-bucket / inner-sample scan order), `Frequencies::frequency` center-value binning + constructor validation order/messages, half-bucket-width — exact. Histogram-clear-on-sample-reset preserved via `Sample::reset` clearing the inline `Option<Histogram>`.
- `TimeUnit`/`MetricsUtils::convert` — exact (Rust's exhaustive match subsumes Java's unreachable `default` throw).
- `RecordingLevel` `for_id`/`for_name`/`should_record` logic + messages — exact.

**Test fidelity:** `SampledStatTest`(3), `HistogramTest`(4, `checkBinningConsistency` included), `MeterTest`(1, with `(Rate)`/`(CumulativeSum)` downcast equivalents), `RateTest`(2, `@CsvSource` 4 cases enumerated as a loop), `TokenBucketTest`(2) faithfully translated. `FrequenciesTest`: 5 translated; the 3 `testWithMetricsStrategy{1,2,3}` deferrals genuinely require `Metrics`+`Sensor` (Phase 0b) — the only deferrals, as documented. `KafkaMetricTest`/`KafkaMetricsContextTest`/`MetricsUtilsTest` are equal-or-stronger in Rust; the 2 dropped cases (null-provider, `contextLabels().clear()` immutability) are unrepresentable in Rust's type system. `MetricNameTest.java`/`MetricNameTemplateTest.java` do not exist in the pinned 4.2.0 snapshot — nothing to translate.

**Conventions:** `internals` module + `metrics_utils` are `pub(crate)`; re-exports at parent-module level; license headers present on all files; no TODO/FIXME.

---

## Rule-update suggestions (per agent-roles.md)
`COMMENTS.FP.md` / `COMMENTS.FN.md` do not exist at repo root yet. If Issue 1
recurs across translated `toString`/message code, consider adding to `CLAUDE.md`
a note that **`double`→string parity requires a Java-`Double.toString`-style
formatter** (Rust `{}` on `f64` drops the trailing `.0` for integral values and
uses different scientific-notation thresholds) whenever the rendered text is part
of a behavioral/message contract.
