# Critic 0 — Milestone 9 Phase 0a — resolved

## Issue 1 (RESOLVED): `f64` Display drops the trailing `.0` — text diverges from Java's `double` string form
- **Introduced by**: the `double`-in-`Display`/message family across `a747461` (`quota.rs`), `2d941c6` (`quota_violation_error.rs`), `de4221e` (`cumulative_sum.rs`), `28625b5` (`token_bucket.rs`), `5eb8905` (`frequency.rs`, `frequencies.rs`, `histogram.rs`).
- **Original report**: Rust `write!("{}", some_f64)` renders an integral double without a trailing `.0` (`5.0` → `"5"`) and prints `"inf"` where Java prints `"Infinity"`; the `QuotaViolationError` message is user-facing and Phase 0b asserts its content.
- **Fix**: added `crate::common::utils::double_to_string` (minimal native `Double.toString`-style formatter: finite integral → trailing `.0`; other finite → shortest decimal; `Infinity`/`-Infinity`/`NaN` spellings; documented to target magnitudes < 1e7, no scientific notation). Applied it to every `f64`-in-message site: `Quota::Display`, `QuotaViolationError::Display`, `Frequency::Display`, `Frequencies::new` validation messages, `Histogram::Display` (infinity label), and — same latent pattern, for consistency — `CumulativeSum::Display` and `TokenBucket::Display`. Updated the `quota.rs` test to assert `"upper=5.0"` and the `quota_violation_error.rs` test to assert `"Threshold: 5.0"`; added a `double_to_string` unit test covering integral/non-integral/Infinity/NaN.
- **Fixup commit**: `2867ad6` (fixup! of `a747461`).
- **Verification**: build + `cargo test --lib` (2073 passed) + format-check + lint all clean.

## Issue 2 (RESOLVED): `Max`/`Min` use `f64::max`/`f64::min` (drop NaN); Java `Math.max`/`Math.min` propagate NaN
- **Introduced by**: `de4221e` (`max.rs`, `min.rs`).
- **Original report**: Rust `f64::max`/`f64::min` return the non-NaN operand (IEEE `maxNum`/`minNum`), so a recorded `NaN` is silently dropped; Java `Math.max`/`Math.min` propagate `NaN`, poisoning the running max/min until the sample resets.
- **Fix**: `Max::update`/`combine` now use a local `nan_max` (returns `NaN` if either operand is `NaN`, else `f64::max`); `Min` uses the symmetric `nan_min`. Added a per-stat regression test recording a `NaN` and asserting `measure()` is `NaN`, and that a subsequent finite record leaves it `NaN` (NaN propagates through `Math.max`/`Math.min`), verified against `Max.java`/`Min.java`.
- **Verification**: build + `cargo test --lib` (2075 passed) + format-check + lint all clean.

---

## Verified clean (from the round-1 review — recorded for reference)

**Documented deviations — each verified genuinely behavior-preserving:**
- `LinearBinScheme::to_bin` clamping negatives to bin 0 instead of throwing (Java `Histogram.java:202`): **unreachable**. Its only caller path is `Percentiles`, whose `update` bounds the value to `[min, max]` *before* `histogram.record`, and `LinearBinScheme` requires `min == 0.0`, so `to_bin` never sees a negative. `HistogramTest.java:106-134` does not test the negative-throw. Deviation is safe.
- `KafkaMetric` dropping the `lock` param: the lock only ever provided mutual exclusion, now subsumed by the provider's own `Mutex`. No method behavior depended on lock identity.
- `MetricValue` / `MetricValueProvider` enum folding of `Gauge<T>`: pre-approved; nothing in Phase 0a needs to distinguish a bare provider from a Gauge.
- `WindowedCount`/`CumulativeCount` no longer subtypes of the `*Sum` classes: `Meter` correctly checks **both** `WindowedSum` and `WindowedCount`, and the `IllegalArgument` message matches. `KafkaMetricsCollector`'s `instanceof CumulativeSum` is out of Phase-0a scope.
- `MetricConfig::with_samples` returning `Result`: message `"The number of samples must be at least 1."` is byte-exact with Java.

**Numeric parity — read line-by-line against Java, faithful:** `SampledStat` window advance / `purge_obsolete_samples` / `oldest` / `is_complete` / ring `advance`; `Rate::window_size` + `SimpleRate` fixed-first-window; `Meter` composition and `stats()` order; `TokenBucket` refill/burst; `Histogram`/`ConstantBinScheme`/`LinearBinScheme`; `Percentiles::value` / `Frequencies::frequency`; `TimeUnit`/`MetricsUtils::convert`; `RecordingLevel`.

**Test fidelity:** all Phase-0a Java test methods faithfully translated; the only deferrals are `FrequenciesTest.testWithMetricsStrategy{1,2,3}` (require `Metrics`+`Sensor`, Phase 0b), and the 2 dropped cases (null-provider, `contextLabels().clear()` immutability) are unrepresentable in Rust's type system. `MetricNameTest`/`MetricNameTemplateTest` do not exist in the pinned 4.2.0 snapshot.

**Conventions:** `internals`/`metrics_utils` are `pub(crate)`; re-exports at parent-module level; license headers present; no TODO/FIXME.
