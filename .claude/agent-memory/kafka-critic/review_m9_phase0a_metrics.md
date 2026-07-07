---
name: review-m9-phase0a-metrics
description: M9 Phase 0a+0b common.metrics review — float-formatting/NaN bug classes, reporter fault-isolation, collection-toString parity, plus deviation-verification heuristics
metadata:
  type: project
---

Milestone-9 Phase 0a (`common.metrics` core + `stats/`) reviewed by Critic 0. The
stats math (SampledStat windowing, Rate prorate, Meter, TokenBucket, Histogram
bins, Percentiles, Frequencies) was faithful. Two low-severity divergences found,
both of which are **recurring translation bug classes** worth checking in every
future Java→Rust translation:

**1. `f64` Display drops the trailing `.0`.** Rust `write!("{}", 5.0_f64)` → `"5"`;
Java `""+ (double)5.0` → `"5.0"`. Any translated `toString`/exception message that
interpolates a `double` via Rust `{}` diverges for integral values (common: quota
bounds, counts). Also Rust `"inf"`/`"NaN"` vs Java `"Infinity"`/`"NaN"`, and Java
switches to sci-notation at `|x|>=1e7`/`<1e-3` while Rust doesn't.
**Why it matters:** DoD elevates error-message text to contract status; the divergence
is user-facing for exceptions (e.g. `QuotaViolationError`). **How to apply:** grep the
diff for `write!`/`format!` with an `f64`/`f32` arg in Display/error paths; watch for
translated tests that *encode the divergent output* with exact-equality (Phase 0a's
`quota.rs:99` asserted `"upper=5"`, matching Rust not Java). Faithful parity needs a
Java-`Double.toString`-style helper.
**Fix landed & RESOLVED:** `common::utils::double_to_string` (2867ad6 = integral `.0`;
35ce114 = full scientific notation). Verified Java-`Double.toString`-faithful: decimal in
`[1e-3,1e7)`, else `<mantissa>E<exp>` (mantissa carries `.0`, exp upper-case no `+`);
`1e7`→`"1.0E7"`, `10485760.0`→`"1.048576E7"`, `0.0001`→`"1.0E-4"`. Issue 3 (my earlier
"known residual") is closed. Any NEW `f64`-in-message site should route through this helper.

**2. `f64::max`/`f64::min` drop NaN; Java `Math.max`/`Math.min` propagate it.**
Rust's IEEE minNum/maxNum returns the non-NaN operand; Java returns NaN. So a NaN
input poisons Java's `Max`/`Min` stat but is silently ignored in Rust. **How to apply:**
whenever a translation replaces `Math.max`/`Math.min` with `.max()`/`.min()` on floats,
flag it (low severity unless NaN inputs are plausible). Same caveat for any float
`.min()`/`.max()` clamp (TokenBucket's `burst.min(...)` — safe there, values finite).

**Deviation-verification heuristics that paid off:**
- "Unreachable throw removed" claims: trace the *only* caller. `LinearBinScheme::to_bin`
  drops Java's negative-input throw, but `Percentiles::update` bounds value to
  `[min,max]` before recording and linear requires `min==0.0`, so it's genuinely
  unreachable. Verify by reading the caller, not trusting the note.
- Java `instanceof Super` where the Rust subclass became a standalone struct: the Rust
  check must test BOTH types. `Meter` correctly does `is::<WindowedSum>() ||
  is::<WindowedCount>()` (Java relied on `WindowedCount extends WindowedSum`).
- Java virtual-method override folded into a wrapper: `SimpleRate` wraps `Rate` and
  re-implements `measure` to call its own `window_size` — verify the wrapper doesn't
  accidentally call the base method.

**Not worth reporting (avoided as FP):** FQCN prefixes in messages (no Rust reflection);
`getClass()` suffixes dropped from error text; test-skips that are unrepresentable in
Rust's type system (null-provider, `unmodifiableMap().clear()` — Rust returns `&Map`).

---

**Phase 0b (Metrics registry + Sensor) — verdict READY, 3 LOW findings.** New reusable
bug classes worth checking in future translations:

**3. Reporter/listener fault isolation.** Java often wraps each callback in a per-element
loop in `try/catch(Exception){ log; continue; }` (e.g. `Metrics.registerMetric`/`removeMetric`/
`close` calling `metricChange`/`metricRemoval`/`close`). If the Rust trait method returns
`()` (as `MetricsReporter` does), a faulting impl can only PANIC, which propagates and
aborts the op + skips remaining reporters — losing Java's isolation. Flag it (LOW) when the
plugin surface is public. Filed as Issue 4; fix deferred to Phase 5 reporter-surface work
(fallible callbacks or `catch_unwind`). Pattern generalizes to any listener/interceptor loop.

**4. `{:?}` on a Rust collection in an error message ≠ Java `toString`.** `format!("{:?}",
hashset)` → `{"a", "b"}` (braces+quotes) vs Java `Set.toString()` → `[a, b]`. Diverges in
any translated message that interpolates a collection (Issue 5, `metric_instance` tag-mismatch).
LOW when unasserted + set is unordered anyway, but it's a real message-contract divergence.

**5. `assertThrows(SpecificException.class,…)` → bare `.is_err()`** loses the failure-kind
check when the Rust method returns the broad `KafkaError` (many variants). Prefer
`matches!(err, KafkaError::IllegalArgument(_))` or `err.message().contains(...)`. LOW when
only one Err path is reachable in-context (Issue 6, 6 sites). Per DoD "not just is_err()".

**Phase 0b heuristics that paid off / non-issues confirmed:**
- Java `synchronized(this)` + inner `synchronized(metricLock)` → Rust single state `Mutex`
  + per-stat `Arc<Mutex>`. Deadlock-free iff readers lock ONLY the stat (never state) and
  record locks state→stat (never the reverse). The `LockingReporter` concurrency test pins it.
- Java collection-view getter (`Metrics.metrics()` returns the live `ConcurrentMap`) → Rust
  snapshot `HashMap`: acceptable adaptation, behaviorally equal for get/size at a point in time.
- Lambda overload binding: `(config,now)->int` binds to `Measurable` (widened `double`), NOT
  the empty marker `MetricValueProvider` and NOT `Gauge` (no such `addMetric` overload). Verify
  by checking which overloads exist + which interface is a functional interface.
- `toHtmlTable` skip is fine: JMX-dependent (`JmxReporter.getMBeanName`) + only doc-gen `main()`
  callers, never in tests/KIP-714 closure.
- Stress-test iteration cut (10000→1000) is fine when the worker threads spin continuously
  (wall-clock-bounded, not iteration-bounded) — deadlock/race still surfaces; only churn drops.
- `MockClock` vs Java `MockTime`: SensorTest/TokenBucketTest construct MockTime with
  `autoTickMs=0` (manual advance), so Rust manual-advance matches; start-time (0 vs
  currentTimeMillis) is benign when tests use time DIFFERENCES or explicit timestamps.
