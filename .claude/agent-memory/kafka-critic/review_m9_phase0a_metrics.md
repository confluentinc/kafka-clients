---
name: review-m9-phase0a-metrics
description: M9 Phase 0a common.metrics review — reusable float-formatting and NaN-semantics translation bug classes, plus deviation-verification heuristics
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
**Fix landed (2867ad6):** `common::utils::double_to_string` — integral→trailing `.0`,
`Infinity`/`-Infinity`/`NaN` spelled out. **KNOWN RESIDUAL (my Issue 3, low, non-blocking):**
it does NOT do Java's scientific notation for `|x|>=1e7` or non-integral `|x|<1e-3`.
Reachable via `Quota::upper_bound(1e7)`→`"upper=10000000.0"` (Java `"1.0E7"`) and, in
**Phase 0b**, `QuotaViolationError` with a ≥10 MB/s byte-rate quota bound. Re-check this
when Phase 0b makes `QuotaViolationError` live/asserted.

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
