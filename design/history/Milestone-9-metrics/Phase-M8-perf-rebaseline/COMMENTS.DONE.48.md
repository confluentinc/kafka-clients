# Critic 48 — Phase M8 (perf re-baseline) — RESOLVED

Commit reviewed: `3431149` (M8: per-record metrics zero-alloc guard + perf
analysis + benchmark methodology). All three doc-honesty notes resolved with
doc/comment-only changes (no logic change). Fixup commit references `3431149`.

---

## 1. [RESOLVED] Per-record guard overstated as a "no per-record sensor call" guard

The doc §3 / test comment claimed "any `Sensor.record` leak fails this test
immediately." That overstated the alloc-count guard: a bare steady-state
`Sensor::record()` per record is allocation-free (preallocated sample `Vec`,
pure mutex + arithmetic) and would NOT trip the budget.

Fix (doc/comment only):
- `completed_fetch.rs` `test_per_record_loop_is_pure_counter_no_sensor_record`
  doc comment, the in-body comment, and the assertion message now describe it
  truthfully: it is an ALLOCATION-COUNT guard that catches an *allocating*
  per-record regression (moving `FetchMetricsAggregator::record` — String + Vec
  — into the loop, or a windowed-stat sample ROTATION). It explicitly states it
  would NOT catch a non-allocating steady-state `Sensor::record`, and that the
  stronger "no per-record sensor CALL at all" invariant is established by code
  inspection of the verified-pure `fetch_records` loop body
  (`records_read += 1; bytes_read += size;`) + the loop-head comment, NOT by
  the alloc test.
- `consumer-metrics-perf-analysis.md` §3 item 3 rewritten with a "What this
  guard catches, precisely" paragraph saying the same. §intro "Short answer"
  softened from "proven by an automated allocation-budget guard" to "verified
  by code inspection of the loop body and backed by an allocation-budget guard
  against *allocating* per-record regressions."
- Test kept as-is (valid alloc guard); only the description corrected.

## 2. [RESOLVED] §3.1 "3.43/record" folded per-fetch overhead into a per-record figure

The 343/100 = 3.43 figure is total allocs ÷ records — it folds the ~22 one-time
per-fetch overhead into a "/record" number. Marginal per-record cost is ~2.2
(key + value `String::from_utf8`).

Fix (doc only): §3 item 1 now states the budget decomposition
(`100 overhead + 4 × 100 = 500`), explains 3.43 is total ÷ records (includes
~22 one-time overhead amortized over the 100-record fixture), and states the
true marginal per-record allocation is ~2.2 (key + value String only), trending
toward ~2.2 at larger batch sizes.

## 3. [RESOLVED] Micro-bench times `record_at` (no clock read), not `record()` — under-count caveat

The §4 micro-bench calls `record_at` with a precomputed `MockTime` timestamp,
excluding the one live `Time::milliseconds()` read that production `record()`
does per call. So ~46 ns/call is a slight UNDER-count.

Fix (doc only): added a "Caveat (conservative direction)" paragraph in §4
noting the bench excludes the per-call clock read that production sites perform,
so the real per-call cost is marginally higher (by ~one clock read), the
direction is conservative for a "metrics are cheap" claim, the clock read is
amortized per-fetch/per-partition (not per record), and the figure remains
negligible and illustrative.

---

Verification: `cargo build`, `cargo test --lib` (2124 passed), `cargo xtask
lint` (no issues), `cargo xtask format-check` (clean) — all green.
