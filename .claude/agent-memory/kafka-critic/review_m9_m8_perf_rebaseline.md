---
name: review-m9-m8-perf-rebaseline
description: M9 Phase M8 perf-rebaseline review — alloc-budget guards only catch ALLOCATING per-record regressions, not steady-state non-allocating Sensor::record
metadata:
  type: project
---

M9 Phase M8 (commit 3431149) added a per-record zero-alloc guard, perf-analysis
doc, micro-bench. Reviewed clean of bugs; 3 doc-honesty notes.

**Key reusable finding — alloc-count guards have a blind spot:** a per-record
`Sensor::record()` in *steady state* does NOT allocate (SampledStat `samples`
Vec is preallocated `with_capacity(DEFAULT_NUM_SAMPLES+1)`; `current_locked`
returns `&mut samples[idx]` with no push until window rollover). So an
allocation-budget test can NOT prove "no per-record sensor call" — it only
catches *allocating* per-record work (the realistic regression here is moving
`FetchMetricsAggregator::record` into the loop, which DOES alloc via
`topic().to_string()` + Vec collect). Any "this test fails if a Sensor.record
leaks into the loop" claim is an overstatement for non-allocating sensors.

**Verification heuristics that worked:**
- Baseline-commit check: `git rev-parse c71c9ba^` == claimed baseline `1694b41`.
- Reproduce every cited number: guard 7/200=0.04; budget tests pass; micro-bench
  45.9 ns ≈ claimed 46.
- Frequency-table cross-check: grep each sensor's `.record` call site, confirm
  it's in the per-PARTITION or per-FETCH loop, NOT the per-RECORD loop
  (`fetch_records` body in completed_fetch.rs = pure i32 accumulation;
  lag/lead in fetch_collector `collect_fetch` per-partition loop ~line 545;
  `record_latency` in abstract_fetch handle_fetch_success once/response).
- "All INFO" claim: `git grep RecordingLevel::Debug|Trace` over consumer
  non-test code returns empty.
- Micro-bench used `record_at` (precomputed time) not `record()` (live clock
  read) — slight under-count, conservative direction, worth a caveat.
- Doc "3.43/record" = total allocs ÷ records (includes one-time overhead);
  marginal per-record is ~2.2 — flag "/record" framing that folds in overhead.
