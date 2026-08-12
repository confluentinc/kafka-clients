---
name: phaseM8-perf-rebaseline
description: Milestone-9 Phase M8 (FINAL) — metrics perf re-baseline; zero per-record-alloc guard test, cost-analysis doc, user-run broker benchmark methodology, in-process Sensor::record micro-bench
metadata:
  type: project
---

Milestone-9 Phase M8 (Actor 48): closes the metrics plan. Branch `consumer-impl`.
No COMMENTS.48.md existed (fresh task). PLAN:
`design/history/Milestone-9-metrics/Phase-M8-perf-rebaseline/PLAN.md`.

**Sandbox has NO broker/Docker** — live `consumer-perf` is a user-run step. M8
delivers the broker-free parts + a runnable methodology.

**Pre-metrics baseline commit = `1694b41`** (parent of first M1 commit
`c71c9ba M1: metrics core foundation`). At `1694b41` every consumer `record*`
was a no-op. `git log --oneline c71c9ba^ -1` confirms.

**Deliverables:**
- `design/current/consumer-metrics-perf-analysis.md` — frequency table (per-fetch
  / per-partition-per-poll / per-poll / per-bg-poll / per-commit / per-heartbeat
  / per-rebalance / per-callback; PER-RECORD row intentionally empty), INFO=Java
  parity (no DEBUG gating, verified M3/M6), lag/lead scales with partition count,
  baseline-commit + worktree before/after commands, conclusion that metrics-free
  numbers are superseded.
- Guard test `completed_fetch::tests::test_per_record_loop_is_pure_counter_no_sensor_record`
  — zero-alloc `LenDeserializer` (decode→byte length, no String) isolates
  structural per-record cost; drives `fetch_records` over 200 records with
  aggregator attached, asserts ≤3 allocs/record. **Measured 7 allocs/200 recs =
  0.04/record** (just Vec doubling). Then calls drain() once and asserts that is
  where `aggregator.record` fires (outside per-record window).
- Documenting comment at `completed_fetch.rs` `fetch_records` loop head.
- Micro-bench `sensor::tests::bench_sensor_record_ns` — `#[ignore]`d (builds in
  CI, runs on demand), times `Sensor::record_at` on Meter+Avg+Max (bytes-fetched
  shape). **Release ~46 ns/call, debug ~409 ns/call** (2M iters, periodic window
  rollover). Run: `cargo test --release --lib bench_sensor_record_ns -- --ignored --nocapture`.

**Existing §27 budget tests pass UNCHANGED with metrics wired in:**
- `fetch_collector::test_collect_fetch_per_record_allocation_budget`: 343/100 recs
  (3.43/record, budget 500).
- `abstract_fetch::test_handle_fetch_success_does_not_copy_payload`: 64/8 parts
  (budget 67). M3 already bumped per-partition budget 7→8 for the per-fetch
  aggregator HashSet<TP> clone (per-fetch, not per-record).

**Patterns reusable later:**
- **Zero-alloc deserializer to isolate metrics regression.** A `Deserializer<usize>`
  returning `data.len()` removes the user-decode String allocations so the alloc
  budget measures ONLY structural per-record cost (ConsumerRecord + Vec). Any
  per-record Sensor.record / windowed-stat push then stands out.
- **In-process micro-bench as `#[ignore]` #[test]** (no criterion dep): warm-up
  loop + Instant timing loop + eprintln ns/call + `assert!(ns > 0.0)` sanity
  floor. Builds in CI, doesn't flake CI (ignored). Build the realistic stat shape
  (Meter via add_compound + Avg/Max via add) to match the production sensor.
- `cargo xtask format` formats the WHOLE workspace; only my 2 files were dirty so
  it was safe, but be aware it can touch unrelated crates (consumer-perf had
  pre-existing M changes — verify git status --short after).

Commit: "M8: per-record metrics zero-alloc guard + perf analysis + benchmark methodology".
All 4 checks green: build, test --lib (2124 pass / 1 ignored), xtask lint, format-check.
