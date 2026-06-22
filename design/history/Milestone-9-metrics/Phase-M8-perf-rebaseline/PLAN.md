# Phase M8 — Perf re-baseline (closes the loop on the user's metrics-cost concern)

Actor: 48. Branch: `consumer-impl`. Closes Milestone-9 metrics plan
(`~/.claude/plans/idempotent-dazzling-beacon.md`).

## Goal

M1–M7 built, wired, and exposed the full consumer metrics framework, all at
**INFO** recording level (full Java parity — verified against the Java source
in M3/M6; there is no DEBUG gating in Java's consumer sensors). The standing
user constraint (CLAUDE.md §11/§27) is: metrics must add **zero per-record
overhead** on the tuned fetch path. This phase proves that and documents the
remaining (per-fetch / per-partition / per-poll) cost so our perf numbers are
honestly comparable to Java.

## Sandbox reality

No broker / Docker is available in the Actor sandbox, so the live
`consumer-perf` benchmark cannot be run here. M8 therefore delivers everything
that does NOT need a broker, plus a runnable methodology + exact commands for
the user to execute the live before/after benchmark themselves.

## Deliverables

### 1. Per-record zero-allocation guard (the key regression test)

The two §27 budget tests already exist and already exercise the
metrics-wired path:

- `fetch_collector.rs::test_collect_fetch_per_record_allocation_budget`
  (builds the `FetchCollector` with `FetchMetricsManager::for_test()`).
- `abstract_fetch.rs::test_handle_fetch_success_does_not_copy_payload`
  (builds `AbstractFetch` with `FetchMetricsManager::for_test()`).

M8 confirms both pass UNCHANGED after all metrics wiring and makes the
"metrics added no per-record cost" claim **explicit** by:

- Adding a focused unit test `test_per_record_loop_is_pure_counter_no_sensor_record`
  in `completed_fetch.rs` that drives the per-record loop (`fetch_records`)
  under the alloc tracker with a metrics aggregator attached, and asserts the
  per-record loop allocates ZERO (the loop body is `records_read += 1;
  bytes_read += size;` — pure i32). The single `aggregator.record(...)` fires
  in `drain()` (once per partition), explicitly OUTSIDE the measured per-record
  window.
- A documenting comment at the per-record loop site reaffirming no
  `Sensor.record` on the per-record path.

### 2. Cost-analysis doc

`design/current/consumer-metrics-perf-analysis.md`: a frequency table of every
recording site (per-fetch, per-partition-per-poll, per-poll, per-bg-poll,
per-commit/heartbeat/rebalance/callback), the explicit statement that NOTHING
records per-record, that all levels MATCH Java (INFO), and that the
per-partition lag/lead cost scales with partition count (user-accepted for Java
parity).

### 3. Benchmark methodology + commands + baseline commit hash

In the same doc: exact steps to run `consumer-perf` (and the cloud-limits
harness) on current HEAD (metrics-on) vs the pre-metrics baseline commit
`1694b41` (parent of the first M1 commit `c71c9ba`), comparing e2e latency /
CPU / throughput.

### 4. Pure in-process micro-bench (no broker)

An `#[ignore]`d `#[test]` `bench_sensor_record_ns` in `sensor.rs` that times
`Sensor::record_at` on a realistic fetch-shaped sensor (Meter = Rate +
CumulativeSum, like `bytes-fetched`) over many iterations and prints ns/call.
Run on demand: `cargo test --lib bench_sensor_record_ns -- --ignored --nocapture`.
Not in CI (ignored), builds in CI. Reports the per-call cost so the user can
multiply by the per-fetch / per-partition record frequency.

### 5. Memory note

Update the perf memory notes (user auto-memory + actor memory) that the
consumer now records Java-parity INFO metrics and the earlier metrics-free
CPU/latency numbers are superseded.

## Verification

`cargo build`, `cargo test --lib` (incl. the new guard test + the two existing
budget tests), `cargo xtask lint`, `cargo xtask format-check` — all green. The
micro-bench builds (ignored by default).

## Out of scope

Running the live broker benchmark (user-run step), criterion harness (the
in-process `#[ignore]` test suffices and adds no dependency).
