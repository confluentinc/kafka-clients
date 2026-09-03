---
name: producer-metrics-p3
description: Phase P3 BufferPool + RecordAccumulator metrics review — clean; try/finally cleanup parity, immediate-path signal-omission is safe, cfg(test) failure seam
metadata:
  type: project
---

Phase P3 (`ca0741f7`) wired BufferPool + RecordAccumulator metrics. Reviewed CLEAN.
Reusable adjudications for future BufferPool / accumulator-metrics reviews:

- **Java `void` method that throws → Rust `Result<(), _>` + `#[cfg(test)]` bool
  seam**: `recordWaitTime` (throws only in the Mockito-spy test) became
  `record_wait_time -> Result<(), KafkaError>`, always `Ok` in prod, with a
  `#[cfg(test)] fail_record_wait_time: AtomicBool` replacing `spy+doThrow`. This
  is a faithful, correct translation of a throwing contract — NOT a false-positive
  target. The seam field is cfg-gated out of prod builds.

- **allocate try/finally parity check**: on any throw path (metrics-exception,
  closed, timeout) the Rust code must restore `non_pooled_available_memory +=
  accumulated`, remove the waiter, AND signal the next waiter before returning —
  = Java inner-finally (`+= accumulated; waiters.remove`) + outer-finally (signal +
  unlock). `accumulated` is a loop-carried local; first-iteration failure restores
  0, later restores the partial. On SUCCESS Java sets `accumulated=0` before the
  finally (memory stays consumed) — Rust just drops the local. Verified equivalent.

- **Immediate (non-blocking) allocate path does NOT signal the next waiter** in
  Rust (Java's outer finally does). This is SAFE / not a hang: the immediate path
  only ever REMOVES memory, never adds, so it creates no new opportunity for a
  blocked waiter. Memory is only ever returned via `deallocate`, which
  unconditionally signals the front waiter. Do not flag this omission as a bug.
  (It is also pre-existing, not P3.)

- **buffer-exhausted recorded ONLY on the TimedOut branch** (value 1.0), and NOT
  when `record_wait_time` fails on a timeout iteration (Java throws in the finally
  before the `waitingTimeElapsed` block). Restore-then-record vs Java
  record-then-restore-in-unwind is observationally identical.

- **metrics-exception test runs ~1s**: `allocate(2, 1000)` with no signaller waits
  the full 1000ms real-clock before `record_wait_time` fails. This matches Java —
  `ReentrantLock.Condition.await(ns)` uses real wall time regardless of Kafka's
  `Time`. Not a slowness defect.

- **`KafkaProducer::new` (pub, "used by tests") builds a SECOND `Metrics` registry**
  separate from the pre-supplied accumulator's — so its `metrics()` misses pool
  gauges. NOT a defect: it takes a pre-built accumulator + sender handle, predates
  P3, and is unreachable from `from_config`/FFI (both coherent — one registry
  cloned into pool + accumulator + sender). Production `metrics()` is complete.

- Gauge closures (`queued`/`available_memory` take the pool `inner` lock) do NOT
  deadlock: pool ops never trigger a metric read while holding `inner`, and the
  closures never take the metrics lock — no reentrancy, no lock-order inversion.
