---
name: phaseP3-buffer-pool-metrics
description: M12 Phase P3 — BufferPool + RecordAccumulator metrics wiring; new_for_test test-churn pattern; record_wait_time fail-seam
metadata:
  type: project
---

# Phase P3 — BufferPool + RecordAccumulator metrics

Landed on branch `producer-metrics`. Threads producer `Arc<Metrics>` into
`BufferPool` and `RecordAccumulator` (KafkaProducer.java:426-438).

**Why:** Milestone-12 producer-metrics; P1/P2 built the registry + sender
metrics, P3 covers the buffer-pool metrics family.

**How to apply / patterns worth reusing:**

- **Test-churn containment for Java-faithful metrics ctors.** Java has no
  metrics-less constructor (tests pass `new Metrics()`). When a ctor gains
  `metrics: Arc<Metrics>` + `metric_grp_name` params and ~20 existing test
  sites would need updating, add a `#[cfg(test)] pub(crate) fn new_for_test(...)`
  that supplies `Arc::new(Metrics::new())` + a system-clock time provider +
  `"producer-metrics"` and delegates to the real ctor. Then mechanically
  rename test call sites `::new(` → `::new_for_test(`. `pub(crate) #[cfg(test)]`
  helpers ARE visible to OTHER modules' tests in the same crate (sender tests
  reached `BufferPool::new_for_test`).
- **`#[cfg(test)]` on struct-literal fields works** — used for
  `fail_record_wait_time: AtomicBool` so the production struct stays clean.
  This is the faithful Rust analog of Java's Mockito `spy(pool).doThrow(OOM)
  .when(pool).recordWaitTime(...)` seam.
- **Infallible-record → Result deviation.** `record_wait_time` returns
  `Result<(), KafkaError>` although Rust sensor recording never fails (always
  `Ok` in prod). This is the faithful translation of Java's `void`-that-can-
  throw and the vehicle for the test seam. `allocate_blocking` runs the Java
  outer-`finally` cleanup (`non_pooled += accumulated`, remove waiter, signal)
  before propagating the error — closes
  `BufferPoolTest.testCleanupMemoryAvailabilityOnMetricsException`.
- **Meter time units:** wait-time uses `Meter::with_unit(TimeUnit::Nanoseconds,
  ratio, ns-total)`; exhausted uses `Meter::new(rate, total)` (seconds
  default). Sensors held as `Arc<Sensor>` fields (DoD #10 — no per-call name
  lookup; immediate/no-wait allocate path never touches a sensor).
- **RecordAccumulator gauges** = `ClosureMeasurable::new(move |_c,_n| ...)`
  over `Arc::clone(&free)` (Java lambdas capture `free`); registered via
  `metrics.add_metric(metric_name(...), Box::new(...))`. Descriptions copied
  verbatim from Java.
- **Metric value read in tests:** `metrics.metric(&mn).unwrap().measurable_value(0)`
  (inherent f64 accessor, no trait import) — or `.metric_value().as_double()`
  if `Metric` trait in scope (kafka_producer_metrics.rs precedent).
- **No producer send-path alloc-budget test exists** — `test_alloc_tracker`
  (`src/test_alloc_tracker.rs`) is consumer-only (abstract_fetch,
  fetch_collector, completed_fetch). DoD #10 for producer is argued
  structurally, not by a test.
</content>
