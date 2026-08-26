# Phase P3 — BufferPool + RecordAccumulator metrics

Translate the metrics wiring of Java's `BufferPool` and
`RecordAccumulator.registerMetrics` (Apache Kafka 4.2) into the Rust
producer, and thread the producer's `Arc<Metrics>` into both at
construction time (KafkaProducer.java:426-438).

## Java sources

- `BufferPool.java`
  - Constructor `BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName)`
    (:70-94) creates:
    - `bufferpool-wait-time` sensor with `Meter(TimeUnit.NANOSECONDS,
      bufferpool-wait-ratio, bufferpool-wait-time-ns-total)` (:79-92).
    - `buffer-exhausted-records` sensor with `Meter(buffer-exhausted-rate,
      buffer-exhausted-total)` (:87-90).
  - `recordWaitTime(long timeNs)` (:210-212) — `waitTime.record(timeNs,
    time.milliseconds())`. `protected` = test seam (Mockito `spy` +
    `doThrow`).
  - `allocate` (:107-207) measures the wait across `moreMemory.await(...)`
    in a `try/finally` and calls `recordWaitTime(timeNs)` in the `finally`
    (:145-154) — recorded even when the wait ends in timeout/close/error.
    On `waitingTimeElapsed` it records `buffer-exhausted-records` and throws
    `BufferExhaustedException` (:159-164). The outer `finally` restores
    `nonPooledAvailableMemory += accumulated` and removes the waiter
    (:185-189), so an exception from `recordWaitTime` still cleans up.
- `RecordAccumulator.java`
  - `registerMetrics(Metrics, String)` (:198-213) — three gauges in group
    `producer-metrics`: `waiting-threads` = `free.queued()`,
    `buffer-total-bytes` = `free.totalMemory()`, `buffer-available-bytes` =
    `free.availableMemory()`. Called from the constructor (:148).

## Design decisions

### BufferPool constructor (Java-faithful, no metrics-less production ctor)

`BufferPool::new(memory: i64, poolable_size: usize, metrics: Arc<Metrics>,
time_provider: Arc<dyn Fn() -> i64 + Send + Sync>, metric_grp_name: &str)
-> Self`.

- `time_provider` supplies POSIX milliseconds — the `record_at` timestamp
  argument, matching Java `time.milliseconds()`. This is the existing
  producer time-source precedent (`SenderMetrics` holds the same
  `Arc<dyn Fn() -> i64>`).
- The wait *duration* (recorded value, nanoseconds) is measured with
  `std::time::Instant` deltas around the `notified().await`, the analog of
  Java's `time.nanoseconds()` monotonic reading (`now_nanos` precedent).
- Sensors are held as `Arc<Sensor>` fields (`wait_time_sensor`,
  `buffer_exhausted_sensor`) rather than re-looked-up by name per call —
  P2 precedent, and required by DoD #10 (no per-wait name-string alloc).
- Metric registration in the ctor uses `.expect(...)`: a duplicate-name
  failure is a construction-time programming error and Java's ctor declares
  no checked throw here. `BufferPool::new` keeps returning `Self` so the
  ~20 `Arc::new(BufferPool::new(...))` call sites are unaffected in shape.

### `record_wait_time` + the metrics-exception test seam

`record_wait_time(&self, time_ns: i64) -> Result<(), KafkaError>` — records
`(time_ns as f64, time_provider())` to the wait-time sensor. Rust sensor
recording is infallible, so production always returns `Ok`; the `Result`
return type is the faithful translation of Java's `void` method that *can*
throw (Mockito injects an `OutOfMemoryError`).

Test seam mirroring Java's `spy(pool).doThrow(...).when(pool).recordWaitTime(...)`:
a `#[cfg(test)]`-gated `fail_record_wait_time: AtomicBool` field
(compiled out of production builds; `cfg` attributes on struct-literal
fields keep the production struct clean). When set, `record_wait_time`
returns an error instead of recording. `allocate_blocking` propagates that
error after running the Java `finally` cleanup (`nonPooledAvailableMemory
+= accumulated`, remove waiter, signal next waiter). This un-defers
`BufferPoolTest.testCleanupMemoryAvailabilityOnMetricsException` (:226).

### `buffer-exhausted-records` recording

Recorded (value 1.0) in the `TimedOut` arm of `allocate_blocking` before
returning `BufferExhausted`, matching Java `:160`
(`metrics.sensor("buffer-exhausted-records").record()` on
`waitingTimeElapsed`). Done outside the pool lock (the recorded value and
timestamp do not depend on pool state), avoiding a sensor lock nested under
the pool mutex.

### RecordAccumulator::register_metrics

`with_log_context` and `new` gain `metrics: Arc<Metrics>` +
`metric_grp_name: &str` params (Java order: after `partitioner_config`,
before `buffer_pool`). `register_metrics(free, metrics, grp)` adds the three
gauges as `ClosureMeasurable`s capturing `Arc<BufferPool>` clones (Java
lambdas capture `free`). Descriptions copied verbatim from Java.

### Test churn containment (endorsed by the task)

Java has no metrics-less constructor; its tests pass `new Metrics()`. Rust
mirrors this with `#[cfg(test)] pub(crate) fn new_for_test(...)` helpers on
both types that build a fresh `Arc::new(Metrics::new())` + system-clock
time provider + `"producer-metrics"` group and delegate to the real ctor.
Existing test sites switch `::new(` → `::new_for_test(` (mechanical, no
arg changes). Sender tests keep using throwaway metrics for the
accumulator/pool, so no buffer/wait metrics land in the sender's registry —
`test_sender_metrics_templates` set-equality is unchanged.

## Deliverables

1. `BufferPool`: metrics ctor, two sensors, `record_wait_time`, exhausted
   recording, test seam. `new_for_test` helper.
2. `RecordAccumulator::register_metrics` + ctor params. `new_for_test`.
3. KafkaProducer `from_config`: move `create_metrics` before pool/accumulator
   construction; thread `Arc<Metrics>` into both (KafkaProducer.java:426-438
   ordering).
4. Tests: `testCleanupMemoryAvailabilityOnMetricsException` translated;
   Rust-added smoke tests for the RA gauges and the BufferPool wait/exhausted
   metric names + recording.

## Self-review (completed)

- [x] All BufferPool/RecordAccumulator construction sites updated: production
      (`kafka_producer.rs` `from_config`) uses the real metrics ctors; ~20 test
      sites across `buffer_pool.rs`, `record_accumulator.rs`, `sender.rs`,
      `kafka_producer.rs` switched to `new_for_test`.
- [x] `cargo build`, `cargo test --lib` (3196 pass), `cargo xtask format-check`,
      `cargo xtask lint` all green.
- [x] DoD #10: the fast (no-wait) `allocate` path (`AllocResult::Immediate`)
      never touches a sensor — `record_wait_time` and the buffer-exhausted
      recording only run on the wait/timeout slow path (Java-identical). Sensors
      are held as `Arc<Sensor>` fields (no per-call name-string alloc). No
      producer send-path allocation-budget test exists (the `test_alloc_tracker`
      harness is consumer-only), so nothing to regress; the change adds zero
      allocations to the immediate path.
- [x] Metric names/descriptions/group match Java constant-for-constant
      (BufferPool.java:80-89, RecordAccumulator.java:200-212). Wait-time Meter
      uses `TimeUnit::Nanoseconds`; exhausted Meter uses the default (seconds)
      unit, matching Java.
- [x] No TODO/FIXME; Apache 2.0 headers intact; bindings untouched (Phase P4).

### Deviations

- `record_wait_time` returns `Result<(), KafkaError>` (Java: `void` that can
  throw). Rust sensor recording is infallible, so production always returns
  `Ok`; the `Result` is the faithful translation of the throwing contract and
  the vehicle for the `#[cfg(test)] fail_record_wait_time` seam that replaces
  Java's Mockito `spy`/`doThrow`.
- `outOfMemoryOnAllocation` (BufferPoolTest.java:318) remains skipped — Rust's
  default allocator aborts on OOM rather than throwing a recoverable error, so
  the `safeAllocateByteBuffer` recovery path does not exist (pre-existing
  decision, unchanged).
</content>
