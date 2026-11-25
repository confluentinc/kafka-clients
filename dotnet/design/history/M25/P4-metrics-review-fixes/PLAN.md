# Plan — review fixes for the live metrics map (PR #362)

Status: **done.** Milestone 25 / Phase 4. The owner asked for every finding of the
PR #362 review (dotnet-critic 136 and three parallel reviews) and every Copilot comment on the PR
to be fixed, **in the .NET binding only: no change to the Rust core or the C FFI.** dotnet-actor and
dotnet-critic N=138 (N=137 was a Rust Actor stopped before any commit; its diff was discarded).
Ledger rows start at 643.

Predecessor: `design/history/M25/P3-metrics-live-view/PLAN.md` (decisions A1–A3, ledger 626–642).
The review reports are summarized in each item below; the Copilot comments are on the PR.

## Core follow-ups (not in this PR; recorded, owner's decision)

- **C1. `BufferPool::close` keeps its pooled buffers.** A metrics view kept after the producer
  closes holds the registry's metric map, whose three buffer gauges hold the `BufferPool`; `close`
  keeps the free list, so a disposed producer retains up to `buffer.memory` of native memory until
  its view and metrics are released, which .NET's garbage collector does not see (Copilot; Critic
  136 finding 1). The owner chose to record it (no GC memory pressure, and the view is not freed at
  `Dispose`, which would undo A2). The core fix: move the free list into
  `non_pooled_available_memory` on `close`, and pool no buffer returned after it.
- **C2. `kafka-metrics-count:count` reads 0 after the client is destroyed.** The gauge holds the
  registry's `MetricsShared` weakly, while a view holds only the map, so a kept view reads 0.0
  where Java reads the number of metrics left (Copilot; Critic 136 finding 2). The core fix: hold a
  `Weak` of the `SharedMetricMap`'s inner `Arc`.
- **C3. `set_samples`' C doc** says `samples < 1` "is not reported as an error"; the core panics
  (the `_err` twin returns `IllegalStateError`). No .NET impact: `MetricConfig.Samples(int)` throws
  Java's `IllegalArgumentException` first.
- **C4. `record-error-rate` / `record-error-total` miss a send that fails before the accumulator.**
  The core's `do_send` records no `errors` sensor (only the sender does), where Java's `doSend` records
  one in its catch blocks (found by dotnet-critic 138 while checking D8).
- **C5. A quota violation never fails the call that records it.** The core records every client
  sensor through `record_or_log` (`rust/src/common/metrics/sensor.rs`), which logs the error. Java
  logs it only on its I/O threads; a recording on the caller's thread throws
  `QuotaViolationException` from the call: `flush` with a quota on `flush-time-ns-total`, the
  transaction calls, and `poll`'s fetch sensors (Copilot's second review). Recorded in
  `KafkaMetric.Config(MetricConfig)`'s remarks. A .NET-side refusal of such quotas would be Kafka
  logic in the binding. The core fix: return the error from the caller-thread recordings.
- **C6. A zero time window with a quota on the consumer's fetch rates fails `Poll`.** Already
  recorded (ledger 650): `Poll` throws the core's caught panic as `IllegalStateException`. Copilot's
  second review adds that the positions may have moved past records `Poll` does not return; not
  verified. The core fix: record the fetch sensors without a panic on a zero window, or leave the
  positions unchanged when a recording fails.

## .NET binding (N=138)

- **D1. Live `Keys` / `Values`** (`Internal/MetricsMap.cs`; Copilot; Java-parity F2). Return lazy
  enumerables that read the snapshot iterator into a list in each `GetEnumerator()`, so a kept
  `Keys` sees later registrations and no unfinished enumerator holds a native iterator. Test: keep
  `Keys` and `Values`, change the mock's metrics (`set_mock_metrics`), enumerate again. Update
  ledger 631's text.
- **D2. The count after close** (C2). Every doc that says the values after close behave as Java's
  (`IProducer`, `IAsyncProducer`, `IConsumerCommon`, `KafkaMetric`, `dotnet/CLAUDE.md`) names the
  exception: `kafka-metrics-count`'s `count` reads 0 once the client is disposed, a recorded core
  gap. No test pins the 0 (a test must not pin a core divergence as behaviour, 135-1).
- **D3. Ledger 633 and `dotnet/CLAUDE.md`'s "holds only the registry"** (C1): the view keeps the
  registry's metric map and what its providers capture, so a disposed producer keeps its buffer
  pool's pooled buffers until its view and metrics are released. Correct both texts (and
  `ffi-marshalling.md` where it repeats them), and say it in the `Metrics()` docs as a recorded core
  gap.
- **D4. Java's `Gauge<T>`** (Java-parity F3): add the public `IGauge<T> : IMetricValueProvider<T>`
  in `Common/Metrics/` with Java's `value(MetricConfig, long)` as `Value(MetricConfig config, long
  now)`, Java's Javadoc, the layout and member-shape test rows, and the analyzer lists as the
  build requires; fix `IMetricValueProvider`'s doc that names `Gauge`.
- **D5. `KafkaMetric`'s class doc** (Java-parity F1): the constructor is left out because Java's
  takes a `Time`, which is not public API; `IMetricValueProvider<T>` and `MetricConfig` now exist.
- **D6. Java's tests** (Java-parity F4): translate `KafkaConsumerTest.testAssignedPartitionsMetrics`
  with the public `Assign` and `consumer-coordinator-metrics:assigned-partitions`; list every
  untranslated Java test of the metrics surface with its reason in the test files' remarks
  (`PublicMetricsLiveMapTests`, `PublicSensorRecordingLevelTests`, …).
- **D7. `MetricConfig.cs`'s comment** that the core "does not check the number of samples" (C3): it panics.
- **D8. Zero time window** (stress F1/F2): document on `KafkaMetric.Config(MetricConfig)` that a
  zero time window with a quota makes every record on the metric's sensor fail, as Java's
  `Rate.windowSize` throws `ArithmeticException` in the thread that records the sensor (`send`'s,
  `poll`'s or the I/O thread's); and on `MetricValue()` /
  `Measure` that a zero window throws `IllegalStateException` (the core's caught panic) where Java
  throws `ArithmeticException` — a recorded difference.
- **D9. Nits** (Java-parity F5/F6): document `Measure(null)`'s `ArgumentNullException` on
  `IMeasurable.Measure` / `KafkaMetric.Measurable()`; `KafkaMetric.MetricName()` and
  `MetricValue()` carry Java's own Javadoc instead of `<inheritdoc/>`.
- **D10. Unpaired surrogates in a `MetricConfig`'s tags** (Copilot): they would come back as `?`
  from `Config()`, where Java keeps the string. Reject an unpaired surrogate in a tag key or value
  with `ArgumentException` wherever a `MetricConfig` crosses to the core (`Config(MetricConfig)`,
  `Measure`), as a NUL is rejected; allow valid pairs; document beside the NUL restriction. Tests.
- **D11. Ill-formed UTF-16 in a lookup key** (Copilot): `ContainsKey` / `TryGetValue` / the
  indexer with a `MetricName` whose name, group or tag holds an unpaired surrogate answer "not
  found" (as Java's map, whose keys are never re-encoded), instead of matching a metric whose `?`
  the encoding produced; valid pairs still allowed. Tests, including a client whose `client.id` is
  `?`.
- **D12. Follow-ups**: add C1–C3 and the core texts that differ from Java (`Quota.toString` prints
  `upper=5`, `Quota` equality is bit-wise, the `Sensor` message lacks Java's class name, the
  `Sensor` path that panics) to STATUS's Open line.

Gate (Actor, filtered): build with 0 warnings, the touched test classes on net8.0 and net10.0,
`dotnet format --verify-no-changes`. The Manager runs the full gate once at the end.

## Commits (the Manager, as the owner, no trailer, not pushed)

1. .NET binding: D1–D11.
2. Records: this plan to history, the review records, STATUS (D12).

## Ledger

Rows for D1–D11, added as the Actor implements them (`Ruling and notes` and `Date` are filled with
the rulings):

| # | ID in the evaluation doc | Decision | My pick | Status | Ruling and notes | Date |
|---|---|---|---|---|---|---|
| 643 | new (PM 138, D1) | `Keys` and `Values` of the live map | live, as Java's `keySet()` and `values()`: each is a view whose every enumeration reads the registry when it starts, into a list it then enumerates; a kept `Keys` or `Values` sees a metric registered later; amends ledger 631 | Evaluate | | |
| 644 | new (PM 138, D2) | `kafka-metrics-count:count` after close | documented on `Metrics()` (producer, consumer, `KafkaMetric`, `dotnet/CLAUDE.md` After close and §6.4, ffi §B2 Category 6): it reads 0 once the client is closed, where Java reads the number of metrics left; a recorded core gap (C2), with no test pinning the core's value | Evaluate | | |
| 645 | new (PM 138, D3) | What a kept view holds | documented on the producers' `Metrics()`, `SafeMetricsViewHandle`, `MetricsMap`, `dotnet/CLAUDE.md` §6.4 and ffi §A2 / §B2: a map kept after close, its `Keys` or `Values` (each holds the view), or its `waiting-threads` / `buffer-total-bytes` / `buffer-available-bytes` metric keeps the buffer pool and its pooled buffers, up to `buffer.memory` of native memory the GC does not see, until released; the GC frees the view once the client, every map over it and every `Keys` or `Values` of one are unreachable; a recorded core gap (C1); amends ledger 633 | Evaluate | | |
| 646 | new (PM 138, D4) | Java's `Gauge<T>` | `IGauge<T> : IMetricValueProvider<T>` in `Confluent.Kafka.Common.Metrics`, with no member of its own, as Java's declares none (its reading is `IMetricValueProvider<T>.Value`); named in `IMetricValueProvider<T>`'s doc; a layout row | Evaluate | | |
| 647 | new (PM 138, D5) | `KafkaMetric`'s missing public constructor | documented on the class: Java's (public for testing) takes a lock, a provider, a `MetricConfig` and a `Time`, and `Time` is not public API | Evaluate | | |
| 648 | new (PM 138, D6) | Java's metrics tests | translated: `KafkaConsumerTest`'s `testAssignedPartitionsMetrics` (without its last step, a partition the group assigns to a subscription, which needs a broker's assignment), `testMetricConfigRecordingLevelInfo`, `testClosingConsumerUnregistersConsumerMetrics`, `testMeasureCommitSyncDurationOnFailure` and `testMeasureCommittedDurationOnFailure` (the call fails by its two-second timeout against an unreachable broker, where Java's fails by an injected authentication error on a `MockTime`; the timeout exceeds Java's 999 ms bound because the core's deadline runs on the wall clock and the metric on the monotonic clock), and `KafkaProducerTest`'s `testMetricConfigRecordingLevel` and `testFlushMeasureLatency` (on real time); every untranslated test of the metrics surface listed with its reason in the remarks of `PublicMetricsLiveMapTests`, `PublicSensorRecordingLevelTests` and `PublicQuotaTests` | Evaluate | | |
| 649 | new (PM 138, D7) | `MetricConfig`'s internal constructor comment | corrected: values are read back as they are, without the setters' checks; samples are at least 1 because the core's `set_samples` panics below 1, as Java's `samples(int)` throws (C3) | Evaluate | | |
| 650 | new (PM 138, D8) | A zero time window | documented: on `Config(MetricConfig)`, with a quota every record on the metric's sensor fails, as Java's `Rate.windowSize` throws `ArithmeticException` in the thread that records the sensor: for some sensors the thread that calls `send` (`buffer-exhausted-rate`, `bufferpool-wait-ratio`, `record-error-rate`) or `poll` (the consumer's fetch rates), for the others the I/O thread; where the core records on the `Send` or `Poll` call (`buffer-exhausted-rate`, `bufferpool-wait-ratio`, the fetch rates), that call throws `IllegalStateException` (the core's caught panic) where Java's throws `ArithmeticException`; on `MetricValue()`, `Measurable()` and `IMeasurable.Measure`, a rate throws `IllegalStateException` where Java throws `ArithmeticException`; recorded differences, no test | Evaluate | | |
| 651 | new (PM 138, D9) | Doc nits | `Measure`'s `ArgumentNullException` for a null config on `IMeasurable.Measure` and `KafkaMetric.Measurable()`; `MetricName()` and `MetricValue()` carry Java's Javadoc instead of `<inheritdoc/>` | Evaluate | | |
| 652 | new (PM 138, D10) | An unpaired surrogate in a `MetricConfig`'s tags | rejected with `ArgumentException` (parameter `config`) wherever the config crosses to the core (`Config(MetricConfig)`, a measurable's `Measure`, a quota error's metric), as a NUL is, the metric's configuration unchanged; a valid pair crosses unchanged; documented beside the NUL restriction and as a "Strings" exception in `dotnet/CLAUDE.md` | Evaluate | | |
| 653 | new (PM 138, D11) | An unpaired surrogate in a lookup key | `ContainsKey`, `TryGetValue` and the indexer answer "not found" without calling the core, rather than matching a metric whose name holds the `?` the encoding would produce (tested over a metric named `?` and a client whose `client.id` is `?`); the description is not part of the key and still crosses; a valid pair crosses unchanged; a "Strings" exception in `dotnet/CLAUDE.md`. The trade-off, recorded there and in `ToNativeName`'s doc: a client whose configuration holds an unpaired surrogate (`client.id`, a topic) registers its metrics under the `?` that configuration crossed as, so a lookup by the configured string does not find the client's own metrics, which Java's finds, and only a lookup by `?` does (tested with `client.id` `"\uD800"`, the binding's own recorded decision) | Evaluate | | |
