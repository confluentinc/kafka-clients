# COMMENTS.DONE.138 — review fixes for the live metrics map (D1–D11)

## 138-1 — D11 hides a client's own metrics from a lookup by the configured `client.id` / topic (Low)

- **Files:** `src/Confluent.Kafka/Internal/Interop/MetricsViewMarshal.cs:117-140, 283-284` (`ToNativeName`,
  `CrossesUnchanged`); its doc at `:121-122`; `dotnet/CLAUDE.md:878-883`; ledger 653
  (`design/history/M25/P4-metrics-review-fixes/PLAN.md:102`).
- **Defect:** a tag value the core registers often comes from the configuration (`client.id` in every
  group's `client-id` tag; a topic in the per-topic tags). Configuration strings cross with the `?` rule
  (`dotnet/CLAUDE.md` §3 **Strings**), so a `client.id` holding an unpaired surrogate is registered as `?`.
  Before D11 a lookup with the configured string crossed the same way and found the client's own metric. Now
  `ToNativeName` returns `IntPtr.Zero` for it, so the lookup answers "not found". A lookup with the literal `"?"`
  still finds the metric. Java does the opposite in both cases: its tag holds the configured `String` unchanged,
  so the configured value finds the metric and `"?"` finds nothing.
- **Failure scenario:** `new KafkaProducer(new() { ["bootstrap.servers"] = "…", ["client.id"] = "\uD800" })`,
  then `producer.Metrics().ContainsKey(new MetricName("flush-time-ns-total", "producer-metrics", "",
  new Dictionary<string, string> { ["client-id"] = "\uD800" }))` returns `false`. Java returns `true`, and the
  binding returned `true` before D11.
- **Evidence:** I ran a scratch console probe against the built net8.0 `Confluent.Kafka.dll`. The registered
  tag was `"?"`. `ContainsKey` with the configured `"\uD800"` returned `False` and with `"?"` returned `True`.
  The ledger 653 test (`PublicMetricsLiveMapTests.cs:330-350`) uses `client.id = "?"` and looks up `"\uD800"`.
  That is the case D11 aims at, not the configured-surrogate case. The texts that justify the rule ("no
  registered name can hold one", "as Java's does") hold for the core's strings but not for the key a caller
  builds from its own configuration.
- **Suggested fix:** this is the owner's trade-off between two corner cases, so record it either way. Either
  keep D11 and name this case in ledger 653, the `dotnet/CLAUDE.md` **Strings** bullet and `ToNativeName`'s
  doc (a client whose configuration holds an unpaired surrogate is found only under `?`, the configured
  string's crossed form), dropping "as Java's does" there. Or limit the filter so that a tag value is not
  filtered, which keeps the pre-D11 lookup. Do not add a test that pins the `?` lookup (135-1).

**Fixed (uncommitted; the Manager commits it with D1–D11).** Manager's decision: D11 is kept and the trade-off is recorded, with no code change.
- `ToNativeName`'s doc (`Internal/Interop/MetricsViewMarshal.cs`), the `dotnet/CLAUDE.md` **Strings** bullet and ledger 653 no longer say "as Java's does". Each now names the case: a client whose configuration holds an unpaired surrogate (`client.id`, a topic) registers its metrics under the `?` that configuration crossed as. A lookup by the configured string therefore does not find the client's own metrics, which Java's finds; only a lookup by `?` does.
- The **Strings** bullet also carries the critic's rule suggestion: a filter justified by "the core holds no such string" also turns away keys a caller builds from its own configuration, since configuration crosses as `?`, so a new filter states its effect on those keys.
- The existing test comment is scoped to its own case, a metric named `?`, where Java's map also finds nothing.
- New test `PublicMetricsLiveMapTests.LiveMap_AClientWhoseClientIdHasAnUnpairedSurrogate_IsFoundOnlyUnderTheQuestionMarkItCrossedAs`, with `client.id` `"\uD800"`:
  - the registered tag is `?`;
  - the lookup by `"\uD800"` is not found through `ContainsKey`, `TryGetValue` or the indexer;
  - the lookup by `"?"` is found.
- Its comment says it pins the binding's own recorded decision, not a core divergence. The Manager overrode the critic's "no test that pins the `?` lookup" here.

## 138-2 — `Config(MetricConfig)`'s zero-window remark names the wrong Java thread and leaves out the .NET difference (Low)

- **Files:** `src/Confluent.Kafka/Common/Metrics/KafkaMetric.cs:116-119`; ledger 650
  (`PLAN.md:99`), D8 (`:56-60`).
- **Defect:** the remark says Java's `Rate.windowSize` throws `ArithmeticException` "in the thread that
  records, its I/O thread". That holds only for sensors the I/O thread records. The quota check runs in
  `Sensor.record` → `checkQuotas` on whichever thread records. For example, `buffer-exhausted-rate` is recorded
  in `BufferPool.allocate` (`recordBufferExhausted`) on the thread that calls `send`, and the consumer's fetch
  rates (`records-consumed-rate`, `bytes-consumed-rate`) in `CompletedFetch.recordAggregatedMetrics`, while
  `FetchCollector` collects on the thread that calls `poll`. On
  those paths Java's caller gets `ArithmeticException` from `send` / `poll`. In the binding, the core's division
  panics, the `_err` twin catches it, and the caller gets `IllegalStateException` (the caught panic). That is the
  same difference the remark on `MetricValue()` (`:72`) and `Measurable()` (`:166`) records, but here it is not
  recorded.
- **Failure scenario:** a user sets `Config(new MetricConfig().TimeWindow(0, …).Quota(…))` on
  `records-consumed-rate`. The next `Poll` that records fails with `IllegalStateException`, where Java's fails
  with `ArithmeticException`. The doc points the reader at the I/O thread.
- **Suggested fix:** say "in the thread that records the sensor (the caller's `Send` or `Poll` for some
  sensors)", and add the recorded difference: on those calls the binding throws `IllegalStateException` where
  Java throws `ArithmeticException`. Amend ledger 650 the same way.

**Fixed (uncommitted).**
- `KafkaMetric.Config(MetricConfig)`'s remark now names the recording thread. Java checks the quota on each record, in the thread that records the sensor:
  - the thread that calls `send`, for `buffer-exhausted-rate`;
  - the thread that calls `poll`, for the consumer's fetch rates (`records-consumed-rate`, `bytes-consumed-rate`);
  - the I/O thread, for the rest.
- The remark records the difference: on the first two, the binding's `Send` / `Poll` throws `IllegalStateException` (the core's caught panic) where Java's throws `ArithmeticException`.
- Ledger 650 is amended the same way.
- The plan's D8 item text, the Manager's spec, is unchanged.

## 138-3 — a kept `Keys` / `Values` also keeps the view and, on a producer, the buffer pool; the retention texts list only the map (Low)

- **Files:** `src/Confluent.Kafka/Internal/MetricsMap.cs:41-42` (each `SnapshotView` lambda captures `view`);
  `Internal/Interop/SafeMetricsViewHandle.cs:19-24` ("after the client and every map over it are
  unreachable"); `Producer/IProducer.cs:349-352`, `Producer/IAsyncProducer.cs:470-473` ("A map kept after the
  producer is closed, or its … metric, also keeps the producer's buffer pool"); `dotnet/CLAUDE.md:1827-1832`
  ("the view: the client and every map over it", "whose map or one of those metrics is kept");
  `.claude/rules/ffi-marshalling.md:1818-1819` (§B2 Category 6; §A2's "whose view … is kept" is already
  right); ledgers 633
  (`design/history/M25/P3-metrics-live-view/PLAN.md:406`) and 645.
- **Defect:** D1 made `Keys` and `Values` objects that outlive the map and hold the `SafeMetricsViewHandle`
  through their closures. A caller that keeps only `producer.Metrics().Keys`, for example in a field used to
  enumerate names later, keeps the native view alive after the producer and the map are gone. On a producer the
  view keeps the registry's map, whose buffer gauges keep the `BufferPool` and up to `buffer.memory` of native
  memory (C1). Every text that lists what keeps the view or the pool names only "the client and every map over
  it" or "a map or its three metrics".
- **Failure scenario:** a monitoring helper stores `p.Metrics().Keys` and disposes the producer. Up to
  `buffer.memory` stays allocated, unseen by the GC, and none of the listed holders is reachable.
- **Suggested fix:** in each of those texts, add "or its `Keys` / `Values`" (or say "every map, and every
  `Keys` or `Values` of one"). No code change is needed.

**Fixed (uncommitted).** No code change. Each retention text now counts a kept `Keys` or `Values` as holding the view, and so, on a producer, the buffer pool:
- `SafeMetricsViewHandle`'s summary;
- `IProducer` / `IAsyncProducer` `Metrics()` remarks;
- `MetricsMap`'s class doc ("A kept `Keys` or `Values` holds the view, as the map does");
- `dotnet/CLAUDE.md` §6.4 ("the client, every map over it and every `Keys` or `Values` of one"; "whose map, a `Keys` or `Values` of one, or one of those metrics is kept");
- `ffi-marshalling.md`: the producer handle table, the consumer table and §B2 Category 6;
- ledgers 633 (predecessor plan) and 645.

§A2's "whose view … is kept" was already right and is unchanged.

## 138-4 — the untranslated-test remarks: one producer test missing, one reason untrue (Low)

- **File:** `tests/Confluent.Kafka.UnitTests/PublicMetricsLiveMapTests.cs:47-53, 72-80`; D6 / ledger 648
  ("every untranslated test of the metrics surface listed with its reason").
- **Defect 1:** `KafkaProducerTest.testExplicitlyOnlyEnableClientTelemetryReporter` (`KafkaProducerTest`,
  next to `testExplicitlyOnlyEnableJmxReporter`) is not listed. The consumer's test of that name is listed; the
  producer's reporter list stops at `testProducerJmxPrefix`.
- **Defect 2:** the remark gives `testFlushMeasureLatency` the reason "they advance a `MockTime` the client runs
  on, or answer its requests through a `MockClient`". Its assertions (`flush-time-ns-total > 0` after one
  `flush()`, and greater again after a second) do not depend on either double. The core records the flush with
  `record_flush(self.time.nanoseconds() - start)` over a monotonic `Instant` (`system_time.rs`), and the binding's
  own `PublicKafkaMetricMembersTests` (`:71-83`) already asserts `> 0` after one real `Flush` on a broker-less
  producer. So the test can be translated as Java writes it. (Likewise `testMeasureCommitSyncDurationOnFailure`
  asserts `>= 999 ms`, and the core records commit-sync time on failure too
  (`async_kafka_consumer.rs`, `commit_sync`). A broker-less consumer whose `CommitSync` times out after 1 s
  would meet that assertion, so the stated reason is weak there as well. That one is a judgement call; the
  flush test is not.)
- **Suggested fix:** add the producer test to the reporter bullet. Translate `testFlushMeasureLatency` (two
  `Flush` calls on a broker-less producer, the same two assertions), or else give the reason that is actually
  true.

**Fixed (uncommitted).**
- `KafkaProducerTest.testExplicitlyOnlyEnableClientTelemetryReporter` is added to the reporters bullet in `PublicMetricsLiveMapTests`.
- `testFlushMeasureLatency` is translated as `PublicProducerMetricsTests.FlushMeasureLatency`: real time, an unreachable broker, two `Flush` calls, and Java's two assertions (`> 0`, then greater).
- The commit-on-failure tests are deterministic with a short timeout, so both are translated in `PublicConsumerMetricsTests`:
  - `MeasureCommitSyncDurationOnFailure`: `Commit(offsets, 1 s)`;
  - `MeasureCommittedDurationOnFailure`: `Committed(partitions, 1 s)`.
  Each runs against an unreachable broker, fails with `Common.Errors.TimeoutException`, then asserts Java's bound: `commit-sync-time-ns-total` / `committed-time-ns-total` `>= 999 ms`.
- Five repeated runs were stable.
- The untranslated bullet now lists only the tests that really need the doubles. Its reason is now: exact values that a `MockTime` produces (the poll tests), or timing calls that succeed only because a `MockClient` answers (a commit, a committed lookup, a transaction).
- The class remarks and ledger 648 list the new translations.

## 138-5 — ledger 648 calls the untranslated step of `testAssignedPartitionsMetrics` "classic-protocol" (Low)

- **File:** `design/history/M25/P4-metrics-review-fixes/PLAN.md:97`.
- **Defect:** the ledger says the test is translated "without its last, classic-protocol step". Java's test is
  `@EnumSource(GroupProtocol.class)`, so it runs on both protocols, and its last step is
  `subscription.assignFromSubscribed(Set.of(tp0))`, an assignment the group makes to a subscription. Nothing in
  it is classic-only. The test's own comment (`PublicConsumerMetricsTests.cs:188-189`, "needs a broker's
  assignment") is correct.
- **Suggested fix:** in the ledger, write "without its last step, a partition the group assigns to a
  subscription, which needs a broker's assignment", matching the test comment.

**Fixed (uncommitted).** Ledger 648 now says "without its last step, a partition the group assigns to a subscription, which needs a broker's assignment", matching the test comment.

## 138-6 — the two commit-on-failure timing tests have a margin of about 2 ms against a wall-clock adjustment (Low)

- **Files:** `tests/Confluent.Kafka.UnitTests/PublicConsumerMetricsTests.cs:268-314`
  (`MeasureCommitSyncDurationOnFailure`, `MeasureCommittedDurationOnFailure`; a 1 s timeout, then
  `AssertAtLeast999Ms`).
- **Defect:** the call's 1 s timeout runs on the core's wall clock, but the measured duration runs on its
  monotonic clock:
  - The deadline is `time.milliseconds() + 1000` (`async_kafka_consumer.rs`, `commit_sync_inner` and
    `committed_inner`). `SystemTime::milliseconds` is `std::time::SystemTime::now()` (`system_time.rs:38-43`).
  - The reaper expires the event once `current_time_ms >= deadline_ms` on that clock
    (`completable_event_reaper.rs:95-101`).
  - The metric is `nanoseconds() - start`, where `nanoseconds()` is `Instant` (`system_time.rs:53-55`).

  So the bound holds only while the two clocks advance together:
  - Load cannot break it. The monotonic start is read before the wall deadline is computed, and the reaper can
    only fire late, so a slow or loaded machine only lengthens the measured value.
  - A forward wall-clock adjustment during the call does break it. If the wall clock moves ahead of the monotonic
    one by more than the margin, the call times out early on the monotonic clock and the assertion fails. Such
    adjustments include an NTP or chrony step, VM time sync (Hyper-V / Azure Windows agents, VMware tools), or
    slewing on macOS and Windows, where the monotonic clock is not slewed.
- **Evidence:** a scratch probe against the built net8.0 DLL ran the same calls six times each (consumer on
  `localhost:1`, `Commit(offsets, 1 s)`, then `Committed(partitions, 1 s)`):
  - Every call failed with `TimeoutException`.
  - `commit-sync-time-ns-total` read 1000.9–1002.5 ms and `committed-time-ns-total` 1000.7–1002.5 ms.
  - The margin over Java's 999 ms is therefore 1.7–3.5 ms.
  - The tests also passed three times on each of net8.0 and net10.0.

  Java's test is exact because it runs on a `MockTime`. Here the 1 s timeout is the translation's own choice;
  Java's call has no timeout and fails by an injected authentication error.
- **Failure scenario:** a Windows CI agent's time service steps the clock forward by 5 ms during the
  1-second wait. The reaper expires the commit about 996 ms after the monotonic start, the metric reads about
  996 ms, and `AssertAtLeast999Ms` fails. It is rare, and on a rerun it passes.
- **Suggested fix:** give the calls a 2 s timeout and keep Java's assertion (`>= 999 ms`) unchanged. That
  leaves about 1 s of margin against any clock adjustment, for one extra second per test. Then say in the
  comments that the timeout is longer than the bound on purpose, because the deadline runs on the wall clock.

**Fixed (uncommitted).**
- `MeasureCommitSyncDurationOnFailure` now calls `Commit(offsets, 2 s)` and `MeasureCommittedDurationOnFailure` `Committed(partitions, 2 s)`; both keep Java's `>= 999 ms` assertion.
- Each test's comment says why the timeout exceeds the bound: the core's deadline runs on the wall clock and the metric on the monotonic clock, so a wall-clock step during the call ends it early, and the extra second keeps the bound.
- Ledger 648 says the same.

## 138-7 — `Config(MetricConfig)`'s new remark lists Java's recording threads as complete, and they are not (Low)

- **Files:** `src/Confluent.Kafka/Common/Metrics/KafkaMetric.cs` (the remark on `Config(MetricConfig)`: "the
  thread that calls `send` for `buffer-exhausted-rate`, the thread that calls `poll` for the consumer's fetch
  rates …, and the I/O thread for the rest"); ledger 650 (`design/history/M25/P4-metrics-review-fixes/PLAN.md:100`), which
  repeats it.
- **Defect:** "the I/O thread for the rest" is false. Java records two more rate metrics on the thread that calls
  `send`:
  - `bufferpool-wait-ratio`, a `Rate` on the `bufferpool-wait-time` sensor, recorded by
    `BufferPool.awaitMemory` → `recordWaitTime` inside `allocate`;
  - `record-error-rate`, a `Meter` on the `errors` sensor, recorded in `KafkaProducer.doSend`'s
    `catch (ApiException | InterruptedException | KafkaException)` blocks (`this.errors.record()`).

  The binding clause ("On the first two the binding's `Send` or `Poll` throws `IllegalStateException`") also
  misses `bufferpool-wait-ratio`. The core records it on the send path too
  (`buffer_pool.rs`, `await_memory` → `record_wait_time`), so a zero window with a quota on it makes the binding's
  `Send` throw `IllegalStateException` when the pool must wait.
- **Failure scenario:** a user sets a zero window and a quota on `bufferpool-wait-ratio`. A `Send` that waits for
  buffer memory then throws `IllegalStateException`. The doc says that metric's failure happens on the I/O thread
  and names `Send` only for `buffer-exhausted-rate`.
- **Suggested fix:** do not present the list as complete. For example: "in the thread that records the sensor,
  for some sensors the thread that calls `send` (`buffer-exhausted-rate`, `bufferpool-wait-ratio`,
  `record-error-rate`) or `poll` (the consumer's fetch rates), for the others the I/O thread". Then: "where the
  native core records on the `Send` or `Poll` call (`buffer-exhausted-rate`, `bufferpool-wait-ratio`, the fetch
  rates), that call throws `IllegalStateException`". Amend ledger 650 to match.
- **Out of scope (for `kafka-critic`, not this binding):** the core's `do_send` records no `errors` sensor. The
  only `metrics.sensor("errors")` is in `sender.rs`. So `record-error-rate` and `record-error-total` do not count
  a send that fails before the accumulator, where Java's `doSend` does. The binding text above should therefore
  not list `record-error-rate` among the metrics on which `Send` throws.

**Fixed (uncommitted).**
- The remark on `Config(MetricConfig)` no longer presents the list as complete: Java records in the thread that records the sensor, for some sensors the thread that calls `send` (`buffer-exhausted-rate`, `bufferpool-wait-ratio`, `record-error-rate`) or `poll` (the consumer's fetch rates), for the others the I/O thread.
- Its binding clause now covers `bufferpool-wait-ratio`: where the core records on the `Send` or `Poll` call (`buffer-exhausted-rate`, `bufferpool-wait-ratio`, the fetch rates), that call throws `IllegalStateException` where Java's throws `ArithmeticException`. `record-error-rate` is not listed there, since the core's `do_send` records no `errors` sensor (the out-of-scope core gap above, for `kafka-critic`).
- Ledger 650 matches.

## Critic 138: checked and found correct

- Owner's constraint: no file under `rust/` changed, the C ABI header is unchanged, and no `DllImport` was
  added. The new checks use existing entry points or none.
- Build: 0 warnings. The touched test classes pass on net8.0 and net10.0. `dotnet format` is clean on the
  touched files.
- D1: each enumeration of `Keys` / `Values` reads a fresh snapshot into a list. No native iterator outlives an
  enumeration. It works after dispose and with repeated or LINQ enumeration, and the test keeps both, changes
  the mock's metrics and enumerates again.
- D2: verified on both clients with a probe: the count reads 0 after `Dispose` / `Close`, and the map still
  answers. Every listed doc names the gap, and no test pins the 0.
- D3: the core facts hold. The buffer gauges capture `Arc<BufferPool>`, and `BufferPool::close` only marks the
  pool closed and keeps its free list. The texts match apart from 138-3.
- D4: `IGauge<T> : IMetricValueProvider<T>` with no member, as Java's `Gauge` declares none. The layout row and
  `IMetricValueProvider`'s cref are added.
- D5, D7, D9: the texts match Java's Javadoc and the core (`set_samples` panics below 1).
- D10: every path that sends a `MetricConfig` to the core checks it: `Config(MetricConfig)`, `Measure`, and
  the quota error's constant metric through `ToNative`. The checks run in the documented order (null key, null
  value, NUL, surrogate) and throw `ArgumentException` with `ParamName` `config`. `HasUnpairedSurrogate` is
  correct at every position, and the tests cover a lone high surrogate at the end, a lone low one at the start,
  low-then-high, and valid pairs. No other string path changed.
- Ledgers 631 and 633 as amended agree with the code, apart from 138-3.
- D6: the translated consumer and producer tests assert what Java asserts. The `MetricsTest` / `SensorTest`
  untranslated lists are complete.
- Memory safety: no new handle, pin or callback. `ToNativeName`'s `IntPtr.Zero` short-circuit frees nothing
  and calls nothing.
- No internal links, ticket IDs or dates were added to the changed files.

## Critic 138: rule-update suggestions

`COMMENTS.FP.md` and `COMMENTS.FN.md` do not exist in `dotnet/` or at the root, so there are no recorded
false positives or false negatives to learn from. One suggestion follows from 138-1 and 138-3:

- (Applied with 138-1: the **Strings** bullet now states it.) `dotnet/CLAUDE.md` §3 **Strings**: when a lookup or input filter rejects strings the `?` rule would change,
  the rule should also state its effect on keys the caller builds from its own configuration. Configuration
  crosses with the `?` rule, so the core holds that configuration's `?` form. A filter keyed on "the core holds
  no such string" also turns away the caller's own configured value.
