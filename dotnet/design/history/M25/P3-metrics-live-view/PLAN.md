# Plan: live `Metrics()` and a complete `KafkaMetric`

Status: **done.** Decisions A1–A3 approved. One phase: three commits and one review fixup, one Critic
review after the third commit (clean after the fixup), and the close gate passed.

## 1. Goal

Make a client's `Metrics()` behave as Java's `metrics()` does:

1. The map is a **live, read-only view** of the client's metric registry: a metric registered later
   appears in a map taken earlier, and a removed one disappears.
2. Every value **is a `KafkaMetric`**, so Java code that casts to it keeps working.
3. `KafkaMetric.MetricValue()` **measures on every read**, so a kept metric shows the current value.
4. `KafkaMetric` has Java's `Config()`, `Config(MetricConfig)`, `IsMeasurable()` and `Measurable()`.

Items 1–3 fix the two medium findings: the snapshot map, and values that are not `KafkaMetric`.
Item 4 fixes the low finding: the missing `KafkaMetric` members.

## 2. Facts this plan rests on

### Java 4.4

- `Producer`/`Consumer.metrics()` return `Map<MetricName, ? extends Metric>`.
  - `KafkaProducer`, `KafkaConsumer`, `ShareConsumer` and `KafkaAdminClient` return
    `Collections.unmodifiableMap(metrics.metrics())`, documented as "an unmodifiable live view …
    Changes to the underlying metrics will be reflected in the returned map".
  - Every value is the registry's `KafkaMetric`.
  - The real clients answer after `close()`; the registry's components remove their own metrics on
    close.
- `MockProducer.metrics()` returns its own mutable map, filled by `setMockMetrics(MetricName, Metric)`.
- `MockConsumer.metrics()` throws `IllegalStateException("This consumer has already been closed.")`
  after close, and otherwise returns an empty map.
- `KafkaMetric`: `@InterfaceAudience.Public`, `final`.
  - Members: `metricName()`; `metricValue()` (measures on each call, under the metric's lock);
    `config()` (returns the metric's own `MetricConfig` object); `config(MetricConfig)`;
    `isMeasurable()`; `measurable()`.
  - `measurable()` throws `IllegalStateException("Not a measurable: " + providerClass)`.
  - `toString()` is `KafkaMetric [metricName=…, metricValueProvider=<class name>]`, or
    `KafkaMetric [metricName=…]` for a lambda or anonymous provider.
  - Equality is identity.
  - The public constructor takes a lock, a `MetricValueProvider`, a `MetricConfig` and a `Time`.
    `Time` is not public API, and Java marks the constructor
    `@SuppressKafkaInternalApiUsage("KIP-1265: ctor leaks internal Time …")`.
- `MetricConfig`, `Measurable`, `MetricValueProvider`, `Quota` and `Sensor` (with its nested enum
  `RecordingLevel`) are all `@InterfaceAudience.Public`. Nothing in them is deprecated.
  - `MetricConfig`: one no-argument constructor; fluent setters that return `this`; one precondition,
    `samples(int)` < 1 → `IllegalArgumentException("The number of samples must be at least 1.")`.
  - `Measurable extends MetricValueProvider<Double>`, with abstract `measure(MetricConfig, long)` and a
    default `value(...)`.
  - `Quota`: `equals` uses double `==`; `hashCode` truncates the bound to `int`; `toString` is
    `upper=5.0` or `lower=5.0`.
- Java tests that pin this behaviour:
  - `KafkaMetricTest`;
  - `SensorTest.testRecordLevelEnum` and `testUpdatingMetricConfigIsReflectedInTheSensor`;
  - `MetricsTest.testQuotasEquality`;
  - in `KafkaProducerTest` and `KafkaConsumerTest`: the `(KafkaMetric) producer.metrics()…` casts,
    `testMetricsRemovedOnClose`, and `testPollTimeMetrics` / `testPollIdleRatio` (re-measurement under
    `MockTime`).

### Rust core and C ABI (current master)

- The Rust clients behave as Java's:
  - `KafkaProducer::metrics` and `AsyncKafkaConsumer::metrics` return a live `MetricsView` and make no
    closed check, so they answer after close.
  - `MockProducer::metrics` answers.
  - `MockConsumer::metrics` panics after close with "This consumer has already been closed.", as
    Java's mock throws.
- `kafka_producer_Producer_metrics_err` and `kafka_consumer_Consumer_metrics_err` return an **owned,
  live** `kafka_common_metrics_MetricsView_t`.
  - Its C doc: "Every call reads the registry as it is at the time of the call. The view stays valid
    after its client is destroyed."
  - Both answer after close. Once the producer's destroy has begun, a new call returns an empty view.
  - A mock producer's view is live over the mock's own map.
  - A mock consumer's view is empty, standalone, and does not throw after close.
- `MetricsView_len_err`, `_contains_key_err` and `_get_err`.
  - `_get_err` returns an **owned** `KafkaMetric_t`, independent of the view, or NULL.
- `MetricsView_iterator_err` iterates a **snapshot**.
  - Each entry's key and value are borrowed until the next `kafka_Iterator_next`.
  - To keep a metric, call `_get_err(view, entryKey)` while the entry is current.
- `KafkaMetric_t`, freed by `KafkaMetric_destroy_err` ("the metric stays registered"):
  - `__as_Metric_err` → `kafka_common_Metric_metric_name_err` (borrowed name) and
    `kafka_common_Metric_metric_value_err` (owned value, **measured at call time**).
  - `_config_err` returns an owned copy; "setting through it does not change the metric".
  - `_set_config_err`, `_is_measurable_err` and `_to_string_err`.
  - `kafka_common_metrics_KafkaMetric_measurable` is fallible. It returns a **borrowed**
    `Measurable_t`, valid as long as the metric handle, or a `LocalIllegalState` error with the text
    `Not a measurable: class <Rust type>`, or `Not a measurable: the metric value provider is a closure` for a
    provider with no type name.
- `kafka_common_metrics_Measurable_measure_err(measurable, config, now)`.
  - Also available: the `MetricConfig_*`, `Quota_*`, `Sensor_RecordingLevel_*` and `TimeUnit_*`
    families, and `kafka_common_MetricName_new_err` (for lookups by a managed `MetricName`).
- The ABI has **no** clone, equality or identity function for `KafkaMetric_t`.
- There is no admin metrics function at all.
- The quota-violation error payload carries only a `MetricName`, a value and a bound: **no
  `KafkaMetric_t`**.

### The .NET binding today

- `MetricsViewMarshal.CopyOutAndDestroy` copies every entry into an internal `Metric : IMetric` and
  frees the view and its iterator, so values are measured once.
  - Called from `NativeProducer.Metrics()` and `NativeConsumer.Metrics()`.
  - The eight public `Metrics()` methods are one-liners over these two.
- The public `KafkaMetric` holds a name and a frozen value, with an internal constructor. It is used
  only by `QuotaViolationException`, a generated class.
- `MetricConfig`, `IMeasurable`, `IMetricValueProvider<T>`, `Quota`, `Sensor` and `RecordingLevel`
  do not exist.
- After close, `Metrics()` throws the closed client's `IllegalStateException`, because `Close`
  destroys the native client. This is a recorded deviation: "goes when the native object outlives
  `Close`". The view does outlive it. Python's binding has the same deviation; this plan does not
  follow it.
- The rules currently mandate copy-out:
  - `dotnet/CLAUDE.md` §6.4: copy-out is the default; keep-alive only through `BytesMemoryManager`.
  - `ffi-marshalling.md` §A2: Category 3 names `Metrics()` as copy-out; "Copy-out is the only mode".

## 3. Scope

One phase, three commits, in this order:

| Commit | Delivers | Fixes |
|---|---|---|
| **1** | Every value of `Metrics()` is a handle-backed `KafkaMetric` that measures on each read; the internal `Metric` class is deleted; the rule amendment (A1) | values that are not `KafkaMetric` (medium) |
| **2** | `Metrics()` returns a live read-only map over a view taken at construction, so it answers after close where Java's does (A2) | the snapshot map (medium) |
| **3** | `KafkaMetric.Config()`, `Config(MetricConfig)`, `IsMeasurable()`, `Measurable()`; new public types `MetricConfig`, `IMeasurable`, `IMetricValueProvider<T>`, `Quota`, `Sensor.RecordingLevel` (+ `RecordingLevelMethods`) | the missing members (low), except the constructor |

Rough size: about 2,500 lines added (≈ 550, 750 and 1,250 per commit), about 40% of them tests.

**Out of scope** (each is its own piece of work):

- `Admin.Metrics()`. It needs a Rust `Admin::metrics` and a C export first.
- `MockProducer.SetMockMetrics(MetricName, Metric)`. The C ABI takes only a native `KafkaMetric_t`,
  not a user `Metric`.
- The public `KafkaMetric` constructor. `Time` is not public API, and Java marks the constructor as a
  leak under review. The existing record stays.
- The stat classes (`Avg`, `Rate`, …) that Java code casts `measurable()` to.
- The Python binding's identical snapshot and after-close behaviour. Same fix, separate plan.
- Core text follow-ups:
  - `KafkaMetric` `toString` and the `Not a measurable` message print Rust type paths where Java
    prints Java class names. For a provider with no type name, which every non-measurable metric a client
    registers has, the message says "the metric value provider is a closure" where Java names the lambda's class.
  - The missing selector, node and app-info metrics.
  - `kafka-metrics-count`'s `count` is a gauge in the core, where Java's `Metrics` constructor registers it
    through `addMetric(MetricName, Measurable)`: its `isMeasurable()` answers `false` and `measurable()` throws,
    where Java's answers `true` and returns the provider (found in commit 3). No test pins it: the translation of
    `testIsMeasurableWithGaugeProvider` runs over a provider the test registers.

## 4. Decisions

### Approved

- **A1 — Rule amendment.** Add handle-backed live metrics as a second sanctioned keep-alive form
  beside `BytesMemoryManager`:
  - in `dotnet/CLAUDE.md` §6.4;
  - in `ffi-marshalling.md` §A2: Category 3 example, table row, "Copy-out is the only mode", and the
    `Metrics` root-freed-once test bullet;
  - and under ffi §B2 Category 6 (caller-owned, weak to its parent).

  The existing sentences that say copy-out is the only mode for metrics are changed so they do not
  contradict the new rule.

- **A2 — `Metrics()` after close: answer, as Java and the Rust core do.** Each real client and each
  `MockProducer` takes **one** view at construction and keeps it. Each `Metrics()` call returns a new
  read-only wrapper over that view, as Java returns a new `unmodifiableMap` each call.
  - So `Metrics()` answers after close, showing the registry after the client removed its metrics.
    This lifts the recorded after-close deviation for `Metrics()` only.
  - `MockConsumer` and `AsyncMockConsumer` keep throwing after close, as Java's mock does and the Rust
    mock panics.
  - The consumer owner-check and mock-wait semantics stay exactly as they are today.
  - Python is not followed here.

- **A3 — The missing members are in this phase** (commit 3), with these shapes:
  - **`IMeasurable` and Java's default `value()`.** Keep Java's chain, `IMeasurable :
    IMetricValueProvider<double>`, and put Java's default in `MeasurableExtensions.Value` (Java-names
    rule 10). netstandard2.0 has no default interface methods, so an implementer writes both methods;
    today nothing accepts a user `IMeasurable`, so no implementer exists yet.
  - **`Sensor.RecordingLevel` placement.** A `Sensor` class with no public constructor holds the
    nested `RecordingLevel` enum, plus a top-level `RecordingLevelMethods` (rule 9). The Java-name
    baseline keeps listing `Sensor`'s other members as absent.

### Within the rules

Each gets a ledger row, and the Critic checks it:

- **D1 — The map.** An internal `MetricsMap : IReadOnlyDictionary<MetricName, IMetric>` over a new
  `SafeMetricsViewHandle`. The public signature is unchanged.
  - `Count` → `_len`.
  - `ContainsKey`, `TryGetValue` and the indexer → a temporary `MetricName_t` from
    `MetricName_new_err`, then `_contains_key` / `_get`.
  - Enumeration, `Keys` and `Values` → the iterator. Each entry's name is copied out, and its value
    is a `KafkaMetric` built from `_get(view, entryKey)` taken while the entry is current. An entry
    removed between the snapshot and the `_get` is skipped, the weak consistency of Java's
    `ConcurrentHashMap`.
  - The view's handle takes no reference on the client's handle (Category 6), and the client's
    `Dispose` never frees it.
- **D2 — The value.** `KafkaMetric` holds an owned `SafeKafkaMetricHandle`.
  - `MetricName()` is read once, at construction.
  - `MetricValue()` calls `__as_Metric` → `Metric_metric_value` on every read.
  - `ToString()` uses `_to_string` (see D7).
  - It is not `IDisposable`. The SafeHandle's finalizer frees it, as for `ConsumerGroupMetadata`.
  - The internal `Metric` class is deleted.
- **D3 — Identity.** Two lookups return two `KafkaMetric` objects over the same native metric, where
  Java returns one object. The ABI has no identity function, and a wrapper cache could return a
  metric that was removed and re-registered. Recorded; no cache.
- **D4 — Read-only.** The map does not implement `IDictionary`, so a cast to `IDictionary` throws
  `InvalidCastException` where Java's `clear()` throws `UnsupportedOperationException`. This is the
  .NET idiom for a read-only map. Recorded.
- **D5 — The quota error's metric.** `QuotaViolationException.Metric()` keeps a `KafkaMetric` with a
  constant source (name + violating value), because the core's payload has no metric handle.
  - The generated class and its emitter stay unchanged: the internal `(MetricName, object)`
    constructor remains, as that source.
  - Commit 3: such a metric answers `IsMeasurable() == false`, `Config() == null`, and `Measurable()`
    throws `IllegalStateException`. Recorded in the class doc.
- **D6 — Config.** `MetricConfig` is a managed value class, as the binding's other Java value classes
  are, with Java's one precondition. It is converted at the boundary (`MetricConfig_new_err` + setters,
  and the getters back).
  - `Config()` returns a copy. `Config(MetricConfig)` replaces the metric's config through
    `_set_config`.
  - So mutating a returned config does not change the metric, where Java's `config()` returns the
    metric's own object. The C ABI behaves the same way. Recorded.
  - Java's `timeWindow(long, TimeUnit)` becomes `TimeWindow(TimeSpan)` (rule 6), sent as
    milliseconds with `TimeUnit_milliseconds`.
- **D7 — Texts from the core.** `ToString()` and the `Measurable()` error use the core's text, which
  names the Rust provider type where Java names the Java class. Recorded here, and filed as a core
  follow-up (§3); the binding adds no mapping of its own.
- **D8 — `Measurable()`.** An internal `NativeMeasurable : IMeasurable` over the borrowed
  `Measurable_t`.
  - It holds its `KafkaMetric` so that handle outlives it.
  - Each `Measure(config, now)` takes a call-scoped reference on the metric's SafeHandle (CKRDA0006),
    converts the config, and calls `Measurable_measure_err`.

## 5. Commit 1: values become live `KafkaMetric`s

### Code (`dotnet/src/Confluent.Kafka/`)

- `Internal/Interop/NativeMethods.cs`. Add these P/Invokes, each over a SafeHandle parameter where one
  exists (CKRDA0005): `MetricsView_get_err`, `KafkaMetric_to_string_err`, `KafkaMetric_destroy_err`,
  `kafka_common_Metric_metric_name_err`.
- New `Internal/Interop/SafeKafkaMetricHandle.cs`, deriving from `SafeHandleZeroIsInvalid`.
- `Common/Metrics/KafkaMetric.cs` (D2, D5): two sources, a native handle or a constant name and value.
- `Internal/Interop/MetricsViewMarshal.cs`. `Metrics()` still returns a snapshot dictionary of names
  in this commit, but each value is a `KafkaMetric` built from `_get(view, entryKey)` while the entry
  is current; the view and iterator are freed as today. `CopyOutName`, which `ErrorPayloadMarshal`
  uses, stays.
- `Common/IMetric.cs` loses the internal `Metric`, and its XML doc drops "measured once".
- `grpc-server/Translate.cs` `MetricToProto`. Read `MetricValue()` once, since a second read now
  measures again.
- **Rules (A1):** `dotnet/CLAUDE.md` §2 (the `Metric` beside `IMetric` sentence, ledger 608) and §6.4;
  `ffi-marshalling.md` §A2 and §B2 Category 6.

### Tests (`dotnet/tests/Confluent.Kafka.UnitTests/`, no broker)

- **Values are `KafkaMetric`.** `m is KafkaMetric` holds for every value, and the cast succeeds, on
  real and mock producers and consumers, sync and async.
- **Live value.** A kept `KafkaMetric` changes between reads. Use a metric that moves without a
  broker; the Actor picks one and states why in the test.
- **Lifetime.** A `KafkaMetric` stays readable after its client is disposed. After `GC.Collect()` +
  `WaitForPendingFinalizers()` no handle is freed twice.
- **Concurrency.** A read of a kept metric during `Send`.
- **Keep** `ErrorPayloadReaderTests.QuotaViolation_…` and
  `PublicValueTypeJavaEqualityTests.KafkaMetric_ToString_IsJavasFormWithoutANamedProvider` (constant
  source).
- Update any existing test that pinned a value's type or a value measured once, in this commit.

## 6. Commit 2: the live map, answering after close

### Code

- `NativeMethods.cs`: `MetricsView_len_err`, `_contains_key_err`, `kafka_common_MetricName_new_err`,
  `MetricName_destroy_err`.
- New `Internal/Interop/SafeMetricsViewHandle.cs`.
- `MetricsViewMarshal.cs`: add `ToNativeName` (managed name → owned `MetricName_t`, freed in a
  `finally`); remove `CopyOutAndDestroy`.
- New internal `Common/MetricsMap.cs` (D1).
- `Internal/NativeProducer.cs` and `Internal/NativeConsumer.cs`. Take the view at construction and
  return `new MetricsMap(view)` (A2).
  - Keep the mock-consumer closed check and the mock wait.
  - Keep `EnterUse(UseKind.OwnedOnMock)` where it exists today.
  - Drop the closed check for `Metrics()` on the real clients and `MockProducer` / `AsyncMockProducer`.
- XML docs:
  - `Producer/IProducer.cs`, `Producer/IAsyncProducer.cs` and `Consumer/IConsumerCommon.cs`: Java's
    "unmodifiable live view" text; a returns line; the after-close note only for the mock consumer.
  - The implementations keep `<inheritdoc/>`.
- **Records:** the `dotnet/CLAUDE.md` after-close list, §3 sketches and §4 "Stays sync"; ffi §B1's
  `Metrics()` after-close sentence; the stale `MetricMap_*` / `MetricMapMarshal` notes.

### Tests

- **Live map.** A map taken earlier sees a metric added later. Use a test-only P/Invoke of
  `kafka_producer_MockProducer_set_mock_metrics_err` with a `KafkaMetric_t` from a real producer, as
  `ErrorPayloadReaderTests` declares test-only imports.
- **Read-only.** The map is not an `IDictionary` (D4).
- **After close (A2).** A real producer and consumer answer, and the map keeps only what the core
  leaves after close; assert against the core's actual set, the analogue of Java's
  `testMetricsRemovedOnClose`. `MockProducer` answers. The mock consumers still throw Java's message.
- **Lifetime.** A map stays readable after the client is disposed. A metric outlives its map wrapper.
- **Concurrency.** Keep `Metrics_ConcurrentWithInFlightPoll_…`.
- **Rewrite the tests that pinned the snapshot:**
  - `Metrics_RealProducer_KeyHasValueIdentity_DescriptionExcluded` and its consumer twin: `Same` →
    equal names (D3).
  - `Metrics_CalledTwice_ReturnsIndependentSnapshots` → "two calls see the same registry".
  - The four producer `…AfterDispose…` tests and `PublicConsumerHandleProtectionTests`' `Metrics` row,
    per A2.
- **Java tests not translated, with the reason in the file:** `testPollTimeMetrics` and
  `testPollIdleRatio` need `MockTime`, which the binding cannot inject. The `…Subscribing…Metrics…`
  tests need `registerMetricForSubscription`, which is not translated.
- **Integration (Docker broker).** The kept producer and consumer counters read 3 after three
  records, and a map taken before the first send gains the topic's metrics.

## 7. Commit 3: the rest of `KafkaMetric`

### Code (`Common/Metrics/`, one file per Java top-level class)

- `MetricConfig.cs`:
  - `DEFAULT_NUM_SAMPLES` and `MetricConfig()`;
  - getter/setter overloads per rule 3, setters returning `this`: `Quota`, `EventWindow`, `Samples`,
    `Tags`, `RecordLevel`;
  - `TimeWindowMs()` and `TimeWindow(TimeSpan)`;
  - Java's `Samples` precondition and message.
- `Quota.cs`:
  - `Quota(double, bool)`, `UpperBound`, `LowerBound`;
  - `IsUpperBound()`, `Bound()`, `Acceptable(double)`;
  - `Equals`/`GetHashCode`/`ToString` per Java, the double printed by `JavaText`.
- `Sensor.cs` (no public constructor) with the nested `RecordingLevel` enum, and a top-level
  `RecordingLevelMethods` (`ForId`, `ForName`, `ShouldRecord`, `Id`, `Name`, `MAX_RECORDING_LEVEL_KEY`,
  `VALUES`), per A3.
- `IMetricValueProvider.cs`, `IMeasurable.cs` and `MeasurableExtensions.cs` (A3).
- `KafkaMetric.cs`: `Config()`, `void Config(MetricConfig)`, `IsMeasurable()` and `Measurable()`, per
  D5, D6 and D8.
- Internal `Internal/Interop/MetricConfigMarshal.cs`: managed ↔ native config, quota and recording
  level.
- Internal `Internal/NativeMeasurable.cs` (D8).
- `NativeMethods.cs`. Add the `KafkaMetric` `_config`, `_set_config`, `_is_measurable` and
  `_measurable` imports, and the `MetricConfig_*`, `Quota_*`, `Sensor_RecordingLevel_*`,
  `TimeUnit_milliseconds` and `Measurable_measure` imports.

### Tests

- **`KafkaMetricTest`, translated where the binding can build the input** (client metrics, not the
  constructor):
  - `testIsMeasurable`: a stat metric.
  - `testIsMeasurableWithGaugeProvider`: a gauge metric whose `Measurable()` throws
    `IllegalStateException`.
  - The constructor tests are skipped with the reason.
- **`MetricConfig`:** defaults; fluent chaining; the `Samples(0)` message; `TimeWindow(TimeSpan)` →
  `TimeWindowMs()`.
- **`Quota`:** `MetricsTest.testQuotasEquality`, plus `Acceptable`, `ToString` and `GetHashCode`.
- **`RecordingLevel`:** `SensorTest.testRecordLevelEnum` (the `ShouldRecord` matrix), plus the
  `ForId` and `ForName` messages.
- **Config round trip:** `Config(new MetricConfig().Samples(5))`, then `Config().Samples() == 5`.
  Mutating the returned copy does not change the metric (D6).
- **`Measurable().Measure(Config(), now)`** agrees with `MetricValue()` for a stat whose value does not
  move between the two calls.
- Update the public-namespace layout rows, the member-shape test, and the Java-name baseline (which
  only shrinks).

## 8. Records to update

Each commit updates the records its change touches (listed in §5–§7). In addition,
`design/current/STATUS.md` gets one entry for the phase: the `Metrics()` entries (snapshot wording,
the stale `MetricMapMarshal` lines) are corrected, and ledger rows from 626 record A1–A3 and D1–D8.

## 9. Acceptance

The audit's repro programs for the three findings are run against the built binding, alongside Java
4.4 and the expected outputs.

- **Live map and live values** (needs a broker): every line prints Java's answer, except the
  `clear()` line, which now throws at the `IDictionary` cast (D4).
- **Values are `KafkaMetric`** (no broker): every line prints Java's answer.
- **Members** (no broker): `config` / `isMeasurable` / `measurable` print `true`. The constructor line
  stays `false` (§3).

The phase is done when, in addition:

- the Critic's review loop is clean;
- `make verify-dotnet` passes, including the AOT smoke test, since the phase adds P/Invokes;
- the full unit and gRPC gate passes once, on net8.0 and net10.0.

## 10. Process

- **Agents.** Actor `dotnet-actor` and Critic `dotnet-critic`, both N = 135, the next unused number in
  `STATUS.md`. The Actor starts from a native library rebuilt from master: the `rust/target/include`
  header is a local build artifact.
- **Order.** The Actor implements the three commits in order (§5, §6, §7), each building and passing
  its filtered tests before the next starts. The Critic is spawned **once**, after the third commit,
  and reviews all three together. Fixes for its comments go in as fixup commits that name the commit
  and the comment they fix, then the Critic re-checks, until the loop is clean.
- **Testing.** Actor and Critic iterate with filtered `dotnet test --filter` runs only. The Manager
  runs the full gate once, when the loop is clean.
- **Commits.** Three commits plus fixups, made by the Manager, authored by the repository's configured
  git user, with no co-author trailer. Nothing is pushed.

## 11. Ledger rows

Rows for A1–A3 and D1–D8, added as each commit implements them (`Ruling and notes` and `Date` are filled
with the rulings):

| # | ID in the evaluation doc | Decision | My pick | Status | Ruling and notes | Date |
|---|---|---|---|---|---|---|
| 626 | new (PM 135, A1) | Handle-backed live metrics as a second sanctioned keep-alive form, beside `BytesMemoryManager` | each `Metrics()` value a `KafkaMetric` over an owned `KafkaMetric_t` from the view's `_get`, released by the GC; `dotnet/CLAUDE.md` §2 and §6.4, ffi §A2 and §B2 Category 6 amended | Agreed | Approved with the plan | |
| 627 | new (PM 135, D2) | The value of `Metrics()` | `KafkaMetric` over an owned `SafeKafkaMetricHandle`: name read once, value measured on each `MetricValue()`, `ToString()` from `_to_string`, not `IDisposable`; the internal `Metric` deleted | Evaluate | | |
| 628 | new (PM 135, D3) | Identity | a new `KafkaMetric` per value read (two `Metrics()` calls give two objects for one native metric, where Java gives one); no wrapper cache | Evaluate | Stated in the `KafkaMetric` doc | |
| 629 | new (PM 135, D5) | The quota error's metric | a constant source (name and violating value), as the core's payload has no metric handle; the generated class and its emitter unchanged | Evaluate | | |
| 630 | new (PM 135, D7) | `ToString()` text | the core's `_to_string`, which names the Rust provider type where Java names the Java class; no binding mapping; a constant-source metric keeps `KafkaMetric [metricName=…]` | Evaluate | Core follow-up (§3) | |
| 631 | new (PM 135, D1) | The map `Metrics()` returns | an internal `MetricsMap : IReadOnlyDictionary<MetricName, IMetric>` over a `SafeMetricsViewHandle`, in `Internal/` (not `Common/`: `dotnet/CLAUDE.md` §2 keeps every internal type there, and the class has no Java counterpart); `Count` → `_len`; a lookup → a `MetricName_t` from `MetricName_new_err`, then `_contains_key` / `_get`, a missing key throwing .NET's `KeyNotFoundException` text and a null key `ArgumentNullException`; enumeration, `Keys` and `Values` read the snapshot iterator into a list, so an unfinished enumerator holds no native iterator; an entry's key is the registry entry's own name, an entry removed before its `_get` is skipped | Evaluate | | |
| 632 | new (PM 135, D4) | Read-only | the map is not an `IDictionary`, so a cast throws `InvalidCastException` where Java's `clear()` throws `UnsupportedOperationException` | Evaluate | | |
| 633 | new (PM 135, A2) | `Metrics()` after close, and the view's lifetime | each real client and each mock producer takes one view at construction and keeps it; every `Metrics()` call wraps it, takes no use and makes no closed check on the real clients, so it answers after close; the mock consumers keep Java's closed check and the mock wait; the client's teardown never frees the view, the GC frees it once the client and every map over it are unreachable (the core's view holds only the registry, so nothing needs it freed at teardown) | Agreed | Approved with the plan | |
| 634 | new (PM 135, D1) | A lookup key the C ABI cannot carry | a `MetricName` whose name, group or a tag key holds a NUL, or whose tag value is null or holds a NUL, is not found (`ContainsKey` false, `TryGetValue` false, the indexer `KeyNotFoundException`), as no registered name can equal it; a NUL in the description cuts the C string there, which is harmless, as equality ignores the description | Evaluate | | |
| 635 | new (PM 135, A2) | The registry after close | each map shows what the core's close leaves registered, which is what Java's leaves: the producer's close removes `KafkaProducerMetrics`' timers, as Java's (the core registers no selector metrics, which Java's close also removes); a consumer without `group.id` keeps only `kafka-metrics-count:count` (Java's `testMetricsRemovedOnClose`, translated); with one it also keeps the coordinator's 17 metrics, as Java's close never closes its commit, heartbeat and rebalance metrics managers; the tests pin each set | Evaluate | | |
| 636 | new (PM 135, A3) | The missing `KafkaMetric` members and the `common.metrics` types | `KafkaMetric.Config()`, `Config(MetricConfig)`, `IsMeasurable()`, `Measurable()`; public `MetricConfig` (not sealed, as Java's is not final), `Quota` (sealed), `IMetricValueProvider<T>`, `IMeasurable : IMetricValueProvider<double>` with `MeasurableExtensions` in its file (`dotnet/CLAUDE.md` §2), `Sensor` (sealed, private constructor) with the nested `RecordingLevel : short` and the top-level `RecordingLevelMethods`; Sensor's other members stay in the Java-name baseline (14 lines, which the type's arrival made visible) | Agreed | Approved with the plan | |
| 637 | new (PM 135, A3) | Java's default `Measurable.value` | `MeasurableExtensions.Value`, Java's body; no `IMeasurableWithValue`, since `Value` is declared on `IMetricValueProvider<double>` and every implementer writes it; the analyzer's `with` finding is allowlisted under that reason; the binding's own `NativeMeasurable.Value` returns `MeasurableExtensions.Value` | Evaluate | | |
| 638 | new (PM 135, A3) | `RecordingLevel`'s members | `ForId`, `ForName` and `ShouldRecord` with Java's three messages, and `MAX_RECORDING_LEVEL_KEY`, in `RecordingLevelMethods`; Java's `id` and `name` fields are the enum's value and `ToString()`, allowlisted with the N2 group (ledger 373, as `TimestampType`'s), so §7's list (`Id`, `Name`, `VALUES`) is not followed for those three: Java has no `VALUES`, and `Id()` / `Name()` would be .NET-only members | Evaluate | | |
| 639 | new (PM 135, D5) | The quota error's metric in commit 3 | `IsMeasurable()` is `false`; `Measurable()` throws `IllegalStateException` "Not a measurable: the metric of a quota violation holds only its name and its value"; `Config()` is `null` until `Config(MetricConfig)` sets one, then a copy of what was set, read back through a native config so it holds what a client's metric would; the same refusals as a client's metric | Evaluate | | |
| 640 | new (PM 135, D6) | `Config` at the boundary | `Config()` copies out of the owned config `_config` returns, and `Config(MetricConfig)` builds a transient one `_set_config` copies, so a change to either object leaves the metric unchanged; a null config throws `ArgumentNullException`; a recording level no member has throws `ForId`'s `IllegalArgumentException`; a null tags map sets no tags (read back empty); a null tag key or value throws `ArgumentNullException` "config must not contain a null tag key/value", a NUL `ArgumentException`, each tag checked once, as it is read; `TimeWindow(TimeSpan)` keeps whole milliseconds, truncated toward zero; a config read back keeps the core's samples and window as they are | Evaluate | | |
| 641 | new (PM 135, D8) | `Measurable()` | a new internal `NativeMeasurable` per call over the `Measurable_t` borrowed from the metric's handle (Java returns the provider itself); it holds its `KafkaMetric`, so it measures after its client is disposed and after the metric is dropped; each `Measure` checks and converts the config, takes a call-scoped reference on the metric's handle (CKRDA0006) and frees the transient config | Evaluate | | |
| 642 | new (PM 135) | `Quota` beside the namespace `Confluent.Kafka.Common.Quota` | Java's name kept; its own namespace finds it by its simple name, and the rest of `Common` writes it `Metrics.Quota` (`dotnet/CLAUDE.md` §2, Names that are also namespaces) | Evaluate | | |
