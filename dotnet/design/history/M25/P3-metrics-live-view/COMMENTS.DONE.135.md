# COMMENTS.DONE.135 — M25/P3 (live `Metrics()` and a complete `KafkaMetric`)

1. **[Medium] The not-measurable test pins a core divergence; the skip reasons and the "one gauge" records are not true.**
   - Commit: `74cf11e0e`.
   - Files:
     - `dotnet/tests/Confluent.Kafka.UnitTests/PublicKafkaMetricMembersTests.cs`: the test at :79-91, the remark at :36-46, the test comment at :81;
     - `dotnet/design/history/M25/P3-metrics-live-view/PLAN.md:408` (ledger 638).
   - **(a) The test asserts the opposite of Java's answer.**
     - The translation of `KafkaMetricTest.testIsMeasurableWithGaugeProvider` runs over `kafka-metrics-count`'s `count`, and asserts `IsMeasurable() == false` and that `Measurable()` throws.
     - Java registers that metric as a `Measurable`. `org.apache.kafka.common.metrics.Metrics#Metrics` calls `addMetric(metricName("count", "kafka-metrics-count", ...), (config, now) -> metrics.size())`. Of the two 2-argument overloads, Java picks `addMetric(MetricName, Measurable)`, the more specific one, because `Measurable <: MetricValueProvider<?>`.
     - So Java's `isMeasurable()` answers `true` and `measurable()` returns the provider. The test comment says so itself, and the plan already records the core's divergence as a core follow-up (§3).
     - What the test pins, then, is the core's divergence, not Java's gauge contract. Once the core follows Java, the test fails while the binding is still right. Until then it says nothing about a real gauge.
     - It also asserts the core's closure text, "Not a measurable: the metric value provider is a closure", which no Java message has.
   - **(b) The remark's skip reason is wrong.** The remark says `testKafkaMetricAcceptsNonMeasurableNonGaugeProvider` and the `testToString*` tests need "a provider of the test's own, which only the constructor takes". The C ABI takes one:
     - `kafka_common_metrics_Metrics_new_err` (BH:29360);
     - `kafka_common_metrics_MetricValueProvider_new_err(self_, value, type_name, out_err)` (BH:29291), whose `value_fn_t` (BH:2467) returns a `kafka_common_MetricValue_t *`, built with `kafka_common_MetricValue_string_err` / `_double_err` (BH:28189-28241);
     - `kafka_common_metrics_Metrics_add_metric_if_absent_err` (BH:29956), which returns an owned `KafkaMetric_t`. A test helper can wrap it with `MetricsViewMarshal.Keep`, as `MockProducerMetrics.cs` already wraps its test-only P/Invoke at :96 (`kafka_producer_MockProducer_set_mock_metrics_err`).

     So two Java tests can be translated through the binding's real `KafkaMetric` path (DoD #3):
     - `testIsMeasurableWithGaugeProvider`, over a non-measurable provider the test builds;
     - `testKafkaMetricAcceptsNonMeasurableNonGaugeProvider`: `MetricValue()` returns the provider's string `"metric value provider"`.

     `testConstructorWithNullProvider` tests the constructor, so its skip stands.
   - **(c) `count` is not the only gauge the core registers.** The remark (:40-41: "the one gauge the core registers for a client"), the test comment (:81: "the only one a client has") and ledger 638 ("the one gauge the core registers") all say it is.
     - The core also registers `preferred-read-replica` per partition through `gauge::new` (`rust/src/consumer/internals/fetch_metrics_manager.rs:438`).
     - That metric is a gauge in Java too: `FetchMetricsManager` registers it as `(Gauge<Integer>) (config, now) -> subscription.preferredReadReplica(tp, 0L).orElse(-1)`.
     - `count` is only the one gauge a client has without a broker.
   - **Fix:**
     - Translate the two Java tests over a provider built through the C ABI by a test helper.
     - If a client-metric check on `count` is kept, name it as the record of the core follow-up, not as the translation of Java's test.
     - Correct the wording in the remark, the test comment and ledger 638.

   Fixed (fixup of 74cf11e0e): the `count` test is gone. `testIsMeasurableWithGaugeProvider`, `testKafkaMetricAcceptsNonMeasurableNonGaugeProvider`, `testToStringWithLambdaProvider` and `testToStringWithAnonymousClassProvider` are translated over providers the test registers in a registry built through test-only P/Invokes (`NativeMetricRegistry.cs`: `Metrics_new_err`, `MetricValueProvider_new_err`, `Measurable_new_err`, `MetricValue_double_err` / `_string_err`, `Metrics_add_metric_if_absent_err`, `Metrics_add_metric_with_measurable`, `Metrics_metric_err`), each kept with `MetricsViewMarshal.Keep`; the gauge test asserts only that the message begins "Not a measurable: ". `testToStringWithStatProvider` stays skipped, its reason now the core's Rust type path (a core follow-up). The remark, ledger 638 and the plan §3 follow-up no longer say "the one gauge"; the `count` divergence stays recorded in plan §3 and the STATUS Open line, with no test.

2. **[Low] The `Measurable()` doc names a core message callers never see.**
   - Commit: `74cf11e0e`.
   - File: `dotnet/src/Confluent.Kafka/Common/Metrics/KafkaMetric.cs:147-151` (the `<exception cref="IllegalStateException">` of `Measurable()`).
   - What is wrong:
     - The doc gives the core's message as "Not a measurable: class &lt;provider&gt;". The core prints that only for a provider that has a type name.
     - For a closure provider it prints "Not a measurable: the metric value provider is a closure" (`rust/src/common/metrics/kafka_metric.rs:113-114`).
     - Every non-measurable metric a .NET caller can get is a core closure: `count` (`rust/src/common/metrics/metrics.rs:361`) and `preferred-read-replica`, both `gauge::new`.
     - The binding's own test asserts the closure text (`PublicKafkaMetricMembersTests.cs:90`). So the documented message is never seen, and the message that is seen is undocumented.
     - The plan's §3 core follow-up ("the `Not a measurable` message print Rust type paths") has the same gap. For the reachable metrics the core prints no type path.
   - Fix: document both core texts, or say the message begins "Not a measurable: ".

   Fixed (fixup of 74cf11e0e): the `<exception>` of `KafkaMetric.Measurable()` says the core's message begins "Not a measurable: " and gives both texts, "class <provider>" and, for a provider with no type name, as every one a client registers, "the metric value provider is a closure". The plan's ABI facts and §3 follow-up name the closure text too.

## Critic 135: Findings

_None open._

## Critic 135: Rulings on the Actor's four points

1. **Baseline (+14 `Sensor` lines): correct.**
   - The 14 lines are exactly Java's 14 public `Sensor` members:
     - `name()` and `shouldRecord()`;
     - the four `record` overloads;
     - the two `checkQuotas` overloads;
     - the four `add` overloads;
     - `hasMetrics()` and `hasExpired()`.
   - Nothing public is left out: `metrics()`, `parents()` and the constructor are package-private, and `StatAndConfig` is private.
   - These are gaps that close once a `Sensor` is handed out (Java's only source is `Metrics.sensor`, which the binding does not expose). They are not deliberate divergences, so the baseline is the right file, not the allowlist. `--add` is the sanctioned route, and A3 approved it.
2. **`RecordingLevel` without `Id()`, `Name()` and `VALUES`: agree.**
   - Java's `public final short id` is the enum's underlying value (`: short`, `INFO = 0` to `TRACE = 2`).
   - Java's `public final String name` equals the constant name for all three levels, so `ToString()` prints it.
   - This is the N2 precedent already used for `TimestampType`, `SecurityProtocol` and `GroupProtocol`.
   - Java has no `VALUES` constant, only `values()`. CKRDA0015 checks only a `VALUES` / `s_values` that exists, so leaving it out fires nothing; the build with the analyzers passed.
   - `MAX_RECORDING_LEVEL_KEY` is present.
3. **One allowlist entry for Java's default `Measurable.value`: agree.**
   - The divergence is permanent by design: `IMeasurable : IMetricValueProvider<double>` declares `Value` on the base interface, so an `IMeasurableWithValue` would only redeclare it.
   - Java's default body is `MeasurableExtensions.Value`.
   - The reason is recorded at the entry (ledger 637). The allowlist, which holds what is kept on purpose, is the right file.
4. **A not-measurable test over `kafka-metrics-count:count`: not acceptable as the translation of `testIsMeasurableWithGaugeProvider`.** See finding 1. That metric is a `Measurable` in Java, so the test pins the core's divergence. A Java-faithful input can be built through the C ABI.

## Critic 135: Not verified

- `MetricsBrokerTests` (Docker is down).
- `tools/AotSmoke` (NU1903 restore failure).
- The full gate, `dotnet format`, and the net462 / net8.0 TFMs. Only net10.0 was run, filtered.
- That a dropped `KafkaMetric` / view runs the native `_destroy`. The finalizer tests show only that the managed objects were collected; the destroy is not observable from C#.

## Critic 135: Re-review of fixup `6405aee9f` (on `74cf11e0e`)

Reviewed `git show 6405aee9f` against items 1 and 2 (now in `COMMENTS.DONE.135.md`). Filtered runs of
`PublicKafkaMetricMembersTests` and `PublicKafkaMetricValueTests`: 43 passed on net10.0, 43 on net8.0.

- **135-1: verified.**
  - The `count` test is gone. The remark at :36-46 now lists only true skip reasons: `measurableValue` is
    package-private; `testConstructorWithNullProvider` tests the constructor; and `testToStringWithStatProvider` hits
    the core's Rust type path. No "one gauge" wording is left in the tests, the plan or the rules.
  - Ledger 638 and plan §3 are fixed. The `count` divergence stays recorded as a core follow-up, with no test pinning
    it.
  - STATUS says 29 cases, which matches the file: 23 `[Fact]` plus 6 `[InlineData]`.
  - `NativeMetricRegistry.cs`: every P/Invoke matches BH in name, Cdecl, `IntPtr` / `long` / `double`, and `out_err`:
    - `Metrics_new_err` (BH:29360);
    - `Metrics_destroy_err`, whose doc says "Sensor and metric handles taken from it stay valid";
    - `MetricValueProvider_new_err` (BH:29291) with `value_fn_t` (BH:2467);
    - `Measurable_new_err` (BH:28866) with `measure_fn_t` (BH:2387);
    - `Metrics_add_metric_if_absent_err` (BH:29956), which returns an owned metric;
    - `Metrics_add_metric_with_measurable`: fallible, called under its own name with `out_err`, and it consumes the
      measurable before it can fail (`rust/src/ffi/common/metrics/metrics.rs:881`), so the "whatever the outcome"
      comment is right;
    - `Metrics_metric_err`: owned, "freed with `KafkaMetric_destroy`", so `MetricsViewMarshal.Keep` is the right
      owner;
    - `MetricValue_double_err` / `_string_err`: owned, the string copied. The reading the callback returns is
      "owned and taken over by Rust", so nothing is freed twice.
  - The registry is destroyed in `finally` and each name in its own `finally`.
  - The delegates are `static readonly` (CKRDA0003) and `self_` is `NULL`, so a kept metric read after its
    registry is gone calls a rooted thunk with no state.
  - The new tests prove what they claim:
    - the gauge test runs Java's input, a provider that is not a `Measurable`, through the binding's real
      `IsMeasurable()` / `Measurable()`;
    - the string-provider test runs `MetricValue()` end to end, through `CopyOutValueAndDestroy`;
    - the two `toString` tests assert Java's exact text.
  - `testToStringWithAnonymousClassProvider` goes through `MetricValueProvider_new` where Java's input is a
    `Measurable`. That is not a gap. Through the C ABI a lambda and an anonymous class are the same input, a
    provider with no type name, and the core's own translation of that test uses `gauge::new` too
    (`rust/src/common/metrics/kafka_metric.rs`, `test_to_string_with_anonymous_class_provider`).
- **135-2: verified.** The `Measurable()` `<exception>` doc now gives both core texts and the common
  "Not a measurable: " prefix. The text matches `kafka_metric.rs:113-114`, and the plan's ABI facts and §3 match it.
- **The scoped `#pragma warning disable CKRDA0004` (`NativeMetricRegistry.cs:133-168`): sound.**
  - **Why it is needed.** `NativeCallbackAnalyzer`'s `Body.Of` accepts only:
    - an empty body;
    - a single non-virtual call to a method of the owner type;
    - one catch-all `try` whose `catch` / `finally` blocks contain only such calls.

    A `return <value>` in a `catch` is not such a call, and neither is `Environment.FailFast`. A constant body
    (`=> 0.0`) is a return of a non-invocation. So no callback that returns a value can pass the rule, and the
    reason in the comment is accurate.
  - **Nothing can throw into native code.**
    - `GaugeValue` and `StringValue` run their whole body inside `try` / `catch (Exception)`, including the
      `using` disposal of the pinned UTF-8. A non-`Exception` throw arrives wrapped in `RuntimeWrappedException`.
    - The catch calls `Environment.FailFast`, which never returns or throws. The `return IntPtr.Zero` after it is
      unreachable at run time, so `NULL` never reaches a core that does not check for it (BH:2464, "never null").
    - `Measure` is a constant and cannot throw.
  - **It is safe across threads.** The callbacks hold no state, so concurrent calls from core threads are safe.
  - **It follows precedent.** The suppression is scoped to the three methods, test-only, and states its reason, as
    the existing scoped `CKRDA0001` / `0005` / `0006` suppressions do. Ending the test host is the right failure
    for test code that cannot build a reading. No production callback returns a value, so the analyzer's limit
    does not affect `src/`.

**New findings from the fixup:** none.

**The review loop is clean:** no item is open.
