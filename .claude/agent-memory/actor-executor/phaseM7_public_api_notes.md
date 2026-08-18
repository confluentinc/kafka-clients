---
name: phaseM7-public-api-notes
description: Milestone-9 Phase M7 — public Consumer::metrics() trait method + config validators + KafkaConsumerTest/AsyncKafkaConsumerTest metrics tests; closes M6-deferred bg-queue e2e
metadata:
  type: project
---

Milestone-9 Phase M7 (Actor 47): expose the metrics registry M1–M6 built.
PLAN: `design/history/Milestone-9-metrics/Phase-M7-public-api/PLAN.md`.

**metrics() trait method (matches Java — NOT a deviation).** Java's
`Consumer.metrics()` returns `Map<MetricName, ? extends Metric>`. Rust:
`fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>>` on the
`Consumer<K, V>` trait (`src/consumer/mod.rs`), NO default method (acceptable
per consumer-threading.md §2 + it's in Java's interface). Sync, cold path.
`KafkaMetric` impls `Metric`, so `Arc<KafkaMetric>` is the faithful
`? extends Metric`. `Metrics::metrics()` ALREADY returns exactly
`HashMap<MetricName, Arc<KafkaMetric>>` (M1) — impl is a thin snapshot.
- AsyncKafkaConsumer: `self.metrics.metrics()` (owned `Arc<Metrics>`, the SAME
  registry all 7 manager families register into — see below). Java:
  `Collections.unmodifiableMap(metrics.metrics())`, AKC.java:1200.
- MockConsumer: `HashMap::new()` (Java `Collections.emptyMap()`, MockConsumer.java:496).
- Re-export `MetricName`/`Metric`/`MetricValue` (from `common`) +
  `KafkaMetric` (from `common::metrics`) at the `consumer` module root.

**KIP-714 telemetry stays OMITTED (not stubbed).** `clientInstanceId(Duration)`,
`registerMetricForSubscription`, `unregisterMetricFromSubscription` remain off
the trait — the existing "Methods NOT translated" doc block already omitted
them; M7 just updated it to say metrics() is now translated. No
unsupported-returning stub (omission is the codebase precedent for these).

**All 7 manager families share ONE Arc<Metrics>** (so metrics() returns the
full set): fetch (create_fetch_metrics_manager 933), kafka-consumer (943),
offset-commit (945), heartbeat (948), async-consumer (959), rebalance
(1186, conditional on group_id present — `Some(..)` else `None`),
rebalance-callback (1438 via invoker.set_metrics). All take `&metrics` /
`Arc::clone(&metrics)`. Test `metrics_returns_full_registry_snapshot` asserts
the always-present async-consumer family + that every entry's metric_name()
matches its key.

**Config validators (deferred since Phase 11).** Added in
`consumer_config.rs::from_properties`: `metrics.num.samples >= 1`
(Java atLeast(1)), `metrics.sample.window.ms >= 0` (atLeast(0)). Message
shape reuses the existing "Value must be at least N" wording. The
num.samples/sample.window/recording.level WIRING into the MetricConfig was
already done in M3 (`create_fetch_metrics_manager` →
`MetricConfig::new().with_samples().with_time_window_ms().with_record_level()`);
M7 only added input validation. Updated the deferred-validator NOTE block
(16→14 remain). No new public getters needed — fields are pub(crate),
accessed within-crate by the ctor.

**Tests (5 new, lib 2118→2123):**
- `test_record_background_event_queue_size_and_time` (AsyncKafkaConsumerTest,
  M6-DEFERRED — now closed): drains a `ConsumerRebalanceListenerCallbackNeeded`
  bg event under a MOCK ThreadTime advanced 10ms, asserts via public
  `metrics()`: queue-size=0, time-avg=10, time-max=10. KEY PATTERN: swap
  `consumer.time` (private field, same-module test) with a local
  `MockThreadTime` impl ThreadTime; stamp `enqueued_ms = mock.milliseconds()`
  BEFORE sleeping, so `self.time.milliseconds() - enqueued_ms == 10`
  deterministically. No-listener event acks Ok — time recording is
  listener-result-independent.
- `metrics_returns_full_registry_snapshot` (AsyncKafkaConsumerTest).
- `test_metrics_returns_empty_map` (MockConsumer; call as `Consumer::metrics(&c)`).
- `test_poll_time_metrics` / `test_poll_idle_ratio` (KafkaConsumerTest rows
  that read consumer.metrics() for `consumer-metrics` group): translated at
  the KafkaConsumerMetrics RECORDING layer, NOT end-to-end. Use
  `Metrics::with_time(MockTime)` (common::metrics::time::mock::MockTime,
  #[cfg(test)] pub(crate)) for a controllable clock — required because
  `last-poll-seconds-ago` reads the registry clock's `now`. SEED the mock
  clock to non-zero (`time.sleep(1_000_000)`) FIRST: last_poll_ms==0 is the
  "no poll yet" sentinel (returns -1), so a first poll at t=0 would look like
  no-poll. time-between-poll-avg/max + poll-idle-ratio are clock-base
  independent (driven by record_poll_start/end args). The end-to-end
  `consumer.poll()` form needs a MockClient bg-task fixture absent from lib
  unit tests — value math is identical at the recording layer.

**KafkaConsumerTest metrics rows OUT OF SCOPE (documented in
kafka_consumer_metrics.rs test-mod doc-comment):** all the
register/unregisterMetricForSubscription rows (KIP-714), the MetricsReporter
plugin-list rows (config-class reflection), and testAssignedPartitionsMetrics
(M5 rebalance family, covered by ConsumerRebalanceMetricsManagerTest).

**Gotcha:** `Metric` trait must be `use`d in-scope to call `.metric_value()` /
`.metric_name()` on `Arc<KafkaMetric>` (E0599 otherwise).
