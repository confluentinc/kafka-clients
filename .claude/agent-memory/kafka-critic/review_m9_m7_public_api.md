---
name: review-m9-m7-public-api
description: M9 Phase M7 (public metrics() API) review — fixture-omits-families test gap; faithful API patterns
metadata:
  type: project
---

Phase M7 added `Consumer::metrics() -> HashMap<MetricName, Arc<KafkaMetric>>`
(faithful analog of Java `Map<MetricName, ? extends Metric>`), config validators
(num.samples>=1, sample.window.ms>=0), and 5 tests.

**Key review heuristic — "full registry snapshot" test fidelity:**
A test named `metrics_returns_full_registry_snapshot` can only prove the families
its FIXTURE actually constructs. The test fixture
(`make_test_consumer_with_channels`) builds `RequestManagers::new(None×7)`, so the
heartbeat/offset-commit/rebalance/rebalance-callback metric families are NEVER
registered (they register only via request managers in the production ctor, lines
944-948/1186/1438). Only fetch + kafka-consumer + async-consumer register eagerly
in the fixture. So a "verifies each manager family present" doc claim is
unprovable in that fixture — flag the doc/name overclaim, not a production bug.
**Production `metrics()` was correct** (`self.metrics.metrics()`); only the test
coverage claim overstated.

**Config validator message parity (Java ConfigException):**
Java format = `"Invalid value " + value + " for configuration " + name + ": " +
message` (ConfigException.java:37) where atLeast message =
`"Value must be at least " + min` (ConfigDef.java:1000). Rust matched exactly:
`Invalid value {v} for configuration {key}: Value must be at least N`. When
reviewing config validators, grep ConfigException.java:37 + ConfigDef Range for
the exact concatenation — message content is a behavioral contract.

**Re-export over-exposure check:** to consume `HashMap<MetricName, Arc<KafkaMetric>>`
a caller needs MetricName (key) + KafkaMetric (value) + Metric (trait w/
metric_value()) + MetricValue (its return). All four required → not over-exposure.

**clientInstanceId/register*/unregister* omission = KIP-714, correctly deferred**
(documented in trait doc + kafka_consumer_metrics.rs skip notes), not stubbed.

**bg-queue test mock-clock pattern:** swap `consumer.time` to a mock ThreadTime,
stamp enqueued_ms, sleep, drain → records `now - enqueued`. Faithful when the
SAME mock drives both stamp and drain read. Java
testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime:1952.
