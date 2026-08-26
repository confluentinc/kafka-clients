---
name: review-producer-metrics-p1
description: Producer-metrics Phase P1 (KafkaProducerMetrics registry) review patterns — recording-level validator case-sensitivity divergence, verified-clean audit heuristics
metadata:
  type: project
---

Producer-metrics Phase P1 = translate `KafkaProducerMetrics.java` (8 latency
sensors) + `Arc<Metrics>` registry in `KafkaProducer` + `Producer::metrics()`.
Reviewed on branch `producer-metrics`.

**Real finding (filed COMMENTS.4 Issue 1):** `metrics.recording.level`
validator used `value.to_ascii_uppercase()` before comparing to
INFO/DEBUG/TRACE — accepts lowercase, but Java `ProducerConfig.java:461-464`
uses `in("INFO","DEBUG","TRACE")` = `ConfigDef.ValidString`, **case-sensitive
exact match**. Java rejects `debug`; Rust accepts. General heuristic: any
Kafka config backed by `ValidString.in(...)` is case-sensitive — flag Rust
validators that uppercase/lowercase before the membership check. (Note
`RecordingLevel::for_name` IS case-insensitive by design — that's the enum
lookup, not the config validator; don't conflate them.)

**Verified clean (heuristics that held):**
- Latency-metric nanosecond source: `SystemTime::nanoseconds()` in
  `src/common/metrics/time.rs` is `Instant`-based (monotonic) = Java
  `System.nanoTime()`. Was previously wall-clock; already fixed. Check this
  when any `*-time-ns-total` metric is added.
- metadata-wait recording placement: Java captures `nowNanos` AFTER the early
  cached-metadata return (`KafkaProducer.java:1120`, return at 1112) and
  records only on successful loop exit (`:1154`); timeouts throw first. Verify
  the Rust `now_nanos` is below the cached early-return and the record sits
  before the success `Ok`, not in the error arms.
- Tag propagation: `KafkaProducerMetrics.metricName` passes an empty tag map
  in Rust and relies on `Metrics::metric_name` merging `config.tags()`
  (client-id). Java passes `metrics.config().tags()` explicitly AND
  `Metrics.metricName` merges again — same effective set, not double-counted.
  This empty-map-relies-on-merge pattern is correct, not a bug.
- `remove_sensor` (metrics.rs:248) also purges the sensor's metrics from the
  shared registry, so `close()` removal tests asserting `metric().is_none()`
  are meaningful.
- Java `KafkaProducerMetricsTest` has NO `recordPrepareTxn`/`txn-prepare`
  test and omits txn-prepare from the removal assertions — faithful Rust
  omission is correct even though `close()` still removes txn-prepare.
- FFI `ProducerKind::Kafka(Box<KafkaProducer>, Runtime)` boxing: strengthens
  the `&*(k as *const)` -> `'static` cast (stable heap addr); not a behavior
  change, not a bug.
