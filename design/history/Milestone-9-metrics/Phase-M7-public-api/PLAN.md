# Phase M7 — Public `metrics()` API + config wiring

Actor 47. Plan: `~/.claude/plans/idempotent-dazzling-beacon.md` (Phase M7).
Branch: `consumer-impl`.

## Goal

Expose the metrics registry that M1–M6 built (`AsyncKafkaConsumer`'s owned
`Arc<Metrics>`, into which all 6 manager families register) through the
public `Consumer::metrics()` accessor — which **is** in Java's `Consumer`
interface, so this MATCHES Java's public surface (no deviation). Finalize the
deferred config validators.

## Scope

### 1. Public `metrics()` on the `Consumer` trait

Java `Consumer.metrics()` (`Consumer.java:187`):
`Map<MetricName, ? extends Metric> metrics()`.

- `AsyncKafkaConsumer.metrics()` (`AsyncKafkaConsumer.java:1200-1202`):
  `return Collections.unmodifiableMap(metrics.metrics());`
- `MockConsumer.metrics()` (`MockConsumer.java:496-499`):
  `ensureNotClosed(); return Collections.emptyMap();`

Rust translation:

- Add to the `Consumer<K, V>` trait (`src/consumer/mod.rs`):
  `fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>>;`
  - Sync (Java `metrics()` does not block).
  - Return type mirrors `? extends Metric`: `Arc<KafkaMetric>` (which
    `impl Metric`). This is exactly what `Metrics::metrics()` already returns,
    so the impl is a thin snapshot under the registry lock (cold path).
  - **NO default method.** Acceptable per consumer-threading.md §2 (pre-1.0,
    user impls expected) AND it matches Java's interface (every Java
    `Consumer` impl provides it). Documented on the trait method.
- `AsyncKafkaConsumer::metrics()` snapshots its owned `Arc<Metrics>`
  (M3 field `metrics`) → `self.metrics.metrics()`. All 6 manager families
  (fetch, kafka-consumer, heartbeat, commit, rebalance + callback, async)
  register against this SAME registry, so the snapshot returns the full set.
- `MockConsumer::metrics()` returns `HashMap::new()` (Java `emptyMap()`).
  Java's `ensureNotClosed()` panics-on-closed; Rust mock accessors elsewhere
  do not panic on closed (the mock has no closed-guard on sync accessors in
  the Rust port — `metrics()` follows the same convention, returns empty).
- Re-export `MetricName`, `Metric`, `MetricValue`, `KafkaMetric` from the
  `consumer` module (they appear in the public `metrics()` signature /
  return value).

### 2. Config wiring + validation (`src/consumer/consumer_config.rs`)

- **Wiring already done (M3):** `create_fetch_metrics_manager` builds the
  `MetricConfig` from `metrics_num_samples` / `metrics_sample_window_ms` /
  `metrics_recording_level`. `recording.level` is wired via
  `with_record_level` so DEBUG-only sensors gate correctly. No change needed
  to the wiring; M7 verifies it.
- **Add the deferred validators** (Java `ConsumerConfig`
  `CommonClientConfigs`):
  - `metrics.num.samples` >= 1 (`atLeast(1)`).
  - `metrics.sample.window.ms` >= 0 (`atLeast(0)`).
  Match Java's `ConfigException` message shape (reuse the existing
  `max.poll.records` validator's "Value must be at least N" wording, which
  is the format already used in this file).
  Update the deferred-validator NOTE comment block to remove these two from
  the deferred list.

### 3. clientInstanceId / KIP-714 telemetry — OUT OF SCOPE

Java `Consumer` has `clientInstanceId(Duration)`,
`registerMetricForSubscription(KafkaMetric)`,
`unregisterMetricFromSubscription(KafkaMetric)`. These are KIP-714 broker
telemetry, explicitly out of scope (plan "Out of scope"). The codebase
already documents this in `src/consumer/mod.rs` ("Methods NOT translated").
M7 keeps them OMITTED from the trait and updates that doc block to note
`metrics()` is now translated while telemetry remains deferred. No
`unsupported`-returning stub is added (the precedent is omission, consistent
with how the trait already omits these).

### 4. Tests

- `KafkaConsumerTest` metrics rows MISSING so far: `consumer.metrics()`
  returns a registered metric set (contents/registration). Translate the
  relevant rows.
- Any `AsyncKafkaConsumerTest` `metrics()` test.
- M6-deferred `testRecordBackgroundEventQueueSizeAndBackgroundEventQueueTime`
  end-to-end test — now that public `metrics()` exists, drive it via the
  consumer fixture.
- `MockConsumer` `metrics()` returns empty (Java parity).

## Commits

1. `M7: metrics() trait + impls + re-exports`
2. `M7: config wiring + validation (num.samples >= 1, sample.window.ms >= 0)`
3. `M7: tests (KafkaConsumerTest/AsyncKafkaConsumerTest metrics rows + M6-deferred bg-queue e2e)`

Each: `cargo build` / `cargo test --lib` / `cargo xtask lint` /
`cargo xtask format-check` green.

## Self-review checklist

- `metrics()` returns the full Java metric set (fetch + consumer + heartbeat +
  commit + rebalance + callback + async) with matching names — verify by
  registering against the live consumer registry.
- Config validation matches Java's `atLeast` semantics + message shape.
- No non-Java public API added (only `metrics()`, which IS Java).
- `metrics()` is cold-path; no hot-path regression.
