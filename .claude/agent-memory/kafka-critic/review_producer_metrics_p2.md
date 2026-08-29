---
name: review-producer-metrics-p2
description: Phase P2 sender-metrics review — throttle recording location, shared in-flight atomic, updateProduceRequestMetrics reorder audit
metadata:
  type: project
---

Phase P2 (SenderMetricsRegistry/ProducerMetrics/SenderMetrics + throttle plumbing) reviewed clean except a duplicated `#[allow(clippy::too_many_arguments)]` on `Sender::new`.

**Why (audit heuristics that mattered):**
- Throttle recording must sit in the UNIFORM completed-receives handler so internal responses (ApiVersions/Metadata) get recorded too — `testQuotaMetrics` avg=250 fails if ApiVersions=400 isn't recorded. Verify the record call is above the `is_internal_request` branch, inside the `Ok(response)` arm (parse-error skips = Java-faithful).
- `requests-in-flight` gauge shares the SAME `Arc<AtomicI32>` (count_handle clones the Arc), so no drift is possible — the only change is the Arc wrapper; increment/decrement sites untouched. MockClient computes count from `requests.len()` (no atomic), keeps zero-handle default — safe because every Java SenderTest asserts `client.inFlightRequestCount()` directly, never the metric value.
- `updateProduceRequestMetrics` reorder (Rust records BEFORE add_to_inflight_batches because Rust moves batches out of the map; Java records AFTER at Sender.java:434): sound because recorded values aren't mutated by intervening steps AND the batch set is identical (Java's `batches` map isn't drained by addToInflight/failExpired either — expiry uses separate lists).

**How to apply:** for any metrics-recording-reorder justification, confirm both (a) values unchanged by intervening ops and (b) same set of items iterated. For per-node `node-<id>.latency`, the `if let Some(sensor)` guard is correct even though those sensors aren't registered yet (Selector metrics, out of scope) — Java-faithful no-op.

Case-sensitive `metrics.recording.level`: Java `ConfigDef.ValidString.in` (ConfigDef.java:1103) is case-SENSITIVE; the case-insensitive variant is :1131. Error msg = `Invalid value X for configuration NAME: String must be one of: INFO, DEBUG, TRACE` (ConfigException.java:37 + validstring order from ProducerConfig.java:464). Same fix applied to both producer (b3d22294) and consumer (8268a9ac) configs.
