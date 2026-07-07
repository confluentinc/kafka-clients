---
name: review-m9-m4-consumer-hb-commit-metrics
description: M9 Phase M4 — consumer/heartbeat/commit metrics wiring audit; heartbeat-send-site enumeration heuristic
metadata:
  type: project
---

M9 Phase M4 (commit 715618f): KafkaConsumerMetrics + HeartbeatMetricsManager + OffsetCommitMetricsManager.
Reviewed CLEAN except ONE finding. Value parity, levels, perf, setter-None plumbing all PASS.

**Audit heuristic that found the bug — enumerate ALL Java send/record sites, not just the obvious two.**
Java `AbstractHeartbeatRequestManager` has THREE heartbeat send sites, ALL routing through
`makeHeartbeatRequest(currentTimeMs, ignoreResponse)` which records `recordHeartbeatSentMs`:
  1. poll-timer-expired leave (`:179`, ignore=true)
  2. normal heartbeat (`:198`, ignore=false)
  3. `pollOnClose` leave heartbeat (`:233`, ignore=true)  ← Rust `poll_on_close` MISSED this one.
Rust wired sites 1+2 but `poll_on_close` builds via `build_heartbeat_request(true)` and never calls
`record_heartbeat_sent_ms`. `build_heartbeat_request` does NOT record it (only `make_heartbeat_poll_result`
+ the explicit poll-timer branch do). Effect: `last-heartbeat-seconds-ago` stale on close path.
**When a phase wires a per-event metric, grep ALL sites that build the event in Java (here:
`grep makeHeartbeatRequest`) and confirm each has a Rust counterpart that records.**

**try/finally → inner-helper split is the established M4 pattern** for poll/commit_sync/committed:
public method captures start time AFTER `ensure_open()?` (= Java capturing after `acquireAndEnsureOpen()`),
calls `*_inner`, then records on ALL exit paths. Verified record-on-error/wakeup/early-return. Closed
consumer records nothing in both Java and Rust (capture is after the open-check). No double-record because
the `*_internal`/`*_timeout` private fn is the sole analogue; overload delegators just forward.

**Latency-on-success-only contract:** both commit (`CommitRequestManager.java:767` top of `onResponse`)
and heartbeat (`response != null` guard in `makeHeartbeatRequest`/`logResponse`) record latency only on a
transport response arriving (NOT on `onFailure`/transport error), regardless of partition/response error
codes. Rust records in `Ok(Ok(client_response))` arm / `Response` variant only — faithful. Latency captured
BEFORE `take_response_body`.

**Setter-None plumbing (set_metrics_manager / set_offset_commit_metrics_manager, Option<Arc<>>):** does NOT
drop metrics in production — both setters fire on the sole live construction path; heartbeat setter is in the
only arm building a heartbeat RM `(Some(coord),Some(membership))`. `None` is test-only, and Java-parity where
the underlying RM is absent (no group → no RM → no metrics either way).

**ThreadTime::nanoseconds()** default derives from millis (`*1_000_000`); `SystemThreadTime` overrides with
real nanos. Production uses SystemThreadTime, so commit-sync/committed-time-ns-total get real precision;
millis-default only affects mock-time tests (which don't assert ns values). Value-neutral, not per-record.

ClosureMeasurable mirrors ClosureGauge; `now` sourced from time.milliseconds() in KafkaMetric::metric_value
(kafka_metric.rs:93) so MockTime advances observed by last-poll/last-heartbeat gauges.
