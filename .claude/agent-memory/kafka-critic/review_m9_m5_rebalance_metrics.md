---
name: review-m9-m5-rebalance-metrics
description: M9 Phase M5 rebalance + callback metrics review — CLEAN; latency-capture verification heuristics, abstract-base fold, retriable latent-bug fix
metadata:
  type: project
---

M9 Phase M5 (ConsumerRebalanceMetricsManager + RebalanceCallbackMetricsManager): reviewed CLEAN, 0 issues.

**Latency-actually-captured check (the key value-parity worry):** a metric that
records `now - start` is worthless if start/end are stamped at the wrong
transition or the wiring test sleeps 0ms. The convincing test drives the real
state machine (transition_to STABLE→RECONCILING, MockTime.sleep(25),
RECONCILING→STABLE) and asserts latency==25.0 — not a helper-direct
record_rebalance_ended call. Demand a state-machine-driven non-zero-elapsed test,
not just "record() then read metric".

**transitionTo bracket parity (AbstractMembershipManager.transitionTo:239-256):**
END recorded BEFORE START in the same call; predicates are
`is_completing = current==RECONCILING && next∈{STABLE,ACKNOWLEDGING}` and
`is_starting = current!=RECONCILING && next==RECONCILING`. Both mutually
exclusive here but order still matters — verify END-before-START.

**rebalanceStarted() #[allow(dead_code)] is fine:** Java's rebalanceStarted() is
also test-only (never called from the state machine). dead_code on it is NOT a
sign the start path is unwired — start is wired via record_rebalance_started
inside transition_to, independent of the predicate. Confirm by grepping the
production caller of record_rebalance_started, not of rebalance_started().

**Retriable latent-bug pattern:** old Rust `on_heartbeat_failure(_retriable)`
ignored the arg → maybe_record_rebalance_failed NEVER fired → failed-rebalance
metric dead. Java gates `if (!retriable)`. When auditing an M-phase "latent bug
fix" claim: confirm (a) the old signature truly dropped the value (underscore
param), (b) both production call sites pass the right value — here :676 hardcoded
false (onErrorResponse), :718 computed retriable (onFailure).

**Abstract-base fold (RebalanceMetricsManager → concrete):** sound per DoD §7
single-impl-trait avoidance. Verify the concrete impl carries ALL base members
(metricGroupName, createMetric, record-* surface, default-no-op
maybeRecordRebalanceFailed). Only Share/Streams (§20 out of scope) consumed the
polymorphism.

**§31 callback-latency additive wiring:** start_ms captured AFTER paused-partition
bookkeeping, immediately before the listener .await (matches Java startMs
placement); subs lock dropped before await; record only on Ok (Java skips on
exception, Avg→NaN at zero count). The metric wiring must NOT touch the
invocation-thread/oneshot handshake — it's purely additive around the existing
inline await.

**Recording level:** these use plain `metrics.sensor()` / `metric_name()` (→ INFO),
NOT the SensorBuilder DEBUG-default trap from M3/M4. Java default also INFO. OK.

**Live clock seam (non-issue, noted):** live Metrics registry uses default
SystemTime; rebalance mgr + invoker each get a fresh SystemTime; Java threads one
`time` into both. Stateless wall-clock → consistent in prod. Only diverges under
injected non-system clock, which no live path does. Latent design seam, not a
defect — flag-as-observation, not as issue.

**Gating parity:** ConsumerRebalanceMetricsManager built only in group path
(membership manager gated on group_id+commit) = Java RequestManagers groupId!=null.
RebalanceCallbackMetricsManager built UNCONDITIONALLY (Java AsyncKafkaConsumer
ctor). Don't flag the callback mgr's unconditional build as a gating miss.
