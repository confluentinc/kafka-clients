---
name: phaseM5-rebalance-metrics
description: Milestone-9 Phase M5 — ConsumerRebalanceMetricsManager + RebalanceCallbackMetricsManager, membership/invoker wiring
metadata:
  type: project
---

Milestone-9 Phase M5 (Actor 45): rebalance + rebalance-callback metrics.

**Classes**: `consumer::internals::consumer_rebalance_metrics_manager::ConsumerRebalanceMetricsManager`
and `consumer::internals::rebalance_callback_metrics_manager::RebalanceCallbackMetricsManager`. Both live
flat under `src/consumer/internals/` (M3/M4 precedent — there is NO `metrics/` subdir despite Java's
`metrics` package). Java's abstract `RebalanceMetricsManager` base was FOLDED into the concrete
ConsumerRebalanceMetricsManager (single concrete impl in scope; Share/Streams out per §20) — documented
as a DoD §7 "don't add a single-impl trait" deviation in module docs + PLAN.

**Recording levels**: every sensor/metric INFO (Java default — no explicit level on either manager, no
DEBUG gating). Matched 1:1.

**Reusable patterns**:
- `last-rebalance-seconds-ago` gauge: `lastRebalanceEndMs` in `Arc<AtomicI64>` (init -1) so the
  ClosureMeasurable gauge reads it on the metric-read path while the record path writes — same swap M4
  used for `last-heartbeat-ms`. `record_occurrence()` (=record(1.0)) for `failedRebalanceSensor.record()`.
- `assigned-partitions` gauge: ClosureMeasurable captures `Arc<Mutex<SubscriptionState>>`, locks briefly
  to read `num_assigned_partitions()` on the metric-read path (low freq, no await).
- `Rate(TimeUnit.HOURS, new WindowedCount(), 1)` → `Rate::with_unit_stat_window(TimeUnit::Hours,
  Arc::new(WindowedCount::new().into_sampled_stat()), 1)`. `TimeUnit` at
  `common::metrics::internals::metrics_utils::TimeUnit`.
- Invoker latency: holds `Option<RebalanceCallbackMetricsManager>` + `time: Arc<dyn Time>`, wired
  post-construction via `set_metrics(manager, time)` (M4 setter precedent; keeps no-arg `new`). Captures
  `start_ms` before the listener `.await`, records `now-start` ONLY on the `Ok(())` arm (Java records
  after the listener returns, skips on exception). §31-safe: only added around existing invoke calls.
- Membership wiring: threaded `Option<Arc<ConsumerRebalanceMetricsManager>>` + `time: Arc<dyn Time>`
  through `ConsumerMembershipManager::new` → `AbstractMembershipManager::new` onto `MembershipInner`.
  `transition_to` records start/end across RECONCILING boundary via `is_completing_rebalance`/
  `is_starting_rebalance` (Java statics). `on_heartbeat_failure` now honors the `retriable` arg
  (was ignored) → `maybe_record_rebalance_failed()` when `!retriable`. Recording while holding the
  MembershipInner guard is fine (atomics + sensor record, no await, no SubscriptionState lock).

**Gotchas hit**:
- `rebalance_started()` (Java `rebalanceStarted()`) is test-only in Java too (membership manager never
  calls it) → `#[allow(dead_code)]` with a note, NOT `#[cfg(test)]` (preserve the Java API surface).
- Clippy `collapsible_if` → used `if !retriable && let Some(..) = ..` let-chain (compiles on this rustc).
- All 8 `ConsumerMembershipManager::new` + 7 `AbstractMembershipManager::new` test call sites needed
  `, None, Arc::new(crate::common::metrics::time::SystemTime)` appended.
- Membership unit tests use a ThreadTime MockTime; metric tests use the `common::metrics::time::mock::MockTime`
  (`#[cfg(test)] pub(crate)`) — different types, both impl their own `Time`.

**Tests**: ConsumerRebalanceMetricsManagerTest (8) + RebalanceCallbackMetricsManagerTest (1) translated
inline; plus invoker records-on-success / no-record-on-error (Avg=NaN at zero count), two membership
wiring tests (transition_to + on_heartbeat_failure drive the records), and a co-registration test (both
managers on one Arc<Metrics> don't conflict). +13 new tests; full lib suite 2099 green.

**Skips/deviations documented**: obsolete membership-test skip-note #5 ("metrics deferred") updated to
point at the new dedicated translation + wiring tests.
