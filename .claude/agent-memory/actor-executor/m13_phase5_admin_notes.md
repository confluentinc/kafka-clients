---
name: m13-phase5-admin-notes
description: M13 Phase 5 admin — KAFKA-20673 handle_node_unavailable hook wiring + admin pump-harness periodic-refresh gotcha
metadata:
  type: project
---

Milestone-13 Phase 5 (agent 65, committed 7165f913 on milestone-12-ak-4.3.1).

**KAFKA-20673 fix shape (Java→Rust mapping).** Java added `Call.handleNodeUnavailable(now)`
(base false) called from `maybeDrainPendingCall`'s node==null branch, and
`AdminApiDriver.maybeRetryLookup(now, spec)`. Rust: a `handle_node_unavailable_fn`
hook on `Call` ([[src/admin/internals/call.rs]]); the `Ok(None)` arm of
`maybe_drain_pending_call` ([[admin_client_runnable]]) calls it and DROPS the call
when it returns true (took corrective action) instead of pushing to still_pending;
`AdminApiDriver::maybe_retry_lookup(now, &scope, &keys)` (Java passes the whole
RequestSpec; Rust already destructured it in `new_driver_call`, so pass scope+keys);
the partition-leader override is a closure set via `set_handle_node_unavailable_fn`
in `new_driver_call` (kafka_admin_client.rs). `DriverContext` gained a `log_context`
field to carry the Java debug log.

**Admin pump-harness gotcha (cost me a hang).** The Rust admin test harness
(`env`/`run_once` pump + `prepare_response*`) does NOT run
`AdminClientUnitTestEnv`'s periodic metadata refresh automatically, but the runnable
DOES issue a periodic broker-info metadata refresh whenever
`metadata_fetch_delay_ms == 0`. `provide(ConstantNodeId)` and the stale-leader path
call `metadata_manager.request_update()` → state `UpdateRequested` → delay =
`retry_backoff_ms - (now - last_attempt)`. With default retry.backoff.ms=100 and
now~1000, that is 0, so a broker-info refresh (empty-topics Metadata) fires and
CONSUMES the next FIFO `prepare_response` (future_responses match by NODE only, not
matcher — matcher only asserts). Symptom: the wrong prepared response is eaten and
the call hangs forever (tokio test has no timeout). Fix used: set
`metadata.max.age.ms` AND `retry.backoff.ms` large (300000) in env_with_props so no
periodic refresh fires during the pump, and drive the departed-broker precondition
by calling `admin.shared.metadata_manager.update(shrunk_cluster, now)` directly
(the metadata_manager Arc<Mutex<Inner>> is shared with the runnable). Deviation from
Java (which drops the node via the periodic refresh) documented in the test + PLAN.
Also assert `is_done()` before the final `.get().await` so a regression fails fast
instead of hanging.

**PartitionLeaderStrategyIntegrationTest** → dedicated file
`src/admin/internals/partition_leader_strategy_integration_test.rs`
(`#[cfg(test)] mod` in internals/mod.rs). Key trick: capture `future.all()` (the
per-tp `KafkaFuture` handles) BEFORE `Box::new(future)` moves it into the driver —
they share completion state via Arc, so `is_done()` observes driver-driven
completions (mirrors Java holding the `result` reference). Driver `HashMap` iteration
is nondeterministic, so a `sort_by_key(|s| s.scope.destination_broker_id())` helper
imposes the positional order the Java assertions assume (lookup scopes sort first).

**Doc-only 4.3.1 delta skips:** `Admin.updateFeatures(Map)` convenience overload NOT
added (Rust has no overloading; trait uniformly requires explicit *Options —
admin-client.md §1). `removeRaftVoter` javadoc note N/A (no Rust counterpart).
