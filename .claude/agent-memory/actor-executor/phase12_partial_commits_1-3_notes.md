---
name: phase12_partial_commits_1-3_notes
description: Milestone-8 Phase 12 commits (1/N)-(3/N) — AsyncKafkaConsumer production ctor scaffold + RequestManagers slot Arc refactor + bg-task spawn + components hand-off
metadata:
  type: project
---

Phase 12 commits (1/N), (2/N), (3/N) — landed Phase 12 production-ctor scaffold.

**Why:** Phase 12 closes Milestone-8 by translating the 350-line Java primary `AsyncKafkaConsumer` constructor end-to-end and wiring it through `new_consumer`. Commits (1/N)-(3/N) land the AsyncKafkaConsumer::new(...) ctor itself; (4/N) flips the factory; (5/N)-(6/N) ship integration tests; (7/N) closes out.

**How to apply:** The patterns landed in commits (1/N)-(3/N) are foundational for any future production-ctor work in this milestone. Key takeaways:

## RequestManagers slot Arc refactor (commit 2/N)

The Phase 8b `RequestManagers` shipped with `coordinator: Option<CoordinatorRequestManager>` and `commit: Option<CommitRequestManager>` (owned). Phase 12 production wiring requires both to be shared with the heartbeat / membership managers (Java holds the same reference in both places via heap refs). The Rust translation refactors both slots:

  - `coordinator: Option<Arc<Mutex<CoordinatorRequestManager>>>` — heartbeat manager holds `Arc<Mutex<...>>` clone.
  - `commit: Option<Arc<CommitRequestManager>>` — membership manager holds `Arc<>` clone. `CommitRequestManager` already has `inner: Arc<CommitRequestManagerInner>` so Arc cloning is safe and cheap.

`entries()` SKIPS both slots (same pattern as `consumer_membership`):
  - `coordinator` is polled separately by the bg-task `run_once` via `coordinator_handle().lock().poll(now)`.
  - `commit`'s dyn-trait `poll` returns empty by design (Phase 9: commit work runs via `ApplicationEventProcessor` event arms which call `poll_with_coordinator` directly).
  - `membership.reconcile()` is driven explicitly (Phase 10 pattern).

`membership_boundary()` collapses to `consumer_heartbeat.is_some() as usize` since coordinator + commit are no longer in `entries()`.

Side-effect changes:
  - `CommitRequestManager::update_timer_and_maybe_commit` takes `&self` (was `&mut self`) — all mutation flows through `Arc<Inner>` Mutex.
  - `CommitRequestManager::maybe_auto_commit_async` likewise.
  - Added `CommitRequestManager::signal_close_shared(&self)` — inherent `&self` variant of the trait `signal_close(&mut self)`, callable from Arc holders.
  - Added `CommitRequestManager::share()` — returns a second handle sharing the same Arc<Inner>. Not used in (2/N) but kept for the RM-supplier-style future.

AEP callsites that did `rm_guard.coordinator.as_mut()` / `rm_guard.commit.as_mut()` migrated to `as_ref()` + `lock()` (for coord) or just `as_ref()` + inherent `&self` methods (for commit).

## Dual-notifier wiring (commit 3/N)

`new_with_components` (Phase 11) constructs a `ConsumerStateNotifier` internally and stores it on `self.state_notifier`. The Phase-12 production ctor ALSO needs to register a notifier on the membership manager BEFORE the bg-task spawn so the first heartbeat cycle drives `update_group_metadata`.

Phase 12 commits (2)/(3) ship a **deliberate dual-notifier** design:
  1. The notifier registered on the membership manager owns its own `group_metadata` / `group_assignment_snapshot` Arc slots which are dropped at end-of-block; the membership manager's listener vec holds the only surviving strong reference.
  2. The notifier inside `new_with_components` drives the app-side accessors `consumer.group_metadata()` / `consumer.group_assignment_snapshot()`.

**Consequence:** Production `consumer.group_metadata()` reads return the stub `ConsumerGroupMetadata::new("")` until tests drive the new_with_components-side notifier directly OR a future commit consolidates the two.

The cleanup path: thread the state_notifier through `AsyncKafkaConsumerComponents` and register it on the membership manager inside `new_with_components`. Phase 11 PLAN.md `state_notifier`-related TODO comments call this out for follow-up.

## Bg-task spawn pattern (commit 3/N)

`ConsumerNetworkThread::running_handle()` (new) returns `Arc<AtomicBool>` clone of the running flag. The Phase-12 ctor calls this BEFORE the thread is moved into `tokio::spawn` to capture into the `signal_close_fn` closure. Pattern:

```rust
let signal_close_running = network_thread.running_handle();
let signal_close_wakeup = wakeup_trigger.clone();
let signal_close_fn: Box<dyn Fn() + Send + Sync> = Box::new(move || {
    signal_close_running.store(false, Ordering::Release);
    signal_close_wakeup.wakeup();
});
let join_handle = tokio::spawn(async move {
    let mut thread = network_thread;
    while thread.is_running() { thread.run_once().await; }
    thread.cleanup().await;
});
```

`max_time_to_wait_ms` is currently a separate Arc<AtomicI64> on the components struct, NOT plumbed back from the bg task. The bg task's internal `cached_max_time_to_wait_ms` updates per iteration but is not visible to the app side until a follow-up commit adds the copy-back. Phase 12 documented this gap.

## Open items for commits (4/N) - (7/N)

  - (4/N): Flip `new_consumer()` factory `GroupProtocol::Consumer` arm to call `AsyncKafkaConsumer::new(...)`. Add a unit smoke test against a refuses-connection localhost listener.
  - (5/N)-(6/N): Integration tests against Kafka 4.2.0 broker via testcontainers (PLAINTEXT only).
  - (7/N): Address Phase-11 `Deferred to Phase 12 (integration tests)` markers (5 occurrences in async_kafka_consumer.rs); close out PLAN.md row 12; agent-memory entries.

Carry-overs to follow-up commits within Phase 12:
  - Consolidate the two `ConsumerStateNotifier` instances into one.
  - Wire `max_time_to_wait_ms` copy-back from the bg task.
  - Production bridge for `FetchRequestManager::{is_unavailable, maybe_throw_auth_failure}` (currently no-op closures — delegate filters at send-time; behavior gap is one extra round-trip per disconnected-node fetch attempt, not a correctness bug).
