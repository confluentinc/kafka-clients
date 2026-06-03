---
name: phase10-consolidated-patterns
description: Milestone-8 Phase 10 — consolidated cross-cutting patterns from commits 1-8 (bg-task driver + ApplicationEventProcessor + 9 wire-prereqs)
metadata:
  type: project
---

# Phase 10 — Cross-cutting patterns (durable summary)

Phase 10 landed the consumer bg-task driver (`ConsumerNetworkThread`) and
the `EventProcessor<ApplicationEvent>` impl. Below are the patterns that
recur across the 12 commits and are worth promoting from per-commit
notes for future phase work. Per-commit detail lives in
`phase10_commit_{2,3a,3b,3c,3d,4,5,6,7,8}_notes.md`.

## 1. Pending-followup mpsc channel — defer `&mut self` from spawned task

Java request-manager `whenComplete` callbacks mutate manager state
(e.g. enqueue `requestsToSend`) from inside the response handler. Rust
spawned futures can't borrow `&mut self`. Pattern:

  - Manager holds `pending_followups_tx: mpsc::UnboundedSender<PendingFollowup>`.
  - Spawned response callback sends an enum-typed message describing
    the deferred work.
  - Manager's `poll(&mut self, now_ms)` drains the channel at the top
    and executes each followup.

Trade-off vs. Java: the deferred `&mut self` work lands one `poll()`
tick later. Java has the same property (its handler runs on the next
`runOnce` iteration anyway).

Used in `OffsetsRequestManager` (commit 3b),
`CommitRequestManager::pending_completion_tx` (Phase 7d carry-over).

## 2. `PollResult::try_connect: Vec<Node>` — connection hint from manager

Java request managers call `networkClientDelegate.tryConnect(node)`
synchronously inside their `poll(...)`. The Rust bg task owns the
delegate, so:

  - Add `pub try_connect: Vec<Node>` to `PollResult` (default empty).
  - Manager owns `Mutex<Vec<Node>>` queued during `&mut self` work,
    drained at `poll(now)` time into the returned `PollResult.try_connect`.
  - Bg task drains `try_connect` BEFORE `add_all_from_poll_result`,
    calling `delegate.try_connect(node, now).await` per entry.

`add_all_from_poll_result` IGNORES `try_connect` — strict separation.
See `consumer_network_thread.rs` Phase 2 ordering and
`offsets_request_manager.rs` `validate_positions_if_needed`.

## 3. `Arc<Mutex<RequestManagers>>` discipline (extends §16)

Java holds `RequestManagers` as a non-shared field on the bg thread.
Rust needs `&mut` access from both `ApplicationEventProcessor::process`
and `ConsumerNetworkThread::run_once`. Wrap in
`Arc<std::sync::Mutex<RequestManagers>>`.

Discipline:

  - Held briefly on the bg task only (single-threaded acquisition);
    contention is nil.
  - NEVER `.await` while holding the guard. Pattern:
    `let value = { let g = rm.lock()...; g.something() }; spawn(...
    .await ...)`.
  - Don't nest `RequestManagers` → `SubscriptionState` unless the inner
    manager is known not to lock back into `RequestManagers`.

The `network_client_delegate` is a `tokio::sync::Mutex` (async) and
CAN be held across `.await` by design — it's the runtime-async lock,
owned solely by the bg task.

## 4. `ConsumerNetworkThread::run_once` phase ordering (Java parity)

Mirror `ConsumerNetworkThread.runOnce()` phase-for-phase. Inserts one
Rust-specific phase:

| Phase | Java line | Rust |
| --- | --- | --- |
| 1: drain app events | 212 | `process_application_events` (try_recv loop) |
| 2: per-RM poll → addAll | 222-226 | rm.poll under sync guard → drop → delegate.try_connect + add_all under async guard |
| 3 (**Rust-only**): membership.reconcile | implicit via `entries()` | `membership.reconcile(now).await` between Phase 2 and Phase 4 |
| 4: network poll | 228 | `select! { biased; token.cancelled; delegate.poll }` — only wakeup-cancel-safe await |
| 5: max time-to-wait | 230-237 | fold to `AtomicI64`, app reads with `Ordering::Acquire` |
| 6: reaper.reap | 239 | `reaper.lock().reap(now)` |
| 7: maybeFailOnMetadataError(uncompletedEvents) | 240-241 | `maybe_fail_on_metadata_error_uncompleted` over the side-list `notifiable_handles` |

Phase 3 is load-bearing because `entries()` skips membership (Phase 8b
Arc shape); the side-effect of Java's `AbstractMembershipManager.poll(...)
{ maybeReconcile(false); return EMPTY; }` must be re-supplied at the
same phase point.

## 5. §31 callback handshake collapses to one event with embedded oneshot

Java's bidirectional rebalance-listener handshake uses TWO events
(bg→app `Needed`, app→bg `Completed`) plus a pending-future map on the
membership manager keyed by event id.

Rust collapses to ONE event:
`BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded` carries an
embedded `tokio::sync::oneshot::Sender<Result<(), KafkaError>>`. The
membership manager awaits the matching receiver directly; the app side
completes the ack by sending on the embedded sender. The oneshot's
identity IS the map.

Consequence: the AEP arm for `ConsumerRebalanceListenerCallbackCompleted`
is a no-op in Rust (kept for parity + diagnostic logging when no
heartbeat manager exists). Do not delete the variant.

## 6. `#[cfg(test)] pub(crate) complete_first_unsent_*_for_test`

Java tests stub manager methods via Mockito; Rust uses real managers
end-to-end. To drive a manager future from a sibling-module test:

  - Add `complete_first_unsent_{commit,fetch,offset_fetch}_for_test(&self,
    payload)` and `fail_first_unsent_*_for_test(&self, err)` helpers on
    the manager.
  - Helper: take state lock, pop the first queued unsent request, drop
    the guard, complete its sender.
  - Test uses `yield_until(predicate, timeout).await` to wait for the
    spawned continuation to register the unsent request before calling
    the helper.

Avoids leaking test-only state into public API while keeping the
sibling-module test surface real. Drop-the-lock-before-await
discipline preserved.

## 7. `SpyRequestManager` + `CountingClient` + `with_dyn_managers`
test infra

Java uses Mockito `verify(rm).poll(...)` for per-call assertions.
Rust observes side effects:

  - `SpyRequestManager` (test-module): records `poll` /
    `maximum_time_to_wait` / `poll_on_close` call counts in shared
    `AtomicUsize`. Returns scripted `PollResult`.
  - `CountingClient`: `KafkaClient` wrapper around `MockClient` that
    records `poll` call count, per-call timeout argument, scripted
    `has_in_flight_requests()` returns.
  - `RequestManagers::with_dyn_managers(Vec<Box<dyn RequestManager>>)`
    (test-only ctor) appends auxiliary managers to `entries()` after
    concrete slots — production path leaves the vec empty.
  - `NetworkClientDelegate::client_for_test_ref(&self) -> &K` —
    `#[cfg(test)]` accessor to read the wrapped client's counters out
    of the delegate.

## 8. `metadata_error_notifiable_handle()` side-list — Rust-reaper limit

Java's `applicationEventReaper.uncompletedEvents()` returns
`List<CompletableEvent<?>>`; Java filters for `MetadataErrorNotifiableEvent`
via `instanceof`. The Rust reaper holds erased handles only — cannot
match against the original variant after the fact.

Workaround: maintain a side-list `notifiable_handles:
Vec<Arc<dyn ErasedHandle>>` populated during
`process_application_events` ONLY for variants where
`is_metadata_error_notifiable() && erased_handle().is_some()`. Prune
done entries via `retain(!is_done)` at the post-poll arm.

`ApplicationEvent::metadata_error_notifiable_handle()` is the accessor:
returns `Some(erased_handle)` for the intersection of notifiable AND
completable variants. `AsyncPoll` is notifiable but not completable
(does not extend `CompletableApplicationEvent`), so it appears in
per-event arms but NOT in the post-poll side-list.

## 9. Lock-acquisition matters for SubscriptionState callers

Java's `synchronized` is reentrant. Rust `std::sync::Mutex` is NOT.
When a listener (e.g. `ClusterResourceListener::on_update`) fires
inside metadata's lock, calling `metadata.current_leader(tp)` from the
listener deadlocks.

Solution: listener sets `AtomicBool metadata_updated` flag and returns
immediately. The actual replay work happens on the NEXT `poll(...)`
call (which holds no metadata locks). See
`OffsetsManagerShared::metadata_updated` (commit 3c).

## 10. Test-only flag-poll accessors must drain side-effects

When a test-only accessor reads state that's used to drive deferred
behavior (e.g. `requests_to_send_count` drains the `metadata_updated`
flag), assertions immediately after `metadata.update(...)` need the
flag side-effect to fire. The accessor swaps the flag itself so
tests see the post-update state without an extra `poll()`.

Pattern: any `#[cfg(test)]` accessor that observes a value affected by
deferred-replay drains the trigger flag.

## Files closing Phase 10

Production (12 files):
- `src/consumer/internals/consumer_network_thread.rs` (new)
- `src/consumer/internals/events/application_event_processor.rs` (new)
- `src/consumer/internals/events/application_event.rs` (extended)
- `src/consumer/internals/request_managers.rs` (offsets + fetch slots)
- `src/consumer/internals/network_client_delegate.rs` (try_connect,
  PollResult.try_connect)
- `src/consumer/internals/offsets_request_manager.rs` (relocate +
  update_fetch_positions + fetch_offsets + try_connect)
- `src/consumer/internals/commit_request_manager.rs` (auto-commit hook,
  drain fix, retry counter, MemberStateListener,
  maybe_auto_commit_sync_before_rebalance)
- `src/consumer/internals/coordinator_request_manager.rs`
  (is_closing accessor)
- `src/consumer/internals/consumer_membership_manager.rs` (Phase-11
  TODO marker for auto-commit-before-rebalance invocation)
- `src/consumer/internals/request_state.rs` (minor)
- `src/consumer/internals/events/mod.rs` (module registration)
- `src/consumer/internals/mod.rs` (module registration)

## What Phase 11 inherits

  - `AsyncKafkaConsumer` glue: spawn the bg task, app-side `wakeup()`
    + token rotation, `process_background_events` per §31,
    `consumer.maximum_time_to_wait()` exposure, `consumer.close(timeout)`.
  - `consumer_membership_manager` auto-commit-before-rebalance
    invocation (TODO marker at line 538 of
    `consumer_membership_manager.rs`).
  - `reset_poll_timer` → `maybe_rejoin_stale_member` invocation in
    `AsyncKafkaConsumer::poll()` epilogue (Phase 8b note).
  - `consumer_membership_manager.leave_group()` /
    `leave_group_on_close()` invocation from `AsyncKafkaConsumer::close()`
    (Phase 8b note).
  - `AsyncConsumerMetrics` translation (3 metric-bearing tests
    deferred across Milestone-8).
