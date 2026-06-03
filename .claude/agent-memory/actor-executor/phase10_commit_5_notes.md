---
name: phase10-commit-5-notes
description: Milestone-8 Phase 10 commit 5/N — ApplicationEventProcessor async-dispatch arms (AsyncPoll, Commit*, FetchCommittedOffsets, ListOffsets, CheckAndUpdatePositions, TopicMetadata, AllTopicsMetadata, Unsubscribe, CreateFetchRequests, LeaveGroupOnClose) + ConsumerRebalanceListenerCallbackCompleted Rust §31 deviation rationale + `commit_async_no_callback` extraction pattern
metadata:
  type: project
---

# Phase 10 (5/N): ApplicationEventProcessor — async-dispatch arms

## What landed

Wired all 11 async-dispatch arms in
`src/consumer/internals/events/application_event_processor.rs`. Each
arm follows the same pattern:

  1. Lock `RequestManagers` briefly to obtain a `oneshot::Receiver` from
     the relevant manager (`commit_request_manager`,
     `offsets_request_manager`, `topic_metadata_request_manager`,
     `fetch_request_manager`) OR an `Arc<ConsumerMembershipManager>` for
     unsubscribe/leave-on-close.
  2. Drop the lock.
  3. `tokio::spawn` a continuation task that `.await`s the receiver and
     calls `handle.complete(...)` or `handle.complete_exceptionally(...)`.

Mirrors Java's `whenComplete(complete(event.future()))` chain.

## Patterns worth recording

### 1. Bare-commit extraction: `commit_async_no_callback`

Java's `CommitRequestManager.commitAsync(Map)` returns
`CompletableFuture<Map<TopicPartition, OffsetAndMetadata>>` — no
callback/invoker plumbing. The Rust `commit_async<K, V>` previously
folded the callback enqueueing INTO the manager (out of mirroring
convenience). The AEP cannot use that signature: the processor is not
generic over `K, V` and has no `OffsetCommitCallbackInvoker<K, V>` to
hand in.

Fix: extract a non-generic `commit_async_no_callback(offsets, now_ms)`
that does the bare commit work. The existing `commit_async<K, V>(...)`
becomes a wrapper that calls `commit_async_no_callback` and layers
callback / interceptor enqueueing on top via a continuation task.

Java parity is improved: Java's bare `commitAsync(Map)` lives on the
manager, the callback wiring lives in `AsyncKafkaConsumer.commitAsync`.
Rust now mirrors that split: bare in the manager, wrapper in
`AsyncKafkaConsumer` (still to land in Phase 11) — and the bg-side AEP
calls the bare variant.

### 2. `ConsumerRebalanceListenerCallbackCompleted` is a no-op arm in Rust

§31's bidirectional handshake in Java uses TWO events: one bg→app
(`Needed`) carrying no completion sender, and one app→bg (`Completed`)
that the AEP routes to `membershipManager.consumerRebalanceListenerCallbackCompleted`,
which completes a future the reconcile loop awaits.

Rust collapses to ONE event by embedding a
`tokio::sync::oneshot::Sender<Result<(), KafkaError>>` directly in
`BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`. The
membership manager `await`s the embedded sender's receiver directly;
the app side completes the ack by sending on the embedded sender.
There is no separate completed-event round-trip.

As a result, the AEP's `ConsumerRebalanceListenerCallbackCompleted`
arm has NO real work in Rust. The arm and event variant are PRESERVED
for Java parity (and for the diagnostic log: if the event arrives with
no heartbeat manager, log warn). Body comment cites the deviation;
do not delete the variant — it's part of the API parity surface.

The previously-planned "pending-callback-future map keyed by id" is
unnecessary because the oneshot's identity IS the map.

### 3. `Unsubscribe` no-group-id sub-path is a sync write

Java's `process(UnsubscribeEvent)` has a no-group-id branch that just
calls `subscriptions.unsubscribe(); event.future().complete(null);`
inline (no manager future to chain on). The Rust translation matches
this: when `consumer_heartbeat.is_none()`, the AEP synchronously locks
`SubscriptionState`, calls `unsubscribe()`, drops the lock, and calls
`handle.complete(())`. No spawn.

This sub-path was deferred from commit 4 explicitly because the
with-group-id branch shares the same arm and didn't land until this
commit. Both sub-paths land together for cohesion.

### 4. `LeaveGroupOnClose` with no membership manager: fail handle (deviation)

Java's `process(LeaveGroupOnCloseEvent)` ignores the event when both
`consumerMembershipManager` and `streamsMembershipManager` are absent
(it logs nothing, returns silently, and the future stays unresolved
forever — the reaper eventually fails it on the deadline).

Rust deviates: we `complete_exceptionally` the handle immediately with
`illegal_state("ConsumerMembershipManager not available ...")`. Justified
by DoD §5 ("no silent drops") and consumer-threading.md §28.
Documented in the arm body and tested.

### 5. AsyncPoll: `maybe_update_pattern_subscription` moved BEFORE spawn

Java's `process(AsyncPollEvent)` calls `maybeUpdatePatternSubscription`
INSIDE step 2 (after `maybeReconcile`, inside the
`commitRequestManager.isPresent()` block). The Rust translation moves
that call BEFORE the spawn because:

  1. `maybe_update_pattern_subscription` mutates
     `self.metadata_version_snapshot` (an inherent field of the
     processor, not Send-shared with the spawned task).
  2. Java's reconcile→pattern-update ordering is a side-channel
     ordering — the pattern update notifies `onSubscriptionUpdated`,
     which the *next* reconcile observes, not this one. Running the
     pattern update first means *this* reconcile sees the new pattern;
     `onSubscriptionUpdated` still fires before the next heartbeat —
     semantically equivalent for the steady-state consumer.

Phase-7 commit-7 will eventually move this back into the bg-task
loop alongside the rest of the membership state machine.

### 6. AsyncPoll's error-mapping helper: `is_ignorable_async_poll_error`

Java's `maybeCompleteAsyncPollEventExceptionally(event, t)` returns
`true` (signal "ignore") for any `TimeoutException`. Other errors fail
the event. The Rust translation lifts that to a free function:

```rust
fn is_ignorable_async_poll_error(err: &KafkaError) -> bool {
    matches!(err, KafkaError::Timeout(_))
}
```

Used by both the reconcile step (Java doesn't do this; Rust adds it
because reconcile-side timeouts during shutdown shouldn't fail the
poll) and the update-positions / create-fetch-requests chain.

### 7. `process_async_poll` does NOT spawn continuations between manager calls

A more naive translation would be: spawn one task per Java
`whenComplete` boundary (reconcile → step2 → updatePositions →
createFetchRequests). That ends up with three nested spawns. The
implementation here uses ONE spawn that awaits each manager future
sequentially in an `async move` block. Equivalent semantics, less task
overhead.

### 8. `current_time_ms_now` free function

The AEP needs a wall-clock millisecond timestamp for `commit_sync` and
`commit_async_no_callback`. Java has `Time.milliseconds()` (mock-
friendly). Rust uses `SystemTime::now().duration_since(UNIX_EPOCH)`
directly. Lives at module scope as a free `fn` rather than on the
processor — mirrors the `current_time_ms_for_followup` precedent in
`OffsetsRequestManager` so future test-time injection can be done in
one place.

## Tests

11 new async-arm smoke tests inside the AEP `tests` module:

  - `check_and_update_positions_resolves_when_no_partitions_pending`
  - `commit_async_without_commit_manager_fails_with_illegal_state`
  - `commit_async_empty_consumed_offsets_completes_handle_ok`
  - `commit_sync_without_commit_manager_fails_with_illegal_state`
  - `fetch_committed_offsets_without_commit_manager_fails_with_illegal_state`
  - `fetch_committed_offsets_empty_partitions_resolves_immediately`
  - `list_offsets_empty_timestamps_resolves_immediately`
  - `create_fetch_requests_without_fetch_manager_fails_with_illegal_state`
  - `unsubscribe_without_group_id_clears_subscription_inline`
  - `leave_group_on_close_without_heartbeat_fails_handle`
  - `async_poll_without_offsets_manager_fails_state`

Plus 1 noop verification:
  - `rebalance_listener_callback_completed_is_noop`

Each test targets the **arm-level wiring**: that the processor spawns
a continuation, awaits the manager future, and completes the event's
handle. The exhaustive Java-parity translation is owned by commit 6/N.

## Tests must use `#[tokio::test]`

Phase 10 commit 4 tests used sync `#[test]` because the AEP was sync.
Commit 5 spawns continuations, so any test exercising an async-arm
must use `#[tokio::test(flavor = "current_thread")]` — otherwise the
spawn panics with "there is no reactor running, must be called from
the context of a Tokio 1.x runtime".

When writing async-arm tests, also use `tokio::time::timeout` around
the receiver `.await` to defend against hangs — a regression that
swallows a handle would otherwise deadlock the test suite.

## Deferrals (still owned by later commits)

  - **Commit 6/N (PLAN.md)**: exhaustive translation of
    `ApplicationEventProcessorTest.java` cases. This commit covers
    arm-level wiring, not Java-for-Java case parity.
  - **Commit 7/N (PLAN.md)**: bg-task `run_once` loop. The processor's
    spawn-continuation pattern is fine as a temporary mechanism;
    commit 7 will move the dispatch under the bg-task `select!` loop
    so cancellation / wakeup work as designed (§11).
  - **Phase 11**: `AsyncKafkaConsumer.commit_async` callback-wrapper
    (lifting `commit_async_no_callback` + `OffsetCommitCallbackInvoker`
    enqueueing from the manager up to the consumer API). Currently
    the wrapper still lives on the manager as
    `CommitRequestManager::commit_async<K, V>`; Phase 11 will move
    it to where Java keeps it.
