---
name: phase41-consumer-handle-notes
description: Phase 41 — ConsumerHandle (cross-task wakeup + in-callback reentrancy) + non-blocking reconcile callback state machine; patterns for splitting a linear async fn into a cross-iteration resume machine
metadata:
  type: project
---

Phase 41 (Actor 41) closed the two coupled gaps from `consumer_remaining_work`
(cross-task wakeup + in-callback rebalance-listener reentrancy). Commits
644f049 (41a), 0054fa9 (41b), b9b1ee4 (41c), d14ad1a (perf fixup).
Links: [[phase40_critic_round1_patterns]] (the WakeupHandle it replaces),
[[phase34_membership_reconcile_notes]] (reconcile flow).

## 41a — ConsumerHandle replaces WakeupHandle

- One `Clone+Send+Sync` `ConsumerHandle` (`src/consumer/async_kafka_consumer.rs`)
  obtained via `Consumer::handle()` (renamed from `wakeup_handle()`). Recovers
  Java's freely-shareable `Consumer` ref: cross-task `wakeup()` AND in-callback
  reentrancy. User captures it into their listener struct = Java capturing the
  `consumer` var. Listener trait stays byte-for-byte Java (only `&self`+parts).
- `ConsumerHandleInner::Async(AsyncConsumerHandleState)` holds only Arc-shared
  state (wakeup_trigger, bg_wakeup poke, application_event_handler,
  subscriptions, fetch_buffer, time, default_api_timeout_ms). Mock variant =
  `Arc<AtomicBool>` flag only (async ops return unsupported_version; mock tests
  drive the concrete MockConsumer).
- Handle async ops (assign/seek/pause/resume/position/committed/begin/end/
  offsets_for_times/commit_sync/commit_async) submit an ApplicationEvent and
  await via a single shared **no-drain** core `await_completion` (handle can't
  own background_event_rx / the invoker, so it does NOT drain — the bg task
  services the event; safe because a single callback never nests a rebalance).
  `submit_and_await` = `add` + `await_completion`. Wakeup-aware select mirrors
  process_background_events_until's enable_wakeup arm. NO #[async_trait], NO
  Pin<Box<dyn Future>> (concrete struct, concrete async fn).
- commit_sync/async on the handle do NOT run interceptor onCommit or the
  callback invoker (handle doesn't own them — stays on &mut self); commit_async
  is no-callback only. Documented deviation.

## 41b — non-blocking reconcile callback (the hard part)

Blocker (b): bg `run_once` → `reconcile().await` → `invoke_rebalance_callback`
`ack_rx.await` froze the WHOLE loop during the callback, deadlocking any
reentrant handle op (the op routes through the bg task; the listener runs on
the app task whose poll() can't return until the listener does).

Pattern: split the linear `reconcile` async fn into a **cross-iteration resume
machine** (translates Java's `revokeAndAssign(...).whenComplete(...)`):
- `AbstractMembershipManager::enqueue_rebalance_callback` = non-blocking
  sibling of `invoke_rebalance_callback`: enqueues the §31 event, returns the
  ack `oneshot::Receiver` (None if no listener) WITHOUT awaiting.
- `PendingReconcile` enum (AfterRevoke / AfterAssign) stores the ack_rx + all
  data needed to resume (resolved, resolved_assignment,
  assigned_topic_partitions, assigned_set, added, current_time_ms) on
  `ConsumerMembershipManager`.
- `reconcile` entry: if `has_pending_reconcile()` → `drive_pending_reconcile`
  (try_recv; Empty → re-store + return; Ok → continuation; Closed → err+complete).
  Else run steps 1-9; at the revoke callback, `enqueue_rebalance_callback` →
  Some → store AfterRevoke + return; None → `continue_after_revoke` inline.
  `continue_after_revoke` (steps 10-13) → store AfterAssign or
  `continue_after_assign` (steps 14-16, sync).
- Lock discipline: take the pending OUT of the mutex before any resumed
  `.await`; never hold the guard across await.
- Release paths (transition_to_fenced/fatal/stale/signal_member_leaving_group)
  KEEP the blocking `invoke_rebalance_callback` — scoped out per the PLAN's
  reconcile-only resume-step list + onPartitionsLost is never reentered in the
  Java callback suite. Documented in COMMENTS.DONE.41.md.
- App-side poke: after `ack.send(...)` in `process_background_events`, call
  `self.network_thread_close.wakeup()` (reuse existing Notify) so the bg loop
  wakes to try_recv the ack — do NOT shrink poll_wait_time_ms (busy-spin).
- §31 in consumer-threading.md UPDATED (loop keeps spinning; only the
  membership STATE TRANSITION is gated on the ack). CLAUDE-rule change flagged
  in COMMENTS.DONE.41.md per agent-roles.md (mandated by the PLAN).

## Perf Contract gotcha (fixup d14ad1a)

`Mutex<Option<PendingReconcile>>` alone means a per-iteration LOCK on the
steady-state reconcile entry → violates "no new lock on steady-state path".
Fix: add a lock-free `AtomicBool pending_reconcile_flag` mirror;
`has_pending_reconcile` = single Acquire load, only touches the mutex when a
callback is actually pending. Set/cleared in lockstep with the mutex (set in
store_pending, cleared at take in drive_pending_reconcile).

## Test-harness impact

The non-blocking reconcile no longer drives to completion in one `await`. The
existing spawn-reconcile/recv-event/ack/await component tests were updated to
spawn a `#[cfg(test)] reconcile_drive_to_completion(can_commit)` helper
(repeated reconcile + `tokio::task::yield_now`, bounded 10k iters) that
reproduces the bg loop — ASSERTIONS unchanged, only the driver call changed.
No-listener direct-call tests (`make_without_listener`) still complete inline
(no pending stored) — unchanged.

New regressions: `reconcile_does_not_advance_until_ack_and_loop_is_not_frozen`
(membership: repeated reconcile returns promptly while ack pending, state stays
RECONCILING, advances after ack) and
`handle_reentrant_op_completes_through_bg_pipeline` (handle.pause via fake bg).

## 41c integration tests

`tests/integration/plaintext_consumer_callback_test.rs`: 8 #[ignore]d stubs →
real tests. `CallbackAction` enum + `ReentrantListener`/`RevokeTrackingListener`
named structs capture a `ConsumerHandle`, run the op on the matching callback,
record the outcome via `Arc<Mutex<Option<_>>>`. `trigger_on_partitions_assigned`
/ `_revoked` mirror Java's helpers. Docker-gated (compile + clippy verified
locally; xtask lint does NOT cover integration-tests cfg — use
`cargo clippy --features integration-tests --test integration`, confirm zero
warnings in YOUR files; sibling-file warnings pre-existing).

## Critic round-1 fixes (Actor 41 fix pass) — Issues 1-4

The release paths were NOT actually blocking the way I first assumed. Key
realizations from the fix pass:

- **`leave_group`/unsubscribe/close `on_partitions_lost` was ALREADY
  non-blocking** — it runs on a `tokio::spawn`ed continuation in
  `process_unsubscribe` / `process_leave_group_on_close` (event processor),
  NOT inline in the bg loop. Only fence/fatal/stale run inline in `run_once`
  Phase 2.4 and froze the loop. Audit the CALL SITE (spawned vs inline) before
  assuming a `.await` freezes the bg loop.

- **Issue 1 (clear pending reconcile on release):** `transition_to_{fenced,
  fatal,stale}` now call `clear_pending_reconcile()` at the top — drops the
  stored `PendingReconcile` + ack receiver (abandons Java's in-flight
  `revokeAndAssign` future) AND calls `mark_reconciliation_completed()` when it
  dropped one (Java's whenComplete→maybeAbortReconciliation clears
  reconciliationInProgress). `clear_pending_reconcile` takes the mutex
  UNCONDITIONALLY (no flag fast-path) because a lock-free flag read races the
  `drive_pending_reconcile` take/store-back window (flag transiently false while
  driver owns the value) — only matters in multi-task component tests; bg loop
  runs reconcile + transitions on the SAME task so they never overlap.

- **Issue 2 (extend non-blocking to release):** mirror the reconcile machine
  with a SEPARATE `pending_release: Mutex<Option<PendingRelease>>` +
  `pending_release_flag: AtomicBool`. `PendingRelease::{Fenced,Fatal,Stale}`
  holds only the ack_rx; a `ReleaseKind` discriminator decouples the tail from
  the owned receiver so try_recv/store-back stays uniform. `transition_to_*`
  enqueue via `enqueue_release_callback` (None ⇒ no owned parts / no listener ⇒
  run tail inline) + `store_pending_release` + return. `drive_pending_release`
  (bg Phase 2.4, BEFORE new-transition processing) runs the tail
  (`continue_after_{fenced,fatal,stale}_release`). Keep reconcile + release
  pending machines SEPARATE (reconcile has revoke→assign sequencing; release is
  a single callback) — do not unify.

- **Phase 2.4 ordering:** drive pending release first; gate new-transition
  draining on `!release_pending` so queued heartbeat classifications stay on the
  mpsc channel (NOT lost) until the release resolves. Single cached atomic load
  in steady state (Perf Contract item 1).

- **Test-harness:** added `#[cfg(test)] drive_release_to_completion()` (sibling
  of reconcile_drive_to_completion) + `reconcile_once` (single non-looping
  reconcile to park at a callback) + `has_pending_reconcile_for_test`. The
  delayed-discard tests were rewritten to the eager-discard flow (no concurrent
  bg loop — single reconcile_once to park, then transition_to_* clears eagerly,
  `let _stuck_ack` since sending to the dropped receiver would Err-on-unwrap).

- **Issue 3:** `ConsumerTestHandles.bg_wakeup_called: Arc<AtomicBool>` (fixture
  bg-wakeup fn sets it) → `process_background_events_ack_pokes_bg_wakeup` proves
  the ack-send poke fires locally.

- **Issue 4:** `ConsumerHandle::assign([])` now REJECTS with IllegalArgument
  pointing at the owning consumer's `unsubscribe()` (handle can't leave the
  group; empty-AssignmentChange would silently diverge from Java's
  assign([])==unsubscribe()).
