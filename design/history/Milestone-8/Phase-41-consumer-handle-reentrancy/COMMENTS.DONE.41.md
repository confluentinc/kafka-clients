# COMMENTS.DONE.41 — Actor 41 notes

This file records Actor-side decisions for Phase 41 that the Critic and
the agent-roles.md change process should be aware of. (No reviewer comments
have been filed yet; this is the Actor's own record per the PLAN's
instruction to flag the CLAUDE-rule change here.)

## CLAUDE-rule change: `consumer-threading.md` §31 (agent-roles.md process)

Phase 41b changes the §31 rebalance-listener invocation contract: the
background loop **no longer blocks on the callback ack** (`ack_rx.await`
inline). It now stores the ack `oneshot::Receiver` as cross-iteration state
on the membership manager and `try_recv`s it each `reconcile` iteration,
gating only the membership *state transition* on the ack — the loop keeps
spinning (heartbeats, fetches, and the reentrant application events the
listener submits all continue).

Per `agent-roles.md`, changing a CLAUDE rule is normally proposed, not made
unilaterally by the Actor. This change was **mandated by the Phase 41
PLAN.md** (locked, user-approved 2026-06-24), which explicitly instructs
(§41b): "Update `consumer-threading.md` §31 … replace 'The bg task
**awaits** the matching `oneshot::Receiver`. Rebalance state does not
advance until the callback completes.' with the non-blocking-loop
description … This CLAUDE-rule change requires the agent-roles.md process —
call it out in the COMMENTS.DONE entry; do not change CLAUDE.md itself."

Accordingly:

- `consumer-threading.md` §31 was edited (step 2, the "How to apply"
  bullet, and the anti-patterns list — including the new busy-spin
  anti-pattern). `CLAUDE.md` itself was **not** touched.
- The change is faithful to the Java source (`ConsumerNetworkThread.runOnce()`
  fires the callback, marks reconciliation in progress, and returns; the
  reconcile is a `revokeAndAssign(...).whenComplete(...)` chain that resumes
  on completion while the network thread keeps spinning). The prior
  inline-`await` was the deviation that introduced the freeze.

Critic: please confirm the §31 wording matches the implemented behavior and
that the rule edit is in-scope for the agent-roles.md process given the
PLAN mandate above.

## Scope decision: 41b covers `reconcile`'s callbacks (not the release paths)

The PLAN's 41b "resume steps" enumerate reconcile-specific work
(`enable_partitions_awaiting_callback`, the assign/revoke completion, the
next callback in the revoke→assign sequence, the failure path) and the two
**verified** blockers are both reconcile-callback-driven. Accordingly the
non-blocking rework targets `ConsumerMembershipManager::reconcile` (the
`onPartitionsRevoked` → `onPartitionsAssigned` sequence driven from
`run_once` Phase 2.5).

The `onPartitionsLost` callbacks on the release paths
(`transition_to_fenced` / `transition_to_fatal` / `transition_to_stale` /
`signal_member_leaving_group`) still use the blocking
`invoke_rebalance_callback` (which `ack_rx.await`s). Rationale: those are
driven from `run_once` Phase 2.4 transitions and Java's callback-reentrancy
test suite (`PlaintextConsumerCallbackTest`) only reenters consumer ops
from `onPartitionsAssigned` / `onPartitionsRevoked`, never from
`onPartitionsLost`. A user who reenters a bg-routed op from
`on_partitions_lost` during a fence/fatal/stale/leave would still briefly
freeze the loop for that callback — this is a narrower residual than the
deadlock 41b closes and is out of scope for the PLAN's resume-step list. If
the Critic considers the release paths in-scope, the same
`enqueue_rebalance_callback` + pending-state pattern can be extended to
them in a follow-up.

## Handle commit ops deviation

`ConsumerHandle::commit_sync` / `commit_async` do NOT run the interceptor
`onCommit` chain or drain the `OffsetCommitCallbackInvoker` (the handle does
not own those — they stay on `&mut self`). A listener that flushes offsets
via `handle.commit_sync()` gets the durability guarantee (the commit RPC
completes) without the interceptor side-channel, which fires from the
owning consumer's own `commit_*` path. `commit_async` on the handle is
no-callback only (mirrors Java `commitAsync()`); user `OffsetCommitCallback`
registration is not exposed on the handle.

---

# Critic 41 review (`3051540..b9b1ee4`) — resolutions

The four issues filed in COMMENTS.41.md were fixed in the fixup commits
below. The original wording is preserved for the record; the resolution
follows each.

## Issue 1 (Bug) — `pending_reconcile` never cleared on fence/fatal/stale — RESOLVED

A release transition (fence/fatal/stale) that interleaved with an in-flight
reconcile callback left the stored `PendingReconcile` (+ flag) stranded, so a
fresh post-rejoin reconcile was gated on the stale ack draining, and
correctness leaned on the abort-check + un-cleared `reconciliation_in_progress`.

**Fix:** `transition_to_fenced` / `transition_to_fatal` / `transition_to_stale`
now call `clear_pending_reconcile()` at their top. It takes the
`pending_reconcile` mutex (no flag fast-path — that would race the
`drive_pending_reconcile` take/store-back window in the multi-task component
tests), drops the stored `PendingReconcile` (abandoning the continuation and
its ack receiver, mirroring Java dropping the in-flight `revokeAndAssign`
future), and — when it actually dropped a pending state — calls
`mark_reconciliation_completed()` (Java's `whenComplete` →
`maybeAbortReconciliation` clears `reconciliationInProgress`). A fresh
post-rejoin reconcile then starts immediately.

**Tests (component):** the three `delayed_reconciliation_result_discarded_*`
tests were reworked to the new eager-discard flow (single non-looping
`reconcile_once` to park, then `transition_to_*` clears the pending reconcile
eagerly). Each asserts (a) no transition to ACKNOWLEDGING with the stale
`resolved_assignment` and (b) `has_pending_reconcile_for_test == false`
immediately after the transition + the fresh post-rejoin assignment
reconciles. Covers fence (assigned-park + revoked-park) and fatal.

## Issue 2 (Design) — extend non-blocking enqueue+pending-ack to the release paths — RESOLVED

The release-path `on_partitions_lost` callbacks (fence/fatal/stale) kept the
blocking `invoke_rebalance_callback` (inline `ack_rx.await`) and, because they
run inline in `run_once` Phase 2.4, froze the bg loop for the whole callback —
a deadlock (until API timeout) for any bg-routed reentrant handle op the
listener submits, NOT a "brief freeze".

**Fix (USER DECISION: extend non-blocking):** `transition_to_{fenced,fatal,
stale}` no longer `.await` the ack. They enqueue the §31 `onPartitionsLost`
callback via `enqueue_release_callback` (None when no owned partitions / no
listener → run the release tail inline), store a `PendingRelease::{Fenced,
Fatal,Stale}` (ack receiver + tail discriminator), and RETURN. The bg loop's
Phase 2.4 drives `drive_pending_release()` (alloc-free `try_recv`) each
iteration, running the type-specific tail (`clearAssignment()` + fence/stale
rejoin) only when the listener acks — Java's `signalPartitionsLost(...)
.whenComplete(...)`. No `select!` shrink of `poll_wait_time_ms`; the existing
`process_background_events` ack poke (shared by reconcile + release) wakes the
loop.

**Issue-1/Issue-2 interaction (pending-state design):** the two pending
machines are kept **separate** — `pending_reconcile` (revoke→assign
sequencing) and `pending_release` (single onPartitionsLost, no sequencing) —
each with its own lock-free flag. A release transition both (1) abandons any
pending reconcile (Issue 1) AND (2) stores its own pending release (Issue 2),
in that order. Phase 2.4 drives the pending release BEFORE processing new
heartbeat-classified transitions, and skips draining new transitions while a
release is pending (so none are lost — they stay on the mpsc channel and are
drained once the release resolves), mirroring Java chaining the next action
onto the in-flight future.

**`leaving` clarification:** the COMMENTS premise that `leaving` is driven
from Phase 2.4 is slightly off. `leave_group` / `signal_member_leaving_group`
(unsubscribe/close) already runs on a `tokio::spawn`ed continuation in
`process_unsubscribe` / `process_leave_group_on_close` — NOT inline in the bg
loop — so its `on_partitions_lost` never froze the loop. It is left unchanged
(already non-blocking w.r.t. the bg loop). Only fence/fatal/stale needed the
rework. Documented in §31.

**Tests:** `release_transition_does_not_block_on_callback_ack` (component)
asserts `transition_to_fenced` returns promptly with the ack NOT sent
(state==FENCED, `has_pending_release==true`, owned partition still assigned),
then ack+drive advances to JOINING. The existing
`handle_reentrant_op_completes_through_bg_pipeline` (consumer level) proves a
reentrant `ConsumerHandle` op submitted from another task (standing in for a
listener body) routes through the bg pipeline and completes — the no-freeze
property now holds for `on_partitions_lost` too. The `on_partitions_lost_*`
and `stale_member_waits_for_callback_to_rejoin_when_timer_reset` component
tests were updated to drive the release via the new
`drive_release_to_completion` test helper.

## Issue 3 (minor) — no local regression for the app-side Notify poke — RESOLVED

**Fix (test added):** `process_background_events_ack_pokes_bg_wakeup`
(`&mut self`-level component test). The `make_test_consumer_with_channels`
fixture now exposes the bg-wakeup flag via `ConsumerTestHandles.bg_wakeup_called`
(set by the fixture's bg-wakeup fn). The test registers a listener, enqueues a
`ConsumerRebalanceListenerCallbackNeeded` event, runs
`process_background_events`, asserts the ack was sent, and asserts
`bg_wakeup_called == true`. Removing the `network_thread_close.wakeup()` poke
now fails this local test.

## Issue 4 (minor) — `handle.assign([])` diverges from Java — RESOLVED

**Fix:** `ConsumerHandle::assign` with an empty collection now REJECTS with a
clear `IllegalArgument` error pointing the caller at the owning consumer's
`unsubscribe()` (on the owning consumer `assign([])` leaves the group, which
the handle does not expose). The previous empty-AssignmentChange path (and its
"mirror the behaviour" comment) is removed. **Test:**
`handle_assign_empty_is_rejected` asserts the error variant + that the message
mentions `unsubscribe`.

---

# CLAUDE-rule change (additional): `consumer-threading.md` §31 release-path coverage (agent-roles.md process)

Phase 41 Issue 2 extends the §31 non-blocking contract to the release-path
`on_partitions_lost` callbacks. §31 was updated (no CLAUDE.md change) to:
  - state that BOTH the reconcile callbacks and the release callback
    (`on_partitions_lost` from fence/fatal/stale) use the non-blocking
    store-receiver + cross-iteration `try_recv` pattern (reconcile:
    `drive_pending_reconcile`; release: `drive_pending_release`);
  - note the `leave_group`/unsubscribe/close `on_partitions_lost` exception
    (already non-blocking — runs on a spawned continuation);
  - record the Issue-1 `clear_pending_reconcile` rule (a release transition
    abandons any in-flight reconcile);
  - record the Issue-4 empty-`assign` rejection on the handle;
  - widen the inline-`ack_rx.await` anti-pattern to cover the release
    transitions.

Per `agent-roles.md`, this §31 wording change extends the rule mandated by the
PLAN; flagged here, CLAUDE.md untouched.
