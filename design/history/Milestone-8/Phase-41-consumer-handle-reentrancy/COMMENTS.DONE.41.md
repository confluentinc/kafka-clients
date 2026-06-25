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
