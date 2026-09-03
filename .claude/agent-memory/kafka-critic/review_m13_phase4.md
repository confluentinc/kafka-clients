---
name: review-m13-phase4
description: M13 Phase 4 consumer rebalance/poll (AK 4.3.1) — three-leg handshake reshape; explicit-vs-new arm adjudication trap; concurrent-COMMENTS-file clobber hazard
metadata:
  type: project
---

Milestone-13 Phase 4 (AK 4.2→4.3.1 consumer rebalance/poll, agent 64). Highest-risk
phase; came back CLEAN (0 correctness bugs, 4 minor observations). The reshape
(KAFKA-20106/20321/20382/20426/20428/20535) was high-fidelity.

**Why: adjudication traps + review heuristics worth reusing.**

- **"New 4.3.1 arm" ≠ "new 4.3.1 behavior."** A `case GROUP_ID_NOT_FOUND:` block
  that is entirely `+` in the 4.2→4.3.1 diff can still be behaviorally a no-op for
  one of its branches. Check the *4.2.0* blob: if the code previously fell to
  `default:` → `handleFatalFailure` (unknown-error path), then the "new" explicit
  fatal branch just makes existing behavior explicit — only the *other* branch
  (here UNSUBSCRIBED→skip) is genuinely new. Rust keeping its pre-existing
  Milestone-8 epoch-conditional recovery for the non-unsubscribed arm is therefore
  a *pre-existing* deviation (from 4.2 too), not a dropped 4.3.1 fix. Verdict:
  defensible + documented, skip of `testGroupIdNotFoundWhileStableIsFatal` OK.
  I initially over-flagged this as "4.3.1 introduces fatal-when-stable"; the
  `git -C kafka show 4.2.0:...` check corrected it. Always run that check before
  claiming a delta introduces new semantics.

- **Three-leg handshake shape (for §28/§31 amendment + future consumer reviews):**
  bg reconcile → `BackgroundEvent::PartitionsAssigned{assigned,added,ack}` (sent
  EVEN WITH NO listener, stores `AfterAssign`, never awaited — `try_recv`
  cross-iteration) → app `poll()` drains, sends `ApplicationEvent::ApplyAssignment`
  (`add_and_get.await`) → AEP `process_apply_assignment` mutates SubscriptionState
  on bg (so `assignment()` changes only within poll) → app runs
  `on_partitions_assigned` → ack. Revoke/lost = `PartitionsRemoved` (rename of
  `ConsumerRebalanceListenerCallbackNeeded`, 4.2 shape). Apply-failure wraps
  "Failed to apply the new assignment" + records first_error (KAFKA-20382).
  `skip_assignment_events` (KAFKA-20428): unsubscribe+close complete
  PartitionsAssigned EXCEPTIONALLY with exact text "Assignment event skipped
  because consumer is unsubscribing", NOT into first_error. `wait_reconciliation
  _check` gates collect_fetch on `has_pending_reconciliation` (Arc<AtomicBool> set
  by `on_member_state_change`==RECONCILING) + AsyncPollState Notify (create-
  notified()-before-check; biased select vs rotating wakeup token; no busy-spin).
  Reconcile gate moved after computing revoked: `!can_commit && (auto_commit ||
  !revoked.is_empty())`.

- **Verify the app-side notifier is registered in the SAME list transition_to
  iterates** (`state_updates_listeners` via `register_state_listener`). If a new
  `on_member_state_change` hook fires from `transition_to` but the notifier lives
  in a different listener list, the flag never flips and the gate silently
  disables — a real bug. Here it was correctly registered.

**Process hazard — concurrent writers clobber COMMENTS.N.md.** I dispatched a
"fork" to cross-check the heartbeat commit; the fork ran as a full reviewer (forks
weren't available from that context), independently reviewed everything, and WROTE
COMMENTS.64.md — clobbering the version I had Write-n. Its content was actually
superior on item 6. Lesson: don't have two agents write the same COMMENTS file.
Either (a) scope sub-agents to REPORT findings only (not write the file), or
(b) merge on completion — I appended my two unique observations (unsorted callback
partitions; import-only test-file deltas not in skip list) into the fork's file
rather than overwrite. agent-roles.md says "take an exclusive lock" on the COMMENTS
file — honor that when parallelizing.
