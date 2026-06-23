# Phase 8b — COMMENTS.1.md resolutions (N=2)

All 13 findings from `COMMENTS.1.md` resolved on `consumer-impl` via two
fixup commits against the original Phase 8b commits.

Fixup commit map:
- `2be008a` fixup! Phase 8b (2/N): COMMENTS.1.md fixes #1, #2, #3, #4, #5, #6, #7, #8, #11, #12
- `f353024` fixup! Phase 8b (3/N + 4/N): heartbeat test translations + CMM rationale (addresses COMMENTS.1.md #9, #10)

## Per-finding resolutions

### #1 — `reconcile` always enqueues even with no listener (deadlock)
**Status: FIXED** (Behavior Mismatch, BLOCKING)

`AbstractMembershipManager::invoke_rebalance_callback` now reads
`subscriptions.rebalance_listener()` (in a short critical section, guard
dropped before any `.await`) and returns `Ok(())` immediately when no
listener is registered. Matches Java's
`subscriptions.rebalanceListener().isPresent()` guard at
`ConsumerMembershipManager.java:352-383`. New regression test:
`invoke_rebalance_callback_no_listener_short_circuits`.

### #2 — `reconcile` returns Ok on listener Err (masks failure)
**Status: FIXED** (Behavior Mismatch)

Both the revoked-callback and assigned-callback error branches in
`ConsumerMembershipManager::reconcile` now `return Err(e)` after
calling `mark_reconciliation_completed`. Phase 10's bg-task driver
sees the listener failure; Java's CompletableFuture chain does the
same via `revocationResult.completeExceptionally(callbackError)`. New
regression test: `reconcile_propagates_assigned_listener_error`.

### #3 — `leave_group` / `leave_group_on_close` not translated
**Status: FIXED** (Missing Requirement, BLOCKING)

Translated three new methods on `ConsumerMembershipManager`:

```rust
pub(crate) async fn leave_group(&self, current_time_ms: i64) -> Result<(), KafkaError>;
pub(crate) async fn leave_group_on_close(
    &self,
    op: GroupMembershipOperation,
    current_time_ms: i64,
) -> Result<(), KafkaError>;

async fn leave_group_inner(&self, run_callbacks: bool, current_time_ms: i64) -> Result<(), KafkaError>;
```

`leave_group_inner` is the shared body, mirroring Java's
`AbstractMembershipManager.leaveGroup(boolean runCallbacks)` phase-for-
phase (already-out-of-group fast path, already-leaving short-circuit,
transition to PREPARE_LEAVING, optional callback step, unsubscribe,
clear assignment, transition to LEAVING). Phase 11's consumer-close
path now has these to call. Translated tests:
`leave_group_epoch_test`, `leave_group_epoch_on_close`,
`listeners_get_notified_on_transitions_to_leaving_group`.

### #4 — `signal_member_leaving_group` not translated
**Status: FIXED** (Missing Requirement, BLOCKING)

Translated `ConsumerMembershipManager::signal_member_leaving_group`,
mirroring Java's `signalMemberLeavingGroup` +
`invokeOnPartitionsRevokedOrLostToReleaseAssignment` collapsed into a
single async method. Picks `onPartitionsRevoked` when `member_epoch > 0`,
`onPartitionsLost` otherwise; dispatches through
`AbstractMembershipManager::invoke_rebalance_callback` (which respects
the §31 short-circuit from #1). Exercised indirectly by the
`leave_group_*` tests.

### #5 — `unsafe impl Send for ConsumerMembershipManager`
**Status: FIXED** (Design Flaw)

Removed. `cargo build` confirms the type is auto-`Send` via its fields.

### #6 — `_force_used` dummy function
**Status: FIXED** (Design Flaw — minor)

Removed. The `BackgroundEvent` import (used only in tests) is now
gated behind `#[cfg(test)]`.

### #7 — `reset_poll_timer` doesn't call `maybe_rejoin_stale_member`
**Status: DOCUMENTED** (Behavior Mismatch, deferred to Phase 10)

Per finding's recommendation: added a rustdoc block on
`AbstractHeartbeatRequestManager::reset_poll_timer` calling out that
the abstract layer lacks the back-reference to the membership manager
needed to drive `maybe_rejoin_stale_member` on expiry, and showing
the call shape Phase 10 must use:

```rust
hb.reset_poll_timer(now);
if hb.poll_timer_is_expired(now) {
    membership_manager.maybe_rejoin_stale_member();
}
```

### #8 — `update_poll_timer` is dead behavior
**Status: FIXED** (Design Flaw — minor)

Removed the field `poll_timer_last_update_ms` and the setter
`update_poll_timer`. The deadline-based timer doesn't need a separate
"last update" cursor. Updated the consumer-heartbeat poll() call site
and the surviving abstract-heartbeat test to match.

### #9 — CMM test coverage gap (13/93)
**Status: PARTIALLY FIXED** (Missing Requirement, DoD §3)

Translated 13 additional cases (13 -> 26 of 93). Specifically the 11
listed in the finding plus the two `leave_group_epoch_*` tests that
depended on fix #3:

- `transition_to_failed_when_trying_to_join`
- `member_id_and_epoch_reset_on_fenced_members`
- `fencing_when_state_is_{stable, reconciling, prepare_leaving, leaving}`
- `listeners_get_notified_on_transitions_to_{fatal, leaving_group}`
- `new_assignment_ignored_when_state_is_prepare_leaving`
- `same_assignment_reconciled_again_when_fenced`
- `leave_group_epoch_test`, `leave_group_epoch_on_close`

Added a comprehensive docstring on the CMM test module enumerating
both the 26 translated cases AND a six-category rationale for the ~67
deferred cases (Mockito-spy verification on internals,
CommitRequestManager auto-commit interaction deferred to Phase 10,
Streams/Share out of scope per §20, CompletableFuture chain shape not
modelable in async/await, MockTime + metrics, real-metadata reconcile
paths).

### #10 — CHRM test coverage gap (6/31)
**Status: PARTIALLY FIXED** (Missing Requirement, DoD §3)

Translated 6 additional cases (6 -> 12 of 31):

- `heartbeat_on_startup` (Java `testHeartbeatOnStartup`)
- `timer_not_due` (Java `testTimerNotDue`)
- `heartbeat_not_sent_if_another_one_in_flight` (subset of Java's)
- `heartbeat_outside_interval` (Java `testHeartbeatOutsideInterval`)
- `handle_specific_unreleased_instance_id_is_fatal` (error-matrix row)
- `handle_specific_failure_unsupported_version_emits_error_event`
  (regression for fix #12)

Added a docstring on the CHRM test module enumerating both the 12
translated cases AND a rationale block for the 19 deferred cases
(Mockito-mocked membership-manager getters, response-delivery harness
that Phase 10 owns, BackgroundEventHandler-add-ordering verification
requiring InOrder, poll-timer-expiration + stale-rejoin which is
Phase 10's epilogue, regex resolution which lands in Phase 9).

Supporting helper: added `CoordinatorRequestManager::set_coordinator_for_test`
(cfg(test)) to inject a coordinator without a full FindCoordinator
round-trip.

### #11 — `transition_to_fatal` doc inconsistent with code
**Status: FIXED** (Documentation)

Rewrote the rustdoc on `ConsumerMembershipManager::transition_to_fatal`
to match the code: Phase 8b DOES invoke `onPartitionsLost` via the §31
handshake and clears the assignment after the await. The previous
"defer to next poll" sentence has been removed; the new doc lists the
four-step flow explicitly.

### #12 — `handle_specific_failure` hardcodes `current_time_ms = 0`
**Status: FIXED** (Bug — low severity)

Signature changed:

```rust
pub(crate) fn handle_specific_failure(
    &mut self,
    error: &KafkaError,
    current_time_ms: i64,
) -> bool
```

The threaded `current_time_ms` is now passed to
`BackgroundEventHandler::add(event, current_time_ms)`. No callers in
production code yet (Phase 10 wires it); regression test
`handle_specific_failure_unsupported_version_emits_error_event`
asserts the method is invoked successfully with a non-zero timestamp.

### #13 — Plan inflates Java test counts
**Status: ACKNOWLEDGED** (Documentation suggestion — not actionable on
the source tree this phase)

The new test-module docstrings on both CMM and CHRM cite the *actual*
Java case counts (93 and 31) so future planning starts from the right
baseline.

---

## Final verification

- `cargo build`: clean
- `cargo test --lib`: **1433 passed** (baseline 1413, +20 new)
- `cargo test --test consumer`: 36 passed
- `cargo test --lib -- --test-threads=1`: 1433 passed in 13s (no hangs)
- `cargo xtask format-check`: clean
- `cargo xtask lint`: clean (no clippy warnings under `#![deny(warnings)]`)
- No new dependencies
- No `panic!` / `unimplemented!` / `todo!` introduced
- No tests timed out under single-threaded execution

## §31 re-audit verdict

PASS — both items called out as PARTIAL in COMMENTS.1.md are now
Java-faithful:

- **Listener-presence short-circuit (#1)**: handshake aborts cleanly
  in `invoke_rebalance_callback` before the enqueue, matching Java's
  `invokeOnPartitions{Revoked,Assigned,Lost}Callback` guard.
- **Listener error propagation (#2)**: `reconcile` returns the error
  to the caller; Phase 10 bg-task driver gets to decide how to handle
  it. Java's `CompletableFuture` chain does the same via
  `whenComplete`.

§16 lock-across-await audit verdict: PASS (unchanged).
