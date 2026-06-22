# Critic #35 — Phase 35 review (STALE-member path + heartbeat field-diff) — RESOLVED

Commits reviewed: d04703d, 350f8cc, 50b73da.
Files: consumer_heartbeat_request_manager.rs, abstract_membership_manager.rs,
consumer_membership_manager.rs, consumer_network_thread.rs, heartbeat_request_state.rs,
subscription_state.rs.

All 32 HB-manager tests + 82 membership-manager tests pass; `cargo build --lib` is
clean (no new warnings).

---

## Issue 1: stale "/ 93" denominator left uncorrected in `consumer_membership_manager.rs` doc — RESOLVED
- **File**: `src/consumer/internals/consumer_membership_manager.rs:1129`
- **Severity**: Documentation inconsistency (not a code bug)
- **Description**: Commit 350f8cc corrected the doc header (line 1081-1083) to
  "`ConsumerMembershipManagerTest`, 84 `@Test` cases … actual coverage is now
  ~52 / 84". I verified the Java count: `grep -c "@Test"` on
  `ConsumerMembershipManagerTest.java` = 84, so the new "84" and the
  "26 / 93 predates Phases 34+35" note are correct. However the
  "Not translated" line was NOT updated in the same pass:

      /// Not translated (~67 / 93) — rationale categories:

  This retains the stale denominator 93 (the very number the header says is
  wrong) and is also arithmetically inconsistent with the corrected header:
  ~52 translated of 84 ⇒ ~32 not translated, not 67.
- **Expected**: `Not translated (~32 / 84)` (or whatever the true residual is),
  consistent with the corrected "~52 / 84".
- **Actual**: `Not translated (~67 / 93)`.
- **Note**: Cosmetic only — no behavioral impact. Flagged because the task
  explicitly asked to confirm the corrected counts are accurate, and this one
  was only half-corrected.

### Resolution
- Verified the Java denominator: `ConsumerMembershipManagerTest.java` has 82
  `@Test` + 2 `@ParameterizedTest` = 84 cases, matching the corrected header.
- Updated line 1129 to `Not translated (~32 / 84)` so it is consistent with the
  corrected header's "~52 / 84" (84 − 52 = ~32 not translated).
- Pure doc/comment change. `cargo xtask format-check` and `cargo build --lib`
  both pass.

---

## Everything else: reviewed and clean

The following were scrutinized and found faithful — recorded so a future pass
need not re-derive them:

### A) Production change — the bug was real and the fix is correct
- **Bug confirmed real.** Pre-Phase-35 (98742ee), the HB-manager poll-timer-expiry
  path built the leave heartbeat but had only ONE `on_heartbeat_request_generated`
  call (the normal-HB path). The leave path never called it, so a member in
  LEAVING (poll-timer-expired) never reached STALE via `poll()`. Java's
  `AbstractHeartbeatRequestManager.makeHeartbeatRequest(currentTimeMs, true)`
  (line 281-284) ALWAYS calls `membershipManager().onHeartbeatRequestGenerated()`,
  which for LEAVING + `isPollTimerExpired` runs `transitionToStale()`
  (AbstractMembershipManager.java:715-720). Fix at
  consumer_heartbeat_request_manager.rs:906 adds the call at the correct point
  (every generated leave HB), and the `heartbeatRequestState.reset()` /
  `resetHeartbeatState()` tail (lines 917-918) matches Java:182-183. The normal
  path (line 943) is unchanged, so steady-state field-diff behavior is unaffected.
- **transitionToStale tail ordering correct.** Java (AbstractMembershipManager.java
  :791-806): `transitionTo(STALE)` → `signalPartitionsLost(assignedPartitions())`
  → `whenComplete{ clearAssignment() }`. Rust splits the sync STALE transition
  (into `on_heartbeat_request_generated`) from the async release
  (`ConsumerMembershipManager::transition_to_stale`), which invokes
  onPartitionsLost THEN clear_assignment — release before clear, matching Java.
  Reads `subscriptions.assigned_partitions()` (not `current_assignment`), matching
  Java's `subscriptions.assignedPartitions()`. The deferral of STALE→JOINING via
  the two bool flags faithfully mirrors Java chaining `transitionToJoining` onto
  the in-flight `staleMemberAssignmentRelease` future (maybeRejoinStaleMember,
  :776-783).
- **No STALE wedge.** `stale_assignment_release_pending` is set true only inside
  `on_heartbeat_request_generated` when entering STALE (always paired with a
  `PendingMembershipTransition::Stale` send from poll(), since STALE is only
  reachable from LEAVING+expired which only the leave-path produces). The flag is
  cleared UNCONDITIONALLY in `transition_to_stale`'s tail. The tx/rx are both owned
  by the same `ConsumerHeartbeatRequestManager` struct, so the send cannot fail
  (receiver never dropped while sender lives), and the bg task drains the channel
  every iteration. `invoke_rebalance_callback` errors are awaited + logged, not
  propagated, so the clear is always reached. `stale_rejoin_requested` is idempotent
  (set true repeatedly, read+cleared once). Conclusion: the side-channel cannot
  wedge the member in STALE.

### B) PERF — neutral
- The new `PendingMembershipTransition::Stale` reuses the EXISTING Fenced/Fatal
  side-channel verbatim: same unbounded mpsc, same `take_pending_membership_transitions()`
  Vec-drain under the `request_managers` lock that the bg task already takes
  (consumer_network_thread.rs:528-560). In the common no-stale case the drain is a
  single empty `try_recv` — zero new await, alloc, lock, or per-record cost.
- The network poll is NOT wrapped/cancelled (CLAUDE.md §10 honored); the new arm
  only adds an `await` inside the already-existing post-poll transition loop, which
  is empty in steady state.
- `on_heartbeat_request_generated` is NOT newly fired on the steady-state HB path
  (line 943 pre-existed); the only new call (906) fires solely on poll-timer expiry.

### C) Test fidelity — solid
- `poll_timer_expiration` and `poll_timer_expiration_should_not_mark_member_stale_if_member_already_leaving`
  drive the REAL `mgr.poll()` path and assert `state == Stale` / `state != Stale`.
  Since `on_heartbeat_request_generated` is the ONLY STALE setter, removing the
  production fix (line 906) makes these FAIL — they genuinely pin the fix. The
  already-leaving test sets up the precondition via `leave_group().await` and asserts
  `is_leaving_group()`, pinning the `!is_leaving_group()` guard (Java:171). 2nd-poll
  0-request assertion pins STALE in `should_skip_heartbeat` (matches Java).
- `stale_member_waits_for_callback_to_rejoin_when_timer_reset` owns a real partition
  via `mock_owned_partitions` (populates SubscriptionState, so onPartitionsLost has a
  non-empty release), parks on the un-acked §31 callback, calls
  `maybe_rejoin_stale_member`, asserts STALE persists, then acks → JOINING. This pins
  the `stale_assignment_release_pending` deferral (removing it would transition to
  JOINING immediately and fail the STALE assertion).
- Field-diff tests assert positive presence with values (first HB), OMISSION of
  unchanged fields (assignor=None, topic_partitions=None), and RE-SEND on change
  (local-epoch change re-sends topic_partitions; rack-id JOINING-only lifecycle;
  regex change/clear/omit lifecycle). Deleting a send-only-if-changed branch would
  fail these.
- toStringBase test builds `expected` from `to_string_base()` (mirroring Java's own
  `target = requestState.toStringBase() + ...` construction) AND pins the
  heartbeat-specific suffix verbatim AND asserts no `Optional`/`Some` leak. Acceptable.
- All new `_for_test` helpers (`build_request_data_for_test`,
  `set_subscription_pattern_for_test`) are `#[cfg(test)]`-gated; `force_*` helpers are
  inside the `#[cfg(test)] mod tests` block. No prod leak.

---

## Verdict

- **Issue count: 1** (Issue 1 — cosmetic doc denominator, not a code bug) — RESOLVED.
- **PERF VERDICT: NEUTRAL.** Reuses the existing Fenced/Fatal side-channel exactly;
  zero added per-iteration cost in the no-stale case; network poll not made
  cancel-unsafe; steady-state HB field-diff unchanged.
- **CORRECTNESS VERDICT (STALE side-channel): CANNOT WEDGE.** The release-pending flag
  is set only when entering STALE (always paired with a `Stale` transition send on an
  unbounded channel whose receiver is co-owned and never dropped), and is cleared
  unconditionally in `transition_to_stale`'s always-reached tail. Ordering, clear-after-
  callback, and the rejoin deferral all match Java (AbstractMembershipManager.java
  :776-806). No path leaves the member stuck in STALE.
