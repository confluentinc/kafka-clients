# Phase 35 — Test parity: STALE-member path + heartbeat field-diff

Actor 35. Branch `consumer-impl`. Test-parity effort (prefer test-only).
Source of truth: `design/current/test-translation-review/02-membership-heartbeat.md`
and the Java tests `ConsumerMembershipManagerTest.java` /
`ConsumerHeartbeatRequestManagerTest.java` (Apache Kafka 4.2).

## HARD CONSTRAINT
Prefer test-only. Production change ONLY for a genuine Java-fidelity bug,
perf/CPU-neutral (no new per-record / hot-path allocation or dynamic
dispatch), called out with the Java line + a one-line perf justification.

---

## (A) STALE-member path

`MemberState::Stale` and the transitions exist; no test drives the
poll-timer-expiry → STALE path.

### Java → Rust mapping

| Java test | Rust test (new) | Notes |
|---|---|---|
| testTransitionToLeavingWhileReconcilingDueToStaleMember | transition_to_leaving_while_reconciling_due_to_stale_member | reach RECONCILING (join+assignment), then leave(true)+HB-gen → STALE |
| testTransitionToLeavingWhileJoiningDueToStaleMember | transition_to_leaving_while_joining_due_to_stale_member | JOINING → leave(true)+HB-gen → STALE |
| testTransitionToLeavingWhileStableDueToStaleMember | transition_to_leaving_while_stable_due_to_stale_member | STABLE → leave(true)+HB-gen → STALE |
| testTransitionToLeavingWhileAcknowledgingDueToStaleMember | transition_to_leaving_while_acknowledging_due_to_stale_member | ACKNOWLEDGING → leave(true)+HB-gen → STALE |
| testStaleMemberDoesNotSendHeartbeatAndAllowsTransitionToJoiningToRecover | stale_member_does_not_send_heartbeat_and_allows_transition_to_joining_to_recover | STALE ⇒ should_skip_heartbeat()==true; maybe_rejoin_stale_member ok |
| testStaleMemberRejoinsWhenTimerResetsNoCallbacks | stale_member_rejoins_when_timer_resets_no_callbacks | STALE (no owned partition) → maybe_rejoin → JOINING |
| testStaleMemberWaitsForCallbackToRejoinWhenTimerReset | stale_member_waits_for_callback_to_rejoin_when_timer_reset | owned partition → onPartitionsLost; maybe_rejoin stays STALE until ack; then JOINING + assignment cleared |
| testLeaveGroupWhenMemberIsStale | leave_group_when_member_is_stale | STALE → leave_group() unsubscribes, stays STALE |
| testPollTimerExpiration (HB mgr) | poll_timer_expiration | HB-mgr poll after max.poll.interval → leave HB + transition_to_sending_leave_group(true) + reset + rejoin |
| testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving (HB mgr) | poll_timer_expiration_should_not_mark_member_stale_if_member_already_leaving | already LEAVING → no transition_to_sending_leave_group; HB still generated |

### Production fidelity gap (STALE assignment release)

`abstract_membership_manager.rs::on_heartbeat_request_generated` (sync)
does Leaving→STALE via `transition_to(Stale)` only. Java's
`AbstractMembershipManager.java:791 transitionToStale()` additionally
(1) `signalPartitionsLost(assignedPartitions)` (onPartitionsLost), and
(2) `clearAssignment()` on completion. The sync Rust path cannot await the
§31 listener.

Decision:
- For all STALE tests with an **empty** assignment (every test except
  `testStaleMemberWaitsForCallbackToRejoinWhenTimerReset`): after
  `transition_to_sending_leave_group(true)` the `current_assignment` is
  already `LocalAssignment::none()` (abstract_membership_manager.rs:608) and
  there are no owned partitions, so onPartitionsLost short-circuits and
  `clearAssignment` is a no-op. These pass **test-only**.
- For the owned-partition callback test: a faithful async STALE release is
  required. Add `ConsumerMembershipManager::transition_to_stale(now)` (async,
  mirroring the existing `transition_to_fenced` override: onPartitionsLost
  via §31 + `clear_assignment`) and route Leaving→STALE through a new
  `PendingMembershipTransition::Stale` side-channel — exactly like
  Fenced/Fatal already do (`consumer_network_thread.rs:539`). The sync
  `on_heartbeat_request_generated` Leaving→Stale arm keeps only the state
  transition; the concrete async path performs the release. **Perf
  justification**: fires only on poll-timer expiry (rare, not per-record /
  not hot path); reuses the existing spawn+mpsc side-channel; zero new
  per-record allocation or dynamic dispatch. Java line:
  `AbstractMembershipManager.java:791-806`.

  The test drives the async release directly via the new
  `transition_to_stale` method (not through the bg-task) — same as the
  existing fence tests drive `transition_to_fenced` directly.

---

## (B) Heartbeat request-field diff + lifecycle gaps

`HeartbeatState::build_request_data` field-omission logic is untested.
Java mocks the membership manager's getters; Rust drives a REAL membership
manager into the right state. Where Java mocks `state()`/`memberId()`/
`memberEpoch()`/`currentAssignment()`/`serverAssignor()`/`rackId()`,
the Rust test reaches the same observable by real transitions or, for the
`HeartbeatState`-only tests, a `#[cfg(test)]` direct `HeartbeatState`
constructor + state-forcing on the inner guard.

| Java test | Rust test (new) | Notes |
|---|---|---|
| testHeartBeatRequestStateToStringBase | heartbeat_request_state_to_string_base | exact Display string, no "Optional" leak |
| testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments | first_heartbeat_includes_required_info_to_join_group | JOINING: member-id/epoch0/topics/rebalanceTimeout/groupId/instanceId/serverAssignor/rackId present |
| testValidateConsumerGroupHeartbeatRequest | validate_consumer_group_heartbeat_request | STABLE after HB success: all required fields w/ correct values |
| testValidateConsumerGroupHeartbeatRequestAssignmentSentWhenLocalEpochChanges | validate_heartbeat_request_assignment_sent_when_local_epoch_changes | HB1 sends TP; HB2 omits (unchanged); HB3 re-sends after local epoch bump |
| testHeartbeatState | heartbeat_state_field_diff_lifecycle | full build_request_data lifecycle: join→stable→rejoin, field present/omit matrix |
| testRackIdInHeartbeatLifecycle | rack_id_in_heartbeat_lifecycle | rackId only on JOINING; omitted otherwise; empty rackId never sent |
| testRegexInHeartbeatLifecycle | regex_in_heartbeat_lifecycle | regex sent on change; "" sent to clear; omitted when unchanged |
| testRegexInJoiningHeartbeat | regex_in_joining_heartbeat | "" sent to unsubscribe; JOINING with no pattern omits regex |
| testPollOnLeaving | poll_on_leaving (matrix loop) | LEAVING: dynamic+RemainInGroup ⇒ no HB; else HB + on_heartbeat_request_generated |
| testSendingLeaveGroupHeartbeatWhenPreviousOneInFlight | sending_leave_group_heartbeat_when_previous_one_in_flight | inflight HB blocks; LEAVING forces leave HB (epoch -1) anyway; then STALE skips |
| testPollOnCloseGeneratesRequestIfNeeded | poll_on_close_generates_request_if_needed (matrix loop) | pollOnClose: leave HB iff leaving (dynamic+RemainInGroup ⇒ none) |
| testisExpiredByUsedForLogging | is_expired_by_used_for_logging | poll past interval → leave HB; reset_poll_timer; behaviour pin via timing accessors |
| testConsumerAcksReconciledAssignmentAfterAckLost | consumer_acks_reconciled_assignment_after_ack_lost | reconcile → HB1 (ack) → timeout resets sent_fields → next HB re-includes TP |

### Test-infra needs
- A `#[cfg(test)]` direct `HeartbeatState::new(subs, mm, rebalance_timeout)`
  reachable from tests (already exists as `HeartbeatState::new`; expose via a
  helper on the manager or test it through the manager's poll). For
  build_request_data lifecycle tests, drive `heartbeat_state` directly — add
  a `#[cfg(test)]` accessor `build_request_data_for_test()` on the manager,
  plus a force-state helper on the membership manager.
- `#[cfg(test)]` builders to set member_id / member_epoch / state on the
  membership manager inner guard (force-state, used like Java's Mockito
  `when(...).thenReturn(...)`). member-id is normally set from a HB response;
  use `on_heartbeat_success(heartbeat_response(member_id, epoch))` to set it
  realistically where possible, else force via inner guard.

---

## Skips (documented)
- HeartbeatTest (7): OUT_OF_SCOPE — classic-coordinator (`AbstractCoordinator`)
  dependency, §20.
- RebalanceMetrics / HeartbeatMetrics tests: OUT_OF_SCOPE — no Rust metrics
  framework (consistent with prior phases).
- testNetworkTimeout / testDisconnect / testFailureOnFatalException /
  InOrder-verify matrix rows: already REDUCED-covered by Phase-12.5 response-
  routing tests; not duplicated.

## Docstring corrections (bookkeeping)
- `consumer_heartbeat_request_manager.rs` header "Translated (12 / 31)" →
  corrected count after Phases 12.5 + 35.
- membership header "~26/93" → corrected count after Phases 34 + 35.

## Commits
1. Phase 35: STALE path (membership + HB-mgr STALE tests; prod
   `transition_to_stale` + `PendingMembershipTransition::Stale` if needed).
2. Phase 35: heartbeat field-diff (build_request_data lifecycle tests).
3. Phase 35: heartbeat lifecycle + docstrings.

After each: cargo build / test --lib / xtask lint / xtask format-check green.
