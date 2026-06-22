---
name: phase35-stale-heartbeat-notes
description: Milestone-8 Phase 35 test-parity — STALE-member path + heartbeat request-field-diff; Java-mock-to-real-state and async-STALE-release patterns
metadata:
  type: project
---

Phase 35 (Actor 35) closed the STALE-member gap and the heartbeat
request-field-diff gap from `design/current/test-translation-review/02-membership-heartbeat.md`.
23 new tests across `consumer_membership_manager.rs`,
`consumer_heartbeat_request_manager.rs`, `heartbeat_request_state.rs`.
Commits: d04703d (STALE), 350f8cc (field-diff + docstrings), 50b73da (lifecycle).

**Production fidelity bug found + fixed (perf-neutral, poll-timer-expiry only):**
The Rust HB poll-timer-expiry path built the leave HB but NEVER called
`on_heartbeat_request_generated()`. Java's `makeHeartbeatRequest(now, ignore)`
ALWAYS calls it (AbstractHeartbeatRequestManager.makeHeartbeatRequest), which for
an expired poll timer drives LEAVING→STALE. Without it the member stayed LEAVING
forever and the STALE path was UNREACHABLE via poll(). When auditing a "feature X
untested" worklist item, check whether the production trigger is even wired —
here the trigger call was missing, not just the test.

**Async STALE-release pattern (mirrors fence/fatal):** Java `transitionToStale`
does (1) transitionTo(STALE) sync inside onHeartbeatRequestGenerated, then (2)
signalPartitionsLost + clearAssignment async (CompletableFuture whenComplete).
Rust split: sync `on_heartbeat_request_generated` Leaving→STALE arm sets
`stale_assignment_release_pending=true`; async
`ConsumerMembershipManager::transition_to_stale(now)` does the §31 onPartitionsLost
+ clear_assignment + clears the flag; routed through a NEW
`PendingMembershipTransition::Stale` side-channel drained+awaited by the bg task
(consumer_network_thread.rs) exactly like Fenced/Fatal. `maybe_rejoin_stale_member`
now defers STALE→JOINING while `stale_assignment_release_pending` is true (records
`stale_rejoin_requested`); the release-completion path does the JOINING transition
— faithful to Java chaining `transitionToJoining` onto the in-flight
`staleMemberAssignmentRelease` future. Two new bool fields on MembershipInner
(prod+test, zero hot-path cost).

**Java-mock → real-state for heartbeat field-diff (testHeartbeatState family):**
Java mocks every membership-manager getter (state/memberId/memberEpoch/
currentAssignment/serverAssignor/rackId) + subscriptions. Rust uses a REAL
manager. Added `#[cfg(test)]` force helpers in the HB test module:
`force_state` / `force_member` / `force_current_assignment` (write
`mm.abstract_mm.inner.lock()` fields directly, bypass transition validity),
`set_subscription` / `set_pattern` (drive shared SubscriptionState). Added
`ConsumerHeartbeatRequestManager::build_request_data_for_test()` (HeartbeatState
is a private inner type) and `SubscriptionState::set_subscription_pattern_for_test`
(sets type AutoPatternRe2j + Option pattern; None+type=Re2j ⇒ subscription_pattern()
returns None = "regex removed but still pattern-type", which is how Java's
`when(subscriptionPattern()).thenReturn(null)` is mirrored). Where Java asserts the
mocked DEFAULT_MEMBER_ID, assert the REAL random-UUID `mm.member_id()` — the
contract is "HB carries the member's own id".

**Field-diff gotcha:** `testHeartbeatState` STABLE build asserts
`topicPartitions()==emptyList()` even though it looks "unchanged" — because Java's
`mockStableMemberData` sets currentAssignment to `LocalAssignment(0, emptyMap)`
which DIFFERS from the JOINING build's `LocalAssignment.NONE` (epoch -1). So the
assignment CHANGED (NONE→epoch-0-empty) and is re-sent as `Some([])`. Set the
STABLE assignment explicitly in the test, don't leave it at none().

**poll_on_leaving real-vs-mock divergence:** Java's testPollOnLeaving isolates
`shouldSendLeaveHeartbeatNow()` by leaving `shouldHeartbeatNow()` at Mockito's
false default. A REAL LEAVING member's `should_heartbeat_now()` is ALSO true
(Acknowledging|Leaving|Joining), so `poll()` can't isolate the predicate (a
dynamic+RemainInGroup LEAVING member would still HB via should_heartbeat_now).
Fix: added `should_send_leave_heartbeat_now_for_test()` and assert the predicate
DIRECTLY (the unit Java tests) + the positive poll() cases. `poll_on_close` is
clean (uses is_leaving_group, not should_heartbeat_now).

**Poll-timer math:** the HB poll timer uses CONFIG `max.poll.interval.ms`
(default 300_000), NOT the membership `rebalance_timeout_ms` (a distinct field
passed to ConsumerMembershipManager::new). Arm via `mgr.inner.reset_poll_timer(0)`
(timer is i64::MAX/unarmed at construction — Issue 9), then poll at
`300_000 + exceeded`.

**toStringBase:** build `expected` from the real `request_state.to_string_base()`
+ Java's `, remainingMs=.., heartbeatIntervalMs=..` suffix (robust to the
ExponentialBackoff Display); assert no "Optional"/"Some(" leak. Owner is the short
struct name "HeartbeatRequestState" (Java uses the FQCN, a Java-only concept).
