---
name: phase12_5_critic_round3_patterns
description: Phase 12.5 Critic round 3 patterns — Java catch-all parity in classifier matches and Java vs Rust ErrorEvent emission split for Fenced vs Fatal heartbeat outcomes
metadata:
  type: feedback
---

Two reusable patterns from Phase 12.5 round 3 fixes (commits `842e046`, `4f84ecd`).

## Pattern A: Java `default:` arms in classifier switches MUST map to a concrete Rust action, not `unwrap_or(Handled)`

Lead rule: When translating a Java `switch` whose `default:` arm calls
`handleFatalFailure(error.exception(errorMessage))` (or any non-trivial
fallback), the Rust mapping MUST translate that fallback explicitly.
`.unwrap_or(HeartbeatErrorAction::Handled)` silently swallows codes
Java would fail on.

**Why:** A Java `switch` with a `default:` arm has THREE possible
outcomes: enumerated handled, enumerated delegated, and the catch-all.
If the Rust catch-all maps to a no-op variant (`Handled`), unknown /
future error codes silently disappear — the user never sees the
failure, and the contract diverges from Java fail-fast.

**How to apply:** When a Java classifier has both a delegation call
(`handleSpecificExceptionInResponse`) AND a `default:` fatal fallback
on the same boolean: Rust must translate the boolean-false branch
explicitly. Use `.unwrap_or_else(|| { log::error!(...);
HeartbeatErrorAction::Fatal(KafkaError::with_message(error, msg.clone())) })`.
The `log::error!` call mirrors Java's `logger.error("{} failed due to
unexpected error {}: {}", ...)` and is part of the contract — not
just the side-channel routing.

Example: `AbstractHeartbeatRequestManager.java:435-441` translated in
`consumer_heartbeat_request_manager.rs::on_response`.

## Pattern B: Java vs Rust ErrorEvent emission MUST distinguish Fenced (internal) vs Fatal (user-visible)

Lead rule: When translating heartbeat / membership error handling,
the Fenced arm (FENCED_MEMBER_EPOCH / UNKNOWN_MEMBER_ID) MUST NOT
emit `BackgroundEvent::Error`. Only `handleFatalFailure`-class outcomes
(Fatal, transport non-retriable, GroupAuthorizationFailed, InvalidRequest,
GroupMaxSizeReached, UnsupportedAssignor, InvalidRegularExpression,
UnsupportedVersion-via-specific-handler) emit `BackgroundEvent::Error`.

**Why:** Java treats fence as an internal state-machine event —
`AbstractHeartbeatRequestManager.java:411-427` calls ONLY
`membershipManager().transitionToFenced()` + `heartbeatRequestState.reset()`.
The user observes `ConsumerRecords::empty()` from `poll()` and rejoins
transparently. The Fatal path (`:455-458`) explicitly emits
`backgroundEventHandler.add(new ErrorEvent(error))` so `poll()`
surfaces the failure to the user. Emitting an ErrorEvent on the
Fenced path makes the consumer return `KafkaError::FencedMemberEpoch`
from `poll()`, where Java rejoins silently — observable user-facing
behavior divergence.

**How to apply:**
1. Cross-check every heartbeat/membership error arm against the Java
   ErrorEvent emission matrix. Specifically, look for
   `backgroundEventHandler.add(...)` in the Java arm.
2. The Fenced (FENCED_MEMBER_EPOCH / UNKNOWN_MEMBER_ID) and
   CoordinatorNotAvailable / NotCoordinator / CoordinatorLoadInProgress
   arms do NOT emit ErrorEvent. The TopicAuthorizationFailed arm emits
   ErrorEvent ONLY (no transition). All `handleFatalFailure` callers
   emit ErrorEvent AND transition to Fatal.
3. Regression test should assert BOTH the transition AND the
   ErrorEvent presence/absence. The Java contract is two-dimensional —
   if you only assert state transitions, you miss the ErrorEvent split.
4. When updating an existing test that asserted divergent behavior:
   add a citation to the Java source line in the assertion message,
   so future readers see "this is the Java contract" not "this is
   what we happened to do".

Example: `consumer_heartbeat_request_manager.rs::on_response` Fenced
arm (no ErrorEvent) vs Fatal arm (ErrorEvent), translated against
`AbstractHeartbeatRequestManager.java:411-427` (Fenced, no event) vs
`:455-458` (handleFatalFailure with event).
