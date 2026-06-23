---
name: phase8-design-notes
description: Milestone-8 Phase 8 (Membership & Heartbeat KIP-848) — scope analysis, what landed, what was deferred, and why.
metadata:
  type: project
---

# Phase 8 design notes

Phase 8 is the largest production phase in Milestone-8 at ~3361 LOC Java
production + ~4557 LOC tests. This memory documents what landed in the
worktree at base `9f79bc6` and what was deferred for follow-up rounds.

## Why these are worth recording

Future Actor passes should NOT re-attempt the deferred pieces blindly —
the deferrals stem from real design constraints, not skipped work.
Specifically the AbstractMembershipManager 1497-LOC class has a §31
bidirectional handshake that MUST be implemented correctly (it's the
critical contract for Phase 8) and would have been improvised
incorrectly under time pressure.

## What landed (commits)

1. `Phase 8 (1/N)`: `ConsumerGroupHeartbeatRequest` + Response wrappers,
   wired into `ConcreteRequest` / `ConcreteResponse`. +9 lib tests.

2. `Phase 8 (2/N)`: `MemberState` enum (10 variants — plan said 8, Java
   actually has 10 incl. ACKNOWLEDGING, PREPARE_LEAVING, STALE) and
   `MemberStateListener` trait. `previous_valid_states()` mirrors
   Java's static initializer block 1:1. +5 lib tests.

3. `Phase 8 (3/N)`: `HeartbeatRequestState` — composes Phase-6
   `RequestState` (not `extends`, per Phase 7a composition precedent)
   with an inline heartbeat-interval timer (`timer_expires_at_ms` /
   `timer_last_update_ms` pair). All 5 Java
   `HeartbeatRequestStateTest` cases translated and passing. +5 lib tests.

Total Phase-8 progress: +19 lib tests (1304 → 1323). 36 integration
tests held.

## What was DEFERRED and WHY

The remaining Phase-8 work is the bulk of the milestone:

- **`AbstractHeartbeatRequestManager` (524 LOC Java)** — concrete
  struct planned to compose the heartbeat lifecycle. Depends on
  `AbstractMembershipManager` for `shouldSkipHeartbeat`,
  `shouldHeartbeatNow`, `onHeartbeatRequestSkipped`,
  `onHeartbeatRequestGenerated`, `onHeartbeatSuccess`,
  `onHeartbeatFailure`, `transitionToFenced`, `transitionToFatal`,
  `transitionToSendingLeaveGroup`, `isLeavingGroup`, `memberId`,
  `memberEpoch`, `groupId`, `state`, `maybeRejoinStaleMember`.
  Without that class, can't faithfully translate.

- **`ConsumerHeartbeatRequestManager` (346 LOC Java)** — extends
  `AbstractHeartbeatRequestManager`. Holds the
  `ConsumerHeartbeatRequestManager.HeartbeatState` inner class that
  builds `ConsumerGroupHeartbeatRequestData`. Inner class depends on
  `SubscriptionState`, `AbstractMembershipManager`, `SubscriptionPattern`.

- **`AbstractMembershipManager` (1497 LOC Java)** — the giant. State
  machine driver, reconciliation queue, **§31 bidirectional
  oneshot handshake** (the CRITICAL contract for Phase 8). Depends on:
    - `Metadata` (we have `ConsumerMetadata`, may need refactor)
    - `CompletableFuture<Void>` patterns (need to translate to
      `oneshot::Receiver` / `JoinHandle`)
    - `LocalAssignment` inner class
    - `SubscriptionState` (we have this, with §16 lock discipline)
    - `RebalanceMetricsManager` (drop per plan)
    - `signalPartitionsLost`, `signalPartitionsRevoked`,
      `signalPartitionsAssigned` — these are the §31 callers that
      enqueue `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`
      with a `oneshot::Sender<Result<(), KafkaError>>` ack, then
      await the receiver before the state machine advances.

- **`ConsumerMembershipManager` (525 LOC Java)** — KIP-848
  specialization that composes `AbstractMembershipManager`.

- **Test files (`ConsumerHeartbeatRequestManagerTest` 1218 LOC,
  `ConsumerMembershipManagerTest` 3079 LOC)** — total 4297 LOC of
  tests. Many use Mockito mocks of `BackgroundEventHandler`,
  `Metrics`, `Metadata`. Each deferred case requires per-test
  rationale per DoD §3.

- **`RequestManagers` slot extension** — the plan specifies populating
  `consumer_heartbeat: Option<ConsumerHeartbeatRequestManager>` and
  `consumer_membership: Option<ConsumerMembershipManager>` fields.
  These cannot be added without the concrete types existing first;
  deferred to the same follow-up round that lands the managers.

## §31 rebalance-listener bidirectional handshake (the critical contract)

Per the plan: `ConsumerMembershipManager::reconcile(...)` is the producer
of `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`. The bg
task MUST enqueue the event with a `oneshot::Sender<Result<(), KafkaError>>`
ack AND await the matching `oneshot::Receiver` BEFORE advancing the
membership state. This is documented in `consumer-threading.md` §31 and
is THE critical correctness invariant for Phase 8.

The Phase 5 `BackgroundEvent` variant already carries the right ack
shape (`tokio::sync::oneshot::Sender<Result<(), KafkaError>>`). The
plumbing is ready; the missing piece is the membership manager that
uses it.

## Java translation pitfalls noted while reading

- `Heartbeat` (the classic-protocol class) is OUT OF SCOPE per §20 —
  only constructed by `AbstractCoordinator` which is classic-only.
  The plan listed it but it's not actually needed for KIP-848.
  Skipped without tests.

- `MemberState` has 10 variants not 8 — plan was wrong. The full set
  matters because `previous_valid_states()` per-variant must mirror
  Java's static initializer exactly, and `Reconciling`'s set is
  what `can_handle_new_assignment()` reads (must contain
  ACKNOWLEDGING).

- `previousValidStates` in Java is "incoming transitions" not
  "outgoing transitions" — naming is the opposite of what the plan
  called it ("validTransitions"). Translation: keep the Java
  semantics, name the Rust method `previous_valid_states()`.

- `HeartbeatRequestState` extends `RequestState` in Java; the Rust
  translation COMPOSES `RequestState` as a field per Phase 7a
  precedent. Override-style methods (`onFailedAttempt`,
  `canSendRequest`) become inherent methods that internally update
  the timer THEN delegate to the inner `request_state`.

## Files added in this phase

- `src/common/requests/consumer_group_heartbeat_request.rs` (NEW)
- `src/common/requests/consumer_group_heartbeat_response.rs` (NEW)
- `src/common/requests/abstract_request.rs` (variant added)
- `src/common/requests/abstract_response.rs` (variant added)
- `src/common/requests/mod.rs` (re-exports)
- `src/consumer/internals/member_state.rs` (NEW, 290 LOC)
- `src/consumer/internals/member_state_listener.rs` (NEW, 110 LOC)
- `src/consumer/internals/heartbeat_request_state.rs` (NEW, 240 LOC)
- `src/consumer/internals/mod.rs` (3 new pub(crate) mod lines)

## Recommendation for the next Actor pass

1. Translate `AbstractMembershipManager` FIRST (it's the dependency for
   everything else). Start with the constructor, state machine
   (`transition_to`), simple getters (`member_id`, `member_epoch`,
   `group_id`, `state`, `leave_group_operation`), and the partition-list
   manipulation helpers. Defer the reconcile path to a sub-commit.

2. Then `ConsumerMembershipManager` — composes the abstract one,
   adds KIP-848-specific bits.

3. Then `AbstractHeartbeatRequestManager` — now its membership-manager
   collaborator exists.

4. Then `ConsumerHeartbeatRequestManager` — composes abstract,
   carries the `HeartbeatState` inner builder.

5. Then `RequestManagers` extension to populate the two slots.

6. Translate `ConsumerHeartbeatRequestManagerTest` cases in priority
   order: state transitions, response handling, error paths, edge
   cases. Mockito-heavy cases (mocked BackgroundEventHandler /
   Metrics) defer to Phase 11.

7. Translate `ConsumerMembershipManagerTest` cases same way.

The §31 handshake MUST be implemented correctly — it's a behavioral
contract, not an optimization. The Phase 5 oneshot infrastructure
is already there; the membership manager must USE it.
