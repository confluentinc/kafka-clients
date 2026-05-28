---
name: phase8b-design-notes
description: Milestone-8 Phase 8b — concrete AbstractHeartbeat/AbstractMembership structs, §31 reconcile, Arc-shared inner state, RequestManagers wiring
metadata:
  type: project
---

Milestone-8 Phase 8b translated `AbstractHeartbeatRequestManager`,
`ConsumerHeartbeatRequestManager`, `AbstractMembershipManager`,
`ConsumerMembershipManager` plus tests. Key load-bearing decisions:

**Concrete-struct composition (not generic over response type)** —
Java's `AbstractHeartbeatRequestManager<R extends AbstractResponse>` and
`AbstractMembershipManager<R extends AbstractResponse>` are abstract
classes with three subclasses (Consumer, Share, StreamsGroup). Phase 8b
in scope = Consumer only per §20, so the Rust translation drops the
type parameter and hard-wires `ConsumerGroupHeartbeatResponse`. This is
Phase 7a `AbstractFetch` precedent applied at a larger scale.

**Shared state via `Arc<Mutex<MembershipInner>>`** —
`ConsumerHeartbeatRequestManager` and `ConsumerMembershipManager` both
need to mutate the membership state machine. Java solves this by
holding plain `&` references on the single bg thread. Rust uses
`Arc<Mutex<MembershipInner>>` so both managers can be in
`Vec<&mut dyn RequestManager>` produced by `RequestManagers::entries()`
at the same time. The `Mutex` is `std::sync::Mutex` (short critical
sections, never crossing `.await`) per §16. `MembershipInner` holds
the state machine fields (group_id, member_id, member_epoch, state,
current/target assignments, listeners).

**`ConsumerMembershipManager` not in `entries()`** —
`Arc<ConsumerMembershipManager>` can't produce `&mut dyn RequestManager`.
Skipping it is safe: Java's `AbstractMembershipManager.poll(...)` returns
`EMPTY` and only side-effects via `maybeReconcile(false)`. The Rust
translation moves reconcile driving out of the sync `entries()` loop;
the bg task (Phase 10) calls `Arc::clone(&membership).reconcile(now).await`
directly when state is `RECONCILING`. Heartbeat manager IS in `entries()`
because it produces real requests.

**§31 async reconcile pipeline** — `ConsumerMembershipManager::reconcile`
is `async fn`. The §31 contract is:
1. Create `oneshot::channel::<Result<(), KafkaError>>()`.
2. Enqueue `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`
   with the sender half via `BackgroundEventHandler::add(event, time_ms)`.
3. `.await` the receiver. The membership state machine does NOT advance
   until this resolves.

Key implementation detail: `MutexGuard`s on `MembershipInner` are scoped
to small blocks `let g = self.inner.lock()...; drop(g);` so they are
ALWAYS dropped before any `.await`. Tests pin this (3 dedicated §31
tests in `abstract_membership_manager.rs` + 1 reconcile-correctness
test in `consumer_membership_manager.rs`).

**Auto-commit-before-rebalance deferred** —
`commit_request_manager.maybe_auto_commit_sync_before_rebalance(...)`
does not exist on the Phase 9 `CommitRequestManager`. Per CLAUDE.md §5,
failing the operation is preferred over silently completing — but Java
itself logs and continues on commit failure, so we log a debug
("not wired yet (Phase 10)") and proceed. Phase 10 supplier wiring will
land this.

**`leave_group_epoch` dispatch** — `-1` for dynamic members and for
`LEAVE_GROUP` op; `-2` (static-leave) for static members with `DEFAULT`
op. Reused the Phase 8 partial wire-protocol constants
`LEAVE_GROUP_MEMBER_EPOCH`, `LEAVE_GROUP_STATIC_MEMBER_EPOCH`,
`JOIN_GROUP_MEMBER_EPOCH` from `consumer_group_heartbeat_request.rs`.

**HeartbeatErrorAction enum** — `classify_response_error` on
`AbstractHeartbeatRequestManager` returns:
- `Handled` — error fully handled at abstract layer
- `Fenced` — caller must mark fenced
- `Fatal(KafkaError)` — caller must mark fatal + propagate
- `DelegateToSpecific` — caller's subclass-specific handler runs
This splits Java's `onErrorResponse` into "what the abstract layer
knows" + "what the subclass injects".

**Tests deferred (Mockito-heavy, see DoD §3 rationale)**:
- All `mock(SubscriptionState.class)` based tests — require real
  `SubscriptionState` setup which is significant boilerplate.
- All `spy(membershipManager)` tests verifying invocation counts on
  `notifyEpochChange`, `markReconciliationInProgress`, etc. — require
  invocation-counting infra (Phase 11 trait abstractions).
- `mockPrepareLeavingStuckOnUserCallback` chains — require the full
  bidirectional event loop (Phase 10/11).
- `ConsumerRebalanceMetricsManager` interaction tests — no metrics
  framework.
- `ConsumerGroupHeartbeatResponse` parsing of full assignment with
  `topicId` resolution via metadata — would need ConsumerMetadata
  mock + topic-cache pre-population to drive `findResolvable...`.

13 tests on `ConsumerMembershipManager` (focused on state transitions,
listener notification, §31 reconcile correctness) + 6 tests on
`ConsumerHeartbeatRequestManager` + 9 tests on
`AbstractMembershipManager` + 9 tests on
`AbstractHeartbeatRequestManager` = 37 new tests. Baseline: 1376;
final: 1413 lib + 36 integration tests.
