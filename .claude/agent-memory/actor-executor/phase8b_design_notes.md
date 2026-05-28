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

## Critic-round-1 fix patterns (COMMENTS.1.md, resolved on consumer-impl)

After Phase 8b landed, the critic returned 13 findings. Patterns
worth remembering:

**§31 listener-presence short-circuit is mandatory** — Java's
`invokeOnPartitions{Revoked,Assigned,Lost}Callback` all guard on
`subscriptions.rebalanceListener().isPresent()` before enqueueing.
Without this guard, the bg-task `.await ack_rx` hangs forever when no
listener is registered (the app side has nothing to invoke and no one
sends the ack). Fix: read `subscriptions.rebalance_listener().is_some()`
under a short lock at the top of `invoke_rebalance_callback`, return
`Ok(())` immediately if `None`.

**Propagate listener errors from reconcile, don't swallow** — Initial
Phase 8b returned `Ok(())` from reconcile after logging a listener
error. Java propagates via CompletableFuture's `whenComplete`. Rust
equivalent: `return Err(e)` after `mark_reconciliation_completed`.
Lets Phase 10 bg task decide log+continue vs escalate.

**`leave_group` translation: collapsed runCallbacks + signalMemberLeavingGroup** —
Java has `leaveGroup()` -> `leaveGroup(true)` and
`leaveGroupOnClose(op)` -> `leaveGroup(false)`. The runCallbacks=true
branch chains `signalMemberLeavingGroup()` (which dispatches between
onPartitionsRevoked and onPartitionsLost based on memberEpoch > 0).
Rust async version is a single `leave_group_inner(run_callbacks: bool)`
method that `.await`s `signal_member_leaving_group(now)` when
`run_callbacks=true`. Don't try to model the CompletableFuture chain
directly.

**`unsafe impl Send` is almost always wrong** — If you reach for
`unsafe impl Send for X {}`, the type is probably already auto-Send
via its fields and the impl is redundant. If it's NOT auto-Send,
there's a specific `!Send` field whose `Send`-ness needs to be
attested in the unsafe block's comment. Remove the impl; `cargo build`
tells you instantly if it was needed.

**`#[cfg(test)] use ...` for test-only imports** — better than a
`_force_used()` dummy function. If a downstream module uses an enum's
variant only in tests, gate the import to test builds.

**Hardcoding `current_time_ms = 0` is a smell** — when an event needs
a timestamp, take it as a parameter from the caller. The composing
`RequestManager::poll` already has `current_time_ms`; thread it
through to handlers that emit events.

**Test helpers: `set_X_for_test` (cfg(test)) over Mockito-style mocks** —
Adding `pub(crate) fn set_coordinator_for_test(&mut self, node: Node)`
gated on `#[cfg(test)]` is cleaner than constructing a FindCoordinator
round-trip in every test. Mirrors `when(mock.coordinator()).thenReturn(...)`.

**Test rationale docstrings satisfy DoD §3** — When deferring Java
tests, a category-based rationale on the test module docstring
(naming each Java test family and why it's deferred — Mockito
verification on internals, Phase 10 wiring, Streams/Share out of
scope, MockTime/metrics, etc.) is sufficient. The bar is "every
un-translated test has a one-line rationale," and grouping by
category is the natural way to satisfy that without writing 67
identical "Mockito-heavy" notes.

Final counts after fixup commits (2be008a, f353024, 9b057fe):
- ConsumerMembershipManagerTest: 13 -> 26 of 93
- ConsumerHeartbeatRequestManagerTest: 6 -> 12 of 31
- lib tests: 1413 -> 1433 (+20)
- consumer integration: 36 (unchanged)
