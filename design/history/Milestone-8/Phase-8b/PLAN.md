# Phase 8b: Membership manager + heartbeat managers

## Goal

Complete the membership/heartbeat layer that Phase 8 partial deferred.
The Phase 8 partial Actor stopped at the foundation (`MemberState`,
`MemberStateListener` trait, `Heartbeat*RequestState`, wire wrappers)
and explicitly refused to ship a half-baked `AbstractMembershipManager`
under time pressure (cited CLAUDE.md §5). Phase 8b focuses on the
remaining ~2400 LOC + 4300 LOC of tests with the foundation already
proven by Phase 8 partial's Critic re-review.

Specifically:

- `AbstractHeartbeatRequestManager` (524 LOC) — shared heartbeat
  lifecycle.
- `ConsumerHeartbeatRequestManager` (346 LOC) — KIP-848 specialization,
  `RequestManager` impl.
- `AbstractMembershipManager` (1497 LOC) — **THE §31 critical site**:
  the bidirectional oneshot handshake for rebalance listener
  invocation lives in this class's `reconcile(...)` path.
- `ConsumerMembershipManager` (525 LOC) — composes
  `AbstractMembershipManager` (mirrors Phase 7a `AbstractFetch`
  precedent — concrete struct, not trait).
- `ConsumerHeartbeatRequestManagerTest` (1218 LOC).
- `ConsumerMembershipManagerTest` (3079 LOC) — biggest single test
  file in the milestone.
- `RequestManagers` skeleton: populate the `consumer_heartbeat` and
  `consumer_membership` reserved slots from Phase 6.

## Branch / worktree

Runs on a worktree of `consumer-impl` (HEAD: `8888bd4` — Phase 8
partial + Phase 9 merged). Not parallel with anything else.

## Java sources

All paths relative to `kafka/clients/src/main/java/`.

### Production

- `org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java` (524)
- `org/apache/kafka/clients/consumer/internals/ConsumerHeartbeatRequestManager.java` (346)
- `org/apache/kafka/clients/consumer/internals/AbstractMembershipManager.java` (1497)
- `org/apache/kafka/clients/consumer/internals/ConsumerMembershipManager.java` (525)

### Tests

- `clients/consumer/internals/ConsumerHeartbeatRequestManagerTest.java` (1218, ~88 cases)
- `clients/consumer/internals/ConsumerMembershipManagerTest.java` (3079, ~150+ cases)

## Out of scope (deferred / dropped)

- **Share/Streams subclasses** (`ShareHeartbeatRequestManager`,
  `ShareMembershipManager`, `StreamsMembershipManager`,
  `StreamsGroupHeartbeatRequestManager`) per `consumer-threading.md`
  §20. `AbstractHeartbeatRequestManager` / `AbstractMembershipManager`
  may have hooks that look unused without the Share/Streams paths —
  translate methods only if the consumer subclass calls them; drop
  the rest.
- **`HeartbeatMetricsManager` / `RebalanceMetricsManager`** — no Rust
  metrics framework. Drop entirely; constructor parameters become
  `()` placeholders or are removed.
- **`StreamsRebalanceData`** parameters — drop.
- **`ClientTelemetryReporter`** — drop.
- **`MembershipManager` Java interface** — Java separates interface
  from impl for testing. Rust collapses to the concrete struct (Phase
  7a / Phase 9 precedent). Tests use the concrete struct directly.
- **`testRevokedPartitionsAfterDisableAutoCommit` and other tests
  that need Mockito mocks of `BackgroundEventHandler`,
  `CommitRequestManager` internals, `Metrics`** — defer to Phase 11
  with one-line rationale per DoD §3.

## §31 — the critical contract

`ConsumerMembershipManager::reconcile(target_assignment)` is the
producer of `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`
events (Phase 5). Per `consumer-threading.md` §31:

1. The bg task computes added vs revoked partitions.
2. For each callback (`onPartitionsRevoked` / `onPartitionsAssigned` /
   `onPartitionsLost`), the bg task constructs a
   `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded` with a
   `oneshot::Sender<Result<(), KafkaError>>` ack.
3. The bg task **awaits the matching `oneshot::Receiver`** — the
   membership state machine does NOT advance until the listener
   completes.
4. App side, on its next `poll()`/`commit*()`/etc., drains the
   `background-events` channel, invokes the listener inline on its
   own task, sends the result on the `ack` half.

The Phase 8 partial Actor's deferral was specifically because they
didn't want to ship this incorrect. Phase 8b's Actor MUST get it right.

**Concrete API shape:**

```rust
impl ConsumerMembershipManager {
    /// Java: `reconcile(...)` (AbstractMembershipManager.java).
    /// Called from `on_heartbeat_response_received` when the response
    /// carries a new assignment.
    pub(crate) async fn reconcile(
        &mut self,
        target_assignment: HashMap<Uuid, Vec<i32>>,
    ) -> Result<(), KafkaError>;
}
```

`reconcile` is `async fn` because it awaits two oneshot acks per
rebalance (revoked + assigned). The state machine transitions
(`RECONCILING` → `ACKNOWLEDGING` → `STABLE`) happen between awaits;
the state must be preserved across each await point.

If the listener returns `Err`, the membership manager treats it as a
non-fatal listener failure per Java's semantics — logs and continues
(rebalance still advances). Verify the exact behavior against
`AbstractMembershipManager.java`.

## Module structure produced by this phase

```
src/consumer/internals/
├── abstract_heartbeat_request_manager.rs  # NEW
├── consumer_heartbeat_request_manager.rs  # NEW
├── abstract_membership_manager.rs         # NEW (concrete struct, ~1500 LOC)
└── consumer_membership_manager.rs         # NEW
```

`src/consumer/internals/mod.rs` gets 4 new `pub(crate) mod` lines.
`src/consumer/internals/request_managers.rs` populates the two
remaining reserved slots:

```rust
pub commit: Option<CommitRequestManager>,                   // landed in 9
pub consumer_heartbeat: Option<ConsumerHeartbeatRequestManager>, // 8b
pub consumer_membership: Option<ConsumerMembershipManager>,      // 8b
```

`entries()` returns the managers in deterministic order per
`consumer-threading.md` §10.

## Type-by-type spec

### `AbstractHeartbeatRequestManager` (`src/consumer/internals/abstract_heartbeat_request_manager.rs`)

`pub(crate)` concrete struct (NOT trait — Phase 7a precedent).
`ConsumerHeartbeatRequestManager` composes it as a field.

Java: `AbstractHeartbeatRequestManager.java:1-524`. Tracks the
heartbeat lifecycle, dispatches `ConsumerGroupHeartbeat` requests via
the coordinator, processes responses to feed the membership manager.

**Key methods:**

- `new(...)` — constructor.
- `poll(current_time_ms)` — produces a `PollResult` containing the
  next `ConsumerGroupHeartbeat` request when due.
- `notify_heartbeat_succeeded(now)` / `notify_heartbeat_failed(now, error)`.
- `handle_heartbeat_response(response, current_time_ms)` — drives the
  membership manager state transitions.
- `should_send_heartbeat(now)` — gated on member state +
  `HeartbeatRequestState::can_send_request`.

**Critical from Phase 8 partial Critic re-review:**

When the response carries a new heartbeat interval, this manager
calls:

```rust
self.heartbeat_request_state.update_heartbeat_interval_ms(
    current_time_ms,
    new_interval_ms,
);
```

(NOTE: signature already takes `current_time_ms` — Phase 8 partial
Critic Issue #2 fixed this.)

### `ConsumerHeartbeatRequestManager` (`src/consumer/internals/consumer_heartbeat_request_manager.rs`)

`pub(crate)`. Composes `AbstractHeartbeatRequestManager`. Implements
`RequestManager` (Phase 6).

Java: `ConsumerHeartbeatRequestManager.java:1-346`. The KIP-848
specialization — fills in the `ConsumerGroupHeartbeatRequestData`
fields (member ID, member epoch, group ID, server assignor, etc.)
from the membership manager's current state.

**Key:**

- `poll(current_time_ms) -> PollResult` per `RequestManager` trait
  (sync `fn`).
- `signal_close()` — per `MemberStateListener`'s semantics, the
  consumer-heartbeat manager is signaled to send a final "leave"
  heartbeat (member_epoch = -1 for member-leaving, -2 for static-leave).

Holds an `Arc<Mutex<ConsumerMembershipManager>>` so it can read the
current state and update on response.

### `AbstractMembershipManager` (`src/consumer/internals/abstract_membership_manager.rs`)

`pub(crate)` concrete struct (NOT trait). **The single largest class
in the milestone — 1497 LOC Java.**

Java models the state machine, target-assignment queue,
reconciliation pipeline, member-state listener wiring,
group-instance-id handling, and rebalance-listener invocation
handshake.

**Public API surface** (subset; full list in Java):

- `new(...)` — constructor.
- `member_id() -> &str`, `member_epoch() -> i32`, `group_id() -> &str`.
- `transition_to(new_state) -> Result<(), KafkaError>` — wraps
  `MemberState::previous_valid_states` check from Phase 8 partial.
- `state() -> MemberState`.
- `on_heartbeat_response_received(response)` — drive state machine.
- **`reconcile(target_assignment)` — async fn, the §31 critical
  site.**
- `transition_to_fenced()`, `transition_to_fatal()`,
  `transition_to_leaving()`.
- `register_state_listener(listener: Arc<dyn MemberStateListener>)`.
- `local_assignment()`, `current_assignment()`, `target_assignment()`.

Holds:

- `Arc<Mutex<SubscriptionState>>` — for assignment changes (§16
  discipline).
- `Arc<ConsumerMetadata>` — for topic-id resolution.
- `Arc<BackgroundEventHandler>` — for enqueueing
  `ConsumerRebalanceListenerCallbackNeeded` events (§31).
- `Vec<Arc<dyn MemberStateListener>>` — registered state listeners.

**Per-listener invocation pattern (§31):**

```rust
async fn invoke_rebalance_callback(
    &self,
    method: ConsumerRebalanceListenerMethodName,
    partitions: Vec<TopicPartition>,
) -> Result<(), KafkaError> {
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let event = BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded {
        method_name: method,
        partitions,
        ack: ack_tx,
    };
    self.background_event_handler.add(event, time_ms);
    // Await the app side's response — the state machine does NOT
    // advance until this resolves.
    match ack_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => {
            log::warn!("Rebalance listener returned error: {e}");
            // Java: non-fatal — log and continue. Verify against
            // AbstractMembershipManager.java's exception-handling
            // around invoke{Revoked,Assigned,Lost}Callbacks.
            Ok(())
        },
        Err(_recv_err) => {
            // App side dropped the receiver before responding —
            // treat as a fatal listener failure.
            Err(KafkaError::illegal_state(
                "Rebalance listener ack receiver dropped before completion",
            ))
        },
    }
}
```

The Phase 5 `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`
variant already carries the `ack: oneshot::Sender<Result<(), KafkaError>>`
field — Phase 8b just uses it.

### `ConsumerMembershipManager` (`src/consumer/internals/consumer_membership_manager.rs`)

`pub(crate)`. Composes `AbstractMembershipManager`. Java: 525 LOC.

KIP-848 specialization. Forwards most methods to
`AbstractMembershipManager`. Adds:

- Group-instance-id static-membership handling.
- Server-assignor field on the heartbeat request.
- `consumer_member_id()` / `consumer_member_epoch()` accessors used
  by `ConsumerHeartbeatRequestManager`.

## Cross-cutting requirements

- **License header**: Apache 2.0 on every new file (CLAUDE.md §7).
- **No `#[async_trait]`** per DoD §11. `RequestManager::poll` is sync.
  But `AbstractMembershipManager::reconcile` is `async fn` (it awaits
  oneshot acks per §31).
- **No `panic!` / `unimplemented!` / `todo!`** in production code.
- **No new dependencies**.
- **§16 lock discipline**: `SubscriptionState` via
  `Arc<Mutex<...>>`; NEVER hold the guard across `.await`. This is
  especially important in `reconcile` which awaits oneshot acks
  between state transitions. The guard MUST be dropped before any
  `.await`.
- **§28 event-variant rule**: `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`
  is the Phase-5 variant. Don't add new variants; use the existing
  shape.
- **§31 callback invocation**: the bg task ONLY enqueues; the app
  side drains and invokes. The bg task awaits the ack receiver.
  Phase 8b is the producer of these events.
- **`MemberStateListener` trait**: ships in Phase 8 partial. Phase 9
  documents a TODO for the `CommitRequestManager` impl; Phase 11
  wires that. Phase 8b's `AbstractMembershipManager` accepts a
  `Vec<Arc<dyn MemberStateListener>>` and calls `on_member_epoch_updated`
  on each registered listener on every epoch change.
- **`HeartbeatRequestState::update_heartbeat_interval_ms`** takes
  `current_time_ms` — Phase 8 partial Critic Issue #2 fix. Phase 8b's
  `AbstractHeartbeatRequestManager` callers must pass it.

## Verification

1. `cargo build` clean
2. `cargo test --lib` — 1376 baseline + new tests pass (target: +200
   to +400 tests depending on translation depth)
3. `cargo test --test consumer` — 36 baseline holds
4. `cargo xtask format-check` clean
5. `cargo xtask lint` clean
6. `cargo test --lib -- --test-threads=1` no hangs

## Commit plan

Suggested:

1. `Phase 8b (1/N): AbstractHeartbeatRequestManager (concrete struct)`
2. `Phase 8b (2/N): ConsumerHeartbeatRequestManager + RequestManager impl + ConsumerHeartbeatRequestManagerTest subset`
3. `Phase 8b (3/N): AbstractMembershipManager fields + state machine + transitions`
4. `Phase 8b (4/N): AbstractMembershipManager reconcile + §31 handshake`
5. `Phase 8b (5/N): ConsumerMembershipManager specialization`
6. `Phase 8b (6/N): ConsumerMembershipManagerTest subset (defer Mockito-heavy cases)`
7. `Phase 8b (7/N): RequestManagers skeleton extended (consumer_heartbeat, consumer_membership slots)`

The two test files (~4300 LOC combined) are the biggest test load
in the milestone. Translate in priority order: state transitions,
heartbeat-response routing, reconciliation correctness, error paths,
edge cases. Mockito-heavy cases that need `BackgroundEventHandler`
mocks or `CommitRequestManager` internals defer to Phase 11 with
one-line rationale per DoD §3.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor implements per this plan on the 8b worktree.
2. Critic reviews, writes findings to
   `design/history/Milestone-8/Phase-8b/COMMENTS.1.md`.
3. Actor fixes, moves resolved items to `COMMENTS.DONE.1.md`,
   `fixup!` commits.
4. Repeat until COMMENTS.1.md is empty.

After Phase 8b closes: Phase 10 (background task + event processor)
becomes the next milestone gate. Phase 10 will:
- Address Phase 9's `inflight_offset_fetches` memory leak.
- Relocate `init_with_committed_offsets_if_needed` to
  `OffsetsRequestManager`.
- Add `entries()` skip note for the commit slot.
- Land `CommitRequestManager`'s `MemberStateListener` impl (Phase 9
  deferred this).
- Wire `commit_sync` in-manager retry.
- Wire `try_connect` for Phase 7d Finding #6.
- Wire `OffsetsClusterListener::on_update` deferred-request replay
  for Phase 7d Finding #7.
