# Phase 8: Membership & Heartbeat (KIP-848)

## Goal

Translate the KIP-848 membership and heartbeat machinery:

- `MemberState` enum + `MemberStateListener` trait — the per-member
  state machine (`UNSUBSCRIBED`, `JOINING`, `RECONCILING`, `STABLE`,
  `LEAVING`, `FENCED`, `UNJOINED`, `FATAL`).
- `Heartbeat` — per-member heartbeat bookkeeping (last sent, interval,
  exponential backoff on errors).
- `HeartbeatRequestState` — extends `RequestState` (Phase 6) with the
  heartbeat-specific request-in-flight + jitter logic.
- `AbstractHeartbeatRequestManager` — common heartbeat lifecycle shared
  with the Share-consumer's heartbeat manager (Share is out of scope per
  §20; only the abstract class methods we need land).
- `ConsumerHeartbeatRequestManager` — the KIP-848 `ConsumerGroupHeartbeat`
  request manager.
- `AbstractMembershipManager` + `ConsumerMembershipManager` — the
  `MembershipManager` interface implementation; drives the state
  machine in response to heartbeat responses, partition reconciliation,
  rebalance callback invocation handshakes (§31), and group-leave on
  close.

Plus the wire-prereq `ConsumerGroupHeartbeatRequest` / Response wrappers.

This is the **largest production phase in the milestone** at ~3361 LOC
Java production + ~4557 LOC tests. Slightly larger than Phase 7's
in-scope portion.

## Branch / worktree

Runs on a worktree of `consumer-impl` (HEAD: `7cdf8ad` — Phase 7 closed)
in PARALLEL with Phase 9 (Commit). Phase 8 and 9 are independent —
8 touches membership/heartbeat files; 9 touches commit files. Both
extend `RequestManagers` to populate their reserved slots, which is a
known merge surface but additive (`Option<>` fields on disjoint slots).

## Java sources

All paths relative to `kafka/clients/src/main/java/`.

### Wire-prereq wrappers (translate first)

- `org/apache/kafka/common/requests/ConsumerGroupHeartbeatRequest.java` (102)
- `org/apache/kafka/common/requests/ConsumerGroupHeartbeatResponse.java` (93)

Auto-generated `*Data` types already exist (verify with `find
target/debug/build -name "consumer_group_heartbeat_*_data.rs"`).

### Phase 8 production

- `org/apache/kafka/clients/consumer/internals/MemberState.java` (162)
- `org/apache/kafka/clients/consumer/internals/MemberStateListener.java` (50)
- `org/apache/kafka/clients/consumer/internals/Heartbeat.java` (144)
- `org/apache/kafka/clients/consumer/internals/HeartbeatRequestState.java` (113)
- `org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java` (524)
- `org/apache/kafka/clients/consumer/internals/ConsumerHeartbeatRequestManager.java` (346)
- `org/apache/kafka/clients/consumer/internals/AbstractMembershipManager.java` (1497)
- `org/apache/kafka/clients/consumer/internals/ConsumerMembershipManager.java` (525)

### Tests

- `clients/consumer/internals/HeartbeatTest.java` (124)
- `clients/consumer/internals/HeartbeatRequestStateTest.java` (136)
- `clients/consumer/internals/ConsumerHeartbeatRequestManagerTest.java` (1218)
- `clients/consumer/internals/ConsumerMembershipManagerTest.java` (3079)

## Out of scope (deferred / dropped)

- **`ShareMembershipManager`, `ShareHeartbeatRequestManager`,
  `StreamsMembershipManager`, `StreamsGroupHeartbeatRequestManager`**
  — Share + Streams out of scope per §20. The `AbstractMembershipManager`
  / `AbstractHeartbeatRequestManager` base classes may have hooks that
  appear unused without Share/Streams subclasses — translate the
  methods anyway if they're called from the consumer subclass path, drop
  the rest.
- **`MembershipManager` Java interface** — Java separates interface from
  impl for testing. The Rust translation collapses to the concrete
  `ConsumerMembershipManager` struct (mirrors Phase 7a's
  composition-over-inheritance precedent for `AbstractFetch`).
  `AbstractMembershipManager` becomes a `pub(crate) struct` composed by
  `ConsumerMembershipManager` (or merged in — Actor decides at
  implementation time).
- **`HeartbeatMetricsManager`** + metrics integration — no Rust metrics
  framework. Drop entirely.
- **`StreamsRebalanceData`** parameters — pass `None` in the consumer-only
  constructor.
- **`ClientTelemetryReporter`** — drop.
- **Tests that exercise Mockito-style mocks of `BackgroundEventHandler`,
  `Metrics`, etc.** — the Actor uses judgment per DoD §3; deferral
  rationale required.

## §31 rebalance-listener bidirectional handshake

`ConsumerMembershipManager` is the producer of the
`BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded` event
(Phase 5). Per `consumer-threading.md` §31:

- The bg task enqueues the event with a `oneshot::Sender<Result<(), KafkaError>>` ack.
- The bg task **awaits the matching `oneshot::Receiver`** before
  advancing the membership state.
- The app side, on its next `poll()`/`commit*()`/etc., drains the
  background-events channel, invokes the listener inline on the
  caller's task, and sends the result on the ack.

The Phase 8 `ConsumerMembershipManager::reconcile(...)` path is THE
critical site for this handshake. Get it right — the regression tests
required by §31 will be written with Phase 11 (`AsyncKafkaConsumer`)
but the contract MUST be in place now.

## Module structure produced by this phase

```
src/common/requests/
├── consumer_group_heartbeat_request.rs   # NEW
└── consumer_group_heartbeat_response.rs  # NEW

src/consumer/internals/
├── member_state.rs                       # NEW
├── member_state_listener.rs              # NEW (trait)
├── heartbeat.rs                          # NEW
├── heartbeat_request_state.rs            # NEW
├── abstract_heartbeat_request_manager.rs # NEW
├── consumer_heartbeat_request_manager.rs # NEW
├── abstract_membership_manager.rs        # NEW (concrete struct, not trait)
└── consumer_membership_manager.rs        # NEW
```

`src/consumer/internals/mod.rs` gets 7 new `pub(crate) mod` lines.
`src/common/requests/mod.rs` gets 2.
`src/consumer/internals/request_managers.rs` is extended to populate
two of its reserved Phase-6 slots: `consumer_heartbeat` and
`consumer_membership` (both `Option<...>`).

## Type-by-type spec

### `MemberState` (`src/consumer/internals/member_state.rs`)

`pub(crate)`. Java: `MemberState.java` — enum of 8 states.

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MemberState {
    Unsubscribed,
    Joining,
    Reconciling,
    Stable,
    Leaving,
    Fenced,
    Unjoined,
    Fatal,
}

impl MemberState {
    /// Per-state valid transitions. Mirrors Java's
    /// `Set<MemberState> validTransitions()` per-variant override.
    pub(crate) fn valid_transitions(&self) -> &'static [MemberState];

    /// Whether the state participates in heartbeat sending.
    pub(crate) fn should_send_heartbeat(&self) -> bool;

    /// Whether the state participates in heartbeat receiving.
    pub(crate) fn should_process_heartbeat_response(&self) -> bool;
}
```

Translation notes:
- Java models the per-variant `validTransitions` via per-enum-constant
  bodies. Rust collapses to a single `match` per method (mirrors Phase
  4's `FetchStates` precedent).

### `MemberStateListener` (`src/consumer/internals/member_state_listener.rs`)

`pub(crate) trait`. Sync (per-state-change, not per-record):

```rust
pub(crate) trait MemberStateListener: Send + Sync + 'static {
    fn on_member_epoch_updated(&self, member_epoch: Option<i32>, member_id: String);
}
```

Java is an interface; Rust is a trait. Implementations live on
`CommitRequestManager` (Phase 9) and the consumer's app-thread state
notifier. Phase 8 just defines the trait.

### `Heartbeat` (`src/consumer/internals/heartbeat.rs`)

`pub(crate)`. Java: 144 LOC. Tracks `lastHeartbeatSend`,
`heartbeatIntervalMs`, `sessionTimeoutMs`. The interesting method is
`maybeHeartbeat(now)` which returns the time until the next heartbeat
should fire. Pure state machine.

### `HeartbeatRequestState` (`src/consumer/internals/heartbeat_request_state.rs`)

`pub(crate)`. Composes `RequestState` (Phase 6) with `heartbeatTimer`.

### `AbstractHeartbeatRequestManager` (`src/consumer/internals/abstract_heartbeat_request_manager.rs`)

`pub(crate)` concrete struct (NOT trait — same precedent as Phase 7a's
`AbstractFetch`). The Java `extends AbstractHeartbeatRequestManager`
relationship becomes composition: `ConsumerHeartbeatRequestManager`
holds an `AbstractHeartbeatRequestManager` as a field.

Key methods:
- `poll(current_time_ms) -> PollResult`
- `handle_heartbeat_response(response, current_time_ms)` — feeds the
  response into the membership manager.
- `notify_heartbeat_succeeded(now)` / `notify_heartbeat_failed(now, error)`.

### `ConsumerHeartbeatRequestManager` (`src/consumer/internals/consumer_heartbeat_request_manager.rs`)

`pub(crate)`. Implements `RequestManager` (Phase 6) by composing
`AbstractHeartbeatRequestManager`. Holds an
`Arc<Mutex<ConsumerMembershipManager>>` to drive state transitions on
heartbeat responses.

### `AbstractMembershipManager` (`src/consumer/internals/abstract_membership_manager.rs`)

`pub(crate)` concrete struct. Java's 1497-LOC abstract class with the
core state machine, reconciliation queue, and rebalance-listener
handshake plumbing.

Methods (subset):
- `transition_to(new_state) -> Result<(), KafkaError>`
- `member_id() -> &str`, `member_epoch() -> i32`, `group_id() -> &str`
- `on_heartbeat_response_received(response)` — drive state machine.
- `reconcile(target_assignment)` — the BIG one. Computes added vs
  revoked partitions, enqueues
  `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded` events
  with oneshot acks, awaits them inline before advancing.

### `ConsumerMembershipManager` (`src/consumer/internals/consumer_membership_manager.rs`)

`pub(crate)`. KIP-848 specialization. Composes `AbstractMembershipManager`.

### Wire wrappers

`ConsumerGroupHeartbeatRequest` / `Response`: mirror the `MetadataRequest`
/ `FindCoordinator` / `Fetch` wrapper precedents. Keep MINIMAL — only
methods called by `ConsumerHeartbeatRequestManager`.

## Cross-cutting requirements

- **License header**: Apache 2.0 on every new file (CLAUDE.md §7).
- **No `#[async_trait]`** per DoD §11 — `RequestManager` is sync.
- **No `panic!` / `unimplemented!` / `todo!`** in production code.
- **No new dependencies**.
- **§16**: `SubscriptionState` access via `Arc<Mutex<...>>`. The
  membership manager updates assignment via `SubscriptionState`; mirror
  the §16 lock discipline.
- **§28 (event variants)**: `ConsumerMembershipManager::reconcile` is the
  CALLER of `BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded`
  — verify the variant shape from Phase 5 matches what 8 needs.
- **§31 (rebalance listener invocation)**: bidirectional oneshot
  handshake from bg task → app side → bg task. Phase 8 implements the
  bg-side enqueue + await. Phase 11 ships the app-side drain + invoke.
- **`oneshot::Sender` idempotent completion** — Phase 5 / Phase 6
  pattern.

## Verification

1. `cargo build` clean
2. `cargo test --lib` — must not regress 1304 baseline; new Phase-8
   tests pass.
3. `cargo test --test consumer` — 36 baseline holds (likely 7+
   integration cases when Phase 11 wires them; Phase 8 unit tests are
   all inline).
4. `cargo xtask format-check` clean
5. `cargo xtask lint` clean
6. `cargo test --lib -- --test-threads=1` no hangs (Phase 5-7 precedent).

## Commit plan

Suggested:

1. `Phase 8 (1/N): ConsumerGroupHeartbeatRequest + Response wrappers`
2. `Phase 8 (2/N): MemberState + MemberStateListener`
3. `Phase 8 (3/N): Heartbeat + HeartbeatRequestState + tests`
4. `Phase 8 (4/N): AbstractHeartbeatRequestManager`
5. `Phase 8 (5/N): ConsumerHeartbeatRequestManager + RequestManager impl + ConsumerHeartbeatRequestManagerTest subset`
6. `Phase 8 (6/N): AbstractMembershipManager (state machine + reconcile + §31 handshake)`
7. `Phase 8 (7/N): ConsumerMembershipManager + ConsumerMembershipManagerTest subset`
8. `Phase 8 (8/N): RequestManagers skeleton extended (consumer_heartbeat, consumer_membership slots)`

The two test files (`ConsumerHeartbeatRequestManagerTest` 1218 LOC, 88+
cases; `ConsumerMembershipManagerTest` 3079 LOC) are the biggest test
loads. Translate in priority order (state transitions, response
handling, error paths, edge cases). Mockito-heavy cases that need
MockClient or `BackgroundEventHandler` mocks defer to Phase 11 — each
deferred test gets a one-line rationale per DoD §3.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor implements per this plan on the 8 worktree.
2. Critic reviews, writes findings to
   `design/history/Milestone-8/Phase-8/COMMENTS.1.md`.
3. Actor fixes, moves resolved items to `COMMENTS.DONE.1.md`,
   `fixup!` commits.
4. Repeat until COMMENTS.1.md is empty.

After 8 closes (and 9 too), merge both worktrees back to
`consumer-impl`. The known merge surface is `RequestManagers` (each
phase populates a different `Option<>` slot) and `mod.rs` files.
