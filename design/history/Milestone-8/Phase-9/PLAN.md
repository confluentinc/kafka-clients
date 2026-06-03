# Phase 9: Commit

## Goal

Translate the consumer's commit path:

- `OffsetCommitCallbackInvoker` (80 LOC) — invokes
  `OffsetCommitCallback` (Phase 2 trait) on the app thread per §31's
  "callback runs on caller's task" rule.
- `CommitRequestManager` (1410 LOC) — implements `RequestManager`
  (Phase 6). Handles `OffsetCommit` and `OffsetFetch` request/response
  cycles, auto-commit timing, sync-commit-blocking, and the
  membership-manager state-listener wire-up.

Plus wire-prereq wrappers: `OffsetCommitRequest`/`Response`,
`OffsetFetchRequest`/`Response`.

Phase 9 also implements `MemberStateListener` (the trait Phase 8 ships)
on `CommitRequestManager` — the commit manager listens for membership
state transitions and clears pending commits on group-leave / fence.

## Branch / worktree

Runs on a worktree of `consumer-impl` (HEAD: `7cdf8ad`) in PARALLEL with
Phase 8 (Membership/Heartbeat). Phase 8 and 9 touch disjoint files; the
only merge surface is `RequestManagers` (each populates a different
reserved `Option<>` slot) and `mod.rs` line-appends.

## Java sources

### Wire-prereq wrappers (translate first)

- `org/apache/kafka/common/requests/OffsetCommitRequest.java` (147)
- `org/apache/kafka/common/requests/OffsetCommitResponse.java` (286)
- `org/apache/kafka/common/requests/OffsetFetchRequest.java` (332)
- `org/apache/kafka/common/requests/OffsetFetchResponse.java` (255)

Auto-generated `*Data` types already exist (`offset_commit_request_data`,
`offset_commit_response_data`, `offset_fetch_request_data`,
`offset_fetch_response_data`).

### Phase 9 production

- `org/apache/kafka/clients/consumer/internals/OffsetCommitCallbackInvoker.java` (80)
- `org/apache/kafka/clients/consumer/internals/CommitRequestManager.java` (1410)

### Tests

- `clients/consumer/internals/OffsetCommitCallbackInvokerTest.java` (143)
- `clients/consumer/internals/CommitRequestManagerTest.java` (1975)

## Out of scope (deferred / dropped)

- **`ShareCommitManager`** / Share-consumer commit path — Share is out
  of scope per §20.
- **`CommitRequestManager`'s Streams hooks** — drop the Streams-related
  branches.
- **`Metrics` / `Sensor` / `KafkaConsumerMetrics`** parameters — no
  Rust metrics framework.
- **`CommitRequestManagerTest` cases that exercise Mockito mocks of
  `BackgroundEventHandler`, `Metrics`, `MembershipManager` internals**
  — defer to Phase 11 with one-line rationale per DoD §3.
- **`OffsetCommitRequestState` / `OffsetFetchRequestState` retry queues
  that depend on `CommitRequestManager.handlePoll` Phase-10 wiring** —
  translate the state machines; Phase 10 wires `handlePoll` to the bg
  task.
- **`update_fetch_positions` path** in `OffsetsRequestManager` (Phase
  7d) that depends on `CommitRequestManager::initWithCommittedOffsetsIfNeeded`
  — Phase 9 lands the `initWithCommittedOffsetsIfNeeded` method, and
  Phase 10 wires the call. The 7d carry-over (#1: `fetch_offsets`
  deferral) lands here when Phase 9's commit manager is available.

## Module structure produced by this phase

```
src/common/requests/
├── offset_commit_request.rs     # NEW
├── offset_commit_response.rs    # NEW
├── offset_fetch_request.rs      # NEW
└── offset_fetch_response.rs     # NEW

src/consumer/internals/
├── offset_commit_callback_invoker.rs   # NEW
└── commit_request_manager.rs           # NEW
```

`src/common/requests/mod.rs` gets 4 new lines.
`src/consumer/internals/mod.rs` gets 2.
`src/consumer/internals/request_managers.rs` is extended to populate the
`commit` slot.

## Type-by-type spec

### Wire wrappers (`src/common/requests/`)

Mirror the precedents (Metadata, FindCoordinator, Fetch, ListOffsets,
OffsetsForLeaderEpoch). Keep MINIMAL — only methods
`CommitRequestManager` and `OffsetCommitCallbackInvoker` actually call.

The `OffsetCommitRequest::Builder` and `OffsetFetchRequest::Builder`
both pick versions based on `ApiVersions`. Translate the version-bumping
logic faithfully — KIP-848 protocol relies on `OffsetCommit` v8+ which
carries `groupInstanceId`, `memberId`, and `generationIdOrMemberEpoch`
on the wire.

### `OffsetCommitCallbackInvoker` (`src/consumer/internals/offset_commit_callback_invoker.rs`)

`pub(crate)`. Java: 80 LOC. Holds a `VecDeque<(Arc<dyn
OffsetCommitCallback>, HashMap<TopicPartition, OffsetAndMetadata>,
Option<KafkaError>)>` queue. The bg task enqueues; the app side drains
on `poll()`/`commit_*()`/etc. and invokes the callback inline.

```rust
pub(crate) struct OffsetCommitCallbackInvoker {
    pending: Mutex<VecDeque<PendingCallback>>,
}

struct PendingCallback {
    callback: Arc<dyn OffsetCommitCallback>,
    offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    error: Option<KafkaError>,
}

impl OffsetCommitCallbackInvoker {
    pub(crate) fn new() -> Self;
    pub(crate) fn enqueue_callback(
        &self,
        callback: Arc<dyn OffsetCommitCallback>,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<KafkaError>,
    );
    /// App-side drain: invoke each pending callback exactly once.
    /// Mirrors `consumer-threading.md` §31 — callback runs on the
    /// caller's task.
    pub(crate) async fn invoke_pending_callbacks(&self);
}
```

Notes:
- **`Mutex` not `tokio::Mutex`** — critical sections are short
  (push/pop) and never await. Locks are dropped before invoking the
  callback (§31).
- **`async fn invoke_pending_callbacks`** because callbacks
  (`OffsetCommitCallback::on_complete`) are `async` per Phase 2.
- **No `await` while lock held** — pop one at a time inside a tight
  scope, release lock, then await the callback.

### `CommitRequestManager` (`src/consumer/internals/commit_request_manager.rs`)

`pub(crate)`. Java: 1410 LOC. The biggest single production file in
Phase 9.

Key components:

```rust
pub(crate) struct CommitRequestManager {
    log_context: LogContext,
    time_ms: Arc<dyn Fn() -> i64 + Send + Sync>, // injected clock
    subscriptions: Arc<Mutex<SubscriptionState>>,
    coordinator_request_manager: Arc<Mutex<CoordinatorRequestManager>>,
    offset_commit_callback_invoker: Arc<OffsetCommitCallbackInvoker>,
    group_id: String,
    group_instance_id: Option<String>,
    member_state: Arc<AtomicI32>,  // current MemberState (Phase 8)
    member_id: Arc<RwLock<String>>,
    member_epoch: Arc<AtomicI32>,
    metadata: Arc<ConsumerMetadata>,
    config: AutoCommitState,
    pending_sync_requests: VecDeque<PendingCommitRequest>,
    pending_async_requests: VecDeque<PendingCommitRequest>,
    inflight_offset_fetches: HashMap<Uuid, PendingOffsetFetch>,
    // ... per Java
}

struct AutoCommitState {
    enabled: bool,
    interval_ms: i64,
    last_auto_commit_ms: i64,
}
```

Methods (subset):
- `new(...)` — large constructor; Phase 10 supplies.
- `commit_sync(offsets, deadline_ms) -> Receiver<Result<(), KafkaError>>`
- `commit_async(offsets, callback) -> ()`
- `fetch_committed_offsets(partitions, deadline_ms) -> Receiver<...>`
- `init_with_committed_offsets_if_needed(...)` — used by Phase 7d's
  `OffsetsRequestManager::update_fetch_positions`.
- `maybe_auto_commit(current_time_ms)` — auto-commit timer logic.
- `signal_close()` / `commit_on_close()`.

Plus `RequestManager` impl:
- `poll(current_time_ms) -> PollResult`
- `poll_on_close(current_time_ms) -> PollResult`
- `signal_close(&mut self)`
- `maximum_time_to_wait(current_time_ms) -> i64` — returns the time
  until the next auto-commit firing.

Plus `MemberStateListener` impl (Phase 8 trait):
- `on_member_epoch_updated(epoch, member_id)` — clears stale commits
  on `FENCED` / `LEAVING` transitions; updates internal `member_id` /
  `member_epoch`.

Translation notes:
- **Atomic member state**: Java reads `memberState` concurrently. Rust
  uses `Arc<AtomicI32>` with a `MemberState::from_i32` round-trip, OR
  `Arc<Mutex<MemberState>>` (cheaper if the read frequency is low —
  Phase 9 reads on each poll, so atomic is preferred).
- **`oneshot::Sender` idempotent completion** mirrors Phase 5/6
  patterns for the commit / fetch-offsets futures.
- **`CommitRequestManager::poll` is sync `fn`** — no `#[async_trait]`.
  Internal `tokio::sync` calls await on the BG task's executor.
- **Auto-commit state machine** — Java's `autoCommitState` is a single
  boolean + timer. Translate as `AutoCommitState` struct above.
- **`fetch_committed_offsets` returns a `Receiver`** — caller awaits
  it. The receiver completes when the response arrives, with the
  fetched offsets.
- **`init_with_committed_offsets_if_needed`** signature is the Phase-7d
  carry-over: callers will move from the `OffsetsRequestManager::update_fetch_positions`
  deferral. Document the integration point in this method's rustdoc.

## Cross-cutting requirements

- **License header**: Apache 2.0 (CLAUDE.md §7).
- **No `#[async_trait]`** per DoD §11.
- **No `panic!` / `unimplemented!` / `todo!`** in production code.
- **No new dependencies**.
- **§16**: lock discipline on `SubscriptionState`.
- **§31**: `OffsetCommitCallback` invocation runs on the app task via
  `OffsetCommitCallbackInvoker`. The bg task NEVER calls
  `callback.on_complete(...)` directly.
- **`oneshot::Sender` idempotent completion** — Phase 5 / 6 / 7 pattern.

## Verification

1. `cargo build` clean
2. `cargo test --lib` — must not regress 1304 baseline; new Phase-9
   tests pass.
3. `cargo test --test consumer` — 36 baseline holds.
4. `cargo xtask format-check` clean
5. `cargo xtask lint` clean
6. `cargo test --lib -- --test-threads=1` no hangs.

## Commit plan

Suggested:

1. `Phase 9 (1/N): OffsetCommitRequest + OffsetCommitResponse wrappers`
2. `Phase 9 (2/N): OffsetFetchRequest + OffsetFetchResponse wrappers`
3. `Phase 9 (3/N): OffsetCommitCallbackInvoker + tests`
4. `Phase 9 (4/N): CommitRequestManager fields + constructor + small accessors`
5. `Phase 9 (5/N): CommitRequestManager commit_sync / commit_async surface`
6. `Phase 9 (6/N): CommitRequestManager OffsetFetch + auto-commit + close paths`
7. `Phase 9 (7/N): CommitRequestManager RequestManager + MemberStateListener impls`
8. `Phase 9 (8/N): CommitRequestManagerTest subset (defer Mockito-heavy cases)`
9. `Phase 9 (9/N): RequestManagers skeleton extended (commit slot)`

The 1975 LOC test file is the biggest single test load in Phase 9.
Same priority order as Phase 8 — state transitions, response handling,
error paths, edge cases; defer purely-mockistic cases with rationale.

## Workflow

Per `.claude/rules/agent-roles.md`. Comments at
`design/history/Milestone-8/Phase-9/COMMENTS.1.md`. After close, merge
back to `consumer-impl` alongside Phase 8.
