# Phase 7d: Offset fetch + Phase-4 deferred validation methods

## Goal

Translate the consumer's offset-discovery and offset-validation paths,
plus the deferred-from-Phase-4 `SubscriptionState` methods that depend
on `EpochEndOffset`:

- `OffsetsForLeaderEpochRequest` / `OffsetsForLeaderEpochResponse` wrappers
  (Java auto-generated `*Data` exists; the Builder + Response Java wrappers
  do not yet in Rust)
- `ListOffsetsRequest` / `ListOffsetsResponse` wrappers (same — `*Data` is
  generated)
- `OffsetsForLeaderEpochClient` (54 LOC) — small wrapper that sends
  `OffsetsForLeaderEpoch` requests
- `OffsetFetcherUtils` (422 LOC) — shared utility used by both
  `Fetcher` (out-of-scope) and `OffsetsRequestManager`
- `OffsetsRequestManager` (925 LOC) — the KIP-848 offset request manager.
  Translates `ListOffset` and `OffsetForLeaderEpoch` requests, processes
  responses, validates positions.
- **`SubscriptionState::maybe_validate_position_for_current_leader`** and
  **`maybe_complete_validation`** — deferred from Phase 4 because they
  depend on `EpochEndOffset` (only available now that
  `OffsetForLeaderEpochResponseData` is in scope). Plus the 4 deferred
  Phase 4 tests.

## Branch / worktree

Runs on a **worktree** of `consumer-impl` for parallel execution with
Phase 7b and 7c. Base SHA: `14f18b7` (Phase 7a closed).

**Coordination note**: 7d touches `src/consumer/internals/subscription_state.rs`
(adds the two deferred methods). 7b/7c do NOT touch this file. Merge
order doesn't matter — 7d's changes are additive.

## Java sources

Production:

- `org/apache/kafka/common/requests/ListOffsetsRequest.java` (204) — wrapper
- `org/apache/kafka/common/requests/ListOffsetsResponse.java` (115) — wrapper
- `org/apache/kafka/common/requests/OffsetsForLeaderEpochRequest.java` (127) — wrapper
- `org/apache/kafka/common/requests/OffsetsForLeaderEpochResponse.java` (83) — wrapper
- `org/apache/kafka/clients/consumer/internals/OffsetsForLeaderEpochClient.java` (54)
- `org/apache/kafka/clients/consumer/internals/OffsetFetcherUtils.java` (422)
- `org/apache/kafka/clients/consumer/internals/OffsetsRequestManager.java` (925)

Tests:

- `clients/consumer/internals/OffsetsRequestManagerTest.java` (1124) — inline or `tests/consumer/internals/offsets_request_manager_test.rs`
- Java has no `OffsetsForLeaderEpochClientTest` and no
  `OffsetFetcherUtilsTest`. Coverage comes via `OffsetsRequestManagerTest`.

Plus the 4-9 deferred Phase-4 `SubscriptionStateTest` cases:

- `testMaybeCompleteValidation`
- `testMaybeCompleteValidationAfterPositionChange`
- `testMaybeCompleteValidationAfterOffsetReset`
- `testMaybeValidatePositionForCurrentLeader`
- `testTruncationDetectionWithResetPolicy`
- `testTruncationDetectionWithoutResetPolicy`
- `testTruncationDetectionUnknownDivergentOffsetWithResetPolicy`
- `testTruncationDetectionUnknownDivergentOffsetWithoutResetPolicy`
- `resetOffsetNoValidation`

These are tagged with a one-line "deferred to Phase 7d" rationale in
Phase 4's `subscription_state.rs` test module — 7d removes the
deferral comments and translates the tests.

## Out of scope

- **`OffsetFetcher.java`** (440 LOC) — used only by `ClassicKafkaConsumer`.
  Out per §20.
- **`OffsetFetcherTest.java`** (1726 LOC) — same.
- **`OffsetFetcherUtilsTest.java`** — none in Java; coverage is implicit
  through `OffsetsRequestManagerTest`.
- **Metrics / Sensor / ClientTelemetry** — drop.

## Module structure produced by this phase

```
src/common/requests/
├── list_offsets_request.rs                # NEW
├── list_offsets_response.rs               # NEW
├── offsets_for_leader_epoch_request.rs    # NEW
└── offsets_for_leader_epoch_response.rs   # NEW

src/consumer/internals/
├── offsets_for_leader_epoch_client.rs     # NEW
├── offset_fetcher_utils.rs                # NEW
└── offsets_request_manager.rs             # NEW

src/consumer/internals/subscription_state.rs  # EDITED: 2 new methods
```

`src/common/requests/mod.rs` gets 4 new `pub mod` lines.
`src/consumer/internals/mod.rs` gets 3 new `pub(crate) mod` lines.

## Type-by-type spec

### Request/Response wrappers (`src/common/requests/`)

`pub`. Mirror the `MetadataRequest`/`Response` and `FindCoordinatorRequest`/`Response`
precedents. Each wraps the auto-generated `*Data` and adds:
- `Builder` (request side) — picks version based on `ApiVersions`.
- Helper accessors for the response side (per-topic / per-partition iteration).

Keep minimal — only methods called by `OffsetsRequestManager` and the
deferred `SubscriptionState` validation methods.

### `OffsetsForLeaderEpochClient` (`src/consumer/internals/offsets_for_leader_epoch_client.rs`)

`pub(crate)`. Java: 54 LOC — small wrapper that builds the
`OffsetsForLeaderEpoch` request from a `Map<TopicPartition, FetchPosition>`.

```rust
pub(crate) struct OffsetsForLeaderEpochClient {
    log_context: LogContext,
}

impl OffsetsForLeaderEpochClient {
    pub(crate) fn new() -> Self;
    pub(crate) fn prepare_request(
        node: &Node,
        partitions: &HashMap<TopicPartition, FetchPosition>,
    ) -> UnsentRequest;
    pub(crate) fn handle_response(
        response: ClientResponse,
        partitions: &HashMap<TopicPartition, FetchPosition>,
    ) -> HashMap<TopicPartition, EpochEndOffset>;
}
```

### `OffsetFetcherUtils` (`src/consumer/internals/offset_fetcher_utils.rs`)

`pub(crate)`. Java: 422 LOC of static helpers + 2 inner static classes
(`ListOffsetData`, `ListOffsetResult`).

```rust
pub(crate) struct ListOffsetData {
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub leader_epoch: Option<i32>,
}

pub(crate) struct ListOffsetResult {
    pub fetched_offsets: HashMap<TopicPartition, ListOffsetData>,
    pub partitions_to_retry: HashSet<TopicPartition>,
}

pub(crate) fn has_usable_offset_for_leader_epoch_version(
    node_api_versions: &NodeApiVersions,
) -> bool;

pub(crate) fn regroup_fetch_positions_by_leader(
    subscriptions: &SubscriptionState,
    partition_positions: &HashMap<TopicPartition, FetchPosition>,
) -> HashMap<Node, HashMap<TopicPartition, FetchPosition>>;

pub(crate) fn handle_list_offsets_response(
    response: &ListOffsetsResponse,
    require_timestamps: bool,
) -> ListOffsetResult;

// Plus other static helpers as needed.
```

### `OffsetsRequestManager` (`src/consumer/internals/offsets_request_manager.rs`)

`pub(crate)`. Java: 925 LOC. Implements `RequestManager`. Implements
`ClusterResourceListener` semantically (registers as a metadata-update
listener).

Methods (subset):
- `new(...)` — constructor.
- `fetch_offsets(timestamps_to_search, require_timestamps, deadline_ms) -> Receiver<HashMap<TopicPartition, OffsetAndTimestamp>>`
- `reset_offsets_if_needed(...)` — drives the offset-reset state machine.
- `validate_positions_if_needed(...)` — drives the OffsetForLeaderEpoch validation state machine.
- `commit_request_manager()` — getter used by tests.
- `on_update(cluster)` — `ClusterResourceListener` callback when
  metadata changes.

Plus `RequestManager` impl — `poll(current_time_ms)` returns the next
batch of `UnsentRequest`s.

Translation notes:
- Java's `AtomicReference<...>` for the latest metadata snapshot becomes
  Rust `Arc<ArcSwap<...>>` or `Mutex<...>` depending on access pattern.
- Java's `AtomicInteger` request counter → `AtomicI32`.
- Java's `ClusterResourceListener.onUpdate(ClusterResource)` translates to
  registering a callback with `Metadata::add_cluster_update_listener`
  (Phase 4 added the override hook).

### `SubscriptionState` deferred methods

Add to `src/consumer/internals/subscription_state.rs`:

```rust
impl SubscriptionState {
    /// Java: `maybeValidatePositionForCurrentLeader(ApiVersions, TopicPartition, LeaderAndEpoch)`
    pub(crate) fn maybe_validate_position_for_current_leader(
        &mut self,
        api_versions: &ApiVersions,
        tp: &TopicPartition,
        leader_and_epoch: &LeaderAndEpoch,
    ) -> bool;

    /// Java: `maybeCompleteValidation(TopicPartition, FetchPosition, EpochEndOffset)`
    pub(crate) fn maybe_complete_validation(
        &mut self,
        tp: &TopicPartition,
        request_position: &FetchPosition,
        epoch_end_offset: &EpochEndOffset,
    ) -> Option<LogTruncation>;
}
```

`EpochEndOffset` comes from `offset_for_leader_epoch_response_data` (the
auto-generated module). `LogTruncation` already exists in Phase 4.

Remove the Phase-4 "deferred to Phase 7d" comments on the 9 tests, and
translate each Java test case faithfully against `SubscriptionStateTest.java`.

## Cross-cutting requirements

- **License header**: Apache 2.0 (CLAUDE.md §7).
- **No `#[async_trait]`** per DoD §11. `RequestManager::poll` is sync.
- **No `panic!` / `unimplemented!` / `todo!`** in production code.
- **No new dependencies**.
- **`oneshot::Sender` idempotent completion** mirrors Phase 5's
  `CompletableEventHandle` pattern.

## Verification

1. `cargo build` clean
2. `cargo test --lib` — must not regress 1208 baseline; new tests pass
3. `cargo test --test consumer` — 36 baseline holds
4. `cargo xtask format-check` clean
5. `cargo xtask lint` clean
6. `cargo test --lib -- --test-threads=1` no hangs
7. **All 9 deferred-from-Phase-4 `SubscriptionStateTest` cases pass**

## Commit plan

Suggested:

1. `Phase 7d (1/N): ListOffsetsRequest + ListOffsetsResponse wrappers`
2. `Phase 7d (2/N): OffsetsForLeaderEpochRequest + Response wrappers`
3. `Phase 7d (3/N): OffsetsForLeaderEpochClient`
4. `Phase 7d (4/N): OffsetFetcherUtils + helpers`
5. `Phase 7d (5/N): SubscriptionState::maybe_validate_position_for_current_leader + maybe_complete_validation`
6. `Phase 7d (6/N): SubscriptionStateTest deferred cases (9 tests)`
7. `Phase 7d (7/N): OffsetsRequestManager + RequestManager impl`
8. `Phase 7d (8/N): OffsetsRequestManagerTest translation (subset of 1124 LOC)`

## Workflow

Actor → Critic loop on the 7d worktree. Comments at
`design/history/Milestone-8/Phase-7d/COMMENTS.1.md`. After close, merge
back to `consumer-impl`.

## After 7b/7c/7d all close

Merge each worktree branch into `consumer-impl`. Conflicts are
expected to be minimal because:
- 7b touches `fetch_collector.rs`, `fetch_request_manager.rs`.
- 7c touches `topic_metadata_request_manager.rs`.
- 7d touches 7 new request-side files + 3 new internals files + adds
  2 methods to `subscription_state.rs`.

The only shared file is `src/consumer/internals/mod.rs` (each adds
new `pub(crate) mod` lines) and `src/common/requests/mod.rs` (7d adds
new `pub mod` lines). These will trivially merge.

Phase 6's `RequestManagers` skeleton has reserved Option<> slots for
`commit`, `consumer_heartbeat`, `consumer_membership`, `offsets`,
`topic_metadata`, `fetch`. After 7b/c/d merge, three of those slots
(`offsets`, `topic_metadata`, `fetch`) become populated. Phase 8/9
fill the remaining three.
