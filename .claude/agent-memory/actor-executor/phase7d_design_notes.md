---
name: phase7d_design_notes
description: Design choices, translation patterns, and deferred-work rationale for Milestone-8 Phase-7d (OffsetsRequestManager + SubscriptionState validation methods)
metadata:
  type: project
---

Phase 7d translated the offset-discovery and offset-validation paths plus the Phase-4-deferred SubscriptionState methods. Key decisions and gotchas.

**Why:** Future Critic/Actor passes will revisit these files; the rationale below explains choices that aren't obvious from reading the code alone.

**How to apply:** When extending OffsetFetcherUtils, OffsetsRequestManager, or the SubscriptionState validation methods — or when reviewer feedback arrives — consult this memory first.

## Submodule init
The `kafka/` directory is a git submodule. After a fresh worktree checkout, run `git submodule update --init kafka` before referencing Java sources. The worktree branch in this task was based on `master` (442ecac), not consumer-impl — `git reset --hard 75649aa` set it to the correct base SHA.

## Per-instance state: OffsetFetcherUtils + PositionsValidator merged
Java has two separate types (OffsetFetcherUtils, PositionsValidator) that hold orthogonal state but only share one another through the OffsetsRequestManager. The Rust translation collapses them into `OffsetFetcherUtilsState` (one struct) because PositionsValidator carries no independent dispatch surface — splitting would force the OffsetsRequestManager to plumb both Arcs through every method.

## ClusterResourceListener registration
`OffsetsRequestManager` Java implements `ClusterResourceListener` directly. The Rust translation uses a separate stateless `OffsetsClusterListener` struct registered via `metadata.add_cluster_update_listener(Box::new(...))`. Rationale: the listener trait requires `Box<dyn ClusterResourceListener>` (owned, `Send`) but the manager owns a `&mut` borrow for its other methods — can't be both at once. The stateless handle is fine because the deferred-request replay queue (`requests_to_retry` in Java) lives on the manager, and its Java `onUpdate` path only fires when `fetch_offsets` has enqueued retries — that path is deferred.

## Pending completion channel
Java composes `CompletableFuture` chains; Rust uses `tokio::sync::mpsc::unbounded_channel<PendingCompletion>` plus a `tokio::spawn` forwarder per request. The manager drains the channel in `RequestManager::poll`. Rationale: `RequestManager::poll` is sync (no `#[async_trait]` per DoD §11), so we can't `.await` a `oneshot::Receiver` directly from poll. The forwarder task awaits the receiver and pipes the result into the mpsc; poll drains via `try_recv`.

## Deferred from Phase 7d (need CommitRequestManager)
- `fetch_offsets(timestamps_to_search, require_timestamps)` — needs commit-fetch wiring.
- `update_fetch_positions(deadline_ms)` — needs `initWithCommittedOffsetsIfNeeded`.
- `ListOffsetsRequestState` retry-on-metadata queue (`requests_to_retry`) — only useful with `fetch_offsets`.
- 22 of 27 OffsetsRequestManagerTest cases — all depend on `fetch_offsets` or Mockito-style mocks of SubscriptionState/Metadata/NetworkClientDelegate that Rust doesn't have.

## TopicPartitionState method translation pattern
`maybe_validate_position` and `update_position_leader_no_validation` are private (`fn`, not `pub(crate) fn`) on `TopicPartitionState`. Java has them private inside `TopicPartitionState` nested class; the Rust translation keeps them private to the module and dispatches via `SubscriptionState::assigned_state_or_null_mut`.

## EpochEndOffset / OffsetForLeader* are auto-generated
Path: `target/debug/build/.../out/generated/offset_for_leader_epoch_response_data.rs` (struct `EpochEndOffset`, fields `partition: i32`, `error_code: i16`, `leader_epoch: i32`, `end_offset: i64`).

## ConcreteRequest/Response wiring
Every new request type must add a variant to BOTH `ConcreteRequest` (in `abstract_request.rs`) AND `ConcreteResponse` (in `abstract_response.rs`), plus `Display`, `parse_request`/`parse`, `get_error_response`, `should_client_throttle`, `error_counts`, `throttle_time_ms`, `maybe_set_throttle_time_ms`, `to_send`, `serialize`, `serialize_with_header`. Easy to miss `should_client_throttle` (defaults to false for responses without a Java override).
