---
name: m11-tier2-phase2-notes
description: Milestone 11 Tier 2 Phase 2 (group offsets) — key decisions, the OffsetDelete surprise, and the disableBatch wiring
metadata:
  type: project
---

Milestone 11 Tier 2 Phase 2 = Admin group offsets (listConsumerGroupOffsets,
alterConsumerGroupOffsets, deleteConsumerGroupOffsets). Actor/Critic N=1.
COMPLETE 2026-07-29. Lib tests 2497 -> 2561 (+64). 4 integration tests pass vs
real broker. Commits: c0b214d (OffsetDelete wire), ce6dd8b (RPCs+handlers+wiring),
cb16905 (client-level tests + MockClient helper), 91fc6ad (integration).

BIGGEST FINDING — the plan/kickoff were WRONG about the delete wire shape.
`DeleteConsumerGroupOffsetsHandler` uses a DEDICATED `OffsetDelete` RPC
(ApiKeys.OFFSET_DELETE = 47, v0-only, flexibleVersions=none), NOT an
`OffsetCommit` with a -1 sentinel offset. Verified against Kafka 4.2
`DeleteConsumerGroupOffsetsHandler.java` (imports OffsetDeleteRequest/Response).
So OffsetDelete was NET-NEW wire work: added src/common/requests/offset_delete_
{request,response}.rs + wired ~11 ConcreteRequest/ConcreteResponse enum arms each
in abstract_{request,response}.rs. The generated data structs
(offset_delete_{request,response}_data) already existed (build.rs emits all 197
specs from generator/messages/). Byte-level known-vector tests: request serialize
(v0 non-flexible: group_id string, topics []{name, partitions []{i32}}), response
parse (error_code i16, throttle i32, topics []{...}). OffsetFetch/OffsetCommit
were genuinely REUSED (no duplication).

disableBatch() WIRING (the plan flagged this as easy-to-under-test):
- The `disable_batch` flag already existed on CoordinatorStrategy but was dead
  (#[allow(dead_code)]) — Phase 1 never wired the driver downgrade path.
- Moved it to a defaulted `AdminApiLookupStrategy::disable_batch(&self) {}` trait
  method (Rust's replacement for Java's `(CoordinatorStrategy) handler.lookupStrategy()`
  downcast); CoordinatorStrategy overrides it. Removed the inherent method (the
  CoordinatorStrategyTest unit tests call the trait method now, trait in scope).
- AdminApiDriver.on_failure: NEW branch BEFORE the generic UnsupportedVersion
  branch (Java's NoBatched* are UnsupportedVersionException subclasses):
  is_no_batched_support(err) -> disable_batch() + retry_lookup(keys ∩ lookup_keys).
- DETECTION is message-substring based (fragile but the only signal available):
  Rust flattens a build-time UnsupportedVersion into KafkaError::Generic carrying
  the builder's message (NetworkClient version-mismatch path loses Java's type).
  Match "does not support batching groups" (OffsetFetch) OR "FindCoordinator
  request because we require features" (FindCoordinator). Documented in the helper.

TESTING THE NO-BATCHING PATH through MockClient (which never calls build_version):
- Added MockClient::prepare_version_mismatch_response(message) — injects a
  ClientResponse with a CUSTOM version_mismatch string (the existing
  prepare_unsupported_version_response uses a generic "Api X with version N" msg
  that does NOT match the NoBatched markers). The two no-batching client tests
  inject the exact NoBatched builder messages.
- GOTCHA: post-disable per-group FindCoordinator responses MUST use the OLD
  single-coordinator form (Java's prepareOldFindCoordinatorResponse: top-level
  node_id/host/port, empty coordinators list -> synthesizes empty key -> strategy
  binds to the requesting scope's single key). Using the batched-form
  find_coordinator_resp (with key set) causes non-deterministic scope-crossing
  under HashMap iteration order -> flaky panic in CoordinatorStrategy.handle_response
  ("unexpected response type") ONLY in the full-suite run. Also make per-group
  OffsetFetch responses contain BOTH groups so response.group(id) never misses
  under crossing (handler extracts only its own key). Ran full lib suite 2x to
  confirm non-flaky.

RESULT-TYPE FIDELITY:
- ListConsumerGroupOffsetsResult value = HashMap<TopicPartition, Option<OffsetAndMetadata>>
  (None = Java's null "no committed offset"; negative committed_offset -> None).
  partitions_to_offset_and_metadata() (no-arg) -> Result (IllegalState if !=1 group);
  ..._for_group(id) -> Result (IllegalArgument if absent). all() = KafkaFuture::join_map.
- Alter/Delete value = HashMap<TopicPartition, Errors>. partition_result / all()
  built via then_apply_try on the public KafkaFuture (no eager whenComplete needed).
  DELETE partition_result is a SYNC throw (Result) if tp not in the request set
  (matches Java's synchronous IllegalArgumentException); ALTER partition_result is
  an ASYNC failure (returns KafkaFuture, fails inside if not-attempted). getSubLevelError
  -> sub_level_error helper (absent -> "not included in the response" IllegalArgument).
- Handler ApiResult "completed" for alter/delete INCLUDES per-partition errors
  (only coordinator-level errors -> unmap/retry); the Result surfaces them.

MOCK (§9): Java MockAdminClient IMPLEMENTS list (real committedOffsets filter,
single-group-only else "Not implemented yet") but THROWS for alter ("Not implement
yet" — Java's own typo, preserved) and delete ("Not implemented yet"). Added
committed_offsets field + update_consumer_group_offsets setter. None/empty spec
topic_partitions -> all committed (Java would NPE on null; we treat None as all to
avoid panic, documented).

Options are timeout-only except ListConsumerGroupOffsetsOptions (requireStable bool).
Handler build-request build tests use NORMALIZED (sorted) topic/partition set
comparisons because groupingBy is HashMap-order-nondeterministic in both Java and Rust.
