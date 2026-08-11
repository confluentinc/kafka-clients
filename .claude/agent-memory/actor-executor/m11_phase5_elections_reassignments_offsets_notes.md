---
name: m11-phase5-elections-reassignments-offsets-notes
description: M11 Tier 1 Phase 5 (Admin electLeaders/alterPartitionReassignments/listPartitionReassignments/listOffsets) — COMPLETE
metadata:
  type: project
---

Milestone 11 (AdminClient) Tier 1 Phase 5 "Elections, reassignment, offsets" (final Tier 1
phase), Actor N=1. COMPLETE + green (build + `cargo test --lib` 2355 pass + format-check +
xtask lint clean + 3 real-broker integration tests). Rust core + tests ONLY (NO C FFI /
Python, same banner). Builds on [[m11-phase4-log-dirs-notes]] / [[m11-phase2-admin-driver-notes]].

**Dispatch paths (verified from Java 4.2 KafkaAdminClient source):**
- `electLeaders` / `alterPartitionReassignments` / `listPartitionReassignments` → plain
  `NodeProvider::Controller` `Call`s (Java uses ControllerNodeProvider; the `(true)` arg only
  matters with bootstrap.controllers which is unsupported here). NOT_CONTROLLER → existing
  `handle_not_controller_error(mm, error_counts)` → `HandleResult::Retry` (clears controller +
  requests metadata update); test drives it with error-resp → metadata-resp(controller=1) →
  success-resp + `time.sleep` per pump (retry-backoff gate).
- `listOffsets` → **canonical first AdminApiDriver user** via new `ListOffsetsHandler` +
  `PartitionLeaderStrategy::with_tolerate_unknown_topics(ctx, false)`. Wired exactly like
  deleteRecords (Phase 2): `new_future` → `invoke_driver(AdminApiDriver::new(...), driver_context(), now)`.

**REUSE (no duplication):** `ListOffsets` wire wrapper + `ListOffsetsRequestBuilder`
(`for_consumer_with_features`) already existed from the Consumer module — reused as-is. NO new
ListOffsets wire type. `OffsetSpec` did NOT exist → added as `src/admin/offset_spec.rs` (closed
enum: Earliest/Latest/MaxTimestamp/EarliestLocal/LatestTiered/EarliestPendingUpload/Timestamp(i64);
Java's nested subclasses collapsed since they carry no behavior beyond `getOffsetFromSpec`
timestamp mapping). `get_offset_from_spec` free fn in kafka_admin_client.rs maps to the wire
sentinels (EARLIEST=-2 LATEST=-1 MAX=-3 EARLIEST_LOCAL=-4 LATEST_TIERED=-5 EARLIEST_PENDING=-6).

**New wire wrappers (net-new, all flexible):** ElectLeaders (apiKey 43, v0-2, flex 0+),
AlterPartitionReassignments (45, v0-1, flex 0+), ListPartitionReassignments (46, v0, flex 0+).
Generated data structs already existed under target/.../out/generated. Wired ALL enum arms in
abstract_request.rs (version/api_key/to_send/serialize_with_header/serialize/get_error_response/
do_parse_request/Display) + abstract_response.rs (api_key/to_send/serialize_with_header/serialize/
error_counts/throttle_time_ms/maybe_set_throttle/should_client_throttle/parse/Display).
- ElectLeadersResponse: `from_results(throttle, code, results, version)` encodes error_code only
  for v1+ (Java constructor). `elect_leaders_result(data) -> HashMap<TP, Option<KafkaError>>`
  (None=success). ElectLeadersRequestBuilder::new(type, Option<Vec<TP>>, timeout): v0 rejects
  non-PREFERRED (UnsupportedVersion). AllowReplicationFactorChange is v1+ (NOT tagged).
- `should_client_throttle` = true for all three (Java). AlterPR error_counts aggregates top-level
  + per-partition; ListPR only top-level.

**New common type:** `common::election_type::ElectionType` (Preferred=0/Unclean=1; value/value_of/
values). value_of invalid → `KafkaError::illegal_argument`.

**POJOs/Results:** NewPartitionReassignment (empty-replicas → illegal_argument),
PartitionReassignment(replicas/adding/removing). ElectLeadersResult (partitions() single future
`HashMap<TP,Option<KafkaError>>`; all() via `then_apply_try` — first Some(err)→Err else Ok).
AlterPartitionReassignmentsResult (values()/all()=all_of). ListPartitionReassignmentsResult
(single future). ListOffsetsResult (partition_result → illegal_argument if not attempted; all()=
join_map) + ListOffsetsResultInfo(offset/timestamp/leader_epoch; UNKNOWN_EPOCH → None).

**Admin trait signatures (null-means-all faithful):** elect_leaders(ElectionType,
Option<HashSet<TP>>, opts); list_partition_reassignments(Option<HashSet<TP>>, opts);
alter_partition_reassignments(&HashMap<TP,Option<NewPartitionReassignment>>, opts);
list_offsets(&HashMap<TP,OffsetSpec>, opts). All plain `fn` (only close() async — §1/§11 spirit).

**alterPartitionReassignments handle_response subtlety:** assertResponseCountMatch — Java THROWS
UnknownServerException before completing individual futures → routes to handleFailure (fails ALL).
Rust can't throw from handle_response; mirror by completing ALL futures exceptionally + return
`HandleResult::Done` when `errors.values().all(is_none) && received != expected`. Grouping uses
BTreeMap<topic, BTreeMap<partition, Option<reassignment>>> (Java TreeMap order). Client-side
guards: unrepresentable name / partition<0 → InvalidTopicException (never sent).

**MockAdminClient (§9 — mirror Java's mock exactly):** alterPartitionReassignments +
listPartitionReassignments + listOffsets are REAL in Java's mock → translated faithfully. Added
State fields `reassignments: HashMap<TP,NewPartitionReassignment>`, `beginning_offsets`,
`end_offsets` + inherent `update_beginning_offsets`/`update_end_offsets`. `find_partition_reassignment`
free fn mirrors Java (adding=target minus current, removing=current minus target). electLeaders →
Java's mock THROWS UnsupportedOperationException (MockAdminClient.java:797) → returns a future
failed with `KafkaError::unsupported_version("Not implemented yet")` (§10.1 no panic). listOffsets
TimestampSpec → same unsupported per-partition (Java throws synchronously, not representable in the
`-> ListOffsetsResult` signature; documented deviation).

**Tests:** ListOffsetsHandlerTest (11, build_batched_request returns the concrete builder so
`oldest_allowed_version()` is inspectable — version matrix 1/2/7/8/9). KafkaAdminClientTest slices:
testElectLeaders (loop over both types), alterPartitionReassignments (too-few-responses /
partition+top-level errors / unrepresentable / NOT_CONTROLLER split into 4 tests), listPartitionReassignments,
listOffsets (happy/non-retriable/retriable-leader-change/MAX_TIMESTAMP-UV-single+multiple/partial-response)
+ 3 mock tests. **SKIPPED** the 3 testListOffsets*MinVersion slices (only assert oldestAllowedVersion,
already covered by build_request_allowed_versions + builder tests — documented in-code).

**Integration (`tests/integration/admin_elections_reassignments_offsets_test.rs`, 3 green on real
4.2.0):** list_offsets earliest=0/latest=N/MAX_TIMESTAMP(range-checked — records share ms timestamp,
broker returns earliest offset with max ts); elect_preferred (ELECTION_NOT_NEEDED on single broker);
alter+list reassignments on `ClusterConfig::with_brokers(3)` — move RF-1 partition, poll describeTopics
until replica set == target. `cargo xtask lint` does NOT compile integration tests; used
`cargo clippy --features integration-tests --test integration --fix` to clear my file's warnings
(remaining 13 are pre-existing in other admin integration files).

**NEXT:** Manager runs Critic N=1. Tier 1 is now COMPLETE. COMMENTS.1.md was a clean placeholder.
