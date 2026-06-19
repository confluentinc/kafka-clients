---
name: phase32-offset-query-test-notes
description: Phase 32 ORM fetch-path + offset-query (offsetsForTimes/beginning/end) test parity — multi-node helpers, requestUpdate(true/false) observability, parameterized error-matrix loops, Java NPE-hang divergence
metadata:
  type: project
---

# Phase 32 — ORM fetch-path + offset-query test parity

Test-only phase (Actor 32) closing the remaining non-reset/validate rows from
report-03. Built on Phase 31's helpers; did NOT disturb its tests. 18 new test
fns (16 in ORM, 2 in AKC) + 3 Phase-31 tests extended. All on `consumer-impl`,
not a worktree.

## metadata.requestUpdate(true|false) observability
`update_requested()` conflates `need_full_update` (requestUpdate) and
`need_partial_update` (request_update_for_new_topics, set by
`add_transient_topics` inside `fetch_offsets`). To assert the specific Java
`verify(metadata).requestUpdate(...)` contract, added
`Metadata::need_full_update_for_test()` (#[cfg(test)] pub(crate),
src/metadata.rs). Both `request_update(true)` AND `request_update(false)` set
`need_full_update` (the bool arg only controls equivalent_response_count
reset), so `need_full_update_for_test()` is the observable for BOTH; the
true/false distinction itself is not separately observable and Java's verify
is the only place it matters. `bootstrap_metadata_with_topic` (via
update_with_current_request_version) RESETS need_full_update to false → clean
baseline per loop iteration; use a fresh manager per error in matrix loops.

## Multi-node test helpers (new, in ORM test module)
- `bootstrap_metadata_with_nodes(metadata, topic, num_partitions, num_nodes)`:
  `metadata_update_with` assigns leader = nodes[partition_index % num_nodes].
  So num_nodes=2 → partition 1→node 1, partition 2→node 0 (distinct leaders,
  = Java LEADER_1/LEADER_2). num_nodes=1 → all partitions one leader → one
  batched request.
- `complete_all_unsent_with_per_partition_response(mgr, topic, &per_partition,
  now)`: drains EVERY unsent ListOffsets request from one poll, builds each
  request's `build()` to read its partition indices, completes each with a
  response containing ONLY its own partitions. The fetch-path handler routes on
  the request's `node_partitions`, not response keys, so per-node responses
  work. `request_builder_mut().build()` is non-destructive (borrows &mut,
  serializes); `handler()` clones an Arc; calling build() then on_complete() is
  fine (forwarder took the receiver at request-creation time).

## Parameterized @MethodSource → Rust loop (DoD §3)
- ORM `RETRIABLE_LIST_OFFSETS_ERRORS` const = Java's 10-error `retriableErrors()`.
- `is_retriable_list_offsets_error(error)` test helper mirrors
  `handle_list_offset_response` classification: retriable = NOT (None |
  UnsupportedForMessageFormat | TopicAuthorizationFailed). NOTE
  BrokerNotAvailable + InvalidRequest hit the `_ =>` arm → retriable
  (partitions_to_retry), even though they're not in the explicit match list.
- offsetsForTimes mixed-error matrix: 8-row struct loop mirroring Java's
  testGetOffsetsForTimesWithError calls; retry round only when a row has a
  retriable error.

## Java NPE-hang divergence (testRequestFailedResponse_NonRetriableErrorTimeout)
Java's `addPartitionsToRetry` does `toMap(tp, timestampsToSearch::get)`. For an
UNREQUESTED partition (response carries TEST_PARTITION_2, only _1 requested),
`get(tp2)` is null → Collectors.toMap NPE inside whenComplete → callback throws
before globalResult.complete → future stays pending → Java asserts
TimeoutException. Rust `add_partitions_to_retry` (offsets_request_manager.rs)
faithfully FILTERS to `timestamps_to_search.get(tp)` (skips unrequested, no
NPE) → global result resolves with `{tp1: None}`. Documented divergence (DoD
§7/§28): assert nothing pending to send/retry + requested partition surfaces no
offset, NOT "stays pending". This is the faithful translation of intent, not
the accidental NPE artifact.

## Duplicate-tp collapse: test at AKC level, not ORM
`beginning_offsets`/`end_offsets`/`offsets_for_times` dedup happens at the
slice→HashMap boundary in `beginning_or_end_offsets` (AKC). At ORM level the
input is already a deduplicated map. So duplicate-collapse tests
(testBeginning/EndOffsetsDuplicateTopicPartition) go at AKC level: drainer
captures the event's `timestamps_to_search` and asserts `.len() == 1`;
multi-partition (non-dup) tests go at ORM level via fetch_offsets.

## Isolation-level on wire
`req.isolation_level()` returns Result<IsolationLevel>; reach it via
`unsent.request_builder_mut().build()` → `ConcreteRequest::ListOffsets(req)`.
req.topics() returns &[ListOffsetsTopic]; topic.partitions is a PUBLIC FIELD
(Vec), p.timestamp / p.partition_index are public fields (no accessor methods).

## Documented skips (folded, not duplicated)
- pure-SubscriptionState paused tests (OUT_OF_SCOPE).
- reset/validate rows + testGetOffsetsIncludesLeaderEpoch + FencedLeaderEpoch:
  Phase 31.
- testGetOffsetsForTimesWhenSomeTopicPartitionLeaders{NotKnownInitially,
  DisconnectException}: same park→update→replay / disconnect→re-park code path
  as the retry tests; folded.

## See also
- [[phase31_reset_validate_test_notes]] — reset/validate helpers, response-driving
- [[phase7d_design_notes]] — ORM/OFU structure
