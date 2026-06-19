# Phase 37 — FetchRequestManager round-trip test parity

Actor 37. Closes report-01 finding #1: there is **no MockClient-driven
FetchRequestManager behavioral harness** in Rust. The 91 `FetchRequestManagerTest`
tests drive full fetch round-trips; the integration-level behaviors are largely
untested anywhere. This phase builds the harness and translates the in-scope
behaviors, grouped 7a / 7b.

Authoritative worklist: `design/current/test-translation-review/01-fetch-path.md`.
Java contract: `kafka/clients/src/test/.../internals/FetchRequestManagerTest.java`
(Apache Kafka 4.2).

## Hard constraint (user)

Test-parity effort — TEST-ONLY changes preferred. Production change ONLY for a
genuine Java-fidelity bug, and it must be perf/CPU-neutral on the receive HOT
path (§27/§11). One such bug found — see "Production change" below.

## Harness design

Java's `sendFetches()` → `client.prepareResponse(...)` → `networkClientDelegate.poll(...)`
→ `fetcher.collectFetch()`. The Rust pipeline that the bg task drives is:

  `FetchRequestManager::poll(now)` builds `UnsentRequest`s from
  `AbstractFetch::prepare_fetch_requests` → the network sends them → on response
  the spawned forwarder routes a `PendingFetchCompletion` back → the next
  `poll(now)` drains it into `AbstractFetch::handle_fetch_success/_failure`
  (→ `FetchBuffer::add`) → `FetchCollector::collect_fetch` decodes.

The harness (`RoundTrip` fixture in `fetch_request_manager.rs` tests) collapses
that to the behaviorally-equivalent synchronous sequence the bg task performs,
without spinning a real socket:

  1. `prepare_fetch_requests(now)` — returns the per-node
     `(Node, FetchSessionRequestData)` map. The harness asserts wire fields by
     building the `FetchRequest` via `create_fetch_request(...).build()` and
     inspecting `req.data().topics[..].topic_id`, `.partitions[..]
     {fetch_offset, current_leader_epoch}`, `req.data().forgotten_topics_data`,
     `req.metadata().session_id()/epoch()`. (This is exactly what Java's
     `fetchRequestMatcher` asserts on the `MockClient`-captured request.)
  2. build a `FetchResponse` with the rich builder (records / hw / lso /
     log-start / preferred-replica / aborted-txn / per-partition error /
     current-leader / node-endpoints / session-id).
  3. `handle_fetch_success(node, request_data, response, version)` (or
     `handle_fetch_failure` for disconnect). This is the same `&mut AbstractFetch`
     call the `drain_pending_completions` step makes — i.e. exactly what
     `networkClientDelegate.poll` ends up invoking through the forwarder. The
     `MockClient` send leg is redundant for decode/position/leadership behavior
     (it only moves the already-owned `FetchResponse` across a channel); the
     existing `test_response_routing_{success,failure}_path` already cover the
     `MockClient` `UnsentRequest` handler dispatch, so the new tests drive
     `handle_*` directly to keep them fast and deterministic.
  4. `FetchCollector::collect_fetch(buffer)` → `ConsumerRecords`. Assert record
     count / offsets / values / headers / leader-epoch, and read
     `SubscriptionState::position` for position-advance assertions.

Shared builders (reused, lifted to a `#[cfg(test)] pub(crate)` helper module
`fetch_test_support` in `completed_fetch.rs` so both files use one copy):
`new_records`, `new_compressed_records`, `new_multi_batch_records`,
`new_records_with_keyed_offsets`, `batch_full`, `partition_data_with_aborted_txn`.
New rich `FetchResponse` builder `full_fetch_response(...)` + variants in the
`fetch_request_manager.rs` test module, mirroring Java's `fullFetchResponse` /
`fetchResponse` / `fullFetchResponseWithAbortedTransactions` /
`fetchResponseWithTopLevelError`.

The fixture seeds metadata (`metadata_update_with_ids` for topic-ids + leader
epoch), assigns + seeks partitions, and exposes `subscriptions` for assertions.

## Production change (Java-fidelity bug)

`AbstractFetch::handle_fetch_success` (abstract_fetch.rs) does NOT implement the
KIP-951 per-partition leadership-change branch that Java's
`AbstractFetch.handleFetchSuccess` carries at `AbstractFetch.java:205-250`:

  - For per-partition `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` with
    `currentLeader().leaderId() != -1 && leaderEpoch() != -1`, accumulate
    `partitionsWithUpdatedLeaderInfo`.
  - After the per-partition loop, build `leaderNodes` from
    `response.data().nodeEndpoints()` and call
    `metadata.updatePartitionLeadership(partitionsWithUpdatedLeaderInfo, leaderNodes)`,
    then `subscriptions.maybeValidatePositionForCurrentLeader(...)` for each
    updated partition.

Rust currently has none of this in `handle_fetch_success`; the leadership-error
partition's record just lands in the buffer and `FetchCollector` later requests a
metadata update, but the new-leader info from the response is dropped. The two
`testWhenFetchResponseReturnsALeaderShipChangeError{AndNewLeaderInformation,
ButNoNewLeaderInformation}` tests assert the new-leader path applies leader+epoch
to metadata and validates the position.

`metadata.update_partition_leadership` and
`subscriptions.maybe_validate_position_for_current_leader` already exist; the fix
is a faithful translation of the missing branch.

**Perf justification (§27/§11):** the new code runs only inside the per-partition
loop guarded by `partition_error == NOT_LEADER_OR_FOLLOWER || FENCED_LEADER_EPOCH`
(an error branch — never on the steady-state happy path) and the post-loop block
guarded by `!partitions_with_updated_leader_info.is_empty()`. On the hot path
(all partitions `NONE`) the only added cost is one `HashMap::is_empty()` check on
an empty map and one `i16` error-code compare per partition — no allocation, no
clone of record bytes, no extra lock. The `current_leader` field is already
decoded into the moved `PartitionData`; reading two `i32`s off it is free. This
matches Java's structure exactly (Java also only allocates the map lazily).

## 7a — fetch-session / topic-id / buffered-partition / leadership

Translated (Java name → Rust snake_case):

- `testFetchWithTopicId`, `testFetchWithNoTopicId` — topic-id present/absent on
  the FetchRequest wire + response decode.
- `testFetchForgetTopicIdWhenUnassigned`, `testFetchForgetTopicIdWhenReplaced` —
  incremental forget/replace lists.
- `testFetchTopicIdUpgradeDowngrade` — session topic-id upgrade then downgrade.
- `testConsumingViaIncrementalFetchRequests` — KIP-227 incremental session
  round-trips (full then incremental, session id/epoch progression).
- `testFetchSessionIdError` — `FETCH_SESSION_TOPIC_ID_ERROR` top-level handling.
- buffered-partition exclusion family: `testFetchRequestWithBufferedPartition`
  + `{NotAssigned, MissingLeader, MissingPosition, Paused, PendingAssignment,
  ResetOffset, Unfetchable}` — partition EXCLUDED from the next FetchRequest for
  each reason (mutation-resistant: assert the request omits it).
- `testFetchCompletedBeforeHandlerAdded` — response for a node with no session
  handler is ignored (no panic, no buffer entry).
- `testFetchSkipsBlackedOutNodes` — unavailable node skipped in build.
- `testEpochSetInFetchRequest`, `testSubscriptionPositionUpdatedWithEpoch` —
  leader epoch on outgoing request; position+epoch update from response.
- `testWhenFetchResponseReturnsALeaderShipChangeErrorButNoNewLeaderInformation`
  (param: FENCED_LEADER_EPOCH, NOT_LEADER_OR_FOLLOWER) — metadata unchanged,
  update requested, preferred-replica cleared for errored partition only.
- `testWhenFetchResponseReturnsALeaderShipChangeErrorAndNewLeaderInformation`
  (param) — new leader+epoch+node applied to metadata, position validated.

## 7b — data / transactions / preferred-replica / pause-seek

- `testHeaders` — record headers survive decode into `ConsumerRecord`.
- `testLeaderEpochInConsumerRecord`, `testMissingLeaderEpochInRecords` — batch
  leader epoch (present / `NO_PARTITION_LEADER_EPOCH`) into `ConsumerRecord`.
- `testFetchMaxPollRecords` — cross-batch maxPollRecords boundary.
- `testFetchNonContinuousRecords` — compacted offset-gap iteration.
- `testUpdatePositionOnEmptyBatch`, `testUpdatePositionWithLastRecordMissingFromBatch`.
- `testMultipleAbortMarkers`, `testReadCommittedAbortMarkerWithNoData`,
  `testReturnAbortedTransactionsInUncommittedMode`,
  `testReadCommittedWithCompactedTopic`,
  `testReadCommittedWithCommittedAndAbortedTransactions`,
  `testConsumerPositionUpdatedWhenSkippingAbortedTransactions`.
- `testFetchPositionAfterException` — position unchanged across deser-exception calls.
- `testFetchedRecordsAfterSeek`, `testSeekBeforeException`, `testStaleOutOfRangeError`.
- `testFetchDiscardedAfterPausedPartitionResumedAndSeekedToNewOffset`,
  `testInFlightFetchOnPausedPartition`,
  `testFetchOnCompletedFetchesForSomePausedPartitions`,
  `testPartialFetchWithPausedPartitions`.
- preferred-read-replica family: `testPreferredReadReplica`,
  `testFetchDisconnectedShouldClearPreferredReadReplica`,
  `testFetchDisconnectedShouldNotClearPreferredReadReplicaIfUnassigned`,
  `testFetchErrorShouldClearPreferredReadReplica`,
  `testPreferredReadReplicaOffsetError`.
- `testFetchDisconnected`, `testClearBufferedDataForTopicPartitions`,
  `testInflightFetchOnPendingPartitions`,
  `testFetchResultNotProcessedForPartitionsAwaitingCallbackCompletion`
  (+ Inflight variant).

## Documented skips (OUT_OF_SCOPE)

- Metrics: `testFetcherMetrics`, `testFetcherLeadMetric`, `testReadCommittedLagMetric`,
  `testQuotaMetrics`, `testFetchResponseMetrics*` (×4), `testFetcherMetricsTemplates`.
  No Rust metrics framework in Milestone-8 (report finding #2). SKIP.
- Classic rebalance: `testFetchDuringEagerRebalance`,
  `testFetchDuringCooperativeRebalance` — classic assignors out of scope (§20). SKIP.

## Folded (not duplicated)

Rows already PRESERVED-folded in `completed_fetch.rs` / `fetch_collector.rs`
(control records, corrupt message, basic skip-aborted, OOR, unauthorized) are NOT
re-translated. New manager round-trips are added only where Java asserts a
manager-level behavior the folded test doesn't cover (e.g. position advance through
`collect_fetch`, topic-id on the wire).

## Commit groups

1. `Phase 37: fetch round-trip harness` — shared builders + rich `FetchResponse`
   builder + `RoundTrip` fixture + the production KIP-951 fix.
2. `Phase 37a: fetch round-trip tests — session/topic-id/buffered/leadership`.
3. `Phase 37b: fetch round-trip tests — data/transactions/preferred-replica/pause-seek`.

Each commit: `cargo build`, `cargo test --lib`, `cargo xtask lint`,
`cargo xtask format-check` all green.
