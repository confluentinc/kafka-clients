# Phase 32 — Test parity: OffsetsRequestManager fetch-path + offset-query (offsetsForTimes / beginning / end)

Actor 32. Test-only phase. Closes the remaining MISSING/REDUCED **non-reset/validate**
rows from `design/current/test-translation-review/03-commit-offsets-coordinator.md`
(Phase 31 closed the reset/validate/LogTruncation family). Production code is fully
implemented; this phase adds tests (and any test-only multi-node response builders
they need) only.

Builds on Phase 31's helpers in `offsets_request_manager.rs`; does NOT disturb
Phase 31's tests.

## Target Rust files (inline `#[cfg(test)]`)
- `src/consumer/internals/offsets_request_manager.rs` — primary; the KIP-848
  `fetchOffsets` path is where Java's `OffsetFetcher.offsetsForTimes` /
  `beginningOffsets` / `endOffsets` mechanics now live.
- `src/consumer/async_kafka_consumer.rs` — only the duplicate-topic-partition
  collapse tests (the `&[TopicPartition]` → `HashMap` dedup is an AKC-level
  concern; at the ORM level the input is already a deduplicated map).

## Reuse (no reinvention)
- `new_manager()`, `new_manager_with_commit()`, `bootstrap_metadata_with_topic`,
  `build_list_offsets_response`, `build_list_offsets_client_response`,
  `complete_first_unsent_with_response`, `await_fetch_result`, `yield_until`.
- New test-only helpers (multi-node only):
  - `bootstrap_metadata_with_nodes(metadata, topic, num_partitions, num_nodes)` —
    mirrors `bootstrap_metadata_with_topic` but with N brokers, so
    `partition % N` spreads partitions across distinct leaders (matching
    `metadata_update_with`'s leader-assignment used by the Java fixture's
    `LEADER_1`/`LEADER_2`).
  - `complete_all_unsent_with_per_partition_response(mgr, &per_partition, now)` —
    drains every unsent `ListOffsets` request and completes each with a response
    containing only the partitions that request carried (the response handler
    routes on the request's `node_partitions`, not on response keys). Needed for
    multi-node tests where one poll yields >1 unsent request.
- Production merge path (`apply_partial_result` / `partial_fetched_for_node`)
  exercised by the partial-failure test; no production change.

## Java → Rust mapping

### OffsetsRequestManagerTest (fetch path)
| Java test | Rust test | Notes |
|---|---|---|
| testListOffsetsRequestMultiplePartitions | `fetch_offsets_multiple_partitions_same_leader` | 2 partitions same leader → 1 request, both offsets |
| testListOffsetsWaitingForMetadataUpdate_Timeout | `fetch_offsets_unknown_leader_parks_on_retry` **(extended)** | add `requestUpdate(true)` assert + future-times-out (receiver pending after poll) |
| testListOffsetsWaitingForMetadataUpdate_RetrySucceeds | `fetch_offsets_metadata_update_retries_successfully` **(extended)** | add `requestUpdate(true)` assert at park time |
| testRequestFailsWithRetriableError_RetrySucceeds (×10) | `fetch_offsets_retriable_error_retries_after_metadata_update` **(rewritten as loop)** | loop over all 10 retriable errors + assert `requestUpdate(false)` after the error response |
| testRequestPartiallyFailsWithRetriableError_RetrySucceeds | `fetch_offsets_partial_retriable_error_merges_after_retry` | 2 brokers, partial success + retriable → `apply_partial_result` merge → retry succeeds; `requestUpdate(false)` |
| testRequestFailedResponse_NonRetriableErrorTimeout | `fetch_offsets_non_retriable_error_for_unrequested_partition` | error keyed on a partition NOT in the request. **Documented divergence:** Java's `addPartitionsToRetry` does `toMap(tp, timestampsToSearch::get)` → NPE for the unrequested tp (null value) → callback throws before `globalResult.complete`, leaving the future pending → Java asserts `TimeoutException`. Rust's `add_partitions_to_retry` faithfully filters to originally-requested partitions (no NPE), so the global result resolves with `{tp1: None}`. Assert: nothing pending to send/retry, requested partition surfaces no offset. Rationale in test rustdoc + DoD §7/§28. |

### OffsetFetcherTest (offsetsForTimes / beginning / end — KIP-848 logic in ORM)
| Java test | Rust test | Notes |
|---|---|---|
| testGetOffsetsForTimes | `offsets_for_times_multi_partition_mixed_errors` | parameterized over Java's mixed-error matrix; both-error / second-error / first-error / unknown-topic / unsupported(→None) / broker-not-available; success after metadata retry |
| testGetOffsetByTimeWithPartitionsRetryCouldTriggerMetadataUpdate (×7) | `offsets_for_times_retriable_retry_triggers_metadata_update` | loop over the 7-error list; partial (tp0 ok / tp1 retriable) → metadata update → tp1 succeeds against new leader; both offsets present |
| testGetOffsetsUnknownLeaderEpoch | `fetch_offsets_unknown_leader_epoch_is_retriable` | UNKNOWN_LEADER_EPOCH retriable → re-parked + `requestUpdate(false)` (fetch-path analogue; reset-path covered by Phase 31) |
| testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially | `fetch_offsets_build_time_partial_park_merges_after_metadata_update` **(added — Critic Issue 1)** | Drives the `Ok`-with-non-empty-`remaining_to_search` (build-time partial park) branch of `build_list_offsets_requests` (Java `OffsetsRequestManager.java:575-583`): 2 known-leader partitions build into requests while a 3rd (unknown-topic) partition parks in `remaining_to_search` and triggers `requestUpdate(true)`. Round-1 responses merge → re-park (`requestUpdate(false)`) → metadata refresh adds the missing topic → replay resolves the 3rd → all three offsets (11/32/54) merged into one result. Distinct path: the all-leaderless tests hit `Err(StaleMetadata)`; the partial-error tests build every partition on round 1. |
| testGetOffsetsForTimesWhenSomeTopicPartitionLeadersDisconnectException | `fetch_offsets_disconnect_fails_global_result_without_reparking` **(added — Critic Issue 3)** | Pins the in-scope ORM fetch-path behavior: a per-node disconnect routes to `fail_request_state` → fails the whole `fetch_offsets` future with `NetworkException`, NOTHING re-parked (Java `OffsetsRequestManager.java:586`/`:600` `globalResult.completeExceptionally`). The Java test's retry-and-succeed is a *classic* `OffsetFetcher`/`ConsumerNetworkClient` property (OUT_OF_SCOPE per §20), so the ORM does not reproduce it. This branch was previously covered only on the reset path (Phase 31), not the fetch path. |
| testBatchedListOffsetsMetadataErrors | `batched_list_offsets_metadata_errors_future_pending` | NOT_LEADER + UNKNOWN_TOPIC batched (1 request, 2 partitions same leader) → both retriable → re-parked, future stays pending (Java's TimeoutException) |
| testBeginningOffsetsMultipleTopicPartitions | `beginning_offsets_multiple_partitions` (ORM) | 3 partitions, EARLIEST_TIMESTAMP on wire, offsets 2/4/6 |
| testEndOffsetsMultipleTopicPartitions | `end_offsets_multiple_partitions` (ORM) | 3 partitions, LATEST_TIMESTAMP on wire, offsets 5/7/9 |
| testBeginningOffsetsDuplicateTopicPartition | `beginning_offsets_duplicate_topic_partition_collapses` (AKC) | `&[tp0, tp0]` → 1 result entry (dedup at slice→map boundary) |
| testEndOffsetsDuplicateTopicPartition | `end_offsets_duplicate_topic_partition_collapses` (AKC) | `&[tp0, tp0]` → 1 result entry |
| isolation-level-on-wire (beginning/end) | `fetch_offsets_request_carries_isolation_level_{read_uncommitted,read_committed}` | assert the built `ListOffsetsRequestBuilder` isolation level matches the manager's config (READ_UNCOMMITTED default + a READ_COMMITTED manager) |

## Documented skips (rationale)
- **Pure-SubscriptionState paused tests** (`testUpdateFetchPositionOfPausedPartitions*`,
  `testFetchingPendingPartitionsBeforeAndAfterSubscriptionReset`) — OUT_OF_SCOPE per
  report; `subscription_state.rs` owns and tests `isFetchable`/`markPendingRevocation`.
- **Reset/validate rows** (`testGetOffsetsFencedLeaderEpoch`, reset behavioral family,
  `testGetOffsetsIncludesLeaderEpoch`, OffsetValidation*) — already done in Phase 31
  (`reset_*`, `validation_*` tests + the `reset_request_includes_current_leader_epoch`
  / `reset_fenced_leader_epoch_still_needs_reset` tests). Not duplicated here.
- **testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially /
  ...DisconnectException** — NO LONGER SKIPPED (Critic Issues 1 & 3). Both are now
  translated as in-scope ORM behavior (see the OffsetFetcherTest table above):
  - The leaders-not-known-initially variant is `fetch_offsets_build_time_partial_park_merges_after_metadata_update`.
    The earlier rationale ("exercises no ORM code the retry tests don't") was WRONG:
    it drives the `Ok`-with-non-empty-`remaining_to_search` *build-time partial park*
    branch of `build_list_offsets_requests`, which neither the all-leaderless park
    tests (which hit `Err(StaleMetadata)`) nor the partial-response-error tests (which
    build every partition on round 1) exercise.
  - The disconnect variant is `fetch_offsets_disconnect_fails_global_result_without_reparking`.
    The earlier rationale ("disconnect→re-park, covered by Phase 31") was WRONG for the
    fetch path: the ORM fetch path does NOT re-park on a disconnect — it fails the global
    result via `fail_request_state` (Java `OffsetsRequestManager.java:586`/`:600`). The
    Java test's retry-and-succeed is a classic-`OffsetFetcher` property (OUT_OF_SCOPE);
    the in-scope ORM behavior (fail-the-future, no re-park) is what the new test pins.
- **testGetOffsetsForTimesTimeout / testListOffsetsWithZeroTimeout /
  testBeginningOffsets / testEndOffsets / testBeginningOffsetsEmpty /
  testEndOffsetsEmpty** — already REDUCED/PRESERVED at AKC level (existing
  `offsets_for_times_*`, `beginning_offsets_*`, `end_offsets_*` tests). Not
  re-translated.

## requestUpdate observability (Critic Issue 2 — true-vs-false now pinned)
`metadata.request_update(reset_equivalent_response_backoff: bool)` sets
`need_full_update = true` for BOTH arguments; the boolean ONLY controls whether the
`equivalent_response_count` backoff counter is reset to 0 (`true`) or left untouched
(`false`). So `need_full_update_for_test()` proves *a* full update was requested but
canNOT distinguish `requestUpdate(true)` from `requestUpdate(false)` — Java's mock
`verify(metadata).requestUpdate(true|false)` checks the exact boolean.

To faithfully pin the argument (chosen option (a): the distinction IS meaningful — it
governs real backoff behavior and Java verifies it exactly), `Metadata` gains two
`#[cfg(test)] pub(crate)` hooks: `equivalent_response_count_for_test()` (reads the
counter) and `set_equivalent_response_count_for_test(n)` (seeds it). Each
`requestUpdate`-asserting test now:
- seeds the counter to a known non-zero value (e.g. 3) before the path under test, then
- for a `requestUpdate(true)` path, asserts the counter was RESET to 0
  (`fetch_offsets_unknown_leader_parks_on_retry`,
  `fetch_offsets_metadata_update_retries_successfully`,
  `fetch_offsets_build_time_partial_park_*`); and
- for a `requestUpdate(false)` path, asserts the counter was NOT reset (stays at the
  seed) (`fetch_offsets_partial_retriable_error_merges_after_retry`,
  `fetch_offsets_retriable_error_retries_after_metadata_update` (×10),
  `fetch_offsets_unknown_leader_epoch_is_retriable`,
  `offsets_for_times_retriable_retry_triggers_metadata_update` (×7)).
This means a regression flipping `request_update(true)` ↔ `request_update(false)` is now
caught, matching Java's mock-verification strength.

## Minor / non-blocking notes (Critic observations)
- **single-leader vs two-leader batching in the mixed-error matrix**:
  `offsets_for_times_multi_partition_mixed_errors` deliberately places both partitions
  on ONE leader (`bootstrap_metadata_with_nodes(..., 3, 1)` → 1 batched request),
  whereas Java's `testGetOffsetsForTimesWithError` uses two distinct leaders. This is a
  fidelity *reduction*, not a coverage hole: the single-node variant still exercises the
  fetched+remaining merge, and the genuine multi-node (two-leader) merge is covered by
  `fetch_offsets_partial_retriable_error_merges_after_retry`,
  `offsets_for_times_retriable_retry_triggers_metadata_update`, and the new
  `fetch_offsets_build_time_partial_park_*`. Acknowledged.
- **destructive `build()` in `complete_all_unsent_with_per_partition_response`**: the
  helper rebuilds each request via `request_builder_mut().build()` to read back partition
  indices, then drives completion through the still-held `handler()`. The `build()` is
  destructive but no test rebuilds the same unsent twice — completion goes through the
  handler/receiver captured at request-creation time, not a re-built request.
  Acknowledged.

## DoD checks run after each commit
`cargo build`, `cargo test --lib` (ORM/AKC modules), `cargo xtask lint`,
`cargo xtask format-check`.
</content>
</invoke>
