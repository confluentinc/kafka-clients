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
| testRequestFailedResponse_NonRetriableErrorTimeout | `fetch_offsets_non_retriable_error_for_unrequested_partition_stays_pending` | error keyed on a partition NOT in the request → future stays pending, no retry/send |

### OffsetFetcherTest (offsetsForTimes / beginning / end — KIP-848 logic in ORM)
| Java test | Rust test | Notes |
|---|---|---|
| testGetOffsetsForTimes | `offsets_for_times_multi_partition_mixed_errors` | parameterized over Java's mixed-error matrix; both-error / second-error / first-error / unknown-topic / unsupported(→None) / broker-not-available; success after metadata retry |
| testGetOffsetByTimeWithPartitionsRetryCouldTriggerMetadataUpdate (×7) | `offsets_for_times_retriable_retry_triggers_metadata_update` | loop over the 7-error list; partial (tp0 ok / tp1 retriable) → metadata update → tp1 succeeds against new leader; both offsets present |
| testGetOffsetsUnknownLeaderEpoch | `fetch_offsets_unknown_leader_epoch_is_retriable` | UNKNOWN_LEADER_EPOCH retriable → re-parked + `requestUpdate(false)` (fetch-path analogue; reset-path covered by Phase 31) |
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
  ...DisconnectException** — staged multi-topic metadata refreshes that resolve
  leaders over several rounds. The Rust `replay_retries_after_metadata_update` path
  is exercised by `offsets_for_times_retriable_retry_triggers_metadata_update` and
  `fetch_offsets_metadata_update_retries_successfully` (same code path: park →
  metadata update → replay → succeed). The leaders-not-known-initially variant adds
  a second-topic staged refresh that exercises no ORM code the retry tests don't;
  the disconnect variant is the disconnect→re-park path covered by Phase 31's
  `reset_disconnect_reparks_and_retries` (shared `pending_completion` failure
  branch). Folded, not duplicated.
- **testGetOffsetsForTimesTimeout / testListOffsetsWithZeroTimeout /
  testBeginningOffsets / testEndOffsets / testBeginningOffsetsEmpty /
  testEndOffsetsEmpty** — already REDUCED/PRESERVED at AKC level (existing
  `offsets_for_times_*`, `beginning_offsets_*`, `end_offsets_*` tests). Not
  re-translated.

## requestUpdate observability
`metadata.requestUpdate(true|false)` is observed via
`mgr.shared.metadata.metadata_arc().update_requested()` (true once `request_update`
is called with either arg). Snapshot before / after to assert the transition, as
Phase 31's reset tests do.

## DoD checks run after each commit
`cargo build`, `cargo test --lib` (ORM/AKC modules), `cargo xtask lint`,
`cargo xtask format-check`.
</content>
</invoke>
