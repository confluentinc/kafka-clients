# Fetch-path test translation review (READ-ONLY)

Scope: how faithfully the Java Kafka consumer **fetch-path** tests were translated to Rust
(KIP-848 in-scope path). Java files reviewed:

- `FetcherTest.java` (81) — CLASSIC Fetcher, largely out of scope per §20
- `FetchRequestManagerTest.java` (91) — KIP-848 async fetch manager (the in-scope one)
- `FetchCollectorTest.java` (21)
- `FetchBufferTest.java` (5)
- `CompletedFetchTest.java` (6)
- `FetchConfigTest.java` (1)

Rust counterparts:
`abstract_fetch.rs`, `fetch_request_manager.rs`, `fetch_collector.rs`, `fetch_buffer.rs`,
`completed_fetch.rs`, `fetch_config.rs`, `fetch_utils.rs`, plus `tests/consumer/*.rs`.

---

## Key findings (read these first)

1. **No MockClient-driven FetchRequestManager behavioral harness exists in Rust.** The 91
   `FetchRequestManagerTest` tests drive *full fetch round-trips* through a `MockClient` and
   assert end-to-end behavior (record decode, position advance, leader-epoch, topic-id session
   negotiation, disconnect/preferred-replica clearing, leadership-change errors, incremental
   fetch sessions, etc.). Rust has **no equivalent integration harness**. The behavior is
   instead split across three lower-level test modules: `completed_fetch.rs` (decode /
   transactions / corruption), `fetch_collector.rs` (error handling / paused / OOR / preferred
   replica), and `abstract_fetch.rs` + `fetch_request_manager.rs` (request build / session /
   response routing — *unit* tests, not round-trips). Many `FetchRequestManagerTest` assertions
   are therefore covered *somewhere*, but a large set of integration-level behaviors are **not
   tested at any layer**. See the FetchRequestManagerTest table for the per-test verdict.

2. **Fetch metrics are entirely untranslated (by documented design).** There is no
   `FetchMetricsManager` / `FetchMetricsAggregator` / `FetchMetricsRegistry` in the Rust
   consumer (Phase 7a plan: "no Rust metrics framework in this milestone"; see
   `completed_fetch.rs:259-261`, `fetch_collector.rs:122-125`). This makes ~11
   `FetchRequestManagerTest` metric tests (`testFetcherMetrics`, `testFetcherLeadMetric`,
   `testReadCommittedLagMetric`, `testQuotaMetrics`, `testFetchResponseMetrics*` ×4,
   `testFetcherMetricsTemplates`, `testFetcherLeadMetric`) OUT_OF_SCOPE. **Material:** lead/lag
   are user-facing behavioral signals, not just internal counters — worth confirming the
   deferral is acceptable for Milestone-8 sign-off.

3. **6 `FetchCollectorTest` "OnNotAssignedPartition" mock tests are MISSING although the logic
   exists.** `FetchCollector::update_partition_state` (`fetch_collector.rs:588-624`) implements
   the exact `try_updating_highWatermark / logStartOffset / lastStableOffset /
   preferredReadReplica` short-circuit-on-`false` (partition-no-longer-assigned) branches, but
   the six Java tests that exercise each early-return branch
   (`testCollectFetchInitializationWithUpdate{HighWatermark,LogStartOffset,LastStableOffset,
   PreferredReplica}OnNotAssignedPartition`, `...WithNullPosition`,
   `...OffsetOutOfRangeErrorWith{NullPosition,OffsetReset}`) are **not translated**. The
   `try_updating_*` setters themselves are unit-tested in `subscription_state.rs:2736-2793`, but
   the *collector* branch that consumes a `false` return is untested. Real gap.

4. **`FetchCollectorTest.testReadCommittedWithAbortedTransaction` is MISSING at the collector
   layer.** The closest Rust coverage is `completed_fetch.rs::test_aborted_transaction_batch_
   skipped_mid_payload`, but the Java test specifically asserts the *FetchCollector* advances
   `nextOffsets` past an all-aborted batch (`new OffsetAndMetadata(recordCount + 1, ...)`) — the
   offset-advance-with-zero-records behavior through `collect_fetch` is not asserted in Rust.

5. **`FetchCollectorTest.testErrorInInitialize` (parameterized, 4 cases) is MISSING.** No Rust
   test verifies that an exception thrown during `CompletedFetch` initialization leaves a
   record-bearing CompletedFetch on the queue but removes an empty one. The
   `recordCount == 0 ? empty : not-empty` queue-state contract is untested.

6. **`CompletedFetchTest.testCorruptedMessage` is REDUCED.** Java asserts structured
   `RecordDeserializationException` fields: `origin`, `offset`, `topicPartition`, `timestamp`,
   and the raw `keyBuffer`/`valueBuffer` bytes plus `headers`. The Rust port
   (`completed_fetch.rs::test_corrupted_message_{key,value}_fails_after_valid_record`) collapses
   to `KafkaError::Serialization(String)` and only string-matches `"KEY"/"VALUE"`, the offset,
   and the partition. **Timestamp, key/value buffer bytes, and headers on the error are not
   asserted** — acceptable given the collapsed error type, but the buffer/headers/timestamp loss
   is a real reduction in the behavioral contract.

7. **`CompletedFetchTest.testAbortedTransactionRecordsRemoved` / `testCommittedTransaction
   RecordsIncluded` are CHANGED (covered by a different fixture).** Rust covers aborted-batch
   skipping via a *multi-batch mid-payload* fixture (`test_aborted_transaction_batch_skipped_
   mid_payload`) and control/commit handling via `test_control_batch_skipped_mid_payload`, but
   there is no direct port of the simple "ABORT marker → 0 records under READ_COMMITTED,
   numRecords under READ_UNCOMMITTED" / "COMMIT marker → all records under READ_COMMITTED"
   assertions. Behavior is equivalent; fixtures differ. Low risk but noted.

8. **Wire-level / byte-vector decoding is well covered on the in-scope path.** `completed_fetch.rs`
   has strong extra tests beyond Java: declared-record-count too-many/too-little
   (`InvalidRecordException` parity, `test_invalid_record_count_*`), control-batch and
   aborted-batch mid-payload skip, compressed (gzip) decode, multi-batch ordering. The §27
   zero-copy receive-path allocation budget is enforced (`fetch_collector.rs:1340`,
   `abstract_fetch.rs:1352`). These are net-positive additions.

---

## FetchConfigTest.java (1 test)

| Java test | Status | Notes |
|---|---|---|
| `testBasicFromConsumerConfig` (covers both `newFetchConfigFromConsumerConfig` + `newFetchConfigFromValues`) | PRESERVED | Split into `fetch_config.rs::test_basic_from_consumer_config` + `test_basic_from_explicit_values`, and *strengthened* with field-by-field assertions (Java only checks the constructor does not throw). Plus extra: `test_from_consumer_config_read_committed`, `test_from_consumer_config_rejects_unknown_isolation_level`, `test_display_matches_java_to_string`. |

---

## FetchBufferTest.java (5 tests)

| Java test | Status | Notes |
|---|---|---|
| `testBasicPeekAndPoll` | PRESERVED | `fetch_buffer.rs:359`. Rust replaces `assertSame(reference)` (object identity) with partition-equality on the popped value — acceptable given Rust ownership. peek/isEmpty/poll all covered. |
| `testCloseClearsData` | PRESERVED | `:372`. |
| `testBufferedPartitions` | PRESERVED | `:390`. Queue + next-in-line partition reporting fully covered. |
| `testAddAllAndRetainAll` | PRESERVED | `:418`. |
| `testWakeup` | PRESERVED | `:446` + bonus race/timeout/already-woken tests (`:467,:485,:536`). Thread→tokio task translation is faithful. |

No gaps. Net-positive (idempotent-close, predicate, and wakeup-race tests added).

---

## CompletedFetchTest.java (6 tests)

| Java test | Status | Notes |
|---|---|---|
| `testSimple` | PRESERVED | `completed_fetch.rs:1224`. Offset/count assertions match; plus `test_simple_compressed` (gzip) and `test_multi_batch_ordering_and_offsets`. |
| `testAbortedTransactionRecordsRemoved` | CHANGED | Covered by `test_aborted_transaction_batch_skipped_mid_payload` (READ_COMMITTED skip) but the simple "READ_UNCOMMITTED returns all numRecords for the same aborted batch" half is not directly asserted. Equivalent behavior, different fixture. |
| `testCommittedTransactionRecordsIncluded` | CHANGED | Covered indirectly by `test_control_batch_skipped_mid_payload` (COMMIT control record skipped, data returned). No direct "COMMIT marker under READ_COMMITTED → all 10 records" assertion. |
| `testNegativeFetchCount` | PRESERVED | `:1592`. |
| `testNoRecordsInFetch` | PRESERVED | `:1606`. |
| `testCorruptedMessage` | REDUCED | `:1669,:1705`. Collapsed error type drops assertions on `timestamp`, raw `keyBuffer`/`valueBuffer`, and `headers()`; keeps origin (KEY/VALUE), offset, partition. See Key finding #6. |

Extra Rust tests (no Java equivalent, net-positive): `test_invalid_record_count_too_many/too_little`,
`test_key_deserialization_failure_caches_error`, `test_drain_idempotent`,
`test_next_fetch_offset_advances`.

---

## FetchCollectorTest.java (21 tests)

| Java test | Status | Notes |
|---|---|---|
| `testFetchNormal` | PRESERVED | `fetch_collector.rs:917`. nextOffsets / position-update / next-in-line-survives-then-drains all covered. |
| `testFetchWithReadReplica` | PRESERVED | `:1201`. |
| `testNoResultsIfInitializing` | PRESERVED | `:959`. |
| `testErrorInInitialize` (parameterized ×4) | MISSING | No Rust test overrides `initialize()` to throw and asserts the `recordCount==0 ? queue-empty : queue-not-empty` contract. Key finding #5. |
| `testFetchingPausedPartitionsYieldsNoRecords` | PRESERVED | `:986`. next-in-line→queue re-enqueue on pause covered. |
| `testFetchWithMetadataRefreshErrors` (parameterized, 8 errors) | PRESERVED | `:1132` loops all 8 errors; asserts empty fetch + preferred-replica cleared + (implicitly) metadata update. Faithful. |
| `testFetchWithOffsetOutOfRange` | PRESERVED | Split into `test_fetch_with_offset_out_of_range_no_default_reset` (`:1017`, error raised, message asserted) + `..._with_default_reset` (`:1052`, silent reset). |
| `testFetchWithOffsetOutOfRangeWithPreferredReadReplica` | PRESERVED | `:1225`. preferred replica cleared. |
| `testFetchWithTopicAuthorizationFailed` | PRESERVED | `:1067`. Asserts `KafkaError::TopicAuthorization` + unauthorized topic set (stronger than Java's `assertThrows(class)`). |
| `testFetchWithUnknownLeaderEpoch` | PRESERVED | `:1086`. |
| `testFetchWithUnknownServerError` | PRESERVED | `:1101`. |
| `testFetchWithCorruptMessage` | PRESERVED | `:1116`. Asserts message "corrupt message" (Java only asserts `KafkaException`). |
| `testFetchWithOtherErrors` (parameterized, all-other Errors) | REDUCED | `:1174` samples only 3 representative errors (`InvalidFetchSize`, `LeaderNotAvailable`, `BrokerNotAvailable`) instead of iterating every `Errors` value as Java does. Stated rationale: avoid coupling to enum evolution. Acceptable but weaker. |
| `testCollectFetchInitializationWithNullPosition` | MISSING | Key finding #3. Mock-based; `positionOrNull→null` branch untested at collector layer. |
| `testCollectFetchInitializationWithUpdateHighWatermarkOnNotAssignedPartition` | MISSING | Key finding #3. `try_updating_high_watermark→false` collector branch untested. |
| `testCollectFetchInitializationWithUpdateLogStartOffsetOnNotAssignedPartition` | MISSING | Key finding #3. |
| `testCollectFetchInitializationWithUpdateLastStableOffsetOnNotAssignedPartition` | MISSING | Key finding #3. |
| `testCollectFetchInitializationWithUpdatePreferredReplicaOnNotAssignedPartition` | MISSING | Key finding #3. |
| `testCollectFetchInitializationOffsetOutOfRangeErrorWithNullPosition` | MISSING | OOR-with-null-position collector branch untested. |
| `testCollectFetchInitializationOffsetOutOfRangeErrorWithOffsetReset` | MISSING | Asserts `requestOffsetResetIfPartitionAssigned` is invoked — untested. |
| `testReadCommittedWithAbortedTransaction` | MISSING | Key finding #4. Offset-advance-past-all-aborted-batch through `collect_fetch` not asserted. |

Extra Rust tests: `test_update_partition_state_uses_time_source`, `test_node_import_smoke`,
`test_collect_fetch_per_record_allocation_budget` (§27).

FetchCollector subtotal: 12 PRESERVED, 2 REDUCED, 7 MISSING (8 counting the parameterized
`testErrorInInitialize` as one).

---

## FetchRequestManagerTest.java (91 tests) — the in-scope file

There is **no MockClient round-trip harness** in Rust. Verdicts below reflect whether the
asserted *behavior* is covered at some layer (CompletedFetch / FetchCollector / AbstractFetch /
FetchRequestManager unit tests) or not at all. "MISSING" means the integration-level behavior is
untested anywhere on the in-scope path; "OUT_OF_SCOPE" cites the documented reason.

| Java test | Status | Notes |
|---|---|---|
| `testFetchNormal` | REDUCED | Decode+position covered by `completed_fetch::test_simple` + `fetch_collector::test_fetch_normal`; the manager-level round-trip (createFetchRequests→send→handle→collect→position) is not exercised end-to-end. |
| `testInflightFetchOnPendingPartitions` | MISSING | No manager test asserts pending-partition fetch suppression via a real response. |
| `testInflightFetchResultNotProcessedForPartitionsAwaitingCallbackCompletion` | MISSING | Rebalance-callback gating of fetch results — untested. |
| `testFetchResultNotProcessedForPartitionsAwaitingCallbackCompletion` | MISSING | Same family. |
| `testCloseShouldBeIdempotent` | PRESERVED | `fetch_request_manager` / `abstract_fetch::test_close_is_idempotent`. |
| `testFetcherCloseClosesFetchSessionsInBroker` | REDUCED | `fetch_request_manager::test_response_routing_{success,failure}_path` + `abstract_fetch::test_prepare_close_fetch_session_requests_marks_each_session` cover the close-session request build + routing, but not the full MockClient broker exchange. |
| `testFetchingPendingPartitions` | MISSING | |
| `testFetchWithNoTopicId` | MISSING | topic-id absent fetch-session negotiation untested. |
| `testFetchWithTopicId` | MISSING | topic-id present path untested. |
| `testFetchForgetTopicIdWhenUnassigned` | MISSING | incremental-fetch forget-list untested. |
| `testFetchForgetTopicIdWhenReplaced` | MISSING | |
| `testFetchTopicIdUpgradeDowngrade` | MISSING | session topic-id upgrade/downgrade untested. |
| `testMissingLeaderEpochInRecords` | MISSING | leader-epoch-from-batch into ConsumerRecord untested at fetch layer (ConsumerRecord field itself tested in `tests/consumer/consumer_record_test.rs`). |
| `testLeaderEpochInConsumerRecord` | MISSING | Same: field-level set tested, fetch-path propagation not. |
| `testClearBufferedDataForTopicPartitions` | REDUCED | Covered indirectly by `fetch_buffer::test_add_all_and_retain_all` (retainAll). No manager-level clear test. |
| `testFetchSkipsBlackedOutNodes` | REDUCED | `abstract_fetch::test_prepare_fetch_requests_all_nodes_unavailable_returns_empty` covers the unavailable-node skip via closure; not the NetworkClient blackout. |
| `testFetcherIgnoresControlRecords` | PRESERVED (folded) | `completed_fetch::test_control_batch_skipped_mid_payload`. |
| `testFetchError` | REDUCED | Error→empty-fetch covered by `fetch_collector` error tests; not the manager round-trip. |
| `testFetchedRecordsRaisesOnSerializationErrors` | PRESERVED (folded) | `completed_fetch::test_corrupted_message_*` + `test_key_deserialization_failure_caches_error`. |
| `testParseCorruptedRecord` | PRESERVED (folded) | `completed_fetch::test_invalid_record_count_*` + corrupted-message tests cover premature-EOF / parse failure. |
| `testInvalidDefaultRecordBatch` | PRESERVED (folded) | `completed_fetch::test_invalid_record_count_too_many_through_fetch_records` (InvalidRecordException parity). |
| `testParseInvalidRecordBatch` | PRESERVED (folded) | Same as above. |
| `testHeaders` | MISSING | No fetch-path test asserts record headers survive decode into ConsumerRecord. (Headers tested elsewhere for ConsumerRecord construction, not via fetch decode.) |
| `testFetchMaxPollRecords` | REDUCED | max-poll-records honored is implicitly exercised by `completed_fetch::test_simple` (fetch_records max arg) and `fetch_collector::test_fetch_normal`; no dedicated cross-batch maxPollRecords boundary test through the manager. |
| `testFetchAfterPartitionWithFetchedRecordsIsUnassigned` | MISSING | |
| `testFetchNonContinuousRecords` | MISSING | offset-gap (compacted) iteration through fetch path untested. |
| `testFetchRequestInternalError` | REDUCED | Generic error path covered by `fetch_collector::test_fetch_with_other_errors`; not the manager. |
| `testUnauthorizedTopic` | PRESERVED (folded) | `fetch_collector::test_fetch_with_topic_authorization_failed`. |
| `testFetchDuringEagerRebalance` | OUT_OF_SCOPE | Classic-protocol eager rebalance (§20 — assignors/classic out of scope). |
| `testFetchDuringCooperativeRebalance` | OUT_OF_SCOPE | Cooperative assignor — §20 out of scope. |
| `testInFlightFetchOnPausedPartition` | MISSING | in-flight-then-paused result discard untested at manager layer. |
| `testFetchOnPausedPartition` | PRESERVED (folded) | `fetch_collector::test_fetching_paused_partitions_yields_no_records`. |
| `testFetchOnCompletedFetchesForPausedAndResumedPartitions` | REDUCED | Pause→no-records folded into FetchCollector paused test; resume-half not asserted. |
| `testFetchOnCompletedFetchesForSomePausedPartitions` | MISSING | multi-partition partial-pause untested. |
| `testFetchOnCompletedFetchesForAllPausedPartitions` | REDUCED | All-paused→empty folded into FetchCollector paused test. |
| `testPartialFetchWithPausedPartitions` | MISSING | |
| `testFetchDiscardedAfterPausedPartitionResumedAndSeekedToNewOffset` | MISSING | seek-invalidates-buffered-fetch untested. |
| `testFetchSessionIdError` | MISSING | fetch-session-id error handling untested. |
| `testHandleFetchResponseError` (parameterized) | REDUCED | Error classification folded into `fetch_collector::test_fetch_with_metadata_refresh_errors` / `test_fetch_with_other_errors`; not the response-level handler. |
| `testEpochSetInFetchRequest` | MISSING | leader-epoch set on outgoing FetchRequest untested. |
| `testFetchOffsetOutOfRange` | PRESERVED (folded) | `fetch_collector::test_fetch_with_offset_out_of_range_*`. |
| `testStaleOutOfRangeError` | MISSING | stale-OOR-after-seek suppression untested. |
| `testFetchedRecordsAfterSeek` | MISSING | seek then fetch-from-new-offset untested at manager layer. |
| `testFetchOffsetOutOfRangeException` | PRESERVED (folded) | `fetch_collector::test_fetch_with_offset_out_of_range_no_default_reset` (raises, message asserted). |
| `testFetchPositionAfterException` | MISSING | position-unchanged-after-deserialization-exception across calls untested at manager layer. |
| `testCompletedFetchRemoval` | REDUCED | Buffer poll/drain covered by `fetch_buffer` + `completed_fetch::test_drain_idempotent`; the multi-step removal scenario not reproduced. |
| `testSeekBeforeException` | MISSING | |
| `testFetchDisconnected` | REDUCED | Disconnect routing partially covered by `fetch_request_manager::test_response_routing_failure_path` (NetworkException); not the fetch-data-discard-on-disconnect assertion. |
| `testQuotaMetrics` | OUT_OF_SCOPE | Metrics deferred (Key finding #2). |
| `testFetcherMetrics` | OUT_OF_SCOPE | Metrics deferred. |
| `testFetcherLeadMetric` | OUT_OF_SCOPE | Metrics deferred. |
| `testReadCommittedLagMetric` | OUT_OF_SCOPE | Metrics deferred. |
| `testFetchResponseMetrics` | OUT_OF_SCOPE | Metrics deferred. |
| `testFetchResponseMetricsWithSkippedOffset` | OUT_OF_SCOPE | Metrics deferred. |
| `testFetchResponseMetricsWithOnePartitionError` | OUT_OF_SCOPE | Metrics deferred. |
| `testFetchResponseMetricsWithOnePartitionAtTheWrongOffset` | OUT_OF_SCOPE | Metrics deferred. |
| `testFetcherMetricsTemplates` | OUT_OF_SCOPE | Metrics deferred. |
| `testSkippingAbortedTransactions` | PRESERVED (folded) | `completed_fetch::test_aborted_transaction_batch_skipped_mid_payload`. |
| `testReturnCommittedTransactions` | PRESERVED (folded) | Covered by committed-data-returned in the mid-payload tests. |
| `testReadCommittedWithCommittedAndAbortedTransactions` | REDUCED | Mixed commit/abort partially folded into `test_aborted_transaction_batch_skipped_mid_payload`; the specific interleaving + offset assertions not reproduced. |
| `testMultipleAbortMarkers` | MISSING | multiple consecutive abort markers untested. |
| `testReadCommittedAbortMarkerWithNoData` | MISSING | abort-marker-with-no-data offset advance untested. |
| `testUpdatePositionWithLastRecordMissingFromBatch` | PARTIAL/REDUCED | `completed_fetch::test_next_fetch_offset_advances` covers nextOffset = batch.nextOffset() on exhaustion, but not the "last record missing from batch" gap case explicitly. |
| `testUpdatePositionOnEmptyBatch` | MISSING | empty-batch position advance untested. |
| `testReadCommittedWithCompactedTopic` | MISSING | compacted-topic offset-gap under READ_COMMITTED untested. |
| `testReturnAbortedTransactionsInUncommittedMode` | CHANGED | READ_UNCOMMITTED returns aborted records — covered conceptually by the control/uncommitted handling but no direct assertion that aborted records ARE returned under READ_UNCOMMITTED. |
| `testConsumerPositionUpdatedWhenSkippingAbortedTransactions` | REDUCED | Offset-advance-past-aborted folded into the mid-payload aborted test (offsets reported), but consumer *position* update (vs nextOffsets) not asserted. |
| `testConsumingViaIncrementalFetchRequests` | MISSING | incremental (KIP-227) fetch-session round-trips untested. Core KIP-848 path — notable gap. |
| `testEmptyControlBatch` | PRESERVED (folded) | `completed_fetch::test_control_batch_skipped_mid_payload`. |
| `testSubscriptionPositionUpdatedWithEpoch` | MISSING | position+epoch update from fetch untested. |
| `testPreferredReadReplica` | REDUCED | preferred-replica set/honored partially via `fetch_collector::test_fetch_with_read_replica`; the lease/expiry round-trip from a fetch response not fully reproduced. |
| `testFetchDisconnectedShouldClearPreferredReadReplica` | MISSING | disconnect-clears-preferred-replica untested. |
| `testFetchDisconnectedShouldNotClearPreferredReadReplicaIfUnassigned` | MISSING | |
| `testFetchErrorShouldClearPreferredReadReplica` | REDUCED | `fetch_collector::test_fetch_with_metadata_refresh_errors` asserts preferred replica cleared on those errors — overlaps but not the disconnect/generic-error case. |
| `testPreferredReadReplicaOffsetError` | MISSING | |
| `testFetchCompletedBeforeHandlerAdded` | MISSING | race: response arrives before handler registered — untested. |
| `testCorruptMessageError` | PRESERVED (folded) | `fetch_collector::test_fetch_with_corrupt_message` + `completed_fetch` corrupt tests. |
| `testWhenFetchResponseReturnsALeaderShipChangeErrorButNoNewLeaderInformation` (parameterized) | MISSING | KIP-951 leadership-change handling untested. |
| `testWhenFetchResponseReturnsALeaderShipChangeErrorAndNewLeaderInformation` (parameterized) | MISSING | KIP-951 new-leader-info handling untested. |
| `testPollWithoutCreateFetchRequests` | PRESERVED | `fetch_request_manager::test_poll_no_pending_returns_empty`. |
| `testPollWithCreateFetchRequests` | PRESERVED | `fetch_request_manager::test_poll_empty_partitions_completes_ack` / `test_enqueue_ack_completes_on_poll`. |
| `testPollWithCreateFetchRequestsError` | REDUCED | `test_drop_completes_pending_acks_with_error` covers ack-fails-on-drop; the in-poll error completion path partially. |
| `testPollWithRedundantCreateFetchRequests` | PRESERVED | `fetch_request_manager::test_create_fetch_requests_completes_all_pending_together` (single-slot semantics). |
| `testFetchRequestWithBufferedPartitions` | MISSING | buffered-partition fetch-request exclusion untested at manager layer. |
| `testFetchRequestWithBufferedPartitionNotAssigned` | MISSING | |
| `testFetchRequestWithBufferedPartitionMissingLeader` | MISSING | |
| `testFetchRequestWithBufferedPartitionMissingPosition` | MISSING | |
| `testFetchRequestWithBufferedPartitionPaused` | REDUCED | Pause exclusion folded into FetchCollector paused test; the "exclude from next fetch request" build-side assertion not made. |
| `testFetchRequestWithBufferedPartitionPendingRevocation` | OUT_OF_SCOPE/MISSING | Pending-revocation is membership-state driven; partial overlap with §20 but the buffered-partition exclusion is in-scope and untested. |
| `testFetchRequestWithBufferedPartitionPendingAssignment` | MISSING | |
| `testFetchRequestWithBufferedPartitionResetOffset` | MISSING | |
| `testFetchRequestWithBufferedPartitionUnfetchable` | REDUCED | `abstract_fetch::test_prepare_fetch_requests_returns_empty_when_nothing_fetchable` covers the unfetchable→empty path generically; not the buffered-partition-specific exclusion. |

FetchRequestManagerTest rough subtotal: ~9 PRESERVED, ~14 PRESERVED-folded, ~20 REDUCED,
~37 MISSING, ~11 OUT_OF_SCOPE (metrics + classic rebalance).

---

## FetcherTest.java (81 tests) — CLASSIC Fetcher

Per `.claude/rules/consumer-threading.md` §20, the **classic** `Fetcher` / `ConsumerCoordinator`
path is out of scope; only KIP-848 `FetchRequestManager` is in scope. `FetcherTest` is a
near-duplicate of `FetchRequestManagerTest` for the decode / record-parsing / offset-update
logic, but exercises the classic synchronous fetch path.

Treatment:
- **Decode / record-parsing / transaction / corruption / control-batch / offset-update tests**
  that apply equally to the async path (e.g. `testFetchNormal`, `testFetcherIgnoresControl
  Records`, `testSkippingAbortedTransactions`, `testReadCommitted*`, `testParseCorruptedRecord`,
  `testHeaders`, `testFetchMaxPollRecords`, etc.) — these duplicate the FetchRequestManagerTest
  entries above and are covered (or gapped) identically. They are **not double-counted** here;
  see the FetchRequestManagerTest table for their verdict, which applies to the shared
  `abstract_fetch.rs` / `completed_fetch.rs` logic.
- **Classic-coordinator-specific tests** (synchronous `fetcher.sendFetches()` +
  `ConsumerNetworkClient` driving, classic rebalance, classic-only metrics wiring) are
  **OUT_OF_SCOPE** per §20.

No separate per-test table is reproduced for `FetcherTest` to avoid double-counting the shared
behavior; the material gaps are already captured in the FetchRequestManagerTest table and the
Key findings. The one thing worth confirming: any `FetcherTest`-only decode assertion that has
*no* `FetchRequestManagerTest` twin would be a true gap — spot-checking the two Java files, the
decode/transaction/parse tests are present in *both*, so the FetchRequestManagerTest table is the
authoritative gap list for shared logic.

---

## Summary counts

In-scope Java tests considered: **124** (91 FRM + 21 FetchCollector + 5 FetchBuffer +
6 CompletedFetch + 1 FetchConfig). FetcherTest's 81 are treated as classic-path duplicates /
out-of-scope and folded into the FRM verdicts (not separately counted).

Approximate status distribution across the 124 in-scope tests:

- PRESERVED (incl. folded): ~38
- REDUCED: ~24
- CHANGED: ~3
- MISSING (in-scope, untested anywhere): ~48
- OUT_OF_SCOPE (metrics / classic rebalance, documented): ~11

The dominant theme: **fine-grained decode/transaction/corruption/error-classification logic is
well covered** (CompletedFetch + FetchCollector layers, often *stronger* than Java). The
**integration-level fetch-manager behaviors are largely untested** because no MockClient
round-trip harness was built for `FetchRequestManager`, and **fetch metrics are deferred
entirely**.
