# Test Translation Review — Commit / Offsets / Coordinator (KIP-848)

Read-only faithfulness review of how the Java Kafka consumer commit / offsets /
coordinator tests were translated to Rust. Scope per §20 (KIP-848 only; classic
`ConsumerCoordinator` path out of scope).

## In-scope totals

| Java file | Java @Test | Rust test fns | PRESERVED | REDUCED | MISSING (in-scope) | CHANGED | OUT_OF_SCOPE |
|---|---|---|---|---|---|---|---|
| CommitRequestManagerTest | 50 | 14 | ~3 | ~11 | ~24 | 0 | 1 (metrics) |
| OffsetsRequestManagerTest | 27 | (in ORM) | ~12 | ~5 | ~9 | 0 | 0 |
| OffsetFetcherTest | 52 | (split: ORM/OFU/OFLE/OATI/AKC) | ~3 | ~9 | ~30 | 0 | ~6 |
| OffsetForLeaderEpochClientTest | 5 | 5 (+2 extra) | 3 | 2 | ~1 | 0 | 0 |
| CoordinatorRequestManagerTest | 9 | 8 (+6 extra) | 6 | 2 | 0 | 2 | 0 |
| OffsetCommitCallbackInvokerTest | 4 | 4 | 3 | 0 | 0 | 1 | 0 |
| **Total** | **147** | — | **~30** | **~29** | **~64** | **3** | **~8** |

Counts are approximate where a Java parameterized matrix (many error codes) was
collapsed to a single representative Rust case — those are scored REDUCED, but
each one hides multiple untested error classifications.

---

## CommitRequestManagerTest (50)

Rust: `src/consumer/internals/commit_request_manager.rs` — 14 test fns. The module
header openly states it translates a "subset" with several Mockito-dependent tests
"deferred to Phase 11."

| Java test | Status | Notes |
|---|---|---|
| testOffsetFetchRequestStateToStringBase | MISSING | `to_string_base` formatting (no "Optional" leak) untested. |
| testPollSkipIfCoordinatorUnknown | MISSING | poll returns 0 unsent when coordinator unknown + pending async commit. |
| testAsyncCommitWhileCoordinatorUnknownIsSentOutWhenCoordinatorDiscovered | MISSING | Coordinator-discovery release semantics. |
| testPollEnsureManualCommitSent | REDUCED | enqueue checked; poll-and-emit-unsent not. |
| testPollEnsureAutocommitSent | REDUCED | enqueue+poll+success covered; commit-rate/total metrics not (deferred). |
| testPollEnsureCorrectInflightRequestBufferSize | REDUCED | single fetch inflight covered; mixed 4-unsent/2-inflight buffer + builder-type asserts not. |
| testPollEnsureEmptyPendingRequestAfterPoll | REDUCED | partial; `unsentOffsetCommitRequests()` emptiness not asserted. |
| testCommitSync | REDUCED | enqueue only; future-completes-with-offsets + epoch-update not. |
| testCommitSyncWithEmptyOffsets | PRESERVED | `commit_sync_empty_offsets_resolves_immediately`. |
| testCommitAsync | REDUCED | async success completion + epoch verify absent at CRM level. |
| testCommitAsyncWithEmptyOffsets | PRESERVED | `commit_async_empty_offsets_resolves_immediately` (+asserts callback not enqueued). |
| testAsyncAutocommitNotRetriedAfterException | MISSING | auto-commit not retried after exception; next only after interval. |
| testCommitSyncRetriedAfterExpectedRetriableException (param ×13) | REDUCED | 1 error (CoordinatorLoadInProgress)→Timeout; 13-error retry/backoff/markUnknown matrix not. |
| testCommitSyncFailsWithCommitFailedExceptionIfUnknownMemberId | MISSING | UNKNOWN_MEMBER_ID→CommitFailedException mapping. |
| testCommitSyncFailsWithCommitFailedExceptionOnStaleMemberEpoch | REDUCED | rebalance-flush analog exists; commitSync→CommitFailedException for STALE_MEMBER_EPOCH not direct. |
| testCommitSyncShouldSucceedWithTopicId | MISSING | topic-ID commit path (apiVersion ≥10). |
| testCommitSyncShouldSucceedWithUnknownOffsetAndMetadata | MISSING | unknown-epoch offset commit success. |
| testAutoCommitAsyncFailsWithStaleMemberEpochContinuesToCommitOnTheInterval | MISSING | fatal stale-epoch on interval → reset timer, retry next interval. |
| testCommitAsyncFailsWithRetriableOnCoordinatorDisconnected | MISSING | disconnect→RetriableCommitFailedException + handleCoordinatorDisconnect. |
| testAutocommitEnsureOnlyOneInflightRequest | REDUCED | `should_auto_commit` predicate only; full no-resend-while-inflight flow not. |
| testAutoCommitBeforeRevocationNotBlockedByAutoCommitOnIntervalInflightRequest | MISSING | concurrent interval-commit + rebalance-commit coexistence. |
| testAutocommitInterceptorsInvoked | MISSING (§31) | interceptor invocation on auto-commit success not verified at CRM level. |
| testAutocommitInterceptorsNotInvokedOnError | MISSING (§31) | interceptor NOT invoked on partition error. |
| testAutoCommitEmptyOffsetsDoesNotGenerateRequest | PRESERVED | `update_timer_and_maybe_commit_resets_timer_when_no_consumed_offsets`. |
| testAutoCommitEmptyDoesNotLeaveInflightRequestFlagOn | MISSING | empty→non-empty sequence; inflight flag not stuck. |
| testAutoCommitOnIntervalSkippedIfPreviousOneInFlight | REDUCED | predicate covered; timer-not-reset-while-inflight + resend-on-completion not. |
| testOffsetFetchRequestEnsureDuplicatedRequestSucceed | MISSING | OffsetFetch dedup/coalescing (2 dup → 1 wire request). |
| testOffsetFetchRequestShouldSucceedWithTopicId | MISSING | topic-ID OffsetFetch dedup. |
| testFetchOffsetsWithTopicIdsDoesNotFailOnUnsubscribedTopics | MISSING | topic-ID not in metadata cache parse tolerance. |
| testOffsetFetchRequestErroredRequests (param ×14) | REDUCED | 1 error driven + drain; retriable/non-retriable matrix + numAttempts not. |
| testOffsetFetchRequestTimeoutRequests (param) | MISSING | retriable fetch retried to timeout → exception class matrix. |
| testSuccessfulOffsetFetch | MISSING | success readback (offset/metadata/leaderEpoch) + inflight drain (only failure-drain exists). |
| testOffsetFetchMarksCoordinatorUnknownOnRetriableCoordinatorErrors (param) | MISSING | markCoordinatorUnknown on NOT_COORDINATOR/COORDINATOR_NOT_AVAILABLE + retry. |
| testOffsetFetchMarksCoordinatorUnknownOnCoordinatorDisconnectedAndRetries | MISSING | disconnect handling + retry for fetch. |
| testOffsetCommitRequestErroredRequestsNotRetriedForAsyncCommit (param) | MISSING | async commit not retried + RetriableCommitFailedException matrix. |
| testOffsetCommitSyncTimeoutNotReturnedOnPollAndFails | REDUCED | spirit covered; REQUEST_TIMED_OUT double-retry-then-expire sequencing not. |
| testOffsetCommitSyncFailedWithRetriableThrowsTimeoutWhenRetryTimeExpires (param ×13) | REDUCED | 1-error version; full matrix + expectedExceptionClass not. |
| testOffsetCommitAsyncFailedWithRetriableThrowsRetriableCommitException | MISSING | async retriable → RetriableCommitFailedException (not Timeout). |
| testOffsetCommitSingleFailedAttemptPerRequestWhenPartitionErrors (param) | MISSING | multi-partition error → exactly ONE numAttempts increment. |
| testEnsureBackoffRetryOnOffsetCommitRequestTimeout | MISSING | onFailure(Timeout)→re-queue + assertRetryBackOff timer asserts. |
| testSyncOffsetFetchFailsWithStaleEpochAndRetriesWithNewEpoch | MISSING | StaleEpoch + new-epoch retry with updated memberId/epoch. |
| testSyncOffsetFetchFailsWithStaleEpochAndNotRetriedIfMemberNotInGroupAnymore | MISSING | fetch StaleEpoch + no-epoch → fail without retry. |
| testAutoCommitSyncBeforeRevocationRetriesOnRetriableAndStaleEpoch (param) | REDUCED | slices covered; full matrix + retried-request epoch/memberId asserts not. |
| testLastEpochSentOnCommit | MISSING | lastEpochSentOnCommit progression across epoch updates. |
| testEnsureCommitSensorRecordsMetric | OUT_OF_SCOPE | metrics framework deferred (justified). |
| testOffsetFetchRequestPartitionDataError (param) | MISSING | per-partition fetch error (partition-level errorCode, retriable vs not). |
| testSignalClose | REDUCED | flag checked; pending async commit still emitted on poll after signalClose not. |
| testPollWithFatalErrorShouldFailAllUnsentRequests | MISSING (in-scope) | **Module header claims "covered by `test_fail_all_with_error_via_coordinator_fatal`" — that fn does not exist.** |
| testPollWithFatalErrorDuringCoordinatorIsEmptyAndClosing | MISSING (in-scope) | closing+fatal → GroupAuthorizationException + asserts message "Fatal error" (message-content lost). |
| testPollWithClosingAndPendingRequests | MISSING (in-scope) | closing+coordinator-unknown → CommitFailedException with exact "Failed to commit offsets: Coordinator unknown and consumer is closing". |

### Key findings — CommitRequestManager
1. **Stale "covered by" claim is false** — header asserts `testPollWithFatalErrorShouldFailAllUnsentRequests` is covered by `test_fail_all_with_error_via_coordinator_fatal`; no such fn exists. Fatal-error-fails-all-unsent path is untested.
2. **§31 callback-obligation tests missing** — `testAutocommitInterceptorsInvoked` / `testAutocommitInterceptorsNotInvokedOnError` (interceptor fires on auto-commit success, suppressed on error) have no CRM-level equivalent.
3. **Commit-on-close error-message assertions dropped** — closing-path tests assert exact strings ("Fatal error"; "Failed to commit offsets: Coordinator unknown and consumer is closing"). DoD §3 requires message-content assertions. None translated.
4. **Per-partition error handling fully missing** — "multi-partition error response registers exactly ONE retry attempt" (`testOffsetCommitSingleFailedAttemptPerRequestWhenPartitionErrors`, `testOffsetFetchRequestPartitionDataError`) untested.
5. **Parameterized error matrices collapsed to single cases** — 3 `@MethodSource` suppliers (~13/14/5 errors) across 6 parameterized tests assert per-error retriable/fatal classification + exact exception class (Timeout vs GroupAuthorization vs OffsetMetadataTooLarge vs InvalidCommitOffsetSize vs CommitFailed vs StaleMemberEpoch vs KafkaException). Rust tests at most one representative error per path. **Largest behavioral gap.**
6. **OffsetFetch dedup/coalescing untested** — `testOffsetFetchRequestEnsureDuplicatedRequestSucceed` + topic-ID variant.
7. **Auto-commit timing edge cases untested** — not-retried-after-exception, stale-epoch-continues-on-interval, empty-doesnt-leave-inflight-flag, interval+revocation coexistence.

---

## OffsetsRequestManagerTest (27)

Rust: `src/consumer/internals/offsets_request_manager.rs` (inline tests).

| Java test | Status | Notes |
|---|---|---|
| testListOffsetsRequest_Success | PRESERVED | `fetch_offsets_success_single_partition` (offset=5). |
| testListOffsetsWaitingForMetadataUpdate_Timeout | REDUCED | park covered; `verify(metadata).requestUpdate(true)` + future-times-out not. |
| testListOffsetsRequestMultiplePartitions | MISSING | single request for 2 partitions same leader, both offsets. |
| testListOffsetsRequestEmpty | PRESERVED | `fetch_offsets_empty_resolves_immediately`. |
| testListOffsetsRequestUnknownOffset | PRESERVED | `fetch_offsets_unknown_offset_in_response_returns_none`. |
| testListOffsetsWaitingForMetadataUpdate_RetrySucceeds | REDUCED | park→update→replay→success; `requestUpdate(true)` verify missing. |
| testRequestFailsWithRetriableError_RetrySucceeds (param ×10) | REDUCED | only UnknownLeaderEpoch; **9/10 error codes untested**; `requestUpdate(false)` missing. |
| testRequestNotSupportedErrorReturnsNullOffset | PRESERVED | `fetch_offsets_unsupported_for_message_format_returns_none`. |
| testRequestWithUnknownOffsetInResponseReturnsNullOffset | PRESERVED | same mapping as unknown-offset. |
| testRequestPartiallyFailsWithRetriableError_RetrySucceeds | MISSING | 2 brokers, partial success+retriable, partial-retry merge (`apply_partial_result`). |
| testRequestFailedResponse_NonRetriableAuthError | PRESERVED | `fetch_offsets_topic_authorization_failed_surfaces_error` (typed + message). |
| testRequestFailedResponse_NonRetriableErrorTimeout | MISSING | error for partition NOT in request → future stays pending. |
| testRequestFails_AuthenticationException | PRESERVED | `fetch_offsets_authentication_exception_completes_exceptionally`. |
| testResetPositionsSendNoRequestIfNoPartitionsNeedingReset | PRESERVED | `reset_positions_send_no_request_if_no_partitions_needing_reset`. |
| testResetPositionsMissingLeader | MISSING | reset+missing leader → `requestUpdate(true)`, requestsToSend==0. |
| testResetPositionsSuccess_NoLeaderEpochInResponse | MISSING | successful reset response; `updateLastSeenEpochIfNewer` never called. |
| testResetPositionsSuccess_LeaderEpochInResponse | MISSING | reset success epoch present → `updateLastSeenEpochIfNewer(tp,epoch)`. |
| testResetOffsetsAuthorizationFailure | MISSING | reset TOPIC_AUTHORIZATION_FAILED cached → re-raised on next resetPositionsIfNeeded. |
| testValidatePositionsSuccess | MISSING | successful OffsetsForLeaderEpoch response + maybeCompleteValidation. |
| testValidatePositionsMissingLeader | MISSING | validate no-node leader → `requestUpdate(true)`. |
| testValidatePositionsFailureWithUnrecoverableAuthException | MISSING | validate auth-failed cached → re-raised on next validatePositionsIfNeeded. |
| testValidatePositionsAbortIfNoApiVersionsToCheckAgainstThenRecovers | PRESERVED | `test_validate_positions_abort_if_no_api_versions...` (+try_connect assert). |
| testUpdatePositionsWithCommittedOffsets | PRESERVED | split: enqueue + apply-on-response (seek to 10). |
| testUpdatePositionsWithCommittedOffsetsReusesRequest | PRESERVED | `update_fetch_positions_reuses_pending_request...`. |
| testUpdatePositionsDoesNotApplyOffsetsIfPartitionNotInitializingAnymore | PRESERVED | `..._does_not_apply_offsets_if_partition_no_longer_initializing`. |
| testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition | PRESERVED | `..._does_not_reset_partitions_added_mid_flight`. |
| testRemoteListOffsetsRequestTimeoutMs | PRESERVED | `fetch_offsets_uses_configured_request_timeout_ms` (timeout==100). |

### Key findings — OffsetsRequestManager
1. **Entire reset-positions and validate-positions success/response paths untested** — 5 tests (`testValidatePositionsSuccess`, `testResetPositionsSuccess_*Epoch*`, `testResetPositionsMissingLeader`, `testValidatePositionsMissingLeader`). `send_offsets_for_leader_epoch_requests_and_validate_positions` has **no response-path coverage at all**.
2. **Deferred-exception (cached auth) paths on reset and validate untested** — response→cache→re-raise round trip; Rust only seeds the cache directly.
3. **Parameterized retriable-error test collapsed 10→1** — INVALID_REQUEST/KAFKA_STORAGE_ERROR/OFFSET_NOT_AVAILABLE etc. classification now unverified.
4. **Multi-node / partial-result paths untested** — `apply_partial_result` / `partial_fetched_for_node` (per-node merge) has no test.
5. **`metadata.requestUpdate(...)` assertions dropped throughout** — "retriable error triggers metadata update" asserted nowhere.

---

## OffsetFetcherTest (52) — classic fetcher, logic split in Rust

KIP-848 equivalent logic split across ORM (`offsets_request_manager.rs`),
OFU (`offset_fetcher_utils.rs`), OFLE (`offsets_for_leader_epoch_client.rs`),
OATI (`offset_and_timestamp_internal.rs`), AKC (`async_kafka_consumer.rs`).

| Java test | Status | Where | Notes |
|---|---|---|---|
| testUpdateFetchPositionNoOpWithPositionSet | REDUCED | ORM | has-positions fast-path; not classic reset+isFetchable asserts. |
| testUpdateFetchPositionResetToDefaultOffset | MISSING | — | reset EARLIEST → position==5, isOffsetResetNeeded==false. |
| testUpdateFetchPositionResetToLatestOffset | MISSING | — | reset LATEST behavioral. |
| testUpdateFetchPositionResetToDurationOffset | MISSING | — | duration/by-timestamp reset. |
| testFetchOffsetErrors | MISSING | — | OFFSET_NOT_AVAILABLE/LEADER_NOT_AVAILABLE retriable in reset path. |
| testListOffsetSendsReadUncommitted | MISSING | — | isolation level on wire (reset path). |
| testListOffsetSendsReadCommitted | MISSING | — | READ_COMMITTED on wire. |
| testresetPositionsSkipsBlackedOutConnections | OUT_OF_SCOPE | — | classic `client.backoff(node)` connection-blackout. |
| testUpdateFetchPositionResetToEarliestOffset | MISSING | — | EARLIEST reset behavioral. |
| testresetPositionsMetadataRefresh | REDUCED | ORM | covered for fetch path, not reset path. |
| testListOffsetNoUpdateMissingEpoch | MISSING | — | leaderEpoch ignore when metadata has none. |
| testListOffsetUpdateEpoch | MISSING | — | higher epoch → metadata update + lastSeenLeaderEpoch. |
| testUpdateFetchPositionDisconnect | MISSING | — | reset disconnect → metadata refresh → backoff → retry. |
| testAssignmentChangeWithInFlightReset | MISSING | — | in-flight reset response discarded after assignment change. |
| testSeekWithInFlightReset | MISSING | — | seek during in-flight reset → discarded, position==237. |
| testEarlierOffsetResetArrivesLate | MISSING | — | strategy changed mid-flight; stale response ignored. |
| testChangeResetWithInFlightReset | MISSING | — | strategy change during reset; discarded. |
| testIdempotentResetWithInFlightReset | MISSING | — | idempotent re-request; applied, position==5. |
| testResetOffsetsAuthorizationFailure | REDUCED | ORM/OFLE | auth surfaced for fetch+OFLE; reset-path raise-once-then-clear not. |
| testFetchingPendingPartitionsBeforeAndAfterSubscriptionReset | OUT_OF_SCOPE | subscription_state | SubscriptionState markPendingRevocation/isFetchable. |
| testUpdateFetchPositionOfPausedPartitionsRequiringOffsetReset | MISSING | — | reset on paused partition; completes but not fetchable. |
| testUpdateFetchPositionOfPausedPartitionsWithoutAValidPosition | OUT_OF_SCOPE | subscription_state | pure SubscriptionState. |
| testUpdateFetchPositionOfPausedPartitionsWithAValidPosition | OUT_OF_SCOPE | subscription_state | pure SubscriptionState. |
| testGetOffsetsForTimesTimeout | REDUCED | AKC | timeout+message at event level. |
| testGetOffsetsForTimes | REDUCED | ORM/OFU | empty/unknown/NONE/UNSUPPORTED covered; multi-partition mixed-error not. |
| testGetOffsetsFencedLeaderEpoch | MISSING | — | FENCED_LEADER_EPOCH reset still needed, timeToNextUpdate==0. |
| testGetOffsetByTimeWithPartitionsRetryCouldTriggerMetadataUpdate | REDUCED | ORM | 1 retriable retry; 7-error loop + leader-rerouting not. |
| testGetOffsetsUnknownLeaderEpoch | REDUCED | ORM | fetch-path structural; reset-path asserts not. |
| testGetOffsetsIncludesLeaderEpoch | MISSING | — | request carries currentLeaderEpoch==99. |
| testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially | MISSING | — | staged metadata refreshes, leader resolution over time. |
| testGetOffsetsForTimesWhenSomeTopicPartitionLeadersDisconnectException | MISSING | — | per-partition disconnect recovery. |
| testListOffsetsWithZeroTimeout | REDUCED | AKC | zero-timeout→empty per partition covered. |
| testBatchedListOffsetsMetadataErrors | MISSING | — | NOT_LEADER+UNKNOWN_TOPIC batched → TimeoutException. |
| testOffsetValidationRequestGrouping | REDUCED | OFLE/OFU | per-leader/topic grouping unit-tested; end-to-end "no partition left awaiting" not. |
| testOffsetValidationAwaitsNodeApiVersion | PRESERVED | ORM | faithful (no request until ApiVersions, then issued). |
| testOffsetValidationSkippedForOldBroker | MISSING | — | OFLE v0–v2 → validation skipped. |
| testOffsetValidationSkippedForOldResponse | MISSING | — | metadata v8 unreliable epoch → skipped. |
| testOffsetValidationresetPositionForUndefinedEpochWithDefinedResetPolicy | MISSING | — | UNDEFINED_EPOCH + EARLIEST → reset. |
| testOffsetValidationresetPositionForUndefinedOffsetWithDefinedResetPolicy | MISSING | — | UNDEFINED_EPOCH_OFFSET + EARLIEST → reset. |
| testOffsetValidationresetPositionForUndefinedEpochWithUndefinedResetPolicy | MISSING | — | UNDEFINED_EPOCH + NONE → LogTruncationException (empty divergent). |
| testOffsetValidationresetPositionForUndefinedOffsetWithUndefinedResetPolicy | MISSING | — | UNDEFINED_EPOCH_OFFSET + NONE → LogTruncationException. |
| testOffsetValidationTriggerLogTruncationForBadOffsetWithUndefinedResetPolicy | MISSING | — | bad offset + NONE → LogTruncationException w/ offsetOutOfRange + divergentOffsets payload. |
| testOffsetValidationHandlesSeekWithInflightOffsetForLeaderRequest | MISSING | — | seek during in-flight OFLE → ignored, still awaiting. |
| testOffsetValidationFencing | MISSING | — | epoch fencing during async validation. |
| testBeginningOffsets | REDUCED | AKC | event-level; no direct beginning_offsets mechanics test. |
| testBeginningOffsetsDuplicateTopicPartition | MISSING | — | duplicate tp collapses to one. |
| testBeginningOffsetsMultipleTopicPartitions | MISSING | — | multi-partition (Rust fetch tests single-partition). |
| testBeginningOffsetsEmpty | REDUCED | AKC/ORM | empty→empty covered. |
| testEndOffsets | REDUCED | AKC/OATI | timeout/empty covered; LATEST_TIMESTAMP mechanics not direct. |
| testEndOffsetsDuplicateTopicPartition | MISSING | — | duplicate tp de-dup. |
| testEndOffsetsMultipleTopicPartitions | MISSING | — | multi-partition. |
| testEndOffsetsEmpty | REDUCED | AKC/ORM | empty→empty covered. |

### Key findings — OffsetFetcher
1. **Entire reset-offsets state machine untested behaviorally (~15 tests)** — zero `is_offset_reset_needed` / `has_valid_position` / `is_fetchable` assertions in the ORM test module. No test drives a ListOffsets reset response and asserts resulting SubscriptionState. Reset path could regress silently.
2. **OffsetValidation → LogTruncation completely untested (#40–#42)** — `LogTruncationException` (the `auto.offset.reset=none` divergent-offset contract) and its `offsetOutOfRangePartitions`/`divergentOffsets` payload have no test. DoD §3 (assert error content) unmet. Validate→reset (#38–#39) and old-broker/old-response skip (#36–#37) also absent. Only `testOffsetValidationAwaitsNodeApiVersion` preserved.
3. **Leader-epoch on the wire and metadata-update-on-epoch-change untested** (#11, #12, #29, #44).
4. **`offsetsForTimes` multi-partition + mixed-error coverage thin** (#25, #27, #30, #31, #33) — Rust fetch tests single-partition only.
5. **beginning/end offsets duplicate-collapse + multi-partition untested; isolation-level-on-wire untested** (#6, #7, #46, #47, #50, #51). Positive `OffsetAndTimestamp` value semantics (timestamp=-1 sentinel) ARE covered by OATI tests (prior regression — good).

---

## OffsetForLeaderEpochClientTest (5)

Rust: `src/consumer/internals/offsets_for_leader_epoch_client.rs` (5 + 2 extra). Rust
tests the static `prepare_request` / `handle_response` helpers directly; Java drives
through `sendAsyncRequest` + MockClient round-trip.

| Java test | Status | Notes |
|---|---|---|
| testEmptyResponse | REDUCED | empty→empty conceptually covered, not standalone. |
| testUnexpectedEmptyResponse | MISSING/REDUCED | requested-but-absent partition → `partitions_to_retry` not asserted (only the inverse `..._ignores_unrequested_partitions`). |
| testOkResponse | PRESERVED | `handle_response_extracts_end_offsets` (end_offset=100); returned errorCode not asserted (minor). |
| testUnauthorizedTopic | PRESERVED | `handle_response_raises_topic_auth_exception` (unauthorized_topics contains "t"). |
| testRetriableError | PRESERVED | `handle_response_marks_retriable_errors` (NotLeaderOrFollower ≈ LEADER_NOT_AVAILABLE). |

### Key findings — OffsetForLeaderEpochClient
- One real gap: `testUnexpectedEmptyResponse` (requested partition missing from response → `partitions_to_retry`) not directly asserted — distinct code path from "unrequested partition ignored."
- Async `sendAsyncRequest` future-resolution wiring not exercised here (only pure helpers). Acceptable per §1/§10.

---

## CoordinatorRequestManagerTest (9)

Rust: `src/consumer/internals/coordinator_request_manager.rs` (8 + 6 extra regressions).

| Java test | Status | Notes |
|---|---|---|
| testSuccessfulResponse | PRESERVED | id=`i32::MAX - node.id()`, host/port; post-discovery poll empty. |
| testMarkCoordinatorUnknownLoggingAccuracy (@Flaky) | CHANGED | no log-capture; asserts internal counters. **Warning message-content (exact millis) dropped** (DoD §3, log line). |
| testMarkCoordinatorUnknown | PRESERVED | backoff-after-success path. |
| testBackoffAfterRetriableFailure | PRESERVED | CoordinatorLoadInProgress; `verifyNoInteractions(backgroundEventHandler)` has no direct counterpart (no BEH wired). |
| testBackoffAfterFatalError | PRESERVED | GroupAuthorizationFailed; asserts fatal_error type (stronger than Java). |
| testNullGroupIdShouldThrow | CHANGED (faithful) | empty `""` + should_panic("group_id must not be empty"). |
| testFindCoordinatorResponseVersions | PRESERVED | v4 and ≤v3 both resolve. |
| testNetworkTimeout | REDUCED | hand-drives mark_coordinator_unknown + on_failed_attempt; production onFailure routing covered separately by extra `test_response_routing_failure_path`. |
| testClearFatalErrorWhenReceivingSuccessfulResponse (param: NONE, COORDINATOR_NOT_AVAILABLE) | PRESERVED | split into 2 fns; faithful. |

### Key findings — CoordinatorRequestManager
- All 9 accounted for; high faithfulness + strong extra forwarder-path coverage.
- `testMarkCoordinatorUnknownLoggingAccuracy`: exact warning message-content not asserted (counter checked instead) — documented trade-off (no log-capture crate), minor (log line, not returned error).
- `testNetworkTimeout` hand-drives the failure path; production routing covered by an extra test.
- `verifyNoInteractions(backgroundEventHandler)` (no fatal event on retriable) has no direct counterpart.

---

## OffsetCommitCallbackInvokerTest (4)

Rust: `src/consumer/internals/offset_commit_callback_invoker.rs` (4). Mock→recording-struct
substitution; ordering via recorded-call vectors ≈ Mockito InOrder.

| Java test | Status | Notes |
|---|---|---|
| testMultipleUserCallbacksInvoked | PRESERVED | not-before-drain, once-each-in-order, no double-fire. |
| testNoOnCommitOnEmptyInterceptors | CHANGED (faithful) | "onCommit never" structurally guaranteed by empty chain; user callback fires. |
| testOnlyInterceptors | PRESERVED | 2 calls FIFO order, no re-invoke. |
| testMixedCallbacksInterceptorsInvoked | PRESERVED | interceptors before user callback, none before drain, no re-fire. |

### Key findings — OffsetCommitCallbackInvoker
- All 4 faithfully translated. §31 callback obligation, FIFO ordering, no-double-fire all asserted. No material gap. (Java has no throwing-callback test, so no omission.)

---

## Top cross-file findings (most material)

1. **Reset-positions state machine has no behavioral/response-path tests** — across OffsetsRequestManager and OffsetFetcher, ~15+ Java tests covering reset EARLIEST/LATEST/duration, retriable errors, disconnect, stale-response-discard (assignment-change/seek/strategy-change), fenced epoch, paused-partition reset have no Rust equivalent. Confirmed zero `is_offset_reset_needed`/`has_valid_position`/`is_fetchable` assertions in the ORM test module. **Largest gap; reset path can regress silently.**

2. **OffsetValidation → LogTruncation completely untested** — `LogTruncationException` and its `offsetOutOfRangePartitions`/`divergentOffsets` payload (the `auto.offset.reset=none` divergence contract) have no test. Validate-success response path (`send_offsets_for_leader_epoch_requests_and_validate_positions`) has no coverage at all.

3. **Parameterized error matrices collapsed to single representatives** — CommitRequestManager (3 suppliers, ~32 error-cases across 6 tests), OffsetsRequestManager (10-error retriable test → 1), OffsetForLeaderEpoch retriable (1 of 7). The retriable-vs-fatal classification and exact-exception-class mapping — explicitly required by DoD §3 — is the most pervasive coverage loss.

4. **Error-message-content assertions dropped on the commit-on-close and coordinator-logging paths** — CommitRequestManager closing-path messages ("Fatal error"; "Failed to commit offsets: Coordinator unknown and consumer is closing") and CoordinatorRequestManager disconnect-warning millis are untested. DoD §3 unmet on these.

5. **§31 callback-obligation gap at CRM level** — auto-commit interceptor invocation (success) / suppression (error) is untested in CommitRequestManager, though the OffsetCommitCallbackInvoker unit itself is faithfully covered.

6. **Stale documentation claim** — `commit_request_manager.rs` header asserts `testPollWithFatalErrorShouldFailAllUnsentRequests` is "covered by `test_fail_all_with_error_via_coordinator_fatal`"; that fn does not exist.

7. **Per-partition error handling, OffsetFetch dedup/coalescing, multi-partition offset queries, leader-epoch-on-the-wire** — all untested across the commit/fetch paths.
