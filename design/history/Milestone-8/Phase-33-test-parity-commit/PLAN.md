# Phase 33 — CommitRequestManagerTest parity (Actor 33)

Close the CommitRequestManagerTest gap (largest single-file test gap in the
consumer). Production code is already implemented; this phase adds tests +
test-only response builders.

Java source: `kafka/clients/.../internals/CommitRequestManagerTest.java` (4.2).
Rust target: `src/consumer/internals/commit_request_manager.rs` (inline tests).
Worklist: `design/current/test-translation-review/03-commit-offsets-coordinator.md`.

## Rust-vs-Java architecture notes (drive the test mechanics)

The Rust manager diverges from Java in ways that change how tests drive failures:

1. **Retry drivers use a LOCAL clock**, not poll time. `commit_sync_with_retries`,
   `fetch_offsets_with_retries`, `auto_commit_sync_before_rebalance_with_retries`
   each advance `current_time_ms += retry_backoff_ms` per retriable failure and
   compare against `deadline_ms`. To trip a timeout, set `deadline_ms` close to
   `now` so a small number of failures crosses it. Java instead uses `MockTime`
   `time.sleep(...)` + re-poll + `isExpired()`.
2. **Each retry creates a FRESH request** (re-enqueued on `unsent_offset_commits` /
   `unsent_offset_fetches`) seeded with `seed_failed_attempts` for backoff
   continuity. There is no stable `numAttempts` field on a single object across
   retries (Java reuses the same object). So Java's `commitRequest.numAttempts`
   assertions map to: peek the head of the unsent queue and read
   `state.num_attempts()`.
3. **Response completion is via the spawned response handler** registered in
   `build_offset_commit_unsent_request` / `build_offset_fetch_unsent_request`.
   Tests call `poll_with_coordinator(&coordinator, now)` to ship a request, then
   `unsent.handler().on_complete(client_response)` / `on_failure(now, err)`. The
   spawned handler routes into the retry driver. Drive with
   `#[tokio::test(flavor="current_thread")]` + `yield_now()` loops (existing
   precedent) for deterministic single-thread scheduling, except deadline-expiry
   loops that need a real scheduler use `multi_thread` (existing precedent in
   `commit_sync_surfaces_timeout_error_after_deadline_expiry`).
4. **maybe_auto_commit_async / interceptor invocation**: the auto-commit success
   path does NOT enqueue interceptor invocation at the CRM level in Rust — Java's
   `requestAutoCommit` passes `offsetCommitCallbackInvoker.enqueueInterceptorInvocation`
   as the success arm of the BiConsumer. **Production-code check required** (see
   below) — if the Rust `maybe_auto_commit_async` does not wire the invoker, the
   §31 interceptor tests cannot pass without a production fix.

## Test-only builders to add (each has a calling test; clippy -D)

- `coordinator_request_manager.rs`: `#[cfg(test)] set_fatal_error_for_test(&self, KafkaError)`
  — for `testPollWithFatalError*`.
- In the CRM test module (private helpers, no production surface):
  - `build_offset_commit_client_response(per_partition: HashMap<TopicPartition, Errors>)`
    → `ClientResponse` wrapping `ConcreteResponse::OffsetCommit` via existing
    `OffsetCommitResponse::from_response_data`.
  - `build_offset_fetch_client_response(group_id, topics: Vec<(name, topic_id, Vec<(partition, offset, leader_epoch, metadata, error)>)>, group_error)`
    → `ClientResponse` wrapping `ConcreteResponse::OffsetFetch` (v8+ groups form so
    `group()` reads `data.groups`).
  - small `complete_commit_with_error(mgr, &coord, now, error)` / `complete_fetch_with_error`
    helpers mirroring Java's `completeOffsetCommitRequestWithError` /
    `completeOffsetFetchRequestWithError`.

## Production-code audit items (fix if broken; cite Java)

- A1. §31 auto-commit interceptor wiring: does `maybe_auto_commit_async`'s success
  arm enqueue `invoker.enqueue_interceptor_invocation(offsets)`? The CRM `Inner`
  does NOT currently hold an `OffsetCommitCallbackInvoker`. Java's CRM holds it
  (`offsetCommitCallbackInvoker` field) and `requestAutoCommit` calls it. If
  missing, this is a real gap — but per §20/§28 the invoker is generic over
  `<K,V>` and CRM is not generic. Investigate; if a CRM-level wiring is
  infeasible without large refactor, the two §31 tests are tested at the
  `OffsetCommitCallbackInvoker` unit (already PRESERVED) + documented skip with
  rationale. **Decide during 3b after reading `maybe_auto_commit_async` + the
  Java `requestAutoCommit`.**
- A2. Stale doc claim: module header line ~2241-2243 claims
  `testPollWithFatalErrorShouldFailAllUnsentRequests` is "covered by
  `test_fail_all_with_error_via_coordinator_fatal`" — that fn does NOT exist.
  Remove/correct the claim and actually translate the test.

## Commit 3a — commit path

Java tests → Rust fns (snake_case, comment cites Java):
- testCommitSync → commit_sync_success_completes_with_offsets_and_updates_epoch
- testCommitAsync → commit_async_success_completes_and_updates_epoch
- testCommitSyncRetriedAfterExpectedRetriableException (×13) → loop over
  offset_commit_exception_supplier(); retriable→pending-not-done after one fail,
  non-retriable→done-exceptionally with exact exception class.
- testOffsetCommitSyncFailedWithRetriableThrowsTimeoutWhenRetryTimeExpires (×13)
  → loop; retriable→Timeout after deadline, non-retriable→specific class.
- testOffsetCommitAsyncFailedWithRetriableThrowsRetriableCommitException
- testOffsetCommitRequestErroredRequestsNotRetriedForAsyncCommit (×13)
- testCommitSyncFailsWithCommitFailedExceptionIfUnknownMemberId
- testCommitSyncFailsWithCommitFailedExceptionOnStaleMemberEpoch
- testCommitSyncShouldSucceedWithTopicId
- testCommitSyncShouldSucceedWithUnknownOffsetAndMetadata
- testOffsetCommitSingleFailedAttemptPerRequestWhenPartitionErrors (multi-partition
  → exactly ONE numAttempts on head of unsent queue)
- testEnsureBackoffRetryOnOffsetCommitRequestTimeout (onFailure(Timeout)→re-queue)
- testLastEpochSentOnCommit
- testCommitAsyncFailsWithRetriableOnCoordinatorDisconnected
- testPollWithFatalErrorShouldFailAllUnsentRequests (+ fix A2)
- testPollWithFatalErrorDuringCoordinatorIsEmptyAndClosing (assert "Fatal error")
- testPollWithClosingAndPendingRequests (exact "Failed to commit offsets:
  Coordinator unknown and consumer is closing")
- testSignalClose
- testPollEnsureManualCommitSent / testPollEnsureAutocommitSent (request emitted)
- testPollEnsureCorrectInflightRequestBufferSize (4 unsent, 2 inflight, builder types)
- testPollEnsureEmptyPendingRequestAfterPoll

## Commit 3b — offset-fetch + auto-commit

- testOffsetFetchRequestEnsureDuplicatedRequestSucceed (dedup: 2 dup → 1 wire req)
- testOffsetFetchRequestShouldSucceedWithTopicId
- testFetchOffsetsWithTopicIdsDoesNotFailOnUnsubscribedTopics
- testOffsetFetchRequestErroredRequests (×14)
- testOffsetFetchRequestTimeoutRequests (×14 retriable→Timeout)
- testSuccessfulOffsetFetch (offset/metadata/leaderEpoch readback + inflight drain)
- testOffsetFetchMarksCoordinatorUnknownOnRetriableCoordinatorErrors (×3)
- testOffsetFetchMarksCoordinatorUnknownOnCoordinatorDisconnectedAndRetries
- testOffsetFetchRequestPartitionDataError (×5)
- testSyncOffsetFetchFailsWithStaleEpochAndRetriesWithNewEpoch
- testSyncOffsetFetchFailsWithStaleEpochAndNotRetriedIfMemberNotInGroupAnymore
- testOffsetFetchRequestStateToStringBase (exact string, no "Optional")
- testAsyncAutocommitNotRetriedAfterException
- testAutoCommitAsyncFailsWithStaleMemberEpochContinuesToCommitOnTheInterval
- testAutoCommitEmptyDoesNotLeaveInflightRequestFlagOn
- testAutoCommitBeforeRevocationNotBlockedByAutoCommitOnIntervalInflightRequest
- testAutocommitEnsureOnlyOneInflightRequest
- testAutoCommitOnIntervalSkippedIfPreviousOneInFlight
- testAutoCommitSyncBeforeRevocationRetriesOnRetriableAndStaleEpoch (×13)
- testAsyncCommitWhileCoordinatorUnknownIsSentOutWhenCoordinatorDiscovered
- testPollSkipIfCoordinatorUnknown
- §31: testAutocommitInterceptorsInvoked / testAutocommitInterceptorsNotInvokedOnError

## Documented skips

- testEnsureCommitSensorRecordsMetric / testPollEnsureAutocommit metrics asserts
  (commit-rate/commit-total/commit-latency) — OUT_OF_SCOPE (metrics framework
  deferred per CLAUDE.md + Phase 9 plan). The request-emission portion of
  testPollEnsureAutocommitSent IS translated; only the metric assertions are dropped.
- `assertRetryBackOff` exact timeUntilNextPollMs stepping (sleep retryBackoffMs-1 →
  1 → poll) is faithful where the Rust poll-time model supports it; where the
  local-clock retry model makes the exact next-poll-ms unobservable, assert the
  re-queue + that a poll after backoff ships the request (documented inline).
