# Phase 13a — Production-code gaps resolved

All 4 production gaps surfaced by the `PlaintextConsumerAssignTest`
pilot translation (commit `972a053`) have been resolved. The single
fix — turning `fetch_offsets_with_retries` into an actual retry loop
and wiring `mark_coordinator_unknown` into the OffsetFetch/OffsetCommit
response paths — closed all 4 issues as the original
"Action items for the Manager" predicted: Issue 1 was the highest-impact
gap and Issues 2, 3, 4 were all downstream of the same root cause.

All 7 previously `#[ignore]`-gated tests have been un-ignored. Final
run: **8/8 tests pass** under
`cargo test --features integration-tests --test integration plaintext_consumer_assign_test -- --include-ignored --test-threads=1`.

---

## Issue 1: `committed()` does not retry on `NotCoordinator`

**Affected Rust tests:**
- `test_async_assign_and_commit_async_not_committed`
- `test_async_assign_and_commit_sync_not_committed`
- `test_async_assign_and_fetch_committed_offsets` (consumer 2 arm)

**Symptom:** A freshly constructed `AsyncKafkaConsumer` calling
`consumer.committed(&[tp])` immediately after `consumer.assign(...)`
surfaces `KafkaError::Generic(... error: NotCoordinator ...)` instead
of retrying coordinator lookup. Java's classic + KIP-848
`OffsetFetchRequestState` re-issues OffsetFetch after refreshing the
coordinator on `NotCoordinator`; the Rust translation did not.

**Root cause:** `fetch_offsets_with_retries` in
`src/consumer/internals/commit_request_manager.rs` was a mis-named
pass-through that did NOT retry — it just forwarded the single
OffsetFetch result to the caller.

**Java contract:** `KafkaConsumer.committed(...)` ultimately calls
`OffsetFetcherUtils.fetchCommittedOffsets(...)` →
`CommitRequestManager.fetchOffsets(...)` → an
`OffsetFetchRequestState` that re-enqueues on retriable errors
(`OffsetFetchRequestState.onResponse` retries `NotCoordinator`,
`CoordinatorLoadInProgress`, `CoordinatorNotAvailable`).

**Resolution:** Fixup commit `fixup! Phase 13a (1/N): Issue 1 —
fetch_offsets_with_retries retry loop` (against `972a053`).

Two-part fix:

1. **`fetch_offsets_with_retries` retry loop.** The driver now mirrors
   `commit_sync_with_retries`: on retriable errors (or
   `StaleMemberEpoch` with a valid member epoch — Java's
   `isStaleEpochErrorAndValidEpochAvailable`), advance the local
   `current_time_ms` by the configured `retry_backoff_ms`, check
   against `deadline_ms` (wrapping as `KafkaError::timeout` on expiry
   per Java's `maybeWrapAsTimeoutException`), and otherwise allocate a
   fresh `OffsetFetchRequestState` with continuity in the
   `RequestState.num_attempts` counter via a new
   `OffsetFetchRequestState::seed_failed_attempts` helper. The retry is
   pushed onto `unsent_offset_fetches` for the bg-task's next
   `poll_with_coordinator` to pick up.

2. **`mark_coordinator_unknown` wiring.**
   `CommitRequestManagerInner` grew a `Mutex<Option<Arc<CoordinatorRequestManager>>>`
   slot, set after construction via a new
   `CommitRequestManager::set_coordinator` method. The consumer's
   construction path (`async_kafka_consumer.rs`) wires the two
   managers up after both are built. Response handlers
   (`handle_offset_fetch_response`, `classify_and_complete_commit`)
   now call `coord.mark_coordinator_unknown(...)` on
   `NotCoordinator`/`CoordinatorNotAvailable` errors — exactly matching
   Java's `OffsetFetchRequestState.onFailure` and
   `OffsetCommitRequestState.onResponse`
   (`CommitRequestManager.java:804,1092`). The next bg-task
   `coord.poll_shared(now)` iteration then re-issues `FindCoordinator`,
   and the retry driver's next request lands on the freshly discovered
   coordinator.

---

## Issue 2: `commit_sync_offsets()` on a fresh consumer never recovers from `NotCoordinator`

**Affected Rust test:**
- `test_async_assign_and_consume_from_committed_offsets` (consumer 1 arm)

**Symptom:** Immediately after `consumer.assign(...)`,
`consumer.commit_sync_offsets(HashMap{tp: OffsetAndMetadata::new(10)})`
timed out at the full 60s `default.api.timeout.ms` budget.

**Resolution:** Closed by Issue 1's `mark_coordinator_unknown` wiring.
`commit_sync_with_retries` was already implementing the retry loop
correctly, but the response handler did not refresh the coordinator on
`NotCoordinator`/`CoordinatorNotAvailable` — so every retry hit the
same stale-coordinator broker. With `classify_and_complete_commit` now
calling `inner.mark_coordinator_unknown(...)`, the bg task's next
`poll_shared(now)` re-issues `FindCoordinator` and the next retry
attempt lands on the freshly discovered coordinator.

---

## Issue 3: `commit_sync()` after a successful poll cycle still times out

**Affected Rust test:**
- `test_async_assign_and_commit_sync_all_consumed`

**Symptom:** A consumer that just polled 10,000 records successfully
then timed out 60s on `commit_sync()`.

**Resolution:** Closed by the same fix as Issues 1/2. The previously
hypothesized "coordinator-channel liveness on assign-only flows" was
not the actual root cause — the broker the consumer initially routed
to for fetches was simply not the coordinator for offset commits, and
without coordinator refresh on NotCoordinator the commit retry loop
never converged. The retry driver was correct; only the trigger to
re-discover the coordinator was missing.

---

## Issue 4: `poll()` surfaces `NotCoordinator` as a fatal error

**Affected Rust tests:**
- `test_async_assign_and_consume`
- `test_async_assign_and_retrieving_committed_offsets_multiple_times`

**Symptom:** `consumer.poll(...)` returned
`Err(Generic(... error: NotCoordinator ...))`. The failing poll was
the first one after `assign(...)` on a fresh consumer with `group.id`
set. Without an explicit `seek(tp, offset)` before polling, the
consumer issued an `OffsetFetch` to resolve the starting position;
that OffsetFetch hit `NotCoordinator` and surfaced as a fatal `poll`
error.

**Resolution:** Closed by Issue 1's `fetch_offsets_with_retries` retry
loop. `OffsetsRequestManager::init_with_committed_offsets_if_needed`
calls `commit_rm.fetch_offsets(...)` to resolve initial positions; the
returned `oneshot::Receiver` now resolves with the retried value (or a
deadline-respecting `KafkaError::timeout`) instead of the first-attempt
error. Both affected tests now complete cleanly.

---

## Test-pool interference

The original note about sequential-run amplification of failures
vanished, as predicted. All 8 tests pass under one
`cargo test` invocation with `--test-threads=1`. The pool issue was
downstream of Issues 1/2/3/4 — leftover stalled-consumer state at
drop-time caused noise that is no longer present.

---

## Summary

One commit, one root cause, all four issues closed. The relevant
production-code diff is concentrated in:

- `src/consumer/internals/commit_request_manager.rs`
  - New `coordinator: Mutex<Option<Arc<CoordinatorRequestManager>>>`
    field on `CommitRequestManagerInner` + `CommitRequestManagerInner::mark_coordinator_unknown`
    helper.
  - New `CommitRequestManager::set_coordinator` setter.
  - `coordinator_node()` now returns the coordinator's currently known
    node when wired (used to be unconditionally `None`).
  - `handle_offset_fetch_response`: calls `mark_coordinator_unknown`
    on `NotCoordinator`/`CoordinatorNotAvailable` before completing
    the future exceptionally.
  - `classify_and_complete_commit`: now takes
    `&Arc<CommitRequestManagerInner>` instead of `&str` so it can
    invoke `mark_coordinator_unknown` on the same cases.
  - `OffsetFetchRequestState::seed_failed_attempts` — new helper
    mirroring `OffsetCommitRequestState::seed_failed_attempts`.
  - `fetch_offsets_with_retries` — rewritten as a real retry loop
    mirroring `commit_sync_with_retries`.
  - `current_time_ms_now` — new file-local helper (response handlers
    don't carry an injected `current_time_ms`).
- `src/consumer/async_kafka_consumer.rs`
  - Wiring call: `commit_arc.set_coordinator(Arc::clone(coord_arc))`
    after both managers are constructed.
- `tests/integration/plaintext_consumer_assign_test.rs`
  - 7 `#[ignore]` attributes removed.
  - Module-level rustdoc rewritten to reflect the closed gaps.
