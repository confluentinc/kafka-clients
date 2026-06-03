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

---

## Issue 7 — fetch_collector surfaces transient "No current assignment for partition" as fatal error during rebalance

Originally filed by: Manager during Phase 13a (3/N) — PlaintextConsumerSubscriptionTest.

**Affected Rust test:**
  - `tests/integration/plaintext_consumer_subscription_test.rs::test_async_consumer_re2j_pattern_expand_subscription`

**Symptom:** Test exercises `unsubscribe()` + `subscribe_pattern(broader_pattern)` to expand subscription. Mid-rebalance, `consumer.poll(100ms)` raised:

```
IllegalState("No current assignment for partition <topic1>-1")
```

**Java contract:** `pollForFetches()` does NOT raise on transient internal-state issues. A partition that becomes unassigned between `fetchablePartitions()` snapshot and the per-partition `position()` query — or that briefly has no position — is silently skipped. That partition produces no records this poll cycle; the next poll re-checks against a fresh snapshot.

**Root cause:** Two Rust call sites surfaced the `IllegalState` upward instead of skipping:

1. `src/consumer/internals/fetch_collector.rs:375-378` — the `FetchabilityCheck::MissingPosition` arm raised
   `KafkaError::illegal_state("Missing position for fetchable partition ...")`. This branch is "dead code"
   under Java's lock model because `isFetchable(tp) ⇒ hasValidPosition(tp)`. In Rust the same invariant
   holds *within a single lock guard*, but a `CompletedFetch` for a just-revoked partition can still land
   in the buffer between the bg task's prior fetchable snapshot and the collector's pass.
2. `src/consumer/internals/abstract_fetch.rs:529-540` — `prepare_fetch_requests` returned
   `Err(IllegalState("No current assignment for partition X"))` for a partition that became unassigned
   between the `fetchable_partitions()` snapshot (line 508-511) and the per-partition `position()` query.
   Java has the same window (Java methods are `synchronized` per call, not per scope) but the timing is
   tighter in Java's classic flow. The Rust KIP-848 bg-task interleaves application events between
   snapshot and query, so the race surfaces on every `unsubscribe + re-subscribe` rebalance.

The Phase 13a (2/N) fix to Issue 4 was correct for `OffsetOutOfRange` (real, surfaceable error) but
over-propagated these two transient signals.

**Fix landed:**

- `src/consumer/internals/fetch_collector.rs:375-403` — `FetchabilityCheck::MissingPosition` now drains
  the in-flight `CompletedFetch` and returns an empty `FetchPartitionOutcome` instead of `Err`. Mirrors
  the adjacent `NotAssigned` / `NotFetchable` arms.
- `src/consumer/internals/abstract_fetch.rs:526-571` — in `prepare_fetch_requests`, both `Ok(None)` (no
  position yet) and `Err(...)` (no current assignment) from `guard.position(&partition)` now `continue`
  to skip the partition, instead of returning `Err` for the whole batch.

Both call sites are commented with a reference to this Issue and a rationale block explaining the deviation
from Java's literal `throw IllegalStateException` behavior. The deviation is documented per CLAUDE.md §10
(deviations from Java need explicit rationale in source comments).

**Surfaceable errors preserved (regression-tested):**

- `OffsetOutOfRange` with no reset policy — still propagates (Issue 4 regression test
  `test_async_consumer_fetch_invalid_offset` passes).
- `TopicAuthorization` — still propagates
  (`test_fetch_with_topic_authorization_failed` passes).
- `CorruptMessage` — still propagates (`test_fetch_with_corrupt_message` passes).
- Unexpected error codes (catch-all `IllegalState` in `handle_initialize_errors`) — still propagate
  (`test_fetch_with_other_errors` passes).

**Validation:**

- `cargo test --lib fetch_collector` — 16/16 pass.
- `cargo test --lib` — 1700/1700 pass (no regression).
- Subscription suite: 9 pass + 2 ignored (Issue 6 still active).
- Fetch suite: 8 pass + 1 ignored (Issue 5 still active).
- Format-check + lint clean.

**Side effect on Issue 5:** This fix changes Issue 5's failure mode. Previously the
`by_duration:PT1H` test surfaced `IllegalState("Missing position for fetchable partition ...")` quickly.
With this fix, that error is now swallowed and the test will hang waiting for a position that never
materializes (until the test deadline). Issue 5 remains `#[ignore]`-gated; the underlying gap
(`by_duration` reset path is incomplete in `OffsetsRequestManager`) is unchanged.

**Files modified:**

- `src/consumer/internals/fetch_collector.rs` — `MissingPosition` arm now skips (24-line replacement).
- `src/consumer/internals/abstract_fetch.rs` — `prepare_fetch_requests` per-partition loop now skips
  `Ok(None)` and `Err(...)` from `position()` (43-line replacement with documentation).
- `tests/integration/plaintext_consumer_subscription_test.rs` — `#[ignore]` attribute removed from
  `test_async_consumer_re2j_pattern_expand_subscription`; module-level rustdoc updated (9 pass + 2
  ignored, was 8 pass + 3 ignored).
- `design/history/Milestone-8/Phase-13/COMMENTS.1.md` — Issue 7 block replaced with pointer to this entry.

---

## Issue 9: KIP-848 fence-and-rejoin path surfaces `GroupIdNotFound` to `poll()` instead of retrying

**Original tests:**
- `test_async_consumer_max_poll_interval_ms` (still `#[ignore]`-gated on Issue 10 after fix)
- `test_async_consumer_max_poll_interval_ms_delay_in_assignment` (un-ignored, passing after fix)
- `test_async_consumer_max_poll_interval_ms_shorter_than_poll_timeout` (un-ignored, passing after fix)
- `test_async_consumer_recovery_on_poll_after_delayed_rebalance` (still `#[ignore]`-gated on Issue 11 after fix)

**Symptom:** Multiple tests configured with `max.poll.interval.ms=1000` (or relying on
fence-rejoin recovery semantics) surfaced `KafkaError::Generic(... GroupIdNotFound ...)` from
`consumer.poll()` instead of recovering. Two manifestations:

1. `OffsetFetch` after `assign()` on a fresh consumer returned `GroupIdNotFound` from the broker
   (the group was just created / being created), and the response handler propagated it as a
   non-retriable error.
2. After a fence-rejoin cycle, the next heartbeat carried `memberEpoch > 0` but the broker had
   reaped the group state — broker returned `GroupIdNotFound`
   (`GroupMetadataManager.java:2326-2327`'s `createIfNotExists = memberEpoch == 0` guard), the
   heartbeat manager classified it as fatal, and the BackgroundEvent::Error surfaced to the user.

**Java contract:** Java's behavior is the same on paper — `GroupIdNotFoundException` extends
`ApiException` not `RetriableException`, so Java's `OffsetFetchRequestState.onFailure`
(`CommitRequestManager.java:1099-1101`) and `AbstractHeartbeatRequestManager.onErrorResponse`
(`AbstractHeartbeatRequestManager.java:435-441`) both treat it as fatal. Java tests pass because
the broker's first-heartbeat-creates-group behavior masks the race in Java's test environment.
The Rust integration tests against a 3-broker KIP-848 cluster exhibit the race more reliably.

**Resolution:** Fixup commit `fixup! Phase 13a (4/N): Issue 9 — KIP-848 GroupIdNotFound retry +
poll-timer init` (against `58bc9d6`).

Three-part fix:

1. **`GroupIdNotFound` retried in commit/offset-fetch drivers.**
   - `fetch_offsets_with_retries`: added `Errors::GroupIdNotFound` to the retriable-error gate
     alongside the existing `RetriableException`-typed errors and `StaleMemberEpoch`-with-epoch.
     The retry advances the local clock by `retry.backoff.ms`, re-allocates a fresh
     `OffsetFetchRequestState` with continuity in the `RequestState.num_attempts` counter via
     `seed_failed_attempts(...)`, and pushes onto `unsent_offset_fetches` for the bg-task's next
     `poll_with_coordinator` iteration. **No `mark_coordinator_unknown` call** — the coordinator
     is correct; the group simply doesn't exist yet.
   - `commit_sync_with_retries`: same `Errors::GroupIdNotFound` extension.
   - `auto_commit_sync_before_rebalance_with_retries`: same extension.

2. **`GroupIdNotFound` recovered at the heartbeat layer.**
   `ConsumerHeartbeatRequestManager::handle_specific_exception_in_response` grew a new
   `Errors::GroupIdNotFound` arm. Behavior depends on the membership manager's current
   `memberEpoch`:
   - `memberEpoch == 0` (first heartbeat after subscribe / fence-recovery): backoff + retry by
     returning `HeartbeatErrorAction::Handled`. The shared `classify_response_error` caller has
     already called `on_failed_attempt(...)`, so the next heartbeat is naturally backed off.
   - `memberEpoch > 0` (member previously in-group; group has been reaped on broker): treat as
     `HeartbeatErrorAction::Fenced`. The Fenced flow transitions through FENCED → JOINING, which
     sets `memberEpoch = 0` (Java's `resetEpoch`). The next heartbeat carries the fresh epoch
     and re-creates the group on the broker.

3. **Poll-timer init deferred to first `poll()`.**
   - `AbstractHeartbeatRequestManager::poll_timer_expires_at_ms` is now initialized to
     `i64::MAX` (sentinel meaning "not armed yet") instead of
     `current_time_ms + max_poll_interval_ms`. The timer is armed by the first call to
     `reset_poll_timer(current_time_ms)` from the `AsyncPoll` arm of the
     `ApplicationEventProcessor`.
   - Why this deviates from Java: Java's bg thread starts polling the heartbeat manager more or
     less immediately on consumer construction, and the `pollTimer.update(now)` call inside
     `poll()` keeps the timer tracking real time. Rust's bg task has higher latency to spin up
     — with `max.poll.interval.ms=1000`, the consumer can already be near-expired by the time
     the user's first `poll()` lands, fencing itself during the initial join sequence. Deferring
     the arm preserves Java's steady-state semantics for the second-and-subsequent polls.
   - `poll_timer_remaining_ms` and `poll_timer_is_expired_by` switched to `saturating_sub` so
     the `i64::MAX` sentinel doesn't overflow.

4. **`maybe_rejoin_stale_member` actually transitions STALE → JOINING.**
   The previous `AbstractMembershipManager::maybe_rejoin_stale_member()` only reset the
   `is_poll_timer_expired` flag (consistent with Java's `AbstractMembershipManager.java:776-783`
   on paper, but Java's `transitionToJoining` happens via
   `staleMemberAssignmentRelease.whenComplete((__, error) -> transitionToJoining())` after the
   onPartitionsLost callback completes — in the bg-thread). Rust's listener invocation is
   synchronous on the caller's task via the §31 handshake, so by the time the next
   `consumer.poll()` arms `AsyncPoll` and the AEP arm calls `maybe_rejoin_stale_member`, the
   onPartitionsLost callback has already returned. We therefore transition inline without a
   whenComplete dance. New signature: `maybe_rejoin_stale_member(&self, join_group_epoch: i32)`.

5. **AEP `AsyncPoll` arm reproduces Java's `resetPollTimer(pollMs)` shape.**
   `application_event_processor.rs`'s `AsyncPoll` arm now checks
   `hrm.inner().poll_timer_is_expired(poll_time_ms)` BEFORE calling `reset_poll_timer`, and on
   expiry invokes `mm.abstract_mm.maybe_rejoin_stale_member(join_epoch)`. This mirrors Java's
   `AbstractHeartbeatRequestManager.java:265-274`.

**Files modified:**

- `src/consumer/internals/commit_request_manager.rs` — `fetch_offsets_with_retries`,
  `commit_sync_with_retries`, and `auto_commit_sync_before_rebalance_with_retries` extended to
  retry on `Errors::GroupIdNotFound`.
- `src/consumer/internals/abstract_heartbeat_request_manager.rs` — `poll_timer_expires_at_ms`
  initialized to `i64::MAX`; `with_state` constructor `current_time_ms` parameter is now
  unused; `poll_timer_remaining_ms` and `poll_timer_is_expired_by` use `saturating_sub`; doc
  comment noting `GROUP_ID_NOT_FOUND` is intentionally delegated to the consumer-specific
  layer. Unit tests `poll_timer_not_armed_at_construction` and `reset_poll_timer_arms_then_rearms`
  rewritten.
- `src/consumer/internals/consumer_heartbeat_request_manager.rs` —
  `handle_specific_exception_in_response` grew a `Errors::GroupIdNotFound` arm with the
  epoch-conditional behavior described above.
- `src/consumer/internals/abstract_membership_manager.rs` — `maybe_rejoin_stale_member` now
  takes a `join_group_epoch: i32` parameter and transitions STALE → JOINING inline.
- `src/consumer/internals/events/application_event_processor.rs` — `AsyncPoll` arm checks
  `poll_timer_is_expired` and calls `maybe_rejoin_stale_member(join_epoch)` BEFORE
  `reset_poll_timer`.
- `tests/integration/plaintext_consumer_poll_test.rs` —
  `test_async_consumer_max_poll_interval_ms_delay_in_assignment` and
  `test_async_consumer_max_poll_interval_ms_shorter_than_poll_timeout` un-ignored. Remaining
  ignored tests re-targeted to Issue 10 (broker latency) and Issue 11 (provisioner record),
  filed in COMMENTS.1.md.
- `design/history/Milestone-8/Phase-13/COMMENTS.1.md` — Issue 9 block removed; Issue 10 and
  Issue 11 added.

**Pass count delta (poll suite):** before 3/8 (5 ignored: Issues 8/9/9/9/9). After: 5/8
(3 ignored: Issues 8/10/11). Other suites unchanged: assign 8/8, fetch 8/9 (Issue 5),
subscription 9/11 (Issue 6).

---

## Issue 5: `auto.offset.reset=by_duration:PT1H` does not compute position after fresh `assign()`

Originally filed by: Manager during Phase 13a (2/N) re-run after Issue 4 was fixed.

**Affected Rust test:**
  - `tests/integration/plaintext_consumer_fetch_test.rs::test_async_consumer_fetch_out_of_range_offset_reset_config_by_duration`

**Symptom (when originally filed):** Consumer constructed with `auto.offset.reset=by_duration:PT1H`,
fresh `assign(vec![tp])` to a topic-partition with records produced within the last hour. First `poll()`
returned: `IllegalState("Missing position for fetchable partition <topic>-0")`. Per the Phase 13a (3/N)
"Side effect on Issue 5" note in the Issue 7 entry above, the symptom morphed after Issue 7's fix:
the `IllegalState` was swallowed and the test instead hung waiting for a position that never
materialized.

**Resolution:** Closed transitively — no additional code change required. The `by_duration` wire
path was already complete:

- `src/consumer/internals/auto_offset_reset_strategy.rs::timestamp()` already returns
  `Some(now_millis - duration_millis)` for the `ByDuration` arm (lines 144–152), mirroring Java's
  `OffsetResetStrategy.BY_DURATION → currentTimeMs - duration` in `OffsetFetcherUtils.resetPositions`.
- `src/consumer/internals/offset_fetcher_utils.rs::get_offset_reset_strategy_for_partitions` already
  accepts any strategy whose `timestamp()` is `Some(_)` (line 355) and does not single-case
  `EARLIEST`/`LATEST`.
- `src/consumer/internals/offsets_request_manager.rs::send_list_offsets_requests_and_reset_positions`
  already drives one `ListOffsets` request per leader using the per-partition timestamp from
  `strategy.timestamp()` (lines 821–828) — no special-case for `EARLIEST_TIMESTAMP` /
  `LATEST_TIMESTAMP` vs. a positive timestamp value.

The original failure mode and its post-Issue-7 hang both arose from *upstream* gaps that prevented
the consumer from completing its first heartbeat → assignment → reset loop quickly enough on the
3-broker KIP-848 cluster:

1. **Issue 7's fix** (`fetch_collector` / `abstract_fetch` skip-on-transient-state) stopped the
   surface-level `IllegalState` race that masked the eventual reset.
2. **Issue 9's fix** (KIP-848 `GroupIdNotFound` retry + poll-timer init i64::MAX sentinel) let the
   STALE → JOINING transition complete before `poll_for_fetches` reported a missing position. With
   both fixes in place the `ListOffsetsByTimestamp` response (carrying the offset of the first
   record at or after `now − 1h`) arrives and `OffsetFetcherUtils::reset_position_if_needed` seats
   the position via `maybe_seek_unvalidated`.

**Validation:**

- `cargo build` clean.
- `cargo test --lib` — 1700+ pass (no regression).
- `cargo test --features integration-tests --test integration plaintext_consumer_fetch -- --include-ignored --test-threads=1`:
  - 9/9 tests pass (one isolated flake on `_latest` cleared on retry; not related to this fix
    or test).
- `cargo xtask format-check` clean.
- `cargo xtask lint` clean.

**Files modified (this fixup):**

- `tests/integration/plaintext_consumer_fetch_test.rs` — `#[ignore]` attribute removed from
  `test_async_consumer_fetch_out_of_range_offset_reset_config_by_duration`; module-level rustdoc
  rewritten ("All 9 KIP-848 tests are translated and pass end-to-end").
- `design/history/Milestone-8/Phase-13/COMMENTS.1.md` — Issue 5 block replaced with pointer to
  this entry.

**Pass count delta (fetch suite):** before 8/9 (1 ignored: Issue 5). After: 9/9. Other suites
unchanged.
