# Critic 1 — Milestone 9 Phase 5 review (ShareConsumeRequestManager + share events)

Reviewed commits `9737fdb..020842f` (`0ea0f6d` events + ack handler, `e0a9545`
manager, `503f8ca`/`37d0ad8`/`48f122f`/`497417e`/`8862b4d` tests) on
`milestone9-share-consumer`. 49 unit tests pass, 1 `#[ignore]`.

Overall the manager is a faithful, careful translation of the 1571-line Java
`ShareConsumeRequestManager`. The per-node in-flight-ack slot routing
(`find_in_flight_ack_slot`), the fetch/acknowledge orchestration, the §31
`maybe_send_share_acknowledgement_event` exactly-once wiring, and the §28 event
hierarchy were all verified against Java and are correct. One real bug and two
lower-severity items follow.

## Issue: COMMIT_ASYNC deadline reset uses time 0, not current time (production premature-timeout)
- **File**: `src/consumer/internals/share_consume_request_manager.rs:488-497` (`processing_complete`) and `:425-430` (`maybe_reset_timer_and_request_state`)
- **Severity**: Bug / Behavior Mismatch
- **Java Reference**: `ShareConsumeRequestManager.java:1371-1377` (`processingComplete` → `maybeResetTimerAndRequestState`) → `TimedRequestState.java:56-58` (`resetTimeout` → `timer.updateAndReset(timeoutMs)`)
- **Description**: `processing_complete()` calls
  `self.maybe_reset_timer_and_request_state(0)` with a hardcoded `now_ms = 0`.
  For a `CommitAsync` state this runs
  `reset_deadline(0.saturating_add(self.timeout_ms))`, i.e. it sets the absolute
  deadline to `timeout_ms` (~60000 for a default api timeout). Java's
  `resetTimeout(timeoutMs)` calls `timer.updateAndReset(timeoutMs)`, which first
  `update()`s the timer to the *current* wall-clock time and then resets it to
  expire `timeoutMs` from now — i.e. deadline = `currentTime + timeoutMs`.
  `reset()` zeroes `num_attempts` (`request_state.rs:120`) and `maybe_expire()`
  is `num_attempts > 0 && is_expired(now)` (`:400-402`), so the wrong deadline is
  latent until the reused async state is sent again and then needs a retry.
- **Failure scenario (production only — masked by the near-zero `MockClock` in tests)**:
  with `SystemShareConsumeTime` (ms since epoch, `now ≈ 1.7e12`):
  1. `commit_async` for node N builds+sends a request; app calls `commit_async`
     again while it is in flight, so new acks are merged into the *same*
     `Tuple.async_request` (`:1481-1486`) — the state is reused, not recreated.
  2. The first response arrives → `processing_complete()` resets
     `deadline_ms = timeout_ms (~60000)` and `num_attempts = 0`.
  3. Next poll builds+sends the reused state (`num_attempts → 1`). A retriable
     partition error moves acks to incomplete and sets `should_retry`
     (`process_retry_logic`, no `processing_complete`), leaving `num_attempts > 0`.
  4. On the following poll, `maybe_build_request` → `maybe_expire()` =
     `num_attempts(≥1) > 0 && is_expired(1.7e12 >= 60000)` = **true** → the async
     acknowledgements are failed with `REQUEST_TIMED_OUT` immediately, instead of
     being retried for the configured `default.api.timeout.ms`. Java would keep
     retrying until `currentTime + timeoutMs`.
- **Expected**: reset the deadline relative to the current time, as Java does.
  `processing_complete` should thread the current time (the acknowledge-response
  handlers already have `response_completion_time_ms`; the session-not-found path
  can read `time.milliseconds()`), i.e. `reset_deadline(now_ms + timeout_ms)`.
- **Actual**: `reset_deadline(0 + timeout_ms)` — an absolute deadline in the
  distant past for any real clock. Tests do not catch this because `MockClock`
  starts at 0 and stays far below `timeout_ms`.

## Issue: `testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange` silently dropped (undocumented, single-node)
- **File**: `src/consumer/internals/share_consume_request_manager.rs` (`mod tests`)
- **Severity**: Missing Requirement (test fidelity) — non-blocking
- **Java Reference**: `ShareConsumeRequestManagerTest.java::testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange`
- **Description**: Of the 59 Java `@Test`/`@ParameterizedTest` methods, 9 are not
  translated. Eight are documented as deferred (metrics/KIP-714, `filterTo`,
  multi-node/`LinkedHashSet` ordering) in the Actor's Phase-5 memory and are
  legitimate (see assessments below). The ninth,
  `testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange`, is
  **not** in the deferred list, not `#[ignore]`, and is **single-node** (only
  `tp0`/node 0) — so the multi-node ordering rationale does not apply. It uniquely
  exercises the *second* loop of `poll_fetch` (session handlers that still hold
  `fetch_acknowledgements_to_send` for a partition dropped from the subscription)
  hitting `maybe_add_acknowledgements(is_new_session = true)` →
  `INVALID_SHARE_SESSION_EPOCH`, after a `SHARE_SESSION_NOT_FOUND` reset and a
  metadata update carrying no topics. That branch (`:1005-1016`) is implemented
  but otherwise untested — the two translated piggyback tests only cover loop-1.
- **Expected**: translate it (it is reproducible single-node), or document why
  it is skipped. Silently dropping it violates DoD item 3.

## Issue: ack-path disconnect maps to a retriable error vs Java's non-retriable UnknownServerException
- **File**: `src/consumer/internals/share_consume_request_manager.rs:2063` (`handle_share_acknowledge_failure`)
- **Severity**: Behavior Mismatch — non-blocking (rooted in the project-wide "no DisconnectException" decision, documented)
- **Java Reference**: `ShareConsumeRequestManager.java:1034` (`handleAcknowledgeErrorCode(tip, Errors.forException(error), …)`); Java test `testServerDisconnectedOnShareAcknowledge` asserts `UnknownServerException`.
- **Description**: On an in-flight ShareAcknowledge disconnect, Java derives the
  error via `Errors.forException(DisconnectException)` → `UNKNOWN_SERVER_ERROR`
  (non-retriable). Rust completes the acknowledgement with `error.error()` =
  `NetworkException` (retriable). The manager's control flow is unaffected (this
  path calls `processing_complete()` and never retries), so the only divergence
  is the exception the user's `AcknowledgementCommitCallback` observes, and its
  `is_retriable()`. The Rust test was adapted to inject `NetworkException` and
  assert it, so the translated test does not surface the difference. This is a
  documented deviation; noting it because the observable retriability flips.

## Deferred / ignored test assessments (all legitimate)
- `testFetchWithLastRecordMissingFromBatch` — **legit**. `MemoryRecords.filterTo`
  is only used to *construct* the compacted test input (a batch whose lastOffset
  exceeds its last physical record). The exercised production logic
  (acquired-range iteration) lives in `ShareCompletedFetch` (Phase 3), not the
  manager. Real (minor) coverage gap in `ShareCompletedFetch`, not a manager bug;
  revisit when `filterTo` is translated.
- `testFetchOneNodeAtATimeForRecordLimitMode` (`#[ignore]`),
  `testShareFetchWithSubscriptionChangeMultipleNodes`(+`EmptyAcknowledgements`),
  the 3 KIP-951 leadership tests (`testWhenFetchResponseReturns…`,
  `testWhenShareFetchResponseReturns…`, `testWhenLeadershipChangeBetween…`), and
  `testWhenLeadershipChangedAfterDisconnected` — **legit**. All are genuinely
  multi-node (2 brokers, per-node `prepareResponseFrom`, order-sensitive
  wire-field assertions relying on Java `LinkedHashSet` partition order). I read
  the production multi-node fetch/leadership code (`poll_fetch` per-node session
  handlers + `nodes_with_pending_requests`; `handle_share_fetch_success_body`
  `NOT_LEADER_OR_FOLLOWER`/`FENCED_LEADER_EPOCH` → `update_partition_leadership`;
  `handle_partition_error`/`update_leader_info_map`). Routing and leadership
  update are faithful to Java — the deferral is a harness-ordering limitation, not
  a masked production bug.
- `testCloseInternalClosesShareFetchMetricsManager` — **legit** (pure metrics,
  KIP-714).

## Non-findings verified (to save the next reviewer time)
- **Per-node in-flight-ack routing** (`find_in_flight_ack_slot`): sound. The
  single-in-flight-per-node invariant (`nodes_with_pending_requests`) plus
  "async/sync in flight ⟹ non-empty in_flight" and "close matched by
  `!is_processed`" make the slot unique; a close with zero acks is correctly
  matched, and a not-yet-sent close can never be picked because no response
  arrives unless something was actually sent.
- **§31 exactly-once callback**: every `maybe_send_share_acknowledgement_event`
  exit (fetch success per-partition + leftover-in-flight; response-level error;
  fetch failure; acknowledge success close/non-close/retry; timeout;
  session-not-found; leader-change in poll/commit_sync/commit_async/
  acknowledge_on_close) removes each in-flight ack exactly once (`shift_remove` /
  `std::mem::take`) before firing — no double-fire, no drop. No `tokio::spawn`.
- **§28 events**: all `CompletableApplicationEvent<T>` classes carry
  `CompletableEventHandle<T>` with the correct `T` (`Void`→`()`,
  `Map<TopicIdPartition,Acknowledgements>`→`ShareAcknowledgeSyncResult`); all bare
  `ApplicationEvent` classes are plain structs. App-side `ApplicationEvent` enum
  integration is a clean Phase-6 deferral (events exist and are correct).
- `maybe_build_request` async-sent semantics, sync-queue one-per-poll break,
  `process_acknowledgements` ordering, and `check_and_remove_completed_acknowledgements`
  all match Java.

## Verdict
Phase 5 has **one blocking bug** (COMMIT_ASYNC deadline reset with `now_ms = 0`)
that manifests only in production (masked by the near-zero test clock) and one
undocumented single-node test drop. The deferred multi-node/metrics/`filterTo`
tests are all legitimate and do not mask a production defect. Recommend fixing
the deadline reset and translating the dropped piggyback test before proceeding.

## RESOLUTION (Actor 1) — all three Phase 5 findings fixed

Fixup commit references `e0a9545`.

1. **BLOCKING deadline-reset bug** — FIXED. `processing_complete` now takes
   `now_ms` and threads it into `maybe_reset_timer_and_request_state(now_ms)` so a
   reused commitAsync state's deadline is `now + timeout` (Java's
   `timer.updateAndReset`). Audited every caller: the only `0` was
   `processing_complete`; the `maybe_build_request` expire-branch already passed
   `current_time_ms`; the session-not-found path now threads `current_time_ms`
   through `build_ack_request` → `handle_acknowledge_share_session_not_found`; the
   four response-handler / `process_retry_logic` calls pass
   `response_completion_time_ms`. New regression test
   `test_reused_async_state_deadline_uses_current_time` starts the clock at a
   production-like ~1.7e12 ms and asserts the reused async state retries instead
   of expiring; it FAILS against the `0` version (verified) and the compiler also
   now rejects a stray `0` (unused `now_ms` under `#![deny(warnings)]`).
2. **Dropped single-node test** — FIXED. Translated
   `test_piggyback_acknowledgements_on_initial_share_session_error_subscription_change`
   (added `update_metadata_no_topics` helper); it exercises `poll_fetch`'s second
   loop hitting `maybe_add_acknowledgements(is_new=true)` → `INVALID_SHARE_SESSION_EPOCH`.
3. **Ack-path disconnect retriability** — KEPT + DOCUMENTED (no equivalent). The
   network layer surfaces disconnects uniformly as `Errors::NetworkException`
   (`NetworkClientDelegate::on_complete`), there is no `DisconnectException`. Added
   an explicit code comment at `handle_share_acknowledge_failure` documenting the
   retriability divergence (Java `Errors.forException(DisconnectException)` =
   non-retriable `UnknownServerException`) and why it is acceptable (control flow
   identical — the flag is informational, and it stays consistent with the fetch
   path). `test_server_disconnected_on_share_acknowledge` asserts `NetworkException`.

Verify: `cargo build`, `cargo test --lib` (2219 pass / 1 ignore / 0 fail),
`cargo xtask format`, `cargo xtask lint` — all green.
