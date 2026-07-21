# Critic 1 — Milestone 9 Phase 1 review — RESOLVED entries

## RESOLVED (fixup 38a3ecf): `AcknowledgementsTest` (20 methods) not translated
Original finding: the tests module in `src/consumer/internals/acknowledgements.rs`
shipped ~10 weaker hand-written tests instead of the 20 Java `AcknowledgementsTest`
methods; the loose `batches.iter().any(...)` checks would not catch an off-by-one in
`maybeOptimiseAcknowledgeTypes`.

Resolution: replaced the weak tests with faithful translations of all 20 Java @Test
methods, asserting exact firstOffset/lastOffset/acknowledgeTypes per split batch,
`testCompleteSuccess` (`complete(null)` leaves exception null + completed true), every
gap/state permutation, and the repeated second `get_acknowledgement_batches()` call
(non-destructive check). Java `add(offset, null)` maps to `add_gap`. 20 tests pass.

## RESOLVED (fixup 38a3ecf): `ShareAcquireMode.Validator` and two tests omitted
Original finding: the nested `Validator` (ConfigDef.Validator) was dropped without
rationale, taking `testValidator`/`testValidatorToString` and the `of("")` case with it.

Resolution (deferral, option b): added an explicit `deferred:` note on
`ShareAcquireMode` documenting that the `Validator` belongs to the config-wiring phase
(Phase 6) — the project has no `ConfigDef::Validator` trait yet — and that an invalid
`share.acquire.mode` is still rejected by `ShareAcquireMode::of` at
`ShareFetchConfig::from_consumer_config` time. Replaced the two weaker `of` tests with
`test_from_string` mirroring `ShareAcquireModeTest.testFromString` including the `of("")`
empty-string rejection; `of(null)` has no Rust analogue (`&str` is non-null) and is
documented as omitted.


---

# Phase 3 review — RESOLVED (fixup on 507fc3f)

Resolution summary:
- Finding 1 (corrupt-batch propagation): CRC / batch-validation failures in
  `share_completed_fetch.rs` now map to `KafkaError::with_message(Errors::CorruptMessage, ..)`
  via the new `corrupt_record_error` helper (not `illegal_state`), so the collector's
  `is_illegal_state` escape no longer treats them as always-propagating. Added regression
  test `test_corrupt_batch_after_good_records_is_swallowed` (partition A valid + partition B
  CRC-corrupt -> Ok(fetch) with A's records, B error deferred). Non-share parallel left
  untouched (out of M9 scope) with an in-code note on the helper.
- Finding 2: `test_fetch_with_other_errors` now sweeps every Kafka error code (0..=130 via
  `Errors::for_code`) minus the handled set, asserting the catch-all IllegalState arm.
- Finding 3: added a comment in `test_fetch_normal` explaining why the isInitialized/
  isConsumed lifecycle assertions are dropped (ownership: cf moved into next-in-line slot).

Original findings below.

# Phase 3 review (commit 507fc3f — share consumer fetch data path)

Reviewed: `share_completed_fetch.rs`, `share_fetch.rs`, `share_fetch_buffer.rs`,
`share_fetch_collector.rs`, `share_fetch_exception.rs`, `node_acknowledgements.rs`,
and the `share_in_flight_batch.rs` `take_in_flight_records` addition, against the
Java sources and all three Java test files.

## Issue: CRC/corrupt-batch errors map to `illegal_state`, so the collector's `IllegalState` escape-hatch propagates them instead of swallowing (Java swallows `CorruptRecordException` when records were already collected)
- **File**: `src/consumer/internals/share_completed_fetch.rs:762-769` (also `:559-566`, `:615-629`) → surfaced via `src/consumer/internals/share_fetch_collector.rs:225-233`
- **Severity**: Behavior Mismatch
- **Java Reference**: `ShareCompletedFetch.java:392-401` (`maybeEnsureValid` throws `CorruptRecordException`); `ShareFetchCollector.java:114` (`throw new ShareFetchException(fetch, cause)`) and `:121-125` (`catch (KafkaException e) { if (fetch.isEmpty()) throw e; }`)
- **Description**: In `ShareCompletedFetch`, a failed CRC / batch validation is mapped to `KafkaError::illegal_state(...)` (the `CollectLoopError::Corrupt` route, set as the `ShareInFlightBatchException` cause at `fetch_records` lines 364-374). In Java the equivalent is `CorruptRecordException`, which is a `KafkaException` (via `RetriableException`/`ApiException`). The collector's end-of-loop error handling (`share_fetch_collector.rs:229`) keys the always-propagate decision on `matches!(&e, KafkaError::IllegalState(_))`, whose stated purpose (comment at `:227-229`) is to escape **only** Java's `IllegalStateException` (the "unexpected error code" path). Because corrupt errors are mislabeled as `IllegalState`, they hit that escape hatch and propagate as `Err(ShareFetchException)` even when records were already collected — whereas Java's `catch (KafkaException e) { if (fetch.isEmpty()) throw e; }` swallows a `CorruptRecordException` when `fetch` is non-empty and returns the good records with no error.
  - Note: deserialization failures are correctly mapped to `KafkaError::serialization` (not `IllegalState`), so they are swallowed as Java's `SerializationException` would be. The mismatch is specific to the CRC/corrupt-batch path.
- **Expected**: A CRC/corrupt-batch failure should be a Kafka-exception-equivalent (e.g. `KafkaError::with_message(Errors::CorruptMessage, ...)`, as already used for the `CORRUPT_MESSAGE` init path at `share_fetch_collector.rs:320-329`) so the collector's `is_illegal_state` check does NOT treat it as always-propagating; when the collector `fetch` is non-empty the corrupt error is swallowed and the good records are returned (Java behavior).
- **Actual**: The corrupt error escapes as `Err`, so the already-collected records are surfaced only as the `ShareFetch` carried inside the error (not returned normally), and the user sees the corrupt error on this poll instead of getting records now and the error deferred.
- **Concrete failure scenario**: A single partition whose fetch payload has two batches: batch 1 valid (its records deserialize and enter `fetch`), batch 2 fails `ensure_valid` (CRC) with `check_crcs = true` (the test harness default). In `collect`: iteration 1 fetches batch-1 records → `fetch` non-empty; iteration 2's `fetch_records` fails on batch 2 with an empty in-flight batch → `reject_record_batch` + `set_exception(illegal_state)`; the collector sets `deferred_error` and breaks; the end check sees `is_illegal_state == true` → returns `Err`. Java returns `Ok(fetch)` with batch-1's records and no exception.
- **Why it matters**: This is the exact contract of Java's outer catch — deliver already-collected records and defer/drop the retriable corrupt error. Breaking it means a corrupt batch anywhere after the first good batch throws to the user (dropping/deferring good records) rather than delivering them.
- **Precedent / scope note (not a false positive, but likely systemic)**: The non-share path has the identical shape — `completed_fetch.rs:786` maps CRC to `illegal_state` and `fetch_collector.rs:349` uses the same `is_illegal_state` escape. So the same divergence may exist in the already-merged non-share collector. The fix should be evaluated for both; flagging on the share code because that is what is under review. If this was a deliberate accepted convention for the non-share path, please record the rationale so the deviation is documented (DoD §1).

## Minor note (test fidelity, non-blocking): `test_fetch_with_other_errors` narrows Java's parameterized sweep to a single error
- **File**: `src/consumer/internals/share_fetch_collector.rs:626-670`
- **Severity**: Missing Requirement (minor)
- **Java Reference**: `ShareFetchCollectorTest.java:286-297` (`@ParameterizedTest` over every `Errors.values()` not in the handled set)
- **Description**: Java exercises the catch-all `IllegalStateException` arm for *all* unhandled error codes, guarding against a newly-added error being silently miscategorized. The Rust test picks one representative (`InvalidRecordState`). The `other =>` arm is trivially correct today, so this is low-risk, but it is a reduction in the defensive coverage DoD §3 asks to preserve. Consider looping over all `Errors` variants minus the handled set (as Java does) rather than a single pick.

## Minor note (test fidelity, non-blocking): `test_fetch_normal` drops the `isConsumed()` lifecycle assertions
- **File**: `src/consumer/internals/share_fetch_collector.rs:457-482`
- **Severity**: Missing Requirement (minor)
- **Java Reference**: `ShareFetchCollectorTest.java:114,117,134` (`assertTrue(isInitialized())`, `assertFalse(isConsumed())`, then `assertTrue(isConsumed())` after the second collect)
- **Description**: Java asserts the `ShareCompletedFetch` initialize→not-consumed→consumed lifecycle across the two collects. The Rust test cannot observe this directly because the `cf` is moved into the buffer's next-in-line slot; it substitutes `has_next_in_line_fetch()`. This is an acceptable structural consequence of the ownership model (documented pattern), noted only so the reduced assertion is on record.

## Confirmed correct (spot-checks that passed)
- **Acquired-record interleave** (`collect_records` / `next_fetched_record`): the peek-then-consume + `pending_record_offset` re-parse model matches Java's `lastRecord` + `records.next()` loop. The three inner-loop arms (== parse/advance/break; < skip/break; > gap/advance) and the trailing "remaining acquired become gaps" match `ShareCompletedFetch.java:205-240`. All 10 Java `ShareCompletedFetchTest` methods are translated, including the overlapping-dedup first-occurrence and odd/gap cases. Control-batch whole-batch skip is equivalent to Java's per-control-record skip (control batches contain only control records); the zero-record control-batch test deviation is sound and documented.
- **§27 zero-copy**: single owned buffer moved (not copied) into the cursor; `topic_arc: Arc<str>` cloned per record; decompress-once into `RecordSource::Owned`; key/value borrowed via `read_ref_from_buffer`. The per-record allocation-budget test is a genuine bound (≤5/record + 120 overhead; also asserts ≥1/record so it can't pass vacuously).
- **`ShareFetch::add`** reads `get_acquisition_lock_timeout_ms()` before the consuming `merge` — verified `merge` does not change the timeout, so the pre-merge read equals Java's post-merge read (carryover note #1 correct).
- **`NodeAcknowledgements`** exists in Java (used by `ShareFetch.takeAcknowledgedRecords`); the Rust type is a faithful, minimal translation (DoD §7 satisfied).
- **`ShareFetchException`** faithfully unifies Java's two exit paths (bare `KafkaException` from `initialize`, `ShareFetchException` from the records branch); callers get `cause()` + the carried `ShareFetch` via `into_parts()`.
- **`ShareFetchBuffer`**: no `MutexGuard` held across `.await` in `await_not_empty` (CLAUDE.md §9.6); `close`/`wakeup`/`buffered_partitions`/`set_next_in_line_fetch` (no-drain) match Java. All 4 Java `ShareFetchBufferTest` methods translated. (Trivial: the already-closed warn text says "share fetch buffer" vs Java's "fetch buffer" — cosmetic, not reported as a defect.)

## Verdict
Phase 3 is **substantially clean**. One real behavioral divergence (corrupt-batch
error propagation vs Java's swallow-when-records-collected) that mirrors the
non-share precedent and should be resolved or explicitly documented; two minor
test-fidelity notes. No zero-copy violations, no missing methods, no missing test
translations.

---

# RESOLVED (fixup 9737fdb) — Milestone 9 Phase 4 review

Resolution: the reconcile pipeline is a hand-duplicated copy (not shared —
AbstractMembershipManager has no reconcile). The inaccurate "shared pipeline"
rationale was corrected, and three ShareMembershipManagerTest cases were
translated against the share reconcile copy, exercising the previously-untested
revoked diff, mark_pending_revocation, onPartitionsRevoked-before-assigned
ordering, other-partitions-owned add-only path, and the same-assignment
short-circuit: reconcile_new_partitions_assigned_and_revoked,
reconcile_new_partitions_assigned_when_other_partitions_owned,
reconciliation_skipped_when_same_assignment_received. Lib tests 2165 -> 2168.

# Critic 1 — Milestone 9 Phase 4 review (share membership + heartbeat + metadata)

Commit `44b8d40`. Files: `share_membership_manager.rs`,
`share_heartbeat_request_manager.rs`, `share_consumer_metadata.rs`.
Java refs under `clients/.../consumer/internals/`.

**Phase 4 is substantially clean — one test-coverage finding (Missing
Requirement), no correctness bugs.** The three managers faithfully mirror the
consumer cousins (`ConsumerMembershipManager`, `ConsumerHeartbeatRequestManager`,
`ConsumerMetadata`) and the Java sources. All 34 new tests pass.

## Issue: share reconcile revocation / other-partitions-owned / same-assignment paths are untranslated and the deferral rationale is factually wrong
- **File**: `src/consumer/internals/share_membership_manager.rs:459-632` (reconcile), doc-comment `:800-804`
- **Severity**: Missing Requirement (test coverage)
- **Java Reference**: `ShareMembershipManagerTest.java:1044` (`testReconcileNewPartitionsAssignedAndRevoked`), `:988` (`testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned`), `:1009` (`testReconciliationSkippedWhenSameAssignmentReceived`)
- **Description**: The Actor's doc comment justifies skipping the metadata /
  reconcile test families by asserting they are "behaviorally identical to the
  `ConsumerMembershipManager` reconcile tests (Phase 34) **since the reconcile
  pipeline is shared**." That premise is false at the Rust level. There is **no**
  `reconcile` (or `maybe_reconcile` / `revoke_and_assign`) method in
  `abstract_membership_manager.rs` (verified by grep) — the ~170-line reconcile
  pipeline is **duplicated by hand** into both `consumer_membership_manager.rs`
  and `share_membership_manager.rs`. The share copy carries its own
  revoked-vs-added set-difference (`:519-520`), `mark_pending_revocation`
  (`:534`), the `onPartitionsRevoked` await (`:541-556`), and the
  same-assignment short-circuit (`:492-501`). None of those branches is
  exercised by any share test: the only reconcile test
  (`reconcile_new_partitions_assigned_when_no_partition_owned`) starts from an
  empty owned set, so `revoked` is always empty and the short-circuit never
  fires. A transposition of `added`/`revoked`, a wrong argument to
  `mark_pending_revocation`, or a broken short-circuit in the share copy would
  compile and pass the current suite.
- **Expected**: Either translate `testReconcileNewPartitionsAssignedAndRevoked`,
  `testReconcileNewPartitionsAssignedWhenOtherPartitionsOwned`, and
  `testReconciliationSkippedWhenSameAssignmentReceived` against the share
  manager (they need an owned assignment first, then a differing target), OR
  correct the doc-comment rationale to state that the reconcile pipeline is a
  *duplicated copy* and explicitly acknowledge these branches are untested in
  the share module. Given the code is copied, translating at least the
  revoked-path test is the safer choice.
- **Actual**: Deferred with a "shared pipeline" rationale that does not hold;
  revoked / owned-partitions / short-circuit reconcile branches have zero
  coverage in the share module.

## Non-findings verified (recorded to save the next reviewer time)
- **State machine parity**: `on_heartbeat_success` matches Java line-for-line —
  LEAVING / UNSUBSCRIBED+epoch<0 / `is_not_in_group` / epoch<0 early-outs, then
  `update_member_epoch` + `can_handle_new_assignment` gate + `process_assignment_received`.
  The captured `state` local (pre-epoch-update) is used for `can_handle_new_assignment`
  exactly as Java. Empty-assignment (`Some(empty_map)`) → RECONCILING is correct.
- **`transition_to_fenced` / `transition_to_fatal` / `transition_to_stale`**
  are faithful copies of the consumer cousins (which duplicate
  `AbstractMembershipManager.transitionTo{Fenced,Fatal,Stale}`). `resetEpoch()`
  → `update_member_epoch(join_group_epoch()=0)`; the empty-partitions guard on
  `onPartitionsLost` is behaviourally equivalent to Java's unconditional
  `signalPartitionsLost(emptySet)`.
- **No auto-commit-before-rebalance step** — CONFIRMED against Java. Java
  `AbstractMembershipManager` ctor's last param is `autoCommitEnabled`; share
  passes `false` (`ShareMembershipManager.java:111`), and share does NOT override
  `signalReconciliationStarted` (that override, with `maybeAutoCommitSyncBeforeRebalance`,
  is consumer-only). The Rust reconcile correctly omits steps 5 (`can_commit` gate)
  and 8a (auto-commit flush).
- **`is_leaving_group()`** correctly uses the base (`PREPARE_LEAVING | LEAVING`),
  no static-member / remain-in-group override — matches Java (share has no
  `groupInstanceId` / `leaveGroupOperation`).
- **Heartbeat build/response**: `build_request_data` field-diff matches Java —
  groupId/memberId/memberEpoch always sent, rackId once (`None` re-set-to-`None`
  is harmless, as Java `setRackId(null)`), subscribedTopicNames on JOINING or
  change. (The Rust stores/sends the *sorted* topic list where Java sends
  `subscription()` iteration order; broker is order-insensitive and the diff is
  order-independent in both — not a bug.) `poll` mirrors
  `AbstractHeartbeatRequestManager.poll` order-for-order (skip → poll-timer-expiry
  → heartbeat-now); `on_response`/`on_failure` match the consumer variant.
- **`UNSUPPORTED_VERSION` → share messages are reachable, not dead code**:
  `classify_response_error` returns `DelegateToSpecific` for `UnsupportedVersion`
  (falls to `_` arm), so `handle_specific_exception_in_response` applies
  `SHARE_PROTOCOL_NOT_SUPPORTED_MSG` (broker-side) and `handle_specific_failure`
  applies `SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG` (client-side). Both tested.
- **`tokio::spawn` forwarder + mpsc channel-back**: identical to
  `consumer_heartbeat_request_manager.rs` (`PendingHeartbeatCompletion` /
  `PendingMembershipTransition`). The per-heartbeat spawn is NOT a per-record hot
  path — it matches the accepted consumer precedent (CLAUDE.md §11 /
  consumer-threading.md §10). No new spawning introduced.
- **§16**: `SubscriptionState` is `Arc<std::sync::Mutex>`; guards are dropped
  before every `.await` (reconcile drops `guard`/`subs` before each
  `invoke_rebalance_callback`; `on_heartbeat_success` drops before
  `process_assignment_received`). No guard held across an await.
- **Rule compliance**: internals `pub(crate)`; member epoch is `i32` (correct —
  Java `memberEpoch` is `int`, not `long`); Apache-2.0 (Confluent Inc) headers;
  one class per file; parent-module re-export imports; `// metrics: deferred to
  KIP-714` at omitted sites.
- **`ShareConsumerMetadata`** faithfully mirrors Java (`newMetadataRequestBuilder`
  scoped to `metadataTopics()`, `retainTopic` = `needsMetadata`,
  `allowAutoTopicCreation`), composing `Metadata` + `MetadataOverrides` like
  `ConsumerMetadata`. Java has no `ShareConsumerMetadataTest`; the 3 hand-written
  tests are appropriate.

## Deferred-test assessment (all defensible EXCEPT the reconcile family above)
- **Mockito-spy-on-internals** (`verify(...never()).markReconciliationInProgress()`,
  etc.): genuinely untranslatable (no mocking of a concrete struct) — accepted
  consumer precedent. BUT note it overlaps the reconcile-coverage gap above:
  spy-based Java tests are the ones that exercised the revoked path.
- **Leave-future completion** (`maybeCompleteLeaveInProgress` on the
  `CompletableFuture` leave result): deferred. VERIFIED not a share regression —
  the consumer path also drops it (grep finds no `maybe_complete_leave_in_progress`
  anywhere), and the LEAVING→UNSUBSCRIBED transition is driven by
  `on_heartbeat_request_generated` (tested), not the response. close()/unsubscribe()
  correctness on the leave *future* is a Phase-5/6 wiring item for BOTH managers,
  not Phase 4.
- **`onComplete`/`onFailure` round-trip**: the classification helpers
  (`on_response`, `on_failure`, `handle_specific_*`) are unit-tested directly; the
  full spawned-forwarder round-trip is deferred to Phase 5/6 bg-loop integration
  (same as consumer Phase-12.5). Does not mask a broken path — the response body
  extraction (`ConcreteResponse::ShareGroupHeartbeat`) and error routing were
  inspected and match the consumer forwarder.
- **KIP-714 metrics**: out of scope.

## Verdict
Phase 4 is clean to proceed **with one Missing-Requirement finding** (share
reconcile revoked/owned/short-circuit branches untested, deferral rationale
factually wrong). No correctness bugs, no rule violations, no false semantics.
The finding is a test-coverage / documentation-accuracy item, not a runtime
defect — the reconcile code itself reads as a correct copy of the reviewed
consumer pipeline.

---


---

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

---

# Critic 1 — Milestone 9 Phase 6 BLOCKING findings — RESOLVED

Both blocking findings from the Phase 6 review are fixed (fixups `225dd26`,
`b637a27` on `milestone9-share-consumer`).

## RESOLVED: `acknowledge(record, RENEW)` silently lost record re-delivery
- **Fixup**: `225dd26` (fixup! blocker 1, `ff3950f`).
- **Resolution**: RENEW re-delivery is now implemented faithfully. `acknowledge(_,
  RENEW)` captures a CLONE of the renewed record (new
  `ShareInFlightBatch.renew_records` + a gated `ConsumerRecord: Clone` derive) —
  the rare path only; the hot ACCEPT/RELEASE/REJECT path records only the offset
  (no clone, §27/§11-compliant). `take_acknowledged_records` routes the captured
  clone (or the still-in-flight object) into `renewing_records`; `renew` /
  `take_renewals` cycle it back into in-flight for re-delivery on a later poll,
  matching Java (`ShareInFlightBatch.java:115-184`, `ShareConsumerImpl.java:709-724`).
  `ShareConsumerImpl<K,V>` gains a share-only `K/V: Clone` bound.
  `test_explicit_mode_renew_and_acknowledge_on_poll` is un-`#[ignore]`d and passes,
  and additionally asserts a post-renew `acknowledge(rec, ACCEPT)` on the
  re-delivered record succeeds (would `Err` if offset tracking had been dropped).

## RESOLVED: `RequestManagers::entries()` share poll order reversed
- **Fixup**: `b637a27` (fixup! blocker 3, `7623a72`).
- **Resolution**: `entries()` now polls `share_heartbeat` BEFORE `share_consume`,
  matching Java's share order `shareHeartbeat → shareMembership → shareConsume`
  (`RequestManagers.java:123-128`), so the consume manager acts on membership
  state already advanced by the heartbeat manager in the same `run_once` iteration
  (§10). Added a `share_membership: Option<Arc<ShareMembershipManager>>` slot
  (Arc-shared with `share_heartbeat`, the analog of
  `consumer_membership`/`consumer_heartbeat`), skipped from `entries()` like
  `consumer_membership` (a `&mut dyn RequestManager` cannot be produced from a
  shared `Arc`); its standalone reconcile driving is tracked for Phase 7. The
  misleading `share_consume` field doc/comment is corrected.

Verify: `cargo build`, `cargo test --lib` (2250 pass / 1 ignore / 0 fail),
`cargo xtask format`, `cargo xtask lint` — all green.

---

# Critic 1 — Phase 6 fixup RE-REVIEW regression — RESOLVED

## RESOLVED: `collect()` in-place restructuring clobbered `acquisition_lock_timeout_ms` on an empty collect
- **Fixup**: `daf4c47` (fixup! `373f307` — the ShareConsumerImpl impl commit; the
  regression was introduced by the in-place `collect` restructuring in `225dd26`).
- **Resolution**: `collect`'s first (non-renewal) branch now assigns
  `self.current_fetch = fetch` ONLY when the freshly collected fetch is non-empty,
  matching Java's `poll` guard (`ShareConsumerImpl.java:628-629`). An empty collect
  leaves `current_fetch` untouched, so it retains the `acquisition_lock_timeout_ms`
  from the last non-empty fetch (which survives `take_records` /
  `take_acknowledged_records`). Renewal-state handling is unchanged (branch gated on
  `!has_renewals()`). Added
  `test_acquisition_lock_timeout_retained_across_empty_poll`: poll returns records
  (asserts `Some(30_000)`), then an empty poll still returns `Some(30_000)` (would be
  `None` under the regressed code).

Verify: `cargo build`, `cargo test --lib` (2251 pass / 1 ignore / 0 fail),
`cargo xtask format`, `cargo xtask lint` — all green.


# Critic 1 — Milestone 9 Phase 7 review: RESOLVED findings (fixups `e1f8a8e`, `89e15f4`)

Both NON-BLOCKING Phase-7 findings are resolved and moved here from `COMMENTS.1.md`.

## Finding 1 — bg-loop share reconcile orchestration now has a test with teeth — RESOLVED (fixup `89e15f4`)

Added `run_once_drives_share_membership_reconcile_and_member_id_propagation`
(`consumer_network_thread.rs`): wires a share `ConsumerNetworkThread` via
`RequestManagers::for_share` + `set_share_membership`, receives an (empty)
assignment so the member is `Reconciling`, drives ONE `run_once`, and asserts
(a) `share_membership.reconcile(now)` advanced `Reconciling -> Acknowledging`
(Phase 2.5s) and (b) the member id was propagated into the
`ShareConsumeRequestManager` (`propagate_share_member_id`, Phase 2.4s). Verified
the test FAILS if the `reconcile(now)` call, the `propagate_share_member_id`
call, or the `set_share_membership` wiring is removed (the
`if let Some(share_membership)` block is then skipped: state stays `Reconciling`,
member id stays `None`). Remaining broker-only gap documented on the test: the
PRODUCTION `mod.rs` `set_share_membership` line's end-to-end group JOIN needs the
deferred MockClient request-matcher harness / a share-group-enabled broker; this
test guards the mechanism that line feeds. New test accessors:
`ShareConsumeRequestManager::member_id_for_test`,
`RequestManagers::share_consume_member_id`.

## Finding 2 — ack-failure completion time — RESOLVED (fixup `e1f8a8e`)

The `PendingShareCompletion::AckFailure` variant now carries
`response_completion_time_ms`, captured in the spawned forwarder at the instant
the failure resolves (the time source is cloned into the forwarder; the
wrong-response-body case reuses the `ClientResponse` received time). The drain
threads it into `handle_share_acknowledge_failure` instead of the drain-time
`self.time.milliseconds()`, matching Java's `handler().completionTimeMs()` used
for BOTH the success and failure paths.

Original finding text follows:

## NON-BLOCKING — Issue: the load-bearing bg-loop share reconcile orchestration has no test with teeth
- **File**: `src/consumer/internals/consumer_network_thread.rs:605-663` (Phase 2.4s/2.5s),
  `src/consumer/internals/request_managers.rs:262-266` (`propagate_share_member_id`),
  `src/consumer/mod.rs:794` (`set_share_membership`)
- **Severity**: Missing test coverage (DoD / test-teeth)
- **Java Reference**: `ConsumerNetworkThread.runOnce` / share `RequestManagers.entries()`
- **Description**: Commit `e50dc7e` is described as "load-bearing, must let the
  consumer JOIN." The `reconcile` logic itself is well unit-tested in isolation
  (`share_membership_manager.rs`, 18 reconcile calls in its test module), and the
  response-routing handlers are unit-tested (`test_response_routing_fetch_success_path`).
  But the Phase-7 **orchestration** that ties them together — `set_share_membership`,
  `propagate_share_member_id`, `take_pending_share_membership_transitions`, and the
  per-iteration `share_membership.reconcile(now)` in `run_once` Phase 2.4s/2.5s — is
  exercised by NO automated test. `set_share_membership` has exactly one caller
  (`mod.rs:794`, production); `for_share` and `propagate_share_member_id` have zero
  test references. The only test touching the real bg loop is the mod-level smoke test
  `new_share_consumer_builds_working_pipeline_and_subscribes`, and its `subscribe`
  completes through `ApplicationEventProcessor::process_share_subscription_change`
  (`application_event_processor.rs:1246-1273`), which updates `SubscriptionState` +
  calls `membership.on_subscription_updated()` and completes the handle synchronously
  — **it never depends on `reconcile` being driven, nor on `set_share_membership`
  having been called** (`membership` is obtained via `share_heartbeat.membership_manager()`,
  present regardless of the bg-loop wiring).
- **Failure scenario**: If the `set_share_membership(...)` call at `mod.rs:794` were
  dropped, or the Phase 2.4s/2.5s block were skipped/mis-ordered, the smoke test would
  STILL PASS — the KIP-932 membership state machine would never advance and the consumer
  would never join, but no test would catch it. The milestone therefore has no automated
  proof that the central "share consumer can JOIN" capability works end-to-end; the
  join handshake is only reachable against a live share-group broker (integration tests
  `#[ignore]`d for AdminClient) or a request-matching MockClient (`KafkaShareConsumerTest`
  deferred — see below). Recommend a bg-loop unit test that installs a fake/spy
  `ShareMembershipManager` via `set_share_membership` and asserts `reconcile` is invoked
  each `run_once` iteration (analog of the consumer-path reconcile driving), so a
  regression in the orchestration is caught.

## NON-BLOCKING — Issue: ack-failure path uses drain-time instead of response-completion time
- **File**: `src/consumer/internals/share_consume_request_manager.rs:455-462` (drain,
  `AckFailure` arm passes `self.time.milliseconds()`)
- **Severity**: Behavior Mismatch (minor)
- **Java Reference**: `ShareConsumeRequestManager.java:1263` —
  `handleShareAcknowledgeFailure(..., unsentRequest.handler().completionTimeMs())`
- **Description**: The success path captures the real response completion time in the
  spawned forwarder (`client_response.received_time_ms()`, line 1073) and threads it
  through `response_completion_time_ms`. The failure path's `PendingShareCompletion::AckFailure`
  variant carries no timestamp, so the drain passes `self.time.milliseconds()` (drain
  time) into `handle_share_acknowledge_failure` → `on_failed_attempt(...)`, which seeds
  the retry-backoff deadline. Java uses `handler().completionTimeMs()` for BOTH success
  and failure. Because the forwarder fires `notify_one()` immediately and the next
  `run_once`/drain follows within microseconds, the divergence shifts the retry deadline
  by well under a millisecond — negligible in practice, but it is an internal
  inconsistency (success captures real time, failure does not) and a divergence from
  Java. Not blocking. (The forwarder has no `ClientResponse` on the transport-failure
  path, so a faithful fix would capture `self.time` into the forwarder closure.)

