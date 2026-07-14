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

