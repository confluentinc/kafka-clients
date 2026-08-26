# Milestone 13 — Bring the Rust client up to Apache Kafka 4.3.1

**Status:** APPROVED 2026-08-26 (user-approved plan; phases execute via Manager → Actor → Critic loop)

**Branch:** `milestone-12-ak-4.3.1` (off `master` @ `7e9d1df`)

**Java source:** Apache Kafka **4.3.1** (`kafka/` submodule to be moved from `a18251b` = 4.2.0 to `26b251a451ce941d3d7a55e6487bcb7f16b5ad48` = tag `4.3.1` in Phase 0). Until Phase 0 lands, diffs below are expressed as `git diff 4.2.0..4.3.1` inside the submodule.

**Agent numbers:** 60–66 (Phase N → agent 6N; COMMENTS.0–4.md are in use by another workstream — do not touch them).

## 1. What this milestone delivers

The Rust translation currently tracks AK 4.2.0. This milestone translates the 4.2.0 → 4.3.1 Java clients delta for every Java file that has a Rust counterpart, syncs the wire-spec corpus (`generator/messages/`) to 4.3.1, and updates the reference pin — producing a client in line with AK 4.3.1.

Measured delta (in `kafka/`): client main code 188 files (+3447/−1430), tests 117 files (+6393/−1154); **81 unique commits** (`git log --cherry-pick --right-only 4.2.0...4.3.1 -- clients/src/main/java/org/apache/kafka/clients clients/src/main/java/org/apache/kafka/common clients/src/main/resources/common/message`). ~74 changed files have Rust counterparts; the rest are untranslated areas or broker-only.

### 1.1 Scope decisions (user-confirmed 2026-08-26)

- **Skip untranslated areas**: Share consumer (KIP-932), Streams-integration consumer internals, Classic consumer/Coordinator, OAuth (`oauthbearer`, incl. new KAFKA-18608 client assertion), telemetry (KIP-714), Schema runtime (`protocol/types` beyond the minimal `types.rs`), `ConfigDef` framework, compression `Compression` hierarchy, monolithic `Utils`/`Bytes`/`Shell`, `ProducerInterceptors`, broker-only code and specs. These have no Rust counterpart to drift; new features there are future milestones.
- **Full sync of `generator/messages/` to the 4.3.1 specs.** Verified: the corpus already matches 4.2.0 exactly (the "36/197 drift" claim in `.claude/agent-memory/kafka-critic/review_spec_corpus_two_sources.md` and `kafka-critic/MEMORY.md` is STALE — correct it in Phase 0). The 4.3.1 delta is 10 modified specs + 1 new (`ControlRecordTypeSchema.json`) + README.
- **CLAUDE.md "Source Reference" line edit (4.2 → 4.3.1) is human-approved via this plan** (agents otherwise avoid CLAUDE.md changes). Only that line; any other CLAUDE.md/rules change goes through the suggestion process.

### 1.2 Method: per-subsystem tree diff, commit list as checklist

Actors translate from the tree diff (`git diff 4.2.0..4.3.1 -- <files>`), NOT commit-by-commit replay. Each phase section lists its covered KAFKA-xxxxx commits so the Actor has the rationale and the Critic can audit completeness. Phase 6 closes with an audit: every one of the 81 in-scope commits maps to a phase or an explicit skip-with-reason entry appended to this file.

## 2. Key findings that shape the phases

1. **Consumer rebalance handshake changed shape (KAFKA-20106/20321/20332)** — largest, riskiest piece:
   - 4.2: bg thread applied the reconciled assignment itself, then enqueued `ConsumerRebalanceListenerCallbackNeededEvent` only if a listener existed; app replied `CallbackCompletedEvent`.
   - 4.3.1: bg reconcile ends with `signalPartitionsAssigned(assignedPartitions, addedPartitions)` → **`PartitionsAssignedEvent`** (bg→app, completable, sent even with NO listener, carries full assignment + added set). The app thread, inside `poll()`, sends **`ApplyAssignmentEvent`** (app→bg, completable) and awaits it — the `SubscriptionState` mutation (`assignFromSubscribedAwaitingCallback`) still executes on the bg side but is now triggered and awaited by the app thread, guaranteeing `consumer.assignment()` changes only within `poll()`. Then the app runs `onPartitionsAssigned` and replies `CallbackCompletedEvent`. Revoke/lost keep the 4.2 shape renamed **`PartitionsRemovedEvent`**; lost partitions are marked pending-revocation *before* callbacks (fetch pause). `AsyncPollEvent` gains `markReconciliationCheckComplete()` / `maybeReconcile(canCommit)` gating; `processBackgroundEvents` gains a `skipAssignmentEvents` flag so non-poll APIs don't apply assignments.
   - Rust: reshape `consumer_membership_manager.rs`'s stored-receiver machine (`drive_pending_reconcile` / `continue_after_assign`) into the three-leg handshake; add `APPLY_ASSIGNMENT` handling to `ApplicationEventProcessor`. The Phase-41 "bg loop keeps spinning during callbacks" design is exactly what makes the app-side await of `ApplyAssignmentEvent` deadlock-free — architecture holds, but **consumer-threading.md §28 (event tables) and §31 (handshake steps) need amending**: Phase 4's Critic drafts the amendment text in its COMMENTS file; the human applies/approves the rules edit.
2. **`common/record` → `common/record/internal` (KAFKA-20128)**: only `TimestampType` stays public. Mirror fully: Rust module `common::record::internal`, `pub(crate)` per the naming rule. Verified zero uses of `common::record` in `src/ffi`, `src/bin`, examples, benches, bindings. `RecordValidationStats` left the client (→ storage): delete the Rust counterpart if present. `ControlRecordType` now reads the generated `ControlRecordTypeSchema` (KAFKA-10863); the generator already handles `"type": "data"` specs.
3. **Producer 2PC public-API revert (c41ff4de0e)** is N/A for the public surface (Rust never exposed `prepare_transaction`/`complete_transaction`/`init_transactions(bool)`); only small `TransactionManager` internal deltas remain. MockProducer's +80 is javadoc + 2PC removals + one `TimeoutException` message change (message text asserted per DoD).
4. **Nothing removed under Rust's feet**: `ConsumerGroupMetadata` unchanged; keep `#[deprecated(since="4.2.0")]` items. `ConsumerRecords` KAFKA-20660 (tainted Map-ctor detection) N/A — Rust never had that ctor.
5. **KIP-1251 client-side is 5 lines** in `ConsumerGroupHeartbeatResponse.java` (rest broker-side).
6. **Bindings (C/Python/FFI) impact: none.** No exposed contract changed.

## 3. Phases

Each phase: Actor (agent 6N) implements & commits per-step; Critic (agent 6N) reviews into `COMMENTS.6N.md`; Actor fixes and moves items to `COMMENTS.DONE.6N.md`; loop until clean. Per-phase gates: `cargo build`, `cargo test`, `cargo xtask lint`, `cargo xtask format-check`.

### Phase 0 — Reference bump + spec corpus + bookkeeping (agent 60)

- Move `kafka/` submodule to tag `4.3.1` (`26b251a451`); commit gitlink.
- Sync `generator/messages/` from `kafka/clients/src/main/resources/common/message/` (10 modified + `ControlRecordTypeSchema.json` + README.md); regenerate; fix any compile fallout (notably `OffsetFetchResponse` — keep the crate compiling ahead of Phase 3, adapting the Rust wrapper minimally if the generated shape changed; the full behavioral translation of the wrapper lands in Phase 3).
- Update `README.md:7` and `CLAUDE.md:93` "(Apache Kafka 4.2)" → "(Apache Kafka 4.3.1)".
- Correct the stale corpus-drift claims in `.claude/agent-memory/kafka-critic/review_spec_corpus_two_sources.md` and `.claude/agent-memory/kafka-critic/MEMORY.md`.
- Rules line-number errata survey: check the Java `file:line` citations in `.claude/rules/producer-transactions.md` (and any in `consumer-threading.md` / `admin-client.md`) against the 4.3.1 tree; write an errata list to `design/history/Milestone-13/rules-errata.md` (do NOT edit the rules files — suggestions only, per agent-roles).

### Phase 1 — common (agent 61)

- `record` → `record::internal` module move (R090–99, content ~unchanged) + `pub(crate)`; keep `TimestampType` in `common::record`; carry over the Java package-info docs. If a crate-external test needs a moved type, prefer moving the test in-crate; otherwise document a visibility deviation per DoD #7.
- `ControlRecordType` via generated `ControlRecordTypeSchema` + `ControlRecordTypeTest` (+118).
- Delete `RecordValidationStats` counterpart if present (moved to storage module).
- Deltas where counterparts exist: `Readable`/`Writable`/`SendBuilder`, `RecordHeaders`, `types.rs` (most of Type.java's +180/−132 is the untranslated Schema runtime → skip-with-reason), requests wrappers small deltas (`FetchRequest`/`FetchResponse`, `ProduceRequest`/`ProduceResponse`, `ListOffsetsResponse`, `OffsetsForLeaderEpochResponse`, `RequestUtils`, `InitProducerIdRequest`, `TxnOffsetCommitRequest`), `PartitionInfo`, `TopicPartitionInfo`, `GroupState` (annotation-only), feature/`FeaturesTest` delta, `ProducerIdAndEpoch`.
- Commits covered: KAFKA-20128, KAFKA-10863, KAFKA-20130, KIP-1247 portions with counterparts (mostly N/A — note reasons), 9c62b8c9f7 (byte[] bounds check) where applicable.

### Phase 2 — producer (agent 62)

- `MockProducer` (+80: javadoc, 2PC removals, `TimeoutException` message change — assert message text), `ProducerConfig` (+26), `KafkaProducer` (+19/−26), `TransactionManager` (+24/−6 internal 2PC revert), `ProducerBatch`/`Sender`/`RecordAccumulator`/`ProduceRequestResult`/`BufferExhaustedException`(KafkaError docs) tiny deltas.
- Tests: `TransactionManagerTest` (+71/−41), `SenderTest` (+18/−15), `KafkaProducerTest` (+13/−5), `RecordMetadata` delta.
- Commits covered: c41ff4de0e (2PC revert), plus small producer MINORs from the 81-commit list.
- Recorded skips:
  - `SenderTest.testAppendInExpiryCallback` (SenderTest.java:414-430): not translated — pre-existing M8 skip (never carried into the current tree). Its sole 4.3.1 delta is the `SENDER_TIMEOUT_MSG` assertion at :430; that behavior (suffixed expired-batch message) is covered by the four translated batch-expiry tests (`test_transition_to_abortable_error_on_batch_expiry`, `_multiple_batch_expiry`, `test_drop_commit_on_batch_expiry`, `_fatal_error_when_retried_batch_is_expired`).
  - `TransactionManagerTest.testDropCommitOnBatchExpiry` 4.3.1 second `SENDER_TIMEOUT_MSG` assertion on the commit result's cause (TransactionManagerTest.java:2986): not translatable — the flat `KafkaError` replaces the retriable batch-expiry message and drops the cause chain (§10.5 deviation 5); documented at the skip site in `test_drop_commit_on_batch_expiry`.

### Phase 3 — consumer offsets/commit (agent 63)

- `CommitRequestManager` (+183/−36; KAFKA-20165 retriable partition errors — new Java inner `OffsetFetchResult` type is legitimate per DoD #7), `OffsetFetchRequest`/`OffsetFetchResponse` wrappers (spec already synced in Phase 0), `OffsetsRequestManager`, `OffsetFetcherUtils` (+41), `offsets_for_leader_epoch_client` delta, `ConsumerGroupHeartbeatResponse` 5-liner (KIP-1251 client side).
- The `FetchCommittedOffsetsEvent` hunk of `ApplicationEventProcessor` is explicitly assigned HERE (Phase 4 also edits that file — avoid overlap).
- Tests: `CommitRequestManagerTest` (+20/−20), `OffsetsRequestManagerTest` (+12/−8).
- Commits covered: KAFKA-20165 (5610f3af0c), c7c7bb72c6 (KIP-1251 client hunk).
- Recorded skips:
  - `OffsetFetchResponse.java` (OffsetFetchResponse.java:36): whole 4.3.1 delta is the import line `RecordBatch.NO_PARTITION_LEADER_EPOCH` → `record.internal.RecordBatch` — the Phase-1 record module move; the Rust wrapper is behaviourally unaffected. No Rust change.
  - `OffsetsForLeaderEpochUtils.java` (OffsetsForLeaderEpochUtils.java:27): whole 4.3.1 delta is the `record` → `record.internal` import line. N/A for `offsets_for_leader_epoch_client.rs` (no behaviour change).
  - `OffsetFetcher.java` (OffsetFetcher.java:127-238): the classic-consumer `OffsetFetcher` is untranslated (consumer-threading.md §20). Its 4.3.1 additions (`currentLag`, and threading `updatePartitionEndOffsetsFlag` through `fetchOffsetsByTimes`/`beginningOrEndOffset` to clear the end-offset-requested flag on `LIST_OFFSETS` failure) have no async-consumer counterpart — the async lag path is inline in `ApplicationEventProcessor::process_current_lag`. The two reusable `OffsetFetcherUtils` helpers `OffsetFetcher` calls (`maybeSetPartitionEndOffsetRequest`/`clearPartitionEndOffsetRequests`) ARE translated into `offset_fetcher_utils.rs`.
  - `OffsetFetcherTest.java` (OffsetFetcherTest.java:851): cosmetic `Utils.mkMap` → `Map.of` refactor in the classic-consumer `OffsetFetcher` test (untranslated, §20).
  - `OffsetFetchRequestTest.java` / `OffsetFetchResponseTest.java`: import-only `record` → `record.internal` (Phase-1 record move); no behavioural test change.
  - `OffsetFetchRequest.requestAllOffsets` translated but its only Java callers are broker-side (`GroupCoordinatorService`/`GroupCoordinatorShard`, out of scope); the Rust method is exposed for wire-parity only, covered by a unit test.
  - `maybeUpdateLastSeenEpochIfNewer(res.offsets())` (CommitRequestManager.java:583,632) is applied downstream in `OffsetsRequestManager::refresh_offsets` rather than inside the Rust fetch retry driver — the pre-4.3.1 translation already located that call at the caller. Documented at the driver site. NOTE this is **not** fully equivalent to Java: `refresh_offsets` is reached only by the `updateFetchPositions` path and is gated on `currently_initializing`, so the public `committed()` path never refreshes the leader-epoch cache that Java refreshes on every fetched offset (pre-existing divergence — see §5).

### Phase 4 — consumer rebalance/poll (agent 64) — largest, highest risk

- Event refactor per §2.1: `PartitionsRemovedEvent` (rename), new `PartitionsAssignedEvent` + `ApplyAssignmentEvent`, `AsyncPollEvent` gating, `ApplicationEvent`/`BackgroundEvent` base deltas, `ApplicationEventProcessor` `APPLY_ASSIGNMENT`.
- `AbstractMembershipManager` / `ConsumerMembershipManager` / `MemberStateListener`, `AsyncKafkaConsumer` (+187/−28: KAFKA-20535 CPU fix, KAFKA-20426 group.id+assign busy loop, KAFKA-20428 unsubscribe fix, `skipAssignmentEvents`), `SubscriptionState`, `WakeupTrigger` (fdece9c358), `ConsumerNetworkThread`, `AbstractFetch`/`FetchCollector`/`CompletedFetch`/`FetchMetricsManager` deltas, `Fetch.forPartition` mutable-maps fix (9945592afc), `ConsumerRecord`/`ConsumerRecords` deltas (KAFKA-20660 portions N/A — Rust never had the Map ctor), `MockConsumer`, `ClientUtils` (drop unused overload), `Metadata` delta.
- Tests: `AsyncKafkaConsumerTest` (+331/−17), `ConsumerMembershipManagerTest` (+199/−42), `KafkaConsumerTest` (+250/−22, classic-only slices skipped with reason), `ApplicationEventProcessorTest`, `WakeupTriggerTest` (+33), `ConsumerHeartbeatRequestManagerTest` (+78), `FetchTest` (+141), `ConsumerRecordsTest` (+65, N/A portions noted), `SubscriptionStateTest` delta.
- Deliverable: drafted amendment text for consumer-threading.md §28/§31 (in `design/history/Milestone-13/rules-errata.md`; human applies).
- Commits covered: KAFKA-20106 (71449aabb6, aa736157d1), KAFKA-20321 (8dad4f93e9), KAFKA-20332 (6b05369445, 5d6248c448), KAFKA-20382 (54d6e39fa6), KAFKA-20426 (e1a062cc07), KAFKA-20428 (67ae18eaae), KAFKA-20535 (1e27a205fa), KAFKA-20660 (0aa8462cdf), KAFKA-20309/20066 client hunks, consumer MINORs (754b347a5b, 0db9f32eb1, f8d5f730e2).
- Recorded skips (production):
  - `WakeupTrigger.java` fdece9c358 (`setActiveTask` keeps the `WakeupFuture` when the current task already completed): N/A for the Rust rotating-`CancellationToken` model (consumer-threading.md §11). Java's `ActiveFuture`/`WakeupFuture` state machine does not exist in Rust; a wakeup persists as a cancelled token until a public API surfaces `KafkaError::Wakeup` and rotates, so the wakeup is structurally never lost. `WakeupTriggerTest`'s 3 new tests (`testExceptionTriggeredWhenTaskAsynchronously{Completed,Failed,Cancelled}BeforeSet`) exercise that Java-only edge and are skipped for the same reason.
  - `ConsumerNetworkThread.java` / `AbstractFetch.java` / `FetchCollector.java` / `FetchMetricsManager.java` / `ConsumerRecord.java` / `Metadata.java`: em-dash→hyphen comment fixes, `record`→`record.internal` import moves (Phase-1 module move), and javadoc-only edits. No Rust behaviour change.
  - `ConsumerUtils.java` (+5/−4): malformed-`<li>`→`<ul><li>` javadoc fix; the Rust rustdoc already uses a well-formed markdown list. N/A.
  - `ClientUtils.java` (−27): drops an unused `createNetworkClient` overload; Rust has no `ClientUtils.createNetworkClient` factory overload to remove. N/A.
  - `KafkaConsumer.java` (+42/−36): javadoc HTML-escaping cleanup (`&quot;`→`"`, `{@code ...}` wrapping). N/A for rustdoc.
  - `Fetch.java` 9945592afc (`forPartition` mutable-maps fix): Rust folded `Fetch` into `ConsumerRecords`; `FetchCollector::collect_fetch` accumulates into one owned mutable `HashMap` (`records_by_partition`), so the immutable-singletonMap bug and the `Fetch.add` merge have no Rust counterpart. `FetchTest`'s 5 new `testAdd*`/`testForPartition*` tests are skipped (no Rust `Fetch.add`).
  - `ConsumerRecords.java` KAFKA-20660 (tainted deprecated-ctor detection + periodic `nextOffsets()` warning): Rust never had the deprecated single-arg `ConsumerRecords(Map)` ctor. `ConsumerRecordsTest`'s 3 new `testNextOffsets*` tests are skipped for the same reason.
  - `AbstractHeartbeatRequestManager.java` GROUP_ID_NOT_FOUND non-unsubscribed arm is fatal in Java 4.3.1; Rust keeps the Issue-9 epoch-conditional recovery (retry epoch==0 / fenced-rejoin epoch>0). `ConsumerHeartbeatRequestManagerTest#testGroupIdNotFoundWhileStableIsFatal` is skipped (pre-existing deviation, documented at the handler site). The UNSUBSCRIBED-skip half of the change IS translated + tested.
  - `ApplicationEventProcessorTest#testSharePollEventCallsShareManagers`: Share consumer (KIP-932), §20 out of scope. The `FetchCommittedOffsets`/`OffsetFetchResult` AEP-test hunk is owned by Phase 3.
  - `AsyncKafkaConsumerTest#testStreamsTasksAssignedEventSendsErrorWhenApplyAssignmentFails`: Streams, §20. `testPollWithManualAssignmentDoesNotBusyLoop` (KAFKA-20426): the underlying `maximum_time_to_wait` UNSUBSCRIBED→i64::MAX behaviour is unit-covered at the heartbeat-manager level; the consumer-level busy-loop timing needs a running bg task the unit harness lacks.
  - `KafkaConsumerTest` (+250/−22): the 4 new protocol-recommendation logging tests (`testClassicProtocolLogsRecommendation…`, `testConsumerProtocolDoesNotLogRecommendation`, `testDefaultProtocolLogsRecommendation…`, `testNoGroupIdDoesNotLogGroupProtocolMessage`) exercise the `KafkaConsumer` facade's classic-vs-consumer nudge; Rust has no `KafkaConsumer` facade (the `new_consumer` factory just picks `AsyncKafkaConsumer` or rejects classic — §20), so N/A. The 3 `@ParameterizedTest` `testCurrentLag*` (over `GroupProtocol`) exercise the `currentLag`/end-offset-requested flag end-to-end through the facade + `OffsetFetcher`; the flag production change (`maybe_clear_partition_end_offset_requested` / `clear_end_offset`) was landed by Phase 3 (KAFKA-20165), the classic `OffsetFetcher` is untranslated (§20), and the async lag path is inline in `ApplicationEventProcessor::process_current_lag` with no facade to drive the end-to-end test — deferred as a follow-up.
  - `SubscriptionStateTest` / `ConsumerNetworkThreadTest`: no test delta at 4.3.1.

### Phase 5 — admin (agent 65)

- KAFKA-20673 (f82d3c0c8d): `AdminApiDriver` (+27), `KafkaAdminClient` (+36) — partition-leader APIs hang when a cached leader left the cluster.
- KAFKA-20441 (cf9f8ad376): cordoned log dirs — `LogDirDescription` (+16), `DescribeLogDirs` spec pair (synced in Phase 0), wrapper/result plumbing.
- `DeleteConsumerGroupsResult` (+3), `Admin` trait javadoc.
- Tests: `KafkaAdminClientTest` (+166/−17) in-scope slices, `AdminApiDriverTest` (+19), new `PartitionLeaderStrategyIntegrationTest` (+37).

### Phase 6 — sweep + close-out (agent 66)

- Remaining small in-scope files from the 176-file list not covered above; `ProtocolRoundTripConsistencyTest` (+180 — against generated types if feasible, else skip-with-reason).
- **Completeness audit**: table of all 81 commits → phase / skip-with-reason, appended to this PLAN.md.
- `make verify` (macOS caveats: PIP_INDEX_URL override for CodeArtifact, Docker arms Linux-CI-only).
- `MILESTONES.md` + `design/current/status.md` updates.

**Ordering**: 0 → 1 → {2, 5 in parallel if desired} and 3 → 4; 6 last. 1 precedes 2–5 (record move renames imports crate-wide); 3 precedes 4 (`OffsetFetchResult` feeds the poll path).

## 4. DoD notes

- DoD applies in full per phase (tests translated incl. exact error-message assertions; no TODOs; `@RepeatedTest` → loops; byte-level wire tests for changed encodings).
- DoD #10 (hot-path allocation audit) applies to Phases 2 and 4 (send/fetch paths touched); state N/A explicitly elsewhere.
- Out-of-scope Java tests (Share/Streams/Classic/OAuth/telemetry/Schema/ConfigDef) skipped with the standing §20-style reasons; each phase lists which ones it skipped.

## 5. Known pre-existing divergences (not part of the 4.2→4.3.1 delta — follow-up candidates)

These predate this milestone (the pre-4.3.1 translation already had them) and are therefore out of scope for the 4.2→4.3.1 delta; recorded here for accuracy and as follow-up candidates.

- **`committed()` never refreshes the leader-epoch cache.** Java's `CommitRequestManager` driver calls `maybeUpdateLastSeenEpochIfNewer(res.offsets())` (`CommitRequestManager.java:583,632`, in `handleSuccessfulOffsetFetch` / `handleRetriablePartitionErrors`) on **all** fetched offsets, so both `updateFetchPositions` **and** public `committed()` refresh the `Metadata` last-seen leader-epoch cache. Rust relocated the update to `OffsetsRequestManager::refresh_offsets` (`src/consumer/internals/offsets_request_manager.rs`), which is reached **only** by the position-init path and gated on `currently_initializing.contains(tp)`; the AEP `FetchCommittedOffsetsEvent` handler (`src/consumer/internals/events/application_event_processor.rs`) just strips + completes. Net: `consumer.committed(..)` in Rust does not refresh epochs, and the update is narrower than Java's "all `res.offsets()`".
- **Related — `committed()` absents errored/uncommitted partitions instead of returning them present-with-null.** Java's `committed()` returns `toOffsetMapWithNulls()`, i.e. requested partitions present with a `null` value for uncommitted and (now, KAFKA-20165) retriable-errored partitions. The Rust AEP handler's target type is `HashMap<TopicPartition, OffsetAndMetadata>` (no `Option`), so those entries are stripped and the partition is **absent** — a caller cannot distinguish "no committed offset" from "not fetched due to a retriable error". Pre-existing for uncommitted partitions; KAFKA-20165 widens the silently-absented set to errored partitions. Faithful translation is blocked by the handle type.
