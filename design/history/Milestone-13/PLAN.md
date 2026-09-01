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
  - `CompletedFetchTest.java` / `FetchCollectorTest.java` / `FetchRequestManagerTest.java` / `FetcherTest.java`: the whole 4.3.1 delta (14–22 lines each) is purely `record`→`record.internal` import moves (Phase-1 module move); no behavioural test change. Covered transitively by the Phase-1 record-module move (Critic 64, Observation 4).
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

Phase 5 completion notes (agent 65):

- KAFKA-20673 (f82d3c0c8d) applied: `Call::handleNodeUnavailable` hook (base false) →
  `src/admin/internals/call.rs` (`handle_node_unavailable_fn` + setter); the
  partition-leader override wired in `new_driver_call`
  (`src/admin/kafka_admin_client.rs`) checks `spec.scope.destinationBrokerId()`
  present + `metadataManager.isReady()` + `nodeById(id) == null` +
  `driver.maybeRetryLookup(...)`, then `maybeSendRequests`. `maybe_retry_lookup`
  added to `AdminApiDriver` (`src/admin/internals/admin_api_driver.rs`); the
  polling loop's `Ok(None)` arm in `maybe_drain_pending_call`
  (`src/admin/internals/admin_client_runnable.rs`) now calls the hook and drops
  the call when it took corrective action (mirrors Java's `else if (call.handleNodeUnavailable(now)) return true;`).
  `DriverContext` gained a `log_context` field to carry the debug log.
- KAFKA-20441 / KIP-1066 `LogDirDescription.isCordoned`: the actual client-side
  change is KIP-1066 (a45d36ca5d, `git diff 4.2.0..4.3.1 LogDirDescription.java`),
  NOT cf9f8ad376 (which is broker/controller-side + specs only). Added
  `is_cordoned` field + 5-arg `with_volume_bytes_and_cordoned` + `is_cordoned()`
  getter + Display, and wired `log_dir_result.is_cordoned` through
  `log_dir_descriptions` (`src/admin/kafka_admin_client.rs`). Spec `IsCordoned`
  v5 was synced in Phase 0.
- **Recorded skips (Phase 5):**
  - `Admin.updateFeatures(Map<String, FeatureUpdate>)` default overload
    (Admin.java:1543-1558, KAFKA javadoc delta) — NOT added. Rationale: Rust has
    no method overloading and the whole `Admin` trait uniformly requires an
    explicit `*Options` argument (admin-client.md §1); a Rust caller passes
    `UpdateFeaturesOptions::new()` directly, so the convenience overload adds no
    capability. Adding a lone no-options variant only here would break the
    trait's consistency. Documented deviation per DoD #7.
  - `Admin.removeRaftVoter` javadoc note about `controller.quorum.auto.join.enable`
    (Admin.java:1927-1930) — N/A: the Raft-voter admin APIs have no Rust
    counterpart (out of scope), so there is no rustdoc to carry it.
  - `Admin.java` "synchronous behaviour → behavior." wording and the
    `forceTerminateTransaction` whitespace fix (Admin.java:71, :2165) — cosmetic
    doc-only, doc-equivalent (Rust rustdoc unaffected).
  - `KafkaAdminClientTest` `Utils.mkMap(...) → Map.of(...)` refactors in
    `batchedListConsumerGroupOffsetsSpec` / feature-update helpers — no-op test
    refactor with no behavioral change; the Rust equivalents already use
    `HashMap::from`.

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
- **Rebalance-listener callbacks receive partitions in nondeterministic order (not sorted).** Java's `AbstractMembershipManager` builds `addedPartitions` as a `SortedSet<TopicPartition>` (`TreeSet` with `TOPIC_PARTITION_COMPARATOR`, `AbstractMembershipManager.java:850`) and hands it to `signalPartitionsAssigned(assignedPartitions, addedPartitions)` (`:1191`, decl `:1229`), so `on_partitions_assigned` sees sorted partitions; `signalPartitionsRevoked`/`signalPartitionsLost` similarly operate on ordered collections. Rust's `ConsumerMembershipManager` carries `added` as a `HashSet` and collects it into a `Vec` via `added.iter().cloned().collect()` (`src/consumer/internals/consumer_membership_manager.rs:1398`) before passing it to `enqueue_partitions_assigned_event` (`src/consumer/internals/abstract_membership_manager.rs:901`, `added_partitions: Vec<TopicPartition>`), so the app-side `on_partitions_assigned` callback observes a nondeterministic order. The revoke/lost `PartitionsRemoved` path has the same property. This predates Phase 4 (the 4.2 `enqueue_rebalance_callback(OnPartitionsAssigned, added_vec)` path had the same `HashSet`→`Vec` shape) and is not part of the 4.2→4.3.1 delta. Follow-up candidate if strict Java ordering parity in listener arguments is desired.

## 6. Commit completeness audit (Phase 6, agent 66)

Every one of the 81 in-scope commits
(`git -C kafka log --oneline --cherry-pick --right-only 4.2.0...4.3.1 -- clients/src/main/java/org/apache/kafka/clients clients/src/main/java/org/apache/kafka/common clients/src/main/resources/common/message`)
mapped to a phase or an explicit skip. **No missed in-scope change** — every
behavioral delta with a Rust counterpart was applied by Phases 0–5; the two
behaviorally-significant commits that were not named in a phase's coverage line
(`1df2ac5b2b` KAFKA-19012, `917d695322` KAFKA-17019) were verified already
present in the Rust tree.

Disposition tally: **Phase 0 = 3, Phase 1 = 7, Phase 2 = 5, Phase 3 = 3,
Phase 4 = 22, Phase 5 = 7** (47 phase-covered); **pre-existing, verified = 1**
(`1df2ac5b2b` KAFKA-19012 — predates M13); **skip untranslated-area = 26,
skip broker-only = 1, skip reverted-within-range = 2, other cosmetic/doc = 4**
(33 skipped). Total 81.

| # | sha | title | disposition |
|---|---|---|---|
| 1 | ab4bb0a64b | Clean up SASL/OAUTHBEARER expected issuer/audience config docs | skip: untranslated-area (OAuth config keys — LOW→HIGH importance) |
| 2 | 9a145e12cd | improve ListDeserializer exception | skip: untranslated-area (serialization framework) |
| 3 | b3e833324c | Validate OAuthBearer server callback handler config | skip: untranslated-area (OAuth) |
| 4 | 9c62b8c9f7 | improve byte[] array size bounds check | skip: untranslated-area (touches only ListDeserializer.java) |
| 5 | f82d3c0c8d | KAFKA-20673 AdminClient partition-leader hang | Phase 5 |
| 6 | 0aa8462cdf | KAFKA-20660 detect legacy ConsumerRecords(Map) ctor | Phase 4 (recorded skip — Rust never had the ctor) |
| 7 | 85280fdad1 | improve ListDeserializer | skip: untranslated-area (serialization framework) |
| 8 | 1e27a205fa | KAFKA-20535 async consumer CPU under low max.poll.records | Phase 4 |
| 9 | 4929f9d660 | Fixed metrics decompression | skip: untranslated-area (telemetry KIP-714) |
| 10 | cf9f8ad376 | KAFKA-20441 handling of cordoned log dirs | skip: broker-only (client side is a45d36ca5d; specs synced Phase 0) |
| 11 | e1a062cc07 | KAFKA-20426 group.id+assign busy loop | Phase 4 |
| 12 | 67ae18eaae | KAFKA-20428 unsubscribe failure w/ assignment updates | Phase 4 |
| 13 | 052e088929 | Revert "KAFKA-18652 task.offset.interval.ms" | skip: reverted-within-range (reverts #30) |
| 14 | f8d5f730e2 | skip output msg if manual assignment used | Phase 4 |
| 15 | e0483a6f5e | KAFKA-20282 classic-consumer startup nudge | skip: untranslated-area (classic/no KafkaConsumer facade; Phase-4 recorded skip) |
| 16 | 5d6248c448 | KAFKA-20332 [2] wakeup on poll reconciliation check | Phase 4 |
| 17 | 6b05369445 | KAFKA-20332 app thread not collecting revoked records | Phase 4 |
| 18 | 5610f3af0c | KAFKA-20165 retriable partition errors on OffsetFetch | Phase 3 |
| 19 | c41ff4de0e | Revert 2PC public API changes | Phase 2 |
| 20 | 54d6e39fa6 | KAFKA-20382 bg error when assignment-update callbacks fail | Phase 4 |
| 21 | dd15ae62f2 | KAFKA-20330 ack handling on broker restart | skip: untranslated-area (Share consumer) |
| 22 | aa736157d1 | KAFKA-20106 [2/2] reconciled assignment within poll | Phase 4 |
| 23 | 9945592afc | mutable maps in Fetch.forPartition | Phase 4 (recorded skip — Fetch folded into ConsumerRecords) |
| 24 | 754b347a5b | Consumer tidying | Phase 4 |
| 25 | 8dad4f93e9 | KAFKA-20321 mark lost partitions before callbacks | Phase 4 |
| 26 | 9be18d2bfc | Remove unused createNetworkClient overload in ClientUtils | Phase 4 (recorded skip — no Rust overload) |
| 27 | 0db9f32eb1 | misc improvements in consumer test & events | Phase 4 |
| 28 | f6ca0f69d6 | replace mkMap/mkEntry with Map.of in clients module | other: cosmetic (no behavioral change; Rust uses HashMap::from) |
| 29 | 363e4ae6dd | KAFKA-20297 move Scheduler from client common to trogdor | skip: untranslated-area (Scheduler moved out of client) |
| 30 | b43d70885d | KAFKA-18652 add task.offset.interval.ms config | skip: reverted-within-range (reverted by #13; Streams) |
| 31 | 71449aabb6 | KAFKA-20106 reconciled assignment within poll | Phase 4 |
| 32 | 696729f6d3 | KAFKA-20116 client.rack via StreamsHeartbeatRequest | skip: untranslated-area (Streams) |
| 33 | 2f2d9b0172 | KAFKA-20309 limit SharePollEvent to single instance | skip: untranslated-area (content is Share-only, incl. its AEP hunk) |
| 34 | 4ebf018a5a | KAFKA-18608 OAuth client assertion for client_credentials | skip: untranslated-area (OAuth) |
| 35 | 3884062d25 | KAFKA-20297 Remove MappedIterator and test | skip: untranslated-area (Utils; removed from client) |
| 36 | 84d4f35387 | Share group tidying | skip: untranslated-area (Share) |
| 37 | 24202c0d9b | KAFKA-17939 make Bytes public API (KIP-1247) | skip: untranslated-area (Bytes/Utils) |
| 38 | 0a7b16c501 | fix share poll event to call share membership manager | skip: untranslated-area (Share) |
| 39 | fdece9c358 | keep pendingTask as WakeupFuture if currentTask completed | Phase 4 (recorded skip — rotating-token model) |
| 40 | c7c7bb72c6 | KAFKA-20066 KIP-1251 assignment epochs [2/N] | Phase 3 (client hunk) |
| 41 | 7bd979bb4f | KAFKA-20173 propagate headers into serde 3/N | skip: untranslated-area (List{Ser,Deser}ializer) |
| 42 | 70e4540b63 | Ignore unassigned records in MockConsumer | Phase 4 |
| 43 | 6aa702fb24 | improve CRC failure handling share groups | skip: untranslated-area (Share) |
| 44 | c8f35f4ea3 | KAFKA-19774 cleanups for KIP-1066 | Phase 5 (LogDirDescription) |
| 45 | d0bf2423ee | miss spelling | other: doc typo (KafkaFutureImpl `dependants`→`dependents`; word absent in Rust) |
| 46 | abcbef6a4c | KAFKA-20131 classic clear endOffsetRequested on failed LIST_OFFSETS | skip: untranslated-area (classic OffsetFetcher; async analog landed Phase 3 KAFKA-20165) |
| 47 | a45d36ca5d | KAFKA-19774 cordon log dirs mechanism (KIP-1066) | Phase 5 (LogDirDescription.isCordoned) |
| 48 | 70e7cddb9d | KAFKA-20137 javadoc for public producer APIs | Phase 2 (javadoc; N/A rustdoc) |
| 49 | d18b97702c | Remove Evolving annotation from GroupState | Phase 1 |
| 50 | d920f8bc1f | KAFKA-10863 ControlRecordType schema auto-generated | Phase 1 |
| 51 | 1af5faef73 | KAFKA-20130 move RecordValidationStats to storage | Phase 1 (delete counterpart) |
| 52 | 53032d2e4f | KAFKA-19361 doc mapKey does not break serde compat | Phase 0 (message-spec README) |
| 53 | 9a9e497ff8 | KAFKA-19833 reduce dup in nullable protocol types | skip: untranslated-area (Schema runtime) |
| 54 | 0166a0342d | KAFKA-20128 TimestampType javadoc + move to internal | Phase 1 |
| 55 | 6586446850 | remove unused method / adjust visibility in Utils.java | skip: untranslated-area (Utils) |
| 56 | 9d5bbf5827 | add javadoc for ConfigDef.convertToString() | skip: untranslated-area (ConfigDef) |
| 57 | b3d77f9891 | add Admin#updateFeatures overload | Phase 5 (recorded skip — overload not added, documented deviation) |
| 58 | 351a8b20da | fix `leader` param desc in TopicPartitionInfo ctor | Phase 1 |
| 59 | c2d7b97ede | fix formatting of Admin#forceTerminateTransaction | Phase 5 (recorded skip — cosmetic doc) |
| 60 | 11688f2129 | @since note to removeRaftVoter in Admin.java | Phase 5 (recorded skip — no Rust counterpart) |
| 61 | 718202dbf4 | KAFKA-15853 delete CoreUtils.scala, migrate to Utils.java | skip: untranslated-area (Utils) |
| 62 | 7157c05cc9 | KAFKA-20065 improve code examples in consumer javadoc | other: javadoc (N/A rustdoc) |
| 63 | 63c8d2b548 | replace "if or not" with "whether" | skip: untranslated-area (Shell.java) |
| 64 | 0a9d9d5832 | fix typo in Sender class comment | Phase 2 (comment) |
| 65 | 1df2ac5b2b | KAFKA-19012 fix rare producer message corruption / buffer reuse | pre-existing, verified present (predates M13; M11 producer-txn work; verified in producer_batch/record_accumulator/sender at base ff13c8c7) — flags+deferral present with KAFKA-19012 citations; faithful + structurally immune (finalization copy); no M13 work |
| 66 | 934094ff8a | KAFKA-20020 UUID nullability desc in API readme | Phase 0 (message-spec README) |
| 67 | aee7a3730d | tolerate GroupIdNotFoundException when leaving a group | Phase 4 (UNSUBSCRIBED-skip half translated; recorded skip for the fatal-arm deviation) |
| 68 | aaca67ceed | fix javadoc parsing issues for Checkstyle | other: javadoc across files (N/A rustdoc) |
| 69 | 380cda94c1 | KAFKA-20021 document Admin#createPartitions throws | Phase 5 (Admin javadoc) |
| 70 | 7a511874a8 | use TransactionOperation enum instead of String | Phase 2 |
| 71 | 9f03f5b8a4 | KAFKA-19993 correct Consumer#committed nonexistent-partition doc | Phase 4 (KafkaConsumer facade javadoc; N/A rustdoc) |
| 72 | 165b27b778 | fix boolean formatting consistency in protocol definitions | Phase 0 (spec JSON, cosmetic) |
| 73 | 09ead68276 | replace non-ASCII dashes with ASCII hyphen | Phase 4 (recorded skip — em-dash→hyphen comments) |
| 74 | 9273cdc491 | Bytes lexicographic comparator could use compiler builtin | skip: untranslated-area (Bytes) |
| 75 | 89aa87c13f | KAFKA-19809 Checkstyle 10→12 upgrade | Phase 1 (TxnOffsetCommitRequest whitespace, cosmetic) |
| 76 | 917d695322 | KAFKA-17019 producer TimeoutException include root cause | Phase 2 (await-reason overload + 4 timeout-message consts, applied at init/sendOffsets/commit/abort) |
| 77 | 58d62d1522 | KAFKA-19634 formalize nullable/non-nullable protocol types | skip: untranslated-area (Schema runtime; README hunk Phase 0) |
| 78 | d27d90ccb3 | refactor OffsetFetch path | Phase 3 (OffsetFetchRequest) |
| 79 | 2dffe32c2a | KAFKA-19249 close(Duration)→close(CloseOptions) | Phase 4 (only a MockConsumer 1-liner in range; CloseOptions predates 4.2) |
| 80 | cf7b4a98b8 | clarify preferred replica documentation | Phase 1 (PartitionInfo javadoc — applied) |
| 81 | 02fd9b1ad9 | fix typo in AbstractHeartbeatRequestManager javadoc | Phase 4 (javadoc) |

**Notes on the two commits verified rather than phase-named:**

- `1df2ac5b2b` (KAFKA-19012) — the buffer-deallocation-deferral machinery
  (`buffer_deallocated`/`inflight` flags + accessors, `deallocate` guard, the
  `abort_batches`/`abort_in_flight_batches` deferral, and `set_inflight(false)`
  on response) is present in `src/producer/internals/{producer_batch,record_accumulator,sender}.rs`
  with explicit `KAFKA-19012` comments. Independently, the Rust write path copies
  finalized batch bytes into an owned `bytes::Bytes` at `take_batch_data()`
  (`memory_records_builder.rs`), so the pooled `Vec<u8>` is never the object
  written to the network — the pre-2.8.0 behavior the bug report credits with
  hiding the defect. No M13 code needed.
- `2f2d9b0172` (KAFKA-20309) — PLAN §3 Phase 4 listed it under "client hunks",
  but on inspection its entire diff (incl. the `ApplicationEventProcessor` hunk)
  is inside `process(SharePollEvent)` / `ShareConsumerImpl`, i.e. Share-only.
  Correctly out of scope per consumer-threading.md §20; the Phase-4
  `testSharePollEventCallsShareManagers` recorded skip already covers its test.
