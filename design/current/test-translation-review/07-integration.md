# Integration Test Translation Review (07) — Java → Rust (KIP-848)

**Scope:** Consumer integration tests under
`kafka/clients/clients-integration-tests/.../consumer/` vs. Rust under
`tests/integration/`. Read-only review. Classic-protocol parameter rows
(`GroupProtocol.CLASSIC`) are OUT_OF_SCOPE per `consumer-threading.md` §20;
this review judges only the KIP-848 (`GroupProtocol.CONSUMER`) arms.

**Status legend**
- PRESERVED — behavior + assertions faithfully translated.
- REDUCED — translated but with weaker/looser assertions or narrower coverage.
- CHANGED — translated but with a meaningful behavioral deviation.
- MISSING (in-scope) — a KIP-848 behavior with NO Rust coverage anywhere.
- OUT_OF_SCOPE — classic-only / metrics / rack-aware / multi-broker-server-side.

---

## Summary

**In-scope (KIP-848-arm) Java integration test behaviors: ~70.**

| Status | Count (approx, in-scope behaviors) |
|---|---|
| PRESERVED | 28 |
| REDUCED | 6 |
| CHANGED | 1 |
| MISSING (in-scope) | ~33 |
| OUT_OF_SCOPE | (all `testClassic*` twins + metrics + rack-aware) |

**Java integration files with a dedicated Rust counterpart (4):**
`PlaintextConsumerAssignTest`, `PlaintextConsumerFetchTest`,
`PlaintextConsumerPollTest`, `PlaintextConsumerSubscriptionTest`.
Plus `consumer_test.rs` (a small bespoke E2E suite, no Java twin) and
`sasl_ssl_consumer_test.rs` (partial SASL coverage).

**Java integration files with NO Rust counterpart (6):**
`PlaintextConsumerCommitTest`, `PlaintextConsumerCallbackTest`,
`PlaintextConsumerTest` (the `BaseConsumerTestcase` surface),
`ConsumerBounceTest`, `ConsumerTopicCreationTest`,
`ConsumerWithLegacyMessageFormatIntegrationTest`. `ConsumerIntegrationTest`
and `SaslPlainPlaintextConsumerTest` are also effectively unmirrored
(see below).

---

## ConsumerIntegrationTest.java — NO dedicated Rust file

| Java test | Status | Notes |
|---|---|---|
| `testAsyncConsumerWithConsumerProtocolDisabled` | OUT_OF_SCOPE-ish / MISSING | Asserts `UnsupportedVersionException` when broker has `rebalance.protocols=classic`. Rust forces `classic,consumer` on the broker, so the scenario is unrepresentable; no Rust equivalent. Borderline in-scope (KIP-848 negotiation failure path) — not covered. |
| `testFetchPartitionsAfterFailedListenerWithGroupProtocolConsumer` | MISSING (in-scope) | Listener throws once on first `onPartitionsAssigned`, consumer must recover and still deliver the record. No Rust coverage. |
| `testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolConsumer` | MISSING (in-scope) | Always-throwing assigned-listener; poll returns 0 records or `KafkaException("User rebalance callback throws an error")`. No Rust coverage. |
| `testLeaderEpoch` | OUT_OF_SCOPE | Requires `shutdownBroker` + leader-epoch increment (3-broker server-side failure). `record.leaderEpoch()` parity untested. |
| `testRackAwareAssignment` | OUT_OF_SCOPE | Rack-aware server-side assignor; explicitly excluded. |
| `testSingleCoordinatorOwnershipAfterPartitionReassignment` | OUT_OF_SCOPE | Broker-side coordinator metric assertions. |

Note: the two failed-listener tests are genuine in-scope rebalance-callback
error-handling gaps (these exercise §31 callback semantics under failure).

---

## PlaintextConsumerAssignTest.java → plaintext_consumer_assign_test.rs

| Java test (CONSUMER arm) | Status | Notes |
|---|---|---|
| `testAsyncAssignAndCommitAsyncNotCommitted` | PRESERVED | `commit_async_with_callback` + `poll_until_true`; asserts committed absent + assignment contains tp. Faithful. |
| `testAsyncAssignAndCommitSyncNotCommitted` | PRESERVED | Faithful. |
| `testAsyncAssignAndCommitSyncAllConsumed` | PRESERVED | 10k records, seek(0), consume all, commit; committed == numRecords. |
| `testAsyncAssignAndConsume` | PRESERVED | Asserts position == numRecords. |
| `testAsyncAssignAndConsumeSkippingPosition` | PRESERVED | seek(1), verify offsets/keys from 1. |
| `testAsyncAssignAndFetchCommittedOffsets` | PRESERVED | Two consumers, same group, cross-consumer committed visibility. |
| `testAsyncAssignAndConsumeFromCommittedOffsets` | PRESERVED | Manual commit offset 10 via `commit_sync_offsets`, second consumer reads from it. |
| `testAsyncAssignAndRetrievingCommittedOffsetsMultipleTimes` | PRESERVED | Idempotent `committed()`. |

All 8 classic twins OUT_OF_SCOPE. **Best-translated file in the suite** —
full per-record field verification (timestamp, timestamp_type, serialized
sizes). No gaps.

---

## PlaintextConsumerFetchTest.java → plaintext_consumer_fetch_test.rs

| Java test (CONSUMER arm) | Status | Notes |
|---|---|---|
| `testAsyncConsumerFetchInvalidOffset` | REDUCED | Java asserts `OffsetOutOfRangeException.offsetOutOfRangePartitions()` (structured map: size 1, value == outOfRangePos). Rust flattens through `KafkaError::IllegalState` and asserts only the `Display` string (`"out of range for partition"` + `offset=N`). The structured payload (`offsetOutOfRangePartitions`) is NOT asserted — documented deviation, but a contract reduction. |
| `testAsyncConsumerFetchOutOfRangeOffsetResetConfigEarliest` | PRESERVED | Reset to 0. |
| `testAsyncConsumerFetchOutOfRangeOffsetResetConfigLatest` | REDUCED/CHANGED | Java: after seek-out-of-range, `poll(50ms)` empty, produce, next record offset==totalRecords. Rust adds a 30s settle loop polling `position()==hwm` before producing — looser timing harness; still asserts the final offset. Acceptable but diverges from Java's tight 50ms timing. |
| `testAsyncConsumerFetchOutOfRangeOffsetResetConfigByDuration` | PRESERVED | Both scenarios (full window reset to 0; 24h-spread → offset 24). Strong translation. |
| `testAsyncConsumerFetchRecordLargerThanFetchMaxBytes` | PRESERVED | KIP-74 single oversized record. |
| `testAsyncConsumerFetchRecordLargerThanMaxPartitionFetchBytes` | PRESERVED | KIP-74. |
| `testAsyncConsumerFetchHonoursFetchSizeIfLargeRecordNotFirst` | PRESERVED | Small-before-large; first poll returns only small. |
| `testAsyncConsumerFetchHonoursMaxPartitionFetchBytesIfLargeRecordNotFirst` | PRESERVED | Faithful. |
| `testAsyncConsumerLowMaxFetchSizeForRequestAndPartition` | PRESERVED | 90 partitions, fetch.max.bytes=500 / max.partition.fetch.bytes=100; per-partition count + field checks. Deadline bumped to 180s (parity-of-outcome, documented). |

All 9 classic twins OUT_OF_SCOPE.

---

## PlaintextConsumerPollTest.java → plaintext_consumer_poll_test.rs

| Java test (CONSUMER arm) | Status | Notes |
|---|---|---|
| `testAsyncConsumerMaxPollRecords` | PRESERVED | max.poll.records cap. |
| `testAsyncConsumerMaxPollIntervalMs` | PRESERVED | Rebalance after exceeding max.poll.interval; assigned/revoked counts. |
| `testAsyncConsumerMaxPollIntervalMsDelayInRevocation` | PRESERVED | Sleep in `on_partitions_revoked`, commit still succeeds; committedPosition==0, commitCompleted. This DOES exercise commit-inside-revocation-callback (§31) — covers a `PlaintextConsumerCommitTest`-adjacent behavior. Implemented via a driver-task channel (Rust listener can't hold `&consumer`). |
| `testAsyncConsumerMaxPollIntervalMsDelayInAssignment` | PRESERVED | Sleep in assigned; `ensureNoRebalance`. |
| `testAsyncConsumerMaxPollIntervalMsShorterThanPollTimeout` | PRESERVED | No extra assignment callbacks. |
| `testAsyncConsumerPollEventuallyReturnsRecordsWithZeroTimeout` | PRESERVED | poll(0) eventually returns. |
| `testAsyncConsumerNoOffsetForPartitionExceptionOnPollZero` | PRESERVED | `waitForPollThrowException` → NoOffsetForPartition. |
| `testAsyncConsumerRecoveryOnPollAfterDelayedRebalance` | PRESERVED | Fence-then-recover after delayed revocation. |
| `testAsyncConsumerPerPartitionLeadWithMaxPollRecords` | MISSING (in-scope, deferred) | SKIP — `consumer.metrics()` not implemented Milestone-8-wide. `records-lead` metric untested. |
| `testAsyncConsumerPerPartitionLagWithMaxPollRecords` | MISSING (in-scope, deferred) | SKIP — same, `records-lag`. |
| `runCloseAsyncConsumerMultiConsumerSessionTimeoutTest` | MISSING (in-scope, deferred) | SKIP — multi-consumer `ConsumerAssignmentPoller` harness deferred to Phase 13b. Group-rebalance-on-member-timeout untested. |
| `runAsyncConsumerMultiConsumerSessionTimeoutTest` | MISSING (in-scope, deferred) | SKIP — same. |

All 10 classic twins OUT_OF_SCOPE. The two metric SKIPs and two
multi-consumer SKIPs are documented and reasonable, but represent real
in-scope coverage gaps (metrics deferral is Milestone-wide).

---

## PlaintextConsumerSubscriptionTest.java → plaintext_consumer_subscription_test.rs

| Java test (CONSUMER arm) | Status | Notes |
|---|---|---|
| `testAsyncConsumerRe2JPatternSubscription` | PRESERVED | Server-side `SubscriptionPattern`. |
| `testAsyncConsumerRe2JPatternSubscriptionFetch` | PRESERVED | Pattern + fetch. |
| `testAsyncConsumerRe2JPatternExpandSubscription` | PRESERVED | Re-subscribe to wider pattern. |
| `testTopicIdSubscriptionWithRe2JRegexAndOffsetsFetch` | PRESERVED | Includes `end_offsets` for known+unknown topics. |
| `testRe2JPatternSubscriptionAndTopicSubscription` | PRESERVED | Pattern ↔ explicit topic switch. |
| `testRe2JPatternSubscriptionInvalidRegex` | PRESERVED | `InvalidRegularExpression` via poll. |
| `testAsyncConsumerExpandingTopicSubscriptions` | PRESERVED | |
| `testAsyncConsumerShrinkingTopicSubscriptions` | PRESERVED | |
| `testAsyncConsumerUnsubscribeTopic` | PRESERVED | subscribe(empty) clears assignment. |
| `testAsyncConsumerSubscribeInvalidTopicCanUnsubscribe` | PRESERVED | InvalidTopic + clean unsubscribe; asserts `"Invalid topics: [...]"` message. |
| `testAsyncConsumerSubscribeInvalidTopicCanClose` | PRESERVED | |
| `testAsyncConsumerPatternSubscription` | MISSING (in-scope) | SKIP — uses client-side `Pattern.compile` overload. Java runs this for CONSUMER too; Rust covers only the `SubscriptionPattern` (Re2J) path. Client-side regex subscribe under KIP-848 is untested. |
| `testAsyncConsumerSubsequentPatternSubscription` | MISSING (in-scope) | SKIP — client-side `Pattern` overload. |
| `testAsyncConsumerPatternUnsubscription` | MISSING (in-scope) | SKIP — client-side `Pattern` overload. |

8 classic twins OUT_OF_SCOPE. The 3 client-side-`Pattern` SKIPs are a
defensible scope call (KIP-848 prefers server-side regex) but Java does
exercise the client `Pattern` overload on the CONSUMER arm, so it is a
real (if low-priority) gap.

---

## PlaintextConsumerCommitTest.java — NO Rust file

In-scope CONSUMER behaviors, NONE with a dedicated Rust test (a few are
indirectly touched by poll-test callbacks):

| Java test | Status | Notes |
|---|---|---|
| `testAsyncConsumerAutoCommitOnClose` | MISSING (in-scope) | enable.auto.commit=true → seek positions auto-committed on close; another consumer sees them. Auto-commit-on-close entirely untested. |
| `testAsyncConsumerAutoCommitOnCloseAfterWakeup` | MISSING (in-scope) | wakeup() before close, auto-commit still flushes. |
| `testAsyncConsumerCommitMetadata` | MISSING (in-scope) | `OffsetAndMetadata` with leaderEpoch + metadata string + null metadata round-trip. Commit-metadata persistence untested. |
| `testAsyncConsumerAsyncCommit` | MISSING (in-scope) | 5× commitAsync, callback success count, final committed value. |
| `testAsyncConsumerAutoCommitIntercept` | MISSING (in-scope) | `MockConsumerInterceptor.ON_COMMIT_COUNT` — interceptor onCommit. (No interceptor integration coverage at all.) |
| `testAsyncConsumerCommitSpecifiedOffsets` | MISSING (in-scope) | Per-partition specific commits, position unchanged, async pickup. |
| `testAsyncConsumerAutoCommitOnRebalance` | MISSING (in-scope) | Auto-commit fires on rebalance; committed reflects seeks. |
| `testAsyncConsumerSubscribeAndCommitSync` | MISSING (in-scope) | member-id propagation into commit. |
| `testAsyncConsumerPositionAndCommit` | MISSING (in-scope) | position() on unsubscribed tp throws IllegalState; commit/position interplay across 2 consumers. |
| `testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose` | MISSING (in-scope) | CONSUMER-only test; asserts `CommitFailedException` msg `"Failed to commit offsets: Coordinator unknown and consumer is closing"` + close duration < 1s. Important close-path contract — untested. |
| `testCommitAsyncCompletedBeforeConsumerCloses` | MISSING (in-scope) | CONSUMER-only; async commits complete before close (callback obligation, CLAUDE.md §9.5). Untested. |
| `testCommitAsyncCompletedBeforeCommitSyncReturns` | MISSING (in-scope) | CONSUMER-only; async-callback-before-commitSync ordering guarantee. Untested. |

**Partial mitigation:** `commit_sync`, `commit_sync_offsets`,
`commit_async_with_callback`, and commit-inside-revocation-callback are
exercised by the assign/poll suites. But auto-commit (on close / on
rebalance), commit metadata, async-completion-ordering guarantees, and the
coordinator-unavailable-during-close error contract are all untested.

---

## PlaintextConsumerCallbackTest.java — NO Rust file

All CONSUMER-arm rebalance-listener-reentrancy behaviors are MISSING
(in-scope). These directly exercise `consumer-threading.md` §31 (listener
runs on caller's task; can call back into the consumer API):

| Java test | Status | Notes |
|---|---|---|
| `testAsyncConsumerRebalanceListenerAssignOnPartitionsAssigned` | MISSING (in-scope) | `assign()` inside assigned-callback → IllegalState `"Subscription to topics, partitions and pattern are mutually exclusive"`. |
| `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsAssigned` | MISSING (in-scope) | `assignment()` inside callback contains tp. |
| `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsAssigned` | MISSING (in-scope) | `beginningOffsets()` inside callback. |
| `testAsyncConsumerRebalanceListenerAssignOnPartitionsRevoked` | MISSING (in-scope) | assign() inside revoked-callback throws. |
| `testAsyncConsumerRebalanceListenerAssignmentOnPartitionsRevoked` | MISSING (in-scope) | assignment() inside revoked. |
| `testAsyncConsumerRebalanceListenerBeginningOffsetsOnPartitionsRevoked` | MISSING (in-scope) | beginningOffsets() inside revoked. |
| `testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer` | MISSING (in-scope) | KIP-848 assigned-callback receives only NEW partitions (incremental). Key protocol-semantics test. |
| `testAsyncConsumerGetPositionOfNewlyAssignedPartitionOnPartitionsAssignedCallback` | MISSING (in-scope) | position() inside assigned-callback does not throw. |
| `testAsyncConsumerSeekPositionAndPauseNewlyAssignedPartitionOnPartitionsAssignedCallback` | MISSING (in-scope) | seek+pause inside callback; resume; verify consumption. **Only place pause/resume is exercised in integration tests** — and it's untested. |

**Note:** `consumer-threading.md` §31 explicitly mandates two regression
tests for callback-on-caller-task / rebalance-blocks-on-callback. Those
exist as *unit* tests per the rule; this review only confirms the
*integration* callback-reentrancy surface (assign/assignment/position/
beginningOffsets/seek/pause inside callbacks) is NOT covered at the
integration level. The poll-test's commit-inside-revocation test is the
sole integration touchpoint.

---

## PlaintextConsumerTest.java (BaseConsumerTestcase surface) — NO Rust file

~40 CONSUMER-arm methods; this is the single largest coverage gap. None
have a dedicated Rust integration test. Highlights (all MISSING in-scope
unless noted):

| Java test | Status | Notes |
|---|---|---|
| `testAsyncConsumerSimpleConsumption` | partial via consumer_test | basic subscribe→produce→poll IS covered by `consumer_test.rs::test_subscribe_and_poll_records`. |
| `testAsyncConsumerClusterResourceListener` | MISSING | ClusterResourceListener callback. |
| `testAsyncConsumeCoordinatorFailover` | MISSING | Coordinator failover (server-side). |
| `testAsyncConsumerCloseOnBrokerShutdown` | MISSING / OUT_OF_SCOPE | broker shutdown. |
| `testAsyncConsumerHeaders` / `HeadersSerializerDeserializer` | MISSING (in-scope) | Record headers round-trip. No header integration coverage at all. |
| `testAsyncConsumerAutoOffsetReset` | MISSING (in-scope) | (earliest/latest base behavior — partially covered by fetch-reset tests). |
| `testAsyncConsumerGroupConsumption` | MISSING (in-scope) | |
| `testAsyncConsumerPartitionsFor` / `PartitionsForAutoCreate` / `PartitionsForInvalidTopic` | MISSING (in-scope) | `partitions_for()` untested. |
| `testAsyncConsumerSeek` | partial via consumer_test | `seek_to_beginning` covered by `test_seek_to_beginning_re_reads_records`; full `testSeek` (seekToEnd, seek mid) NOT covered. |
| `testAsyncConsumerPartitionPauseAndResume` | MISSING (in-scope) | pause/resume — untested anywhere. |
| `testAsyncConsumerInterceptors` / `InterceptorsWithWrongKeyValue` | MISSING (in-scope) | ConsumerInterceptor — untested. |
| `testAsyncConsumerConsumeMessagesWithCreateTime` / `WithLogAppendTime` | MISSING (in-scope) | LogAppendTime timestamp-type untested (assign suite verifies CreateTime only). |
| `testAsyncConsumerListTopics` | MISSING (in-scope) | `list_topics()` untested. |
| `testAsyncConsumerPauseStateNotPreservedByRebalance` | MISSING (in-scope) | |
| `testAsyncConsumer*MetricsCleanUp*` / `QuotaMetrics*` | OUT_OF_SCOPE | metrics deferred. |
| `testAsyncConsumerSeekThrowsIllegalStateIfPartitionsNotAssigned` | MISSING (in-scope) | seek() error contract. |
| `testAsyncConsumerConsumingWithNullGroupId` | MISSING (in-scope) | groupless consumption (partially via `test_assign_partitions_and_poll`). |
| `testAsyncConsumerNullGroupIdNotSupportedIfCommitting` | MISSING (in-scope) | commit without group.id error. |
| `testAsyncConsumerStaticConsumerDetectsNewPartitionCreatedAfterRestart` | MISSING (in-scope) | group.instance.id static membership. |
| `testAsyncConsumerEndOffsets` | partial | end_offsets touched by subscription topic-id test; dedicated test missing. |
| `testAsyncConsumerFetchOffsetsForTime` | MISSING (in-scope) | `offsets_for_times()` — untested (also see Legacy file). |
| `testAsyncConsumerPositionRespectsTimeout` | MISSING (in-scope) | position() timeout. |
| `testAsyncConsumerPositionRespectsWakeup` | MISSING (in-scope) | **wakeup() during position()** — `KafkaError::Wakeup` (§11) untested at integration level. |
| `testAsyncConsumerPositionWithErrorConnectionRespectsWakeup` | MISSING (in-scope) | wakeup() during a failing connection. |
| `testAsyncConsumerCloseLeavesGroupOnInterrupt` | MISSING (in-scope) | |
| `testAsyncConsumerOffsetRelatedWhenTimeoutZero` | MISSING (in-scope) | zero-timeout offset queries. |
| `testAsyncConsumerStallBetweenPoll` | MISSING (in-scope) | |

---

## ConsumerBounceTest.java — NO Rust file

All MISSING but largely OUT_OF_SCOPE (require server-side broker
kill/restart, `BounceBrokerScheduler`, `findCoordinators`, multi-consumer
group-max-size):

| Java test | Status | Notes |
|---|---|---|
| `testAsyncConsumerConsumptionWithBrokerFailures` | OUT_OF_SCOPE | broker bounce. |
| `testAsyncConsumerSeekAndCommitWithBrokerFailures` | OUT_OF_SCOPE | broker bounce. |
| `testAsyncSubscribeWhenTopicUnavailable` | MISSING (in-scope) | topic-created-after-subscribe; partly representable without broker kill — gap. |
| `testAsyncClose` | MISSING (in-scope) | close good-path / coordinator-failure / cluster-failure timing contract. |
| `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize` | MISSING (in-scope) | `GroupMaxSizeReachedException` — KIP-848 group-max-size fatal error untested. |
| `testAsyncCloseDuringRebalance` | MISSING (in-scope) / OUT_OF_SCOPE | close during rebalance + broker shutdown. |

---

## ConsumerTopicCreationTest.java — NO Rust file

| Java test | Status | Notes |
|---|---|---|
| `testAsyncConsumerTopicCreationIfConsumerAllowToCreateTopic` | MISSING (in-scope) | `allow.auto.create.topics=true` + broker auto-create → topic created on subscribe+poll. Untested. |
| `testAsyncConsumerTopicCreationIfConsumerDisallowToCreateTopic` | MISSING (in-scope) | `allow.auto.create.topics=false` → topic NOT created. Untested. |

---

## ConsumerWithLegacyMessageFormatIntegrationTest.java — NO Rust file

| Java test | Status | Notes |
|---|---|---|
| `testOffsetsForTimesWithAsyncConsumer` | MISSING (in-scope) | `offsets_for_times()` across v0/v1/v2 message formats; asserts null timestamp for v0, empty leaderEpoch for v1, present for v2. Requires broker-side legacy-record append (`appendAsLeaderWithRecordVersion`) — hard to set up, but `offsets_for_times` is in-scope and has zero coverage. |
| `testEarliestOrLatestOffsetsWithAsyncConsumer` | MISSING (in-scope) | `beginning_offsets` / `end_offsets` across legacy formats. (end_offsets partially touched elsewhere; beginning_offsets untested.) |

---

## SaslPlainPlaintextConsumerTest.java → (partial) sasl_ssl_consumer_test.rs

| Java test | Status | Notes |
|---|---|---|
| `testAsyncConsumerSimpleConsumption` (SASL_PLAINTEXT) | REDUCED | Rust covers SASL over **SASL_SSL** (`test_sasl_ssl_consume_records`), not SASL_PLAINTEXT. Behavior (auth + consume) is equivalent; transport differs. `ssl_sasl_test.rs::test_sasl_plaintext_connection` covers SASL_PLAINTEXT connect but not consume. |
| `testAsyncConsumerClusterResourceListener` (SASL) | MISSING (in-scope) | ClusterResourceListener over SASL. |
| `testAsyncConsumeCoordinatorFailover` (SASL) | MISSING (in-scope) / OUT_OF_SCOPE | coordinator failover. |
| (bonus) wrong-credentials | PRESERVED+ | `test_sasl_ssl_wrong_credentials` adds an auth-failure-doesn't-hang test with no direct Java twin (good). |

---

## Key findings

1. **Three Java integration files are entirely unmirrored and represent the
   bulk of in-scope coverage loss:** `PlaintextConsumerCommitTest` (12
   CONSUMER-arm tests), `PlaintextConsumerCallbackTest` (9), and
   `PlaintextConsumerTest`/BaseConsumerTestcase (~40). Together that is the
   majority of the in-scope integration surface.

2. **Rebalance-listener reentrancy (§31) is the highest-value gap.** The
   entire `PlaintextConsumerCallbackTest` — assign/assignment/position/
   beginningOffsets/seek/pause *inside* a rebalance callback, and the
   KIP-848-specific "assigned-callback gets only NEW partitions" — has no
   integration coverage. Only commit-inside-revocation is touched (in the
   poll suite). This is precisely the contract `consumer-threading.md` §31
   calls load-bearing.

3. **Auto-commit and async-commit-completion contracts are untested.**
   auto-commit-on-close, auto-commit-on-rebalance, commit metadata
   round-trip, and the three CONSUMER-only "async callbacks complete before
   close / before commitSync" ordering guarantees (callback obligation,
   CLAUDE.md §9.5) have no integration test.

4. **Several core public-API methods have zero integration coverage:**
   `pause`/`resume`, `partitions_for`, `list_topics`, `offsets_for_times`,
   `beginning_offsets`, record `headers`, and `ConsumerInterceptor`. `seek`
   is only partially covered (seek_to_beginning; not seek_to_end/seek-mid).

5. **`wakeup()` (§11) is untested at the integration level.** Java's
   `testAsyncConsumerPositionRespectsWakeup` /
   `PositionWithErrorConnectionRespectsWakeup` /
   `AutoCommitOnCloseAfterWakeup` exercise the rotating-CancellationToken
   path; none are translated.

6. **The four translated files are high quality.** Assign and Fetch suites
   verify the full per-record field set (offset, timestamp, timestamp_type,
   serialized sizes, key/value). Poll and Subscription faithfully translate
   the in-scope arms. SKIP rationales are documented inline.

7. **Two genuine assertion reductions:** (a) `FetchInvalidOffset` asserts
   only the OffsetOutOfRange `Display` string, dropping Java's structured
   `offsetOutOfRangePartitions()` map assertion; (b) `ResetConfigLatest`
   replaces Java's tight 50ms timing with a 30s settle loop. Both are
   documented but loosen the contract.

8. **Metrics-dependent and multi-consumer-harness tests are deferred, not
   missing-by-oversight** (poll-test SKIPs for `records-lead`/`records-lag`
   and `runMultiConsumerSessionTimeoutTest`), but they remain real in-scope
   gaps pending the metrics impl and Phase-13b multi-consumer harness.

9. **`consumer_test.rs` is a useful bespoke E2E suite** (subscribe/poll,
   assign/poll, commit-then-resume, seek-to-beginning) with no Java twin; it
   partially backfills `PlaintextConsumerTest::SimpleConsumption` and
   `Seek`, and the assign suite's groupless variant.
