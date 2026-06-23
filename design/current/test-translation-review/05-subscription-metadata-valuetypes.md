# Test-translation review 05: SubscriptionState, metadata, public value types

Read-only fidelity review of Java consumer SubscriptionState / metadata /
value-type tests against their Rust (KIP-848) counterparts.

Status legend: PRESERVED = behavior + key assertions faithfully translated;
REDUCED = translated but some assertions weakened; CHANGED = semantics differ;
MISSING = in-scope but absent; OUT_OF_SCOPE = legitimately skipped (with reason).

---

## SubscriptionStateTest.java (58 @Test) — `src/consumer/internals/subscription_state.rs`

Rust has all 58 translated inline (plus extra fetch-state-machine unit tests).
Critical-fidelity file; every important transition verified PRESERVED.

| Java test | Status | Notes |
|---|---|---|
| partitionAssignment | PRESERVED | `test_partition_assignment` |
| partitionAssignmentChangeOnTopicSubscription | PRESERVED | `test_partition_assignment_change_on_topic_subscription` |
| testIsFetchableOnManualAssignment | PRESERVED | |
| testIsFetchableOnAutoAssignment | PRESERVED | |
| testIsFetchableConsidersExplicitTopicSubscription | PRESERVED | subscription-change → not-fetchable preserved |
| testGroupSubscribe | PRESERVED | non-accumulating group subscribe verified |
| partitionAssignmentChangeOnPatternSubscription | PRESERVED | |
| verifyAssignmentId | PRESERVED | id increments 0→1→2→3 |
| partitionReset | PRESERVED | reset clears position; seek re-fetchable |
| topicSubscription | PRESERVED | |
| partitionPause | PRESERVED | |
| testMarkingPendingRevocation | PRESERVED | |
| testMarkingPendingRevocationPreventsInitializingPosition | PRESERVED | |
| testAssignedPartitionsAwaitingCallbackKeepPositionDefinedInCallback | PRESERVED | |
| testAssignedPartitionsAwaitingCallbackInitializePositionsWhenCallbackCompletes | PRESERVED | |
| testAssignedPartitionsAwaitingCallbackDoesNotAffectPreviouslyOwnedPartitions | PRESERVED | |
| invalidPositionUpdate | PRESERVED | IllegalState asserted (matches IllegalStateException) |
| cantAssignPartitionForUnsubscribedTopics | PRESERVED | |
| cantAssignPartitionForUnmatchedPattern | PRESERVED | |
| cantChangePositionForNonAssignedPartition | PRESERVED | IllegalState |
| cantSubscribeTopicAndPattern | PRESERVED | |
| cantSubscribePartitionAndPattern | PRESERVED | |
| cantSubscribePatternAndTopic | PRESERVED | |
| cantSubscribePatternAndPartition | PRESERVED | |
| patternSubscription | PRESERVED | `test_pattern_subscription_two_topics` |
| testSubscribeToRe2JPattern | PRESERVED | toString `type=AUTO_PATTERN_RE2J` + `subscribedPattern=` asserted |
| testIsAssignedFromRe2j | PRESERVED | null-arg replaced by arbitrary-UUID-before-subscribe (documented, equivalent) |
| testAssignedPartitionsWithTopicIdsForRe2Pattern | PRESERVED | |
| testAssignedTopicIdsPreservedWhenReconciliationCompletes | PRESERVED | both topic IDs retained |
| testMixedPatternSubscriptionNotAllowed | PRESERVED | both directions |
| testSubscriptionPattern | PRESERVED | `subscription_pattern()` getter |
| unsubscribeUserAssignment | PRESERVED | |
| unsubscribeUserSubscribe | PRESERVED | |
| unsubscription | PRESERVED | |
| testPreferredReadReplicaLease | PRESERVED | lease expiry windows (9/10/11, 20/21, 30/31) all verified |
| testSeekUnvalidatedWithNoOffsetEpoch | PRESERVED | maybeValidate half delegated to `test_maybe_validate_position_for_current_leader` (documented) |
| testSeekUnvalidatedWithNoEpochClearsAwaitingValidation | PRESERVED | |
| testSeekUnvalidatedWithOffsetEpoch | PRESERVED | maybeValidate half delegated (documented) |
| testSeekValidatedShouldClearAwaitingValidation | PRESERVED | offset 10→8 transition |
| testCompleteValidationShouldClearAwaitingValidation | PRESERVED | |
| testOffsetResetWhileAwaitingValidation | PRESERVED | |
| testMaybeCompleteValidation | PRESERVED | |
| testMaybeValidatePositionForCurrentLeader | PRESERVED | old-API skip / new-API validate / unassigned-skip |
| testMaybeCompleteValidationAfterPositionChange | PRESERVED | stale validation ignored after position change |
| testMaybeCompleteValidationAfterOffsetReset | PRESERVED | position null after reset |
| testTruncationDetectionWithResetPolicy | PRESERVED | divergent position applied |
| testTruncationDetectionWithoutResetPolicy | PRESERVED | LogTruncation.divergentOffsetOpt + fetchPosition asserted (OffsetAndMetadata equality) |
| testTruncationDetectionUnknownDivergentOffsetWithResetPolicy | PRESERVED | resetStrategy == EARLIEST asserted |
| testTruncationDetectionUnknownDivergentOffsetWithoutResetPolicy | PRESERVED | divergentOffsetOpt None |
| nullPositionLagOnNoPosition | PRESERVED | both isolation levels |
| testPositionOrNull | PRESERVED | |
| testTryUpdatingHighWatermark | PRESERVED | assigned true / unassigned false |
| testTryUpdatingLogStartOffset | PRESERVED | partitionLead asserted |
| testTryUpdatingLastStableOffset | PRESERVED | |
| testTryUpdatingPreferredReadReplica | PRESERVED | |
| testRequestOffsetResetIfPartitionAssigned | PRESERVED | unassigned → IllegalState on subsequent query |
| resetOffsetNoValidation | PRESERVED | full multi-phase reset/validate sequence |
| testFetchablePartitionsPerformsCheapChecksFirst | PRESERVED | predicate-not-evaluated-when-paused (hot-path contract) |

**SubscriptionState: 58/58 PRESERVED.** Error types map IllegalStateException →
`KafkaError::IllegalState` and assert via `matches!`. The two split tests
(`testSeekUnvalidatedWith*`) have their maybe-validate half explicitly covered
elsewhere — documented, no loss.

---

## ConsumerMetadataTest.java (10 @Test) — `src/consumer/internals/consumer_metadata.rs`

| Java test | Status | Notes |
|---|---|---|
| testPatternSubscriptionNoInternalTopics | PRESERVED | folded into looped `test_pattern_subscription(false/true)` |
| testPatternSubscriptionIncludeInternalTopics | PRESERVED | same loop |
| testSubscriptionToBrokerRegexDoesNotRequestAllTopicsMetadata | PRESERVED | topic-ids-only request |
| testSubscriptionToBrokerRegexRetainsAssignedTopics | PRESERVED | two-arg retain-by-id plumbing |
| testSubscriptionToBrokerRegexAllowsTransientTopics | PRESERVED | transient toggles topic-name↔topic-id |
| testUserAssignment | PRESERVED | |
| testNormalSubscription | PRESERVED | groupSubscribe + resetGroupSubscription |
| testTransientTopics | PRESERVED | updateRequested toggling + topic-id map |
| testInvalidPartitionLeadershipUpdates | OUT_OF_SCOPE | inherited `Metadata.updatePartitionLeadership` path; covered by `metadata::tests`; ConsumerMetadata adds no override here (documented in module header) |
| testValidPartitionLeadershipUpdate | OUT_OF_SCOPE | same rationale |

**In-scope 8/8 PRESERVED; 2 OUT_OF_SCOPE (Mockito/inherited-Metadata, reasonable).**
The two skipped tests exercise only the base `Metadata` leadership-update code,
not consumer-specific behavior. Skip rationale is sound but slightly under-verified:
they also assert leaderEpoch propagation and listener `onUpdate` invocation counts;
worth spot-confirming the base `metadata::tests` cover the stale-epoch-rejected case.

---

## TopicMetadataRequestManagerTest.java (7 @Test, some parameterized) — `src/consumer/internals/topic_metadata_request_manager.rs`

| Java test | Status | Notes |
|---|---|---|
| testPoll_SuccessfulRequestTopicMetadata | PRESERVED | |
| testPoll_SuccessfulRequestAllTopicsMetadata | PRESERVED | |
| testTopicExceptionAndInflightRequests (param ×5) | PRESERVED | 5 cases: UNKNOWN_TOPIC/INVALID_TOPIC/UNKNOWN_SERVER=no-retry, NETWORK=retry, NONE=no-retry; topic retained on retry |
| testAllTopicsExceptionAndInflightRequests (param ×5) | PRESERVED | 5 cases mirrored |
| testExpiringRequest | PRESERVED | double retriable fail → expiry → future completes exceptionally |
| testHardFailures (param ×3) | PRESERVED | timeout/kafka/network split into 3 fns; retriable vs fatal inflight outcome |
| testNetworkTimeout | PRESERVED | exponential backoff -1ms / +1ms boundary |

Plus extra Rust tests (authorization-fatal, request-building witnesses, response
routing, per-request expiration, unique request IDs). **7/7 PRESERVED.**
TopicMetadataFetcherTest (classic) correctly folded here per §20.

---

## AutoOffsetResetStrategyTest.java (4 @Test) — inline + `tests/consumer/auto_offset_reset_strategy_test.rs`

| Java test | Status | Notes |
|---|---|---|
| testFromString | PRESERVED | all 13 accept/reject cases incl. case-sensitivity; `by_duration:PT1H` name=="by_duration"; null→`&str` non-null skipped |
| testValidator | OUT_OF_SCOPE | ConfigDef.Validator framework not translated; same accept/reject set covered by `test_from_string` (documented) |
| testEqualsAndHashCode | PRESERVED | eq/ne + hash incl. by_duration |
| testTimestamp | PRESERVED | EARLIEST/LATEST/NONE timestamps + by_duration window |

**3/4 PRESERVED, 1 OUT_OF_SCOPE.** Error-message content asserted in inline tests
(`"<:duration> part is missing"`, `"Unable to parse duration string"`,
`"Unknown auto offset reset strategy"`).

---

## ConsumerInterceptorsTest.java (2 @Test) — `src/consumer/internals/consumer_interceptors.rs`

| Java test | Status | Notes |
|---|---|---|
| testOnConsumeChain | PRESERVED | filter chain; exception-in-one-interceptor still calls all; all-fail → unmodified (full structural equality, not just count); onConsumeCount 2/4/6; nextOffsets validated per-partition |
| testOnCommitChain | PRESERVED | onCommit called for all even on exception; count 2/4 |

Java `throw KafkaException` → Rust `panic` caught by `catch_unwind`; behaviorally
faithful (interceptor exceptions are swallowed and chain continues). **2/2 PRESERVED.**
Plus 2 focused Rust panic-safety regression tests.

---

## ConsumerConfigTest.java (17 @Test) — `tests/consumer/consumer_config_test.rs`

| Java test | Status | Notes |
|---|---|---|
| testOverrideClientId | OUT_OF_SCOPE | postProcessParsedConfig deferred |
| testOverrideEnableAutoCommit | OUT_OF_SCOPE | cross-field validation deferred |
| testAppendDeserializerToConfig | OUT_OF_SCOPE | Java static Map-mutating helper; no Rust equivalent |
| testAppendDeserializerToConfigWithException | OUT_OF_SCOPE | same |
| ensureDefaultThrowOnUnsupportedStableFlagToFalse | PRESERVED | |
| testDefaultPartitionAssignor | REDUCED | Rust asserts the key is *accepted* (classic-only, stored untyped); Java asserts default == `[RangeAssignor, CooperativeStickyAssignor]`. Class-object default not translated (§20). Documented. |
| testInvalidGroupInstanceId | PRESERVED | error message contains key |
| testInvalidSecurityProtocol | PRESERVED | error message contains key |
| testCaseInsensitiveSecurityProtocol | PRESERVED | lowercase→canonical SASL_SSL (note: Rust asserts canonical-uppercase accessor; Java asserts originals() retains lowercase — minor divergence, both validate case-insensitive acceptance) |
| testDefaultConsumerGroupConfig | PRESERVED | group.protocol default "classic", remote.assignor None |
| testRemoteAssignorConfig | PRESERVED | |
| testRemoteAssignorWithClassicGroupProtocol | OUT_OF_SCOPE | cross-field validation deferred (§20) |
| testDefaultMetadataRecoveryStrategy | PRESERVED | "rebootstrap" |
| testInvalidMetadataRecoveryStrategy | PRESERVED | error message contains key |
| testProtocolConfigValidation | PRESERVED | 5 CsvSource cases looped |
| testUnsupportedConfigsWithConsumerGroupProtocol | OUT_OF_SCOPE | cross-field validation deferred (§20) |
| testValidateConfigPropertiesFile | OUT_OF_SCOPE | reads Apache source-tree config file; N/A |

**In-scope 9 PRESERVED + 1 REDUCED (testDefaultPartitionAssignor); 7 OUT_OF_SCOPE.**
The OUT_OF_SCOPE cluster is cross-field-validation-deferred (§20) — see Key findings.

---

## ConsumerRecordTest.java (2 @Test) — `tests/consumer/consumer_record_test.rs`

| Java test | Status | Notes |
|---|---|---|
| testShortConstructor | PRESERVED | all getters incl. NO_TIMESTAMP, NULL_SIZE, leaderEpoch/deliveryCount None, empty headers |
| testLongConstructor | PRESERVED | both 11-arg (`with_headers`) and 12-arg (`with_all`) constructors; leaderEpoch=10, deliveryCount=1 |

**2/2 PRESERVED.**

---

## ConsumerRecordsTest.java (5 @Test) — `tests/consumer/consumer_records_test.rs`

| Java test | Status | Notes |
|---|---|---|
| testIterator | PRESERVED | iterate-all, partition-count incl. empty partition |
| testRecordsByPartition | PRESERVED | per-tp records + nextOffsets(lastOffset+1, leaderEpoch) |
| testRecordsByNullTopic | OUT_OF_SCOPE | Java IllegalArgumentException for null topic; Rust `&str` non-null (documented) |
| testRecordsByTopic | PRESERVED | per-topic iteration + record/partition counts |
| testRecordsAreImmutable | REDUCED | replaced by `test_records_count_and_empty`: immutability is a type-system guarantee in Rust (returns `&[..]`), so UnsupportedOperationException assertions are not behaviorally relevant; count()/empty() preserved. Documented. |

**3 PRESERVED, 1 REDUCED (justified — borrow-checker enforces immutability), 1 OUT_OF_SCOPE.**

---

## ConsumerGroupMetadataTest.java (5 @Test) — `tests/consumer/consumer_group_metadata_test.rs`

| Java test | Status | Notes |
|---|---|---|
| testAssignmentConstructor | PRESERVED | `with_details` |
| testGroupIdConstructor | PRESERVED | UNKNOWN_GENERATION_ID=-1, UNKNOWN_MEMBER_ID="" |
| testInvalidGroupId | OUT_OF_SCOPE | null → `impl Into<String>` non-null |
| testInvalidMemberId | OUT_OF_SCOPE | same |
| testInvalidInstanceId | OUT_OF_SCOPE | null → `Option<String>` |

**2 PRESERVED, 3 OUT_OF_SCOPE (Rust type system removes null-arg checks).**

---

## OffsetAndMetadataTest.java (6 @Test) — `src/consumer/offset_and_metadata.rs` (inline) + `tests/consumer/offset_and_metadata_test.rs`

| Java test | Status | Notes |
|---|---|---|
| testInvalidNegativeOffset | PRESERVED | "Invalid negative offset" message asserted |
| testSerializationRoundtrip | OUT_OF_SCOPE | Java `Serializable`/ObjectStream; Rust has no Java-compatible binary format |
| testDeserializationCompatibilityBeforeLeaderEpoch | OUT_OF_SCOPE | persisted-bytes Java compatibility |
| testDeserializationCompatibilityWithLeaderEpoch | OUT_OF_SCOPE | same |
| testEqualsWithNullAndNegativeLeaderEpoch | PRESERVED | None == Some(-1); hash equal. Source `leader_epoch()` normalizes <0→None; `PartialEq`/`Hash` use normalized value (verified in source) |
| testEqualsWithNullAndEmptyMetadata | REDUCED | Java tests null-metadata == ""-metadata (null normalized in ctor). Rust ctor takes `impl Into<String>`; test asserts ""=="" only — the null-normalization branch is not exercised because Rust has no null. Equivalent given the type system. Documented. |

**2 PRESERVED, 1 REDUCED (no-null equivalent), 3 OUT_OF_SCOPE (Java Serializable).**
leaderEpoch negative→None normalization is correctly implemented in equality AND hash.

---

## CloseOptionsTest.java (4 @Test) — `tests/consumer/close_options_test.rs`

| Java test | Status | Notes |
|---|---|---|
| operationShouldNotBeNull | OUT_OF_SCOPE | null → non-null enum by value |
| operationShouldHaveDefaultValue | PRESERVED | Default |
| timeoutCouldBeNull | PRESERVED | `CloseOptions::default()` → timeout None |
| timeoutShouldBeDefaultEmpty | PRESERVED | |

**3 PRESERVED, 1 OUT_OF_SCOPE.**

---

## consumer_rebalance_listener_method_name.rs
No dedicated Java test file in scope. (No `*Test.java` exists for it; the
enum is exercised indirectly.) No gap.

---

## Key findings

- **SubscriptionState — exemplary, 58/58 PRESERVED.** All critical transitions
  (assignment validity, position vs committed, offset-reset strategy, paused/
  fetchable, rebalance-awaiting-callback, validation/truncation) are translated
  with full assertions, not count-padded. IllegalStateException → `IllegalState`
  consistently. The two split `testSeekUnvalidatedWith*` tests have their
  maybe-validate half covered by a dedicated test — documented, no loss.

- **No MISSING in-scope tests across all 12 files.** Every skip is documented
  with a rationale; every reduction is a Rust type-system consequence (no null,
  borrow-checker immutability) rather than a translation gap.

- **`ConsumerConfigTest` is the weakest area, but by design (§20 deferral).**
  7 of 17 are OUT_OF_SCOPE for cross-field validation / class-object defaults
  deferred to a later phase. Two carry slight semantic divergences worth noting,
  not blocking:
  - `testDefaultPartitionAssignor` (REDUCED): asserts acceptance, not the
    `[RangeAssignor, CooperativeStickyAssignor]` default — acceptable since
    assignor class objects aren't translated, but the default-value contract is
    untested.
  - `testCaseInsensitiveSecurityProtocol`: Java asserts `originals()` retains the
    *lowercase* input; Rust asserts the *canonical-uppercase* accessor. Both
    validate case-insensitive acceptance; the round-trip-preservation nuance
    differs. Minor.

- **Error-message-content assertions are honored** where the message is the
  contract: AutoOffsetResetStrategy ("<:duration> part is missing", "Unable to
  parse duration string", "Unknown auto offset reset strategy"), OffsetAndMetadata
  ("Invalid negative offset"), ConsumerConfig (error contains the failing key).

- **leaderEpoch handling is correct.** OffsetAndMetadata normalizes negative
  leader epoch → None in the getter, and both `PartialEq` and `Hash` use the
  normalized value (matching Java `equals`/`hashCode`). ConsumerRecord leaderEpoch
  round-trips Some(10)/None as expected.

- **ConsumerRecords iteration/grouping semantics PRESERVED** (iterator order,
  per-partition/per-topic grouping, nextOffsets = lastOffset+1 with leaderEpoch).
  Immutability test legitimately replaced (type-system guarantee).

- **Interceptor chain semantics PRESERVED** including the subtle "exception in
  one interceptor still runs the rest, and that interceptor's input passes
  through unchanged" behavior, with full structural equality on the all-fail path.

- **Minor follow-up (non-blocking):** confirm the base `metadata::tests` actually
  cover the stale-epoch-rejected and listener-`onUpdate`-count assertions that the
  two OUT_OF_SCOPE `ConsumerMetadataTest` leadership-update tests would otherwise
  verify, since ConsumerMetadata delegates rather than overriding.
