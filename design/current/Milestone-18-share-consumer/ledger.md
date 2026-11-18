# Milestone 18 — coverage ledger

Scope: the Java 4.4.0 share-consumer closure (KIP-932), tag `4.4.0` = `kafka/` @ `1156b2752a`. Every
row was derived from the 4.4.0 tree with `git ls-files` + `grep`, not from the plan. P11 audits this
file: every row must end as `done` or `skipped` with a reason.

Conventions:

- **Rust target** follows CLAUDE.md §2: no `clients` segment, one Java class per file, nested classes
  in the same file. Paths are relative to `rust/src/`.
- **Visibility** is `pub` only when the Java type is `@InterfaceAudience.Public` in 4.4
  (`design/current/interface-audience-public-4.4.txt`, cross-checked against the annotation in each
  file). Everything under `internals` is `pub(crate)`. `common::requests` is a `pub(crate) mod`
  (`common/mod.rs`), so request wrappers are crate-private whatever their Java modifier.
- **PR** is decision D3 (settled): PR-A = P1–P2, PR-B = P3–P5, PR-C = P6–P9, PR-D = P10.
- **Status**: `todo`, `exists` (already on master, verified), `partial` (some pieces on master),
  `done`, `skipped`.
- Java line counts are `wc -l` of the 4.4.0 file (javadoc included).

## 1. Java main files

### 1.1 Wire wrappers (`org.apache.kafka.common.requests`)

| Java file | Lines | Phase | PR | Rust target | Visibility | Status |
|---|---|---|---|---|---|---|
| `common/requests/ShareFetchRequest.java` | 241 | P1 | A | `common/requests/share_fetch_request.rs` | crate | done |
| `common/requests/ShareFetchResponse.java` | 215 | P1 | A | `common/requests/share_fetch_response.rs` | crate | done |
| `common/requests/ShareAcknowledgeRequest.java` | 120 | P1 | A | `common/requests/share_acknowledge_request.rs` | crate | done |
| `common/requests/ShareAcknowledgeResponse.java` | 146 | P1 | A | `common/requests/share_acknowledge_response.rs` | crate | done |
| `common/requests/ShareGroupHeartbeatRequest.java` | 80 | P1 | A | `common/requests/share_group_heartbeat_request.rs` | crate | done |
| `common/requests/ShareGroupHeartbeatResponse.java` | 88 | P1 | A | `common/requests/share_group_heartbeat_response.rs` | crate | done |
| `common/requests/ShareGroupDescribeRequest.java` | 96 | P1 | A | `common/requests/share_group_describe_request.rs` | crate | done |
| `common/requests/ShareGroupDescribeResponse.java` | 77 | P1 | A | `common/requests/share_group_describe_response.rs` | crate | done |
| `common/requests/ShareRequestMetadata.java` | 115 | P1 | A | `common/requests/share_request_metadata.rs` | crate | done |
| `common/requests/DescribeShareGroupOffsetsRequest.java` | 93 | P1 (used by P9) | A | `common/requests/describe_share_group_offsets_request.rs` | crate | done |
| `common/requests/DescribeShareGroupOffsetsResponse.java` | 103 | P1 (used by P9) | A | `common/requests/describe_share_group_offsets_response.rs` | crate | done |
| `common/requests/AlterShareGroupOffsetsRequest.java` | 94 | P1 (used by P9) | A | `common/requests/alter_share_group_offsets_request.rs` | crate | done |
| `common/requests/AlterShareGroupOffsetsResponse.java` | 113 | P1 (used by P9) | A | `common/requests/alter_share_group_offsets_response.rs` | crate | done |
| `common/requests/DeleteShareGroupOffsetsRequest.java` | 96 | P1 (used by P9) | A | `common/requests/delete_share_group_offsets_request.rs` | crate | done |
| `common/requests/DeleteShareGroupOffsetsResponse.java` | 65 | P1 (used by P9) | A | `common/requests/delete_share_group_offsets_response.rs` | crate | done |
| `ConcreteRequest` / `ConcreteResponse` arms (Java `AbstractRequest#parseRequest`, `AbstractResponse#parseResponse` share cases) for the 7 client APIs above | edits | P1 | A | `common/requests/abstract_request.rs`, `abstract_response.rs` | crate | done (every match arm, incl. `parse_request` / `parse` and the schema-derived `should_client_throttle` arm; the 7 response types join `RequestResponseTest#testClientThrottlesResponsesWithThrottleTime`) |
| Inter-broker share-state wrappers (`Initialize/Read/Write/DeleteShareGroupState*`, `ReadShareGroupStateSummary*`, 10 files) | — | — | — | — | — | skipped: `ApiKeys` marks them `cluster_action` (broker-to-broker); no client code sends them. Re-check at P11 that no client test needs their `ConcreteResponse` arm. |

The three admin share-offsets request/response pairs (6 files) are **missing from PLAN §4 P1**,
which lists only ShareFetch / ShareAcknowledge / ShareGroupHeartbeat / ShareGroupDescribe. They are
needed by P9 (D1 = yes). Put them in P1 (all wire types in one place, PR-A) or at the start of P9; this
ledger assigns them to P1. Together they are 564 Java lines.

### 1.2 Public API and public value types (`org.apache.kafka.clients.consumer`)

| Java file | Lines | Phase | PR | Rust target | Visibility | Status |
|---|---|---|---|---|---|---|
| `consumer/AcknowledgeType.java` | 75 | P2 | A | `consumer/acknowledge_type.rs` | pub (`@InterfaceAudience.Public`) | todo |
| `consumer/AcknowledgementCommitCallback.java` | 59 | P2 | A | `consumer/acknowledgement_commit_callback.rs` | pub | todo |
| `consumer/ShareConsumerConfig.java` | 80 | P2 | A | `consumer/share_consumer_config.rs` | **crate** — the Java class is package-private (`class ShareConsumerConfig extends ConsumerConfig`, no `public`) and is not in the 4.4 audience list. PLAN §1.2 groups it with "public value types"; that is wrong. | todo |
| `consumer/ShareConsumer.java` | 137 | P8 | C | `consumer/share_consumer.rs` (one file per class, CLAUDE.md §2; note the `Consumer` trait precedent lives in `consumer/mod.rs`) | pub | todo |
| `consumer/KafkaShareConsumer.java` | 857 | P8 | C | `consumer/kafka_share_consumer.rs` | pub | todo |
| `consumer/MockShareConsumer.java` | 192 | P8 | C | `consumer/mock_share_consumer.rs` | pub (it is `@InterfaceAudience.Public` and in the main jar, so no test-jar exception is needed, unlike `MockAdminClient`) | todo |
| *(Rust-only)* `DynShareConsumer` | — | P8 | C | `consumer/dyn_share_consumer.rs` | pub | todo — DoD §7 + R8 test plan |
| *(Rust-only, D6 pending)* `ShareConsumerHandle` | — | P7/P8 | C | TBD | pub | todo — only if D6 adopts it; R8 test plan required |

### 1.3 Internals (`org.apache.kafka.clients.consumer.internals`, all `pub(crate)`)

| Java file | Lines | Phase | PR | Rust target | Status |
|---|---|---|---|---|---|
| `internals/ShareAcknowledgementMode.java` | 116 | P2 | A | `consumer/internals/share_acknowledgement_mode.rs` | partial: its nested `Validator` is already translated as `share_acknowledgement_mode_validator()` in `consumer/consumer_config.rs`; P2 should move it to the type's file or have `consumer_config.rs` call the type |
| `internals/ShareAcquireMode.java` | 93 | P2 | A | `consumer/internals/share_acquire_mode.rs` | partial: `Validator` as `share_acquire_mode_validator()` in `consumer_config.rs` (same note) |
| `internals/Acknowledgements.java` | 331 | P2 | A | `consumer/internals/acknowledgements.rs` | todo |
| `internals/AcknowledgementBatch.java` | 108 | P2 | A | `consumer/internals/acknowledgement_batch.rs` | todo |
| `internals/NodeAcknowledgements.java` | 40 | P2 | A | `consumer/internals/node_acknowledgements.rs` | todo |
| `internals/AcknowledgementCommitCallbackHandler.java` | 59 | P2 | A | `consumer/internals/acknowledgement_commit_callback_handler.rs` | todo |
| `internals/ShareSessionHandler.java` | 371 | P3a | B | `consumer/internals/share_session_handler.rs` | todo |
| `internals/ShareInFlightBatch.java` | 210 | P3a | B | `consumer/internals/share_in_flight_batch.rs` | todo (shape gated by D5) |
| `internals/ShareInFlightBatchException.java` | 42 | P3a | B | `consumer/internals/share_in_flight_batch_error.rs` | todo — see gap G9 |
| `internals/ShareCompletedFetch.java` | 461 | P3a | B | `consumer/internals/share_completed_fetch.rs` | todo (shape gated by D5) |
| `internals/ShareFetchConfig.java` | 111 | P3a | B | `consumer/internals/share_fetch_config.rs` | todo |
| `internals/ShareFetchException.java` | 40 | P3a | B | `consumer/internals/share_fetch_error.rs` | todo — see gap G9 |
| `internals/ShareFetchBuffer.java` | 262 | P3b | B | `consumer/internals/share_fetch_buffer.rs` | todo |
| `internals/ShareFetch.java` | 280 | P3b | B | `consumer/internals/share_fetch.rs` | todo |
| `internals/ShareFetchCollector.java` | 192 | P3b | B | `consumer/internals/share_fetch_collector.rs` | todo |
| `internals/ShareFetchMetricsRegistry.java` | 101 | P3b | B | `consumer/internals/share_fetch_metrics_registry.rs` | todo |
| `internals/ShareFetchMetricsManager.java` | 102 | P3b | B | `consumer/internals/share_fetch_metrics_manager.rs` | todo |
| `internals/ShareFetchMetricsAggregator.java` | 61 | P3b | B | `consumer/internals/share_fetch_metrics_aggregator.rs` | todo |
| `internals/ShareMembershipManager.java` | 181 | P4 | B | `consumer/internals/share_membership_manager.rs` | todo |
| `internals/ShareHeartbeatRequestManager.java` | 259 | P4 | B | `consumer/internals/share_heartbeat_request_manager.rs` | todo |
| `internals/ShareConsumerMetadata.java` | 78 | P4 | B | `consumer/internals/share_consumer_metadata.rs` | todo |
| `internals/ShareConsumeRequestManager.java` | 1,799 | P5a + P5b | B | `consumer/internals/share_consume_request_manager.rs` | todo |
| `internals/ShareConsumerImpl.java` | 1,418 | P7 | C | `consumer/internals/share_consumer_impl.rs` | todo |
| `internals/ShareConsumerMetrics.java` | 34 | P7 | C | `consumer/internals/share_consumer_metrics.rs` | todo |
| `internals/ShareConsumerDelegate.java` | 40 | P8 | C | — | skipped: collapses into `KafkaShareConsumer`'s constructor, as `ConsumerDelegate` did (DoD §7 note in P8) |
| `internals/ShareConsumerDelegateCreator.java` | 80 | P8 | C | — | skipped: same reason |

### 1.4 Events and metrics (`internals.events`, `internals.metrics`, all `pub(crate)`)

| Java file | Lines | Java `extends` | Phase | PR | Rust target | Status |
|---|---|---|---|---|---|---|
| `events/SharePollEvent.java` | 67 | `ApplicationEvent` | P6 | C | `ApplicationEvent::SharePoll` in `consumer/internals/events/application_event.rs` | todo |
| `events/ShareFetchEvent.java` | 41 | `ApplicationEvent` | P6 | C | `ApplicationEvent::ShareFetch` | todo |
| `events/ShareAcknowledgeAsyncEvent.java` | 48 | `ApplicationEvent` | P6 | C | `ApplicationEvent::ShareAcknowledgeAsync` | todo |
| `events/ShareAcknowledgementCommitCallbackRegistrationEvent.java` | 31 | `ApplicationEvent` | P6 | C | `ApplicationEvent::ShareAcknowledgementCommitCallbackRegistration` | todo |
| `events/ShareAcknowledgeSyncEvent.java` | 42 | `CompletableApplicationEvent<Map<TopicIdPartition, Acknowledgements>>` | P6 | C | `ApplicationEvent::ShareAcknowledgeSync` | todo |
| `events/ShareAcknowledgeOnCloseEvent.java` | 41 | `CompletableApplicationEvent<Void>` | P6 | C | `ApplicationEvent::ShareAcknowledgeOnClose` | todo |
| `events/ShareSubscriptionChangeEvent.java` | 45 | `CompletableApplicationEvent<Void>` | P6 | C | `ApplicationEvent::ShareSubscriptionChange` | todo |
| `events/ShareUnsubscribeEvent.java` | 31 | `CompletableApplicationEvent<Void>` | P6 | C | `ApplicationEvent::ShareUnsubscribe` | todo |
| `events/ShareAcknowledgementEvent.java` | 54 | none (plain class, **not** a `BackgroundEvent`; `BackgroundEvent.Type` has no share value) | P6 | C | `consumer/internals/events/share_acknowledgement_event.rs` | todo |
| `events/ShareAcknowledgementEventHandler.java` | 60 | none | P6 | C | `consumer/internals/events/share_acknowledgement_event_handler.rs` | todo |
| `metrics/ShareRebalanceMetricsManager.java` | 67 | `RebalanceMetricsManager` | P4 | B | `consumer/internals/metrics/share_rebalance_metrics_manager.rs` | todo — see gap G3 |
| `metrics/KafkaShareConsumerMetrics.java` | 86 | — | P7 | C | `consumer/internals/metrics/kafka_share_consumer_metrics.rs` | todo |

The `events/` directory holds 8 `Share*Event` classes that are application events, plus
`ShareAcknowledgementEvent` and `ShareAcknowledgementEventHandler`. PLAN §1.2 / §4 P6 say "10
`Share*Event`"; the count of `Share*Event.java` files is 9 (8 application events +
`ShareAcknowledgementEvent`).

### 1.5 Admin share-group RPCs (`org.apache.kafka.clients.admin`) — P9, in scope (D1 = yes)

| Java file | Lines | Phase | PR | Rust target | Visibility | Status |
|---|---|---|---|---|---|---|
| `admin/DescribeShareGroupsOptions.java` | 38 | P9 | C | `admin/options/describe_share_groups_options.rs` | pub | todo |
| `admin/DescribeShareGroupsResult.java` | 66 | P9 | C | `admin/describe_share_groups_result.rs` | pub | todo |
| `admin/AlterShareGroupOffsetsOptions.java` | 29 | P9 | C | `admin/options/alter_share_group_offsets_options.rs` | pub | todo |
| `admin/AlterShareGroupOffsetsResult.java` | 94 | P9 | C | `admin/alter_share_group_offsets_result.rs` | pub | todo |
| `admin/ListShareGroupOffsetsOptions.java` | 29 | P9 | C | `admin/options/list_share_group_offsets_options.rs` | pub | todo |
| `admin/ListShareGroupOffsetsResult.java` | 75 | P9 | C | `admin/list_share_group_offsets_result.rs` | pub | todo |
| `admin/ListShareGroupOffsetsSpec.java` | 76 | P9 | C | `admin/list_share_group_offsets_spec.rs` | pub | todo |
| `admin/DeleteShareGroupOffsetsOptions.java` | 29 | P9 | C | `admin/options/delete_share_group_offsets_options.rs` | pub | todo |
| `admin/DeleteShareGroupOffsetsResult.java` | 99 | P9 | C | `admin/delete_share_group_offsets_result.rs` | pub | todo |
| `admin/DeleteShareGroupsOptions.java` | 29 | P9 | C | `admin/options/delete_share_groups_options.rs` | pub | todo |
| `admin/DeleteShareGroupsResult.java` | 54 | P9 | C | `admin/delete_share_groups_result.rs` | pub | todo |
| `admin/ShareGroupDescription.java` | 149 | P9 | C | `admin/share_group_description.rs` | pub | todo |
| `admin/ShareMemberDescription.java` | 125 | P9 | C | `admin/share_member_description.rs` | pub | todo |
| `admin/ShareMemberAssignment.java` | 70 | P9 | C | `admin/share_member_assignment.rs` | pub | todo |
| `admin/SharePartitionOffsetInfo.java` | 100 | P9 | C | `admin/share_partition_offset_info.rs` | pub | todo |
| `admin/internals/DescribeShareGroupsHandler.java` | 196 | P9 | C | `admin/internals/describe_share_groups_handler.rs` | crate | todo (`AdminApiHandler.Batched`) |
| `admin/internals/AlterShareGroupOffsetsHandler.java` | 180 | P9 | C | `admin/internals/alter_share_group_offsets_handler.rs` | crate | todo (`AdminApiHandler.Batched`) |
| `admin/internals/ListShareGroupOffsetsHandler.java` | 203 | P9 | C | `admin/internals/list_share_group_offsets_handler.rs` | crate | todo (`AdminApiHandler.Batched`) |
| `admin/internals/DeleteShareGroupOffsetsHandler.java` | 174 | P9 | C | `admin/internals/delete_share_group_offsets_handler.rs` | crate | todo (`AdminApiHandler.Batched`) |
| `admin/internals/DeleteShareGroupsHandler.java` | 38 | P9 | C | `admin/internals/delete_share_groups_handler.rs` | crate | todo. Java `extends DeleteGroupsHandler`; the Rust `DeleteGroupsHandler::new(api_name, display_name, log_context)` is already parameterised and `delete_consumer_groups_handler.rs` composes it the same way, so this is a thin wrapper |
| `Admin` trait: `describeShareGroups`, `alterShareGroupOffsets`, `listShareGroupOffsets`, `deleteShareGroupOffsets`, `deleteShareGroups` (5 RPCs, each a default no-options form + options form → 10 Rust methods per `admin-client.md` §1) | edits | P9 | C | `admin/mod.rs` | pub | todo |
| `KafkaAdminClient`: the 5 `*_with_options` implementations | edits | P9 | C | `admin/kafka_admin_client.rs` | pub | todo |
| `MockAdminClient` (testFixtures): all 5 share methods `throw new UnsupportedOperationException("Not implemented yet")` in Java (`MockAdminClient#describeShareGroups`, `#alterShareGroupOffsets`, `#listShareGroupOffsets`, `#deleteShareGroupOffsets`, `#deleteShareGroups`) | edits | P9 | C | `admin/mock_admin_client.rs` | pub | todo — return `Error::unsupported_version("Not implemented yet")`, citing each Java method, per `admin-client.md` §9 |
| `ForwardingAdmin` share forwards | — | — | — | — | — | skipped: `ForwardingAdmin` is not translated (no `forwarding_admin.rs` in `admin/`) |
| Already on master: `ListGroupsOptions#forShareGroups`, `GroupType::Share`, `CoordinatorType::Share` | — | — | — | `admin/options/list_groups_options.rs`, `common/group_type.rs`, `admin/internals/coordinator_key.rs` | pub | exists |

### 1.6 Errors (`org.apache.kafka.common.errors`)

| Java file | Phase | Rust target | Visibility | Status |
|---|---|---|---|---|
| `errors/ShareSessionNotFoundException.java` | — | `common/errors/share_session_not_found_error.rs` | pub | exists (verified) |
| `errors/ShareSessionLimitReachedException.java` | — | `common/errors/share_session_limit_reached_error.rs` | pub | exists (verified) |
| `errors/InvalidShareSessionEpochException.java` (not `Share*`-named, but share-only) | — | `common/errors/invalid_share_session_epoch_error.rs` | pub | exists (verified) |
| `errors/InvalidRecordStateException.java` (share-only) | — | `common/errors/invalid_record_state_error.rs` | pub | exists (verified) |

## 2. Java test files

Counts are `grep` of annotations at method level on the 4.4.0 source. `@ParameterizedTest` bodies
run once per source row; the source is noted so the Rust loop can use the exact Java bounds. No share
test file uses `@RepeatedTest`, `@Disabled` or `@Flaky`.

### 2.1 Unit tests

| Java test file | Lines | `@Test` | `@ParameterizedTest` (source) | Phase | PR | Rust location | Status |
|---|---|---|---|---|---|---|---|
| `consumer/ShareConsumerConfigTest.java` | 52 | 1 | 0 | P2 | A | `consumer/share_consumer_config.rs` tests | todo |
| `consumer/internals/ShareAcknowledgementModeTest.java` | 67 | 3 | 0 | P2 | A | `share_acknowledgement_mode.rs` | todo |
| `consumer/internals/ShareAcquireModeTest.java` | 57 | 3 | 0 | P2 | A | `share_acquire_mode.rs` | todo |
| `consumer/internals/AcknowledgementsTest.java` | 529 | 20 | 0 | P2 | A | `acknowledgements.rs` | todo |
| `consumer/internals/AcknowledgementCommitCallbackHandlerTest.java` | 175 | 4 | 0 | P2 | A | `acknowledgement_commit_callback_handler.rs` | todo |
| `consumer/internals/ShareSessionHandlerTest.java` | 667 | 2 | 9 (8 × `@MethodSource("shareFetchConfigProvider")`, 1 × `@EnumSource` INVALID_SHARE_SESSION_EPOCH / SHARE_SESSION_NOT_FOUND / SHARE_SESSION_LIMIT_REACHED) | P3a | B | `share_session_handler.rs` | todo |
| `consumer/internals/ShareCompletedFetchTest.java` | 1,080 | 22 | 0 | P3a | B | `share_completed_fetch.rs` | todo |
| `consumer/internals/ShareFetchBufferTest.java` | 331 | 8 | 0 | P3b | B | `share_fetch_buffer.rs` | todo |
| `consumer/internals/ShareFetchTest.java` | 205 | 9 | 0 | P3b | B | `share_fetch.rs` | todo |
| `consumer/internals/ShareFetchCollectorTest.java` | 575 | 9 | 3 (2 × `testErrorInInitializeSource`, 1 × `testFetchWithOtherErrorsSource`) | P3b | B | `share_fetch_collector.rs` | todo |
| `consumer/internals/ShareFetchMetricsManagerTest.java` | 175 | 6 | 0 | P3b | B | `share_fetch_metrics_manager.rs` | todo |
| `consumer/internals/ShareMembershipManagerTest.java` | 1,688 | 55 | 2 (`notInGroupStates`, booleans) | P4 | B | `share_membership_manager.rs` | todo |
| `consumer/internals/ShareHeartbeatRequestManagerTest.java` | 618 | 10 | 4 (`@ApiKeyVersionsSource(SHARE_GROUP_HEARTBEAT)`, booleans, 2 × single-string `@ValueSource`) | P4 | B | `share_heartbeat_request_manager.rs` | todo |
| `consumer/internals/metrics/ShareRebalanceMetricsManagerTest.java` | 56 | 1 | 0 | P4 | B | `share_rebalance_metrics_manager.rs` | todo |
| `consumer/internals/ShareConsumeRequestManagerTest.java` | 3,955 | 70 | 6 (3 × `@EnumSource` FENCED_LEADER_EPOCH / NOT_LEADER_OR_FOLLOWER, 1 × the same + UNKNOWN_TOPIC_OR_PARTITION, 1 × `handleFetchResponseErrorSupplier`, 1 × booleans) | P5a/P5b (split by test group) | B | `share_consume_request_manager.rs` | todo |
| `consumer/internals/ShareConsumerImplTest.java` | 1,113 | 38 | 1 (`@EnumSource` TOPIC_AUTHORIZATION_FAILED / GROUP_AUTHORIZATION_FAILED / INVALID_TOPIC_EXCEPTION) | P7 | C | `share_consumer_impl.rs` | todo |
| `consumer/KafkaShareConsumerTest.java` | 480 | 5 | 0 | P8 | C | `kafka_share_consumer.rs` | todo |
| `consumer/MockShareConsumerTest.java` | 51 | 1 | 0 | P8 | C | `mock_share_consumer.rs` | todo |
| `consumer/KafkaShareConsumerMetricsTest.java` | 414 | 10 | 0 | P8 | C | `kafka_share_consumer_metrics.rs` (or `kafka_share_consumer.rs`) | todo |
| **Subtotal (19 files)** | 12,288 | **277** | **25** | | | | |

### 2.2 Share cases inside shared test files

| Java test | Share content | Phase | PR | Status |
|---|---|---|---|---|
| `common/requests/RequestResponseTest#testShareFetchResponseShouldNotHaveNullRecords` | 1 `@Test` | P1 | A | done: `share_fetch_response.rs::test_share_fetch_response_should_not_have_null_records` |
| `RequestResponseTest#testSerialization` / `#testErrorCountsIncludesNone` share arms | `createShare{GroupHeartbeat,GroupDescribe,Fetch,Acknowledge}{Request,Response}` and `create{Describe,Alter,Delete}ShareGroupOffsets{Request,Response}` helpers; 4 `errorCounts` asserts (heartbeat 1, describe 1, fetch 2, acknowledge 2) | P1 | A | done — distributed per wrapper file: each `*_request.rs` has `test_serialization_all_versions` (`checkRequest` + `checkErrorResponse`), each `*_response.rs` has the `checkResponse` half; the 4 `errorCounts` asserts are `test_error_counts_includes_none` in the heartbeat, describe, fetch and acknowledge response files. The private `checkRequest` / `checkResponse` / `checkErrorResponse` helpers live once in the test-only `common/requests/request_response_test.rs`. Every wrapper also has a hand-derived byte-level vector test. |
| `consumer/internals/events/ApplicationEventProcessorTest#testSharePollEventCallsShareManagers` | 1 `@Test` (`setupShareProcessor()` used twice: the fixture + this test) | P6 | C | todo |
| `consumer/internals/WakeupTriggerTest#testWakeupFromShareFetchAction` | 1 `@Test` | P6 | C | todo — the Rust `WakeupTrigger` collapses Java's `FetchAction` / `ShareFetchAction` into "the await `select!`s on the token" (`wakeup_trigger.rs` module doc), so the test becomes "a share `poll` blocked on the share fetch buffer returns `Wakeup`"; record as a deviation at the test |
| `consumer/internals/metrics/AsyncConsumerMetricsTest` | 11 tests parameterised by `groupNameProvider` = {`CONSUMER_METRIC_GROUP`, `CONSUMER_SHARE_METRIC_GROUP`} | — | — | exists: `async_consumer_metrics.rs` `group_name_provider()` already loops both groups |
| `admin/ListGroupsOptionsTest#testForShareGroups` | 1 `@Test` | — | — | exists: `list_groups_options.rs::test_for_share_groups` |
| `ConsumerConfigTest`, `SubscriptionStateTest`, `RequestManagersTest`, `ApplicationEventHandlerTest`, `ConsumerNetworkThreadTest`, `CompletedFetchTest` | no share tests (`grep -i 'share\|acknowledg'`: 0 hits, or only the English word "shared") | — | — | n/a |

### 2.3 Admin tests (P9)

| Java test file | Lines | `@Test` | Phase | PR | Rust location | Status |
|---|---|---|---|---|---|---|
| `admin/KafkaAdminClientShareGroupTest.java` | 987 | 22 | P9 | C | `admin/kafka_admin_client.rs` tests (or a sibling test module) | todo. Of the 22, 4 (`testListShareGroups`, `…MetadataFailure`, `…WithStates`, `…WithStatesOlderBrokerVersion`) exercise `listGroups`, which exists; check at P9 whether they are already covered by `test_list_groups_with_types*` and mark them |
| `admin/internals/DeleteShareGroupsHandlerTest.java` | 26 | 0 own; inherits 5 from `DeleteGroupsHandlerTest` | P9 | C | `delete_share_groups_handler.rs` | todo — run the 5 inherited cases against the share handler |
| `KafkaAdminClientTest`, `MockAdminClientTest` | no share cases (0 `ShareGroup` refs) | — | — | n/a |

No dedicated Java tests exist for the admin share options/results/value types or for
`DescribeShareGroupsHandler` / `AlterShareGroupOffsetsHandler` / `ListShareGroupOffsetsHandler` /
`DeleteShareGroupOffsetsHandler` (no `*Share*HandlerTest` besides the delete one).

### 2.4 Integration tests (`clients/clients-integration-tests/.../consumer/`)

"Methods" counts test methods; "invocations" counts cluster configurations run (an
`@ClusterTests({ @ClusterTest, @ClusterTest })` method runs twice). PLAN §1.2's "140 `@ClusterTest`s"
and §4's per-file counts (71 / 27 / 14 / 9 / 9 / 6 / 4) came from a raw `grep -c '@ClusterTest'`,
which also matches each file's `@ClusterTestDefaults` and `@ClusterTests`; the real numbers are below.

| Java class | Lines | Methods | Invocations | Needs admin share RPCs (P9) | Phase | PR | Status |
|---|---|---|---|---|---|---|---|
| `ShareConsumerTestBase.java` (abstract base + helpers) | 772 | 0 | 0 | helpers `sharePartitionOffsetInfo`, `alterShareGroupOffsets`, `deleteShareGroupOffsets`, `waitForAssignment` | P10a | D | todo |
| `ShareConsumerTest.java` | 2,095 | 54 (46 single + 8 `@ClusterTests` × 2 configs) | 62 | 1 (`testDescribeShareGroupOffsetsForEmptySharePartition`) | P10a | D | todo |
| `ShareConsumerDeliveryTest.java` | 1,301 | 26 | 26 | 0 (uses `incrementalAlterConfigs` on group configs) | P10b | D | todo |
| `ShareConsumerDLQTest.java` | 1,036 | 13 | 13 | 0 (uses `describeConfigs` / `incrementalAlterConfigs`) | P10b | D | todo |
| `ShareConsumerLagTest.java` | 417 | 8 | 8 | **all 8** (lag via `listShareGroupOffsets`; two also alter/delete offsets) | P10b | D | todo |
| `ShareConsumerCallbackTest.java` | 345 | 8 | 8 | 0 | P10b | D | todo |
| `ShareConsumerRenewTest.java` | 303 | 5 | 5 | 0 | P10b | D | todo |
| `ShareConsumerRackAwareTest.java` | 200 | 1 (`@ClusterTests` × 2 configs) | 2 | 1 (`describeShareGroups`) | P10b | D | todo |
| **Total** | 6,469 | **115** | **124** | **10** | | | |

The multi-config methods vary only `group.share.assignment.interval.ms` (`0` vs `1000`).

## 3. Closure gaps (shared helpers and shared-class edits)

Found by grepping every share main file's imports and same-package identifiers and checking each
against `rust/src`. "Plan?" says whether PLAN §4 already names it.

| # | Item (Java) | Rust today | Phase | Plan? |
|---|---|---|---|---|
| G1 | `RequestManagers` share fields + `entries()` order + the share `RequestManagers#supplier(...)` overload (second `supplier`, takes `ShareAcknowledgementEventHandler`, `ShareConsumerMetadata`, `ShareFetchBuffer`, `ShareFetchMetricsManager`) | consumer-only | P6 | yes |
| G2 | `ApplicationEventProcessor` share arms: 8 `process(Share…Event)` overloads (`SharePollEvent`, `ShareFetchEvent`, `ShareAcknowledgeSyncEvent`, `ShareAcknowledgeAsyncEvent`, `ShareSubscriptionChangeEvent`, `ShareUnsubscribeEvent`, `ShareAcknowledgeOnCloseEvent`, `ShareAcknowledgementCommitCallbackRegistrationEvent`), plus the `ApplicationEvent.Type` share values | `application_event.rs` module doc: "Share-consumer (KIP-932) variants are NOT translated" | P6 | yes |
| G3 | `metrics/RebalanceMetricsManager` abstract base (`ShareRebalanceMetricsManager extends` it) | **folded into** `consumer_rebalance_metrics_manager.rs` (module doc: "A `RebalanceMetricsManager` trait can be extracted … if/when Share/Streams are translated") | P4 — extract the trait in its own behaviour-neutral commit | **no** |
| G4 | `ConsumerUtils#createShareFetchMetricsManager(Metrics)` | listed as skipped in `consumer_utils.rs` module doc ("metrics framework is deferred"); the two share group-name constants exist | P3b (with `ShareFetchMetricsManager`) | **no** |
| G5 | `TopicIdPartitionSet` (used by `ShareMembershipManager#signalPartitionsAssigned` and `AbstractMembershipManager`) | no type; `AbstractMembershipManager::find_resolvable_assignment_and_trigger_metadata_update` returns `Vec<(Uuid, String, Vec<i32>)>` | P4 — decide whether the share override can take the existing shape (likely), or translate the class | **no** |
| G6 | `SubscriptionState#subscribeToShareGroup` and `SubscriptionType.AUTO_TOPICS_SHARE` (incl. the share branches in `fetchablePartitions` / `hasAutoAssignedPartitions`) | **already exists**: `subscription_state.rs::subscribe_to_share_group`, `SubscriptionType::AutoTopicsShare` | P6 verifies only | plan P6 lists it as new work — correction |
| G7 | `WakeupTrigger#setShareFetchAction` / `ShareFetchAction` | no fetch-action concept in Rust (token + `select!`); nothing to add to `wakeup_trigger.rs`, but the share poll must `select!` the token while waiting on `ShareFetchBuffer` | P6/P7 | partial |
| G8 | `GroupRebalanceConfig` with `ProtocolType.SHARE` (rack id, group id, etc.) | no `GroupRebalanceConfig` type; `async_kafka_consumer.rs` inlines the reads it needs | P7 (inline in `ShareConsumerImpl` the same way) | **no** |
| G9 | `ShareFetchException` / `ShareInFlightBatchException` both `extends SerializationException` and carry payload (`ShareFetchException` holds a generic `ShareFetch<?, ?>`); neither reaches the user. `ShareInFlightBatchException` is never thrown: `ShareCompletedFetch` stores it on the batch (`ShareInFlightBatch#setException`), and `ShareFetch#acknowledge(String, int, long, AcknowledgeType)` / `ShareFetchCollector#collect` read it back. `ShareFetchException` is thrown by `ShareFetchCollector#collect` (`new ShareFetchException(fetch, batch.getException().cause())`) and caught in `ShareConsumerImpl#poll`, which sets `currentFetch = e.shareFetch()` and rethrows `e.cause()` (e.g. a `RecordDeserializationException` or `CorruptRecordException`) | no counterpart. Both become **crate-private carriers**, not `Error` variants. At the `poll` boundary Rust sets `current_fetch` from the carried fetch and returns the carried cause `Error` unchanged; it does **not** map to `Error::Serialization`. A test asserts that the error `poll` returns has the cause's variant and message, and that `current_fetch` was replaced | P3a (carriers), P7 (`poll` catch site) | **no** |
| G10 | `ConsumerConfig` `CLASSIC_` / `CONSUMER_PROTOCOL_UNSUPPORTED_CONFIGS` include the two share keys | exists (`consumer_config.rs` lists `SHARE_ACKNOWLEDGEMENT_MODE_CONFIG` in both) | — | exists |
| G11 | `AbstractRequest` / `AbstractResponse` parse arms for the client share APIs | missing | P1 | yes (as `ConcreteRequest` arms) |
| G12 | Admin share-offsets wire wrappers (6 files, §1.1) | missing | P1 | **no** (plan P1 lists only the 4 consumer APIs) |
| G13 | `ErrorEvent` (`BackgroundEvent`) used by share code | exists as `BackgroundEvent::Error` | — | exists |
| G14 | `IdempotentCloser` (used by `ShareFetchBuffer`, `ShareConsumeRequestManager`) | not translated; consumer precedent uses a flag (`fetch_buffer.rs`, `request_managers.rs` "Why a flag, not `IdempotentCloser`") | follow precedent in P3b/P5 | n/a |
| G15 | `CloseableIterator` (used by `ShareCompletedFetch` over record batches) | none; `completed_fetch.rs` uses its own iterator | P3a, follow `CompletedFetch` | n/a |
| G16 | `ClientTelemetryReporter` / `ClientTelemetryUtils` / `AppInfoParser` (used by `ShareConsumerImpl`, `RequestManagers` share supplier) | not translated for the consumer either (`async_kafka_consumer.rs` module doc) | P7: skip with the same reason (and D2 for KIP-714 methods) | n/a |
| G17 | `ShareAcknowledgementEvent` is delivered through its own `ShareAcknowledgementEventHandler` queue, **not** the background-event queue | new channel; `ShareConsumerImpl` drains it | P6/P7 | implicit |

Plan-vs-code notes for the parent: PLAN §4 P6 lists `SubscriptionState::subscribe_to_share_group`
as work, but it already exists (G6). G3, G4, G5, G8, G9 and G12 are not in the plan; none changes a
phase boundary, but G3 and G5 belong in the P4 behaviour-neutral refactor commit, and G12 adds ~6
small wrappers to P1.

The `rust/xtask/public-audience-allowlist.txt` file named in `admin-client.md` §9 and
`consumer-threading.md` §2 does not exist on this branch; the test-jar exception is
`TEST_JAR_CLASSES` in `rust/xtask/src/java_api.rs`. `MockShareConsumer` is itself
`@InterfaceAudience.Public`, so P8 needs no exception for it; only a Rust-only public handle (D6)
would.

## 4. Deferral ledger (pilot rule R2)

Every "deferred to Phase N", "documented carry-over", stub closure or `unsupported` placeholder in
share code gets a row. A phase cannot close with an unowned row.

| Item | Where in the code | Owning phase | Reason |
|---|---|---|---|
| `ShareFetchRequest` `Builder::build_version` compares the acquire mode against a local `SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED_ID` (`0`, Java `ShareAcquireMode.BATCH_OPTIMIZED.id()`) instead of `ShareAcquireMode` | `rust/src/common/requests/share_fetch_request.rs` (`SHARE_ACQUIRE_MODE_BATCH_OPTIMIZED_ID`) | P3a | `ShareAcquireMode` is translated in P2, in parallel with P1; P3a is the first phase that depends on both. Replace the constant with the P2 type's id and delete it. Behaviour is identical meanwhile. |
