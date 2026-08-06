# COMMENTS.DONE.1 — Milestone 11, Tier 1 Phase 1 (Foundation + Topics CRUD)

Resolved review items from Critic (N=1). Each entry below was fixed by the
Actor (N=1) in the fix cycle and verified against the Java source
(`KafkaAdminClient.java` / `KafkaAdminClientTest.java`, Apache Kafka 4.2).

---

# Tier 1 Phase 3 (Cluster & configs)

## Should-fix

## RESOLVED — `MockAdminClient::describe_cluster` never decremented `timeout_next_requests`
- **File**: `src/admin/mock_admin_client.rs` (`describe_cluster`)
- **Java Reference**: `MockAdminClient.java:340-360` (`describeCluster`, the
  `--timeoutNextRequests` on the timeout branch)
- **Fix**: `describe_cluster` now takes `let mut state` and decrements
  `state.timeout_next_requests -= 1` inside the timeout branch, mirroring Java
  and every sibling mock method. A seeded timeout now recovers on the next call
  instead of timing out forever. Also switched the timeout error from the ad-hoc
  `KafkaError::timeout("Mock timeout")` to the shared `timeout_error()` helper
  (`"The mock timed out the request."`) used by the other methods for
  consistency. New tests: `describe_cluster_timeout_recovers_on_next_call`
  (asserts every future times out on call 1, then call 2 succeeds) and
  `describe_cluster_returns_brokers_and_controller`.

## RESOLVED — mock `describe_configs` / `incremental_alter_configs` / `list_config_resources` wrongly stubbed as "unsupported"
- **File**: `src/admin/mock_admin_client.rs` (the three config methods + `State`)
- **Java Reference**: `MockAdminClient.java:821-895` (`describeConfigs` +
  `getResourceDescription` + `toConfigObject`), `897-1029`
  (`incrementalAlterConfigs` + `handleIncrementalResourceAlteration`),
  `1398-1427` (`listConfigResources`), and constructor `241-276` (seeds
  `brokerConfigs` with `default.replication.factor`)
- **Fix**: All three now translate Java's real in-memory logic. Added the
  backing maps to `State`: `broker_configs: Vec<BTreeMap<String,String>>` (one
  per broker, seeded with `default.replication.factor` in `create()`),
  `client_metrics_configs`, `group_configs`, and `default_group_configs`
  (mirroring Java's fields). `describe_configs` honors the seeded-timeout branch
  (decrementing the counter) then reads per-resource-type config via a new
  `get_resource_description` helper (BROKER → broker config, TOPIC → topic
  configs with `fetchesRemainingUntilVisible` decrement + UnknownTopicOrPartition,
  CLIENT_METRICS/GROUP with empty-name InvalidRequest, GROUP overlays
  `default_group_configs` via `putIfAbsent`). `incremental_alter_configs`
  applies SET/DELETE ops per resource via `handle_incremental_resource_alteration`
  + `apply_alter_ops` (Append/Subtract → InvalidRequest, matching Java's
  `default` branch), creating CLIENT_METRICS/GROUP resources on demand.
  `list_config_resources` lists TOPIC/BROKER/BROKER_LOGGER/CLIENT_METRICS/GROUP
  from the in-memory maps, honoring the empty-set-means-all filter. Removed the
  factually-incorrect comment claiming Java throws
  `UnsupportedOperationException` for `listConfigResources`. New tests:
  `describe_configs_topic_returns_stored_configs`,
  `describe_configs_broker_returns_default_replication_factor`,
  `describe_configs_unknown_topic_is_unknown_topic_error` (exact message),
  `describe_configs_unknown_broker_is_invalid_request` (exact message),
  `describe_configs_timeout_recovers_on_next_call`,
  `incremental_alter_configs_topic_set_and_delete`,
  `incremental_alter_configs_unknown_topic_is_unknown_topic_error` (exact
  message), `incremental_alter_configs_client_metrics_creates_resource`,
  `incremental_alter_configs_empty_client_metrics_name_is_invalid_request`
  (exact message), `list_config_resources_all_types_when_empty`,
  `list_config_resources_filters_by_type`.

## Rule clarification (applied)
- **File**: `.claude/rules/admin-client.md` §9
- **Fix**: Rewrote §9 to state the governing principle explicitly — the
  mock-method decision (real logic vs. unsupported error) is driven **solely by
  what Java's own `MockAdminClient` does for that same method**, not by
  tier/phase. Methods Java implements (topics, `describeCluster`, and the three
  config methods) MUST be translated faithfully; "no in-scope test exercises it"
  is not a licence to stub. Only methods Java leaves as
  `UnsupportedOperationException` (e.g. `createPartitions`, non-empty
  `deleteRecords`) may return `KafkaError::unsupported_version`, and every such
  site must cite the exact Java line that throws (added line citations to the
  two remaining stubs). Attaching a "Java throws unsupported" justification to a
  method Java actually implements is now explicitly a flaggable false statement.

---

# Tier 1 Phase 2 (Partitions & records)

## Should-fix

## RESOLVED — AdminApiDriver's two signature branches (fulfillment-unmap + disconnect-retry) were untested
- **File**: `src/admin/internals/admin_api_driver.rs`,
  `src/admin/kafka_admin_client.rs`
- **Java Reference**: `AdminApiDriverTest.java` — `testFulfillmentUnmapping`,
  `testRetryLookupAfterDisconnect`, `testStaticMapping`, `testCoalescedLookup`,
  `testCoalescedFulfillment`, `testRecoalescedLookup`,
  `testLookupRetryBookkeeping`, `testFulfillmentRetryBookkeeping`
- **Fix**: Added a `#[cfg(test)] pub(crate) mod test_support` to
  `admin_api_driver.rs` translating the Java `MockAdminApiHandler` /
  `MockLookupStrategy` / `TestContext` fakes (fake `AdminApiHandler` /
  `AdminApiFuture` / `AdminApiLookupStrategy` keyed off the request's key set,
  driving the pure synchronous driver logic). Added a `#[cfg(test)]`
  `AdminApiDriver::key_to_broker_id` accessor mirroring Java's `keyToBrokerId`.
  Translated 8 driver-level tests:
  - `fulfillment_unmapping` — **branch (1)**: fulfillment returns a stale-leader
    (`NOT_LEADER_OR_FOLLOWER`) key as `unmapped`; the driver unmaps it, re-issues
    a metadata lookup, re-maps it to a leader, re-sends fulfillment, and
    completes the key.
  - `retry_lookup_after_disconnect` — **branch (2)** at the driver level: a
    `NetworkException` on a fulfillment request unmaps the key back to lookup;
    asserts the retry lookup spec has `tries == 1` and `next_allowed_try_ms ==
    now` (no backoff for lookup), then completes against the new leader.
  - `static_mapping` (static-cache fast-path gap), `coalesced_lookup`,
    `coalesced_fulfillment`, `recoalesced_lookup`, `lookup_retry_bookkeeping`,
    `fulfillment_retry_bookkeeping` (backoff-math gap: fulfillment retry applies
    one jittered `backoff(0)` step; lookup retry applies none).
  Added 2 tests to `kafka_admin_client.rs` covering the new Phase-2
  `Call::set_maybe_retry_fn` / `MaybeRetryOutcome` hook end-to-end via the real
  `new_driver_call`:
  - `driver_call_maybe_retry_disconnect_redrives_lookup` — a `NetworkException`
    through the real Call hook returns `MaybeRetryOutcome::Handled`, unmaps the
    key, and enqueues a fresh least-loaded lookup call (not a re-send to the dead
    node).
  - `driver_call_maybe_retry_non_network_requeues` — a non-network error returns
    `MaybeRetryOutcome::Requeue`, leaving driver state untouched and enqueuing
    nothing.
  Deviation (documented in the module + per test): the closed `ApiRequestScope`
  enum coalesces all dynamic keys into one `SingleLookup` request (Java's
  `MockRequestScope` carries a lookup-context id that can split them). The
  stage-transition logic under test is identical; only the initial lookup
  fan-out coalesces. The `NOT_LEADER_OR_FOLLOWER` → `unmapped_keys`
  *classification* remains covered by the `delete_records_handler` unit test;
  these tests cover the driver's *reaction* to `unmapped_keys`, mirroring how
  Java splits `AdminApiDriverTest` from `DeleteRecordsHandlerTest`.

---

## Should-fix

## RESOLVED — describe_topics-by-id disabled with a factually wrong rationale
- **File**: `src/admin/kafka_admin_client.rs:798-811` (and module doc `:23-34`)
- **Java Reference**: `handleDescribeTopicsByIds` (`KafkaAdminClient.java:2357-2419`,
  uses `convertTopicIdsToMetadataRequestTopic` at `:2379`)
- **Fix**: Implemented describe-by-id via the Metadata API in a new
  `get_describe_topics_by_ids_call` (built with
  `MetadataRequest::convert_topic_ids_to_metadata_request_topic` +
  `build_cluster()` + `topic_description_from_cluster`, keyed by id via
  `cluster.topic_name(id)` / `errors_by_topic_id()`). Removed the wrong
  "requires DescribeTopicPartitions" rationale from the code and module doc and
  replaced it with a note that Java also uses the Metadata API for by-id.
  Replaced `test_describe_topics_by_id_unsupported` with `test_describe_topics_by_ids`
  translating `testDescribeTopicsByIds` (`KafkaAdminClientTest.java:2818`): valid
  id described, non-existent id → `UnknownTopicId` ("TopicId <id> not found."),
  ZERO_UUID → `InvalidTopicException` (client-side, exact message asserted).

## RESOLVED — quota-retry Java test slices not translated (DoD #3)
- **Java Reference**: `KafkaAdminClientTest.java:1059, 1094, 1141` (create),
  `:1266, 1328, 1408` (delete)
- **Fix**: Translated all six quota-retry slices for both create and delete
  (the "UntilRequestTimeOut" pair too, since the carry-forward mechanism below
  makes them testable): `test_create_topics_retry_throttling_exception_when_enabled`,
  `test_create_topics_dont_retry_throttling_exception_when_disabled`,
  `test_create_topics_retry_throttling_exception_when_enabled_until_request_timeout`,
  and the three delete analogues (each covering both by-name and by-id).

## RESOLVED — maybeCompleteQuotaExceededException simplification changed the timeout error
- **File**: `src/admin/kafka_admin_client.rs` create/delete/delete-by-id calls
- **Java Reference**: `KafkaAdminClient.java:1755-1767` +
  `getCreateTopicsCall.handleFailure` (`:1884-1892`)
- **Fix**: Implemented the carry-forward faithfully. Added a
  `ThrottlingQuotaExceeded` variant to `KafkaError` (struct
  `ThrottlingQuotaExceededError` carrying `throttle_time_ms`, plus
  `KafkaError::throttling_quota_exceeded(...)` / `throttle_time_ms()`), mirroring
  Java's `ThrottlingQuotaExceededException`. Each create/delete call now carries a
  per-key quota-exception map forward across quota retries and records its own
  creation `now`; on terminal `handleFailure` a new `maybe_complete_quota_exceeded`
  helper (mirroring `maybeCompleteQuotaExceededException`) completes those futures
  with the quota exception (throttle reduced by the elapsed delta, `max(0, …)`)
  when the failure is a `Timeout`. The non-retry path now completes with the quota
  exception carrying `throttleTimeMs` too. Tests assert `throttle_time_ms() ==
  Some(1000)` (don't-retry) and `Some(0)` (until-timeout).

## RESOLVED — NOT_CONTROLLER retry path entirely untested (DoD #3)
- **Java Reference**: `KafkaAdminClientTest.java:1038`
- **Fix**: Translated `testCreateTopicsHandleNotControllerException` as
  `test_create_topics_handle_not_controller_exception` (NOT_CONTROLLER response →
  metadata refresh advancing the controller → successful retry). The retry is
  routed through the retry-backoff gate, so the test advances the mock clock per
  pump iteration (mirroring the existing disconnect-retry test).

## Minor

## RESOLVED — CreateTopics response config parsing dropped source/is_sensitive/read_only
- **File**: `src/admin/kafka_admin_client.rs` (create handle_response)
- **Java Reference**: `KafkaAdminClient.java:1872-1882` (`configEntry`)
- **Fix**: Response configs are now built with `ConfigEntry::with_metadata(...)`
  populated from `CreatableTopicConfigs.{config_source, is_sensitive, read_only}`.
  Added `ConfigSource::for_id(i8)` mapping the wire config-source id to the public
  `ConfigSource` (mirrors `DescribeConfigsResponse.ConfigSource.forId` composed
  with `KafkaAdminClient.configSource`; returns `Unknown` for unrecognized ids
  rather than panicking, per CLAUDE.md §10).

## RESOLVED — client-side topicNameIsUnrepresentable / topicIdIsUnrepresentable checks missing
- **File**: `src/admin/kafka_admin_client.rs` create_topics / delete_topics / describe_topics
- **Java Reference**: `KafkaAdminClient.java:1727-1733, 1775-1783, 1912, 2322, 2362`
- **Fix**: Added `topic_name_is_unrepresentable` / `topic_id_is_unrepresentable`
  guards to all three by-name methods and both by-id paths (delete + describe).
  Empty names and ZERO_UUID ids are completed client-side with
  `InvalidTopicException("The given topic {name|id} '…' cannot be represented in a
  request.")` and never sent. Exact messages asserted in
  `test_create_topics_invalid_name_unrepresentable`,
  `test_delete_topics_invalid_name_unrepresentable`, and the ZERO_UUID slice of
  `test_describe_topics_by_ids`.

## RESOLVED — testDeleteTopicsPartialResponse and testCreateTopicsRetryBackoff not translated
- **Java Reference**: `KafkaAdminClientTest.java:1234, 990`
- **Fix**: Translated `test_delete_topics_partial_response` (by-name and by-id
  partial responses exercising the unrealized-futures path) and
  `test_create_topics_retry_backoff` (asserts the retry is gated by the backoff:
  the retry does not fire until the mock clock advances past the jittered
  upper-bound backoff).

---

# Tier 2 Phase 2 (Group offsets)

Resolved review items from the Critic (N=1) review of the client-level
offset-RPC tests (introduced in commit `cb16905`). Fixed by the Actor (N=1)
in the fix cycle.

## RESOLVED — `requireStable` option→wire propagation was not test-covered for `list_consumer_group_offsets` (DoD #3)
- **File**: `src/admin/kafka_admin_client.rs` (client-level tests)
- **Java Reference**: `KafkaAdminClientTest.java::testListConsumerGroupOffsetsOptionsWithBatchedApi`
  → helper `verifyListConsumerGroupOffsetsOptions()`
- **Fix**: Added `test_list_consumer_group_offsets_options_with_batched_api`,
  a faithful client-level translation of `verifyListConsumerGroupOffsetsOptions`.
  It builds the request with
  `ListConsumerGroupOffsetsOptions::new().require_stable(true).timeout_ms(Some(300))`
  and a single-partition spec (`TopicPartition("A", 0)`), prepares a
  `FindCoordinator` success, then pumps until the built `OffsetFetch` request
  is queued and inspects it at the wire level. Asserts, matching the Java
  assertions one-for-one:
  - `data.require_stable == true` (the core contract — the flag reaches the wire),
  - the request's `request_timeout_ms() == 300` (Java's
    `clientRequest.requestTimeoutMs()` — translated because the Rust runnable
    derives the per-request timeout from the options-driven deadline, see
    `admin_client_runnable.rs:404`),
  - the built groups map to exactly `[GROUP_ID]`, the group's topics to
    exactly `["A"]`, and the topic's `partition_indexes` to `[0]`.
  The `FindCoordinator` request is matched to the prepared response at send
  time (MockClient `send`), so it never enters the request queue — the first
  queued request is the `OffsetFetch`, which `pump_until_request_queued`
  stops on without advancing the mock clock (keeping the derived timeout at
  exactly 300).
  **Teeth verified**: with the handler temporarily inverted to
  `data.set_require_stable(!self.require_stable)` the new test fails at the
  `assert!(data.require_stable)` line; reverted to the correct
  `self.require_stable` it passes. (Hardcoding `false` instead makes the
  `require_stable` field dead code under `#![deny(warnings)]`, which is itself
  a compile-time guard that the field has exactly one use site.)
  `cargo test --lib` count: 2561 → 2562.

## RESOLVED (documentation note) — undocumented fold of the retriable client tests
- **Java Reference**: `testListConsumerGroupOffsetsRetriableErrors`,
  `testAlterConsumerGroupOffsetsRetriableErrors`,
  `testAlterConsumerGroupOffsetsFindCoordinatorRetriableErrors`,
  `testDeleteConsumerGroupOffsetsFindCoordinatorRetriableErrors`.
- **Resolution (no code change)**: Recording the fold explicitly, as the
  Critic requested, so the omission is auditable per DoD #3. These four Java
  tests have no 1:1 Rust counterpart, and that is a **defensible fold**, not a
  gap, for the following reason: the driver retry/re-lookup loop they exercise
  is handler-agnostic and is already covered end-to-end by
  `test_list_consumer_group_offsets`, which drives a retriable `FindCoordinator`
  error (retried), a retriable `OffsetFetch` error (`CoordinatorLoadInProgress`,
  retried), and `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE` responses (which
  trigger coordinator re-lookup) all in one test. Each handler's own
  retry-vs-unmap-vs-fail *decision* is unit-tested per handler
  (`list`/`alter`/`delete` `*_handle_response` tests, matching the Java
  `*HandlerTest` method-for-method). Because the driver loop does not vary by
  handler, the alter/delete end-to-end retriable variants would re-exercise the
  exact same driver code path with a different handler that is independently
  unit-tested — adding no uncovered behavior. The fold therefore preserves
  coverage of every distinct code path while avoiding redundant end-to-end
  scaffolding.

---

# Tier 2 Phase 1 (Group listing & describe)

Resolved review items from the Critic (N=1) review of `5bfebe8..HEAD`
(`8ff6a3c`..`947042a`). All three were test/completeness gaps; no functional
production-code bug was reported. Fixed by the Actor (N=1) in the fix cycle.

## RESOLVED — `describeClassicGroups` RPC had zero test coverage (DoD #3)
- **File**: `src/admin/kafka_admin_client.rs` (client-level tests)
- **Java Reference**: `KafkaAdminClientTest.java:7075`
  (`testDescribeClassicGroups`), `:7163`
  (`testDescribeClassicGroupsWithAuthorizedOperationsOmitted`), `:7187`
  (`testDescribeMultipleClassicGroups`).
- **Fix**: Translated all three client-level slices using the existing
  `env()` / `find_coordinator_resp` / `describe_groups_*_resp` harness:
  - `test_describe_classic_groups`: retriable `FindCoordinator` errors are
    retried; retriable / `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE`
    `DescribeGroups` errors trigger a re-lookup; the final response's two
    members (one a static member) have their assignment bytes decoded via
    `ConsumerProtocol::deserialize_assignment` into the expected 3 partitions;
    asserts `ClassicGroupState::Stable`.
  - `test_describe_classic_groups_with_authorized_operations_omitted`: asserts
    `authorized_operations()` is empty when omitted (Java returns `null`;
    `valid_acl_operations(AUTHORIZED_OPERATIONS_OMITTED)` returns an empty set).
  - `test_describe_multiple_classic_groups`: two group ids on one coordinator,
    batched into a single `DescribeGroups` request; asserts both keys present in
    `described_groups()`.
  New helpers: `find_coordinator_error_resp`, `described_member`,
  `describe_groups_full_resp`. Commit `fixup! ...wire group RPCs...`.

## RESOLVED — list-groups states/types filter wiring + older-broker path untested (DoD #3)
- **File**: `src/admin/kafka_admin_client.rs` (client-level tests)
- **Java Reference**: `KafkaAdminClientTest.java:3229`
  (`testListGroupsWithTypes`), `:3266`
  (`testListGroupsWithTypesOlderBrokerVersion`), `:3471`
  (`testListConsumerGroupsWithStates`), `:3644`
  (`testListConsumerGroupsWithTypesOlderBrokerVersion`), the deprecated
  variants, and the metadata-failure slices (`:3448`).
- **Fix**: Translated `test_list_groups_with_types`,
  `test_list_groups_with_types_older_broker_version`,
  `test_list_consumer_groups_with_states`,
  `test_list_consumer_groups_with_types_older_broker_version`,
  `test_list_consumer_groups_deprecated_with_states_and_types`,
  `test_list_consumer_groups_deprecated_older_broker_version`,
  `test_list_consumer_groups_metadata_failure`, and
  `test_list_groups_metadata_failure`. The wiring tests inspect the emitted
  `ListGroups` request (built from the queued `ClientRequest`) and assert its
  `states_filter` / `types_filter` match the options-derived filter — a swap or
  drop of those fields now fails CI. The metadata-failure tests assert
  `handle_failure` wraps the error as
  "Failed to find brokers to send list{Groups,ConsumerGroups}".
  - **Deviation (documented)**: the Rust `MockClient` does not negotiate API
    versions and never invokes `ListGroupsRequestBuilder::build_version`, so the
    real broker-side downgrade cannot run end-to-end through the mock. It is
    modeled two ways: the *omit* path builds the emitted request at v4
    (the negotiated version) and asserts the types filter is dropped; the
    *reject* path uses `prepare_unsupported_version_response` (the same
    version-mismatch response the real `NetworkClient` produces when the builder
    throws `UnsupportedVersionException`), and asserts the future surfaces
    `UnsupportedVersion`. The builder's own version-gating remains unit-tested in
    `list_groups_request.rs`. New helpers: `env_nodes_with_props`,
    `pump_until_request_queued`, `listed_groups`. Commit `fixup! ...wire group RPCs...`.

## RESOLVED — `ConsumerProtocol` omitted 3 public methods (DoD #2, LOW)
- **File**: `src/consumer/internals/consumer_protocol.rs`
- **Java Reference**: `ConsumerProtocol.java:128-145`
  (`deserializeConsumerProtocolSubscription` x2), `:198-213`
  (`deserializeConsumerProtocolAssignment` x2), `:167-170`
  (`serializeAssignment(ConsumerProtocolAssignment, short)`).
- **Fix**: Added `deserialize_consumer_protocol_subscription` (+`_versioned`),
  `deserialize_consumer_protocol_assignment` (+`_versioned`), and
  `serialize_assignment_data` (the `serializeAssignment(data, short)` overload —
  renamed with a `_data` suffix since Rust cannot overload `serialize_assignment`).
  Each is a one-liner over the already-present generated `read` /
  `to_version_prefixed_byte_buffer`, with two new round-trip unit tests. The
  `static {}` LOWEST/HIGHEST cross-schema invariant check is intentionally not
  ported (harmless compile-time-constant assertion). Commit
  `fixup! ...translate ConsumerProtocol...`.

---

# Tier 2 Phase 3 (Group / member deletion)

## RESOLVED — `removeMembersFromConsumerGroup` (removeAll) reused one up-front deadline for both the describe and LeaveGroup drivers (Behavior Mismatch)
- **File**: `src/admin/kafka_admin_client.rs` (`remove_members_from_consumer_group`, removeAll branch)
- **Java Reference**: `KafkaAdminClient.java:4169-4187` (`getMembersFromGroup` →
  `describeConsumerGroups(Collections.singleton(groupId))`, DEFAULT options) and
  `:4224-4230` (`memFuture.whenComplete` → `invokeDriver(handler, adminFuture,
  options.timeoutMs())` → `invokeDriver` computes
  `calcDeadlineMs(time.milliseconds(), timeoutMs)` at that later moment).
- **Fix**: Both divergences corrected faithfully to Java's control flow.
  1. **Describe step timeout source.** The describe driver's deadline is now
     `calc_deadline_ms(now, None, default_api_timeout_ms)` (i.e.
     `now + defaultApiTimeoutMs`), independent of the removeMembers request's
     `options.timeout()` — matching Java issuing the describe with a default
     `DescribeConsumerGroupsOptions` (`timeoutMs == null`). Previously the
     describe deadline was tied to `options.timeout()`.
  2. **LeaveGroup deadline recomputation.** The `LeaveGroup` driver's deadline is
     now recomputed INSIDE the describe-completion callback from the current
     time: `let leave_now = (ctx.time_provider)();
     let leave_deadline = calc_deadline_ms(leave_now, options_timeout,
     default_api_timeout_ms);`, and `invoke_driver(driver, ctx, leave_now)` uses
     that fresh `now`. This gives LeaveGroup a fresh full timeout window starting
     when describe completes (Java `calcDeadlineMs(time.milliseconds(), ...)` at
     `whenComplete` time), instead of baking the pre-describe `deadline`/`now`.
  - **Regression tests added** (both fail against the pre-fix code, verified by
    temporarily reverting the two deadline lines):
    - `test_remove_all_describe_uses_default_api_timeout`: with
      `request.timeout.ms=30000`, `default.api.timeout.ms=20000`,
      `options.timeout_ms(Some(5000))`, asserts the describe coordinator-lookup
      request carries `request_timeout_ms == 20000` (default-API budget), not the
      buggy `5000` (options budget).
    - `test_remove_all_leave_group_deadline_computed_after_describe`: advances the
      mock clock to `10000` (past the pre-describe LeaveGroup window
      `1000 + 5000 = 6000`) before describe is driven; asserts the LeaveGroup
      coordinator lookup is issued with a fresh `request_timeout_ms == 5000`
      (deadline `10000 + 5000`). Against the buggy code the baked window is
      expired and no LeaveGroup request is ever queued (operation times out).
  - Commit `fixup! Milestone 11 Tier 2 Phase 3: group/member deletion` referencing `6bb701c`.

## RESOLVED — `LeaveGroup` wire type had no flexible-framing byte-level known-vector test (DoD #3)
- **File**: `src/common/requests/leave_group_request.rs`,
  `src/common/requests/leave_group_response.rs`
- **Java Reference**: `generator/messages/LeaveGroupRequest.json`
  (`flexibleVersions: "4+"`, `Reason` field `versions: "5+"`) and
  `LeaveGroupResponse.json` (`flexibleVersions: "4+"`).
- **Fix**: Added hand-computed flexible (v5) known-vector tests, derived from the
  spec (not by pasting serializer output):
  - `serialize_known_byte_vector_v5_flexible` (request): builds a v5
    `LeaveGroupRequest` with a member carrying a NON-NULL `Reason` and asserts the
    exact body bytes
    `[0x02,0x67, 0x02, 0x02,0x6D, 0x02,0x69, 0x02,0x72, 0x00, 0x00]` — compact
    `group_id`, compact `[]MemberIdentity` array, compact member id / nullable
    group_instance_id / nullable `Reason` "r" (0x02 0x72, the v5 headline field),
    plus member-level and top-level tagged-field bytes.
  - `parse_known_byte_vector_v5_flexible` (response): the symmetric v5 vector
    `[0x00,0x00,0x00,0x00, 0x00,0x00, 0x02, 0x02,0x6D, 0x02,0x69, 0x00,0x19, 0x00,
    0x00]` (throttle, error, compact members array, compact member id / nullable
    group_instance_id, member error 25, member + top-level tagged fields).
  - Commit `fixup! Milestone 11 Tier 2 Phase 3: LeaveGroup/DeleteGroups wire types` referencing `9ed0648`.

---

# Tier 3 Phase 1 (ACLs)

## RESOLVED — `AclOperationTest` / `AclPermissionTypeTest` not translated 1:1 (exhaustive `testName`/`testIsUnknown` loops reduced to spot-checks) (DoD #3, LOW)
- **File**: `src/common/acl/acl_operation.rs` (`#[cfg(test)] mod tests`),
  `src/common/acl/acl_permission_type.rs` (`#[cfg(test)] mod tests`)
- **Java Reference**: `AclOperationTest.java` (`testIsUnknown`, `testCode`,
  `testName`, `testExhaustive`) and `AclPermissionTypeTest.java` (same four
  methods).
- **Fix**: Both test modules rewritten to mirror the faithfully-translated
  `ResourceTypeTest` (`src/common/resource/resource_type.rs`). Each now builds
  an `INFOS`-style table `(operation/ty, code, lowercase_name, unknown)` for
  ALL 16 `AclOperation` variants and ALL 4 `AclPermissionType` variants (in
  declaration/code order, transcribed 1:1 from the Java `INFOS[]` arrays), plus
  a test-local `VALUES` array mirroring Java's `values()` (a test-only fixture;
  no production `VALUES` const added, so production code is untouched). Four
  loop-driven tests per file replace the prior spot-checks, matching Java
  method-for-method:
  - `test_is_unknown` (Java `testIsUnknown`) — loops every variant asserting
    `is_unknown()` matches the table.
  - `test_code` (Java `testCode`) — asserts `VALUES.len() == INFOS.len()`,
    loops asserting `code()` and `from_code(code)` round-trip for every variant,
    and `from_code(120) == Unknown`.
  - `test_name` (Java `testName`) — loops every variant asserting
    `from_string(lowercase_name) == variant` (all 16 ops / 4 perms), plus
    `from_string("something") == Unknown`. This is the arm that pins every
    `from_string` match arm.
  - `test_exhaustive` (Java `testExhaustive`) — asserts `INFOS` and `VALUES`
    agree element-for-element in order.
  The three prior Rust-added spot-check tests (`code_round_trips_for_all_variants`,
  `code_values_match_java_wire_values`, `unknown_code_maps_to_unknown`) are
  subsumed by the exhaustive `test_code`; the `from_string_*` / `is_unknown`
  spot-checks are subsumed by `test_name` / `test_is_unknown`. No coverage lost;
  13 previously-unchecked `from_string` arms on `AclOperation` and 1 on
  `AclPermissionType` are now pinned.
  - **Teeth verified**: temporarily corrupting the `"IDEMPOTENT_WRITE" =>
    AclOperation::IdempotentWrite` arm to `=> AclOperation::Unknown` makes
    `test_name` fail (`from_string(idempotent_write) was supposed to be
    IDEMPOTENT_WRITE`); reverted to the correct arm it passes.
  - `cargo test --lib` count: 2710 → 2709 (net −1; the four exhaustive tests
    per file replace five/four spot-check tests while strictly widening
    coverage). Lint clean, format-check clean.
  - Commit `fixup! Milestone 11 Phase 1: review-only Admin API skeleton`
    referencing `9fe737e`.

---

## [RESOLVED] Tier 3 Phase 2 — `ClientQuotaEntity` map value type cannot represent a null entity name (default quota entity) — wire-incompatible with Java

**Original issue** (from `COMMENTS.1.md`, Severity: Behavior Mismatch / latent
wire-compat bug):

> Java's `ClientQuotaEntity` is a `Map<String, String>` whose values may be
> `null`, and a `null` entity name is the protocol representation of the
> *default* client-quota entity (e.g. `--entity-type users --entity-default`).
> The Rust translation modeled the map as `HashMap<String, String>`, which
> cannot hold a null value. Consequences: (1) cannot express a default entity
> for an alteration; (2) encode always emits a non-null empty string (`0x01`)
> instead of wire-null (`0x00`), so default-quota `alterClientQuotas` targets an
> entity literally named `""`, not the default — wire-incompatible with Java;
> (3) both response decoders coerce null → `""` via `unwrap_or_default()`,
> collapsing Java's null (default) and an empty-named entity into one key.

**Java references:** `org/apache/kafka/common/quota/ClientQuotaEntity.java:28,49`
(`Map<String, String> entries`, javadoc "If a name is null, then it is mapped to
the built-in default entity name"); `AlterClientQuotasRequest.java:50-51`
(`setEntityName(...)` sends the raw, possibly-null value);
`DescribeClientQuotasResponse.java:53` / `AlterClientQuotasResponse.java:46`
(store null names verbatim); wire specs `AlterClientQuotasRequest.json:31`,
`DescribeClientQuotasResponse.json:35` (`EntityName` `"nullableVersions": "0+"`).

**Fix applied:**
- `src/common/quota/client_quota_entity.rs`: `entries` field changed from
  `HashMap<String, String>` to `HashMap<String, Option<String>>` (`None` = the
  built-in default entity, the faithful Rust representation of Java's nullable
  map value). Constructor and `entries()` accessor updated to the new value
  type; rustdoc documents the Java `Map<String,String>`-nullable ↔ Rust
  `Option<String>` correspondence and the `None` vs `Some("")` distinction.
  The `USER` / `CLIENT_ID` / `IP` constants and `is_valid_entity_type` are
  unchanged (Java's `ClientQuotaEntity` has no `TYPES` set — only these three).
- **Encode** (3 sites) now maps `None` → `set_entity_name(None)` (wire-null,
  `0x00`) and `Some(name)` → `set_entity_name(Some(name))` (non-null, possibly
  empty): `alter_client_quotas_request.rs` builder,
  `alter_client_quotas_response.rs::from_quota_entities`,
  `describe_client_quotas_response.rs::from_quota_entities`.
- **Decode** (3 sites) drops `unwrap_or_default()` and stores the wire
  `Option<String>` verbatim (null → `None`, non-null → `Some(name)`):
  `alter_client_quotas_request.rs::entries`,
  `alter_client_quotas_response.rs::results`,
  `describe_client_quotas_response.rs::entities`. (The `get_error_response` site
  already echoed the raw `Option<String>` name verbatim — unchanged.)
- Callers updated to the new value type: `kafka_admin_client.rs`
  `new_client_quota_entity` test helper, `client_quota_alteration.rs` test
  helper, all per-file `entity(...)` test helpers, and the
  `tests/integration/admin_quotas_test.rs` `client_id_entity` helper (all wrap
  concrete names in `Some(...)`, faithful to Java's `newClientQuotaEntity`).

**Public API faithfulness:** Java exposes `Map<String, String>` with nullable
values; Rust cannot express a null `String`, so `HashMap<String, Option<String>>`
is the faithful translation. Documented in the type's rustdoc.

**Tests added (DoD #3):**
- `known_wire_vector_default_entity_null_name` (alter request): hand-computed
  byte vector asserting a default entity (`None`) encodes the entity name as a
  wire-NULL compact-nullable string (`0x00`), and directly contrasts an
  empty-string name (`Some("")`) encoding to non-null zero-length (`0x01`) at
  the same byte offset. **Teeth:** the pre-fix `HashMap<String,String>` cannot
  even construct a `None`-named entity; the closest proxy (`Some("")`) encodes
  to `0x01`, so the `bytes[7] == 0x00` assertion fails against pre-fix behavior.
- `default_entity_and_empty_name_survive_round_trip_distinctly` (alter request):
  encode→serialize→parse→decode; `None` stays `None`, `Some("")` stays
  `Some("")`, and the two entities remain distinct.
- `results_preserve_default_entity_null_name` (alter response) and
  `entities_preserve_default_entity_null_name` (describe response): default
  (`None`) and empty-named (`Some("")`) entities decode back distinctly through
  each response path (pre-fix `unwrap_or_default()` collapsed both to `""`).
- `default_entity_none_distinct_from_empty_name` (entity unit test): `None` and
  `Some("")` are representable and unequal.

**Verification:** `cargo build` clean; `cargo test --lib` 2742 → 2747 (+5 new
tests, 0 failures); `cargo xtask lint` clean; `cargo xtask format-check` clean.
Commit `fixup! Milestone 11 Phase 1: review-only Admin API skeleton` referencing
POJO commit `60f7977` (wire encode/decode noted against `c6c7a31`).

---

# M11 bindings B0 + B1 (Critic round 1)

## Issue: `join_map_short_circuits_on_first_error` does not discriminate short-circuiting from collect-all

**Fixed.** The test completed the failing key first and a succeeding key second,
then asserted only that the error surfaced — which a collect-all-then-return-the-
first-error implementation satisfies identically.

Rewritten with three entries: `bad` (fails, first), `ok` (succeeds), and
`never`, whose `KafkaFutureImpl` is **never completed**. `joined.get()` is driven
under `tokio::time::timeout(5s)`, so a short-circuiting implementation abandons
`never` and returns the error at once, while any collect-all implementation
awaits it and trips the timeout. `join_map_results`' counterpart test was already
discriminating (failing key first); the asymmetry is gone.

**Teeth verified:** with `JoinMapFuture::get` temporarily rewritten to
collect-all-then-return-the-first-error, the test fails with
`join_map must abandon the keys after the failing one, not await them: Elapsed(())`
after 5.01s; it passes again once reverted.

Commit: `fixup! Milestone 11 bindings B0: add KafkaFuture::join_map_results`
(`2823cc27`).

## Issue: `test_mock_admin.c`'s NULL-`out_result` banner still describes the pre-`aebb75f2` (leaking) design

**Fixed.** The banner claimed "the handle is freed internally", which is exactly
the behaviour `aebb75f2` removed as a bug — the handle cannot be freed from
`finish_sync`'s generic context, because `R` is the opaque `[u8; 0]` marker
rather than the inner state. Replaced with the actual contract: a NULL
`out_result` means the caller does not want the result, so the handle is never
built (pointing the reader at `finish_sync`).

Commit: `fixup! Milestone 11 bindings B1: fix finish_sync leak on a NULL out_result`
(`aebb75f2`).

## Issue: `_async` callbacks are documented as firing on the dispatcher thread, but three paths fire inline on the caller's thread

**Fixed.** The behaviour was right (an inline fire is what keeps the callback
obligation total); the documented contract was wrong, and cbindgen copies it into
`target/include/confluent_kafka.h`, so it is what a C consumer reads.

- The module doc's API-shape bullet no longer asserts a thread; it points at a
  new **`# Callback thread`** section that states all three cases: normally the
  handle's dispatcher thread; **synchronously on the calling thread, before the
  entry point returns**, when `admin` is NULL or argument marshaling fails before
  submission (explicitly noting this is reachable on plain bad input via an
  unparseable base64 topic id, not only on a programming error); and a tokio
  worker thread once the dispatcher has been torn down. It spells out the two
  consequences the old wording hid: do not hold a lock across `..._async(...)`
  and re-acquire it in the callback, and publish `user_data` before the submit.
- `kafka_admin_AdminClient_close_async` and `_create_topics_async` — the two
  entry points that made the unqualified claim — now say "normally on the
  handle's dispatcher thread, but **synchronously on the calling thread** if
  `admin` is NULL", and cross-reference the section.
- `_delete_topics_by_ids_async` / `_describe_topics_by_ids_async` gained an
  explicit note that an unparseable or NULL id fires the callback synchronously
  before the function returns, since that is the production-reachable case.
- The internal helper docs on `admin_async_void_op` / `admin_async_value_op` were
  corrected the same way, so the next slice copies accurate wording.

Commit: `fixup! Milestone 11 bindings B0: admin C FFI foundation` (`bb01dbc1`).

## Issue: B1 is missing two of its six RPCs, with no recorded deferral

**Fixed by landing both RPCs.** Manager resolution: `PLAN-bindings.md` §4's B1 row
is the source of truth (`createTopics, deleteTopics, listTopics, describeTopics,
createPartitions, deleteRecords`) and the slice is titled "Topics & partitions",
so `createPartitions` belongs in it. The task brief named only the four topic
RPCs — a brief error, not a deferral.

`createPartitions` and `deleteRecords` now have the same surface as the other
four: sync + `_async` entry points, a per-key result handle with `_get_error(i)`,
a callback typedef, cbindgen allowlist entries, C tests, Python bindings and
Python tests.

- `kafka_admin_NewPartitions_t` mirrors `NewTopic`'s two-constructor handling:
  `_new(total_count)` is `NewPartitions.increaseTo(int)` and `_add_assignment`
  switches to `increaseTo(int, List<List<Integer>>)`.
- `createPartitions`' `Map<String, NewPartitions>` becomes two parallel arrays;
  a pair is skipped when **either** side is NULL, because skipping one side alone
  would shift every later pairing.
- `deleteRecords` needs no input handle (`RecordsToDelete` carries only
  `beforeOffset`): three parallel arrays. Its result exposes
  `_get_topic` / `_get_partition` / `_get_low_watermark` / `_get_error`;
  `TopicPartition` is not `Ord`, so entries sort by `(topic, partition)`
  explicitly to keep index addressing reproducible.
- `CreatePartitionsResult` has no `_get_value`, matching `DeleteTopicsResult`:
  Java's per-topic future is `KafkaFuture<Void>`, so a null error *is* success.

**Mock coverage is thin by Java parity, as anticipated.** Java's
`MockAdminClient.createPartitions` throws
`UnsupportedOperationException("Not implemented yet")`
(MockAdminClient.java:626-628, verified against the in-tree Java source) and
`deleteRecords` returns an empty result for an empty request but otherwise throws
the same (:630-638). Per `admin-client.md` §9 the Rust mock returns a per-key
`KafkaError::unsupported_version("Not implemented yet")` instead of panicking,
which the tests assert exactly (code 35, exact message) with the Java lines
cited. The mock therefore still exercises the whole marshaling + flattening path;
only the success outcome is unavailable, which is why the issue below matters more
for these two RPCs.

Commit `ce295635`.

## Issue: the production `AdminClient` success path has no test in any language

**Fixed.** New `bindings/c/tests/test_kafka_admin.c` (11 tests) registered in
`CMakeLists.txt` as the `kafka_admin` CTest target, plus 6 Python counterparts in
`test_admin.py`. Broker-less, following `test_kafka_consumer.c:15-22`. Covers
exactly the B0 code the Critic identified as uncovered:

- runtime construction **and** `runtime.enter()` so `new_admin_client` can spawn
  the admin background task (three construction variants: `_from_configs`,
  `_put`, and a NULL `out_error`);
- `close` / `_close_async` / `_destroy` against `AdminKind::Kafka`, including
  close idempotence and the negative (Java no-argument) timeout;
- `mock_ref`'s rejection arm, asserting the exact message
  `"this operation is only supported on a MockAdminClient"` (DoD #3), plus its
  null-handle message.

Where an outcome depends on whether anything is listening on localhost:9092, both
outcomes are accepted and freed, so the test asserts termination and the ownership
contract rather than a round-trip.

**This suite found a real production bug** (commit `21e9c3f0`):
`AdminClientRunnable::should_exit` counted *every* outstanding call, whereas
Java's `threadShouldExit` consults `hasActiveExternalCalls()`, which skips calls
with `internal == true` (KafkaAdminClient.java:1419-1441). The internal metadata
refresh is re-created on every backoff expiry, so `close(timeout)` blocked for the
full timeout and `close()` with no timeout (`Duration::from_millis(i64::MAX)`)
never returned at all. Fixed to mirror Java, with two regression tests whose teeth
were verified by reverting the predicate.

Commit `afeeff35` (tests), `21e9c3f0` (the bug it found).

---

# M11 bindings B0 + B1 (Critic round 2)

## Issue: `close(timeout)` can overrun the hard-shutdown deadline by up to `request.timeout.ms` — the poll-timeout clamp to `curHardShutdownTimeMs` was never translated

**Fixed** in `src/admin/internals/admin_client_runnable.rs` (`run_once`, phase 2).
The Critic's diagnosis was exact: `hard_shutdown_deadline_ms` was read in one
place only (`should_exit`), so nothing bounded the poll itself.

Java (`KafkaAdminClient.java:1500-1502`, verified in the in-tree source):

```java
long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
}
```

The Rust translation now sits at the same point in the phase order — immediately
after the timeout-processor minimum (`handle_timeouts`) and before the
node-assignment / metadata / send phases, all of which only ever lower the value
with `.min`, so the clamp cannot be undone later:

```rust
let hard_shutdown_deadline_ms = self.shutdown.hard_shutdown_deadline_ms.load(Ordering::Acquire);
if hard_shutdown_deadline_ms != NO_HARD_SHUTDOWN {
    poll_timeout = poll_timeout.min(hard_shutdown_deadline_ms.saturating_sub(now));
}
```

`saturating_sub` rather than Java's plain subtraction: `close()` with a negative
(Java no-argument) timeout stores `i64::MAX` as the deadline, and saturating
arithmetic keeps that unreachable-deadline case overflow-free for every value of
`now`. For all reachable values the result is identical to Java's.

**Test**: `close_bounds_the_poll_timeout_by_the_hard_shutdown_deadline` in
`src/admin/kafka_admin_client.rs`. It exercises `run_once`/`run` rather than the
`should_exit` predicate, which is what the two round-1 shutdown tests could not
reach:

- an external `listTopics` call is driven until it is genuinely in flight
  (`correlation_id_to_calls`), so `pending_calls` is empty and the
  `retry_backoff_ms` floor does not apply — the Critic's reachable path;
- `closing` + a hard deadline 100 ms out stand in for
  `close(Duration::from_millis(100))`;
- the client is a new test-only `WaitingClient` wrapper that records the timeout
  each `poll` is handed and, once armed, advances the mock clock by it — i.e. it
  behaves like a real `poll` that finds an idle socket and waits its whole
  budget. `MockClient::poll` ignores its timeout, so without this the wait the
  loop *would* have performed is unobservable. This mirrors Mockito's
  `verify(client).poll(captor.capture(), anyLong())`; the same wrapper shape
  already exists for the consumer (`CountingClient` in
  `consumer/internals/consumer_network_thread.rs`), which is the DoD #7
  justification for a struct with no Java counterpart.

The test asserts both that every post-`close()` poll timeout is `<= 100` and
that the loop advanced the clock by at most 100 ms before exiting. Teeth
verified by deleting the clamp: it fails with
`every poll after close() must be clamped to the remaining shutdown budget (100 ms), got [60000]`
— i.e. the unclamped loop waits on the `default.api.timeout.ms`-derived call
deadline (and in production on `NetworkClient`'s `request.timeout.ms` cap, 30 s).

## Issue (LOW): the per-entry-point callback-thread docs drop the third case, while the module doc claims they restate it "in full"

**Fixed** by taking the Critic's preferred option — adding the missing case to
all 9 `_async` entry points, so the module-doc claim becomes true and C readers
get the thread-affinity contract in the only place they read.

Each of the 9 (`close`, `create_topics`, `delete_topics`,
`delete_topics_by_ids`, `list_topics`, `describe_topics`,
`describe_topics_by_ids`, `create_partitions`, `delete_records`) now restates all
three cases: dispatcher thread, synchronously on the calling thread, **and** on a
tokio worker thread when the dispatcher has already been torn down by the time
the result arrives (reachable only during `kafka_admin_AdminClient_destroy`),
with the consequence spelled out — callbacks are not guaranteed to be serialised
on one thread. The wording is also now uniform across the 9 (the previous text
had drifted into two line-wrapping variants); the two by-ids entry points keep
their extra unparseable-id clause.

Case 3 was re-verified before documenting it: `enqueue_or_run_inline`
(`src/ffi/common.rs:241-245`) runs the job on the sending thread when
`tx.send` fails, and that sender is the spawned tokio task.

Verified in the **generated** header, not the source:
`grep -c "tokio worker" target/include/confluent_kafka.h` → 9.

---

# Bindings B0 + B1 — Critic round 3

## RESOLVED — `close(timeout)` is not bounded by `timeout` (Java's `thread.join(waitTimeMs)` is)
- **File**: `src/admin/kafka_admin_client.rs` (`KafkaAdminClient::close`),
  `src/admin/mod.rs` (`Admin::close` rustdoc), `src/ffi/admin.rs`
  (`kafka_admin_AdminClient_close` rustdoc → generated header)
- **Java Reference**: `KafkaAdminClient.java:698-707`

**Fixed** by translating the *timed* join, which the round-2 clamp had left as
the missing half of the contract:

```rust
let mut handle = self.shared.bg_handle.lock().unwrap().take();
if let Some(join_handle) = handle.as_mut() {
    let joined = tokio::time::timeout(Duration::from_millis(wait_time_ms as u64), join_handle)
        .await
        .is_ok();
    if !joined {
        *self.shared.bg_handle.lock().unwrap() = handle;
    }
}
```

The hard-shutdown deadline is only a hint to the I/O loop; the timed join is the
caller's guarantee, and it matters more here than in Java for exactly the reason
the Critic identified: Java's `sendEligibleCalls` calls the non-blocking NIO
`client.ready(...)`, while ours awaits `NetworkClient::ready` →
`initiate_connect` → `Selector::connect` → `socket.connect(address).await`, which
no shutdown deadline can interrupt. `client.close().await` after the loop is a
second such segment. Both `Admin::close`'s rustdoc and the exported header
promised the bound, so this was a broken documented contract that the FFI's
`block_on` handed to C and Python callers.

On expiry the task is left running (as Java leaves the I/O thread running after
an expired join) and the `JoinHandle` is put back, so a later `close()` can still
join it — which also narrows, without closing, the Critic's related note that a
second `close()` returns immediately where Java's second `close()` also joins.
Two *concurrent* `close()` calls still race for the handle and one sees `None`;
closing that would need a separate completion signal, which is beyond these three
findings.

Two points the Critic asked to be answered explicitly:

- **Java's `Thread.currentThread() != thread` self-deadlock guard has no Rust
  analogue, and none is added.** The I/O task owns no `Admin` handle (only the
  `AdminClientRunnable`), and every per-`Call` hook it runs — `create_request` /
  `handle_response` / `handle_failure` — is a sync closure (`admin-client.md` §2),
  so no callback can re-enter `close()` from it. Should that ever change, the
  timed join bounds the wait instead of deadlocking, where Java skips the join
  entirely — i.e. the failure mode is strictly better than Java's, not worse.
- **Deliberate divergence on `Duration::ZERO`.** Java's `Thread.join(0)` means
  "wait forever", so Java's `close(Duration.ZERO)` is formally unbounded (in
  practice near-immediate, because its loop reaches `threadShouldExit` at once).
  We treat 0 as 0: the rustdoc and the header both promise a return within
  `timeout`, and with the uninterruptible connect await above the Java reading
  would be a genuine hang rather than Java's prompt return. Documented at the
  call site.

Also translated `Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs)`
(`KafkaAdminClient.java:673`, "Limit the timeout to a year") as
`MAX_CLOSE_WAIT_TIME_MS`. The Critic had marked the missing clamp a non-defect
because it was unobservable; it stops being unobservable once the timeout drives
a `tokio::time::Sleep`, and it keeps the derived deadline finite for the FFI's
negative → `i64::MAX` ms mapping. Java's `waitTimeMs < 0`
→ `IllegalArgumentException` stays unrepresentable: `Duration` cannot be negative.

**Tests** (both in `src/admin/kafka_admin_client.rs`):

- `close_returns_within_its_timeout_even_when_the_io_task_cannot_exit` — arms a
  new `WaitingClient::stuck` flag so `poll` never returns (standing in for the
  uninterruptible connect await), spawns the real background task, puts an
  external `listTopics` call in flight so `should_exit` cannot short-circuit on
  "all work has been completed" either, then asserts `close(50ms)` returns in
  under a second. The assertion is wrapped in a 5 s `tokio::time::timeout` so a
  regression fails rather than hangs the suite.
- `close_clamps_the_wait_to_a_year` — `close(i64::MAX ms)` installs
  `now + MAX_CLOSE_WAIT_TIME_MS`.

Teeth verified by reverting the timed join to `let _ = join_handle.await`:
`close(50ms) must return even though the I/O task can never exit; it was still
blocked after 5s`.

## RESOLVED — `close()` extends an existing hard-shutdown deadline; Java only ever moves it earlier
- **File**: `src/admin/kafka_admin_client.rs` (`KafkaAdminClient::close`),
  `src/admin/internals/admin_client_runnable.rs` (`NO_HARD_SHUTDOWN` visibility)
- **Java Reference**: `KafkaAdminClient.java:680-694`

**Fixed** by translating Java's compare-and-set loop, whose whole purpose is
monotonicity, in place of the plain `store`:

```rust
let mut prev = NO_HARD_SHUTDOWN;
loop {
    match self.shared.shutdown.hard_shutdown_deadline_ms.compare_exchange(
        prev, new_hard_shutdown_time_ms, Ordering::AcqRel, Ordering::Acquire,
    ) {
        Ok(_) => break,
        Err(actual) => {
            if actual < new_hard_shutdown_time_ms {
                break;      // an earlier (more urgent) deadline is installed
            }
            prev = actual;
        },
    }
}
```

`NO_HARD_SHUTDOWN` (`i64::MIN`) is now `pub(crate)` and documented as playing
Java's `INVALID_SHUTDOWN_TIME` role: it is lower than every reachable deadline,
so the "is an earlier deadline already installed?" comparison orders the same way
as Java's `-1`. Java's `newHardShutdownTimeMs = prev` reassignment on the
already-earlier branch has no counterpart because it only feeds a debug log, and
`Shared` carries no `LogContext`.

Ordering note: Java calls `client.wakeup()` inside the successful CAS arm. Here
the wakeup follows the `closing` store, so a woken I/O task is guaranteed to
observe both, and it is issued on the already-earlier-deadline path too — a
harmless no-op there, since the `close()` that installed that deadline has
already woken the task.

**Test**: `close_never_widens_an_existing_hard_shutdown_deadline` —
`close(100ms)` installs `now + 100`; a following `close(60s)` leaves it at
`now + 100`; a following `close(10ms)` moves it to `now + 10`. Teeth verified by
restoring the plain store: `a later, more relaxed close() must keep the earlier
deadline — left: 61000, right: 1100`.

## RESOLVED — the header states an unreachable trigger for the tokio-worker callback case
- **File**: `src/ffi/admin.rs` (module doc + the 9 `_async` entry points),
  `src/ffi/common.rs` (adjacent pre-existing comment)

**Fixed** by taking the Critic's second option — stating the real trigger rather
than dropping the parenthetical, since the *consequence* (callbacks are not
serialised on one thread) is true and worth keeping. The reachability claim was
wrong in the direction that ties the case to teardown, which is precisely what a
C reader must not conclude: `enqueue_or_run_inline` takes the inline branch only
when the **receiver** is gone, and both admin async helpers clone the sender
*before* `spawn` and hold it inside the task for its whole life, so
`kafka_admin_AdminClient_destroy` dropping the handle's `completion_tx` cannot
disconnect the queue while an operation is outstanding. All 9 blocks now read:

> …if the dispatcher's completion queue can no longer be reached when the result
> arrives. Destroying the handle does not cause that — an outstanding operation
> holds its own sender, so it cannot disconnect the queue; what remains is a
> dispatcher thread that terminated abnormally, i.e. a panic inside an earlier
> callback.

Verified in the **generated** header, not the source:
`grep -c "terminated abnormally" target/include/confluent_kafka.h` → 9, and
`grep -c "kafka_admin_AdminClient_destroy is" …` → 0.

The adjacent pre-existing claim in `src/ffi/common.rs` ("user callbacks run on one
predictable thread and **never on a tokio worker**"), which the same analysis
contradicts, was reconciled in the same pass: it now states that the dispatcher
thread is the normal path rather than a guarantee, names the inline fallbacks as
the reason, and says callbacks can run on a tokio worker. Blast radius: that file
is shared with the producer and consumer FFIs, so their internal rustdoc changes
too — it is an internal comment (not `///`), so no generated header text moves.
