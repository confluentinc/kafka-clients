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

# M11 bindings B2 — Critic round 5

Verdict on the slice was **done**; both design decisions I flagged were verified
and endorsed (the dispatch-helper generalisation by derivation, and
name-not-ordinal for `ConfigSource`/`ConfigType` — endorsed more strongly than I
argued it, since `ConfigSource` has had constants inserted mid-list so an ordinal
contract would have silently renumbered). Three filed findings plus the grouped
coverage observation, all addressed below.

## Resolved: `DescribeReplicaLogDirsResult_count` documented a mock-only behaviour as Java's general contract

The doc said replicas of an unknown topic are "silently omitted, mirroring Java's
`describeReplicaLogDirs`". That is `MockAdminClient`'s behaviour only
(`MockAdminClient.java:1112`). Production Java seeds one future per requested
replica (`KafkaAdminClient.java:3066-3068`) and completes all of them
(`:3141-3145`), so an unknown topic yields a *present* entry holding a default
`ReplicaLogDirInfo` — null current/future log dir, `-1` offset lags. The Rust
production client is faithful to that, so the divergence really was mock-only,
and the wrong sentence was shipping in the public header where the reader is
overwhelmingly using the real client.

Rewrote the rustdoc at `src/ffi/admin.rs` to lead with the production contract,
state explicitly that absence is *not* the unknown-topic signal (a null current
log dir is), and attribute the omission to `MockAdminClient.describeReplicaLogDirs`
by name. Verified in the generated header (`target/include/confluent_kafka.h`),
not just the source. The same mis-attribution appeared in the Python test
docstring for `test_describe_replica_log_dirs_omits_unknown_topics`; corrected
there too, including its line cite (1110 → 1112, the `if`).

## Resolved: wrong Java line cited for the `describeConfigs` BROKER_LOGGER branch

`bindings/c/tests/test_mock_admin.c` cited `MockAdminClient.java:895`; the throw
is at 885 (895 is the closing brace of `toConfigObject`). Corrected, and appended
the "at kafka a18251bae0b8" suffix the neighbouring comments carry. Verified
against the submodule, which is checked out at exactly that revision.

## Resolved: `Py_BuildValue("(NN)", ...)` failure path double-decrefs — swept across B1 and B2

Confirmed the mechanism: on `PyTuple_New` failure CPython's `do_mktuple` calls
`do_ignore`, which re-runs `do_mkvalue` over the remaining format units and
releases each result; for `'N'` that consumes the caller's reference. So the
unconditional `Py_XDECREF` pair after a NULL `Py_BuildValue` is a second release.

Rather than patch five copies, added one `error_value_pair(err, value)` helper in
`bindings/python/_confluentkafka.c` that consumes both references on every path
and documents why the caller must not release after a NULL return. All five
`(NN)` sites now go through it — 2 from B1 (`createTopics`, `describeTopics`) and
3 from B2 (`describeConfigs`, `describeLogDirs`, `describeReplicaLogDirs`).

Swept the rest of the file for the same shape while I was there. One more site
had it and was not in the filed list: `py_DeleteRecordsResult_drain`'s
`Py_BuildValue("(NL)", err, ...)` (B1), whose guard is `(key && err)` rather than
`(err && value)`, so it needed restructuring rather than the helper — the second
slot is a plain long, and the `key == NULL` case must still release `err`. Fixed
in place with a comment pointing at the helper for the shared rule. The remaining
seven `'N'`-unit call sites are unconditional returns with no follow-up decref
and are correct as written; checked each.

## Resolved: coverage gap — every field the mock never populates

Agreed this was the largest un-evidenced surface, and the transposition
demonstration was the convincing part: swapping two boolean option flags anywhere
along Python → C extension → FFI → `*Options` passed all 66 C and all 63 Python
tests, because the mock ignores `options` entirely.

Closed by unit-testing the pure helpers directly rather than only end-to-end.

**Rust (`src/ffi/admin.rs`, 19 new tests — the file previously had none, against
`src/ffi/producer.rs`'s 57):**

- All six option builders, each called twice with *asymmetric* flag values so a
  transposition cannot satisfy both cases. This is the test that has teeth:
  covers `describe_cluster`, `describe_configs`, `describe_topics`,
  `create_topics`, `create_partitions`, `delete_topics`.
- `option_timeout`, including that `0` is a real timeout and only negatives mean
  unset.
- `config_source_name` / `config_type_name` over all 19 constants (the mock only
  ever produces `UNKNOWN`, so 17 were asserted nowhere).
- `ConfigEntryC::new` over a hand-built entry with a non-`UNKNOWN` source and
  type, a documentation string and two synonyms — asserting synonym precedence
  order is preserved, and a nullable synonym value stays null. Plus the
  all-nulls entry, the `is_default`-tracks-`DEFAULT_CONFIG` derivation, and
  `from_config`'s sort-by-name.
- `LogDirDescriptionInner::new` with an error and real volume bytes, with the
  `-1` `UNKNOWN_VOLUME_BYTES` sentinels, and the replica sort (which pins that
  partition ordering is numeric, not lexicographic: 2 before 10).
  `LogDirDescriptionMapInner::new` sort-by-path. `ReplicaLogDirInfoInner::new`
  with both dirs present and both null.

**Python (`test/unit/test_admin.py`, 7 new tests, 63 → 70):**
`_to_full_config_entry` over a full 9-tuple with two synonyms (pins the
three-tuple field order and arity that would otherwise ship silently) and over
the all-nulls shape; `_to_log_dir_description` with error + volume bytes and with
the `-1` sentinels; the per-broker and per-replica error arms of
`_to_describe_log_dirs` / `_to_describe_replica_log_dirs`, both unreachable from
the mock. Plus the async error-arm case from the second, smaller observation.

**Teeth verified, not assumed.** Transposed
`include_authorized_operations`/`include_fenced_brokers` in
`describe_cluster_options` and confirmed
`describe_cluster_options_maps_each_flag_to_its_own_field` fails; restored and
re-confirmed green.

One correction to the suggested async test: I first wrote it against
`describe_configs`, and it did not raise. That is correct behaviour, not a bug —
`describe_configs` is a multi-key RPC, so the mock fails each resource's future
individually (`mock_admin_client.rs`, the `timeout_next_requests` branch) and the
timeout lands as a per-resource `KafkaError` in the drained dict, matching Java's
per-key futures. The call-failure arm needs an RPC whose futures all fail
together, so the test uses `describe_cluster` — the exact async mirror of the
existing `test_describe_cluster_call_failure_raises`.

## Resolved (folded in): the `describe_log_dirs` NPE divergence needs a comment

Agreed, and agreed the code should not change. Added one comment above the loop
in `src/admin/mock_admin_client.rs` covering both guards — the
`partition_log_dirs.first()` stand-in for Java's `partitionLogDirs.get(0)`
(`IndexOutOfBoundsException`) and the `unwrapped.get_mut(&node.id())` stand-in
for Java's unchecked `unwrappedResults.get(node.id())` (NPE) — citing
`MockAdminClient.java:1082-1083` and noting that reproducing either would mean
panicking in a public API (CLAUDE.md §10.1). This is a Tier-1 Phase-4 artifact
that B2 only made reachable from C; folded in since it stayed a single comment.

## Not actioned

- The `&'static CStr` suggestion for `ConfigEntryC`'s `source_c` /
  `config_type_c` and `ConfigSynonymC`'s `source_c` — explicitly raised for B3+
  rather than as a change here, and DoD #10 is N/A for Admin.
- Listing the nine/ten enum constants in the `_source` / `_type` rustdoc — noted
  as "not filing it". The new exhaustive Rust tests now pin every name, so the
  value set is at least enumerated somewhere authoritative.
- Stating DoD #10/#11 N/A in commit messages — stated in this entry and in the
  commit for this round.

---

# Critic 1 round 6 — verification pass (`c7b5394d..0f7589b7`)

Three of four round-5 resolutions verified clean. One missed sibling, resolved
below. The two adjudications carried no action.

## Resolved: the C test carries the same mock-as-Java mis-attribution

`bindings/c/tests/test_mock_admin.c:2189-2192` repeated both halves of the
round-5 defect that the fixup corrected in `src/ffi/admin.rs` and
`bindings/python/test/unit/test_admin.py`: it attributed a mock-only behaviour
to "Java's `describeReplicaLogDirs`", and cited `MockAdminClient.java:1110`
(the `for`) for a quote that lives at `:1112` (the `if (topicMetadata != null)`
guard). Correct: same file, same commit's blast radius, missed because the
round-5 grep was for the 885 citation rather than for the claim.

The comment now names `MockAdminClient.describeReplicaLogDirs` explicitly,
cites 1112, and adds the production contrast — `KafkaAdminClient` seeds one
future per requested replica (`KafkaAdminClient.java:3066-3068`) and completes
all of them (`:3141-3145`), so a real broker returns an unknown topic as
*present* with a null current replica log dir. Wording is now the same account
as the Python docstring at `test_admin.py:806-813`. Swept the tree for
`MockAdminClient.java:1110` and for `Java's describeReplicaLogDirs`: no other
occurrence in `src/` or `bindings/`.

## Noted, no action (adjudications)

- **The async error-arm test move.** Conclusion stands; the operative reason is
  result *shape*, not per-key-vs-together failure. Both mocks fail every future
  they own on the timeout branch (`MockAdminClient.java:822-831` and
  `:346-351`). `_to_describe_configs` has a per-key slot an error can occupy;
  `_to_cluster_description` collapses four futures into one object with nowhere
  to put one, so it must raise — and Java agrees, `describeCluster().nodes()
  .get()` throws. Already documented at `bindings/python/admin.py:25-57`;
  nothing added. The memory note has been corrected so the wrong reason is not
  carried into B3+.
- **The option-flag coverage claim is one hop of three.** The 19 Rust tests pin
  the FFI-helper → `*Options` hop only. `admin.py`'s `_*_spec` and
  `_confluentkafka.c`'s `PyArg_ParseTuple` forwarding stay unpinned because the
  mock ignores `options`, so no end-to-end test can reach them. Not claiming
  broader coverage in B3.

---

# Slice B3 — elections, reassignments, offsets (self-review)

No open reviewer comments at the start of this slice; `COMMENTS.1.md` was
unchanged past round 6, and `COMMENTS.FP.md` / `COMMENTS.FN.md` still do not
exist on either tree.

## Scope escalation into `src/admin/` (flagged, one commit of its own)

`MockAdminClient::find_partition_reassignment` panicked on Java's
`RuntimeException` branch, and its rustdoc claimed that branch was reachable
only through internal corruption. It is not: `delete_topics` removes the topic
from `all_topics` without pruning `reassignments` (as Java's does), so
create → alter-reassignment → delete-topic → list-reassignments reaches it.
Exposing `listPartitionReassignments` to C would have made a process abort
reachable from a legal C call sequence. Both branches now return
`KafkaError::illegal_state` with Java's exact message and the single result
future is failed instead — the accommodation `list_offsets` already makes for a
`TimestampSpec`. Two mock tests added; the regression one asserts the exact
message, which is what proves the branch is taken rather than merely present.

## Design decisions worth reviewing

1. **No admin-specific `TopicPartition` type, and no reuse of
   `kafka_consumer_TopicPartition_t` either.** The brief asked for the latter.
   Partition keys instead cross as parallel `topics[]` / `partitions[]` arrays
   on input and `_get_topic(i)` / `_get_partition(i)` on output, matching
   `deleteRecords` / `DeleteRecordsResult` and `read_topic_partitions` in
   `src/ffi/consumer.rs`. The reason for the deviation: `kafka_consumer_
   TopicPartition_t` is **output-only** today — it has `_topic`, `_partition`
   and `_destroy` but no constructor — so using it as an input would have meant
   widening the consumer FFI for admin's sake, and using it for result keys
   would have meant one allocation per key plus a second key style inside the
   same module. The directive's substance (do not define a competing admin
   type) is met; if the manager wants the handle literally, say so and it is a
   contained change.

2. **Each of Java's three `Optional`s gets an explicit boolean discriminant**
   (`all_partitions`, `cancel[i]`, `is_timestamp[i]`) rather than an overloaded
   NULL or sentinel. The third is the one that is not merely stylistic:
   `KafkaAdminClient.getOffsetFromSpec` is not injective, so
   `forTimestamp(-2)` and `earliest()` are the same `long`, and
   `MockAdminClient` treats them differently. Both the C and the Python suites
   assert that difference directly.

3. **Result accessors follow Java's future shape, not a fixed template.**
   `electLeaders` and `alterPartitionReassignments` get `_get_error(i)` and no
   `_get_value` (Java: `Optional<Throwable>` and `KafkaFuture<Void>`);
   `listPartitionReassignments` gets `_get_value(i)` and no `_get_error` (one
   future for the whole listing, so any failure is a call failure — the
   `listTopics` shape); only `listOffsets` has both. Stated because D2 is
   phrased as "per-key value *and* error", which only the last of the four can
   honour literally.

4. **`OffsetSpec` codes are not invented.** The six no-argument factories are
   selected by the `ListOffsets` wire sentinels (-1..-6) that Java's own
   `getOffsetFromSpec` emits, so there is no new code space for a reader to
   check against Java. `ElectionType` and `IsolationLevel` both have an `id()`
   in Java and cross as that, per the B2 rule.

## Coverage

16 Rust FFI tests, 21 C mock tests (66 -> 87), 2 C production tests (16 -> 18),
17 Python tests (70 -> 87). Teeth verified by transposing
`adding_replicas`/`removing_replicas` and the two `list_offsets_options`
arguments: each failed exactly the intended test and nothing else, then was
reverted. Not claiming more than that: as in B2, the `admin.py` `_*_spec` and
`_confluentkafka.c` `PyArg_ParseTuple` hops stay unpinned for `*Options` fields
the mock ignores.

DoD #10 (hot-path allocation audit) is N/A for Admin per `admin-client.md` §10;
#11 does not apply.

---

# Critic 1 round 7 (slice B3) — the four LOW carryovers

Round 7's verdict was "no defects"; these are its four LOW observations, all
resolved. Nothing here changes shipped behaviour except LOW 3, which removes a
Python default argument.

## LOW 1 — the second `find_partition_reassignment` branch was untested

`list_partition_reassignments_after_topic_shrink_fails_the_future`
(`src/admin/mock_admin_client.rs`) walks the Critic's sequence: create `rt2`
with **2** partitions, reassign `rt2-1`, delete the topic, recreate it with
**1** partition, list. The stale reassignment now names a partition index the
recreated topic no longer has, so the second guard
(`MockAdminClient.java:1190-1192`) fires and the test asserts its exact
message, `"... found reassignment for rt2-1, but no TopicPartitionInfo"`.

Kept as its own test rather than an extension of `admin_with_reassignment`,
because the helper creates a single-partition topic and the branch needs a
reassignment on an index that survives the delete but not the recreate.

The rustdoc already records why this branch is live in Rust and dead in Java
(`ArrayList.get` throws where `Vec::get` returns `None`); the test comment
repeats it, since a reader arriving at the test will ask.

## LOW 2 — no C-level regression test for the abort

`test_mock_admin_list_partition_reassignments_after_delete_returns_error`
(`bindings/c/tests/test_mock_admin.c`) drives create -> alter -> delete -> list
through the C API and asserts a non-NULL `err`, a NULL result handle, and the
exact message. Its comment states the property under test explicitly: the
evidence is that the *binary survives* the sequence, since the pre-fix panic
would have aborted the process at the `extern "C"` frame rather than failing an
assertion. That is the artifact the escalation commit message was missing.

88 C mock tests now (87 -> 88).

## LOW 3 — `elect_leaders`' invented cluster-wide default

Fixed by matching Java rather than documenting the deviation. `partitions` is
now a required positional parameter on both `Admin.elect_leaders` and
`AsyncAdmin.elect_leaders`; callers pass `None` explicitly for a cluster-wide
election, exactly as a Java caller must. Java has no no-argument overload —
`Admin.java:1092` and the three-argument form both take the `Set` — so the
default was ours, and an omitted argument combined with `ElectionType.UNCLEAN`
meant a cluster-wide unclean election.

The `None` -> "all partitions" *mapping* is unchanged and still correct; only
the default is gone. The neighbouring `list_partition_reassignments` keeps its
`partitions=None` default, which **is** Java (`Admin.java:1245-1247` has the
no-argument overload) — the two methods are no longer symmetric, and that
asymmetry is now the faithful one.

Two existing call sites updated to pass `None`, plus a new
`test_elect_leaders_requires_partitions_explicitly` asserting the `TypeError`,
so the requirement cannot silently regress to a default.

## LOW 4 — the second private `read_topic_partitions`

Twelve lines of rustdoc on the admin copy (`src/ffi/admin.rs`) stating that the
divergence from the consumer's same-named helper is deliberate, what the
difference is (empty for a NULL array, skip a NULL topic entry), and why it is
load-bearing: four admin entry points document "an entry with a NULL topic is
skipped", cbindgen ships that sentence into the public header, and delegating
to the consumer helper would make all four false. Also says what a future
unification must preserve, rather than only forbidding it.

## Adjudications accepted without code change

- **Priority 2.1** — the allocation argument is withdrawn, as the Critic asks;
  `admin-client.md` §10 rules the hot-path audit N/A for Admin, so it cannot be
  load-bearing. The decisive reason is the Critic's: `TopicPartition` is
  `org.apache.kafka.common`, so under CLAUDE.md §3 the correct spelling is
  `kafka_common_TopicPartition_t`, and reusing the mis-namespaced consumer type
  would propagate the error into a second public C API. Recorded in memory so
  B4-B6 do not re-litigate it.
- **Priority 2.2 / 2.3** — no change requested; the amended D2 (committed as
  `da4cfd44`) now makes B3's accessor shape the rule.

## Verification

`cargo build` (both feature settings), `cargo test` (3045 lib tests, +1),
`cargo xtask format-check`, `cargo clippy --all-targets --features ffi -D
warnings`, `test_mock_admin` (88 tests), and the Python admin suite in Docker
(88 tests, +1). DoD #10 N/A per `admin-client.md` §10; #11 does not apply.

---

# Milestone 11 bindings B4 — self-review (groups and group offsets)

Nine RPCs: `listGroups`, `listConsumerGroups`, `describeConsumerGroups`,
`describeClassicGroups`, `listConsumerGroupOffsets`,
`alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets`,
`deleteConsumerGroups`, `removeMembersFromConsumerGroup`. Each has a bare sync
and an `_async` C entry point (D1), one flattened result handle, a
`*_callback_t`, a Python sync method and an `AsyncAdmin` coroutine.

## Design decisions worth stating

1. **Three result shapes, not one.** D2 as amended says accessors mirror the
   Java future's shape, and B4 is where that stops being a formality:

   | Java shape | RPCs | C accessors |
   |---|---|---|
   | `Map<K, KafkaFuture<V>>` | describeConsumerGroups, describeClassicGroups, listConsumerGroupOffsets | `_get_value(i)` + `_get_error(i)` |
   | `Map<K, KafkaFuture<Void>>` / one future over `Map<K, Errors>` | deleteConsumerGroups, alterConsumerGroupOffsets, deleteConsumerGroupOffsets, removeMembersFromConsumerGroup | `_get_error(i)` only |
   | one future split by `valid()` / `errors()` | listGroups, listConsumerGroups | two independent sequences, no key |

   The third is new. `ListGroupsResult` has **no** per-key future at all
   (`ListGroupsResult.java:82,95`): one source future yields a mixed
   collection, and `valid()` / `errors()` split it into a listing list and an
   **unkeyed** `Collection<Throwable>` of generally different length. So those
   two handles expose `_valid_count`/`_get_valid(i)` and
   `_error_count`/`_get_error(i)`, deliberately *not* `_count`/`_get_error(i)`
   — the usual naming would invite indexing the errors by the listing count,
   which reads past the end on any partial success. The rustdoc on
   `_get_error` says so in as many words, and the Python side returns a
   `(valid, errors)` pair rather than a dict for the same reason.

2. **No `kafka_admin_OffsetAndMetadata_t`.** `OffsetAndMetadata` is
   `org.apache.kafka.clients.consumer`, so an `kafka_admin_`-prefixed handle
   would be mis-namespaced under CLAUDE.md §3 — the same error the round-7
   review identified in `kafka_consumer_TopicPartition_t`, which I am not going
   to reproduce in the other direction. The three fields are flattened into
   indexed accessors on `kafka_admin_OffsetAndMetadataMap_t`, which is what B2
   already did for `LogDirDescription.ReplicaInfo`. On the Python side the
   correct move is the opposite one: `admin.py` imports `OffsetAndMetadata`
   from `consumer.py`, exactly as it already imports `Node`.

3. **`has_offset(i)` is a discriminant, not a convenience.** Java's
   `Map<TopicPartition, OffsetAndMetadata>` value is nullable —
   `listConsumerGroupOffsets` reports a requested partition the group has never
   committed for as *present with a null value*. Without the flag that would be
   indistinguishable from a committed offset of 0.

4. **`remove_all` is a flag, not an empty array.** Java's
   `RemoveMembersFromConsumerGroupOptions(Collection)` *rejects* an empty
   collection while the no-argument constructor means "remove every member", so
   the two must not share a spelling. In `removeAll` mode Java refuses
   `memberResult` outright and `all()` is the only observable, so the C result
   carries no keys and the outcome is the call's error. Same reasoning as B3's
   `cancel[i]`; the Python method takes `members` positionally-required,
   applying the round-7 LOW 3 lesson before it could recur.

5. **Empty key set on a single-source-future RPC.** `alterConsumerGroupOffsets`,
   `deleteConsumerGroupOffsets` and `removeMembersFromConsumerGroup` back every
   per-key future with one source future. When the request selects no keys there
   is no per-key slot for a failure, so the source future is awaited directly and
   its error becomes the call's error — which is what Java's `all()` reports
   there too. `empty_outcomes` is the one place this happens and it is
   documented.

6. **Enum names, not invented codes.** `GroupState`, `GroupType`,
   `ConsumerGroupState` and `ClassicGroupState` have no `id()` in Java, so per
   the B2 rule they cross as the enum's `toString()` name in both directions.
   Those names are `"Consumer"` / `"Classic"` — capitalised, and *not* the
   lower-case `"consumer"` protocol-type string that sits next to them in
   `GroupListing`. The first draft of the rustdoc and the Rust tests both got
   this wrong and the tests caught it; both fields are now asserted together in
   the C, Rust and Python suites precisely because they are adjacent and
   confusable. Parsing goes through `GroupState::parse` / `GroupType::parse`, so
   an unrecognised name becomes `UNKNOWN` as in Java rather than erroring.

7. **`listConsumerGroups` is deprecated in Java 4.1** and is mirrored anyway
   because it is still on the `Admin` trait; the deprecation is stated in the
   C entry point's rustdoc (and therefore in the shipped header) and in the
   Python docstring. Java's deprecated `inStates(Set<ConsumerGroupState>)` is
   *defined* as `inGroupStates` over `GroupState.parse` of the same names, so
   the C surface exposes one `group_states` array and says why.

## One defect found and fixed in my own work

`consumer_group_description_to_py` shipped with ten `Py_BuildValue` format
units for eleven arguments, which would have raised on unpack against a real
broker. **No test could catch it**: Java's own `MockAdminClient.describeConsumerGroups`
throws, so the Rust mock fails every per-group future and the success branch is
unreachable from the mock — the suite covers the error branch thoroughly and
never touches the other one. Found by auditing every new `Py_BuildValue` site's
unit count against its argument count, not by a failing test. Fixed in a fixup
commit; the other six sites were audited the same way and are correct. Recorded
as a memory note, since the same blind spot exists for every mock-unsupported
RPC in B5 and B6.

## Observation for adjudication (not fixed, not B4's code)

`remove_members_from_consumer_group_result.rs`'s `describe_identity` renders a
`MemberIdentity` as `MemberIdentity(memberId=x, groupInstanceId=y)`, whereas
Java concatenates the generated `MemberIdentity.toString()`, which quotes
non-null strings and includes the `reason` field. The two error messages that
embed it — `removeAll`'s "Encounter exception when trying to remove: ..." and
`sub_level_error`'s "Member \"...\" was not included in the removal response" —
therefore match Java in their wrapper text but not in the embedded rendering.
This is pre-existing (Tier 2 Phase 3), its rustdoc claims only "renders a
`MemberIdentity` for error messages" rather than Java parity, and closing it
properly means teaching the message generator to emit a Java-compatible
`Display` — a change far wider than this slice. Flagging rather than fixing or
silently leaving it.

## Coverage

  - 29 Rust FFI unit tests: every option builder called twice with asymmetric
    values, both ragged-array readers, `read_required_string`, and every
    flattener over a hand-built fixture exercising the nullable and absent arms.
    This carries most of the weight, because seven of the nine RPCs are
    `UnsupportedOperation` in Java's mock and cannot be driven end to end.
  - 29 C mock tests (88 -> 117) and 2 production-client tests (18 -> 20),
    including every `_async` inline-firing path and a NULL `out_result` sweep
    over all nine sync entry points.
  - 25 Python tests (88 -> 113; 191 pass across the suite), including direct
    `_to_*` converter tests for the fully populated descriptions the mock
    cannot produce.
  - Teeth: three Rust call-site argument swaps (protocol/protocolData sources,
    clientId/host sources, the protocol-types vs types arrays in
    `list_groups_options`) and two Python ones (clientId/host in
    `_to_member_description`, offset vs leader-epoch in
    `_alter_consumer_group_offsets_spec`). Each failed exactly its own test and
    nothing else; all reverted.
  - Gates: `cargo build` both feature settings, `cargo test`,
    `cargo xtask format-check`, `cargo clippy --all-targets --features ffi -D
    warnings`, all six `ctest` binaries, and the Python suite in Docker. The
    generated header was deleted and rebuilt: all 25 new cbindgen entries
    resolve, all 30 admin `_async` declarations carry the six-clause
    callback-thread contract (counted at block level — a line grep undercounts
    because the sentence wraps), and all 29 sync entry points with an
    `out_result` route through `finish_sync`.

Not claiming more than that: as in B2 and B3, the `*Options` fields the mock
ignores stay unpinned below the Rust layer. `list_groups`' three filter arrays
are covered by the Rust FFI unit test but not end to end, because
`MockAdminClient.listGroups` discards its options argument entirely.

DoD #10 (hot-path allocation audit) is N/A for Admin per `admin-client.md` §10;
#11 does not apply. Scope was task 1 plus B4; B5 was not started.

---

# Actor 1 — round 8 resolutions (Critic 1 round 8) + slice B5a

Critic round 8 reported no defects in shipped code. It raised one plan gap
(LOW 1, already amended by the Manager in `4e32342f`), one adjudication
(LOW 2), and one tooling proposal (Priority 3). Both actionable items are
resolved below, followed by the B5a self-review.

## LOW 2 — `describe_identity` vs Java's generated `toString()` — RESOLVED

Fixed locally, as the Critic recommended, in the fixup commit on top of
`8a77a694`.

`describe_identity` printed two of `MemberIdentity`'s three declared fields and
did not quote strings, so where Java renders

    MemberIdentity(memberId='', groupInstanceId='inst-1', reason=null)

Rust rendered

    MemberIdentity(memberId=, groupInstanceId=inst-1)

Verified against the submodule rather than the review text:
`MessageDataGenerator.generateFieldToString` emits, for a string field,
`"<name>=" + ((<name> == null) ? "null" : "'" + <name> + "'")`, over all of
`struct.fields()`; `LeaveGroupRequest.json` declares `MemberId`,
`GroupInstanceId` and `Reason` on `MemberIdentity`. Unknown tagged fields are
not printed — the generated loop iterates only declared fields.

One thing the fix exposes that the review did not name:
`MemberToRemove.toMemberIdentity()` sets the member id to `UNKNOWN_MEMBER_ID`,
the **empty string**, which Java quotes as `''` rather than printing as `null`.
The empty-versus-null distinction is asserted directly.

Both user-visible messages are now pinned with exact-text assertions instead of
`contains`, plus a dedicated test of the three renderings (all-default, all-set,
and empty-but-present nullable strings).

Not attempted, per the Critic's own recommendation: the systematic generator
fix. `generator/src/lib.rs:1994` emits `Display` as `{:?}` for every message
struct, so a faithful change there alters the public `Display` of all of them at
once and must handle each field kind the way `MessageDataGenerator` does. That
is its own slice.

## Priority 3 — the arity sweep is now a gate — RESOLVED

`cargo xtask check-bindings`, wired into `make verify` and `make
verify-sandbox`. Reimplemented in Rust rather than ported, per CLAUDE.md #6.

It strips comments, tracks string and character literals, splits arguments on
depth-zero commas only, concatenates adjacent string literals, and counts
format units under the correct grammar per function — the two-argument units
(`s#`, `O&`, `es`), the three-argument `es#`, the parse-only `|`/`$`, the
`:`/`;` terminators (which are dict separators, not terminators, in a build
format) and the structural `()[]{}`.

Two decisions worth flagging:

  - **A non-literal format is a failure, not a skip.** A site the gate cannot
    verify is a hole in the gate. This is not hypothetical — it fired on B5a's
    own `PyArg_ParseTuple(item, nullable ? "izizzii" : "isissii", ...)`, which
    is now two literal-format calls.
  - **The target runs `cargo test -p xtask` first.** `cargo test` at the
    workspace root only tests the root package, so nothing else exercises the
    scanner's 21 unit tests. For the same reason `cargo xtask lint`/`lint-fix`
    now include `-p xtask`; the build tooling had been escaping the lint gate
    entirely.

**This is a repo-wide gap, not an xtask one — flagging it for the Manager
rather than fixing it unilaterally.** `cargo test` and `cargo clippy` at the
workspace root cover only the root package. Every other workspace member
(`generator`, `xtask`, `consumer-perf`, `multilanguage-test-server`) is in the
same position. Measured: **`cargo test -p generator` runs 57 unit tests that
`cargo test` never runs** — the wire-protocol code generator's own suite is in
no standard gate, and `make test-rust` (`cargo test`) misses them too. Lint is
narrower but similar: `cargo xtask lint` reaches `generator` only because it
names that manifest explicitly, and reached nothing else until this slice added
`-p xtask`. I limited my change to `xtask`, which I had just added 500 lines
to; whether `test-rust` should become `--workspace` is a Manager call.

Teeth, the way the Critic did it: run against `761da3b2^` it reports exactly
that revision's one defect —
`prefix.c:4140 fmt='(sONsssNNNN)' 10 units / 11 args` — and nothing else, and
exits 1. Against HEAD it inspects 42 `Py_BuildValue` and 144 `PyArg_Parse*`
sites and finds nothing, independently reproducing the Critic's counts exactly.

It does **not** close the ordering gap, and nothing in this slice claims it
does.

## Slice B5a — ACLs and client quotas (5 RPCs)

`createAcls`, `describeAcls`, `deleteAcls`, `describeClientQuotas`,
`alterClientQuotas`. Both a bare sync and an `_async` variant each (D1);
result handles per the amended D2. `PLAN-bindings.md` §4 records the B5 split.

### Three result shapes, all already in D2's table

  - `Map<K, KafkaFuture<Void>>` → per-key error, no value: `createAcls` (keyed
    by `AclBinding`), `alterClientQuotas` (keyed by `ClientQuotaEntity`).
  - One future for the whole call → a listing and **no** `_get_error`; a
    failure is the call's error: `describeAcls`, `describeClientQuotas`.
  - `Map<K, KafkaFuture<V>>` where `V` is itself a collection → `deleteAcls`,
    which needs two index levels: `_get_error(i)` for the filter's own future,
    and `_get_binding(i, j)` / `_get_result_error(i, j)` over the
    `FilterResults`. Java's `FilterResult` holds exactly one of a binding or an
    exception, so for an in-range entry precisely one of the two is non-null.

D2 needs no further amendment for B5a.

### Namespacing: three new `kafka_common_*` types

`AclBinding`, `AclBindingFilter` (`org.apache.kafka.common.acl`) and
`ClientQuotaEntity` (`.quota`) are `kafka_common_*`, not `kafka_admin_*`. This
is round 7's rule applied in the direction that *creates* types rather than
forbidding reuse; the existing precedent is `kafka_common_Node_t` /
`kafka_common_KafkaError_t`. Naming them `kafka_admin_*` would have repeated
the `kafka_consumer_TopicPartition_t` error in a second public surface.

All three are **output-only and borrowed**; requests cross as parallel arrays,
so no handle in the module is both caller-owned and borrowed.
`AclBinding.pattern()` / `.entry()` are flattened onto the binding, following
B2's `LogDirDescription.ReplicaInfo`.

### Null-versus-absent, decided per kind rather than uniformly

B3's rule was "give every Java `Optional` an explicit discriminant". Applied
literally that would add redundant discriminants here, so the sharper rule used
is:

  - Nullable **string** → a null pointer, no discriminant. It cannot collide
    with a pointer to `""`, and an empty resource name / principal / host /
    quota-entity name is a legal, distinct value. Asserted both ways, in Rust,
    C and Python.
  - Nullable **number** → an explicit `bool`. `op_has_values[i][j] == false` is
    Java's `Op(key, null)`, i.e. *remove*; every `double` including 0 is a legal
    quota value, so no sentinel could carry it. The same reasoning makes
    `DescribeClientQuotasResult_get_quota_value` an out-param rather than a
    sentinel return.
  - A tri-state that is **not** string-nullability → an explicit `int32`. A
    quota filter component's match is EXACT / DEFAULT / ANY, and DEFAULT and
    ANY both carry no name, so a null name alone could not separate them — and
    they differ in equality *and* in the wire match-type byte. The discriminant
    reuses Kafka's own constants (`MATCH_TYPE_EXACT` 0, `DEFAULT` 1,
    `SPECIFIED` 2), not invented codes.

The four ACL enums cross as Java `code()` values per the B2 rule, pinned as
literals by a test.

### Verification

  - **Gates:** `cargo build` both feature settings, `cargo test` (3047),
    `cargo xtask format-check`, `cargo clippy --all-targets --features ffi -D
    warnings`, `cargo xtask lint`, `cargo xtask check-bindings`, all six
    `ctest` binaries, and the Python suite in Docker.
  - **Header:** regenerated with `--features ffi` (a plain release build
    silently produces none). All 13 new cbindgen entries resolve; each of the
    five RPCs has exactly one sync and one `_async` declaration; and all 35
    `kafka_admin_*_async` declarations — the five new ones included — carry all
    six clauses of the callback-thread contract at block level, checked after
    stripping the ` * ` line prefixes.
  - **Tests:** 21 Rust FFI unit tests (59 → 80 in `ffi::admin`), 21 C tests
    (117 → 138 in `mock_admin`), and 30 Python tests (113 → 143; 25 functions,
    one of them a five-case `parametrize` over the constructor rejections).
  - **Teeth, Rust/C:** five call-site mutations that compile cleanly —
    transposing principal/host in `AclBindingInner`; swapping the resource-type
    and pattern-type arrays; swapping the entity-type and entity-name arrays;
    inverting the `op_has_values` discriminant; and substituting `op_counts`
    for `entity_counts`. Each failed exactly its own tests. A sixth, deleting
    the discriminant read outright, was rejected by `unused_variable` instead —
    which is why the check has to be an argument swap, not a field deletion.
  - **Teeth, Python:** two mutations. Transposing principal/host in
    `_acl_binding_rows` failed exactly 3 tests. The second —
    `float(o.value or 0.0)`, collapsing a `None` op value into `0.0`, i.e.
    turning *remove this quota* into *set it to zero* — **passed the entire
    suite**, and that is reported below as a coverage hole I then closed rather
    than as a badly chosen mutation.

### The Python teeth run found a real coverage hole

Java's mock throws before echoing any op back, so the **outbound** half of
`alterClientQuotas` had no end-to-end observable at all: nothing in the suite
could tell "remove this quota" from "set it to 0.0". Earlier slices applied
round 5's "unit-test the pure flatteners" rule only to the *response*
direction; this is the same rule owed to the *request* direction.

Fix: the row builders are now pure static methods with their own tests —
`_acl_binding_rows`, `_acl_filter_rows` and the newly extracted
`_quota_alteration_rows`. Re-running the same mutation now fails exactly one
test, `test_quota_alteration_rows_keep_both_nulls`, with
`('consumer_byte_rate', 0.0) != ('consumer_byte_rate', None)`.

Generalisation for B5b/B6: **for every mock-unsupported RPC, ask what part of
the request the mock discards** — that part needs a direct test of the row
builder, because no end-to-end test can reach it.

  - **A fixture correction the teeth found:** the C and Rust
    `alterClientQuotas` fixtures both used entity counts `(2, 1)` and op counts
    `(2, 1)`. Identical, so reading one count array where the other belonged
    would have passed both suites. They are now `(2, 1)` and `(1, 2)`, and that
    fifth mutation fails both. The lesson is about fixture **shape**, not only
    fixture values: whenever two arrays of the same C type sit side by side in
    a signature, their per-row lengths must differ.

### Panic audit

Clean, but traced rather than assumed. `ResourcePatternFilter::matches` **can**
`panic!` on an unsupported pattern type, but nothing outside its own unit tests
calls it, so it is unreachable from these entry points.
`AccessControlEntry::principal()` / `host()` `.expect()` a present value; every
construction site in the crate goes through `AccessControlEntry::new`, which
always stores both, and the wire decoders propagate the constructor's `Result`
rather than unwrapping it.

### What this slice does not claim

Java's own `MockAdminClient` throws for all five RPCs
(`MockAdminClient.java:806`, `:811`, `:816`, `:1243`, `:1248`), so the success
path of every drain here is unreachable end to end. The Rust FFI unit tests
against hand-built fixtures, and the new arity gate, are the coverage those
paths have; field *order* in the Python tuples rests on review against the
matching `_to_*` unpacker, which the gate explicitly cannot check.

No core `src/admin/` bug was found, so no scope escalation. DoD #10 (hot-path
allocation audit) is N/A for Admin per `admin-client.md` §10 — admin calls are
batch/administrative with no per-record path; #11 does not apply. Scope was
tasks 1 and 2 only; B5b was not started.

---

## Round 9 — MED 1 and MED 2: the request-direction sweep, extended

**MED 1** (`_describe_client_quotas_spec` was B5a's fourth outbound builder and
was not extracted) and **MED 2** (the same generalisation was owed in six
earlier places) are resolved together, because they are one rule applied to
seven sites.

Seven inline request comprehensions are now pure static row builders on
`_AdminBase`, each with its own direct test in
`bindings/python/test/unit/test_admin.py`:

| builder | discriminant it carries | Java mock throws at |
|---|---|---|
| `_quota_filter_rows` | the EXACT / DEFAULT / SPECIFIED match type | `MockAdminClient.java:1243` |
| `_alter_consumer_group_offsets_rows` | leader-epoch present-flag, `metadata` null-vs-`""` | `:1213` |
| `_remove_members_rows` | remove-all vs. an empty member list | `:801-803` |
| `_elect_leaders_rows` | all-partitions vs. an empty selection | `:797` |
| `_create_partitions_rows` | `increaseTo(int)` vs. `increaseTo(int, assignments)` | `:626-628` |
| `_delete_records_rows` | column order only | `:631-638` |
| `_delete_consumer_group_offsets_rows` | column order only | `:783` |

`_alter_partition_reassignments_spec` and `_list_consumer_group_offsets_spec`
were deliberately left inline, for the reason the review gives: their mocks are
implemented, so their flags are observable end to end.

### Teeth — seven call-site mutations, each run against the whole suite

Each mutation is a silent one that compiles and runs (an argument transposition
or an inverted discriminant), not a deletion.

| mutation | tests that failed |
|---|---|
| `_quota_filter_rows`: swap the `entity_type` and `match_name` columns | 2 — its own, plus `test_describe_client_quotas_raises_and_accepts_every_match_type` |
| `_create_partitions_rows`: swap the `for topic, np` loop variables | 4 |
| `_delete_records_rows`: swap the partition and before-offset columns | 3 |
| `_elect_leaders_rows`: `partitions is None` -> `is not None` | 3 |
| `_alter_consumer_group_offsets_rows`: `leader_epoch is not None` -> `is None` | **1 — only its own test** |
| `_delete_consumer_group_offsets_rows`: swap the `for t, p` unpack | 3 |
| `_remove_members_rows`: `members is None` -> `is not None` | 5 |

The `_alter_consumer_group_offsets_rows` row is the one that justifies the
sweep: inverting the leader-epoch present-flag — "epoch 0" versus "no epoch",
two different broker requests — was caught by **nothing at all** before this
commit. All seven mutations were reverted; `git status --porcelain` clean.

Where a mutation also failed pre-existing tests, that is not redundancy: those
tests catch it only because a marshaling error changes the *error message* the
mock's throw is compared against, which is incidental. None of them pins a
column.

## Round 9 — LOW 1: one narrowing rule for every `int32_t` enum code

`src/ffi/admin.rs` had three answers to `i32 -> i8`: `i8::try_from` with a
reject (quota match types), a bare `as i8` (four ACL enum codes, two
`ConfigResourceType` sites and `OpType::for_id`). Bare `as i8` *truncates*:
259 becomes 3, a valid code in most of these enums, so an out-of-range value
was read as a different legitimate member.

One rule now, stated on `narrow_enum_code`: **never `as i8`**. What a miss
means is decided by the enum, and there are exactly two cases —

  - the enum has an `UNKNOWN` member (all four ACL enums and
    `ConfigResourceType`, code `0` in every one): fall through to it via
    `enum_code_or_unknown`, which extends `from_code`'s own
    `getOrDefault(code, UNKNOWN)` to the wider C input type;
  - it has none (`MATCH_TYPE_*` are bare wire constants; `OpType::for_id`
    returns an `Option`): return an `IllegalArgument` naming the value.

Nine call sites converted; `grep "as i8" src/ffi/admin.rs` now matches only the
rule's own doc comment.

## Round 9 — LOW 2: the `alterClientQuotas` duplicate-entity rationale was wrong about Java

Confirmed against the submodule: `KafkaAdminClient.java:4301-4313` puts each
alteration's future into a `Map` keyed by entity (so the *earlier future* is
dropped) but passes the `Collection` verbatim to
`new AlterClientQuotasRequest.Builder(entries, ...)`, so **both alterations are
sent**. "Two alterations of the same entity cannot both be represented" was
therefore a false statement of the Java contract.

Reconsidered whether to stop rejecting, and kept the rejection, now labelled
**Deviation from Java, deliberate** with the real reason: the C result is a
flat, index-addressed array built from that collapsed map, so a duplicate
yields fewer rows than the request had and the caller — who passed parallel
arrays, not a map — cannot learn which of its two rows the surviving outcome
describes. A Java caller holds the map and can see it shrink. The cost is
stated too: a C caller cannot express "send two alterations for one entity".
Corrected in four places (the FFI rustdoc, the Rust unit test, the C test and
the `admin.py` docstring); the `listConsumerGroupOffsets` duplicate-key comments
were left alone, because Java really does take a `Map` there.

## Round 9 — LOW 3: `check-bindings` now scans `PyObject_CallFunction`

Added as a fourth `Kind` (format index 1, 2 fixed arguments, `Py_BuildValue`
grammar). All 8 sites in `_confluentkafka.c` are clean, matching the review's
hand-check; the summary line now reads `46 Py_BuildValue, 8
PyObject_CallFunction and 160 PyArg_Parse* call site(s) inspected`. Three new
scanner tests: a matching and a short call, that the build grammar (not the
parse grammar) is used, and that `PyObject_CallFunctionObjArgs` — a strict
prefix match with no format argument at all — is not scanned. 21 -> 24 scanner
tests.

The module doc's "what it does not check" list was expanded from one line to
five, naming argument order, argument *types*, `N`-versus-`O` reference
stealing, `PyArg_ParseTupleAndKeywords` kwlist length and `PyErr_Format`, so
the gate cannot be cited as broader than it is.

## Round 9 — LOW 4: D2 gains a fifth rule, for a collection-valued `V`

`PLAN-bindings.md` §7 D2 now states it, and B5b applies it.

> Flatten to a second index when the collection's **element** is scalar-only;
> mint a value handle as soon as that element **itself contains a collection**,
> because the handle gives the inner collection an index space starting at 0
> whereas flattening would need a third index. **Two index levels is the
> limit.**

I did not adopt the review's proposed discriminator ("mint when the value is a
named Java type users hold, flatten when it is an anonymous list wrapper"):
`DeleteAclsResult.FilterResults` and `LogDirDescription` are both named, public,
user-visible Java classes, so that test does not separate the two precedents.
Index depth does, and it is the property that actually decides whether the C
surface stays usable. Both shipped shapes fall out of the rule unchanged —
`describeLogDirs` would have needed `_get_replica_topic(i, j, k)`, `deleteAcls`
stops at `(i, j)`.

Two clarifications are recorded with it so B6 does not over-apply the rule: a
`V` that is a single record of scalars keeps flattening onto index `i`
(`fenceProducers`), and a record with scalars *and* one collection also
flattens, scalars at `i` and the collection at `(i, j)`
(`describeTransactions`).

---

# Slice B5b — SCRAM, delegation tokens and features (self-review)

Eight RPCs, each with a sync entry point and an `_async` one (D1):
`describeUserScramCredentials`, `alterUserScramCredentials`,
`createDelegationToken`, `renewDelegationToken`, `expireDelegationToken`,
`describeDelegationToken`, `describeFeatures`, `updateFeatures`.

### Result shapes, decided by D2's new fifth rule

The rule landed this round (round 9, LOW 4) and B5b is the first slice decided
by it rather than by taste:

  - **Three handles minted**, all `kafka_common_*` because `KafkaPrincipal` is
    `org.apache.kafka.common.security.auth` and `DelegationToken` /
    `TokenInformation` are `...security.token.delegation` — none is under
    `clients.admin`. `DelegationToken` → `TokenInformation` →
    `List<KafkaPrincipal>` is three index levels once flattened, and
    `DelegationToken` is the value of *two* results, so both halves of the rule
    point the same way. Output-only and borrowed, as B5a's are.
  - **`ScramCredentialInfo`** is two scalars → flattened to `(i, j)` on
    `DescribeUserScramCredentialsResult`.
  - **`FeatureMetadata`, `FinalizedVersionRange`, `SupportedVersionRange`** are
    a single record keyed directly by the result, holding two maps of
    two-scalar ranges → everything lives on `DescribeFeaturesResult_t`, nothing
    minted. The two maps are **independently indexed**, which the accessor
    rustdoc says explicitly (the `ListGroupsResult.valid()/errors()` shape).
  - **`FeatureUpdate`** and the SCRAM alterations are *inputs* and cross as
    parallel arrays, never as handles.

Per-RPC accessor sets follow each Java result's future shape:
`alterUserScramCredentials` / `updateFeatures` are `Map<K, KafkaFuture<Void>>`
→ key + error, no value; the four token RPCs and `describeFeatures` hold one
future for the whole call → value accessors and no `_get_error`, because a
failure is the call's error. The two `KafkaFuture<Long>` results still get a
handle rather than passing the timestamp through the callback: D2 is one
result handle per RPC, and a uniform callback signature across all 46 is worth
more than saving two allocations.

### Composing Java's three SCRAM-describe views into one C result

`DescribeUserScramCredentialsResult` has `all()`, `users()` and
`description(user)` over one response future, with different failure semantics.
C has one handle, so `submit_describe_user_scram_credentials` composes them
using only public API: `all()`'s map when it succeeds (its keys are then the
complete user set and no row carries an error); otherwise `users()` plus
`description(u)` per user, which yields exactly the per-user errors Java
reports. Nothing Java can reach is lost — the users omitted at that point are
the ones Java's `all()` also declines to report. The empty-composition case
returns the `all()` error rather than an empty success, which is the trap B4 hit
with `removeMembersFromConsumerGroup`.

### Discriminants

Three explicit flags, each because the payload cannot carry the absent case
(B5a's narrowed rule):

  - `is_deletions[i]` on `alterUserScramCredentials` — an upsertion and a
    deletion both carry a user and a mechanism, so "password is NULL" would
    conflate a deletion with a malformed upsertion.
  - `has_owners_filter` on `describeDelegationToken` — Java's `owners()` is a
    nullable list where null describes *every* token; a count of zero cannot
    say that.
  - `has_node_id` on `describeFeatures` — node id 0 is a legal broker.

And one non-flag: the **salt** is nullable *bytes*, so a NULL pointer already
means "absent" and selects Java's salt-generating constructor. The nullable
*number* case appears on the way out — `finalized_features_epoch` uses the
`bool fn(..., int64_t *out)` shape, because every `int64_t` is a legal epoch.

### Making two dead drains live: the mock feature-level setter

Java's `MockAdminClient` throws for both SCRAM RPCs
(`MockAdminClient.java:1251-1259`) but *implements* the four token RPCs and
both feature ones. `describeFeatures` / `updateFeatures` nevertheless returned
nothing worth asserting until the three feature-level maps were seeded, which
Java does on its `Builder` (`:188-200`) and the Rust mock exposes as
`set_feature_levels`. Exporting
`kafka_admin_MockAdminClient_set_feature_levels` (beside the existing
`update_beginning_offsets` setters, and rejecting a production handle the same
way) turned both drains into real end-to-end coverage, including
`updateFeatures`' apply-versus-`validate_only` difference and its
`Can't upgrade above 21` per-feature error.

### What is *not* observable, and what covers it instead

The two SCRAM RPCs. Applying the round-9 request-direction lens **while
writing** rather than afterwards, both directions got pinned directly:

  - request: `_scram_alteration_rows` and `_principal_rows` are pure static
    methods with their own tests, and `read_scram_alterations` /
    `read_kafka_principals` have Rust unit tests over hand-built fixtures;
  - response: `_to_describe_user_scram_credentials` has a converter test, and
    `box_describe_user_scram_credentials_result` a Rust one.

`cargo xtask check-bindings` covers the `Py_BuildValue` arity of both drains
(53 build sites now, up from 46).

### Java methods deliberately not on the C surface

`TokenInformation.ownerAsString()` / `renewersAsString()` are
`principal.toString()` over accessors C already has, and Python's
`KafkaPrincipal.__str__` provides the same string. `ownerOrRenewer(principal)`
is a predicate the caller can evaluate from `owner()` and the renewer list.
`ScramMechanism.mechanismName()` / `fromMechanismName()` are name↔code
conversions; the C boundary carries the numeric `type()` per the B2 rule, and
Python exposes `ScramMechanism.mechanism_name`.
`DescribeUserScramCredentialsResult.users()` is not a separate accessor because
the per-user rows subsume it (see above).

### Verification

  - **Gates:** `cargo build` both feature settings, `cargo test --workspace`
    (3047 + 57 generator + 24 xtask + 152 others) **and**
    `cargo test --features ffi` (3204 — the root `cargo test` does not build
    the `ffi` feature, so `ffi::admin`'s tests are invisible to
    `make test-rust` even now that it is `--workspace`),
    `cargo xtask format-check`, `cargo clippy --all-targets --features ffi -D
    warnings`, `cargo clippy -p xtask`, `cargo clippy --workspace`,
    `cargo xtask check-bindings`, all six `ctest` binaries, and the Python
    suite in Docker.
  - **Header:** regenerated from scratch with `--features ffi`. All 19 new
    cbindgen entries resolve (3 `kafka_common_*` value types, 8 `*Result_t`, 8
    `*_callback_t`); each of the eight RPCs has exactly one sync and one
    `_async` declaration; and all **43** `kafka_admin_*_async` declarations —
    the eight new ones included — carry all six clauses of the callback-thread
    contract, checked at block level after stripping the ` * ` prefixes.
  - **Tests:** 20 new Rust FFI unit tests (80 → 100 in `ffi::admin`), 17 new C
    tests (138 → 153 in `mock_admin`, plus 2 in `kafka_admin`), and 16 new
    Python tests (228 → 244).

### A real defect the C suite caught, in the test itself

`test_mock_admin_delegation_token_lifecycle` first kept the `const char
*token_id` returned by `kafka_common_TokenInformation_token_id` across the
owning result handle's `_destroy` and compared it after. That is exactly the
borrowed-pointer contract these handles document, and it passed on the first
run and failed on the second — `Expected '\x1C\xA2.\x9Bo`w\xD8...' Was
'7Wziz54NQVW_QcsIjTY0zg'`. Fixed by copying the id, with a comment saying why.
Worth recording because it is the failure mode a C consumer will hit: the
getters return borrows, and an intermittent test is the only warning.

### Teeth — nine call-site mutations, all reverted

Rust (`cargo test --features ffi ffi::admin`, 100 tests):

| mutation | result |
|---|---|
| swap `owner` and `token_requester` in `TokenInformationInner::new` | 1 failed |
| invert the `is_deletions` read in `read_scram_alterations` | 3 failed |
| swap the finalized min/max columns in `box_describe_features_result` | 1 failed |
| swap `min_levels`/`max_levels` at the `read_feature_levels` call site | **0 failed in Rust.** The mock setter has no Rust-side unit test, so this one is caught end to end instead: rebuilt and re-run, it fails 3 of the 153 C tests (`describe_features_reports_seeded_levels`, `update_features_applies_and_validates`, `b5b_token_and_feature_async`). Reverted, rebuilt, 153/153 green again |

Python (`pytest test/unit/test_admin.py`):

| mutation | result |
|---|---|
| `_scram_alteration_rows`: invert the is-deletion column | 4 failed |
| `_describe_delegation_token_spec`: invert the owners-filter flag | 1 failed |
| `_feature_update_rows`: swap max-version-level and upgrade-type | 4 failed |
| `_to_feature_metadata`: swap the finalized and supported lists | 4 failed |
| `_to_delegation_token`: swap owner and requester | 1 failed -- only its converter test, because the mock's owner and requester are the same principal end to end |

### DoD

No core `src/admin/` bug was found, so no scope escalation. **DoD #10 (hot-path
allocation audit) is N/A** per `admin-client.md` §10 — admin calls are
batch/administrative with no per-record path. **DoD #11 does not apply** to the
Admin trait, but its spirit holds: every per-RPC entry point is a plain
`extern "C" fn`, no `#[async_trait]` reaches the `Call`/driver types. No TODO
or FIXME. Scope was tasks 1–4; **B6 was not started.**
