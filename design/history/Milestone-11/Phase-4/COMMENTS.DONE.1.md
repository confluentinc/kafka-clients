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
