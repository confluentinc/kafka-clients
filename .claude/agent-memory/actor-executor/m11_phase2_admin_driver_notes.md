---
name: m11-phase2-admin-driver-notes
description: M11 Tier 1 Phase 2 (Admin createPartitions/deleteRecords) — AdminApiDriver engine pulled forward; Rust core+unit+integration COMPLETE
metadata:
  type: project
---

Milestone 11 (AdminClient) Tier 1 Phase 2 "Partitions & records", Actor N=1. COMPLETE + green
(build + `cargo test --lib` 2169 pass + format-check + lint + 5 real-broker integration tests).
Scope: Rust core + tests ONLY (NO C FFI, NO Python — same banner as Phase 1). Builds on Phase 1
foundation (see [[m11-phase1-admin-notes]]).

**Dispatch paths (verified from Java 4.2 source):**
- `createPartitions` → plain controller `Call` with quota-retry (mirrors createTopics exactly).
  `get_create_partitions_call` free fn; futures are `KafkaFutureImpl<()>`.
- `deleteRecords` → `invokeDriver(DeleteRecordsHandler, PartitionLeaderFuture)` = the
  AdminApiDriver/PartitionLeaderStrategy engine. **Pulled the engine FORWARD from Phase 5** (it's
  now available for listOffsets Phase 5, which reuses PartitionLeaderStrategy with
  K=TopicPartition, V=ListOffsetsResultInfo).

**AdminApiDriver engine (src/admin/internals/): all generic over `<K,V>`.**
- `api_request_scope.rs`: `ApiRequestScope` enum = `SingleLookup | Fulfillment(i32)` (Java's
  interface collapsed; CoordinatorStrategy Tier-2 will need a per-key lookup variant).
  `destination_broker_id()` → None for lookup, Some(id) for fulfillment.
- `admin_api_lookup_strategy.rs` / `admin_api_handler.rs` / `admin_api_future.rs`: plain (NON-async)
  traits `: Send`. Handler folds Java's Batched/Unbatched split into build_request directly (only
  DeleteRecordsHandler in scope; it's Batched). SimpleAdminApiFuture NOT translated (unused).
- `admin_api_driver.rs`: driver + RequestSpec + RequestState + BiMultimap. **KEY simplification:**
  RequestState stores `has_inflight: bool` NOT the RequestSpec (Java only reads isPresent()).
  RequestSpec carries the pre-built `Box<dyn RequestBuilder>` by value (moved into the Call).
  `on_failure` detects Java's DisconnectException via `error.error()==Errors::NetworkException`
  (that's what the admin runnable emits for was_disconnected). UV via Errors::UnsupportedVersion.
- `partition_leader_strategy.rs`: PartitionLeaderStrategy + PartitionLeaderFuture<V:Clone+Send+Sync>.
  Topic-error fatal cases build `KafkaError::topic_authorization({topic})`/`invalid_topics({topic})`
  (carry the topic set — PartitionLeaderStrategyTest asserts unauthorizedTopics()/invalidTopics()).
  handleTopicError fallthrough: UNKNOWN_TOPIC_OR_PARTITION with tolerate=true → retriable(skip).
- `delete_records_handler.rs`: handlePartitionError order is is_invalid_metadata()→unmapped FIRST,
  then is_retriable()→retriable(left out of result), else→failed(KafkaError::new(error)).
- `partition_leader_cache.rs`: Arc<PartitionLeaderCache> with std::sync::Mutex, held in `Shared`.

**Driver↔Call bridge (in kafka_admin_client.rs, generic free fns — Java's newCall/maybeSendRequests):**
- `DriverContext{tx, wakeup, time_provider}` cloned into every Call closure (all Send).
- Driver wrapped `Arc<Mutex<AdminApiDriver<K,V>>>` (needed for Send across the mpsc channel).
- new_driver_call: create_request hands over the prebuilt builder ONCE (`Option::take`), rebuilds
  via `driver.build_request_for_spec` only on the rare non-disconnect retriable re-send.
  handle_response/handle_failure lock driver→on_response/on_failure→maybe_send_requests (submit new
  Calls via tx). Node for handler passed as `Node::new(broker_id,"",-1)` (only broker.id() used, in
  the sanity-check msg). maybe_retry override: NetworkException→driver.on_failure+submit→Handled;
  else Requeue.
- Calls submitted to the SAME admin_tx channel; drained next run_once — timing matches Java's newCalls.

**Call framework extensions (call.rs): NodeProvider::ConstantNodeId(i32)** (ConstantNodeIdProvider)
+ **`maybe_retry_fn: Option<MaybeRetryFn>` + `MaybeRetryOutcome{Requeue,Handled}`**. Runnable's
fail_call retriable branch now `match call.maybe_retry(err,now)`. Default (None)=Requeue (unchanged
for topic RPCs).

**KEY TEST-HARNESS insight (deleteRecords unit tests):** the driver's lookup retries have NO backoff
(clearInflightRequest sets next=now for lookup scope), so lookup-retry tests pass WITHOUT advancing
the mock clock — pump_until(N) suffices. The seeded mock_cluster must contain the leader node id the
metadata RESPONSE points at (ConstantNodeId needs node_by_id). The runnable's internal metadata call
does NOT fire (metadata_manager seeded ready → metadata_fetch_delay_ms=MAX), so prepared Metadata
responses are consumed ONLY by driver lookups (FIFO ordering holds).

**MockClient limitation (documented deviation):** `authentication_error` always returns None → can't
simulate a pending SaslAuthenticationException. testDeleteRecordsMultipleSends adapted to a per-broker
fatal partition error (TOPIC_AUTHORIZATION_FAILED) to exercise the same multi-broker fan-out.

**Integration nonexistent-partition:** leader lookup never resolves → future fails on API-timeout
elapse (used short default.api.timeout.ms=8000 to keep it fast). Assert is_retriable()||RequestTimedOut.

**Wire wrappers:** CreatePartitions v3 / DeleteRecords v2 are FLEXIBLE (2+). Byte-vector tests hand-
derived from the flexible wire format matched the serializer. DeleteRecordsResponse::INVALID_LOW_WATERMARK=-1.

**Tests translated:** DeleteRecordsHandlerTest (10) + PartitionLeaderStrategyTest (10) fully;
KafkaAdminClientTest createPartitions (4: base+quota enabled/disabled/until-timeout) + deleteRecords
(4: full round-trip, topic-auth, multi-sends, mock-empty). AdminApiDriverTest (917 lines) NOT
translated — task scoped it out ("relevant DeleteRecordsHandlerTest/PartitionLeaderStrategyTest cases").
