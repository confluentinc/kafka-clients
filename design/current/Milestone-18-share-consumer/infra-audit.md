# Milestone 18 — P0 infra audit: shared consumer classes the share consumer reuses

Scope: share-consumer work only (see `.claude/rules/share-consumer.md`). Java is 4.4.0 (`kafka/`
submodule). Rust is `milestone-18-share-consumer` @ `150c54b97`. Paths are under
`rust/src/consumer/internals/` unless stated. Citations are `file:function` / `Class#method`, never line
numbers (pilot rule R7).

Verdict vocabulary:

- **plugs in**: the share class/usage can be built against the current Rust shape, with only
  additive edits (a new field, enum variant or match arm) in the phase that owns the share class.
- **refactor first**: the Rust shape is consumer-specialised where Java is generic; a
  behaviour-neutral refactor, in its own commit, must land before the share subclass. The phase is
  named.

## Summary

| Java class | Verdict | Phase |
|---|---|---|
| `AbstractMembershipManager` | **refactor first** — group-agnostic Java logic lives in `ConsumerMembershipManager`; response type, metadata and metrics types are hard-wired | P4 |
| `AbstractHeartbeatRequestManager` | **refactor first** — Java's `poll` / `pollOnClose` / `maximumTimeToWait` / `onResponse` / `onFailure` live in `ConsumerHeartbeatRequestManager` | P4 |
| `HeartbeatRequestState` | plugs in | P4 |
| `ConsumerNetworkThread` | plugs in, additive (share membership + share consume drive, close path) | P6 |
| `RequestManagers` | plugs in, additive (three share slots; share consume manager is `Arc`-shared) | P6 |
| `ApplicationEventProcessor` | plugs in after a one-line type change (`Arc<ConsumerMetadata>` → `Arc<Metadata>`) | P6 (the type change can go with P4's metadata change) |
| `ApplicationEventHandler` | plugs in | P6/P7 |
| `CompletableEventReaper` | plugs in | — |
| `SubscriptionState` | plugs in; `subscribeToShareGroup` **already translated** | — |
| `ConsumerMetadata` | not reused: Java `ShareConsumerMetadata extends Metadata`, not `ConsumerMetadata` | P4 (new type over `Metadata` + `MetadataOverrides`) |
| `WakeupTrigger` | plugs in; no `ShareFetchAction` needed | — |
| `NetworkClientDelegate` | plugs in | P7 |
| `FetchConfig` | not reused: Java `ShareFetchConfig` is a standalone class | P3a |
| `CompletedFetch` / batch decoder | **refactor first** — the decoder pieces are private to `completed_fetch.rs` | P3a |
| `Deserializers` | plugs in | — |
| `ConsumerRecords` / `ConsumerRecord` | plugs in (`with_next_offsets`, `delivery_count` exist); retention for RENEW is D5, out of this audit | — |
| Metrics bases | `AbstractConsumerMetricsManager`, `HeartbeatMetricsManager`, `AsyncConsumerMetrics` plug in; `RebalanceMetricsManager` **refactor first** (folded into `ConsumerRebalanceMetricsManager`) | P4 |

## 1. Membership and heartbeat

### `AbstractMembershipManager` — refactor first (P4)

What Java's share subclass needs: `ShareMembershipManager extends
AbstractMembershipManager<ShareGroupHeartbeatResponse>` overrides only `errorCode`, `memberEpoch`,
`extractAssignment`, `signalPartitionsAssigned`, `joinGroupEpoch`, `leaveGroupEpoch`, `poll`
(`maybeReconcile(true)`), and adds `rackId`. Everything else is inherited from the base:
`AbstractMembershipManager#onHeartbeatSuccess`, `#transitionToFenced`, `#transitionToFatal`,
`#transitionToStale`, `#maybeRejoinStaleMember`, `#leaveGroup` / `#leaveGroupOnClose`,
`#maybeReconcile`, `#revokeAndAssign`, `#assignPartitions`, and the hooks
`#signalReconciliationStarted` / `#signalReconciliationCompleting` / `#signalMemberLeavingGroup` /
`#signalPartitionsLost` / `#signalPartitionsBeingRevoked` / `#signalPartitionsRevoked`, whose base
bodies are no-ops or `CompletableFuture.completedFuture(null)`. The share subclass uses those defaults.

Rust shape (`abstract_membership_manager.rs`):

- `AbstractMembershipManager::on_heartbeat_success` takes `&ConsumerGroupHeartbeatResponse` and calls
  `ConsumerMembershipManager::member_epoch_with_response` / `::extract_assignment` / `::error_code`
  directly (`return_if_unexpected_error`). The module doc says it is "hard-wired" because Share was out
  of scope.
- Fields `metadata: Arc<ConsumerMetadata>` (Java: `Metadata`) and `MembershipInner::metrics_manager:
  Option<Arc<ConsumerRebalanceMetricsManager>>` (Java: abstract `RebalanceMetricsManager`). The
  metadata is only used through `metadata_arc()` (`find_resolvable_assignment_and_trigger_metadata_update`).
- The Java base methods listed above are **not** in the abstract struct. They are in
  `consumer_membership_manager.rs`: `ConsumerMembershipManager::transition_to_fenced`,
  `::transition_to_stale`, `::reconcile` (+ `drive_pending_reconcile` / `continue_after_revoke` /
  `continue_after_assign`), `::leave_group`, `::leave_group_on_close`,
  `::transition_to_sending_leave_group`, `::drive_pending_release`. These are built around the §31
  ack-driven callback state machine (stored `oneshot::Receiver`s), which is consumer-specific in
  practice but is Java's *base* `whenComplete` chain.
- `enqueue_rebalance_callback*` take `ConsumerRebalanceListenerMethodName` (consumer-only; fine to
  leave consumer-side, Java has it in `ConsumerMembershipManager` too).

Needed refactor: make the abstract layer generic over the response type (or a small hooks trait
carrying `error_code` / `member_epoch` / `extract_assignment` / `signal_*` / `join_group_epoch` /
`leave_group_epoch`), take `Arc<Metadata>`, take a rebalance-metrics abstraction (see §5), and move the
base-class methods (fence / fatal / stale / leave / reconcile chain) from `ConsumerMembershipManager`
into it, with the consumer's listener-callback steps expressed through the `signal_*` hooks. Without
it, `ShareMembershipManager` would have to duplicate ~2k lines of state-machine code (DoD #6).

Size note: `consumer_membership_manager.rs` is 7,196 lines (main ≈ 2,330); the reconcile/leave/fence
code is the bulk of it, and it is the area of the #279 single-reconcile-driver work. This is the
riskiest refactor in the milestone and is not reflected in PLAN §4's P4 estimate (~5k, all share
code). Uncertain: how much of the ack-driven state machine can stay consumer-side behind a hook that
for share completes immediately; that is a P4 design question.

### `AbstractHeartbeatRequestManager` — refactor first (P4)

Java: `ShareHeartbeatRequestManager extends AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse>`
overrides `handleSpecificFailure`, `handleSpecificExceptionInResponse`, `resetHeartbeatState`,
`buildHeartbeatRequest`, `heartbeatRequestName`, `errorForResponse`, `errorMessageForResponse`,
`heartbeatIntervalForResponse`, `membershipManager`, `shouldSendLeaveHeartbeatNow`, and has its own
nested `HeartbeatState`. It inherits `AbstractHeartbeatRequestManager#poll`, `#pollOnClose`,
`#maximumTimeToWait`, `#resetPollTimer`, `#onResponse`, `#onErrorResponse`, `#onFailure`,
`#handleFatalFailure`, `#makeHeartbeatRequest`. Its constructor passes
`new HeartbeatMetricsManager(metrics, CONSUMER_SHARE_METRIC_GROUP_PREFIX)`.

Rust: `abstract_heartbeat_request_manager.rs:AbstractHeartbeatRequestManager` is a concrete struct
holding the poll timer, `HeartbeatRequestState`, coordinator handle and metrics, with
`classify_response_error`, `on_successful_response`, `on_failure`,
`maybe_propagate_coordinator_fatal_error_event`, `make_heartbeat_poll_result`. The module doc says
the response type is hard-wired to `ConsumerGroupHeartbeatResponse`. The Java base's `poll`,
`pollOnClose`, `maximumTimeToWait`, `onResponse`, `onErrorResponse` are implemented in
`consumer_heartbeat_request_manager.rs` (`impl RequestManager for ConsumerHeartbeatRequestManager`:
`poll`, `poll_on_close`, `maximum_time_to_wait`; inherent `on_response`, `on_failure`,
`drain_pending_completions`), and they call `self.membership_manager.abstract_mm...` /
`ConsumerMembershipManager` methods directly.

Needed refactor: lift the base `poll` / `poll_on_close` / `maximum_time_to_wait` / response routing
into the abstract layer, dispatching to the subclass hooks and to the (refactored) abstract membership
manager. Depends on the membership refactor above. `HeartbeatMetricsManager::with_prefix` already
exists, so the share prefix plugs in.

### `HeartbeatRequestState` — plugs in

`heartbeat_request_state.rs:HeartbeatRequestState` depends only on `RequestState`; no consumer types.
Java `ShareHeartbeatRequestManager`'s test constructor takes one directly, as the consumer's does.

## 2. Background task and events

### `ConsumerNetworkThread` — plugs in, additive (P6)

Java's `ConsumerNetworkThread#runOnce` has no share code; it polls `requestManagers.entries()`. For a
share consumer `RequestManagers` (share constructor) lists coordinator → share heartbeat → share
membership → share consume.

Rust `consumer_network_thread.rs:ConsumerNetworkThread<K: KafkaClient>` is generic only over the
client, but `run_once` drives several managers explicitly rather than via `entries()`: coordinator
(`poll_shared`), commit (`poll_with_coordinator`), `membership: Option<Arc<ConsumerMembershipManager>>`
(Phase 2.4 `drive_pending_release`, Phase 2.5 `reconcile(now, false)`, leave-callback drive), and
fetch (`poll(&delegate, now)`). All consumer-specific steps are guarded by `Option`s, so a share
thread with them `None` is valid.

Share needs (additive): a share-membership drive equivalent to `ShareMembershipManager#poll`
(`maybeReconcile(true)`) between share heartbeat and share consume, a share-consume poll, and
`poll_on_close` for share consume in the close path (Java's `ShareConsumeRequestManager#pollOnClose`
carries the on-close acknowledgements). The thread is constructed in
`async_kafka_consumer.rs:AsyncKafkaConsumer` setup (Java constructs it inside `ApplicationEventHandler`);
`ShareConsumerImpl` mirrors that wiring. §10 of `consumer-threading.md` (never race the poll) applies
unchanged.

PLAN §7.2's closing note ("`ConsumerNetworkThread` is generic only over `KafkaClient` … the share
managers plug in as Java does") holds, with the caveat that the plug-in is explicit drive code in
`run_once`, not a list entry.

### `RequestManagers` — plugs in, additive (P6)

Rust `request_managers.rs:RequestManagers` has consumer `Option` slots and an `entries()` that skips
the `Arc`-shared slots (coordinator, commit, membership) and fetch. Share needs `share_heartbeat`
(goes in `entries()`), `share_membership` (`Arc`, shared with the heartbeat manager, driven
explicitly like `consumer_membership`) and `share_consume`.

Design point for P5/P6: Java registers `ShareConsumeRequestManager` as a `MemberStateListener`
(`RequestManagers#supplier`: `shareMembershipManager.registerStateListener(shareConsumeRequestManager)`),
and `ShareConsumeRequestManager#onMemberEpochUpdated` mutates its state. Rust
`member_state_listener.rs:MemberStateListener` methods take `&self` and listeners are
`Arc<dyn MemberStateListener>`. So the share consume manager must be `Arc`-shared with interior
mutability and polled explicitly — the `CommitRequestManager` precedent (`commit_handle`) — not
held `&mut` in `entries()`. `RequestManagers::close` must also close it (Java: it is `Closeable`).

### `ApplicationEventProcessor` — plugs in after a type change

Java share arms: `ApplicationEventProcessor#process(SharePollEvent)` (`maybeReconcile(true)`,
`onConsumerPoll`, `resetPollTimer`), `(ShareFetchEvent)`, `(ShareAcknowledgeSyncEvent)`,
`(ShareAcknowledgeAsyncEvent)`, `(ShareSubscriptionChangeEvent)` (`subscribeToShareGroup`,
`requestUpdateForNewTopics`, `onSubscriptionUpdated`), `(ShareUnsubscribeEvent)` (`leaveGroup`),
`(ShareAcknowledgeOnCloseEvent)`, `(ShareAcknowledgementCommitCallbackRegistrationEvent)`.

Rust `events/application_event_processor.rs:ApplicationEventProcessor` dispatches on the
`ApplicationEvent` enum; arms are additive. Its `metadata` field is `Arc<ConsumerMetadata>` where Java
holds `Metadata`; every use (`request_update_for_new_topics`, `update_last_seen_epoch_if_newer`,
`current_leader`, `update_version`, `fetch`) reaches `Metadata` through `Deref`, so changing it to
`Arc<Metadata>` is behaviour-neutral and needed because the share consumer has no `ConsumerMetadata`.

### `ApplicationEventHandler` — plugs in

`events/application_event_handler.rs:ApplicationEventHandler` is an mpsc sender + `Notify` + optional
`AsyncConsumerMetrics`; no consumer types. Share events become `ApplicationEvent` variants.
`add_and_get` serves `addAndGet`.

### `CompletableEventReaper` — plugs in

`events/completable_event_reaper.rs:CompletableEventReaper` stores `Arc<dyn
CompletableEventErasedHandle>`; type-agnostic. Java `ShareConsumerImpl` creates one per side
(application and background), as `AsyncKafkaConsumer` does.

## 3. State, metadata, wakeup, network

### `SubscriptionState` — plugs in; already done

`subscription_state.rs:SubscriptionState::subscribe_to_share_group` exists and matches
`SubscriptionState#subscribeToShareGroup` (registers an empty listener, sets `AutoTopicsShare`,
`change_subscription`). `AutoTopicsShare` is handled in `has_auto_assigned_partitions`,
`fetchable_partitions`, `has_fetchable_partitions` and the `Display` text "Subscribe to Share
Group(...)", matching `#hasAutoAssignedPartitions`, `#fetchablePartitions`, `#hasFetchablePartitions`,
`#toString`. `assign_from_subscribed_awaiting_callback` exists for `signalPartitionsAssigned`.
PLAN §4 P6 lists `SubscriptionState::subscribe_to_share_group` as work; it is already on master.

### `ConsumerMetadata` — not the share base

Java `ShareConsumerMetadata extends Metadata` (not `ConsumerMetadata`) and overrides
`newMetadataRequestBuilder` (subscription `metadataTopics()`) and `retainTopic`
(`subscription.needsMetadata`). Rust `consumer_metadata.rs:ConsumerMetadata` composes
`Arc<Metadata>` + `MetadataOverrides` (`retain_topic_fn`, `request_builder_fn`, …), so
`ShareConsumerMetadata` can be a sibling with the same composition. What blocks it is consumers of
`Arc<ConsumerMetadata>` in shared code that Java types as `Metadata`: `AbstractMembershipManager`
(§1) and `ApplicationEventProcessor` (§2). `NetworkClientDelegate` already takes `Arc<Metadata>`.

### `WakeupTrigger` — plugs in; no share action

Java `WakeupTrigger#setShareFetchAction` / `ShareFetchAction` call `ShareFetchBuffer#wakeup`. Rust
`wakeup_trigger.rs:WakeupTrigger` replaced Java's `Wakeupable` state machine with a rotating
`CancellationToken` (`consumer-threading.md` §11); its module doc already maps `ShareFetchAction` to
"the await is `select!`ing on the token". The consumer's `fetch_buffer.rs:FetchBuffer::await_wakeup`
is woken that way with no `FetchAction`; the share fetch buffer follows the same pattern. PLAN §4 P6's
"`WakeupTrigger` share action" therefore has no Rust counterpart to add; a test should show
`wakeup()` interrupts a share `poll` waiting on the buffer.

### `NetworkClientDelegate` — plugs in

`network_client_delegate.rs:NetworkClientDelegate::new(config, client, Arc<Metadata>,
background_event_handler, notify_metadata_errors_via_error_queue)` matches
`NetworkClientDelegate#supplier`'s arguments as `ShareConsumerImpl` passes them (`true` for the error
queue). `AsyncConsumerMetrics` is set via `set_async_consumer_metrics`. The throttle-time sensor Java
passes (`shareFetchMetricsManager.throttleTimeSensor()`) goes on the client through
`network_client.rs:NetworkClient::set_throttle_time_sensor`. Java `ShareConsumeRequestManager` does
not call the delegate (`isUnavailable` / `maybeThrowAuthFailure` / `tryConnect` do not appear), so
unlike `FetchRequestManager` it needs no delegate lent to its `poll`.

### `FetchConfig` — not reused

`ShareFetchConfig` is a standalone Java class (its javadoc: "similar to `FetchConfig`"). No share class
references `FetchConfig`. `fetch_config.rs` is only a precedent for config extraction.

## 4. Receive path

### `CompletedFetch` / batch decoder — refactor first (P3a)

Java `ShareCompletedFetch` is its own class (not a `CompletedFetch` subclass). It iterates
`RecordBatch`es, uses `currentBatch.streamingIterator(decompressionBufferSupplier)` for records
(`ShareCompletedFetch#nextFetchedRecord`), `maybeEnsureValid` for CRCs, and
`ShareCompletedFetch#parseRecord` builds a `ConsumerRecord` with `Optional.of(deliveryCount)`.

Rust `completed_fetch.rs` implements that decoding in private types: `BatchCursor` (batch walk over
`MemoryRecords`), `RecordSource`, and `RecordStream` (streaming decompression through
`ChunkedBytesStream` + `DecompressingReader`). They are module-private to `CompletedFetch`. PLAN §3
item 4 says `ShareCompletedFetch` "reuses the consumer's batch decode"; that requires extracting these
into a crate-private shared module (behaviour-neutral, P3a, own commit) or duplicating them (DoD #6
says no). Coordinate with k-raina's chunked-decompression work (#242) if it is still open.

Buffer ownership, as of `150c54b97` (facts for the D5 discussion, no proposal):

1. **`PartitionData.records` is a refcounted `bytes::Bytes` slice of the whole response payload, not a
   per-partition allocation.** The generated `FetchResponseData` `PartitionData.records` is
   `Option<Bytes>` and is read with `Readable::read_bytes_owned`
   (`rust/src/common/protocol/bytes_reader.rs:BytesReader::read_bytes_owned`: `self.buf.slice(..)`, no
   copy). `rust/src/network_client.rs` (response handling in `poll`) builds one
   `BytesReader::new(Bytes::from(payload_bytes))` per response, so every partition's `records` shares
   that one response allocation. `CompletedFetch::ensure_cursor` moves the `Bytes` into
   `MemoryRecords` (O(1)). `ShareFetchResponse.json` `Records` uses the same `records` type, so the
   share response will be generated the same way. (The `completed_fetch.rs` module doc still says
   `Option<Vec<u8>>`; the code comment in `ensure_cursor` and the generated type say `Bytes`. The doc
   is stale.)
2. **Uncompressed batches:** records are borrowed in place from that slice (`DefaultRecordRef`).
3. **Compressed batches are decompressed one record at a time, but not into pooled record buffers.**
   `RecordStream` reads each record into its own allocation (`RecordStream::read_record`), handed out
   as a `Bytes`; the buffer is reused for the next record only when nothing else holds it and it is at
   most `MAX_RECORD_PREALLOCATION`. What is pooled is the codec read buffer: `ChunkedBytesStream` takes
   it from the fetch's `BufferSupplier` and returns it when the batch's stream is dropped. A whole
   batch is never decompressed into one buffer. The `RecordStream` doc records this as a deviation
   from `consumer-threading.md` §27 (one allocation per compressed record). PLAN §7.1 D5(c)'s wording
   "decompresses one record at a time into pooled buffers" is therefore imprecise: per-record buffers,
   pooled read buffer.
4. `ConsumerRecord` holds only the deserialized `K`/`V`, owned `RecordHeaders` and an `Arc<str>` topic;
   it does not hold the response `Bytes`. A renewed-record design that keeps raw bytes would be the
   first holder of the response buffer beyond `CompletedFetch`.

### `Deserializers` — plugs in

`deserializers.rs:Deserializers::new(key, value)` over `DynDeserializer`, as `AsyncKafkaConsumer`
uses it. Java `ShareConsumerImpl` uses `Deserializers(config, k, v, metrics)` and
`Deserializers(k, v, metrics)`, the same constructors the consumer uses.

### `ConsumerRecords` / `ConsumerRecord` — plugs in

`ShareConsumerImpl#poll` returns `new ConsumerRecords<>(fetch.records(), Map.of())`;
`consumer_records.rs:ConsumerRecords::with_next_offsets(IndexMap<TopicPartition, Vec<ConsumerRecord>>,
HashMap)` takes the same shape (Java `ShareFetch#records` returns a `HashMap`, so order is unspecified
in Java). `consumer_record.rs:ConsumerRecord` has `delivery_count: Option<i16>` and the builder sets
it. Record retention for RENEW (D5) is out of this audit.

## 5. Metrics bases

- `AbstractConsumerMetricsManager` (Java base of `ShareFetchMetricsManager` and
  `KafkaShareConsumerMetrics`): `metrics/abstract_consumer_metrics_manager.rs` is an embeddable struct
  over `MetricsLedger`; plugs in.
- `HeartbeatMetricsManager`: `metrics/heartbeat_metrics_manager.rs:HeartbeatMetricsManager::with_prefix`
  takes the group prefix; `ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX` / `CONSUMER_SHARE_METRIC_GROUP`
  exist in `consumer_utils.rs`. Plugs in.
- `AsyncConsumerMetrics`: `metrics/async_consumer_metrics.rs:AsyncConsumerMetrics::new(metrics,
  group_name)`; Java `ShareConsumerImpl` passes `CONSUMER_SHARE_METRIC_GROUP`. Plugs in.
- `RebalanceMetricsManager`: **refactor first (P4).**
  `metrics/consumer_rebalance_metrics_manager.rs` folds the Java abstract base into
  `ConsumerRebalanceMetricsManager` (module doc: "A `RebalanceMetricsManager` trait can be extracted
  with no API change if/when Share/Streams are translated"). `ShareRebalanceMetricsManager extends
  RebalanceMetricsManager` is a second implementation, and `MembershipInner::metrics_manager` must
  hold either. Extract the trait (or an enum) before `ShareRebalanceMetricsManager`.
- `ShareFetchMetricsAggregator`, `ShareFetchMetricsRegistry`, `ShareConsumerMetrics` extend nothing in
  Java; the consumer's `fetch_metrics_*` files are precedents only.

## 6. Effect on the plan

- P4 is larger than estimated: it carries the membership + heartbeat base-class refactors (moving
  Java-base logic out of `consumer_membership_manager.rs` / `consumer_heartbeat_request_manager.rs`),
  the `RebalanceMetricsManager` extraction, and `Arc<ConsumerMetadata>` → `Arc<Metadata>` in
  `AbstractMembershipManager` / `ApplicationEventProcessor`. These touch the most-churned consumer
  files (PLAN §5 risk 1/2); merge `origin/master` before starting P4, and keep each refactor a
  separate behaviour-neutral commit with the full consumer suite green.
- P3a gains the decoder extraction from `completed_fetch.rs`.
- P6 loses two items that already exist or have no Rust counterpart:
  `SubscriptionState::subscribe_to_share_group` (done) and the `WakeupTrigger` share action (covered
  by the token design).
