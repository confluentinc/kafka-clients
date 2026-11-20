# Milestone 18 method coverage (PLAN §8 process check)

One table per Java class a phase translates or changes. Each row is a Java
method (public, protected, package-private and private, constructors
included) and the Rust function that translates it, found by name or by
`#[doc(alias = "...#method")]`, or a one-line reason it has none. An unmapped
row without a reason is a Critic finding.

Java reference: Kafka 4.4.0. Paths are under `rust/src/consumer/internals/`.

## P4b

### `AbstractMembershipManager` (`abstract_membership_manager.rs`)

Hooks are methods of the `MembershipManagerHooks` trait (Java's `abstract`
and overridable `protected` methods); the rest are on
`AbstractMembershipManager` or its locked state `MembershipInner`.

| Java method | Rust |
|---|---|
| `AbstractMembershipManager(String, SubscriptionState, Metadata, Logger, Time, RebalanceMetricsManager, boolean)` | `AbstractMembershipManager::new` (the `Logger` is the `log` crate) |
| `transitionTo(MemberState)` | `MembershipInner::transition_to` |
| `isCompletingRebalance(MemberState, MemberState)` (private static) | `MembershipInner::is_completing_rebalance` |
| `isStartingRebalance(MemberState, MemberState)` (private static) | `MembershipInner::is_starting_rebalance` |
| `groupId()` | `AbstractMembershipManager::group_id` |
| `memberId()` | `AbstractMembershipManager::member_id` |
| `memberEpoch()` | `AbstractMembershipManager::member_epoch` |
| `leaveGroupOperation()` | `AbstractMembershipManager::leave_group_operation` (written by `leave_group_on_close` through `set_leave_group_operation`) |
| `onHeartbeatSuccess(R)` | `AbstractMembershipManager::on_heartbeat_success` |
| `throwIfUnexpectedError(R)` (private) | `AbstractMembershipManager::return_if_unexpected_error` |
| `errorCode(R)` (abstract) | `MembershipManagerHooks::error_code` |
| `memberEpoch(R)` (abstract) | `MembershipManagerHooks::member_epoch_with_response` (CLAUDE.md §2: beside the `memberEpoch()` getter) |
| `extractAssignment(R)` (abstract) | `MembershipManagerHooks::extract_assignment` |
| `onHeartbeatFailure(boolean)` | `AbstractMembershipManager::on_heartbeat_failure` |
| `maybeCompleteLeaveInProgress()` | `AbstractMembershipManager::maybe_complete_leave_in_progress` (with `MembershipInner::take_leave_in_progress` and `complete_leave` for callers that hold the lock) |
| `isNotInGroup()` | `MembershipInner::is_not_in_group` |
| `processAssignmentReceived(Map)` | `AbstractMembershipManager::process_assignment_received` |
| `replaceTargetAssignmentWithNewAssignment(Map)` (private) | inlined in `process_assignment_received`, its only caller |
| `transitionToFenced()` | `AbstractMembershipManager::transition_to_fenced` (release tail: `continue_after_fenced_release`) |
| `transitionToFatal()` | `AbstractMembershipManager::transition_to_fatal` (release tail: `continue_after_fatal_release`) |
| `onSubscriptionUpdated()` | `AbstractMembershipManager::on_subscription_updated` |
| `onConsumerPoll()` | `AbstractMembershipManager::on_consumer_poll` |
| `clearAssignment()` (private) | `AbstractMembershipManager::clear_assignment` |
| `transitionToJoining()` | `AbstractMembershipManager::transition_to_joining` |
| `leaveGroupOnClose(GroupMembershipOperation)` | `AbstractMembershipManager::leave_group_on_close` |
| `leaveGroup()` | `AbstractMembershipManager::leave_group` |
| `leaveGroup(boolean runCallbacks)` | `AbstractMembershipManager::leave_group_with_run_callbacks` (its `whenComplete` tail: `drive_pending_leave_callback`) |
| `clearAssignmentAndLeaveGroup()` (private) | `AbstractMembershipManager::clear_assignment_and_leave_group` |
| `transitionToSendingLeaveGroup(boolean)` | `AbstractMembershipManager::transition_to_sending_leave_group` |
| `notifyEpochChange(Optional<Integer>)` | `MembershipInner::notify_epoch_change` |
| `notifyAssignmentChange(Set)` | `MembershipInner::notify_assignment_change` |
| `shouldHeartbeatNow()` | `MembershipInner::should_heartbeat_now` |
| `onHeartbeatRequestGenerated()` | `AbstractMembershipManager::on_heartbeat_request_generated` |
| `onHeartbeatRequestSkipped()` | `AbstractMembershipManager::on_heartbeat_request_skipped` |
| `targetAssignmentReconciled()` (private) | `MembershipInner::target_assignment_reconciled` |
| `shouldSkipHeartbeat()` | `MembershipInner::should_skip_heartbeat` |
| `isLeavingGroup()` | `MembershipManagerHooks::is_leaving_group` (default body; also `MembershipInner::is_leaving_group_base`) |
| `maybeRejoinStaleMember()` | `AbstractMembershipManager::maybe_rejoin_stale_member` |
| `transitionToStale()` | `AbstractMembershipManager::transition_to_stale` (release tail: `continue_after_stale_release`) |
| `maybeReconcile(boolean)` | `AbstractMembershipManager::maybe_reconcile` (the commit `whenComplete`: `continue_after_commit`, driven by `drive_pending_reconcile`) |
| `getDeadlineMsForTimeout(long)` (package-private) | inlined in its only caller, `ConsumerMembershipManager::signal_reconciliation_started` (`saturating_add`, Java's overflow clamp) |
| `revokeAndAssign(...)` (private) | split across `continue_after_commit` (revoke), `continue_after_revoke` (assign), `continue_after_assign` (completion), the resume points of its `CompletableFuture` chain |
| `maybeAbortReconciliation()` | `AbstractMembershipManager::maybe_abort_reconciliation` |
| `updateAssignment(Map)` (visible for testing) | none: Java tests only; the Rust tests set `MembershipInner::current_assignment` directly |
| `signalReconciliationStarted()` | `MembershipManagerHooks::signal_reconciliation_started` |
| `signalReconciliationCompleting()` | `MembershipManagerHooks::signal_reconciliation_completing` |
| `signalMemberLeavingGroup()` | `MembershipManagerHooks::signal_member_leaving_group` |
| `signalPartitionsLost(Set)` | `MembershipManagerHooks::signal_partitions_lost` |
| `markReconciliationInProgress()` | `AbstractMembershipManager::mark_reconciliation_in_progress` |
| `markReconciliationCompleted()` | `AbstractMembershipManager::mark_reconciliation_completed` |
| `findResolvableAssignmentAndTriggerMetadataUpdate()` (private) | `AbstractMembershipManager::find_resolvable_assignment_and_trigger_metadata_update` |
| `findTopicNameInGlobalOrLocalCache(Uuid)` (private) | inlined in `find_resolvable_assignment_and_trigger_metadata_update`, its only caller |
| `revokePartitions(Set)` | `AbstractMembershipManager::revoke_partitions` (**new in P4b**) |
| `assignPartitions(TopicIdPartitionSet, SortedSet)` (private) | split across `continue_after_revoke` (the `signalPartitionsAssigned` call) and `continue_after_assign` (its `whenComplete`: enable partitions / failure log, then the topic-name cache trim). Divergence: Java trims the cache unconditionally right after `signalPartitionsAssigned` returns; Rust trims only after the assign callback succeeded. For the consumer a failed `onPartitionsAssigned` therefore skips the trim (backtest S2-4, confirmed by Critic 245; consumer follow-up). The share manager is unaffected: its assignment applies inline, and a failure there throws before Java's trim too |
| `signalPartitionsAssigned(TopicIdPartitionSet, SortedSet)` (abstract) | `MembershipManagerHooks::signal_partitions_assigned` (takes `TopicIdPartitionSet#topicPartitions`, ledger G5) |
| `signalPartitionsBeingRevoked(Set)` | `MembershipManagerHooks::signal_partitions_being_revoked` (**new in P4b**) |
| `signalPartitionsRevoked(Set)` | `MembershipManagerHooks::signal_partitions_revoked` |
| `markPendingRevocationToPauseFetching(Set)` | `AbstractMembershipManager::mark_pending_revocation_to_pause_fetching` (**new in P4b**) |
| `clearPendingAssignmentsAndLocalNamesCache()` (private) | `MembershipInner::clear_pending_assignments_and_local_names_cache` |
| `resetEpoch()` | inlined at its two callers (`transition_to_fenced`, `transition_to_joining`) as `update_member_epoch(join_group_epoch)` |
| `joinGroupEpoch()` (abstract) | `MembershipManagerHooks::join_group_epoch` |
| `leaveGroupEpoch()` (abstract) | `MembershipManagerHooks::leave_group_epoch` |
| `updateMemberEpoch(int)` | `MembershipInner::update_member_epoch` |
| `state()` | `AbstractMembershipManager::state` |
| `currentAssignment()` | `AbstractMembershipManager::current_assignment` |
| `topicsAwaitingReconciliation()` (visible for testing) | none in production: a test helper in `consumer_membership_manager.rs` (`topics_awaiting_reconciliation`) reads the same state |
| `topicPartitionsAwaitingReconciliation()` (visible for testing) | none in production: test helper `topic_partitions_awaiting_reconciliation` |
| `reconciliationInProgress()` (visible for testing) | none in production: test helper `reconciliation_in_progress` reads `MembershipInner::reconciliation_in_progress` |
| `registerStateListener(MemberStateListener)` | `AbstractMembershipManager::register_state_listener` |
| `poll(long)` | none on the base: each subclass's `RequestManager::poll` (the consumer's bg loop calls `ConsumerMembershipManager::reconcile(now, false)` itself; `ShareMembershipManager::poll` overrides with `maybeReconcile(true)`) |
| `stateListeners()` (visible for testing) | none: Java tests only; Rust tests read `MembershipInner::state_updates_listeners` |
| `subscriptionUpdated()` (visible for testing) | none: Java tests only; Rust tests read `MembershipInner::subscription_updated` |

`AbstractMembershipManager.LocalAssignment` (nested):

| Java method | Rust |
|---|---|
| `LocalAssignment(long, Map)` | `LocalAssignment::new` |
| `LocalAssignment(long, TopicIdPartitionSet)` | none: `TopicIdPartitionSet` is not translated (ledger G5); its caller builds the map |
| `toString()` | `#[derive(Debug)]` |
| `equals(Object)` / `hashCode()` | `#[derive(PartialEq, Eq)]` |
| `isNone()` | `LocalAssignment::is_none` |
| `updateWith(Map)` | `LocalAssignment::update_with` |
| `NONE` (constant) | `LocalAssignment::none` |

### `ShareMembershipManager` (`share_membership_manager.rs`)

| Java method | Rust |
|---|---|
| `ShareMembershipManager(LogContext, String, String, SubscriptionState, Metadata, Time, Metrics)` | `ShareMembershipManager::new` |
| `ShareMembershipManager(LogContext, String, String, SubscriptionState, Metadata, Time, ShareRebalanceMetricsManager)` (visible for testing) | `ShareMembershipManager::with_metrics_manager` |
| `rackId()` | `ShareMembershipManager::rack_id` |
| `errorCode(ShareGroupHeartbeatResponse)` | `MembershipManagerHooks::error_code` impl |
| `memberEpoch(ShareGroupHeartbeatResponse)` | `MembershipManagerHooks::member_epoch_with_response` impl |
| `extractAssignment(ShareGroupHeartbeatResponse)` | `MembershipManagerHooks::extract_assignment` impl |
| `signalPartitionsAssigned(TopicIdPartitionSet, SortedSet)` | `MembershipManagerHooks::signal_partitions_assigned` impl |
| `joinGroupEpoch()` | `MembershipManagerHooks::join_group_epoch` impl |
| `leaveGroupEpoch()` | `MembershipManagerHooks::leave_group_epoch` impl |
| `poll(long)` | `ShareMembershipManager::poll` (and its `RequestManager::poll` impl) |

### `ShareHeartbeatRequestManager` (`share_heartbeat_request_manager.rs`)

| Java method | Rust |
|---|---|
| `ShareHeartbeatRequestManager(LogContext, Time, ConsumerConfig, CoordinatorRequestManager, SubscriptionState, ShareMembershipManager, BackgroundEventHandler, Metrics)` | `ShareHeartbeatRequestManager::new` |
| `ShareHeartbeatRequestManager(LogContext, Timer, ConsumerConfig, CoordinatorRequestManager, ShareMembershipManager, HeartbeatState, HeartbeatRequestState, BackgroundEventHandler, Metrics)` (visible for testing) | `ShareHeartbeatRequestManager::with_state` |
| `handleSpecificFailure(Throwable)` | `HeartbeatRequestManagerHooks::handle_specific_failure` impl |
| `handleSpecificExceptionInResponse(ShareGroupHeartbeatResponse, long)` | `HeartbeatRequestManagerHooks::handle_specific_error_in_response` impl |
| `resetHeartbeatState()` | `HeartbeatRequestManagerHooks::reset_heartbeat_state` impl |
| `buildHeartbeatRequest()` | `HeartbeatRequestManagerHooks::build_heartbeat_request` impl |
| `heartbeatRequestName()` | `HeartbeatRequestManagerHooks::heartbeat_request_name` impl |
| `errorForResponse(ShareGroupHeartbeatResponse)` | `HeartbeatRequestManagerHooks::error_for_response` impl |
| `errorMessageForResponse(ShareGroupHeartbeatResponse)` | `HeartbeatRequestManagerHooks::error_message_for_response` impl |
| `heartbeatIntervalForResponse(ShareGroupHeartbeatResponse)` | `HeartbeatRequestManagerHooks::heartbeat_interval_for_response` impl |
| `membershipManager()` | `HeartbeatRequestManagerHooks::membership_manager` impl |
| `shouldSendLeaveHeartbeatNow()` | `HeartbeatRequestManagerHooks::should_send_leave_heartbeat_now` impl |
| `SHARE_PROTOCOL_NOT_SUPPORTED_MSG`, `SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG` (constants) | `ShareHeartbeatRequestManager::SHARE_PROTOCOL_NOT_SUPPORTED_MSG`, `::SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG` |
| `HeartbeatState(SubscriptionState, ShareMembershipManager)` | `HeartbeatState::new` |
| `HeartbeatState#reset()` | `HeartbeatState::reset` |
| `HeartbeatState#buildRequestData()` | `HeartbeatState::build_request_data` |
| `HeartbeatState.SentFields()` | `SentFields::new` |
| `HeartbeatState.SentFields#reset()` | `SentFields::reset` |

Rust-only: `set_completion_notify` (the bg-task wake, as the consumer
manager's), `inner` / `inner_mut` / `reset_poll_timer` (access to the
composed base), and `response_body` (Java's cast of the response body).

### `ShareRebalanceMetricsManager` (`metrics/share_rebalance_metrics_manager.rs`)

| Java method | Rust |
|---|---|
| `ShareRebalanceMetricsManager(Metrics)` | `ShareRebalanceMetricsManager::new` |
| `ShareRebalanceMetricsManager(MetricsLedger)` (private) | folded into `new`, its only caller |
| `recordRebalanceStarted(long)` | `RebalanceMetricsManager::record_rebalance_started` impl |
| `recordRebalanceEnded(long)` | `RebalanceMetricsManager::record_rebalance_ended` impl |
| `rebalanceStarted()` | `RebalanceMetricsManager::rebalance_started` impl |
| `maybeRecordRebalanceFailed()` (inherited, not overridden) | the trait's default (empty) body |
| `close()` (inherited from `AbstractConsumerMetricsManager`) | `ShareRebalanceMetricsManager::close` |

### `ShareConsumerMetadata` (`share_consumer_metadata.rs`)

| Java method | Rust |
|---|---|
| `ShareConsumerMetadata(long, long, long, boolean, SubscriptionState, LogContext, ClusterResourceListeners)` | `ShareConsumerMetadata::new` (the `LogContext` is dropped, as for `ConsumerMetadata`) |
| `ShareConsumerMetadata(ConsumerConfig, SubscriptionState, LogContext, ClusterResourceListeners)` | `ShareConsumerMetadata::with_config` |
| `allowAutoTopicCreation()` | `ShareConsumerMetadata::allow_auto_topic_creation` |
| `newMetadataRequestBuilder()` (override) | the `request_builder_fn` override passed to `Metadata::with_overrides` |
| `retainTopic(String, boolean, long)` (override) | the `retain_topic_fn` override passed to `Metadata::with_overrides` |

Rust-only: `metadata_arc` and `Deref<Target = Metadata>` stand in for Java's
`extends Metadata`.

### Summary

| Class | Java methods | Mapped to a Rust function | No Rust function, with reason |
|---|---|---|---|
| `AbstractMembershipManager` (+ `LocalAssignment`) | 79 | 65 | 14: 6 inlined into their only caller or split into the `CompletableFuture` chain steps, 6 visible-for-testing accessors, the base `poll` (each subclass polls), the `TopicIdPartitionSet` constructor (G5) |
| `ShareMembershipManager` | 10 | 10 | 0 |
| `ShareHeartbeatRequestManager` (+ `HeartbeatState`, `SentFields`) | 18 | 18 | 0 |
| `ShareRebalanceMetricsManager` | 7 | 6 | 1 (private constructor folded in) |
| `ShareConsumerMetadata` | 5 | 5 | 0 |

No unmapped method.

## P3a

### `ShareFetchConfig` (`share_fetch_config.rs`)

| Java method | Rust |
|---|---|
| `ShareFetchConfig(int, int, int, int, int, boolean, String, IsolationLevel, ShareAcquireMode)` | `ShareFetchConfig::new` |
| `ShareFetchConfig(ConsumerConfig)` | `ShareFetchConfig::with_consumer_config` (isolation level through `ConsumerUtils::configured_isolation_level`, acquire mode through `ShareAcquireMode::of`) |
| `toString()` | `impl Display` |
| public final fields `minBytes` … `shareAcquireMode` | `pub` fields on the crate-private struct, as `FetchConfig` |

### `ShareSessionHandler` (`share_session_handler.rs`)

| Java method | Rust |
|---|---|
| `ShareSessionHandler(LogContext, int, Uuid)` | `ShareSessionHandler::new(node, member_id)` (the `LogContext` is dropped; `log` crate) |
| `sessionPartitionMap()` | `ShareSessionHandler::session_partition_map` |
| `addPartitionToFetch(TopicIdPartition, Acknowledgements)` | `ShareSessionHandler::add_partition_to_fetch` (`Option<Acknowledgements>` for the nullable argument) |
| `addPartitionToAcknowledgeOnly(TopicIdPartition, Acknowledgements)` | `ShareSessionHandler::add_partition_to_acknowledge_only` |
| `isNewSession()` | `ShareSessionHandler::is_new_session` |
| `isSessionEmpty()` | `ShareSessionHandler::is_session_empty` |
| `newShareFetchBuilder(String, ShareFetchConfig, boolean)` | `ShareSessionHandler::new_share_fetch_builder` (`None` for Java's `null`) |
| `newShareAcknowledgeBuilder(String)` | `ShareSessionHandler::new_share_acknowledge_builder` (`None` for Java's `null`) |
| `topicIdPartitionsToLogString(Collection)` (private) | `ShareSessionHandler::topic_id_partitions_to_log_string` |
| `handleResponse(ShareFetchResponse, short)` | `ShareSessionHandler::handle_response_with_share_fetch_response` (CLAUDE.md §2: same parameter name, different type) |
| `handleResponse(ShareAcknowledgeResponse, short)` | `ShareSessionHandler::handle_response_with_share_acknowledge_response` |
| `notifyClose()` | `ShareSessionHandler::notify_close` |
| `handleError(Throwable)` | `ShareSessionHandler::handle_error(&Error)` |

### `ShareInFlightBatch` (`share_in_flight_batch.rs`)

The record objects Java keeps are offset → delivery-count entries (D5 option
(c)); renewed records are decoded again behind the private seam
`redelivery_walk` (one pass over the record batches per `take_renewals`).

| Java method | Rust |
|---|---|
| `ShareInFlightBatch(int, TopicIdPartition, Optional<Integer>)` | `ShareInFlightBatch::new` |
| `addAcknowledgement(long, AcknowledgeType)` | `ShareInFlightBatch::add_acknowledgement` |
| `acknowledge(ConsumerRecord, AcknowledgeType)` | `ShareInFlightBatch::acknowledge` (`Err(IllegalStateError)` with Java's text) |
| `acknowledgeAll(AcknowledgeType)` | `ShareInFlightBatch::acknowledge_all` |
| `checkAllInFlightAreAcknowledged()` | `ShareInFlightBatch::check_all_in_flight_are_acknowledged` |
| `addRecord(ConsumerRecord)` | `ShareInFlightBatch::add_record` |
| `addGap(long)` | `ShareInFlightBatch::add_gap` |
| `isInFlight(long)` | `ShareInFlightBatch::is_in_flight` |
| `getInFlightRecords()` | `ShareInFlightBatch::in_flight_records` (hands each record out once; see the type's doc) |
| `numRecords()` | `ShareInFlightBatch::num_records` |
| `nodeId()` | `ShareInFlightBatch::node_id` |
| `takeAcknowledgedRecords()` | `ShareInFlightBatch::take_acknowledged_records` (also applies the copy-on-renew memory policy) |
| `renew(Acknowledgements)` | `ShareInFlightBatch::renew` (`Err(IllegalStateError)` with Java's text) |
| `hasRenewals()` | `ShareInFlightBatch::has_renewals` |
| `takeRenewals()` | `ShareInFlightBatch::take_renewals(&Deserializers)` (async: redelivers through the seam) |
| `getAcknowledgements()` | `ShareInFlightBatch::acknowledgements` |
| `getAcquisitionLockTimeoutMs()` | `ShareInFlightBatch::acquisition_lock_timeout_ms` |
| `isEmpty()` | `ShareInFlightBatch::is_empty` |
| `setException(ShareInFlightBatchException)` | `ShareInFlightBatch::set_error` |
| `getException()` | `ShareInFlightBatch::error` |
| `setHasCachedException(boolean)` | `ShareInFlightBatch::set_has_cached_error` |
| `hasCachedException()` | `ShareInFlightBatch::has_cached_error` |

Rust-only: `set_record_batches` (the seam's input from `ShareCompletedFetch`),
`redelivery_walk` + `RedeliveryWalk` (the seam: `redeliver_record`, `step_to`,
`not_found`), `share_stream_error` (Java's bare stream `KafkaException`),
`retain_record_batches_for_redelivery` (memory
policy), `release_after_failed_delivery`, `status` + `ShareInFlightBatchStatus`
(Java shares the batch with `ShareCompletedFetch#deliveredBatch`),
`publish_status`; `corrupt_batch_error` (Java's
`ShareCompletedFetch#maybeEnsureValid` text, shared with the redelivery decoder).

### `ShareInFlightBatchException` (`consumer_share_in_flight_batch_error.rs`)

| Java method | Rust |
|---|---|
| `ShareInFlightBatchException(KafkaException, Set<Long>)` | `ConsumerShareInFlightBatchError::new` |
| `cause()` | `ConsumerShareInFlightBatchError::cause` |
| `offsets()` | `ConsumerShareInFlightBatchError::offsets` |

### `ShareCompletedFetch` (`share_completed_fetch.rs`)

| Java method | Rust |
|---|---|
| `ShareCompletedFetch(LogContext, BufferSupplier, int, TopicIdPartition, PartitionData, Optional<Integer>, ShareFetchMetricsAggregator, short)` | `ShareCompletedFetch::new` (`LogContext` dropped) |
| `buildAcquiredRecordList(List)` (private) | `ShareCompletedFetch::build_acquired_record_list` (preallocation capped at 65 536) |
| `isInitialized()` | `ShareCompletedFetch::is_initialized` |
| `setInitialized()` | `ShareCompletedFetch::set_initialized` |
| `isConsumed()` | `ShareCompletedFetch::is_consumed` |
| `trackDeliveredBatch(ShareInFlightBatch)` | `ShareCompletedFetch::track_delivered_batch` (takes the batch's `ShareInFlightBatchStatus`), called from `hand_out` as the batch is returned, not at the start of `fetch_records`, so a dropped call's batch is never tracked |
| `hasPendingAcknowledgements()` | `ShareCompletedFetch::has_pending_acknowledgements` |
| `drain()` | `ShareCompletedFetch::drain` (also drops the cursor, releasing this fetch's hold on the response) |
| `recordAggregatedMetrics(int, int)` | `ShareCompletedFetch::record_aggregated_metrics` |
| `fetchRecords(Deserializers, int, boolean)` | `ShareCompletedFetch::fetch_records` (async; the `try` body is `fetch_records_loop`) |
| `initializeNextAcquired()` (private) | `ShareCompletedFetch::initialize_next_acquired` |
| `nextAcquiredRecord()` (private) | `ShareCompletedFetch::next_acquired_record` |
| `rejectRecordBatch(ShareInFlightBatch, RecordBatch)` (private) | `ShareCompletedFetch::reject_record_batch` (reads the current batch from the fetch) |
| `parseRecord(Deserializers, TopicIdPartition, Optional, TimestampType, Record, short)` | `batch_cursor::parse_record` (shared with `CompletedFetch`), called with the delivery count |
| `newRecordDeserializationException(...)` (private static) | `batch_cursor::wrap_deserialization_error` with tail `"The record has been released."` |
| `nextFetchedRecord(boolean)` (private) | `ShareCompletedFetch::next_fetched_record` |
| `maybeLeaderEpoch(int)` (private) | `batch_cursor::maybe_leader_epoch` |
| `maybeEnsureValid(RecordBatch, boolean)` (private) | `ShareCompletedFetch::maybe_ensure_valid` (+ `BatchCursor::ensure_valid`) |
| `maybeEnsureValid(Record, boolean)` (private) | none: `DefaultRecord#ensureValid` is a no-op for v2 records, and v0/v1 batches are refused by the decoder (consumer D7) |
| `maybeCloseRecordStream()` (private) | `ShareCompletedFetch::maybe_close_record_stream` |
| `OffsetAndDeliveryCount` (nested, `toString`) | `(i64, i16)` tuples; the `toString` is never called |

Rust-only: `records_size` (the collector's trace line), `consume_current_record`
(Java's `records.next()` consumes on read; an acquired record is consumed once
its parse returns, and `parse_pending` makes a call dropped mid-parse resume on
the same record, so the next call decodes it again), `hand_out` (tracks the
batch as it is returned), `skip_current_batch` (leaves a failed batch unread).
Failures are moved past as Java's iterators do: a record that fails to be read
is consumed, and a batch refused by the decoder is the current batch and is
moved past (`BatchCursor::next_batch_moving_past_refused`).

### `CompletedFetch` decoder extraction (`batch_cursor.rs`, behaviour-neutral)

| Moved from `completed_fetch.rs` | Now |
|---|---|
| `BatchMetadata`, `BatchCursor`, `RecordSource`, `PendingRecordSource`, `RecordStream` | `batch_cursor.rs`, same bodies (`BatchMetadata` gains `batch_start`/`batch_end`) |
| `CompletedFetch::{read_current_record, ensure_none_remaining, peek_current_record, advance_record_cursor, current_record_source_bytes}` | `BatchCursor` methods taking the partition; `CompletedFetch` keeps thin wrappers |
| `CompletedFetch::load_next_batch` phases 1 and 3 | `BatchCursor::next_batch` / `BatchCursor::install_batch`; READ_COMMITTED stays in `CompletedFetch` |
| `CompletedFetch::maybe_close_record_stream` body | `BatchCursor::close_record_stream` |
| the per-record parse in `fetch_records_loop` | `batch_cursor::parse_record` |
| `wrap_deserialization_error`, `premature_eof_error`, `record_size_is_premature_eof`, `read_record`, `records_remaining_error`, `raw_base_offset`, `maybe_leader_epoch` | `batch_cursor.rs` |

## P3b

### `ShareFetch` (`share_fetch.rs`)

| Java method | Rust |
|---|---|
| `empty()` | `ShareFetch::empty` |
| `ShareFetch(Map, Optional)` (private) | folded into `empty`, its only caller |
| `add(TopicIdPartition, ShareInFlightBatch)` | `ShareFetch::add` |
| `records()` | `ShareFetch::records` (owned map; records handed out once) |
| `numRecords()` | `ShareFetch::num_records` |
| `isEmpty()` | `ShareFetch::is_empty` |
| `acquisitionLockTimeoutMs()` | `ShareFetch::acquisition_lock_timeout_ms` |
| `hasRenewals()` | `ShareFetch::has_renewals` |
| `takeRenewedRecords()` | `ShareFetch::take_renewed_records(&Deserializers)` (async, `Result`: redelivery failure when nothing is in flight) |
| `acknowledge(ConsumerRecord, AcknowledgeType)` | `ShareFetch::acknowledge_with_record` |
| `acknowledge(String, int, long, AcknowledgeType)` | `ShareFetch::acknowledge_with_topic_partition_offset` |
| `acknowledgeAll(AcknowledgeType)` | `ShareFetch::acknowledge_all` |
| `checkAllInFlightAreAcknowledged()` | `ShareFetch::check_all_in_flight_are_acknowledged` |
| `takeAcknowledgedRecords()` | `ShareFetch::take_acknowledged_records` (`IndexMap`, R5) |
| `renew(Map, Optional)` | `ShareFetch::renew` |

### `ShareFetchException` (`consumer_share_fetch_error.rs`)

| Java method | Rust |
|---|---|
| `ShareFetchException(ShareFetch, KafkaException)` | `ConsumerShareFetchError::new` |
| `shareFetch()` | `ConsumerShareFetchError::share_fetch` |
| `cause()` | `ConsumerShareFetchError::cause` |

Rust-only: `into_parts` (P7's `currentFetch = e.shareFetch(); throw e.cause();` by value).

### `ShareFetchBuffer` (`share_fetch_buffer.rs`)

| Java method | Rust |
|---|---|
| `ShareFetchBuffer(LogContext)` | `ShareFetchBuffer::new` (`LogContext` dropped) |
| `isEmpty()` | `ShareFetchBuffer::is_empty` |
| `add(List)` | `ShareFetchBuffer::add` |
| `nextInLineFetch()` | `ShareFetchBuffer::next_in_line_fetch(f)` (calls `f` with the reference under the lock) |
| `setNextInLineFetch(ShareCompletedFetch)` | `ShareFetchBuffer::set_next_in_line_fetch` |
| `peek()` | `ShareFetchBuffer::peek(f)` |
| `poll()` | `ShareFetchBuffer::poll` |
| `awaitNotEmpty(Timer)` | `ShareFetchBuffer::await_not_empty(Duration)` (async) |
| `wakeup()` | `ShareFetchBuffer::wakeup` |
| `bufferedPartitions()` | `ShareFetchBuffer::buffered_partitions` |
| `bufferedNodes()` | `ShareFetchBuffer::buffered_nodes` |
| `prunePendingAcknowledgementFetches()` (private) | `ShareFetchBuffer::prune_pending_acknowledgement_fetches` |
| `drainAll()` (private) | `ShareFetchBuffer::drain_all` |
| `close()` | `ShareFetchBuffer::close` (closed flag for `IdempotentCloser`, G14) |

Rust-only: `take_next_in_line_fetch`, `poll_checked_out`, `push_front`,
`clear_checked_out` (the collector reads a fetch out of the lock; the
checked-out marker keeps it buffered, as Java's in-place read does).

### `ShareFetchCollector` (`share_fetch_collector.rs`)

| Java method | Rust |
|---|---|
| `ShareFetchCollector(LogContext, ShareConsumerMetadata, SubscriptionState, ShareFetchConfig, Deserializers)` | `ShareFetchCollector::new` (`LogContext` dropped; `Arc<Deserializers>` shared with the consumer, as `FetchCollector`) |
| `collect(ShareFetchBuffer)` | `ShareFetchCollector::collect` (async; the `try` body is `collect_into`) |
| `initialize(ShareCompletedFetch)` (protected) | `ShareFetchCollector::initialize` (returns whether to read the fetch; Java returns it or `null`) |
| `handleInitializeSuccess(ShareCompletedFetch)` (private) | `ShareFetchCollector::handle_initialize_success` |
| `handleInitializeErrors(ShareCompletedFetch, Errors)` (private) | `ShareFetchCollector::handle_initialize_errors` |

Rust-only: `request_metadata_update` (forwards to `FetchUtils`, now taking
`&Metadata` as Java's does), `CheckedOut` (drop guard), the test-only
`set_force_initialize_error` (Java's anonymous `initialize` overrides).

### `ShareFetchMetricsRegistry` (`share_fetch_metrics_registry.rs`)

| Java method | Rust |
|---|---|
| `ShareFetchMetricsRegistry()` | `ShareFetchMetricsRegistry::new` |
| `ShareFetchMetricsRegistry(String)` | `ShareFetchMetricsRegistry::with_metric_grp_prefix` |
| `ShareFetchMetricsRegistry(Set, String)` | `ShareFetchMetricsRegistry::with_tags_metric_grp_prefix` (`IndexSet`, as `FetchMetricsRegistry::new`) |
| 18 public `MetricNameTemplate` fields | `pub(crate)` fields with the same snake_case names (the type is crate-private) |

### `ShareFetchMetricsManager` (`share_fetch_metrics_manager.rs`)

| Java method | Rust |
|---|---|
| `ShareFetchMetricsManager(Metrics, ShareFetchMetricsRegistry)` | `ShareFetchMetricsManager::new` |
| `ShareFetchMetricsManager(MetricsLedger, ShareFetchMetricsRegistry)` (private) | folded into `new`, its only caller |
| `throttleTimeSensor()` | `ShareFetchMetricsManager::throttle_time_sensor` |
| `recordLatency(String, long)` | `ShareFetchMetricsManager::record_latency` |
| `recordBytesFetched(int)` | `ShareFetchMetricsManager::record_bytes_fetched` |
| `recordRecordsFetched(int)` | `ShareFetchMetricsManager::record_records_fetched` |
| `recordAcknowledgementSent(int)` | `ShareFetchMetricsManager::record_acknowledgement_sent` |
| `recordFailedAcknowledgements(int)` | `ShareFetchMetricsManager::record_failed_acknowledgements` |
| `close()` (inherited from `AbstractConsumerMetricsManager`) | `ShareFetchMetricsManager::close` (forwards to the composed base) |

### `ShareFetchMetricsAggregator` (`share_fetch_metrics_aggregator.rs`)

| Java method | Rust |
|---|---|
| `ShareFetchMetricsAggregator(ShareFetchMetricsManager, Set)` | `ShareFetchMetricsAggregator::new` |
| `record(TopicPartition, int, int)` | `ShareFetchMetricsAggregator::record` |
| `maybeRecordMetrics(TopicPartition)` (private) | `ShareFetchMetricsAggregator::maybe_record_metrics` (returns the totals so `record` writes them outside the lock) |
| `FetchMetrics#increment(int, int)` (nested, private) | `FetchMetrics::increment` |

### `ConsumerUtils` (G4, `consumer_utils.rs`)

| Java method | Rust |
|---|---|
| `createShareFetchMetricsManager(Metrics)` | `ConsumerUtils::create_share_fetch_metrics_manager` |

### Summary (P3a + P3b)

| Class | Java methods | Mapped to a Rust function | No Rust function, with reason |
|---|---|---|---|
| `ShareFetchConfig` | 3 (+ fields) | 3 | 0 |
| `ShareSessionHandler` | 13 | 13 | 0 |
| `ShareInFlightBatch` | 22 | 22 | 0 |
| `ShareInFlightBatchException` | 3 | 3 | 0 |
| `ShareCompletedFetch` (+ `OffsetAndDeliveryCount`) | 21 | 19 | 2: `maybeEnsureValid(Record, boolean)` (no-op for v2), `OffsetAndDeliveryCount#toString` (a tuple; never called) |
| `ShareFetch` | 15 | 14 | 1: private constructor folded into `empty` |
| `ShareFetchException` | 3 | 3 | 0 |
| `ShareFetchBuffer` | 14 | 14 | 0 |
| `ShareFetchCollector` | 5 | 5 | 0 |
| `ShareFetchMetricsRegistry` | 3 | 3 | 0 (the 18 fields map to fields) |
| `ShareFetchMetricsManager` | 9 | 8 | 1: private constructor folded into `new` |
| `ShareFetchMetricsAggregator` (+ `FetchMetrics`) | 4 | 4 | 0 |
| `ConsumerUtils#createShareFetchMetricsManager` | 1 | 1 | 0 |

No unmapped method.

## P5

`ShareConsumeRequestManager` and its nested classes are in
`share_consume_request_manager.rs`; the two event classes pulled forward from
P6 are under `events/`. A nested class's methods that reach the outer
instance in Java take the manager as a parameter (`manager`).

### `ShareConsumeRequestManager` (`share_consume_request_manager.rs`)

| Java method | Rust |
|---|---|
| `ShareConsumeRequestManager(Time, LogContext, String, ShareConsumerMetadata, SubscriptionState, ShareFetchConfig, ShareFetchBuffer, ShareAcknowledgementEventHandler, ShareFetchMetricsManager, long, long)` | `ShareConsumeRequestManager::new` (the `LogContext` is the `log` crate) |
| `poll(long)` | `RequestManager::poll` (runs `handle_completed_requests` first) |
| `isShareAcquireModeRecordLimit()` (private) | `is_share_acquire_mode_record_limit` |
| `maybeAddAcknowledgements(ShareSessionHandler, Node, TopicIdPartition, Acknowledgements)` (private) | `maybe_add_acknowledgements` (returns the copy for the session handler instead of `boolean`) |
| `prepareRemainingSessionHandlerForRequest(Node, ShareSessionHandler, Map)` (private) | `prepare_remaining_session_handler_for_request` |
| `isSessionCloseCandidate(Node, ShareSessionHandler, Map, Set)` (private) | `is_session_close_candidate` |
| `nodeHasPendingAcknowledgements(int)` (private) | `node_has_pending_acknowledgements` |
| `removeSessionHandlersForMissingNodes(Set)` (private) | `remove_session_handlers_for_missing_nodes` |
| `fetch(Map)` | `fetch` |
| `processAcknowledgementsMap(Map)` (private) | `process_acknowledgements_map` |
| `processAcknowledgements(long)` (private) | `process_acknowledgements` (`Option<PollResult>` for Java's nullable) |
| `isNodeFree(int)` (private) | `is_node_free` |
| `setAcknowledgementCommitCallbackRegistered(boolean)` | `set_acknowledgement_commit_callback_registered` |
| `maybeSendShareAcknowledgementEvent(Map, boolean, Optional)` (private) | `maybe_send_share_acknowledgement_event` (takes the map by value) |
| `maybeBuildRequest(AcknowledgeRequestState, long, boolean, AtomicBoolean)` (private) | `maybe_build_request` (the `finally`) + `do_maybe_build_request` (the `try` body) |
| `checkAndRemoveCompletedAcknowledgements()` (private) | `check_and_remove_completed_acknowledgements` |
| `isRequestStateInProgress(AcknowledgeRequestState)` (private) | `is_request_state_in_progress` |
| `areRequestStatesInProgress(Queue)` (private) | `are_request_states_in_progress` |
| `commitSync(Map, long)` | `commit_sync` (returns `KafkaFutureImpl<HashMap<..>>`, Java's `CompletableFuture`) |
| `commitAsync(Map, long)` | `commit_async` |
| `acknowledgeOnClose(Map, long)` | `acknowledge_on_close` (returns the `close_future` handle) |
| `handleShareFetchSuccess(Node, ShareFetchRequestData, ClientResponse)` (private) | `handle_share_fetch_success` (`finally`) + `do_handle_share_fetch_success` (`try`) |
| `handleShareFetchFailure(Node, ShareFetchRequestData, Throwable)` (private) | `handle_share_fetch_failure` |
| `maybeRemoveSessionHandlerOnClose(int, ShareFetchRequestData)` (private) | `maybe_remove_session_handler_on_close` (takes the epoch, the only field read) |
| `handleShareAcknowledgeSuccess(Node, ShareAcknowledgeRequestData, AcknowledgeRequestState, ClientResponse, long)` (private) | `handle_share_acknowledge_success` (`finally`) + `do_handle_share_acknowledge_success` (`try`) |
| `handleShareAcknowledgeFailure(Node, ShareAcknowledgeRequestData, AcknowledgeRequestState, Throwable, long)` (private) | `handle_share_acknowledge_failure` |
| `handlePartitionError(...)` (private) | `handle_partition_error` |
| `processRetryLogic(AcknowledgeRequestState, AtomicBoolean, long)` (private) | `process_retry_logic` |
| `updateLeaderInfoMap(...)` (private) | `update_leader_info_map` |
| `maybeUpdateLeaderCache(TopicIdPartition, int, int)` (private) | `maybe_update_leader_cache` |
| `acknowledgementsCannotBeSentError(int, TopicIdPartition)` (private) | `acknowledgements_cannot_be_sent_error` |
| `lookupTopicId(Uuid, int)` (private) | `lookup_topic_id` (one `topic_names()` snapshot per response) |
| `partitionsToFetch()` (private) | `partitions_to_fetch` |
| `sessionHandler(int)` | `session_handler` |
| `hasCompletedFetches()` | `has_completed_fetches` |
| `closeInternal()` (protected) | `close_internal` |
| `close()` | `close` (Java's `IdempotentCloser` is the `closed` flag) |
| `onMemberEpochUpdated(Optional, String)` | `on_member_epoch_updated` (inherent, `&mut self`) + `impl MemberStateListener for Mutex<ShareConsumeRequestManager>` |
| `requestStates(int)` | `request_states` |
| `shareSessionNodeId(TopicIdPartition)` | `share_session_node_id` (test-only in Java too) |
| `RequestManager#pollOnClose`, `#maximumTimeToWait`, `#signalClose` | not overridden in Java; the `RequestManager` trait defaults apply |
| *(Rust-only)* | `handle_completed_requests`: runs the `whenComplete` callback bodies (DoD §7: Java runs them inside the network poll); `acknowledgements_by_node`: the grouping loop Java repeats in `commitSync` / `commitAsync` / `acknowledgeOnClose` |

### `ShareConsumeRequestManager.AcknowledgeRequestState`

`extends TimedRequestState`: a `base: TimedRequestState` field; the inherited
`canSendRequest`, `onSendAttempt`, `onSuccessfulAttempt`, `onFailedAttempt`
delegate to it.

| Java method | Rust |
|---|---|
| `AcknowledgeRequestState(LogContext, String, long, long, long, ShareSessionHandler, int, Map, ResultHandler, AcknowledgeRequestType)` | `AcknowledgeRequestState::new` (takes `&dyn Time`, Java's outer `time`) |
| `buildRequest()` | `build_request(manager, this)` |
| `getInFlightAcknowledgementsCount(TopicIdPartition)` | `in_flight_acknowledgements_count` |
| `getIncompleteAcknowledgementsCount(TopicIdPartition)` | `incomplete_acknowledgements_count` |
| `getAcknowledgementsToSendCount(TopicIdPartition)` | `acknowledgements_to_send_count` (test-only in Java too) |
| `isEmpty()` | `is_empty` |
| `maybeResetTimerAndRequestState()` | `maybe_reset_timer_and_request_state` |
| `handleAcknowledgeErrorCode(TopicIdPartition, Errors, boolean, Optional)` | `handle_acknowledge_error_code` |
| `handleAcknowledgeTimedOut(TopicIdPartition)` | `handle_acknowledge_timed_out` (takes the entry out; its only caller clears the map next) |
| `failPendingAcknowledgementsCannotBeSent()` | `fail_pending_acknowledgements_cannot_be_sent` |
| `sessionHandler()` | `session_handler` |
| `processingComplete()` | `processing_complete` |
| `processPendingInFlightAcknowledgements(KafkaException)` (private) | `process_pending_in_flight_acknowledgements` |
| `moveAllToIncompleteAcks()` | `move_all_to_incomplete_acks` |
| `maybeExpire()` | `maybe_expire` |
| `moveToIncompleteAcks(TopicIdPartition)` | `move_to_incomplete_acks` |
| `isCloseRequest()` | `is_close_request` |

### `ShareConsumeRequestManager.ResultHandler`, `Tuple`, `IdAndPartition`, `LeaderIdAndEpoch`, `AcknowledgeRequestType`

| Java method | Rust |
|---|---|
| `ResultHandler(Optional)` | `ResultHandler::new` |
| `ResultHandler(AtomicInteger, Optional)` | `ResultHandler::with_remaining_results_future` |
| `ResultHandler#complete(TopicIdPartition, Acknowledgements, AcknowledgeRequestType, boolean, Optional)` | `ResultHandler::complete(manager, ..)` |
| `ResultHandler#completeIfEmpty()` | `ResultHandler::complete_if_empty` |
| `Tuple(V, Queue, V)` | `Tuple::new` |
| `Tuple#setAsyncRequest`, `#nullifySyncRequestQueue`, `#addSyncRequest`, `#setCloseRequest` | `set_async_request`, `nullify_sync_request_queue`, `add_sync_request`, `set_close_request` |
| `Tuple#getAsyncRequest`, `#getSyncRequestQueue`, `#getCloseRequest` | `async_request`, `sync_request_queue`, `close_request` |
| `IdAndPartition(Uuid, int)` | `IdAndPartition::new` |
| `IdAndPartition#hashCode`, `#equals` | `#[derive(Hash, PartialEq, Eq)]` |
| `LeaderIdAndEpoch(int, int)` | `LeaderIdAndEpoch::new` |
| `AcknowledgeRequestType` constants, `id` field | enum variants, `AcknowledgeRequestType::id` |
| `AcknowledgeRequestType(byte)` (enum constructor) | none: a Rust enum has no constructor |
| `AcknowledgeRequestType#toString()` | `impl Display` (lower-case name) |

### `ShareAcknowledgementEvent` / `ShareAcknowledgementEventHandler` (`events/`, pulled forward from P6)

| Java method | Rust |
|---|---|
| `ShareAcknowledgementEvent(Map, boolean, Optional)` | `ShareAcknowledgementEvent::new` (takes the map by value; Java's `Map.copyOf`) |
| `acknowledgementsMap()`, `checkForRenewAcknowledgements()`, `acquisitionLockTimeoutMs()` | same names, snake_case |
| `ShareAcknowledgementEventHandler(BlockingQueue)` | `ShareAcknowledgementEventHandler::new` (owns the channel) |
| `add(ShareAcknowledgementEvent)` | `add` |
| `drainEvents()` | `drain_events` |

### Summary

| Class | Java methods | Mapped to a Rust function | No Rust function, with reason |
|---|---|---|---|
| `ShareConsumeRequestManager` | 40 (+ 3 inherited defaults) | 40 | 0 (the 3 defaults are not overridden in Java) |
| `AcknowledgeRequestState` | 17 | 17 | 0 |
| `ResultHandler` | 4 | 4 | 0 |
| `Tuple` | 8 | 8 | 0 |
| `IdAndPartition` | 3 | 3 (2 by derive) | 0 |
| `LeaderIdAndEpoch` | 1 | 1 | 0 |
| `AcknowledgeRequestType` | 2 (+ constructor) | 2 | 1: the enum constructor |
| `ShareAcknowledgementEvent` | 4 | 4 | 0 |
| `ShareAcknowledgementEventHandler` | 3 | 3 | 0 |

No unmapped method.

## P6

The eight share application events are `ApplicationEvent` variants in
`events/application_event.rs` (the consumer's one-enum translation of the
`ApplicationEvent` hierarchy); a Java field is a variant field, and an
accessor of a field is the field read by the `match` that destructures the
variant. A completable event's `deadlineMs()` and `future()` are its
`handle` (`CompletableEventHandle::deadline_ms`, the handle itself). Java's
`toStringBase()` overrides have no Rust function: as for every consumer
event, `Display` prints the variant name (`ApplicationEvent::type_name`).

### `SharePollEvent` (`ApplicationEvent::SharePoll`)

| Java method | Rust |
|---|---|
| `SharePollEvent(long, long)` | the variant (`deadline_ms`, `poll_time_ms`, `is_complete`) |
| `deadlineMs()`, `pollTimeMs()` | the fields |
| `isComplete()` | `is_complete.load(..)`: the application side keeps a clone of the `Arc<AtomicBool>` (P7) |
| `completeSuccessfully()` | `is_complete.store(true, ..)` in `ApplicationEventProcessor::process_share_poll` |
| `isExpired(Time)` | none: the event moves to the background task, so the application side compares the deadline it kept (P7, as `AsyncKafkaConsumer`'s `InflightPoll` does for `AsyncPollEvent`) |
| `toStringBase()` | `Display` (variant name) |

### `ShareFetchEvent`, `ShareAcknowledgeAsyncEvent`, `ShareAcknowledgementCommitCallbackRegistrationEvent`

| Java method | Rust |
|---|---|
| `ShareFetchEvent(Map)` | `ApplicationEvent::ShareFetch { acknowledgements_map }` |
| `ShareFetchEvent#acknowledgementsMap()` | the field |
| `ShareFetchEvent#toStringBase()` | `Display` (variant name) |
| `ShareAcknowledgeAsyncEvent(Map, long)` | `ApplicationEvent::ShareAcknowledgeAsync { acknowledgements_map, deadline_ms }` |
| `ShareAcknowledgeAsyncEvent#acknowledgementsMap()`, `#deadlineMs()` | the fields |
| `ShareAcknowledgeAsyncEvent#toStringBase()` | `Display` (variant name) |
| `ShareAcknowledgementCommitCallbackRegistrationEvent(boolean)` | `ApplicationEvent::ShareAcknowledgementCommitCallbackRegistration { is_callback_registered }` |
| `ShareAcknowledgementCommitCallbackRegistrationEvent#isCallbackRegistered()` | the field |

### `ShareAcknowledgeSyncEvent`, `ShareAcknowledgeOnCloseEvent`, `ShareSubscriptionChangeEvent`, `ShareUnsubscribeEvent`

| Java method | Rust |
|---|---|
| `ShareAcknowledgeSyncEvent(Map, long)` | `ApplicationEvent::ShareAcknowledgeSync { handle, acknowledgements_map }` (`CompletableEventHandle<HashMap<TopicIdPartition, Acknowledgements>>`, deadline on the handle) |
| `ShareAcknowledgeSyncEvent#acknowledgementsMap()` | the field |
| `ShareAcknowledgeSyncEvent#toStringBase()` | `Display` (variant name) |
| `ShareAcknowledgeOnCloseEvent(Map, long)` | `ApplicationEvent::ShareAcknowledgeOnClose { handle, acknowledgements_map }` |
| `ShareAcknowledgeOnCloseEvent#acknowledgementsMap()` | the field |
| `ShareAcknowledgeOnCloseEvent#toStringBase()` | `Display` (variant name) |
| `ShareSubscriptionChangeEvent(Collection)` | `ApplicationEvent::ShareSubscriptionChange { handle, topics }` (`HashSet`: Java's `Set.copyOf`; handle deadline `i64::MAX`, Java's `Long.MAX_VALUE`) |
| `ShareSubscriptionChangeEvent#topics()` | the field |
| `ShareSubscriptionChangeEvent#toStringBase()` | `Display` (variant name) |
| `ShareUnsubscribeEvent(long)` | `ApplicationEvent::ShareUnsubscribe { handle }` |
| `ApplicationEvent.Type` share values (`SHARE_POLL`, …) | the variant discriminants (`ApplicationEvent::type_name`) |

### `ApplicationEventProcessor` (`events/application_event_processor.rs`, share arms)

| Java method | Rust |
|---|---|
| `process(ApplicationEvent)`, the eight `SHARE_*` cases | the share arms of `EventProcessor::process`'s `match` |
| `process(SharePollEvent)` (private) | `process_share_poll` |
| `process(ShareFetchEvent)` (private) | `process_share_fetch` |
| `process(ShareAcknowledgeSyncEvent)` (private) | `process_share_acknowledge_sync` |
| `process(ShareAcknowledgeAsyncEvent)` (private) | `process_share_acknowledge_async` |
| `process(ShareSubscriptionChangeEvent)` (private) | `process_share_subscription_change` |
| `process(ShareUnsubscribeEvent)` (private) | `process_share_unsubscribe` |
| `process(ShareAcknowledgeOnCloseEvent)` (private) | `process_share_acknowledge_on_close` |
| `process(ShareAcknowledgementCommitCallbackRegistrationEvent)` (private) | `process_share_acknowledgement_commit_callback_registration` |
| `complete(CompletableFuture)` (private) | `ApplicationEventProcessor::complete` (first use; the consumer arms keep their own `complete_handle_on_leave`) |
| *(Rust-only)* | `share_heartbeat_membership_manager`, `share_consume_request_manager`: clone the manager out of `RequestManagers` so its lock is released before the call (Java reads the `Optional` fields); `lock`, the poison-tolerant lock of the share consume manager |

### `RequestManagers` (`request_managers.rs`, share members)

| Java member | Rust |
|---|---|
| `shareHeartbeatRequestManager`, `shareMembershipManager`, `shareConsumeRequestManager` (fields) | `share_heartbeat`, `share_membership` (`Arc`), `share_consume` (`Arc<Mutex<_>>`) |
| `RequestManagers(LogContext, ShareConsumeRequestManager, Optional, Optional, Optional)` | `RequestManagers::with_share_consume_request_manager` (`LogContext` dropped) |
| `supplier(Time, LogContext, ShareAcknowledgementEventHandler, BackgroundEventHandler, ShareConsumerMetadata, SubscriptionState, ShareFetchBuffer, ConsumerConfig, GroupRebalanceConfig, ShareFetchMetricsManager, Optional<ClientTelemetryReporter>, Metrics)` (static) | `RequestManagers::supplier_with_share_acknowledgement_event_handler`: returns the managers, not a `Supplier`; `GroupRebalanceConfig` → `group_id`, `rack_id` (G8); `LogContext`, `ClientTelemetryReporter` and the member-id label update dropped (G16); Rust-only `completion_notify` |
| `entries()` | `entries` / `for_each_entry`: share heartbeat → share membership → share consume after the (absent) consumer heartbeat |
| `close()` | `close`, which now also closes the share consume request manager (Java's `Closeable` filter) |
| *(Rust-only)* | `handle_completed_requests` (routes the hook below), `for_each_entry` (the non-allocating walk `entries` and the hook share) |

### `RequestManager` (`request_manager.rs`) and `ConsumerNetworkThread` (`consumer_network_thread.rs`)

| Java member | Rust |
|---|---|
| *(Rust-only)* `RequestManager::handle_completed_requests` | default no-op; overridden by `ShareConsumeRequestManager` and `Arc<Mutex<ShareConsumeRequestManager>>` (DoD §7: Java runs `whenComplete` inside `NetworkClientDelegate#poll`) |
| `ConsumerNetworkThread#runOnce` | `run_once`: Phase 4.5 calls `RequestManagers::handle_completed_requests` right after the network poll, before `maximumTimeToWait` and the reaper |
| `ConsumerNetworkThread#sendUnsentRequests` (in `cleanup`) | `cleanup` step 2: the hook after each close poll |

### `ShareConsumeRequestManager` and `ShareMembershipManager` (P6 additions)

| Java member | Rust |
|---|---|
| `ShareConsumeRequestManager` as a `RequestManagers` entry | *(Rust-only)* `impl RequestManager for Arc<Mutex<ShareConsumeRequestManager>>` (each method locks for the call; `signal_close` keeps the default, as Java's manager does not override it) |
| `ShareConsumeRequestManager` `whenComplete` bodies | `RequestManager::handle_completed_requests` override, forwarding to the inherent `handle_completed_requests` |
| Mockito `verify(manager).setAcknowledgementCommitCallbackRegistered(b)` | *(test-only)* `is_acknowledgement_commit_callback_registered_for_test` |
| `AbstractMembershipManager#maybeReconcile(boolean)` (inherited by `ShareMembershipManager`) | `ShareMembershipManager::maybe_reconcile` |
| `AbstractMembershipManager#registerStateListener(MemberStateListener)` (inherited) | `ShareMembershipManager::register_state_listener` |
| `ShareMembershipManager` as a `RequestManagers` entry | *(Rust-only)* `impl RequestManager for Arc<ShareMembershipManager>` (its `poll`) |

### `SubscriptionState` (share members, `subscription_state.rs`; verified, unchanged)

| Java member | Rust |
|---|---|
| `SubscriptionType.AUTO_TOPICS_SHARE` | `SubscriptionType::AutoTopicsShare` |
| `subscribeToShareGroup(Set)` | `subscribe_to_share_group` |
| `prettyString()`, `AUTO_TOPICS_SHARE` case | `pretty_string` (`"Subscribe to Share Group(..)"`) |
| `fetchablePartitions(Predicate)`, `hasFetchablePartitions(Predicate)`, `AUTO_TOPICS_SHARE` branch | `fetchable_partitions`, `has_fetchable_partitions` |
| `hasAutoAssignedPartitions()`, `AUTO_TOPICS_SHARE` operand | `has_auto_assigned_partitions` |

### `WakeupTrigger` (share members)

| Java member | Rust |
|---|---|
| `setShareFetchAction(ShareFetchBuffer)` | none: the trigger has no task state (module docs); the share `poll` `select!`s on the token around `ShareFetchBuffer::await_not_empty` (P7) |
| `ShareFetchAction` (nested class), `#fetchBuffer()` | none, for the same reason |
| `wakeup()`, `ShareFetchAction` branch (`fetchBuffer().wakeup()`) | `wakeup` cancels the token, which ends the `select!` |
| `clearTask()` / `disableWakeups()`, `ShareFetchAction` cases | `rotate` / `disable` (no per-task case) |

### Summary

| Class | Java members | Mapped to a Rust function or field | No Rust function, with reason |
|---|---|---|---|
| `SharePollEvent` | 7 | 6 (`toStringBase` as `Display`) | 1: `isExpired`, read on the application side (P7) |
| `ShareFetchEvent` | 3 | 3 | 0 |
| `ShareAcknowledgeAsyncEvent` | 4 | 4 | 0 |
| `ShareAcknowledgementCommitCallbackRegistrationEvent` | 2 | 2 | 0 |
| `ShareAcknowledgeSyncEvent` | 3 | 3 | 0 |
| `ShareAcknowledgeOnCloseEvent` | 3 | 3 | 0 |
| `ShareSubscriptionChangeEvent` | 3 | 3 | 0 |
| `ShareUnsubscribeEvent` | 1 | 1 | 0 |
| `ApplicationEventProcessor` (share) | 10 | 10 | 0 |
| `RequestManagers` (share) | 7 | 7 | 0 |
| `SubscriptionState` (share) | 5 | 5 | 0 |
| `WakeupTrigger` (share) | 4 | 2 | 2: `setShareFetchAction`, `ShareFetchAction` (no task state in the Rust trigger) |

No unmapped method.

## P7

`ShareConsumerImpl` (`share_consumer_impl.rs`), `KafkaShareConsumerMetrics`
(`metrics/kafka_share_consumer_metrics.rs`), `ShareConsumerMetrics`
(`share_consumer_metrics.rs`), and the shared helpers P7 added. Overload names
follow CLAUDE.md §2 provisionally; P8 fixes the public ones
(`share-consumer.md` §1).

### `ShareConsumerImpl`

| Java member | Rust |
|---|---|
| `ShareConsumerImpl(ConsumerConfig, Deserializer, Deserializer)` | `ShareConsumerImpl::new` (wraps every failure as "Failed to construct Kafka share consumer"; body `new_inner`) |
| `ShareConsumerImpl(ConsumerConfig, Deserializer, Deserializer, Time, ApplicationEventHandlerFactory, CompletableEventReaperFactory, ShareFetchCollectorFactory, LinkedBlockingQueue, LinkedBlockingQueue)` (visible for testing) | none: it exists to inject Mockito mocks. The tests that use it assert construction through `new` and behaviour through `with_components` (`ShareConsumerImplTest` module doc) |
| `ShareConsumerImpl(LogContext, String, String, ConsumerConfig, Deserializer, Deserializer, Time, KafkaClient, SubscriptionState, ShareConsumerMetadata)` (visible for testing) | `ShareConsumerImpl::with_client` (`#[cfg(test)]`; `LogContext` dropped; the background task is spawned on the caller's runtime) |
| `ShareConsumerImpl(LogContext, String, Deserializer, Deserializer, ShareFetchBuffer, ShareFetchCollector, ShareFetchMetricsManager, Time, ApplicationEventHandler, BlockingQueue, BlockingQueue, CompletableEventReaper, Metrics, SubscriptionState, ShareConsumerMetadata, int, int, String, String)` (visible for testing) | `ShareConsumerImpl::with_components(ShareConsumerImplComponents)`, which the other constructors end in; the acknowledgement mode comes parsed |
| `ApplicationEventHandlerFactory#build`, `ShareFetchCollectorFactory#build` (test interfaces) | none: the Mockito seams; `collect_stub` / `await_not_empty_stub` (`#[cfg(test)]`) stand in for the mocked collector and buffer |
| `ShareAcknowledgementEventProcessor()`, `#process(ShareAcknowledgementEvent)` | inlined in `process_acknowledgement_events`, its only caller (renew, then queue for the callback) |
| `BackgroundEventProcessor()`, `#process(BackgroundEvent)`, `#process(ErrorEvent)` | inlined in `process_background_events` (`BackgroundEvent::Error` → its error; any other variant → `"Background event type .. was not expected"`) |
| `subscription()` | `subscription` (`&mut self`; panics when closed) |
| `subscribe(Collection)` | `subscribe` |
| `unsubscribe()` | `unsubscribe` |
| `poll(Duration)` | `poll` (try body `poll_inner`; `finally` = `RecordPollEnd` guard; `catch (ShareFetchException)` = `PollError::ShareFetch`) |
| `checkInFlightPoll(Timer, boolean)` | `check_in_flight_poll` |
| `maybeClearPreviousInFlightPoll()` | `maybe_clear_previous_in_flight_poll` |
| `maybeClearCurrentInFlightPoll(boolean)` | `maybe_clear_current_in_flight_poll` |
| `pollForFetches(Timer)` | `poll_for_fetches` (+ `await_not_empty`, the buffer wait raced on the wakeup token) |
| `collect(Map)` | `collect` (+ `collect_from_buffer`, `maybe_send_share_fetch_event`) |
| `sendShareAcknowledgeAsyncEvent(Map)` | `send_share_acknowledge_async_event` |
| `acknowledge(ConsumerRecord)` | `acknowledge_with_record` |
| `acknowledge(ConsumerRecord, AcknowledgeType)` | `acknowledge_with_record_type` (`Result`, D7) |
| `acknowledge(String, int, long, AcknowledgeType)` | `acknowledge_with_options(AcknowledgeOptions)` (`Result`, D7; renamed in P8 by the CLAUDE.md §2 `Options` rule) |
| `commitSync()` | `commit_sync` |
| `commitSync(Duration)` | `commit_sync_with_timeout` |
| `commitAsync()` | `commit_async` (async: runs the callback) |
| `setAcknowledgementCommitCallback(AcknowledgementCommitCallback)` | `set_acknowledgement_commit_callback` (`Option`; panics when closed) |
| `clientInstanceId(Duration)` | `client_instance_id` (Java's no-reporter `IllegalStateException`, G16) |
| `acquisitionLockTimeoutMs()` | `acquisition_lock_timeout_ms` (`&mut self`; panics when closed) |
| `metrics()` | `metrics` |
| `registerMetricForSubscription(KafkaMetric)`, `unregisterMetricFromSubscription(KafkaMetric)` | none: decision D2 (P8 deferral row) |
| `close()` | `close` |
| `close(Duration)` | `close_with_timeout` (`closed` is set once `close_internal` has run, as Java's `finally` runs after every step; a dropped `close` leaves the consumer open and the next one resumes it through `CloseProgress`, never sending the acknowledge-on-close or the stop signal twice; a `Duration` cannot be negative) |
| `close(Duration, boolean)` (private) | `close_internal` |
| `stopFindCoordinatorOnClose()` | `stop_find_coordinator_on_close` |
| `createTimerForCloseRequests(Duration)` | `create_timer_for_close_requests` (returns the deadline) |
| `sendAcknowledgementsAndLeaveGroup(Timer, AtomicReference)` | `send_acknowledgements_and_leave_group` |
| `wakeup()` | `wakeup` |
| `acquireAndEnsureOpen()` | `acquire_and_ensure_open` (closed check, bootstrap fatal error) and `panic_if_closed` for the methods without an error channel (`subscription`, `acquisition_lock_timeout_ms`, `set_acknowledgement_commit_callback`), which skip the bootstrap fatal check, as the consumer's sync getters do |
| `acquire()`, `release()` | none: `&mut self` (module doc, D6) |
| `createLogContext(String, String)` (static) | `create_log_context` |
| `maybeThrowInvalidGroupIdException()` | `maybe_return_invalid_group_id_error` |
| `handleCompletedAcknowledgements()` | `handle_completed_acknowledgements` (one entry at a time, removed before its callback) |
| `acknowledgeBatchIfImplicitAcknowledgement()` | `acknowledge_batch_if_implicit_acknowledgement` (records handed out only) |
| `ensureInFlightAcknowledgedIfExplicitAcknowledgement()` | `ensure_in_flight_acknowledged_if_explicit_acknowledgement` (records handed out only) |
| `acknowledgementsToSend()` | `acknowledgements_to_send` |
| `ensureExplicitAcknowledgement()` | `ensure_explicit_acknowledgement` |
| `initializeAcknowledgementMode(ConsumerConfig)` (static) | `initialize_acknowledgement_mode` |
| `processAcknowledgementEvents()` | `process_acknowledgement_events` |
| `processBackgroundEventsOnClose()` | `process_background_events_on_close` |
| `processBackgroundEvents()` | `process_background_events` |
| `processBackgroundEvents(Future, Timer, Predicate)` | `process_background_events_with_future_deadline_ms_ignore_error_event_error` |
| `completeQuietly(ThrowingRunnable, String, AtomicReference)` | `complete_quietly` (takes the outcome of the started call) |
| `clientId()` | `client_id` |
| `metricsRegistry()` | `metrics_registry` |
| `asyncConsumerMetrics()` (visible for testing) | `async_consumer_metrics` (`#[cfg(test)]`) |
| `kafkaShareConsumerMetrics()` | `kafka_share_consumer_metrics` |
| `NO_CURRENT_THREAD`, `currentThread`, `refCount` | none: `acquire` / `release` state |
| *(Rust-only)* | `ShareConsumerImplComponents`; `InFlightPoll` (the kept `SharePollEvent` deadline and flag); `Collected`, `PollError`; the drop guards `PendingAcknowledgements` and `RecordPollEnd`; `CloseProgress` (how far a dropped `close` got: a retry awaits the acknowledge-on-close it sent, and only joins once the task was told to stop); `start_background_task` and the `BackgroundTask` it returns (Java's `ApplicationEventHandler` constructor starting the network thread, and the handler owning it); `has_records`, `is_not_empty`, `pending_acknowledgements`, `result_error` (`ConsumerUtils#getResult`'s mapping), `swallow` / `close_quietly_failed` (`Utils#swallow` / `#closeQuietly`), `lock_subscriptions`, `maybe_trigger_wakeup`, `is_closed`; `Debug` |

### `KafkaShareConsumerMetrics`

| Java method | Rust |
|---|---|
| `KafkaShareConsumerMetrics(Metrics)` / private `(MetricsLedger)` | `KafkaShareConsumerMetrics::new` |
| `recordPollStart(long)` | `record_poll_start` |
| `recordPollEnd(long)` | `record_poll_end` |
| `close()` (inherited from `AbstractConsumerMetricsManager`) | `close` |

### `ShareConsumerMetrics`

| Java method | Rust |
|---|---|
| `ShareConsumerMetrics()` | `ShareConsumerMetrics::new` |
| `ShareConsumerMetrics(Set, String)` | `ShareConsumerMetrics::with_metrics_tags_metric_grp_prefix` |
| `shareFetchMetrics` (public field) | `share_fetch_metrics` (`pub(crate)`) |

### Additions to translated classes

| Class | Rust (all Rust-only, cancellation, module doc of `share_consumer_impl.rs`) |
|---|---|
| `ShareCompletedFetch` | `fetch_records_into` (Java's `fetchRecords` into a caller-kept batch; `fetch_records` delegates), `new_in_flight_batch` |
| `ShareFetchCollector` | `collect_with_state` (Java's `collect` into a caller-kept `ShareFetchCollectState`; `collect` delegates), `ShareFetchCollectState`; `deserializers` / `close_deserializers` (Java's `ShareConsumerImpl` holds the shared `Deserializers` itself) |
| `ShareInFlightBatch` | `acknowledge_all_handed_out`, `check_all_handed_out_are_acknowledged` (Java's `acknowledgeAll` / `checkAllInFlightAreAcknowledged` over the records handed out), `has_records_not_handed_out` (`#[cfg(test)]`) |
| `ShareFetch` | the same three, over every batch |
| `ShareAcknowledgementEvent` | `into_acknowledgements_map` |
| `NetworkClientDelegate` | `supplier`: Java's `NetworkClientDelegate#supplier(Time, LogContext, Metadata, ConsumerConfig, ApiVersions, Metrics, Sensor, ClientTelemetrySender, BackgroundEventHandler, boolean, AsyncConsumerMetrics)`, eager; Java's `(Time, ConsumerConfig, LogContext, KafkaClient, ..)` overload is `NetworkClientDelegate::new` plus `set_async_consumer_metrics` at its one caller (`with_client`) |
| `ConsumerUtils` | `create_metrics`: Java's `ConsumerUtils#createMetrics(ConsumerConfig, Time, List)` (no reporters) |
| `RequestManager` | `type_name_for_test` (`#[cfg(test)]`, the `entries()` order pin) |

### Summary

| Class | Java members | Mapped to a Rust function | No Rust function, with reason |
|---|---|---|---|
| `ShareConsumerImpl` | 4 constructors, 49 methods, 2 nested processors (5 members), 2 test factory interfaces | 3 constructors, 45 methods, the 5 processor members (inlined) | 1 constructor (Mockito injection); 4 methods: `registerMetricForSubscription`, `unregisterMetricFromSubscription` (D2: not translated, decided 2026-11-20), `acquire`, `release` (`&mut self`); the 2 factory interfaces (Mockito seams) |
| `KafkaShareConsumerMetrics` | 4 | 4 | 0 |
| `ShareConsumerMetrics` | 3 | 3 | 0 |

No unmapped method.

## P8

The public share consumer API: `ShareConsumer` (`share_consumer.rs`),
`KafkaShareConsumer` (`kafka_share_consumer.rs`), `MockShareConsumer`
(`mock_share_consumer.rs`), plus the Rust-only `DynShareConsumer`,
`ShareConsumerHandle` and `AcknowledgeOptions` / `AcknowledgeOptionsBuilder`
(R8 test plan in PLAN §4.1). `KafkaShareConsumer` forwards every trait method to
the `ShareConsumerImpl` method of the same Rust name (P7 table).

### `ShareConsumer` (trait)

| Java method | Rust |
|---|---|
| `subscription()` | `ShareConsumer::subscription` (`&mut self`; panics on a closed consumer, CLAUDE.md §12.5) |
| `subscribe(Collection)` | `ShareConsumer::subscribe(&[String])` (the only overload, so the plain name) |
| `unsubscribe()` | `ShareConsumer::unsubscribe` |
| `poll(Duration)` | `ShareConsumer::poll` |
| `acknowledge(ConsumerRecord)` | `ShareConsumer::acknowledge_with_record` (`Result`, D7 as P7) |
| `acknowledge(ConsumerRecord, AcknowledgeType)` | `ShareConsumer::acknowledge_with_record_type` |
| `acknowledge(String, int, long, AcknowledgeType)` | `ShareConsumer::acknowledge_with_options(AcknowledgeOptions)` (four parameters: CLAUDE.md §2 `Options` rule) |
| `commitSync()` | `ShareConsumer::commit_sync` |
| `commitSync(Duration)` | `ShareConsumer::commit_sync_with_timeout` |
| `commitAsync()` | `ShareConsumer::commit_async` (async: it runs the callback, `share-consumer.md` §1) |
| `setAcknowledgementCommitCallback(AcknowledgementCommitCallback)` | `ShareConsumer::set_acknowledgement_commit_callback(Option<Arc<dyn …>>)` (`None` is Java's `null`) |
| `clientInstanceId(Duration)` | not on the trait: KIP-714, decision D2 (as `Consumer`); `ShareConsumerImpl::client_instance_id` stays crate-private |
| `acquisitionLockTimeoutMs()` | `ShareConsumer::acquisition_lock_timeout_ms` |
| `metrics()` | `ShareConsumer::metrics` (`&self`) |
| `registerMetricForSubscription(KafkaMetric)` | not translated: KIP-714, decision D2 |
| `unregisterMetricFromSubscription(KafkaMetric)` | not translated: KIP-714, decision D2 |
| `close()` | `ShareConsumer::close` |
| `close(Duration)` | `ShareConsumer::close_with_timeout` (not deprecated on `ShareConsumer`, unlike `Consumer#close(Duration)`) |
| `wakeup()` | `ShareConsumer::wakeup` (`&self`) |
| *(Rust-only)* | `ShareConsumer::handle` → `ShareConsumerHandle` (D6) |

### `KafkaShareConsumer`

| Java member | Rust |
|---|---|
| `KafkaShareConsumer(Map)`, `KafkaShareConsumer(Properties)` | not translated: the deserializers would come from class-valued keys, which the Rust configuration does not have (CLAUDE.md §2) |
| `KafkaShareConsumer(Properties, Deserializer, Deserializer)`, `KafkaShareConsumer(Map, Deserializer, Deserializer)` | `KafkaShareConsumer::new(&HashMap<String, String>, impl Deserializer<K>, impl Deserializer<V>)`: both take the same string map in Rust |
| `KafkaShareConsumer(ShareConsumerConfig, Deserializer, Deserializer)` (package-private) | inlined in `new`: it only calls the delegate creator |
| `KafkaShareConsumer(LogContext, String, String, ShareConsumerConfig, Deserializer, Deserializer, Time, KafkaClient, SubscriptionState, ShareConsumerMetadata)` (package-private, tests) | `KafkaShareConsumer::with_client` (`#[cfg(test)]`; `LogContext` dropped, as `ShareConsumerImpl::with_client`) |
| the 19 `ShareConsumer` overrides | `impl ShareConsumer for KafkaShareConsumer`, forwarding to the delegate (KIP-714 three per D2) |
| `clientId()`, `metricsRegistry()`, `kafkaShareConsumerMetrics()` (package-private, tests) | `client_id`, `metrics_registry`, `kafka_share_consumer_metrics` (`#[cfg(test)]`) |

### `MockShareConsumer`

| Java member | Rust |
|---|---|
| `MockShareConsumer()` | `MockShareConsumer::new` (and `Default`) |
| `subscription()` | `subscription` (panics when closed: Java's `ensureNotClosed` throws, CLAUDE.md §12.5) |
| `subscribe(Collection)` | `subscribe` (`SubscriptionState::subscribe_with_topics`, as Java's `subscribe(Set, Optional.empty())`) |
| `unsubscribe()` | `unsubscribe` |
| `poll(Duration)` | `poll` (drains the records; empty next offsets) |
| `acknowledge(ConsumerRecord)`, `acknowledge(ConsumerRecord, AcknowledgeType)`, `acknowledge(String, int, long, AcknowledgeType)` | `acknowledge_with_record`, `acknowledge_with_record_type`, `acknowledge_with_options` (no-ops returning `Ok`) |
| `commitSync()`, `commitSync(Duration)`, `commitAsync()` | `commit_sync`, `commit_sync_with_timeout` (empty maps), `commit_async` |
| `setAcknowledgementCommitCallback(AcknowledgementCommitCallback)` | `set_acknowledgement_commit_callback` (no-op) |
| `setClientInstanceId(Uuid)`, `clientInstanceId(Duration)` | not translated: KIP-714, decision D2 (`MockConsumer` has neither) |
| `metrics()` | `metrics` (panics when closed, as Java's `ensureNotClosed` throws) |
| `acquisitionLockTimeoutMs()` | `acquisition_lock_timeout_ms` (`None`) |
| `registerMetricForSubscription`, `unregisterMetricFromSubscription` | not translated: KIP-714, decision D2 |
| `close()`, `close(Duration)` | `close` (forwards the 30 s default), `close_with_timeout` |
| `wakeup()` | `wakeup` (sets the flag, which Java's mock never reads) |
| `addRecord(ConsumerRecord)` | `add_record` (`Result`, as `MockConsumer::add_record`) |
| `ensureNotClosed()` (private) | `ensure_not_closed` (and `panic_if_closed` for the methods with no error channel) |
| *(Rust-only)* | `handle` (the mock's flag), `wakeup_requested` (`#[cfg(test)]`, reads the flag) |

### `ShareConsumerDelegate`, `ShareConsumerDelegateCreator`

| Java member | Rust |
|---|---|
| `ShareConsumerDelegate` (interface extending `ShareConsumer` with `clientId`, `metricsRegistry`, `kafkaShareConsumerMetrics`) | none: one delegate, held as `ShareConsumerImpl`, whose three methods of the same names `KafkaShareConsumer`'s test accessors call (`share-consumer.md` §1) |
| `ShareConsumerDelegateCreator#create(ConsumerConfig, Deserializer, Deserializer)` | collapsed into `KafkaShareConsumer::new`; the "Failed to construct Kafka share consumer" wrap is `ShareConsumerImpl::new`'s |
| `ShareConsumerDelegateCreator#create(LogContext, …, ShareConsumerMetadata)` | collapsed into `KafkaShareConsumer::with_client` |

### Summary

| Class | Java members | Mapped to a Rust function | No Rust function, with reason |
|---|---|---|---|
| `ShareConsumer` | 19 | 16 | 3: `clientInstanceId`, `registerMetricForSubscription`, `unregisterMetricFromSubscription` (D2) |
| `KafkaShareConsumer` | 6 constructors, 19 overrides, 3 test accessors | 2 constructors (`new`, `with_client`) covering 4 Java ones, 16 overrides, 3 accessors | 2 constructors (class-valued deserializer keys), 3 overrides (D2) |
| `MockShareConsumer` | 1 constructor, 21 methods, 1 private | 1, 17, 1 | 4 methods: `setClientInstanceId`, `clientInstanceId`, `registerMetricForSubscription`, `unregisterMetricFromSubscription` (D2) |
| `ShareConsumerDelegate` / `ShareConsumerDelegateCreator` | 3 own / 2 | 3 (on `ShareConsumerImpl`) / 2 (collapsed) | the interface type itself (one delegate) |

No unmapped method.

## P9

Admin share-group RPCs (`org.apache.kafka.clients.admin`). Paths are under
`rust/src/admin/`. Every options class inherits `AbstractOptions#timeoutMs(Integer)`
/ `#timeoutMs()`, mapped once below to each class's `set_timeout_ms` / `timeout_ms`.
`equals` / `hashCode` map to `#[derive(PartialEq, Eq, Hash)]` (or a manual `Hash`
where Java hashes a set), `toString` to `Display`.

### Options (`options/*_share_group*_options.rs`)

| Java method | Rust |
|---|---|
| `DescribeShareGroupsOptions()` (implicit) | `DescribeShareGroupsOptions::new` (+ `Default`) |
| `DescribeShareGroupsOptions#includeAuthorizedOperations(boolean)` | `DescribeShareGroupsOptions::set_include_authorized_operations` |
| `DescribeShareGroupsOptions#includeAuthorizedOperations()` | `DescribeShareGroupsOptions::include_authorized_operations` |
| `AlterShareGroupOffsetsOptions()` (implicit) | `AlterShareGroupOffsetsOptions::new` |
| `ListShareGroupOffsetsOptions()` (implicit) | `ListShareGroupOffsetsOptions::new` |
| `DeleteShareGroupOffsetsOptions()` (implicit) | `DeleteShareGroupOffsetsOptions::new` |
| `DeleteShareGroupsOptions()` (implicit) | `DeleteShareGroupsOptions::new` |
| `AbstractOptions#timeoutMs(Integer)` (each of the 5) | `set_timeout_ms` on each |
| `AbstractOptions#timeoutMs()` (each of the 5) | `timeout_ms` on each |

### Results

| Java method | Rust |
|---|---|
| `DescribeShareGroupsResult(Map)` (public) | `DescribeShareGroupsResult::new` (`pub`) |
| `DescribeShareGroupsResult#describedGroups()` | `DescribeShareGroupsResult::described_groups` |
| `DescribeShareGroupsResult#all()` | `DescribeShareGroupsResult::all` |
| `AlterShareGroupOffsetsResult(KafkaFuture)` (package-private) | `AlterShareGroupOffsetsResult::new` (`pub(crate)`) |
| `AlterShareGroupOffsetsResult#partitionResult(TopicPartition)` | `AlterShareGroupOffsetsResult::partition_result` |
| `AlterShareGroupOffsetsResult#all()` | `AlterShareGroupOffsetsResult::all` |
| `ListShareGroupOffsetsResult(Map)` (package-private) | `ListShareGroupOffsetsResult::new` (`pub(crate)`; takes the map already keyed by group id, as `ListConsumerGroupOffsetsResult::new`) |
| `ListShareGroupOffsetsResult#all()` | `ListShareGroupOffsetsResult::all` |
| `ListShareGroupOffsetsResult#partitionsToOffsetInfo(String)` | `ListShareGroupOffsetsResult::partitions_to_offset_info` (`Result`, for Java's `IllegalArgumentException`) |
| `DeleteShareGroupOffsetsResult(KafkaFuture, Set)` (package-private) | `DeleteShareGroupOffsetsResult::new` (`pub(crate)`) |
| `DeleteShareGroupOffsetsResult#all()` | `DeleteShareGroupOffsetsResult::all` |
| `DeleteShareGroupOffsetsResult#topicResult(String)` | `DeleteShareGroupOffsetsResult::topic_result` (`Result`, for Java's `IllegalArgumentException`) |
| `DeleteShareGroupOffsetsResult#maybeCompleteExceptionally(Map, String, KafkaFutureImpl)` (private) | free fn `maybe_complete_with_error`, returning the `Err` its callers complete with |
| `DeleteShareGroupsResult(Map)` (package-private) | `DeleteShareGroupsResult::new` (`pub(crate)`) |
| `DeleteShareGroupsResult#deletedGroups()` | `DeleteShareGroupsResult::deleted_groups` |
| `DeleteShareGroupsResult#all()` | `DeleteShareGroupsResult::all` |

### Value types

| Java method | Rust |
|---|---|
| `ShareGroupDescription(String, Collection, GroupState, Node, int, int)` | `ShareGroupDescription::new` |
| `ShareGroupDescription(String, Collection, GroupState, Node, int, int, Set)` | `ShareGroupDescription::with_authorized_operations` |
| `ShareGroupDescription#groupId()` / `#members()` / `#groupState()` / `#coordinator()` / `#authorizedOperations()` / `#groupEpoch()` / `#targetAssignmentEpoch()` | the getters of the same snake_case names |
| `ShareGroupDescription#equals` / `#hashCode` / `#toString` | derives / `Display` |
| `ShareMemberDescription(String, Optional, String, String, ShareMemberAssignment, int)` | `ShareMemberDescription::new` |
| `ShareMemberDescription#consumerId()` / `#rackId()` / `#clientId()` / `#host()` / `#assignment()` / `#memberEpoch()` | the getters of the same snake_case names |
| `ShareMemberDescription#equals` / `#hashCode` / `#toString` | derives / `Display` |
| `ShareMemberAssignment(Set)` | `ShareMemberAssignment::new` |
| `ShareMemberAssignment#topicPartitions()` | `ShareMemberAssignment::topic_partitions` |
| `ShareMemberAssignment#equals` / `#hashCode` / `#toString` | derive `PartialEq`, manual order-independent `Hash`, `Display` |
| `SharePartitionOffsetInfo(long, Optional, Optional)` | `SharePartitionOffsetInfo::new` |
| `SharePartitionOffsetInfo#startOffset()` / `#leaderEpoch()` / `#lag()` | the getters of the same snake_case names |
| `SharePartitionOffsetInfo#equals` / `#hashCode` / `#toString` | derives / `Display` |
| `ListShareGroupOffsetsSpec()` (implicit) | `ListShareGroupOffsetsSpec::new` (+ `Default`) |
| `ListShareGroupOffsetsSpec#topicPartitions(Collection)` | `ListShareGroupOffsetsSpec::set_topic_partitions` (CLAUDE.md §2: setter beside the getter) |
| `ListShareGroupOffsetsSpec#topicPartitions()` | `ListShareGroupOffsetsSpec::topic_partitions` |
| `ListShareGroupOffsetsSpec#equals` / `#hashCode` / `#toString` | derives / `Display` |

### Handlers (`internals/`)

| Java method | Rust |
|---|---|
| `DescribeShareGroupsHandler(boolean, LogContext)` | `DescribeShareGroupsHandler::new` |
| `DescribeShareGroupsHandler#buildKeySet(Collection)` (private static) | `DescribeShareGroupsHandler::build_key_set` |
| `DescribeShareGroupsHandler#newFuture(Collection)` | `DescribeShareGroupsHandler::new_future` |
| `DescribeShareGroupsHandler#apiName()` | `AdminApiHandler::api_name` impl |
| `DescribeShareGroupsHandler#lookupStrategy()` | `AdminApiHandler::lookup_strategy` impl |
| `DescribeShareGroupsHandler#buildBatchedRequest(int, Set)` | `DescribeShareGroupsHandler::build_batched_request` (called by `AdminApiHandler::build_request`, Java's `Batched#buildRequest`) |
| `DescribeShareGroupsHandler#handleResponse(Node, Set, AbstractResponse)` | `AdminApiHandler::handle_response` impl |
| `DescribeShareGroupsHandler#convertAssignment(Assignment)` (private) | `DescribeShareGroupsHandler::convert_assignment` |
| `DescribeShareGroupsHandler#handleError(...)` (private) | `DescribeShareGroupsHandler::handle_error` |
| `AlterShareGroupOffsetsHandler(String, Map, LogContext)` | `AlterShareGroupOffsetsHandler::new` |
| `AlterShareGroupOffsetsHandler#newFuture(String)` | `AlterShareGroupOffsetsHandler::new_future` |
| `AlterShareGroupOffsetsHandler#validateKeys(Set)` (private) | `AlterShareGroupOffsetsHandler::validate_keys` (`Result`; an `Err` fails every key, as Java's throw from `handleResponse` does) |
| `AlterShareGroupOffsetsHandler#buildBatchedRequest(int, Set)` | `AlterShareGroupOffsetsHandler::build_batched_request` |
| `AlterShareGroupOffsetsHandler#apiName()` | `AdminApiHandler::api_name` impl |
| `AlterShareGroupOffsetsHandler#handleResponse(Node, Set, AbstractResponse)` | `AdminApiHandler::handle_response` impl |
| `AlterShareGroupOffsetsHandler#handleError(...)` (private) | `AlterShareGroupOffsetsHandler::handle_error` (the `groupId` parameter is always the handler's own, so it reads `self.group_id`) |
| `AlterShareGroupOffsetsHandler#lookupStrategy()` | `AdminApiHandler::lookup_strategy` impl |
| `ListShareGroupOffsetsHandler(Map, LogContext)` | `ListShareGroupOffsetsHandler::new` |
| `ListShareGroupOffsetsHandler#newFuture(Collection)` | `ListShareGroupOffsetsHandler::new_future` |
| `ListShareGroupOffsetsHandler#apiName()` | `AdminApiHandler::api_name` impl |
| `ListShareGroupOffsetsHandler#lookupStrategy()` | `AdminApiHandler::lookup_strategy` impl |
| `ListShareGroupOffsetsHandler#buildBatchedRequest(int, Set)` | `ListShareGroupOffsetsHandler::build_batched_request` |
| `ListShareGroupOffsetsHandler#handleResponse(Node, Set, AbstractResponse)` | `AdminApiHandler::handle_response` impl |
| `ListShareGroupOffsetsHandler#coordinatorKeys(Collection)` (private static) | `ListShareGroupOffsetsHandler::coordinator_keys` |
| `ListShareGroupOffsetsHandler#validateKeys(Set)` (private) | `ListShareGroupOffsetsHandler::validate_keys` (`Result`, as above) |
| `ListShareGroupOffsetsHandler#handleGroupError(...)` (private) | `ListShareGroupOffsetsHandler::handle_group_error` |
| `DeleteShareGroupOffsetsHandler(String, Set, LogContext)` | `DeleteShareGroupOffsetsHandler::new` |
| `DeleteShareGroupOffsetsHandler#apiName()` | `AdminApiHandler::api_name` impl |
| `DeleteShareGroupOffsetsHandler#lookupStrategy()` | `AdminApiHandler::lookup_strategy` impl |
| `DeleteShareGroupOffsetsHandler#newFuture(String)` | `DeleteShareGroupOffsetsHandler::new_future` |
| `DeleteShareGroupOffsetsHandler#validateKeys(Set)` (private) | `DeleteShareGroupOffsetsHandler::validate_keys` (`Result`, as above) |
| `DeleteShareGroupOffsetsHandler#buildBatchedRequest(int, Set)` | `DeleteShareGroupOffsetsHandler::build_batched_request` |
| `DeleteShareGroupOffsetsHandler#handleResponse(Node, Set, AbstractResponse)` | `AdminApiHandler::handle_response` impl |
| `DeleteShareGroupOffsetsHandler#handleGroupError(...)` (private) | `DeleteShareGroupOffsetsHandler::handle_group_error` (the `groupId` parameter is always the handler's own, so it reads `self.group_id`) |
| `DeleteShareGroupsHandler(LogContext)` | `DeleteShareGroupsHandler::new`, returning the base `DeleteGroupsHandler` configured with the subclass names (as `DeleteConsumerGroupsHandler::new`) |
| `DeleteShareGroupsHandler#apiName()` | `DeleteGroupsHandler::api_name` (returns `"deleteShareGroups"`) |
| `DeleteShareGroupsHandler#displayName()` | `DeleteGroupsHandler::display_name` (returns `"DeleteShareGroups"`) |

### `Admin` / `KafkaAdminClient` / `MockAdminClient`

| Java method | Rust |
|---|---|
| `Admin#describeShareGroups(Collection)` (default) | `Admin::describe_share_groups` (default) |
| `Admin#describeShareGroups(Collection, DescribeShareGroupsOptions)` | `Admin::describe_share_groups_with_options` |
| `Admin#alterShareGroupOffsets(String, Map)` (default) | `Admin::alter_share_group_offsets` (default) |
| `Admin#alterShareGroupOffsets(String, Map, AlterShareGroupOffsetsOptions)` | `Admin::alter_share_group_offsets_with_options` |
| `Admin#listShareGroupOffsets(Map)` (default) | `Admin::list_share_group_offsets` (default) |
| `Admin#listShareGroupOffsets(Map, ListShareGroupOffsetsOptions)` | `Admin::list_share_group_offsets_with_options` |
| `Admin#deleteShareGroupOffsets(String, Set)` (default) | `Admin::delete_share_group_offsets` (default) |
| `Admin#deleteShareGroupOffsets(String, Set, DeleteShareGroupOffsetsOptions)` | `Admin::delete_share_group_offsets_with_options` |
| `Admin#deleteShareGroups(Collection)` (default) | `Admin::delete_share_groups` (default) |
| `Admin#deleteShareGroups(Collection, DeleteShareGroupsOptions)` | `Admin::delete_share_groups_with_options` |
| `KafkaAdminClient#describeShareGroups` / `#alterShareGroupOffsets` / `#listShareGroupOffsets` / `#deleteShareGroupOffsets` / `#deleteShareGroups` | the five `*_with_options` in `impl Admin for KafkaAdminClient` |
| `MockAdminClient#describeShareGroups` / `#alterShareGroupOffsets` / `#listShareGroupOffsets` / `#deleteShareGroupOffsets` / `#deleteShareGroups` | the five `*_with_options` in `impl Admin for MockAdminClient`, failing every future with `local_unsupported_operation("Not implemented yet")` |
| `ForwardingAdmin` share overrides | not translated: `ForwardingAdmin` has no Rust counterpart (out of the admin translation) |

### Summary

| Class | Java methods | Mapped to a Rust function | No Rust function, with reason |
|---|---|---|---|
| 5 options classes | 17 (7 own: 5 constructors and the `includeAuthorizedOperations` pair; 10 inherited: `timeoutMs` ×2 on each) | 17 | 0 |
| 5 result classes | 16 | 16 | 0 |
| 5 value types | 40 (incl. `equals` / `hashCode` / `toString`) | 40 | 0 |
| 5 handlers | 37 | 37 | 0 |
| `Admin` / `KafkaAdminClient` / `MockAdminClient` | 20 | 20 | 0 |

No unmapped method. `ForwardingAdmin`'s five overrides are outside the
translated surface (no `ForwardingAdmin` in Rust).
