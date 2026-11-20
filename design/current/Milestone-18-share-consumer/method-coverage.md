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
