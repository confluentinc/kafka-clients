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
| `assignPartitions(TopicIdPartitionSet, SortedSet)` (private) | split across `continue_after_revoke` (signal + topic-name cache trim) and `continue_after_assign` (its `whenComplete`: enable partitions / failure log) |
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
| `AbstractMembershipManager` (+ `LocalAssignment`) | 78 | 64 | 14: 6 inlined into their only caller or split into the `CompletableFuture` chain steps, 6 visible-for-testing accessors, the base `poll` (each subclass polls), the `TopicIdPartitionSet` constructor (G5) |
| `ShareMembershipManager` | 10 | 10 | 0 |
| `ShareHeartbeatRequestManager` (+ `HeartbeatState`, `SentFields`) | 18 | 18 | 0 |
| `ShareRebalanceMetricsManager` | 7 | 6 | 1 (private constructor folded in) |
| `ShareConsumerMetadata` | 5 | 5 | 0 |

No unmapped method.
