// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the Java sources (the generated modules of lib/common/errors/);
// `cargo xtask check-generated` checks it is up to date.

'use strict';

// `lib/common/errors`: the generated modules of lib/common/errors/.

const { ApiError } = require('./api_error');
const { ApplicationRecoverableError } = require('./application_recoverable_error');
const { AuthenticationError } = require('./authentication_error');
const { AuthorizationError } = require('./authorization_error');
const { AuthorizerNotReadyError } = require('./authorizer_not_ready_error');
const { BootstrapResolutionError } = require('./bootstrap_resolution_error');
const { BrokerIdNotRegisteredError } = require('./broker_id_not_registered_error');
const { BrokerNotAvailableError } = require('./broker_not_available_error');
const { ClusterAuthorizationError } = require('./cluster_authorization_error');
const { ConcurrentTransactionsError } = require('./concurrent_transactions_error');
const { ControllerIdNotRegisteredError } = require('./controller_id_not_registered_error');
const { ControllerMovedError } = require('./controller_moved_error');
const { CoordinatorLoadInProgressError } = require('./coordinator_load_in_progress_error');
const { CoordinatorNotAvailableError } = require('./coordinator_not_available_error');
const { CorruptRecordError } = require('./corrupt_record_error');
const { DelegationTokenAuthorizationError } = require('./delegation_token_authorization_error');
const { DelegationTokenDisabledError } = require('./delegation_token_disabled_error');
const { DelegationTokenExpiredError } = require('./delegation_token_expired_error');
const { DelegationTokenNotFoundError } = require('./delegation_token_not_found_error');
const { DelegationTokenOwnerMismatchError } = require('./delegation_token_owner_mismatch_error');
const { DisconnectError } = require('./disconnect_error');
const { DuplicateBrokerRegistrationError } = require('./duplicate_broker_registration_error');
const { DuplicateResourceError } = require('./duplicate_resource_error');
const { DuplicateSequenceError } = require('./duplicate_sequence_error');
const { DuplicateVoterError } = require('./duplicate_voter_error');
const { ElectionNotNeededError } = require('./election_not_needed_error');
const { EligibleLeadersNotAvailableError } = require('./eligible_leaders_not_available_error');
const { FeatureUpdateFailedError } = require('./feature_update_failed_error');
const { FencedInstanceIdError } = require('./fenced_instance_id_error');
const { FencedLeaderEpochError } = require('./fenced_leader_epoch_error');
const { FencedMemberEpochError } = require('./fenced_member_epoch_error');
const { FencedStateEpochError } = require('./fenced_state_epoch_error');
const { FetchSessionIdNotFoundError } = require('./fetch_session_id_not_found_error');
const { FetchSessionTopicIdError } = require('./fetch_session_topic_id_error');
const { GroupAuthorizationError } = require('./group_authorization_error');
const { GroupDeletionFailedError } = require('./group_deletion_failed_error');
const { GroupIdNotFoundError } = require('./group_id_not_found_error');
const { GroupMaxSizeReachedError } = require('./group_max_size_reached_error');
const { GroupNotEmptyError } = require('./group_not_empty_error');
const { GroupSubscribedToTopicError } = require('./group_subscribed_to_topic_error');
const { IllegalGenerationError } = require('./illegal_generation_error');
const { IllegalSaslStateError } = require('./illegal_sasl_state_error');
const { InconsistentClusterIdError } = require('./inconsistent_cluster_id_error');
const { InconsistentGroupProtocolError } = require('./inconsistent_group_protocol_error');
const { InconsistentTopicIdError } = require('./inconsistent_topic_id_error');
const { InconsistentVoterSetError } = require('./inconsistent_voter_set_error');
const { IneligibleReplicaError } = require('./ineligible_replica_error');
const { InterruptError } = require('./interrupt_error');
const { InvalidCommitOffsetSizeError } = require('./invalid_commit_offset_size_error');
const { InvalidConfigurationError } = require('./invalid_configuration_error');
const { InvalidFetchSessionEpochError } = require('./invalid_fetch_session_epoch_error');
const { InvalidFetchSizeError } = require('./invalid_fetch_size_error');
const { InvalidGroupIdError } = require('./invalid_group_id_error');
const { InvalidMetadataError } = require('./invalid_metadata_error');
const { InvalidOffsetError } = require('./invalid_offset_error');
const { InvalidPartitionsError } = require('./invalid_partitions_error');
const { InvalidPidMappingError } = require('./invalid_pid_mapping_error');
const { InvalidPrincipalTypeError } = require('./invalid_principal_type_error');
const { InvalidProducerEpochError } = require('./invalid_producer_epoch_error');
const { InvalidRecordStateError } = require('./invalid_record_state_error');
const { InvalidRegistrationError } = require('./invalid_registration_error');
const { InvalidRegularExpression } = require('./invalid_regular_expression');
const { InvalidReplicaAssignmentError } = require('./invalid_replica_assignment_error');
const { InvalidReplicationFactorError } = require('./invalid_replication_factor_error');
const { InvalidRequestError } = require('./invalid_request_error');
const { InvalidRequiredAcksError } = require('./invalid_required_acks_error');
const { InvalidSessionTimeoutError } = require('./invalid_session_timeout_error');
const { InvalidShareSessionEpochError } = require('./invalid_share_session_epoch_error');
const { InvalidTimestampError } = require('./invalid_timestamp_error');
const { InvalidTopicError } = require('./invalid_topic_error');
const { InvalidTxnStateError } = require('./invalid_txn_state_error');
const { InvalidTxnTimeoutError } = require('./invalid_txn_timeout_error');
const { InvalidUpdateVersionError } = require('./invalid_update_version_error');
const { InvalidVoterKeyError } = require('./invalid_voter_key_error');
const { KafkaStorageError } = require('./kafka_storage_error');
const { LeaderNotAvailableError } = require('./leader_not_available_error');
const { ListenerNotFoundError } = require('./listener_not_found_error');
const { LogDirNotFoundError } = require('./log_dir_not_found_error');
const { MemberIdRequiredError } = require('./member_id_required_error');
const { MismatchedEndpointTypeError } = require('./mismatched_endpoint_type_error');
const { NetworkError } = require('./network_error');
const { NewLeaderElectedError } = require('./new_leader_elected_error');
const { NoReassignmentInProgressError } = require('./no_reassignment_in_progress_error');
const { NotControllerError } = require('./not_controller_error');
const { NotCoordinatorError } = require('./not_coordinator_error');
const { NotEnoughReplicasAfterAppendError } = require('./not_enough_replicas_after_append_error');
const { NotEnoughReplicasError } = require('./not_enough_replicas_error');
const { NotLeaderOrFollowerError } = require('./not_leader_or_follower_error');
const { OffsetMetadataTooLarge } = require('./offset_metadata_too_large');
const { OffsetMovedToTieredStorageError } = require('./offset_moved_to_tiered_storage_error');
const { OffsetNotAvailableError } = require('./offset_not_available_error');
const { OffsetOutOfRangeError } = require('./offset_out_of_range_error');
const { OperationNotAttemptedError } = require('./operation_not_attempted_error');
const { OutOfOrderSequenceError } = require('./out_of_order_sequence_error');
const { PolicyViolationError } = require('./policy_violation_error');
const { PositionOutOfRangeError } = require('./position_out_of_range_error');
const { PreferredLeaderNotAvailableError } = require('./preferred_leader_not_available_error');
const { PrincipalDeserializationError } = require('./principal_deserialization_error');
const { ProducerFencedError } = require('./producer_fenced_error');
const { ReassignmentInProgressError } = require('./reassignment_in_progress_error');
const { RebalanceInProgressError } = require('./rebalance_in_progress_error');
const { RebootstrapRequiredError } = require('./rebootstrap_required_error');
const { RecordBatchTooLargeError } = require('./record_batch_too_large_error');
const { RecordDeserializationError } = require('./record_deserialization_error');
const { RecordTooLargeError } = require('./record_too_large_error');
const { RefreshRetriableError } = require('./refresh_retriable_error');
const { ReplicaNotAvailableError } = require('./replica_not_available_error');
const { ResourceNotFoundError } = require('./resource_not_found_error');
const { RetriableError } = require('./retriable_error');
const { SaslAuthenticationError } = require('./sasl_authentication_error');
const { SecurityDisabledError } = require('./security_disabled_error');
const { SerializationError } = require('./serialization_error');
const { ShareSessionLimitReachedError } = require('./share_session_limit_reached_error');
const { ShareSessionNotFoundError } = require('./share_session_not_found_error');
const { SnapshotNotFoundError } = require('./snapshot_not_found_error');
const { SslAuthenticationError } = require('./ssl_authentication_error');
const { StaleBrokerEpochError } = require('./stale_broker_epoch_error');
const { StaleMemberEpochError } = require('./stale_member_epoch_error');
const { StreamsInvalidTopologyEpochError } = require('./streams_invalid_topology_epoch_error');
const { StreamsInvalidTopologyError } = require('./streams_invalid_topology_error');
const { StreamsTopologyDescriptionUpdateFailedError } = require('./streams_topology_description_update_failed_error');
const { StreamsTopologyFencedError } = require('./streams_topology_fenced_error');
const { TelemetryTooLargeError } = require('./telemetry_too_large_error');
const { ThrottlingQuotaExceededError } = require('./throttling_quota_exceeded_error');
const { TimeoutError } = require('./timeout_error');
const { TopicAuthorizationError } = require('./topic_authorization_error');
const { TopicDeletionDisabledError } = require('./topic_deletion_disabled_error');
const { TopicExistsError } = require('./topic_exists_error');
const { TransactionAbortableError } = require('./transaction_abortable_error');
const { TransactionAbortedError } = require('./transaction_aborted_error');
const { TransactionCoordinatorFencedError } = require('./transaction_coordinator_fenced_error');
const { TransactionalIdAuthorizationError } = require('./transactional_id_authorization_error');
const { TransactionalIdNotFoundError } = require('./transactional_id_not_found_error');
const { UnacceptableCredentialError } = require('./unacceptable_credential_error');
const { UnknownControllerIdError } = require('./unknown_controller_id_error');
const { UnknownLeaderEpochError } = require('./unknown_leader_epoch_error');
const { UnknownMemberIdError } = require('./unknown_member_id_error');
const { UnknownProducerIdError } = require('./unknown_producer_id_error');
const { UnknownServerError } = require('./unknown_server_error');
const { UnknownSubscriptionIdError } = require('./unknown_subscription_id_error');
const { UnknownTopicIdError } = require('./unknown_topic_id_error');
const { UnknownTopicOrPartitionError } = require('./unknown_topic_or_partition_error');
const { UnreleasedInstanceIdError } = require('./unreleased_instance_id_error');
const { UnstableOffsetCommitError } = require('./unstable_offset_commit_error');
const { UnsupportedAssignorError } = require('./unsupported_assignor_error');
const { UnsupportedByAuthenticationError } = require('./unsupported_by_authentication_error');
const { UnsupportedCompressionTypeError } = require('./unsupported_compression_type_error');
const { UnsupportedEndpointTypeError } = require('./unsupported_endpoint_type_error');
const { UnsupportedForMessageFormatError } = require('./unsupported_for_message_format_error');
const { UnsupportedSaslMechanismError } = require('./unsupported_sasl_mechanism_error');
const { UnsupportedVersionError } = require('./unsupported_version_error');
const { VoterNotFoundError } = require('./voter_not_found_error');
const { WakeupError } = require('./wakeup_error');

module.exports = {
  ApiError,
  ApplicationRecoverableError,
  AuthenticationError,
  AuthorizationError,
  AuthorizerNotReadyError,
  BootstrapResolutionError,
  BrokerIdNotRegisteredError,
  BrokerNotAvailableError,
  ClusterAuthorizationError,
  ConcurrentTransactionsError,
  ControllerIdNotRegisteredError,
  ControllerMovedError,
  CoordinatorLoadInProgressError,
  CoordinatorNotAvailableError,
  CorruptRecordError,
  DelegationTokenAuthorizationError,
  DelegationTokenDisabledError,
  DelegationTokenExpiredError,
  DelegationTokenNotFoundError,
  DelegationTokenOwnerMismatchError,
  DisconnectError,
  DuplicateBrokerRegistrationError,
  DuplicateResourceError,
  DuplicateSequenceError,
  DuplicateVoterError,
  ElectionNotNeededError,
  EligibleLeadersNotAvailableError,
  FeatureUpdateFailedError,
  FencedInstanceIdError,
  FencedLeaderEpochError,
  FencedMemberEpochError,
  FencedStateEpochError,
  FetchSessionIdNotFoundError,
  FetchSessionTopicIdError,
  GroupAuthorizationError,
  GroupDeletionFailedError,
  GroupIdNotFoundError,
  GroupMaxSizeReachedError,
  GroupNotEmptyError,
  GroupSubscribedToTopicError,
  IllegalGenerationError,
  IllegalSaslStateError,
  InconsistentClusterIdError,
  InconsistentGroupProtocolError,
  InconsistentTopicIdError,
  InconsistentVoterSetError,
  IneligibleReplicaError,
  InterruptError,
  InvalidCommitOffsetSizeError,
  InvalidConfigurationError,
  InvalidFetchSessionEpochError,
  InvalidFetchSizeError,
  InvalidGroupIdError,
  InvalidMetadataError,
  InvalidOffsetError,
  InvalidPartitionsError,
  InvalidPidMappingError,
  InvalidPrincipalTypeError,
  InvalidProducerEpochError,
  InvalidRecordStateError,
  InvalidRegistrationError,
  InvalidRegularExpression,
  InvalidReplicaAssignmentError,
  InvalidReplicationFactorError,
  InvalidRequestError,
  InvalidRequiredAcksError,
  InvalidSessionTimeoutError,
  InvalidShareSessionEpochError,
  InvalidTimestampError,
  InvalidTopicError,
  InvalidTxnStateError,
  InvalidTxnTimeoutError,
  InvalidUpdateVersionError,
  InvalidVoterKeyError,
  KafkaStorageError,
  LeaderNotAvailableError,
  ListenerNotFoundError,
  LogDirNotFoundError,
  MemberIdRequiredError,
  MismatchedEndpointTypeError,
  NetworkError,
  NewLeaderElectedError,
  NoReassignmentInProgressError,
  NotControllerError,
  NotCoordinatorError,
  NotEnoughReplicasAfterAppendError,
  NotEnoughReplicasError,
  NotLeaderOrFollowerError,
  OffsetMetadataTooLarge,
  OffsetMovedToTieredStorageError,
  OffsetNotAvailableError,
  OffsetOutOfRangeError,
  OperationNotAttemptedError,
  OutOfOrderSequenceError,
  PolicyViolationError,
  PositionOutOfRangeError,
  PreferredLeaderNotAvailableError,
  PrincipalDeserializationError,
  ProducerFencedError,
  ReassignmentInProgressError,
  RebalanceInProgressError,
  RebootstrapRequiredError,
  RecordBatchTooLargeError,
  RecordDeserializationError,
  RecordTooLargeError,
  RefreshRetriableError,
  ReplicaNotAvailableError,
  ResourceNotFoundError,
  RetriableError,
  SaslAuthenticationError,
  SecurityDisabledError,
  SerializationError,
  ShareSessionLimitReachedError,
  ShareSessionNotFoundError,
  SnapshotNotFoundError,
  SslAuthenticationError,
  StaleBrokerEpochError,
  StaleMemberEpochError,
  StreamsInvalidTopologyEpochError,
  StreamsInvalidTopologyError,
  StreamsTopologyDescriptionUpdateFailedError,
  StreamsTopologyFencedError,
  TelemetryTooLargeError,
  ThrottlingQuotaExceededError,
  TimeoutError,
  TopicAuthorizationError,
  TopicDeletionDisabledError,
  TopicExistsError,
  TransactionAbortableError,
  TransactionAbortedError,
  TransactionCoordinatorFencedError,
  TransactionalIdAuthorizationError,
  TransactionalIdNotFoundError,
  UnacceptableCredentialError,
  UnknownControllerIdError,
  UnknownLeaderEpochError,
  UnknownMemberIdError,
  UnknownProducerIdError,
  UnknownServerError,
  UnknownSubscriptionIdError,
  UnknownTopicIdError,
  UnknownTopicOrPartitionError,
  UnreleasedInstanceIdError,
  UnstableOffsetCommitError,
  UnsupportedAssignorError,
  UnsupportedByAuthenticationError,
  UnsupportedCompressionTypeError,
  UnsupportedEndpointTypeError,
  UnsupportedForMessageFormatError,
  UnsupportedSaslMechanismError,
  UnsupportedVersionError,
  VoterNotFoundError,
  WakeupError,
};
