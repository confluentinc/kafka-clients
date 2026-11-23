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
// the Java sources (the generated modules of src/common/errors/);
// `cargo xtask check-generated` checks it is up to date.

// `src/common/errors`: the generated modules of src/common/errors/.

export { ApiError } from './api_error';
export { ApplicationRecoverableError } from './application_recoverable_error';
export { AuthenticationError } from './authentication_error';
export { AuthorizationError } from './authorization_error';
export { AuthorizerNotReadyError } from './authorizer_not_ready_error';
export { BootstrapResolutionError } from './bootstrap_resolution_error';
export { BrokerIdNotRegisteredError } from './broker_id_not_registered_error';
export { BrokerNotAvailableError } from './broker_not_available_error';
export { ClusterAuthorizationError } from './cluster_authorization_error';
export { ConcurrentTransactionsError } from './concurrent_transactions_error';
export { ControllerIdNotRegisteredError } from './controller_id_not_registered_error';
export { ControllerMovedError } from './controller_moved_error';
export { CoordinatorLoadInProgressError } from './coordinator_load_in_progress_error';
export { CoordinatorNotAvailableError } from './coordinator_not_available_error';
export { CorruptRecordError } from './corrupt_record_error';
export { DelegationTokenAuthorizationError } from './delegation_token_authorization_error';
export { DelegationTokenDisabledError } from './delegation_token_disabled_error';
export { DelegationTokenExpiredError } from './delegation_token_expired_error';
export { DelegationTokenNotFoundError } from './delegation_token_not_found_error';
export { DelegationTokenOwnerMismatchError } from './delegation_token_owner_mismatch_error';
export { DisconnectError } from './disconnect_error';
export { DuplicateBrokerRegistrationError } from './duplicate_broker_registration_error';
export { DuplicateResourceError } from './duplicate_resource_error';
export { DuplicateSequenceError } from './duplicate_sequence_error';
export { DuplicateVoterError } from './duplicate_voter_error';
export { ElectionNotNeededError } from './election_not_needed_error';
export { EligibleLeadersNotAvailableError } from './eligible_leaders_not_available_error';
export { FeatureUpdateFailedError } from './feature_update_failed_error';
export { FencedInstanceIdError } from './fenced_instance_id_error';
export { FencedLeaderEpochError } from './fenced_leader_epoch_error';
export { FencedMemberEpochError } from './fenced_member_epoch_error';
export { FencedStateEpochError } from './fenced_state_epoch_error';
export { FetchSessionIdNotFoundError } from './fetch_session_id_not_found_error';
export { FetchSessionTopicIdError } from './fetch_session_topic_id_error';
export { GroupAuthorizationError } from './group_authorization_error';
export { GroupDeletionFailedError } from './group_deletion_failed_error';
export { GroupIdNotFoundError } from './group_id_not_found_error';
export { GroupMaxSizeReachedError } from './group_max_size_reached_error';
export { GroupNotEmptyError } from './group_not_empty_error';
export { GroupSubscribedToTopicError } from './group_subscribed_to_topic_error';
export { IllegalGenerationError } from './illegal_generation_error';
export { IllegalSaslStateError } from './illegal_sasl_state_error';
export { InconsistentClusterIdError } from './inconsistent_cluster_id_error';
export { InconsistentGroupProtocolError } from './inconsistent_group_protocol_error';
export { InconsistentTopicIdError } from './inconsistent_topic_id_error';
export { InconsistentVoterSetError } from './inconsistent_voter_set_error';
export { IneligibleReplicaError } from './ineligible_replica_error';
export { InterruptError } from './interrupt_error';
export { InvalidCommitOffsetSizeError } from './invalid_commit_offset_size_error';
export { InvalidConfigurationError } from './invalid_configuration_error';
export { InvalidFetchSessionEpochError } from './invalid_fetch_session_epoch_error';
export { InvalidFetchSizeError } from './invalid_fetch_size_error';
export { InvalidGroupIdError } from './invalid_group_id_error';
export { InvalidMetadataError } from './invalid_metadata_error';
export { InvalidOffsetError } from './invalid_offset_error';
export { InvalidPartitionsError } from './invalid_partitions_error';
export { InvalidPidMappingError } from './invalid_pid_mapping_error';
export { InvalidPrincipalTypeError } from './invalid_principal_type_error';
export { InvalidProducerEpochError } from './invalid_producer_epoch_error';
export { InvalidRecordStateError } from './invalid_record_state_error';
export { InvalidRegistrationError } from './invalid_registration_error';
export { InvalidRegularExpression } from './invalid_regular_expression';
export { InvalidReplicaAssignmentError } from './invalid_replica_assignment_error';
export { InvalidReplicationFactorError } from './invalid_replication_factor_error';
export { InvalidRequestError } from './invalid_request_error';
export { InvalidRequiredAcksError } from './invalid_required_acks_error';
export { InvalidSessionTimeoutError } from './invalid_session_timeout_error';
export { InvalidShareSessionEpochError } from './invalid_share_session_epoch_error';
export { InvalidTimestampError } from './invalid_timestamp_error';
export { InvalidTopicError } from './invalid_topic_error';
export { InvalidTxnStateError } from './invalid_txn_state_error';
export { InvalidTxnTimeoutError } from './invalid_txn_timeout_error';
export { InvalidUpdateVersionError } from './invalid_update_version_error';
export { InvalidVoterKeyError } from './invalid_voter_key_error';
export { KafkaStorageError } from './kafka_storage_error';
export { LeaderNotAvailableError } from './leader_not_available_error';
export { ListenerNotFoundError } from './listener_not_found_error';
export { LogDirNotFoundError } from './log_dir_not_found_error';
export { MemberIdRequiredError } from './member_id_required_error';
export { MismatchedEndpointTypeError } from './mismatched_endpoint_type_error';
export { NetworkError } from './network_error';
export { NewLeaderElectedError } from './new_leader_elected_error';
export { NoReassignmentInProgressError } from './no_reassignment_in_progress_error';
export { NotControllerError } from './not_controller_error';
export { NotCoordinatorError } from './not_coordinator_error';
export { NotEnoughReplicasAfterAppendError } from './not_enough_replicas_after_append_error';
export { NotEnoughReplicasError } from './not_enough_replicas_error';
export { NotLeaderOrFollowerError } from './not_leader_or_follower_error';
export { OffsetMetadataTooLarge } from './offset_metadata_too_large';
export { OffsetMovedToTieredStorageError } from './offset_moved_to_tiered_storage_error';
export { OffsetNotAvailableError } from './offset_not_available_error';
export { OffsetOutOfRangeError } from './offset_out_of_range_error';
export { OperationNotAttemptedError } from './operation_not_attempted_error';
export { OutOfOrderSequenceError } from './out_of_order_sequence_error';
export { PolicyViolationError } from './policy_violation_error';
export { PositionOutOfRangeError } from './position_out_of_range_error';
export { PreferredLeaderNotAvailableError } from './preferred_leader_not_available_error';
export { PrincipalDeserializationError } from './principal_deserialization_error';
export { ProducerFencedError } from './producer_fenced_error';
export { ReassignmentInProgressError } from './reassignment_in_progress_error';
export { RebalanceInProgressError } from './rebalance_in_progress_error';
export { RebootstrapRequiredError } from './rebootstrap_required_error';
export { RecordBatchTooLargeError } from './record_batch_too_large_error';
export { RecordDeserializationError } from './record_deserialization_error';
export { RecordTooLargeError } from './record_too_large_error';
export { RefreshRetriableError } from './refresh_retriable_error';
export { ReplicaNotAvailableError } from './replica_not_available_error';
export { ResourceNotFoundError } from './resource_not_found_error';
export { RetriableError } from './retriable_error';
export { SaslAuthenticationError } from './sasl_authentication_error';
export { SecurityDisabledError } from './security_disabled_error';
export { SerializationError } from './serialization_error';
export { ShareSessionLimitReachedError } from './share_session_limit_reached_error';
export { ShareSessionNotFoundError } from './share_session_not_found_error';
export { SnapshotNotFoundError } from './snapshot_not_found_error';
export { SslAuthenticationError } from './ssl_authentication_error';
export { StaleBrokerEpochError } from './stale_broker_epoch_error';
export { StaleMemberEpochError } from './stale_member_epoch_error';
export { StreamsInvalidTopologyEpochError } from './streams_invalid_topology_epoch_error';
export { StreamsInvalidTopologyError } from './streams_invalid_topology_error';
export { StreamsTopologyDescriptionUpdateFailedError } from './streams_topology_description_update_failed_error';
export { StreamsTopologyFencedError } from './streams_topology_fenced_error';
export { TelemetryTooLargeError } from './telemetry_too_large_error';
export { ThrottlingQuotaExceededError } from './throttling_quota_exceeded_error';
export { TimeoutError } from './timeout_error';
export { TopicAuthorizationError } from './topic_authorization_error';
export { TopicDeletionDisabledError } from './topic_deletion_disabled_error';
export { TopicExistsError } from './topic_exists_error';
export { TransactionAbortableError } from './transaction_abortable_error';
export { TransactionAbortedError } from './transaction_aborted_error';
export { TransactionCoordinatorFencedError } from './transaction_coordinator_fenced_error';
export { TransactionalIdAuthorizationError } from './transactional_id_authorization_error';
export { TransactionalIdNotFoundError } from './transactional_id_not_found_error';
export { UnacceptableCredentialError } from './unacceptable_credential_error';
export { UnknownControllerIdError } from './unknown_controller_id_error';
export { UnknownLeaderEpochError } from './unknown_leader_epoch_error';
export { UnknownMemberIdError } from './unknown_member_id_error';
export { UnknownProducerIdError } from './unknown_producer_id_error';
export { UnknownServerError } from './unknown_server_error';
export { UnknownSubscriptionIdError } from './unknown_subscription_id_error';
export { UnknownTopicIdError } from './unknown_topic_id_error';
export { UnknownTopicOrPartitionError } from './unknown_topic_or_partition_error';
export { UnreleasedInstanceIdError } from './unreleased_instance_id_error';
export { UnstableOffsetCommitError } from './unstable_offset_commit_error';
export { UnsupportedAssignorError } from './unsupported_assignor_error';
export { UnsupportedByAuthenticationError } from './unsupported_by_authentication_error';
export { UnsupportedCompressionTypeError } from './unsupported_compression_type_error';
export { UnsupportedEndpointTypeError } from './unsupported_endpoint_type_error';
export { UnsupportedForMessageFormatError } from './unsupported_for_message_format_error';
export { UnsupportedSaslMechanismError } from './unsupported_sasl_mechanism_error';
export { UnsupportedVersionError } from './unsupported_version_error';
export { VoterNotFoundError } from './voter_not_found_error';
export { WakeupError } from './wakeup_error';

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined`.
export declare const __esModule: true;
