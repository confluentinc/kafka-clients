# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# GENERATED, DO NOT EDIT (stub for the confluent_kafka.common.errors package).

from .api_error import ApiError as ApiError
from .application_recoverable_error import ApplicationRecoverableError as ApplicationRecoverableError
from .authentication_error import AuthenticationError as AuthenticationError
from .authorization_error import AuthorizationError as AuthorizationError
from .authorizer_not_ready_error import AuthorizerNotReadyError as AuthorizerNotReadyError
from .broker_id_not_registered_error import BrokerIdNotRegisteredError as BrokerIdNotRegisteredError
from .broker_not_available_error import BrokerNotAvailableError as BrokerNotAvailableError
from .cluster_authorization_error import ClusterAuthorizationError as ClusterAuthorizationError
from .concurrent_transactions_error import ConcurrentTransactionsError as ConcurrentTransactionsError
from .controller_moved_error import ControllerMovedError as ControllerMovedError
from .coordinator_load_in_progress_error import CoordinatorLoadInProgressError as CoordinatorLoadInProgressError
from .coordinator_not_available_error import CoordinatorNotAvailableError as CoordinatorNotAvailableError
from .corrupt_record_error import CorruptRecordError as CorruptRecordError
from .delegation_token_authorization_error import DelegationTokenAuthorizationError as DelegationTokenAuthorizationError
from .delegation_token_disabled_error import DelegationTokenDisabledError as DelegationTokenDisabledError
from .delegation_token_expired_error import DelegationTokenExpiredError as DelegationTokenExpiredError
from .delegation_token_not_found_error import DelegationTokenNotFoundError as DelegationTokenNotFoundError
from .delegation_token_owner_mismatch_error import DelegationTokenOwnerMismatchError as DelegationTokenOwnerMismatchError
from .disconnect_error import DisconnectError as DisconnectError
from .duplicate_broker_registration_error import DuplicateBrokerRegistrationError as DuplicateBrokerRegistrationError
from .duplicate_resource_error import DuplicateResourceError as DuplicateResourceError
from .duplicate_sequence_error import DuplicateSequenceError as DuplicateSequenceError
from .duplicate_voter_error import DuplicateVoterError as DuplicateVoterError
from .election_not_needed_error import ElectionNotNeededError as ElectionNotNeededError
from .eligible_leaders_not_available_error import EligibleLeadersNotAvailableError as EligibleLeadersNotAvailableError
from .feature_update_failed_error import FeatureUpdateFailedError as FeatureUpdateFailedError
from .fenced_instance_id_error import FencedInstanceIdError as FencedInstanceIdError
from .fenced_leader_epoch_error import FencedLeaderEpochError as FencedLeaderEpochError
from .fenced_member_epoch_error import FencedMemberEpochError as FencedMemberEpochError
from .fenced_state_epoch_error import FencedStateEpochError as FencedStateEpochError
from .fetch_session_id_not_found_error import FetchSessionIdNotFoundError as FetchSessionIdNotFoundError
from .fetch_session_topic_id_error import FetchSessionTopicIdError as FetchSessionTopicIdError
from .group_authorization_error import GroupAuthorizationError as GroupAuthorizationError
from .group_id_not_found_error import GroupIdNotFoundError as GroupIdNotFoundError
from .group_max_size_reached_error import GroupMaxSizeReachedError as GroupMaxSizeReachedError
from .group_not_empty_error import GroupNotEmptyError as GroupNotEmptyError
from .group_subscribed_to_topic_error import GroupSubscribedToTopicError as GroupSubscribedToTopicError
from .illegal_generation_error import IllegalGenerationError as IllegalGenerationError
from .illegal_sasl_state_error import IllegalSaslStateError as IllegalSaslStateError
from .inconsistent_cluster_id_error import InconsistentClusterIdError as InconsistentClusterIdError
from .inconsistent_group_protocol_error import InconsistentGroupProtocolError as InconsistentGroupProtocolError
from .inconsistent_topic_id_error import InconsistentTopicIdError as InconsistentTopicIdError
from .inconsistent_voter_set_error import InconsistentVoterSetError as InconsistentVoterSetError
from .ineligible_replica_error import IneligibleReplicaError as IneligibleReplicaError
from .interrupt_error import InterruptError as InterruptError
from .invalid_commit_offset_size_error import InvalidCommitOffsetSizeError as InvalidCommitOffsetSizeError
from .invalid_configuration_error import InvalidConfigurationError as InvalidConfigurationError
from .invalid_fetch_session_epoch_error import InvalidFetchSessionEpochError as InvalidFetchSessionEpochError
from .invalid_fetch_size_error import InvalidFetchSizeError as InvalidFetchSizeError
from .invalid_group_id_error import InvalidGroupIdError as InvalidGroupIdError
from .invalid_metadata_error import InvalidMetadataError as InvalidMetadataError
from .invalid_offset_error import InvalidOffsetError as InvalidOffsetError
from .invalid_partitions_error import InvalidPartitionsError as InvalidPartitionsError
from .invalid_pid_mapping_error import InvalidPidMappingError as InvalidPidMappingError
from .invalid_principal_type_error import InvalidPrincipalTypeError as InvalidPrincipalTypeError
from .invalid_producer_epoch_error import InvalidProducerEpochError as InvalidProducerEpochError
from .invalid_record_state_error import InvalidRecordStateError as InvalidRecordStateError
from .invalid_registration_error import InvalidRegistrationError as InvalidRegistrationError
from .invalid_regular_expression_error import InvalidRegularExpressionError as InvalidRegularExpressionError
from .invalid_replica_assignment_error import InvalidReplicaAssignmentError as InvalidReplicaAssignmentError
from .invalid_replication_factor_error import InvalidReplicationFactorError as InvalidReplicationFactorError
from .invalid_request_error import InvalidRequestError as InvalidRequestError
from .invalid_required_acks_error import InvalidRequiredAcksError as InvalidRequiredAcksError
from .invalid_session_timeout_error import InvalidSessionTimeoutError as InvalidSessionTimeoutError
from .invalid_share_session_epoch_error import InvalidShareSessionEpochError as InvalidShareSessionEpochError
from .invalid_timestamp_error import InvalidTimestampError as InvalidTimestampError
from .invalid_topic_error import InvalidTopicError as InvalidTopicError
from .invalid_txn_state_error import InvalidTxnStateError as InvalidTxnStateError
from .invalid_txn_timeout_error import InvalidTxnTimeoutError as InvalidTxnTimeoutError
from .invalid_update_version_error import InvalidUpdateVersionError as InvalidUpdateVersionError
from .invalid_voter_key_error import InvalidVoterKeyError as InvalidVoterKeyError
from .kafka_storage_error import KafkaStorageError as KafkaStorageError
from .leader_not_available_error import LeaderNotAvailableError as LeaderNotAvailableError
from .listener_not_found_error import ListenerNotFoundError as ListenerNotFoundError
from .log_dir_not_found_error import LogDirNotFoundError as LogDirNotFoundError
from .member_id_required_error import MemberIdRequiredError as MemberIdRequiredError
from .mismatched_endpoint_type_error import MismatchedEndpointTypeError as MismatchedEndpointTypeError
from .network_error import NetworkError as NetworkError
from .new_leader_elected_error import NewLeaderElectedError as NewLeaderElectedError
from .no_reassignment_in_progress_error import NoReassignmentInProgressError as NoReassignmentInProgressError
from .not_controller_error import NotControllerError as NotControllerError
from .not_coordinator_error import NotCoordinatorError as NotCoordinatorError
from .not_enough_replicas_after_append_error import NotEnoughReplicasAfterAppendError as NotEnoughReplicasAfterAppendError
from .not_enough_replicas_error import NotEnoughReplicasError as NotEnoughReplicasError
from .not_leader_or_follower_error import NotLeaderOrFollowerError as NotLeaderOrFollowerError
from .offset_metadata_too_large_error import OffsetMetadataTooLargeError as OffsetMetadataTooLargeError
from .offset_moved_to_tiered_storage_error import OffsetMovedToTieredStorageError as OffsetMovedToTieredStorageError
from .offset_not_available_error import OffsetNotAvailableError as OffsetNotAvailableError
from .offset_out_of_range_error import OffsetOutOfRangeError as OffsetOutOfRangeError
from .operation_not_attempted_error import OperationNotAttemptedError as OperationNotAttemptedError
from .out_of_order_sequence_error import OutOfOrderSequenceError as OutOfOrderSequenceError
from .policy_violation_error import PolicyViolationError as PolicyViolationError
from .position_out_of_range_error import PositionOutOfRangeError as PositionOutOfRangeError
from .preferred_leader_not_available_error import PreferredLeaderNotAvailableError as PreferredLeaderNotAvailableError
from .principal_deserialization_error import PrincipalDeserializationError as PrincipalDeserializationError
from .producer_fenced_error import ProducerFencedError as ProducerFencedError
from .reassignment_in_progress_error import ReassignmentInProgressError as ReassignmentInProgressError
from .rebalance_in_progress_error import RebalanceInProgressError as RebalanceInProgressError
from .rebootstrap_required_error import RebootstrapRequiredError as RebootstrapRequiredError
from .record_batch_too_large_error import RecordBatchTooLargeError as RecordBatchTooLargeError
from .record_deserialization_error import RecordDeserializationError as RecordDeserializationError
from .record_too_large_error import RecordTooLargeError as RecordTooLargeError
from .refresh_retriable_error import RefreshRetriableError as RefreshRetriableError
from .replica_not_available_error import ReplicaNotAvailableError as ReplicaNotAvailableError
from .resource_not_found_error import ResourceNotFoundError as ResourceNotFoundError
from .retriable_error import RetriableError as RetriableError
from .sasl_authentication_error import SaslAuthenticationError as SaslAuthenticationError
from .security_disabled_error import SecurityDisabledError as SecurityDisabledError
from .serialization_error import SerializationError as SerializationError
from .share_session_limit_reached_error import ShareSessionLimitReachedError as ShareSessionLimitReachedError
from .share_session_not_found_error import ShareSessionNotFoundError as ShareSessionNotFoundError
from .snapshot_not_found_error import SnapshotNotFoundError as SnapshotNotFoundError
from .ssl_authentication_error import SslAuthenticationError as SslAuthenticationError
from .stale_broker_epoch_error import StaleBrokerEpochError as StaleBrokerEpochError
from .stale_member_epoch_error import StaleMemberEpochError as StaleMemberEpochError
from .streams_invalid_topology_epoch_error import StreamsInvalidTopologyEpochError as StreamsInvalidTopologyEpochError
from .streams_invalid_topology_error import StreamsInvalidTopologyError as StreamsInvalidTopologyError
from .streams_topology_fenced_error import StreamsTopologyFencedError as StreamsTopologyFencedError
from .telemetry_too_large_error import TelemetryTooLargeError as TelemetryTooLargeError
from .throttling_quota_exceeded_error import ThrottlingQuotaExceededError as ThrottlingQuotaExceededError
from .timeout_error import TimeoutError as TimeoutError
from .topic_authorization_error import TopicAuthorizationError as TopicAuthorizationError
from .topic_deletion_disabled_error import TopicDeletionDisabledError as TopicDeletionDisabledError
from .topic_exists_error import TopicExistsError as TopicExistsError
from .transaction_abortable_error import TransactionAbortableError as TransactionAbortableError
from .transaction_aborted_error import TransactionAbortedError as TransactionAbortedError
from .transaction_coordinator_fenced_error import TransactionCoordinatorFencedError as TransactionCoordinatorFencedError
from .transactional_id_authorization_error import TransactionalIdAuthorizationError as TransactionalIdAuthorizationError
from .transactional_id_not_found_error import TransactionalIdNotFoundError as TransactionalIdNotFoundError
from .unacceptable_credential_error import UnacceptableCredentialError as UnacceptableCredentialError
from .unknown_controller_id_error import UnknownControllerIdError as UnknownControllerIdError
from .unknown_leader_epoch_error import UnknownLeaderEpochError as UnknownLeaderEpochError
from .unknown_member_id_error import UnknownMemberIdError as UnknownMemberIdError
from .unknown_producer_id_error import UnknownProducerIdError as UnknownProducerIdError
from .unknown_server_error import UnknownServerError as UnknownServerError
from .unknown_subscription_id_error import UnknownSubscriptionIdError as UnknownSubscriptionIdError
from .unknown_topic_id_error import UnknownTopicIdError as UnknownTopicIdError
from .unknown_topic_or_partition_error import UnknownTopicOrPartitionError as UnknownTopicOrPartitionError
from .unreleased_instance_id_error import UnreleasedInstanceIdError as UnreleasedInstanceIdError
from .unstable_offset_commit_error import UnstableOffsetCommitError as UnstableOffsetCommitError
from .unsupported_assignor_error import UnsupportedAssignorError as UnsupportedAssignorError
from .unsupported_by_authentication_error import UnsupportedByAuthenticationError as UnsupportedByAuthenticationError
from .unsupported_compression_type_error import UnsupportedCompressionTypeError as UnsupportedCompressionTypeError
from .unsupported_endpoint_type_error import UnsupportedEndpointTypeError as UnsupportedEndpointTypeError
from .unsupported_for_message_format_error import UnsupportedForMessageFormatError as UnsupportedForMessageFormatError
from .unsupported_sasl_mechanism_error import UnsupportedSaslMechanismError as UnsupportedSaslMechanismError
from .unsupported_version_error import UnsupportedVersionError as UnsupportedVersionError
from .voter_not_found_error import VoterNotFoundError as VoterNotFoundError
from .wakeup_error import WakeupError as WakeupError

__all__ = [
    "ApiError",
    "ApplicationRecoverableError",
    "AuthenticationError",
    "AuthorizationError",
    "AuthorizerNotReadyError",
    "BrokerIdNotRegisteredError",
    "BrokerNotAvailableError",
    "ClusterAuthorizationError",
    "ConcurrentTransactionsError",
    "ControllerMovedError",
    "CoordinatorLoadInProgressError",
    "CoordinatorNotAvailableError",
    "CorruptRecordError",
    "DelegationTokenAuthorizationError",
    "DelegationTokenDisabledError",
    "DelegationTokenExpiredError",
    "DelegationTokenNotFoundError",
    "DelegationTokenOwnerMismatchError",
    "DisconnectError",
    "DuplicateBrokerRegistrationError",
    "DuplicateResourceError",
    "DuplicateSequenceError",
    "DuplicateVoterError",
    "ElectionNotNeededError",
    "EligibleLeadersNotAvailableError",
    "FeatureUpdateFailedError",
    "FencedInstanceIdError",
    "FencedLeaderEpochError",
    "FencedMemberEpochError",
    "FencedStateEpochError",
    "FetchSessionIdNotFoundError",
    "FetchSessionTopicIdError",
    "GroupAuthorizationError",
    "GroupIdNotFoundError",
    "GroupMaxSizeReachedError",
    "GroupNotEmptyError",
    "GroupSubscribedToTopicError",
    "IllegalGenerationError",
    "IllegalSaslStateError",
    "InconsistentClusterIdError",
    "InconsistentGroupProtocolError",
    "InconsistentTopicIdError",
    "InconsistentVoterSetError",
    "IneligibleReplicaError",
    "InterruptError",
    "InvalidCommitOffsetSizeError",
    "InvalidConfigurationError",
    "InvalidFetchSessionEpochError",
    "InvalidFetchSizeError",
    "InvalidGroupIdError",
    "InvalidMetadataError",
    "InvalidOffsetError",
    "InvalidPartitionsError",
    "InvalidPidMappingError",
    "InvalidPrincipalTypeError",
    "InvalidProducerEpochError",
    "InvalidRecordStateError",
    "InvalidRegistrationError",
    "InvalidRegularExpressionError",
    "InvalidReplicaAssignmentError",
    "InvalidReplicationFactorError",
    "InvalidRequestError",
    "InvalidRequiredAcksError",
    "InvalidSessionTimeoutError",
    "InvalidShareSessionEpochError",
    "InvalidTimestampError",
    "InvalidTopicError",
    "InvalidTxnStateError",
    "InvalidTxnTimeoutError",
    "InvalidUpdateVersionError",
    "InvalidVoterKeyError",
    "KafkaStorageError",
    "LeaderNotAvailableError",
    "ListenerNotFoundError",
    "LogDirNotFoundError",
    "MemberIdRequiredError",
    "MismatchedEndpointTypeError",
    "NetworkError",
    "NewLeaderElectedError",
    "NoReassignmentInProgressError",
    "NotControllerError",
    "NotCoordinatorError",
    "NotEnoughReplicasAfterAppendError",
    "NotEnoughReplicasError",
    "NotLeaderOrFollowerError",
    "OffsetMetadataTooLargeError",
    "OffsetMovedToTieredStorageError",
    "OffsetNotAvailableError",
    "OffsetOutOfRangeError",
    "OperationNotAttemptedError",
    "OutOfOrderSequenceError",
    "PolicyViolationError",
    "PositionOutOfRangeError",
    "PreferredLeaderNotAvailableError",
    "PrincipalDeserializationError",
    "ProducerFencedError",
    "ReassignmentInProgressError",
    "RebalanceInProgressError",
    "RebootstrapRequiredError",
    "RecordBatchTooLargeError",
    "RecordDeserializationError",
    "RecordTooLargeError",
    "RefreshRetriableError",
    "ReplicaNotAvailableError",
    "ResourceNotFoundError",
    "RetriableError",
    "SaslAuthenticationError",
    "SecurityDisabledError",
    "SerializationError",
    "ShareSessionLimitReachedError",
    "ShareSessionNotFoundError",
    "SnapshotNotFoundError",
    "SslAuthenticationError",
    "StaleBrokerEpochError",
    "StaleMemberEpochError",
    "StreamsInvalidTopologyEpochError",
    "StreamsInvalidTopologyError",
    "StreamsTopologyFencedError",
    "TelemetryTooLargeError",
    "ThrottlingQuotaExceededError",
    "TimeoutError",
    "TopicAuthorizationError",
    "TopicDeletionDisabledError",
    "TopicExistsError",
    "TransactionAbortableError",
    "TransactionAbortedError",
    "TransactionCoordinatorFencedError",
    "TransactionalIdAuthorizationError",
    "TransactionalIdNotFoundError",
    "UnacceptableCredentialError",
    "UnknownControllerIdError",
    "UnknownLeaderEpochError",
    "UnknownMemberIdError",
    "UnknownProducerIdError",
    "UnknownServerError",
    "UnknownSubscriptionIdError",
    "UnknownTopicIdError",
    "UnknownTopicOrPartitionError",
    "UnreleasedInstanceIdError",
    "UnstableOffsetCommitError",
    "UnsupportedAssignorError",
    "UnsupportedByAuthenticationError",
    "UnsupportedCompressionTypeError",
    "UnsupportedEndpointTypeError",
    "UnsupportedForMessageFormatError",
    "UnsupportedSaslMechanismError",
    "UnsupportedVersionError",
    "VoterNotFoundError",
    "WakeupError",
]
