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

"""Generated Kafka error hierarchy for ``confluent_kafka.common.errors``.

GENERATED, DO NOT EDIT. Produced from the Java exception sources by
`cargo xtask generate-error-codes`, cross-checked against the FFI
`kafka_common_ErrorCode_t` enum, and validated for staleness by
`cargo xtask check-generated`.
"""

from __future__ import annotations

from typing import ClassVar

from confluent_kafka._generated_errors import IllegalStateError
from confluent_kafka.common.errors._base import KafkaError

__all__ = [
    "ApiError",
    "ApplicationRecoverableError",
    "AuthenticationError",
    "AuthorizationError",
    "AuthorizerNotReadyError",
    "BrokerIdNotRegisteredError",
    "BrokerNotAvailableError",
    "BufferExhaustedError",
    "ClusterAuthorizationError",
    "ConcurrentTransactionsError",
    "ControllerMovedError",
    "CoordinatorLoadInProgressError",
    "CoordinatorNotAvailableError",
    "CorrelationIdMismatchError",
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
    "InvalidReceiveError",
    "InvalidRecordError",
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
    "QuotaViolationError",
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
    "SchemaError",
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


class ApiError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ApiException``."""

    _ffi_id: ClassVar[int] = -6  # kafka_common_ErrorCode_API


class ApplicationRecoverableError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ApplicationRecoverableException``."""

    def __init__(self, *args: object) -> None:
        if type(self) is ApplicationRecoverableError:
            raise TypeError(
                "ApplicationRecoverableError is an abstract catch-only base; it is never raised directly"
            )
        super().__init__(*args)


class BrokerIdNotRegisteredError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.BrokerIdNotRegisteredException``."""

    _ffi_id: ClassVar[int] = 102  # kafka_common_ErrorCode_BROKER_ID_NOT_REGISTERED


class BrokerNotAvailableError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.BrokerNotAvailableException``."""

    _ffi_id: ClassVar[int] = 8  # kafka_common_ErrorCode_BROKER_NOT_AVAILABLE


class ControllerMovedError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ControllerMovedException``."""

    _ffi_id: ClassVar[int] = 11  # kafka_common_ErrorCode_STALE_CONTROLLER_EPOCH


class CorrelationIdMismatchError(IllegalStateError):
    """Mirrors Java's ``org.apache.kafka.common.requests.CorrelationIdMismatchException``."""

    _ffi_id: ClassVar[int] = -24  # kafka_common_ErrorCode_CORRELATION_ID_MISMATCH


class DelegationTokenDisabledError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DelegationTokenDisabledException``."""

    _ffi_id: ClassVar[int] = 61  # kafka_common_ErrorCode_DELEGATION_TOKEN_AUTH_DISABLED


class DelegationTokenExpiredError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DelegationTokenExpiredException``."""

    _ffi_id: ClassVar[int] = 66  # kafka_common_ErrorCode_DELEGATION_TOKEN_EXPIRED


class DelegationTokenNotFoundError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DelegationTokenNotFoundException``."""

    _ffi_id: ClassVar[int] = 62  # kafka_common_ErrorCode_DELEGATION_TOKEN_NOT_FOUND


class DelegationTokenOwnerMismatchError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException``."""

    _ffi_id: ClassVar[int] = 63  # kafka_common_ErrorCode_DELEGATION_TOKEN_OWNER_MISMATCH


class DuplicateBrokerRegistrationError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DuplicateBrokerRegistrationException``."""

    _ffi_id: ClassVar[int] = 101  # kafka_common_ErrorCode_DUPLICATE_BROKER_REGISTRATION


class DuplicateResourceError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DuplicateResourceException``."""

    _ffi_id: ClassVar[int] = 92  # kafka_common_ErrorCode_DUPLICATE_RESOURCE


class DuplicateSequenceError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DuplicateSequenceException``."""

    _ffi_id: ClassVar[int] = 46  # kafka_common_ErrorCode_DUPLICATE_SEQUENCE_NUMBER


class DuplicateVoterError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DuplicateVoterException``."""

    _ffi_id: ClassVar[int] = 126  # kafka_common_ErrorCode_DUPLICATE_VOTER


class FeatureUpdateFailedError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.FeatureUpdateFailedException``."""

    _ffi_id: ClassVar[int] = 96  # kafka_common_ErrorCode_FEATURE_UPDATE_FAILED


class FencedInstanceIdError(ApplicationRecoverableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.FencedInstanceIdException``."""

    _ffi_id: ClassVar[int] = 82  # kafka_common_ErrorCode_FENCED_INSTANCE_ID


class FencedMemberEpochError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.FencedMemberEpochException``."""

    _ffi_id: ClassVar[int] = 110  # kafka_common_ErrorCode_FENCED_MEMBER_EPOCH


class FencedStateEpochError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.FencedStateEpochException``."""

    _ffi_id: ClassVar[int] = 124  # kafka_common_ErrorCode_FENCED_STATE_EPOCH


class GroupIdNotFoundError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.GroupIdNotFoundException``."""

    _ffi_id: ClassVar[int] = 69  # kafka_common_ErrorCode_GROUP_ID_NOT_FOUND


class GroupMaxSizeReachedError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.GroupMaxSizeReachedException``."""

    _ffi_id: ClassVar[int] = 81  # kafka_common_ErrorCode_GROUP_MAX_SIZE_REACHED


class GroupNotEmptyError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.GroupNotEmptyException``."""

    _ffi_id: ClassVar[int] = 68  # kafka_common_ErrorCode_NON_EMPTY_GROUP


class GroupSubscribedToTopicError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.GroupSubscribedToTopicException``."""

    _ffi_id: ClassVar[int] = 86  # kafka_common_ErrorCode_GROUP_SUBSCRIBED_TO_TOPIC


class IllegalGenerationError(ApplicationRecoverableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.IllegalGenerationException``."""

    _ffi_id: ClassVar[int] = 22  # kafka_common_ErrorCode_ILLEGAL_GENERATION


class InconsistentClusterIdError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InconsistentClusterIdException``."""

    _ffi_id: ClassVar[int] = 104  # kafka_common_ErrorCode_INCONSISTENT_CLUSTER_ID


class InconsistentGroupProtocolError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InconsistentGroupProtocolException``."""

    _ffi_id: ClassVar[int] = 23  # kafka_common_ErrorCode_INCONSISTENT_GROUP_PROTOCOL


class InconsistentVoterSetError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InconsistentVoterSetException``."""

    _ffi_id: ClassVar[int] = 94  # kafka_common_ErrorCode_INCONSISTENT_VOTER_SET


class IneligibleReplicaError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.IneligibleReplicaException``."""

    _ffi_id: ClassVar[int] = 107  # kafka_common_ErrorCode_INELIGIBLE_REPLICA


class InterruptError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InterruptException``."""

    _ffi_id: ClassVar[int] = -12  # kafka_common_ErrorCode_INTERRUPT


class InvalidCommitOffsetSizeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidCommitOffsetSizeException``."""

    _ffi_id: ClassVar[int] = 28  # kafka_common_ErrorCode_INVALID_COMMIT_OFFSET_SIZE


class InvalidConfigurationError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidConfigurationException``."""

    _ffi_id: ClassVar[int] = 40  # kafka_common_ErrorCode_INVALID_CONFIG


class InvalidFetchSizeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidFetchSizeException``."""

    _ffi_id: ClassVar[int] = 4  # kafka_common_ErrorCode_INVALID_FETCH_SIZE


class InvalidGroupIdError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidGroupIdException``."""

    _ffi_id: ClassVar[int] = 24  # kafka_common_ErrorCode_INVALID_GROUP_ID


class InvalidOffsetError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidOffsetException``."""

    _ffi_id: ClassVar[int] = -13  # kafka_common_ErrorCode_INVALID_OFFSET


class InvalidPartitionsError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidPartitionsException``."""

    _ffi_id: ClassVar[int] = 37  # kafka_common_ErrorCode_INVALID_PARTITIONS


class InvalidPidMappingError(ApplicationRecoverableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidPidMappingException``."""

    _ffi_id: ClassVar[int] = 49  # kafka_common_ErrorCode_INVALID_PRODUCER_ID_MAPPING


class InvalidPrincipalTypeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidPrincipalTypeException``."""

    _ffi_id: ClassVar[int] = 67  # kafka_common_ErrorCode_INVALID_PRINCIPAL_TYPE


class InvalidProducerEpochError(ApplicationRecoverableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidProducerEpochException``."""

    _ffi_id: ClassVar[int] = 47  # kafka_common_ErrorCode_INVALID_PRODUCER_EPOCH


class InvalidReceiveError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.common.network.InvalidReceiveException``."""

    _ffi_id: ClassVar[int] = -25  # kafka_common_ErrorCode_INVALID_RECEIVE


class InvalidRecordError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.InvalidRecordException``."""

    _ffi_id: ClassVar[int] = 87  # kafka_common_ErrorCode_INVALID_RECORD


class InvalidRecordStateError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidRecordStateException``."""

    _ffi_id: ClassVar[int] = 121  # kafka_common_ErrorCode_INVALID_RECORD_STATE


class InvalidRegistrationError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidRegistrationException``."""

    _ffi_id: ClassVar[int] = 119  # kafka_common_ErrorCode_INVALID_REGISTRATION


class InvalidRegularExpressionError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidRegularExpression``."""

    _ffi_id: ClassVar[int] = 128  # kafka_common_ErrorCode_INVALID_REGULAR_EXPRESSION


class InvalidReplicaAssignmentError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidReplicaAssignmentException``."""

    _ffi_id: ClassVar[int] = 39  # kafka_common_ErrorCode_INVALID_REPLICA_ASSIGNMENT


class InvalidReplicationFactorError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidReplicationFactorException``."""

    _ffi_id: ClassVar[int] = 38  # kafka_common_ErrorCode_INVALID_REPLICATION_FACTOR


class InvalidRequestError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidRequestException``."""

    _ffi_id: ClassVar[int] = 42  # kafka_common_ErrorCode_INVALID_REQUEST


class InvalidRequiredAcksError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidRequiredAcksException``."""

    _ffi_id: ClassVar[int] = 21  # kafka_common_ErrorCode_INVALID_REQUIRED_ACKS


class InvalidSessionTimeoutError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidSessionTimeoutException``."""

    _ffi_id: ClassVar[int] = 26  # kafka_common_ErrorCode_INVALID_SESSION_TIMEOUT


class InvalidTimestampError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidTimestampException``."""

    _ffi_id: ClassVar[int] = 32  # kafka_common_ErrorCode_INVALID_TIMESTAMP


class InvalidTopicError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidTopicException``."""

    _ffi_id: ClassVar[int] = 17  # kafka_common_ErrorCode_INVALID_TOPIC_ERROR


class InvalidTxnStateError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidTxnStateException``."""

    _ffi_id: ClassVar[int] = 48  # kafka_common_ErrorCode_INVALID_TXN_STATE


class InvalidTxnTimeoutError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidTxnTimeoutException``."""

    _ffi_id: ClassVar[int] = 50  # kafka_common_ErrorCode_INVALID_TRANSACTION_TIMEOUT


class InvalidUpdateVersionError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidUpdateVersionException``."""

    _ffi_id: ClassVar[int] = 95  # kafka_common_ErrorCode_INVALID_UPDATE_VERSION


class InvalidVoterKeyError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidVoterKeyException``."""

    _ffi_id: ClassVar[int] = 125  # kafka_common_ErrorCode_INVALID_VOTER_KEY


class LogDirNotFoundError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.LogDirNotFoundException``."""

    _ffi_id: ClassVar[int] = 57  # kafka_common_ErrorCode_LOG_DIR_NOT_FOUND


class MemberIdRequiredError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.MemberIdRequiredException``."""

    _ffi_id: ClassVar[int] = 79  # kafka_common_ErrorCode_MEMBER_ID_REQUIRED


class MismatchedEndpointTypeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.MismatchedEndpointTypeException``."""

    _ffi_id: ClassVar[int] = 114  # kafka_common_ErrorCode_MISMATCHED_ENDPOINT_TYPE


class NewLeaderElectedError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.NewLeaderElectedException``."""

    _ffi_id: ClassVar[int] = 108  # kafka_common_ErrorCode_NEW_LEADER_ELECTED


class NoReassignmentInProgressError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.NoReassignmentInProgressException``."""

    _ffi_id: ClassVar[int] = 85  # kafka_common_ErrorCode_NO_REASSIGNMENT_IN_PROGRESS


class OffsetMetadataTooLargeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.OffsetMetadataTooLarge``."""

    _ffi_id: ClassVar[int] = 12  # kafka_common_ErrorCode_OFFSET_METADATA_TOO_LARGE


class OffsetMovedToTieredStorageError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.OffsetMovedToTieredStorageException``."""

    _ffi_id: ClassVar[int] = 109  # kafka_common_ErrorCode_OFFSET_MOVED_TO_TIERED_STORAGE


class OffsetOutOfRangeError(InvalidOffsetError):
    """Mirrors Java's ``org.apache.kafka.common.errors.OffsetOutOfRangeException``."""

    _ffi_id: ClassVar[int] = 1  # kafka_common_ErrorCode_OFFSET_OUT_OF_RANGE


class OperationNotAttemptedError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.OperationNotAttemptedException``."""

    _ffi_id: ClassVar[int] = 55  # kafka_common_ErrorCode_OPERATION_NOT_ATTEMPTED


class OutOfOrderSequenceError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.OutOfOrderSequenceException``."""

    _ffi_id: ClassVar[int] = 45  # kafka_common_ErrorCode_OUT_OF_ORDER_SEQUENCE_NUMBER


class PolicyViolationError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.PolicyViolationException``."""

    _ffi_id: ClassVar[int] = 44  # kafka_common_ErrorCode_POLICY_VIOLATION


class PositionOutOfRangeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.PositionOutOfRangeException``."""

    _ffi_id: ClassVar[int] = 99  # kafka_common_ErrorCode_POSITION_OUT_OF_RANGE


class PrincipalDeserializationError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.PrincipalDeserializationException``."""

    _ffi_id: ClassVar[int] = 97  # kafka_common_ErrorCode_PRINCIPAL_DESERIALIZATION_FAILURE


class ProducerFencedError(ApplicationRecoverableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ProducerFencedException``."""

    _ffi_id: ClassVar[int] = 90  # kafka_common_ErrorCode_PRODUCER_FENCED


class QuotaViolationError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.common.metrics.QuotaViolationException``."""

    _ffi_id: ClassVar[int] = -26  # kafka_common_ErrorCode_QUOTA_VIOLATION


class ReassignmentInProgressError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ReassignmentInProgressException``."""

    _ffi_id: ClassVar[int] = 60  # kafka_common_ErrorCode_REASSIGNMENT_IN_PROGRESS


class RebalanceInProgressError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.RebalanceInProgressException``."""

    _ffi_id: ClassVar[int] = 27  # kafka_common_ErrorCode_REBALANCE_IN_PROGRESS


class RebootstrapRequiredError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.RebootstrapRequiredException``."""

    _ffi_id: ClassVar[int] = 129  # kafka_common_ErrorCode_REBOOTSTRAP_REQUIRED


class RecordBatchTooLargeError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.RecordBatchTooLargeException``."""

    _ffi_id: ClassVar[int] = 18  # kafka_common_ErrorCode_RECORD_LIST_TOO_LARGE


class RecordTooLargeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.RecordTooLargeException``."""

    _ffi_id: ClassVar[int] = 10  # kafka_common_ErrorCode_MESSAGE_TOO_LARGE


class ResourceNotFoundError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ResourceNotFoundException``."""

    _ffi_id: ClassVar[int] = 91  # kafka_common_ErrorCode_RESOURCE_NOT_FOUND


class RetriableError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.RetriableException``."""

    def __init__(self, *args: object) -> None:
        if type(self) is RetriableError:
            raise TypeError(
                "RetriableError is an abstract catch-only base; it is never raised directly"
            )
        super().__init__(*args)


class SchemaError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.common.protocol.types.SchemaException``."""

    _ffi_id: ClassVar[int] = -14  # kafka_common_ErrorCode_SCHEMA


class SecurityDisabledError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.SecurityDisabledException``."""

    _ffi_id: ClassVar[int] = 54  # kafka_common_ErrorCode_SECURITY_DISABLED


class SerializationError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.common.errors.SerializationException``."""

    _ffi_id: ClassVar[int] = -15  # kafka_common_ErrorCode_SERIALIZATION


class ShareSessionLimitReachedError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ShareSessionLimitReachedException``."""

    _ffi_id: ClassVar[int] = 133  # kafka_common_ErrorCode_SHARE_SESSION_LIMIT_REACHED


class ShareSessionNotFoundError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ShareSessionNotFoundException``."""

    _ffi_id: ClassVar[int] = 122  # kafka_common_ErrorCode_SHARE_SESSION_NOT_FOUND


class SnapshotNotFoundError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.SnapshotNotFoundException``."""

    _ffi_id: ClassVar[int] = 98  # kafka_common_ErrorCode_SNAPSHOT_NOT_FOUND


class StaleBrokerEpochError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.StaleBrokerEpochException``."""

    _ffi_id: ClassVar[int] = 77  # kafka_common_ErrorCode_STALE_BROKER_EPOCH


class StaleMemberEpochError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.StaleMemberEpochException``."""

    _ffi_id: ClassVar[int] = 113  # kafka_common_ErrorCode_STALE_MEMBER_EPOCH


class StreamsInvalidTopologyEpochError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException``."""

    _ffi_id: ClassVar[int] = 131  # kafka_common_ErrorCode_STREAMS_INVALID_TOPOLOGY_EPOCH


class StreamsInvalidTopologyError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.StreamsInvalidTopologyException``."""

    _ffi_id: ClassVar[int] = 130  # kafka_common_ErrorCode_STREAMS_INVALID_TOPOLOGY


class StreamsTopologyFencedError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.StreamsTopologyFencedException``."""

    _ffi_id: ClassVar[int] = 132  # kafka_common_ErrorCode_STREAMS_TOPOLOGY_FENCED


class TelemetryTooLargeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TelemetryTooLargeException``."""

    _ffi_id: ClassVar[int] = 118  # kafka_common_ErrorCode_TELEMETRY_TOO_LARGE


class ThrottlingQuotaExceededError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ThrottlingQuotaExceededException``."""

    _ffi_id: ClassVar[int] = 89  # kafka_common_ErrorCode_THROTTLING_QUOTA_EXCEEDED


class TimeoutError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TimeoutException``."""

    _ffi_id: ClassVar[int] = 7  # kafka_common_ErrorCode_REQUEST_TIMED_OUT


class TopicDeletionDisabledError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TopicDeletionDisabledException``."""

    _ffi_id: ClassVar[int] = 73  # kafka_common_ErrorCode_TOPIC_DELETION_DISABLED


class TopicExistsError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TopicExistsException``."""

    _ffi_id: ClassVar[int] = 36  # kafka_common_ErrorCode_TOPIC_ALREADY_EXISTS


class TransactionAbortableError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TransactionAbortableException``."""

    _ffi_id: ClassVar[int] = 120  # kafka_common_ErrorCode_TRANSACTION_ABORTABLE


class TransactionAbortedError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TransactionAbortedException``."""

    _ffi_id: ClassVar[int] = -17  # kafka_common_ErrorCode_TRANSACTION_ABORTED


class TransactionCoordinatorFencedError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TransactionCoordinatorFencedException``."""

    _ffi_id: ClassVar[int] = 52  # kafka_common_ErrorCode_TRANSACTION_COORDINATOR_FENCED


class TransactionalIdNotFoundError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TransactionalIdNotFoundException``."""

    _ffi_id: ClassVar[int] = 105  # kafka_common_ErrorCode_TRANSACTIONAL_ID_NOT_FOUND


class UnacceptableCredentialError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnacceptableCredentialException``."""

    _ffi_id: ClassVar[int] = 93  # kafka_common_ErrorCode_UNACCEPTABLE_CREDENTIAL


class UnknownControllerIdError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnknownControllerIdException``."""

    _ffi_id: ClassVar[int] = 116  # kafka_common_ErrorCode_UNKNOWN_CONTROLLER_ID


class UnknownLeaderEpochError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnknownLeaderEpochException``."""

    _ffi_id: ClassVar[int] = 75  # kafka_common_ErrorCode_UNKNOWN_LEADER_EPOCH


class UnknownMemberIdError(ApplicationRecoverableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnknownMemberIdException``."""

    _ffi_id: ClassVar[int] = 25  # kafka_common_ErrorCode_UNKNOWN_MEMBER_ID


class UnknownProducerIdError(OutOfOrderSequenceError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnknownProducerIdException``."""

    _ffi_id: ClassVar[int] = 59  # kafka_common_ErrorCode_UNKNOWN_PRODUCER_ID


class UnknownServerError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnknownServerException``."""

    _ffi_id: ClassVar[int] = -1  # kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR


class UnknownSubscriptionIdError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnknownSubscriptionIdException``."""

    _ffi_id: ClassVar[int] = 117  # kafka_common_ErrorCode_UNKNOWN_SUBSCRIPTION_ID


class UnreleasedInstanceIdError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnreleasedInstanceIdException``."""

    _ffi_id: ClassVar[int] = 111  # kafka_common_ErrorCode_UNRELEASED_INSTANCE_ID


class UnstableOffsetCommitError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnstableOffsetCommitException``."""

    _ffi_id: ClassVar[int] = 88  # kafka_common_ErrorCode_UNSTABLE_OFFSET_COMMIT


class UnsupportedAssignorError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnsupportedAssignorException``."""

    _ffi_id: ClassVar[int] = 112  # kafka_common_ErrorCode_UNSUPPORTED_ASSIGNOR


class UnsupportedByAuthenticationError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnsupportedByAuthenticationException``."""

    _ffi_id: ClassVar[int] = 64  # kafka_common_ErrorCode_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED


class UnsupportedCompressionTypeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnsupportedCompressionTypeException``."""

    _ffi_id: ClassVar[int] = 76  # kafka_common_ErrorCode_UNSUPPORTED_COMPRESSION_TYPE


class UnsupportedEndpointTypeError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnsupportedEndpointTypeException``."""

    _ffi_id: ClassVar[int] = 115  # kafka_common_ErrorCode_UNSUPPORTED_ENDPOINT_TYPE


class UnsupportedForMessageFormatError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnsupportedForMessageFormatException``."""

    _ffi_id: ClassVar[int] = 43  # kafka_common_ErrorCode_UNSUPPORTED_FOR_MESSAGE_FORMAT


class UnsupportedVersionError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnsupportedVersionException``."""

    _ffi_id: ClassVar[int] = 35  # kafka_common_ErrorCode_UNSUPPORTED_VERSION


class VoterNotFoundError(ApiError):
    """Mirrors Java's ``org.apache.kafka.common.errors.VoterNotFoundException``."""

    _ffi_id: ClassVar[int] = 127  # kafka_common_ErrorCode_VOTER_NOT_FOUND


class WakeupError(KafkaError):
    """Mirrors Java's ``org.apache.kafka.common.errors.WakeupException``."""

    _ffi_id: ClassVar[int] = -18  # kafka_common_ErrorCode_WAKEUP


class AuthenticationError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.AuthenticationException``."""

    _ffi_id: ClassVar[int] = -7  # kafka_common_ErrorCode_AUTHENTICATION


class AuthorizationError(InvalidConfigurationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.AuthorizationException``."""

    _ffi_id: ClassVar[int] = -9  # kafka_common_ErrorCode_AUTHORIZATION


class AuthorizerNotReadyError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.AuthorizerNotReadyException``."""

    _ffi_id: ClassVar[int] = -8  # kafka_common_ErrorCode_AUTHORIZER_NOT_READY


class BufferExhaustedError(TimeoutError):
    """Mirrors Java's ``org.apache.kafka.clients.producer.BufferExhaustedException``."""

    _ffi_id: ClassVar[int] = -28  # kafka_common_ErrorCode_PRODUCER_BUFFER_EXHAUSTED


class ClusterAuthorizationError(AuthorizationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ClusterAuthorizationException``."""

    _ffi_id: ClassVar[int] = 31  # kafka_common_ErrorCode_CLUSTER_AUTHORIZATION_FAILED


class ConcurrentTransactionsError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ConcurrentTransactionsException``."""

    _ffi_id: ClassVar[int] = 51  # kafka_common_ErrorCode_CONCURRENT_TRANSACTIONS


class CoordinatorLoadInProgressError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.CoordinatorLoadInProgressException``."""

    _ffi_id: ClassVar[int] = 14  # kafka_common_ErrorCode_COORDINATOR_LOAD_IN_PROGRESS


class CorruptRecordError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.CorruptRecordException``."""

    _ffi_id: ClassVar[int] = 2  # kafka_common_ErrorCode_CORRUPT_MESSAGE


class DelegationTokenAuthorizationError(AuthorizationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DelegationTokenAuthorizationException``."""

    _ffi_id: ClassVar[int] = 65  # kafka_common_ErrorCode_DELEGATION_TOKEN_AUTHORIZATION_FAILED


class DisconnectError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.DisconnectException``."""

    _ffi_id: ClassVar[int] = -11  # kafka_common_ErrorCode_DISCONNECT


class FetchSessionIdNotFoundError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.FetchSessionIdNotFoundException``."""

    _ffi_id: ClassVar[int] = 70  # kafka_common_ErrorCode_FETCH_SESSION_ID_NOT_FOUND


class FetchSessionTopicIdError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.FetchSessionTopicIdException``."""

    _ffi_id: ClassVar[int] = 106  # kafka_common_ErrorCode_FETCH_SESSION_TOPIC_ID_ERROR


class GroupAuthorizationError(AuthorizationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.GroupAuthorizationException``."""

    _ffi_id: ClassVar[int] = 30  # kafka_common_ErrorCode_GROUP_AUTHORIZATION_FAILED


class IllegalSaslStateError(AuthenticationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.IllegalSaslStateException``."""

    _ffi_id: ClassVar[int] = 34  # kafka_common_ErrorCode_ILLEGAL_SASL_STATE


class InvalidFetchSessionEpochError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidFetchSessionEpochException``."""

    _ffi_id: ClassVar[int] = 71  # kafka_common_ErrorCode_INVALID_FETCH_SESSION_EPOCH


class InvalidShareSessionEpochError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidShareSessionEpochException``."""

    _ffi_id: ClassVar[int] = 123  # kafka_common_ErrorCode_INVALID_SHARE_SESSION_EPOCH


class NotControllerError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.NotControllerException``."""

    _ffi_id: ClassVar[int] = 41  # kafka_common_ErrorCode_NOT_CONTROLLER


class NotEnoughReplicasAfterAppendError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException``."""

    _ffi_id: ClassVar[int] = 20  # kafka_common_ErrorCode_NOT_ENOUGH_REPLICAS_AFTER_APPEND


class NotEnoughReplicasError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.NotEnoughReplicasException``."""

    _ffi_id: ClassVar[int] = 19  # kafka_common_ErrorCode_NOT_ENOUGH_REPLICAS


class OffsetNotAvailableError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.OffsetNotAvailableException``."""

    _ffi_id: ClassVar[int] = 78  # kafka_common_ErrorCode_OFFSET_NOT_AVAILABLE


class RecordDeserializationError(SerializationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.RecordDeserializationException``."""

    _ffi_id: ClassVar[int] = -27  # kafka_common_ErrorCode_RECORD_DESERIALIZATION


class RefreshRetriableError(RetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.RefreshRetriableException``."""

    def __init__(self, *args: object) -> None:
        if type(self) is RefreshRetriableError:
            raise TypeError(
                "RefreshRetriableError is an abstract catch-only base; it is never raised directly"
            )
        super().__init__(*args)


class SaslAuthenticationError(AuthenticationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.SaslAuthenticationException``."""

    _ffi_id: ClassVar[int] = 58  # kafka_common_ErrorCode_SASL_AUTHENTICATION_FAILED


class SslAuthenticationError(AuthenticationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.SslAuthenticationException``."""

    _ffi_id: ClassVar[int] = -16  # kafka_common_ErrorCode_SSL_AUTHENTICATION


class TopicAuthorizationError(AuthorizationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TopicAuthorizationException``."""

    _ffi_id: ClassVar[int] = 29  # kafka_common_ErrorCode_TOPIC_AUTHORIZATION_FAILED


class TransactionalIdAuthorizationError(AuthorizationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.TransactionalIdAuthorizationException``."""

    _ffi_id: ClassVar[int] = 53  # kafka_common_ErrorCode_TRANSACTIONAL_ID_AUTHORIZATION_FAILED


class UnsupportedSaslMechanismError(AuthenticationError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnsupportedSaslMechanismException``."""

    _ffi_id: ClassVar[int] = 33  # kafka_common_ErrorCode_UNSUPPORTED_SASL_MECHANISM


class CoordinatorNotAvailableError(RefreshRetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.CoordinatorNotAvailableException``."""

    _ffi_id: ClassVar[int] = 15  # kafka_common_ErrorCode_COORDINATOR_NOT_AVAILABLE


class InvalidMetadataError(RefreshRetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InvalidMetadataException``."""

    def __init__(self, *args: object) -> None:
        if type(self) is InvalidMetadataError:
            raise TypeError(
                "InvalidMetadataError is an abstract catch-only base; it is never raised directly"
            )
        super().__init__(*args)


class KafkaStorageError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.KafkaStorageException``."""

    _ffi_id: ClassVar[int] = 56  # kafka_common_ErrorCode_KAFKA_STORAGE_ERROR


class LeaderNotAvailableError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.LeaderNotAvailableException``."""

    _ffi_id: ClassVar[int] = 5  # kafka_common_ErrorCode_LEADER_NOT_AVAILABLE


class ListenerNotFoundError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ListenerNotFoundException``."""

    _ffi_id: ClassVar[int] = 72  # kafka_common_ErrorCode_LISTENER_NOT_FOUND


class NetworkError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.NetworkException``."""

    _ffi_id: ClassVar[int] = 13  # kafka_common_ErrorCode_NETWORK_ERROR


class NotCoordinatorError(RefreshRetriableError):
    """Mirrors Java's ``org.apache.kafka.common.errors.NotCoordinatorException``."""

    _ffi_id: ClassVar[int] = 16  # kafka_common_ErrorCode_NOT_COORDINATOR


class NotLeaderOrFollowerError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.NotLeaderOrFollowerException``."""

    _ffi_id: ClassVar[int] = 6  # kafka_common_ErrorCode_NOT_LEADER_OR_FOLLOWER


class PreferredLeaderNotAvailableError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.PreferredLeaderNotAvailableException``."""

    _ffi_id: ClassVar[int] = 80  # kafka_common_ErrorCode_PREFERRED_LEADER_NOT_AVAILABLE


class ReplicaNotAvailableError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ReplicaNotAvailableException``."""

    _ffi_id: ClassVar[int] = 9  # kafka_common_ErrorCode_REPLICA_NOT_AVAILABLE


class UnknownTopicIdError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnknownTopicIdException``."""

    _ffi_id: ClassVar[int] = 100  # kafka_common_ErrorCode_UNKNOWN_TOPIC_ID


class UnknownTopicOrPartitionError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.UnknownTopicOrPartitionException``."""

    _ffi_id: ClassVar[int] = 3  # kafka_common_ErrorCode_UNKNOWN_TOPIC_OR_PARTITION


class ElectionNotNeededError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.ElectionNotNeededException``."""

    _ffi_id: ClassVar[int] = 84  # kafka_common_ErrorCode_ELECTION_NOT_NEEDED


class EligibleLeadersNotAvailableError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.EligibleLeadersNotAvailableException``."""

    _ffi_id: ClassVar[int] = 83  # kafka_common_ErrorCode_ELIGIBLE_LEADERS_NOT_AVAILABLE


class FencedLeaderEpochError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.FencedLeaderEpochException``."""

    _ffi_id: ClassVar[int] = 74  # kafka_common_ErrorCode_FENCED_LEADER_EPOCH


class InconsistentTopicIdError(InvalidMetadataError):
    """Mirrors Java's ``org.apache.kafka.common.errors.InconsistentTopicIdException``."""

    _ffi_id: ClassVar[int] = 103  # kafka_common_ErrorCode_INCONSISTENT_TOPIC_ID
