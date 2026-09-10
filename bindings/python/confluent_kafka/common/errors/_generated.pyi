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

# GENERATED, DO NOT EDIT (stubs for the generated error hierarchy).

from typing import ClassVar
from confluent_kafka._generated_errors import IllegalStateError
from confluent_kafka.common.errors._base import KafkaError
from confluent_kafka.common.topic_partition import TopicPartition

__all__: list[str]

class ApiError(KafkaError):
    _ffi_id: ClassVar[int]

class ApplicationRecoverableError(ApiError):
    def __init__(self, *args: object) -> None: ...

class BrokerIdNotRegisteredError(ApiError):
    _ffi_id: ClassVar[int]

class BrokerNotAvailableError(ApiError):
    _ffi_id: ClassVar[int]

class ControllerMovedError(ApiError):
    _ffi_id: ClassVar[int]

class CorrelationIdMismatchError(IllegalStateError):
    _ffi_id: ClassVar[int]
    def request_correlation_id(self) -> int: ...
    def response_correlation_id(self) -> int: ...

class DelegationTokenDisabledError(ApiError):
    _ffi_id: ClassVar[int]

class DelegationTokenExpiredError(ApiError):
    _ffi_id: ClassVar[int]

class DelegationTokenNotFoundError(ApiError):
    _ffi_id: ClassVar[int]

class DelegationTokenOwnerMismatchError(ApiError):
    _ffi_id: ClassVar[int]

class DuplicateBrokerRegistrationError(ApiError):
    _ffi_id: ClassVar[int]

class DuplicateResourceError(ApiError):
    _ffi_id: ClassVar[int]
    def resource(self) -> str | None: ...

class DuplicateSequenceError(ApiError):
    _ffi_id: ClassVar[int]

class DuplicateVoterError(ApiError):
    _ffi_id: ClassVar[int]

class FeatureUpdateFailedError(ApiError):
    _ffi_id: ClassVar[int]

class FencedInstanceIdError(ApplicationRecoverableError):
    _ffi_id: ClassVar[int]

class FencedMemberEpochError(ApiError):
    _ffi_id: ClassVar[int]

class FencedStateEpochError(ApiError):
    _ffi_id: ClassVar[int]

class GroupIdNotFoundError(ApiError):
    _ffi_id: ClassVar[int]

class GroupMaxSizeReachedError(ApiError):
    _ffi_id: ClassVar[int]

class GroupNotEmptyError(ApiError):
    _ffi_id: ClassVar[int]

class GroupSubscribedToTopicError(ApiError):
    _ffi_id: ClassVar[int]

class IllegalGenerationError(ApplicationRecoverableError):
    _ffi_id: ClassVar[int]

class InconsistentClusterIdError(ApiError):
    _ffi_id: ClassVar[int]

class InconsistentGroupProtocolError(ApiError):
    _ffi_id: ClassVar[int]

class InconsistentVoterSetError(ApiError):
    _ffi_id: ClassVar[int]

class IneligibleReplicaError(ApiError):
    _ffi_id: ClassVar[int]

class InterruptError(KafkaError):
    _ffi_id: ClassVar[int]

class InvalidCommitOffsetSizeError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidConfigurationError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidFetchSizeError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidGroupIdError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidOffsetError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidPartitionsError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidPidMappingError(ApplicationRecoverableError):
    _ffi_id: ClassVar[int]

class InvalidPrincipalTypeError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidProducerEpochError(ApplicationRecoverableError):
    _ffi_id: ClassVar[int]

class InvalidReceiveError(KafkaError):
    _ffi_id: ClassVar[int]

class InvalidRecordError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]

class InvalidRecordStateError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidRegistrationError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidRegularExpressionError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidReplicaAssignmentError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidReplicationFactorError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]

class InvalidRequestError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidRequiredAcksError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]

class InvalidSessionTimeoutError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidTimestampError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidTopicError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]
    def invalid_topics(self) -> set[str]: ...

class InvalidTxnStateError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidTxnTimeoutError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidUpdateVersionError(ApiError):
    _ffi_id: ClassVar[int]

class InvalidVoterKeyError(ApiError):
    _ffi_id: ClassVar[int]

class LogDirNotFoundError(ApiError):
    _ffi_id: ClassVar[int]

class MemberIdRequiredError(ApiError):
    _ffi_id: ClassVar[int]

class MismatchedEndpointTypeError(ApiError):
    _ffi_id: ClassVar[int]

class NewLeaderElectedError(ApiError):
    _ffi_id: ClassVar[int]

class NoReassignmentInProgressError(ApiError):
    _ffi_id: ClassVar[int]

class OffsetMetadataTooLargeError(ApiError):
    _ffi_id: ClassVar[int]

class OffsetMovedToTieredStorageError(ApiError):
    _ffi_id: ClassVar[int]

class OffsetOutOfRangeError(InvalidOffsetError):
    _ffi_id: ClassVar[int]
    def offset_out_of_range_partitions(self) -> dict[TopicPartition, int]: ...
    def partitions(self) -> set[TopicPartition]: ...

class OperationNotAttemptedError(ApiError):
    _ffi_id: ClassVar[int]

class OutOfOrderSequenceError(ApiError):
    _ffi_id: ClassVar[int]

class PolicyViolationError(ApiError):
    _ffi_id: ClassVar[int]

class PositionOutOfRangeError(ApiError):
    _ffi_id: ClassVar[int]

class PrincipalDeserializationError(ApiError):
    _ffi_id: ClassVar[int]

class ProducerFencedError(ApplicationRecoverableError):
    _ffi_id: ClassVar[int]

class QuotaViolationError(KafkaError):
    _ffi_id: ClassVar[int]
    def metric_name(self) -> str | None: ...
    def metric_group(self) -> str | None: ...
    def value(self) -> float: ...
    def bound(self) -> float: ...

class ReassignmentInProgressError(ApiError):
    _ffi_id: ClassVar[int]

class RebalanceInProgressError(ApiError):
    _ffi_id: ClassVar[int]

class RebootstrapRequiredError(ApiError):
    _ffi_id: ClassVar[int]

class RecordBatchTooLargeError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]

class RecordTooLargeError(ApiError):
    _ffi_id: ClassVar[int]
    def record_too_large_partitions(self) -> dict[TopicPartition, int] | None: ...

class ResourceNotFoundError(ApiError):
    _ffi_id: ClassVar[int]
    def resource(self) -> str | None: ...

class RetriableError(ApiError):
    def __init__(self, *args: object) -> None: ...

class SchemaError(KafkaError):
    _ffi_id: ClassVar[int]

class SecurityDisabledError(ApiError):
    _ffi_id: ClassVar[int]

class SerializationError(KafkaError):
    _ffi_id: ClassVar[int]

class ShareSessionLimitReachedError(RetriableError):
    _ffi_id: ClassVar[int]

class ShareSessionNotFoundError(RetriableError):
    _ffi_id: ClassVar[int]

class SnapshotNotFoundError(ApiError):
    _ffi_id: ClassVar[int]

class StaleBrokerEpochError(ApiError):
    _ffi_id: ClassVar[int]

class StaleMemberEpochError(ApiError):
    _ffi_id: ClassVar[int]

class StreamsInvalidTopologyEpochError(ApiError):
    _ffi_id: ClassVar[int]

class StreamsInvalidTopologyError(ApiError):
    _ffi_id: ClassVar[int]

class StreamsTopologyFencedError(ApiError):
    _ffi_id: ClassVar[int]

class TelemetryTooLargeError(ApiError):
    _ffi_id: ClassVar[int]

class ThrottlingQuotaExceededError(RetriableError):
    _ffi_id: ClassVar[int]
    def throttle_time_ms(self) -> int: ...

class TimeoutError(RetriableError):
    _ffi_id: ClassVar[int]

class TopicDeletionDisabledError(ApiError):
    _ffi_id: ClassVar[int]

class TopicExistsError(ApiError):
    _ffi_id: ClassVar[int]

class TransactionAbortableError(ApiError):
    _ffi_id: ClassVar[int]

class TransactionAbortedError(ApiError):
    _ffi_id: ClassVar[int]

class TransactionCoordinatorFencedError(ApiError):
    _ffi_id: ClassVar[int]

class TransactionalIdNotFoundError(ApiError):
    _ffi_id: ClassVar[int]

class UnacceptableCredentialError(ApiError):
    _ffi_id: ClassVar[int]

class UnknownControllerIdError(ApiError):
    _ffi_id: ClassVar[int]

class UnknownLeaderEpochError(RetriableError):
    _ffi_id: ClassVar[int]

class UnknownMemberIdError(ApplicationRecoverableError):
    _ffi_id: ClassVar[int]

class UnknownProducerIdError(OutOfOrderSequenceError):
    _ffi_id: ClassVar[int]

class UnknownServerError(ApiError):
    _ffi_id: ClassVar[int]

class UnknownSubscriptionIdError(ApiError):
    _ffi_id: ClassVar[int]

class UnreleasedInstanceIdError(ApiError):
    _ffi_id: ClassVar[int]

class UnstableOffsetCommitError(RetriableError):
    _ffi_id: ClassVar[int]

class UnsupportedAssignorError(ApiError):
    _ffi_id: ClassVar[int]

class UnsupportedByAuthenticationError(ApiError):
    _ffi_id: ClassVar[int]

class UnsupportedCompressionTypeError(ApiError):
    _ffi_id: ClassVar[int]

class UnsupportedEndpointTypeError(ApiError):
    _ffi_id: ClassVar[int]

class UnsupportedForMessageFormatError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]

class UnsupportedVersionError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]

class VoterNotFoundError(ApiError):
    _ffi_id: ClassVar[int]

class WakeupError(KafkaError):
    _ffi_id: ClassVar[int]

class AuthenticationError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]

class AuthorizationError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]

class AuthorizerNotReadyError(RetriableError):
    _ffi_id: ClassVar[int]

class BufferExhaustedError(TimeoutError):
    _ffi_id: ClassVar[int]

class ClusterAuthorizationError(AuthorizationError):
    _ffi_id: ClassVar[int]

class ConcurrentTransactionsError(RetriableError):
    _ffi_id: ClassVar[int]

class CoordinatorLoadInProgressError(RetriableError):
    _ffi_id: ClassVar[int]

class CorruptRecordError(RetriableError):
    _ffi_id: ClassVar[int]

class DelegationTokenAuthorizationError(AuthorizationError):
    _ffi_id: ClassVar[int]

class DisconnectError(RetriableError):
    _ffi_id: ClassVar[int]

class FetchSessionIdNotFoundError(RetriableError):
    _ffi_id: ClassVar[int]

class FetchSessionTopicIdError(RetriableError):
    _ffi_id: ClassVar[int]

class GroupAuthorizationError(AuthorizationError):
    _ffi_id: ClassVar[int]
    def group_id(self) -> str | None: ...

class IllegalSaslStateError(AuthenticationError):
    _ffi_id: ClassVar[int]

class InvalidFetchSessionEpochError(RetriableError):
    _ffi_id: ClassVar[int]

class InvalidShareSessionEpochError(RetriableError):
    _ffi_id: ClassVar[int]

class NotControllerError(RetriableError):
    _ffi_id: ClassVar[int]

class NotEnoughReplicasAfterAppendError(RetriableError):
    _ffi_id: ClassVar[int]

class NotEnoughReplicasError(RetriableError):
    _ffi_id: ClassVar[int]

class OffsetNotAvailableError(RetriableError):
    _ffi_id: ClassVar[int]

class RecordDeserializationError(SerializationError):
    _ffi_id: ClassVar[int]

class RefreshRetriableError(RetriableError):
    def __init__(self, *args: object) -> None: ...

class SaslAuthenticationError(AuthenticationError):
    _ffi_id: ClassVar[int]

class SslAuthenticationError(AuthenticationError):
    _ffi_id: ClassVar[int]

class TopicAuthorizationError(AuthorizationError):
    _ffi_id: ClassVar[int]
    def unauthorized_topics(self) -> set[str]: ...

class TransactionalIdAuthorizationError(AuthorizationError):
    _ffi_id: ClassVar[int]

class UnsupportedSaslMechanismError(AuthenticationError):
    _ffi_id: ClassVar[int]

class CoordinatorNotAvailableError(RefreshRetriableError):
    _ffi_id: ClassVar[int]

class InvalidMetadataError(RefreshRetriableError):
    def __init__(self, *args: object) -> None: ...

class KafkaStorageError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class LeaderNotAvailableError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class ListenerNotFoundError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class NetworkError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class NotCoordinatorError(RefreshRetriableError):
    _ffi_id: ClassVar[int]

class NotLeaderOrFollowerError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class PreferredLeaderNotAvailableError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class ReplicaNotAvailableError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class UnknownTopicIdError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class UnknownTopicOrPartitionError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class ElectionNotNeededError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class EligibleLeadersNotAvailableError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class FencedLeaderEpochError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

class InconsistentTopicIdError(InvalidMetadataError):
    _ffi_id: ClassVar[int]

