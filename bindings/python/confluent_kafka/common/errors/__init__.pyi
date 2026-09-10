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

# GENERATED, DO NOT EDIT (typed re-export stub for confluent_kafka.common.errors).

from typing import Any

from ._base import KafkaError as KafkaError
from ._generated import ApiError as ApiError
from ._generated import ApplicationRecoverableError as ApplicationRecoverableError
from ._generated import AuthenticationError as AuthenticationError
from ._generated import AuthorizationError as AuthorizationError
from ._generated import AuthorizerNotReadyError as AuthorizerNotReadyError
from ._generated import BrokerIdNotRegisteredError as BrokerIdNotRegisteredError
from ._generated import BrokerNotAvailableError as BrokerNotAvailableError
from ._generated import BufferExhaustedError as BufferExhaustedError
from ._generated import ClusterAuthorizationError as ClusterAuthorizationError
from ._generated import ConcurrentTransactionsError as ConcurrentTransactionsError
from ._generated import ControllerMovedError as ControllerMovedError
from ._generated import CoordinatorLoadInProgressError as CoordinatorLoadInProgressError
from ._generated import CoordinatorNotAvailableError as CoordinatorNotAvailableError
from ._generated import CorrelationIdMismatchError as CorrelationIdMismatchError
from ._generated import CorruptRecordError as CorruptRecordError
from ._generated import DelegationTokenAuthorizationError as DelegationTokenAuthorizationError
from ._generated import DelegationTokenDisabledError as DelegationTokenDisabledError
from ._generated import DelegationTokenExpiredError as DelegationTokenExpiredError
from ._generated import DelegationTokenNotFoundError as DelegationTokenNotFoundError
from ._generated import DelegationTokenOwnerMismatchError as DelegationTokenOwnerMismatchError
from ._generated import DisconnectError as DisconnectError
from ._generated import DuplicateBrokerRegistrationError as DuplicateBrokerRegistrationError
from ._generated import DuplicateResourceError as DuplicateResourceError
from ._generated import DuplicateSequenceError as DuplicateSequenceError
from ._generated import DuplicateVoterError as DuplicateVoterError
from ._generated import ElectionNotNeededError as ElectionNotNeededError
from ._generated import EligibleLeadersNotAvailableError as EligibleLeadersNotAvailableError
from ._generated import FeatureUpdateFailedError as FeatureUpdateFailedError
from ._generated import FencedInstanceIdError as FencedInstanceIdError
from ._generated import FencedLeaderEpochError as FencedLeaderEpochError
from ._generated import FencedMemberEpochError as FencedMemberEpochError
from ._generated import FencedStateEpochError as FencedStateEpochError
from ._generated import FetchSessionIdNotFoundError as FetchSessionIdNotFoundError
from ._generated import FetchSessionTopicIdError as FetchSessionTopicIdError
from ._generated import GroupAuthorizationError as GroupAuthorizationError
from ._generated import GroupIdNotFoundError as GroupIdNotFoundError
from ._generated import GroupMaxSizeReachedError as GroupMaxSizeReachedError
from ._generated import GroupNotEmptyError as GroupNotEmptyError
from ._generated import GroupSubscribedToTopicError as GroupSubscribedToTopicError
from ._generated import IllegalGenerationError as IllegalGenerationError
from ._generated import IllegalSaslStateError as IllegalSaslStateError
from ._generated import InconsistentClusterIdError as InconsistentClusterIdError
from ._generated import InconsistentGroupProtocolError as InconsistentGroupProtocolError
from ._generated import InconsistentTopicIdError as InconsistentTopicIdError
from ._generated import InconsistentVoterSetError as InconsistentVoterSetError
from ._generated import IneligibleReplicaError as IneligibleReplicaError
from ._generated import InterruptError as InterruptError
from ._generated import InvalidCommitOffsetSizeError as InvalidCommitOffsetSizeError
from ._generated import InvalidConfigurationError as InvalidConfigurationError
from ._generated import InvalidFetchSessionEpochError as InvalidFetchSessionEpochError
from ._generated import InvalidFetchSizeError as InvalidFetchSizeError
from ._generated import InvalidGroupIdError as InvalidGroupIdError
from ._generated import InvalidMetadataError as InvalidMetadataError
from ._generated import InvalidOffsetError as InvalidOffsetError
from ._generated import InvalidPartitionsError as InvalidPartitionsError
from ._generated import InvalidPidMappingError as InvalidPidMappingError
from ._generated import InvalidPrincipalTypeError as InvalidPrincipalTypeError
from ._generated import InvalidProducerEpochError as InvalidProducerEpochError
from ._generated import InvalidReceiveError as InvalidReceiveError
from ._generated import InvalidRecordError as InvalidRecordError
from ._generated import InvalidRecordStateError as InvalidRecordStateError
from ._generated import InvalidRegistrationError as InvalidRegistrationError
from ._generated import InvalidRegularExpressionError as InvalidRegularExpressionError
from ._generated import InvalidReplicaAssignmentError as InvalidReplicaAssignmentError
from ._generated import InvalidReplicationFactorError as InvalidReplicationFactorError
from ._generated import InvalidRequestError as InvalidRequestError
from ._generated import InvalidRequiredAcksError as InvalidRequiredAcksError
from ._generated import InvalidSessionTimeoutError as InvalidSessionTimeoutError
from ._generated import InvalidShareSessionEpochError as InvalidShareSessionEpochError
from ._generated import InvalidTimestampError as InvalidTimestampError
from ._generated import InvalidTopicError as InvalidTopicError
from ._generated import InvalidTxnStateError as InvalidTxnStateError
from ._generated import InvalidTxnTimeoutError as InvalidTxnTimeoutError
from ._generated import InvalidUpdateVersionError as InvalidUpdateVersionError
from ._generated import InvalidVoterKeyError as InvalidVoterKeyError
from ._generated import KafkaStorageError as KafkaStorageError
from ._generated import LeaderNotAvailableError as LeaderNotAvailableError
from ._generated import ListenerNotFoundError as ListenerNotFoundError
from ._generated import LogDirNotFoundError as LogDirNotFoundError
from ._generated import MemberIdRequiredError as MemberIdRequiredError
from ._generated import MismatchedEndpointTypeError as MismatchedEndpointTypeError
from ._generated import NetworkError as NetworkError
from ._generated import NewLeaderElectedError as NewLeaderElectedError
from ._generated import NoReassignmentInProgressError as NoReassignmentInProgressError
from ._generated import NotControllerError as NotControllerError
from ._generated import NotCoordinatorError as NotCoordinatorError
from ._generated import NotEnoughReplicasAfterAppendError as NotEnoughReplicasAfterAppendError
from ._generated import NotEnoughReplicasError as NotEnoughReplicasError
from ._generated import NotLeaderOrFollowerError as NotLeaderOrFollowerError
from ._generated import OffsetMetadataTooLargeError as OffsetMetadataTooLargeError
from ._generated import OffsetMovedToTieredStorageError as OffsetMovedToTieredStorageError
from ._generated import OffsetNotAvailableError as OffsetNotAvailableError
from ._generated import OffsetOutOfRangeError as OffsetOutOfRangeError
from ._generated import OperationNotAttemptedError as OperationNotAttemptedError
from ._generated import OutOfOrderSequenceError as OutOfOrderSequenceError
from ._generated import PolicyViolationError as PolicyViolationError
from ._generated import PositionOutOfRangeError as PositionOutOfRangeError
from ._generated import PreferredLeaderNotAvailableError as PreferredLeaderNotAvailableError
from ._generated import PrincipalDeserializationError as PrincipalDeserializationError
from ._generated import ProducerFencedError as ProducerFencedError
from ._generated import QuotaViolationError as QuotaViolationError
from ._generated import ReassignmentInProgressError as ReassignmentInProgressError
from ._generated import RebalanceInProgressError as RebalanceInProgressError
from ._generated import RebootstrapRequiredError as RebootstrapRequiredError
from ._generated import RecordBatchTooLargeError as RecordBatchTooLargeError
from ._generated import RecordDeserializationError as RecordDeserializationError
from ._generated import RecordTooLargeError as RecordTooLargeError
from ._generated import RefreshRetriableError as RefreshRetriableError
from ._generated import ReplicaNotAvailableError as ReplicaNotAvailableError
from ._generated import ResourceNotFoundError as ResourceNotFoundError
from ._generated import RetriableError as RetriableError
from ._generated import SaslAuthenticationError as SaslAuthenticationError
from ._generated import SchemaError as SchemaError
from ._generated import SecurityDisabledError as SecurityDisabledError
from ._generated import SerializationError as SerializationError
from ._generated import ShareSessionLimitReachedError as ShareSessionLimitReachedError
from ._generated import ShareSessionNotFoundError as ShareSessionNotFoundError
from ._generated import SnapshotNotFoundError as SnapshotNotFoundError
from ._generated import SslAuthenticationError as SslAuthenticationError
from ._generated import StaleBrokerEpochError as StaleBrokerEpochError
from ._generated import StaleMemberEpochError as StaleMemberEpochError
from ._generated import StreamsInvalidTopologyEpochError as StreamsInvalidTopologyEpochError
from ._generated import StreamsInvalidTopologyError as StreamsInvalidTopologyError
from ._generated import StreamsTopologyFencedError as StreamsTopologyFencedError
from ._generated import TelemetryTooLargeError as TelemetryTooLargeError
from ._generated import ThrottlingQuotaExceededError as ThrottlingQuotaExceededError
from ._generated import TimeoutError as TimeoutError
from ._generated import TopicAuthorizationError as TopicAuthorizationError
from ._generated import TopicDeletionDisabledError as TopicDeletionDisabledError
from ._generated import TopicExistsError as TopicExistsError
from ._generated import TransactionAbortableError as TransactionAbortableError
from ._generated import TransactionAbortedError as TransactionAbortedError
from ._generated import TransactionCoordinatorFencedError as TransactionCoordinatorFencedError
from ._generated import TransactionalIdAuthorizationError as TransactionalIdAuthorizationError
from ._generated import TransactionalIdNotFoundError as TransactionalIdNotFoundError
from ._generated import UnacceptableCredentialError as UnacceptableCredentialError
from ._generated import UnknownControllerIdError as UnknownControllerIdError
from ._generated import UnknownLeaderEpochError as UnknownLeaderEpochError
from ._generated import UnknownMemberIdError as UnknownMemberIdError
from ._generated import UnknownProducerIdError as UnknownProducerIdError
from ._generated import UnknownServerError as UnknownServerError
from ._generated import UnknownSubscriptionIdError as UnknownSubscriptionIdError
from ._generated import UnknownTopicIdError as UnknownTopicIdError
from ._generated import UnknownTopicOrPartitionError as UnknownTopicOrPartitionError
from ._generated import UnreleasedInstanceIdError as UnreleasedInstanceIdError
from ._generated import UnstableOffsetCommitError as UnstableOffsetCommitError
from ._generated import UnsupportedAssignorError as UnsupportedAssignorError
from ._generated import UnsupportedByAuthenticationError as UnsupportedByAuthenticationError
from ._generated import UnsupportedCompressionTypeError as UnsupportedCompressionTypeError
from ._generated import UnsupportedEndpointTypeError as UnsupportedEndpointTypeError
from ._generated import UnsupportedForMessageFormatError as UnsupportedForMessageFormatError
from ._generated import UnsupportedSaslMechanismError as UnsupportedSaslMechanismError
from ._generated import UnsupportedVersionError as UnsupportedVersionError
from ._generated import VoterNotFoundError as VoterNotFoundError
from ._generated import WakeupError as WakeupError

def from_ffi_error(handle: int, *, cause: BaseException | None = ...) -> BaseException: ...
def to_ffi_id(error: BaseException) -> int: ...

def __getattr__(name: str) -> Any: ...

__all__ = [
    "KafkaError",
    "from_ffi_error",
    "to_ffi_id",
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
