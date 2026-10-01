// Copyright 2025 Confluent Inc.
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

//! Error-code constants -- GENERATED, DO NOT EDIT.
//!
//! Generated from `kafka_common_ErrorCode_t` in `src/ffi/common.rs` by
//! `cargo xtask generate-error-codes`, and checked for staleness by
//! `cargo xtask check-generated`.
//!
//! The multilanguage harness decodes a proto `KafkaError` back into an
//! [`Error`](confluent_kafka::common::Error) by its code, and cannot use the
//! enum itself: `src/ffi` is behind the `ffi` feature, which the multilanguage
//! test targets do not enable.
//!
//! Values are the FFI error codes: Java's wire codes at Java's own values, plus
//! negatives for the classes only the client raises. They are injective over
//! the error classes, so the code alone identifies the class.

use std::collections::HashSet;

use confluent_kafka::common::Error;
use confluent_kafka::common::InvalidRecordError;
use confluent_kafka::common::errors::*;

pub const UNKNOWN_SERVER_ERROR: i32 = -1;
pub const NONE: i32 = 0;
pub const OFFSET_OUT_OF_RANGE: i32 = 1;
pub const CORRUPT_MESSAGE: i32 = 2;
pub const UNKNOWN_TOPIC_OR_PARTITION: i32 = 3;
pub const INVALID_FETCH_SIZE: i32 = 4;
pub const LEADER_NOT_AVAILABLE: i32 = 5;
pub const NOT_LEADER_OR_FOLLOWER: i32 = 6;
pub const REQUEST_TIMED_OUT: i32 = 7;
pub const BROKER_NOT_AVAILABLE: i32 = 8;
pub const REPLICA_NOT_AVAILABLE: i32 = 9;
pub const MESSAGE_TOO_LARGE: i32 = 10;
pub const STALE_CONTROLLER_EPOCH: i32 = 11;
pub const OFFSET_METADATA_TOO_LARGE: i32 = 12;
pub const NETWORK_ERROR: i32 = 13;
pub const COORDINATOR_LOAD_IN_PROGRESS: i32 = 14;
pub const COORDINATOR_NOT_AVAILABLE: i32 = 15;
pub const NOT_COORDINATOR: i32 = 16;
pub const INVALID_TOPIC_ERROR: i32 = 17;
pub const RECORD_LIST_TOO_LARGE: i32 = 18;
pub const NOT_ENOUGH_REPLICAS: i32 = 19;
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i32 = 20;
pub const INVALID_REQUIRED_ACKS: i32 = 21;
pub const ILLEGAL_GENERATION: i32 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i32 = 23;
pub const INVALID_GROUP_ID: i32 = 24;
pub const UNKNOWN_MEMBER_ID: i32 = 25;
pub const INVALID_SESSION_TIMEOUT: i32 = 26;
pub const REBALANCE_IN_PROGRESS: i32 = 27;
pub const INVALID_COMMIT_OFFSET_SIZE: i32 = 28;
pub const TOPIC_AUTHORIZATION_FAILED: i32 = 29;
pub const GROUP_AUTHORIZATION_FAILED: i32 = 30;
pub const CLUSTER_AUTHORIZATION_FAILED: i32 = 31;
pub const INVALID_TIMESTAMP: i32 = 32;
pub const UNSUPPORTED_SASL_MECHANISM: i32 = 33;
pub const ILLEGAL_SASL_STATE: i32 = 34;
pub const UNSUPPORTED_VERSION: i32 = 35;
pub const TOPIC_ALREADY_EXISTS: i32 = 36;
pub const INVALID_PARTITIONS: i32 = 37;
pub const INVALID_REPLICATION_FACTOR: i32 = 38;
pub const INVALID_REPLICA_ASSIGNMENT: i32 = 39;
pub const INVALID_CONFIG: i32 = 40;
pub const NOT_CONTROLLER: i32 = 41;
pub const INVALID_REQUEST: i32 = 42;
pub const UNSUPPORTED_FOR_MESSAGE_FORMAT: i32 = 43;
pub const POLICY_VIOLATION: i32 = 44;
pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i32 = 45;
pub const DUPLICATE_SEQUENCE_NUMBER: i32 = 46;
pub const INVALID_PRODUCER_EPOCH: i32 = 47;
pub const INVALID_TXN_STATE: i32 = 48;
pub const INVALID_PRODUCER_ID_MAPPING: i32 = 49;
pub const INVALID_TRANSACTION_TIMEOUT: i32 = 50;
pub const CONCURRENT_TRANSACTIONS: i32 = 51;
pub const TRANSACTION_COORDINATOR_FENCED: i32 = 52;
pub const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i32 = 53;
pub const SECURITY_DISABLED: i32 = 54;
pub const OPERATION_NOT_ATTEMPTED: i32 = 55;
pub const KAFKA_STORAGE_ERROR: i32 = 56;
pub const LOG_DIR_NOT_FOUND: i32 = 57;
pub const SASL_AUTHENTICATION_FAILED: i32 = 58;
pub const UNKNOWN_PRODUCER_ID: i32 = 59;
pub const REASSIGNMENT_IN_PROGRESS: i32 = 60;
pub const DELEGATION_TOKEN_AUTH_DISABLED: i32 = 61;
pub const DELEGATION_TOKEN_NOT_FOUND: i32 = 62;
pub const DELEGATION_TOKEN_OWNER_MISMATCH: i32 = 63;
pub const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i32 = 64;
pub const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i32 = 65;
pub const DELEGATION_TOKEN_EXPIRED: i32 = 66;
pub const INVALID_PRINCIPAL_TYPE: i32 = 67;
pub const NON_EMPTY_GROUP: i32 = 68;
pub const GROUP_ID_NOT_FOUND: i32 = 69;
pub const FETCH_SESSION_ID_NOT_FOUND: i32 = 70;
pub const INVALID_FETCH_SESSION_EPOCH: i32 = 71;
pub const LISTENER_NOT_FOUND: i32 = 72;
pub const TOPIC_DELETION_DISABLED: i32 = 73;
pub const FENCED_LEADER_EPOCH: i32 = 74;
pub const UNKNOWN_LEADER_EPOCH: i32 = 75;
pub const UNSUPPORTED_COMPRESSION_TYPE: i32 = 76;
pub const STALE_BROKER_EPOCH: i32 = 77;
pub const OFFSET_NOT_AVAILABLE: i32 = 78;
pub const MEMBER_ID_REQUIRED: i32 = 79;
pub const PREFERRED_LEADER_NOT_AVAILABLE: i32 = 80;
pub const GROUP_MAX_SIZE_REACHED: i32 = 81;
pub const FENCED_INSTANCE_ID: i32 = 82;
pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: i32 = 83;
pub const ELECTION_NOT_NEEDED: i32 = 84;
pub const NO_REASSIGNMENT_IN_PROGRESS: i32 = 85;
pub const GROUP_SUBSCRIBED_TO_TOPIC: i32 = 86;
pub const INVALID_RECORD: i32 = 87;
pub const UNSTABLE_OFFSET_COMMIT: i32 = 88;
pub const THROTTLING_QUOTA_EXCEEDED: i32 = 89;
pub const PRODUCER_FENCED: i32 = 90;
pub const RESOURCE_NOT_FOUND: i32 = 91;
pub const DUPLICATE_RESOURCE: i32 = 92;
pub const UNACCEPTABLE_CREDENTIAL: i32 = 93;
pub const INCONSISTENT_VOTER_SET: i32 = 94;
pub const INVALID_UPDATE_VERSION: i32 = 95;
pub const FEATURE_UPDATE_FAILED: i32 = 96;
pub const PRINCIPAL_DESERIALIZATION_FAILURE: i32 = 97;
pub const SNAPSHOT_NOT_FOUND: i32 = 98;
pub const POSITION_OUT_OF_RANGE: i32 = 99;
pub const UNKNOWN_TOPIC_ID: i32 = 100;
pub const DUPLICATE_BROKER_REGISTRATION: i32 = 101;
pub const BROKER_ID_NOT_REGISTERED: i32 = 102;
pub const INCONSISTENT_TOPIC_ID: i32 = 103;
pub const INCONSISTENT_CLUSTER_ID: i32 = 104;
pub const TRANSACTIONAL_ID_NOT_FOUND: i32 = 105;
pub const FETCH_SESSION_TOPIC_ID_ERROR: i32 = 106;
pub const INELIGIBLE_REPLICA: i32 = 107;
pub const NEW_LEADER_ELECTED: i32 = 108;
pub const OFFSET_MOVED_TO_TIERED_STORAGE: i32 = 109;
pub const FENCED_MEMBER_EPOCH: i32 = 110;
pub const UNRELEASED_INSTANCE_ID: i32 = 111;
pub const UNSUPPORTED_ASSIGNOR: i32 = 112;
pub const STALE_MEMBER_EPOCH: i32 = 113;
pub const MISMATCHED_ENDPOINT_TYPE: i32 = 114;
pub const UNSUPPORTED_ENDPOINT_TYPE: i32 = 115;
pub const UNKNOWN_CONTROLLER_ID: i32 = 116;
pub const UNKNOWN_SUBSCRIPTION_ID: i32 = 117;
pub const TELEMETRY_TOO_LARGE: i32 = 118;
pub const INVALID_REGISTRATION: i32 = 119;
pub const TRANSACTION_ABORTABLE: i32 = 120;
pub const INVALID_RECORD_STATE: i32 = 121;
pub const SHARE_SESSION_NOT_FOUND: i32 = 122;
pub const INVALID_SHARE_SESSION_EPOCH: i32 = 123;
pub const FENCED_STATE_EPOCH: i32 = 124;
pub const INVALID_VOTER_KEY: i32 = 125;
pub const DUPLICATE_VOTER: i32 = 126;
pub const VOTER_NOT_FOUND: i32 = 127;
pub const INVALID_REGULAR_EXPRESSION: i32 = 128;
pub const REBOOTSTRAP_REQUIRED: i32 = 129;
pub const STREAMS_INVALID_TOPOLOGY: i32 = 130;
pub const STREAMS_INVALID_TOPOLOGY_EPOCH: i32 = 131;
pub const STREAMS_TOPOLOGY_FENCED: i32 = 132;
pub const SHARE_SESSION_LIMIT_REACHED: i32 = 133;
pub const LOCAL_CONCURRENT_MODIFICATION: i32 = -2;
pub const LOCAL_ILLEGAL_ARGUMENT: i32 = -3;
pub const LOCAL_ILLEGAL_STATE: i32 = -4;
pub const LOCAL_TIMEOUT: i32 = -5;
pub const API: i32 = -6;
pub const AUTHENTICATION: i32 = -7;
pub const AUTHORIZER_NOT_READY: i32 = -8;
pub const AUTHORIZATION: i32 = -9;
pub const CONFIG: i32 = -10;
pub const DISCONNECT: i32 = -11;
pub const INTERRUPT: i32 = -12;
pub const INVALID_OFFSET: i32 = -13;
pub const SCHEMA: i32 = -14;
pub const SERIALIZATION: i32 = -15;
pub const SSL_AUTHENTICATION: i32 = -16;
pub const TRANSACTION_ABORTED: i32 = -17;
pub const WAKEUP: i32 = -18;
pub const CONSUMER_COMMIT_FAILED: i32 = -19;
pub const CONSUMER_LOG_TRUNCATION: i32 = -20;
pub const CONSUMER_NO_OFFSET_FOR_PARTITION: i32 = -21;
pub const CONSUMER_OFFSET_OUT_OF_RANGE: i32 = -22;
pub const CONSUMER_RETRIABLE_COMMIT_FAILED: i32 = -23;
pub const CORRELATION_ID_MISMATCH: i32 = -24;
pub const INVALID_RECEIVE: i32 = -25;
pub const QUOTA_VIOLATION: i32 = -26;
pub const RECORD_DESERIALIZATION: i32 = -27;
pub const PRODUCER_BUFFER_EXHAUSTED: i32 = -28;

/// Rebuild the error class that owns the protocol error `code`, carrying
/// `message` -- the table of `Errors::error_with_message` (Java's
/// `Errors.exception(String)`), spelled with the classes' public constructors
/// because `Errors` itself is not public API.
///
/// `None` for a code no broker-side class owns: `NONE`, and the negatives of
/// the classes only the client raises.
pub fn error_with_message(code: i32, message: String) -> Option<Error> {
    match code {
        BROKER_ID_NOT_REGISTERED => Some(Error::BrokerIdNotRegistered(BrokerIdNotRegisteredError::new(message))),
        BROKER_NOT_AVAILABLE => Some(Error::BrokerNotAvailable(BrokerNotAvailableError::new(message))),
        CLUSTER_AUTHORIZATION_FAILED => Some(Error::ClusterAuthorization(ClusterAuthorizationError::new(message))),
        CONCURRENT_TRANSACTIONS => Some(Error::ConcurrentTransactions(ConcurrentTransactionsError::new(message))),
        COORDINATOR_LOAD_IN_PROGRESS => {
            Some(Error::CoordinatorLoadInProgress(CoordinatorLoadInProgressError::new(message)))
        },
        COORDINATOR_NOT_AVAILABLE => Some(Error::CoordinatorNotAvailable(CoordinatorNotAvailableError::new(message))),
        CORRUPT_MESSAGE => Some(Error::CorruptRecord(CorruptRecordError::new(message))),
        DELEGATION_TOKEN_AUTHORIZATION_FAILED => Some(Error::DelegationTokenAuthorization(
            DelegationTokenAuthorizationError::new(message),
        )),
        DELEGATION_TOKEN_AUTH_DISABLED => {
            Some(Error::DelegationTokenDisabled(DelegationTokenDisabledError::new(message)))
        },
        DELEGATION_TOKEN_EXPIRED => Some(Error::DelegationTokenExpired(DelegationTokenExpiredError::new(message))),
        DELEGATION_TOKEN_NOT_FOUND => Some(Error::DelegationTokenNotFound(DelegationTokenNotFoundError::new(message))),
        DELEGATION_TOKEN_OWNER_MISMATCH => Some(Error::DelegationTokenOwnerMismatch(
            DelegationTokenOwnerMismatchError::new(message),
        )),
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED => Some(Error::UnsupportedByAuthentication(
            UnsupportedByAuthenticationError::new(message),
        )),
        DUPLICATE_BROKER_REGISTRATION => Some(Error::DuplicateBrokerRegistration(
            DuplicateBrokerRegistrationError::new(message),
        )),
        DUPLICATE_RESOURCE => Some(Error::DuplicateResource(DuplicateResourceError::new(message))),
        DUPLICATE_SEQUENCE_NUMBER => Some(Error::DuplicateSequence(DuplicateSequenceError::new(message))),
        DUPLICATE_VOTER => Some(Error::DuplicateVoter(DuplicateVoterError::new(message))),
        ELECTION_NOT_NEEDED => Some(Error::ElectionNotNeeded(ElectionNotNeededError::new(message))),
        ELIGIBLE_LEADERS_NOT_AVAILABLE => Some(Error::EligibleLeadersNotAvailable(
            EligibleLeadersNotAvailableError::new(message),
        )),
        FEATURE_UPDATE_FAILED => Some(Error::FeatureUpdateFailed(FeatureUpdateFailedError::new(message))),
        FENCED_INSTANCE_ID => Some(Error::FencedInstanceId(FencedInstanceIdError::new(message))),
        FENCED_LEADER_EPOCH => Some(Error::FencedLeaderEpoch(FencedLeaderEpochError::new(message))),
        FENCED_MEMBER_EPOCH => Some(Error::FencedMemberEpoch(FencedMemberEpochError::new(message))),
        FENCED_STATE_EPOCH => Some(Error::FencedStateEpoch(FencedStateEpochError::new(message))),
        FETCH_SESSION_ID_NOT_FOUND => Some(Error::FetchSessionIdNotFound(FetchSessionIdNotFoundError::new(message))),
        FETCH_SESSION_TOPIC_ID_ERROR => Some(Error::FetchSessionTopicId(FetchSessionTopicIdError::new(message))),
        GROUP_AUTHORIZATION_FAILED => {
            Some(Error::GroupAuthorization(GroupAuthorizationError::new(String::new(), message)))
        },
        GROUP_ID_NOT_FOUND => Some(Error::GroupIdNotFound(GroupIdNotFoundError::new(message))),
        GROUP_MAX_SIZE_REACHED => Some(Error::GroupMaxSizeReached(GroupMaxSizeReachedError::new(message))),
        GROUP_SUBSCRIBED_TO_TOPIC => Some(Error::GroupSubscribedToTopic(GroupSubscribedToTopicError::new(message))),
        ILLEGAL_GENERATION => Some(Error::IllegalGeneration(IllegalGenerationError::new(message))),
        ILLEGAL_SASL_STATE => Some(Error::IllegalSaslState(IllegalSaslStateError::new(message))),
        INCONSISTENT_CLUSTER_ID => Some(Error::InconsistentClusterId(InconsistentClusterIdError::new(message))),
        INCONSISTENT_GROUP_PROTOCOL => {
            Some(Error::InconsistentGroupProtocol(InconsistentGroupProtocolError::new(message)))
        },
        INCONSISTENT_TOPIC_ID => Some(Error::InconsistentTopicId(InconsistentTopicIdError::new(message))),
        INCONSISTENT_VOTER_SET => Some(Error::InconsistentVoterSet(InconsistentVoterSetError::new(message))),
        INELIGIBLE_REPLICA => Some(Error::IneligibleReplica(IneligibleReplicaError::new(message))),
        INVALID_COMMIT_OFFSET_SIZE => Some(Error::InvalidCommitOffsetSize(InvalidCommitOffsetSizeError::new(message))),
        INVALID_CONFIG => Some(Error::InvalidConfiguration(InvalidConfigurationError::new(message))),
        INVALID_FETCH_SESSION_EPOCH => {
            Some(Error::InvalidFetchSessionEpoch(InvalidFetchSessionEpochError::new(message)))
        },
        INVALID_FETCH_SIZE => Some(Error::InvalidFetchSize(InvalidFetchSizeError::new(message))),
        INVALID_GROUP_ID => Some(Error::InvalidGroupId(InvalidGroupIdError::new(message))),
        INVALID_PARTITIONS => Some(Error::InvalidPartitions(InvalidPartitionsError::new(message))),
        INVALID_PRINCIPAL_TYPE => Some(Error::InvalidPrincipalType(InvalidPrincipalTypeError::new(message))),
        INVALID_PRODUCER_EPOCH => Some(Error::InvalidProducerEpoch(InvalidProducerEpochError::new(message))),
        INVALID_PRODUCER_ID_MAPPING => Some(Error::InvalidPidMapping(InvalidPidMappingError::new(message))),
        INVALID_RECORD => Some(Error::InvalidRecord(InvalidRecordError::new(message))),
        INVALID_RECORD_STATE => Some(Error::InvalidRecordState(InvalidRecordStateError::new(message))),
        INVALID_REGISTRATION => Some(Error::InvalidRegistration(InvalidRegistrationError::new(message))),
        INVALID_REGULAR_EXPRESSION => Some(Error::InvalidRegularExpression(InvalidRegularExpression::new(message))),
        INVALID_REPLICATION_FACTOR => {
            Some(Error::InvalidReplicationFactor(InvalidReplicationFactorError::new(message)))
        },
        INVALID_REPLICA_ASSIGNMENT => {
            Some(Error::InvalidReplicaAssignment(InvalidReplicaAssignmentError::new(message)))
        },
        INVALID_REQUEST => Some(Error::InvalidRequest(InvalidRequestError::new(message))),
        INVALID_REQUIRED_ACKS => Some(Error::InvalidRequiredAcks(InvalidRequiredAcksError::new(message))),
        INVALID_SESSION_TIMEOUT => Some(Error::InvalidSessionTimeout(InvalidSessionTimeoutError::new(message))),
        INVALID_SHARE_SESSION_EPOCH => {
            Some(Error::InvalidShareSessionEpoch(InvalidShareSessionEpochError::new(message)))
        },
        INVALID_TIMESTAMP => Some(Error::InvalidTimestamp(InvalidTimestampError::new(message))),
        INVALID_TOPIC_ERROR => Some(Error::InvalidTopic(InvalidTopicError::with_message(HashSet::new(), message))),
        INVALID_TRANSACTION_TIMEOUT => Some(Error::InvalidTxnTimeout(InvalidTxnTimeoutError::new(message))),
        INVALID_TXN_STATE => Some(Error::InvalidTxnState(InvalidTxnStateError::new(message))),
        INVALID_UPDATE_VERSION => Some(Error::InvalidUpdateVersion(InvalidUpdateVersionError::new(message))),
        INVALID_VOTER_KEY => Some(Error::InvalidVoterKey(InvalidVoterKeyError::new(message))),
        KAFKA_STORAGE_ERROR => Some(Error::KafkaStorage(KafkaStorageError::new(message))),
        LEADER_NOT_AVAILABLE => Some(Error::LeaderNotAvailable(LeaderNotAvailableError::new(message))),
        LISTENER_NOT_FOUND => Some(Error::ListenerNotFound(ListenerNotFoundError::new(message))),
        LOG_DIR_NOT_FOUND => Some(Error::LogDirNotFound(LogDirNotFoundError::new(message))),
        MEMBER_ID_REQUIRED => Some(Error::MemberIdRequired(MemberIdRequiredError::new(message))),
        MESSAGE_TOO_LARGE => Some(Error::RecordTooLarge(RecordTooLargeError::new(message))),
        MISMATCHED_ENDPOINT_TYPE => Some(Error::MismatchedEndpointType(MismatchedEndpointTypeError::new(message))),
        NETWORK_ERROR => Some(Error::Network(NetworkError::new(message))),
        NEW_LEADER_ELECTED => Some(Error::NewLeaderElected(NewLeaderElectedError::new(message))),
        NON_EMPTY_GROUP => Some(Error::GroupNotEmpty(GroupNotEmptyError::new(message))),
        NOT_CONTROLLER => Some(Error::NotController(NotControllerError::new(message))),
        NOT_COORDINATOR => Some(Error::NotCoordinator(NotCoordinatorError::new(message))),
        NOT_ENOUGH_REPLICAS => Some(Error::NotEnoughReplicas(NotEnoughReplicasError::new(message))),
        NOT_ENOUGH_REPLICAS_AFTER_APPEND => Some(Error::NotEnoughReplicasAfterAppend(
            NotEnoughReplicasAfterAppendError::new(message),
        )),
        NOT_LEADER_OR_FOLLOWER => Some(Error::NotLeaderOrFollower(NotLeaderOrFollowerError::new(message))),
        NO_REASSIGNMENT_IN_PROGRESS => {
            Some(Error::NoReassignmentInProgress(NoReassignmentInProgressError::new(message)))
        },
        OFFSET_METADATA_TOO_LARGE => Some(Error::OffsetMetadataTooLarge(OffsetMetadataTooLarge::new(message))),
        OFFSET_MOVED_TO_TIERED_STORAGE => {
            Some(Error::OffsetMovedToTieredStorage(OffsetMovedToTieredStorageError::new(message)))
        },
        OFFSET_NOT_AVAILABLE => Some(Error::OffsetNotAvailable(OffsetNotAvailableError::new(message))),
        OFFSET_OUT_OF_RANGE => Some(Error::OffsetOutOfRange(OffsetOutOfRangeError::new(message))),
        OPERATION_NOT_ATTEMPTED => Some(Error::OperationNotAttempted(OperationNotAttemptedError::new(message))),
        OUT_OF_ORDER_SEQUENCE_NUMBER => Some(Error::OutOfOrderSequence(OutOfOrderSequenceError::new(message))),
        POLICY_VIOLATION => Some(Error::PolicyViolation(PolicyViolationError::new(message))),
        POSITION_OUT_OF_RANGE => Some(Error::PositionOutOfRange(PositionOutOfRangeError::new(message))),
        PREFERRED_LEADER_NOT_AVAILABLE => Some(Error::PreferredLeaderNotAvailable(
            PreferredLeaderNotAvailableError::new(message),
        )),
        PRINCIPAL_DESERIALIZATION_FAILURE => {
            Some(Error::PrincipalDeserialization(PrincipalDeserializationError::new(message)))
        },
        PRODUCER_FENCED => Some(Error::ProducerFenced(ProducerFencedError::new(message))),
        REASSIGNMENT_IN_PROGRESS => Some(Error::ReassignmentInProgress(ReassignmentInProgressError::new(message))),
        REBALANCE_IN_PROGRESS => Some(Error::RebalanceInProgress(RebalanceInProgressError::new(message))),
        REBOOTSTRAP_REQUIRED => Some(Error::RebootstrapRequired(RebootstrapRequiredError::new(message))),
        RECORD_LIST_TOO_LARGE => Some(Error::RecordBatchTooLarge(RecordBatchTooLargeError::new(message))),
        REPLICA_NOT_AVAILABLE => Some(Error::ReplicaNotAvailable(ReplicaNotAvailableError::new(message))),
        REQUEST_TIMED_OUT => Some(Error::Timeout(TimeoutError::new(message))),
        RESOURCE_NOT_FOUND => Some(Error::ResourceNotFound(ResourceNotFoundError::new(message))),
        SASL_AUTHENTICATION_FAILED => Some(Error::SaslAuthentication(SaslAuthenticationError::new(message))),
        SECURITY_DISABLED => Some(Error::SecurityDisabled(SecurityDisabledError::new(message))),
        SHARE_SESSION_LIMIT_REACHED => {
            Some(Error::ShareSessionLimitReached(ShareSessionLimitReachedError::new(message)))
        },
        SHARE_SESSION_NOT_FOUND => Some(Error::ShareSessionNotFound(ShareSessionNotFoundError::new(message))),
        SNAPSHOT_NOT_FOUND => Some(Error::SnapshotNotFound(SnapshotNotFoundError::new(message))),
        STALE_BROKER_EPOCH => Some(Error::StaleBrokerEpoch(StaleBrokerEpochError::new(message))),
        STALE_CONTROLLER_EPOCH => Some(Error::ControllerMoved(ControllerMovedError::new(message))),
        STALE_MEMBER_EPOCH => Some(Error::StaleMemberEpoch(StaleMemberEpochError::new(message))),
        STREAMS_INVALID_TOPOLOGY => Some(Error::StreamsInvalidTopology(StreamsInvalidTopologyError::new(message))),
        STREAMS_INVALID_TOPOLOGY_EPOCH => Some(Error::StreamsInvalidTopologyEpoch(
            StreamsInvalidTopologyEpochError::new(message),
        )),
        STREAMS_TOPOLOGY_FENCED => Some(Error::StreamsTopologyFenced(StreamsTopologyFencedError::new(message))),
        TELEMETRY_TOO_LARGE => Some(Error::TelemetryTooLarge(TelemetryTooLargeError::new(message))),
        THROTTLING_QUOTA_EXCEEDED => {
            Some(Error::ThrottlingQuotaExceeded(ThrottlingQuotaExceededError::new(0, message)))
        },
        TOPIC_ALREADY_EXISTS => Some(Error::TopicExists(TopicExistsError::new(message))),
        TOPIC_AUTHORIZATION_FAILED => Some(Error::TopicAuthorization(TopicAuthorizationError::with_message(
            HashSet::new(),
            message,
        ))),
        TOPIC_DELETION_DISABLED => Some(Error::TopicDeletionDisabled(TopicDeletionDisabledError::new(message))),
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED => Some(Error::TransactionalIdAuthorization(
            TransactionalIdAuthorizationError::new(message),
        )),
        TRANSACTIONAL_ID_NOT_FOUND => Some(Error::TransactionalIdNotFound(TransactionalIdNotFoundError::new(message))),
        TRANSACTION_ABORTABLE => Some(Error::TransactionAbortable(TransactionAbortableError::new(message))),
        TRANSACTION_COORDINATOR_FENCED => Some(Error::TransactionCoordinatorFenced(
            TransactionCoordinatorFencedError::new(message),
        )),
        UNACCEPTABLE_CREDENTIAL => Some(Error::UnacceptableCredential(UnacceptableCredentialError::new(message))),
        UNKNOWN_CONTROLLER_ID => Some(Error::UnknownControllerId(UnknownControllerIdError::new(message))),
        UNKNOWN_LEADER_EPOCH => Some(Error::UnknownLeaderEpoch(UnknownLeaderEpochError::new(message))),
        UNKNOWN_MEMBER_ID => Some(Error::UnknownMemberId(UnknownMemberIdError::new(message))),
        UNKNOWN_PRODUCER_ID => Some(Error::UnknownProducerId(UnknownProducerIdError::new(message))),
        UNKNOWN_SERVER_ERROR => Some(Error::UnknownServer(UnknownServerError::new(message))),
        UNKNOWN_SUBSCRIPTION_ID => Some(Error::UnknownSubscriptionId(UnknownSubscriptionIdError::new(message))),
        UNKNOWN_TOPIC_ID => Some(Error::UnknownTopicId(UnknownTopicIdError::new(message))),
        UNKNOWN_TOPIC_OR_PARTITION => Some(Error::UnknownTopicOrPartition(UnknownTopicOrPartitionError::new(message))),
        UNRELEASED_INSTANCE_ID => Some(Error::UnreleasedInstanceId(UnreleasedInstanceIdError::new(message))),
        UNSTABLE_OFFSET_COMMIT => Some(Error::UnstableOffsetCommit(UnstableOffsetCommitError::new(message))),
        UNSUPPORTED_ASSIGNOR => Some(Error::UnsupportedAssignor(UnsupportedAssignorError::new(message))),
        UNSUPPORTED_COMPRESSION_TYPE => {
            Some(Error::UnsupportedCompressionType(UnsupportedCompressionTypeError::new(message)))
        },
        UNSUPPORTED_ENDPOINT_TYPE => Some(Error::UnsupportedEndpointType(UnsupportedEndpointTypeError::new(message))),
        UNSUPPORTED_FOR_MESSAGE_FORMAT => Some(Error::UnsupportedForMessageFormat(
            UnsupportedForMessageFormatError::new(message),
        )),
        UNSUPPORTED_SASL_MECHANISM => {
            Some(Error::UnsupportedSaslMechanism(UnsupportedSaslMechanismError::new(message)))
        },
        UNSUPPORTED_VERSION => Some(Error::UnsupportedVersion(UnsupportedVersionError::new(message))),
        VOTER_NOT_FOUND => Some(Error::VoterNotFound(VoterNotFoundError::new(message))),
        _ => None,
    }
}
