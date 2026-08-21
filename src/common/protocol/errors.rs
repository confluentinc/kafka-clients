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

//! Kafka protocol error codes.
//!
//! This module contains all the client-server errors — those errors that must be sent from the
//! server to the client. These are thus part of the protocol. The names can be changed but the
//! error code cannot.

use std::collections::HashSet;
use std::fmt;

use crate::common::Error;
use crate::common::errors::*;
use crate::common::invalid_record_error::InvalidRecordError;

/// All Kafka protocol error codes.
///
/// Note that client library will convert an unknown error code to the non-retriable
/// `UnknownServerError` if the client library version is old and does not recognize
/// the newly-added error code.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum Errors {
    UnknownServerError = -1,
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkError = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicError = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,
    PolicyViolation = 44,
    OutOfOrderSequenceNumber = 45,
    DuplicateSequenceNumber = 46,
    InvalidProducerEpoch = 47,
    InvalidTxnState = 48,
    InvalidProducerIdMapping = 49,
    InvalidTransactionTimeout = 50,
    ConcurrentTransactions = 51,
    TransactionCoordinatorFenced = 52,
    TransactionalIdAuthorizationFailed = 53,
    SecurityDisabled = 54,
    OperationNotAttempted = 55,
    KafkaStorageError = 56,
    LogDirNotFound = 57,
    SaslAuthenticationFailed = 58,
    UnknownProducerId = 59,
    ReassignmentInProgress = 60,
    DelegationTokenAuthDisabled = 61,
    DelegationTokenNotFound = 62,
    DelegationTokenOwnerMismatch = 63,
    DelegationTokenRequestNotAllowed = 64,
    DelegationTokenAuthorizationFailed = 65,
    DelegationTokenExpired = 66,
    InvalidPrincipalType = 67,
    NonEmptyGroup = 68,
    GroupIdNotFound = 69,
    FetchSessionIdNotFound = 70,
    InvalidFetchSessionEpoch = 71,
    ListenerNotFound = 72,
    TopicDeletionDisabled = 73,
    FencedLeaderEpoch = 74,
    UnknownLeaderEpoch = 75,
    UnsupportedCompressionType = 76,
    StaleBrokerEpoch = 77,
    OffsetNotAvailable = 78,
    MemberIdRequired = 79,
    PreferredLeaderNotAvailable = 80,
    GroupMaxSizeReached = 81,
    FencedInstanceId = 82,
    EligibleLeadersNotAvailable = 83,
    ElectionNotNeeded = 84,
    NoReassignmentInProgress = 85,
    GroupSubscribedToTopic = 86,
    InvalidRecord = 87,
    UnstableOffsetCommit = 88,
    ThrottlingQuotaExceeded = 89,
    ProducerFenced = 90,
    ResourceNotFound = 91,
    DuplicateResource = 92,
    UnacceptableCredential = 93,
    InconsistentVoterSet = 94,
    InvalidUpdateVersion = 95,
    FeatureUpdateFailed = 96,
    PrincipalDeserializationFailure = 97,
    SnapshotNotFound = 98,
    PositionOutOfRange = 99,
    UnknownTopicId = 100,
    DuplicateBrokerRegistration = 101,
    BrokerIdNotRegistered = 102,
    InconsistentTopicId = 103,
    InconsistentClusterId = 104,
    TransactionalIdNotFound = 105,
    FetchSessionTopicIdError = 106,
    IneligibleReplica = 107,
    NewLeaderElected = 108,
    OffsetMovedToTieredStorage = 109,
    FencedMemberEpoch = 110,
    UnreleasedInstanceId = 111,
    UnsupportedAssignor = 112,
    StaleMemberEpoch = 113,
    MismatchedEndpointType = 114,
    UnsupportedEndpointType = 115,
    UnknownControllerId = 116,
    UnknownSubscriptionId = 117,
    TelemetryTooLarge = 118,
    InvalidRegistration = 119,
    TransactionAbortable = 120,
    InvalidRecordState = 121,
    ShareSessionNotFound = 122,
    InvalidShareSessionEpoch = 123,
    FencedStateEpoch = 124,
    InvalidVoterKey = 125,
    DuplicateVoter = 126,
    VoterNotFound = 127,
    InvalidRegularExpression = 128,
    RebootstrapRequired = 129,
    StreamsInvalidTopology = 130,
    StreamsInvalidTopologyEpoch = 131,
    StreamsTopologyFenced = 132,
    ShareSessionLimitReached = 133,
}

impl Errors {
    /// The error code for this error.
    pub fn code(&self) -> i16 {
        *self as i16
    }

    /// The name of this enum constant, e.g. `"CORRUPT_MESSAGE"`.
    ///
    /// This is the Rust equivalent of Java's `Enum.name()` on `Errors`. Java's
    /// `Errors` overrides no `toString()`, so `Enum.toString()` — which returns
    /// `name()` — is what every `"..." + error` / `String.format("%s", error)`
    /// site renders. `Sender.formatErrMsg`'s own javadoc spells the expected
    /// output: `"NETWORK_EXCEPTION. Error Message: Disconnected from node 0"`
    /// (`Sender.java:742`).
    ///
    /// It is **not** [`message`](Self::message), the long human description
    /// carried as the constant's second constructor argument, and not
    /// [`error_name`](Self::error_name), the exception class name.
    ///
    /// Two constants diverge from Java's spelling — see the comments at their
    /// arms.
    pub fn enum_name(&self) -> &'static str {
        match self {
            Self::UnknownServerError => "UNKNOWN_SERVER_ERROR",
            Self::None => "NONE",
            Self::OffsetOutOfRange => "OFFSET_OUT_OF_RANGE",
            Self::CorruptMessage => "CORRUPT_MESSAGE",
            Self::UnknownTopicOrPartition => "UNKNOWN_TOPIC_OR_PARTITION",
            Self::InvalidFetchSize => "INVALID_FETCH_SIZE",
            Self::LeaderNotAvailable => "LEADER_NOT_AVAILABLE",
            Self::NotLeaderOrFollower => "NOT_LEADER_OR_FOLLOWER",
            Self::RequestTimedOut => "REQUEST_TIMED_OUT",
            Self::BrokerNotAvailable => "BROKER_NOT_AVAILABLE",
            Self::ReplicaNotAvailable => "REPLICA_NOT_AVAILABLE",
            Self::MessageTooLarge => "MESSAGE_TOO_LARGE",
            Self::StaleControllerEpoch => "STALE_CONTROLLER_EPOCH",
            Self::OffsetMetadataTooLarge => "OFFSET_METADATA_TOO_LARGE",
            // Java's constant is `NETWORK_EXCEPTION`, which carries the word
            // CLAUDE.md §2 bans from Rust code — string literals included. The Rust
            // variant is therefore `NetworkError` and the rendered constant follows
            // it. Same divergence this file already applies to its 24 renamed error
            // descriptions: §2 wins, and the site documents it.
            Self::NetworkError => "NETWORK_ERROR",
            Self::CoordinatorLoadInProgress => "COORDINATOR_LOAD_IN_PROGRESS",
            Self::CoordinatorNotAvailable => "COORDINATOR_NOT_AVAILABLE",
            Self::NotCoordinator => "NOT_COORDINATOR",
            // Java's constant is `INVALID_TOPIC_EXCEPTION`; renamed for the same
            // CLAUDE.md §2 reason as `NETWORK_ERROR` above, so the rendered constant
            // follows the Rust variant `InvalidTopicError`.
            Self::InvalidTopicError => "INVALID_TOPIC_ERROR",
            Self::RecordListTooLarge => "RECORD_LIST_TOO_LARGE",
            Self::NotEnoughReplicas => "NOT_ENOUGH_REPLICAS",
            Self::NotEnoughReplicasAfterAppend => "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
            Self::InvalidRequiredAcks => "INVALID_REQUIRED_ACKS",
            Self::IllegalGeneration => "ILLEGAL_GENERATION",
            Self::InconsistentGroupProtocol => "INCONSISTENT_GROUP_PROTOCOL",
            Self::InvalidGroupId => "INVALID_GROUP_ID",
            Self::UnknownMemberId => "UNKNOWN_MEMBER_ID",
            Self::InvalidSessionTimeout => "INVALID_SESSION_TIMEOUT",
            Self::RebalanceInProgress => "REBALANCE_IN_PROGRESS",
            Self::InvalidCommitOffsetSize => "INVALID_COMMIT_OFFSET_SIZE",
            Self::TopicAuthorizationFailed => "TOPIC_AUTHORIZATION_FAILED",
            Self::GroupAuthorizationFailed => "GROUP_AUTHORIZATION_FAILED",
            Self::ClusterAuthorizationFailed => "CLUSTER_AUTHORIZATION_FAILED",
            Self::InvalidTimestamp => "INVALID_TIMESTAMP",
            Self::UnsupportedSaslMechanism => "UNSUPPORTED_SASL_MECHANISM",
            Self::IllegalSaslState => "ILLEGAL_SASL_STATE",
            Self::UnsupportedVersion => "UNSUPPORTED_VERSION",
            Self::TopicAlreadyExists => "TOPIC_ALREADY_EXISTS",
            Self::InvalidPartitions => "INVALID_PARTITIONS",
            Self::InvalidReplicationFactor => "INVALID_REPLICATION_FACTOR",
            Self::InvalidReplicaAssignment => "INVALID_REPLICA_ASSIGNMENT",
            Self::InvalidConfig => "INVALID_CONFIG",
            Self::NotController => "NOT_CONTROLLER",
            Self::InvalidRequest => "INVALID_REQUEST",
            Self::UnsupportedForMessageFormat => "UNSUPPORTED_FOR_MESSAGE_FORMAT",
            Self::PolicyViolation => "POLICY_VIOLATION",
            Self::OutOfOrderSequenceNumber => "OUT_OF_ORDER_SEQUENCE_NUMBER",
            Self::DuplicateSequenceNumber => "DUPLICATE_SEQUENCE_NUMBER",
            Self::InvalidProducerEpoch => "INVALID_PRODUCER_EPOCH",
            Self::InvalidTxnState => "INVALID_TXN_STATE",
            Self::InvalidProducerIdMapping => "INVALID_PRODUCER_ID_MAPPING",
            Self::InvalidTransactionTimeout => "INVALID_TRANSACTION_TIMEOUT",
            Self::ConcurrentTransactions => "CONCURRENT_TRANSACTIONS",
            Self::TransactionCoordinatorFenced => "TRANSACTION_COORDINATOR_FENCED",
            Self::TransactionalIdAuthorizationFailed => "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
            Self::SecurityDisabled => "SECURITY_DISABLED",
            Self::OperationNotAttempted => "OPERATION_NOT_ATTEMPTED",
            Self::KafkaStorageError => "KAFKA_STORAGE_ERROR",
            Self::LogDirNotFound => "LOG_DIR_NOT_FOUND",
            Self::SaslAuthenticationFailed => "SASL_AUTHENTICATION_FAILED",
            Self::UnknownProducerId => "UNKNOWN_PRODUCER_ID",
            Self::ReassignmentInProgress => "REASSIGNMENT_IN_PROGRESS",
            Self::DelegationTokenAuthDisabled => "DELEGATION_TOKEN_AUTH_DISABLED",
            Self::DelegationTokenNotFound => "DELEGATION_TOKEN_NOT_FOUND",
            Self::DelegationTokenOwnerMismatch => "DELEGATION_TOKEN_OWNER_MISMATCH",
            Self::DelegationTokenRequestNotAllowed => "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
            Self::DelegationTokenAuthorizationFailed => "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
            Self::DelegationTokenExpired => "DELEGATION_TOKEN_EXPIRED",
            Self::InvalidPrincipalType => "INVALID_PRINCIPAL_TYPE",
            Self::NonEmptyGroup => "NON_EMPTY_GROUP",
            Self::GroupIdNotFound => "GROUP_ID_NOT_FOUND",
            Self::FetchSessionIdNotFound => "FETCH_SESSION_ID_NOT_FOUND",
            Self::InvalidFetchSessionEpoch => "INVALID_FETCH_SESSION_EPOCH",
            Self::ListenerNotFound => "LISTENER_NOT_FOUND",
            Self::TopicDeletionDisabled => "TOPIC_DELETION_DISABLED",
            Self::FencedLeaderEpoch => "FENCED_LEADER_EPOCH",
            Self::UnknownLeaderEpoch => "UNKNOWN_LEADER_EPOCH",
            Self::UnsupportedCompressionType => "UNSUPPORTED_COMPRESSION_TYPE",
            Self::StaleBrokerEpoch => "STALE_BROKER_EPOCH",
            Self::OffsetNotAvailable => "OFFSET_NOT_AVAILABLE",
            Self::MemberIdRequired => "MEMBER_ID_REQUIRED",
            Self::PreferredLeaderNotAvailable => "PREFERRED_LEADER_NOT_AVAILABLE",
            Self::GroupMaxSizeReached => "GROUP_MAX_SIZE_REACHED",
            Self::FencedInstanceId => "FENCED_INSTANCE_ID",
            Self::EligibleLeadersNotAvailable => "ELIGIBLE_LEADERS_NOT_AVAILABLE",
            Self::ElectionNotNeeded => "ELECTION_NOT_NEEDED",
            Self::NoReassignmentInProgress => "NO_REASSIGNMENT_IN_PROGRESS",
            Self::GroupSubscribedToTopic => "GROUP_SUBSCRIBED_TO_TOPIC",
            Self::InvalidRecord => "INVALID_RECORD",
            Self::UnstableOffsetCommit => "UNSTABLE_OFFSET_COMMIT",
            Self::ThrottlingQuotaExceeded => "THROTTLING_QUOTA_EXCEEDED",
            Self::ProducerFenced => "PRODUCER_FENCED",
            Self::ResourceNotFound => "RESOURCE_NOT_FOUND",
            Self::DuplicateResource => "DUPLICATE_RESOURCE",
            Self::UnacceptableCredential => "UNACCEPTABLE_CREDENTIAL",
            Self::InconsistentVoterSet => "INCONSISTENT_VOTER_SET",
            Self::InvalidUpdateVersion => "INVALID_UPDATE_VERSION",
            Self::FeatureUpdateFailed => "FEATURE_UPDATE_FAILED",
            Self::PrincipalDeserializationFailure => "PRINCIPAL_DESERIALIZATION_FAILURE",
            Self::SnapshotNotFound => "SNAPSHOT_NOT_FOUND",
            Self::PositionOutOfRange => "POSITION_OUT_OF_RANGE",
            Self::UnknownTopicId => "UNKNOWN_TOPIC_ID",
            Self::DuplicateBrokerRegistration => "DUPLICATE_BROKER_REGISTRATION",
            Self::BrokerIdNotRegistered => "BROKER_ID_NOT_REGISTERED",
            Self::InconsistentTopicId => "INCONSISTENT_TOPIC_ID",
            Self::InconsistentClusterId => "INCONSISTENT_CLUSTER_ID",
            Self::TransactionalIdNotFound => "TRANSACTIONAL_ID_NOT_FOUND",
            Self::FetchSessionTopicIdError => "FETCH_SESSION_TOPIC_ID_ERROR",
            Self::IneligibleReplica => "INELIGIBLE_REPLICA",
            Self::NewLeaderElected => "NEW_LEADER_ELECTED",
            Self::OffsetMovedToTieredStorage => "OFFSET_MOVED_TO_TIERED_STORAGE",
            Self::FencedMemberEpoch => "FENCED_MEMBER_EPOCH",
            Self::UnreleasedInstanceId => "UNRELEASED_INSTANCE_ID",
            Self::UnsupportedAssignor => "UNSUPPORTED_ASSIGNOR",
            Self::StaleMemberEpoch => "STALE_MEMBER_EPOCH",
            Self::MismatchedEndpointType => "MISMATCHED_ENDPOINT_TYPE",
            Self::UnsupportedEndpointType => "UNSUPPORTED_ENDPOINT_TYPE",
            Self::UnknownControllerId => "UNKNOWN_CONTROLLER_ID",
            Self::UnknownSubscriptionId => "UNKNOWN_SUBSCRIPTION_ID",
            Self::TelemetryTooLarge => "TELEMETRY_TOO_LARGE",
            Self::InvalidRegistration => "INVALID_REGISTRATION",
            Self::TransactionAbortable => "TRANSACTION_ABORTABLE",
            Self::InvalidRecordState => "INVALID_RECORD_STATE",
            Self::ShareSessionNotFound => "SHARE_SESSION_NOT_FOUND",
            Self::InvalidShareSessionEpoch => "INVALID_SHARE_SESSION_EPOCH",
            Self::FencedStateEpoch => "FENCED_STATE_EPOCH",
            Self::InvalidVoterKey => "INVALID_VOTER_KEY",
            Self::DuplicateVoter => "DUPLICATE_VOTER",
            Self::VoterNotFound => "VOTER_NOT_FOUND",
            Self::InvalidRegularExpression => "INVALID_REGULAR_EXPRESSION",
            Self::RebootstrapRequired => "REBOOTSTRAP_REQUIRED",
            Self::StreamsInvalidTopology => "STREAMS_INVALID_TOPOLOGY",
            Self::StreamsInvalidTopologyEpoch => "STREAMS_INVALID_TOPOLOGY_EPOCH",
            Self::StreamsTopologyFenced => "STREAMS_TOPOLOGY_FENCED",
            Self::ShareSessionLimitReached => "SHARE_SESSION_LIMIT_REACHED",
        }
    }

    /// Get a friendly description of the error.
    pub fn message(&self) -> &'static str {
        match self {
            Self::UnknownServerError => "The server experienced an unexpected error when processing the request.",
            Self::None => "",
            Self::OffsetOutOfRange => {
                "The requested offset is not within the range of offsets maintained by the server."
            },
            Self::CorruptMessage => {
                "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt."
            },
            Self::UnknownTopicOrPartition => "This server does not host this topic-partition.",
            Self::InvalidFetchSize => "The requested fetch size is invalid.",
            Self::LeaderNotAvailable => {
                "There is no leader for this topic-partition as we are in the middle of a leadership election."
            },
            Self::NotLeaderOrFollower => {
                "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition."
            },
            Self::RequestTimedOut => "The request timed out.",
            Self::BrokerNotAvailable => "The broker is not available.",
            Self::ReplicaNotAvailable => {
                "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition."
            },
            Self::MessageTooLarge => {
                "The request included a message larger than the max message size the server will accept."
            },
            Self::StaleControllerEpoch => "The controller moved to another broker.",
            Self::OffsetMetadataTooLarge => "The metadata field of the offset request was too large.",
            Self::NetworkError => "The server disconnected before a response was received.",
            Self::CoordinatorLoadInProgress => "The coordinator is loading and hence can't process requests.",
            Self::CoordinatorNotAvailable => "The coordinator is not available.",
            Self::NotCoordinator => "This is not the correct coordinator.",
            Self::InvalidTopicError => "The request attempted to perform an operation on an invalid topic.",
            Self::RecordListTooLarge => {
                "The request included message batch larger than the configured segment size on the server."
            },
            Self::NotEnoughReplicas => "Messages are rejected since there are fewer in-sync replicas than required.",
            Self::NotEnoughReplicasAfterAppend => {
                "Messages are written to the log, but to fewer in-sync replicas than required."
            },
            Self::InvalidRequiredAcks => "Produce request specified an invalid value for required acks.",
            Self::IllegalGeneration => "Specified group generation id is not valid.",
            Self::InconsistentGroupProtocol => {
                "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list."
            },
            Self::InvalidGroupId => "The group id is invalid.",
            Self::UnknownMemberId => "The coordinator is not aware of this member.",
            Self::InvalidSessionTimeout => {
                "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."
            },
            Self::RebalanceInProgress => "The group is rebalancing, so a rejoin is needed.",
            Self::InvalidCommitOffsetSize => "The committing offset data size is not valid.",
            Self::TopicAuthorizationFailed => "Topic authorization failed.",
            Self::GroupAuthorizationFailed => "Group authorization failed.",
            Self::ClusterAuthorizationFailed => "Cluster authorization failed.",
            Self::InvalidTimestamp => "The timestamp of the message is out of acceptable range.",
            Self::UnsupportedSaslMechanism => "The broker does not support the requested SASL mechanism.",
            Self::IllegalSaslState => "Request is not valid given the current SASL state.",
            Self::UnsupportedVersion => "The version of API is not supported.",
            Self::TopicAlreadyExists => "Topic with this name already exists.",
            Self::InvalidPartitions => "Number of partitions is below 1.",
            Self::InvalidReplicationFactor => {
                "Replication factor is below 1 or larger than the number of available brokers."
            },
            Self::InvalidReplicaAssignment => "Replica assignment is invalid.",
            Self::InvalidConfig => "Configuration is invalid.",
            Self::NotController => "This is not the correct controller for this cluster.",
            Self::InvalidRequest => {
                "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."
            },
            Self::UnsupportedForMessageFormat => {
                "The message format version on the broker does not support the request."
            },
            Self::PolicyViolation => "Request parameters do not satisfy the configured policy.",
            Self::OutOfOrderSequenceNumber => "The broker received an out of order sequence number.",
            Self::DuplicateSequenceNumber => "The broker received a duplicate sequence number.",
            Self::InvalidProducerEpoch => "Producer attempted to produce with an old epoch.",
            Self::InvalidTxnState => "The producer attempted a transactional operation in an invalid state.",
            Self::InvalidProducerIdMapping => {
                "The producer attempted to use a producer id which is not currently assigned to its transactional id."
            },
            Self::InvalidTransactionTimeout => {
                "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms)."
            },
            Self::ConcurrentTransactions => {
                "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing."
            },
            Self::TransactionCoordinatorFenced => {
                "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer."
            },
            Self::TransactionalIdAuthorizationFailed => "Transactional Id authorization failed.",
            Self::SecurityDisabled => "Security features are disabled.",
            Self::OperationNotAttempted => {
                "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest."
            },
            Self::KafkaStorageError => "Disk error when trying to access log file on the disk.",
            Self::LogDirNotFound => "The user-specified log directory is not found in the broker config.",
            Self::SaslAuthenticationFailed => "SASL Authentication failed.",
            // Java (`Errors.java:310-314`): "This exception is raised by the broker
            // if it could not locate the producer metadata associated with the
            // producerId in question. ... future appends by the producer will
            // return this exception." CLAUDE.md §2 bans the word from Rust code,
            // so both occurrences read "error" here.
            Self::UnknownProducerId => {
                "This error is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this error."
            },
            Self::ReassignmentInProgress => "A partition reassignment is in progress.",
            Self::DelegationTokenAuthDisabled => "Delegation Token feature is not enabled.",
            Self::DelegationTokenNotFound => "Delegation Token is not found on server.",
            Self::DelegationTokenOwnerMismatch => "Specified Principal is not valid Owner/Renewer.",
            Self::DelegationTokenRequestNotAllowed => {
                "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels."
            },
            Self::DelegationTokenAuthorizationFailed => "Delegation Token authorization failed.",
            Self::DelegationTokenExpired => "Delegation Token is expired.",
            Self::InvalidPrincipalType => "Supplied principalType is not supported.",
            Self::NonEmptyGroup => "The group is not empty.",
            Self::GroupIdNotFound => "The group id does not exist.",
            Self::FetchSessionIdNotFound => "The fetch session ID was not found.",
            Self::InvalidFetchSessionEpoch => "The fetch session epoch is invalid.",
            Self::ListenerNotFound => {
                "There is no listener on the leader broker that matches the listener on which metadata request was processed."
            },
            Self::TopicDeletionDisabled => "Topic deletion is disabled.",
            Self::FencedLeaderEpoch => "The leader epoch in the request is older than the epoch on the broker.",
            Self::UnknownLeaderEpoch => "The leader epoch in the request is newer than the epoch on the broker.",
            Self::UnsupportedCompressionType => {
                "The requesting client does not support the compression type of given partition."
            },
            Self::StaleBrokerEpoch => "Broker epoch has changed.",
            Self::OffsetNotAvailable => {
                "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing."
            },
            Self::MemberIdRequired => {
                "The group member needs to have a valid member id before actually entering a consumer group."
            },
            Self::PreferredLeaderNotAvailable => "The preferred leader was not available.",
            Self::GroupMaxSizeReached => "The group has reached its maximum size.",
            Self::FencedInstanceId => {
                "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id."
            },
            Self::EligibleLeadersNotAvailable => "Eligible topic partition leaders are not available.",
            Self::ElectionNotNeeded => "Leader election not needed for topic partition.",
            Self::NoReassignmentInProgress => "No partition reassignment is in progress.",
            Self::GroupSubscribedToTopic => {
                "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it."
            },
            Self::InvalidRecord => "This record has failed the validation on broker and hence will be rejected.",
            Self::UnstableOffsetCommit => "There are unstable offsets that need to be cleared.",
            Self::ThrottlingQuotaExceeded => "The throttling quota has been exceeded.",
            Self::ProducerFenced => {
                "There is a newer producer with the same transactionalId which fences the current one."
            },
            Self::ResourceNotFound => "A request illegally referred to a resource that does not exist.",
            Self::DuplicateResource => "A request illegally referred to the same resource twice.",
            Self::UnacceptableCredential => "Requested credential would not meet criteria for acceptability.",
            Self::InconsistentVoterSet => {
                "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters."
            },
            Self::InvalidUpdateVersion => "The given update version was invalid.",
            Self::FeatureUpdateFailed => "Unable to update finalized features due to an unexpected server error.",
            Self::PrincipalDeserializationFailure => {
                "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup."
            },
            Self::SnapshotNotFound => "Requested snapshot was not found.",
            Self::PositionOutOfRange => {
                "Requested position is not greater than or equal to zero, and less than the size of the snapshot."
            },
            Self::UnknownTopicId => "This server does not host this topic ID.",
            Self::DuplicateBrokerRegistration => "This broker ID is already in use.",
            Self::BrokerIdNotRegistered => "The given broker ID was not registered.",
            Self::InconsistentTopicId => "The log's topic ID did not match the topic ID in the request.",
            Self::InconsistentClusterId => "The clusterId in the request does not match that found on the server.",
            Self::TransactionalIdNotFound => "The transactionalId could not be found.",
            Self::FetchSessionTopicIdError => "The fetch session encountered inconsistent topic ID usage.",
            Self::IneligibleReplica => "The new ISR contains at least one ineligible replica.",
            Self::NewLeaderElected => {
                "The AlterPartition request successfully updated the partition state but the leader has changed."
            },
            Self::OffsetMovedToTieredStorage => "The requested offset is moved to tiered storage.",
            Self::FencedMemberEpoch => {
                "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin."
            },
            Self::UnreleasedInstanceId => {
                "The instance ID is still used by another member in the consumer group. That member must leave first."
            },
            Self::UnsupportedAssignor => "The assignor or its version range is not supported by the consumer group.",
            Self::StaleMemberEpoch => {
                "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API."
            },
            Self::MismatchedEndpointType => "The request was sent to an endpoint of the wrong type.",
            Self::UnsupportedEndpointType => "This endpoint type is not supported yet.",
            Self::UnknownControllerId => "This controller ID is not known.",
            Self::UnknownSubscriptionId => {
                "Client sent a push telemetry request with an invalid or outdated subscription ID."
            },
            Self::TelemetryTooLarge => {
                "Client sent a push telemetry request larger than the maximum size the broker will accept."
            },
            Self::InvalidRegistration => "The controller has considered the broker registration to be invalid.",
            Self::TransactionAbortable => {
                "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID."
            },
            Self::InvalidRecordState => {
                "The record state is invalid. The acknowledgement of delivery could not be completed."
            },
            Self::ShareSessionNotFound => "The share session was not found.",
            Self::InvalidShareSessionEpoch => "The share session epoch is invalid.",
            Self::FencedStateEpoch => {
                "The share coordinator rejected the request because the share-group state epoch did not match."
            },
            Self::InvalidVoterKey => "The voter key doesn't match the receiving replica's key.",
            Self::DuplicateVoter => "The voter is already part of the set of voters.",
            Self::VoterNotFound => "The voter is not part of the set of voters.",
            Self::InvalidRegularExpression => "The regular expression is not valid.",
            Self::RebootstrapRequired => {
                "Client metadata is stale. The client should rebootstrap to obtain new metadata."
            },
            Self::StreamsInvalidTopology => "The supplied topology is invalid.",
            Self::StreamsInvalidTopologyEpoch => "The supplied topology epoch is invalid.",
            Self::StreamsTopologyFenced => "The supplied topology epoch is outdated.",
            Self::ShareSessionLimitReached => "The limit of share sessions has been reached.",
        }
    }

    /// The exception class this code names, with its default message.
    ///
    /// Translates Java's `Errors.exception()` (`protocol/Errors.java:452`). Java
    /// stores a `Function<String, ApiException>` per constant and applies it to
    /// the constant's message literal; this match is that per-constant factory,
    /// and the literals stay in [`message`](Self::message) exactly as Java keeps
    /// them on the enum.
    ///
    /// `None` for [`Errors::None`], which Java declares as
    /// `NONE(0, null, message -> null)`.
    pub fn error(&self) -> Option<Error> {
        match self {
            Self::None => None,
            Self::BrokerIdNotRegistered => {
                Some(Error::BrokerIdNotRegistered(BrokerIdNotRegisteredError::with_default_message()))
            },
            Self::BrokerNotAvailable => {
                Some(Error::BrokerNotAvailable(BrokerNotAvailableError::with_default_message()))
            },
            Self::ClusterAuthorizationFailed => {
                Some(Error::ClusterAuthorization(ClusterAuthorizationError::with_default_message()))
            },
            Self::ConcurrentTransactions => Some(Error::ConcurrentTransactions(
                ConcurrentTransactionsError::with_default_message(),
            )),
            Self::CoordinatorLoadInProgress => Some(Error::CoordinatorLoadInProgress(
                CoordinatorLoadInProgressError::with_default_message(),
            )),
            Self::CoordinatorNotAvailable => Some(Error::CoordinatorNotAvailable(
                CoordinatorNotAvailableError::with_default_message(),
            )),
            Self::CorruptMessage => Some(Error::CorruptRecord(CorruptRecordError::with_default_message())),
            Self::DelegationTokenAuthorizationFailed => Some(Error::DelegationTokenAuthorization(
                DelegationTokenAuthorizationError::with_default_message(),
            )),
            Self::DelegationTokenAuthDisabled => Some(Error::DelegationTokenDisabled(
                DelegationTokenDisabledError::with_default_message(),
            )),
            Self::DelegationTokenExpired => Some(Error::DelegationTokenExpired(
                DelegationTokenExpiredError::with_default_message(),
            )),
            Self::DelegationTokenNotFound => Some(Error::DelegationTokenNotFound(
                DelegationTokenNotFoundError::with_default_message(),
            )),
            Self::DelegationTokenOwnerMismatch => Some(Error::DelegationTokenOwnerMismatch(
                DelegationTokenOwnerMismatchError::with_default_message(),
            )),
            Self::DelegationTokenRequestNotAllowed => Some(Error::UnsupportedByAuthentication(
                UnsupportedByAuthenticationError::with_default_message(),
            )),
            Self::DuplicateBrokerRegistration => Some(Error::DuplicateBrokerRegistration(
                DuplicateBrokerRegistrationError::with_default_message(),
            )),
            Self::DuplicateResource => Some(Error::DuplicateResource(DuplicateResourceError::with_default_message())),
            Self::DuplicateSequenceNumber => {
                Some(Error::DuplicateSequence(DuplicateSequenceError::with_default_message()))
            },
            Self::DuplicateVoter => Some(Error::DuplicateVoter(DuplicateVoterError::with_default_message())),
            Self::ElectionNotNeeded => Some(Error::ElectionNotNeeded(ElectionNotNeededError::with_default_message())),
            Self::EligibleLeadersNotAvailable => Some(Error::EligibleLeadersNotAvailable(
                EligibleLeadersNotAvailableError::with_default_message(),
            )),
            Self::FeatureUpdateFailed => {
                Some(Error::FeatureUpdateFailed(FeatureUpdateFailedError::with_default_message()))
            },
            Self::FencedInstanceId => Some(Error::FencedInstanceId(FencedInstanceIdError::with_default_message())),
            Self::FencedLeaderEpoch => Some(Error::FencedLeaderEpoch(FencedLeaderEpochError::with_default_message())),
            Self::FencedMemberEpoch => Some(Error::FencedMemberEpoch(FencedMemberEpochError::with_default_message())),
            Self::FencedStateEpoch => Some(Error::FencedStateEpoch(FencedStateEpochError::with_default_message())),
            Self::FetchSessionIdNotFound => Some(Error::FetchSessionIdNotFound(
                FetchSessionIdNotFoundError::with_default_message(),
            )),
            Self::FetchSessionTopicIdError => {
                Some(Error::FetchSessionTopicId(FetchSessionTopicIdError::with_default_message()))
            },
            Self::GroupAuthorizationFailed => {
                Some(Error::GroupAuthorization(GroupAuthorizationError::with_default_message()))
            },
            Self::GroupIdNotFound => Some(Error::GroupIdNotFound(GroupIdNotFoundError::with_default_message())),
            Self::GroupMaxSizeReached => {
                Some(Error::GroupMaxSizeReached(GroupMaxSizeReachedError::with_default_message()))
            },
            Self::GroupSubscribedToTopic => Some(Error::GroupSubscribedToTopic(
                GroupSubscribedToTopicError::with_default_message(),
            )),
            Self::IllegalGeneration => Some(Error::IllegalGeneration(IllegalGenerationError::with_default_message())),
            Self::IllegalSaslState => Some(Error::IllegalSaslState(IllegalSaslStateError::with_default_message())),
            Self::InconsistentClusterId => {
                Some(Error::InconsistentClusterId(InconsistentClusterIdError::with_default_message()))
            },
            Self::InconsistentGroupProtocol => Some(Error::InconsistentGroupProtocol(
                InconsistentGroupProtocolError::with_default_message(),
            )),
            Self::InconsistentTopicId => {
                Some(Error::InconsistentTopicId(InconsistentTopicIdError::with_default_message()))
            },
            Self::InconsistentVoterSet => {
                Some(Error::InconsistentVoterSet(InconsistentVoterSetError::with_default_message()))
            },
            Self::IneligibleReplica => Some(Error::IneligibleReplica(IneligibleReplicaError::with_default_message())),
            Self::InvalidCommitOffsetSize => Some(Error::InvalidCommitOffsetSize(
                InvalidCommitOffsetSizeError::with_default_message(),
            )),
            Self::InvalidConfig => Some(Error::InvalidConfiguration(InvalidConfigurationError::with_default_message())),
            Self::InvalidFetchSessionEpoch => Some(Error::InvalidFetchSessionEpoch(
                InvalidFetchSessionEpochError::with_default_message(),
            )),
            Self::InvalidFetchSize => Some(Error::InvalidFetchSize(InvalidFetchSizeError::with_default_message())),
            Self::InvalidGroupId => Some(Error::InvalidGroupId(InvalidGroupIdError::with_default_message())),
            Self::InvalidPartitions => Some(Error::InvalidPartitions(InvalidPartitionsError::with_default_message())),
            Self::InvalidPrincipalType => {
                Some(Error::InvalidPrincipalType(InvalidPrincipalTypeError::with_default_message()))
            },
            Self::InvalidProducerEpoch => {
                Some(Error::InvalidProducerEpoch(InvalidProducerEpochError::with_default_message()))
            },
            Self::InvalidProducerIdMapping => {
                Some(Error::InvalidPidMapping(InvalidPidMappingError::with_default_message()))
            },
            Self::InvalidRecord => Some(Error::InvalidRecord(InvalidRecordError::with_default_message())),
            Self::InvalidRecordState => {
                Some(Error::InvalidRecordState(InvalidRecordStateError::with_default_message()))
            },
            Self::InvalidRegistration => {
                Some(Error::InvalidRegistration(InvalidRegistrationError::with_default_message()))
            },
            Self::InvalidRegularExpression => Some(Error::InvalidRegularExpression(
                InvalidRegularExpressionError::with_default_message(),
            )),
            Self::InvalidReplicationFactor => Some(Error::InvalidReplicationFactor(
                InvalidReplicationFactorError::with_default_message(),
            )),
            Self::InvalidReplicaAssignment => Some(Error::InvalidReplicaAssignment(
                InvalidReplicaAssignmentError::with_default_message(),
            )),
            Self::InvalidRequest => Some(Error::InvalidRequest(InvalidRequestError::with_default_message())),
            Self::InvalidRequiredAcks => {
                Some(Error::InvalidRequiredAcks(InvalidRequiredAcksError::with_default_message()))
            },
            Self::InvalidSessionTimeout => {
                Some(Error::InvalidSessionTimeout(InvalidSessionTimeoutError::with_default_message()))
            },
            Self::InvalidShareSessionEpoch => Some(Error::InvalidShareSessionEpoch(
                InvalidShareSessionEpochError::with_default_message(),
            )),
            Self::InvalidTimestamp => Some(Error::InvalidTimestamp(InvalidTimestampError::with_default_message())),
            Self::InvalidTopicError => Some(Error::InvalidTopic(InvalidTopicError::with_default_message())),
            Self::InvalidTransactionTimeout => {
                Some(Error::InvalidTxnTimeout(InvalidTxnTimeoutError::with_default_message()))
            },
            Self::InvalidTxnState => Some(Error::InvalidTxnState(InvalidTxnStateError::with_default_message())),
            Self::InvalidUpdateVersion => {
                Some(Error::InvalidUpdateVersion(InvalidUpdateVersionError::with_default_message()))
            },
            Self::InvalidVoterKey => Some(Error::InvalidVoterKey(InvalidVoterKeyError::with_default_message())),
            Self::KafkaStorageError => Some(Error::KafkaStorage(KafkaStorageError::with_default_message())),
            Self::LeaderNotAvailable => {
                Some(Error::LeaderNotAvailable(LeaderNotAvailableError::with_default_message()))
            },
            Self::ListenerNotFound => Some(Error::ListenerNotFound(ListenerNotFoundError::with_default_message())),
            Self::LogDirNotFound => Some(Error::LogDirNotFound(LogDirNotFoundError::with_default_message())),
            Self::MemberIdRequired => Some(Error::MemberIdRequired(MemberIdRequiredError::with_default_message())),
            Self::MessageTooLarge => Some(Error::RecordTooLarge(RecordTooLargeError::with_default_message())),
            Self::MismatchedEndpointType => Some(Error::MismatchedEndpointType(
                MismatchedEndpointTypeError::with_default_message(),
            )),
            Self::NetworkError => Some(Error::Network(NetworkError::with_default_message())),
            Self::NewLeaderElected => Some(Error::NewLeaderElected(NewLeaderElectedError::with_default_message())),
            Self::NonEmptyGroup => Some(Error::GroupNotEmpty(GroupNotEmptyError::with_default_message())),
            Self::NotController => Some(Error::NotController(NotControllerError::with_default_message())),
            Self::NotCoordinator => Some(Error::NotCoordinator(NotCoordinatorError::with_default_message())),
            Self::NotEnoughReplicas => Some(Error::NotEnoughReplicas(NotEnoughReplicasError::with_default_message())),
            Self::NotEnoughReplicasAfterAppend => Some(Error::NotEnoughReplicasAfterAppend(
                NotEnoughReplicasAfterAppendError::with_default_message(),
            )),
            Self::NotLeaderOrFollower => {
                Some(Error::NotLeaderOrFollower(NotLeaderOrFollowerError::with_default_message()))
            },
            Self::NoReassignmentInProgress => Some(Error::NoReassignmentInProgress(
                NoReassignmentInProgressError::with_default_message(),
            )),
            Self::OffsetMetadataTooLarge => Some(Error::OffsetMetadataTooLarge(
                OffsetMetadataTooLargeError::with_default_message(),
            )),
            Self::OffsetMovedToTieredStorage => Some(Error::OffsetMovedToTieredStorage(
                OffsetMovedToTieredStorageError::with_default_message(),
            )),
            Self::OffsetNotAvailable => {
                Some(Error::OffsetNotAvailable(OffsetNotAvailableError::with_default_message()))
            },
            Self::OffsetOutOfRange => Some(Error::OffsetOutOfRange(OffsetOutOfRangeError::with_default_message())),
            Self::OperationNotAttempted => {
                Some(Error::OperationNotAttempted(OperationNotAttemptedError::with_default_message()))
            },
            Self::OutOfOrderSequenceNumber => {
                Some(Error::OutOfOrderSequence(OutOfOrderSequenceError::with_default_message()))
            },
            Self::PolicyViolation => Some(Error::PolicyViolation(PolicyViolationError::with_default_message())),
            Self::PositionOutOfRange => {
                Some(Error::PositionOutOfRange(PositionOutOfRangeError::with_default_message()))
            },
            Self::PreferredLeaderNotAvailable => Some(Error::PreferredLeaderNotAvailable(
                PreferredLeaderNotAvailableError::with_default_message(),
            )),
            Self::PrincipalDeserializationFailure => Some(Error::PrincipalDeserialization(
                PrincipalDeserializationError::with_default_message(),
            )),
            Self::ProducerFenced => Some(Error::ProducerFenced(ProducerFencedError::with_default_message())),
            Self::ReassignmentInProgress => Some(Error::ReassignmentInProgress(
                ReassignmentInProgressError::with_default_message(),
            )),
            Self::RebalanceInProgress => {
                Some(Error::RebalanceInProgress(RebalanceInProgressError::with_default_message()))
            },
            Self::RebootstrapRequired => {
                Some(Error::RebootstrapRequired(RebootstrapRequiredError::with_default_message()))
            },
            Self::RecordListTooLarge => {
                Some(Error::RecordBatchTooLarge(RecordBatchTooLargeError::with_default_message()))
            },
            Self::ReplicaNotAvailable => {
                Some(Error::ReplicaNotAvailable(ReplicaNotAvailableError::with_default_message()))
            },
            Self::RequestTimedOut => Some(Error::Timeout(TimeoutError::with_default_message())),
            Self::ResourceNotFound => Some(Error::ResourceNotFound(ResourceNotFoundError::with_default_message())),
            Self::SaslAuthenticationFailed => {
                Some(Error::SaslAuthentication(SaslAuthenticationError::with_default_message()))
            },
            Self::SecurityDisabled => Some(Error::SecurityDisabled(SecurityDisabledError::with_default_message())),
            Self::ShareSessionLimitReached => Some(Error::ShareSessionLimitReached(
                ShareSessionLimitReachedError::with_default_message(),
            )),
            Self::ShareSessionNotFound => {
                Some(Error::ShareSessionNotFound(ShareSessionNotFoundError::with_default_message()))
            },
            Self::SnapshotNotFound => Some(Error::SnapshotNotFound(SnapshotNotFoundError::with_default_message())),
            Self::StaleBrokerEpoch => Some(Error::StaleBrokerEpoch(StaleBrokerEpochError::with_default_message())),
            Self::StaleControllerEpoch => Some(Error::ControllerMoved(ControllerMovedError::with_default_message())),
            Self::StaleMemberEpoch => Some(Error::StaleMemberEpoch(StaleMemberEpochError::with_default_message())),
            Self::StreamsInvalidTopology => Some(Error::StreamsInvalidTopology(
                StreamsInvalidTopologyError::with_default_message(),
            )),
            Self::StreamsInvalidTopologyEpoch => Some(Error::StreamsInvalidTopologyEpoch(
                StreamsInvalidTopologyEpochError::with_default_message(),
            )),
            Self::StreamsTopologyFenced => {
                Some(Error::StreamsTopologyFenced(StreamsTopologyFencedError::with_default_message()))
            },
            Self::TelemetryTooLarge => Some(Error::TelemetryTooLarge(TelemetryTooLargeError::with_default_message())),
            Self::ThrottlingQuotaExceeded => Some(Error::ThrottlingQuotaExceeded(ThrottlingQuotaExceededError::new(
                0,
                self.message(),
            ))),
            Self::TopicAlreadyExists => Some(Error::TopicExists(TopicExistsError::with_default_message())),
            Self::TopicAuthorizationFailed => {
                Some(Error::TopicAuthorization(TopicAuthorizationError::with_default_message()))
            },
            Self::TopicDeletionDisabled => {
                Some(Error::TopicDeletionDisabled(TopicDeletionDisabledError::with_default_message()))
            },
            Self::TransactionalIdAuthorizationFailed => Some(Error::TransactionalIdAuthorization(
                TransactionalIdAuthorizationError::with_default_message(),
            )),
            Self::TransactionalIdNotFound => Some(Error::TransactionalIdNotFound(
                TransactionalIdNotFoundError::with_default_message(),
            )),
            Self::TransactionAbortable => {
                Some(Error::TransactionAbortable(TransactionAbortableError::with_default_message()))
            },
            Self::TransactionCoordinatorFenced => Some(Error::TransactionCoordinatorFenced(
                TransactionCoordinatorFencedError::with_default_message(),
            )),
            Self::UnacceptableCredential => Some(Error::UnacceptableCredential(
                UnacceptableCredentialError::with_default_message(),
            )),
            Self::UnknownControllerId => {
                Some(Error::UnknownControllerId(UnknownControllerIdError::with_default_message()))
            },
            Self::UnknownLeaderEpoch => {
                Some(Error::UnknownLeaderEpoch(UnknownLeaderEpochError::with_default_message()))
            },
            Self::UnknownMemberId => Some(Error::UnknownMemberId(UnknownMemberIdError::with_default_message())),
            Self::UnknownProducerId => Some(Error::UnknownProducerId(UnknownProducerIdError::with_default_message())),
            Self::UnknownServerError => Some(Error::UnknownServer(UnknownServerError::with_default_message())),
            Self::UnknownSubscriptionId => {
                Some(Error::UnknownSubscriptionId(UnknownSubscriptionIdError::with_default_message()))
            },
            Self::UnknownTopicId => Some(Error::UnknownTopicId(UnknownTopicIdError::with_default_message())),
            Self::UnknownTopicOrPartition => Some(Error::UnknownTopicOrPartition(
                UnknownTopicOrPartitionError::with_default_message(),
            )),
            Self::UnreleasedInstanceId => {
                Some(Error::UnreleasedInstanceId(UnreleasedInstanceIdError::with_default_message()))
            },
            Self::UnstableOffsetCommit => {
                Some(Error::UnstableOffsetCommit(UnstableOffsetCommitError::with_default_message()))
            },
            Self::UnsupportedAssignor => {
                Some(Error::UnsupportedAssignor(UnsupportedAssignorError::with_default_message()))
            },
            Self::UnsupportedCompressionType => Some(Error::UnsupportedCompressionType(
                UnsupportedCompressionTypeError::with_default_message(),
            )),
            Self::UnsupportedEndpointType => Some(Error::UnsupportedEndpointType(
                UnsupportedEndpointTypeError::with_default_message(),
            )),
            Self::UnsupportedForMessageFormat => Some(Error::UnsupportedForMessageFormat(
                UnsupportedForMessageFormatError::with_default_message(),
            )),
            Self::UnsupportedSaslMechanism => Some(Error::UnsupportedSaslMechanism(
                UnsupportedSaslMechanismError::with_default_message(),
            )),
            Self::UnsupportedVersion => {
                Some(Error::UnsupportedVersion(UnsupportedVersionError::with_default_message()))
            },
            Self::VoterNotFound => Some(Error::VoterNotFound(VoterNotFoundError::with_default_message())),
        }
    }

    /// The name of the error class this code names, or `None` for
    /// [`Errors::None`].
    ///
    /// Translates Java's `Errors.exceptionName()` (`protocol/Errors.java:474`),
    /// which returns `exception.getClass().getName()` — the *fully qualified*
    /// name, e.g. `org.apache.kafka.common.errors.UnknownServerException`. Rust
    /// has no Java package to report, so this returns the bare Rust type name
    /// (`"UnknownServerError"`): the same string `Display` prefixes, since that
    /// translates `Throwable.toString()`, which uses the same
    /// `getClass().getName()`.
    ///
    /// Java's only client-side caller is `FetchCollector.java:325`
    /// (`log.debug("Error in fetch for partition {}: {}", tp,
    /// error.exceptionName())`), which names the class when a fetch response
    /// carries an error.
    pub fn error_name(&self) -> Option<&'static str> {
        // `ErrorName` is the per-class answer, so this does not repeat
        // `error()`'s 135-arm match — the class it resolves to answers for
        // itself, exactly as `getClass().getName()` does in Java.
        //
        // It does build the class to ask it, which costs the default message's
        // `String` where Java reads a cached instance. That is [`error`]'s
        // existing cost, not a new one, and this is a diagnostic accessor with
        // no hot-path caller (Java's is one log statement in `FetchCollector`);
        // the alternative is a second 135-arm match that can drift from the
        // first.
        self.error().map(|e| crate::common::kafka_error::ErrorName::name(&e))
    }

    /// The exception class this code names, carrying `message` instead of the
    /// code's default text.
    ///
    /// Translates Java's `Errors.exception(String)`. Java returns the cached
    /// default instance when `message` is null; Rust expresses "no message" by
    /// calling [`error`](Self::error) instead, so this always builds a fresh one.
    pub fn error_with_message(&self, message: impl Into<String>) -> Option<Error> {
        let message = message.into();
        match self {
            Self::None => None,
            Self::BrokerIdNotRegistered => Some(Error::BrokerIdNotRegistered(BrokerIdNotRegisteredError::new(message))),
            Self::BrokerNotAvailable => Some(Error::BrokerNotAvailable(BrokerNotAvailableError::new(message))),
            Self::ClusterAuthorizationFailed => {
                Some(Error::ClusterAuthorization(ClusterAuthorizationError::new(message)))
            },
            Self::ConcurrentTransactions => {
                Some(Error::ConcurrentTransactions(ConcurrentTransactionsError::new(message)))
            },
            Self::CoordinatorLoadInProgress => {
                Some(Error::CoordinatorLoadInProgress(CoordinatorLoadInProgressError::new(message)))
            },
            Self::CoordinatorNotAvailable => {
                Some(Error::CoordinatorNotAvailable(CoordinatorNotAvailableError::new(message)))
            },
            Self::CorruptMessage => Some(Error::CorruptRecord(CorruptRecordError::new(message))),
            Self::DelegationTokenAuthorizationFailed => Some(Error::DelegationTokenAuthorization(
                DelegationTokenAuthorizationError::new(message),
            )),
            Self::DelegationTokenAuthDisabled => {
                Some(Error::DelegationTokenDisabled(DelegationTokenDisabledError::new(message)))
            },
            Self::DelegationTokenExpired => {
                Some(Error::DelegationTokenExpired(DelegationTokenExpiredError::new(message)))
            },
            Self::DelegationTokenNotFound => {
                Some(Error::DelegationTokenNotFound(DelegationTokenNotFoundError::new(message)))
            },
            Self::DelegationTokenOwnerMismatch => Some(Error::DelegationTokenOwnerMismatch(
                DelegationTokenOwnerMismatchError::new(message),
            )),
            Self::DelegationTokenRequestNotAllowed => Some(Error::UnsupportedByAuthentication(
                UnsupportedByAuthenticationError::new(message),
            )),
            Self::DuplicateBrokerRegistration => Some(Error::DuplicateBrokerRegistration(
                DuplicateBrokerRegistrationError::new(message),
            )),
            Self::DuplicateResource => Some(Error::DuplicateResource(DuplicateResourceError::new(message))),
            Self::DuplicateSequenceNumber => Some(Error::DuplicateSequence(DuplicateSequenceError::new(message))),
            Self::DuplicateVoter => Some(Error::DuplicateVoter(DuplicateVoterError::new(message))),
            Self::ElectionNotNeeded => Some(Error::ElectionNotNeeded(ElectionNotNeededError::new(message))),
            Self::EligibleLeadersNotAvailable => Some(Error::EligibleLeadersNotAvailable(
                EligibleLeadersNotAvailableError::new(message),
            )),
            Self::FeatureUpdateFailed => Some(Error::FeatureUpdateFailed(FeatureUpdateFailedError::new(message))),
            Self::FencedInstanceId => Some(Error::FencedInstanceId(FencedInstanceIdError::new(message))),
            Self::FencedLeaderEpoch => Some(Error::FencedLeaderEpoch(FencedLeaderEpochError::new(message))),
            Self::FencedMemberEpoch => Some(Error::FencedMemberEpoch(FencedMemberEpochError::new(message))),
            Self::FencedStateEpoch => Some(Error::FencedStateEpoch(FencedStateEpochError::new(message))),
            Self::FetchSessionIdNotFound => {
                Some(Error::FetchSessionIdNotFound(FetchSessionIdNotFoundError::new(message)))
            },
            Self::FetchSessionTopicIdError => Some(Error::FetchSessionTopicId(FetchSessionTopicIdError::new(message))),
            Self::GroupAuthorizationFailed => Some(Error::GroupAuthorization(GroupAuthorizationError::with_message(
                String::new(),
                message,
            ))),
            Self::GroupIdNotFound => Some(Error::GroupIdNotFound(GroupIdNotFoundError::new(message))),
            Self::GroupMaxSizeReached => Some(Error::GroupMaxSizeReached(GroupMaxSizeReachedError::new(message))),
            Self::GroupSubscribedToTopic => {
                Some(Error::GroupSubscribedToTopic(GroupSubscribedToTopicError::new(message)))
            },
            Self::IllegalGeneration => Some(Error::IllegalGeneration(IllegalGenerationError::new(message))),
            Self::IllegalSaslState => Some(Error::IllegalSaslState(IllegalSaslStateError::new(message))),
            Self::InconsistentClusterId => Some(Error::InconsistentClusterId(InconsistentClusterIdError::new(message))),
            Self::InconsistentGroupProtocol => {
                Some(Error::InconsistentGroupProtocol(InconsistentGroupProtocolError::new(message)))
            },
            Self::InconsistentTopicId => Some(Error::InconsistentTopicId(InconsistentTopicIdError::new(message))),
            Self::InconsistentVoterSet => Some(Error::InconsistentVoterSet(InconsistentVoterSetError::new(message))),
            Self::IneligibleReplica => Some(Error::IneligibleReplica(IneligibleReplicaError::new(message))),
            Self::InvalidCommitOffsetSize => {
                Some(Error::InvalidCommitOffsetSize(InvalidCommitOffsetSizeError::new(message)))
            },
            Self::InvalidConfig => Some(Error::InvalidConfiguration(InvalidConfigurationError::new(message))),
            Self::InvalidFetchSessionEpoch => {
                Some(Error::InvalidFetchSessionEpoch(InvalidFetchSessionEpochError::new(message)))
            },
            Self::InvalidFetchSize => Some(Error::InvalidFetchSize(InvalidFetchSizeError::new(message))),
            Self::InvalidGroupId => Some(Error::InvalidGroupId(InvalidGroupIdError::new(message))),
            Self::InvalidPartitions => Some(Error::InvalidPartitions(InvalidPartitionsError::new(message))),
            Self::InvalidPrincipalType => Some(Error::InvalidPrincipalType(InvalidPrincipalTypeError::new(message))),
            Self::InvalidProducerEpoch => Some(Error::InvalidProducerEpoch(InvalidProducerEpochError::new(message))),
            Self::InvalidProducerIdMapping => Some(Error::InvalidPidMapping(InvalidPidMappingError::new(message))),
            Self::InvalidRecord => Some(Error::InvalidRecord(InvalidRecordError::new(message))),
            Self::InvalidRecordState => Some(Error::InvalidRecordState(InvalidRecordStateError::new(message))),
            Self::InvalidRegistration => Some(Error::InvalidRegistration(InvalidRegistrationError::new(message))),
            Self::InvalidRegularExpression => {
                Some(Error::InvalidRegularExpression(InvalidRegularExpressionError::new(message)))
            },
            Self::InvalidReplicationFactor => {
                Some(Error::InvalidReplicationFactor(InvalidReplicationFactorError::new(message)))
            },
            Self::InvalidReplicaAssignment => {
                Some(Error::InvalidReplicaAssignment(InvalidReplicaAssignmentError::new(message)))
            },
            Self::InvalidRequest => Some(Error::InvalidRequest(InvalidRequestError::new(message))),
            Self::InvalidRequiredAcks => Some(Error::InvalidRequiredAcks(InvalidRequiredAcksError::new(message))),
            Self::InvalidSessionTimeout => Some(Error::InvalidSessionTimeout(InvalidSessionTimeoutError::new(message))),
            Self::InvalidShareSessionEpoch => {
                Some(Error::InvalidShareSessionEpoch(InvalidShareSessionEpochError::new(message)))
            },
            Self::InvalidTimestamp => Some(Error::InvalidTimestamp(InvalidTimestampError::new(message))),
            Self::InvalidTopicError => {
                Some(Error::InvalidTopic(InvalidTopicError::with_message(HashSet::new(), message)))
            },
            Self::InvalidTransactionTimeout => Some(Error::InvalidTxnTimeout(InvalidTxnTimeoutError::new(message))),
            Self::InvalidTxnState => Some(Error::InvalidTxnState(InvalidTxnStateError::new(message))),
            Self::InvalidUpdateVersion => Some(Error::InvalidUpdateVersion(InvalidUpdateVersionError::new(message))),
            Self::InvalidVoterKey => Some(Error::InvalidVoterKey(InvalidVoterKeyError::new(message))),
            Self::KafkaStorageError => Some(Error::KafkaStorage(KafkaStorageError::new(message))),
            Self::LeaderNotAvailable => Some(Error::LeaderNotAvailable(LeaderNotAvailableError::new(message))),
            Self::ListenerNotFound => Some(Error::ListenerNotFound(ListenerNotFoundError::new(message))),
            Self::LogDirNotFound => Some(Error::LogDirNotFound(LogDirNotFoundError::new(message))),
            Self::MemberIdRequired => Some(Error::MemberIdRequired(MemberIdRequiredError::new(message))),
            Self::MessageTooLarge => Some(Error::RecordTooLarge(RecordTooLargeError::new(message))),
            Self::MismatchedEndpointType => {
                Some(Error::MismatchedEndpointType(MismatchedEndpointTypeError::new(message)))
            },
            Self::NetworkError => Some(Error::Network(NetworkError::new(message))),
            Self::NewLeaderElected => Some(Error::NewLeaderElected(NewLeaderElectedError::new(message))),
            Self::NonEmptyGroup => Some(Error::GroupNotEmpty(GroupNotEmptyError::new(message))),
            Self::NotController => Some(Error::NotController(NotControllerError::new(message))),
            Self::NotCoordinator => Some(Error::NotCoordinator(NotCoordinatorError::new(message))),
            Self::NotEnoughReplicas => Some(Error::NotEnoughReplicas(NotEnoughReplicasError::new(message))),
            Self::NotEnoughReplicasAfterAppend => Some(Error::NotEnoughReplicasAfterAppend(
                NotEnoughReplicasAfterAppendError::new(message),
            )),
            Self::NotLeaderOrFollower => Some(Error::NotLeaderOrFollower(NotLeaderOrFollowerError::new(message))),
            Self::NoReassignmentInProgress => {
                Some(Error::NoReassignmentInProgress(NoReassignmentInProgressError::new(message)))
            },
            Self::OffsetMetadataTooLarge => {
                Some(Error::OffsetMetadataTooLarge(OffsetMetadataTooLargeError::new(message)))
            },
            Self::OffsetMovedToTieredStorage => {
                Some(Error::OffsetMovedToTieredStorage(OffsetMovedToTieredStorageError::new(message)))
            },
            Self::OffsetNotAvailable => Some(Error::OffsetNotAvailable(OffsetNotAvailableError::new(message))),
            Self::OffsetOutOfRange => Some(Error::OffsetOutOfRange(OffsetOutOfRangeError::new(message))),
            Self::OperationNotAttempted => Some(Error::OperationNotAttempted(OperationNotAttemptedError::new(message))),
            Self::OutOfOrderSequenceNumber => Some(Error::OutOfOrderSequence(OutOfOrderSequenceError::new(message))),
            Self::PolicyViolation => Some(Error::PolicyViolation(PolicyViolationError::new(message))),
            Self::PositionOutOfRange => Some(Error::PositionOutOfRange(PositionOutOfRangeError::new(message))),
            Self::PreferredLeaderNotAvailable => Some(Error::PreferredLeaderNotAvailable(
                PreferredLeaderNotAvailableError::new(message),
            )),
            Self::PrincipalDeserializationFailure => {
                Some(Error::PrincipalDeserialization(PrincipalDeserializationError::new(message)))
            },
            Self::ProducerFenced => Some(Error::ProducerFenced(ProducerFencedError::new(message))),
            Self::ReassignmentInProgress => {
                Some(Error::ReassignmentInProgress(ReassignmentInProgressError::new(message)))
            },
            Self::RebalanceInProgress => Some(Error::RebalanceInProgress(RebalanceInProgressError::new(message))),
            Self::RebootstrapRequired => Some(Error::RebootstrapRequired(RebootstrapRequiredError::new(message))),
            Self::RecordListTooLarge => Some(Error::RecordBatchTooLarge(RecordBatchTooLargeError::new(message))),
            Self::ReplicaNotAvailable => Some(Error::ReplicaNotAvailable(ReplicaNotAvailableError::new(message))),
            Self::RequestTimedOut => Some(Error::Timeout(TimeoutError::new(message))),
            Self::ResourceNotFound => Some(Error::ResourceNotFound(ResourceNotFoundError::new(message))),
            Self::SaslAuthenticationFailed => Some(Error::SaslAuthentication(SaslAuthenticationError::new(message))),
            Self::SecurityDisabled => Some(Error::SecurityDisabled(SecurityDisabledError::new(message))),
            Self::ShareSessionLimitReached => {
                Some(Error::ShareSessionLimitReached(ShareSessionLimitReachedError::new(message)))
            },
            Self::ShareSessionNotFound => Some(Error::ShareSessionNotFound(ShareSessionNotFoundError::new(message))),
            Self::SnapshotNotFound => Some(Error::SnapshotNotFound(SnapshotNotFoundError::new(message))),
            Self::StaleBrokerEpoch => Some(Error::StaleBrokerEpoch(StaleBrokerEpochError::new(message))),
            Self::StaleControllerEpoch => Some(Error::ControllerMoved(ControllerMovedError::new(message))),
            Self::StaleMemberEpoch => Some(Error::StaleMemberEpoch(StaleMemberEpochError::new(message))),
            Self::StreamsInvalidTopology => {
                Some(Error::StreamsInvalidTopology(StreamsInvalidTopologyError::new(message)))
            },
            Self::StreamsInvalidTopologyEpoch => Some(Error::StreamsInvalidTopologyEpoch(
                StreamsInvalidTopologyEpochError::new(message),
            )),
            Self::StreamsTopologyFenced => Some(Error::StreamsTopologyFenced(StreamsTopologyFencedError::new(message))),
            Self::TelemetryTooLarge => Some(Error::TelemetryTooLarge(TelemetryTooLargeError::new(message))),
            Self::ThrottlingQuotaExceeded => {
                Some(Error::ThrottlingQuotaExceeded(ThrottlingQuotaExceededError::new(0, message)))
            },
            Self::TopicAlreadyExists => Some(Error::TopicExists(TopicExistsError::new(message))),
            Self::TopicAuthorizationFailed => Some(Error::TopicAuthorization(TopicAuthorizationError::with_message(
                HashSet::new(),
                message,
            ))),
            Self::TopicDeletionDisabled => Some(Error::TopicDeletionDisabled(TopicDeletionDisabledError::new(message))),
            Self::TransactionalIdAuthorizationFailed => Some(Error::TransactionalIdAuthorization(
                TransactionalIdAuthorizationError::new(message),
            )),
            Self::TransactionalIdNotFound => {
                Some(Error::TransactionalIdNotFound(TransactionalIdNotFoundError::new(message)))
            },
            Self::TransactionAbortable => Some(Error::TransactionAbortable(TransactionAbortableError::new(message))),
            Self::TransactionCoordinatorFenced => Some(Error::TransactionCoordinatorFenced(
                TransactionCoordinatorFencedError::new(message),
            )),
            Self::UnacceptableCredential => {
                Some(Error::UnacceptableCredential(UnacceptableCredentialError::new(message)))
            },
            Self::UnknownControllerId => Some(Error::UnknownControllerId(UnknownControllerIdError::new(message))),
            Self::UnknownLeaderEpoch => Some(Error::UnknownLeaderEpoch(UnknownLeaderEpochError::new(message))),
            Self::UnknownMemberId => Some(Error::UnknownMemberId(UnknownMemberIdError::new(message))),
            Self::UnknownProducerId => Some(Error::UnknownProducerId(UnknownProducerIdError::new(message))),
            Self::UnknownServerError => Some(Error::UnknownServer(UnknownServerError::new(message))),
            Self::UnknownSubscriptionId => Some(Error::UnknownSubscriptionId(UnknownSubscriptionIdError::new(message))),
            Self::UnknownTopicId => Some(Error::UnknownTopicId(UnknownTopicIdError::new(message))),
            Self::UnknownTopicOrPartition => {
                Some(Error::UnknownTopicOrPartition(UnknownTopicOrPartitionError::new(message)))
            },
            Self::UnreleasedInstanceId => Some(Error::UnreleasedInstanceId(UnreleasedInstanceIdError::new(message))),
            Self::UnstableOffsetCommit => Some(Error::UnstableOffsetCommit(UnstableOffsetCommitError::new(message))),
            Self::UnsupportedAssignor => Some(Error::UnsupportedAssignor(UnsupportedAssignorError::new(message))),
            Self::UnsupportedCompressionType => {
                Some(Error::UnsupportedCompressionType(UnsupportedCompressionTypeError::new(message)))
            },
            Self::UnsupportedEndpointType => {
                Some(Error::UnsupportedEndpointType(UnsupportedEndpointTypeError::new(message)))
            },
            Self::UnsupportedForMessageFormat => Some(Error::UnsupportedForMessageFormat(
                UnsupportedForMessageFormatError::new(message),
            )),
            Self::UnsupportedSaslMechanism => {
                Some(Error::UnsupportedSaslMechanism(UnsupportedSaslMechanismError::new(message)))
            },
            Self::UnsupportedVersion => Some(Error::UnsupportedVersion(UnsupportedVersionError::new(message))),
            Self::VoterNotFound => Some(Error::VoterNotFound(VoterNotFoundError::new(message))),
        }
    }

    /// Look up an error by its code. Returns `UnknownServerError` for unknown codes.
    pub fn for_code(code: i16) -> Self {
        match code {
            -1 => Self::UnknownServerError,
            0 => Self::None,
            1 => Self::OffsetOutOfRange,
            2 => Self::CorruptMessage,
            3 => Self::UnknownTopicOrPartition,
            4 => Self::InvalidFetchSize,
            5 => Self::LeaderNotAvailable,
            6 => Self::NotLeaderOrFollower,
            7 => Self::RequestTimedOut,
            8 => Self::BrokerNotAvailable,
            9 => Self::ReplicaNotAvailable,
            10 => Self::MessageTooLarge,
            11 => Self::StaleControllerEpoch,
            12 => Self::OffsetMetadataTooLarge,
            13 => Self::NetworkError,
            14 => Self::CoordinatorLoadInProgress,
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            17 => Self::InvalidTopicError,
            18 => Self::RecordListTooLarge,
            19 => Self::NotEnoughReplicas,
            20 => Self::NotEnoughReplicasAfterAppend,
            21 => Self::InvalidRequiredAcks,
            22 => Self::IllegalGeneration,
            23 => Self::InconsistentGroupProtocol,
            24 => Self::InvalidGroupId,
            25 => Self::UnknownMemberId,
            26 => Self::InvalidSessionTimeout,
            27 => Self::RebalanceInProgress,
            28 => Self::InvalidCommitOffsetSize,
            29 => Self::TopicAuthorizationFailed,
            30 => Self::GroupAuthorizationFailed,
            31 => Self::ClusterAuthorizationFailed,
            32 => Self::InvalidTimestamp,
            33 => Self::UnsupportedSaslMechanism,
            34 => Self::IllegalSaslState,
            35 => Self::UnsupportedVersion,
            36 => Self::TopicAlreadyExists,
            37 => Self::InvalidPartitions,
            38 => Self::InvalidReplicationFactor,
            39 => Self::InvalidReplicaAssignment,
            40 => Self::InvalidConfig,
            41 => Self::NotController,
            42 => Self::InvalidRequest,
            43 => Self::UnsupportedForMessageFormat,
            44 => Self::PolicyViolation,
            45 => Self::OutOfOrderSequenceNumber,
            46 => Self::DuplicateSequenceNumber,
            47 => Self::InvalidProducerEpoch,
            48 => Self::InvalidTxnState,
            49 => Self::InvalidProducerIdMapping,
            50 => Self::InvalidTransactionTimeout,
            51 => Self::ConcurrentTransactions,
            52 => Self::TransactionCoordinatorFenced,
            53 => Self::TransactionalIdAuthorizationFailed,
            54 => Self::SecurityDisabled,
            55 => Self::OperationNotAttempted,
            56 => Self::KafkaStorageError,
            57 => Self::LogDirNotFound,
            58 => Self::SaslAuthenticationFailed,
            59 => Self::UnknownProducerId,
            60 => Self::ReassignmentInProgress,
            61 => Self::DelegationTokenAuthDisabled,
            62 => Self::DelegationTokenNotFound,
            63 => Self::DelegationTokenOwnerMismatch,
            64 => Self::DelegationTokenRequestNotAllowed,
            65 => Self::DelegationTokenAuthorizationFailed,
            66 => Self::DelegationTokenExpired,
            67 => Self::InvalidPrincipalType,
            68 => Self::NonEmptyGroup,
            69 => Self::GroupIdNotFound,
            70 => Self::FetchSessionIdNotFound,
            71 => Self::InvalidFetchSessionEpoch,
            72 => Self::ListenerNotFound,
            73 => Self::TopicDeletionDisabled,
            74 => Self::FencedLeaderEpoch,
            75 => Self::UnknownLeaderEpoch,
            76 => Self::UnsupportedCompressionType,
            77 => Self::StaleBrokerEpoch,
            78 => Self::OffsetNotAvailable,
            79 => Self::MemberIdRequired,
            80 => Self::PreferredLeaderNotAvailable,
            81 => Self::GroupMaxSizeReached,
            82 => Self::FencedInstanceId,
            83 => Self::EligibleLeadersNotAvailable,
            84 => Self::ElectionNotNeeded,
            85 => Self::NoReassignmentInProgress,
            86 => Self::GroupSubscribedToTopic,
            87 => Self::InvalidRecord,
            88 => Self::UnstableOffsetCommit,
            89 => Self::ThrottlingQuotaExceeded,
            90 => Self::ProducerFenced,
            91 => Self::ResourceNotFound,
            92 => Self::DuplicateResource,
            93 => Self::UnacceptableCredential,
            94 => Self::InconsistentVoterSet,
            95 => Self::InvalidUpdateVersion,
            96 => Self::FeatureUpdateFailed,
            97 => Self::PrincipalDeserializationFailure,
            98 => Self::SnapshotNotFound,
            99 => Self::PositionOutOfRange,
            100 => Self::UnknownTopicId,
            101 => Self::DuplicateBrokerRegistration,
            102 => Self::BrokerIdNotRegistered,
            103 => Self::InconsistentTopicId,
            104 => Self::InconsistentClusterId,
            105 => Self::TransactionalIdNotFound,
            106 => Self::FetchSessionTopicIdError,
            107 => Self::IneligibleReplica,
            108 => Self::NewLeaderElected,
            109 => Self::OffsetMovedToTieredStorage,
            110 => Self::FencedMemberEpoch,
            111 => Self::UnreleasedInstanceId,
            112 => Self::UnsupportedAssignor,
            113 => Self::StaleMemberEpoch,
            114 => Self::MismatchedEndpointType,
            115 => Self::UnsupportedEndpointType,
            116 => Self::UnknownControllerId,
            117 => Self::UnknownSubscriptionId,
            118 => Self::TelemetryTooLarge,
            119 => Self::InvalidRegistration,
            120 => Self::TransactionAbortable,
            121 => Self::InvalidRecordState,
            122 => Self::ShareSessionNotFound,
            123 => Self::InvalidShareSessionEpoch,
            124 => Self::FencedStateEpoch,
            125 => Self::InvalidVoterKey,
            126 => Self::DuplicateVoter,
            127 => Self::VoterNotFound,
            128 => Self::InvalidRegularExpression,
            129 => Self::RebootstrapRequired,
            130 => Self::StreamsInvalidTopology,
            131 => Self::StreamsInvalidTopologyEpoch,
            132 => Self::StreamsTopologyFenced,
            133 => Self::ShareSessionLimitReached,
            _ => Self::UnknownServerError,
        }
    }
}

/// Renders the error the way Java renders it when the enum value is
/// interpolated into a string.
///
/// Java's `Errors` overrides no `toString()`, so `Enum.toString()` — i.e.
/// `Enum.name()` — is what `String.format("%s", error)` produces. Callers that
/// want the long human description must ask for
/// [`message`](Errors::message) explicitly.
impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.enum_name())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// Java's `Errors` constant names paired with their wire codes, in
    /// `Errors.java` declaration order.
    ///
    /// This is the reference side of the constant-name check: it is transcribed
    /// from the Java source, not derived from the Rust enum, so a Rust variant
    /// renamed or renumbered on its own fails the test.
    ///
    /// Two rows diverge from Java's spelling: code 13 (`NETWORK_EXCEPTION`) and
    /// code 17 (`INVALID_TOPIC_EXCEPTION`), whose Java constants carry the word
    /// CLAUDE.md §2 bans from Rust code. See the comments at their arms in
    /// [`Errors::enum_name`].
    const JAVA_CONSTANT_NAMES: [(i16, &str); 135] = [
        (-1, "UNKNOWN_SERVER_ERROR"),
        (0, "NONE"),
        (1, "OFFSET_OUT_OF_RANGE"),
        (2, "CORRUPT_MESSAGE"),
        (3, "UNKNOWN_TOPIC_OR_PARTITION"),
        (4, "INVALID_FETCH_SIZE"),
        (5, "LEADER_NOT_AVAILABLE"),
        (6, "NOT_LEADER_OR_FOLLOWER"),
        (7, "REQUEST_TIMED_OUT"),
        (8, "BROKER_NOT_AVAILABLE"),
        (9, "REPLICA_NOT_AVAILABLE"),
        (10, "MESSAGE_TOO_LARGE"),
        (11, "STALE_CONTROLLER_EPOCH"),
        (12, "OFFSET_METADATA_TOO_LARGE"),
        (13, "NETWORK_ERROR"),
        (14, "COORDINATOR_LOAD_IN_PROGRESS"),
        (15, "COORDINATOR_NOT_AVAILABLE"),
        (16, "NOT_COORDINATOR"),
        (17, "INVALID_TOPIC_ERROR"),
        (18, "RECORD_LIST_TOO_LARGE"),
        (19, "NOT_ENOUGH_REPLICAS"),
        (20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND"),
        (21, "INVALID_REQUIRED_ACKS"),
        (22, "ILLEGAL_GENERATION"),
        (23, "INCONSISTENT_GROUP_PROTOCOL"),
        (24, "INVALID_GROUP_ID"),
        (25, "UNKNOWN_MEMBER_ID"),
        (26, "INVALID_SESSION_TIMEOUT"),
        (27, "REBALANCE_IN_PROGRESS"),
        (28, "INVALID_COMMIT_OFFSET_SIZE"),
        (29, "TOPIC_AUTHORIZATION_FAILED"),
        (30, "GROUP_AUTHORIZATION_FAILED"),
        (31, "CLUSTER_AUTHORIZATION_FAILED"),
        (32, "INVALID_TIMESTAMP"),
        (33, "UNSUPPORTED_SASL_MECHANISM"),
        (34, "ILLEGAL_SASL_STATE"),
        (35, "UNSUPPORTED_VERSION"),
        (36, "TOPIC_ALREADY_EXISTS"),
        (37, "INVALID_PARTITIONS"),
        (38, "INVALID_REPLICATION_FACTOR"),
        (39, "INVALID_REPLICA_ASSIGNMENT"),
        (40, "INVALID_CONFIG"),
        (41, "NOT_CONTROLLER"),
        (42, "INVALID_REQUEST"),
        (43, "UNSUPPORTED_FOR_MESSAGE_FORMAT"),
        (44, "POLICY_VIOLATION"),
        (45, "OUT_OF_ORDER_SEQUENCE_NUMBER"),
        (46, "DUPLICATE_SEQUENCE_NUMBER"),
        (47, "INVALID_PRODUCER_EPOCH"),
        (48, "INVALID_TXN_STATE"),
        (49, "INVALID_PRODUCER_ID_MAPPING"),
        (50, "INVALID_TRANSACTION_TIMEOUT"),
        (51, "CONCURRENT_TRANSACTIONS"),
        (52, "TRANSACTION_COORDINATOR_FENCED"),
        (53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"),
        (54, "SECURITY_DISABLED"),
        (55, "OPERATION_NOT_ATTEMPTED"),
        (56, "KAFKA_STORAGE_ERROR"),
        (57, "LOG_DIR_NOT_FOUND"),
        (58, "SASL_AUTHENTICATION_FAILED"),
        (59, "UNKNOWN_PRODUCER_ID"),
        (60, "REASSIGNMENT_IN_PROGRESS"),
        (61, "DELEGATION_TOKEN_AUTH_DISABLED"),
        (62, "DELEGATION_TOKEN_NOT_FOUND"),
        (63, "DELEGATION_TOKEN_OWNER_MISMATCH"),
        (64, "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED"),
        (65, "DELEGATION_TOKEN_AUTHORIZATION_FAILED"),
        (66, "DELEGATION_TOKEN_EXPIRED"),
        (67, "INVALID_PRINCIPAL_TYPE"),
        (68, "NON_EMPTY_GROUP"),
        (69, "GROUP_ID_NOT_FOUND"),
        (70, "FETCH_SESSION_ID_NOT_FOUND"),
        (71, "INVALID_FETCH_SESSION_EPOCH"),
        (72, "LISTENER_NOT_FOUND"),
        (73, "TOPIC_DELETION_DISABLED"),
        (74, "FENCED_LEADER_EPOCH"),
        (75, "UNKNOWN_LEADER_EPOCH"),
        (76, "UNSUPPORTED_COMPRESSION_TYPE"),
        (77, "STALE_BROKER_EPOCH"),
        (78, "OFFSET_NOT_AVAILABLE"),
        (79, "MEMBER_ID_REQUIRED"),
        (80, "PREFERRED_LEADER_NOT_AVAILABLE"),
        (81, "GROUP_MAX_SIZE_REACHED"),
        (82, "FENCED_INSTANCE_ID"),
        (83, "ELIGIBLE_LEADERS_NOT_AVAILABLE"),
        (84, "ELECTION_NOT_NEEDED"),
        (85, "NO_REASSIGNMENT_IN_PROGRESS"),
        (86, "GROUP_SUBSCRIBED_TO_TOPIC"),
        (87, "INVALID_RECORD"),
        (88, "UNSTABLE_OFFSET_COMMIT"),
        (89, "THROTTLING_QUOTA_EXCEEDED"),
        (90, "PRODUCER_FENCED"),
        (91, "RESOURCE_NOT_FOUND"),
        (92, "DUPLICATE_RESOURCE"),
        (93, "UNACCEPTABLE_CREDENTIAL"),
        (94, "INCONSISTENT_VOTER_SET"),
        (95, "INVALID_UPDATE_VERSION"),
        (96, "FEATURE_UPDATE_FAILED"),
        (97, "PRINCIPAL_DESERIALIZATION_FAILURE"),
        (98, "SNAPSHOT_NOT_FOUND"),
        (99, "POSITION_OUT_OF_RANGE"),
        (100, "UNKNOWN_TOPIC_ID"),
        (101, "DUPLICATE_BROKER_REGISTRATION"),
        (102, "BROKER_ID_NOT_REGISTERED"),
        (103, "INCONSISTENT_TOPIC_ID"),
        (104, "INCONSISTENT_CLUSTER_ID"),
        (105, "TRANSACTIONAL_ID_NOT_FOUND"),
        (106, "FETCH_SESSION_TOPIC_ID_ERROR"),
        (107, "INELIGIBLE_REPLICA"),
        (108, "NEW_LEADER_ELECTED"),
        (109, "OFFSET_MOVED_TO_TIERED_STORAGE"),
        (110, "FENCED_MEMBER_EPOCH"),
        (111, "UNRELEASED_INSTANCE_ID"),
        (112, "UNSUPPORTED_ASSIGNOR"),
        (113, "STALE_MEMBER_EPOCH"),
        (114, "MISMATCHED_ENDPOINT_TYPE"),
        (115, "UNSUPPORTED_ENDPOINT_TYPE"),
        (116, "UNKNOWN_CONTROLLER_ID"),
        (117, "UNKNOWN_SUBSCRIPTION_ID"),
        (118, "TELEMETRY_TOO_LARGE"),
        (119, "INVALID_REGISTRATION"),
        (120, "TRANSACTION_ABORTABLE"),
        (121, "INVALID_RECORD_STATE"),
        (122, "SHARE_SESSION_NOT_FOUND"),
        (123, "INVALID_SHARE_SESSION_EPOCH"),
        (124, "FENCED_STATE_EPOCH"),
        (125, "INVALID_VOTER_KEY"),
        (126, "DUPLICATE_VOTER"),
        (127, "VOTER_NOT_FOUND"),
        (128, "INVALID_REGULAR_EXPRESSION"),
        (129, "REBOOTSTRAP_REQUIRED"),
        (130, "STREAMS_INVALID_TOPOLOGY"),
        (131, "STREAMS_INVALID_TOPOLOGY_EPOCH"),
        (132, "STREAMS_TOPOLOGY_FENCED"),
        (133, "SHARE_SESSION_LIMIT_REACHED"),
    ];

    /// Java's `Errors` overrides no `toString()`, so `Enum.name()` — the
    /// constant — is what every `"..." + error` site renders. Pin the whole
    /// table in both directions:
    ///
    /// - every code Java declares renders exactly Java's constant, through both
    ///   [`Errors::enum_name`] and `Display`;
    /// - every code the Rust enum declares has a row in the table, so a code
    ///   added to the enum without a name fails here rather than silently
    ///   rendering something else.
    ///
    /// A sampled test cannot catch a single wrong or missing arm, which is why
    /// this walks all 135 codes (the precedent is
    /// `test_retriable_errors_match_java_hierarchy`).
    #[test]
    fn test_enum_name_matches_java_constant_for_every_code() {
        // Direction 1: table -> rendered name.
        for (code, java_name) in JAVA_CONSTANT_NAMES {
            let error = Errors::for_code(code);
            assert_eq!(error.code(), code, "for_code({code}) is not the variant with that code");
            assert_eq!(error.enum_name(), java_name, "wrong constant name for code {code}");
            assert_eq!(error.to_string(), java_name, "Display must render the constant for code {code}");
            // The constant is not the long description: a copy-paste from
            // `message()` into an `enum_name()` arm would pass the two
            // assertions above only if the table were derived from Rust, so
            // check the two accessors stay distinct.
            assert_ne!(error.enum_name(), error.message(), "code {code} renders its description");
        }

        // Direction 2: every declared code -> table.
        let names: HashSet<&str> = JAVA_CONSTANT_NAMES.iter().map(|(_, name)| *name).collect();
        assert_eq!(names.len(), JAVA_CONSTANT_NAMES.len(), "duplicate constant name in the table");
        let codes: HashSet<i16> = JAVA_CONSTANT_NAMES.iter().map(|(code, _)| *code).collect();
        assert_eq!(codes.len(), JAVA_CONSTANT_NAMES.len(), "duplicate code in the table");
        for code in -1..=133i16 {
            assert_eq!(
                Errors::for_code(code).code(),
                code,
                "code {code} is no longer a declared variant — update the table"
            );
            assert!(codes.contains(&code), "declared code {code} has no row in the table");
        }
        // The table ends where the enum ends: a newly added code falls back to
        // `UnknownServerError`, which is the signal to extend both.
        assert_eq!(
            Errors::for_code(134),
            Errors::UnknownServerError,
            "a new error code was added — add it to JAVA_CONSTANT_NAMES"
        );
    }

    /// One row of the §10.4 hierarchy-parity table: the predicate's name (for
    /// failure messages), the set of codes Java says it covers, and the predicate.
    type PredicateCase<'a> = (&'a str, &'a HashSet<Errors>, fn(&Errors) -> bool);

    #[test]
    fn test_error_code_round_trip() {
        // Every error's code should round-trip through for_code
        let all_errors = [
            Errors::UnknownServerError,
            Errors::None,
            Errors::OffsetOutOfRange,
            Errors::CorruptMessage,
            Errors::UnknownTopicOrPartition,
            Errors::InvalidFetchSize,
            Errors::LeaderNotAvailable,
            Errors::NotLeaderOrFollower,
            Errors::RequestTimedOut,
            Errors::BrokerNotAvailable,
            Errors::ReplicaNotAvailable,
            Errors::MessageTooLarge,
            Errors::StaleControllerEpoch,
            Errors::OffsetMetadataTooLarge,
            Errors::NetworkError,
            Errors::CoordinatorLoadInProgress,
            Errors::CoordinatorNotAvailable,
            Errors::NotCoordinator,
            Errors::InvalidTopicError,
            Errors::RecordListTooLarge,
            Errors::NotEnoughReplicas,
            Errors::NotEnoughReplicasAfterAppend,
            Errors::InvalidRequiredAcks,
            Errors::IllegalGeneration,
            Errors::InconsistentGroupProtocol,
            Errors::InvalidGroupId,
            Errors::UnknownMemberId,
            Errors::InvalidSessionTimeout,
            Errors::RebalanceInProgress,
            Errors::InvalidCommitOffsetSize,
            Errors::TopicAuthorizationFailed,
            Errors::GroupAuthorizationFailed,
            Errors::ClusterAuthorizationFailed,
            Errors::InvalidTimestamp,
            Errors::UnsupportedSaslMechanism,
            Errors::IllegalSaslState,
            Errors::UnsupportedVersion,
            Errors::TopicAlreadyExists,
            Errors::InvalidPartitions,
            Errors::InvalidReplicationFactor,
            Errors::InvalidReplicaAssignment,
            Errors::InvalidConfig,
            Errors::NotController,
            Errors::InvalidRequest,
            Errors::UnsupportedForMessageFormat,
            Errors::PolicyViolation,
            Errors::OutOfOrderSequenceNumber,
            Errors::DuplicateSequenceNumber,
            Errors::InvalidProducerEpoch,
            Errors::InvalidTxnState,
            Errors::InvalidProducerIdMapping,
            Errors::InvalidTransactionTimeout,
            Errors::ConcurrentTransactions,
            Errors::TransactionCoordinatorFenced,
            Errors::TransactionalIdAuthorizationFailed,
            Errors::SecurityDisabled,
            Errors::OperationNotAttempted,
            Errors::KafkaStorageError,
            Errors::LogDirNotFound,
            Errors::SaslAuthenticationFailed,
            Errors::UnknownProducerId,
            Errors::ReassignmentInProgress,
            Errors::DelegationTokenAuthDisabled,
            Errors::DelegationTokenNotFound,
            Errors::DelegationTokenOwnerMismatch,
            Errors::DelegationTokenRequestNotAllowed,
            Errors::DelegationTokenAuthorizationFailed,
            Errors::DelegationTokenExpired,
            Errors::InvalidPrincipalType,
            Errors::NonEmptyGroup,
            Errors::GroupIdNotFound,
            Errors::FetchSessionIdNotFound,
            Errors::InvalidFetchSessionEpoch,
            Errors::ListenerNotFound,
            Errors::TopicDeletionDisabled,
            Errors::FencedLeaderEpoch,
            Errors::UnknownLeaderEpoch,
            Errors::UnsupportedCompressionType,
            Errors::StaleBrokerEpoch,
            Errors::OffsetNotAvailable,
            Errors::MemberIdRequired,
            Errors::PreferredLeaderNotAvailable,
            Errors::GroupMaxSizeReached,
            Errors::FencedInstanceId,
            Errors::EligibleLeadersNotAvailable,
            Errors::ElectionNotNeeded,
            Errors::NoReassignmentInProgress,
            Errors::GroupSubscribedToTopic,
            Errors::InvalidRecord,
            Errors::UnstableOffsetCommit,
            Errors::ThrottlingQuotaExceeded,
            Errors::ProducerFenced,
            Errors::ResourceNotFound,
            Errors::DuplicateResource,
            Errors::UnacceptableCredential,
            Errors::InconsistentVoterSet,
            Errors::InvalidUpdateVersion,
            Errors::FeatureUpdateFailed,
            Errors::PrincipalDeserializationFailure,
            Errors::SnapshotNotFound,
            Errors::PositionOutOfRange,
            Errors::UnknownTopicId,
            Errors::DuplicateBrokerRegistration,
            Errors::BrokerIdNotRegistered,
            Errors::InconsistentTopicId,
            Errors::InconsistentClusterId,
            Errors::TransactionalIdNotFound,
            Errors::FetchSessionTopicIdError,
            Errors::IneligibleReplica,
            Errors::NewLeaderElected,
            Errors::OffsetMovedToTieredStorage,
            Errors::FencedMemberEpoch,
            Errors::UnreleasedInstanceId,
            Errors::UnsupportedAssignor,
            Errors::StaleMemberEpoch,
            Errors::MismatchedEndpointType,
            Errors::UnsupportedEndpointType,
            Errors::UnknownControllerId,
            Errors::UnknownSubscriptionId,
            Errors::TelemetryTooLarge,
            Errors::InvalidRegistration,
            Errors::TransactionAbortable,
            Errors::InvalidRecordState,
            Errors::ShareSessionNotFound,
            Errors::InvalidShareSessionEpoch,
            Errors::FencedStateEpoch,
            Errors::InvalidVoterKey,
            Errors::DuplicateVoter,
            Errors::VoterNotFound,
            Errors::InvalidRegularExpression,
            Errors::RebootstrapRequired,
            Errors::StreamsInvalidTopology,
            Errors::StreamsInvalidTopologyEpoch,
            Errors::StreamsTopologyFenced,
            Errors::ShareSessionLimitReached,
        ];
        for error in &all_errors {
            assert_eq!(
                Errors::for_code(error.code()),
                *error,
                "Round-trip failed for {:?} (code {})",
                error,
                error.code()
            );
        }
    }

    #[test]
    fn test_unknown_code_returns_unknown_server_error() {
        assert_eq!(Errors::for_code(9999), Errors::UnknownServerError);
        assert_eq!(Errors::for_code(-2), Errors::UnknownServerError);
    }

    #[test]
    fn test_none_is_not_retriable() {
        assert!(!Errors::None.error().is_some_and(|x| x.is_retriable_error()));
    }

    #[test]
    fn test_retriable_errors() {
        assert!(Errors::RequestTimedOut.error().is_some_and(|x| x.is_retriable_error()));
        assert!(Errors::LeaderNotAvailable.error().is_some_and(|x| x.is_retriable_error()));
        assert!(Errors::NotLeaderOrFollower.error().is_some_and(|x| x.is_retriable_error()));
        assert!(
            Errors::CoordinatorLoadInProgress
                .error()
                .is_some_and(|x| x.is_retriable_error())
        );
        assert!(Errors::CoordinatorNotAvailable.error().is_some_and(|x| x.is_retriable_error()));
        assert!(Errors::NotCoordinator.error().is_some_and(|x| x.is_retriable_error()));
        assert!(Errors::NetworkError.error().is_some_and(|x| x.is_retriable_error()));
        assert!(Errors::NotEnoughReplicas.error().is_some_and(|x| x.is_retriable_error()));
        assert!(Errors::NotController.error().is_some_and(|x| x.is_retriable_error()));
        assert!(Errors::UnknownTopicOrPartition.error().is_some_and(|x| x.is_retriable_error()));
    }

    /// Each hierarchy predicate (CLAUDE.md §10.4) must be `true` for **exactly** the
    /// error codes whose Java exception class extends the corresponding class.
    ///
    /// Sets derived from the Apache Kafka 4.2 source in `kafka/` by taking the
    /// transitive closure of each root over `extends`, then keeping the classes with
    /// a protocol code. Asserted in both directions over every assigned code, for
    /// the same reason as the retriable/fatal tests: a sampled test cannot catch a
    /// code wrongly added to, or missing from, a set.
    ///
    /// Together with `test_retriable_errors_match_java_hierarchy`,
    /// `test_new_intermediate_predicates_match_java_hierarchy` and
    /// `test_fatal_errors_match_java_request_utils`, this covers all fifteen
    /// §10.4 predicates over every code. Some columns are degenerate — all-`true`
    /// for `is_kafka_error` / `is_api_error`, all-`false` for the three
    /// client-side-only families — and that is deliberate: the degenerate answer
    /// IS the Java contract, and asserting it is what catches a code mapped to
    /// the wrong kind of payload.
    #[test]
    fn test_hierarchy_predicates_match_java() {
        // AuthenticationException — 3 of its 5 classes carry a code (the base and
        // SslAuthenticationException are client-side only).
        let authn: HashSet<Errors> = [
            Errors::UnsupportedSaslMechanism,
            Errors::IllegalSaslState,
            Errors::SaslAuthenticationFailed,
        ]
        .into_iter()
        .collect();

        // AuthorizationException — the 5 *AuthorizationFailed codes.
        let authz: HashSet<Errors> = [
            Errors::TopicAuthorizationFailed,
            Errors::GroupAuthorizationFailed,
            Errors::ClusterAuthorizationFailed,
            Errors::TransactionalIdAuthorizationFailed,
            Errors::DelegationTokenAuthorizationFailed,
        ]
        .into_iter()
        .collect();

        // InvalidMetadataException — 13 codes.
        let invalid_metadata: HashSet<Errors> = [
            Errors::UnknownTopicOrPartition,
            Errors::LeaderNotAvailable,
            Errors::NotLeaderOrFollower,
            Errors::ReplicaNotAvailable,
            Errors::NetworkError,
            Errors::KafkaStorageError,
            Errors::ListenerNotFound,
            Errors::FencedLeaderEpoch,
            Errors::PreferredLeaderNotAvailable,
            Errors::EligibleLeadersNotAvailable,
            Errors::ElectionNotNeeded,
            Errors::UnknownTopicId,
            Errors::InconsistentTopicId,
        ]
        .into_iter()
        .collect();

        // RefreshRetriableException — the 13 above plus the 2 coordinator codes,
        // since InvalidMetadataException extends RefreshRetriableException.
        let mut refresh_retriable = invalid_metadata.clone();
        refresh_retriable.insert(Errors::CoordinatorNotAvailable);
        refresh_retriable.insert(Errors::NotCoordinator);

        // `Errors.java` declares its factory as `Function<String, ApiException>`,
        // so EVERY assigned code names an `ApiException` subclass — and therefore
        // a `KafkaException` one. Both columns are all-`true`, which is the
        // assertion that matters: a code accidentally mapped to a code-less
        // payload (`Serialization`, `Wakeup`, a `java.lang` error) would flip it,
        // and `ConsumerUtils.maybeWrapAsKafkaException` /
        // `KafkaProducer.doSend`'s `catch (ApiException e)` both dispatch on it.
        let all_coded: HashSet<Errors> = (-1i16..=200).map(Errors::for_code).filter(|e| *e != Errors::None).collect();

        // TimeoutException — `REQUEST_TIMED_OUT` is its only coded member;
        // `BufferExhaustedException`, its one subclass, has no entry in `Errors`.
        let timeout: HashSet<Errors> = [Errors::RequestTimedOut].into_iter().collect();

        // Three client-side-only families: `SerializationException` and the two
        // `clients.consumer` offset classes are raised by the client, never
        // reported by a broker, so no code may answer `true`. An all-`false`
        // column is still a real assertion — it is what fails if one of these
        // predicates is ever wired to a coded payload by mistake.
        let client_side_only: HashSet<Errors> = HashSet::new();

        let cases: [PredicateCase; 10] = [
            ("is_kafka_error", &all_coded, |e| e.error().is_some_and(|x| x.is_kafka_error())),
            ("is_api_error", &all_coded, |e| e.error().is_some_and(|x| x.is_api_error())),
            ("is_timeout_error", &timeout, |e| {
                e.error().is_some_and(|x| x.is_timeout_error())
            }),
            ("is_serialization_error", &client_side_only, |e| {
                e.error().is_some_and(|x| x.is_serialization_error())
            }),
            ("is_consumer_invalid_offset_error", &client_side_only, |e| {
                e.error().is_some_and(|x| x.is_consumer_invalid_offset_error())
            }),
            ("is_consumer_offset_out_of_range_error", &client_side_only, |e| {
                e.error().is_some_and(|x| x.is_consumer_offset_out_of_range_error())
            }),
            ("is_authentication_error", &authn, |e| {
                e.error().is_some_and(|x| x.is_authentication_error())
            }),
            ("is_authorization_error", &authz, |e| {
                e.error().is_some_and(|x| x.is_authorization_error())
            }),
            ("is_invalid_metadata_error", &invalid_metadata, |e| {
                e.error().is_some_and(|x| x.is_invalid_metadata_error())
            }),
            ("is_refresh_retriable_error", &refresh_retriable, |e| {
                e.error().is_some_and(|x| x.is_refresh_retriable_error())
            }),
        ];

        let mut seen: HashSet<Errors> = HashSet::new();
        for code in -1i16..=200 {
            let error = Errors::for_code(code);
            if !seen.insert(error) {
                continue;
            }
            for (name, expected, predicate) in &cases {
                assert_eq!(
                    expected.contains(&error),
                    predicate(&error),
                    "{name}: {error:?} (code {}) disagrees with the Java hierarchy: expected {}, got {}",
                    error.code(),
                    expected.contains(&error),
                    predicate(&error)
                );
            }
        }
        // Guard against `for_code` collapsing the enum and passing vacuously.
        for (name, expected, _) in &cases {
            for error in expected.iter() {
                assert!(seen.contains(error), "{name}: {error:?} was never reached by for_code");
            }
        }
    }

    /// The predicates must nest the way Java's `extends` chain does:
    /// `InvalidMetadataException` -> `RefreshRetriableException` ->
    /// `RetriableException`, and the auth families are disjoint from each other and
    /// both entirely fatal. Cheap to assert, and it catches a set edited in one
    /// predicate but not its parent.
    #[test]
    fn test_hierarchy_predicates_nest() {
        for code in -1i16..=200 {
            let e = Errors::for_code(code);
            if e.error().is_some_and(|x| x.is_invalid_metadata_error()) {
                assert!(
                    e.error().is_some_and(|x| x.is_refresh_retriable_error()),
                    "{e:?}: invalid-metadata must be refresh-retriable"
                );
            }
            if e.error().is_some_and(|x| x.is_refresh_retriable_error()) {
                assert!(
                    e.error().is_some_and(|x| x.is_retriable_error()),
                    "{e:?}: refresh-retriable must be retriable"
                );
            }
            if e.error().is_some_and(|x| x.is_authentication_error())
                || e.error().is_some_and(|x| x.is_authorization_error())
            {
                // Kafka 4.2: `AuthenticationException` and `AuthorizationException`
                // both extend `InvalidConfigurationException`, not `ApiException`.
                assert!(
                    e.error().is_some_and(|x| x.is_invalid_configuration_error()),
                    "{e:?}: auth/authz must be invalid-configuration"
                );
                assert!(
                    e.error()
                        .is_some_and(|x| crate::common::requests::request_utils::is_fatal_error(&x)),
                    "{e:?}: auth/authz errors must be fatal"
                );
                assert!(
                    !e.error().is_some_and(|x| x.is_retriable_error()),
                    "{e:?}: auth/authz errors must not be retriable"
                );
            }
            assert!(
                !(e.error().is_some_and(|x| x.is_authentication_error())
                    && e.error().is_some_and(|x| x.is_authorization_error())),
                "{e:?}: the two auth families are disjoint"
            );
            // The four remaining intermediate classes are direct `ApiException`
            // children, so none of them is retriable.
            for (name, holds) in [
                (
                    "invalid-configuration",
                    e.error().is_some_and(|x| x.is_invalid_configuration_error()),
                ),
                (
                    "application-recoverable",
                    e.error().is_some_and(|x| x.is_application_recoverable_error()),
                ),
                ("invalid-offset", e.error().is_some_and(|x| x.is_invalid_offset_error())),
                (
                    "out-of-order-sequence",
                    e.error().is_some_and(|x| x.is_out_of_order_sequence_error()),
                ),
            ] {
                if holds {
                    assert!(
                        !e.error().is_some_and(|x| x.is_retriable_error()),
                        "{e:?}: {name} must not be retriable"
                    );
                }
            }
        }
    }

    /// The four intermediate classes added alongside the `ErrorHierarchy`
    /// refactor must cover **exactly** the codes whose Java class transitively
    /// extends them — checked in both directions over every code, per
    /// CLAUDE.md §10.4. Sets derived from `common/errors/*.java` in Kafka 4.2.
    #[test]
    fn test_new_intermediate_predicates_match_java_hierarchy() {
        let invalid_configuration: HashSet<Errors> = [
            // InvalidConfigurationException's own subclasses ...
            Errors::InvalidConfig,
            // `InvalidRecordException` lives in `common`, not `common.errors`.
            Errors::InvalidRecord,
            Errors::InvalidReplicationFactor,
            Errors::InvalidRequiredAcks,
            Errors::InvalidTopicError,
            Errors::RecordListTooLarge,
            Errors::UnsupportedForMessageFormat,
            Errors::UnsupportedVersion,
            // ... plus everything under AuthenticationException ...
            Errors::UnsupportedSaslMechanism,
            Errors::IllegalSaslState,
            Errors::SaslAuthenticationFailed,
            // ... and under AuthorizationException.
            Errors::TopicAuthorizationFailed,
            Errors::GroupAuthorizationFailed,
            Errors::ClusterAuthorizationFailed,
            Errors::TransactionalIdAuthorizationFailed,
            Errors::DelegationTokenAuthorizationFailed,
        ]
        .into_iter()
        .collect();

        let application_recoverable: HashSet<Errors> = [
            Errors::FencedInstanceId,
            Errors::IllegalGeneration,
            Errors::InvalidProducerEpoch,
            Errors::InvalidProducerIdMapping,
            Errors::ProducerFenced,
            Errors::UnknownMemberId,
        ]
        .into_iter()
        .collect();

        let invalid_offset: HashSet<Errors> = [Errors::OffsetOutOfRange].into_iter().collect();

        let out_of_order_sequence: HashSet<Errors> = [Errors::OutOfOrderSequenceNumber, Errors::UnknownProducerId]
            .into_iter()
            .collect();

        #[allow(clippy::type_complexity)]
        let families: &[(&str, &HashSet<Errors>, fn(&Errors) -> bool)] = &[
            ("is_invalid_configuration_error", &invalid_configuration, |e| {
                e.error().is_some_and(|x| x.is_invalid_configuration_error())
            }),
            ("is_application_recoverable_error", &application_recoverable, |e| {
                e.error().is_some_and(|x| x.is_application_recoverable_error())
            }),
            ("is_invalid_offset_error", &invalid_offset, |e| {
                e.error().is_some_and(|x| x.is_invalid_offset_error())
            }),
            ("is_out_of_order_sequence_error", &out_of_order_sequence, |e| {
                e.error().is_some_and(|x| x.is_out_of_order_sequence_error())
            }),
        ];

        for (name, expected, pred) in families {
            for code in -1i16..=200 {
                let e = Errors::for_code(code);
                if code != -1 && e == Errors::UnknownServerError {
                    continue; // unassigned code, maps to the catch-all
                }
                assert_eq!(
                    pred(&e),
                    expected.contains(&e),
                    "{name}: {e:?} (code {code}) disagrees with the Java extends chain"
                );
            }
        }
    }

    /// `RequestUtils.isFatalException` (translated in `request_utils::is_fatal_error`) must be `true` for **exactly** the error codes whose
    /// Java exception class satisfies `RequestUtils.isFatalException`
    /// (`common/requests/RequestUtils.java:88`). Java has no per-exception
    /// fatal flag, so that class test IS the definition; if this set drifts,
    /// `AdminMetadataManager::update_failed` — Java's only caller — stops
    /// recording fatal errors that Java records, or starts recording ones it
    /// does not.
    ///
    /// Fatality is deliberately NOT exported to C (CLAUDE.md §10.4, and the note
    /// at `ffi/common.rs:161`): a C caller composes the classification from the
    /// exported predicates, so this table is also what keeps that composition
    /// answering what Java answers.
    ///
    /// The expected set was derived from the Apache Kafka 4.2 source in
    /// `kafka/` by taking the transitive closure of the seven classes named in
    /// `isFatalException` (16 classes, since `AuthenticationException` and
    /// `AuthorizationException` are base classes) and keeping those with an
    /// error code (13).
    ///
    /// Asserted in both directions over every code, for the same reason as the
    /// retriable test: a sampled test cannot catch a wrongly-added code.
    #[test]
    fn test_fatal_errors_match_java_request_utils() {
        // Java's own `RequestUtilsTest.testIsFatalException` runs first, verbatim:
        // it asserts on the base classes `AuthenticationException` /
        // `AuthorizationException`, which carry no protocol code and so are
        // unreachable from the code-set test below. They are reachable now that
        // those base classes are translated.
        use crate::common::errors::{
            AuthenticationError, AuthorizationError, DisconnectError, MismatchedEndpointTypeError,
            SecurityDisabledError, SslAuthenticationError, UnsupportedEndpointTypeError,
            UnsupportedForMessageFormatError, UnsupportedVersionError,
        };
        use crate::common::requests::request_utils::is_fatal_error;
        assert!(is_fatal_error(&Error::Authentication(AuthenticationError::new(""))));
        assert!(is_fatal_error(&Error::Authorization(AuthorizationError::new(""))));
        // SslAuthenticationException extends AuthenticationException — codeless,
        // so only reachable now that the base class is translated.
        assert!(is_fatal_error(&Error::SslAuthentication(SslAuthenticationError::new(""))));
        assert!(is_fatal_error(&Error::MismatchedEndpointType(
            MismatchedEndpointTypeError::new("")
        )));
        assert!(is_fatal_error(&Error::SecurityDisabled(SecurityDisabledError::new(""))));
        assert!(is_fatal_error(&Error::UnsupportedEndpointType(
            UnsupportedEndpointTypeError::new("")
        )));
        assert!(is_fatal_error(&Error::UnsupportedForMessageFormat(
            UnsupportedForMessageFormatError::new("")
        )));
        assert!(is_fatal_error(&Error::UnsupportedVersion(UnsupportedVersionError::new(""))));
        // retriable exceptions
        assert!(!is_fatal_error(&Error::Disconnect(DisconnectError::new(""))));

        run_fatal_errors_match_java_request_utils();
    }

    fn run_fatal_errors_match_java_request_utils() {
        let expected: HashSet<Errors> = [
            // AuthorizationException subclasses
            Errors::TopicAuthorizationFailed,
            Errors::GroupAuthorizationFailed,
            Errors::ClusterAuthorizationFailed,
            Errors::TransactionalIdAuthorizationFailed,
            Errors::DelegationTokenAuthorizationFailed,
            // AuthenticationException subclasses that carry a code
            Errors::UnsupportedSaslMechanism,
            Errors::IllegalSaslState,
            Errors::SaslAuthenticationFailed,
            // standalone classes
            Errors::UnsupportedVersion,
            Errors::UnsupportedForMessageFormat,
            Errors::SecurityDisabled,
            Errors::MismatchedEndpointType,
            Errors::UnsupportedEndpointType,
        ]
        .into_iter()
        .collect();

        let mut seen: HashSet<Errors> = HashSet::new();
        for code in -1i16..=200 {
            let error = Errors::for_code(code);
            if !seen.insert(error) {
                continue;
            }
            assert_eq!(
                expected.contains(&error),
                error
                    .error()
                    .is_some_and(|x| crate::common::requests::request_utils::is_fatal_error(&x)),
                "{error:?} (code {}) disagrees with Java's fatal classification: expected fatal={}, got {}",
                error.code(),
                expected.contains(&error),
                error
                    .error()
                    .is_some_and(|x| crate::common::requests::request_utils::is_fatal_error(&x))
            );
        }
        for error in &expected {
            assert!(seen.contains(error), "{error:?} was never reached by for_code");
        }
    }

    /// A retriable error is never fatal and vice versa: Java's two hierarchies
    /// (`RetriableException` vs. the `isFatalException` family) are disjoint,
    /// and code that retries on one while giving up on the other depends on
    /// that. Cheap to assert, and it catches a code added to both lists.
    #[test]
    fn test_fatal_and_retriable_are_disjoint() {
        for code in -1i16..=200 {
            let error = Errors::for_code(code);
            assert!(
                !(error
                    .error()
                    .is_some_and(|x| crate::common::requests::request_utils::is_fatal_error(&x))
                    && error.error().is_some_and(|x| x.is_retriable_error())),
                "{error:?} (code {}) is both fatal and retriable",
                error.code()
            );
        }
    }

    /// [`Error::is_retriable_error`] must be `true` for **exactly** the error codes
    /// whose Java exception class extends `RetriableException`. Java has no
    /// `Errors.isRetriable()` — and neither does [`Errors`]: the predicate lives on
    /// the class the code names, reached here as `code.error().is_some_and(..)`.
    /// Callers write `e instanceof RetriableException`, so the Rust predicate is
    /// the translation of that `instanceof` and any divergence silently changes
    /// retry behaviour.
    ///
    /// The expected set below was derived from the Apache Kafka 4.2 source in
    /// `kafka/` by walking each `Errors` constant's exception class up its
    /// `extends` chain. Three intermediate classes make the hierarchy wider
    /// than it looks, and each contributes codes here:
    ///
    ///  - `RefreshRetriableException extends RetriableException`
    ///    (`COORDINATOR_NOT_AVAILABLE`, `NOT_COORDINATOR`)
    ///  - `InvalidMetadataException extends RefreshRetriableException`
    ///    (`LEADER_NOT_AVAILABLE`, `NOT_LEADER_OR_FOLLOWER`,
    ///    `REPLICA_NOT_AVAILABLE`, `KAFKA_STORAGE_ERROR`, `LISTENER_NOT_FOUND`,
    ///    `FENCED_LEADER_EPOCH`, `UNKNOWN_TOPIC_ID`, `INCONSISTENT_TOPIC_ID`,
    ///    `NETWORK_EXCEPTION`, `UNKNOWN_TOPIC_OR_PARTITION`,
    ///    `PREFERRED_LEADER_NOT_AVAILABLE`, `ELIGIBLE_LEADERS_NOT_AVAILABLE`,
    ///    `ELECTION_NOT_NEEDED`)
    ///  - `TimeoutException extends RetriableException` (`REQUEST_TIMED_OUT`)
    ///
    /// A sampled test cannot catch the two failure modes that matter — a code
    /// wrongly ADDED to the list, or a newly-translated retriable code left
    /// OUT — so this asserts both directions over every code.
    #[test]
    fn test_retriable_errors_match_java_hierarchy() {
        // Java error codes whose exception extends RetriableException.
        let expected: HashSet<Errors> = [
            Errors::CorruptMessage,
            Errors::UnknownTopicOrPartition,
            Errors::LeaderNotAvailable,
            Errors::NotLeaderOrFollower,
            Errors::RequestTimedOut,
            Errors::ReplicaNotAvailable,
            Errors::NetworkError,
            Errors::CoordinatorLoadInProgress,
            Errors::CoordinatorNotAvailable,
            Errors::NotCoordinator,
            Errors::NotEnoughReplicas,
            Errors::NotEnoughReplicasAfterAppend,
            Errors::NotController,
            Errors::KafkaStorageError,
            Errors::FetchSessionIdNotFound,
            Errors::FetchSessionTopicIdError,
            Errors::InvalidFetchSessionEpoch,
            Errors::ListenerNotFound,
            Errors::FencedLeaderEpoch,
            Errors::UnknownLeaderEpoch,
            Errors::OffsetNotAvailable,
            Errors::PreferredLeaderNotAvailable,
            Errors::EligibleLeadersNotAvailable,
            Errors::ElectionNotNeeded,
            Errors::ConcurrentTransactions,
            Errors::ThrottlingQuotaExceeded,
            Errors::UnstableOffsetCommit,
            Errors::UnknownTopicId,
            Errors::InconsistentTopicId,
            Errors::InvalidShareSessionEpoch,
            Errors::ShareSessionNotFound,
            Errors::ShareSessionLimitReached,
        ]
        .into_iter()
        .collect();

        // Walk every assigned code (Rust has no `Errors::values()`; unassigned
        // codes fold into `UnknownServerError`, which is not retriable).
        let mut seen: HashSet<Errors> = HashSet::new();
        for code in -1i16..=200 {
            let error = Errors::for_code(code);
            if !seen.insert(error) {
                continue;
            }
            assert_eq!(
                expected.contains(&error),
                error.error().is_some_and(|x| x.is_retriable_error()),
                "{error:?} (code {}) disagrees with Java: expected retriable={}, got {}",
                error.code(),
                expected.contains(&error),
                error.error().is_some_and(|x| x.is_retriable_error())
            );
        }

        // Guard against `for_code` collapsing the enum and vacuously passing.
        for error in &expected {
            assert!(seen.contains(error), "{error:?} was never reached by for_code");
        }
    }

    #[test]
    fn test_non_retriable_errors() {
        assert!(!Errors::UnknownServerError.error().is_some_and(|x| x.is_retriable_error()));
        assert!(!Errors::InvalidRequest.error().is_some_and(|x| x.is_retriable_error()));
        assert!(!Errors::UnsupportedVersion.error().is_some_and(|x| x.is_retriable_error()));
        assert!(!Errors::TopicAuthorizationFailed.error().is_some_and(|x| x.is_retriable_error()));
        assert!(!Errors::GroupAuthorizationFailed.error().is_some_and(|x| x.is_retriable_error()));
    }

    #[test]
    fn test_error_message() {
        assert!(Errors::None.message().is_empty());
        assert!(!Errors::UnknownServerError.message().is_empty());
        assert!(Errors::RequestTimedOut.message().contains("timed out"));
    }

    #[test]
    fn test_unique_codes() {
        // Verify no duplicate codes exist by checking round-trip for all codes in range
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for code in -1..=133 {
            let error = Errors::for_code(code);
            if error != Errors::UnknownServerError || code == -1 {
                assert!(seen.insert(error.code()), "Duplicate code: {}", error.code());
            }
        }
    }

    // -----------------------------------------------------------------------
    // ErrorsTest.java (Apache Kafka 4.2,
    // clients/src/test/java/org/apache/kafka/common/protocol/ErrorsTest.java)
    //
    // Java iterates `Errors.values()`. Rust has no `values()`, so each of these
    // walks `for_code(-1..=133)` through [`all_codes`], which asserts up front
    // that the walk really does reach all 135 declared constants — otherwise a
    // shrunken walk would let every one of these pass vacuously.
    // -----------------------------------------------------------------------

    /// The 135 declared `Errors` constants, standing in for Java's
    /// `Errors.values()`.
    ///
    /// The codes are contiguous from -1 to 133, so the walk is exhaustive; the
    /// distinctness assertion is what keeps it that way if a constant is ever
    /// added out of range.
    fn all_codes() -> Vec<Errors> {
        let codes: Vec<Errors> = (-1i16..=133).map(Errors::for_code).collect();
        let distinct: HashSet<Errors> = codes.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            codes.len(),
            "for_code(-1..=133) must reach each constant exactly once, or every \
             ErrorsTest translation below passes vacuously"
        );
        assert_eq!(codes.len(), 135, "Errors declares 135 constants in Kafka 4.2");
        codes
    }

    /// `ErrorsTest.testUniqueErrorCodes`: the codes must be unique.
    ///
    /// Java compares `codeSet.size()` against `Errors.values().length`. The Rust
    /// enum's discriminants ARE the codes, so a duplicate would not compile —
    /// but the same is not true of `code()`, which the assertion below covers.
    #[test]
    fn errors_test_unique_error_codes() {
        let codes: HashSet<i16> = all_codes().iter().map(|e| e.code()).collect();
        assert_eq!(codes.len(), all_codes().len(), "Error codes must be unique");
    }

    /// `ErrorsTest.testUniqueExceptions`: every constant but `NONE` must name a
    /// **distinct** error class.
    ///
    /// This is the guard for the redesign's central invariant — the flat `Error`
    /// enum holds one variant per Java class, and [`Errors::error`] is the 1:1
    /// code-to-class map. Two codes collapsing onto one class would silently
    /// merge two Java exceptions, and every hierarchy predicate for one of them
    /// would then answer for the other.
    #[test]
    fn errors_test_unique_error_classes() {
        let mut classes: HashSet<&'static str> = HashSet::new();
        for error in all_codes() {
            if error != Errors::None {
                let name = error.error_name().expect("every code but None names a class");
                assert!(
                    classes.insert(name),
                    "Error classes must be unique: {name} is named by more than one code, \
                     the second being {error:?} (code {})",
                    error.code()
                );
            }
        }
        // Java: `assertEquals(exceptionSet.size(), Errors.values().length - 1)`.
        assert_eq!(classes.len(), all_codes().len() - 1, "Error classes must be unique");
    }

    /// `ErrorsTest.testExceptionsAreNotGeneric`: no constant may map to the bare
    /// `ApiException`.
    ///
    /// [`Error::Api`] is that class (`common/errors/api_error.rs`) — Java throws
    /// it directly where no subclass applies, so it exists as a variant, but no
    /// wire code may resolve to it. A code that did would report
    /// `is_api_error()` while carrying none of the subclass's meaning.
    #[test]
    fn errors_test_error_classes_are_not_generic() {
        for error in all_codes() {
            if error != Errors::None {
                assert_ne!(
                    error.error_name(),
                    Some("ApiError"),
                    "Generic ApiError should not be used: {error:?} (code {})",
                    error.code()
                );
                assert!(
                    !matches!(error.error(), Some(Error::Api(_))),
                    "Generic ApiError should not be used: {error:?} (code {})",
                    error.code()
                );
            }
        }
    }

    /// `ErrorsTest.testNoneException`: `NONE` has no error class.
    ///
    /// Java declares it `NONE(0, null, message -> null)`, so all three
    /// accessors must answer "nothing" rather than a zero-valued placeholder.
    #[test]
    fn errors_test_none_has_no_error_class() {
        assert!(Errors::None.error().is_none(), "The NONE error should not have an error class");
        assert!(Errors::None.error_with_message("ignored").is_none());
        assert!(Errors::None.error_name().is_none());
    }

    /// `ErrorsTest.testForExceptionInheritance`: a subclass that has no code of
    /// its own reports its nearest coded ancestor's code.
    ///
    /// Java declares a local `ExtendedTimeoutException extends TimeoutException`
    /// and checks `Errors.forException` walks up. Rust cannot subclass, and the
    /// walk happens once at translation time instead: an error class states the
    /// code its Java ancestry resolves to, and `BufferExhaustedException extends
    /// TimeoutException` is the in-tree instance of exactly Java's case — it has
    /// no `Errors` entry, so `Errors.forException` walks to `REQUEST_TIMED_OUT`,
    /// and [`ProducerBufferExhaustedError`] carries that code.
    #[test]
    fn errors_test_for_error_walks_the_superclass_chain() {
        use crate::common::Error;

        let parent = Error::timeout("late").error();
        let subclass = Error::buffer_exhausted("pool full").error();
        assert_eq!(subclass, parent, "the subclass must resolve to its superclass's code");
        assert_eq!(subclass, Errors::RequestTimedOut);
        // Both directions: the code still names the PARENT class, not the subclass.
        assert_eq!(Errors::RequestTimedOut.error_name(), Some("TimeoutError"));
    }

    /// `ErrorsTest.testForExceptionDefault`: a class with no code anywhere in its
    /// ancestry defaults to `UNKNOWN_SERVER_ERROR`.
    ///
    /// The bare `ApiException` is Java's own example, and it is the `ErrorCode`
    /// trait default in Rust. Checked here for the two families that take it:
    /// the code-less `KafkaException` subclasses and the `java.lang` errors.
    #[test]
    fn errors_test_for_error_defaults_to_unknown() {
        use crate::common::Error;
        use crate::common::errors::ApiError;

        assert_eq!(Error::Api(ApiError::new("generic")).error(), Errors::UnknownServerError);
        assert_eq!(Error::serialization("bad bytes").error(), Errors::UnknownServerError);
        assert_eq!(Error::wakeup("woken").error(), Errors::UnknownServerError);
        assert_eq!(Error::local_illegal_state("misuse").error(), Errors::UnknownServerError);
        // And the bare `KafkaException`, which is not even an `ApiException`.
        assert_eq!(Error::kafka("no code of its own").error(), Errors::UnknownServerError);
    }

    /// `ErrorsTest.testExceptionName`: the code reports the name of its class.
    ///
    /// Java asserts the fully qualified name
    /// (`"org.apache.kafka.common.errors.UnknownServerException"`). Rust has no
    /// Java package to report, so [`Errors::error_name`] returns the bare type
    /// name; the assertions below are Java's three, with the Rust names. The
    /// last one also pins the name against `Display`, which translates
    /// `Throwable.toString()` and so must use the same `getClass().getName()`.
    #[test]
    fn errors_test_error_name() {
        assert_eq!(Errors::UnknownServerError.error_name(), Some("UnknownServerError"));
        assert_eq!(Errors::None.error_name(), None);
        assert_eq!(Errors::InvalidTopicError.error_name(), Some("InvalidTopicError"));

        // `error_name()` and `Display`'s prefix are the same Java accessor.
        for error in all_codes() {
            if let Some(name) = error.error_name() {
                let rendered = error.error().expect("a named class is constructible").to_string();
                assert!(
                    rendered.starts_with(&format!("{name}:")),
                    "{error:?}: Display must open with the class name, got {rendered:?}"
                );
            }
        }
    }
}
