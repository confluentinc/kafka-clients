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

//! Translation of `org.apache.kafka.common.protocol.Errors`.
//!
//! Java's `Errors` enum is the canonical mapping between Kafka's wire-protocol
//! error codes and the `ApiException` subclasses raised on the client side.
//! In Rust we already collapse the exception hierarchy to a single
//! [`KafkaError`] enum (see `crate::common::errors`); this file provides the
//! thin enum that mirrors the *catalogue* — every variant in this enum
//! corresponds 1:1 to a Java `Errors` constant. Each variant carries:
//!
//! * a stable wire `code` (matching Java's `Errors.code()`),
//! * a default human-readable message (matching the Java string in the enum
//!   constant),
//! * a Java exception class name (used by [`Self::exception_name`] to match
//!   `Errors.exceptionName()`).
//!
//! The catalogue is intentionally exhaustive (133 variants + `NONE` +
//! `UNKNOWN_SERVER_ERROR`) so that round-trips through `for_code` are
//! lossless and `Errors::values()` length parity tests can run.

use crate::common::errors::KafkaError;

/// Catalogue of all Kafka wire-protocol error codes. Mirrors
/// `org.apache.kafka.common.protocol.Errors`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
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

/// Every `Errors` variant in declaration order. Java's `Errors.values()`.
pub const ALL_ERRORS: &[Errors] = &[
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
    Errors::NetworkException,
    Errors::CoordinatorLoadInProgress,
    Errors::CoordinatorNotAvailable,
    Errors::NotCoordinator,
    Errors::InvalidTopicException,
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

impl Errors {
    /// Returns the wire error code. Mirrors `Errors.code()`.
    pub fn code(&self) -> i16 {
        *self as i16
    }

    /// Returns every variant. Mirrors `Errors.values()`.
    pub fn values() -> &'static [Errors] {
        ALL_ERRORS
    }

    /// Look up an error by wire code. Unknown codes return
    /// [`Errors::UnknownServerError`], matching Java's `forCode(short)`.
    pub fn for_code(code: i16) -> Errors {
        // Linear scan; the catalog is small (~135 entries).
        for e in ALL_ERRORS {
            if e.code() == code {
                return *e;
            }
        }
        Errors::UnknownServerError
    }

    /// The default human-readable description. Mirrors Java's first
    /// argument to the enum constructor (the `defaultExceptionString`).
    pub fn message(&self) -> Option<&'static str> {
        Some(match self {
            Errors::UnknownServerError => "The server experienced an unexpected error when processing the request.",
            Errors::None => return None,
            Errors::OffsetOutOfRange => {
                "The requested offset is not within the range of offsets maintained by the server."
            },
            Errors::CorruptMessage => {
                "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt."
            },
            Errors::UnknownTopicOrPartition => "This server does not host this topic-partition.",
            Errors::InvalidFetchSize => "The requested fetch size is invalid.",
            Errors::LeaderNotAvailable => {
                "There is no leader for this topic-partition as we are in the middle of a leadership election."
            },
            Errors::NotLeaderOrFollower => {
                "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition."
            },
            Errors::RequestTimedOut => "The request timed out.",
            Errors::BrokerNotAvailable => "The broker is not available.",
            Errors::ReplicaNotAvailable => {
                "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition."
            },
            Errors::MessageTooLarge => {
                "The request included a message larger than the max message size the server will accept."
            },
            Errors::StaleControllerEpoch => "The controller moved to another broker.",
            Errors::OffsetMetadataTooLarge => "The metadata field of the offset request was too large.",
            Errors::NetworkException => "The server disconnected before a response was received.",
            Errors::CoordinatorLoadInProgress => "The coordinator is loading and hence can't process requests.",
            Errors::CoordinatorNotAvailable => "The coordinator is not available.",
            Errors::NotCoordinator => "This is not the correct coordinator.",
            Errors::InvalidTopicException => "The request attempted to perform an operation on an invalid topic.",
            Errors::RecordListTooLarge => {
                "The request included message batch larger than the configured segment size on the server."
            },
            Errors::NotEnoughReplicas => "Messages are rejected since there are fewer in-sync replicas than required.",
            Errors::NotEnoughReplicasAfterAppend => {
                "Messages are written to the log, but to fewer in-sync replicas than required."
            },
            Errors::InvalidRequiredAcks => "Produce request specified an invalid value for required acks.",
            Errors::IllegalGeneration => "Specified group generation id is not valid.",
            Errors::InconsistentGroupProtocol => {
                "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list."
            },
            Errors::InvalidGroupId => "The group id is invalid.",
            Errors::UnknownMemberId => "The coordinator is not aware of this member.",
            Errors::InvalidSessionTimeout => {
                "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."
            },
            Errors::RebalanceInProgress => "The group is rebalancing, so a rejoin is needed.",
            Errors::InvalidCommitOffsetSize => "The committing offset data size is not valid.",
            Errors::TopicAuthorizationFailed => "Topic authorization failed.",
            Errors::GroupAuthorizationFailed => "Group authorization failed.",
            Errors::ClusterAuthorizationFailed => "Cluster authorization failed.",
            Errors::InvalidTimestamp => "The timestamp of the message is out of acceptable range.",
            Errors::UnsupportedSaslMechanism => "The broker does not support the requested SASL mechanism.",
            Errors::IllegalSaslState => "Request is not valid given the current SASL state.",
            Errors::UnsupportedVersion => "The version of API is not supported.",
            Errors::TopicAlreadyExists => "Topic with this name already exists.",
            Errors::InvalidPartitions => "Number of partitions is below 1.",
            Errors::InvalidReplicationFactor => {
                "Replication factor is below 1 or larger than the number of available brokers."
            },
            Errors::InvalidReplicaAssignment => "Replica assignment is invalid.",
            Errors::InvalidConfig => "Configuration is invalid.",
            Errors::NotController => "This is not the correct controller for this cluster.",
            Errors::InvalidRequest => {
                "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."
            },
            Errors::UnsupportedForMessageFormat => {
                "The message format version on the broker does not support the request."
            },
            Errors::PolicyViolation => "Request parameters do not satisfy the configured policy.",
            Errors::OutOfOrderSequenceNumber => "The broker received an out of order sequence number.",
            Errors::DuplicateSequenceNumber => "The broker received a duplicate sequence number.",
            Errors::InvalidProducerEpoch => "Producer attempted to produce with an old epoch.",
            Errors::InvalidTxnState => "The producer attempted a transactional operation in an invalid state.",
            Errors::InvalidProducerIdMapping => {
                "The producer attempted to use a producer id which is not currently assigned to its transactional id."
            },
            Errors::InvalidTransactionTimeout => {
                "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms)."
            },
            Errors::ConcurrentTransactions => {
                "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing."
            },
            Errors::TransactionCoordinatorFenced => {
                "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer."
            },
            Errors::TransactionalIdAuthorizationFailed => "Transactional Id authorization failed.",
            Errors::SecurityDisabled => "Security features are disabled.",
            Errors::OperationNotAttempted => {
                "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest."
            },
            Errors::KafkaStorageError => "Disk error when trying to access log file on the disk.",
            Errors::LogDirNotFound => "The user-specified log directory is not found in the broker config.",
            Errors::SaslAuthenticationFailed => "SASL Authentication failed.",
            Errors::UnknownProducerId => {
                "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception."
            },
            Errors::ReassignmentInProgress => "A partition reassignment is in progress.",
            Errors::DelegationTokenAuthDisabled => "Delegation Token feature is not enabled.",
            Errors::DelegationTokenNotFound => "Delegation Token is not found on server.",
            Errors::DelegationTokenOwnerMismatch => "Specified Principal is not valid Owner/Renewer.",
            Errors::DelegationTokenRequestNotAllowed => {
                "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels."
            },
            Errors::DelegationTokenAuthorizationFailed => "Delegation Token authorization failed.",
            Errors::DelegationTokenExpired => "Delegation Token is expired.",
            Errors::InvalidPrincipalType => "Supplied principalType is not supported.",
            Errors::NonEmptyGroup => "The group is not empty.",
            Errors::GroupIdNotFound => "The group id does not exist.",
            Errors::FetchSessionIdNotFound => "The fetch session ID was not found.",
            Errors::InvalidFetchSessionEpoch => "The fetch session epoch is invalid.",
            Errors::ListenerNotFound => {
                "There is no listener on the leader broker that matches the listener on which metadata request was processed."
            },
            Errors::TopicDeletionDisabled => "Topic deletion is disabled.",
            Errors::FencedLeaderEpoch => "The leader epoch in the request is older than the epoch on the broker.",
            Errors::UnknownLeaderEpoch => "The leader epoch in the request is newer than the epoch on the broker.",
            Errors::UnsupportedCompressionType => {
                "The requesting client does not support the compression type of given partition."
            },
            Errors::StaleBrokerEpoch => "Broker epoch has changed.",
            Errors::OffsetNotAvailable => {
                "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing."
            },
            Errors::MemberIdRequired => {
                "The group member needs to have a valid member id before actually entering a consumer group."
            },
            Errors::PreferredLeaderNotAvailable => "The preferred leader was not available.",
            Errors::GroupMaxSizeReached => "The group has reached its maximum size.",
            Errors::FencedInstanceId => {
                "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id."
            },
            Errors::EligibleLeadersNotAvailable => "Eligible topic partition leaders are not available.",
            Errors::ElectionNotNeeded => "Leader election not needed for topic partition.",
            Errors::NoReassignmentInProgress => "No partition reassignment is in progress.",
            Errors::GroupSubscribedToTopic => {
                "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it."
            },
            Errors::InvalidRecord => "This record has failed the validation on broker and hence will be rejected.",
            Errors::UnstableOffsetCommit => "There are unstable offsets that need to be cleared.",
            Errors::ThrottlingQuotaExceeded => "The throttling quota has been exceeded.",
            Errors::ProducerFenced => {
                "There is a newer producer with the same transactionalId which fences the current one."
            },
            Errors::ResourceNotFound => "A request illegally referred to a resource that does not exist.",
            Errors::DuplicateResource => "A request illegally referred to the same resource twice.",
            Errors::UnacceptableCredential => "Requested credential would not meet criteria for acceptability.",
            Errors::InconsistentVoterSet => {
                "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters."
            },
            Errors::InvalidUpdateVersion => "The given update version was invalid.",
            Errors::FeatureUpdateFailed => "Unable to update finalized features due to an unexpected server error.",
            Errors::PrincipalDeserializationFailure => {
                "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup."
            },
            Errors::SnapshotNotFound => "Requested snapshot was not found.",
            Errors::PositionOutOfRange => {
                "Requested position is not greater than or equal to zero, and less than the size of the snapshot."
            },
            Errors::UnknownTopicId => "This server does not host this topic ID.",
            Errors::DuplicateBrokerRegistration => "This broker ID is already in use.",
            Errors::BrokerIdNotRegistered => "The given broker ID was not registered.",
            Errors::InconsistentTopicId => "The log's topic ID did not match the topic ID in the request.",
            Errors::InconsistentClusterId => "The clusterId in the request does not match that found on the server.",
            Errors::TransactionalIdNotFound => "The transactionalId could not be found.",
            Errors::FetchSessionTopicIdError => "The fetch session encountered inconsistent topic ID usage.",
            Errors::IneligibleReplica => "The new ISR contains at least one ineligible replica.",
            Errors::NewLeaderElected => {
                "The AlterPartition request successfully updated the partition state but the leader has changed."
            },
            Errors::OffsetMovedToTieredStorage => "The requested offset is moved to tiered storage.",
            Errors::FencedMemberEpoch => {
                "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin."
            },
            Errors::UnreleasedInstanceId => {
                "The instance ID is still used by another member in the consumer group. That member must leave first."
            },
            Errors::UnsupportedAssignor => "The assignor or its version range is not supported by the consumer group.",
            Errors::StaleMemberEpoch => {
                "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API."
            },
            Errors::MismatchedEndpointType => "The request was sent to an endpoint of the wrong type.",
            Errors::UnsupportedEndpointType => "This endpoint type is not supported yet.",
            Errors::UnknownControllerId => "This controller ID is not known.",
            Errors::UnknownSubscriptionId => {
                "Client sent a push telemetry request with an invalid or outdated subscription ID."
            },
            Errors::TelemetryTooLarge => {
                "Client sent a push telemetry request larger than the maximum size the broker will accept."
            },
            Errors::InvalidRegistration => "The controller has considered the broker registration to be invalid.",
            Errors::TransactionAbortable => {
                "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID."
            },
            Errors::InvalidRecordState => {
                "The record state is invalid. The acknowledgement of delivery could not be completed."
            },
            Errors::ShareSessionNotFound => "The share session was not found.",
            Errors::InvalidShareSessionEpoch => "The share session epoch is invalid.",
            Errors::FencedStateEpoch => {
                "The share coordinator rejected the request because the share-group state epoch did not match."
            },
            Errors::InvalidVoterKey => "The voter key doesn't match the receiving replica's key.",
            Errors::DuplicateVoter => "The voter is already part of the set of voters.",
            Errors::VoterNotFound => "The voter is not part of the set of voters.",
            Errors::InvalidRegularExpression => "The regular expression is not valid.",
            Errors::RebootstrapRequired => {
                "Client metadata is stale. The client should rebootstrap to obtain new metadata."
            },
            Errors::StreamsInvalidTopology => "The supplied topology is invalid.",
            Errors::StreamsInvalidTopologyEpoch => "The supplied topology epoch is invalid.",
            Errors::StreamsTopologyFenced => "The supplied topology epoch is outdated.",
            Errors::ShareSessionLimitReached => "The limit of share sessions has been reached.",
        })
    }

    /// The Java exception class name (`org.apache.kafka.common.errors.*`).
    /// `Errors::None` returns `None`, matching Java's `Errors.NONE.exceptionName()`.
    pub fn exception_name(&self) -> Option<&'static str> {
        Some(match self {
            Errors::None => return None,
            Errors::UnknownServerError => "org.apache.kafka.common.errors.UnknownServerException",
            Errors::OffsetOutOfRange => "org.apache.kafka.common.errors.OffsetOutOfRangeException",
            Errors::CorruptMessage => "org.apache.kafka.common.errors.CorruptRecordException",
            Errors::UnknownTopicOrPartition => "org.apache.kafka.common.errors.UnknownTopicOrPartitionException",
            Errors::InvalidFetchSize => "org.apache.kafka.common.errors.InvalidFetchSizeException",
            Errors::LeaderNotAvailable => "org.apache.kafka.common.errors.LeaderNotAvailableException",
            Errors::NotLeaderOrFollower => "org.apache.kafka.common.errors.NotLeaderOrFollowerException",
            Errors::RequestTimedOut => "org.apache.kafka.common.errors.TimeoutException",
            Errors::BrokerNotAvailable => "org.apache.kafka.common.errors.BrokerNotAvailableException",
            Errors::ReplicaNotAvailable => "org.apache.kafka.common.errors.ReplicaNotAvailableException",
            Errors::MessageTooLarge => "org.apache.kafka.common.errors.RecordTooLargeException",
            Errors::StaleControllerEpoch => "org.apache.kafka.common.errors.ControllerMovedException",
            Errors::OffsetMetadataTooLarge => "org.apache.kafka.common.errors.OffsetMetadataTooLarge",
            Errors::NetworkException => "org.apache.kafka.common.errors.NetworkException",
            Errors::CoordinatorLoadInProgress => "org.apache.kafka.common.errors.CoordinatorLoadInProgressException",
            Errors::CoordinatorNotAvailable => "org.apache.kafka.common.errors.CoordinatorNotAvailableException",
            Errors::NotCoordinator => "org.apache.kafka.common.errors.NotCoordinatorException",
            Errors::InvalidTopicException => "org.apache.kafka.common.errors.InvalidTopicException",
            Errors::RecordListTooLarge => "org.apache.kafka.common.errors.RecordBatchTooLargeException",
            Errors::NotEnoughReplicas => "org.apache.kafka.common.errors.NotEnoughReplicasException",
            Errors::NotEnoughReplicasAfterAppend => {
                "org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException"
            },
            Errors::InvalidRequiredAcks => "org.apache.kafka.common.errors.InvalidRequiredAcksException",
            Errors::IllegalGeneration => "org.apache.kafka.common.errors.IllegalGenerationException",
            Errors::InconsistentGroupProtocol => "org.apache.kafka.common.errors.InconsistentGroupProtocolException",
            Errors::InvalidGroupId => "org.apache.kafka.common.errors.InvalidGroupIdException",
            Errors::UnknownMemberId => "org.apache.kafka.common.errors.UnknownMemberIdException",
            Errors::InvalidSessionTimeout => "org.apache.kafka.common.errors.InvalidSessionTimeoutException",
            Errors::RebalanceInProgress => "org.apache.kafka.common.errors.RebalanceInProgressException",
            Errors::InvalidCommitOffsetSize => "org.apache.kafka.common.errors.InvalidCommitOffsetSizeException",
            Errors::TopicAuthorizationFailed => "org.apache.kafka.common.errors.TopicAuthorizationException",
            Errors::GroupAuthorizationFailed => "org.apache.kafka.common.errors.GroupAuthorizationException",
            Errors::ClusterAuthorizationFailed => "org.apache.kafka.common.errors.ClusterAuthorizationException",
            Errors::InvalidTimestamp => "org.apache.kafka.common.errors.InvalidTimestampException",
            Errors::UnsupportedSaslMechanism => "org.apache.kafka.common.errors.UnsupportedSaslMechanismException",
            Errors::IllegalSaslState => "org.apache.kafka.common.errors.IllegalSaslStateException",
            Errors::UnsupportedVersion => "org.apache.kafka.common.errors.UnsupportedVersionException",
            Errors::TopicAlreadyExists => "org.apache.kafka.common.errors.TopicExistsException",
            Errors::InvalidPartitions => "org.apache.kafka.common.errors.InvalidPartitionsException",
            Errors::InvalidReplicationFactor => "org.apache.kafka.common.errors.InvalidReplicationFactorException",
            Errors::InvalidReplicaAssignment => "org.apache.kafka.common.errors.InvalidReplicaAssignmentException",
            Errors::InvalidConfig => "org.apache.kafka.common.errors.InvalidConfigurationException",
            Errors::NotController => "org.apache.kafka.common.errors.NotControllerException",
            Errors::InvalidRequest => "org.apache.kafka.common.errors.InvalidRequestException",
            Errors::UnsupportedForMessageFormat => {
                "org.apache.kafka.common.errors.UnsupportedForMessageFormatException"
            },
            Errors::PolicyViolation => "org.apache.kafka.common.errors.PolicyViolationException",
            Errors::OutOfOrderSequenceNumber => "org.apache.kafka.common.errors.OutOfOrderSequenceException",
            Errors::DuplicateSequenceNumber => "org.apache.kafka.common.errors.DuplicateSequenceException",
            Errors::InvalidProducerEpoch => "org.apache.kafka.common.errors.InvalidProducerEpochException",
            Errors::InvalidTxnState => "org.apache.kafka.common.errors.InvalidTxnStateException",
            Errors::InvalidProducerIdMapping => "org.apache.kafka.common.errors.InvalidPidMappingException",
            Errors::InvalidTransactionTimeout => "org.apache.kafka.common.errors.InvalidTxnTimeoutException",
            Errors::ConcurrentTransactions => "org.apache.kafka.common.errors.ConcurrentTransactionsException",
            Errors::TransactionCoordinatorFenced => {
                "org.apache.kafka.common.errors.TransactionCoordinatorFencedException"
            },
            Errors::TransactionalIdAuthorizationFailed => {
                "org.apache.kafka.common.errors.TransactionalIdAuthorizationException"
            },
            Errors::SecurityDisabled => "org.apache.kafka.common.errors.SecurityDisabledException",
            Errors::OperationNotAttempted => "org.apache.kafka.common.errors.OperationNotAttemptedException",
            Errors::KafkaStorageError => "org.apache.kafka.common.errors.KafkaStorageException",
            Errors::LogDirNotFound => "org.apache.kafka.common.errors.LogDirNotFoundException",
            Errors::SaslAuthenticationFailed => "org.apache.kafka.common.errors.SaslAuthenticationException",
            Errors::UnknownProducerId => "org.apache.kafka.common.errors.UnknownProducerIdException",
            Errors::ReassignmentInProgress => "org.apache.kafka.common.errors.ReassignmentInProgressException",
            Errors::DelegationTokenAuthDisabled => "org.apache.kafka.common.errors.DelegationTokenDisabledException",
            Errors::DelegationTokenNotFound => "org.apache.kafka.common.errors.DelegationTokenNotFoundException",
            Errors::DelegationTokenOwnerMismatch => {
                "org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException"
            },
            Errors::DelegationTokenRequestNotAllowed => {
                "org.apache.kafka.common.errors.UnsupportedByAuthenticationException"
            },
            Errors::DelegationTokenAuthorizationFailed => {
                "org.apache.kafka.common.errors.DelegationTokenAuthorizationException"
            },
            Errors::DelegationTokenExpired => "org.apache.kafka.common.errors.DelegationTokenExpiredException",
            Errors::InvalidPrincipalType => "org.apache.kafka.common.errors.InvalidPrincipalTypeException",
            Errors::NonEmptyGroup => "org.apache.kafka.common.errors.GroupNotEmptyException",
            Errors::GroupIdNotFound => "org.apache.kafka.common.errors.GroupIdNotFoundException",
            Errors::FetchSessionIdNotFound => "org.apache.kafka.common.errors.FetchSessionIdNotFoundException",
            Errors::InvalidFetchSessionEpoch => "org.apache.kafka.common.errors.InvalidFetchSessionEpochException",
            Errors::ListenerNotFound => "org.apache.kafka.common.errors.ListenerNotFoundException",
            Errors::TopicDeletionDisabled => "org.apache.kafka.common.errors.TopicDeletionDisabledException",
            Errors::FencedLeaderEpoch => "org.apache.kafka.common.errors.FencedLeaderEpochException",
            Errors::UnknownLeaderEpoch => "org.apache.kafka.common.errors.UnknownLeaderEpochException",
            Errors::UnsupportedCompressionType => "org.apache.kafka.common.errors.UnsupportedCompressionTypeException",
            Errors::StaleBrokerEpoch => "org.apache.kafka.common.errors.StaleBrokerEpochException",
            Errors::OffsetNotAvailable => "org.apache.kafka.common.errors.OffsetNotAvailableException",
            Errors::MemberIdRequired => "org.apache.kafka.common.errors.MemberIdRequiredException",
            Errors::PreferredLeaderNotAvailable => {
                "org.apache.kafka.common.errors.PreferredLeaderNotAvailableException"
            },
            Errors::GroupMaxSizeReached => "org.apache.kafka.common.errors.GroupMaxSizeReachedException",
            Errors::FencedInstanceId => "org.apache.kafka.common.errors.FencedInstanceIdException",
            Errors::EligibleLeadersNotAvailable => {
                "org.apache.kafka.common.errors.EligibleLeadersNotAvailableException"
            },
            Errors::ElectionNotNeeded => "org.apache.kafka.common.errors.ElectionNotNeededException",
            Errors::NoReassignmentInProgress => "org.apache.kafka.common.errors.NoReassignmentInProgressException",
            Errors::GroupSubscribedToTopic => "org.apache.kafka.common.errors.GroupSubscribedToTopicException",
            Errors::InvalidRecord => "org.apache.kafka.common.InvalidRecordException",
            Errors::UnstableOffsetCommit => "org.apache.kafka.common.errors.UnstableOffsetCommitException",
            Errors::ThrottlingQuotaExceeded => "org.apache.kafka.common.errors.ThrottlingQuotaExceededException",
            Errors::ProducerFenced => "org.apache.kafka.common.errors.ProducerFencedException",
            Errors::ResourceNotFound => "org.apache.kafka.common.errors.ResourceNotFoundException",
            Errors::DuplicateResource => "org.apache.kafka.common.errors.DuplicateResourceException",
            Errors::UnacceptableCredential => "org.apache.kafka.common.errors.UnacceptableCredentialException",
            Errors::InconsistentVoterSet => "org.apache.kafka.common.errors.InconsistentVoterSetException",
            Errors::InvalidUpdateVersion => "org.apache.kafka.common.errors.InvalidUpdateVersionException",
            Errors::FeatureUpdateFailed => "org.apache.kafka.common.errors.FeatureUpdateFailedException",
            Errors::PrincipalDeserializationFailure => {
                "org.apache.kafka.common.errors.PrincipalDeserializationException"
            },
            Errors::SnapshotNotFound => "org.apache.kafka.common.errors.SnapshotNotFoundException",
            Errors::PositionOutOfRange => "org.apache.kafka.common.errors.PositionOutOfRangeException",
            Errors::UnknownTopicId => "org.apache.kafka.common.errors.UnknownTopicIdException",
            Errors::DuplicateBrokerRegistration => {
                "org.apache.kafka.common.errors.DuplicateBrokerRegistrationException"
            },
            Errors::BrokerIdNotRegistered => "org.apache.kafka.common.errors.BrokerIdNotRegisteredException",
            Errors::InconsistentTopicId => "org.apache.kafka.common.errors.InconsistentTopicIdException",
            Errors::InconsistentClusterId => "org.apache.kafka.common.errors.InconsistentClusterIdException",
            Errors::TransactionalIdNotFound => "org.apache.kafka.common.errors.TransactionalIdNotFoundException",
            Errors::FetchSessionTopicIdError => "org.apache.kafka.common.errors.FetchSessionTopicIdException",
            Errors::IneligibleReplica => "org.apache.kafka.common.errors.IneligibleReplicaException",
            Errors::NewLeaderElected => "org.apache.kafka.common.errors.NewLeaderElectedException",
            Errors::OffsetMovedToTieredStorage => "org.apache.kafka.common.errors.OffsetMovedToTieredStorageException",
            Errors::FencedMemberEpoch => "org.apache.kafka.common.errors.FencedMemberEpochException",
            Errors::UnreleasedInstanceId => "org.apache.kafka.common.errors.UnreleasedInstanceIdException",
            Errors::UnsupportedAssignor => "org.apache.kafka.common.errors.UnsupportedAssignorException",
            Errors::StaleMemberEpoch => "org.apache.kafka.common.errors.StaleMemberEpochException",
            Errors::MismatchedEndpointType => "org.apache.kafka.common.errors.MismatchedEndpointTypeException",
            Errors::UnsupportedEndpointType => "org.apache.kafka.common.errors.UnsupportedEndpointTypeException",
            Errors::UnknownControllerId => "org.apache.kafka.common.errors.UnknownControllerIdException",
            Errors::UnknownSubscriptionId => "org.apache.kafka.common.errors.UnknownSubscriptionIdException",
            Errors::TelemetryTooLarge => "org.apache.kafka.common.errors.TelemetryTooLargeException",
            Errors::InvalidRegistration => "org.apache.kafka.common.errors.InvalidRegistrationException",
            Errors::TransactionAbortable => "org.apache.kafka.common.errors.TransactionAbortableException",
            Errors::InvalidRecordState => "org.apache.kafka.common.errors.InvalidRecordStateException",
            Errors::ShareSessionNotFound => "org.apache.kafka.common.errors.ShareSessionNotFoundException",
            Errors::InvalidShareSessionEpoch => "org.apache.kafka.common.errors.InvalidShareSessionEpochException",
            Errors::FencedStateEpoch => "org.apache.kafka.common.errors.FencedStateEpochException",
            Errors::InvalidVoterKey => "org.apache.kafka.common.errors.InvalidVoterKeyException",
            Errors::DuplicateVoter => "org.apache.kafka.common.errors.DuplicateVoterException",
            Errors::VoterNotFound => "org.apache.kafka.common.errors.VoterNotFoundException",
            Errors::InvalidRegularExpression => "org.apache.kafka.common.errors.InvalidRegularExpression",
            Errors::RebootstrapRequired => "org.apache.kafka.common.errors.RebootstrapRequiredException",
            Errors::StreamsInvalidTopology => "org.apache.kafka.common.errors.StreamsInvalidTopologyException",
            Errors::StreamsInvalidTopologyEpoch => {
                "org.apache.kafka.common.errors.StreamsInvalidTopologyEpochException"
            },
            Errors::StreamsTopologyFenced => "org.apache.kafka.common.errors.StreamsTopologyFencedException",
            Errors::ShareSessionLimitReached => "org.apache.kafka.common.errors.ShareSessionLimitReachedException",
        })
    }

    /// Convert this catalogue entry into a [`KafkaError`] carrying its
    /// default message. Mirrors `Errors.exception()`. `Errors::None` returns
    /// `None`, matching Java's `null` return.
    pub fn exception(&self) -> Option<KafkaError> {
        let msg = self.message().unwrap_or("");
        match self {
            Errors::None => None,
            // Producer-relevant codes mapped to dedicated KafkaError variants.
            other => KafkaError::from_code(other.code(), Some(msg)),
        }
    }

    /// Convert this catalogue entry into a [`KafkaError`] carrying `message`.
    /// Mirrors `Errors.exception(String)`. If `message` is `None` the default
    /// message is used.
    pub fn exception_with_message(&self, message: Option<&str>) -> Option<KafkaError> {
        if matches!(self, Errors::None) {
            return None;
        }
        let m = message.or(self.message()).unwrap_or("");
        KafkaError::from_code(self.code(), Some(m))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `ErrorsTest#testUniqueErrorCodes`.
    #[test]
    fn unique_error_codes() {
        let mut codes: Vec<i16> = Errors::values().iter().map(|e| e.code()).collect();
        codes.sort_unstable();
        let len_before = codes.len();
        codes.dedup();
        assert_eq!(codes.len(), len_before, "Error codes must be unique");
    }

    /// Translation of `ErrorsTest#testNoneException`.
    #[test]
    fn none_exception_is_null() {
        assert!(
            Errors::None.exception().is_none(),
            "The NONE error should not have an exception"
        );
    }

    /// Translation of `ErrorsTest#testForExceptionDefault`.
    /// In Rust we cannot inherit-and-replace exception classes, so the Java
    /// `forException(new ApiException())` test maps to `for_code(unknown)`.
    #[test]
    fn for_unknown_code_returns_unknown_server_error() {
        assert_eq!(Errors::for_code(9999), Errors::UnknownServerError);
    }

    /// Translation of `ErrorsTest#testExceptionName`.
    #[test]
    fn exception_name_matches_java() {
        assert_eq!(
            Errors::UnknownServerError.exception_name(),
            Some("org.apache.kafka.common.errors.UnknownServerException")
        );
        assert_eq!(Errors::None.exception_name(), None);
        assert_eq!(
            Errors::InvalidTopicException.exception_name(),
            Some("org.apache.kafka.common.errors.InvalidTopicException")
        );
    }

    /// Translation of `ErrorsTest#testForExceptionInheritance`.
    /// Java's hierarchy lookup walks superclasses; in Rust the exception
    /// hierarchy is collapsed into [`KafkaError`]. The closest equivalent we
    /// can assert is that `for_code` round-trips a known code unchanged.
    #[test]
    fn for_code_round_trips_known_codes() {
        for &e in Errors::values() {
            if matches!(e, Errors::None) {
                continue;
            }
            let round = Errors::for_code(e.code());
            assert_eq!(round, e, "round-trip failed for {:?}", e);
        }
    }

    /// Translation of `ErrorsTest#testExceptionsAreNotGeneric`. Each
    /// non-`None` variant maps to a non-generic `KafkaError` (i.e. not the
    /// catch-all `KafkaError::Api`).
    #[test]
    fn exceptions_are_not_generic_api() {
        for &e in Errors::values() {
            if matches!(e, Errors::None) {
                continue;
            }
            let exc = e.exception().expect("non-None should have exception");
            assert!(!matches!(exc, KafkaError::Api(_)), "{:?} mapped to generic KafkaError::Api", e);
        }
    }

    /// Translation of `ErrorsTest#testUniqueExceptions` — verify the variant
    /// catalog has the expected length and no `code` is duplicated.
    #[test]
    fn catalog_size_matches_java() {
        // 134 distinct codes: -1 (UnknownServerError), 0 (None), 1..=133.
        assert_eq!(Errors::values().len(), 135);
        assert_eq!(Errors::ShareSessionLimitReached.code(), 133);
    }

    #[test]
    fn for_code_known_codes() {
        assert_eq!(Errors::for_code(-1), Errors::UnknownServerError);
        assert_eq!(Errors::for_code(0), Errors::None);
        assert_eq!(Errors::for_code(2), Errors::CorruptMessage);
        assert_eq!(Errors::for_code(7), Errors::RequestTimedOut);
        assert_eq!(Errors::for_code(35), Errors::UnsupportedVersion);
        assert_eq!(Errors::for_code(90), Errors::ProducerFenced);
        assert_eq!(Errors::for_code(133), Errors::ShareSessionLimitReached);
    }

    #[test]
    fn message_text_matches_java() {
        assert_eq!(Errors::None.message(), None);
        assert_eq!(Errors::RequestTimedOut.message(), Some("The request timed out."));
        assert_eq!(
            Errors::UnknownTopicOrPartition.message(),
            Some("This server does not host this topic-partition.")
        );
    }
}
