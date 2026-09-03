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

use std::fmt;

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

impl Errors {
    /// The error code for this error.
    pub fn code(&self) -> i16 {
        *self as i16
    }

    /// Builds a [`KafkaError`](crate::common::KafkaError) for this code, using
    /// `message` when the broker sent one and falling back to this code's own
    /// [`message`](Self::message) text when it did not.
    ///
    /// Corresponds to `Errors.exception(String message)`:
    ///
    /// ```java
    /// public ApiException exception(String message) {
    ///     if (message == null) {
    ///         // If no error message was specified, return an exception with the default error message.
    ///         return exception;
    ///     }
    ///     // Return an exception with the given error message.
    ///     return builder.apply(message);
    /// }
    /// ```
    ///
    /// The test is on nullness alone, so a broker that sends an empty but
    /// non-null message keeps that empty message, exactly as in Java. Note that
    /// `error_message.clone().unwrap_or_default()` is **not** equivalent: it
    /// turns a wire null into `Some("")`, which then shadows the default text and
    /// leaves the caller with a bare error code.
    pub(crate) fn exception(&self, message: Option<&str>) -> crate::common::KafkaError {
        match message {
            Some(message) => crate::common::KafkaError::with_message(*self, message),
            None => crate::common::KafkaError::new(*self),
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
            Self::ReplicaNotAvailable => "The replica is not available for the requested topic-partition.",
            Self::MessageTooLarge => {
                "The request included a message larger than the max message size the server will accept."
            },
            Self::StaleControllerEpoch => "The controller moved to another broker.",
            Self::OffsetMetadataTooLarge => "The metadata field of the offset request was too large.",
            Self::NetworkException => "The server disconnected before a response was received.",
            Self::CoordinatorLoadInProgress => "The coordinator is loading and hence can't process requests.",
            Self::CoordinatorNotAvailable => "The coordinator is not available.",
            Self::NotCoordinator => "This is not the correct coordinator.",
            Self::InvalidTopicException => "The request attempted to perform an operation on an invalid topic.",
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
            Self::UnknownProducerId => {
                "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception."
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
            Self::PrincipalDeserializationFailure => "Request principal deserialization failed during forwarding.",
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
            Self::UnreleasedInstanceId => "The instance ID is still used by another member in the consumer group.",
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
            Self::InvalidRecordState => "The record state is invalid.",
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

    /// Whether this error is retriable, i.e. whether it makes sense to retry a request
    /// that failed with this error.
    ///
    /// The retriable classification matches the Java client's exception hierarchy where
    /// exceptions extending `RetriableException` (directly or indirectly via
    /// `RefreshRetriableException` / `InvalidMetadataException`) are considered retriable.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::CorruptMessage
                | Self::UnknownTopicOrPartition
                | Self::LeaderNotAvailable
                | Self::NotLeaderOrFollower
                | Self::RequestTimedOut
                | Self::ReplicaNotAvailable
                | Self::NetworkException
                | Self::CoordinatorLoadInProgress
                | Self::CoordinatorNotAvailable
                | Self::NotCoordinator
                | Self::NotEnoughReplicas
                | Self::NotEnoughReplicasAfterAppend
                | Self::NotController
                | Self::KafkaStorageError
                | Self::FetchSessionIdNotFound
                | Self::FetchSessionTopicIdError
                | Self::InvalidFetchSessionEpoch
                | Self::ListenerNotFound
                | Self::FencedLeaderEpoch
                | Self::UnknownLeaderEpoch
                | Self::OffsetNotAvailable
                | Self::PreferredLeaderNotAvailable
                | Self::EligibleLeadersNotAvailable
                | Self::ElectionNotNeeded
                | Self::ConcurrentTransactions
                | Self::ThrottlingQuotaExceeded
                | Self::UnstableOffsetCommit
                | Self::UnknownTopicId
                | Self::InconsistentTopicId
                | Self::InvalidShareSessionEpoch
                | Self::ShareSessionNotFound
                | Self::ShareSessionLimitReached
        )
    }

    /// Whether this error corresponds to an `InvalidMetadataException` in Java,
    /// i.e. errors that indicate the client's cached metadata may be stale.
    ///
    /// The classification matches the Java client's exception hierarchy where
    /// exceptions extending `InvalidMetadataException` are considered invalid
    /// metadata errors.
    pub fn is_invalid_metadata(&self) -> bool {
        matches!(
            self,
            Self::UnknownTopicOrPartition
                | Self::LeaderNotAvailable
                | Self::NotLeaderOrFollower
                | Self::ReplicaNotAvailable
                | Self::ListenerNotFound
                | Self::FencedLeaderEpoch
                | Self::UnknownTopicId
                | Self::NetworkException
                | Self::KafkaStorageError
                | Self::InconsistentTopicId
                | Self::PreferredLeaderNotAvailable
                | Self::EligibleLeadersNotAvailable
                | Self::ElectionNotNeeded
        )
    }

    /// Whether the transaction must be aborted due to this error.
    ///
    /// Currently only `TransactionAbortable` requires a transaction abort.
    pub fn txn_requires_abort(&self) -> bool {
        matches!(self, Self::TransactionAbortable)
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
            13 => Self::NetworkException,
            14 => Self::CoordinatorLoadInProgress,
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            17 => Self::InvalidTopicException,
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

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::None {
            write!(f, "NONE")
        } else {
            write!(f, "{}", self.message())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(!Errors::None.is_retriable());
    }

    #[test]
    fn test_retriable_errors() {
        assert!(Errors::RequestTimedOut.is_retriable());
        assert!(Errors::LeaderNotAvailable.is_retriable());
        assert!(Errors::NotLeaderOrFollower.is_retriable());
        assert!(Errors::CoordinatorLoadInProgress.is_retriable());
        assert!(Errors::CoordinatorNotAvailable.is_retriable());
        assert!(Errors::NotCoordinator.is_retriable());
        assert!(Errors::NetworkException.is_retriable());
        assert!(Errors::NotEnoughReplicas.is_retriable());
        assert!(Errors::NotController.is_retriable());
        assert!(Errors::UnknownTopicOrPartition.is_retriable());
    }

    #[test]
    fn test_non_retriable_errors() {
        assert!(!Errors::UnknownServerError.is_retriable());
        assert!(!Errors::InvalidRequest.is_retriable());
        assert!(!Errors::UnsupportedVersion.is_retriable());
        assert!(!Errors::TopicAuthorizationFailed.is_retriable());
        assert!(!Errors::GroupAuthorizationFailed.is_retriable());
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

    #[test]
    fn exception_with_no_message_keeps_the_codes_own_text() {
        // Java: `Errors.exception(null)` returns the pre-built exception, whose
        // message is the code's default text.
        let error = Errors::NoReassignmentInProgress.exception(None);
        assert_eq!(error.error(), Errors::NoReassignmentInProgress);
        assert_eq!(error.message(), "No partition reassignment is in progress.");
        assert!(!error.message().is_empty());
    }

    #[test]
    fn exception_with_a_message_overrides_the_default_text() {
        let error = Errors::NoReassignmentInProgress.exception(Some("from the broker"));
        assert_eq!(error.error(), Errors::NoReassignmentInProgress);
        assert_eq!(error.message(), "from the broker");
    }

    #[test]
    fn exception_keeps_an_empty_but_present_message_empty() {
        // Java tests `message == null` only, so an empty but non-null message is
        // used verbatim. This is what distinguishes `exception(...)` from
        // `unwrap_or_default()`, which cannot tell the two apart and so silently
        // shadows the default text.
        assert_eq!(Errors::NoReassignmentInProgress.exception(Some("")).message(), "");
    }
}
