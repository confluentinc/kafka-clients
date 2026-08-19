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

//! Kafka's exception classes.
//!
//! Translates `org.apache.kafka.common.errors`. Each Java exception class is one
//! struct in its own file, named with the `Exception` -> `Error` substitution of
//! CLAUDE.md §2, and declares its `extends` chain through its
//! [`ErrorHierarchy`](crate::common::kafka_error::ErrorHierarchy) impl rather
//! than through a predicate that enumerates it from the outside.
//!
//! The error code for each class lives in its
//! [`ErrorCode`](crate::common::kafka_error::ErrorCode) impl — Java keeps the
//! same association in `Errors`' `CLASS_TO_ERROR` map — while the default
//! message strings stay in [`Errors`](crate::common::protocol::Errors), exactly
//! as Java stores them on the enum constant and passes them to the class's
//! constructor.

pub mod api_error;
pub mod authentication_error;
pub mod authorization_error;
pub mod authorizer_not_ready_error;
pub mod broker_id_not_registered_error;
pub mod broker_not_available_error;
pub mod cluster_authorization_error;
pub mod concurrent_transactions_error;
pub mod controller_moved_error;
pub mod coordinator_load_in_progress_error;
pub mod coordinator_not_available_error;
pub mod corrupt_record_error;
pub mod delegation_token_authorization_error;
pub mod delegation_token_disabled_error;
pub mod delegation_token_expired_error;
pub mod delegation_token_not_found_error;
pub mod delegation_token_owner_mismatch_error;
pub mod disconnect_error;
pub mod duplicate_broker_registration_error;
pub mod duplicate_resource_error;
pub mod duplicate_sequence_error;
pub mod duplicate_voter_error;
pub mod election_not_needed_error;
pub mod eligible_leaders_not_available_error;
pub mod feature_update_failed_error;
pub mod fenced_instance_id_error;
pub mod fenced_leader_epoch_error;
pub mod fenced_member_epoch_error;
pub mod fenced_state_epoch_error;
pub mod fetch_session_id_not_found_error;
pub mod fetch_session_topic_id_error;
pub mod group_authorization_error;
pub mod group_id_not_found_error;
pub mod group_max_size_reached_error;
pub mod group_not_empty_error;
pub mod group_subscribed_to_topic_error;
pub mod illegal_generation_error;
pub mod illegal_sasl_state_error;
pub mod inconsistent_cluster_id_error;
pub mod inconsistent_group_protocol_error;
pub mod inconsistent_topic_id_error;
pub mod inconsistent_voter_set_error;
pub mod ineligible_replica_error;
pub mod interrupt_error;
pub mod invalid_commit_offset_size_error;
pub mod invalid_configuration_error;
pub mod invalid_fetch_session_epoch_error;
pub mod invalid_fetch_size_error;
pub mod invalid_group_id_error;
pub mod invalid_offset_error;
pub mod invalid_partitions_error;
pub mod invalid_pid_mapping_error;
pub mod invalid_principal_type_error;
pub mod invalid_producer_epoch_error;
pub mod invalid_record_state_error;
pub mod invalid_registration_error;
pub mod invalid_regular_expression_error;
pub mod invalid_replica_assignment_error;
pub mod invalid_replication_factor_error;
pub mod invalid_request_error;
pub mod invalid_required_acks_error;
pub mod invalid_session_timeout_error;
pub mod invalid_share_session_epoch_error;
pub mod invalid_timestamp_error;
pub mod invalid_topic_error;
pub mod invalid_txn_state_error;
pub mod invalid_txn_timeout_error;
pub mod invalid_update_version_error;
pub mod invalid_voter_key_error;
pub mod kafka_storage_error;
pub mod leader_not_available_error;
pub mod listener_not_found_error;
pub mod log_dir_not_found_error;
pub mod member_id_required_error;
pub mod mismatched_endpoint_type_error;
pub mod network_error;
pub mod new_leader_elected_error;
pub mod no_reassignment_in_progress_error;
pub mod not_controller_error;
pub mod not_coordinator_error;
pub mod not_enough_replicas_after_append_error;
pub mod not_enough_replicas_error;
pub mod not_leader_or_follower_error;
pub mod offset_metadata_too_large_error;
pub mod offset_moved_to_tiered_storage_error;
pub mod offset_not_available_error;
pub mod offset_out_of_range_error;
pub mod operation_not_attempted_error;
pub mod out_of_order_sequence_error;
pub mod policy_violation_error;
pub mod position_out_of_range_error;
pub mod preferred_leader_not_available_error;
pub mod principal_deserialization_error;
pub mod producer_fenced_error;
pub mod reassignment_in_progress_error;
pub mod rebalance_in_progress_error;
pub mod rebootstrap_required_error;
pub mod record_batch_too_large_error;
pub mod record_deserialization_error;
pub mod record_too_large_error;
pub mod replica_not_available_error;
pub mod resource_not_found_error;
pub mod sasl_authentication_error;
pub mod security_disabled_error;
pub mod serialization_error;
pub mod share_session_limit_reached_error;
pub mod share_session_not_found_error;
pub mod snapshot_not_found_error;
pub mod ssl_authentication_error;
pub mod stale_broker_epoch_error;
pub mod stale_member_epoch_error;
pub mod streams_invalid_topology_epoch_error;
pub mod streams_invalid_topology_error;
pub mod streams_topology_fenced_error;
pub mod telemetry_too_large_error;
pub mod throttling_quota_exceeded_error;
pub mod timeout_error;
pub mod topic_authorization_error;
pub mod topic_deletion_disabled_error;
pub mod topic_exists_error;
pub mod transaction_abortable_error;
pub mod transaction_aborted_error;
pub mod transaction_coordinator_fenced_error;
pub mod transactional_id_authorization_error;
pub mod transactional_id_not_found_error;
pub mod unacceptable_credential_error;
pub mod unknown_controller_id_error;
pub mod unknown_leader_epoch_error;
pub mod unknown_member_id_error;
pub mod unknown_producer_id_error;
pub mod unknown_server_error;
pub mod unknown_subscription_id_error;
pub mod unknown_topic_id_error;
pub mod unknown_topic_or_partition_error;
pub mod unreleased_instance_id_error;
pub mod unstable_offset_commit_error;
pub mod unsupported_assignor_error;
pub mod unsupported_by_authentication_error;
pub mod unsupported_compression_type_error;
pub mod unsupported_endpoint_type_error;
pub mod unsupported_for_message_format_error;
pub mod unsupported_sasl_mechanism_error;
pub mod unsupported_version_error;
pub mod voter_not_found_error;
pub mod wakeup_error;

/// Render a set of strings the way Java's `AbstractCollection.toString()` does —
/// `[a, b]`, elements unquoted and comma-space separated — so the messages of
/// [`TopicAuthorizationError`](topic_authorization_error::TopicAuthorizationError)
/// and [`InvalidTopicError`](invalid_topic_error::InvalidTopicError) match Java's
/// `getMessage()` format. Rust's `HashSet` `Debug` would instead print
/// `{"a", "b"}`. Iteration order is unspecified, exactly as for a Java `HashSet`.
fn format_java_set(set: &std::collections::HashSet<String>) -> String {
    let mut out = String::from("[");
    for (i, item) in set.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(item);
    }
    out.push(']');
    out
}

pub use api_error::ApiError;
pub use authentication_error::AuthenticationError;
pub use authorization_error::AuthorizationError;
pub use authorizer_not_ready_error::AuthorizerNotReadyError;
pub use broker_id_not_registered_error::BrokerIdNotRegisteredError;
pub use broker_not_available_error::BrokerNotAvailableError;
pub use cluster_authorization_error::ClusterAuthorizationError;
pub use concurrent_transactions_error::ConcurrentTransactionsError;
pub use controller_moved_error::ControllerMovedError;
pub use coordinator_load_in_progress_error::CoordinatorLoadInProgressError;
pub use coordinator_not_available_error::CoordinatorNotAvailableError;
pub use corrupt_record_error::CorruptRecordError;
pub use delegation_token_authorization_error::DelegationTokenAuthorizationError;
pub use delegation_token_disabled_error::DelegationTokenDisabledError;
pub use delegation_token_expired_error::DelegationTokenExpiredError;
pub use delegation_token_not_found_error::DelegationTokenNotFoundError;
pub use delegation_token_owner_mismatch_error::DelegationTokenOwnerMismatchError;
pub use disconnect_error::DisconnectError;
pub use duplicate_broker_registration_error::DuplicateBrokerRegistrationError;
pub use duplicate_resource_error::DuplicateResourceError;
pub use duplicate_sequence_error::DuplicateSequenceError;
pub use duplicate_voter_error::DuplicateVoterError;
pub use election_not_needed_error::ElectionNotNeededError;
pub use eligible_leaders_not_available_error::EligibleLeadersNotAvailableError;
pub use feature_update_failed_error::FeatureUpdateFailedError;
pub use fenced_instance_id_error::FencedInstanceIdError;
pub use fenced_leader_epoch_error::FencedLeaderEpochError;
pub use fenced_member_epoch_error::FencedMemberEpochError;
pub use fenced_state_epoch_error::FencedStateEpochError;
pub use fetch_session_id_not_found_error::FetchSessionIdNotFoundError;
pub use fetch_session_topic_id_error::FetchSessionTopicIdError;
pub use group_authorization_error::GroupAuthorizationError;
pub use group_id_not_found_error::GroupIdNotFoundError;
pub use group_max_size_reached_error::GroupMaxSizeReachedError;
pub use group_not_empty_error::GroupNotEmptyError;
pub use group_subscribed_to_topic_error::GroupSubscribedToTopicError;
pub use illegal_generation_error::IllegalGenerationError;
pub use illegal_sasl_state_error::IllegalSaslStateError;
pub use inconsistent_cluster_id_error::InconsistentClusterIdError;
pub use inconsistent_group_protocol_error::InconsistentGroupProtocolError;
pub use inconsistent_topic_id_error::InconsistentTopicIdError;
pub use inconsistent_voter_set_error::InconsistentVoterSetError;
pub use ineligible_replica_error::IneligibleReplicaError;
pub use interrupt_error::InterruptError;
pub use invalid_commit_offset_size_error::InvalidCommitOffsetSizeError;
pub use invalid_configuration_error::InvalidConfigurationError;
pub use invalid_fetch_session_epoch_error::InvalidFetchSessionEpochError;
pub use invalid_fetch_size_error::InvalidFetchSizeError;
pub use invalid_group_id_error::InvalidGroupIdError;
pub use invalid_offset_error::InvalidOffsetError;
pub use invalid_partitions_error::InvalidPartitionsError;
pub use invalid_pid_mapping_error::InvalidPidMappingError;
pub use invalid_principal_type_error::InvalidPrincipalTypeError;
pub use invalid_producer_epoch_error::InvalidProducerEpochError;
pub use invalid_record_state_error::InvalidRecordStateError;
pub use invalid_registration_error::InvalidRegistrationError;
pub use invalid_regular_expression_error::InvalidRegularExpressionError;
pub use invalid_replica_assignment_error::InvalidReplicaAssignmentError;
pub use invalid_replication_factor_error::InvalidReplicationFactorError;
pub use invalid_request_error::InvalidRequestError;
pub use invalid_required_acks_error::InvalidRequiredAcksError;
pub use invalid_session_timeout_error::InvalidSessionTimeoutError;
pub use invalid_share_session_epoch_error::InvalidShareSessionEpochError;
pub use invalid_timestamp_error::InvalidTimestampError;
pub use invalid_topic_error::InvalidTopicError;
pub use invalid_txn_state_error::InvalidTxnStateError;
pub use invalid_txn_timeout_error::InvalidTxnTimeoutError;
pub use invalid_update_version_error::InvalidUpdateVersionError;
pub use invalid_voter_key_error::InvalidVoterKeyError;
pub use kafka_storage_error::KafkaStorageError;
pub use leader_not_available_error::LeaderNotAvailableError;
pub use listener_not_found_error::ListenerNotFoundError;
pub use log_dir_not_found_error::LogDirNotFoundError;
pub use member_id_required_error::MemberIdRequiredError;
pub use mismatched_endpoint_type_error::MismatchedEndpointTypeError;
pub use network_error::NetworkError;
pub use new_leader_elected_error::NewLeaderElectedError;
pub use no_reassignment_in_progress_error::NoReassignmentInProgressError;
pub use not_controller_error::NotControllerError;
pub use not_coordinator_error::NotCoordinatorError;
pub use not_enough_replicas_after_append_error::NotEnoughReplicasAfterAppendError;
pub use not_enough_replicas_error::NotEnoughReplicasError;
pub use not_leader_or_follower_error::NotLeaderOrFollowerError;
pub use offset_metadata_too_large_error::OffsetMetadataTooLargeError;
pub use offset_moved_to_tiered_storage_error::OffsetMovedToTieredStorageError;
pub use offset_not_available_error::OffsetNotAvailableError;
pub use offset_out_of_range_error::OffsetOutOfRangeError;
pub use operation_not_attempted_error::OperationNotAttemptedError;
pub use out_of_order_sequence_error::OutOfOrderSequenceError;
pub use policy_violation_error::PolicyViolationError;
pub use position_out_of_range_error::PositionOutOfRangeError;
pub use preferred_leader_not_available_error::PreferredLeaderNotAvailableError;
pub use principal_deserialization_error::PrincipalDeserializationError;
pub use producer_fenced_error::ProducerFencedError;
pub use reassignment_in_progress_error::ReassignmentInProgressError;
pub use rebalance_in_progress_error::RebalanceInProgressError;
pub use rebootstrap_required_error::RebootstrapRequiredError;
pub use record_batch_too_large_error::RecordBatchTooLargeError;
pub use record_deserialization_error::RecordDeserializationError;
pub use record_too_large_error::RecordTooLargeError;
pub use replica_not_available_error::ReplicaNotAvailableError;
pub use resource_not_found_error::ResourceNotFoundError;
pub use sasl_authentication_error::SaslAuthenticationError;
pub use security_disabled_error::SecurityDisabledError;
pub use serialization_error::SerializationError;
pub use share_session_limit_reached_error::ShareSessionLimitReachedError;
pub use share_session_not_found_error::ShareSessionNotFoundError;
pub use snapshot_not_found_error::SnapshotNotFoundError;
pub use ssl_authentication_error::SslAuthenticationError;
pub use stale_broker_epoch_error::StaleBrokerEpochError;
pub use stale_member_epoch_error::StaleMemberEpochError;
pub use streams_invalid_topology_epoch_error::StreamsInvalidTopologyEpochError;
pub use streams_invalid_topology_error::StreamsInvalidTopologyError;
pub use streams_topology_fenced_error::StreamsTopologyFencedError;
pub use telemetry_too_large_error::TelemetryTooLargeError;
pub use throttling_quota_exceeded_error::ThrottlingQuotaExceededError;
pub use timeout_error::TimeoutError;
pub use topic_authorization_error::TopicAuthorizationError;
pub use topic_deletion_disabled_error::TopicDeletionDisabledError;
pub use topic_exists_error::TopicExistsError;
pub use transaction_abortable_error::TransactionAbortableError;
pub use transaction_aborted_error::TransactionAbortedError;
pub use transaction_coordinator_fenced_error::TransactionCoordinatorFencedError;
pub use transactional_id_authorization_error::TransactionalIdAuthorizationError;
pub use transactional_id_not_found_error::TransactionalIdNotFoundError;
pub use unacceptable_credential_error::UnacceptableCredentialError;
pub use unknown_controller_id_error::UnknownControllerIdError;
pub use unknown_leader_epoch_error::UnknownLeaderEpochError;
pub use unknown_member_id_error::UnknownMemberIdError;
pub use unknown_producer_id_error::UnknownProducerIdError;
pub use unknown_server_error::UnknownServerError;
pub use unknown_subscription_id_error::UnknownSubscriptionIdError;
pub use unknown_topic_id_error::UnknownTopicIdError;
pub use unknown_topic_or_partition_error::UnknownTopicOrPartitionError;
pub use unreleased_instance_id_error::UnreleasedInstanceIdError;
pub use unstable_offset_commit_error::UnstableOffsetCommitError;
pub use unsupported_assignor_error::UnsupportedAssignorError;
pub use unsupported_by_authentication_error::UnsupportedByAuthenticationError;
pub use unsupported_compression_type_error::UnsupportedCompressionTypeError;
pub use unsupported_endpoint_type_error::UnsupportedEndpointTypeError;
pub use unsupported_for_message_format_error::UnsupportedForMessageFormatError;
pub use unsupported_sasl_mechanism_error::UnsupportedSaslMechanismError;
pub use unsupported_version_error::UnsupportedVersionError;
pub use voter_not_found_error::VoterNotFoundError;
pub use wakeup_error::WakeupError;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_error::{ErrorCode, ErrorHierarchy};
    use crate::common::protocol::Errors;
    use crate::producer::ProducerBufferExhaustedError;

    /// `BufferExhaustedException extends TimeoutException` and has no entry of its
    /// own in `Errors`, so Java's `Errors.forException` walks up and finds
    /// `REQUEST_TIMED_OUT`. It is therefore retriable — which the class states
    /// directly, and which no predicate on `Errors` could have expressed.
    #[test]
    fn buffer_exhausted_inherits_its_code_from_timeout() {
        let e = ProducerBufferExhaustedError::new("pool full");
        assert!(ErrorHierarchy::is_retriable_error(&e));
        assert_eq!(ErrorCode::error(&e), Errors::RequestTimedOut);
    }

    /// The 12 classes with no entry in `Errors`: raised client-side, or concrete
    /// bases whose coded subclasses carry the code instead. Nothing in `Errors`
    /// can express their ancestry, so it is asserted directly here.
    ///
    /// `ErrorCode` gives them `UnknownServerError`, matching Java: a class with
    /// no entry and no coded superclass falls through `Errors.forException`'s
    /// walk to `UNKNOWN_SERVER_ERROR`.
    // Fatality is intentionally NOT checked here: it is not an `ErrorHierarchy`
    // predicate but `request_utils::is_fatal_error` over an `Error`. The codeless
    // fatal classes are covered there instead.
    #[test]
    fn codeless_classes_state_their_own_ancestry() {
        #[allow(clippy::type_complexity)]
        let cases: Vec<(&str, Box<dyn ErrorHierarchy>, &[&str])> = vec![
            ("ApiError", Box::new(ApiError::new("m")), &["kafka", "api"]),
            (
                "AuthenticationError",
                Box::new(AuthenticationError::new("m")),
                &["kafka", "api", "invalid_configuration", "authentication"],
            ),
            (
                "AuthorizationError",
                Box::new(AuthorizationError::new("m")),
                &["kafka", "api", "invalid_configuration", "authorization"],
            ),
            (
                "AuthorizerNotReadyError",
                Box::new(AuthorizerNotReadyError::new("m")),
                &["kafka", "api", "retriable"],
            ),
            (
                "DisconnectError",
                Box::new(DisconnectError::new("m")),
                &["kafka", "api", "retriable"],
            ),
            ("InterruptError", Box::new(InterruptError::new("m")), &["kafka"]),
            (
                "InvalidOffsetError",
                Box::new(InvalidOffsetError::new("m")),
                &["kafka", "api", "invalid_offset"],
            ),
            (
                "RecordDeserializationError",
                Box::new(RecordDeserializationError::new(
                    crate::common::errors::record_deserialization_error::DeserializationErrorOrigin::Value,
                    crate::common::TopicPartition::new("t".to_string(), 0),
                    0,
                    -1,
                    crate::common::record::TimestampType::NoTimestampType,
                    None,
                    None,
                    None,
                    "m",
                )),
                &["kafka", "serialization"],
            ),
            (
                "SerializationError",
                Box::new(SerializationError::new("m")),
                &["kafka", "serialization"],
            ),
            (
                "SslAuthenticationError",
                Box::new(SslAuthenticationError::new("m")),
                &["kafka", "api", "invalid_configuration", "authentication"],
            ),
            (
                "TransactionAbortedError",
                Box::new(TransactionAbortedError::new("m")),
                &["kafka", "api"],
            ),
            ("WakeupError", Box::new(WakeupError::new("m")), &["kafka"]),
        ];

        for (name, class, expected) in &cases {
            let actual: Vec<&str> = [
                ("kafka", class.is_kafka_error()),
                ("api", class.is_api_error()),
                ("retriable", class.is_retriable_error()),
                ("refresh_retriable", class.is_refresh_retriable_error()),
                ("timeout", class.is_timeout_error()),
                ("invalid_metadata", class.is_invalid_metadata_error()),
                ("invalid_configuration", class.is_invalid_configuration_error()),
                ("application_recoverable", class.is_application_recoverable_error()),
                ("invalid_offset", class.is_invalid_offset_error()),
                ("consumer_invalid_offset", class.is_consumer_invalid_offset_error()),
                ("consumer_offset_out_of_range", class.is_consumer_offset_out_of_range_error()),
                ("out_of_order_sequence", class.is_out_of_order_sequence_error()),
                ("serialization", class.is_serialization_error()),
                ("authentication", class.is_authentication_error()),
                ("authorization", class.is_authorization_error()),
            ]
            .into_iter()
            .filter_map(|(n, holds)| holds.then_some(n))
            .collect();
            assert_eq!(&actual, expected, "{name}: ancestry diverges from its Java extends chain");
        }
    }
}
