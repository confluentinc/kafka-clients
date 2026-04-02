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

//! Identifiers for all the Kafka APIs.

#[cfg(not(feature = "skip-generated"))]
use crate::api_message_type::{ApiMessageType, ListenerType};

/// Identifiers for all the Kafka APIs.
///
/// Each variant wraps the generated [`ApiMessageType`] enum to provide
/// version ranges, header version logic, and listener information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ApiKeys {
    #[cfg(not(feature = "skip-generated"))]
    message_type: ApiMessageType,
    cluster_action: bool,
    forwardable: bool,
}

// Versions 0-2 were removed in Apache Kafka 4.0, version 3 is the new baseline.
// Due to a bug in librdkafka, version `0` has to be included in the api versions response
// (see KAFKA-18659).
#[cfg(not(feature = "skip-generated"))]
pub const PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION: i16 = 0;

#[cfg(not(feature = "skip-generated"))]
impl ApiKeys {
    const fn new(message_type: ApiMessageType) -> Self {
        Self { message_type, cluster_action: false, forwardable: false }
    }

    const fn cluster_action(message_type: ApiMessageType) -> Self {
        Self { message_type, cluster_action: true, forwardable: false }
    }

    const fn forwardable(message_type: ApiMessageType) -> Self {
        Self { message_type, cluster_action: false, forwardable: true }
    }

    const fn cluster_action_and_forwardable(message_type: ApiMessageType) -> Self {
        Self { message_type, cluster_action: true, forwardable: true }
    }

    // All API keys, matching Java ApiKeys enum definition order.
    pub const PRODUCE: Self = Self::new(ApiMessageType::PRODUCE);
    pub const FETCH: Self = Self::new(ApiMessageType::FETCH);
    pub const LIST_OFFSETS: Self = Self::new(ApiMessageType::LIST_OFFSETS);
    pub const METADATA: Self = Self::new(ApiMessageType::METADATA);
    pub const LEADER_AND_ISR: Self = Self::cluster_action(ApiMessageType::LEADER_AND_ISR);
    pub const STOP_REPLICA: Self = Self::cluster_action(ApiMessageType::STOP_REPLICA);
    pub const UPDATE_METADATA: Self = Self::cluster_action(ApiMessageType::UPDATE_METADATA);
    pub const CONTROLLED_SHUTDOWN: Self = Self::cluster_action(ApiMessageType::CONTROLLED_SHUTDOWN);
    pub const OFFSET_COMMIT: Self = Self::new(ApiMessageType::OFFSET_COMMIT);
    pub const OFFSET_FETCH: Self = Self::new(ApiMessageType::OFFSET_FETCH);
    pub const FIND_COORDINATOR: Self = Self::new(ApiMessageType::FIND_COORDINATOR);
    pub const JOIN_GROUP: Self = Self::new(ApiMessageType::JOIN_GROUP);
    pub const HEARTBEAT: Self = Self::new(ApiMessageType::HEARTBEAT);
    pub const LEAVE_GROUP: Self = Self::new(ApiMessageType::LEAVE_GROUP);
    pub const SYNC_GROUP: Self = Self::new(ApiMessageType::SYNC_GROUP);
    pub const DESCRIBE_GROUPS: Self = Self::new(ApiMessageType::DESCRIBE_GROUPS);
    pub const LIST_GROUPS: Self = Self::new(ApiMessageType::LIST_GROUPS);
    pub const SASL_HANDSHAKE: Self = Self::new(ApiMessageType::SASL_HANDSHAKE);
    pub const API_VERSIONS: Self = Self::new(ApiMessageType::API_VERSIONS);
    pub const CREATE_TOPICS: Self = Self::forwardable(ApiMessageType::CREATE_TOPICS);
    pub const DELETE_TOPICS: Self = Self::forwardable(ApiMessageType::DELETE_TOPICS);
    pub const DELETE_RECORDS: Self = Self::new(ApiMessageType::DELETE_RECORDS);
    pub const INIT_PRODUCER_ID: Self = Self::new(ApiMessageType::INIT_PRODUCER_ID);
    pub const OFFSET_FOR_LEADER_EPOCH: Self = Self::new(ApiMessageType::OFFSET_FOR_LEADER_EPOCH);
    pub const ADD_PARTITIONS_TO_TXN: Self = Self::new(ApiMessageType::ADD_PARTITIONS_TO_TXN);
    pub const ADD_OFFSETS_TO_TXN: Self = Self::new(ApiMessageType::ADD_OFFSETS_TO_TXN);
    pub const END_TXN: Self = Self::new(ApiMessageType::END_TXN);
    pub const WRITE_TXN_MARKERS: Self = Self::cluster_action(ApiMessageType::WRITE_TXN_MARKERS);
    pub const TXN_OFFSET_COMMIT: Self = Self::new(ApiMessageType::TXN_OFFSET_COMMIT);
    pub const DESCRIBE_ACLS: Self = Self::new(ApiMessageType::DESCRIBE_ACLS);
    pub const CREATE_ACLS: Self = Self::forwardable(ApiMessageType::CREATE_ACLS);
    pub const DELETE_ACLS: Self = Self::forwardable(ApiMessageType::DELETE_ACLS);
    pub const DESCRIBE_CONFIGS: Self = Self::new(ApiMessageType::DESCRIBE_CONFIGS);
    pub const ALTER_CONFIGS: Self = Self::forwardable(ApiMessageType::ALTER_CONFIGS);
    pub const ALTER_REPLICA_LOG_DIRS: Self = Self::new(ApiMessageType::ALTER_REPLICA_LOG_DIRS);
    pub const DESCRIBE_LOG_DIRS: Self = Self::new(ApiMessageType::DESCRIBE_LOG_DIRS);
    pub const SASL_AUTHENTICATE: Self = Self::new(ApiMessageType::SASL_AUTHENTICATE);
    pub const CREATE_PARTITIONS: Self = Self::forwardable(ApiMessageType::CREATE_PARTITIONS);
    pub const CREATE_DELEGATION_TOKEN: Self = Self::forwardable(ApiMessageType::CREATE_DELEGATION_TOKEN);
    pub const RENEW_DELEGATION_TOKEN: Self = Self::forwardable(ApiMessageType::RENEW_DELEGATION_TOKEN);
    pub const EXPIRE_DELEGATION_TOKEN: Self = Self::forwardable(ApiMessageType::EXPIRE_DELEGATION_TOKEN);
    pub const DESCRIBE_DELEGATION_TOKEN: Self = Self::new(ApiMessageType::DESCRIBE_DELEGATION_TOKEN);
    pub const DELETE_GROUPS: Self = Self::new(ApiMessageType::DELETE_GROUPS);
    pub const ELECT_LEADERS: Self = Self::forwardable(ApiMessageType::ELECT_LEADERS);
    pub const INCREMENTAL_ALTER_CONFIGS: Self = Self::forwardable(ApiMessageType::INCREMENTAL_ALTER_CONFIGS);
    pub const ALTER_PARTITION_REASSIGNMENTS: Self = Self::forwardable(ApiMessageType::ALTER_PARTITION_REASSIGNMENTS);
    pub const LIST_PARTITION_REASSIGNMENTS: Self = Self::forwardable(ApiMessageType::LIST_PARTITION_REASSIGNMENTS);
    pub const OFFSET_DELETE: Self = Self::new(ApiMessageType::OFFSET_DELETE);
    pub const DESCRIBE_CLIENT_QUOTAS: Self = Self::new(ApiMessageType::DESCRIBE_CLIENT_QUOTAS);
    pub const ALTER_CLIENT_QUOTAS: Self = Self::forwardable(ApiMessageType::ALTER_CLIENT_QUOTAS);
    pub const DESCRIBE_USER_SCRAM_CREDENTIALS: Self = Self::new(ApiMessageType::DESCRIBE_USER_SCRAM_CREDENTIALS);
    pub const ALTER_USER_SCRAM_CREDENTIALS: Self = Self::forwardable(ApiMessageType::ALTER_USER_SCRAM_CREDENTIALS);
    pub const VOTE: Self = Self::cluster_action(ApiMessageType::VOTE);
    pub const BEGIN_QUORUM_EPOCH: Self = Self::cluster_action(ApiMessageType::BEGIN_QUORUM_EPOCH);
    pub const END_QUORUM_EPOCH: Self = Self::cluster_action(ApiMessageType::END_QUORUM_EPOCH);
    pub const DESCRIBE_QUORUM: Self = Self::cluster_action_and_forwardable(ApiMessageType::DESCRIBE_QUORUM);
    pub const ALTER_PARTITION: Self = Self::cluster_action(ApiMessageType::ALTER_PARTITION);
    pub const UPDATE_FEATURES: Self = Self::cluster_action_and_forwardable(ApiMessageType::UPDATE_FEATURES);
    pub const ENVELOPE: Self = Self::cluster_action(ApiMessageType::ENVELOPE);
    pub const FETCH_SNAPSHOT: Self = Self::new(ApiMessageType::FETCH_SNAPSHOT);
    pub const DESCRIBE_CLUSTER: Self = Self::new(ApiMessageType::DESCRIBE_CLUSTER);
    pub const DESCRIBE_PRODUCERS: Self = Self::new(ApiMessageType::DESCRIBE_PRODUCERS);
    pub const BROKER_REGISTRATION: Self = Self::cluster_action(ApiMessageType::BROKER_REGISTRATION);
    pub const BROKER_HEARTBEAT: Self = Self::cluster_action(ApiMessageType::BROKER_HEARTBEAT);
    pub const UNREGISTER_BROKER: Self = Self::forwardable(ApiMessageType::UNREGISTER_BROKER);
    pub const DESCRIBE_TRANSACTIONS: Self = Self::new(ApiMessageType::DESCRIBE_TRANSACTIONS);
    pub const LIST_TRANSACTIONS: Self = Self::new(ApiMessageType::LIST_TRANSACTIONS);
    pub const ALLOCATE_PRODUCER_IDS: Self = Self::cluster_action_and_forwardable(ApiMessageType::ALLOCATE_PRODUCER_IDS);
    pub const CONSUMER_GROUP_HEARTBEAT: Self = Self::new(ApiMessageType::CONSUMER_GROUP_HEARTBEAT);
    pub const CONSUMER_GROUP_DESCRIBE: Self = Self::new(ApiMessageType::CONSUMER_GROUP_DESCRIBE);
    pub const CONTROLLER_REGISTRATION: Self = Self::new(ApiMessageType::CONTROLLER_REGISTRATION);
    pub const GET_TELEMETRY_SUBSCRIPTIONS: Self = Self::new(ApiMessageType::GET_TELEMETRY_SUBSCRIPTIONS);
    pub const PUSH_TELEMETRY: Self = Self::new(ApiMessageType::PUSH_TELEMETRY);
    pub const ASSIGN_REPLICAS_TO_DIRS: Self = Self::new(ApiMessageType::ASSIGN_REPLICAS_TO_DIRS);
    pub const LIST_CONFIG_RESOURCES: Self = Self::new(ApiMessageType::LIST_CONFIG_RESOURCES);
    pub const DESCRIBE_TOPIC_PARTITIONS: Self = Self::new(ApiMessageType::DESCRIBE_TOPIC_PARTITIONS);
    pub const SHARE_GROUP_HEARTBEAT: Self = Self::new(ApiMessageType::SHARE_GROUP_HEARTBEAT);
    pub const SHARE_GROUP_DESCRIBE: Self = Self::new(ApiMessageType::SHARE_GROUP_DESCRIBE);
    pub const SHARE_FETCH: Self = Self::new(ApiMessageType::SHARE_FETCH);
    pub const SHARE_ACKNOWLEDGE: Self = Self::new(ApiMessageType::SHARE_ACKNOWLEDGE);
    pub const ADD_RAFT_VOTER: Self = Self::forwardable(ApiMessageType::ADD_RAFT_VOTER);
    pub const REMOVE_RAFT_VOTER: Self = Self::forwardable(ApiMessageType::REMOVE_RAFT_VOTER);
    pub const UPDATE_RAFT_VOTER: Self = Self::new(ApiMessageType::UPDATE_RAFT_VOTER);
    pub const INITIALIZE_SHARE_GROUP_STATE: Self = Self::cluster_action(ApiMessageType::INITIALIZE_SHARE_GROUP_STATE);
    pub const READ_SHARE_GROUP_STATE: Self = Self::cluster_action(ApiMessageType::READ_SHARE_GROUP_STATE);
    pub const WRITE_SHARE_GROUP_STATE: Self = Self::cluster_action(ApiMessageType::WRITE_SHARE_GROUP_STATE);
    pub const DELETE_SHARE_GROUP_STATE: Self = Self::cluster_action(ApiMessageType::DELETE_SHARE_GROUP_STATE);
    pub const READ_SHARE_GROUP_STATE_SUMMARY: Self =
        Self::cluster_action(ApiMessageType::READ_SHARE_GROUP_STATE_SUMMARY);
    pub const STREAMS_GROUP_HEARTBEAT: Self = Self::new(ApiMessageType::STREAMS_GROUP_HEARTBEAT);
    pub const STREAMS_GROUP_DESCRIBE: Self = Self::new(ApiMessageType::STREAMS_GROUP_DESCRIBE);
    pub const DESCRIBE_SHARE_GROUP_OFFSETS: Self = Self::new(ApiMessageType::DESCRIBE_SHARE_GROUP_OFFSETS);
    pub const ALTER_SHARE_GROUP_OFFSETS: Self = Self::new(ApiMessageType::ALTER_SHARE_GROUP_OFFSETS);
    pub const DELETE_SHARE_GROUP_OFFSETS: Self = Self::new(ApiMessageType::DELETE_SHARE_GROUP_OFFSETS);

    /// All known API keys.
    pub const ALL: &[ApiKeys] = &[
        Self::PRODUCE,
        Self::FETCH,
        Self::LIST_OFFSETS,
        Self::METADATA,
        Self::LEADER_AND_ISR,
        Self::STOP_REPLICA,
        Self::UPDATE_METADATA,
        Self::CONTROLLED_SHUTDOWN,
        Self::OFFSET_COMMIT,
        Self::OFFSET_FETCH,
        Self::FIND_COORDINATOR,
        Self::JOIN_GROUP,
        Self::HEARTBEAT,
        Self::LEAVE_GROUP,
        Self::SYNC_GROUP,
        Self::DESCRIBE_GROUPS,
        Self::LIST_GROUPS,
        Self::SASL_HANDSHAKE,
        Self::API_VERSIONS,
        Self::CREATE_TOPICS,
        Self::DELETE_TOPICS,
        Self::DELETE_RECORDS,
        Self::INIT_PRODUCER_ID,
        Self::OFFSET_FOR_LEADER_EPOCH,
        Self::ADD_PARTITIONS_TO_TXN,
        Self::ADD_OFFSETS_TO_TXN,
        Self::END_TXN,
        Self::WRITE_TXN_MARKERS,
        Self::TXN_OFFSET_COMMIT,
        Self::DESCRIBE_ACLS,
        Self::CREATE_ACLS,
        Self::DELETE_ACLS,
        Self::DESCRIBE_CONFIGS,
        Self::ALTER_CONFIGS,
        Self::ALTER_REPLICA_LOG_DIRS,
        Self::DESCRIBE_LOG_DIRS,
        Self::SASL_AUTHENTICATE,
        Self::CREATE_PARTITIONS,
        Self::CREATE_DELEGATION_TOKEN,
        Self::RENEW_DELEGATION_TOKEN,
        Self::EXPIRE_DELEGATION_TOKEN,
        Self::DESCRIBE_DELEGATION_TOKEN,
        Self::DELETE_GROUPS,
        Self::ELECT_LEADERS,
        Self::INCREMENTAL_ALTER_CONFIGS,
        Self::ALTER_PARTITION_REASSIGNMENTS,
        Self::LIST_PARTITION_REASSIGNMENTS,
        Self::OFFSET_DELETE,
        Self::DESCRIBE_CLIENT_QUOTAS,
        Self::ALTER_CLIENT_QUOTAS,
        Self::DESCRIBE_USER_SCRAM_CREDENTIALS,
        Self::ALTER_USER_SCRAM_CREDENTIALS,
        Self::VOTE,
        Self::BEGIN_QUORUM_EPOCH,
        Self::END_QUORUM_EPOCH,
        Self::DESCRIBE_QUORUM,
        Self::ALTER_PARTITION,
        Self::UPDATE_FEATURES,
        Self::ENVELOPE,
        Self::FETCH_SNAPSHOT,
        Self::DESCRIBE_CLUSTER,
        Self::DESCRIBE_PRODUCERS,
        Self::BROKER_REGISTRATION,
        Self::BROKER_HEARTBEAT,
        Self::UNREGISTER_BROKER,
        Self::DESCRIBE_TRANSACTIONS,
        Self::LIST_TRANSACTIONS,
        Self::ALLOCATE_PRODUCER_IDS,
        Self::CONSUMER_GROUP_HEARTBEAT,
        Self::CONSUMER_GROUP_DESCRIBE,
        Self::CONTROLLER_REGISTRATION,
        Self::GET_TELEMETRY_SUBSCRIPTIONS,
        Self::PUSH_TELEMETRY,
        Self::ASSIGN_REPLICAS_TO_DIRS,
        Self::LIST_CONFIG_RESOURCES,
        Self::DESCRIBE_TOPIC_PARTITIONS,
        Self::SHARE_GROUP_HEARTBEAT,
        Self::SHARE_GROUP_DESCRIBE,
        Self::SHARE_FETCH,
        Self::SHARE_ACKNOWLEDGE,
        Self::ADD_RAFT_VOTER,
        Self::REMOVE_RAFT_VOTER,
        Self::UPDATE_RAFT_VOTER,
        Self::INITIALIZE_SHARE_GROUP_STATE,
        Self::READ_SHARE_GROUP_STATE,
        Self::WRITE_SHARE_GROUP_STATE,
        Self::DELETE_SHARE_GROUP_STATE,
        Self::READ_SHARE_GROUP_STATE_SUMMARY,
        Self::STREAMS_GROUP_HEARTBEAT,
        Self::STREAMS_GROUP_DESCRIBE,
        Self::DESCRIBE_SHARE_GROUP_OFFSETS,
        Self::ALTER_SHARE_GROUP_OFFSETS,
        Self::DELETE_SHARE_GROUP_OFFSETS,
    ];

    /// The permanent and immutable id of this API.
    pub fn id(&self) -> i16 {
        self.message_type.api_key()
    }

    /// An english description of the api — used for debugging and metric names.
    pub fn name(&self) -> &'static str {
        self.message_type.name()
    }

    /// Whether this is a ClusterAction request used only by brokers.
    pub fn is_cluster_action(&self) -> bool {
        self.cluster_action
    }

    /// Whether the API is enabled for forwarding.
    pub fn is_forwardable(&self) -> bool {
        self.forwardable
    }

    /// The latest supported version of this API.
    pub fn latest_version(&self) -> i16 {
        self.message_type.highest_supported_version(true)
    }

    /// The latest supported version, with optional control over unstable versions.
    pub fn latest_version_with_unstable(&self, enable_unstable_last_version: bool) -> i16 {
        self.message_type.highest_supported_version(enable_unstable_last_version)
    }

    /// The oldest supported version of this API.
    pub fn oldest_version(&self) -> i16 {
        self.message_type.lowest_supported_version()
    }

    /// Whether the given API version is within the supported range.
    pub fn is_version_supported(&self, api_version: i16) -> bool {
        api_version >= self.oldest_version() && api_version <= self.latest_version()
    }

    /// Whether the given API version is enabled.
    ///
    /// ApiVersions API is a special case — the client always sends the highest version
    /// it supports, and the server falls back to version 0 if it does not know it.
    pub fn is_version_enabled(&self, api_version: i16, enable_unstable_last_version: bool) -> bool {
        if *self == Self::API_VERSIONS {
            return true;
        }
        api_version >= self.oldest_version()
            && api_version <= self.latest_version_with_unstable(enable_unstable_last_version)
    }

    /// Whether the given version is deprecated.
    pub fn is_version_deprecated(&self, api_version: i16) -> bool {
        api_version >= self.message_type.lowest_deprecated_version()
            && api_version <= self.message_type.highest_deprecated_version()
    }

    /// Returns `true` if there is at least one valid version, `false` otherwise.
    ///
    /// When `false` is returned, it typically means that the protocol API is no longer
    /// supported, but the API key remains assigned so we do not accidentally reuse it.
    pub fn has_valid_version(&self) -> bool {
        self.oldest_version() <= self.latest_version()
    }

    /// The request header version for a given API version.
    pub fn request_header_version(&self, api_version: i16) -> i16 {
        self.message_type.request_header_version(api_version)
    }

    /// The response header version for a given API version.
    pub fn response_header_version(&self, api_version: i16) -> i16 {
        self.message_type.response_header_version(api_version)
    }

    /// Returns a list of all supported versions for this API.
    pub fn all_versions(&self) -> Vec<i16> {
        (self.oldest_version()..=self.latest_version()).collect()
    }

    /// Whether this API is in scope for the given listener type.
    pub fn in_scope(&self, listener: ListenerType) -> bool {
        self.message_type.listeners().contains(&listener)
    }

    /// Look up an `ApiKeys` by its numeric API key id.
    pub fn for_id(id: i16) -> Option<&'static ApiKeys> {
        Self::ALL.iter().find(|k| k.id() == id)
    }

    /// Check if the given id corresponds to a known API key.
    pub fn has_id(id: i16) -> bool {
        Self::ALL.iter().any(|k| k.id() == id)
    }

    /// Returns all API keys that are in scope for the broker listener.
    pub fn broker_apis() -> Vec<&'static ApiKeys> {
        Self::apis_for_listener(ListenerType::Broker)
    }

    /// Returns all API keys that are in scope for the controller listener.
    pub fn controller_apis() -> Vec<&'static ApiKeys> {
        Self::apis_for_listener(ListenerType::Controller)
    }

    /// Returns all API keys available to clients (same as broker APIs).
    pub fn client_apis() -> Vec<&'static ApiKeys> {
        Self::broker_apis()
    }

    /// Returns all API keys that are in scope for the given listener type.
    pub fn apis_for_listener(listener: ListenerType) -> Vec<&'static ApiKeys> {
        Self::ALL.iter().filter(|k| k.in_scope(listener)).collect()
    }
}

#[cfg(test)]
#[cfg(not(feature = "skip-generated"))]
mod tests {
    use super::*;

    #[test]
    fn test_api_key_id() {
        assert_eq!(ApiKeys::PRODUCE.id(), 0);
        assert_eq!(ApiKeys::FETCH.id(), 1);
        assert_eq!(ApiKeys::METADATA.id(), 3);
        assert_eq!(ApiKeys::API_VERSIONS.id(), 18);
    }

    #[test]
    fn test_api_key_name() {
        assert_eq!(ApiKeys::PRODUCE.name(), "Produce");
        assert_eq!(ApiKeys::FETCH.name(), "Fetch");
        assert_eq!(ApiKeys::METADATA.name(), "Metadata");
        assert_eq!(ApiKeys::API_VERSIONS.name(), "ApiVersions");
    }

    #[test]
    fn test_for_id() {
        let produce = ApiKeys::for_id(0).unwrap();
        assert_eq!(produce.id(), 0);
        assert_eq!(produce.name(), "Produce");

        let metadata = ApiKeys::for_id(3).unwrap();
        assert_eq!(metadata.name(), "Metadata");

        assert!(ApiKeys::for_id(9999).is_none());
    }

    #[test]
    fn test_has_id() {
        assert!(ApiKeys::has_id(0));
        assert!(ApiKeys::has_id(18));
        assert!(!ApiKeys::has_id(9999));
    }

    #[test]
    fn test_cluster_action() {
        assert!(!ApiKeys::PRODUCE.is_cluster_action());
        assert!(ApiKeys::LEADER_AND_ISR.is_cluster_action());
        assert!(ApiKeys::STOP_REPLICA.is_cluster_action());
    }

    #[test]
    fn test_forwardable() {
        assert!(!ApiKeys::PRODUCE.is_forwardable());
        assert!(ApiKeys::CREATE_TOPICS.is_forwardable());
        assert!(ApiKeys::DELETE_TOPICS.is_forwardable());
    }

    #[test]
    fn test_version_range() {
        // Metadata has valid versions
        assert!(ApiKeys::METADATA.has_valid_version());
        assert!(ApiKeys::METADATA.oldest_version() <= ApiKeys::METADATA.latest_version());

        // Removed APIs have no valid versions
        assert!(!ApiKeys::LEADER_AND_ISR.has_valid_version());
    }

    #[test]
    fn test_version_supported() {
        let metadata = ApiKeys::METADATA;
        assert!(metadata.is_version_supported(metadata.oldest_version()));
        assert!(metadata.is_version_supported(metadata.latest_version()));
        assert!(!metadata.is_version_supported(-1));
        assert!(!metadata.is_version_supported(metadata.latest_version() + 1));
    }

    #[test]
    fn test_api_versions_always_enabled() {
        // ApiVersions is always enabled for any version
        assert!(ApiKeys::API_VERSIONS.is_version_enabled(0, false));
        assert!(ApiKeys::API_VERSIONS.is_version_enabled(100, false));
    }

    #[test]
    fn test_header_versions() {
        // Metadata: non-flexible versions use request header v1, flexible use v2
        let oldest = ApiKeys::METADATA.oldest_version();
        let latest = ApiKeys::METADATA.latest_version();

        // Request headers should be 1 or 2
        let req_hdr = ApiKeys::METADATA.request_header_version(oldest);
        assert!(req_hdr == 1 || req_hdr == 2);

        let req_hdr_latest = ApiKeys::METADATA.request_header_version(latest);
        assert!(req_hdr_latest == 1 || req_hdr_latest == 2);

        // ApiVersions response always uses header v0 (KIP-511)
        assert_eq!(ApiKeys::API_VERSIONS.response_header_version(0), 0);
        assert_eq!(ApiKeys::API_VERSIONS.response_header_version(3), 0);
    }

    #[test]
    fn test_unique_ids() {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for key in ApiKeys::ALL {
            assert!(seen.insert(key.id()), "Duplicate API key id: {}", key.id());
        }
    }

    #[test]
    fn test_for_id_with_invalid_id_low() {
        assert!(ApiKeys::for_id(-1).is_none());
    }

    #[test]
    fn test_for_id_with_invalid_id_high() {
        assert!(ApiKeys::for_id(10000).is_none());
    }

    #[test]
    fn test_alter_partition_is_cluster_action() {
        assert!(ApiKeys::ALTER_PARTITION.is_cluster_action());
    }

    #[test]
    fn test_has_valid_versions() {
        let no_valid_versions = [
            ApiKeys::LEADER_AND_ISR,
            ApiKeys::STOP_REPLICA,
            ApiKeys::UPDATE_METADATA,
            ApiKeys::CONTROLLED_SHUTDOWN,
        ];
        for key in ApiKeys::ALL {
            if no_valid_versions.contains(key) {
                assert!(!key.has_valid_version(), "{} should have no valid versions", key.name());
            } else {
                assert!(key.has_valid_version(), "{} should have valid versions", key.name());
            }
        }
    }

    #[test]
    fn test_api_scope() {
        use std::collections::HashSet;
        let mut apis_missing_scope = HashSet::new();
        for key in ApiKeys::ALL {
            if key.message_type.listeners().is_empty() && key.has_valid_version() {
                apis_missing_scope.insert(key.id());
            }
        }
        assert!(
            apis_missing_scope.is_empty(),
            "Found some APIs missing scope definition: {:?}",
            apis_missing_scope
        );
    }
}
