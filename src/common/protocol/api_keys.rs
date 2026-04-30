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

//! Translation of `org.apache.kafka.common.protocol.ApiKeys`.
//!
//! Java's `ApiKeys` is an `enum` whose constructor pulls the `apiKey()`,
//! `name`, `requestSchemas[]`, `responseSchemas[]`, header versions, and
//! listener set from the *generated* `ApiMessageType` enum. The generated
//! `ApiMessageType` lives in Phase 2d (`src/common/message/`), so for Phase
//! 2c this catalogue is a hand-coded mirror of the Java entries — id, name,
//! cluster-action flag, forwardable flag, and listener scope. Phase 2d will
//! refactor this so the catalogue is auto-populated from the generated
//! `ApiMessageType` table; until then, the values here are kept in lock-step
//! with `ApiKeys.java` (Apache Kafka 4.2).
//!
//! See `design/history/Milestone-1/Phase-2/NOTES.md` for the rationale on
//! representing Java's `enum` as a Rust `struct` catalogue rather than a
//! `#[derive(Copy, Clone)] enum`.

use crate::common::errors::KafkaError;

/// Listener types. Re-exported from the generated `ApiMessageType` so the
/// hand-coded catalogue and the JSON spec agree on the listener vocabulary
/// (post-KIP-833: `Broker` and `Controller` only — `ZkBroker` is no longer
/// part of the spec).
pub use crate::common::message::api_message_type::ListenerType;

/// One entry in the API key catalogue. Mirrors the Java `enum` instance.
///
/// `listeners` is deliberately *not* a field on this struct: in Java the
/// listener set is sourced from `ApiMessageType.listeners()` (the JSON spec),
/// not from `ApiKeys.java`'s constructor. We delegate to the generated
/// `ApiMessageType::listeners()` via [`Self::listeners`] / [`Self::in_scope`]
/// so the two tables can never silently disagree.
#[derive(Debug, Clone)]
pub struct ApiKey {
    /// Wire id. Mirrors `ApiKeys.id`.
    pub id: i16,
    /// Display name (used in metrics, logs). Mirrors `ApiKeys.name`.
    pub name: &'static str,
    /// Whether the API is reserved to inter-broker traffic. Mirrors
    /// `clusterAction`.
    pub cluster_action: bool,
    /// Whether the API supports being forwarded by a broker to the active
    /// controller. Mirrors `forwardable`.
    pub forwardable: bool,
}

/// The full API catalogue. Order matches the Java declaration order so any
/// future code that depends on declaration order is unaffected.
pub const ALL_API_KEYS: &[ApiKey] = &[
    ApiKey { id: 0, name: "Produce", cluster_action: false, forwardable: false },
    ApiKey { id: 1, name: "Fetch", cluster_action: false, forwardable: false },
    ApiKey { id: 2, name: "ListOffsets", cluster_action: false, forwardable: false },
    ApiKey { id: 3, name: "Metadata", cluster_action: false, forwardable: false },
    ApiKey { id: 4, name: "LeaderAndIsr", cluster_action: true, forwardable: false },
    ApiKey { id: 5, name: "StopReplica", cluster_action: true, forwardable: false },
    ApiKey { id: 6, name: "UpdateMetadata", cluster_action: true, forwardable: false },
    ApiKey { id: 7, name: "ControlledShutdown", cluster_action: true, forwardable: false },
    ApiKey { id: 8, name: "OffsetCommit", cluster_action: false, forwardable: false },
    ApiKey { id: 9, name: "OffsetFetch", cluster_action: false, forwardable: false },
    ApiKey { id: 10, name: "FindCoordinator", cluster_action: false, forwardable: false },
    ApiKey { id: 11, name: "JoinGroup", cluster_action: false, forwardable: false },
    ApiKey { id: 12, name: "Heartbeat", cluster_action: false, forwardable: false },
    ApiKey { id: 13, name: "LeaveGroup", cluster_action: false, forwardable: false },
    ApiKey { id: 14, name: "SyncGroup", cluster_action: false, forwardable: false },
    ApiKey { id: 15, name: "DescribeGroups", cluster_action: false, forwardable: false },
    ApiKey { id: 16, name: "ListGroups", cluster_action: false, forwardable: false },
    ApiKey { id: 17, name: "SaslHandshake", cluster_action: false, forwardable: false },
    ApiKey { id: 18, name: "ApiVersions", cluster_action: false, forwardable: false },
    ApiKey { id: 19, name: "CreateTopics", cluster_action: false, forwardable: true },
    ApiKey { id: 20, name: "DeleteTopics", cluster_action: false, forwardable: true },
    ApiKey { id: 21, name: "DeleteRecords", cluster_action: false, forwardable: false },
    ApiKey { id: 22, name: "InitProducerId", cluster_action: false, forwardable: false },
    ApiKey { id: 23, name: "OffsetForLeaderEpoch", cluster_action: false, forwardable: false },
    ApiKey { id: 24, name: "AddPartitionsToTxn", cluster_action: false, forwardable: false },
    ApiKey { id: 25, name: "AddOffsetsToTxn", cluster_action: false, forwardable: false },
    ApiKey { id: 26, name: "EndTxn", cluster_action: false, forwardable: false },
    ApiKey { id: 27, name: "WriteTxnMarkers", cluster_action: true, forwardable: false },
    ApiKey { id: 28, name: "TxnOffsetCommit", cluster_action: false, forwardable: false },
    ApiKey { id: 29, name: "DescribeAcls", cluster_action: false, forwardable: false },
    ApiKey { id: 30, name: "CreateAcls", cluster_action: false, forwardable: true },
    ApiKey { id: 31, name: "DeleteAcls", cluster_action: false, forwardable: true },
    ApiKey { id: 32, name: "DescribeConfigs", cluster_action: false, forwardable: false },
    ApiKey { id: 33, name: "AlterConfigs", cluster_action: false, forwardable: true },
    ApiKey { id: 34, name: "AlterReplicaLogDirs", cluster_action: false, forwardable: false },
    ApiKey { id: 35, name: "DescribeLogDirs", cluster_action: false, forwardable: false },
    ApiKey { id: 36, name: "SaslAuthenticate", cluster_action: false, forwardable: false },
    ApiKey { id: 37, name: "CreatePartitions", cluster_action: false, forwardable: true },
    ApiKey { id: 38, name: "CreateDelegationToken", cluster_action: false, forwardable: true },
    ApiKey { id: 39, name: "RenewDelegationToken", cluster_action: false, forwardable: true },
    ApiKey { id: 40, name: "ExpireDelegationToken", cluster_action: false, forwardable: true },
    ApiKey {
        id: 41,
        name: "DescribeDelegationToken",
        cluster_action: false,
        forwardable: false,
    },
    ApiKey { id: 42, name: "DeleteGroups", cluster_action: false, forwardable: false },
    ApiKey { id: 43, name: "ElectLeaders", cluster_action: false, forwardable: true },
    ApiKey {
        id: 44,
        name: "IncrementalAlterConfigs",
        cluster_action: false,
        forwardable: true,
    },
    ApiKey {
        id: 45,
        name: "AlterPartitionReassignments",
        cluster_action: false,
        forwardable: true,
    },
    ApiKey {
        id: 46,
        name: "ListPartitionReassignments",
        cluster_action: false,
        forwardable: true,
    },
    ApiKey { id: 47, name: "OffsetDelete", cluster_action: false, forwardable: false },
    ApiKey { id: 48, name: "DescribeClientQuotas", cluster_action: false, forwardable: false },
    ApiKey { id: 49, name: "AlterClientQuotas", cluster_action: false, forwardable: true },
    ApiKey {
        id: 50,
        name: "DescribeUserScramCredentials",
        cluster_action: false,
        forwardable: false,
    },
    ApiKey {
        id: 51,
        name: "AlterUserScramCredentials",
        cluster_action: false,
        forwardable: true,
    },
    ApiKey { id: 52, name: "Vote", cluster_action: true, forwardable: false },
    ApiKey { id: 53, name: "BeginQuorumEpoch", cluster_action: true, forwardable: false },
    ApiKey { id: 54, name: "EndQuorumEpoch", cluster_action: true, forwardable: false },
    ApiKey { id: 55, name: "DescribeQuorum", cluster_action: true, forwardable: true },
    ApiKey { id: 56, name: "AlterPartition", cluster_action: true, forwardable: false },
    ApiKey { id: 57, name: "UpdateFeatures", cluster_action: true, forwardable: true },
    ApiKey { id: 58, name: "Envelope", cluster_action: true, forwardable: false },
    ApiKey { id: 59, name: "FetchSnapshot", cluster_action: false, forwardable: false },
    ApiKey { id: 60, name: "DescribeCluster", cluster_action: false, forwardable: false },
    ApiKey { id: 61, name: "DescribeProducers", cluster_action: false, forwardable: false },
    ApiKey { id: 62, name: "BrokerRegistration", cluster_action: true, forwardable: false },
    ApiKey { id: 63, name: "BrokerHeartbeat", cluster_action: true, forwardable: false },
    ApiKey { id: 64, name: "UnregisterBroker", cluster_action: false, forwardable: true },
    ApiKey { id: 65, name: "DescribeTransactions", cluster_action: false, forwardable: false },
    ApiKey { id: 66, name: "ListTransactions", cluster_action: false, forwardable: false },
    ApiKey { id: 67, name: "AllocateProducerIds", cluster_action: true, forwardable: true },
    ApiKey {
        id: 68,
        name: "ConsumerGroupHeartbeat",
        cluster_action: false,
        forwardable: false,
    },
    ApiKey { id: 69, name: "ConsumerGroupDescribe", cluster_action: false, forwardable: false },
    ApiKey {
        id: 70,
        name: "ControllerRegistration",
        cluster_action: false,
        forwardable: false,
    },
    ApiKey {
        id: 71,
        name: "GetTelemetrySubscriptions",
        cluster_action: false,
        forwardable: false,
    },
    ApiKey { id: 72, name: "PushTelemetry", cluster_action: false, forwardable: false },
    ApiKey { id: 73, name: "AssignReplicasToDirs", cluster_action: false, forwardable: false },
    ApiKey { id: 74, name: "ListConfigResources", cluster_action: false, forwardable: false },
    ApiKey {
        id: 75,
        name: "DescribeTopicPartitions",
        cluster_action: false,
        forwardable: false,
    },
    ApiKey { id: 76, name: "ShareGroupHeartbeat", cluster_action: false, forwardable: false },
    ApiKey { id: 77, name: "ShareGroupDescribe", cluster_action: false, forwardable: false },
    ApiKey { id: 78, name: "ShareFetch", cluster_action: false, forwardable: false },
    ApiKey { id: 79, name: "ShareAcknowledge", cluster_action: false, forwardable: false },
    ApiKey { id: 80, name: "AddRaftVoter", cluster_action: false, forwardable: true },
    ApiKey { id: 81, name: "RemoveRaftVoter", cluster_action: false, forwardable: true },
    ApiKey { id: 82, name: "UpdateRaftVoter", cluster_action: false, forwardable: false },
    ApiKey {
        id: 83,
        name: "InitializeShareGroupState",
        cluster_action: true,
        forwardable: false,
    },
    ApiKey { id: 84, name: "ReadShareGroupState", cluster_action: true, forwardable: false },
    ApiKey { id: 85, name: "WriteShareGroupState", cluster_action: true, forwardable: false },
    ApiKey { id: 86, name: "DeleteShareGroupState", cluster_action: true, forwardable: false },
    ApiKey {
        id: 87,
        name: "ReadShareGroupStateSummary",
        cluster_action: true,
        forwardable: false,
    },
    ApiKey { id: 88, name: "StreamsGroupHeartbeat", cluster_action: false, forwardable: false },
    ApiKey { id: 89, name: "StreamsGroupDescribe", cluster_action: false, forwardable: false },
    ApiKey {
        id: 90,
        name: "DescribeShareGroupOffsets",
        cluster_action: false,
        forwardable: false,
    },
    ApiKey {
        id: 91,
        name: "AlterShareGroupOffsets",
        cluster_action: false,
        forwardable: false,
    },
    ApiKey {
        id: 92,
        name: "DeleteShareGroupOffsets",
        cluster_action: false,
        forwardable: false,
    },
];

/// Catalogue accessor helpers. Mirrors the Java `ApiKeys` static methods.
pub struct ApiKeys;

impl ApiKeys {
    /// Mirrors `ApiKeys.values()`.
    pub fn values() -> &'static [ApiKey] {
        ALL_API_KEYS
    }

    /// Look up by id. Mirrors `ApiKeys.forId(int)` — returns
    /// [`KafkaError::IllegalArgument`] for unknown ids.
    pub fn for_id(id: i32) -> Result<&'static ApiKey, KafkaError> {
        for k in ALL_API_KEYS {
            if k.id as i32 == id {
                return Ok(k);
            }
        }
        Err(KafkaError::IllegalArgument(format!("Unexpected api key: {id}")))
    }

    /// Mirrors `ApiKeys.hasId(int)`.
    pub fn has_id(id: i32) -> bool {
        ALL_API_KEYS.iter().any(|k| k.id as i32 == id)
    }

    /// Filter the catalogue by `listener`. Mirrors `apisForListener`.
    pub fn apis_for_listener(listener: ListenerType) -> Vec<&'static ApiKey> {
        ALL_API_KEYS.iter().filter(|k| k.in_scope(listener)).collect()
    }

    /// Mirrors `brokerApis` / `clientApis` (the latter is an alias for the
    /// former in Java).
    pub fn broker_apis() -> Vec<&'static ApiKey> {
        Self::apis_for_listener(ListenerType::Broker)
    }

    /// Mirrors `controllerApis`.
    pub fn controller_apis() -> Vec<&'static ApiKey> {
        Self::apis_for_listener(ListenerType::Controller)
    }

    /// Mirrors `clientApis`.
    pub fn client_apis() -> Vec<&'static ApiKey> {
        Self::broker_apis()
    }
}

impl ApiKey {
    /// The listener types this API is exposed on. Delegates to the generated
    /// `ApiMessageType::listeners()` so the catalogue tracks the JSON spec
    /// instead of a stale hand-coded copy.
    pub fn listeners(&self) -> &'static [ListenerType] {
        self.message_type().listeners()
    }

    /// Whether this API is exposed on the given listener. Mirrors
    /// `ApiKeys.inScope(ListenerType)`.
    pub fn in_scope(&self, listener: ListenerType) -> bool {
        self.listeners().contains(&listener)
    }

    /// The corresponding generated `ApiMessageType` enum variant. Mirrors
    /// Java's `ApiKeys.messageType` field.
    pub fn message_type(&self) -> crate::common::message::api_message_type::ApiMessageType {
        crate::common::message::api_message_type::ApiMessageType::from_api_key(self.id)
            .expect("ApiKey.id has a matching ApiMessageType variant")
    }

    /// Lowest supported version. Mirrors `ApiKeys.oldestVersion()`.
    pub fn oldest_version(&self) -> i16 {
        self.message_type().lowest_supported_version()
    }

    /// Highest supported (released) version. Mirrors
    /// `ApiKeys.latestVersion()`.
    pub fn latest_version(&self) -> i16 {
        self.message_type().highest_supported_version(false)
    }

    /// Highest supported version, optionally including unstable releases.
    /// Mirrors `ApiKeys.latestVersion(boolean enableUnstableLastVersion)`.
    pub fn latest_version_unstable(&self, enable_unstable_last_version: bool) -> i16 {
        self.message_type().highest_supported_version(enable_unstable_last_version)
    }

    /// Returns true if `version` is in the inclusive range
    /// `[oldestVersion(), latestVersion()]`. Mirrors
    /// `ApiKeys.isVersionSupported(short)`.
    pub fn is_version_supported(&self, version: i16) -> bool {
        version >= self.oldest_version() && version <= self.latest_version()
    }

    /// Whether the API has any released versions. Mirrors
    /// `ApiKeys.hasValidVersion()`.
    pub fn has_valid_version(&self) -> bool {
        self.oldest_version() <= self.latest_version()
    }

    /// Whether the given version is deprecated. Mirrors
    /// `ApiKeys.isVersionDeprecated(short)`.
    pub fn is_version_deprecated(&self, version: i16) -> bool {
        let mt = self.message_type();
        version >= mt.lowest_deprecated_version() && version <= mt.highest_deprecated_version()
    }

    /// Request header version for a given API version. Mirrors
    /// `ApiKeys.requestHeaderVersion(short)`.
    pub fn request_header_version(&self, version: i16) -> i16 {
        self.message_type().request_header_version(version)
    }

    /// Response header version for a given API version. Mirrors
    /// `ApiKeys.responseHeaderVersion(short)`.
    pub fn response_header_version(&self, version: i16) -> i16 {
        self.message_type().response_header_version(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translation of `ApiKeysTest#testForIdWithInvalidIdLow`.
    #[test]
    fn for_id_with_invalid_id_low() {
        assert!(ApiKeys::for_id(-1).is_err());
    }

    /// Translation of `ApiKeysTest#testForIdWithInvalidIdHigh`.
    #[test]
    fn for_id_with_invalid_id_high() {
        assert!(ApiKeys::for_id(10000).is_err());
    }

    /// Translation of `ApiKeysTest#testAlterPartitionIsClusterAction`.
    #[test]
    fn alter_partition_is_cluster_action() {
        let alter_partition = ApiKeys::for_id(56).expect("ALTER_PARTITION exists");
        assert!(alter_partition.cluster_action);
    }

    /// Translation of `ApiKeysTest#testApiScope`. Every API with at least
    /// one supported version must be exposed on a listener. APIs with no
    /// valid versions (`LEADER_AND_ISR`, `STOP_REPLICA`, `UPDATE_METADATA`,
    /// `CONTROLLED_SHUTDOWN`) are exempt — Java applies the same
    /// `hasValidVersion()` guard.
    #[test]
    fn every_api_has_a_listener() {
        for k in ApiKeys::values() {
            if !k.has_valid_version() {
                continue;
            }
            assert!(!k.listeners().is_empty(), "missing scope for {}", k.name);
        }
    }

    #[test]
    fn for_id_known_apis() {
        assert_eq!(ApiKeys::for_id(0).unwrap().name, "Produce");
        assert_eq!(ApiKeys::for_id(3).unwrap().name, "Metadata");
        assert_eq!(ApiKeys::for_id(18).unwrap().name, "ApiVersions");
    }

    #[test]
    fn has_id_known() {
        assert!(ApiKeys::has_id(0));
        assert!(!ApiKeys::has_id(-1));
        assert!(!ApiKeys::has_id(10000));
    }

    #[test]
    fn produce_is_in_broker_scope() {
        let produce = ApiKeys::for_id(0).unwrap();
        assert!(produce.in_scope(ListenerType::Broker));
    }

    // Note: `testResponseThrottleTime`, `testHasValidVersions`, and
    // `testHtmlOnlyHaveStableApi` need request/response Schemas exposed via
    // ApiMessageType.responseSchemas[], which is generated in Phase 2d. They
    // are deferred until then; the Phase 2d Actor will translate them when
    // the message catalog is wired up.

    /// Drift check between Phase 2c's hand-coded `ALL_API_KEYS` table and the
    /// Phase 2d generated `ApiMessageType` enum.
    ///
    /// Phase 2c's NOTES.md committed to either replacing the hand-coded
    /// table with one populated from `ApiMessageType` or — if the refactor
    /// was deferred — adding a test that asserts they agree on every
    /// observable field. We chose the latter because the generated
    /// `ApiMessageType` is mid-evolution (Phase 2d) and converting the
    /// catalogue in-place would lock subsequent generator changes against
    /// the hand-coded shape.
    ///
    /// We compare `id` ↔ `api_key()` and `name` ↔ `name()` in both
    /// directions. Listeners are not stored on `ApiKey` at all — the field
    /// was removed when [`ApiKey::listeners`] was wired to delegate to the
    /// generated `ApiMessageType::listeners()` (Issue 4 fix), so
    /// listener-set drift is impossible by construction.
    ///
    /// `cluster_action`/`forwardable` live on `ApiKeys.java`'s enum
    /// constructor, not on `ApiMessageType`. They remain on the hand-coded
    /// table; the generated catalogue is not the source of truth here.
    #[test]
    fn api_keys_match_generated_api_message_type() {
        use crate::common::message::api_message_type::ApiMessageType;

        for k in ApiKeys::values() {
            let generated = ApiMessageType::from_api_key(k.id)
                .unwrap_or_else(|| panic!("ApiMessageType missing variant for api_key={}", k.id));

            assert_eq!(
                generated.api_key(),
                k.id,
                "api_key mismatch for {}: generated={}, hand-coded={}",
                k.name,
                generated.api_key(),
                k.id
            );

            assert_eq!(
                generated.name(),
                k.name,
                "name mismatch for api_key={}: generated={}, hand-coded={}",
                k.id,
                generated.name(),
                k.name
            );
        }

        // Reverse direction: every generated variant must be in the
        // hand-coded table.
        for id in 0..i16::MAX {
            if let Some(generated) = ApiMessageType::from_api_key(id) {
                assert!(
                    ApiKeys::has_id(id as i32),
                    "ApiMessageType has variant for api_key={} ({}) but ALL_API_KEYS does not",
                    id,
                    generated.name()
                );
            }
        }
    }
}
