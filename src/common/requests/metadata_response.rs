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

//! Translation of `org.apache.kafka.common.requests.MetadataResponse`.

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::common::Node;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::errors::KafkaError;
use crate::common::message::metadata_response_data::MetadataResponseData;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::{ApiKey, ApiKeys, Errors, Message};
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::AbstractResponse;
use crate::common::requests::abstract_response;
use crate::common::requests::request_utils;
use crate::common::uuid::{Uuid, ZERO_UUID};

/// Per-topic metadata derived from a [`MetadataResponse`].
///
/// Mirrors the Java inner class `MetadataResponse.TopicMetadata`. Authored
/// operations are mutable in Java via the `authorizedOperations(int)`
/// setter; we expose it via [`TopicMetadata::set_authorized_operations`]
/// rather than a direct public field to keep the struct's invariants
/// owned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicMetadata {
    error: Errors,
    topic: String,
    topic_id: Uuid,
    is_internal: bool,
    partition_metadata: Vec<PartitionMetadata>,
    authorized_operations: i32,
}

impl TopicMetadata {
    /// Mirrors the 6-arg Java constructor.
    pub fn new(
        error: Errors,
        topic: impl Into<String>,
        topic_id: Uuid,
        is_internal: bool,
        partition_metadata: Vec<PartitionMetadata>,
        authorized_operations: i32,
    ) -> Self {
        TopicMetadata {
            error,
            topic: topic.into(),
            topic_id,
            is_internal,
            partition_metadata,
            authorized_operations,
        }
    }

    /// Mirrors the 4-arg Java convenience constructor (`Uuid.ZERO_UUID`,
    /// `AUTHORIZED_OPERATIONS_OMITTED`).
    pub fn new_simple(
        error: Errors,
        topic: impl Into<String>,
        is_internal: bool,
        partition_metadata: Vec<PartitionMetadata>,
    ) -> Self {
        TopicMetadata::new(
            error,
            topic,
            ZERO_UUID,
            is_internal,
            partition_metadata,
            MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        )
    }

    /// Mirrors `error()`.
    pub fn error(&self) -> Errors {
        self.error
    }

    /// Mirrors `topic()`.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Mirrors `topicId()`.
    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }

    /// Mirrors `isInternal()`.
    pub fn is_internal(&self) -> bool {
        self.is_internal
    }

    /// Mirrors `partitionMetadata()`.
    pub fn partition_metadata(&self) -> &[PartitionMetadata] {
        &self.partition_metadata
    }

    /// Mirrors `authorizedOperations()`.
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }

    /// Mirrors `authorizedOperations(int)`.
    pub fn set_authorized_operations(&mut self, authorized_operations: i32) {
        self.authorized_operations = authorized_operations;
    }
}

/// Per-partition state derived from a [`MetadataResponse`].
///
/// Mirrors the Java inner class `MetadataResponse.PartitionMetadata`. All
/// fields are immutable after construction; Java exposes them as `public
/// final` so we expose getters rather than public fields to keep the
/// `TopicPartition` interning under our control.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Per-partition error code.
    pub error: Errors,
    /// The (topic, partition) tuple for this entry.
    pub topic_partition: TopicPartition,
    /// Current leader id, if known.
    pub leader_id: Option<i32>,
    /// Current leader epoch, if known.
    pub leader_epoch: Option<i32>,
    /// All assigned replica ids (may be empty if unknown).
    pub replica_ids: Vec<i32>,
    /// Subset of replica ids that are in sync with the leader.
    pub in_sync_replica_ids: Vec<i32>,
    /// Subset of replica ids that are offline.
    pub offline_replica_ids: Vec<i32>,
}

impl PartitionMetadata {
    /// Mirrors the Java 7-arg constructor.
    pub fn new(
        error: Errors,
        topic_partition: TopicPartition,
        leader_id: Option<i32>,
        leader_epoch: Option<i32>,
        replica_ids: Vec<i32>,
        in_sync_replica_ids: Vec<i32>,
        offline_replica_ids: Vec<i32>,
    ) -> Self {
        PartitionMetadata {
            error,
            topic_partition,
            leader_id,
            leader_epoch,
            replica_ids,
            in_sync_replica_ids,
            offline_replica_ids,
        }
    }

    /// Mirrors `partition()` — convenience getter for the partition id.
    pub fn partition(&self) -> i32 {
        self.topic_partition.partition()
    }

    /// Mirrors `topic()` — convenience getter for the topic name.
    pub fn topic(&self) -> &str {
        self.topic_partition.topic()
    }

    /// Mirrors `withoutLeaderEpoch()` — returns a clone with leader epoch cleared.
    pub fn without_leader_epoch(&self) -> PartitionMetadata {
        PartitionMetadata {
            error: self.error,
            topic_partition: self.topic_partition.clone(),
            leader_id: self.leader_id,
            leader_epoch: None,
            replica_ids: self.replica_ids.clone(),
            in_sync_replica_ids: self.in_sync_replica_ids.clone(),
            offline_replica_ids: self.offline_replica_ids.clone(),
        }
    }
}

/// Translation of `org.apache.kafka.common.requests.MetadataResponse`.
pub struct MetadataResponse {
    data: MetadataResponseData,
    has_reliable_leader_epochs: bool,
    /// Cached projection of [`Self::data`] into the higher-level
    /// `TopicMetadata` / broker map view used by [`Self::topic_metadata`],
    /// [`Self::brokers_by_id`], and [`Self::controller`]. Mirrors Java's
    /// volatile `holder` field but built lazily on first call.
    holder: OnceLock<Holder>,
}

/// Cached projection over the raw [`MetadataResponseData`]. Built lazily
/// from [`MetadataResponse::holder`] on first access. Mirrors the private
/// `MetadataResponse.Holder` class.
struct Holder {
    brokers: HashMap<i32, Node>,
    controller: Option<Node>,
    topic_metadata: Vec<TopicMetadata>,
}

impl MetadataResponse {
    /// Mirrors `MetadataResponse.NO_CONTROLLER_ID = -1`.
    pub const NO_CONTROLLER_ID: i32 = -1;
    /// Mirrors `MetadataResponse.NO_LEADER_ID = -1`.
    pub const NO_LEADER_ID: i32 = -1;
    /// Mirrors `MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED = Integer.MIN_VALUE`.
    pub const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

    /// Mirrors `new MetadataResponse(MetadataResponseData, boolean)`.
    pub fn new(data: MetadataResponseData, has_reliable_leader_epochs: bool) -> Self {
        MetadataResponse { data, has_reliable_leader_epochs, holder: OnceLock::new() }
    }

    /// Mirrors `new MetadataResponse(MetadataResponseData, short version)`.
    pub fn new_for_version(data: MetadataResponseData, version: i16) -> Self {
        MetadataResponse::new(data, Self::has_reliable_leader_epochs_for(version))
    }

    /// Mirrors the package-private `hasReliableLeaderEpochs(short)`.
    pub fn has_reliable_leader_epochs_for(version: i16) -> bool {
        version >= 9
    }

    /// Mirrors `MetadataResponse.data()`.
    pub fn response_data(&self) -> &MetadataResponseData {
        &self.data
    }

    /// Mirrors `MetadataResponse.hasReliableLeaderEpochs()`.
    pub fn has_reliable_leader_epochs(&self) -> bool {
        self.has_reliable_leader_epochs
    }

    /// Mirrors `MetadataResponse.errors()`. Returns a map of
    /// topic-name → error for every topic with a non-NONE error.
    pub fn errors(&self) -> Result<HashMap<String, Errors>, KafkaError> {
        let mut out = HashMap::new();
        for metadata in &self.data.topics {
            if metadata.name.is_none() {
                return Err(KafkaError::IllegalArgument(
                    "Use errorsByTopicId() when managing topic using topic id".to_owned(),
                ));
            }
            let err = Errors::for_code(metadata.error_code);
            if err != Errors::None {
                out.insert(metadata.name.clone().unwrap_or_default(), err);
            }
        }
        Ok(out)
    }

    /// Mirrors `MetadataResponse.errorsByTopicId()`.
    pub fn errors_by_topic_id(&self) -> Result<HashMap<Uuid, Errors>, KafkaError> {
        let mut out = HashMap::new();
        for metadata in &self.data.topics {
            if metadata.topic_id == ZERO_UUID {
                return Err(KafkaError::IllegalArgument(
                    "Use errors() when managing topic using topic name".to_owned(),
                ));
            }
            let err = Errors::for_code(metadata.error_code);
            if err != Errors::None {
                out.insert(metadata.topic_id, err);
            }
        }
        Ok(out)
    }

    /// Mirrors `MetadataResponse.topLevelError()`.
    pub fn top_level_error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Mirrors `MetadataResponse.clusterId()`.
    pub fn cluster_id(&self) -> Option<&str> {
        self.data.cluster_id.as_deref()
    }

    /// Mirrors `MetadataResponse.clusterAuthorizedOperations()`.
    pub fn cluster_authorized_operations(&self) -> i32 {
        self.data.cluster_authorized_operations
    }

    /// Mirrors `MetadataResponse.topicAuthorizedOperations(String)`.
    pub fn topic_authorized_operations(&self, topic_name: &str) -> Option<i32> {
        self.data
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic_name))
            .map(|t| t.topic_authorized_operations)
    }

    /// Mirrors `MetadataResponse.topicsByError(Errors)`.
    pub fn topics_by_error(&self, error: Errors) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        for metadata in &self.data.topics {
            if metadata.error_code == error.code()
                && let Some(ref name) = metadata.name
            {
                out.insert(name.clone());
            }
        }
        out
    }

    /// Mirrors `MetadataResponse.parse(Readable, short)`.
    pub fn parse(accessor: &mut ByteBufferAccessor, version: i16) -> Result<Self, KafkaError> {
        let data = MetadataResponseData::read(accessor, version)?;
        Ok(MetadataResponse::new(data, Self::has_reliable_leader_epochs_for(version)))
    }

    /// Lazily build (or fetch) the cached [`Holder`] projection of
    /// [`Self::data`]. Mirrors Java's double-checked-locking `holder()`
    /// method.
    fn holder(&self) -> &Holder {
        self.holder.get_or_init(|| Self::build_holder(&self.data))
    }

    fn build_holder(data: &MetadataResponseData) -> Holder {
        let mut brokers: HashMap<i32, Node> = HashMap::with_capacity(data.brokers.len());
        for broker in &data.brokers {
            // Java passes the broker's rack to `Node`; rack is `Option<String>`
            // so we forward as-is.
            let node = Node::new_with_rack(broker.node_id, broker.host.clone(), broker.port, broker.rack.clone());
            brokers.insert(broker.node_id, node);
        }

        let mut topic_metadata: Vec<TopicMetadata> = Vec::with_capacity(data.topics.len());
        for topic in &data.topics {
            let topic_error = Errors::for_code(topic.error_code);
            // `name` is nullable in flexible versions; substitute empty so
            // the `TopicPartition::topic` is still a valid `Arc<str>`. Java
            // permits `null` here but the producer-side `Metadata` code we
            // translate later treats `name == null` as `errorsByTopicId()`
            // territory. We carry the empty string through; equality with
            // a non-empty topic name is impossible.
            let topic_name = topic.name.clone().unwrap_or_default();
            let topic_id = topic.topic_id;
            let is_internal = topic.is_internal;

            let mut partition_metadata: Vec<PartitionMetadata> = Vec::with_capacity(topic.partitions.len());
            for p in &topic.partitions {
                let partition_error = Errors::for_code(p.error_code);
                let partition_index = p.partition_index;
                let leader_id_opt = if p.leader_id < 0 { None } else { Some(p.leader_id) };
                let leader_epoch = request_utils::get_leader_epoch(p.leader_epoch);
                let topic_partition = TopicPartition::new(topic_name.clone(), partition_index);
                partition_metadata.push(PartitionMetadata::new(
                    partition_error,
                    topic_partition,
                    leader_id_opt,
                    leader_epoch,
                    p.replica_nodes.clone(),
                    p.isr_nodes.clone(),
                    p.offline_replicas.clone(),
                ));
            }

            topic_metadata.push(TopicMetadata::new(
                topic_error,
                topic_name,
                topic_id,
                is_internal,
                partition_metadata,
                topic.topic_authorized_operations,
            ));
        }

        let controller = brokers.get(&data.controller_id).cloned();
        Holder { brokers, controller, topic_metadata }
    }

    /// Mirrors `MetadataResponse.brokers()` — collection of all broker nodes.
    pub fn brokers(&self) -> Vec<Node> {
        self.holder().brokers.values().cloned().collect()
    }

    /// Mirrors `MetadataResponse.brokersById()` — id → node map.
    pub fn brokers_by_id(&self) -> HashMap<i32, Node> {
        self.holder().brokers.clone()
    }

    /// Mirrors `MetadataResponse.controller()` — the controller node, or
    /// `None` if the response has no controller (e.g.
    /// `controllerId == NO_CONTROLLER_ID`).
    pub fn controller(&self) -> Option<Node> {
        self.holder().controller.clone()
    }

    /// Mirrors `MetadataResponse.topicMetadata()` — projected topic metadata
    /// for every topic in the response.
    pub fn topic_metadata(&self) -> &[TopicMetadata] {
        &self.holder().topic_metadata
    }

    /// Mirrors `MetadataResponse.toPartitionInfo(PartitionMetadata, Map<Integer, Node>)`.
    ///
    /// Resolves replica/isr/offline node-ids against `nodes_by_id`,
    /// substituting `Node::new(replicaId, "", -1)` for missing nodes
    /// (matching Java's `convertToNodeArray` fallback).
    pub fn to_partition_info(metadata: &PartitionMetadata, nodes_by_id: &HashMap<i32, Node>) -> PartitionInfo {
        let leader = metadata.leader_id.and_then(|id| nodes_by_id.get(&id).cloned());
        let replicas = convert_to_node_array(&metadata.replica_ids, nodes_by_id);
        let in_sync = convert_to_node_array(&metadata.in_sync_replica_ids, nodes_by_id);
        let offline = convert_to_node_array(&metadata.offline_replica_ids, nodes_by_id);
        PartitionInfo::new_with_offline(metadata.topic(), metadata.partition(), leader, replicas, in_sync, offline)
    }
}

/// Mirror of Java's private `convertToNodeArray` helper. Allocates a
/// `Vec<Node>` rather than the Java fixed-size array since callers (and
/// `PartitionInfo`) consume slices.
fn convert_to_node_array(replica_ids: &[i32], nodes_by_id: &HashMap<i32, Node>) -> Vec<Node> {
    let mut nodes = Vec::with_capacity(replica_ids.len());
    for &replica_id in replica_ids {
        let node = match nodes_by_id.get(&replica_id) {
            Some(n) => n.clone(),
            None => Node::new(replica_id, String::new(), -1),
        };
        nodes.push(node);
    }
    nodes
}

impl AbstractRequestResponse for MetadataResponse {
    fn data(&self) -> &dyn Message {
        &self.data
    }
}

impl AbstractResponse for MetadataResponse {
    fn api_key(&self) -> &'static ApiKey {
        // `ApiKeys::for_id(3)` is infallible (METADATA is always wired in)
        // but `expect()` would still panic from a public-API entry point
        // which violates CLAUDE.md rule 10.1. Cache the lookup once via
        // `OnceLock` so subsequent calls are a single load.
        static METADATA: OnceLock<&'static ApiKey> = OnceLock::new();
        METADATA.get_or_init(|| ApiKeys::for_id(3).expect("METADATA api_key always present in ALL_API_KEYS"))
    }

    fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut out: HashMap<Errors, i32> = HashMap::new();
        for metadata in &self.data.topics {
            for partition in &metadata.partitions {
                abstract_response::update_error_counts(&mut out, Errors::for_code(partition.error_code));
            }
            abstract_response::update_error_counts(&mut out, Errors::for_code(metadata.error_code));
        }
        out
    }

    fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.throttle_time_ms = throttle_time_ms;
    }

    fn should_client_throttle(&self, version: i16) -> bool {
        version >= 6
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::metadata_response_data::MetadataResponseTopic;

    #[test]
    fn parse_round_trip_v12() {
        let topic = MetadataResponseTopic {
            error_code: Errors::None.code(),
            name: Some("topic1".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let resp_data = MetadataResponseData {
            throttle_time_ms: 5,
            brokers: Vec::new(),
            cluster_id: Some("cluster".to_owned()),
            controller_id: 1,
            topics: vec![topic],
            cluster_authorized_operations: 0,
            error_code: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let resp = MetadataResponse::new_for_version(resp_data, 12);
        let mut serialized = AbstractResponse::serialize(&resp, 12).expect("serialize");
        let parsed = MetadataResponse::parse(&mut serialized, 12).expect("parse");
        assert_eq!(parsed.cluster_id(), Some("cluster"));
        assert_eq!(parsed.response_data().topics.len(), 1);
        assert!(parsed.has_reliable_leader_epochs());
    }

    #[test]
    fn errors_returns_only_non_none() {
        let topic_ok = MetadataResponseTopic {
            error_code: Errors::None.code(),
            name: Some("ok".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let topic_err = MetadataResponseTopic {
            error_code: Errors::UnknownTopicOrPartition.code(),
            name: Some("missing".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let resp = MetadataResponse::new(
            MetadataResponseData { topics: vec![topic_ok, topic_err], ..MetadataResponseData::new() },
            true,
        );
        let map = resp.errors().expect("errors");
        assert_eq!(map.len(), 1);
        assert_eq!(map.get("missing"), Some(&Errors::UnknownTopicOrPartition));
    }

    #[test]
    fn errors_by_topic_id_when_named_only_returns_error() {
        let zero_id_topic = MetadataResponseTopic {
            error_code: Errors::None.code(),
            name: Some("ok".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: Vec::new(),
        };
        let resp = MetadataResponse::new(
            MetadataResponseData { topics: vec![zero_id_topic], ..MetadataResponseData::new() },
            true,
        );
        let result = resp.errors_by_topic_id();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("Use errors()"), "expected guidance message, got {msg}");
    }

    #[test]
    fn topic_authorized_operations_returns_value_or_none() {
        let topic = MetadataResponseTopic {
            error_code: Errors::None.code(),
            name: Some("t".to_owned()),
            topic_id: ZERO_UUID,
            is_internal: false,
            partitions: Vec::new(),
            topic_authorized_operations: 0xabc,
            unknown_tagged_fields: Vec::new(),
        };
        let resp = MetadataResponse::new(
            MetadataResponseData { topics: vec![topic], ..MetadataResponseData::new() },
            true,
        );
        assert_eq!(resp.topic_authorized_operations("t"), Some(0xabc));
        assert_eq!(resp.topic_authorized_operations("missing"), None);
    }
}
