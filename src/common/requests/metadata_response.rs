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

#![allow(dead_code)]
//! Metadata response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.MetadataResponse`.
//!
//! Possible topic-level error codes:
//!  - `UnknownTopicOrPartition` (3)
//!  - `LeaderNotAvailable` (5)
//!  - `InvalidTopicException` (17)
//!  - `TopicAuthorizationFailed` (29)
//!
//! Possible partition-level error codes:
//!  - `LeaderNotAvailable` (5)
//!  - `ReplicaNotAvailable` (9)

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::OnceLock;

use crate::MetadataResponseData;
use crate::common::Cluster;
use crate::common::Node;
use crate::common::PartitionInfo;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponseTopic};

use super::AbstractResponse;
use super::RequestUtils;

/// A Metadata response.
///
/// Corresponds to `org.apache.kafka.common.requests.MetadataResponse`.
#[derive(Debug, Clone)]
pub struct MetadataResponse {
    data: MetadataResponseData,
    has_reliable_leader_epochs: bool,
    holder: OnceLock<Holder>,
}

impl MetadataResponse {
    /// Sentinel value indicating that the controller ID is unknown.
    pub const NO_CONTROLLER_ID: i32 = -1;

    /// Sentinel value indicating that the partition has no leader.
    pub const NO_LEADER_ID: i32 = -1;

    /// Sentinel value indicating that authorized operations have been omitted.
    pub const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

    /// Creates a new `MetadataResponse` from data and version.
    pub fn new_version(data: MetadataResponseData, version: i16) -> Self {
        Self::new_has_reliable_leader_epochs(data, has_reliable_leader_epochs(version))
    }

    /// Creates a new `MetadataResponse` from data with explicit epoch reliability flag.
    pub fn new_has_reliable_leader_epochs(data: MetadataResponseData, has_reliable_leader_epochs: bool) -> Self {
        Self { data, has_reliable_leader_epochs, holder: OnceLock::new() }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::METADATA
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &MetadataResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub fn data_mut(&mut self) -> &mut MetadataResponseData {
        // Invalidate the holder when the data changes
        self.holder = OnceLock::new();
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns a map of topic names to their errors for topics with non-zero error codes.
    ///
    /// # Panics
    ///
    /// Panics if any topic has a `None` name (use `errors_by_topic_id()` instead).
    pub fn errors(&self) -> HashMap<String, Errors> {
        let mut errors = HashMap::new();
        for metadata in &self.data.topics {
            let name = metadata
                .name
                .as_ref()
                .expect("Use errors_by_topic_id() when managing topic using topic id");
            if metadata.error_code != Errors::None.code() {
                errors.insert(name.clone(), Errors::for_code(metadata.error_code));
            }
        }
        errors
    }

    /// Returns the top-level error.
    pub fn top_level_error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns a map of topic IDs to their errors for topics with non-zero error codes.
    ///
    /// # Panics
    ///
    /// Panics if any topic has a zero UUID (use `errors()` instead).
    pub fn errors_by_topic_id(&self) -> HashMap<Uuid, Errors> {
        let mut errors = HashMap::new();
        for metadata in &self.data.topics {
            assert!(
                metadata.topic_id != Uuid::zero(),
                "Use errors() when managing topic using topic name"
            );
            if metadata.error_code != Errors::None.code() {
                errors.insert(metadata.topic_id, Errors::for_code(metadata.error_code));
            }
        }
        errors
    }

    /// Returns error counts aggregating both topic-level and partition-level errors.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut error_counts = HashMap::new();
        for metadata in &self.data.topics {
            for p in &metadata.partitions {
                AbstractResponse::update_error_counts(&mut error_counts, Errors::for_code(p.error_code));
            }
            AbstractResponse::update_error_counts(&mut error_counts, Errors::for_code(metadata.error_code));
        }
        error_counts
    }

    /// Returns the set of topics with the specified error.
    pub fn topics_by_error(&self, error: Errors) -> HashSet<String> {
        let mut error_topics = HashSet::new();
        for metadata in &self.data.topics {
            if metadata.error_code == error.code()
                && let Some(name) = &metadata.name
            {
                error_topics.insert(name.clone());
            }
        }
        error_topics
    }

    /// Builds a snapshot of the cluster metadata from this response.
    pub fn build_cluster(&self) -> Cluster {
        let mut internal_topics = HashSet::new();
        let mut partitions = Vec::new();
        let mut topic_ids = HashMap::new();

        for metadata in self.topic_metadata() {
            if metadata.error == Errors::None {
                if metadata.is_internal {
                    internal_topics.insert(metadata.topic.clone());
                }
                if metadata.topic_id != Uuid::zero() {
                    topic_ids.insert(metadata.topic.clone(), metadata.topic_id);
                }
                for partition_metadata in &metadata.partition_metadata {
                    partitions.push(Self::to_partition_info(partition_metadata, &self.holder().brokers));
                }
            }
        }
        Cluster::new_invalid_topics_controller_topic_ids(
            self.data.cluster_id.clone(),
            self.brokers().to_vec(),
            partitions,
            self.topics_by_error(Errors::TopicAuthorizationFailed),
            self.topics_by_error(Errors::InvalidTopicError),
            internal_topics,
            self.controller().cloned(),
            topic_ids,
        )
    }

    /// Converts a `PartitionMetadata` to a `PartitionInfo`.
    pub fn to_partition_info(metadata: &PartitionMetadata, nodes_by_id: &HashMap<i32, Node>) -> PartitionInfo {
        let leader = metadata.leader_id.and_then(|id| nodes_by_id.get(&id)).cloned();

        let replicas = convert_to_node_vec(&metadata.replica_ids, nodes_by_id);
        let isr = convert_to_node_vec(&metadata.in_sync_replica_ids, nodes_by_id);
        let offline = convert_to_node_vec(&metadata.offline_replica_ids, nodes_by_id);

        PartitionInfo::new_offline_replicas(
            metadata.topic_partition.topic().to_string(),
            metadata.topic_partition.partition(),
            leader,
            replicas,
            isr,
            offline,
        )
    }

    /// Returns a 32-bit bitfield representing authorized operations for a topic.
    pub fn topic_authorized_operations(&self, topic_name: &str) -> Option<i32> {
        self.data
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic_name))
            .map(|t| t.topic_authorized_operations)
    }

    /// Returns a 32-bit bitfield representing authorized operations for this cluster.
    pub fn cluster_authorized_operations(&self) -> i32 {
        self.data.cluster_authorized_operations
    }

    fn holder(&self) -> &Holder {
        self.holder.get_or_init(|| Holder::new(&self.data))
    }

    /// Returns all brokers returned in the metadata response.
    pub fn brokers(&self) -> &[Node] {
        &self.holder().broker_list
    }

    /// Returns brokers indexed by id.
    pub fn brokers_by_id(&self) -> &HashMap<i32, Node> {
        &self.holder().brokers
    }

    /// Returns all topic metadata returned in the metadata response.
    pub fn topic_metadata(&self) -> &[TopicMetadata] {
        &self.holder().topic_metadata
    }

    /// The controller node returned in metadata response, or `None` if not known.
    pub fn controller(&self) -> Option<&Node> {
        self.holder().controller.as_ref()
    }

    /// The cluster identifier returned in the metadata response.
    pub fn cluster_id(&self) -> Option<&str> {
        self.data.cluster_id.as_deref()
    }

    /// Check whether the leader epochs returned from the response can be relied on
    /// for epoch validation in Fetch, ListOffsets, and OffsetsForLeaderEpoch requests.
    pub fn has_reliable_leader_epochs(&self) -> bool {
        self.has_reliable_leader_epochs
    }

    /// Parses a `MetadataResponse` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> std::io::Result<Self> {
        let data = MetadataResponseData::read(readable, version)?;
        Ok(Self::new_has_reliable_leader_epochs(data, has_reliable_leader_epochs(version)))
    }

    /// Returns whether the client should throttle upon receiving this response.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 6
    }

    /// Constructs a `MetadataResponse` for testing.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_response_version(
        version: i16,
        throttle_time_ms: i32,
        brokers: &[Node],
        cluster_id: Option<String>,
        controller_id: i32,
        topics: Vec<MetadataResponseTopic>,
        cluster_authorized_operations: i32,
    ) -> Self {
        Self::prepare_response_has_reliable_epoch(
            has_reliable_leader_epochs(version),
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            cluster_authorized_operations,
        )
    }

    /// Constructs a `MetadataResponse` with explicit leader epoch reliability flag.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_response_has_reliable_epoch(
        has_reliable_epoch: bool,
        throttle_time_ms: i32,
        brokers: &[Node],
        cluster_id: Option<String>,
        controller_id: i32,
        topics: Vec<MetadataResponseTopic>,
        cluster_authorized_operations: i32,
    ) -> Self {
        let mut response_data = MetadataResponseData::new();
        response_data.set_throttle_time_ms(throttle_time_ms);

        let broker_data: Vec<MetadataResponseBroker> = brokers
            .iter()
            .map(|broker| {
                let mut b = MetadataResponseBroker::new();
                b.set_node_id(broker.id());
                b.set_host(broker.host().to_string());
                b.set_port(broker.port());
                b.set_rack(broker.rack().map(|r| r.to_string()));
                b
            })
            .collect();
        response_data.set_brokers(broker_data);

        response_data.set_cluster_id(cluster_id);
        response_data.set_controller_id(controller_id);
        response_data.set_cluster_authorized_operations(cluster_authorized_operations);
        response_data.set_topics(topics);

        Self::new_has_reliable_leader_epochs(response_data, has_reliable_epoch)
    }
}

impl std::fmt::Display for MetadataResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetadataResponse(data={:?})", self.data)
    }
}

/// Prior to Kafka version 2.4 (which coincides with Metadata version 9), the broker
/// does not propagate leader epoch information accurately while a reassignment is in
/// progress. Relying on a stale epoch can lead to FENCED_LEADER_EPOCH errors which
/// can prevent consumption throughout the course of a reassignment.
fn has_reliable_leader_epochs(version: i16) -> bool {
    version >= 9
}

fn convert_to_node_vec(replica_ids: &[i32], nodes_by_id: &HashMap<i32, Node>) -> Vec<Node> {
    replica_ids
        .iter()
        .map(|&id| {
            nodes_by_id
                .get(&id)
                .cloned()
                .unwrap_or_else(|| Node::new(id, String::new(), -1))
        })
        .collect()
}

/// Lazy holder for computed broker/topic metadata.
#[derive(Debug, Clone)]
struct Holder {
    brokers: HashMap<i32, Node>,
    broker_list: Vec<Node>,
    controller: Option<Node>,
    topic_metadata: Vec<TopicMetadata>,
}

impl Holder {
    fn new(data: &MetadataResponseData) -> Self {
        let brokers = Self::create_brokers(data);
        let broker_list: Vec<Node> = brokers.values().cloned().collect();
        let controller = brokers.get(&data.controller_id).cloned();
        let topic_metadata = Self::create_topic_metadata(data);
        Self { brokers, broker_list, controller, topic_metadata }
    }

    fn create_brokers(data: &MetadataResponseData) -> HashMap<i32, Node> {
        let mut map = HashMap::new();
        for b in &data.brokers {
            let node = Node::new_rack(b.node_id, b.host.clone(), b.port, b.rack.clone());
            map.insert(b.node_id, node);
        }
        map
    }

    fn create_topic_metadata(data: &MetadataResponseData) -> Vec<TopicMetadata> {
        let mut topic_metadata_list = Vec::new();
        for topic_meta in &data.topics {
            let topic_error = Errors::for_code(topic_meta.error_code);
            let topic = topic_meta.name.clone().unwrap_or_default();
            let topic_id = topic_meta.topic_id;
            let is_internal = topic_meta.is_internal;
            let mut partition_metadata_list = Vec::new();

            for partition_meta in &topic_meta.partitions {
                let partition_error = Errors::for_code(partition_meta.error_code);
                let partition_index = partition_meta.partition_index;

                let leader_id = partition_meta.leader_id;
                let leader_id_opt = if leader_id < 0 { None } else { Some(leader_id) };

                let leader_epoch = RequestUtils::get_leader_epoch(partition_meta.leader_epoch);
                let topic_partition = TopicPartition::new(topic.clone(), partition_index);
                partition_metadata_list.push(PartitionMetadata {
                    error: partition_error,
                    topic_partition,
                    leader_id: leader_id_opt,
                    leader_epoch,
                    replica_ids: partition_meta.replica_nodes.clone(),
                    in_sync_replica_ids: partition_meta.isr_nodes.clone(),
                    offline_replica_ids: partition_meta.offline_replicas.clone(),
                });
            }

            topic_metadata_list.push(TopicMetadata {
                error: topic_error,
                topic,
                topic_id,
                is_internal,
                partition_metadata: partition_metadata_list,
                authorized_operations: topic_meta.topic_authorized_operations,
            });
        }
        topic_metadata_list
    }
}

/// Topic-level metadata from a MetadataResponse.
///
/// Corresponds to `MetadataResponse.TopicMetadata` in Java.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    /// The topic-level error.
    pub error: Errors,
    /// The topic name.
    pub topic: String,
    /// The topic ID.
    pub topic_id: Uuid,
    /// Whether this is an internal topic.
    pub is_internal: bool,
    /// Partition metadata for this topic.
    pub partition_metadata: Vec<PartitionMetadata>,
    /// Authorized operations bitfield.
    pub authorized_operations: i32,
}

impl TopicMetadata {
    /// Creates a new `TopicMetadata` with default authorized operations.
    pub fn new(error: Errors, topic: String, is_internal: bool, partition_metadata: Vec<PartitionMetadata>) -> Self {
        Self {
            error,
            topic,
            topic_id: Uuid::zero(),
            is_internal,
            partition_metadata,
            authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        }
    }

    /// Returns the topic-level error.
    pub fn error(&self) -> Errors {
        self.error
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the topic ID.
    pub fn topic_id(&self) -> Uuid {
        self.topic_id
    }

    /// Returns whether this is an internal topic.
    pub fn is_internal(&self) -> bool {
        self.is_internal
    }

    /// Returns the partition metadata.
    pub fn partition_metadata(&self) -> &[PartitionMetadata] {
        &self.partition_metadata
    }

    /// Returns the authorized operations bitfield.
    pub fn authorized_operations(&self) -> i32 {
        self.authorized_operations
    }

    /// Sets the authorized operations bitfield.
    pub fn set_authorized_operations(&mut self, authorized_operations: i32) {
        self.authorized_operations = authorized_operations;
    }
}

impl std::fmt::Display for TopicMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopicMetadata{{error={:?}, topic='{}', topicId='{}', isInternal={}, partitionMetadata={:?}, authorizedOperations={}}}",
            self.error,
            self.topic,
            self.topic_id,
            self.is_internal,
            self.partition_metadata,
            self.authorized_operations
        )
    }
}

impl std::hash::Hash for TopicMetadata {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.error.hash(state);
        self.topic.hash(state);
        self.is_internal.hash(state);
        self.partition_metadata.hash(state);
        self.authorized_operations.hash(state);
    }
}

/// Partition-level metadata from a MetadataResponse.
///
/// Corresponds to `MetadataResponse.PartitionMetadata` in Java.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionMetadata {
    /// The partition-level error.
    pub error: Errors,
    /// The topic-partition.
    pub topic_partition: TopicPartition,
    /// The leader node ID, or `None` if there is no leader.
    pub leader_id: Option<i32>,
    /// The leader epoch, or `None` if not known.
    pub leader_epoch: Option<i32>,
    /// Replica node IDs.
    pub replica_ids: Vec<i32>,
    /// In-sync replica node IDs.
    pub in_sync_replica_ids: Vec<i32>,
    /// Offline replica node IDs.
    pub offline_replica_ids: Vec<i32>,
}

impl PartitionMetadata {
    /// Returns the partition index.
    pub fn partition(&self) -> i32 {
        self.topic_partition.partition()
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        self.topic_partition.topic()
    }

    /// Returns a copy of this partition metadata without leader epoch information.
    pub fn without_leader_epoch(&self) -> Self {
        Self {
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

impl std::fmt::Display for PartitionMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PartitionMetadata(error={:?}, partition={}, leader={:?}, leaderEpoch={:?}, replicas={}, isr={}, offlineReplicas={})",
            self.error,
            self.topic_partition,
            self.leader_id,
            self.leader_epoch,
            self.replica_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(","),
            self.in_sync_replica_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(","),
            self.offline_replica_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Translated from `MetadataResponseTest.buildClusterTest`.
    #[test]
    fn test_build_cluster() {
        let zero_uuid = Uuid::new(0, 0);
        let random_uuid = Uuid::random_uuid();

        let mut topic_metadata1 = MetadataResponseTopic::new();
        topic_metadata1.set_name(Some("topic1".to_string()));
        topic_metadata1.set_error_code(Errors::None.code());
        topic_metadata1.set_partitions(Vec::new());
        topic_metadata1.set_is_internal(false);

        let mut topic_metadata2 = MetadataResponseTopic::new();
        topic_metadata2.set_name(Some("topic2".to_string()));
        topic_metadata2.set_error_code(Errors::None.code());
        topic_metadata2.set_topic_id(zero_uuid);
        topic_metadata2.set_partitions(Vec::new());
        topic_metadata2.set_is_internal(false);

        let mut topic_metadata3 = MetadataResponseTopic::new();
        topic_metadata3.set_name(Some("topic3".to_string()));
        topic_metadata3.set_error_code(Errors::None.code());
        topic_metadata3.set_topic_id(random_uuid);
        topic_metadata3.set_partitions(Vec::new());
        topic_metadata3.set_is_internal(false);

        let topics = vec![topic_metadata1, topic_metadata2, topic_metadata3];
        let mut data = MetadataResponseData::new();
        data.set_topics(topics);

        let metadata_response = MetadataResponse::new_version(data, ApiKeys::METADATA.latest_version());
        let cluster = metadata_response.build_cluster();
        assert!(cluster.topic_name(&Uuid::zero()).is_none());
        assert!(cluster.topic_name(&zero_uuid).is_none());
        assert_eq!(Some("topic3"), cluster.topic_name(&random_uuid));
    }
}
