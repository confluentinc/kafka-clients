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

//! An internal immutable snapshot of nodes, topics, and partitions in the Kafka cluster.
//!
//! Corresponds to `org.apache.kafka.clients.MetadataSnapshot`.
//!
//! This keeps an up-to-date [`Cluster`] instance which is optimized for read access.
//! Prefer to extend `MetadataSnapshot`'s API for internal client usage vs. the public
//! [`Cluster`].

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;

use crate::common::cluster::Cluster;
use crate::common::cluster_resource::ClusterResource;
use crate::common::node::Node;
use crate::common::requests::metadata_response::{MetadataResponse, PartitionMetadata};
use crate::common::topic_partition::TopicPartition;
use crate::common::uuid::Uuid;

/// An internal immutable snapshot of nodes, topics, and partitions in the Kafka cluster.
///
/// This keeps an up-to-date [`Cluster`] instance which is optimized for read access.
/// Prefer to extend `MetadataSnapshot`'s API for internal client usage vs. the public
/// [`Cluster`].
#[derive(Clone, Debug)]
pub struct MetadataSnapshot {
    cluster_id: Option<String>,
    nodes: HashMap<i32, Node>,
    unauthorized_topics: HashSet<String>,
    invalid_topics: HashSet<String>,
    internal_topics: HashSet<String>,
    controller: Option<Node>,
    metadata_by_partition: HashMap<TopicPartition, PartitionMetadata>,
    topic_ids: HashMap<String, Uuid>,
    topic_names: HashMap<Uuid, String>,
    cluster_instance: Cluster,
}

impl MetadataSnapshot {
    /// Creates a new `MetadataSnapshot`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_id: Option<String>,
        nodes: HashMap<i32, Node>,
        partitions: Vec<PartitionMetadata>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
        topic_ids: HashMap<String, Uuid>,
    ) -> Self {
        Self::new_with_cluster(
            cluster_id,
            nodes,
            partitions,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            topic_ids,
            None,
        )
    }

    /// Creates a new `MetadataSnapshot` with an optional pre-built cluster instance.
    ///
    /// Visible for testing.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_cluster(
        cluster_id: Option<String>,
        nodes: HashMap<i32, Node>,
        partitions: Vec<PartitionMetadata>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
        topic_ids: HashMap<String, Uuid>,
        cluster_instance: Option<Cluster>,
    ) -> Self {
        let topic_names: HashMap<Uuid, String> = topic_ids.iter().map(|(name, id)| (*id, name.clone())).collect();

        let mut metadata_by_partition = HashMap::with_capacity(partitions.len());
        for p in partitions {
            metadata_by_partition.insert(p.topic_partition.clone(), p);
        }

        let cluster_instance = cluster_instance.unwrap_or_else(|| {
            Self::compute_cluster_view(
                &cluster_id,
                &nodes,
                &metadata_by_partition,
                &unauthorized_topics,
                &invalid_topics,
                &internal_topics,
                &controller,
                &topic_ids,
            )
        });

        Self {
            cluster_id,
            nodes,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            metadata_by_partition,
            topic_ids,
            topic_names,
            cluster_instance,
        }
    }

    /// Returns the cached cluster instance.
    pub fn cluster(&self) -> &Cluster {
        &self.cluster_instance
    }

    /// Returns the partition metadata for the given topic partition.
    pub fn partition_metadata(&self, topic_partition: &TopicPartition) -> Option<&PartitionMetadata> {
        self.metadata_by_partition.get(topic_partition)
    }

    /// Returns the topic IDs mapping (topic name -> topic ID).
    pub fn topic_ids(&self) -> &HashMap<String, Uuid> {
        &self.topic_ids
    }

    /// Returns the topic names mapping (topic ID -> topic name).
    pub fn topic_names(&self) -> &HashMap<Uuid, String> {
        &self.topic_names
    }

    /// Returns the node with the given ID, if present.
    pub fn node_by_id(&self, id: i32) -> Option<&Node> {
        self.nodes.get(&id)
    }

    /// Gets the leader epoch for the given partition.
    ///
    /// Returns `None` if the partition is not found or the leader epoch is not set.
    pub fn leader_epoch_for(&self, tp: &TopicPartition) -> Option<i32> {
        self.metadata_by_partition.get(tp).and_then(|pm| pm.leader_epoch)
    }

    /// Returns the [`ClusterResource`] for this snapshot.
    pub fn cluster_resource(&self) -> ClusterResource {
        ClusterResource::new(self.cluster_id.clone())
    }

    /// Merges this snapshot with new metadata, returning a new snapshot.
    ///
    /// The provided metadata is presumed to be more recent, so all overlapping
    /// metadata will be overridden. The `retain_topic` predicate determines whether
    /// a pre-existing topic's metadata should be retained. It receives the topic name
    /// and whether the topic is internal.
    #[allow(clippy::too_many_arguments)]
    pub fn merge_with<F>(
        &self,
        new_cluster_id: Option<String>,
        new_nodes: HashMap<i32, Node>,
        add_partitions: Vec<PartitionMetadata>,
        add_unauthorized_topics: HashSet<String>,
        add_invalid_topics: HashSet<String>,
        add_internal_topics: HashSet<String>,
        new_controller: Option<Node>,
        add_topic_ids: HashMap<String, Uuid>,
        retain_topic: F,
    ) -> Self
    where
        F: Fn(&str, bool) -> bool,
    {
        let should_retain_topic = |topic: &str| retain_topic(topic, self.internal_topics.contains(topic));

        let mut new_metadata_by_partition = HashMap::with_capacity(add_partitions.len());

        // We want the most recent topic ID. We start with the previous ID stored for
        // retained topics and then update with newest information from the MetadataResponse.
        let mut new_topic_ids: HashMap<String, Uuid> = self
            .topic_ids
            .iter()
            .filter(|(topic, _)| should_retain_topic(topic))
            .map(|(k, v)| (k.clone(), *v))
            .collect();

        for partition in add_partitions {
            let topic = partition.topic().to_string();
            new_metadata_by_partition.insert(partition.topic_partition.clone(), partition);
            if let Some(id) = add_topic_ids.get(&topic) {
                new_topic_ids.insert(topic, *id);
            } else {
                // Remove if the latest metadata does not have a topic ID
                new_topic_ids.remove(&topic);
            }
        }

        for (tp, pm) in &self.metadata_by_partition {
            if should_retain_topic(tp.topic()) {
                new_metadata_by_partition.entry(tp.clone()).or_insert_with(|| pm.clone());
            }
        }

        let new_unauthorized_topics =
            Self::fill_set(&add_unauthorized_topics, &self.unauthorized_topics, &should_retain_topic);
        let new_invalid_topics = Self::fill_set(&add_invalid_topics, &self.invalid_topics, &should_retain_topic);
        let new_internal_topics = Self::fill_set(&add_internal_topics, &self.internal_topics, &should_retain_topic);

        let partitions: Vec<PartitionMetadata> = new_metadata_by_partition.into_values().collect();

        Self::new(
            new_cluster_id,
            new_nodes,
            partitions,
            new_unauthorized_topics,
            new_invalid_topics,
            new_internal_topics,
            new_controller,
            new_topic_ids,
        )
    }

    /// Creates a bootstrap metadata snapshot from a list of addresses.
    pub fn bootstrap(addresses: &[SocketAddr]) -> Self {
        let mut nodes = HashMap::new();
        let mut node_id: i32 = -1;
        for address in addresses {
            nodes.insert(node_id, Node::new(node_id, address.ip().to_string(), address.port() as i32));
            node_id -= 1;
        }
        Self::new_with_cluster(
            None,
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
            Some(Cluster::bootstrap(addresses)),
        )
    }

    /// Creates an empty metadata snapshot.
    pub fn empty() -> Self {
        Self::new_with_cluster(
            None,
            HashMap::new(),
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
            Some(Cluster::empty()),
        )
    }

    /// Copies `base_set` and adds all non-existent elements in `fill_set` for which
    /// the predicate returns `true`.
    fn fill_set<F>(base_set: &HashSet<String>, fill_set: &HashSet<String>, predicate: &F) -> HashSet<String>
    where
        F: Fn(&str) -> bool,
    {
        let mut result = base_set.clone();
        for element in fill_set {
            if predicate(element) {
                result.insert(element.clone());
            }
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_cluster_view(
        cluster_id: &Option<String>,
        nodes: &HashMap<i32, Node>,
        metadata_by_partition: &HashMap<TopicPartition, PartitionMetadata>,
        unauthorized_topics: &HashSet<String>,
        invalid_topics: &HashSet<String>,
        internal_topics: &HashSet<String>,
        controller: &Option<Node>,
        topic_ids: &HashMap<String, Uuid>,
    ) -> Cluster {
        let partition_infos: Vec<_> = metadata_by_partition
            .values()
            .map(|metadata| MetadataResponse::to_partition_info(metadata, nodes))
            .collect();

        Cluster::new(
            cluster_id.clone(),
            nodes.values().cloned().collect(),
            partition_infos,
            unauthorized_topics.clone(),
            invalid_topics.clone(),
            internal_topics.clone(),
            controller.clone(),
            topic_ids.clone(),
        )
    }
}

impl fmt::Display for MetadataSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MetadataSnapshot{{clusterId={:?}, nodes={:?}, partitions={:?}, controller={:?}}}",
            self.cluster_id,
            self.nodes,
            self.metadata_by_partition.values().collect::<Vec<_>>(),
            self.controller,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::Errors;

    /// Translated from `MetadataSnapshotTest.testMissingLeaderEndpoint`.
    #[test]
    fn test_missing_leader_endpoint() {
        let topic_partition = TopicPartition::new("topic".to_string(), 0);

        let partition_metadata = PartitionMetadata {
            error: Errors::None,
            topic_partition: topic_partition.clone(),
            leader_id: Some(5),
            leader_epoch: Some(10),
            replica_ids: vec![5, 6, 7],
            in_sync_replica_ids: vec![5, 6, 7],
            offline_replica_ids: Vec::new(),
        };

        let mut nodes_by_id = HashMap::new();
        nodes_by_id.insert(6, Node::new(6, "localhost".to_string(), 2077));
        nodes_by_id.insert(7, Node::new(7, "localhost".to_string(), 2078));
        nodes_by_id.insert(8, Node::new(8, "localhost".to_string(), 2079));

        let cache = MetadataSnapshot::new(
            Some("clusterId".to_string()),
            nodes_by_id.clone(),
            vec![partition_metadata],
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        let cluster = cache.cluster();
        assert!(cluster.leader_for(&topic_partition).is_none());

        let partition_info = cluster.partition(&topic_partition).unwrap();
        let replicas: HashMap<i32, &Node> = partition_info.replicas().iter().map(|n| (n.id(), n)).collect();
        assert!(partition_info.leader().is_none());
        assert_eq!(3, replicas.len());
        assert!(replicas[&5].is_empty());
        assert_eq!(&nodes_by_id[&6], *replicas.get(&6).unwrap());
        assert_eq!(&nodes_by_id[&7], *replicas.get(&7).unwrap());
    }

    /// Translated from `MetadataSnapshotTest.testMergeWithThatPreExistingPartitionIsRetainedPostMerge`.
    #[test]
    fn test_merge_with_pre_existing_partition_is_retained_post_merge() {
        let topic1 = "topic1";
        let topic1_partition = TopicPartition::new(topic1.to_string(), 1);
        let partition_metadata1 = PartitionMetadata {
            error: Errors::None,
            topic_partition: topic1_partition.clone(),
            leader_id: Some(5),
            leader_epoch: Some(10),
            replica_ids: vec![5, 6, 7],
            in_sync_replica_ids: vec![5, 6, 7],
            offline_replica_ids: Vec::new(),
        };

        let mut nodes_by_id = HashMap::new();
        nodes_by_id.insert(6, Node::new(6, "localhost".to_string(), 2077));
        nodes_by_id.insert(7, Node::new(7, "localhost".to_string(), 2078));
        nodes_by_id.insert(8, Node::new(8, "localhost".to_string(), 2079));

        let topic1_id = Uuid::random_uuid();
        let mut topic_ids = HashMap::new();
        topic_ids.insert(topic1.to_string(), topic1_id);

        let cache = MetadataSnapshot::new(
            Some("clusterId".to_string()),
            nodes_by_id.clone(),
            vec![partition_metadata1],
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            topic_ids,
        );

        let cluster = cache.cluster();
        assert_eq!(1, cluster.topics().count());
        assert_eq!(topic1_id, cluster.topic_id(topic1));
        assert_eq!(Some(topic1), cluster.topic_name(&topic1_id));

        // Merge with a new partition for topic2
        let topic2 = "topic2";
        let topic2_partition = TopicPartition::new(topic2.to_string(), 2);
        let partition_metadata2 = PartitionMetadata {
            error: Errors::None,
            topic_partition: topic2_partition,
            leader_id: Some(5),
            leader_epoch: Some(10),
            replica_ids: vec![5, 6, 7],
            in_sync_replica_ids: vec![5, 6, 7],
            offline_replica_ids: Vec::new(),
        };

        let topic2_id = Uuid::random_uuid();
        let mut add_topic_ids = HashMap::new();
        add_topic_ids.insert(topic2.to_string(), topic2_id);

        let cache = cache.merge_with(
            Some("clusterId".to_string()),
            nodes_by_id,
            vec![partition_metadata2],
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            add_topic_ids,
            |_topic, _retain| true,
        );

        let cluster = cache.cluster();
        // Both topics should be present
        assert_eq!(2, cluster.topics().count());

        assert_eq!(topic1_id, cluster.topic_id(topic1));
        assert_eq!(Some(topic1), cluster.topic_name(&topic1_id));

        assert_eq!(topic2_id, cluster.topic_id(topic2));
        assert_eq!(Some(topic2), cluster.topic_name(&topic2_id));
    }

    /// Translated from `MetadataSnapshotTest.testTopicNamesCacheBuiltFromTopicIds`.
    #[test]
    fn test_topic_names_cache_built_from_topic_ids() {
        let mut topic_ids = HashMap::new();
        topic_ids.insert("topic1".to_string(), Uuid::random_uuid());
        topic_ids.insert("topic2".to_string(), Uuid::random_uuid());

        let mut nodes = HashMap::new();
        nodes.insert(6, Node::new(6, "localhost".to_string(), 2077));

        let cache = MetadataSnapshot::new(
            Some("clusterId".to_string()),
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            topic_ids.clone(),
        );

        let expected_names: HashMap<Uuid, String> = topic_ids.iter().map(|(name, id)| (*id, name.clone())).collect();
        assert_eq!(&expected_names, cache.topic_names());
    }

    /// Translated from `MetadataSnapshotTest.testEmptyTopicNamesCacheBuiltFromTopicIds`.
    #[test]
    fn test_empty_topic_names_cache_built_from_topic_ids() {
        let mut nodes = HashMap::new();
        nodes.insert(6, Node::new(6, "localhost".to_string(), 2077));

        let cache = MetadataSnapshot::new(
            Some("clusterId".to_string()),
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        assert!(cache.topic_names().is_empty());
    }

    /// Translated from `MetadataSnapshotTest.testLeaderEpochFor`.
    #[test]
    fn test_leader_epoch_for() {
        // Partition 0 with leader epoch of 10
        let tp1 = TopicPartition::new("topic".to_string(), 0);
        let pm1 = PartitionMetadata {
            error: Errors::None,
            topic_partition: tp1.clone(),
            leader_id: Some(5),
            leader_epoch: Some(10),
            replica_ids: vec![5, 6, 7],
            in_sync_replica_ids: vec![5, 6, 7],
            offline_replica_ids: Vec::new(),
        };

        // Partition 1 with unknown leader epoch
        let tp2 = TopicPartition::new("topic".to_string(), 1);
        let pm2 = PartitionMetadata {
            error: Errors::None,
            topic_partition: tp2.clone(),
            leader_id: Some(5),
            leader_epoch: None,
            replica_ids: vec![5, 6, 7],
            in_sync_replica_ids: vec![5, 6, 7],
            offline_replica_ids: Vec::new(),
        };

        let mut nodes_by_id = HashMap::new();
        nodes_by_id.insert(5, Node::new(5, "localhost".to_string(), 2077));
        nodes_by_id.insert(6, Node::new(6, "localhost".to_string(), 2078));
        nodes_by_id.insert(7, Node::new(7, "localhost".to_string(), 2079));

        let cache = MetadataSnapshot::new(
            Some("clusterId".to_string()),
            nodes_by_id,
            vec![pm1, pm2],
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        assert_eq!(Some(10), cache.leader_epoch_for(&tp1));
        assert_eq!(None, cache.leader_epoch_for(&tp2));
        assert_eq!(
            None,
            cache.leader_epoch_for(&TopicPartition::new("topic_missing".to_string(), 0))
        );
    }
}
