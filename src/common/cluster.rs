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

//! An immutable representation of a subset of the nodes, topics, and partitions in the Kafka cluster.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;

use super::cluster_resource::ClusterResource;
use super::node::Node;
use super::partition_info::PartitionInfo;
use super::topic_partition::TopicPartition;
use super::uuid::Uuid;

/// An immutable representation of a subset of the nodes, topics, and partitions
/// in the Kafka cluster.
#[derive(Clone, Debug)]
pub struct Cluster {
    is_bootstrap_configured: bool,
    nodes: Vec<Node>,
    unauthorized_topics: HashSet<String>,
    invalid_topics: HashSet<String>,
    internal_topics: HashSet<String>,
    controller: Option<Node>,
    partitions_by_topic_partition: HashMap<TopicPartition, PartitionInfo>,
    partitions_by_topic: HashMap<String, Vec<PartitionInfo>>,
    available_partitions_by_topic: HashMap<String, Vec<PartitionInfo>>,
    partitions_by_node: HashMap<i32, Vec<PartitionInfo>>,
    nodes_by_id: HashMap<i32, Node>,
    cluster_resource: ClusterResource,
    topic_ids: HashMap<String, Uuid>,
    topic_names: HashMap<Uuid, String>,
}

impl Cluster {
    /// Create a new cluster with the given id, nodes and partitions.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_id: Option<String>,
        nodes: Vec<Node>,
        partitions: Vec<PartitionInfo>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
        topic_ids: HashMap<String, Uuid>,
    ) -> Self {
        Self::new_internal(
            cluster_id,
            false,
            nodes,
            partitions,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            topic_ids,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_internal(
        cluster_id: Option<String>,
        is_bootstrap_configured: bool,
        nodes: Vec<Node>,
        partitions: Vec<PartitionInfo>,
        unauthorized_topics: HashSet<String>,
        invalid_topics: HashSet<String>,
        internal_topics: HashSet<String>,
        controller: Option<Node>,
        topic_ids: HashMap<String, Uuid>,
    ) -> Self {
        let cluster_resource = ClusterResource::new(cluster_id);

        // Index the nodes for quick lookup
        let mut nodes_by_id = HashMap::with_capacity(nodes.len());
        let mut partitions_by_node: HashMap<i32, Vec<PartitionInfo>> = HashMap::with_capacity(nodes.len());
        for node in &nodes {
            nodes_by_id.insert(node.id(), node.clone());
            partitions_by_node.insert(node.id(), Vec::new());
        }

        // Index the partition infos by topic, topic+partition, and node
        let mut partitions_by_topic_partition = HashMap::with_capacity(partitions.len());
        let mut partitions_by_topic: HashMap<String, Vec<PartitionInfo>> = HashMap::new();
        for p in &partitions {
            let tp = TopicPartition::new(p.topic().to_string(), p.partition());
            partitions_by_topic_partition.insert(tp, p.clone());
            partitions_by_topic.entry(p.topic().to_string()).or_default().push(p.clone());

            // The leader may not be known
            if let Some(leader) = p.leader() {
                if leader.is_empty() {
                    continue;
                }
                if let Some(parts) = partitions_by_node.get_mut(&leader.id()) {
                    parts.push(p.clone());
                }
            }
        }

        // Populate available partitions by topic
        let mut available_partitions_by_topic = HashMap::with_capacity(partitions_by_topic.len());
        for (topic, topic_partitions) in &partitions_by_topic {
            let has_unavailable = topic_partitions.iter().any(|p| p.leader().is_none());
            if has_unavailable {
                let available: Vec<PartitionInfo> =
                    topic_partitions.iter().filter(|p| p.leader().is_some()).cloned().collect();
                available_partitions_by_topic.insert(topic.clone(), available);
            } else {
                available_partitions_by_topic.insert(topic.clone(), topic_partitions.clone());
            }
        }

        // Build reverse topic_names map
        let topic_names: HashMap<Uuid, String> = topic_ids.iter().map(|(name, id)| (*id, name.clone())).collect();

        Self {
            is_bootstrap_configured,
            nodes,
            unauthorized_topics,
            invalid_topics,
            internal_topics,
            controller,
            partitions_by_topic_partition,
            partitions_by_topic,
            available_partitions_by_topic,
            partitions_by_node,
            nodes_by_id,
            cluster_resource,
            topic_ids,
            topic_names,
        }
    }

    /// Create an empty cluster instance with no nodes and no topic-partitions.
    pub fn empty() -> Self {
        Self::new(
            None,
            Vec::new(),
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        )
    }

    /// Create a "bootstrap" cluster using the given list of socket addresses.
    pub fn bootstrap(addresses: &[SocketAddr]) -> Self {
        let mut nodes = Vec::with_capacity(addresses.len());
        let mut node_id: i32 = -1;
        for address in addresses {
            nodes.push(Node::new(node_id, address.ip().to_string(), address.port() as i32));
            node_id -= 1;
        }
        Self::new_internal(
            None,
            true,
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        )
    }

    /// Return a copy of this cluster combined with additional partitions.
    pub fn with_partitions(&self, partitions: HashMap<TopicPartition, PartitionInfo>) -> Self {
        let mut combined = self.partitions_by_topic_partition.clone();
        combined.extend(partitions);
        let all_partitions: Vec<PartitionInfo> = combined.into_values().collect();
        Self::new(
            self.cluster_resource.cluster_id().map(|s| s.to_string()),
            self.nodes.clone(),
            all_partitions,
            self.unauthorized_topics.clone(),
            self.invalid_topics.clone(),
            self.internal_topics.clone(),
            self.controller.clone(),
            self.topic_ids.clone(),
        )
    }

    /// The known set of nodes.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Get the node by the node id (or `None` if the node is not online or does not exist).
    pub fn node_by_id(&self, id: i32) -> Option<&Node> {
        self.nodes_by_id.get(&id)
    }

    /// Get the node by node id if the replica for the given partition is online.
    pub fn node_if_online(&self, partition: &TopicPartition, id: i32) -> Option<&Node> {
        let node = self.nodes_by_id.get(&id)?;
        let info = self.partitions_by_topic_partition.get(partition)?;

        let is_offline = info.offline_replicas().iter().any(|n| n.id() == node.id());
        let is_replica = info.replicas().iter().any(|n| n.id() == node.id());

        if !is_offline && is_replica { Some(node) } else { None }
    }

    /// Get the current leader for the given topic-partition.
    pub fn leader_for(&self, topic_partition: &TopicPartition) -> Option<&Node> {
        self.partitions_by_topic_partition
            .get(topic_partition)
            .and_then(|info| info.leader())
    }

    /// Get the metadata for the specified partition.
    pub fn partition(&self, topic_partition: &TopicPartition) -> Option<&PartitionInfo> {
        self.partitions_by_topic_partition.get(topic_partition)
    }

    /// Get the list of partitions for this topic.
    pub fn partitions_for_topic(&self, topic: &str) -> &[PartitionInfo] {
        self.partitions_by_topic.get(topic).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get the number of partitions for the given topic.
    pub fn partition_count_for_topic(&self, topic: &str) -> Option<usize> {
        self.partitions_by_topic.get(topic).map(|v| v.len())
    }

    /// Get the list of available partitions for this topic.
    pub fn available_partitions_for_topic(&self, topic: &str) -> &[PartitionInfo] {
        self.available_partitions_by_topic
            .get(topic)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get the list of partitions whose leader is this node.
    pub fn partitions_for_node(&self, node_id: i32) -> &[PartitionInfo] {
        self.partitions_by_node.get(&node_id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get all topics.
    pub fn topics(&self) -> impl Iterator<Item = &str> {
        self.partitions_by_topic.keys().map(|s| s.as_str())
    }

    /// Unauthorized topics.
    pub fn unauthorized_topics(&self) -> &HashSet<String> {
        &self.unauthorized_topics
    }

    /// Invalid topics.
    pub fn invalid_topics(&self) -> &HashSet<String> {
        &self.invalid_topics
    }

    /// Internal topics.
    pub fn internal_topics(&self) -> &HashSet<String> {
        &self.internal_topics
    }

    /// Whether bootstrap is configured.
    pub fn is_bootstrap_configured(&self) -> bool {
        self.is_bootstrap_configured
    }

    /// The cluster resource metadata.
    pub fn cluster_resource(&self) -> &ClusterResource {
        &self.cluster_resource
    }

    /// The controller node, if known.
    pub fn controller(&self) -> Option<&Node> {
        self.controller.as_ref()
    }

    /// All topic IDs.
    pub fn topic_ids(&self) -> impl Iterator<Item = &Uuid> {
        self.topic_ids.values()
    }

    /// Get the topic ID for a given topic name.
    pub fn topic_id(&self, topic: &str) -> Uuid {
        self.topic_ids.get(topic).copied().unwrap_or(Uuid::zero())
    }

    /// Get the topic name for a given topic ID.
    pub fn topic_name(&self, topic_id: &Uuid) -> Option<&str> {
        self.topic_names.get(topic_id).map(|s| s.as_str())
    }
}

impl fmt::Display for Cluster {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Cluster(id = {:?}, nodes = {:?}, partitions = {:?}, controller = {:?})",
            self.cluster_resource.cluster_id(),
            self.nodes,
            self.partitions_by_topic_partition.values().collect::<Vec<_>>(),
            self.controller,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: i32) -> Node {
        Node::new(id, format!("host{id}"), 9092)
    }

    fn make_partition(topic: &str, partition: i32, leader_id: i32) -> PartitionInfo {
        let leader = make_node(leader_id);
        let replicas = vec![make_node(leader_id)];
        let isr = vec![make_node(leader_id)];
        PartitionInfo::new(topic.to_string(), partition, Some(leader), replicas, isr)
    }

    #[test]
    fn test_empty_cluster() {
        let cluster = Cluster::empty();
        assert!(cluster.nodes().is_empty());
        assert_eq!(cluster.topics().count(), 0);
    }

    #[test]
    fn test_bootstrap_cluster() {
        let addrs: Vec<SocketAddr> = vec!["127.0.0.1:9092".parse().unwrap(), "127.0.0.1:9093".parse().unwrap()];
        let cluster = Cluster::bootstrap(&addrs);
        assert!(cluster.is_bootstrap_configured());
        assert_eq!(cluster.nodes().len(), 2);
        assert_eq!(cluster.nodes()[0].id(), -1);
        assert_eq!(cluster.nodes()[1].id(), -2);
    }

    #[test]
    fn test_cluster_with_partitions() {
        let nodes = vec![make_node(0), make_node(1)];
        let partitions = vec![make_partition("test", 0, 0), make_partition("test", 1, 1)];
        let cluster = Cluster::new(
            Some("cluster1".to_string()),
            nodes,
            partitions,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        assert_eq!(cluster.partitions_for_topic("test").len(), 2);
        assert_eq!(cluster.partition_count_for_topic("test"), Some(2));
        assert_eq!(cluster.partition_count_for_topic("nonexistent"), None);
        assert_eq!(cluster.available_partitions_for_topic("test").len(), 2);
    }

    #[test]
    fn test_node_by_id() {
        let nodes = vec![make_node(0), make_node(1)];
        let cluster = Cluster::new(
            None,
            nodes,
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        assert!(cluster.node_by_id(0).is_some());
        assert!(cluster.node_by_id(1).is_some());
        assert!(cluster.node_by_id(99).is_none());
    }

    #[test]
    fn test_leader_for() {
        let nodes = vec![make_node(0)];
        let partitions = vec![make_partition("test", 0, 0)];
        let cluster = Cluster::new(
            None,
            nodes,
            partitions,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        let tp = TopicPartition::new("test".to_string(), 0);
        let leader = cluster.leader_for(&tp).unwrap();
        assert_eq!(leader.id(), 0);

        let tp2 = TopicPartition::new("test".to_string(), 99);
        assert!(cluster.leader_for(&tp2).is_none());
    }

    #[test]
    fn test_topic_ids() {
        let mut topic_ids = HashMap::new();
        topic_ids.insert("test".to_string(), Uuid::random_uuid());

        let cluster = Cluster::new(
            None,
            Vec::new(),
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            topic_ids.clone(),
        );

        let id = topic_ids["test"];
        assert_eq!(cluster.topic_id("test"), id);
        assert_eq!(cluster.topic_name(&id), Some("test"));
        assert_eq!(cluster.topic_id("nonexistent"), Uuid::zero());
    }

    #[test]
    fn test_controller() {
        let controller = make_node(0);
        let cluster = Cluster::new(
            None,
            vec![make_node(0)],
            Vec::new(),
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            Some(controller),
            HashMap::new(),
        );

        assert_eq!(cluster.controller().unwrap().id(), 0);
    }

    #[test]
    fn test_partitions_for_node() {
        let nodes = vec![make_node(0), make_node(1)];
        let partitions = vec![
            make_partition("test", 0, 0),
            make_partition("test", 1, 0),
            make_partition("test", 2, 1),
        ];
        let cluster = Cluster::new(
            None,
            nodes,
            partitions,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            None,
            HashMap::new(),
        );

        assert_eq!(cluster.partitions_for_node(0).len(), 2);
        assert_eq!(cluster.partitions_for_node(1).len(), 1);
        assert_eq!(cluster.partitions_for_node(99).len(), 0);
    }
}
